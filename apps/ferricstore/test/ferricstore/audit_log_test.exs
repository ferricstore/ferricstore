defmodule Ferricstore.AuditLogTest do
  @moduledoc """
  Tests for the Ferricstore.AuditLog GenServer.

  Covers the ETS ring buffer, event logging, eviction, enable/disable
  toggle, formatting, and integration with CONFIG SET, FLUSHDB/FLUSHALL,
  and DEBUG commands.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.AuditLog
  alias Ferricstore.Test.ShardHelpers

  # Reset audit log state before each test to prevent cross-test leakage.
  setup do
    ShardHelpers.flush_all_keys()
    AuditLog.reset()
    # Enable audit logging for every test by default.
    Application.put_env(:ferricstore, :audit_log_enabled, true)

    on_exit(fn ->
      # Restore default (disabled) after tests.
      Application.put_env(:ferricstore, :audit_log_enabled, false)
      Application.put_env(:ferricstore, :audit_log_max_entries, 128)

      # Reset any config params changed by tests (e.g. hz) to defaults
      # so subsequent test modules see clean state.
      defaults = Ferricstore.Config.defaults()

      Enum.each(defaults, fn {k, v} ->
        try do
          Ferricstore.Config.set(k, v)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
        end
      end)
    end)
  end

  # ---------------------------------------------------------------------------
  # GenServer lifecycle
  # ---------------------------------------------------------------------------

  describe "GenServer lifecycle" do
    test "AuditLog process is alive after application start" do
      pid = Process.whereis(AuditLog)
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "ETS table :ferricstore_audit_log exists" do
      assert :ets.whereis(:ferricstore_audit_log) != :undefined
    end

    test "uses ordered traversal instead of copying and sorting the complete ring" do
      assert :ets.info(:ferricstore_audit_log, :type) == :ordered_set

      source = File.read!(Path.expand("../../lib/ferricstore/audit_log.ex", __DIR__))
      refute source =~ ":ets.tab2list"
      refute source =~ "Enum.sort_by"
    end

    test "responds to ping" do
      assert GenServer.call(AuditLog, :ping) == :pong
    end
  end

  # ---------------------------------------------------------------------------
  # Logging events
  # ---------------------------------------------------------------------------

  describe "log/2" do
    test "logs an auth_success event" do
      AuditLog.log(:auth_success, %{username: "default", client_ip: "127.0.0.1:1234"})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, event_type, details}] = AuditLog.get()
      assert event_type == :auth_success
      assert details.username == "default"
      assert details.client_ip == "127.0.0.1:1234"
    end

    test "logs an auth_failure event" do
      AuditLog.log(:auth_failure, %{username: "admin", client_ip: "10.0.0.1:5678"})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, event_type, details}] = AuditLog.get()
      assert event_type == :auth_failure
      assert details.username == "admin"
    end

    test "logs a config_change event" do
      AuditLog.log(:config_change, %{parameter: "hz", old_value: "10", new_value: "100"})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, :config_change, details}] = AuditLog.get()
      assert details.parameter == "hz"
      assert details.old_value == "10"
      assert details.new_value == "100"
    end

    test "logs a connection_open event" do
      AuditLog.log(:connection_open, %{client_id: 42, client_ip: "192.168.1.1:9999"})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, :connection_open, details}] = AuditLog.get()
      assert details.client_id == 42
      assert details.client_ip == "192.168.1.1:9999"
    end

    test "logs a connection_close event with duration" do
      AuditLog.log(:connection_close, %{
        client_id: 42,
        client_ip: "192.168.1.1:9999",
        duration_ms: 5000
      })

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, :connection_close, details}] = AuditLog.get()
      assert details.duration_ms == 5000
    end

    test "logs a dangerous_command event" do
      AuditLog.log(:dangerous_command, %{command: "FLUSHDB", args: []})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "audit entry not recorded",
        20,
        10
      )

      [{_id, _ts, :dangerous_command, details}] = AuditLog.get()
      assert details.command == "FLUSHDB"
    end

    test "is a no-op when audit logging is disabled" do
      Application.put_env(:ferricstore, :audit_log_enabled, false)

      AuditLog.log(:auth_success, %{username: "default", client_ip: "127.0.0.1:1234"})
      # Give the GenServer time to process the cast (it should be a no-op)
      ShardHelpers.eventually(
        fn ->
          # After a brief wait, len should still be 0
          AuditLog.len() == 0
        end,
        "audit log should remain empty when disabled",
        10,
        10
      )
    end

    test "returns :ok regardless of enabled state" do
      Application.put_env(:ferricstore, :audit_log_enabled, false)
      assert AuditLog.log(:auth_success, %{}) == :ok

      Application.put_env(:ferricstore, :audit_log_enabled, true)
      assert AuditLog.log(:auth_success, %{}) == :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Retrieval
  # ---------------------------------------------------------------------------

  describe "get/1" do
    test "returns entries newest first" do
      for i <- 1..5 do
        AuditLog.log(:auth_success, %{seq: i})
        Process.sleep(5)
      end

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 5
        end,
        "all 5 entries not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      assert length(entries) == 5

      ids = Enum.map(entries, fn {id, _, _, _} -> id end)
      assert ids == Enum.sort(ids, :desc)
    end

    test "returns at most count entries when count is given" do
      for i <- 1..10 do
        AuditLog.log(:auth_success, %{seq: i})
        Process.sleep(2)
      end

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 10
        end,
        "all 10 entries not recorded",
        20,
        10
      )

      entries = AuditLog.get(3)
      assert length(entries) == 3

      # Should be the 3 most recent
      ids = Enum.map(entries, fn {id, _, _, _} -> id end)
      assert ids == Enum.sort(ids, :desc)
    end

    test "returns all entries when count exceeds log size" do
      AuditLog.log(:auth_success, %{seq: 1})
      AuditLog.log(:auth_success, %{seq: 2})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 2
        end,
        "entries not recorded",
        20,
        10
      )

      entries = AuditLog.get(100)
      assert length(entries) == 2
    end

    test "returns empty list when log is empty" do
      assert AuditLog.get() == []
      assert AuditLog.get(10) == []
    end
  end

  # ---------------------------------------------------------------------------
  # len/0
  # ---------------------------------------------------------------------------

  describe "len/0" do
    test "returns 0 for empty log" do
      assert AuditLog.len() == 0
    end

    test "returns correct count after logging" do
      AuditLog.log(:auth_success, %{})
      AuditLog.log(:auth_failure, %{})
      AuditLog.log(:config_change, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 3
        end,
        "3 entries not recorded",
        20,
        10
      )
    end
  end

  # ---------------------------------------------------------------------------
  # reset/0
  # ---------------------------------------------------------------------------

  describe "reset/0" do
    test "clears all entries" do
      for _ <- 1..5 do
        AuditLog.log(:auth_success, %{})
        Process.sleep(2)
      end

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() > 0
        end,
        "entries not recorded",
        20,
        10
      )

      AuditLog.reset()
      assert AuditLog.len() == 0
      assert AuditLog.get() == []
    end

    test "resets ID counter so new entries start from 0" do
      AuditLog.log(:auth_success, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "entry not recorded",
        20,
        10
      )

      [{id_before, _, _, _}] = AuditLog.get()
      assert id_before == 0

      AuditLog.reset()
      AuditLog.log(:auth_success, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "entry not recorded after reset",
        20,
        10
      )

      [{id_after, _, _, _}] = AuditLog.get()
      assert id_after == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Ring buffer eviction
  # ---------------------------------------------------------------------------

  describe "ring buffer eviction" do
    test "evicts oldest entries when max_entries is exceeded" do
      Application.put_env(:ferricstore, :audit_log_max_entries, 5)

      for i <- 1..10 do
        AuditLog.log(:auth_success, %{seq: i})
        Process.sleep(5)
      end

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()

          length(entries) == 5 and
            Enum.map(entries, fn {_, _, _, details} -> details.seq end) |> Enum.sort() == [
              6,
              7,
              8,
              9,
              10
            ]
        end,
        "eviction not complete",
        20,
        10
      )

      assert AuditLog.len() == 5

      entries = AuditLog.get()
      seqs = Enum.map(entries, fn {_, _, _, details} -> details.seq end)
      # The 5 most recent entries (seq 6..10) should remain
      assert Enum.sort(seqs) == [6, 7, 8, 9, 10]
    end

    test "respects configured max_entries" do
      Application.put_env(:ferricstore, :audit_log_max_entries, 3)

      for i <- 1..8 do
        AuditLog.log(:dangerous_command, %{command: "FLUSHDB", seq: i})
        Process.sleep(5)
      end

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 3
        end,
        "eviction to max_entries not complete",
        20,
        10
      )
    end
  end

  # ---------------------------------------------------------------------------
  # enabled?/0 and max_entries/0
  # ---------------------------------------------------------------------------

  describe "enabled?/0" do
    test "returns true when :audit_log_enabled is true" do
      Application.put_env(:ferricstore, :audit_log_enabled, true)
      assert AuditLog.enabled?() == true
    end

    test "returns false when :audit_log_enabled is false" do
      Application.put_env(:ferricstore, :audit_log_enabled, false)
      assert AuditLog.enabled?() == false
    end

    test "defaults to false when not configured" do
      Application.delete_env(:ferricstore, :audit_log_enabled)
      assert AuditLog.enabled?() == false
    end
  end

  describe "max_entries/0" do
    test "returns configured value" do
      Application.put_env(:ferricstore, :audit_log_max_entries, 256)
      assert AuditLog.max_entries() == 256
    end

    test "defaults to 128 when not configured" do
      Application.delete_env(:ferricstore, :audit_log_max_entries)
      assert AuditLog.max_entries() == 128
    end

    test "falls back safely for invalid values" do
      for invalid <- [-1, "unbounded", nil] do
        Application.put_env(:ferricstore, :audit_log_max_entries, invalid)
        assert AuditLog.max_entries() == 128
      end
    end
  end

  test "copies and bounds binary metadata instead of retaining request buffers" do
    request = :binary.copy("x", 1_000_000) <> "user"
    username = binary_part(request, 1_000_000, 4)
    AuditLog.log(:auth_success, %{username: username, payload: request})

    ShardHelpers.eventually(
      fn -> AuditLog.len() == 1 end,
      "entry not recorded",
      20,
      10
    )

    [{_, _, :auth_success, details}] = AuditLog.get()
    assert details.username == "user"
    assert :binary.referenced_byte_size(details.username) == byte_size(details.username)
    assert byte_size(details.payload) < 512
    assert :binary.referenced_byte_size(details.payload) == byte_size(details.payload)
  end

  # ---------------------------------------------------------------------------
  # format_entries/1
  # ---------------------------------------------------------------------------

  describe "format_entries/1" do
    test "formats entries into list-of-lists structure" do
      AuditLog.log(:auth_success, %{username: "default", client_ip: "127.0.0.1:80"})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      formatted = AuditLog.format_entries(entries)

      assert length(formatted) == 1
      [entry] = formatted
      [id, timestamp, event_type, details_str] = entry
      assert is_integer(id)
      assert is_integer(timestamp)
      assert event_type == "auth_success"
      assert is_binary(details_str)
      assert details_str =~ "username="
      assert details_str =~ "client_ip="
    end

    test "formats empty details as empty string" do
      AuditLog.log(:auth_success, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      [[_id, _ts, _type, details_str]] = AuditLog.format_entries(entries)
      assert details_str == ""
    end

    test "formats multiple entries preserving order" do
      AuditLog.log(:auth_success, %{seq: 1})
      Process.sleep(5)
      AuditLog.log(:auth_failure, %{seq: 2})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 2
        end,
        "entries not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      formatted = AuditLog.format_entries(entries)
      assert length(formatted) == 2

      types = Enum.map(formatted, fn [_, _, type, _] -> type end)
      # Newest first
      assert types == ["auth_failure", "auth_success"]
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: CONFIG SET logs config_change
  # ---------------------------------------------------------------------------

  describe "CONFIG SET integration" do
    test "CONFIG SET logs a config_change audit event" do
      AuditLog.reset()
      store = build_test_store()
      # Route through the Server handler, which is where CONFIG SET audit logging lives.
      Ferricstore.Commands.Server.handle("CONFIG", ["SET", "hz", "50"], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          Enum.any?(entries, fn {_, _, type, _} -> type == :config_change end)
        end,
        "config_change audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      config_entries = Enum.filter(entries, fn {_, _, type, _} -> type == :config_change end)

      assert config_entries != []
      {_, _, :config_change, details} = List.first(config_entries)
      assert details.parameter == "hz"
      assert details.new_value == "50"
    end

    test "CONFIG SET records old_value before change" do
      AuditLog.reset()
      store = build_test_store()
      # First set hz to a known value
      Ferricstore.Commands.Server.handle("CONFIG", ["SET", "hz", "20"], store)

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() >= 1
        end,
        "first config_change not recorded",
        20,
        10
      )

      AuditLog.reset()

      # Now change it and verify old_value is captured
      Ferricstore.Commands.Server.handle("CONFIG", ["SET", "hz", "100"], store)

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() >= 1
        end,
        "second config_change not recorded",
        20,
        10
      )

      [{_, _, :config_change, details}] = AuditLog.get()
      assert details.old_value == "20"
      assert details.new_value == "100"
    end

    test "CONFIG SET does not log on validation failure" do
      AuditLog.reset()
      store = build_test_store()
      Ferricstore.Commands.Server.handle("CONFIG", ["SET", "hz", "not_a_number"], store)

      # Give the GenServer time to process
      ShardHelpers.eventually(
        fn ->
          # We expect no config_change entries
          config_entries =
            AuditLog.get()
            |> Enum.filter(fn {_, _, type, _} -> type == :config_change end)

          config_entries == []
        end,
        "should have no config_change entries",
        10,
        20
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: FLUSHDB/FLUSHALL log dangerous_command
  # ---------------------------------------------------------------------------

  describe "FLUSHDB/FLUSHALL integration" do
    test "FLUSHDB logs a dangerous_command audit event" do
      AuditLog.reset()
      store = build_test_store()
      Ferricstore.Commands.Server.handle("FLUSHDB", [], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          Enum.any?(entries, fn {_, _, type, _} -> type == :dangerous_command end)
        end,
        "dangerous_command audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
      assert dangerous != []
      {_, _, :dangerous_command, details} = List.first(dangerous)
      assert details.command == "FLUSHDB"
    end

    test "FLUSHALL logs a dangerous_command audit event" do
      AuditLog.reset()
      store = build_test_store()
      Ferricstore.Commands.Server.handle("FLUSHALL", [], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          Enum.any?(entries, fn {_, _, type, _} -> type == :dangerous_command end)
        end,
        "dangerous_command audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
      assert dangerous != []
      {_, _, :dangerous_command, details} = List.first(dangerous)
      assert details.command == "FLUSHALL"
    end

    test "FLUSHDB with ASYNC mode also logs" do
      AuditLog.reset()
      store = build_test_store()
      Ferricstore.Commands.Server.handle("FLUSHDB", ["ASYNC"], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          Enum.any?(entries, fn {_, _, type, _} -> type == :dangerous_command end)
        end,
        "dangerous_command audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
      assert dangerous != []
      {_, _, :dangerous_command, details} = List.first(dangerous)
      assert details.args == ["ASYNC"]
    end
  end

  # ---------------------------------------------------------------------------
  # Integration: DEBUG logs dangerous_command
  # ---------------------------------------------------------------------------

  describe "DEBUG integration" do
    test "DEBUG SLEEP logs a dangerous_command audit event" do
      AuditLog.reset()
      store = build_test_store()
      # Use 0 seconds to avoid blocking
      Ferricstore.Commands.Server.handle("DEBUG", ["SLEEP", "0"], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          Enum.any?(entries, fn {_, _, type, _} -> type == :dangerous_command end)
        end,
        "dangerous_command audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
      assert dangerous != []
      {_, _, :dangerous_command, details} = List.first(dangerous)
      assert details.command == "DEBUG"
      assert details.args == ["SLEEP", "0"]
    end

    test "DEBUG FLUSHALL logs a dangerous_command audit event" do
      AuditLog.reset()
      store = build_test_store()
      Ferricstore.Commands.Server.handle("DEBUG", ["FLUSHALL"], store)

      ShardHelpers.eventually(
        fn ->
          entries = AuditLog.get()
          dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
          dangerous != []
        end,
        "dangerous_command audit entry not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      dangerous = Enum.filter(entries, fn {_, _, type, _} -> type == :dangerous_command end)
      # Should log both "DEBUG FLUSHALL" and "FLUSHALL"
      assert dangerous != []
    end
  end

  # ---------------------------------------------------------------------------
  # Entry structure
  # ---------------------------------------------------------------------------

  describe "entry structure" do
    test "each entry has monotonically increasing ID" do
      for _ <- 1..5, do: AuditLog.log(:auth_success, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 5
        end,
        "5 entries not recorded",
        20,
        10
      )

      entries = AuditLog.get()
      ids = Enum.map(entries, fn {id, _, _, _} -> id end) |> Enum.sort()
      assert ids == Enum.to_list(0..4)
    end

    test "each entry has a microsecond timestamp" do
      before_us = System.os_time(:microsecond)
      AuditLog.log(:auth_success, %{})

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 1
        end,
        "entry not recorded",
        20,
        10
      )

      after_us = System.os_time(:microsecond)

      [{_, timestamp_us, _, _}] = AuditLog.get()
      assert timestamp_us >= before_us
      assert timestamp_us <= after_us
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent logging
  # ---------------------------------------------------------------------------

  describe "concurrent logging" do
    test "handles concurrent writes without data loss" do
      Application.put_env(:ferricstore, :audit_log_max_entries, 500)

      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            AuditLog.log(:auth_success, %{seq: i})
          end)
        end

      Task.await_many(tasks, 5_000)

      ShardHelpers.eventually(
        fn ->
          AuditLog.len() == 100
        end,
        "not all 100 concurrent entries recorded",
        20,
        10
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp build_test_store do
    %{
      get: fn _key -> nil end,
      get_meta: fn _key -> nil end,
      put: fn _key, _value, _expire_at_ms -> :ok end,
      delete: fn _key -> :ok end,
      exists?: fn _key -> false end,
      keys: fn -> [] end,
      flush: fn -> :ok end,
      dbsize: fn -> 0 end,
      incr: fn _key, _delta -> {:ok, 0} end,
      incr_float: fn _key, _delta -> {:ok, "0"} end,
      append: fn _key, _suffix -> {:ok, 0} end,
      getset: fn _key, _value -> nil end,
      getdel: fn _key -> nil end,
      getex: fn _key, _expire -> nil end,
      setrange: fn _key, _offset, _value -> {:ok, 0} end,
      cas: fn _key, _exp, _new, _ttl -> nil end,
      lock: fn _key, _owner, _ttl -> :ok end,
      unlock: fn _key, _owner -> 1 end,
      extend: fn _key, _owner, _ttl -> 1 end,
      ratelimit_add: fn _key, _window, _max, _count -> ["allowed", 0, 0, 0] end,
      list_op: fn _key, _op -> nil end
    }
  end
end
