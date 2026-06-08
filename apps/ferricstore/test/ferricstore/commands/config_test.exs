defmodule Ferricstore.Commands.ConfigTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state
  import ExUnit.CaptureLog

  alias Ferricstore.Commands.Server
  alias Ferricstore.Config
  alias Ferricstore.Stats
  alias Ferricstore.Test.MockStore

  # Reset config to defaults before AND after each test to avoid cross-test
  # and cross-module contamination (e.g. AuditLogTest setting hz to "100").
  setup do
    # Capture original Application env values that CONFIG SET may alter
    orig_eviction = Application.get_env(:ferricstore, :eviction_policy)
    orig_keydir_max_ram = Application.get_env(:ferricstore, :keydir_max_ram)
    orig_hot_cache_max_ram = Application.get_env(:ferricstore, :hot_cache_max_ram)
    orig_hot_cache_min_ram = Application.get_env(:ferricstore, :hot_cache_min_ram)
    orig_hot_cache_max_value_size = Application.get_env(:ferricstore, :hot_cache_max_value_size)
    orig_slowlog_us = Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    orig_slowlog_max = Application.get_env(:ferricstore, :slowlog_max_len)
    orig_data_dir = Application.get_env(:ferricstore, :data_dir)

    # Reset read-write config params to defaults BEFORE each test so that
    # leftover state from other modules does not leak into this module.
    reset_config_defaults()

    on_exit(fn ->
      # Restore Application env first. `reset_config_defaults/0` reads from
      # Application env, so doing this in the opposite order would preserve a
      # CONFIG SET keydir-max-ram value as the next test's "default".
      restore_env(:eviction_policy, orig_eviction)
      restore_env(:keydir_max_ram, orig_keydir_max_ram)
      restore_env(:hot_cache_max_ram, orig_hot_cache_max_ram)
      restore_env(:hot_cache_min_ram, orig_hot_cache_min_ram)
      restore_env(:hot_cache_max_value_size, orig_hot_cache_max_value_size)

      if orig_data_dir do
        Application.put_env(:ferricstore, :data_dir, orig_data_dir)
      else
        Application.delete_env(:ferricstore, :data_dir)
      end

      if orig_slowlog_us, do: Ferricstore.SlowLog.set_threshold(orig_slowlog_us)
      if orig_slowlog_max, do: Ferricstore.SlowLog.set_max_len(orig_slowlog_max)

      reset_config_defaults()
    end)

    :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp reset_config_defaults do
    defaults = Config.defaults()

    Enum.each(defaults, fn {k, v} ->
      try do
        Config.set(k, v)
      rescue
        _ -> :ok
      catch
        :exit, _ -> :ok
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET -- read-only parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG GET read-only parameters" do
    test "CONFIG GET maxmemory returns integer string from Application env" do
      result = Server.handle("CONFIG", ["GET", "maxmemory"], MockStore.make())
      assert ["maxmemory", value] = result
      # The value should be a string representation of an integer
      assert {_n, ""} = Integer.parse(value)
    end

    test "CONFIG GET maxclients returns integer string" do
      result = Server.handle("CONFIG", ["GET", "maxclients"], MockStore.make())
      assert ["maxclients", value] = result
      assert {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG GET tcp-port returns the configured port" do
      result = Server.handle("CONFIG", ["GET", "tcp-port"], MockStore.make())
      assert ["tcp-port", value] = result
      assert {_n, ""} = Integer.parse(value)
    end

    test "CONFIG GET data-dir returns the configured data directory" do
      result = Server.handle("CONFIG", ["GET", "data-dir"], MockStore.make())
      assert ["data-dir", value] = result
      assert is_binary(value)
      assert String.length(value) > 0
    end

    # raft-enabled was removed — Raft is always on
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET -- read-write parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG GET read-write parameters" do
    test "CONFIG GET maxmemory-policy returns string policy name" do
      result = Server.handle("CONFIG", ["GET", "maxmemory-policy"], MockStore.make())
      assert ["maxmemory-policy", value] = result
      assert value in ["volatile-lru", "allkeys-lru", "volatile-ttl", "noeviction"]
    end

    test "CONFIG GET notify-keyspace-events returns string" do
      result = Server.handle("CONFIG", ["GET", "notify-keyspace-events"], MockStore.make())
      assert ["notify-keyspace-events", value] = result
      assert is_binary(value)
    end

    test "CONFIG GET slowlog-log-slower-than returns integer string" do
      result = Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], MockStore.make())
      assert ["slowlog-log-slower-than", value] = result
      assert {_n, ""} = Integer.parse(value)
    end

    test "CONFIG GET slowlog-max-len returns integer string" do
      result = Server.handle("CONFIG", ["GET", "slowlog-max-len"], MockStore.make())
      assert ["slowlog-max-len", value] = result
      assert {n, ""} = Integer.parse(value)
      assert n > 0
    end

    test "CONFIG GET hz returns integer string" do
      result = Server.handle("CONFIG", ["GET", "hz"], MockStore.make())
      assert ["hz", hz_val] = result
      assert {_, ""} = Integer.parse(hz_val), "hz should be a valid integer string"
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET * -- all parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG GET *" do
    test "CONFIG GET * returns all parameters as flat key-value list" do
      result = Server.handle("CONFIG", ["GET", "*"], MockStore.make())
      assert is_list(result)
      # Result is a flat list [key, value, key, value, ...]
      assert rem(length(result), 2) == 0

      # Extract keys (every even-indexed element)
      keys = every_other(result, 0)

      # All spec-required parameters should be present
      assert "maxmemory" in keys
      assert "maxclients" in keys
      assert "tcp-port" in keys
      assert "data-dir" in keys
      # raft-enabled removed — Raft always on
      assert "maxmemory-policy" in keys
      assert "notify-keyspace-events" in keys
      assert "slowlog-log-slower-than" in keys
      assert "slowlog-max-len" in keys
      assert "hz" in keys

      # Legacy params should also be present
      assert "requirepass" in keys
      assert "bind" in keys
      assert "timeout" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG GET -- pattern matching
  # ---------------------------------------------------------------------------

  describe "CONFIG GET pattern matching" do
    test "CONFIG GET with pattern filters results" do
      result = Server.handle("CONFIG", ["GET", "max*"], MockStore.make())
      assert is_list(result)
      keys = every_other(result, 0)
      assert "maxmemory" in keys
      assert "maxmemory-policy" in keys
      assert "maxclients" in keys
      refute "hz" in keys
    end

    test "CONFIG GET with non-matching pattern returns empty list" do
      result = Server.handle("CONFIG", ["GET", "nonexistent"], MockStore.make())
      assert result == []
    end

    test "CONFIG GET with ? wildcard matches single character" do
      result = Server.handle("CONFIG", ["GET", "h?"], MockStore.make())
      keys = every_other(result, 0)
      assert "hz" in keys
    end

    test "CONFIG GET with no args returns error" do
      result = Server.handle("CONFIG", ["GET"], MockStore.make())
      assert {:error, _} = result
    end

    test "CONFIG GET slowlog-* returns both slowlog parameters" do
      result = Server.handle("CONFIG", ["GET", "slowlog-*"], MockStore.make())
      keys = every_other(result, 0)
      assert "slowlog-log-slower-than" in keys
      assert "slowlog-max-len" in keys
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET -- read-write parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG SET read-write parameters" do
    test "CONFIG SET maxmemory-policy volatile-ttl then GET returns new value" do
      assert :ok =
               Server.handle(
                 "CONFIG",
                 ["SET", "maxmemory-policy", "volatile-ttl"],
                 MockStore.make()
               )

      result = Server.handle("CONFIG", ["GET", "maxmemory-policy"], MockStore.make())
      assert ["maxmemory-policy", "volatile-ttl"] = result
    end

    test "CONFIG SET maxmemory-policy updates Application env" do
      Server.handle("CONFIG", ["SET", "maxmemory-policy", "allkeys-lru"], MockStore.make())
      assert :allkeys_lru == Application.get_env(:ferricstore, :eviction_policy)
    end

    test "CONFIG SET maxmemory-policy reconfigures MemoryGuard" do
      Server.handle("CONFIG", ["SET", "maxmemory-policy", "noeviction"], MockStore.make())
      assert :noeviction == Ferricstore.MemoryGuard.eviction_policy()
    end

    test "CONFIG SET maxmemory-policy with invalid value returns error" do
      result =
        Server.handle("CONFIG", ["SET", "maxmemory-policy", "invalid-policy"], MockStore.make())

      assert {:error, msg} = result
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-log-slower-than updates threshold" do
      assert :ok =
               Server.handle(
                 "CONFIG",
                 ["SET", "slowlog-log-slower-than", "5000"],
                 MockStore.make()
               )

      result = Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], MockStore.make())
      assert ["slowlog-log-slower-than", "5000"] = result
      # Verify Application env was updated (SlowLog reads from here)
      assert 5000 == Application.get_env(:ferricstore, :slowlog_log_slower_than_us)
    end

    test "CONFIG SET slowlog-log-slower-than with -1 disables slowlog" do
      assert :ok =
               Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "-1"], MockStore.make())

      result = Server.handle("CONFIG", ["GET", "slowlog-log-slower-than"], MockStore.make())
      assert ["slowlog-log-slower-than", "-1"] = result
    end

    test "CONFIG SET slowlog-log-slower-than with non-integer returns error" do
      result =
        Server.handle("CONFIG", ["SET", "slowlog-log-slower-than", "abc"], MockStore.make())

      assert {:error, msg} = result
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET slowlog-max-len updates max entries" do
      assert :ok = Server.handle("CONFIG", ["SET", "slowlog-max-len", "256"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "slowlog-max-len"], MockStore.make())
      assert ["slowlog-max-len", "256"] = result
      assert 256 == Application.get_env(:ferricstore, :slowlog_max_len)
    end

    test "CONFIG SET slowlog-max-len with negative returns error" do
      result = Server.handle("CONFIG", ["SET", "slowlog-max-len", "-1"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "Invalid argument"
    end

    test "CONFIG SET hz accepts valid values" do
      assert :ok = Server.handle("CONFIG", ["SET", "hz", "100"], MockStore.make())
      result = Server.handle("CONFIG", ["GET", "hz"], MockStore.make())
      assert ["hz", "100"] = result
    end

    test "CONFIG SET hz with invalid value returns error" do
      result = Server.handle("CONFIG", ["SET", "hz", "0"], MockStore.make())
      assert {:error, _} = result
    end

    test "CONFIG SET notify-keyspace-events accepts flag strings" do
      assert :ok =
               Server.handle("CONFIG", ["SET", "notify-keyspace-events", "KEA"], MockStore.make())

      result = Server.handle("CONFIG", ["GET", "notify-keyspace-events"], MockStore.make())
      assert ["notify-keyspace-events", "KEA"] = result
    end

    test "CONFIG SET notify-keyspace-events accepts empty string to disable" do
      Server.handle("CONFIG", ["SET", "notify-keyspace-events", "KEA"], MockStore.make())

      assert :ok =
               Server.handle("CONFIG", ["SET", "notify-keyspace-events", ""], MockStore.make())

      result = Server.handle("CONFIG", ["GET", "notify-keyspace-events"], MockStore.make())
      assert ["notify-keyspace-events", ""] = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET -- read-only parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG SET read-only parameters" do
    test "CONFIG SET maxmemory returns error" do
      result = Server.handle("CONFIG", ["SET", "maxmemory", "999"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "read-only"
    end

    test "CONFIG SET tcp-port returns error" do
      result = Server.handle("CONFIG", ["SET", "tcp-port", "9999"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "read-only"
    end

    test "CONFIG SET data-dir returns error" do
      result = Server.handle("CONFIG", ["SET", "data-dir", "/tmp/new"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "read-only"
    end

    # raft-enabled removed — Raft always on

    test "CONFIG SET maxclients returns error" do
      result = Server.handle("CONFIG", ["SET", "maxclients", "5000"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "read-only"
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET -- unknown parameters
  # ---------------------------------------------------------------------------

  describe "CONFIG SET unknown parameter" do
    test "CONFIG SET unknown-param returns error" do
      result = Server.handle("CONFIG", ["SET", "unknown-param", "value"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "Unsupported CONFIG parameter"
    end

    test "CONFIG SET totally-bogus returns error" do
      result = Server.handle("CONFIG", ["SET", "totally-bogus-param", "123"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET -- argument validation
  # ---------------------------------------------------------------------------

  describe "CONFIG SET argument errors" do
    test "CONFIG SET with no args returns error" do
      result = Server.handle("CONFIG", ["SET"], MockStore.make())
      assert {:error, _} = result
    end

    test "CONFIG SET with only key and no value returns error" do
      result = Server.handle("CONFIG", ["SET", "hz"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG SET -- telemetry
  # ---------------------------------------------------------------------------

  describe "CONFIG SET telemetry" do
    test "CONFIG SET emits [:ferricstore, :config, :changed] telemetry event" do
      ref = make_ref()
      test_pid = self()

      handler_id = "config-changed-test-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :config, :changed],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:config_changed, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      Server.handle("CONFIG", ["SET", "hz", "50"], MockStore.make())

      assert_receive {:config_changed, metadata}, 1_000
      assert metadata.param == "hz"
      assert metadata.value == "50"
      assert is_binary(metadata.old_value)
    end

    test "CONFIG SET emits telemetry when MemoryGuard reconfigure side effect fails" do
      parent = self()
      handler_id = {:config_side_effect_failed, parent, make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :config, :side_effect_failed],
        &__MODULE__.handle_config_side_effect_failed/4,
        parent
      )

      old_hook = Application.get_env(:ferricstore, :config_memory_guard_reconfigure_hook)

      Application.put_env(
        :ferricstore,
        :config_memory_guard_reconfigure_hook,
        fn %{keydir_max_ram: 12345} ->
          raise "memory guard reconfigure exploded"
        end
      )

      on_exit(fn ->
        :telemetry.detach(handler_id)

        case old_hook do
          nil -> Application.delete_env(:ferricstore, :config_memory_guard_reconfigure_hook)
          hook -> Application.put_env(:ferricstore, :config_memory_guard_reconfigure_hook, hook)
        end
      end)

      assert :ok = Server.handle("CONFIG", ["SET", "keydir-max-ram", "12345"], MockStore.make())

      assert_receive {:config_side_effect_failed, [:ferricstore, :config, :side_effect_failed],
                      %{count: 1},
                      %{
                        param: "keydir-max-ram",
                        phase: :memory_guard_reconfigure,
                        kind: :error,
                        reason: %RuntimeError{message: "memory guard reconfigure exploded"}
                      }}
    end
  end

  def handle_config_side_effect_failed(event, measurements, metadata, parent) do
    send(parent, {:config_side_effect_failed, event, measurements, metadata})
  end

  # ---------------------------------------------------------------------------
  # CONFIG RESETSTAT
  # ---------------------------------------------------------------------------

  describe "CONFIG RESETSTAT" do
    test "CONFIG RESETSTAT returns OK" do
      assert :ok = Server.handle("CONFIG", ["RESETSTAT"], MockStore.make())
    end

    test "CONFIG RESETSTAT resets Stats counters" do
      # Increment some counters
      Stats.incr_connections()
      Stats.incr_connections()
      Stats.incr_commands()

      assert Stats.total_connections() > 0 or Stats.total_commands() > 0

      Server.handle("CONFIG", ["RESETSTAT"], MockStore.make())

      # Allow small values since concurrent processes (health checks, etc.)
      # may have incremented a counter between the reset and this assertion.
      assert Stats.total_connections() <= 2
      assert Stats.total_commands() <= 2
    end

    test "CONFIG RESETSTAT resets slowlog" do
      Ferricstore.SlowLog.set_threshold(0)
      Ferricstore.SlowLog.set_max_len(8)
      Ferricstore.SlowLog.reset()

      # Add a deterministic slow log entry via cast, then send a synchronous
      # :ping to the GenServer to guarantee the preceding cast has processed.
      Ferricstore.SlowLog.maybe_log(["SET", "key", "val"], 1)
      GenServer.call(Ferricstore.SlowLog, :ping)

      assert Ferricstore.SlowLog.len() > 0

      Server.handle("CONFIG", ["RESETSTAT"], MockStore.make())

      # Sync with the SlowLog GenServer to ensure reset has been fully processed,
      # then verify. Allow a small count since concurrent operations may have
      # added an entry between the reset and this check.
      GenServer.call(Ferricstore.SlowLog, :ping)
      assert Ferricstore.SlowLog.len() <= 1
    end

    test "CONFIG RESETSTAT with args returns error" do
      result = Server.handle("CONFIG", ["RESETSTAT", "extra"], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG REWRITE
  # ---------------------------------------------------------------------------

  describe "CONFIG REWRITE" do
    test "CONFIG REWRITE returns OK (stub)" do
      assert :ok = Server.handle("CONFIG", ["REWRITE"], MockStore.make())
    end

    test "CONFIG REWRITE with args returns error" do
      result = Server.handle("CONFIG", ["REWRITE", "extra"], MockStore.make())
      assert {:error, _} = result
    end

    test "CONFIG REWRITE reports tmp cleanup failure" do
      dir =
        Path.join(
          System.tmp_dir!(),
          "ferricstore-config-rewrite-#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(dir)
      Application.put_env(:ferricstore, :data_dir, dir)

      tmp_path = Ferricstore.Config.config_file_path() <> ".tmp"
      File.mkdir!(tmp_path)
      parent = self()
      handler_id = {:config_rewrite_cleanup_failed, parent, make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :config, :rewrite, :cleanup_failed],
        fn event, measurements, metadata, _config ->
          send(parent, {:cleanup_failed, event, measurements, metadata})
        end,
        nil
      )

      try do
        log =
          capture_log(fn ->
            assert {:error, _reason} = Server.handle("CONFIG", ["REWRITE"], MockStore.make())
          end)

        assert log =~ "failed to remove config rewrite tmp file"

        assert_receive {:cleanup_failed, [:ferricstore, :config, :rewrite, :cleanup_failed],
                        %{count: 1}, %{path: ^tmp_path, reason: {_kind, _message}}},
                       1_000
      after
        :telemetry.detach(handler_id)
        File.rm_rf(dir)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # CONFIG unknown subcommand
  # ---------------------------------------------------------------------------

  describe "CONFIG unknown" do
    test "unknown CONFIG subcommand returns error" do
      result = Server.handle("CONFIG", ["BADSUBCMD"], MockStore.make())
      assert {:error, msg} = result
      assert msg =~ "unknown subcommand"
    end

    test "CONFIG with no args returns error" do
      result = Server.handle("CONFIG", [], MockStore.make())
      assert {:error, _} = result
    end
  end

  # ---------------------------------------------------------------------------
  # Config GenServer direct API
  # ---------------------------------------------------------------------------

  describe "Config GenServer API" do
    test "get/1 returns matching pairs" do
      pairs = Config.get("hz")
      assert pairs == [{"hz", "10"}]
    end

    test "set/2 updates a read-write value" do
      assert :ok = Config.set("hz", "50")
      assert [{"hz", "50"}] = Config.get("hz")
    end

    test "set/2 returns error for read-only param" do
      assert {:error, _} = Config.set("maxmemory", "999")
    end

    test "set/2 returns error for unknown param" do
      assert {:error, _} = Config.set("totally_unknown_key", "val")
    end

    test "get_value/1 returns single value" do
      [{"hz", hz_val}] = Config.get("hz")
      assert hz_val == Config.get_value("hz")
    end

    test "get_value/1 returns nil for unknown key" do
      assert nil == Config.get_value("totally_unknown_key")
    end
  end

  # ---------------------------------------------------------------------------
  # Read-only parameter values reflect Application env
  # ---------------------------------------------------------------------------

  describe "read-only params reflect Application env" do
    test "maxmemory reflects :max_memory_bytes Application env" do
      expected = Application.get_env(:ferricstore, :max_memory_bytes, 0) |> to_string()
      assert [{"maxmemory", ^expected}] = Config.get("maxmemory")
    end

    test "raft-enabled config parameter was removed (Raft always on)" do
      assert [] = Config.get("raft-enabled")
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Extract every other element from a flat list starting at the given offset.
  defp every_other(list, offset) do
    list
    |> Enum.drop(offset)
    |> Enum.take_every(2)
  end
end
