defmodule Ferricstore.Commands.ServerInfoTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server
  alias Ferricstore.Test.MockStore

  @server_path Path.expand("../../../lib/ferricstore/commands/server.ex", __DIR__)

  # ---------------------------------------------------------------------------
  # INFO — default (all sections)
  # ---------------------------------------------------------------------------

  describe "INFO (all sections)" do
    test "keyspace expiry stats use the same HLC clock as expiry decisions" do
      source = File.read!(@server_path)
      [_before, rest] = String.split(source, "defp compute_expiry_stats", parts: 2)
      [body | _after] = String.split(rest, "\n  # INFO formatting helpers", parts: 2)

      assert body =~ "HLC.now_ms()"
      refute body =~ "System.os_time(:millisecond)"
    end

    test "INFO with no args returns all sections" do
      store = MockStore.make(%{"k1" => {"v1", 0}, "k2" => {"v2", 0}})
      result = Server.handle("INFO", [], store)

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "# Clients"
      assert result =~ "# Memory"
      assert result =~ "# Keyspace"
      assert result =~ "# Stats"
      assert result =~ "# Persistence"
      assert result =~ "# Replication"
      assert result =~ "# CPU"
    end

    test "INFO all returns all sections explicitly" do
      store = MockStore.make()
      result = Server.handle("INFO", ["all"], store)

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "# Clients"
      assert result =~ "# Memory"
      assert result =~ "# Keyspace"
      assert result =~ "# Stats"
      assert result =~ "# Persistence"
      assert result =~ "# Replication"
      assert result =~ "# CPU"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO server
  # ---------------------------------------------------------------------------

  describe "INFO server" do
    test "returns server section with expected fields" do
      result = Server.handle("INFO", ["server"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "redis_version:7.4.0"
      assert result =~ ~r/ferricstore_version:\d+\.\d+\.\d+/
      assert result =~ "tcp_port:"
      assert result =~ "uptime_in_seconds:"
      assert result =~ "process_id:"
      assert result =~ "run_id:"
    end

    test "run_id is a 40-char hex string" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      run_id_line = Enum.find(fields, &String.starts_with?(&1, "run_id:"))
      assert run_id_line != nil

      run_id = String.trim_leading(run_id_line, "run_id:")
      assert byte_size(run_id) == 40
      assert Regex.match?(~r/^[0-9a-f]+$/, run_id)
    end

    test "uptime_in_seconds is a non-negative integer" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      uptime_line = Enum.find(fields, &String.starts_with?(&1, "uptime_in_seconds:"))
      uptime_str = String.trim_leading(uptime_line, "uptime_in_seconds:")
      {uptime, ""} = Integer.parse(uptime_str)
      assert uptime >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # INFO memory
  # ---------------------------------------------------------------------------

  describe "INFO memory" do
    test "returns memory section with used_memory and used_memory_human" do
      result = Server.handle("INFO", ["memory"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Memory"
      assert result =~ "used_memory:"
      assert result =~ "used_memory_human:"
    end

    test "used_memory is a positive integer" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      mem_line = Enum.find(fields, &String.starts_with?(&1, "used_memory:"))
      mem_str = String.trim_leading(mem_line, "used_memory:")
      {mem, ""} = Integer.parse(mem_str)
      assert mem > 0
    end

    test "used_memory_human contains a size suffix" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      human_line = Enum.find(fields, &String.starts_with?(&1, "used_memory_human:"))
      human_str = String.trim_leading(human_line, "used_memory_human:")
      # Should end with K, M, G, or B
      assert Regex.match?(~r/[KMGB]$/, human_str)
    end
  end

  # ---------------------------------------------------------------------------
  # INFO keyspace
  # ---------------------------------------------------------------------------

  describe "INFO keyspace" do
    test "reflects actual key count" do
      store = MockStore.make(%{"a" => {"1", 0}, "b" => {"2", 0}, "c" => {"3", 0}})
      result = Server.handle("INFO", ["keyspace"], store)

      assert is_binary(result)
      assert result =~ "# Keyspace"
      assert result =~ "keys=3"
    end

    test "empty store shows keys=0" do
      store = MockStore.make()
      result = Server.handle("INFO", ["keyspace"], store)

      assert result =~ "keys=0"
    end
  end

  describe "INFO bitcask" do
    test "exposes release cursor frontier metrics per shard" do
      result = Server.handle("INFO", ["bitcask"], MockStore.make())

      assert result =~ "# Bitcask"
      assert result =~ "shard_0_last_applied_index:"
      assert result =~ "shard_0_last_released_cursor_index:"
      assert result =~ "shard_0_replay_safe_index:"
      assert result =~ "shard_0_replay_safe_requested_index:"
      assert result =~ "shard_0_replay_safe_lag:"
      assert result =~ "shard_0_replay_safe_persist_failures:"
      assert result =~ "shard_0_release_cursor_gap:"
      assert result =~ "shard_0_release_cursor_blocked_apply_count:"
      assert result =~ "shard_0_checkpoint_dirty:"
      assert result =~ "shard_0_checkpoint_in_flight:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO clients
  # ---------------------------------------------------------------------------

  describe "INFO clients" do
    test "returns clients section with connected_clients field" do
      result = Server.handle("INFO", ["clients"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Clients"
      assert result =~ "connected_clients:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO stats
  # ---------------------------------------------------------------------------

  describe "INFO stats" do
    test "returns stats section with counter fields" do
      result = Server.handle("INFO", ["stats"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Stats"
      assert result =~ "total_connections_received:"
      assert result =~ "total_commands_processed:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO with unknown section
  # ---------------------------------------------------------------------------

  describe "INFO unknown section" do
    test "returns empty string for unknown section" do
      result = Server.handle("INFO", ["nonexistent"], MockStore.make())
      assert result == ""
    end
  end

  # ---------------------------------------------------------------------------
  # INFO case insensitivity
  # ---------------------------------------------------------------------------

  describe "INFO section case insensitivity" do
    test "INFO SERVER (uppercase) returns server section" do
      result = Server.handle("INFO", ["SERVER"], MockStore.make())
      assert result =~ "# Server"
    end

    test "INFO Memory (mixed case) returns memory section" do
      result = Server.handle("INFO", ["Memory"], MockStore.make())
      assert result =~ "# Memory"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO persistence
  # ---------------------------------------------------------------------------

  describe "INFO persistence" do
    test "returns persistence section header" do
      result = Server.handle("INFO", ["persistence"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Persistence"
    end

    test "contains loading field set to 0" do
      result = Server.handle("INFO", ["persistence"], MockStore.make())
      assert result =~ "loading:0"
    end

    test "contains rdb_changes_since_last_save stub" do
      result = Server.handle("INFO", ["persistence"], MockStore.make())
      assert result =~ "rdb_changes_since_last_save:0"
    end

    test "contains rdb_last_save_time" do
      result = Server.handle("INFO", ["persistence"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)
      line = Enum.find(fields, &String.starts_with?(&1, "rdb_last_save_time:"))
      {timestamp, ""} = line |> String.trim_leading("rdb_last_save_time:") |> Integer.parse()
      assert timestamp >= 0
    end
  end

  describe "SAVE" do
    test "calls the injected persistence barrier" do
      parent = self()

      store =
        MockStore.make()
        |> Map.put(:persistence_barrier, fn ->
          send(parent, :save_barrier_called)
          :ok
        end)

      assert :ok = Server.handle("SAVE", [], store)
      assert_receive :save_barrier_called
    end

    test "updates LASTSAVE after the persistence barrier succeeds" do
      before_save = Server.handle("LASTSAVE", [], MockStore.make())
      assert is_integer(before_save)

      store = Map.put(MockStore.make(), :persistence_barrier, fn -> :ok end)
      assert :ok = Server.handle("SAVE", [], store)

      assert Server.handle("LASTSAVE", [], store) >= before_save
    end

    test "returns an error when the persistence barrier fails" do
      store =
        MockStore.make()
        |> Map.put(:persistence_barrier, fn -> {:error, :fsync_failed} end)

      assert {:error, msg} = Server.handle("SAVE", [], store)
      assert msg =~ "save failed"
      assert msg =~ "fsync_failed"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO replication
  # ---------------------------------------------------------------------------

  describe "INFO replication" do
    test "returns replication section header" do
      result = Server.handle("INFO", ["replication"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# Replication"
    end

    test "shows role:master" do
      result = Server.handle("INFO", ["replication"], MockStore.make())
      assert result =~ "role:master"
    end

    test "shows connected_slaves:0" do
      result = Server.handle("INFO", ["replication"], MockStore.make())
      assert result =~ "connected_slaves:0"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO memory — new fields
  # ---------------------------------------------------------------------------

  describe "INFO memory (extended fields)" do
    test "has keydir_used_bytes" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "keydir_used_bytes:"
    end

    test "has hot_cache_used_bytes" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "hot_cache_used_bytes:"
    end

    test "has used_memory_rss" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "used_memory_rss:"
    end

    test "has used_memory_peak" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "used_memory_peak:"
    end

    test "has mem_fragmentation_ratio" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "mem_fragmentation_ratio:"
    end

    test "has beam_process_memory" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      assert result =~ "beam_process_memory:"
    end

    test "keydir_used_bytes is a non-negative integer" do
      result = Server.handle("INFO", ["memory"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "keydir_used_bytes:"))
      val_str = String.trim_leading(line, "keydir_used_bytes:")
      {val, ""} = Integer.parse(val_str)
      assert val >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # INFO clients — new fields
  # ---------------------------------------------------------------------------

  describe "INFO clients (extended fields)" do
    test "has blocked_clients" do
      result = Server.handle("INFO", ["clients"], MockStore.make())
      assert result =~ "blocked_clients:"
    end

    test "has tracking_clients" do
      result = Server.handle("INFO", ["clients"], MockStore.make())
      assert result =~ "tracking_clients:"
    end

    test "has maxclients" do
      result = Server.handle("INFO", ["clients"], MockStore.make())
      assert result =~ "maxclients:10000"
    end

    test "blocked_clients is a non-negative integer" do
      result = Server.handle("INFO", ["clients"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "blocked_clients:"))
      val_str = String.trim_leading(line, "blocked_clients:")
      {val, ""} = Integer.parse(val_str)
      assert val >= 0
    end

    test "tracking_clients is a non-negative integer" do
      result = Server.handle("INFO", ["clients"], MockStore.make())
      [_header | fields] = String.split(result, "\r\n", trim: true)

      line = Enum.find(fields, &String.starts_with?(&1, "tracking_clients:"))
      val_str = String.trim_leading(line, "tracking_clients:")
      {val, ""} = Integer.parse(val_str)
      assert val >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # INFO server — new fields
  # ---------------------------------------------------------------------------

  describe "INFO server (extended fields)" do
    test "has redis_mode" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "redis_mode:"
    end

    test "has os field" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "os:"
    end

    test "has arch_bits" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "arch_bits:64"
    end

    test "has uptime_in_days" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "uptime_in_days:"
    end

    test "has hz and configured_hz" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "hz:10"
      assert result =~ "configured_hz:10"
    end

    test "has ferricstore_git_sha" do
      result = Server.handle("INFO", ["server"], MockStore.make())
      assert result =~ "ferricstore_git_sha:dev"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO everything — alias for all
  # ---------------------------------------------------------------------------

  describe "INFO everything" do
    test "returns all sections same as all" do
      store = MockStore.make()
      result = Server.handle("INFO", ["everything"], store)

      assert is_binary(result)
      assert result =~ "# Server"
      assert result =~ "# Clients"
      assert result =~ "# Memory"
      assert result =~ "# Keyspace"
      assert result =~ "# Stats"
      assert result =~ "# Persistence"
      assert result =~ "# Replication"
      assert result =~ "# CPU"
    end

    test "everything matches all output" do
      store = MockStore.make()
      result_all = Server.handle("INFO", ["all"], store)
      result_everything = Server.handle("INFO", ["everything"], store)

      # Both should contain the same section headers (actual values may differ
      # due to timing, but the structure must match).
      all_headers =
        result_all
        |> String.split("\r\n")
        |> Enum.filter(&String.starts_with?(&1, "# "))
        |> Enum.sort()

      everything_headers =
        result_everything
        |> String.split("\r\n")
        |> Enum.filter(&String.starts_with?(&1, "# "))
        |> Enum.sort()

      assert all_headers == everything_headers
    end
  end

  # ---------------------------------------------------------------------------
  # INFO cpu
  # ---------------------------------------------------------------------------

  describe "INFO cpu" do
    test "returns cpu section header" do
      result = Server.handle("INFO", ["cpu"], MockStore.make())

      assert is_binary(result)
      assert result =~ "# CPU"
    end

    test "contains used_cpu_sys field" do
      result = Server.handle("INFO", ["cpu"], MockStore.make())
      assert result =~ "used_cpu_sys:"
    end

    test "contains used_cpu_user field" do
      result = Server.handle("INFO", ["cpu"], MockStore.make())
      assert result =~ "used_cpu_user:"
    end
  end

  # ---------------------------------------------------------------------------
  # INFO with too many args
  # ---------------------------------------------------------------------------

  describe "INFO error cases" do
    test "INFO with two section args returns error" do
      assert {:error, _} = Server.handle("INFO", ["server", "memory"], MockStore.make())
    end
  end
end
