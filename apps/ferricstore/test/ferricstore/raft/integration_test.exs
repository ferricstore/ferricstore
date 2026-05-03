defmodule Ferricstore.Raft.IntegrationTest do
  @moduledoc """
  Integration tests for the Raft consensus layer.

  These tests exercise the full Raft write path:
    Router -> Shard -> Batcher -> ra -> StateMachine -> Bitcask + ETS

  They verify that data written through the Raft consensus layer is:
  - Immediately readable from ETS (hot path)
  - Persisted to Bitcask (cold path)
  - Durable across shard restarts
  - Consistent across all layers

  The ra lifecycle is managed entirely within this test module so the tests
  The application starts the ra system, ra servers, and batchers for
  shards 0-3 before these tests run.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()

    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  defp ukey(base), do: "raft_int_#{base}_#{:rand.uniform(9_999_999)}"

  defp keydir_for(key), do: :"keydir_#{Router.shard_for(FerricStore.Instance.get(:default), key)}"

  defp shard_pid_for(key) do
    name =
      Router.shard_name(
        FerricStore.Instance.get(:default),
        Router.shard_for(FerricStore.Instance.get(:default), key)
      )

    Process.whereis(name)
  end

  # ---------------------------------------------------------------------------
  # Write path verification
  # ---------------------------------------------------------------------------

  describe "Raft write path" do
    test "state machine receives checkpoint-safe instance name for per-shard accounting" do
      shard_id = Cluster.shard_server_id(0)

      assert {:ok, {_, true}, _leader} =
               :ra.local_query(shard_id, fn state ->
                 state.instance_ctx == nil and state.instance_name == :default
               end)
    end

    test "default state machine marks checkpoint dirty through instance name fallback" do
      ctx = FerricStore.Instance.get(:default)
      key = ukey("checkpoint_dirty_default")
      shard_index = Router.shard_for(ctx, key)

      :atomics.put(ctx.checkpoint_flags, shard_index + 1, 0)

      assert :ok = Router.put(ctx, key, "value", 0)

      assert :atomics.get(ctx.checkpoint_flags, shard_index + 1) == 1
    end

    test "SET through Router writes via Raft and is readable" do
      k = ukey("set_via_raft")

      Router.put(FerricStore.Instance.get(:default), k, "raft_value", 0)
      assert "raft_value" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "SET writes to both ETS and Bitcask" do
      k = ukey("dual_write")

      Router.put(FerricStore.Instance.get(:default), k, "dual_val", 0)

      # ETS should have the value
      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{^k, "dual_val", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_for(k), k)

      # Flush pending writes to Bitcask before checking NIF directly
      shard_name =
        Router.shard_name(
          FerricStore.Instance.get(:default),
          Router.shard_for(FerricStore.Instance.get(:default), k)
        )

      GenServer.call(shard_name, :flush)

      # Flush background BitcaskWriter so deferred writes are on disk
      Ferricstore.Store.BitcaskWriter.flush_all()

      # v2: verify value is on disk via ETS 7-tuple location + pread
      [{^k, _, _, _, fid, off, _}] = :ets.lookup(keydir_for(k), k)
      shard_pid = shard_pid_for(k)
      state = :sys.get_state(shard_pid)
      sp = Ferricstore.DataDir.shard_data_path(state.data_dir, state.index)
      log_path = Path.join(sp, "#{String.pad_leading(Integer.to_string(fid), 5, "0")}.log")
      assert {:ok, "dual_val"} = NIF.v2_pread_at(log_path, off)
    end

    test "DEL removes from ETS and Bitcask" do
      k = ukey("del_both")

      Router.put(FerricStore.Instance.get(:default), k, "to_delete", 0)
      assert "to_delete" == Router.get(FerricStore.Instance.get(:default), k)

      Router.delete(FerricStore.Instance.get(:default), k)
      assert nil == Router.get(FerricStore.Instance.get(:default), k)

      # ETS should be empty
      assert [] == :ets.lookup(keydir_for(k), k)
    end

    test "multiple writes to same key return latest value" do
      k = ukey("overwrite")

      Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
      Router.put(FerricStore.Instance.get(:default), k, "v2", 0)
      Router.put(FerricStore.Instance.get(:default), k, "v3", 0)

      assert "v3" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "writes with TTL are respected" do
      k = ukey("ttl_write")
      future = System.os_time(:millisecond) + 60_000

      Router.put(FerricStore.Instance.get(:default), k, "ttl_val", future)

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-shard write consistency
  # ---------------------------------------------------------------------------

  describe "multi-shard write consistency" do
    test "writes to different shards are independent" do
      # Generate keys that map to different shards
      keys = for i <- 1..20, do: ukey("multi_#{i}")

      shard_indices =
        Enum.map(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)
        |> Enum.uniq()

      # Should have keys in multiple shards
      assert length(shard_indices) >= 2

      # Write all keys
      for k <- keys do
        Router.put(FerricStore.Instance.get(:default), k, "shard_val_#{k}", 0)
      end

      # All keys should be readable
      for k <- keys do
        assert "shard_val_#{k}" == Router.get(FerricStore.Instance.get(:default), k),
               "Key #{k} (shard #{Router.shard_for(FerricStore.Instance.get(:default), k)}) should be readable"
      end
    end

    test "keys() returns all written keys across shards" do
      prefix = ukey("all_shards")
      keys = for i <- 1..10, do: "#{prefix}_#{i}"

      for k <- keys do
        Router.put(FerricStore.Instance.get(:default), k, "v", 0)
      end

      all_keys = Router.keys(FerricStore.Instance.get(:default))

      for k <- keys do
        assert k in all_keys, "Key #{k} should be in keys()"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Group commit batching
  # ---------------------------------------------------------------------------

  describe "group commit batching" do
    test "many concurrent writes complete successfully" do
      keys = for i <- 1..50, do: ukey("gc_#{i}")

      tasks =
        Enum.map(keys, fn k ->
          Task.async(fn ->
            Router.put(FerricStore.Instance.get(:default), k, "concurrent_val", 0)
          end)
        end)

      results = Task.await_many(tasks, 15_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- keys do
        assert "concurrent_val" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end

    test "interleaved reads and writes are consistent" do
      k = ukey("interleave")

      # Write, read, write, read -- each read should see the latest write
      Router.put(FerricStore.Instance.get(:default), k, "v1", 0)
      assert "v1" == Router.get(FerricStore.Instance.get(:default), k)

      Router.put(FerricStore.Instance.get(:default), k, "v2", 0)
      assert "v2" == Router.get(FerricStore.Instance.get(:default), k)

      Router.put(FerricStore.Instance.get(:default), k, "v3", 0)
      assert "v3" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Ra cluster status
  # ---------------------------------------------------------------------------

  describe "ra cluster status" do
    test "all 4 shard ra servers have leaders" do
      for i <- 0..3 do
        server_id = Cluster.shard_server_id(i)

        case :ra.members(server_id) do
          {:ok, members, leader} ->
            assert length(members) == 1, "Single-node cluster should have 1 member"
            assert leader == server_id, "Leader should be the local server"

          {:error, reason} ->
            flunk("ra.members failed for shard #{i}: #{inspect(reason)}")

          {:timeout, _} ->
            flunk("ra.members timed out for shard #{i}")
        end
      end
    end

    test "ra server IDs follow naming convention" do
      for i <- 0..3 do
        {name, node_name} = Cluster.shard_server_id(i)
        assert name == :"ferricstore_shard_#{i}"
        assert node_name == node()
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Durability through shard restart
  # ---------------------------------------------------------------------------

  describe "durability through shard restart" do
    @tag :capture_log
    test "data survives shard crash and restart" do
      k = ukey("durable")
      Router.put(FerricStore.Instance.get(:default), k, "survives_crash", 0)
      assert "survives_crash" == Router.get(FerricStore.Instance.get(:default), k)

      # Flush to ensure data is durable
      pid = shard_pid_for(k)
      :ok = GenServer.call(pid, :flush)

      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), shard_index)

      # Kill the shard (simulates crash)
      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000

      # Wait for supervisor to restart the shard
      ShardHelpers.wait_shards_alive()

      new_pid = Process.whereis(shard_name)
      assert is_pid(new_pid)
      assert new_pid != pid

      # Data should be recoverable from Bitcask
      ShardHelpers.eventually(
        fn -> "survives_crash" == Router.get(FerricStore.Instance.get(:default), k) end,
        "data should survive shard crash and be recovered from Bitcask"
      )
    end
  end

  # ---------------------------------------------------------------------------
  # Ra system lifecycle
  # ---------------------------------------------------------------------------

  describe "ra system lifecycle" do
    test "ra system is started and accessible" do
      config = :ra_system.fetch(Cluster.system_name())
      assert config != :undefined
      assert config.name == Cluster.system_name()
    end
  end
end
