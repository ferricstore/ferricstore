defmodule Ferricstore.Raft.BatcherTest do
  @moduledoc """
  Tests for `Ferricstore.Raft.Batcher`.

  These tests exercise the group commit batcher GenServer using a locally
  managed ra system and shard servers. The batcher accumulates writes for
  up to `batch_window_ms`, then submits them as a single Raft log entry.

  The application starts the ra system, ra servers, and batchers for shards
  0-3 before these tests run.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()

    :ok
  end

  setup do
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Helper to generate unique keys
  defp ukey(base), do: "batcher_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Batcher process lifecycle
  # ---------------------------------------------------------------------------

  describe "batcher process lifecycle" do
    test "batchers are registered under expected names" do
      for i <- 0..3 do
        name = Batcher.batcher_name(i)
        pid = Process.whereis(name)
        assert is_pid(pid), "Expected batcher #{i} registered as #{name}"
        assert Process.alive?(pid)
      end
    end

    test "batcher_name/1 returns expected atom" do
      assert Batcher.batcher_name(0) == :"Ferricstore.Raft.Batcher.0"
      assert Batcher.batcher_name(3) == :"Ferricstore.Raft.Batcher.3"
    end
  end

  # ---------------------------------------------------------------------------
  # Single writes through batcher
  # ---------------------------------------------------------------------------

  describe "single writes through batcher" do
    test "write put command succeeds" do
      k = ukey("single_put")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      result = Batcher.write(shard_index, {:put, k, "val", 0})
      assert result == :ok
    end

    test "write delete command succeeds" do
      k = ukey("single_del")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      # First put, then delete
      :ok = Batcher.write(shard_index, {:put, k, "to_delete", 0})
      result = Batcher.write(shard_index, {:delete, k})
      assert result == :ok
    end

    test "data written through batcher is readable from Router" do
      k = ukey("readable")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "batcher_val", 0})
      assert "batcher_val" == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "delete through batcher removes key from Router" do
      k = ukey("del_readable")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "temp", 0})
      assert "temp" == Router.get(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:delete, k})
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ---------------------------------------------------------------------------
  # Batch window accumulation
  # ---------------------------------------------------------------------------

  describe "batch window accumulation" do
    test "multiple concurrent writes are batched" do
      keys =
        for i <- 1..10 do
          ukey("concurrent_#{i}")
        end

      # Group keys by shard
      by_shard =
        Enum.group_by(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)

      # Pick a shard that has multiple keys to test batching
      {shard_idx, shard_keys} = Enum.max_by(by_shard, fn {_, ks} -> length(ks) end)

      # Send all writes concurrently -- they should be batched
      tasks =
        Enum.map(shard_keys, fn k ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, k, "batched", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      # All keys should be readable
      for k <- shard_keys do
        assert "batched" == Router.get(FerricStore.Instance.get(:default), k)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Flush
  # ---------------------------------------------------------------------------

  describe "flush" do
    test "flush returns :ok when batch is empty" do
      assert :ok == Batcher.flush(0)
    end

    test "flush after writes ensures all data is committed" do
      k = ukey("flush_verify")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "flushed", 0})
      :ok = Batcher.flush(shard_index)

      assert "flushed" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  describe "local apply gating" do
    test "await_local_applied returns timeout instead of exiting" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)

      assert {:error, :timeout} =
               Batcher.await_local_applied(shard_index, last_local_applied + 1_000, 25)
    end

    test "local quorum caller is not replied before local apply reaches the raft index" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{self(), reply_ref}],
          :single,
          System.monotonic_time()
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      refute_receive {^reply_ref, :ok}, 50

      send(batcher, {:locally_applied, ra_index})
      assert_receive {^reply_ref, :ok}, 1_000
    end

    test "flush waits for local apply waiters after raft apply drains pending" do
      shard_index = 0
      batcher = Batcher.batcher_name(shard_index)
      %{last_local_applied: last_local_applied} = :sys.get_state(batcher)
      ra_index = last_local_applied + 1_000
      corr = make_ref()
      reply_ref = make_ref()

      on_exit(fn ->
        send(batcher, {:locally_applied, ra_index})
        Batcher.reset_pending(shard_index)
      end)

      :ok =
        Batcher.__inject_quorum_pending_at__(
          shard_index,
          corr,
          [{:remote_origin, :remote@nohost, {self(), reply_ref}}],
          :single,
          System.monotonic_time()
        )

      send(batcher, {:ra_event, :leader, {:applied, [{corr, {:applied_at, ra_index, :ok}}]}})

      flush_task = Task.async(fn -> Batcher.flush(shard_index) end)
      refute Task.yield(flush_task, 50)

      send(batcher, {:locally_applied, ra_index})
      assert :ok = Task.await(flush_task, 1_000)
      assert_receive {^reply_ref, {:remote_applied_at, ^ra_index, :ok}}, 1_000
    end
  end

  # ---------------------------------------------------------------------------
  # Expiry through batcher
  # ---------------------------------------------------------------------------

  describe "expiry through batcher" do
    test "put with TTL stores correct expiry" do
      k = ukey("ttl_batcher")
      future = System.os_time(:millisecond) + 60_000
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), k)

      :ok = Batcher.write(shard_index, {:put, k, "ttl_val", future})

      {value, expire_at_ms} = Router.get_meta(FerricStore.Instance.get(:default), k)
      assert value == "ttl_val"
      assert expire_at_ms == future
    end
  end
end
