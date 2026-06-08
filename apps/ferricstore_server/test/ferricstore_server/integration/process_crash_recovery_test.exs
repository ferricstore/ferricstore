defmodule FerricstoreServer.Integration.ProcessCrashRecoveryTest do
  @moduledoc """
  Crash recovery tests for supervised processes.

  Each test kills exactly ONE process, waits for supervisor restart, and
  verifies data integrity. Tests are spaced with sleep to avoid exhausting
  the top-level supervisor's max_restarts budget (default: 3 in 5 seconds).
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state
  @moduletag :shard_kill
  @moduletag timeout: 600_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive(30_000)
    :ok
  end

  setup do
    # Space out kills to stay within supervisor max_restarts budget.
    Process.sleep(1_000)
    ShardHelpers.wait_shards_alive(30_000)
    ShardHelpers.flush_all_keys()
    # Compact WAL so restart doesn't replay 6600 tests' worth of history
    ShardHelpers.compact_wal()

    on_exit(fn ->
      Ferricstore.MemoryGuard.reset_pressure_flags()
      ShardHelpers.wait_shards_alive(30_000)
    end)
  end

  defp ukey(base), do: "pcr_#{base}_#{:rand.uniform(9_999_999)}"

  defp kill_and_wait(name, timeout \\ 5000) do
    pid = Process.whereis(name)
    assert pid != nil, "Process #{inspect(name)} not found"
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, timeout

    Enum.reduce_while(1..div(timeout, 50), :waiting, fn _, _ ->
      case Process.whereis(name) do
        pid when is_pid(pid) ->
          if Process.alive?(pid), do: {:halt, :ok}, else: {:cont, :waiting}

        nil ->
          Process.sleep(50)
          {:cont, :waiting}
      end
    end)
  end

  # ===========================================================================
  # Shard GenServer — most critical
  # ===========================================================================

  test "shard crash: data survives and new writes work" do
    idx = 0
    k = ShardHelpers.key_for_shard(idx)
    Router.put(FerricStore.Instance.get(:default), k, "before_crash")
    ShardHelpers.flush_all_shards()

    ShardHelpers.kill_shard_safely(idx)
    ShardHelpers.wait_shards_alive(30_000)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "before_crash" end,
      "data should survive shard crash"
    )

    k2 = ukey("after_shard")

    ShardHelpers.eventually(
      fn ->
        Router.put(FerricStore.Instance.get(:default), k2, "new_write") == :ok
      end,
      "write should succeed after shard crash"
    )

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k2) == "new_write" end,
      "new writes should work after shard crash"
    )
  end

  test "shard crash: other shards unaffected" do
    keys =
      for i <- 0..3 do
        k = ShardHelpers.key_for_shard(i)
        Router.put(FerricStore.Instance.get(:default), k, "shard_#{i}")
        k
      end

    ShardHelpers.flush_all_shards()

    ShardHelpers.kill_shard_safely(0)
    ShardHelpers.wait_shards_alive(30_000)

    for {k, i} <- Enum.with_index(keys) do
      ShardHelpers.eventually(
        fn -> Router.get(FerricStore.Instance.get(:default), k) == "shard_#{i}" end,
        "shard #{i} data should be unaffected by shard 0 crash"
      )
    end
  end

  # ===========================================================================
  # Data-path processes
  # ===========================================================================

  test "Batcher crash: writes before are durable, writes after succeed" do
    k = ukey("batcher")
    Router.put(FerricStore.Instance.get(:default), k, "durable")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Raft.Batcher.batcher_name(0))

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "durable" end,
      "data should survive batcher crash"
    )

    k2 = ukey("batcher_post")
    Router.put(FerricStore.Instance.get(:default), k2, "after")
    ShardHelpers.flush_all_shards()

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k2) == "after" end,
      "writes should work after batcher crash"
    )
  end

  test "BitcaskWriter crash: data survives and writes resume" do
    k = ukey("writer")
    Router.put(FerricStore.Instance.get(:default), k, "durable")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Store.BitcaskWriter.writer_name(0))

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "durable" end,
      "data should survive BitcaskWriter crash"
    )

    k2 = ukey("writer_post")
    Router.put(FerricStore.Instance.get(:default), k2, "after")
    ShardHelpers.flush_all_shards()

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k2) == "after" end,
      "writes should work after BitcaskWriter crash"
    )
  end

  test "Batcher crash: quorum writes unaffected" do
    k = ukey("batcher")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    ShardHelpers.flush_all_shards()

    kill_and_wait(Ferricstore.Raft.Batcher.batcher_name(0))

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive Batcher crash"
    )
  end

  # ===========================================================================
  # Ancillary singletons
  # ===========================================================================

  test "Stats crash: data unaffected" do
    k = ukey("stats")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    kill_and_wait(Ferricstore.Stats)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive Stats crash"
    )
  end

  test "SlowLog crash: data unaffected" do
    k = ukey("slowlog")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    kill_and_wait(Ferricstore.SlowLog)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive SlowLog crash"
    )
  end

  test "PubSub crash: data unaffected" do
    k = ukey("pubsub")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    kill_and_wait(Ferricstore.PubSub)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive PubSub crash"
    )
  end

  test "Config crash: data unaffected" do
    k = ukey("config")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    kill_and_wait(Ferricstore.Config)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive Config crash"
    )
  end

  test "MemoryGuard crash: data unaffected" do
    k = ukey("memguard")
    Router.put(FerricStore.Instance.get(:default), k, "safe")
    kill_and_wait(Ferricstore.MemoryGuard)

    ShardHelpers.eventually(
      fn -> Router.get(FerricStore.Instance.get(:default), k) == "safe" end,
      "data should survive MemoryGuard crash"
    )
  end
end
