defmodule Ferricstore.Raft.WARaftSegmentLogMemoryRegistryTest do
  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  import Ferricstore.Test.Eventually, only: [assert_eventually: 1]

  @registry :ferricstore_waraft_segment_log_memory_registry

  # Every shard's segment log keeps its memory stats in one shared table. One
  # shard's Raft log restarting must not take the table, and with it the other
  # shards' stats, down with it: an append on another shard would then fail on
  # the missing table.
  test "a shard's Raft log restarting keeps the other shards' segment log memory stats" do
    ShardHelpers.flush_all_keys()
    ctx = FerricStore.Instance.get(:default)
    tag = System.unique_integer([:positive])
    for i <- 1..64, do: assert(:ok = Router.put(ctx, "registry-#{tag}-#{i}", "v", 0))

    logs = @registry |> :ets.tab2list() |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    assert length(logs) > 1

    # The log whose process holds the table, if one does, else the first.
    owner = :ets.info(@registry, :owner)
    restarted = Enum.find(logs, hd(logs), &(Process.whereis(&1) == owner))
    others = logs -- [restarted]

    pid = Process.whereis(restarted)
    monitor = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :killed}, 5_000

    try do
      assert_eventually(fn ->
        assert is_pid(Process.whereis(restarted)) and Process.whereis(restarted) != pid
      end)

      assert :ets.whereis(@registry) != :undefined

      for name <- others do
        assert [{^name, _dir, _count, _bytes, _first, _last}] = :ets.lookup(@registry, name)
      end
    after
      ShardHelpers.flush_all_keys()
    end
  end
end
