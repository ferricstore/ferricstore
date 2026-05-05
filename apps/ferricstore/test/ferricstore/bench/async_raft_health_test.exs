defmodule Ferricstore.Bench.AsyncRaftHealthTest do
  use ExUnit.Case, async: false
  @moduletag skip: "async durability feature removed; quorum is the only supported durability"
  @moduletag :bench
  @moduletag timeout: 120_000

  test "async write blast: check raft leader mailbox and replication" do
    Ferricstore.NamespaceConfig.set("ahealth", "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set("ahealth", "durability", "quorum")
    end)

    value = String.duplicate("v", 100)

    IO.puts("\n=== Async Raft Health Check ===\n")

    # Get ra leader PID for shard 0
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(0)
    {:ok, _, leader} = :ra.members(shard_id)
    {leader_name, _node} = leader
    leader_pid = Process.whereis(leader_name)

    IO.puts("  Leader: #{inspect(leader)} (PID: #{inspect(leader_pid)})")

    # Check mailbox before
    {:message_queue_len, mq_before} = Process.info(leader_pid, :message_queue_len)
    IO.puts("  Mailbox before: #{mq_before}")

    # Blast 100K async writes
    num_writes = 100_000
    {write_us, _} = :timer.tc(fn ->
      for i <- 1..num_writes do
        FerricStore.set("ahealth:k#{i}", value)
      end
    end)

    wps = div(num_writes * 1_000_000, write_us)
    IO.puts("  Wrote #{num_writes} keys in #{div(write_us, 1000)}ms (#{wps}/sec)")

    # Check mailbox immediately after
    {:message_queue_len, mq_after} = Process.info(leader_pid, :message_queue_len)
    IO.puts("  Mailbox immediately after: #{mq_after}")

    # Check at intervals
    for delay <- [100, 500, 1000, 2000, 5000] do
      Process.sleep(delay)
      case Process.info(leader_pid, :message_queue_len) do
        {:message_queue_len, mq} ->
          IO.puts("  Mailbox after #{delay}ms: #{mq}")
        nil ->
          IO.puts("  Mailbox after #{delay}ms: LEADER DEAD!")
      end
    end

    # Verify leader is still alive
    alive = Process.alive?(leader_pid)
    IO.puts("\n  Leader still alive: #{alive}")

    # Verify data is readable (from local ETS)
    sample_results =
      for i <- [1, 1000, 50_000, 100_000] do
        {:ok, result} = FerricStore.get("ahealth:k#{i}")
        {i, result != nil}
      end

    IO.puts("  Sample reads (from ETS): #{inspect(sample_results)}")

    # Verify raft can still process quorum writes
    {quorum_us, _} = :timer.tc(fn ->
      Ferricstore.Store.Router.put(FerricStore.Instance.get(:default), "quorum_health_check", "alive", 0)
    end)
    IO.puts("  Quorum write after blast: #{quorum_us}us")
    IO.puts("  Quorum read: #{inspect(Ferricstore.Store.Router.get(FerricStore.Instance.get(:default), "quorum_health_check"))}")

    assert alive, "Ra leader should still be alive after 100K async writes"
  end
end
