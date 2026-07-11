defmodule Ferricstore.Cluster.FlowStateMetaRetentionClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :flow
  @moduletag timeout: 180_000

  alias Ferricstore.Test.ClusterHelper

  @shards 2

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "requires OTP 25+ for :peer"
    end

    :ok
  end

  test "retention cleanup removes indexed state_meta projections after leader failover" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards, timeout: 60_000)

    try do
      writer = hd(nodes)
      now = System.system_time(:millisecond)
      unique = System.unique_integer([:positive])
      type = "cluster-state-meta-retention-type-#{unique}"

      assert {:ok, policy} =
               remote_flow(writer.name, :flow_policy_set, [
                 type,
                 [indexed_state_meta: "version"]
               ])

      assert policy.indexed_state_meta == "version"

      specs =
        for shard <- 0..(@shards - 1) do
          id = "cluster-state-meta-retention-#{unique}-#{shard}"
          partition_key = partition_for_shard(writer.name, id, shard)

          assert remote_flow_state_shard(writer.name, id, partition_key) == shard

          %{
            id: id,
            partition_key: partition_key,
            initial_version: shard + 1,
            terminal_version: shard + 101,
            shard: shard
          }
        end

      Enum.each(specs, fn spec ->
        create_and_complete_expired_flow(writer.name, type, spec, now)
      end)

      Enum.each(specs, fn spec ->
        eventually(fn ->
          assert {:ok, records} =
                   remote_flow(writer.name, :flow_search, [
                     [
                       type: type,
                       partition_key: spec.partition_key,
                       state_meta: %{"completed" => %{"version" => spec.terminal_version}},
                       consistent_projection: true,
                       count: 10
                     ]
                   ])

          assert Enum.map(records, & &1.id) == [spec.id]
        end)
      end)

      target_shard = hd(specs).shard
      {_killed, remaining} = ClusterHelper.kill_leader(nodes, target_shard)
      assert :ok = ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 60_000)

      cleaner = hd(remaining)

      assert cleanup_after_failover(cleaner.name, now + 2_000, length(specs)) >= length(specs)

      Enum.each(specs, fn spec ->
        eventually(fn ->
          Enum.each(remaining, fn node ->
            assert :ok = remote_flush_lmdb(node.name)

            assert {:ok, []} =
                     remote_flow(node.name, :flow_search, [
                       [
                         type: type,
                         partition_key: spec.partition_key,
                         state_meta: %{"completed" => %{"version" => spec.terminal_version}},
                         consistent_projection: true,
                         count: 10
                       ]
                     ])

            assert flow_absent?(
                     remote_flow(node.name, :flow_get, [
                       spec.id,
                       [partition_key: spec.partition_key]
                     ])
                   )
          end)
        end)
      end)
    after
      ClusterHelper.stop_cluster(nodes)
    end
  end

  defp create_and_complete_expired_flow(node_name, type, spec, now) do
    assert :ok =
             remote_flow(node_name, :flow_create, [
               spec.id,
               [
                 type: type,
                 state: "accept",
                 partition_key: spec.partition_key,
                 state_meta: %{"version" => spec.initial_version},
                 retention_ttl_ms: 1_000,
                 run_at_ms: now,
                 now_ms: now
               ]
             ])

    assert {:ok, [claimed]} =
             remote_flow(node_name, :flow_claim_due, [
               type,
               [
                 states: ["accept"],
                 partition_key: spec.partition_key,
                 worker: "cluster-state-meta-worker",
                 limit: 1,
                 now_ms: now + 1
               ]
             ])

    assert :ok =
             remote_flow(node_name, :flow_complete, [
               spec.id,
               claimed.lease_token,
               [
                 fencing_token: claimed.fencing_token,
                 partition_key: spec.partition_key,
                 state_meta: %{"version" => spec.terminal_version},
                 now_ms: now + 2
               ]
             ])
  end

  defp remote_flow(node_name, fun, args) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    :erpc.call(node_name, FerricStore, fun, args, 60_000)
  end

  defp remote_flush_lmdb(node_name) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)

    :erpc.call(
      node_name,
      Ferricstore.Flow.LMDBWriter,
      :flush_all,
      [ctx.name, ctx.shard_count, 30_000],
      30_000
    )
  end

  defp remote_flow_state_shard(node_name, id, partition_key) do
    :ok = ClusterHelper.ensure_node_reachable(node_name, timeout: 5_000)
    ctx = :erpc.call(node_name, FerricStore.Instance, :get, [:default], 5_000)
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    :erpc.call(node_name, Ferricstore.Store.Router, :shard_for, [ctx, state_key], 5_000)
  end

  defp partition_for_shard(node_name, id, shard) do
    Enum.find_value(1..10_000, fn index ->
      partition_key = "cluster-state-meta-partition-#{shard}-#{index}"
      if remote_flow_state_shard(node_name, id, partition_key) == shard, do: partition_key
    end) || flunk("could not find Flow partition key for shard #{shard}")
  end

  defp flow_absent?({:ok, nil}), do: true
  defp flow_absent?({:error, _reason}), do: true
  defp flow_absent?(_other), do: false

  defp cleanup_after_failover(node_name, now_ms, expected, attempts \\ 120, cleaned \\ 0)

  defp cleanup_after_failover(_node_name, _now_ms, expected, _attempts, cleaned)
       when cleaned >= expected,
       do: cleaned

  defp cleanup_after_failover(node_name, now_ms, expected, attempts, cleaned)
       when attempts > 0 do
    assert {:ok, result} =
             remote_flow(node_name, :flow_retention_cleanup, [[limit: 20, now_ms: now_ms]])

    cleaned = cleaned + result.flows

    if cleaned >= expected do
      cleaned
    else
      Process.sleep(250)
      cleanup_after_failover(node_name, now_ms, expected, attempts - 1, cleaned)
    end
  end

  defp cleanup_after_failover(_node_name, _now_ms, expected, 0, cleaned) do
    flunk("retention cleanup removed #{cleaned} of #{expected} flows after failover")
  end

  defp eventually(fun, attempts \\ 120, interval_ms \\ 250)

  defp eventually(fun, attempts, interval_ms) when attempts > 0 do
    fun.()
  rescue
    error in [ExUnit.AssertionError, RuntimeError] ->
      if attempts == 1 do
        reraise(error, __STACKTRACE__)
      else
        Process.sleep(interval_ms)
        eventually(fun, attempts - 1, interval_ms)
      end
  end
end
