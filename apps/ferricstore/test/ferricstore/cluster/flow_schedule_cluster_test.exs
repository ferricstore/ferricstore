defmodule Ferricstore.Cluster.FlowScheduleClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :flow
  @moduletag timeout: 180_000

  alias Ferricstore.Test.ClusterHelper

  @shards 1

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  test "due schedule can fire once after Raft leader failover" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

    [writer | _] = nodes
    now_ms = System.system_time(:millisecond) + 60_000
    schedule_id = unique_id("cluster-schedule")
    target_id = unique_id("cluster-schedule-target")
    target_type = unique_id("cluster-schedule-type")
    target_partition = unique_id("cluster-schedule-partition")

    assert {:ok, _schedule} =
             remote_schedule_create(writer.name, schedule_id,
               kind: :one_shot,
               at_ms: now_ms,
               now_ms: now_ms - 1,
               target: [
                 id: target_id,
                 type: target_type,
                 partition_key: target_partition,
                 payload: "cluster-fire"
               ]
             )

    {_killed, remaining} = ClusterHelper.kill_leader(nodes, 0)
    assert :ok = ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 30_000)

    results =
      remaining
      |> Task.async_stream(
        fn node ->
          remote_schedule_fire_due(node.name,
            now_ms: now_ms,
            worker: "cluster-scheduler-#{node.index}"
          )
        end,
        max_concurrency: length(remaining),
        timeout: 60_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.all?(results, &match?({:ok, %{errors: []}}, &1))
    assert results |> Enum.map(fn {:ok, result} -> result.fired end) |> Enum.sum() == 1
    assert results |> Enum.map(fn {:ok, result} -> result.claimed end) |> Enum.sum() == 1

    reader = hd(remaining)

    eventually(fn ->
      assert {:ok, target} =
               remote_flow_get(reader.name, target_id,
                 partition_key: target_partition,
                 payload: true
               )

      assert target.type == target_type
      assert target.payload == "cluster-fire"
    end)

    assert {:ok, %{fired: 0, claimed: 0, errors: []}} =
             remote_schedule_fire_due(reader.name,
               now_ms: now_ms + 1,
               worker: "cluster-scheduler"
             )
  end

  defp remote_schedule_create(node_name, id, opts),
    do: :erpc.call(node_name, FerricStore, :flow_schedule_create, [id, opts], 30_000)

  defp remote_schedule_fire_due(node_name, opts),
    do: :erpc.call(node_name, FerricStore, :flow_schedule_fire_due, [opts], 60_000)

  defp remote_flow_get(node_name, id, opts),
    do: :erpc.call(node_name, FerricStore, :flow_get, [id, opts], 30_000)

  defp eventually(fun, attempts \\ 80, interval_ms \\ 100) do
    fun.()
  rescue
    e in [ExUnit.AssertionError] ->
      if attempts <= 1, do: reraise(e, __STACKTRACE__)
      Process.sleep(interval_ms)
      eventually(fun, attempts - 1, interval_ms)
  end

  defp unique_id(prefix), do: "#{prefix}:#{System.unique_integer([:positive])}"
end
