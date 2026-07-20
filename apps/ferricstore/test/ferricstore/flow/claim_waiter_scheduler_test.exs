defmodule Ferricstore.Flow.ClaimWaiterSchedulerTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.{
    ClaimWaiters,
    ClaimWaiterScheduler,
    Keys,
    LMDB,
    Locator,
    StorageScope
  }

  alias Ferricstore.Store.Router

  test "schedule_next_due fails closed for unsupported inputs" do
    assert ClaimWaiterScheduler.schedule_next_due(%{}, :bad_type, :any, nil, :any, nil) == :ok
  end

  test "schedule_next_due tolerates missing router/index context" do
    assert ClaimWaiterScheduler.schedule_next_due(%{}, "email", :any, nil, :any, nil) == :ok
  end

  test "broad hot scheduling aggregates the earliest score without copying all due keys" do
    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/claim_waiter_scheduler.ex", __DIR__))

    assert source =~ "Router.flow_earliest_due_score"
    refute source =~ "Router.flow_due_count_keys"
  end

  test "scoped auto scheduling uses one aggregate probe instead of per-bucket fanout" do
    scope = <<11::unsigned-big-64>>

    partitions =
      Enum.map(Keys.auto_partition_keys(), fn logical_partition ->
        assert {:ok, physical_partition} =
                 StorageScope.physical_partition_key(logical_partition, scope)

        physical_partition
      end)

    calls =
      traced_router_calls(fn ->
        ClaimWaiterScheduler.schedule_next_due(
          %{},
          "email",
          "queued",
          nil,
          partitions,
          0
        )
      end)

    assert calls.rank == 0
    assert calls.aggregate <= 1
  end

  test "unbounded due-count-key router and shard endpoints are not compiled" do
    router_source =
      File.read!(Path.expand("../../../lib/ferricstore/store/router/part_11.ex", __DIR__))

    shard_source =
      File.read!(Path.expand("../../../lib/ferricstore/store/shard/calls.ex", __DIR__))

    refute router_source =~ "def flow_due_count_keys("
    refute router_source =~ ":flow_due_count_keys"
    refute shard_source =~ "handle_call(:flow_due_count_keys"
  end

  test "broad cold scheduling stays correct when a bucket exceeds the scan limit" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-claim-waiter-scheduler-#{System.unique_integer([:positive])}"
      )

    lmdb_path = data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
    File.mkdir_p!(lmdb_path)

    previous_hibernation = Application.get_env(:ferricstore, :flow_hibernation_enabled)
    Application.put_env(:ferricstore, :flow_hibernation_enabled, true)
    Ferricstore.Flow.Hibernation.refresh_config!()

    type = "cold-overflow"
    matching_state = "z-matching-state"
    due_at_ms = System.system_time(:millisecond) + 5 * 60_000
    bucket_ms = LMDB.cold_due_bucket_ms(due_at_ms)
    park_key = LMDB.cold_park_key_for_state_key("matching-state-key")

    locator =
      Locator.new!(
        flow_id: "matching-flow",
        kind: :state,
        version: 1,
        raft_index: 1,
        file_id: 0,
        offset: 0,
        value_size: 0
      )

    decoy_ops =
      for index <- 1..1_000 do
        due_key =
          LMDB.cold_due_key(
            bucket_ms: bucket_ms,
            type: type,
            state: "a-decoy-#{String.pad_leading(Integer.to_string(index), 4, "0")}",
            partition_key: "decoy",
            priority: 0,
            due_at_ms: due_at_ms,
            flow_id: "decoy-#{index}",
            version: 1
          )

        {:put, due_key, "missing-park-row"}
      end

    matching_due_key =
      LMDB.cold_due_key(
        bucket_ms: bucket_ms,
        type: type,
        state: matching_state,
        partition_key: "matching-partition",
        priority: 0,
        due_at_ms: due_at_ms,
        flow_id: "matching-flow",
        version: 1
      )

    matching_park =
      LMDB.encode_cold_park(locator,
        due_at_ms: due_at_ms,
        type: type,
        state: matching_state,
        partition_key: "matching-partition",
        state_key: "matching-state-key",
        priority: 0
      )

    timer_key = {type, matching_state, 0, :any, bucket_ms}

    on_exit(fn ->
      ClaimWaiters.notify_scheduled_ready(timer_key)
      File.rm_rf!(data_dir)

      case previous_hibernation do
        nil -> Application.delete_env(:ferricstore, :flow_hibernation_enabled)
        value -> Application.put_env(:ferricstore, :flow_hibernation_enabled, value)
      end

      Ferricstore.Flow.Hibernation.refresh_config!()
    end)

    assert :ok = LMDB.write_batch(lmdb_path, decoy_ops)
    before_count = ClaimWaiters.scheduled_count()

    assert :ok =
             ClaimWaiterScheduler.schedule_next_due(
               %{data_dir: data_dir, shard_count: 1},
               type,
               matching_state,
               0,
               :any,
               10 * 60_000
             )

    assert ClaimWaiters.scheduled_count() == before_count

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, matching_due_key, park_key},
               {:put, park_key, matching_park}
             ])

    assert :ok =
             ClaimWaiterScheduler.schedule_next_due(
               %{data_dir: data_dir, shard_count: 1},
               type,
               matching_state,
               0,
               :any,
               10 * 60_000
             )

    assert ClaimWaiters.scheduled_count() == before_count + 1
  end

  test "cold scheduling stops probing after the earliest matching bucket" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-claim-waiter-probes-#{System.unique_integer([:positive])}"
      )

    lmdb_path = data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
    File.mkdir_p!(lmdb_path)

    previous_hibernation = Application.get_env(:ferricstore, :flow_hibernation_enabled)
    Application.put_env(:ferricstore, :flow_hibernation_enabled, true)
    Ferricstore.Flow.Hibernation.refresh_config!()

    type = "cold-probe-count"
    state = "queued"
    partition = "tenant"
    priority = 0
    due_at_ms = System.system_time(:millisecond) + 30_000
    bucket_ms = LMDB.cold_due_bucket_ms(due_at_ms)
    park_key = LMDB.cold_park_key_for_state_key("probe-state-key")

    locator =
      Locator.new!(
        flow_id: "probe-flow",
        kind: :state,
        version: 1,
        raft_index: 1,
        file_id: 0,
        offset: 0,
        value_size: 0
      )

    due_key =
      LMDB.cold_due_key(
        bucket_ms: bucket_ms,
        type: type,
        state: state,
        partition_key: partition,
        priority: priority,
        due_at_ms: due_at_ms,
        flow_id: "probe-flow",
        version: 1
      )

    park =
      LMDB.encode_cold_park(locator,
        due_at_ms: due_at_ms,
        type: type,
        state: state,
        partition_key: partition,
        state_key: "probe-state-key",
        priority: priority
      )

    timer_due_at_ms = div(due_at_ms + 9, 10) * 10
    timer_key = {type, state, priority, partition, timer_due_at_ms}

    on_exit(fn ->
      ClaimWaiters.notify_scheduled_ready(timer_key)
      File.rm_rf!(data_dir)

      case previous_hibernation do
        nil -> Application.delete_env(:ferricstore, :flow_hibernation_enabled)
        value -> Application.put_env(:ferricstore, :flow_hibernation_enabled, value)
      end

      Ferricstore.Flow.Hibernation.refresh_config!()
    end)

    assert :ok = LMDB.write_batch(lmdb_path, [{:put, due_key, park_key}, {:put, park_key, park}])

    prefix_calls =
      traced_prefix_call_count(fn ->
        ClaimWaiterScheduler.schedule_next_due(
          %{data_dir: data_dir, shard_count: 1},
          type,
          state,
          priority,
          partition,
          24 * 60 * 60 * 1_000
        )
      end)

    assert prefix_calls <= 4
  end

  test "exact cold scheduling stays conservative when stale rows fill the scan window" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-claim-waiter-exact-overflow-#{System.unique_integer([:positive])}"
      )

    lmdb_path = data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
    File.mkdir_p!(lmdb_path)

    previous_hibernation = Application.get_env(:ferricstore, :flow_hibernation_enabled)
    Application.put_env(:ferricstore, :flow_hibernation_enabled, true)
    Ferricstore.Flow.Hibernation.refresh_config!()

    type = "exact-overflow"
    state = "queued"
    partition = "tenant"
    priority = 0
    due_at_ms = System.system_time(:millisecond) + 5 * 60_000
    bucket_ms = LMDB.cold_due_bucket_ms(due_at_ms)
    park_key = LMDB.cold_park_key_for_state_key("exact-matching-state-key")

    locator =
      Locator.new!(
        flow_id: "z-matching-flow",
        kind: :state,
        version: 1,
        raft_index: 1,
        file_id: 0,
        offset: 0,
        value_size: 0
      )

    stale_ops =
      for index <- 1..1_000 do
        due_key =
          LMDB.cold_due_key(
            bucket_ms: bucket_ms,
            type: type,
            state: state,
            partition_key: partition,
            priority: priority,
            due_at_ms: due_at_ms,
            flow_id: "a-stale-#{String.pad_leading(Integer.to_string(index), 4, "0")}",
            version: 1
          )

        {:put, due_key, "missing-park-row"}
      end

    matching_due_key =
      LMDB.cold_due_key(
        bucket_ms: bucket_ms,
        type: type,
        state: state,
        partition_key: partition,
        priority: priority,
        due_at_ms: due_at_ms,
        flow_id: "z-matching-flow",
        version: 1
      )

    matching_park =
      LMDB.encode_cold_park(locator,
        due_at_ms: due_at_ms,
        type: type,
        state: state,
        partition_key: partition,
        state_key: "exact-matching-state-key",
        priority: priority
      )

    timer_due_at_ms = div(bucket_ms + 9, 10) * 10
    timer_key = {type, state, priority, partition, timer_due_at_ms}

    on_exit(fn ->
      ClaimWaiters.notify_scheduled_ready(timer_key)
      File.rm_rf!(data_dir)

      case previous_hibernation do
        nil -> Application.delete_env(:ferricstore, :flow_hibernation_enabled)
        value -> Application.put_env(:ferricstore, :flow_hibernation_enabled, value)
      end

      Ferricstore.Flow.Hibernation.refresh_config!()
    end)

    assert :ok = LMDB.write_batch(lmdb_path, stale_ops)
    before_count = ClaimWaiters.scheduled_count()

    assert :ok =
             ClaimWaiterScheduler.schedule_next_due(
               %{data_dir: data_dir, shard_count: 1},
               type,
               state,
               priority,
               partition,
               10 * 60_000
             )

    assert ClaimWaiters.scheduled_count() == before_count

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, matching_due_key, park_key},
               {:put, park_key, matching_park}
             ])

    assert :ok =
             ClaimWaiterScheduler.schedule_next_due(
               %{data_dir: data_dir, shard_count: 1},
               type,
               state,
               priority,
               partition,
               10 * 60_000
             )

    assert ClaimWaiters.scheduled_count() == before_count + 1
  end

  test "cold auto-partition matching accepts only canonical generated buckets" do
    assert ClaimWaiterScheduler.__cold_partition_match_for_test__("__flow_auto__:0", :auto)
    assert ClaimWaiterScheduler.__cold_partition_match_for_test__("__flow_auto__:255", :auto)

    refute ClaimWaiterScheduler.__cold_partition_match_for_test__("__flow_auto__:01", :auto)
    refute ClaimWaiterScheduler.__cold_partition_match_for_test__("__flow_auto__:256", :auto)
    refute ClaimWaiterScheduler.__cold_partition_match_for_test__("__flow_auto__:manual", :auto)

    tenant_scope = <<11::unsigned-big-64>>
    other_scope = <<22::unsigned-big-64>>

    assert {:ok, scoped_auto} =
             StorageScope.physical_partition_key("__flow_auto__:17", tenant_scope)

    assert {:ok, scoped_explicit} =
             StorageScope.physical_partition_key("explicit", tenant_scope)

    assert {:ok, other_tenant_auto} =
             StorageScope.physical_partition_key("__flow_auto__:17", other_scope)

    assert ClaimWaiterScheduler.__cold_partition_match_for_test__(
             scoped_auto,
             {:scoped_auto, tenant_scope}
           )

    refute ClaimWaiterScheduler.__cold_partition_match_for_test__(
             scoped_explicit,
             {:scoped_auto, tenant_scope}
           )

    refute ClaimWaiterScheduler.__cold_partition_match_for_test__(
             other_tenant_auto,
             {:scoped_auto, tenant_scope}
           )
  end

  defp traced_prefix_call_count(fun) do
    parent = self()

    pid =
      spawn(fn ->
        result = fun.()
        send(parent, {:cold_schedule_done, self(), result})
      end)

    1 = :erlang.trace(pid, true, [:call, {:tracer, parent}])
    1 = :erlang.trace_pattern({LMDB, :prefix_entries, 3}, true, [:local])

    try do
      collect_prefix_calls(pid, 0)
    after
      :erlang.trace_pattern({LMDB, :prefix_entries, 3}, false, [:local])
    end
  end

  defp traced_router_calls(fun) do
    parent = self()

    pid =
      spawn(fn ->
        receive do
          :run ->
            result = fun.()
            send(parent, {:router_schedule_done, self(), result})
        end
      end)

    1 = :erlang.trace(pid, true, [:call, {:tracer, parent}])
    1 = :erlang.trace_pattern({Router, :flow_index_rank_range, 5}, true, [:local])
    1 = :erlang.trace_pattern({Router, :flow_earliest_due_score, 4}, true, [:local])
    send(pid, :run)

    try do
      collect_router_calls(pid, %{rank: 0, aggregate: 0})
    after
      :erlang.trace_pattern({Router, :flow_index_rank_range, 5}, false, [:local])
      :erlang.trace_pattern({Router, :flow_earliest_due_score, 4}, false, [:local])
    end
  end

  defp collect_router_calls(pid, counts) do
    receive do
      {:trace, ^pid, :call, {Router, :flow_index_rank_range, _arguments}} ->
        collect_router_calls(pid, Map.update!(counts, :rank, &(&1 + 1)))

      {:trace, ^pid, :call, {Router, :flow_earliest_due_score, _arguments}} ->
        collect_router_calls(pid, Map.update!(counts, :aggregate, &(&1 + 1)))

      {:router_schedule_done, ^pid, :ok} ->
        counts
    after
      5_000 -> flunk("scoped auto scheduling did not complete")
    end
  end

  defp collect_prefix_calls(pid, count) do
    receive do
      {:trace, ^pid, :call, {LMDB, :prefix_entries, _args}} ->
        collect_prefix_calls(pid, count + 1)

      {:cold_schedule_done, ^pid, :ok} ->
        count
    after
      10_000 -> flunk("cold scheduling did not finish")
    end
  end
end
