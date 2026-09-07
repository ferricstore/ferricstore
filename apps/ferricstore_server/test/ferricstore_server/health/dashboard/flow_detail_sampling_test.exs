defmodule FerricstoreServer.Health.Dashboard.FlowDetailSamplingTest do
  use FerricstoreServer.Test.DashboardCase

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Health.Dashboard
  alias FerricstoreServer.Health.Dashboard.Flow.Sample

  test "FIFO detail shares one bounded durable sample between record lookup and lane rendering" do
    type = "detail-sample-#{System.unique_integer([:positive])}"
    partition = type
    assert {:ok, _} = FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})

    for n <- 1..12 do
      assert :ok =
               FerricStore.flow_create("#{type}-#{n}",
                 type: type,
                 state: "queued",
                 partition_key: partition
               )
    end

    id = "#{type}-1"

    {data, calls} =
      trace_collection(fn ->
        Dashboard.collect_flow_detail_page(id, partition_key: partition, values: false)
      end)

    assert data.record_status == :ok
    assert data.values_status == :skipped
    assert data.fifo_lane.count == 12
    assert is_integer(data.record.state_enter_seq)

    assert Enum.find(data.fifo_lane.members, &(&1.id == id)).state_enter_seq ==
             data.record.state_enter_seq

    assert Enum.count(calls, &match?({Sample, :collect_flow_records_sample, [400]}, &1)) == 1

    keys = for {Router, :get, [_ctx, key]} <- calls, Keys.state_key?(key), do: key
    assert keys != []
    assert length(keys) == length(Enum.uniq(keys))

    assert :ok =
             FerricStore.flow_create("#{type}-13",
               type: type,
               state: "queued",
               partition_key: partition
             )

    refreshed = Dashboard.collect_flow_detail_page(id, partition_key: partition, values: false)
    assert refreshed.fifo_lane.count == 13
  end

  test "one shared sample retains the bounded payload-free fallback for an unsampled FIFO record" do
    type = "detail-fallback-#{System.unique_integer([:positive])}"
    previous_get = Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun)
    previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
    on_exit(fn -> restore_env(:flow_dashboard_flow_get_fun, previous_get) end)
    on_exit(fn -> restore_env(:flow_dashboard_flow_history_fun, previous_history) end)
    assert {:ok, _} = FerricStore.flow_policy_set(type, states: %{"queued" => [mode: :fifo]})
    parent = self()
    record = %{id: type, type: type, state: "queued", partition_key: type, run_at_ms: 1}

    Application.put_env(:ferricstore, :flow_dashboard_flow_get_fun, fn id, opts ->
      send(parent, {:fallback, id, opts})
      {:ok, record}
    end)

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn _, _ -> {:ok, []} end)

    {data, calls} =
      trace_collection(fn ->
        Dashboard.collect_flow_detail_page(type, partition_key: type, values: false)
      end)

    assert_receive {:fallback, ^type, [payload: false, partition_key: ^type]}
    assert data.record == record
    assert data.fifo_lane.count == 1
    refute data.fifo_lane.order_known
    assert Enum.count(calls, &match?({Sample, :collect_flow_records_sample, [400]}, &1)) == 1
  end

  defp trace_collection(fun) do
    parent = self()

    pid =
      spawn(fn ->
        receive do
          :collect ->
            send(parent, {:collected, self(), fun.()})
            receive do: (:stop -> :ok)
        end
      end)

    patterns = [{Sample, :collect_flow_records_sample, 1}, {Router, :get, 2}]

    Enum.each(patterns, fn {module, _, _} = pattern ->
      Code.ensure_loaded!(module)
      assert :erlang.trace_pattern(pattern, true, [:local]) == 1
    end)

    :erlang.trace(pid, true, [:call, {:tracer, parent}])

    try do
      send(pid, :collect)
      assert_receive {:collected, ^pid, data}, 10_000
      delivered = :erlang.trace_delivered(pid)
      assert_receive {:trace_delivered, ^pid, ^delivered}, 5_000
      {data, drain_calls(pid, [])}
    after
      :erlang.trace(pid, false, [:call])
      Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local]))
      send(pid, :stop)
    end
  end

  defp drain_calls(pid, calls) do
    receive do
      {:trace, ^pid, :call, call} -> drain_calls(pid, [call | calls])
    after
      0 -> Enum.reverse(calls)
    end
  end
end
