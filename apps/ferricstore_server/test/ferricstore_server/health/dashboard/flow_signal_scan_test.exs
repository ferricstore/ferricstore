defmodule FerricstoreServer.Health.Dashboard.FlowSignalScanTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    previous_history = Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun)
    previous_limit = Application.get_env(:ferricstore, :flow_dashboard_signal_scan_max_flows)

    on_exit(fn ->
      restore_env(:flow_dashboard_flow_history_fun, previous_history)
      restore_env(:flow_dashboard_signal_scan_max_flows, previous_limit)
    end)

    :ok
  end

  test "history scan enforces its configured flow budget" do
    flow_type = "dashboard-signal-budget-#{System.unique_integer([:positive])}"
    test_pid = self()

    ids =
      Enum.map(1..7, fn sequence ->
        id = "dashboard-signal-budget-#{sequence}-#{System.unique_integer([:positive])}"

        assert :ok =
                 FerricStore.flow_create(id,
                   type: flow_type,
                   state: "queued",
                   run_at_ms: sequence,
                   now_ms: sequence
                 )

        id
      end)

    Application.put_env(:ferricstore, :flow_dashboard_signal_scan_max_flows, 4)

    Application.put_env(:ferricstore, :flow_dashboard_flow_history_fun, fn id, _opts ->
      send(test_pid, {:bounded_signal_history_scan, id})
      {:ok, []}
    end)

    data = Dashboard.collect_flow_signals_page(type: flow_type, scan_history: true, limit: 40)

    scanned_ids =
      Enum.map(1..4, fn _ ->
        assert_receive {:bounded_signal_history_scan, id}
        id
      end)

    refute_receive {:bounded_signal_history_scan, _id}
    assert MapSet.subset?(MapSet.new(scanned_ids), MapSet.new(ids))
    assert data.signal_scan.inspected_flows == 4
    assert data.signal_scan.completed_flows == 4
    assert data.signal_scan.failed_flows == 0
    assert data.signal_scan.sampled_flows == 7
    assert data.signal_scan.truncated
    refute data.signal_scan.auto_refresh
  end
end
