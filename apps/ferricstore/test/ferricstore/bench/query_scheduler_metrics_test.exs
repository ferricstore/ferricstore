Code.require_file(Path.expand("../../../../../bench/support/scheduler_metrics.exs", __DIR__))

defmodule Ferricstore.Bench.SchedulerMetricsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bench.SchedulerMetrics

  test "calculates aggregate and busiest-scheduler utilization from deltas" do
    before = %{
      normal: %{1 => {10, 100}, 2 => {20, 100}},
      dirty_cpu: %{3 => {0, 100}},
      dirty_io: %{4 => {50, 100}, 5 => {25, 100}}
    }

    after_snapshot = %{
      normal: %{1 => {30, 200}, 2 => {60, 200}},
      dirty_cpu: %{3 => {10, 200}},
      dirty_io: %{4 => {150, 200}, 5 => {50, 200}}
    }

    assert SchedulerMetrics.utilization(before, after_snapshot) == %{
             normal: %{
               schedulers: 2,
               utilization_percent: 30.0,
               max_scheduler_utilization_percent: 40.0
             },
             dirty_cpu: %{
               schedulers: 1,
               utilization_percent: 10.0,
               max_scheduler_utilization_percent: 10.0
             },
             dirty_io: %{
               schedulers: 2,
               utilization_percent: 62.5,
               max_scheduler_utilization_percent: 100.0
             }
           }
  end

  test "returns zero utilization for an empty observation interval" do
    snapshot = %{normal: %{}, dirty_cpu: %{}, dirty_io: %{}}

    assert SchedulerMetrics.utilization(snapshot, snapshot) == %{
             normal: %{
               schedulers: 0,
               utilization_percent: 0.0,
               max_scheduler_utilization_percent: 0.0
             },
             dirty_cpu: %{
               schedulers: 0,
               utilization_percent: 0.0,
               max_scheduler_utilization_percent: 0.0
             },
             dirty_io: %{
               schedulers: 0,
               utilization_percent: 0.0,
               max_scheduler_utilization_percent: 0.0
             }
           }
  end
end
