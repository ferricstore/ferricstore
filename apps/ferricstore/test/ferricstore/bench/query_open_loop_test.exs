Code.require_file(Path.expand("../../../../../bench/support/open_loop.exs", __DIR__))

defmodule Ferricstore.Bench.OpenLoopTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Bench.OpenLoop

  test "keeps offering work while enforcing concurrency and queue bounds" do
    {:ok, generator} =
      OpenLoop.start_link(
        target_qps: 1_000,
        concurrency: 1,
        max_queue: 2,
        job_fun: fn sequence ->
          %{sequence: sequence, phase: "overload", workload: "query"}
        end,
        execute_fun: fn _job ->
          Process.sleep(10)
          :ok
        end
      )

    Process.sleep(60)
    stats = OpenLoop.stop(generator, 5_000)

    assert stats.offered >= 30
    assert stats.started == stats.completed
    assert stats.completed + stats.dropped == stats.offered
    assert stats.succeeded == stats.completed
    assert stats.failed == 0
    assert stats.dropped > 0
    assert stats.max_in_flight == 1
    assert stats.max_queue_depth == 2
    assert stats.completed_at_stop < stats.completed
    assert stats.queue_depth_at_stop <= 2
    assert stats.in_flight_at_stop <= 1
    assert stats.offer_duration_us > 0
    assert stats.drain_duration_us > 0
    assert stats.latency_histograms.scheduler.count == stats.completed
    assert stats.latency_histograms.dispatch.count == stats.completed
    assert stats.latency_histograms.queue.count == stats.completed
    assert stats.latency_histograms.service.count == stats.completed
    assert stats.latency_histograms.end_to_end.count == stats.completed

    summary = OpenLoop.histogram_percentiles(stats.latency_histograms.end_to_end)
    assert summary.min <= summary.p50
    assert summary.p50 <= summary.p99
    assert summary.p99 <= summary.max

    phase = stats.phases["overload"]
    workload = phase.workloads["query"]

    assert phase.offered == stats.offered
    assert phase.completed == stats.completed
    assert phase.dropped == stats.dropped
    assert phase.completed_at_close < phase.completed
    assert phase.outstanding_at_close == phase.offered - phase.completed_at_close - phase.dropped
    assert phase.pending_at_close <= 2
    assert phase.in_flight_at_close <= 1
    assert workload.offered == stats.offered
    assert workload.completed == stats.completed
    assert is_integer(workload.first_scheduled_us)
    assert workload.last_scheduled_us >= workload.first_scheduled_us
    refute Process.alive?(generator)
  end

  test "records execution failures without confusing them with admission drops" do
    {:ok, generator} =
      OpenLoop.start_link(
        target_qps: 100,
        concurrency: 4,
        max_queue: 8,
        job_fun: fn sequence ->
          %{
            sequence: sequence,
            phase: "steady",
            workload: if(rem(sequence, 2) == 0, do: "even", else: "odd")
          }
        end,
        execute_fun: fn job ->
          if rem(job.sequence, 3) == 0,
            do: {:error, :synthetic_failure},
            else:
              {:ok,
               %{
                 measurements_us: %{executor: job.sequence + 1},
                 observations: %{
                   scanned_entries: job.sequence + 2,
                   query_executions: if(rem(job.sequence, 10) == 1, do: 2, else: 1)
                 }
               }}
        end
      )

    Process.sleep(100)
    stats = OpenLoop.stop(generator, 5_000)

    assert stats.offered >= 5
    assert stats.dropped == 0
    assert stats.started == stats.offered
    assert stats.completed == stats.offered
    assert stats.succeeded + stats.failed == stats.completed
    assert stats.failed > 0
    assert stats.failures[inspect(:synthetic_failure)] == stats.failed
    assert stats.measurement_histograms.executor.count == stats.succeeded

    executor_summary = OpenLoop.histogram_percentiles(stats.measurement_histograms.executor)
    assert executor_summary.min >= 1
    assert executor_summary.max >= executor_summary.p99

    scan_summary = OpenLoop.histogram_percentiles(stats.observation_histograms.scanned_entries)
    assert scan_summary.min >= 2
    assert scan_summary.max >= scan_summary.p99

    execution_summary =
      OpenLoop.histogram_percentiles(stats.observation_histograms.query_executions)

    assert execution_summary.min == 1
    assert execution_summary.p50 == 1
    assert execution_summary.max == 2
    assert execution_summary.count == stats.succeeded
    assert stats.phases["steady"].offered == stats.offered

    attributed =
      stats.phases["steady"].workloads
      |> Map.values()
      |> Enum.sum_by(& &1.offered)

    assert attributed == stats.offered
  end
end
