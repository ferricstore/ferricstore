Code.require_file("support/open_loop.exs", __DIR__)
Code.require_file("support/scheduler_metrics.exs", __DIR__)
Code.require_file("support/query_workload_matrix.exs", __DIR__)
Code.require_file("support/query_soak_fixture.exs", __DIR__)

defmodule Ferricstore.Flow.Query.ShapeSoak do
  @moduledoc false

  alias Ferricstore.Flow.Query.CompositeRangeReader

  alias Ferricstore.Bench.{
    OpenLoop,
    QuerySoakFixture,
    QueryWorkloadMatrix,
    SchedulerMetrics
  }

  alias Ferricstore.Flow.Query.{
    CountResult,
    ExecutionResult,
    Executor,
    Planner,
    Response
  }

  @cursor_key :binary.copy(<<0x73>>, 32)
  @now_ms 2_000_000_000_000
  @profile_key {__MODULE__, :profile}

  def run do
    record_count = env_integer("MATRIX_RECORDS", 100_000, 1_200, 140_000)
    steady_s = env_integer("MATRIX_STEADY_SECONDS", 120, 1, 86_400)
    concurrency = env_integer("MATRIX_CONCURRENCY", 16, 1, 128)
    target_qps = env_integer("MATRIX_TARGET_QPS", 100, 1, 100_000)
    max_queue = env_integer("MATRIX_MAX_QUEUE", 5_000, 0, 1_000_000)
    drain_s = env_integer("MATRIX_DRAIN_SECONDS", 120, 1, 3_600)
    cursor_every = env_integer("MATRIX_CURSOR_EVERY", 100, 1, 1_000_000)
    previous_scheduler_wall_time = SchedulerMetrics.enable_wall_time()

    fixture = QuerySoakFixture.prepare!(record_count)

    try do
      scenarios = QueryWorkloadMatrix.scenarios(record_count, fixture.records)
      weighted = weighted_scenarios(scenarios)
      running = :atomics.new(1, signed: false)
      :atomics.put(running, 1, 1)
      scheduler_start = SchedulerMetrics.wall_time_snapshot()

      {:ok, generator} =
        OpenLoop.start_link(
          target_qps: target_qps,
          concurrency: concurrency,
          max_queue: max_queue,
          job_fun: &job(&1, weighted, cursor_every),
          execute_fun: &execute_job(fixture, &1)
        )

      sampler = Task.async(fn -> sample_resources(fixture.data_dir, running) end)
      Process.sleep(steady_s * 1_000)
      stats = OpenLoop.stop(generator, drain_s * 1_000)
      scheduler_stop = SchedulerMetrics.wall_time_snapshot()
      :atomics.put(running, 1, 0)
      resources = Task.await(sampler, 10_000)

      report = %{
        benchmark: "ferric.flow.query.shape-matrix/v1",
        generated_at: DateTime.utc_now() |> DateTime.to_iso8601(),
        runtime: %{
          elixir: System.version(),
          otp: System.otp_release(),
          schedulers: System.schedulers_online(),
          dirty_cpu_schedulers: :erlang.system_info(:dirty_cpu_schedulers_online),
          dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
          records: record_count,
          steady_seconds: steady_s,
          concurrency: concurrency,
          target_qps: target_qps,
          max_queue: max_queue,
          drain_seconds: drain_s,
          cursor_every_cycles: cursor_every,
          catalog_version: fixture.catalog.version,
          catalog_digest: Base.encode16(fixture.catalog.digest, case: :lower)
        },
        projection: %{
          elapsed_ms: Float.round(fixture.projection.elapsed_us / 1_000, 3),
          records_per_second:
            Float.round(
              fixture.projection.projected_records * 1_000_000 /
                fixture.projection.elapsed_us,
              2
            ),
          projected_records: fixture.projection.projected_records,
          index_entries: fixture.projection.written_entries,
          write_operations: fixture.projection.write_ops,
          written_bytes: fixture.projection.written_bytes,
          source_logical_bytes: fixture.source_bytes
        },
        scenarios: Enum.map(scenarios, &scenario_report/1),
        capacity: summarize(stats, target_qps),
        scheduler_utilization: SchedulerMetrics.utilization(scheduler_start, scheduler_stop),
        resources: resources
      }

      encoded = Jason.encode!(report, pretty: true)
      IO.puts(encoded)
      write_report(encoded)

      if stats.failed > 0 or stats.dropped > 0 or stats.queue_depth_at_stop > 0 do
        raise "query shape soak failed or exceeded sustainable capacity; inspect the JSON report"
      end
    after
      SchedulerMetrics.restore_wall_time(previous_scheduler_wall_time)
      QuerySoakFixture.cleanup(fixture)
    end
  end

  defp weighted_scenarios(scenarios) do
    scenarios
    |> Enum.flat_map(fn scenario -> List.duplicate(scenario, scenario.weight) end)
    |> List.to_tuple()
  end

  defp job(sequence, weighted, cursor_every) do
    width = tuple_size(weighted)
    scenario = elem(weighted, rem(sequence, width))

    %{
      phase: "steady:shape_matrix",
      workload: scenario.name,
      scenario: scenario,
      check_cursor:
        scenario.cursor? and
          rem(:erlang.phash2({scenario.name, sequence}), cursor_every) == 0
    }
  end

  defp execute_job(fixture, %{scenario: scenario} = job) do
    indexes = QueryWorkloadMatrix.select_indexes(fixture.indexes, scenario)

    with {:ok, first} <- execute_query(fixture, scenario, scenario.request, indexes, :first),
         {:ok, second} <- maybe_execute_cursor(fixture, scenario, indexes, first.result, job) do
      {:ok,
       %{
         measurements_us: merge_samples(first.measurements_us, second.measurements_us),
         observations: merge_samples(first.observations, second.observations)
       }}
    end
  end

  defp execute_query(fixture, scenario, request, indexes, page) do
    reset_profile()

    with {planner_us, {:ok, plan}} <-
           timed(fn ->
             Planner.plan(request, indexes, budget: scenario.budget, now_ms: @now_ms)
           end),
         {verification_us, :ok} <-
           timed(fn -> QueryWorkloadMatrix.verify_plan(scenario, plan) end),
         {executor_us, result} <-
           timed(fn ->
             Executor.execute(fixture.ctx, 0, request, plan,
               cursor_key: @cursor_key,
               now_ms: @now_ms,
               range_read: &profiled_range_read/5
             )
           end),
         profile <- take_profile(),
         {:ok, result_verification_us, response_us, observations} <-
           verify_execution(scenario, result, page, profile) do
      {:ok,
       %{
         result: result,
         measurements_us: %{
           planner: planner_us,
           executor: executor_us,
           range_read: profile.range_read_us,
           executor_other: max(executor_us - profile.range_read_us, 0),
           verification: verification_us + result_verification_us,
           response: response_us
         },
         observations: observations
       }}
    else
      {:error, _reason} = error ->
        Process.delete(@profile_key)
        error

      failure ->
        Process.delete(@profile_key)
        {:error, {:unexpected_query_result, failure}}
    end
  rescue
    error ->
      Process.delete(@profile_key)
      {:error, {:query_exception, error.__struct__}}
  catch
    kind, _reason ->
      Process.delete(@profile_key)
      {:error, {:query_catch, kind}}
  end

  defp verify_execution(scenario, {:error, reason}, _page, profile) do
    {verification_us, verification} =
      timed(fn -> QueryWorkloadMatrix.verify_error(scenario, reason) end)

    case verification do
      :ok ->
        observations =
          profile
          |> profile_observations()
          |> Map.put(rejection_observation(scenario, reason), 1)

        {:ok, verification_us, 0, observations}

      {:error, _reason} = error ->
        error
    end
  end

  defp verify_execution(scenario, {:ok, result}, page, _profile) do
    {verification_us, verification} =
      timed(fn -> QueryWorkloadMatrix.verify_result(scenario, result, page) end)

    with :ok <- verification,
         {response_us, {:ok, _response}} <-
           timed(fn -> build_response(result, scenario.budget) end) do
      {:ok, verification_us, response_us, usage_observations(result.usage)}
    else
      {:error, _reason} = error -> error
      failure -> {:error, {:unexpected_response_result, failure}}
    end
  end

  defp verify_execution(_scenario, invalid, _page, _profile),
    do: {:error, {:invalid_executor_result, invalid}}

  defp build_response(%ExecutionResult{} = result, budget) do
    Response.build(
      result.records,
      result.has_more,
      result.continuation,
      result.quality,
      result.usage,
      budget
    )
  end

  defp build_response(%CountResult{} = result, budget),
    do: Response.build_count(result.count, result.quality, result.usage, budget)

  defp maybe_execute_cursor(
         fixture,
         scenario,
         indexes,
         {:ok, %ExecutionResult{has_more: true, continuation: cursor}},
         %{check_cursor: true}
       ) do
    request = %{scenario.request | cursor: {:literal, :keyword, cursor}}
    execute_query(fixture, scenario, request, indexes, :second)
  end

  defp maybe_execute_cursor(_fixture, _scenario, _indexes, _result, _job),
    do: {:ok, %{measurements_us: %{}, observations: %{}}}

  defp profiled_range_read(path, range, cursor, max_entries, max_bytes) do
    {elapsed_us, result} =
      timed(fn -> CompositeRangeReader.read(path, range, cursor, max_entries, max_bytes) end)

    profile = Process.get(@profile_key, empty_profile())

    profile =
      case result do
        {:ok, page} ->
          %{
            profile
            | range_read_us: profile.range_read_us + elapsed_us,
              range_pages: profile.range_pages + 1,
              scanned_entries: profile.scanned_entries + page.scanned_entries,
              scanned_bytes: profile.scanned_bytes + page.scanned_bytes
          }

        _error ->
          %{profile | range_read_us: profile.range_read_us + elapsed_us}
      end

    Process.put(@profile_key, profile)
    result
  end

  defp reset_profile, do: Process.put(@profile_key, empty_profile())
  defp take_profile, do: Process.delete(@profile_key) || empty_profile()

  defp empty_profile do
    %{range_read_us: 0, range_pages: 0, scanned_entries: 0, scanned_bytes: 0}
  end

  defp profile_observations(profile) do
    %{
      query_executions: 1,
      range_pages: profile.range_pages,
      scanned_entries: profile.scanned_entries,
      scanned_bytes: profile.scanned_bytes
    }
  end

  defp usage_observations(usage) do
    usage
    |> Map.take([
      :range_seeks,
      :range_pages,
      :scanned_entries,
      :scanned_bytes,
      :hydrated_records,
      :residual_checks,
      :duplicate_entries,
      :result_records,
      :memory_high_water_bytes,
      :wall_time_us
    ])
    |> Map.put(:query_executions, 1)
  end

  defp merge_samples(left, right) do
    Map.merge(left, right, fn _name, left_value, right_value -> left_value + right_value end)
  end

  defp scenario_report(scenario) do
    %{
      name: scenario.name,
      class: scenario.class,
      weight: scenario.weight,
      cursor_checked: scenario.cursor?,
      eligible_indexes: scenario.index_ids,
      expected_plan: scenario.plan,
      expected_outcome: outcome_report(scenario.outcome),
      expected_usage:
        Map.new(scenario.usage, fn {field, bound} -> {field, bound_report(bound)} end)
    }
  end

  defp bound_report({operator, value}) when operator in [:eq, :min, :max],
    do: %{operator: operator, value: value}

  defp outcome_report({:records, _first, first_more, _second, second_more}),
    do: %{kind: "records", first_has_more: first_more, second_has_more: second_more}

  defp outcome_report({:count, count}), do: %{kind: "count", value: count}
  defp outcome_report({:error, reason}), do: %{kind: "bounded_error", reason: reason}

  defp rejection_observation(_scenario, _reason), do: :expected_rejections

  defp summarize(stats, target_qps) do
    duration_us = stats.offer_duration_us

    %{
      target_qps: target_qps,
      offered: stats.offered,
      started: stats.started,
      completed: stats.completed,
      succeeded: stats.succeeded,
      failed: stats.failed,
      dropped: stats.dropped,
      failures: stats.failures,
      offered_qps: rate(stats.offered, duration_us),
      completed_qps_at_stop: rate(stats.completed_at_stop, duration_us),
      queue_depth_at_stop: stats.queue_depth_at_stop,
      in_flight_at_stop: stats.in_flight_at_stop,
      max_in_flight: stats.max_in_flight,
      max_queue_depth: stats.max_queue_depth,
      offer_duration_ms: Float.round(duration_us / 1_000, 3),
      drain_duration_ms: Float.round(stats.drain_duration_us / 1_000, 3),
      sustainable: stats.dropped == 0 and stats.queue_depth_at_stop == 0,
      latency_us: histogram_summary(stats.latency_histograms),
      measurements_us: histogram_summary(stats.measurement_histograms),
      observations: histogram_summary(stats.observation_histograms),
      workloads: summarize_workloads(stats.phases["steady:shape_matrix"], duration_us)
    }
  end

  defp summarize_workloads(phase, duration_us) do
    Map.new(phase.workloads, fn {name, workload} ->
      {name,
       %{
         offered: workload.offered,
         completed: workload.completed,
         succeeded: workload.succeeded,
         failed: workload.failed,
         dropped: workload.dropped,
         offered_qps: rate(workload.offered, duration_us),
         failures: workload.failures,
         latency_us: histogram_summary(workload.latency_histograms),
         measurements_us: histogram_summary(workload.measurement_histograms),
         observations: histogram_summary(workload.observation_histograms)
       }}
    end)
  end

  defp histogram_summary(histograms) do
    Map.new(histograms, fn {name, histogram} ->
      {name, OpenLoop.histogram_percentiles(histogram)}
    end)
  end

  defp rate(_count, 0), do: 0.0
  defp rate(count, duration_us), do: Float.round(count * 1_000_000 / duration_us, 2)

  defp sample_resources(data_dir, running) do
    sample_resources(data_dir, running, %{
      samples: 0,
      max_total_memory_bytes: 0,
      max_process_memory_bytes: 0,
      max_binary_memory_bytes: 0,
      max_ets_memory_bytes: 0,
      max_process_count: 0,
      max_disk_bytes: 0,
      max_normal_run_queue_total: 0,
      max_normal_run_queue_single: 0,
      max_dirty_cpu_run_queue: 0,
      max_dirty_io_run_queue: 0
    })
  end

  defp sample_resources(data_dir, running, acc) do
    memory = :erlang.memory()
    disk_bytes = directory_bytes(data_dir)
    run_queues = SchedulerMetrics.run_queues()

    acc = %{
      samples: acc.samples + 1,
      max_total_memory_bytes: max(acc.max_total_memory_bytes, memory[:total]),
      max_process_memory_bytes: max(acc.max_process_memory_bytes, memory[:processes]),
      max_binary_memory_bytes: max(acc.max_binary_memory_bytes, memory[:binary]),
      max_ets_memory_bytes: max(acc.max_ets_memory_bytes, memory[:ets]),
      max_process_count: max(acc.max_process_count, :erlang.system_info(:process_count)),
      max_disk_bytes: max(acc.max_disk_bytes, disk_bytes),
      max_normal_run_queue_total: max(acc.max_normal_run_queue_total, run_queues.normal_total),
      max_normal_run_queue_single: max(acc.max_normal_run_queue_single, run_queues.normal_max),
      max_dirty_cpu_run_queue: max(acc.max_dirty_cpu_run_queue, run_queues.dirty_cpu),
      max_dirty_io_run_queue: max(acc.max_dirty_io_run_queue, run_queues.dirty_io)
    }

    if :atomics.get(running, 1) == 1 do
      Process.sleep(1_000)
      sample_resources(data_dir, running, acc)
    else
      acc
    end
  end

  defp directory_bytes(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce(0, fn entry, bytes ->
      case File.stat(entry) do
        {:ok, %File.Stat{type: :regular, size: size}} -> bytes + size
        _other -> bytes
      end
    end)
  end

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    {System.monotonic_time(:microsecond) - started, result}
  end

  defp write_report(encoded) do
    case System.get_env("MATRIX_OUTPUT") do
      nil -> :ok
      "" -> :ok
      path -> File.write!(path, encoded <> "\n")
    end
  end

  defp env_integer(name, default, minimum, maximum) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= minimum and parsed <= maximum -> parsed
          _invalid -> raise "#{name} must be an integer in #{minimum}..#{maximum}"
        end
    end
  end
end

Ferricstore.Flow.Query.ShapeSoak.run()
