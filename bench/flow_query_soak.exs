Code.require_file("support/open_loop.exs", __DIR__)
Code.require_file("support/scheduler_metrics.exs", __DIR__)
Code.require_file("support/query_dataset.exs", __DIR__)

defmodule Ferricstore.Flow.Query.Soak do
  @moduledoc false

  alias FerricStore.Flow.MetadataExtension
  alias Ferricstore.Flow.{Codec, Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeBackfill,
    CompositeCounter,
    CompositeIndex,
    CompositeRangeReader,
    Field,
    IndexDefinition,
    MandatoryScope,
    RegisteredIndex,
    Request
  }

  alias Ferricstore.Flow.Query.{Budget, Executor, IndexCatalog, Planner, Response}
  alias Ferricstore.Bench.{OpenLoop, QueryDataset, SchedulerMetrics}

  @cursor_key :binary.copy(<<0x6B>>, 32)
  @backfill_page_records 16
  @integrity_page_records 4_096
  @maximum_exact_integer 9_007_199_254_740_991
  @executor_measurements_key {__MODULE__, :executor_measurements}

  def run do
    record_count = env_integer("SOAK_RECORDS", 100_000, 32, 1_000_000)
    steady_s = env_integer("SOAK_STEADY_SECONDS", 120, 1, 86_400)
    concurrency = env_integer("SOAK_CONCURRENCY", 32, 1, 128)
    target_qps = env_integer("SOAK_TARGET_QPS", 1_000, 1, 1_000_000)
    max_queue = env_integer("SOAK_MAX_QUEUE", 5_000, 0, 1_000_000)
    drain_s = env_integer("SOAK_DRAIN_SECONDS", 120, 1, 3_600)
    cursor_every = env_integer("SOAK_CURSOR_EVERY", 1_000, 1, 1_000_000)
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_query_soak_#{suffix}")

    {:ok, catalog} = IndexCatalog.load()
    definitions = catalog.definitions
    records = QueryDataset.records(record_count)
    ctx = context(data_dir)
    previous_scheduler_wall_time = SchedulerMetrics.enable_wall_time()

    try do
      source_bytes = write_source_records(ctx, records)
      encoded_by_key = encoded_records(records)
      read_entries = read_entries_fun(encoded_by_key)
      active_indexes = Enum.map(definitions, &active_index/1)

      workloads =
        Enum.map(definitions, fn definition ->
          request = request_for(definition.id, record_count)
          {definition.id, request, expected_pages(definition.id, records, request.limit)}
        end)
        |> List.to_tuple()

      active_count = :atomics.new(1, signed: false)
      sampling = :atomics.new(1, signed: false)

      [first | remaining] = definitions

      phase_names =
        remaining
        |> Enum.map(&"backfill:#{&1.id}")
        |> Kernel.++(["steady:all_indexes"])
        |> List.to_tuple()

      {first_us, first_metrics} =
        timed(fn -> project_definition(ctx, records, first, [first], read_entries) end)

      :atomics.put(active_count, 1, 1)
      :atomics.put(sampling, 1, 1)
      started_us = System.monotonic_time(:microsecond)
      load_scheduler_start = SchedulerMetrics.wall_time_snapshot()

      {:ok, load_generator} =
        start_open_loop(
          ctx,
          List.to_tuple(active_indexes),
          workloads,
          active_count,
          phase_names,
          concurrency,
          target_qps,
          max_queue,
          cursor_every
        )

      sampler = Task.async(fn -> sample_resources(data_dir, sampling) end)

      {backfills, _projected} =
        Enum.reduce(
          remaining,
          {[backfill_result(first, first_us, first_metrics)], [first]},
          fn definition, {results, projected} ->
            projection_definitions = projected ++ [definition]

            {elapsed_us, metrics} =
              timed(fn ->
                project_definition(
                  ctx,
                  records,
                  definition,
                  projection_definitions,
                  read_entries
                )
              end)

            :atomics.put(active_count, 1, length(projection_definitions))
            IO.puts(:stderr, "soak_index_active id=#{definition.id}")

            {[backfill_result(definition, elapsed_us, metrics) | results], projection_definitions}
          end
        )

      steady_scheduler_start = SchedulerMetrics.wall_time_snapshot()
      Process.sleep(steady_s * 1_000)
      load = OpenLoop.stop(load_generator, drain_s * 1_000)
      scheduler_stop = SchedulerMetrics.wall_time_snapshot()
      :atomics.put(sampling, 1, 0)
      resources = Task.await(sampler, 10_000)
      capacity = format_load_stats(load, target_qps)
      integrity = verify_integrity(ctx, records, definitions)

      report = %{
        benchmark: "ferric.flow.query.open-loop-capacity/v1",
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
          cursor_every: cursor_every,
          catalog_version: catalog.version,
          catalog_digest: Base.encode16(catalog.digest, case: :lower)
        },
        source_logical_bytes: source_bytes,
        elapsed_ms: Float.round((System.monotonic_time(:microsecond) - started_us) / 1_000, 3),
        backfills: Enum.reverse(backfills),
        capacity: capacity,
        scheduler_utilization: %{
          load: SchedulerMetrics.utilization(load_scheduler_start, scheduler_stop),
          steady: SchedulerMetrics.utilization(steady_scheduler_start, scheduler_stop)
        },
        resources: resources,
        integrity: integrity
      }

      encoded = Jason.encode!(report, pretty: true)
      IO.puts(encoded)
      write_report(encoded)

      if load.failed > 0 or integrity.status != "passed" do
        raise "query soak failed; inspect the JSON report"
      end
    after
      SchedulerMetrics.restore_wall_time(previous_scheduler_wall_time)
      File.rm_rf!(data_dir)
    end
  end

  defp start_open_loop(
         ctx,
         active_indexes,
         workloads,
         active_count,
         phase_names,
         concurrency,
         target_qps,
         max_queue,
         cursor_every
       ) do
    job_fun = fn sequence ->
      count = :atomics.get(active_count, 1)
      position = rem(sequence, count)
      {workload, _request, _expectation} = elem(workloads, position)

      %{
        phase: elem(phase_names, count - 1),
        workload: workload,
        active_count: count,
        workload_position: position,
        check_cursor: rem(sequence, cursor_every) == 0
      }
    end

    execute_fun = fn job ->
      execute_job(ctx, active_indexes, workloads, job)
    end

    OpenLoop.start_link(
      target_qps: target_qps,
      concurrency: concurrency,
      max_queue: max_queue,
      job_fun: job_fun,
      execute_fun: execute_fun
    )
  end

  defp execute_job(ctx, active_indexes, workloads, job) do
    {workload, request, expectation} = elem(workloads, job.workload_position)

    indexes =
      active_indexes
      |> Tuple.to_list()
      |> Enum.take(job.active_count)

    with {:ok, first, first_measurements} <-
           execute_query(ctx, request, indexes, workload, expectation.first),
         {:ok, cursor_measurements} <-
           maybe_check_cursor(
             job.check_cursor,
             ctx,
             request,
             indexes,
             workload,
             expectation,
             first
           ) do
      {:ok, %{measurements_us: merge_measurements(first_measurements, cursor_measurements)}}
    end
  end

  defp execute_query(ctx, request, indexes, expected_index, expectation) do
    with {planner_us, {:ok, %{path: :ordered_range, index_id: ^expected_index} = plan}} <-
           timed(fn -> Planner.plan(request, indexes, now_ms: 2_000_000_000_000) end),
         {executor_us, {:ok, result}} <-
           timed(fn ->
             Process.put(@executor_measurements_key, %{})

             Executor.execute(ctx, 0, request, plan,
               cursor_key: @cursor_key,
               now_ms: 2_000_000_000_000,
               range_read: &profiled_range_read/5,
               record_read: &profiled_record_read/4
             )
           end),
         executor_measurements <- Process.delete(@executor_measurements_key) || %{},
         {verification_us, :ok} <- timed(fn -> verify_result(request, result, expectation) end),
         {response_us, {:ok, _response}} <-
           timed(fn ->
             Response.build(
               result.records,
               result.has_more,
               result.continuation,
               result.quality,
               result.usage,
               Budget.default()
             )
           end) do
      {:ok, result,
       executor_measurements
       |> Map.put(:planner, planner_us)
       |> Map.put(:executor, executor_us)
       |> Map.put(
         :executor_other,
         max(executor_us - exclusive_executor_measurement_us(executor_measurements), 0)
       )
       |> Map.put(:verification, verification_us)
       |> Map.put(:response, response_us)}
    else
      {:error, reason} -> {:error, reason}
      failure -> {:error, {:unexpected_query_result, failure}}
    end
  rescue
    error -> {:error, {:query_exception, error.__struct__}}
  catch
    kind, _reason -> {:error, {:query_catch, kind}}
  end

  defp profiled_range_read(path, range, cursor, max_entries, max_bytes) do
    measure_executor_stage(:range_read, fn ->
      CompositeRangeReader.read(path, range, cursor, max_entries, max_bytes)
    end)
  end

  defp profiled_record_read(path, state_keys, now_ms, max_value_bytes) do
    with {:ok, values, _value_bytes} <-
           measure_executor_stage(:record_fetch, fn ->
             LMDB.get_many_bounded(path, state_keys, max_value_bytes)
           end) do
      measure_executor_stage(:record_decode, fn ->
        with {:ok, inputs, encoded_records} <-
               prepare_profiled_record_batch(values, now_ms, [], []) do
          decode_profiled_record_batch(inputs, encoded_records)
        end
      end)
    else
      {:error, :batch_value_budget_exceeded} ->
        {:error, :query_hydration_batch_too_large}

      {:error, _reason} ->
        {:error, :query_storage_unavailable}
    end
  end

  defp prepare_profiled_record_batch([], _now_ms, inputs, encoded_records),
    do: {:ok, Enum.reverse(inputs), Enum.reverse(encoded_records)}

  defp prepare_profiled_record_batch([:not_found | values], now_ms, inputs, encoded_records),
    do: prepare_profiled_record_batch(values, now_ms, [:missing | inputs], encoded_records)

  defp prepare_profiled_record_batch(
         [{:ok, wrapper} | values],
         now_ms,
         inputs,
         encoded_records
       )
       when is_binary(wrapper) do
    case LMDB.decode_value(wrapper, now_ms) do
      {:ok, encoded_record} ->
        prepare_profiled_record_batch(
          values,
          now_ms,
          [:record | inputs],
          [encoded_record | encoded_records]
        )

      :expired ->
        prepare_profiled_record_batch(values, now_ms, [:missing | inputs], encoded_records)

      :error ->
        {:error, :query_storage_inconsistent}
    end
  end

  defp prepare_profiled_record_batch(_invalid, _now_ms, _inputs, _encoded_records),
    do: {:error, :query_storage_inconsistent}

  defp decode_profiled_record_batch(inputs, encoded_records) do
    records = Codec.decode_records(encoded_records)
    restore_profiled_record_batch(inputs, records, [])
  rescue
    _error -> {:error, :query_storage_inconsistent}
  end

  defp restore_profiled_record_batch([], [], acc), do: {:ok, Enum.reverse(acc)}

  defp restore_profiled_record_batch([:missing | inputs], records, acc),
    do: restore_profiled_record_batch(inputs, records, [nil | acc])

  defp restore_profiled_record_batch([:record | inputs], [record | records], acc)
       when is_map(record),
       do: restore_profiled_record_batch(inputs, records, [record | acc])

  defp restore_profiled_record_batch(_inputs, _records, _acc),
    do: {:error, :query_storage_inconsistent}

  defp measure_executor_stage(stage, fun) do
    {elapsed_us, result} = timed(fun)

    measurements = Process.get(@executor_measurements_key, %{})

    Process.put(
      @executor_measurements_key,
      Map.update(measurements, stage, elapsed_us, &(&1 + elapsed_us))
    )

    result
  end

  defp exclusive_executor_measurement_us(measurements) do
    Enum.sum_by([:range_read, :record_fetch, :record_decode], &Map.get(measurements, &1, 0))
  end

  defp verify_result(
         %Request{order_by: [{field, direction}], limit: limit},
         result,
         %{ids: expected_ids, has_more: expected_has_more}
       ) do
    values = Enum.map(result.records, &field_value!(&1, field))
    ids = Enum.map(result.records, &Map.fetch!(&1, :id))

    cond do
      length(ids) > limit or ids != expected_ids ->
        {:error, :unexpected_query_records}

      length(ids) != length(Enum.uniq(ids)) ->
        {:error, :duplicate_query_record}

      not ordered?(values, direction) ->
        {:error, :misordered_query_page}

      result.has_more != expected_has_more ->
        {:error, :incorrect_query_lookahead}

      expected_has_more and not is_binary(result.continuation) ->
        {:error, :missing_query_cursor}

      not expected_has_more and not is_nil(result.continuation) ->
        {:error, :unexpected_query_cursor}

      true ->
        :ok
    end
  end

  defp verify_result(_request, _result, _expectation), do: {:error, :invalid_query_result}

  defp maybe_check_cursor(true, ctx, request, indexes, expected_index, expectation, first)
       when first.has_more do
    next_request = %{request | cursor: {:literal, :keyword, first.continuation}}

    with {:ok, second, measurements} <-
           execute_query(ctx, next_request, indexes, expected_index, expectation.second),
         first_ids <- MapSet.new(first.records, & &1.id),
         false <- Enum.any?(second.records, &MapSet.member?(first_ids, &1.id)) do
      {:ok, measurements}
    else
      true -> {:error, :overlapping_cursor_page}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_check_cursor(_check?, _ctx, _request, _indexes, _index, _expectation, _first),
    do: {:ok, %{}}

  defp merge_measurements(left, right) do
    Map.merge(left, right, fn _name, left_value, right_value -> left_value + right_value end)
  end

  defp project_definition(ctx, records, definition, projection_definitions, read_entries) do
    records
    |> Enum.chunk_every(@backfill_page_records)
    |> Enum.reduce(empty_metrics(), fn page, total ->
      projected =
        Enum.map(page, fn record ->
          %{
            state_key: Keys.state_key(record.id, record.partition_key),
            record: record,
            expire_at_ms: 0
          }
        end)

      {:ok, metrics} =
        CompositeBackfill.project_page(ctx, 0, projected, [definition],
          projection_definitions: projection_definitions,
          read_entries_fun: read_entries
        )

      merge_metrics(total, metrics)
    end)
  end

  defp verify_integrity(ctx, records, definitions) do
    path = lmdb_path(ctx)
    records_by_id = Map.new(records, &{&1.id, &1})

    with {:ok, index_rows} <- verify_index_rows(path, definitions, records_by_id),
         {:ok, reverse_rows} <- verify_reverse_rows(path, definitions, records_by_id),
         {:ok, counter_rows} <- verify_counter_rows(path, definitions, records) do
      %{
        status: "passed",
        index_rows: index_rows,
        reverse_rows: reverse_rows,
        counter_rows: counter_rows
      }
    else
      {:error, reason} -> %{status: "failed", reason: inspect(reason)}
    end
  end

  defp verify_index_rows(path, definitions, records_by_id) do
    expected = map_size(records_by_id)

    Enum.reduce_while(definitions, {:ok, %{}}, fn definition, {:ok, counts} ->
      prefix = IndexDefinition.storage_prefix(definition)

      result =
        LMDB.reduce_prefix_entries(
          path,
          prefix,
          @integrity_page_records,
          0,
          fn rows, count -> verify_index_page(rows, definition, records_by_id, count) end
        )

      case result do
        {:ok, ^expected} -> {:cont, {:ok, Map.put(counts, definition.id, expected)}}
        {:ok, actual} -> {:halt, {:error, {:index_row_count, definition.id, expected, actual}}}
        {:error, reason} -> {:halt, {:error, {:index_scan, definition.id, reason}}}
      end
    end)
  end

  defp verify_index_page(rows, definition, records_by_id, count) do
    Enum.reduce_while(rows, {:ok, count}, fn {key, value}, {:ok, acc} ->
      with {:ok, %{id: id, state_key: state_key}} <- CompositeIndex.decode_entry_value(value),
           {:ok, record} <- Map.fetch(records_by_id, id),
           true <- state_key == Keys.state_key(record.id, record.partition_key),
           {:ok, expected_entries} <-
             CompositeIndex.entries_validated(definition, record, state_key, 0),
           true <- Enum.any?(expected_entries, &(&1.key == key and &1.value == value)) do
        {:cont, {:ok, acc + 1}}
      else
        _invalid -> {:halt, {:error, :invalid_index_entry}}
      end
    end)
  end

  defp verify_reverse_rows(path, definitions, records_by_id) do
    expected = map_size(records_by_id)

    result =
      LMDB.reduce_prefix_entries(
        path,
        CompositeIndex.reverse_prefix(),
        @integrity_page_records,
        0,
        fn rows, count -> verify_reverse_page(rows, definitions, records_by_id, count) end
      )

    case result do
      {:ok, ^expected} -> {:ok, expected}
      {:ok, actual} -> {:error, {:reverse_row_count, expected, actual}}
      {:error, reason} -> {:error, {:reverse_scan, reason}}
    end
  end

  defp verify_reverse_page(rows, definitions, records_by_id, count) do
    Enum.reduce_while(rows, {:ok, count}, fn {key, value}, {:ok, acc} ->
      with {:ok, {state_key, keys, 0}} <- CompositeIndex.decode_reverse_row(key, value),
           {:ok, id} <- Keys.run_id_from_state_key(state_key),
           {:ok, record} <- Map.fetch(records_by_id, id),
           true <- state_key == Keys.state_key(record.id, record.partition_key),
           {:ok, expected_keys} <- expected_projection_keys(definitions, record, state_key),
           true <- Enum.sort(keys) == Enum.sort(expected_keys) do
        {:cont, {:ok, acc + 1}}
      else
        _invalid -> {:halt, {:error, :invalid_reverse_row}}
      end
    end)
  end

  defp expected_projection_keys(definitions, record, state_key) do
    Enum.reduce_while(definitions, {:ok, []}, fn definition, {:ok, keys} ->
      case CompositeIndex.entries_validated(definition, record, state_key, 0) do
        {:ok, entries} -> {:cont, {:ok, Enum.map(entries, & &1.key) ++ keys}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp verify_counter_rows(path, definitions, records) do
    Enum.reduce_while(definitions, {:ok, %{}}, fn definition, {:ok, totals} ->
      expected = expected_counters(definition, records)
      prefix = CompositeCounter.definition_storage_prefix(definition)

      result =
        LMDB.reduce_prefix_entries(
          path,
          prefix,
          @integrity_page_records,
          MapSet.new(),
          fn rows, seen -> verify_counter_page(rows, definition, expected, seen) end
        )

      case result do
        {:ok, seen} ->
          seen_count = MapSet.size(seen)

          if map_size(expected) == seen_count do
            {:cont, {:ok, Map.put(totals, definition.id, seen_count)}}
          else
            {:halt, {:error, {:counter_row_count, definition.id, map_size(expected), seen_count}}}
          end

        {:error, reason} ->
          {:halt, {:error, {:counter_scan, definition.id, reason}}}
      end
    end)
  end

  defp verify_counter_page(rows, definition, expected, seen) do
    Enum.reduce_while(rows, {:ok, seen}, fn {key, value}, {:ok, acc} ->
      with {:ok, expected_count} <- Map.fetch(expected, key),
           {:ok, %{count: count, expiring_count: 0, physical_count: physical_count}} <-
             CompositeCounter.decode_validated_storage_entry(definition, key, value),
           true <- count == expected_count and physical_count == expected_count do
        {:cont, {:ok, MapSet.put(acc, key)}}
      else
        _invalid -> {:halt, {:error, :invalid_counter_entry}}
      end
    end)
  end

  defp expected_counters(%IndexDefinition{} = definition, records) do
    Enum.reduce(definition.count_prefixes, %{}, fn prefix_length, expected ->
      fields = Enum.take(definition.fields, prefix_length)

      Enum.reduce(records, expected, fn record, acc ->
        values =
          Enum.map(fields, fn {field, _direction, _encoding} -> field_value!(record, field) end)

        {:ok, prefix} = CompositeIndex.encode_prefix(definition, nil, values)
        key = CompositeCounter.key(definition, prefix)
        Map.update(acc, key, 1, &(&1 + 1))
      end)
    end)
  end

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

  defp format_load_stats(stats, target_qps) do
    %{
      target_qps: target_qps,
      offered: stats.offered,
      started: stats.started,
      completed: stats.completed,
      succeeded: stats.succeeded,
      failed: stats.failed,
      dropped: stats.dropped,
      failures: stats.failures,
      offered_qps: rate(stats.offered, stats.offer_duration_us),
      started_qps_at_stop: rate(stats.started_at_stop, stats.offer_duration_us),
      completed_qps_at_stop: rate(stats.completed_at_stop, stats.offer_duration_us),
      completed_at_stop: stats.completed_at_stop,
      started_at_stop: stats.started_at_stop,
      outstanding_at_stop: stats.offered - stats.completed_at_stop - stats.dropped,
      queue_depth_at_stop: stats.queue_depth_at_stop,
      in_flight_at_stop: stats.in_flight_at_stop,
      max_in_flight: stats.max_in_flight,
      max_queue_depth: stats.max_queue_depth,
      offer_duration_ms: Float.round(stats.offer_duration_us / 1_000, 3),
      drain_duration_ms: Float.round(stats.drain_duration_us / 1_000, 3),
      sustainable: stats.dropped == 0 and stats.queue_depth_at_stop == 0,
      latency_us: latency_summary(stats),
      measurements_us: measurement_summary(stats),
      phases:
        Map.new(stats.phases, fn {phase, values} ->
          {phase, format_bucket(values, target_qps)}
        end)
    }
  end

  defp format_bucket(values, target_qps) do
    duration_us = bucket_duration_us(values, target_qps)

    %{
      offered: values.offered,
      started: values.started,
      completed: values.completed,
      succeeded: values.succeeded,
      failed: values.failed,
      dropped: values.dropped,
      failures: values.failures,
      offered_qps: rate(values.offered, duration_us),
      started_qps_at_close: rate(values.started_at_close, duration_us),
      completed_qps_at_close: rate(values.completed_at_close, duration_us),
      started_at_close: values.started_at_close,
      completed_at_close: values.completed_at_close,
      outstanding_at_close: values.outstanding_at_close,
      pending_at_close: values.pending_at_close,
      in_flight_at_close: values.in_flight_at_close,
      sustainable: values.dropped == 0 and values.pending_at_close == 0,
      duration_ms: Float.round(duration_us / 1_000, 3),
      latency_us: latency_summary(values),
      measurements_us: measurement_summary(values),
      workloads:
        Map.new(values.workloads, fn {workload, workload_values} ->
          {workload, format_workload(workload_values, duration_us)}
        end)
    }
  end

  defp format_workload(values, phase_duration_us) do
    %{
      offered: values.offered,
      started: values.started,
      completed: values.completed,
      succeeded: values.succeeded,
      failed: values.failed,
      dropped: values.dropped,
      failures: values.failures,
      offered_qps: rate(values.offered, phase_duration_us),
      latency_us: latency_summary(values),
      measurements_us: measurement_summary(values)
    }
  end

  defp latency_summary(values) do
    Map.new(values.latency_histograms, fn {metric, histogram} ->
      {metric, OpenLoop.histogram_percentiles(histogram)}
    end)
  end

  defp measurement_summary(values) do
    Map.new(values.measurement_histograms, fn {measurement, histogram} ->
      {measurement, OpenLoop.histogram_percentiles(histogram)}
    end)
  end

  defp bucket_duration_us(%{first_scheduled_us: nil}, _target_qps), do: 0

  defp bucket_duration_us(values, target_qps) do
    values.last_scheduled_us - values.first_scheduled_us + ceil(1_000_000 / target_qps)
  end

  defp rate(_count, 0), do: 0.0
  defp rate(count, duration_us), do: Float.round(count * 1_000_000 / duration_us, 2)

  defp encoded_records(records) do
    Map.new(records, fn record ->
      {Keys.state_key(record.id, record.partition_key), Codec.encode_record(record)}
    end)
  end

  defp read_entries_fun(encoded_by_key) do
    fn _ctx, 0, keys ->
      {:ok,
       Enum.map(keys, fn key ->
         case Map.fetch(encoded_by_key, key) do
           {:ok, encoded} -> {encoded, 0}
           :error -> nil
         end
       end)}
    end
  end

  defp write_source_records(ctx, records) do
    records
    |> Enum.chunk_every(256)
    |> Enum.reduce(0, fn page, total_bytes ->
      ops =
        Enum.map(page, fn record ->
          key = Keys.state_key(record.id, record.partition_key)
          value = LMDB.encode_value(Codec.encode_record(record), 0)
          {:put, key, value}
        end)

      :ok = LMDB.write_batch(lmdb_path(ctx), ops)

      total_bytes +
        Enum.reduce(ops, 0, fn {:put, key, value}, bytes ->
          bytes + byte_size(key) + byte_size(value)
        end)
    end)
  end

  defp request_for("flow_runs_tenant_updated", record_count) do
    collection([time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_state_updated", record_count) do
    collection([eq(:state, "failed"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_updated", record_count) do
    collection([eq(:type, "invoice"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_state_updated", record_count) do
    collection([eq(:type, "invoice"), eq(:state, "failed"), time_window(record_count)])
  end

  defp request_for("flow_runs_tenant_type_state_lease_deadline", record_count) do
    Request.collection(
      :execute,
      [
        eq(:partition_key, "benchmark-tenant"),
        eq(:type, "workflow"),
        eq(:state, "running"),
        {:range, :lease_deadline_ms, integer(0), integer(record_count + 1)}
      ],
      [{:lease_deadline_ms, :asc}],
      25,
      :record
    )
  end

  defp request_for(id, _record_count), do: raise("unsupported launch index: #{id}")

  defp expected_pages(id, records, limit) do
    ids = expected_ids(id, records)

    %{
      first: expected_page(ids, 0, limit),
      second: expected_page(ids, limit, limit)
    }
  end

  defp expected_page(ids, offset, limit) do
    %{
      ids: ids |> Enum.drop(offset) |> Enum.take(limit),
      has_more: length(ids) > offset + limit
    }
  end

  defp expected_ids("flow_runs_tenant_updated", records) do
    ordered_ids(records, :updated_at_ms, :desc)
  end

  defp expected_ids("flow_runs_tenant_state_updated", records) do
    records
    |> Enum.filter(&(&1.state == "failed"))
    |> ordered_ids(:updated_at_ms, :desc)
  end

  defp expected_ids("flow_runs_tenant_type_updated", records) do
    records
    |> Enum.filter(&(&1.type == "invoice"))
    |> ordered_ids(:updated_at_ms, :desc)
  end

  defp expected_ids("flow_runs_tenant_type_state_updated", records) do
    records
    |> Enum.filter(&(&1.type == "invoice" and &1.state == "failed"))
    |> ordered_ids(:updated_at_ms, :desc)
  end

  defp expected_ids("flow_runs_tenant_type_state_lease_deadline", records) do
    records
    |> Enum.filter(&(&1.type == "workflow" and &1.state == "running"))
    |> ordered_ids(:lease_deadline_ms, :asc)
  end

  defp expected_ids(id, _records), do: raise("unsupported launch index: #{id}")

  defp ordered_ids(records, field, direction) do
    records
    |> Enum.sort_by(&Map.fetch!(&1, field), direction)
    |> Enum.map(& &1.id)
  end

  defp collection(predicates) do
    Request.collection(
      :execute,
      [eq(:partition_key, "benchmark-tenant") | predicates],
      [{:updated_at_ms, :desc}],
      25,
      :record
    )
  end

  defp time_window(record_count) do
    {:time_window, :updated_at_ms, integer(0), integer(record_count + 1)}
  end

  defp active_index(definition) do
    RegisteredIndex.new!(definition, :active,
      coverage: %{complete_shards: 1, total_shards: 1, validation: :passed}
    )
  end

  defp backfill_result(definition, elapsed_us, metrics) do
    %{
      id: definition.id,
      elapsed_ms: Float.round(elapsed_us / 1_000, 3),
      records_per_second: Float.round(metrics.projected_records * 1_000_000 / elapsed_us, 2),
      projected_records: metrics.projected_records,
      index_entries: metrics.written_entries,
      write_operations: metrics.write_ops,
      written_bytes: metrics.written_bytes
    }
  end

  defp empty_metrics do
    %{projected_records: 0, written_entries: 0, write_ops: 0, written_bytes: 0}
  end

  defp merge_metrics(left, right) do
    Map.new(left, fn {field, value} -> {field, value + Map.fetch!(right, field)} end)
  end

  defp ordered?([], _direction), do: true
  defp ordered?([_value], _direction), do: true

  defp ordered?([left, right | values], :asc),
    do: left <= right and ordered?([right | values], :asc)

  defp ordered?([left, right | values], :desc),
    do: left >= right and ordered?([right | values], :desc)

  defp field_value!(record, field) do
    case Field.fetch(record, field) do
      {:ok, value} -> value
      :missing -> raise "soak record is missing #{inspect(field)}"
    end
  end

  defp timed(fun) do
    started = System.monotonic_time(:microsecond)
    result = fun.()
    {System.monotonic_time(:microsecond) - started, result}
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

  defp write_report(encoded) do
    case System.get_env("SOAK_OUTPUT") do
      nil -> :ok
      "" -> :ok
      path -> File.write!(path, encoded <> "\n")
    end
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp context(data_dir) do
    {:ok, metadata_snapshot} =
      MetadataExtension.configure(FerricStore.Flow.MetadataExtension.Disabled, [])

    %{
      name: :flow_query_soak,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024)),
      flow_metadata_snapshot: metadata_snapshot,
      query_mandatory_scope: MandatoryScope.dedicated()
    }
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

  defp eq(field, value), do: {:eq, field, keyword(value)}
  defp keyword(value), do: {:literal, :keyword, value}
  defp integer(value) when value <= @maximum_exact_integer, do: {:literal, :integer, value}
end

Ferricstore.Flow.Query.Soak.run()
