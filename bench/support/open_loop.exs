defmodule Ferricstore.Bench.OpenLoop do
  @moduledoc false

  use GenServer

  @type stats :: map()

  @fine_latency_max_us 10_000
  @medium_latency_max_us 100_000
  @coarse_latency_max_us 1_000_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec stop(pid(), timeout()) :: stats()
  def stop(pid, timeout \\ 30_000) when is_pid(pid), do: GenServer.call(pid, :stop, timeout)

  @spec histogram_percentiles(map()) :: map() | nil
  def histogram_percentiles(%{count: 0}), do: nil

  def histogram_percentiles(%{count: count, min: minimum, max: maximum, bins: bins}) do
    sorted = Enum.sort_by(bins, &elem(&1, 0))

    %{
      count: count,
      min: minimum,
      p50: min(histogram_percentile(sorted, count, 50), maximum),
      p95: min(histogram_percentile(sorted, count, 95), maximum),
      p99: min(histogram_percentile(sorted, count, 99), maximum),
      p99_9: min(histogram_percentile(sorted, count, 99.9), maximum),
      max: maximum
    }
  end

  @impl true
  def init(opts) do
    target_qps = Keyword.fetch!(opts, :target_qps)
    concurrency = Keyword.fetch!(opts, :concurrency)
    max_queue = Keyword.fetch!(opts, :max_queue)
    job_fun = Keyword.fetch!(opts, :job_fun)
    execute_fun = Keyword.fetch!(opts, :execute_fun)

    with true <- is_integer(target_qps) and target_qps > 0,
         true <- is_integer(concurrency) and concurrency > 0,
         true <- is_integer(max_queue) and max_queue >= 0,
         true <- is_function(job_fun, 1),
         true <- is_function(execute_fun, 1) do
      manager = self()

      workers =
        for _ordinal <- 1..concurrency do
          spawn_link(fn -> worker_loop(manager, execute_fun) end)
        end

      started_us = monotonic_us()
      send(self(), :offer)

      {:ok,
       %{
         target_qps: target_qps,
         max_queue: max_queue,
         job_fun: job_fun,
         workers: workers,
         idle: :queue.from_list(workers),
         pending: :queue.new(),
         in_flight: %{},
         next_sequence: 0,
         started_us: started_us,
         current_phase: nil,
         accepting: true,
         stop_from: nil,
         stopped_us: nil,
         stats: empty_stats()
       }}
    else
      _invalid -> {:stop, :invalid_open_loop_options}
    end
  end

  @impl true
  def handle_call(:stop, from, %{accepting: true} = state) do
    state = close_current_phase(state)

    stats = %{
      state.stats
      | completed_at_stop: state.stats.completed,
        started_at_stop: state.stats.started + map_size(state.in_flight),
        queue_depth_at_stop: :queue.len(state.pending),
        in_flight_at_stop: map_size(state.in_flight)
    }

    state = %{
      state
      | accepting: false,
        stop_from: from,
        stopped_us: monotonic_us(),
        stats: stats
    }

    finish_or_wait(state, :call)
  end

  def handle_call(:stop, _from, state), do: {:reply, state.stats, state}

  @impl true
  def handle_info(:offer, %{accepting: true} = state) do
    now_us = monotonic_us()
    state = offer_due(state, now_us)
    schedule_next_offer(state, now_us)
    {:noreply, state}
  end

  def handle_info(:offer, state), do: {:noreply, state}

  def handle_info({:completed, worker, sequence, result, started_us, completed_us}, state) do
    case Map.pop(state.in_flight, worker) do
      {%{sequence: ^sequence} = job, in_flight} ->
        stats = record_completion(state.stats, job, result, started_us, completed_us)
        state = %{state | in_flight: in_flight, stats: stats}
        state = dispatch_or_idle(state, worker)
        finish_or_wait(state, :info)

      _invalid ->
        {:stop, :invalid_open_loop_completion, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.workers, &send(&1, :shutdown))
    :ok
  end

  defp offer_due(state, now_us) do
    scheduled_us = scheduled_us(state, state.next_sequence)

    if scheduled_us <= now_us do
      state
      |> offer_one(scheduled_us)
      |> offer_due(now_us)
    else
      state
    end
  end

  defp offer_one(state, scheduled_us) do
    sequence = state.next_sequence
    offered_us = monotonic_us()
    metadata = state.job_fun.(sequence)

    unless is_map(metadata) and is_binary(metadata[:phase]) and is_binary(metadata[:workload]) do
      raise ArgumentError, "open-loop jobs require binary phase and workload fields"
    end

    job =
      metadata
      |> Map.put(:sequence, sequence)
      |> Map.put(:scheduled_us, scheduled_us)
      |> Map.put(:offered_us, offered_us)

    state = transition_phase(state, job.phase)

    state = %{
      state
      | next_sequence: sequence + 1,
        stats: record_offered(state.stats, job)
    }

    dispatch_or_enqueue(state, job)
  end

  defp dispatch_or_enqueue(state, job) do
    case :queue.out(state.idle) do
      {{:value, worker}, idle} ->
        send(worker, {:execute, job})
        in_flight = Map.put(state.in_flight, worker, job)

        stats = %{
          state.stats
          | max_in_flight: max(state.stats.max_in_flight, map_size(in_flight))
        }

        %{state | idle: idle, in_flight: in_flight, stats: stats}

      {:empty, _idle} ->
        if :queue.len(state.pending) < state.max_queue do
          pending = :queue.in(job, state.pending)

          stats = %{
            state.stats
            | max_queue_depth: max(state.stats.max_queue_depth, :queue.len(pending))
          }

          %{state | pending: pending, stats: stats}
        else
          %{state | stats: record_dropped(state.stats, job)}
        end
    end
  end

  defp dispatch_or_idle(state, worker) do
    case :queue.out(state.pending) do
      {{:value, job}, pending} ->
        send(worker, {:execute, job})
        %{state | pending: pending, in_flight: Map.put(state.in_flight, worker, job)}

      {:empty, _pending} ->
        %{state | idle: :queue.in(worker, state.idle)}
    end
  end

  defp schedule_next_offer(state, now_us) do
    delay_us = max(scheduled_us(state, state.next_sequence) - now_us, 0)
    Process.send_after(self(), :offer, max(div(delay_us + 999, 1_000), 1))
  end

  defp scheduled_us(state, sequence) do
    state.started_us + div(sequence * 1_000_000, state.target_qps)
  end

  defp finish_or_wait(state, source) do
    drained = map_size(state.in_flight) == 0 and :queue.is_empty(state.pending)

    if not state.accepting and drained do
      completed_us = monotonic_us()

      stats =
        state.stats
        |> Map.put(:offer_duration_us, state.stopped_us - state.started_us)
        |> Map.put(:drain_duration_us, completed_us - state.stopped_us)

      Enum.each(state.workers, &send(&1, :shutdown))

      case source do
        :call ->
          {:stop, :normal, stats, %{state | stats: stats}}

        :info ->
          GenServer.reply(state.stop_from, stats)
          {:stop, :normal, %{state | stats: stats}}
      end
    else
      case source do
        :call -> {:noreply, state}
        :info -> {:noreply, state}
      end
    end
  end

  defp worker_loop(manager, execute_fun) do
    receive do
      {:execute, job} ->
        started_us = monotonic_us()

        result =
          try do
            execute_fun.(job)
          rescue
            error -> {:error, {:exception, error.__struct__}}
          catch
            kind, _reason -> {:error, {:caught, kind}}
          end

        completed_us = monotonic_us()
        send(manager, {:completed, self(), job.sequence, result, started_us, completed_us})
        worker_loop(manager, execute_fun)

      :shutdown ->
        :ok
    end
  end

  defp record_offered(stats, job) do
    update_buckets(stats, job, fn bucket ->
      bucket
      |> Map.update!(:offered, fn count -> count + 1 end)
      |> Map.update!(:first_scheduled_us, &(&1 || job.scheduled_us))
      |> Map.put(:last_scheduled_us, job.scheduled_us)
    end)
  end

  defp record_dropped(stats, job) do
    update_buckets(stats, job, &Map.update!(&1, :dropped, fn count -> count + 1 end))
  end

  defp record_completion(stats, job, result, started_us, completed_us) do
    scheduler_us = max(job.offered_us - job.scheduled_us, 0)
    dispatch_us = max(started_us - job.offered_us, 0)
    queue_us = max(started_us - job.scheduled_us, 0)
    service_us = max(completed_us - started_us, 0)
    end_to_end_us = max(completed_us - job.scheduled_us, 0)

    update_buckets(stats, job, fn bucket ->
      bucket
      |> Map.update!(:started, &(&1 + 1))
      |> Map.update!(:completed, &(&1 + 1))
      |> record_latency(:scheduler, scheduler_us)
      |> record_latency(:dispatch, dispatch_us)
      |> record_latency(:queue, queue_us)
      |> record_latency(:service, service_us)
      |> record_latency(:end_to_end, end_to_end_us)
      |> record_result(result)
    end)
  end

  defp record_latency(bucket, metric, value) do
    histogram = bucket.latency_histograms |> Map.fetch!(metric) |> histogram_record(value)
    %{bucket | latency_histograms: Map.put(bucket.latency_histograms, metric, histogram)}
  end

  defp histogram_record(histogram, value) do
    bin = latency_bin(value)

    %{
      count: histogram.count + 1,
      min: if(is_nil(histogram.min), do: value, else: min(histogram.min, value)),
      max: if(is_nil(histogram.max), do: value, else: max(histogram.max, value)),
      bins: Map.update(histogram.bins, bin, 1, fn count -> count + 1 end)
    }
  end

  defp latency_bin(value) when value <= @fine_latency_max_us, do: ceil_to(value, 10)
  defp latency_bin(value) when value <= @medium_latency_max_us, do: ceil_to(value, 100)
  defp latency_bin(value) when value <= @coarse_latency_max_us, do: ceil_to(value, 1_000)
  defp latency_bin(value), do: ceil_to(value, 10_000)

  defp ceil_to(0, _resolution), do: 0
  defp ceil_to(value, resolution), do: div(value + resolution - 1, resolution) * resolution

  defp histogram_percentile(sorted, count, percentile) do
    target = max(ceil(count * percentile / 100), 1)

    sorted
    |> Enum.reduce_while(0, fn {upper_bound, bin_count}, seen ->
      next = seen + bin_count
      if next >= target, do: {:halt, upper_bound}, else: {:cont, next}
    end)
  end

  defp record_result(bucket, :ok), do: Map.update!(bucket, :succeeded, &(&1 + 1))

  defp record_result(bucket, {:ok, %{measurements_us: measurements} = result})
       when is_map(measurements) do
    observations = Map.get(result, :observations, %{})

    if valid_samples?(measurements) and valid_samples?(observations) do
      bucket
      |> Map.update!(:succeeded, &(&1 + 1))
      |> record_measurements(measurements)
      |> record_observations(observations)
    else
      record_result(bucket, {:error, :invalid_open_loop_measurements})
    end
  end

  defp record_result(bucket, {:ok, _value}), do: Map.update!(bucket, :succeeded, &(&1 + 1))

  defp record_result(bucket, {:error, reason}) do
    bucket
    |> Map.update!(:failed, &(&1 + 1))
    |> Map.update!(:failures, &Map.update(&1, inspect(reason), 1, fn count -> count + 1 end))
  end

  defp record_result(bucket, invalid), do: record_result(bucket, {:error, {:invalid, invalid}})

  defp record_measurements(bucket, measurements) do
    histograms =
      Enum.reduce(measurements, bucket.measurement_histograms, fn {name, value}, histograms ->
        Map.update(histograms, name, histogram_record(empty_histogram(), value), fn histogram ->
          histogram_record(histogram, value)
        end)
      end)

    %{bucket | measurement_histograms: histograms}
  end

  defp record_observations(bucket, observations) do
    histograms =
      Enum.reduce(observations, bucket.observation_histograms, fn {name, value}, histograms ->
        Map.update(
          histograms,
          name,
          observation_record(empty_histogram(), value),
          &observation_record(&1, value)
        )
      end)

    %{bucket | observation_histograms: histograms}
  end

  defp valid_samples?(samples) when is_map(samples) do
    Enum.all?(samples, fn {name, value} ->
      (is_atom(name) or is_binary(name)) and is_integer(value) and value >= 0
    end)
  end

  defp valid_samples?(_samples), do: false

  defp observation_record(histogram, value) do
    bin = observation_bin(value)

    %{
      count: histogram.count + 1,
      min: if(is_nil(histogram.min), do: value, else: min(histogram.min, value)),
      max: if(is_nil(histogram.max), do: value, else: max(histogram.max, value)),
      bins: Map.update(histogram.bins, bin, 1, fn count -> count + 1 end)
    }
  end

  defp observation_bin(value) when value <= 100, do: value
  defp observation_bin(value) when value <= 10_000, do: ceil_to(value, 100)
  defp observation_bin(value) when value <= 100_000, do: ceil_to(value, 1_000)
  defp observation_bin(value) when value <= 1_000_000, do: ceil_to(value, 10_000)
  defp observation_bin(value), do: ceil_to(value, 1_000_000)

  defp transition_phase(%{current_phase: nil} = state, phase),
    do: %{state | current_phase: phase}

  defp transition_phase(%{current_phase: phase} = state, phase), do: state

  defp transition_phase(state, phase) do
    state
    |> close_current_phase()
    |> Map.put(:current_phase, phase)
  end

  defp close_current_phase(%{current_phase: nil} = state), do: state

  defp close_current_phase(state) do
    phase_name = state.current_phase
    phase = Map.fetch!(state.stats.phases, phase_name)

    pending_at_close = count_phase_jobs(state.pending, phase_name)

    in_flight_at_close =
      Enum.count(state.in_flight, fn {_worker, job} -> job.phase == phase_name end)

    phase = %{
      phase
      | completed_at_close: phase.completed,
        started_at_close: phase.completed + in_flight_at_close,
        outstanding_at_close: phase.offered - phase.completed - phase.dropped,
        pending_at_close: pending_at_close,
        in_flight_at_close: in_flight_at_close
    }

    stats = %{state.stats | phases: Map.put(state.stats.phases, phase_name, phase)}
    %{state | current_phase: nil, stats: stats}
  end

  defp count_phase_jobs(queue, phase_name) do
    queue
    |> :queue.to_list()
    |> Enum.count(&(&1.phase == phase_name))
  end

  defp update_buckets(stats, job, update) do
    phase = Map.get(stats.phases, job.phase, empty_phase())
    workload = Map.get(phase.workloads, job.workload, empty_bucket())
    workload = update.(workload)

    phase =
      phase |> update.() |> Map.put(:workloads, Map.put(phase.workloads, job.workload, workload))

    stats = update.(stats)
    %{stats | phases: Map.put(stats.phases, job.phase, phase)}
  end

  defp empty_stats do
    empty_bucket()
    |> Map.merge(%{
      max_in_flight: 0,
      max_queue_depth: 0,
      completed_at_stop: 0,
      started_at_stop: 0,
      queue_depth_at_stop: 0,
      in_flight_at_stop: 0,
      offer_duration_us: 0,
      drain_duration_us: 0,
      phases: %{}
    })
  end

  defp empty_phase do
    empty_bucket()
    |> Map.merge(%{
      workloads: %{},
      completed_at_close: nil,
      started_at_close: nil,
      outstanding_at_close: nil,
      pending_at_close: nil,
      in_flight_at_close: nil
    })
  end

  defp empty_bucket do
    %{
      offered: 0,
      started: 0,
      completed: 0,
      succeeded: 0,
      failed: 0,
      dropped: 0,
      failures: %{},
      first_scheduled_us: nil,
      last_scheduled_us: nil,
      measurement_histograms: %{},
      observation_histograms: %{},
      latency_histograms: %{
        scheduler: empty_histogram(),
        dispatch: empty_histogram(),
        queue: empty_histogram(),
        service: empty_histogram(),
        end_to_end: empty_histogram()
      }
    }
  end

  defp empty_histogram, do: %{count: 0, min: nil, max: nil, bins: %{}}

  defp monotonic_us, do: System.monotonic_time(:microsecond)
end
