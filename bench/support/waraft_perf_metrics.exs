defmodule FerricstoreBench.WARaftPerfMetrics do
  @moduledoc false
  use GenServer
  @behaviour :wa_raft_metrics
  @timings [
    :"storage.apply.func",
    :"leader.apply.func",
    :"apply_log.latency_us",
    :"acceptor.commit.func",
    :"leader.heartbeat.interval_ms",
    :"leader.heartbeat.size",
    :"leader.follower.lag"
  ]
  @counts [
    :"log.append",
    :"log.append.ok",
    :"log.append.error",
    :"commit.batch.delay",
    :"apply.delay",
    :"leader.append.failure",
    :"leader.heartbeat"
  ]
  @append_mfa {:ferricstore_waraft_spike_segment_log, :append, 4}
  @trace_mfas [
    @append_mfa,
    {:wa_raft_server, :heartbeat, 2},
    {:wa_raft_server, :handle_heartbeat, 9},
    {:wa_raft_server, :commit_pending, 2},
    {:ferricstore_waraft_spike_segment_log, :get, 2},
    {:ferricstore_waraft_spike_segment_log, :trim, 3}
  ]

  def start(opts \\ []), do: GenServer.start(__MODULE__, opts, name: __MODULE__)

  @impl GenServer
  def init(opts) do
    :ets.new(__MODULE__, [:named_table, :public, :set, write_concurrency: :auto])
    :ok = :wa_raft_metrics.install(__MODULE__)

    :ok =
      :telemetry.attach(
        __MODULE__,
        [:ferricstore, :waraft, :storage, :payload_fsync],
        &__MODULE__.payload_fsync/4,
        nil
      )

    processes =
      for name <- Process.registered(),
          role = process_role(name),
          role != nil,
          pid = Process.whereis(name),
          is_pid(pid),
          do: {pid, role}

    if Keyword.get(opts, :trace_calls, true) do
      for mfa <- @trace_mfas do
        :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])
      end

      for {pid, :server} <- processes do
        :erlang.trace(pid, true, [:call, :arity, :monotonic_timestamp, {:tracer, self()}])
      end
    end

    Process.send_after(self(), :sample_queues, 10)
    {:ok, %{processes: processes, appends: %{}}}
  end

  def reset, do: GenServer.call(__MODULE__, :reset)

  @impl GenServer
  def handle_call(:reset, _from, state) do
    :ets.delete_all_objects(__MODULE__)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:sample_queues, state) do
    {:message_queue_len, own_queue} = Process.info(self(), :message_queue_len)
    record({:mailbox, :profiler}, own_queue)

    for {pid, role} <- state.processes do
      case Process.info(pid, :message_queue_len) do
        {:message_queue_len, count} -> record({:mailbox, role}, count)
        nil -> :ok
      end
    end

    Process.send_after(self(), :sample_queues, 10)
    {:noreply, state}
  end

  def handle_info({:trace_ts, pid, :call, mfa, time}, state) when mfa in @trace_mfas do
    {:noreply, %{state | appends: Map.update(state.appends, {pid, mfa}, [time], &[time | &1])}}
  end

  def handle_info({:trace_ts, pid, :return_from, mfa, _result, time}, state)
      when mfa in @trace_mfas do
    case Map.get(state.appends, {pid, mfa}, []) do
      [started | rest] ->
        {module, function, arity} = mfa

        metric =
          if mfa == @append_mfa,
            do: :append_wall_us,
            else: "call.#{module}.#{function}.#{arity}.us"

        record(metric, System.convert_time_unit(time - started, :native, :microsecond))
        {:noreply, %{state | appends: Map.put(state.appends, {pid, mfa}, rest)}}

      [] ->
        {:noreply, state}
    end
  end

  def traced_put(ctx, key, value, ttl) do
    previous = Ferricstore.LatencyTrace.start()

    try do
      Ferricstore.Store.Router.put(ctx, key, value, ttl)
    after
      for {span, duration} <- Ferricstore.LatencyTrace.finish(previous) do
        record({:request_span, span}, duration)
      end
    end
  end

  @impl :wa_raft_metrics
  def count(metric), do: countv(metric, 1)

  @impl :wa_raft_metrics
  def countv({:raft, _table, metric}, value) when metric in @counts do
    :ets.update_counter(__MODULE__, {:count, metric}, {2, value}, {{:count, metric}, 0})
    :ok
  end

  def countv(_metric, _value), do: :ok

  @impl :wa_raft_metrics
  def gather({:raft, _table, metric}, value) when metric in @timings and is_integer(value) do
    record(metric, value)
  end

  def gather(_metric, _value), do: :ok

  @impl :wa_raft_metrics
  def gather_latency(metric, value), do: gather(metric, value)

  def payload_fsync(_event, %{duration: duration}, _metadata, _config) do
    record(:payload_fsync_us, System.convert_time_unit(duration, :native, :microsecond))
  end

  def snapshot do
    rows = :ets.tab2list(__MODULE__)
    counts = for {{:count, metric}, value} <- rows, into: %{}, do: {to_string(metric), value}

    metrics =
      Enum.uniq(
        @timings ++ [:payload_fsync_us] ++ for({{:sample, metric, _}, _} <- rows, do: metric)
      )

    timings =
      for metric <- metrics, into: %{} do
        buckets = for {{:sample, ^metric, bucket}, count} <- rows, do: {bucket, count}
        count = Enum.sum(Enum.map(buckets, &elem(&1, 1)))

        {metric_name(metric),
         %{
           count: count,
           p50: quantile(buckets, count, 0.5),
           p95: quantile(buckets, count, 0.95),
           p99: quantile(buckets, count, 0.99),
           max: if(count > 0, do: buckets |> Enum.map(&elem(&1, 0)) |> Enum.max(), else: nil)
         }}
      end

    %{counts: counts, histograms: timings}
  end

  defp process_role(name) do
    case Atom.to_string(name) do
      "raft_server_ferricstore_waraft_backend_" <> _ -> :server
      "raft_storage_ferricstore_waraft_backend_" <> _ -> :storage
      "raft_acceptor_ferricstore_waraft_backend_" <> _ -> :acceptor
      _ -> nil
    end
  end

  defp metric_name({kind, name}), do: "#{kind}.#{name}"
  defp metric_name(metric), do: to_string(metric)

  defp record(metric, value) do
    bucket =
      cond do
        value < 1_000 -> value
        value < 10_000 -> div(value + 9, 10) * 10
        value < 100_000 -> div(value + 99, 100) * 100
        true -> div(value + 999, 1_000) * 1_000
      end

    key = {:sample, metric, bucket}
    :ets.update_counter(__MODULE__, key, {2, 1}, {key, 0})
    :ok
  end

  defp quantile(_buckets, 0, _q), do: nil

  defp quantile(buckets, count, q) do
    target = ceil(count * q)

    Enum.reduce_while(Enum.sort(buckets), 0, fn {value, n}, seen ->
      if seen + n >= target, do: {:halt, value}, else: {:cont, seen + n}
    end)
  end
end
