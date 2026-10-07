defmodule FerricstoreBench.HashStallProfiler do
  @moduledoc false
  use GenServer

  @mfas [
    {:ferricstore_waraft_spike_segment_log, :append, 4},
    {Ferricstore.Store.Promotion, :await_compaction_latch, 2},
    {Ferricstore.Bitcask.NIF, :v2_append_batch, 2},
    {Ferricstore.Bitcask.NIF, :v2_append_record, 4},
    {Ferricstore.Bitcask.NIF, :v2_fsync, 1},
    {Ferricstore.Bitcask.NIF, :v2_fsync_dir, 1},
    {Ferricstore.Bitcask.NIF, :fs_atomic_replace_nofollow, 3},
    {:file, :datasync, 1},
    {:file, :sync, 1}
  ]
  @events [
    [:ferricstore, :dedicated, :compaction],
    [:ferricstore, :dedicated, :compaction_failed],
    [:ferricstore, :waraft, :segment_projection_checkpoint, :start],
    [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
    [:ferricstore, :flow, :lmdb_writer, :flush],
    [:ferricstore, :waraft, :storage_blocked]
  ]

  def start(ctx), do: GenServer.start(__MODULE__, ctx, name: __MODULE__)

  def operation(kind, shard, fun) do
    previous = Ferricstore.LatencyTrace.start()
    started = System.monotonic_time(:microsecond)

    try do
      fun.()
    after
      finished = System.monotonic_time(:microsecond)
      trace = Ferricstore.LatencyTrace.finish(previous)

      if finished - started >= 100_000 do
        GenServer.cast(
          __MODULE__,
          {:slow,
           %{
             kind: kind,
             shard: shard,
             started_us: started,
             finished_us: finished,
             duration_us: finished - started,
             trace: trace
           }}
        )
      end
    end
  end

  def finish do
    :erlang.trace(:all, false, [:call, :set_on_spawn])
    for mfa <- @mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    :telemetry.detach(__MODULE__)
    report = GenServer.call(__MODULE__, :snapshot)
    GenServer.stop(__MODULE__)
    report
  end

  def event(event, measurements, metadata, pid) do
    GenServer.cast(
      pid,
      {:event,
       %{
         event: Enum.join(event, "."),
         at_us: System.monotonic_time(:microsecond),
         measurements: measurements,
         shard: Map.get(metadata, :shard_index),
         result: inspect(Map.get(metadata, :result)),
         reason: inspect(Map.get(metadata, :reason))
       }}
    )
  end

  @impl GenServer
  def init(ctx) do
    targets =
      for shard <- 0..3,
          {role, name} <- [
            {:shard, Ferricstore.Store.Router.shard_name(ctx, shard)},
            {:raft, :wa_raft_server.registered_name(:ferricstore_waraft_backend, shard + 1)},
            {:storage, :wa_raft_storage.registered_name(:ferricstore_waraft_backend, shard + 1)}
          ],
          pid = Process.whereis(name),
          is_pid(pid),
          do: {pid, role, shard}

    for mfa <- @mfas, do: :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])

    :erlang.trace(:all, true, [
      :call,
      :arity,
      :monotonic_timestamp,
      :set_on_spawn,
      {:tracer, self()}
    ])

    :ok = :telemetry.attach_many(__MODULE__, @events, &__MODULE__.event/4, self())
    Process.send_after(self(), :sample, 10)
    {:ok, %{targets: targets, history: [], calls: %{}, slow: [], events: [], timings: %{}}}
  end

  @impl GenServer
  def handle_info(:sample, state) do
    at = System.monotonic_time(:microsecond)

    actors =
      for {pid, role, shard} <- state.targets,
          info =
            Process.info(
              pid,
              [:status, :message_queue_len, :current_stacktrace, :reductions]
            ),
          info != nil do
        %{
          role: role,
          shard: shard,
          pid: inspect(pid),
          status: info[:status],
          queue: info[:message_queue_len],
          reductions: info[:reductions],
          stack: Enum.take(info[:current_stacktrace], 8) |> Enum.map(&inspect/1)
        }
      end

    Process.send_after(self(), :sample, 10)
    sample = %{at_us: at, actors: actors, run_queues: :erlang.statistics(:run_queue_lengths_all)}
    {:noreply, %{state | history: Enum.take([sample | state.history], 200)}}
  end

  def handle_info({:trace_ts, pid, :call, mfa, at}, state) when mfa in @mfas do
    {:noreply, %{state | calls: Map.update(state.calls, {pid, mfa}, [at], &[at | &1])}}
  end

  def handle_info({:trace_ts, pid, :return_from, mfa, _result, at}, state) when mfa in @mfas do
    case Map.get(state.calls, {pid, mfa}, []) do
      [started | rest] ->
        us = System.convert_time_unit(at - started, :native, :microsecond)
        metric = inspect(mfa)
        bucket = if us < 1_000, do: us, else: div(us + 999, 1_000) * 1_000

        timings =
          Map.update(
            state.timings,
            metric,
            %{bucket => 1},
            &Map.update(&1, bucket, 1, fn n -> n + 1 end)
          )

        calls =
          if rest == [],
            do: Map.delete(state.calls, {pid, mfa}),
            else: Map.put(state.calls, {pid, mfa}, rest)

        events =
          if us >= 100_000,
            do:
              Enum.take(
                [
                  %{
                    event: metric,
                    pid: inspect(pid),
                    started_us: System.convert_time_unit(started, :native, :microsecond),
                    duration_us: us
                  }
                  | state.events
                ],
                500
              ),
            else: state.events

        {:noreply, %{state | calls: calls, timings: timings, events: events}}

      [] ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast({:slow, operation}, state) do
    samples =
      for sample <- state.history,
          sample.at_us >= operation.started_us and
            sample.at_us <= operation.finished_us do
        %{sample | actors: Enum.filter(sample.actors, &(&1.shard == operation.shard))}
      end

    slow =
      [Map.put(operation, :samples, Enum.reverse(samples)) | state.slow]
      |> Enum.sort_by(& &1.duration_us, :desc)
      |> Enum.take(24)

    {:noreply, %{state | slow: slow}}
  end

  def handle_cast({:event, event}, state),
    do: {:noreply, %{state | events: Enum.take([event | state.events], 500)}}

  @impl GenServer
  def handle_call(:snapshot, _from, state),
    do:
      {:reply,
       %{
         slow_operations: state.slow,
         events: Enum.reverse(state.events),
         timing_buckets: state.timings,
         sample_interval_ms: 10,
         history_limit: 200,
         slow_limit: 24,
         slow_threshold_us: 100_000,
         schedulers: :erlang.system_info(:schedulers_online),
         dirty_cpu_schedulers: :erlang.system_info(:dirty_cpu_schedulers_online),
         dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers)
       }, state}
end
