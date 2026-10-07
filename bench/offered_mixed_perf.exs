# Fixed offered-load cycles with bounded per-worker queues and explicit overload counts.
Code.require_file("support/hset_group_variant.exs", __DIR__)
Code.require_file("support/promoted_read_variant.exs", __DIR__)
Code.require_file("support/write_timeline_profiler.exs", __DIR__)
Code.require_file("support/metadata_route_variant.exs", __DIR__)
Code.require_file("support/compaction_latch_variant.exs", __DIR__)

defmodule FerricstoreBench.OfferedMixed do
  alias FerricStore.Impl
  alias Ferricstore.Store.Router

  def run do
    rate = integer("BENCH_CYCLES_PER_SECOND", 200)
    seconds = integer("BENCH_SECONDS", 60)
    warmup = integer("BENCH_WARMUP_SECONDS", 10)
    clients = integer("BENCH_CLIENTS", 16)
    depth = integer("BENCH_QUEUE_DEPTH", 8)
    fields = integer("BENCH_FIELDS", 4_096)
    timeline? = System.get_env("BENCH_WRITE_TIMELINE") == "1"
    {mode, policy_source, policy_root} = FerricstoreBench.HsetGroupVariant.prepare()

    {compaction_latch, compaction_latch_source, compaction_latch_root} =
      FerricstoreBench.CompactionLatchVariant.prepare()

    {metadata_route, metadata_source, promotion_source, metadata_root} =
      FerricstoreBench.MetadataRouteVariant.prepare()

    root = Path.join([System.tmp_dir!(), "opencode", "offered-mixed-#{System.pid()}"])
    if File.exists?(root), do: raise("fixture exists")

    for {key, value} <- [
          data_dir: root,
          node_name: nil,
          shard_count: 4,
          max_memory_bytes: 1_073_741_824
        ],
        do: Application.put_env(:ferricstore, key, value)

    Logger.configure(level: :error)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore)
      ctx = FerricStore.Instance.get(:default)

      hashes =
        for shard <- 0..3 do
          Stream.iterate(0, &(&1 + 1))
          |> Enum.find_value(fn i ->
            key = "offered:{s#{i}}:hash"
            if Router.shard_for(ctx, key) == shard, do: key
          end)
        end

      seed = :binary.copy("s", 4096)

      for hash <- hashes do
        for page <- Enum.chunk_every(1..fields, 64) do
          {:ok, _} = Impl.hset(ctx, hash, Map.new(page, &{"seed-#{&1}", seed}))
        end

        true =
          GenServer.call(
            Router.shard_name(ctx, Router.shard_for(ctx, hash)),
            {:promoted?, hash},
            30_000
          )
      end

      parent = self()

      workers =
        for id <- 0..(clients - 1) do
          hash = Enum.at(hashes, rem(id, 4))
          field = "client-#{id}"
          key = hash <> ":kv:#{id}"
          :ok = Router.put(ctx, key, "control", 0)
          {:ok, 1} = Impl.hset(ctx, hash, %{field => value(0)})
          task = Task.async(fn -> worker(parent, ctx, id, hash, field, key, seed, fields, 0) end)
          {id, %{task: task, busy: false, queue: :queue.new(), queued: 0}}
        end
        |> Map.new()

      handler = {__MODULE__, make_ref()}

      :telemetry.attach_many(
        handler,
        [[:ferricstore, :dedicated, :compaction], [:ferricstore, :dedicated, :compaction_failed]],
        fn event, measurements, metadata, pid ->
          send(
            pid,
            {:maintenance,
             %{
               event: List.last(event),
               at_us: System.monotonic_time(:microsecond),
               measurements: measurements,
               shard: metadata[:shard_index]
             }}
          )
        end,
        self()
      )

      if timeline?, do: {:ok, _} = FerricstoreBench.WriteTimelineProfiler.start(ctx)
      started = System.monotonic_time(:microsecond)
      measured = started + warmup * 1_000_000
      deadline = measured + seconds * 1_000_000

      state = %{
        workers: workers,
        next: 0,
        rate: rate,
        start: started,
        measured: measured,
        deadline: deadline,
        depth: depth,
        offered: 0,
        dropped: 0,
        completed: 0,
        completed_in_window: 0,
        histograms: %{},
        events: [],
        max_queued: 0
      }

      state = schedule(state)
      finished = System.monotonic_time(:microsecond)

      versions =
        state.workers
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(fn {_, w} -> w.task end)
        |> Task.await_many(30_000)

      for {id, version} <-
            Enum.with_index(versions) |> Enum.map(fn {version, id} -> {id, version} end) do
        {:ok, actual} = Impl.hget(ctx, Enum.at(hashes, rem(id, 4)), "client-#{id}")
        true = actual == value(version)
      end

      timeline = if timeline?, do: FerricstoreBench.WriteTimelineProfiler.finish()
      Process.sleep(10_000)
      events = collect_maintenance(state.events)
      :telemetry.detach(handler)

      maintenance =
        for shard <- 0..3 do
          state = :sys.get_state(Router.shard_name(ctx, shard))

          %{
            shard: shard,
            active: state.promoted_compaction_worker != nil,
            pending: MapSet.size(state.promoted_compaction_pending),
            retries: map_size(state.promoted_compaction_retry_timers)
          }
        end

      report = %{
        mode: mode,
        compaction_latch: compaction_latch,
        compaction_latch_source: compaction_latch_source,
        shard_beam_md5: Base.encode16(Ferricstore.Store.Shard.module_info(:md5), case: :lower),
        metadata_route: metadata_route,
        metadata_source: metadata_source,
        promotion_source: promotion_source,
        segment_log_md5:
          Base.encode16(:ferricstore_waraft_spike_segment_log.module_info(:md5), case: :lower),
        promotion_md5: Base.encode16(Ferricstore.Store.Promotion.module_info(:md5), case: :lower),
        rate: rate,
        seconds: seconds,
        warmup_seconds: warmup,
        clients: clients,
        queue_depth_per_worker: depth,
        maximum_queued: state.max_queued,
        offered: state.offered,
        dropped: state.dropped,
        completed: state.completed,
        completed_in_window: state.completed_in_window,
        drain_seconds: max(finished - deadline, 0) / 1.0e6,
        errors: 0,
        compactions:
          Enum.count(
            events,
            &(&1.event == :compaction and &1.at_us >= measured and &1.at_us < deadline)
          ),
        quiet_compactions:
          Enum.count(events, &(&1.event == :compaction and &1.at_us >= deadline)),
        compaction_failures: Enum.count(events, &(&1.event == :compaction_failed)),
        maintenance: maintenance,
        histogram_bounds: :upper,
        histograms: summarize(state.histograms),
        publication_identity: FerricstoreBench.PromotedReadVariant.source_identity(),
        hset_policy_source: policy_source,
        backend_md5:
          Base.encode16(Ferricstore.Raft.WARaftBackend.module_info(:md5), case: :lower),
        hset_coalescing_enabled:
          Application.get_env(:ferricstore, :waraft_single_hset_coalescing, false)
      }

      report = if timeline?, do: Map.put(report, :write_timeline, timeline), else: report
      File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))

      IO.puts(
        Jason.encode!(
          Map.drop(report, [
            :publication_identity,
            :write_timeline,
            :hset_policy_source,
            :metadata_source,
            :promotion_source,
            :compaction_latch_source
          ])
        )
      )
    after
      Application.stop(:ferricstore)
      File.rm_rf!(root)
      if policy_root, do: File.rm_rf!(policy_root)
      if metadata_root, do: File.rm_rf!(metadata_root)
      if compaction_latch_root, do: File.rm_rf!(compaction_latch_root)
    end
  end

  defp schedule(state) do
    state = offer_due(state, System.monotonic_time(:microsecond))
    next_time = state.start + div(state.next * 1_000_000, state.rate)

    all_done? =
      Enum.all?(state.workers, fn {_, worker} -> not worker.busy and worker.queued == 0 end)

    if next_time >= state.deadline and all_done? do
      for {_, worker} <- state.workers, do: send(worker.task.pid, :stop)
      state
    else
      wait =
        if next_time >= state.deadline,
          do: 1_000,
          else: max(div(next_time - System.monotonic_time(:microsecond) + 999, 1_000), 0)

      receive do
        {:done, id, scheduled, finished, timings} ->
          state =
            if scheduled >= state.measured do
              %{
                state
                | completed: state.completed + 1,
                  completed_in_window:
                    state.completed_in_window + if(finished < state.deadline, do: 1, else: 0),
                  histograms:
                    Enum.reduce(timings, state.histograms, fn {key, us}, acc ->
                      record(acc, key, us)
                    end)
              }
            else
              state
            end

          worker = Map.fetch!(state.workers, id)

          {worker, job} =
            case :queue.out(worker.queue) do
              {{:value, job}, queue} -> {%{worker | queue: queue, queued: worker.queued - 1}, job}
              {:empty, _} -> {%{worker | busy: false}, nil}
            end

          if job, do: send(worker.task.pid, {:job, job})
          schedule(%{state | workers: Map.put(state.workers, id, worker)})

        {:maintenance, event} ->
          schedule(%{state | events: [event | state.events]})
      after
        wait -> schedule(state)
      end
    end
  end

  defp offer_due(state, now) do
    scheduled = state.start + div(state.next * 1_000_000, state.rate)

    if scheduled <= now and scheduled < state.deadline do
      id = rem(state.next, map_size(state.workers))
      worker = Map.fetch!(state.workers, id)
      measured? = scheduled >= state.measured
      job = {state.next + 1, scheduled}

      {worker, dropped} =
        cond do
          not worker.busy ->
            send(worker.task.pid, {:job, job})
            {%{worker | busy: true}, false}

          worker.queued < state.depth ->
            {%{worker | queue: :queue.in(job, worker.queue), queued: worker.queued + 1}, false}

          true ->
            {worker, true}
        end

      workers = Map.put(state.workers, id, worker)

      state = %{
        state
        | workers: workers,
          next: state.next + 1,
          offered: state.offered + if(measured?, do: 1, else: 0),
          dropped: state.dropped + if(measured? and dropped, do: 1, else: 0),
          max_queued:
            max(state.max_queued, Enum.sum(Enum.map(workers, fn {_, w} -> w.queued end)))
      }

      offer_due(state, now)
    else
      state
    end
  end

  defp worker(parent, ctx, id, hash, field, key, seed, fields, version) do
    receive do
      {:job, {next, scheduled}} ->
        start = System.monotonic_time(:microsecond)
        {:ok, 0} = Impl.hset(ctx, hash, %{field => value(next)})
        written = System.monotonic_time(:microsecond)
        "control" = Router.get(ctx, key)
        kv_read = System.monotonic_time(:microsecond)
        {:ok, actual} = Impl.hget(ctx, hash, field)
        true = actual == value(next)
        hash_read = System.monotonic_time(:microsecond)
        {:ok, ^seed} = Impl.hget(ctx, hash, "seed-#{rem(next, fields) + 1}")
        finished = System.monotonic_time(:microsecond)

        send(
          parent,
          {:done, id, scheduled, finished,
           [
             client_queue_us: start - scheduled,
             write_service_us: written - start,
             write_total_us: written - scheduled,
             cycle_service_us: finished - start,
             cycle_total_us: finished - scheduled,
             kv_read_us: kv_read - written,
             hash_read_us: hash_read - kv_read,
             hash_read_us: finished - hash_read
           ]}
        )

        worker(parent, ctx, id, hash, field, key, seed, fields, next)

      :stop ->
        version
    end
  end

  defp record(histograms, key, us) do
    bucket =
      cond do
        us < 1_000 -> us
        us < 100_000 -> div(us + 99, 100) * 100
        true -> div(us + 999, 1_000) * 1_000
      end

    Map.update(histograms, key, %{count: 1, max: us, buckets: %{bucket => 1}}, fn stat ->
      %{
        count: stat.count + 1,
        max: max(stat.max, us),
        buckets: Map.update(stat.buckets, bucket, 1, &(&1 + 1))
      }
    end)
  end

  defp summarize(histograms) do
    Map.new(histograms, fn {key, stat} ->
      quantiles =
        for {name, q} <- [p50: 0.5, p95: 0.95, p99: 0.99, p999: 0.999], into: %{} do
          value =
            Enum.reduce_while(Enum.sort(stat.buckets), 0, fn {us, n}, seen ->
              if seen + n >= stat.count * q, do: {:halt, us}, else: {:cont, seen + n}
            end)

          {name, value}
        end

      {key, Map.merge(quantiles, %{count: stat.count, max: stat.max})}
    end)
  end

  defp collect_maintenance(events) do
    receive do
      {:maintenance, event} -> collect_maintenance([event | events])
    after
      0 -> events
    end
  end

  defp value(version), do: <<version::unsigned-64, 0::size(4088 * 8)>>

  defp integer(key, default) do
    value = System.get_env(key, Integer.to_string(default)) |> String.to_integer()
    if value <= 0, do: raise("#{key} must be positive")
    value
  end
end

FerricstoreBench.OfferedMixed.run()
