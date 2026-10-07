# ERL_FLAGS='+S 8:8' BENCH_COMPACTION=auto BENCH_TRIAL=1 \
#   mise exec -- mix run --no-start bench/sustained_mixed_perf.exs
# The "deferred" diagnostic control postpones only promoted-compaction cooldowns.
Code.require_file("support/hash_stall_profiler.exs", __DIR__)
Code.require_file("support/promoted_read_variant.exs", __DIR__)
Code.require_file("support/waraft_perf_metrics.exs", __DIR__)
Code.require_file("support/compaction_variant.exs", __DIR__)
Code.require_file("support/compaction_sync_variant.exs", __DIR__)
Code.require_file("support/hset_group_variant.exs", __DIR__)
Code.require_file("support/write_timeline_profiler.exs", __DIR__)
Code.require_file("support/compaction_latch_variant.exs", __DIR__)

defmodule FerricstoreBench.SustainedMixed do
  alias FerricStore.Impl
  alias Ferricstore.Store.Router

  @events [
    [:ferricstore, :dedicated, :compaction],
    [:ferricstore, :dedicated, :compaction_failed]
  ]

  def run do
    mode = System.get_env("BENCH_COMPACTION", "auto")
    if mode not in ["auto", "deferred"], do: raise("invalid compaction mode")
    seconds = env_int("BENCH_SECONDS", 120)
    warmup = env_int("BENCH_WARMUP_SECONDS", 10)
    clients = env_int("BENCH_CLIENTS", 16)
    cycle_period_ms = env_int("BENCH_CYCLE_MS", 0)
    fields = env_int("BENCH_FIELDS", 4_096)
    trial = System.get_env("BENCH_TRIAL", "1")
    profile? = System.get_env("BENCH_PROFILE") == "1"
    request_spans? = System.get_env("BENCH_REQUEST_SPANS") == "1"
    metrics? = System.get_env("BENCH_METRICS") == "1"
    timeline? = System.get_env("BENCH_WRITE_TIMELINE") == "1"

    if timeline? and profile?,
      do: raise("timeline and old call profiler require separate diagnostic runs")

    root = Path.join(System.tmp_dir!(), "ferricstore-sustained-#{System.pid()}")
    if File.exists?(root), do: raise("fixture already exists")
    {read_variant, router_source, code_root} = FerricstoreBench.PromotedReadVariant.prepare()

    {single_hset_variant, hash_source, hash_code_root} =
      FerricstoreBench.PromotedReadVariant.prepare_single_hset()

    {compaction_admission, compaction_source, compaction_code_root} =
      FerricstoreBench.CompactionVariant.prepare()

    {compaction_sync, compaction_sync_source, compaction_sync_root} =
      FerricstoreBench.CompactionSyncVariant.prepare()

    {hset_group, hset_group_source, hset_group_root} = FerricstoreBench.HsetGroupVariant.prepare()

    {compaction_latch, compaction_latch_source, compaction_latch_root} =
      FerricstoreBench.CompactionLatchVariant.prepare()

    Application.put_env(:ferricstore, :data_dir, root)
    Application.put_env(:ferricstore, :node_name, nil)
    Application.put_env(:ferricstore, :shard_count, 4)
    Application.put_env(:ferricstore, :max_memory_bytes, 1_073_741_824)
    Logger.configure(level: :error)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore)
      ctx = FerricStore.Instance.get(:default)

      hashes =
        for shard <- 0..3 do
          Stream.iterate(0, &(&1 + 1))
          |> Enum.find_value(fn i ->
            key = "sustained:{s#{i}}:hash"
            if Router.shard_for(ctx, key) == shard, do: key
          end)
        end

      seed = :binary.copy("s", 4_096)

      for hash <- hashes do
        for page <- Enum.chunk_every(1..fields, 64) do
          {:ok, _} = Impl.hset(ctx, hash, Map.new(page, &{"seed-#{&1}", seed}))
        end

        shard = Router.shard_name(ctx, Router.shard_for(ctx, hash))
        true = GenServer.call(shard, {:promoted?, hash}, 30_000)

        if mode == "deferred" do
          :sys.replace_state(shard, fn state ->
            instances =
              Map.update!(state.promoted_instances, hash, fn info ->
                Map.put(info, :last_compacted_at, System.monotonic_time(:millisecond) + 3_600_000)
              end)

            %{state | promoted_instances: instances}
          end)
        end
      end

      parent = self()
      if profile?, do: FerricstoreBench.HashStallProfiler.start(ctx)
      if metrics?, do: FerricstoreBench.WARaftPerfMetrics.start(trace_calls: false)
      if timeline?, do: FerricstoreBench.WriteTimelineProfiler.start(ctx)
      :ok = :telemetry.attach_many(__MODULE__, @events, &__MODULE__.event/4, parent)
      started = System.monotonic_time(:microsecond)
      measured_start = started + warmup * 1_000_000
      deadline = measured_start + seconds * 1_000_000
      before = memory()

      tasks =
        for id <- 0..(clients - 1) do
          Task.async(fn ->
            Process.put(
              :hash_stall_profile,
              profile? and System.get_env("BENCH_PROFILE_SPANS", "1") == "1"
            )

            Process.put(:hash_request_spans, request_spans?)
            Process.put(:hash_cycle_period_us, cycle_period_ms * 1_000)

            Process.put(
              :hash_next_cycle_us,
              System.monotonic_time(:microsecond) + div(id * cycle_period_ms * 1_000, clients)
            )

            hash = Enum.at(hashes, rem(id, 4))
            field = "client-#{id}"
            key = hash <> ":kv:#{id}"
            :ok = Router.put(ctx, key, "control", 0)

            case Impl.hset(ctx, hash, %{field => value(0)}) do
              {:ok, _} ->
                :ok

              error ->
                if id == 0, do: dump_write_failure(ctx, hash, error)
                raise "initial hash write failed: #{inspect(error)}"
            end

            loop(ctx, hash, field, key, seed, fields, measured_start, deadline, 0, 0, %{})
          end)
        end

      samples = sample_until(deadline, [])
      reports = Task.await_many(tasks, 60_000)
      finished = System.monotonic_time(:microsecond)
      profile = if profile?, do: FerricstoreBench.HashStallProfiler.finish(), else: nil
      metrics = if metrics?, do: FerricstoreBench.WARaftPerfMetrics.snapshot(), else: nil
      timeline = if timeline?, do: FerricstoreBench.WriteTimelineProfiler.finish(), else: nil
      windows = Enum.reduce(reports, %{}, &merge_windows/2)
      events = collect_events([])

      for {task_report, id} <- Enum.zip(reports, 0..(clients - 1)) do
        hash = Enum.at(hashes, rem(id, 4))
        {:ok, expected} = Impl.hget(ctx, hash, "client-#{id}")
        true = expected == value(task_report.version)
      end

      Process.sleep(10_000)
      after_quiet = memory()
      after_quiet_events = collect_events([])
      :telemetry.detach(__MODULE__)

      compaction_status =
        for index <- 0..3 do
          state = :sys.get_state(Router.shard_name(ctx, index))

          %{
            shard: index,
            active: state.promoted_compaction_worker != nil,
            pending: MapSet.size(state.promoted_compaction_pending),
            retries: map_size(state.promoted_compaction_retry_timers)
          }
        end

      result = %{
        mode: mode,
        trial: trial,
        seconds: seconds,
        warmup_seconds: warmup,
        clients: clients,
        cycle_period_ms: cycle_period_ms,
        request_spans_enabled: request_spans?,
        shards: 4,
        fields_per_hash: fields,
        promoted_read_variant: read_variant,
        single_hset_variant: single_hset_variant,
        hset_group: hset_group,
        hset_coalescing_enabled:
          Application.get_env(:ferricstore, :waraft_single_hset_coalescing, false),
        hset_group_source: hset_group_source,
        backend_beam_md5:
          Base.encode16(Ferricstore.Raft.WARaftBackend.module_info(:md5), case: :lower),
        compaction_admission: compaction_admission,
        compaction_sync: compaction_sync,
        compaction_latch: compaction_latch,
        compaction_latch_source: compaction_latch_source,
        compaction_sync_source: compaction_sync_source,
        promoted_beam_md5:
          Base.encode16(Ferricstore.Store.Shard.Compound.Promoted.module_info(:md5), case: :lower),
        compaction_source: compaction_source,
        shard_beam_md5: Base.encode16(Ferricstore.Store.Shard.module_info(:md5), case: :lower),
        compaction_status: compaction_status,
        after_quiet_events: after_quiet_events,
        hash_source: hash_source,
        hash_beam_md5: Base.encode16(Ferricstore.Commands.Hash.module_info(:md5), case: :lower),
        router_source: router_source,
        router_beam_md5: Base.encode16(Router.module_info(:md5), case: :lower),
        publication_identity: FerricstoreBench.PromotedReadVariant.source_identity(),
        value_bytes: 4096,
        measured_elapsed_seconds: (finished - measured_start) / 1.0e6,
        configured_cache_bytes: ctx.max_memory_bytes,
        before: before,
        after_quiet: after_quiet,
        memory_samples: Enum.reverse(samples),
        events: events,
        windows: summarize_windows(windows),
        errors: 0
      }

      result = if profile?, do: Map.put(result, :profile, profile), else: result
      result = if metrics?, do: Map.put(result, :waraft_metrics, metrics), else: result
      result = if timeline?, do: Map.put(result, :write_timeline, timeline), else: result

      output =
        System.get_env("BENCH_OUTPUT", "bench/results/sustained-mixed-#{mode}-#{trial}.json")

      File.mkdir_p!(Path.dirname(output))
      File.write!(output, Jason.encode!(result, pretty: true))

      IO.puts(
        "SUSTAINED_RESULT " <>
          Jason.encode!(
            Map.drop(result, [
              :windows,
              :memory_samples,
              :events,
              :profile,
              :write_timeline,
              :compaction_latch_source,
              :router_source,
              :hash_source,
              :compaction_source,
              :compaction_sync_source,
              :hset_group_source
            ])
          )
      )

      IO.puts(
        "compactions=#{Enum.count(events, &(&1.event == "compaction"))} failures=#{Enum.count(events, &(&1.event == "compaction_failed"))}"
      )
    after
      if Process.whereis(:telemetry_handler_table), do: :telemetry.detach(__MODULE__)
      Application.stop(:ferricstore)
      File.rm_rf!(root)
      if code_root, do: File.rm_rf!(code_root)
      if hash_code_root, do: File.rm_rf!(hash_code_root)
      if compaction_code_root, do: File.rm_rf!(compaction_code_root)
      if compaction_sync_root, do: File.rm_rf!(compaction_sync_root)
      if hset_group_root, do: File.rm_rf!(hset_group_root)
      if compaction_latch_root, do: File.rm_rf!(compaction_latch_root)
    end
  end

  def event(event, measurements, metadata, parent) do
    send(
      parent,
      {:compaction,
       %{
         event: to_string(List.last(event)),
         at_us: System.monotonic_time(:microsecond),
         measurements: measurements,
         shard: metadata.shard_index,
         layout: Map.get(metadata, :layout, :whole)
       }}
    )
  end

  defp dump_write_failure(ctx, key, error) do
    index = Router.shard_for(ctx, key)

    names = [
      Router.shard_name(ctx, index),
      :wa_raft_server.registered_name(:ferricstore_waraft_backend, index + 1),
      :wa_raft_storage.registered_name(:ferricstore_waraft_backend, index + 1)
    ]

    actors =
      for name <- names,
          pid = Process.whereis(name),
          is_pid(pid),
          do: {name, Process.info(pid, [:status, :current_stacktrace, :message_queue_len])}

    IO.inspect(
      %{error: error, actors: actors, latches: :ets.tab2list(elem(ctx.latch_refs, index))},
      label: "WRITE_FAILURE",
      limit: :infinity
    )
  end

  defp loop(ctx, hash, field, key, seed, fields, start, deadline, i, version, windows) do
    if rem(i, 4) == 0, do: pace_cycle()
    t0 = System.monotonic_time(:microsecond)
    if rem(i, 4) == 0, do: Process.put(:hash_cycle_started_us, t0)

    if t0 >= deadline do
      %{windows: windows, version: version}
    else
      operation = fn ->
        case rem(i, 4) do
          0 ->
            {:ok, 0} = Impl.hset(ctx, hash, %{field => value(version + 1)})
            {:hash_write, version + 1}

          1 ->
            "control" = Router.get(ctx, key)
            {:kv_read, version}

          2 ->
            {:ok, actual} = Impl.hget(ctx, hash, field)
            true = actual == value(version)
            {:hash_read, version}

          3 ->
            {:ok, ^seed} = Impl.hget(ctx, hash, "seed-#{rem(i, fields) + 1}")
            {:hash_read, version}
        end
      end

      previous = if Process.get(:hash_request_spans), do: Ferricstore.LatencyTrace.start()

      {kind, version} =
        if Process.get(:hash_stall_profile),
          do:
            FerricstoreBench.HashStallProfiler.operation(
              if(rem(i, 4) == 0,
                do: :hash_write,
                else: if(rem(i, 4) == 1, do: :kv_read, else: :hash_read)
              ),
              Router.shard_for(ctx, hash),
              operation
            ),
          else: operation.()

      elapsed = System.monotonic_time(:microsecond) - t0

      spans =
        if Process.get(:hash_request_spans),
          do: Ferricstore.LatencyTrace.finish(previous),
          else: []

      windows =
        if t0 >= start,
          do: record(windows, {div(t0 - start, 10_000_000), kind}, elapsed),
          else: windows

      windows =
        if t0 >= start do
          Enum.reduce(spans, windows, fn {span, us}, acc ->
            record(acc, {div(t0 - start, 10_000_000), "#{kind}.#{span}"}, us)
          end)
        else
          windows
        end

      cycle_started = Process.get(:hash_cycle_started_us)

      windows =
        if rem(i, 4) == 3 and cycle_started >= start do
          record(
            windows,
            {div(cycle_started - start, 10_000_000), :cycle},
            System.monotonic_time(:microsecond) - cycle_started
          )
        else
          windows
        end

      loop(ctx, hash, field, key, seed, fields, start, deadline, i + 1, version, windows)
    end
  end

  defp pace_cycle do
    period = Process.get(:hash_cycle_period_us, 0)

    if period > 0 do
      now = System.monotonic_time(:microsecond)
      target = Process.get(:hash_next_cycle_us, now)
      if target > now, do: Process.sleep(div(target - now + 999, 1_000))
      Process.put(:hash_next_cycle_us, max(target, System.monotonic_time(:microsecond)) + period)
    end
  end

  defp value(version), do: <<version::unsigned-64, 0::size(4088 * 8)>>

  defp record(windows, key, us) do
    bucket =
      cond do
        us < 1_000 -> us
        us < 10_000 -> div(us + 9, 10) * 10
        us < 100_000 -> div(us + 99, 100) * 100
        true -> div(us + 999, 1_000) * 1_000
      end

    Map.update(windows, key, %{count: 1, max_us: us, buckets: %{bucket => 1}}, fn stat ->
      %{
        count: stat.count + 1,
        max_us: max(stat.max_us, us),
        buckets: Map.update(stat.buckets, bucket, 1, &(&1 + 1))
      }
    end)
  end

  defp merge_windows(%{windows: windows}, acc) do
    Map.merge(acc, windows, fn _, a, b ->
      %{
        count: a.count + b.count,
        max_us: max(a.max_us, b.max_us),
        buckets: Map.merge(a.buckets, b.buckets, fn _, x, y -> x + y end)
      }
    end)
  end

  defp summarize_windows(windows) do
    for {{window, kind}, stat} <- Enum.sort(windows) do
      Map.merge(stat, %{
        window: window,
        operation: kind,
        p95_us: quantile(stat, 0.95),
        p99_us: quantile(stat, 0.99)
      })
    end
  end

  defp quantile(stat, q) do
    target = ceil(stat.count * q)

    Enum.reduce_while(Enum.sort(stat.buckets), 0, fn {us, n}, seen ->
      if seen + n >= target, do: {:halt, us}, else: {:cont, seen + n}
    end)
  end

  defp memory do
    %{
      operational_pressure: Ferricstore.OperationalGuard.pressure?(),
      writes_rejected: Ferricstore.OperationalGuard.reject_writes?(),
      at_us: System.monotonic_time(:microsecond),
      rss_bytes: Ferricstore.MemoryGuard.process_rss_bytes(),
      beam_bytes: :erlang.memory(:total),
      binary_bytes: :erlang.memory(:binary),
      ets_bytes: :erlang.memory(:ets),
      processes: :erlang.system_info(:process_count)
    }
  end

  defp sample_until(deadline, samples) do
    if System.monotonic_time(:microsecond) >= deadline,
      do: samples,
      else:
        (
          sample = memory()
          Process.sleep(1_000)
          sample_until(deadline, [sample | samples])
        )
  end

  defp collect_events(events) do
    receive do
      {:compaction, event} -> collect_events([event | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp env_int(name, default), do: System.get_env(name, to_string(default)) |> String.to_integer()
end

FerricstoreBench.SustainedMixed.run()
