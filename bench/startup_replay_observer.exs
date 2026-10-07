# Run by startup_replay_compare.py inside a disposable release container.
defmodule FerricstoreBench.StartupReplayObserver do
  alias Ferricstore.Store.Router

  @events [
    [:ferricstore, :shard, :startup_phase],
    [:ferricstore, :waraft, :backend, :startup_phase],
    [:ferricstore, :waraft, :storage, :startup_phase],
    [:ferricstore, :waraft, :segment_log, :startup_phase]
  ]

  def run do
    Logger.configure(level: :warning)
    quiescent? = System.get_env("BENCH_QUIESCENT") == "1"

    if quiescent? do
      for key <- [
            :flow_scheduler_enabled,
            :flow_retention_sweeper_enabled,
            :flow_policy_migration_worker_enabled
          ] do
        Application.put_env(:ferricstore, key, false)
      end
    end

    {:ok, _} = Application.ensure_all_started(:telemetry)
    :ok = :telemetry.attach_many(__MODULE__, @events, &__MODULE__.event/4, self())
    started = System.monotonic_time(:microsecond)
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok = FerricStore.await_ready(timeout: 120_000, interval: 50)
    ready_ms = (System.monotonic_time(:microsecond) - started) / 1_000
    peak = "/sys/fs/cgroup/memory.peak" |> File.read!() |> String.trim() |> String.to_integer()
    :ok = :telemetry.detach(__MODULE__)
    ctx = FerricStore.Instance.get(:default)
    phases = collect_phases(%{})

    if quiescent? do
      for shard <- 0..(ctx.shard_count - 1),
          do: :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, shard, 30_000)

      :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, 30_000)
      Process.sleep(2_000)
    end

    cutoff_ms = System.system_time(:millisecond)

    fingerprints =
      for table <- Tuple.to_list(ctx.keydir_refs) do
        {count, digest, live_count, live_digest} =
          :ets.foldl(
            fn row, {count, sum, live_count, live_sum} ->
              key = elem(row, 0)
              expires = elem(row, 2)

              <<hash::unsigned-256>> =
                :crypto.hash(:sha256, :erlang.term_to_binary({key, expires}))

              live? = expires == 0 or expires > cutoff_ms

              {count + 1, Bitwise.band(sum + hash, Bitwise.bsl(1, 256) - 1),
               live_count + if(live?, do: 1, else: 0),
               Bitwise.band(live_sum + if(live?, do: hash, else: 0), Bitwise.bsl(1, 256) - 1)}
            end,
            {0, 0, 0, 0},
            table
          )

        %{
          count: count,
          key_expiry_digest: Integer.to_string(digest, 16),
          live_count: live_count,
          expired_count: count - live_count,
          live_key_expiry_digest: Integer.to_string(live_digest, 16)
        }
      end

    startup = %{
      quiescent_diagnostic: quiescent?,
      ready_ms: ready_ms,
      startup_cgroup_peak_bytes: peak,
      phases: phases,
      keydir_fingerprints: fingerprints,
      shard_count: ctx.shard_count,
      schedulers: :erlang.system_info(:schedulers_online),
      preopen_concurrency:
        Ferricstore.Raft.WARaftBackend.StartupPreopen.default_concurrency(
          ctx.max_memory_bytes,
          ctx.memory_limit
        )
    }

    IO.puts("STARTUP_BENCH " <> Jason.encode!(startup))

    if System.get_env("BENCH_CAPTURE_KEYS") == "1" do
      File.open!("/data/benchmark-keydir.tsv", [:write, :binary], fn io ->
        for {table, shard} <- ctx.keydir_refs |> Tuple.to_list() |> Enum.with_index() do
          rows = :ets.tab2list(table)

          IO.binwrite(
            io,
            Enum.map(rows, fn row ->
              [
                Integer.to_string(shard),
                "\t",
                Base.encode64(elem(row, 0)),
                "\t",
                Integer.to_string(elem(row, 2)),
                "\n"
              ]
            end)
          )
        end
      end)
    end

    live =
      if System.get_env("BENCH_MODE") == "live" do
        concurrencies = if System.get_env("BENCH_FLOW_CONFIRM") == "1", do: [16], else: [1, 16]

        scenarios =
          if System.get_env("BENCH_FLOW_CONFIRM") == "1",
            do: [:empty_flow_claim],
            else: [:put_get, :empty_flow_claim]

        duration_ms = if System.get_env("BENCH_FLOW_CONFIRM") == "1", do: 30_000, else: 5_000

        for concurrency <- concurrencies, scenario <- scenarios do
          measure(ctx, scenario, concurrency, 1_000)
          result = measure(ctx, scenario, concurrency, duration_ms)
          Map.merge(result, %{scenario: scenario, concurrency: concurrency})
        end
      else
        []
      end

    IO.puts("STARTUP_BENCH_COMPLETE " <> Jason.encode!(%{startup: startup, live: live}))
    # All measured commands have acknowledged. This clone is discarded; shutdown
    # projection work is deliberately outside the startup/request comparison.
    System.halt(0)
  end

  def event(event, measurements, metadata, pid),
    do: send(pid, {:phase, Enum.join(event, ":"), measurements, metadata})

  defp collect_phases(acc) do
    receive do
      {:phase, event, %{duration_us: us}, metadata} ->
        key = event <> ":" <> to_string(metadata[:phase])

        acc =
          Map.update(acc, key, %{count: 1, sum_ms: us / 1_000, max_ms: us / 1_000}, fn item ->
            %{
              count: item.count + 1,
              sum_ms: item.sum_ms + us / 1_000,
              max_ms: max(item.max_ms, us / 1_000)
            }
          end)

        collect_phases(acc)

      {:phase, _, _, _} ->
        collect_phases(acc)
    after
      0 -> acc
    end
  end

  defp measure(ctx, scenario, concurrency, duration_ms) do
    parent = self()

    tasks =
      for id <- 1..concurrency do
        Task.async(fn ->
          key = "startup-review:live:#{id}"
          value = :binary.copy("v", 256)

          operation =
            case scenario do
              :put_get ->
                fn ->
                  :ok = Router.put(ctx, key, value, 0)
                  ^value = Router.get(ctx, key)
                end

              :empty_flow_claim ->
                fn ->
                  {:ok, []} =
                    Ferricstore.Flow.claim_due(ctx, "startup-review-empty-type",
                      worker: "bench-worker",
                      partition_key: key,
                      state: "queued",
                      limit: 1
                    )
                end
            end

          send(parent, {:ready, self()})

          receive do
            {:run, deadline} -> loop(operation, deadline, [])
          end
        end)
      end

    for %{pid: pid} <- tasks do
      receive do
        {:ready, ^pid} -> :ok
      after
        5_000 -> raise("client failed to start")
      end
    end

    started = System.monotonic_time(:microsecond)
    for %{pid: pid} <- tasks, do: send(pid, {:run, started + duration_ms * 1_000})
    reports = Task.await_many(tasks, duration_ms + 60_000)
    seconds = (Enum.max(Enum.map(reports, &elem(&1, 1))) - started) / 1.0e6
    samples = Enum.flat_map(reports, &elem(&1, 0)) |> Enum.sort()

    %{
      operations: length(samples),
      seconds: seconds,
      ops_per_second: length(samples) / seconds,
      p95_us: percentile(samples, 0.95),
      p99_us: percentile(samples, 0.99)
    }
  end

  defp loop(operation, deadline, samples) do
    started = System.monotonic_time(:microsecond)

    if started >= deadline do
      {samples, started}
    else
      operation.()
      elapsed = System.monotonic_time(:microsecond) - started
      loop(operation, deadline, [elapsed | samples])
    end
  end

  defp percentile(samples, fraction), do: Enum.at(samples, ceil(length(samples) * fraction) - 1)
end

FerricstoreBench.StartupReplayObserver.run()
