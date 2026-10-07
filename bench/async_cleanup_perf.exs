# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/async_cleanup_perf.exs
# Focused, same-VM comparison of the lifecycle-safe review helper and two small
# cleanup optimizations. No application, listener, or existing data is opened.

defmodule FerricstoreBench.AsyncCleanup do
  alias Ferricstore.Bitcask.NIF

  @source "apps/ferricstore/lib/ferricstore/bitcask/async.ex"
  @output "bench/results/async-cleanup-perf.json"
  @baseline_sha "9404cf0327a2f59cba027685a12ec886d0cdabed2241e5fb1820422ab5ea9f5b"

  def run do
    fsync_confirm? = System.get_env("BENCH_FSYNC_CONFIRM") == "1"
    duration_ms = if fsync_confirm?, do: 20_000, else: 3_000

    output =
      if fsync_confirm?, do: "bench/results/async-cleanup-fsync-confirm.json", else: @output

    baseline =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    if sha(baseline) != @baseline_sha, do: raise("unexpected review baseline source")

    implicit_cleanup =
      baseline
      |> String.replace("        try do\n", "")
      |> String.replace(
        "        after\n          Process.demonitor(caller_monitor, [:flush])\n        end\n",
        ""
      )
      |> format_source()

    early_demonitor =
      implicit_cleanup
      |> String.replace(
        "      cleanup_alias(parent, ref)\n      Process.demonitor(proxy_monitor, [:flush])",
        "      Process.demonitor(proxy_monitor, [:flush])\n      cleanup_alias(parent, ref)"
      )
      |> format_source()

    sources = [
      baseline: baseline,
      implicit_cleanup: implicit_cleanup,
      early_demonitor: early_demonitor
    ]

    variants =
      for {name, source} <- sources do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        renamed =
          String.replace(
            source,
            "defmodule Ferricstore.Bitcask.Async do",
            "defmodule #{inspect(module)} do"
          )

        [{^module, _}] = Code.compile_string(renamed)
        {name, Function.capture(module, :await, 2)}
      end

    root = Path.join(System.tmp_dir!(), "ferricstore-async-cleanup-perf-#{System.pid()}")
    if File.exists?(root), do: raise("benchmark directory already exists")
    File.mkdir_p!(root)

    try do
      path = Path.join(root, "reads.log")
      value = :binary.copy("r", 4096)
      records = for i <- 0..255, do: {"key-#{i}", value, 0}
      {:ok, locations} = NIF.v2_append_batch(path, records)
      :ok = NIF.v2_fsync(path)
      reads = Enum.zip(locations, records) |> List.to_tuple()

      concurrencies = if fsync_confirm?, do: [16], else: [1, 16]

      scenarios =
        if fsync_confirm?,
          do: [:append_fsync_256b],
          else: [:proxy_only, :pread_4k, :append_fsync_256b]

      variants =
        if fsync_confirm?,
          do: Enum.reject(variants, &(elem(&1, 0) == :early_demonitor)),
          else: variants

      results =
        for concurrency <- concurrencies,
            kind <- scenarios,
            trial <- 1..5,
            {name, await} <- order(variants, trial) do
          prepare = fn worker -> prepare(kind, await, worker, root, path, reads, value) end
          sample_every = if kind == :proxy_only, do: 100, else: 1
          measure(prepare, concurrency, 500, 0)
          measurement = measure(prepare, concurrency, duration_ms, sample_every)

          row =
            Map.merge(measurement, %{
              variant: name,
              scenario: kind,
              concurrency: concurrency,
              trial: trial
            })

          IO.puts(Jason.encode!(row))
          row
        end

      result = %{
        baseline_source: baseline,
        source_sha256: Map.new(sources, fn {name, source} -> {name, sha(source)} end),
        candidate_sources: Map.new(sources),
        elixir: System.version(),
        otp: to_string(:erlang.system_info(:otp_release)),
        schedulers: :erlang.system_info(:schedulers_online),
        warmup_ms: 500,
        measurement_ms: duration_ms,
        trials_per_scenario: 5,
        results: results
      }

      File.mkdir_p!(Path.dirname(output))
      File.write!(output, Jason.encode!(result, pretty: true))
      summarize(results)
    after
      File.rm_rf!(root)
    end
  end

  defp order([_, _] = variants, trial) do
    if rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants
  end

  defp order(variants, trial) do
    shift = rem(trial - 1, length(variants))
    rotated = Enum.drop(variants, shift) ++ Enum.take(variants, shift)
    if rem(trial, 2) == 0, do: Enum.reverse(rotated), else: rotated
  end

  defp prepare(:proxy_only, await, _worker, _root, _path, _reads, _value) do
    fn _i ->
      {:ok, :ok} =
        await.(
          fn pid, id ->
            send(pid, {:tokio_complete, id, :ok})
            :ok
          end,
          5_000
        )
    end
  end

  defp prepare(:pread_4k, await, worker, _root, path, reads, value) do
    fn i ->
      {{offset, _size}, {key, _, _}} = elem(reads, rem(i + worker * 17, tuple_size(reads)))
      {:ok, ^value} = await.(&NIF.v2_pread_at_key_async(&1, &2, path, offset, key), 5_000)
    end
  end

  defp prepare(:append_fsync_256b, await, worker, root, _path, _reads, _value) do
    path = Path.join(root, "write-#{worker}.log")
    records = [{"key", :binary.copy("w", 256), 0}]

    fn _i ->
      {:ok, [_]} = await.(&NIF.v2_append_batch_async(&1, &2, path, records), 5_000)
      {:ok, :ok} = await.(&NIF.v2_fsync_async(&1, &2, path), 5_000)
    end
  end

  defp measure(prepare, concurrency, duration_ms, sample_every) do
    parent = self()

    workers =
      for id <- 1..concurrency do
        spawn_monitor(fn ->
          operation = prepare.(id)
          send(parent, {:ready, self()})

          receive do
            {:run, deadline} ->
              send(parent, {:result, self(), loop(operation, deadline, sample_every, 0, [])})
          end
        end)
      end

    for {pid, _} <- workers do
      receive do
        {:ready, ^pid} -> :ok
      after
        5_000 -> raise("worker startup failed")
      end
    end

    started = now_ns()
    for {pid, _} <- workers, do: send(pid, {:run, started + duration_ms * 1_000_000})

    reports =
      for {pid, ref} <- workers do
        report =
          receive do
            {:result, ^pid, result} -> result
            {:DOWN, ^ref, :process, ^pid, reason} -> raise("worker failed: #{inspect(reason)}")
          after
            duration_ms + 10_000 -> raise("worker timed out")
          end

        receive do
          {:DOWN, ^ref, :process, ^pid, :normal} -> :ok
        after
          5_000 -> raise("worker exit timed out")
        end

        report
      end

    count = Enum.sum(Enum.map(reports, &elem(&1, 0)))
    seconds = (Enum.max(Enum.map(reports, &elem(&1, 2))) - started) / 1.0e9
    samples = Enum.flat_map(reports, &elem(&1, 1)) |> Enum.sort()

    %{
      operations: count,
      seconds: seconds,
      ops_per_second: count / seconds,
      samples: length(samples),
      p50_us: percentile(samples, 0.5),
      p95_us: percentile(samples, 0.95),
      p99_us: percentile(samples, 0.99)
    }
  end

  defp loop(operation, deadline, every, count, samples) do
    started = now_ns()

    if started >= deadline do
      {count, samples, started}
    else
      operation.(count)
      elapsed_us = (now_ns() - started) / 1_000
      samples = if every > 0 and rem(count, every) == 0, do: [elapsed_us | samples], else: samples
      loop(operation, deadline, every, count + 1, samples)
    end
  end

  defp summarize(results) do
    for concurrency <- [1, 16],
        scenario <- [:proxy_only, :pread_4k, :append_fsync_256b],
        variant <- [:baseline, :implicit_cleanup, :early_demonitor] do
      rows =
        Enum.filter(
          results,
          &(&1.concurrency == concurrency and &1.scenario == scenario and &1.variant == variant)
        )

      if rows != [] do
        stats =
          for metric <- [:ops_per_second, :p50_us, :p95_us, :p99_us], into: %{} do
            values = Enum.map(rows, &Map.fetch!(&1, metric)) |> Enum.sort()
            {metric, %{median: Enum.at(values, 2), min: hd(values), max: List.last(values)}}
          end

        IO.puts("SUMMARY #{scenario}/#{concurrency}/#{variant} " <> Jason.encode!(stats))
      end
    end
  end

  defp percentile([], _p), do: nil
  defp percentile(values, p), do: Enum.at(values, ceil(length(values) * p) - 1)
  defp now_ns, do: System.monotonic_time(:nanosecond)
  defp sha(source), do: Base.encode16(:crypto.hash(:sha256, source), case: :lower)

  defp format_source(source),
    do: (source |> Code.format_string!() |> IO.iodata_to_binary()) <> "\n"
end

FerricstoreBench.AsyncCleanup.run()
