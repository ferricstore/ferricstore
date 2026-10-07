# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/embedded_dispatch_perf.exs
# Real hot embedded operations; paired adapter/Instance dispatch in one VM.

defmodule FerricstoreBench.EmbeddedDispatch do
  @source "apps/ferricstore/lib/ferricstore/impl.ex"
  @output "bench/results/embedded-dispatch-perf.json"

  def run do
    source =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    candidate =
      String.replace(
        source,
        "  defp build_store(ctx) do",
        "  defp build_store(%FerricStore.Instance{} = ctx), do: ctx\n\n  defp build_store(ctx) do"
      )

    variants =
      for {name, code} <- [baseline: source, instance: candidate] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          code
          |> String.replace("defmodule FerricStore.Impl do", "defmodule #{inspect(module)} do")
          |> Code.compile_string()

        {name, module}
      end

    root = Path.join(System.tmp_dir!(), "ferricstore-embedded-dispatch-#{System.pid()}")
    if File.exists?(root), do: raise("fixture exists")
    Application.put_env(:ferricstore, :data_dir, root)
    Application.put_env(:ferricstore, :shard_count, 4)
    Application.put_env(:ferricstore, :node_name, nil)
    Logger.configure(level: :error)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore)
      ctx = FerricStore.Instance.get(:default)
      :ok = FerricStore.Impl.set(ctx, "dispatch:string", "value")
      {:ok, 1} = FerricStore.Impl.hset(ctx, "dispatch:hash", %{"field" => "value"})
      {:ok, 1} = FerricStore.Impl.sadd(ctx, "dispatch:set", ["value"])
      {:ok, 1} = FerricStore.Impl.rpush(ctx, "dispatch:list", ["value"])

      scenarios = [
        {:get_hit, :get, ["dispatch:string"], {:ok, "value"}},
        {:get_miss, :get, ["dispatch:missing"], {:ok, nil}},
        {:hget, :hget, ["dispatch:hash", "field"], {:ok, "value"}},
        {:scard, :scard, ["dispatch:set"], {:ok, 1}},
        {:llen, :llen, ["dispatch:list"], {:ok, 1}}
      ]

      results =
        for {scenario, function, args, expected} <- scenarios,
            trial <- 1..5,
            {variant, module} <-
              if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
          operation = fn -> ^expected = apply(module, function, [ctx | args]) end
          measure(operation, 100)

          result =
            measure(operation, 1_000)
            |> Map.merge(%{scenario: scenario, variant: variant, trial: trial})

          IO.puts(Jason.encode!(result))
          result
        end

      File.mkdir_p!(Path.dirname(@output))

      File.write!(
        @output,
        Jason.encode!(
          %{
            baseline_source: source,
            candidate_source: candidate,
            schedulers: :erlang.system_info(:schedulers_online),
            results: results
          },
          pretty: true
        )
      )
    after
      Application.stop(:ferricstore)
      File.rm_rf!(root)
    end
  end

  defp measure(operation, ms) do
    Task.async(fn ->
      :erlang.garbage_collect()
      {:reductions, before} = Process.info(self(), :reductions)
      started = System.monotonic_time(:nanosecond)
      {count, samples, finished} = loop(operation, started + ms * 1_000_000, 0, [])
      {:reductions, after_count} = Process.info(self(), :reductions)
      seconds = (finished - started) / 1.0e9
      samples = Enum.sort(samples)

      %{
        operations: count,
        ops_per_second: count / seconds,
        reductions_per_op: (after_count - before) / count,
        p50_us: percentile(samples, 0.5),
        p95_us: percentile(samples, 0.95),
        p99_us: percentile(samples, 0.99)
      }
    end)
    |> Task.await(ms + 30_000)
  end

  defp loop(operation, deadline, count, samples) do
    started = System.monotonic_time(:nanosecond)

    if started >= deadline do
      {count, samples, started}
    else
      operation.()
      elapsed = (System.monotonic_time(:nanosecond) - started) / 1_000
      samples = if rem(count, 100) == 0, do: [elapsed | samples], else: samples
      loop(operation, deadline, count + 1, samples)
    end
  end

  defp percentile(samples, q), do: Enum.at(samples, ceil(length(samples) * q) - 1)
end

FerricstoreBench.EmbeddedDispatch.run()
