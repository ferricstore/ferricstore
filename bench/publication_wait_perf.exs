# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/publication_wait_perf.exs
defmodule FerricstoreBench.PublicationWait do
  def run do
    path = "apps/ferricstore/lib/ferricstore/store/publication_epoch.ex"
    current = File.read!(path)

    baseline =
      current
      |> String.replace("  @read_spin_retries 8\n", "")
      |> String.replace(
        "  defp read_stable(ref, descriptors, fun), do: read_stable(ref, descriptors, fun, 0)\n\n  defp read_stable(ref, descriptors, fun, retries) do",
        "  defp read_stable(ref, descriptors, fun) do"
      )
      |> String.replace(
        "      pause_publication_read(retries)\n      read_stable(ref, descriptors, fun, retries + 1)",
        "      :erlang.yield()\n      read_stable(ref, descriptors, fun)"
      )
      |> String.replace(
        "        pause_publication_read(retries)\n        read_stable(ref, descriptors, fun, retries + 1)",
        "        read_stable(ref, descriptors, fun)"
      )
      |> String.replace(
        "  defp pause_publication_read(retries) when retries < @read_spin_retries, do: :erlang.yield()\n  defp pause_publication_read(_retries), do: Process.sleep(1)\n\n",
        ""
      )

    baseline =
      baseline
      |> String.replace(
        "  defp acquire_writer_latch(latch_table, latch_key) do\n    acquire_writer_latch(latch_table, latch_key, 0)\n  end\n\n  defp acquire_writer_latch(latch_table, latch_key, retries) do",
        "  defp acquire_writer_latch(latch_table, latch_key) do"
      )
      |> String.replace("pause_publication_read(retries)", ":erlang.yield()")
      |> String.replace(
        "acquire_writer_latch(latch_table, latch_key, retries + 1)",
        "acquire_writer_latch(latch_table, latch_key)"
      )

    true = baseline != current and not String.contains?(baseline, "pause_publication_read")

    variants =
      for {variant, source} <- [baseline: baseline, bounded: current] do
        module = Module.concat(__MODULE__, variant |> to_string() |> Macro.camelize())

        [{^module, _}] =
          Code.compile_string(
            String.replace(
              source,
              "defmodule Ferricstore.Store.PublicationEpoch do",
              "defmodule #{inspect(module)} do"
            )
          )

        {variant, module}
      end

    rows =
      for readers <- [1, 16],
          trial <- 1..3,
          {variant, module} <- if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
        table = :ets.new(:pub_wait_bench, [:set, :public])
        ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {table}}
        token = module.begin_write(ctx, 0)
        parent = self()

        tasks =
          for _ <- 1..readers do
            Task.async(fn ->
              send(parent, {:ready, self()})
              module.read(ctx, [0], fn -> :stable end)
            end)
          end

        for %{pid: pid} <- tasks do
          receive do
            {:ready, ^pid} -> :ok
          end
        end

        before = for %{pid: pid} <- tasks, do: elem(Process.info(pid, :reductions), 1)
        started = System.monotonic_time(:microsecond)
        Process.sleep(100)
        waited_us = System.monotonic_time(:microsecond) - started
        after_count = for %{pid: pid} <- tasks, do: elem(Process.info(pid, :reductions), 1)
        release = System.monotonic_time(:microsecond)
        :ok = module.end_write(token)
        true = Enum.all?(Task.await_many(tasks), &(&1 == :stable))
        wake_us = System.monotonic_time(:microsecond) - release
        reductions = Enum.sum(Enum.zip_with(before, after_count, &(&2 - &1)))

        result = %{
          variant: variant,
          readers: readers,
          trial: trial,
          waited_us: waited_us,
          reductions: reductions,
          reductions_per_reader_per_ms: reductions / readers / (waited_us / 1_000),
          wake_us: wake_us
        }

        :ets.delete(table)
        IO.puts(Jason.encode!(result))
        result
      end

    writers =
      for trial <- 1..3, {variant, module} <- variants do
        table = :ets.new(:pub_queued_writer_bench, [:set, :public])
        ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {table}}
        token = module.begin_write(ctx, 0)
        parent = self()

        task =
          Task.async(fn ->
            send(parent, :writer_ready)
            next = module.begin_write(ctx, 0)
            module.end_write(next)
          end)

        receive do
          :writer_ready -> :ok
        end

        before = elem(Process.info(task.pid, :reductions), 1)
        started = System.monotonic_time(:microsecond)
        Process.sleep(100)
        waited_us = System.monotonic_time(:microsecond) - started
        reductions = elem(Process.info(task.pid, :reductions), 1) - before
        release = System.monotonic_time(:microsecond)
        module.end_write(token)
        :ok = Task.await(task)
        wake_us = System.monotonic_time(:microsecond) - release
        :ets.delete(table)

        %{
          variant: variant,
          trial: trial,
          reductions: reductions,
          waited_us: waited_us,
          wake_us: wake_us
        }
      end

    controls =
      for trial <- 1..3, {variant, module} <- variants do
        table = :ets.new(:pub_uncontended_bench, [:set, :public])
        ctx = %{publication_epoch: :atomics.new(1, signed: false), latch_refs: {table}}
        repetitions = 200_000
        started = System.monotonic_time(:nanosecond)
        for _ <- 1..repetitions, do: :stable = module.read(ctx, [0], fn -> :stable end)
        us = (System.monotonic_time(:nanosecond) - started) / 1_000 / repetitions
        :ets.delete(table)
        %{variant: variant, trial: trial, us_per_read: us}
      end

    File.write!(
      "bench/results/publication-wait-perf.json",
      Jason.encode!(
        %{
          baseline_source: baseline,
          current_source: current,
          results: rows,
          uncontended: controls,
          queued_writers: writers
        },
        pretty: true
      )
    )

    IO.inspect(controls, label: "UNCONTENDED")
  end
end

FerricstoreBench.PublicationWait.run()
