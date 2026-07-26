# Measures the removed all-in-BEAM compaction-retention accumulator against the
# production hybrid collector. The reference path exists only as benchmark
# evidence for the bounded-memory storage change.

defmodule Ferricstore.Bench.ApplyProjectionRetention do
  @moduledoc false

  alias Ferricstore.Raft.WARaftStorage.ApplyProjectionRetention

  @default_large_entries 25_000
  @default_large_value_bytes 4_096
  @default_sample_every 256

  def run do
    large_entries = int_env("BENCH_RETENTION_ENTRIES", @default_large_entries, 4_097)
    large_value_bytes = int_env("BENCH_RETENTION_VALUE_BYTES", @default_large_value_bytes, 64)
    sample_every = int_env("BENCH_RETENTION_SAMPLE_EVERY", @default_sample_every, 1)

    IO.puts("apply-projection retention benchmark")
    IO.puts("memory includes the worker heap plus referenced off-heap binaries")

    run_shape("small/in-memory", 2_048, 1_024, sample_every)
    run_shape("large/spilled", large_entries, large_value_bytes, sample_every)
  end

  defp run_shape(label, entry_count, value_bytes, sample_every) do
    reference =
      measure(
        fn sample ->
          entries =
            Enum.reduce(1..entry_count, [], fn number, acc ->
              next = [candidate(number, value_bytes) | acc]
              sample.(number)
              next
            end)

          true = length(entries) == entry_count
          %{mode: :memory, disk_bytes: 0}
        end,
        sample_every
      )

    production =
      measure(
        fn sample ->
          root = temp_root(label)

          try do
            {:ok, retention} = ApplyProjectionRetention.open(root, entry_count + 1)

            retention =
              Enum.reduce(1..entry_count, retention, fn number, current ->
                {:ok, next} =
                  ApplyProjectionRetention.put(current, candidate(number, value_bytes))

                sample.(number)
                next
              end)

            {:ok, retention} = ApplyProjectionRetention.finish(retention)
            mode = ApplyProjectionRetention.mode(retention)

            0 =
              if mode == :disk,
                do: ApplyProjectionRetention.memory_entry_count(retention),
                else: 0

            result = %{mode: mode, disk_bytes: directory_bytes(root)}
            :ok = ApplyProjectionRetention.cleanup(retention)
            result
          after
            File.rm_rf!(root)
          end
        end,
        sample_every
      )

    reduction = percentage(reference.peak_bytes - production.peak_bytes, reference.peak_bytes)
    runtime_ratio = production.elapsed_us / max(reference.elapsed_us, 1)

    IO.puts("")
    IO.puts("#{label}: entries=#{entry_count} value_bytes=#{value_bytes}")

    IO.puts(
      "  reference peak=#{format_bytes(reference.peak_bytes)} elapsed=#{format_ms(reference.elapsed_us)}"
    )

    IO.puts(
      "  production peak=#{format_bytes(production.peak_bytes)} elapsed=#{format_ms(production.elapsed_us)} " <>
        "mode=#{production.result.mode} spill=#{format_bytes(production.result.disk_bytes)}"
    )

    IO.puts(
      "  retained-memory reduction=#{Float.round(reduction, 2)}% runtime_ratio=#{Float.round(runtime_ratio, 2)}x"
    )
  end

  defp measure(fun, sample_every) do
    parent = self()

    pid =
      spawn(fn ->
        :erlang.garbage_collect()
        initial = footprint_bytes()
        Process.put(:retention_bench_peak, initial)

        sample = fn number ->
          if rem(number, sample_every) == 0 do
            :erlang.garbage_collect()
            peak = max(Process.get(:retention_bench_peak, 0), footprint_bytes())
            Process.put(:retention_bench_peak, peak)
          end
        end

        started = System.monotonic_time(:microsecond)
        result = fun.(sample)
        elapsed_us = System.monotonic_time(:microsecond) - started
        :erlang.garbage_collect()
        peak = max(Process.get(:retention_bench_peak, 0), footprint_bytes())
        send(parent, {:retention_bench_result, self(), result, elapsed_us, peak})
      end)

    receive do
      {:retention_bench_result, ^pid, result, elapsed_us, peak_bytes} ->
        %{result: result, elapsed_us: elapsed_us, peak_bytes: peak_bytes}
    after
      300_000 ->
        Process.exit(pid, :kill)
        raise "apply-projection retention benchmark timed out"
    end
  end

  defp candidate(number, value_bytes) do
    key = "retained:#{number |> Integer.to_string() |> String.pad_leading(8, "0")}"
    suffix = :binary.copy(<<rem(number, 251)>>, value_bytes - 8)
    value = <<number::unsigned-big-64, suffix::binary>>
    {{number, key}, {key, value, 0}}
  end

  defp footprint_bytes do
    {:memory, process_bytes} = Process.info(self(), :memory)

    binary_bytes =
      case Process.info(self(), :binary) do
        {:binary, references} ->
          references
          |> Map.new(fn {reference, bytes, _ref_count} -> {reference, bytes} end)
          |> Map.values()
          |> Enum.sum()

        _unavailable ->
          0
      end

    process_bytes + binary_bytes
  end

  defp directory_bytes(root) do
    root
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce(0, fn path, total ->
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} -> total + size
        _other -> total
      end
    end)
  end

  defp temp_root(label) do
    safe_label = String.replace(label, ~r/[^a-z0-9]+/i, "-")

    Path.join(
      System.tmp_dir!(),
      "apply-projection-retention-bench-#{safe_label}-#{System.unique_integer([:positive])}"
    )
  end

  defp int_env(name, default, minimum) do
    case System.get_env(name) do
      nil -> default
      value -> max(String.to_integer(value), minimum)
    end
  end

  defp percentage(_difference, 0), do: 0.0
  defp percentage(difference, baseline), do: difference * 100 / baseline

  defp format_bytes(bytes) when bytes >= 1_048_576,
    do: "#{Float.round(bytes / 1_048_576, 2)} MiB"

  defp format_bytes(bytes) when bytes >= 1_024,
    do: "#{Float.round(bytes / 1_024, 2)} KiB"

  defp format_bytes(bytes), do: "#{bytes} B"
  defp format_ms(microseconds), do: "#{Float.round(microseconds / 1_000, 2)} ms"
end

Ferricstore.Bench.ApplyProjectionRetention.run()
