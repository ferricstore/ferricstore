# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/fragment_memory_perf.exs
# Compares fragment-counted payload accounting with constant-time metadata charge.
defmodule FerricstoreBench.FragmentMemory do
  alias FerricstoreServer.Native.Codec
  @source "apps/ferricstore_server/lib/ferricstore_server/native/connection/frame_buffer.ex"

  def run do
    baseline =
      Jason.decode!(File.read!("bench/results/frame-accounting-perf.json"))["candidate_source"]

    current = File.read!(@source)

    variants =
      for {name, source} <- [payload_only: baseline, metadata: current] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          source
          |> String.replace(
            "defmodule FerricstoreServer.Native.Connection.FrameBuffer do",
            "defmodule #{inspect(module)} do"
          )
          |> Code.compile_string()

        {name, module}
      end

    frame = Codec.encode_frame(0x0003, 0, 1, :binary.copy("x", 16_384))

    results =
      for chunk_size <- [1, 64, 4096, byte_size(frame)],
          trial <- 1..5,
          {variant, module} <- if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
        chunks = chunks(frame, chunk_size, [])

        count =
          cond do
            chunk_size == 1 -> 50
            chunk_size == 64 -> 2_000
            chunk_size == 4096 -> 50_000
            true -> 100_000
          end

        charge =
          if variant == :metadata,
            do: &module.retained_bytes/1,
            else: &Map.fetch!(&1, :buffered_bytes)

        result =
          Task.async(fn ->
            :erlang.garbage_collect()
            {:reductions, before} = Process.info(self(), :reductions)
            started = System.monotonic_time(:nanosecond)

            Enum.each(1..count, fn _ ->
              buffer =
                Enum.reduce(chunks, module.new(), fn chunk, buffer ->
                  {_, next} = module.append(buffer, chunk, 16_384, 16_408)
                  stats = module.stats(next)
                  true = charge.(stats) >= stats.buffered_bytes
                  next
                end)

              ^frame = module.materialize(buffer)
            end)

            elapsed_us = (System.monotonic_time(:nanosecond) - started) / 1_000
            {:reductions, after_count} = Process.info(self(), :reductions)

            buffer =
              Enum.reduce(chunks, module.new(), fn chunk, b ->
                {_, n} = module.append(b, chunk, 16_384, 16_408)
                n
              end)

            stats = module.stats(buffer)

            %{
              variant: variant,
              trial: trial,
              chunk_bytes: chunk_size,
              fragments: length(chunks),
              repetitions: count,
              us_per_frame: elapsed_us / count,
              reductions_per_frame: (after_count - before) / count,
              charge_bytes: charge.(stats)
            }
          end)
          |> Task.await(30_000)

        IO.puts(Jason.encode!(result))
        result
      end

    File.mkdir_p!("bench/results")

    File.write!(
      System.get_env("BENCH_OUTPUT", "bench/results/fragment-memory-perf.json"),
      Jason.encode!(%{baseline_source: baseline, current_source: current, results: results},
        pretty: true
      )
    )
  end

  defp chunks(<<>>, _, acc), do: Enum.reverse(acc)

  defp chunks(binary, n, acc) do
    size = min(byte_size(binary), n)
    <<chunk::binary-size(^size), rest::binary>> = binary
    chunks(rest, n, [chunk | acc])
  end
end

FerricstoreBench.FragmentMemory.run()
