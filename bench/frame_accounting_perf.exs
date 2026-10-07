# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/frame_accounting_perf.exs
# Mirrors append + stats on every receive, then materializes the complete frame.

defmodule FerricstoreBench.FrameAccounting do
  alias FerricstoreServer.Native.Codec
  @source "apps/ferricstore_server/lib/ferricstore_server/native/connection/frame_buffer.ex"
  @output "bench/results/frame-accounting-perf.json"

  def run do
    source =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    candidate =
      source
      |> String.replace(
        "defstruct chunks_rev: [],",
        "defstruct chunks_rev: [],\n             chunk_count: 0,"
      )
      |> String.replace(
        "chunks_rev: [binary()],",
        "chunks_rev: [binary()],\n          chunk_count: non_neg_integer(),"
      )
      |> String.replace(
        "chunk_count: length(buffer.chunks_rev)",
        "chunk_count: buffer.chunk_count"
      )
      |> String.replace(
        "| chunks_rev: [data | buffer.chunks_rev],",
        "| chunks_rev: [data | buffer.chunks_rev],\n        chunk_count: buffer.chunk_count + 1,"
      )

    variants =
      for {name, code} <- [baseline: source, counted: candidate] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          code
          |> String.replace(
            "defmodule FerricstoreServer.Native.Connection.FrameBuffer do",
            "defmodule #{inspect(module)} do"
          )
          |> Code.compile_string()

        {name, module}
      end

    frame = Codec.encode_frame(0x0001, 0, 1, :binary.copy("v", 16_384))

    results =
      for chunk_size <- [1, 64, 4_096, byte_size(frame)],
          trial <- 1..5,
          {variant, module} <- if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
        chunks = chunks(frame, chunk_size, [])
        repetitions = if chunk_size == 1, do: 1, else: 100

        result =
          Task.async(fn ->
            :erlang.garbage_collect()
            {:reductions, before} = Process.info(self(), :reductions)

            {us, _} =
              :timer.tc(fn ->
                for _ <- 1..repetitions do
                  buffer =
                    Enum.reduce(chunks, module.new(), fn chunk, buffer ->
                      {status, next} = module.append(buffer, chunk, 16_384, 16_408)
                      true = status in [:ready, :incomplete]
                      stats = module.stats(next)
                      true = stats.buffered_bytes <= 16_408
                      next
                    end)

                  true = module.stats(buffer).complete?
                  ^frame = module.materialize(buffer)
                end
              end)

            {:reductions, after_count} = Process.info(self(), :reductions)

            %{
              variant: variant,
              trial: trial,
              chunk_size: chunk_size,
              fragments: length(chunks),
              us_per_frame: us / repetitions,
              reductions_per_frame: (after_count - before) / repetitions
            }
          end)
          |> Task.await(30_000)

        IO.puts(Jason.encode!(result))
        result
      end

    File.mkdir_p!(Path.dirname(@output))

    File.write!(
      @output,
      Jason.encode!(%{baseline_source: source, candidate_source: candidate, results: results},
        pretty: true
      )
    )
  end

  defp chunks(<<>>, _size, acc), do: Enum.reverse(acc)

  defp chunks(data, size, acc) do
    take = min(byte_size(data), size)
    <<chunk::binary-size(^take), rest::binary>> = data
    chunks(rest, size, [chunk | acc])
  end
end

FerricstoreBench.FrameAccounting.run()
