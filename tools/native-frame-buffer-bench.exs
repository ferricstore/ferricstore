# Run with mise exec -- env ERL_FLAGS='+S 2:2' elixir tools/native-frame-buffer-bench.exs.
path = "apps/ferricstore_server/lib/ferricstore_server/native/connection/frame_buffer.ex"
Code.require_file(path)

{source, 0} =
  System.cmd("git", [
    "show",
    System.get_env("FRAME_BENCH_BASELINE_REF", "v0.11.18") <> ":" <> path
  ])

{:defmodule, metadata, [_name, body]} = Code.string_to_quoted!(source)
Code.compile_quoted({:defmodule, metadata, [FrameBaseline, body]})

for {size, fragments, rounds} <- [
      {1, 16_000, 10},
      {8192, 8, 500},
      {8192, 64, 200},
      {65536, 64, 50}
    ],
    implementation <- [FrameBaseline, FerricstoreServer.Native.Connection.FrameBuffer] do
  data = :binary.copy("x", size)
  limit = fragments * size + 1
  header = <<"FSNP", 1, 0, 1::32, 256::16, 1::64, limit::32>>
  :erlang.garbage_collect()
  {:reductions, before_reductions} = Process.info(self(), :reductions)
  {before_cpu, _} = :erlang.statistics(:runtime)

  {us, _} =
    :timer.tc(fn ->
      for _ <- 1..rounds do
        {:incomplete, initial} =
          implementation.append(implementation.new(), header, limit, limit + 24)

        buffer =
          Enum.reduce(1..fragments, initial, fn _, buffer ->
            {:incomplete, next} = implementation.append(buffer, data, limit, limit + 24)
            implementation.stats(next)
            next
          end)

        {:ready, final} = implementation.append(buffer, "!", limit, limit + 24)
        binary = implementation.materialize(final)
        if byte_size(binary) != limit + 24, do: raise("bad size")
      end
    end)

  {after_cpu, _} = :erlang.statistics(:runtime)
  {:reductions, after_reductions} = Process.info(self(), :reductions)

  IO.inspect(%{
    implementation: implementation,
    size: size,
    fragments: fragments,
    rounds: rounds,
    us: us,
    cpu_ms: after_cpu - before_cpu,
    reductions: after_reductions - before_reductions
  })
end
