# BENCH_VARIANT=baseline BENCH_TRIAL=1 ERL_FLAGS='+S 8:8' \
#   mise exec -- mix run --no-start bench/fragment_socket_perf.exs
# Fresh-VM ordinary PING traffic; compares only the inbound metadata charge.

defmodule FerricstoreBench.FragmentSocket do
  alias FerricstoreServer.Native.{Codec, Listener}
  @source "apps/ferricstore_server/lib/ferricstore_server/native/connection.ex"

  def run do
    variant = System.get_env("BENCH_VARIANT", "current")
    if variant not in ["baseline", "current"], do: raise("invalid variant")
    trial = System.get_env("BENCH_TRIAL", "1")
    current = File.read!(@source)

    source =
      if variant == "baseline",
        do:
          current
          |> String.replace(
            "FrameBuffer.retained_bytes(buffer_stats) + state.decoded_retained_bytes +\n      state.queued_request_bytes",
            "buffer_stats.buffered_bytes + state.decoded_retained_bytes + state.queued_request_bytes"
          )
          |> String.replace(
            "FrameBuffer.retained_frame_bytes(byte_size(body(frame)))",
            "FrameBuffer.frame_bytes(byte_size(body(frame)))"
          ),
        else: current

    if variant == "baseline" and source == current, do: raise("baseline transform failed")
    root = Path.join(System.tmp_dir!(), "ferricstore-fragment-socket-#{System.pid()}")
    if File.exists?(root), do: raise("fixture exists")
    File.mkdir_p!(root)
    Code.compiler_options(ignore_module_conflict: true)

    for {module, beam} <- Code.compile_string(source, @source) do
      file = Path.join(root, "#{module}.beam")
      File.write!(file, beam)
      :code.purge(module)
      {:module, ^module} = :code.load_binary(module, String.to_charlist(file), beam)
    end

    for {key, value} <- [
          data_dir: Path.join(root, "data"),
          node_name: nil,
          shard_count: 4,
          native_port: 0,
          health_port: 0,
          health_probe_port: 0
        ] do
      Application.put_env(:ferricstore, key, value)
    end

    Logger.configure(level: :error)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)

      results =
        for clients <- [1, 16] do
          parent = self()

          workers =
            for _ <- 1..clients do
              Task.async(fn ->
                {:ok, socket} =
                  :gen_tcp.connect(
                    {127, 0, 0, 1},
                    Listener.port(),
                    [:binary, active: false, nodelay: true],
                    5_000
                  )

                try do
                  warmup = loop(socket, System.monotonic_time(:microsecond) + 1_000_000, 0, 0, [])
                  send(parent, {:ready, self()})

                  receive do
                    {:run, deadline} -> loop(socket, deadline, warmup.count, 10, [])
                  end
                after
                  :gen_tcp.close(socket)
                end
              end)
            end

          for %{pid: pid} <- workers do
            receive do
              {:ready, ^pid} -> :ok
            after
              10_000 -> raise("warmup failed")
            end
          end

          started = System.monotonic_time(:microsecond)
          for %{pid: pid} <- workers, do: send(pid, {:run, started + 5_000_000})
          reports = Task.await_many(workers, 15_000)
          count = Enum.sum(Enum.map(reports, & &1.measured_count))
          elapsed = (Enum.max(Enum.map(reports, & &1.finished)) - started) / 1.0e6
          samples = Enum.flat_map(reports, & &1.samples) |> Enum.sort()

          result = %{
            clients: clients,
            operations: count,
            ops_per_second: count / elapsed,
            p50_us: q(samples, 0.5),
            p95_us: q(samples, 0.95),
            p99_us: q(samples, 0.99)
          }

          IO.puts(Jason.encode!(result))
          result
        end

      File.mkdir_p!("bench/results")

      prefix = System.get_env("BENCH_REPORT_PREFIX", "fragment-socket")

      File.write!(
        "bench/results/#{prefix}-#{variant}-#{trial}.json",
        Jason.encode!(
          %{
            variant: variant,
            trial: trial,
            results: results,
            source_sha256: Base.encode16(:crypto.hash(:sha256, source), case: :lower)
          },
          pretty: true
        )
      )
    after
      Application.stop(:ferricstore_server)
      Application.stop(:ferricstore)
      File.rm_rf!(root)
    end
  end

  defp loop(socket, deadline, id, every, samples),
    do: loop(socket, deadline, id, every, 0, samples)

  defp loop(socket, deadline, id, every, count, samples) do
    started = System.monotonic_time(:microsecond)

    if started >= deadline do
      %{count: id, measured_count: count, samples: samples, finished: started}
    else
      :ok = :gen_tcp.send(socket, Codec.encode_frame(0x0003, 0, id + 1, ""))

      {:ok,
       <<"FSNP", 0x81, _flags, 0::unsigned-32, 0x0003::unsigned-16, response_id::unsigned-64,
         bytes::unsigned-32>>} = :gen_tcp.recv(socket, 24, 5_000)

      true = response_id == id + 1
      {:ok, <<0::unsigned-16, _payload::binary>>} = :gen_tcp.recv(socket, bytes, 5_000)
      us = System.monotonic_time(:microsecond) - started
      samples = if every > 0 and rem(count, every) == 0, do: [us | samples], else: samples
      loop(socket, deadline, id + 1, every, count + 1, samples)
    end
  end

  defp q(samples, fraction), do: Enum.at(samples, ceil(length(samples) * fraction) - 1)
end

FerricstoreBench.FragmentSocket.run()
