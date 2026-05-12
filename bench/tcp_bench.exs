# bench/tcp_bench.exs
#
# End-to-end TCP benchmarks: connect, send RESP3 commands, receive responses.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/tcp_bench.exs
#
# Starts the full application with an ephemeral port and connects over TCP.

alias FerricstoreServer.Resp.Encoder

Logger.configure(level: :warning)

bench_warmup = System.get_env("BENCH_WARMUP", "2") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "5") |> String.to_integer()

# ---------------------------------------------------------------------------
# Application startup
# ---------------------------------------------------------------------------

bench_data_dir = System.tmp_dir!() <> "/ferricstore_tcp_bench_#{:rand.uniform(9_999_999)}"
File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)

{:ok, _} = Application.ensure_all_started(:ferricstore_server)

# Discover the ephemeral port Ranch assigned
port = FerricstoreServer.Listener.port()
IO.puts("=== TCP End-to-End Benchmarks ===")
IO.puts("Listening on port: #{port}")
IO.puts("Data dir: #{bench_data_dir}\n")

# ---------------------------------------------------------------------------
# TCP helpers
# ---------------------------------------------------------------------------

defmodule TcpBench do
  @moduledoc false

  @doc "Open a TCP connection and return the socket."
  def connect(port) do
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    sock
  end

  @doc "Send raw iodata and receive all available bytes (up to timeout)."
  def send_recv(sock, data, timeout \\ 5_000) do
    :ok = :gen_tcp.send(sock, data)
    recv_all(sock, <<>>, timeout)
  end

  @doc "Receive until the socket has no more data within timeout."
  def recv_all(sock, acc, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, chunk} -> recv_all(sock, acc <> chunk, timeout)
      {:error, :timeout} -> acc
      {:error, _} -> acc
    end
  end

  @doc """
  Receive exactly `expected_bytes` bytes from the socket.
  For SET pipelines, each response is `+OK\\r\\n` (5 bytes), so pass `n * 5`.
  """
  def recv_exact(sock, expected_bytes, timeout \\ 10_000) do
    recv_exact_loop(sock, expected_bytes, <<>>, timeout)
  end

  defp recv_exact_loop(_sock, 0, acc, _timeout), do: acc
  defp recv_exact_loop(_sock, remaining, acc, _timeout) when remaining < 0, do: acc

  defp recv_exact_loop(sock, remaining, acc, timeout) do
    case :gen_tcp.recv(sock, 0, timeout) do
      {:ok, chunk} ->
        new_remaining = remaining - byte_size(chunk)

        if new_remaining <= 0 do
          acc <> chunk
        else
          recv_exact_loop(sock, new_remaining, acc <> chunk, timeout)
        end

      {:error, _} ->
        acc
    end
  end

  @doc "Build a RESP3 array command, e.g. [\"SET\", \"k\", \"v\"] -> *3\\r\\n$3\\r\\nSET\\r\\n..."
  def build_command(args) do
    Encoder.encode(args) |> IO.iodata_to_binary()
  end
end

# ---------------------------------------------------------------------------
# Pre-build wire payloads
# ---------------------------------------------------------------------------

ping_cmd = TcpBench.build_command(["PING"])
set_cmd = TcpBench.build_command(["SET", "tcp_key", "tcp_value"])
get_cmd = TcpBench.build_command(["GET", "tcp_key"])

# 100 pipelined SET commands in a single buffer
pipeline_100 =
  Enum.map(1..100, fn i ->
    TcpBench.build_command(["SET", "pipe_key_#{i}", "pipe_val_#{i}"])
  end)
  |> IO.iodata_to_binary()

# Varying pipeline batch sizes
pipeline_1 = TcpBench.build_command(["SET", "pipe1_key", "pipe1_val"])

pipeline_10 =
  Enum.map(1..10, fn i ->
    TcpBench.build_command(["SET", "pipe10_key_#{i}", "pipe10_val_#{i}"])
  end)
  |> IO.iodata_to_binary()

pipeline_1000 =
  Enum.map(1..1000, fn i ->
    TcpBench.build_command(["SET", "pipe1000_key_#{i}", "pipe1000_val_#{i}"])
  end)
  |> IO.iodata_to_binary()

# ---------------------------------------------------------------------------
# Section 1: Single-command roundtrips
# ---------------------------------------------------------------------------

IO.puts("--- Single-command TCP roundtrips ---\n")

# Persistent connections reused across iterations to avoid OS port exhaustion.
ping_sock = TcpBench.connect(port)
set_get_sock = TcpBench.connect(port)

Benchee.run(
  %{
    "TCP PING roundtrip" => fn ->
      :ok = :gen_tcp.send(ping_sock, ping_cmd)
      {:ok, _data} = :gen_tcp.recv(ping_sock, 0, 5_000)
    end,

    "TCP SET + GET (2 roundtrips)" => fn ->
      :ok = :gen_tcp.send(set_get_sock, set_cmd)
      {:ok, _set_resp} = :gen_tcp.recv(set_get_sock, 0, 5_000)
      :ok = :gen_tcp.send(set_get_sock, get_cmd)
      {:ok, _get_resp} = :gen_tcp.recv(set_get_sock, 0, 5_000)
    end
  },
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/tcp_single.html", auto_open: false}
  ]
)

:gen_tcp.close(ping_sock)
:gen_tcp.close(set_get_sock)

# ---------------------------------------------------------------------------
# Section 2: Pipelining throughput
# ---------------------------------------------------------------------------

IO.puts("\n--- Pipeline throughput ---\n")

# Persistent connections reused across all pipeline iterations.
pipe1_sock = TcpBench.connect(port)
pipe10_sock = TcpBench.connect(port)
pipe100_sock = TcpBench.connect(port)
pipe1000_sock = TcpBench.connect(port)

Benchee.run(
  %{
    "Pipeline: 1 SET" => fn ->
      :ok = :gen_tcp.send(pipe1_sock, pipeline_1)
      {:ok, _resp} = :gen_tcp.recv(pipe1_sock, 0, 5_000)
    end,

    "Pipeline: 10 SETs" => fn ->
      :ok = :gen_tcp.send(pipe10_sock, pipeline_10)
      _resp = TcpBench.recv_exact(pipe10_sock, 50)
    end,

    "Pipeline: 100 SETs" => fn ->
      :ok = :gen_tcp.send(pipe100_sock, pipeline_100)
      _resp = TcpBench.recv_exact(pipe100_sock, 500)
    end,

    "Pipeline: 1000 SETs" => fn ->
      :ok = :gen_tcp.send(pipe1000_sock, pipeline_1000)
      _resp = TcpBench.recv_exact(pipe1000_sock, 5000)
    end
  },
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/tcp_pipeline.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Cleanup pipeline sockets
# ---------------------------------------------------------------------------

for sock <- [pipe1_sock, pipe10_sock, pipe100_sock, pipe1000_sock] do
  :gen_tcp.close(sock)
end

# ---------------------------------------------------------------------------
# Section 3: Concurrent clients (group-commit throughput)
#
# N concurrent clients each fire one SET simultaneously.  All writes land in
# the same 1ms group-commit window → shared fsync → total ops/s >> single-
# client sequential.
# ---------------------------------------------------------------------------

IO.puts("\n--- Concurrent writers (group commit) ---\n")
IO.puts("Each iteration spawns N tasks that each send one SET and wait for +OK.\n")

defmodule ConcurrentBench do
  @moduledoc false

  @doc "Fire `n` concurrent SETs with unique keys, each on its own socket, return when all done."
  def concurrent_sets(port, n) do
    tasks =
      for i <- 1..n do
        Task.async(fn ->
          cmd = TcpBench.build_command(["SET", "ckey_#{i}", "cval"])
          sock = TcpBench.connect(port)
          :ok = :gen_tcp.send(sock, cmd)
          {:ok, _resp} = :gen_tcp.recv(sock, 0, 5_000)
          :gen_tcp.close(sock)
        end)
      end

    Task.await_many(tasks, 10_000)
    :ok
  end
end

Benchee.run(
  %{
    "Concurrent  10 writers" => fn -> ConcurrentBench.concurrent_sets(port, 10) end,
    "Concurrent  50 writers" => fn -> ConcurrentBench.concurrent_sets(port, 50) end,
    "Concurrent 100 writers" => fn -> ConcurrentBench.concurrent_sets(port, 100) end,
    "Concurrent 200 writers" => fn -> ConcurrentBench.concurrent_sets(port, 200) end
  },
  time: bench_time,
  warmup: bench_warmup,
  memory_time: 1,
  formatters: [
    Benchee.Formatters.Console,
    {Benchee.Formatters.HTML, file: "bench/output/tcp_concurrent.html", auto_open: false}
  ]
)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
File.rm_rf!(bench_data_dir)
