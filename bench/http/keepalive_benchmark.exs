defmodule FerricstoreHttp.BenchBackend do
  @moduledoc false

  @behaviour FerricstoreHttp.Backend

  @impl FerricstoreHttp.Backend
  def authenticate("benchmark", "benchmark", _opts), do: {:ok, :benchmark_session}
  def authenticate(_username, _password, _opts), do: {:error, :unauthenticated}

  @impl FerricstoreHttp.Backend
  def execute_batch(:benchmark_session, commands, _opts) do
    {:ok, Enum.map(commands, fn _command -> %{status: :ok, value: "PONG"} end)}
  end

  @impl FerricstoreHttp.Backend
  def ready?, do: true
end

defmodule FerricstoreHttp.HttpKeepaliveBenchmark do
  @moduledoc false

  @defaults [clients: 50, requests_per_client: 100, commands_per_request: 1, warmup: 5]
  @request_timeout 5_000

  def run(argv) do
    options = parse_options(argv)
    start_http_dependencies()
    {:ok, config} = benchmark_config(options)
    {:ok, server} = FerricstoreHttp.Server.start_link(config)
    port = FerricstoreHttp.Listener.port()
    body = request_body(options[:commands_per_request])
    request = http_request(body)
    workers = start_workers(options, port, request)

    await_ready(workers)
    started_at = System.monotonic_time()
    Enum.each(workers, &send(&1, :run))
    responses = await_results(workers, 0)
    elapsed_native = System.monotonic_time() - started_at
    elapsed_seconds = System.convert_time_unit(elapsed_native, :native, :microsecond) / 1_000_000
    total_requests = options[:clients] * options[:requests_per_client]

    :ok = Supervisor.stop(server)
    print_result(options, responses, total_requests, elapsed_seconds)
  end

  defp start_http_dependencies do
    Enum.each([:ssl, :cowboy, :jason, :msgpax], fn application ->
      {:ok, _started} = Application.ensure_all_started(application)
    end)
  end

  defp parse_options(argv) do
    switches = [
      clients: :integer,
      requests_per_client: :integer,
      commands_per_request: :integer,
      warmup: :integer
    ]

    {parsed, remaining, invalid} = OptionParser.parse(argv, strict: switches)

    if remaining != [] or invalid != [] do
      raise ArgumentError, "invalid benchmark arguments"
    end

    options = Keyword.merge(@defaults, parsed)

    Enum.each(options, fn {name, value} ->
      if not (is_integer(value) and value > 0) do
        raise ArgumentError, "--#{String.replace(to_string(name), "_", "-")} must be positive"
      end
    end)

    options
  end

  defp benchmark_config(options) do
    FerricstoreHttp.Config.new(
      enabled: true,
      port: 0,
      backend: FerricstoreHttp.BenchBackend,
      max_connections: options[:clients] + 16,
      max_in_flight_requests: options[:clients] + 16
    )
  end

  defp request_body(commands_per_request) do
    commands = List.duplicate(["PING"], commands_per_request)
    Jason.encode!(%{"commands" => commands})
  end

  defp http_request(body) do
    authorization = Base.encode64("benchmark:benchmark")

    "POST /v1/commands HTTP/1.1\r\n" <>
      "host: 127.0.0.1\r\n" <>
      "authorization: Basic #{authorization}\r\n" <>
      "content-type: application/json\r\n" <>
      "content-length: #{byte_size(body)}\r\n\r\n" <>
      body
  end

  defp start_workers(options, port, request) do
    parent = self()

    Enum.map(1..options[:clients], fn _client ->
      spawn_link(fn -> worker(parent, port, request, options) end)
    end)
  end

  defp worker(parent, port, request, options) do
    {:ok, socket} =
      :gen_tcp.connect(
        {127, 0, 0, 1},
        port,
        [:binary, active: false, nodelay: true],
        @request_timeout
      )

    :ok = repeat_requests(socket, request, options[:warmup])
    send(parent, {:ready, self()})

    receive do
      :run -> :ok
    after
      @request_timeout -> exit(:benchmark_start_timeout)
    end

    :ok = repeat_requests(socket, request, options[:requests_per_client])
    :ok = :gen_tcp.close(socket)
    send(parent, {:complete, self()})
  end

  defp repeat_requests(socket, request, count) do
    Enum.reduce_while(1..count, :ok, fn _request_number, :ok ->
      with :ok <- :gen_tcp.send(socket, request),
           {:ok, 200} <- receive_response(socket) do
        {:cont, :ok}
      else
        error -> {:halt, exit({:benchmark_request_failed, error})}
      end
    end)
  end

  defp receive_response(socket) do
    with {:ok, headers, remaining} <- receive_headers(socket, ""),
         [status_line | header_lines] <- String.split(headers, "\r\n", trim: true),
         [_, status, _reason] <- String.split(status_line, " ", parts: 3),
         {content_length, ""} <- Integer.parse(header_value(header_lines, "content-length")),
         {:ok, _body} <- receive_body(socket, remaining, content_length) do
      {:ok, String.to_integer(status)}
    end
  end

  defp receive_headers(socket, buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, 4} ->
        headers = binary_part(buffer, 0, index)
        remaining = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, headers, remaining}

      :nomatch ->
        with {:ok, bytes} <- :gen_tcp.recv(socket, 0, @request_timeout) do
          receive_headers(socket, buffer <> bytes)
        end
    end
  end

  defp receive_body(_socket, buffer, length) when byte_size(buffer) >= length,
    do: {:ok, binary_part(buffer, 0, length)}

  defp receive_body(socket, buffer, length) do
    with {:ok, bytes} <- :gen_tcp.recv(socket, length - byte_size(buffer), @request_timeout) do
      receive_body(socket, buffer <> bytes, length)
    end
  end

  defp header_value(lines, expected_name) do
    Enum.find_value(lines, fn line ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> matching_header_value(name, value, expected_name)
        _invalid -> nil
      end
    end)
  end

  defp matching_header_value(name, value, expected_name) do
    if String.downcase(name) == expected_name, do: String.trim(value)
  end

  defp await_ready(workers), do: await_messages(workers, :ready)

  defp await_messages([], _kind), do: :ok

  defp await_messages(workers, kind) do
    receive do
      {^kind, worker} -> await_messages(List.delete(workers, worker), kind)
    after
      @request_timeout * 4 -> exit({:benchmark_worker_timeout, kind, length(workers)})
    end
  end

  defp await_results([], completed), do: completed

  defp await_results(workers, completed) do
    receive do
      {:complete, worker} -> await_results(List.delete(workers, worker), completed + 1)
    after
      @request_timeout * 20 -> exit({:benchmark_completion_timeout, length(workers)})
    end
  end

  defp print_result(options, responses, total_requests, elapsed_seconds) do
    requests_per_second = total_requests / elapsed_seconds
    commands_per_second = requests_per_second * options[:commands_per_request]

    IO.puts("FerricStore HTTP/1.1 keep-alive benchmark")
    IO.puts("clients: #{options[:clients]}")
    IO.puts("completed clients: #{responses}")
    IO.puts("requests: #{total_requests}")
    IO.puts("commands/request: #{options[:commands_per_request]}")
    IO.puts("elapsed: #{Float.round(elapsed_seconds, 3)} s")
    IO.puts("throughput: #{round(requests_per_second)} requests/s")
    IO.puts("command rate: #{round(commands_per_second)} commands/s")
  end
end

FerricstoreHttp.HttpKeepaliveBenchmark.run(System.argv())
