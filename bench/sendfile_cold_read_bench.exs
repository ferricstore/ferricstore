defmodule SendfileColdReadBench do
  @moduledoc """
  TCP cold large GET benchmark for the sendfile path.

  Run with `mix run --no-start bench/sendfile_cold_read_bench.exs` so the script
  can install an isolated data dir before the application starts.
  """

  def run do
    data_dir =
      System.get_env("DATA_DIR") ||
        Path.join(System.tmp_dir!(), "ferricstore_sendfile_bench_#{System.os_time(:millisecond)}")

    transport = transport()

    tls_files =
      if transport == :tls do
        configure_tls_listener()
      end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    Mix.Task.run("app.start")

    host = {127, 0, 0, 1}
    port = env_int("PORT", default_port(transport))
    key_count = env_int("KEYS", 32)
    value_bytes = env_int("VALUE_BYTES", 256 * 1024)
    rounds = env_int("ROUNDS", 100)
    pipelines = env_list("PIPELINES", [1, 8, 32])
    prefix = System.get_env("PREFIX") || "bench:sendfile:"

    keys = Enum.map(1..key_count, &"#{prefix}#{&1}")
    values = Map.new(keys, fn key -> {key, value_for(key, value_bytes)} end)

    attach_stream_counter(transport)
    seed(host, port, values)

    IO.puts(
      "transport,pipeline,ops,seconds,ops_per_sec,mb_per_sec,stream_events,stream_mb,checksum_events,checksum_mb,checksum_ms"
    )

    for pipeline <- pipelines do
      reset_stream_counter()
      {ops, micros} = measure(host, port, keys, values, rounds, pipeline)
      seconds = micros / 1_000_000
      total_bytes = ops * value_bytes
      {events, stream_bytes, checksum_events, checksum_bytes, checksum_us} = stream_counter()

      IO.puts(
        Enum.join(
          [
            transport,
            pipeline,
            ops,
            Float.round(seconds, 4),
            Float.round(ops / seconds, 1),
            Float.round(total_bytes / seconds / 1_048_576, 1),
            events,
            Float.round(stream_bytes / 1_048_576, 1),
            checksum_events,
            Float.round(checksum_bytes / 1_048_576, 1),
            Float.round(checksum_us / 1_000, 1)
          ],
          ","
        )
      )
    end

    cleanup_tls_files(tls_files)
  end

  defp seed(host, port, values) do
    sock = connect(host, port)

    Enum.each(values, fn {key, value} ->
      :ok = sock_send(sock, command(["SET", key, value]))
      {{:simple, "OK"}, ""} = recv_one(sock, "")
    end)

    sock_close(sock)
  end

  defp measure(host, port, keys, values, rounds, pipeline) do
    sock = connect(host, port)
    key_count = length(keys)

    {micros, :ok} =
      :timer.tc(fn ->
        Enum.each(0..(rounds - 1), fn round ->
          batch =
            Enum.map(0..(pipeline - 1), fn offset ->
              Enum.at(keys, rem(round * pipeline + offset, key_count))
            end)

          :ok = sock_send(sock, IO.iodata_to_binary(Enum.map(batch, &command(["GET", &1]))))
          recv_and_verify(sock, batch, values, "")
        end)
      end)

    sock_close(sock)
    {rounds * pipeline, micros}
  end

  defp recv_and_verify(_sock, [], _values, _buf), do: :ok

  defp recv_and_verify(sock, [key | rest], values, buf) do
    expected = Map.fetch!(values, key)
    {value, next_buf} = recv_value(sock, buf)

    unless value == expected do
      raise "value mismatch for #{inspect(key)}: got #{byte_size(value)} bytes"
    end

    recv_and_verify(sock, rest, values, next_buf)
  end

  defp recv_value(sock, buf) do
    case parse_one(buf) do
      {:ok, value, rest} ->
        {value, rest}

      :more ->
        {:ok, data} = sock_recv(sock, 30_000)
        recv_value(sock, buf <> data)
    end
  end

  defp recv_one(sock, buf) do
    case parse_one(buf) do
      {:ok, value, rest} ->
        {value, rest}

      :more ->
        {:ok, data} = sock_recv(sock, 30_000)
        recv_one(sock, buf <> data)
    end
  end

  defp parse_one(<<"+", rest::binary>>) do
    case :binary.match(rest, "\r\n") do
      {idx, 2} ->
        {:ok, {:simple, binary_part(rest, 0, idx)},
         binary_part(rest, idx + 2, byte_size(rest) - idx - 2)}

      :nomatch ->
        :more
    end
  end

  defp parse_one(<<"$", rest::binary>>) do
    with {idx, 2} <- :binary.match(rest, "\r\n"),
         {len, ""} <- Integer.parse(binary_part(rest, 0, idx)) do
      start = idx + 2
      need = start + len + 2

      if byte_size(rest) >= need do
        value = binary_part(rest, start, len)
        suffix = binary_part(rest, start + len, 2)

        if suffix != "\r\n" do
          raise "invalid bulk trailer"
        end

        {:ok, value, binary_part(rest, need, byte_size(rest) - need)}
      else
        :more
      end
    else
      :nomatch -> :more
      _ -> raise "invalid bulk header"
    end
  end

  defp parse_one(<<"_", "\r\n", rest::binary>>), do: {:ok, nil, rest}

  defp parse_one(<<"-", rest::binary>>) do
    case :binary.match(rest, "\r\n") do
      {idx, 2} -> raise binary_part(rest, 0, idx)
      :nomatch -> :more
    end
  end

  defp parse_one(_buf), do: :more

  defp connect(host, port) do
    case transport() do
      :tcp -> connect_tcp(host, port)
      :tls -> connect_tls(port)
    end
  end

  defp connect_tcp(host, port) do
    {:ok, sock} = :gen_tcp.connect(host, port, [:binary, active: false, packet: :raw])
    {:tcp, sock}
  end

  defp connect_tls(port) do
    {:ok, sock} =
      :ssl.connect(
        ~c"127.0.0.1",
        port,
        [:binary, active: false, packet: :raw, verify: :verify_none],
        5_000
      )

    {:tls, sock}
  end

  defp sock_send({:tcp, sock}, data), do: :gen_tcp.send(sock, data)
  defp sock_send({:tls, sock}, data), do: :ssl.send(sock, data)

  defp sock_recv({:tcp, sock}, timeout_ms), do: :gen_tcp.recv(sock, 0, timeout_ms)
  defp sock_recv({:tls, sock}, timeout_ms), do: :ssl.recv(sock, 0, timeout_ms)

  defp sock_close({:tcp, sock}), do: :gen_tcp.close(sock)
  defp sock_close({:tls, sock}), do: :ssl.close(sock)

  defp command(parts) do
    [
      "*",
      Integer.to_string(length(parts)),
      "\r\n",
      Enum.map(parts, fn part ->
        ["$", Integer.to_string(byte_size(part)), "\r\n", part, "\r\n"]
      end)
    ]
  end

  defp value_for(key, size) do
    seed = :erlang.phash2(key, 251) + 1
    :binary.copy(<<seed>>, size)
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp env_list(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> value |> String.split(",", trim: true) |> Enum.map(&String.to_integer/1)
    end
  end

  defp attach_stream_counter(transport) do
    :persistent_term.put({__MODULE__, :counter}, :counters.new(5, []))

    :telemetry.attach(
      {__MODULE__, :stream_counter},
      telemetry_event(transport),
      &__MODULE__.handle_stream/4,
      nil
    )

    :telemetry.attach(
      {__MODULE__, :blob_checksum_counter},
      [:ferricstore, :server, :sendfile, :blob_checksum],
      &__MODULE__.handle_blob_checksum/4,
      nil
    )
  rescue
    ArgumentError -> :ok
  end

  def handle_stream(_event, %{bytes: bytes}, %{result: :ok}, _config) do
    counters = :persistent_term.get({__MODULE__, :counter})
    :counters.add(counters, 1, 1)
    :counters.add(counters, 2, bytes)
  end

  def handle_stream(_event, _measurements, _metadata, _config), do: :ok

  def handle_blob_checksum(
        _event,
        %{bytes: bytes, duration_us: duration_us},
        %{result: :ok},
        _config
      ) do
    counters = :persistent_term.get({__MODULE__, :counter})
    :counters.add(counters, 3, 1)
    :counters.add(counters, 4, bytes)
    :counters.add(counters, 5, duration_us)
  end

  def handle_blob_checksum(_event, _measurements, _metadata, _config), do: :ok

  defp reset_stream_counter do
    counters = :persistent_term.get({__MODULE__, :counter})
    :counters.put(counters, 1, 0)
    :counters.put(counters, 2, 0)
    :counters.put(counters, 3, 0)
    :counters.put(counters, 4, 0)
    :counters.put(counters, 5, 0)
  end

  defp stream_counter do
    counters = :persistent_term.get({__MODULE__, :counter})

    {
      :counters.get(counters, 1),
      :counters.get(counters, 2),
      :counters.get(counters, 3),
      :counters.get(counters, 4),
      :counters.get(counters, 5)
    }
  end

  defp telemetry_event(:tcp), do: [:ferricstore, :server, :sendfile]
  defp telemetry_event(:tls), do: [:ferricstore, :server, :file_stream]

  defp default_port(:tcp), do: FerricstoreServer.Listener.port()
  defp default_port(:tls), do: FerricstoreServer.TlsListener.port()

  defp transport do
    case String.downcase(System.get_env("TRANSPORT", "tcp")) do
      "tcp" -> :tcp
      "tls" -> :tls
      other -> raise "unsupported TRANSPORT=#{inspect(other)}; use tcp or tls"
    end
  end

  defp configure_tls_listener do
    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_sendfile_bench_tls_#{System.os_time(:millisecond)}"
      )

    File.mkdir_p!(dir)
    Code.require_file("apps/ferricstore/test/support/tls_cert_helper.ex")
    {cert_path, key_path} = apply(Ferricstore.Test.TlsCertHelper, :generate_self_signed, [dir])
    Application.put_env(:ferricstore, :tls_port, env_int("PORT", 0))
    Application.put_env(:ferricstore, :tls_cert_file, cert_path)
    Application.put_env(:ferricstore, :tls_key_file, key_path)
    {dir, cert_path, key_path}
  end

  defp cleanup_tls_files(nil), do: :ok

  defp cleanup_tls_files({dir, cert_path, key_path}) do
    File.rm(cert_path)
    File.rm(key_path)
    File.rmdir(dir)
  end
end

SendfileColdReadBench.run()
