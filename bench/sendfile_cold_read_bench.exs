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

    Application.put_env(:ferricstore, :data_dir, data_dir)
    Mix.Task.run("app.start")

    host = {127, 0, 0, 1}
    port = env_int("PORT", FerricstoreServer.Listener.port())
    key_count = env_int("KEYS", 32)
    value_bytes = env_int("VALUE_BYTES", 256 * 1024)
    rounds = env_int("ROUNDS", 100)
    pipelines = env_list("PIPELINES", [1, 8, 32])
    prefix = System.get_env("PREFIX") || "bench:sendfile:"

    keys = Enum.map(1..key_count, &"#{prefix}#{&1}")
    values = Map.new(keys, fn key -> {key, value_for(key, value_bytes)} end)

    attach_sendfile_counter()
    seed(host, port, values)

    IO.puts("pipeline,ops,seconds,ops_per_sec,mb_per_sec,sendfile_events,sendfile_mb")

    for pipeline <- pipelines do
      reset_sendfile_counter()
      {ops, micros} = measure(host, port, keys, values, rounds, pipeline)
      seconds = micros / 1_000_000
      total_bytes = ops * value_bytes
      {events, sendfile_bytes} = sendfile_counter()

      IO.puts(
        Enum.join(
          [
            pipeline,
            ops,
            Float.round(seconds, 4),
            Float.round(ops / seconds, 1),
            Float.round(total_bytes / seconds / 1_048_576, 1),
            events,
            Float.round(sendfile_bytes / 1_048_576, 1)
          ],
          ","
        )
      )
    end
  end

  defp seed(host, port, values) do
    sock = connect(host, port)

    Enum.each(values, fn {key, value} ->
      :ok = :gen_tcp.send(sock, command(["SET", key, value]))
      {{:simple, "OK"}, ""} = recv_one(sock, "")
    end)

    :gen_tcp.close(sock)
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

          :ok = :gen_tcp.send(sock, IO.iodata_to_binary(Enum.map(batch, &command(["GET", &1]))))
          recv_and_verify(sock, batch, values, "")
        end)
      end)

    :gen_tcp.close(sock)
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
        {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
        recv_value(sock, buf <> data)
    end
  end

  defp recv_one(sock, buf) do
    case parse_one(buf) do
      {:ok, value, rest} ->
        {value, rest}

      :more ->
        {:ok, data} = :gen_tcp.recv(sock, 0, 30_000)
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
    {:ok, sock} = :gen_tcp.connect(host, port, [:binary, active: false, packet: :raw])
    sock
  end

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

  defp attach_sendfile_counter do
    :persistent_term.put({__MODULE__, :counter}, :counters.new(2, []))

    :telemetry.attach(
      {__MODULE__, :sendfile_counter},
      [:ferricstore, :server, :sendfile],
      &__MODULE__.handle_sendfile/4,
      nil
    )
  rescue
    ArgumentError -> :ok
  end

  def handle_sendfile(_event, %{bytes: bytes}, %{result: :ok}, _config) do
    counters = :persistent_term.get({__MODULE__, :counter})
    :counters.add(counters, 1, 1)
    :counters.add(counters, 2, bytes)
  end

  def handle_sendfile(_event, _measurements, _metadata, _config), do: :ok

  defp reset_sendfile_counter do
    counters = :persistent_term.get({__MODULE__, :counter})
    :counters.put(counters, 1, 0)
    :counters.put(counters, 2, 0)
  end

  defp sendfile_counter do
    counters = :persistent_term.get({__MODULE__, :counter})
    {:counters.get(counters, 1), :counters.get(counters, 2)}
  end
end

SendfileColdReadBench.run()
