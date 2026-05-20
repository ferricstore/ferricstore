defmodule Ferricstore.Bench.RespRouterLoad do
  @moduledoc false

  @ok_response "+OK\r\n"

  def payload(size) when is_integer(size) and size >= 0 do
    :binary.copy("x", size)
  end

  def work_ranges(total, concurrency)
      when is_integer(total) and total >= 0 and is_integer(concurrency) and concurrency > 0 do
    workers = min(total, concurrency)

    if workers == 0 do
      []
    else
      base = div(total, workers)
      extra = rem(total, workers)

      {ranges, _next} =
        Enum.map_reduce(0..(workers - 1), 0, fn idx, start ->
          count = base + if(idx < extra, do: 1, else: 0)
          {{start, count}, start + count}
        end)

      ranges
    end
  end

  def pipeline(mode, start_index, count, payload, key_count)
      when mode in [:set, :get, :mixed] and is_integer(count) and count >= 0 do
    key_count = max(key_count, 1)

    {parts, expected_bytes} =
      Enum.reduce(0..(count - 1)//1, {[], 0}, fn offset, {parts, bytes} ->
        op_index = start_index + offset
        command_mode = command_mode(mode, op_index)
        key = key_for(op_index, key_count)

        args =
          case command_mode do
            :set -> ["SET", key, payload]
            :get -> ["GET", key]
          end

        {[encode_command(args) | parts], bytes + response_bytes(command_mode, payload)}
      end)

    {IO.iodata_to_binary(Enum.reverse(parts)), expected_bytes, count}
  end

  def response_bytes(:set, _payload), do: byte_size(@ok_response)

  def response_bytes(:get, payload) when is_binary(payload) do
    size = byte_size(payload)
    1 + byte_size(Integer.to_string(size)) + 2 + size + 2
  end

  def run(port, opts) when is_integer(port) do
    mode = Keyword.fetch!(opts, :mode)
    total = Keyword.fetch!(opts, :total)
    concurrency = Keyword.fetch!(opts, :concurrency)
    pipeline_size = Keyword.fetch!(opts, :pipeline)
    payload = Keyword.fetch!(opts, :payload)
    key_count = Keyword.fetch!(opts, :key_count)

    ranges = work_ranges(total, concurrency)
    started = System.monotonic_time(:microsecond)

    results =
      ranges
      |> Task.async_stream(
        fn {start, count} ->
          run_worker(port, mode, start, count, pipeline_size, payload, key_count)
        end,
        max_concurrency: max(length(ranges), 1),
        timeout: :infinity,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    elapsed_us = max(System.monotonic_time(:microsecond) - started, 1)
    summarize(results, elapsed_us, payload)
  end

  def preload(port, key_count, payload, opts \\ []) when key_count >= 0 do
    concurrency = Keyword.get(opts, :concurrency, 16)
    pipeline_size = Keyword.get(opts, :pipeline, 100)

    run(port,
      mode: :set,
      total: key_count,
      concurrency: concurrency,
      pipeline: pipeline_size,
      payload: payload,
      key_count: key_count
    )
  end

  defp run_worker(port, mode, start, count, pipeline_size, payload, key_count) do
    {:ok, socket} =
      :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 5_000)

    try do
      run_worker_loop(socket, mode, start, count, pipeline_size, payload, key_count, %{
        ops: 0,
        short_reads: 0,
        latencies_us: []
      })
    after
      :gen_tcp.close(socket)
    end
  end

  defp run_worker_loop(_socket, _mode, _index, 0, _pipeline_size, _payload, _key_count, acc),
    do: acc

  defp run_worker_loop(socket, mode, index, remaining, pipeline_size, payload, key_count, acc) do
    count = min(remaining, pipeline_size)
    {wire, expected_bytes, command_count} = pipeline(mode, index, count, payload, key_count)

    started = System.monotonic_time(:microsecond)
    :ok = :gen_tcp.send(socket, wire)
    response = recv_exact(socket, expected_bytes, 30_000)
    elapsed_us = System.monotonic_time(:microsecond) - started

    short_read? = byte_size(response) < expected_bytes

    acc = %{
      acc
      | ops: acc.ops + command_count,
        short_reads: acc.short_reads + if(short_read?, do: 1, else: 0),
        latencies_us: [elapsed_us | acc.latencies_us]
    }

    run_worker_loop(
      socket,
      mode,
      index + command_count,
      remaining - command_count,
      pipeline_size,
      payload,
      key_count,
      acc
    )
  end

  defp summarize(results, elapsed_us, payload) do
    ops = Enum.reduce(results, 0, &(&1.ops + &2))
    short_reads = Enum.reduce(results, 0, &(&1.short_reads + &2))
    latencies = results |> Enum.flat_map(& &1.latencies_us) |> Enum.sort()
    seconds = elapsed_us / 1_000_000
    ops_per_sec = ops / seconds
    mb_per_sec = ops * byte_size(payload) / seconds / 1_048_576

    %{
      ops: ops,
      elapsed_us: elapsed_us,
      ops_per_sec: ops_per_sec,
      mb_per_sec: mb_per_sec,
      short_reads: short_reads,
      batches: length(latencies),
      batch_p50_us: percentile(latencies, 50),
      batch_p95_us: percentile(latencies, 95),
      batch_p99_us: percentile(latencies, 99),
      batch_p999_us: percentile(latencies, 99.9)
    }
  end

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    index =
      ((length(sorted) - 1) * p / 100)
      |> Float.ceil()
      |> trunc()

    Enum.at(sorted, min(index, length(sorted) - 1))
  end

  defp command_mode(:mixed, op_index) do
    if rem(op_index, 2) == 0, do: :get, else: :set
  end

  defp command_mode(mode, _op_index), do: mode

  defp key_for(op_index, key_count) do
    "bench:#{rem(op_index, key_count) + 1}"
  end

  defp encode_command(args) do
    ["*", Integer.to_string(length(args)), "\r\n", Enum.map(args, &encode_bulk/1)]
  end

  defp encode_bulk(value) when is_binary(value) do
    ["$", Integer.to_string(byte_size(value)), "\r\n", value, "\r\n"]
  end

  defp recv_exact(socket, expected_bytes, timeout) do
    recv_exact(socket, expected_bytes, <<>>, timeout)
  end

  defp recv_exact(_socket, remaining, acc, _timeout) when remaining <= 0, do: acc

  defp recv_exact(socket, remaining, acc, timeout) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, chunk} -> recv_exact(socket, remaining - byte_size(chunk), acc <> chunk, timeout)
      {:error, _reason} -> acc
    end
  end
end
