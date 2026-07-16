defmodule FerricstoreServer.Native.ChunksTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.ResourceBudget
  alias FerricstoreServer.Native.Connection.Chunks

  @compressed_flag 0x08
  @more_chunks_flag 0x20

  test "chunk streams and bytes are bounded across connections" do
    budget = :"native_chunk_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 5}}
    )

    base_state = %{
      chunk_buffers: %{},
      chunk_assembly_deadline_ms: nil,
      pending_chunk_bytes: 0,
      frame_assembly_timeout_ms: 15_000,
      max_pending_chunks: 10,
      max_pending_chunk_bytes: 20,
      max_frame_bytes: 20,
      resource_budget: budget
    }

    assert {:pending, state} =
             Chunks.reassemble({1, 0x0100, 1, @more_chunks_flag, "1234"}, base_state)

    assert %{chunk_streams: 1, chunk_bytes: 4} = ResourceBudget.usage(budget)

    assert {:error, "ERR native global pending chunk stream limit exceeded", ^base_state} =
             Chunks.reassemble({1, 0x0100, 2, @more_chunks_flag, "x"}, base_state)

    assert {:error, "ERR native global pending chunk bytes limit exceeded", emptied_state} =
             Chunks.reassemble({1, 0x0100, 1, 0, "56"}, state)

    assert emptied_state.chunk_buffers == %{}
    assert eventually(fn -> ResourceBudget.usage(budget).chunk_streams == 0 end)
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "request decompression is incremental and output-bounded" do
    source =
      File.read!(
        Path.expand("../../../lib/ferricstore_server/native/connection/chunks.ex", __DIR__)
      )

    assert source =~ ":zlib.safeInflate"
    refute source =~ ":zlib.uncompress"

    compressed = :zlib.compress(String.duplicate("x", 2 * 1024 * 1024))
    frame = {1, 0x0100, 1, @compressed_flag, compressed}

    assert {:error, "ERR native decompressed frame exceeds max_frame_bytes"} =
             Chunks.maybe_uncompress(frame, %{max_frame_bytes: 1024})
  end

  test "request decompression accepts output exactly at the frame limit" do
    body = String.duplicate("bounded", 128)
    frame = {1, 0x0100, 1, @compressed_flag, :zlib.compress(body)}

    assert {:ok, {1, 0x0100, 1, 0, ^body}} =
             Chunks.maybe_uncompress(frame, %{max_frame_bytes: byte_size(body)})
  end

  test "request decompression rejects a truncated zlib stream" do
    compressed = :zlib.compress(String.duplicate("payload", 128))
    truncated = binary_part(compressed, 0, byte_size(compressed) - 2)
    frame = {1, 0x0100, 1, @compressed_flag, truncated}

    assert {:error, "ERR native compressed frame body is invalid"} =
             Chunks.maybe_uncompress(frame, %{max_frame_bytes: 4_096})
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end
end
