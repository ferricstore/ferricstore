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

  test "empty continuation frames do not retain metadata" do
    name = :"native_empty_chunk_budget_#{System.unique_integer([:positive])}"

    budget =
      start_supervised!({ResourceBudget, name: name, limits: %{chunk_streams: 1, chunk_bytes: 1}})

    state = chunk_state(budget, max_frame_bytes: 64, max_pending_chunk_bytes: 1)

    state =
      Enum.reduce(1..1_000, state, fn _, state ->
        assert {:pending, state} =
                 Chunks.reassemble({1, 0x0100, 99, @more_chunks_flag, ""}, state)

        state
      end)

    assert [{_, {_, chunks, 0, _, _, _, 0}}] = Map.to_list(state.chunk_buffers)
    assert chunks == []
    assert Map.get(state, :pending_chunk_metadata_bytes, 0) == 0
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "tracks one-byte continuations without changing reassembly" do
    body = String.duplicate("x", 1_000)
    name = :"native_byte_chunk_budget_#{System.unique_integer([:positive])}"

    budget =
      start_supervised!(
        {ResourceBudget, name: name, limits: %{chunk_streams: 1, chunk_bytes: 1_000}}
      )

    state = chunk_state(budget, max_frame_bytes: 1_000, max_pending_chunk_bytes: 1_000)

    state =
      Enum.reduce(1..1_000, state, fn _, state ->
        assert {:pending, state} =
                 Chunks.reassemble({1, 0x0100, 100, @more_chunks_flag, "x"}, state)

        state
      end)

    assert [{_, {_, chunks, 1_000, _, _, _, 1_000}}] = Map.to_list(state.chunk_buffers)
    assert length(chunks) == 1_000
    assert Chunks.retained_metadata_bytes(state) == 1_000 * 64

    assert {:ready, {1, 0x0100, 100, 0, ^body}, ready_state} =
             Chunks.reassemble({1, 0x0100, 100, 0, ""}, state)

    assert ready_state.chunk_buffers == %{}
    assert Map.get(ready_state, :pending_chunk_metadata_bytes, 0) == 0
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "tracks ordinary continuation fragments independently" do
    fragment = String.duplicate("x", 8 * 1024)
    name = :"native_large_chunk_budget_#{System.unique_integer([:positive])}"

    budget =
      start_supervised!(
        {ResourceBudget,
         name: name, limits: %{chunk_streams: 1, chunk_bytes: byte_size(fragment) * 64}}
      )

    state =
      chunk_state(
        budget,
        max_frame_bytes: byte_size(fragment) * 64,
        max_pending_chunk_bytes: byte_size(fragment) * 64
      )

    state =
      Enum.reduce(1..64, state, fn _, state ->
        assert {:pending, state} =
                 Chunks.reassemble({1, 0x0100, 104, @more_chunks_flag, fragment}, state)

        state
      end)

    assert [{_, {_, chunks, _, _, _, _, 64}}] = Map.to_list(state.chunk_buffers)
    assert length(chunks) == 64
    assert Chunks.retained_metadata_bytes(state) == 64 * 64

    assert {:ready, _frame, ready_state} =
             Chunks.reassemble({1, 0x0100, 104, 0, ""}, state)

    assert ready_state.chunk_buffers == %{}
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "an empty final continuation completes a valid stream" do
    name = :"native_empty_final_budget_#{System.unique_integer([:positive])}"

    budget =
      start_supervised!({ResourceBudget, name: name, limits: %{chunk_streams: 1, chunk_bytes: 7}})

    state = chunk_state(budget, max_frame_bytes: 7, max_pending_chunk_bytes: 7)

    assert {:pending, state} =
             Chunks.reassemble({1, 0x0100, 101, @more_chunks_flag, "payload"}, state)

    assert {:ready, {1, 0x0100, 101, 0, "payload"}, ready_state} =
             Chunks.reassemble({1, 0x0100, 101, 0, ""}, state)

    assert ready_state.chunk_buffers == %{}
    assert ResourceBudget.usage(budget).chunk_bytes == 0

    assert {:pending, empty_state} =
             Chunks.reassemble({1, 0x0100, 103, @more_chunks_flag, ""}, ready_state)

    assert {:ready, {1, 0x0100, 103, 0, ""}, empty_ready_state} =
             Chunks.reassemble({1, 0x0100, 103, 0, ""}, empty_state)

    assert empty_ready_state.chunk_buffers == %{}
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "preserves nonuniform fragment order across large continuation counts" do
    fragments = for index <- 1..4_097, do: <<rem(index, 251)>>
    expected = IO.iodata_to_binary(fragments)
    name = :"native_fragment_order_budget_#{System.unique_integer([:positive])}"

    budget =
      start_supervised!(
        {ResourceBudget,
         name: name, limits: %{chunk_streams: 1, chunk_bytes: byte_size(expected)}}
      )

    state =
      chunk_state(
        budget,
        max_frame_bytes: byte_size(expected),
        max_pending_chunk_bytes: byte_size(expected)
      )

    {last, pending} = List.pop_at(fragments, -1)

    state =
      Enum.reduce(pending, state, fn fragment, state ->
        assert {:pending, state} =
                 Chunks.reassemble({1, 0x0100, 102, @more_chunks_flag, fragment}, state)

        state
      end)

    assert {:ready, {1, 0x0100, 102, 0, ^expected}, ready_state} =
             Chunks.reassemble({1, 0x0100, 102, 0, last}, state)

    assert ready_state.chunk_buffers == %{}
    assert ResourceBudget.usage(budget).chunk_bytes == 0
  end

  test "final chunk assembly yields an exact-size body" do
    name = :"native_chunk_exact_size_#{System.unique_integer([:positive])}"
    size = 256 * 1024

    budget =
      start_supervised!(
        {ResourceBudget, name: name, limits: %{chunk_streams: 1, chunk_bytes: 4 * size}}
      )

    state =
      chunk_state(budget, max_frame_bytes: 4 * size, max_pending_chunk_bytes: 4 * size)

    first = :binary.copy("a", size)
    last = :binary.copy("b", size)

    assert {:pending, state} =
             Chunks.reassemble({1, 0x0100, 7, @more_chunks_flag, first}, state)

    assert {:ready, {1, 0x0100, 7, 0, body}, _state} =
             Chunks.reassemble({1, 0x0100, 7, 0, last}, state)

    assert body == first <> last
    # One exact allocation: no second copy and no append growth headroom
    # retained for the life of the assembled request.
    assert :binary.referenced_byte_size(body) == byte_size(body)
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

  test "unauthenticated chunk reassembly uses the smaller logical frame limit" do
    budget = :"native_preauth_chunk_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: budget,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 2, chunk_bytes: 64}}
    )

    state = %{
      authenticated: false,
      require_auth: true,
      preauth_max_frame_bytes: 8,
      max_frame_bytes: 64,
      chunk_buffers: %{},
      chunk_assembly_deadline_ms: nil,
      pending_chunk_bytes: 0,
      frame_assembly_timeout_ms: 15_000,
      max_pending_chunks: 2,
      max_pending_chunk_bytes: 64,
      resource_budget: budget
    }

    assert {:error, "ERR native chunked request exceeds max_frame_bytes", ^state} =
             Chunks.reassemble(
               {1, 0x0100, 6, @more_chunks_flag, "123456789"},
               state
             )

    assert %{chunk_streams: 0, chunk_bytes: 0} = ResourceBudget.usage(budget)

    assert {:pending, state} =
             Chunks.reassemble({1, 0x0100, 7, @more_chunks_flag, "12345"}, state)

    assert {:error, "ERR native chunked request exceeds max_frame_bytes", rejected_state} =
             Chunks.reassemble({1, 0x0100, 7, 0, "6789"}, state)

    assert rejected_state.chunk_buffers == %{}
    assert Chunks.retained_metadata_bytes(rejected_state) == 0

    authenticated = %{rejected_state | authenticated: true}

    assert {:pending, authenticated} =
             Chunks.reassemble(
               {1, 0x0100, 8, @more_chunks_flag, "12345"},
               authenticated
             )

    assert {:ready, {1, 0x0100, 8, 0, "123456789"}, ready_state} =
             Chunks.reassemble({1, 0x0100, 8, 0, "6789"}, authenticated)

    assert ready_state.chunk_buffers == %{}
    assert Chunks.retained_metadata_bytes(ready_state) == 0
  end

  test "unauthenticated decompression uses the smaller logical frame limit" do
    body = String.duplicate("x", 65)
    frame = {1, 0x0100, 9, @compressed_flag, :zlib.compress(body)}

    preauth_state = %{
      authenticated: false,
      require_auth: true,
      preauth_max_frame_bytes: 64,
      max_frame_bytes: 1_024
    }

    assert {:error, "ERR native decompressed frame exceeds max_frame_bytes"} =
             Chunks.maybe_uncompress(frame, preauth_state)

    assert {:ok, {1, 0x0100, 9, 0, ^body}} =
             Chunks.maybe_uncompress(frame, %{preauth_state | authenticated: true})
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

  defp chunk_state(budget, overrides) do
    Map.merge(
      %{
        chunk_buffers: %{},
        chunk_assembly_deadline_ms: nil,
        pending_chunk_bytes: 0,
        frame_assembly_timeout_ms: 15_000,
        max_pending_chunks: 10,
        max_pending_chunk_bytes: 20,
        max_frame_bytes: 20,
        resource_budget: budget
      },
      Map.new(overrides)
    )
  end
end
