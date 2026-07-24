defmodule FerricstoreServer.Native.FlowQueryResultCodecTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.ResultCodec
  alias Ferricstore.NativeValueCodec
  alias FerricstoreServer.Native.Codec

  @op_command_exec 0x0100
  @op_flow_query 0x0231
  @query_codecs MapSet.new(["flow_query_result_v1"])

  test "explicitly selected FLOW.QUERY responses use the fixed-schema result codec" do
    result = page_result()

    [frame] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 42, :ok, result,
        compact_response_codecs: @query_codecs
      )

    assert {flags, <<0::unsigned-16, payload::binary>>} = response(frame, @op_flow_query)
    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    assert payload == ResultCodec.encode(result)
    assert <<tag, _rest::binary>> = payload
    assert tag == ResultCodec.tag()
  end

  test "COMMAND_EXEC gets the same compact query result contract" do
    [frame] =
      Codec.encode_command_response_frames(@op_command_exec, 3, 43, :ok, count_result(),
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs
      )

    assert {flags, <<0::unsigned-16, tag, 1, _rest::binary>>} =
             response(frame, @op_command_exec)

    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    assert tag == ResultCodec.tag()
  end

  test "the broad compact flag cannot opt a client into an undeclared query codec" do
    result = page_result()

    [plain] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 44, :ok, result,
        compact_flow_responses: true
      )

    {plain_flags, <<0::unsigned-16, plain_payload::binary>>} = response(plain, @op_flow_query)
    assert Bitwise.band(plain_flags, Codec.flags().custom_payload) == 0
    assert {:ok, %{"version" => "ferric.flow.query.result/v1"}} = Codec.decode_body(plain_payload)

    explain = %{version: "ferric.flow.explain/v1", status: "planned"}

    [fallback] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 45, :ok, explain,
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs
      )

    {fallback_flags, <<0::unsigned-16, fallback_payload::binary>>} =
      response(fallback, @op_flow_query)

    assert Bitwise.band(fallback_flags, Codec.flags().custom_payload) == 0

    assert {:ok, %{"version" => "ferric.flow.explain/v1"}} =
             Codec.decode_body(fallback_payload)

    future_result = put_in(result, [:records], [%{id: "run-1", future_field: "preserved"}])

    [future_fallback] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 46, :ok, future_result,
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs
      )

    {future_flags, <<0::unsigned-16, future_payload::binary>>} =
      response(future_fallback, @op_flow_query)

    assert Bitwise.band(future_flags, Codec.flags().custom_payload) == 0

    assert {:ok, %{"records" => [%{"future_field" => "preserved"}]}} =
             Codec.decode_body(future_payload)
  end

  test "response byte limits use the selected compact representation" do
    result = page_result(20)
    compact_bytes = result |> ResultCodec.encode() |> byte_size()
    generic_bytes = NativeValueCodec.encoded_size(result)

    assert compact_bytes < generic_bytes

    [accepted] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 46, :ok, result,
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs,
        max_response_bytes: compact_bytes + 2
      )

    {accepted_flags, <<0::unsigned-16, _payload::binary>>} = response(accepted, @op_flow_query)
    assert Bitwise.band(accepted_flags, Codec.flags().custom_payload) != 0

    [rejected] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 47, :ok, result,
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs,
        max_response_bytes: compact_bytes + 1
      )

    {rejected_flags, <<6::unsigned-16, error_payload::binary>>} =
      response(rejected, @op_flow_query)

    assert Bitwise.band(rejected_flags, Codec.flags().custom_payload) == 0

    assert {:ok, %{"message" => "ERR native response byte limit exceeded"}} =
             Codec.decode_body(error_payload)
  end

  test "compact query results retain custom framing across bounded chunks" do
    frames =
      Codec.encode_command_response_frames(@op_flow_query, 3, 48, :ok, page_result(20),
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs,
        chunk_bytes: 64
      )

    assert length(frames) > 1

    body =
      frames
      |> Enum.map(fn frame ->
        {flags, body} = response(frame, @op_flow_query)
        assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
        body
      end)
      |> IO.iodata_to_binary()

    assert <<0::unsigned-16, tag, _payload::binary>> = body
    assert tag == ResultCodec.tag()
  end

  test "compact query results compose with negotiated response compression" do
    [frame] =
      Codec.encode_command_response_frames(@op_flow_query, 3, 49, :ok, page_result(20),
        compact_flow_responses: true,
        compact_response_codecs: @query_codecs,
        compression: :zlib
      )

    {flags, compressed_body} = response(frame, @op_flow_query)
    assert Bitwise.band(flags, Codec.flags().custom_payload) != 0
    assert Bitwise.band(flags, Codec.flags().compressed) != 0

    assert <<0::unsigned-16, tag, _payload::binary>> = :zlib.uncompress(compressed_body)
    assert tag == ResultCodec.tag()
  end

  defp response(frame, opcode) do
    frame = IO.iodata_to_binary(frame)

    assert <<"FSNP", 0x81, flags, 3::unsigned-32, ^opcode::unsigned-16, _request_id::unsigned-64,
             body_len::unsigned-32, body::binary>> = frame

    assert body_len == byte_size(body)
    {flags, body}
  end

  defp page_result(count \\ 1) do
    records =
      for index <- 1..count do
        %{
          id: "run-#{index}",
          type: "invoice",
          state: "failed",
          version: index,
          partition_key: "tenant-a",
          attributes: %{"customer" => "customer-#{index}"}
        }
      end

    %{
      version: "ferric.flow.query.result/v1",
      records: records,
      page: %{has_more: false, cursor: nil},
      quality: quality("complete"),
      usage: usage(count)
    }
  end

  defp count_result do
    %{
      version: "ferric.flow.query.result/v1",
      result: %{kind: "count", value: 42},
      quality: quality("none"),
      usage: usage(1)
    }
  end

  defp quality(pagination) do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: pagination
    }
  end

  defp usage(result_records) do
    %{
      range_seeks: 1,
      range_pages: 1,
      scanned_entries: result_records,
      scanned_bytes: result_records * 32,
      hydrated_records: result_records,
      residual_checks: result_records,
      duplicate_entries: 0,
      result_records: result_records,
      response_bytes: 0,
      memory_high_water_bytes: 256,
      wall_time_us: 100
    }
  end
end
