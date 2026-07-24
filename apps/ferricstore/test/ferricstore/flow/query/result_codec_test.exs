defmodule Ferricstore.Flow.Query.ResultCodecTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Ferricstore.Flow.Query.{RecordProjection, Response, ResultCodec, Usage}
  alias Ferricstore.NativeValueCodec

  @codec_tag 0xA0

  test "the frozen wire schema covers the complete public result contract" do
    assert ResultCodec.contract() == Response.contract()
    assert ResultCodec.record_fields() == RecordProjection.fields() ++ [:event_id, :fields]
    assert ResultCodec.usage_fields() == Usage.fields()
    assert MapSet.new(ResultCodec.quality_fields()) == MapSet.new(Response.quality_fields())
  end

  test "encodes the shared cross-SDK golden corpus byte for byte" do
    corpus =
      Path.expand(
        "../../../../../../test/fixtures/flow_query_result_v1.json",
        __DIR__
      )
      |> File.read!()
      |> Jason.decode!()

    assert corpus["codec"] == "flow_query_result_v1"
    assert corpus["contract"] == ResultCodec.contract()
    assert corpus["tag"] == ResultCodec.tag()
    assert corpus["record_fields"] == Enum.map(ResultCodec.record_fields(), &Atom.to_string/1)
    assert corpus["quality_fields"] == Enum.map(ResultCodec.quality_fields(), &Atom.to_string/1)
    assert corpus["usage_fields"] == Enum.map(ResultCodec.usage_fields(), &Atom.to_string/1)

    for vector <- corpus["vectors"] do
      response =
        case vector["kind"] do
          "records" ->
            %{
              "version" => corpus["contract"],
              "records" => vector["records"],
              "page" => vector["page"],
              "quality" => vector["quality"],
              "usage" => vector["usage"]
            }

          "count" ->
            %{
              "version" => corpus["contract"],
              "result" => %{
                "kind" => "count",
                "value" => String.to_integer(vector["count_decimal"])
              },
              "quality" => vector["quality"],
              "usage" => vector["usage"]
            }
        end

      assert response |> ResultCodec.encode() |> Base.encode16(case: :lower) ==
               vector["payload_hex"],
             vector["name"]
    end
  end

  test "encodes a record page with one fixed schema and exact response bytes" do
    record = %{
      id: "run-1",
      type: "invoice",
      state: "failed",
      version: 7,
      priority: 3,
      partition_key: "tenant-a",
      run_state: nil,
      attributes: %{"customer" => "c-1"},
      state_meta: %{"failed" => %{"reason" => "timeout"}}
    }

    response = page_response([record], true, "fqc1_cursor")
    payload = ResultCodec.encode(response)

    assert is_binary(payload)
    assert ResultCodec.tag() == @codec_tag

    assert <<
             @codec_tag,
             0,
             1,
             1,
             0,
             3,
             1::unsigned-64,
             2::unsigned-64,
             3::unsigned-64,
             4::unsigned-64,
             5::unsigned-64,
             6::unsigned-64,
             7::unsigned-64,
             1::unsigned-64,
             response_bytes::unsigned-64,
             8::unsigned-64,
             9::unsigned-64,
             rest::binary
           >> = payload

    assert response_bytes == byte_size(payload)

    bitmap =
      1 <<< 0 ||| 1 <<< 1 ||| 1 <<< 2 ||| 1 <<< 3 ||| 1 <<< 4 |||
        1 <<< 5 ||| 1 <<< 11 ||| 1 <<< 16 ||| 1 <<< 17

    values =
      [
        "run-1",
        "invoice",
        "failed",
        7,
        3,
        "tenant-a",
        nil,
        %{"customer" => "c-1"},
        %{"failed" => %{"reason" => "timeout"}}
      ]
      |> Enum.map(&NativeValueCodec.encode/1)
      |> IO.iodata_to_binary()

    assert <<1, 11::unsigned-32, "fqc1_cursor", 1::unsigned-32, ^bitmap::unsigned-32,
             ^values::binary>> = rest
  end

  test "encodes scalar counts without a record or page envelope" do
    response = count_response(42)
    payload = ResultCodec.encode(response)

    assert <<
             @codec_tag,
             1,
             1,
             1,
             0,
             0,
             _first_eight_usage_fields::binary-size(64),
             response_bytes::unsigned-64,
             _last_two_usage_fields::binary-size(16),
             42::unsigned-64
           >> = payload

    assert response_bytes == byte_size(payload)
  end

  test "encodes history records and preserves present nil separately from missing fields" do
    fields = %{"event" => "transitioned", "error" => nil}
    response = page_response([%{event_id: "1000-7", fields: fields}], false, nil)
    payload = ResultCodec.encode(response)

    <<
      @codec_tag,
      0,
      _quality::binary-size(4),
      _usage::binary-size(88),
      0,
      0xFFFF_FFFF::unsigned-32,
      1::unsigned-32,
      bitmap::unsigned-32,
      values::binary
    >> = payload

    assert bitmap == (1 <<< 18 ||| 1 <<< 19)
    assert values == NativeValueCodec.encode("1000-7") <> NativeValueCodec.encode(fields)
  end

  test "encodes sparse projected rows without placeholders for omitted fields" do
    record = %{id: "run-1", state: "ready", attributes: %{"customer" => "customer-a"}}
    response = page_response([record], false, nil)
    payload = ResultCodec.encode(response)

    <<
      @codec_tag,
      0,
      _quality::binary-size(4),
      _usage::binary-size(88),
      0,
      0xFFFF_FFFF::unsigned-32,
      1::unsigned-32,
      bitmap::unsigned-32,
      values::binary
    >> = payload

    assert bitmap == (1 <<< 0 ||| 1 <<< 2 ||| 1 <<< 16)

    assert values ==
             NativeValueCodec.encode("run-1") <>
               NativeValueCodec.encode("ready") <>
               NativeValueCodec.encode(%{"customer" => "customer-a"})
  end

  test "explicit projection materially reduces encoded result work for metadata-heavy rows" do
    full = %{
      id: "run-1",
      type: "invoice",
      state: "ready",
      version: 7,
      partition_key: "tenant-a",
      updated_at_ms: 1_000,
      attributes: Map.new(1..16, &{"attribute-#{&1}", String.duplicate("a", 256)}),
      state_meta: %{
        "ready" => Map.new(1..16, &{"metadata-#{&1}", String.duplicate("m", 256)})
      }
    }

    assert {:ok, projected} =
             RecordProjection.project_result({:ok, full}, :runs, [:run_id, :state])

    full_bytes = full |> List.wrap() |> page_response(false, nil) |> ResultCodec.encode()

    projected_bytes =
      projected
      |> List.wrap()
      |> page_response(false, nil)
      |> ResultCodec.encode()

    assert byte_size(projected_bytes) < div(byte_size(full_bytes), 4)
  end

  test "encodes every currently emitted quality combination" do
    qualities = [
      {"authoritative", "current", "none"},
      {"authoritative", "current", "complete"},
      {"authoritative", "current", "authenticated_seek"},
      {"projected_exact", "projection_watermark", "none"},
      {"projected_exact", "projection_watermark", "complete"},
      {"projected_exact", "projection_watermark", "live_seek"},
      {"exact", "not_applicable", "none"}
    ]

    for {exactness, freshness, pagination} <- qualities do
      response =
        page_response([], false, nil)
        |> put_in([:quality], %{
          exactness: exactness,
          freshness: freshness,
          coverage: "complete",
          pagination: pagination
        })

      assert <<@codec_tag, 0, _rest::binary>> = ResultCodec.encode(response)
    end
  end

  test "keeps every public run field at its frozen bitmap position" do
    record =
      RecordProjection.fields()
      |> Enum.with_index()
      |> Map.new(fn {field, index} -> {field, index} end)

    payload = ResultCodec.encode(page_response([record], false, nil))

    <<
      @codec_tag,
      0,
      _quality::binary-size(4),
      _usage::binary-size(88),
      0,
      0xFFFF_FFFF::unsigned-32,
      1::unsigned-32,
      bitmap::unsigned-32,
      _values::binary
    >> = payload

    assert bitmap == (1 <<< length(RecordProjection.fields())) - 1
  end

  test "accepts string-keyed native response maps without changing their wire schema" do
    atom_payload = ResultCodec.encode(page_response([%{id: "run-1", state: nil}], false, nil))

    string_response = %{
      "version" => "ferric.flow.query.result/v1",
      "records" => [%{"id" => "run-1", "state" => nil}],
      "page" => %{"has_more" => false, "cursor" => nil},
      "quality" => stringify_keys(quality("live_seek")),
      "usage" => stringify_keys(usage(1))
    }

    assert ResultCodec.encode(string_response) == atom_payload
  end

  test "falls back for unknown contracts, fields, quality values, and unencodable values" do
    response = page_response([%{id: "run-1"}], false, nil)

    assert ResultCodec.encode(%{response | version: "ferric.flow.explain/v1"}) == nil

    assert response
           |> put_in([:records], [%{id: "run-1", internal_secret: "no"}])
           |> ResultCodec.encode() == nil

    assert response
           |> put_in([:quality, :freshness], "future_mode")
           |> ResultCodec.encode() == nil

    assert response
           |> put_in([:records], [%{id: "run-1", version: 0x1_0000_0000_0000_0000}])
           |> ResultCodec.encode() == nil
  end

  test "fails closed outside the public count, cursor, row, and usage bounds" do
    above_signed_native = 0x8000_0000_0000_0000

    assert ResultCodec.encode(count_response(above_signed_native)) == nil

    assert count_response(1)
           |> put_in([:usage, :range_pages], above_signed_native)
           |> ResultCodec.encode() == nil

    assert page_response([], true, String.duplicate("c", 4_097))
           |> ResultCodec.encode() == nil

    records = List.duplicate(%{id: "run"}, 101)
    assert ResultCodec.encode(page_response(records, false, nil)) == nil
  end

  test "keeps representative record pages materially smaller than typed maps" do
    lean_records =
      for index <- 1..100 do
        %{
          id: "run-#{index}",
          type: "invoice",
          state: "failed",
          version: index,
          priority: rem(index, 10),
          partition_key: "tenant-a",
          created_at_ms: index,
          updated_at_ms: index + 1,
          attempts: 2
        }
      end

    metadata_records =
      Enum.map(lean_records, fn record ->
        Map.merge(record, %{
          attributes: %{"customer" => "customer-123", "region" => "eu-west"},
          state_meta: %{"failed" => %{"reason" => "timeout", "worker" => "worker-1"}}
        })
      end)

    lean = page_response(lean_records, false, nil)
    metadata = page_response(metadata_records, false, nil)
    lean_payload = ResultCodec.encode(lean)
    metadata_payload = ResultCodec.encode(metadata)

    assert byte_size(lean_payload) * 100 <= NativeValueCodec.encoded_size(lean) * 50
    assert byte_size(metadata_payload) * 100 <= NativeValueCodec.encoded_size(metadata) * 70
  end

  defp page_response(records, has_more, cursor) do
    %{
      version: "ferric.flow.query.result/v1",
      records: records,
      page: %{has_more: has_more, cursor: cursor},
      quality: quality("live_seek"),
      usage: usage(length(records))
    }
  end

  defp count_response(count) do
    %{
      version: "ferric.flow.query.result/v1",
      result: %{kind: "count", value: count},
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
      range_pages: 2,
      scanned_entries: 3,
      scanned_bytes: 4,
      hydrated_records: 5,
      residual_checks: 6,
      duplicate_entries: 7,
      result_records: result_records,
      response_bytes: 0,
      memory_high_water_bytes: 8,
      wall_time_us: 9
    }
  end

  defp stringify_keys(map),
    do: Map.new(map, fn {key, value} -> {Atom.to_string(key), value} end)
end
