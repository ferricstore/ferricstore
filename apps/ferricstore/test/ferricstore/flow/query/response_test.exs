defmodule Ferricstore.Flow.Query.ResponseTest do
  use ExUnit.Case, async: true

  alias Ferricstore.{Flow.Query.Limits, NativeValueCodec}
  alias Ferricstore.Flow.Query.{Budget, Response}

  test "builds one versioned page contract and accounts its complete encoded size" do
    usage = usage(1)
    records = [%{id: "run-1", partition_key: "tenant-a", state: "failed"}]

    assert {:ok, response} =
             Response.build(records, true, "fqc1_cursor", quality(), usage, Budget.default())

    assert response.version == "ferric.flow.query.result/v1"
    assert response.records == records
    assert response.page == %{has_more: true, cursor: "fqc1_cursor"}
    assert response.quality == quality()
    assert response.usage.result_records == 1

    assert response.usage.response_bytes == NativeValueCodec.encoded_size(response)
  end

  test "response byte accounting is stable after writing any signed response size" do
    response = %{
      version: "ferric.flow.query.result/v1",
      records: [%{id: "run-1"}],
      page: %{has_more: false, cursor: nil},
      quality: quality(),
      usage: usage(1)
    }

    expected = NativeValueCodec.encoded_size(response)

    for response_bytes <- [0, 1, expected, 0x7FFF_FFFF_FFFF_FFFF] do
      assert response
             |> put_in([:usage, :response_bytes], response_bytes)
             |> NativeValueCodec.encoded_size() == expected
    end
  end

  test "rejects the full response instead of returning a partial page" do
    {:ok, budget} = Budget.lower(Budget.default(), response_bytes: 256)
    records = [%{id: "run-1", state: :binary.copy("x", 1_024)}]

    assert {:error, :query_response_budget_exceeded} =
             Response.build(records, false, nil, quality(), usage(1), budget)
  end

  test "enforces the response budget against the native wire representation" do
    {:ok, budget} = Budget.lower(Budget.default(), response_bytes: 900)

    state_meta =
      for state <- 1..2, into: %{} do
        entries = for entry <- 1..16, into: %{}, do: {"k#{entry}", entry}
        {"s#{state}", entries}
      end

    records = [%{id: "run-1", state_meta: state_meta}]

    assert {:error, :query_response_budget_exceeded} =
             Response.build(records, false, nil, quality(), usage(1), budget)
  end

  test "rejects inconsistent pagination and usage metadata" do
    assert {:error, :query_engine_failure} =
             Response.build([], false, "cursor", quality(), usage(0), Budget.default())

    assert {:error, :query_engine_failure} =
             Response.build([], true, nil, quality(), usage(0), Budget.default())

    assert {:error, :query_engine_failure} =
             Response.build([%{id: "run-1"}], false, nil, quality(), usage(0), Budget.default())
  end

  test "rejects pagination metadata above the public cursor limit" do
    oversized_cursor = :binary.copy("x", Limits.max_cursor_bytes() + 1)

    assert {:error, :query_engine_failure} =
             Response.build([], true, oversized_cursor, quality(), usage(0), Budget.default())
  end

  test "fails closed for usage counters outside the native signed integer contract" do
    oversized = 0x8000_0000_0000_0000

    for field <- [:range_pages, :residual_checks] do
      invalid_usage = Map.put(usage(0), field, oversized)

      assert {:error, :query_engine_failure} =
               Response.build([], false, nil, quality(), invalid_usage, Budget.default())
    end
  end

  test "fails closed instead of raising for an unencodable nested record value" do
    records = [%{id: "run-1", attributes: %{"oversized" => 0x8000_0000_0000_0000}}]

    assert {:error, :query_engine_failure} =
             Response.build(records, false, nil, quality(), usage(1), Budget.default())
  end

  test "builds a non-paginated scalar count without inventing row hydration" do
    count_usage = %{usage(0) | result_records: 1, scanned_entries: 7, hydrated_records: 7}

    assert {:ok, response} =
             Response.build_count(7, quality("none"), count_usage, Budget.default())

    assert response == %{
             version: "ferric.flow.query.result/v1",
             result: %{kind: "count", value: 7},
             quality: quality("none"),
             usage: response.usage
           }

    assert response.usage.response_bytes == NativeValueCodec.encoded_size(response)

    zero_usage = %{usage(0) | result_records: 1}

    assert {:ok, %{result: %{value: 0}}} =
             Response.build_count(0, quality("none"), zero_usage, Budget.default())
  end

  defp usage(result_records) do
    %{
      range_seeks: 1,
      range_pages: 1,
      scanned_entries: result_records,
      scanned_bytes: 0,
      hydrated_records: result_records,
      residual_checks: result_records,
      duplicate_entries: 0,
      result_records: result_records,
      response_bytes: 0,
      memory_high_water_bytes: 0,
      wall_time_us: 10
    }
  end

  defp quality(pagination \\ "live_seek") do
    %{
      exactness: "projected_exact",
      freshness: "projection_watermark",
      coverage: "complete",
      pagination: pagination
    }
  end
end
