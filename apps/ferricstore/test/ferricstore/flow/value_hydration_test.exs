defmodule Ferricstore.Flow.ValueHydrationTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.ValueHydration
  alias Ferricstore.Store.ReadResult

  test "payload_result preserves nil and non-record results" do
    assert ValueHydration.payload_result(:ctx, {:ok, nil}, %{enabled?: true, max_bytes: 10}) ==
             {:ok, nil}

    assert ValueHydration.payload_result(:ctx, {:error, "ERR"}, %{enabled?: true, max_bytes: 10}) ==
             {:error, "ERR"}
  end

  test "payload_records and named_value_records no-op disabled or empty cases" do
    records = [%{id: "flow-1"}]

    assert ValueHydration.payload_records(:ctx, records, %{enabled?: false}) == records
    assert ValueHydration.payload_records(:ctx, [], %{enabled?: true, max_bytes: 10}) == []
    assert ValueHydration.named_value_records(:ctx, records, nil) == records
    assert ValueHydration.named_value_records(:ctx, records, []) == records
    assert ValueHydration.named_value_records(:ctx, [], :all) == []
  end

  test "payload hydration marks storage failures without exposing their reason" do
    failure = ReadResult.failure({:missing_file, "/private/secret/path"})

    assert %{payload_error: "ERR storage read failed"} =
             ValueHydration.__apply_value_result_for_test__(
               %{payload_ref: "ref"},
               :payload,
               "ref",
               failure,
               10
             )
  end

  test "file-reference payload size is the exact stored byte count" do
    assert %{payload_omitted: true, payload_size: 100} =
             ValueHydration.__apply_value_result_for_test__(
               %{payload_ref: "ref"},
               :payload,
               "ref",
               {:file_ref, "/segment", 10, 100},
               10
             )
  end

  test "payload hydration marks corrupted typed values" do
    corrupted = "FSV2" <> <<2>> <> "not-an-external-term"

    hydrated =
      ValueHydration.__apply_value_result_for_test__(
        %{payload_ref: "ref"},
        :payload,
        "ref",
        corrupted,
        1_024
      )

    assert hydrated.payload_error == "ERR invalid flow value"
    refute Map.has_key?(hydrated, :payload)
  end

  test "named value hydration marks corrupted typed values" do
    corrupted = "FSV2" <> <<2>> <> "not-an-external-term"

    assert [%{values_error: "ERR invalid flow value"} = hydrated] =
             ValueHydration.__hydrate_named_values_for_test__(
               [%{id: "flow-1"}],
               [{0, "document", "ref"}],
               %{"ref" => corrupted}
             )

    refute Map.has_key?(hydrated, :values)
  end

  test "hydration rejects short and extra batch replies instead of treating slots as missing" do
    for results <- [[nil], [nil, nil, nil], :invalid] do
      values =
        ValueHydration.__map_fetched_values_for_test__(
          ["ref-a", "ref-b"],
          results
        )

      assert %{
               "ref-a" => {:error, {:storage_read_failed, :batch_result_mismatch}},
               "ref-b" => {:error, {:storage_read_failed, :batch_result_mismatch}}
             } = values
    end
  end

  test "exact hydration batches preserve missing values by reference" do
    assert %{"ref-a" => nil, "ref-b" => "encoded"} =
             ValueHydration.__map_fetched_values_for_test__(
               ["ref-a", "ref-b"],
               [nil, "encoded"]
             )
  end
end
