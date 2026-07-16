defmodule Ferricstore.Flow.ValueStoreTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.ReadResult

  test "mget result decoding preserves order and enforces exact cardinality" do
    refs = ["ref-a", "ref-b"]

    assert {:ok, [:a, nil]} =
             ValueStore.__decode_mget_results_for_test__(
               refs,
               [Codec.encode_value(:a), nil],
               nil
             )

    assert {:error, "ERR flow value mget result mismatch"} =
             ValueStore.__decode_mget_results_for_test__(refs, [Codec.encode_value(:a)], nil)
  end

  test "mget result decoding sanitizes storage read failures" do
    failure = ReadResult.failure({:missing_file, "/private/secret/path"})

    assert {:error, "ERR storage read failed"} =
             ValueStore.__decode_mget_results_for_test__(["ref"], [failure], nil)
  end

  test "mget rejects corrupted typed values instead of returning encoded bytes" do
    corrupted = "FSV2" <> <<2>> <> "not-an-external-term"

    assert {:error, "ERR invalid flow value"} =
             ValueStore.__decode_mget_results_for_test__(["ref"], [corrupted], nil)
  end

  test "shared value batch mapping rejects extra backend results" do
    prepared = [{:ok, %{ref: "ref", partition_key: nil, owner_flow_id: nil, return: nil}}]

    assert [{:error, "ERR flow value put result mismatch"}] =
             ValueStore.__map_shared_value_put_results_for_test__(prepared, [:ok, :ok])
  end

  test "named value puts reject independently expiring values" do
    assert {:error, "ERR flow ttl_ms is not supported for named values"} =
             ValueStore.named_value_attrs("value",
               owner_flow_id: "flow-1",
               name: "document",
               ttl_ms: 1_000
             )
  end

  test "named value identity components must be non-empty" do
    assert {:error, "ERR flow owner_flow_id must be a non-empty string"} =
             ValueStore.named_value_attrs("value", owner_flow_id: "", name: "document")

    assert {:error, "ERR flow name must be a non-empty string"} =
             ValueStore.named_value_attrs("value", owner_flow_id: "flow-1", name: "")
  end
end
