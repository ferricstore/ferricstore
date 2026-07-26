defmodule Ferricstore.Flow.ColdLMDBSchemaTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.{LMDB, Locator}
  alias Ferricstore.TermCodec

  @max_key_bytes 65_535
  @max_encoded_bytes 384 * 1_024

  test "cold park state-key keys are scoped by exact Flow state key" do
    tenant_a = LMDB.cold_park_key_for_state_key("flow/state/tenant-a/shared-id")
    tenant_b = LMDB.cold_park_key_for_state_key("flow/state/tenant-b/shared-id")

    assert tenant_a != tenant_b
    assert String.starts_with?(tenant_a, "flow:park:v1:key:")
    assert String.starts_with?(tenant_b, "flow:park:v1:key:")
  end

  test "cold park rows round-trip versioned state locator metadata" do
    locator = state_locator(version: 7, raft_index: 70)

    encoded =
      LMDB.encode_cold_park(locator,
        due_at_ms: 123_456,
        type: "email",
        state: "waiting",
        partition_key: "tenant-1",
        state_key: "flow/state/tenant-1/flow-1",
        priority: 3,
        lease_until_ms: 0,
        fencing_token: 7,
        retention_at_ms: 999_999,
        value_refs_digest: :crypto.hash(:sha256, "refs"),
        checksum: <<4, 5, 6>>
      )

    assert {:ok, {:flow_cold_park, 2, _fields}} = TermCodec.decode(encoded)
    assert {:ok, fields} = LMDB.decode_cold_park(encoded)
    assert fields.locator == locator
    assert fields.due_at_ms == 123_456
    assert fields.type == "email"
    assert fields.state == "waiting"
    assert fields.partition_key == "tenant-1"
    assert fields.state_key == "flow/state/tenant-1/flow-1"
    assert fields.priority == 3
    assert fields.fencing_token == 7
    refute Map.has_key?(fields, :state_value)
  end

  test "cold park schema rejects embedded state records in both directions" do
    locator = state_locator(version: 7, raft_index: 70)

    assert_raise ArgumentError, "cold park cannot embed a Flow state record", fn ->
      LMDB.encode_cold_park(locator,
        due_at_ms: 123_456,
        type: "email",
        state: "waiting",
        state_key: "flow/state/tenant-1/flow-1",
        state_value: "authoritative-record-must-stay-in-the-log"
      )
    end

    old_fields = %{
      locator: locator,
      due_at_ms: 123_456,
      type: "email",
      state: "waiting",
      partition_key: "tenant-1",
      state_key: "flow/state/tenant-1/flow-1",
      priority: 0,
      lease_until_ms: nil,
      fencing_token: nil,
      retention_at_ms: nil,
      value_refs_digest: nil,
      state_value: "authoritative-record-must-stay-in-the-log",
      checksum: nil
    }

    assert :error =
             LMDB.decode_cold_park(TermCodec.encode({:flow_cold_park, 1, old_fields}))
  end

  test "cold park schema rejects the removed version even without an embedded record" do
    locator = state_locator(version: 7, raft_index: 70)

    assert :error =
             LMDB.decode_cold_park(
               TermCodec.encode({:flow_cold_park, 1, valid_park_fields(locator)})
             )
  end

  test "cold park decoder rejects invalid blobs and invalid locators" do
    assert :not_cold_park = LMDB.decode_cold_park(:erlang.term_to_binary({:other, 1}))
    assert :error = LMDB.decode_cold_park(<<1, 2, 3>>)

    invalid =
      :erlang.term_to_binary({:flow_cold_park, 2, %{locator: %{flow_id: "flow-1"}}})

    assert :error = LMDB.decode_cold_park(invalid)
  end

  test "cold park decoder rejects compressed and trailing external terms" do
    locator = state_locator(flow_id: String.duplicate("flow", 2_048))
    encoded = LMDB.encode_cold_park(locator, state_key: "state-key")

    compressed =
      encoded |> :erlang.binary_to_term([:safe]) |> :erlang.term_to_binary(compressed: 9)

    assert <<131, 80, _rest::binary>> = compressed

    assert :error = LMDB.decode_cold_park(compressed)
    assert :error = LMDB.decode_cold_park(encoded <> <<0>>)
  end

  test "cold due keys sort by bucket then exact due time then flow id/version" do
    key_a =
      LMDB.cold_due_key(
        bucket_ms: 60_000,
        type: "email",
        state: "waiting",
        partition_key: "tenant-1",
        priority: 0,
        due_at_ms: 61_000,
        flow_id: "flow-a",
        version: 1
      )

    key_b =
      LMDB.cold_due_key(
        bucket_ms: 60_000,
        type: "email",
        state: "waiting",
        partition_key: "tenant-1",
        priority: 0,
        due_at_ms: 62_000,
        flow_id: "flow-b",
        version: 1
      )

    assert key_a < key_b
    assert LMDB.cold_due_bucket_ms(119_999, 60_000) == 60_000
  end

  test "reverse segment keys are exact physical-location keys" do
    original =
      state_locator(version: 1, raft_index: 10, file_id: {:waraft_segment, 10}, offset: 10)

    relocated = Locator.relocate!(original, file_id: {:waraft_segment, 11}, offset: 20)

    same_position_new_generation =
      state_locator(version: 2, raft_index: 20, file_id: {:waraft_segment, 10}, offset: 10)

    original_key = LMDB.cold_by_segment_key(original)
    relocated_key = LMDB.cold_by_segment_key(relocated)

    assert original_key != relocated_key
    assert original_key =~ "flow:cold:by-segment:v1"
    assert relocated_key =~ "flow:cold:by-segment:v1"
    assert String.starts_with?(original_key, LMDB.cold_by_segment_prefix({:waraft_segment, 10}))
    assert original_key == LMDB.cold_by_segment_key(same_position_new_generation)
    assert original_key == LMDB.cold_by_segment_key({:waraft_segment, 10}, 10)
  end

  test "cold value locator rows round-trip owner generation and locator" do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :value,
        version: 4,
        raft_index: 40,
        file_id: {:waraft_segment, 40},
        offset: 100,
        value_size: 200,
        checksum: <<9>>
      )

    encoded =
      LMDB.encode_cold_value_locator("value-ref", "flow-1", 4, locator,
        ref_kind: :payload,
        expire_at_ms: 1_000,
        checksum: <<8>>
      )

    assert {:ok, fields} = LMDB.decode_cold_value_locator(encoded)
    assert fields.value_ref == "value-ref"
    assert fields.owner_flow_id == "flow-1"
    assert fields.owner_version == 4
    assert fields.locator == locator
    assert fields.ref_kind == :payload
  end

  test "cold value locator decoder rejects trailing external terms" do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :value,
        version: 4,
        raft_index: 40,
        file_id: {:waraft_segment, 40},
        offset: 100,
        value_size: 200,
        checksum: <<9>>
      )

    encoded = LMDB.encode_cold_value_locator("value-ref", "flow-1", 4, locator)
    assert :error = LMDB.decode_cold_value_locator(encoded <> <<0>>)
  end

  test "cold due keys enforce unsigned times and signed priorities" do
    attrs = [
      type: "email",
      state: "waiting",
      partition_key: "tenant-1",
      priority: 0,
      due_at_ms: 60_000,
      flow_id: "flow-1",
      version: 1
    ]

    for invalid_attrs <- [
          Keyword.put(attrs, :due_at_ms, 18_446_744_073_709_551_616),
          Keyword.put(attrs, :version, 18_446_744_073_709_551_616),
          Keyword.put(attrs, :priority, 9_223_372_036_854_775_808),
          Keyword.put(attrs, :priority, -9_223_372_036_854_775_809)
        ] do
      assert_raise ArgumentError, fn -> LMDB.cold_due_key(invalid_attrs) end
    end
  end

  test "cold park rows enforce durable locator and field bounds symmetrically" do
    locator = state_locator()
    oversized = String.duplicate("x", @max_key_bytes + 1)
    oversized_checksum = String.duplicate("c", 65)

    assert_raise ArgumentError, "invalid Flow cold park fields", fn ->
      LMDB.encode_cold_park(locator, type: oversized)
    end

    assert_raise ArgumentError, "invalid Flow cold park fields", fn ->
      LMDB.encode_cold_park(%{locator | file_id: {:flow_state, 0}}, [])
    end

    assert_raise ArgumentError, "invalid Flow cold park fields", fn ->
      LMDB.encode_cold_park(%{locator | checksum: oversized_checksum}, [])
    end

    assert_raise ArgumentError, "invalid Flow cold park fields", fn ->
      LMDB.encode_cold_park(locator, value_refs_digest: String.duplicate("d", 31))
    end

    for overrides <- [
          %{type: oversized},
          %{locator: %{locator | file_id: {:flow_state, 0}}},
          %{locator: %{locator | checksum: oversized_checksum}},
          %{value_refs_digest: String.duplicate("d", 31)}
        ] do
      fields = Map.merge(valid_park_fields(locator), overrides)
      assert :error = LMDB.decode_cold_park(TermCodec.encode({:flow_cold_park, 2, fields}))
    end
  end

  test "cold value rows enforce durable locator and field bounds symmetrically" do
    locator = value_locator()
    oversized = String.duplicate("x", @max_key_bytes + 1)
    oversized_checksum = String.duplicate("c", 65)

    assert_raise ArgumentError, "invalid Flow cold value locator fields", fn ->
      LMDB.encode_cold_value_locator(oversized, "flow-1", 4, locator)
    end

    assert_raise ArgumentError, "invalid Flow cold value locator fields", fn ->
      LMDB.encode_cold_value_locator("value-ref", "flow-1", 4, %{
        locator
        | checksum: oversized_checksum
      })
    end

    assert_raise ArgumentError, "invalid Flow cold value locator fields", fn ->
      LMDB.encode_cold_value_locator("value-ref", "flow-1", 4, locator, ref_kind: :unknown)
    end

    for overrides <- [
          %{value_ref: oversized},
          %{locator: %{locator | checksum: oversized_checksum}},
          %{ref_kind: :unknown},
          %{checksum: oversized_checksum}
        ] do
      fields = Map.merge(valid_value_fields(locator), overrides)

      assert :error =
               LMDB.decode_cold_value_locator(
                 TermCodec.encode({:flow_cold_value_locator, 1, fields})
               )
    end
  end

  test "cold decoders reject oversized blobs before external-term decoding" do
    oversized = String.duplicate("x", @max_encoded_bytes)
    encoded = TermCodec.encode({:other, oversized})
    assert byte_size(encoded) > @max_encoded_bytes

    assert :error = LMDB.decode_cold_park(encoded)
    assert :error = LMDB.decode_cold_value_locator(encoded)
  end

  test "cold park schema validates scheduling fields symmetrically" do
    locator = state_locator(version: 7, raft_index: 70)

    assert_raise ArgumentError, fn ->
      LMDB.encode_cold_park(locator,
        due_at_ms: -1,
        type: "",
        state: "waiting",
        state_key: "state-key"
      )
    end

    corrupt =
      TermCodec.encode(
        {:flow_cold_park, 2,
         %{
           locator: locator,
           due_at_ms: "soon",
           type: "email",
           state: "waiting",
           partition_key: nil,
           state_key: "state-key",
           priority: 0,
           lease_until_ms: nil,
           fencing_token: nil,
           retention_at_ms: nil,
           value_refs_digest: nil,
           state_value: nil,
           checksum: nil
         }}
      )

    assert :error = LMDB.decode_cold_park(corrupt)
  end

  test "cold value locator ownership must match its physical locator" do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :value,
        version: 4,
        raft_index: 40,
        file_id: {:waraft_segment, 40},
        offset: 100,
        value_size: 200,
        checksum: <<9>>
      )

    assert_raise ArgumentError, fn ->
      LMDB.encode_cold_value_locator("value-ref", "other-flow", 4, locator)
    end

    corrupt =
      TermCodec.encode(
        {:flow_cold_value_locator, 1,
         %{
           value_ref: "value-ref",
           owner_flow_id: "other-flow",
           owner_version: 4,
           locator: locator,
           ref_kind: :payload,
           expire_at_ms: 1_000,
           checksum: <<8>>
         }}
      )

    assert :error = LMDB.decode_cold_value_locator(corrupt)
  end

  defp state_locator(overrides \\ []) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: {:waraft_segment, 1},
      offset: 0,
      value_size: 1,
      checksum: <<0>>
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end

  defp value_locator do
    Locator.new!(
      flow_id: "flow-1",
      kind: :value,
      version: 4,
      raft_index: 40,
      file_id: {:waraft_segment, 40},
      offset: 100,
      value_size: 200,
      checksum: <<9>>
    )
  end

  defp valid_park_fields(locator) do
    %{
      locator: locator,
      due_at_ms: 123_456,
      type: "email",
      state: "waiting",
      partition_key: "tenant-1",
      state_key: "flow/state/tenant-1/flow-1",
      priority: 0,
      lease_until_ms: nil,
      fencing_token: nil,
      retention_at_ms: nil,
      value_refs_digest: :crypto.hash(:sha256, "refs"),
      checksum: <<8>>
    }
  end

  defp valid_value_fields(locator) do
    %{
      value_ref: "value-ref",
      owner_flow_id: "flow-1",
      owner_version: 4,
      locator: locator,
      ref_kind: :payload,
      expire_at_ms: 1_000,
      checksum: <<8>>
    }
  end
end
