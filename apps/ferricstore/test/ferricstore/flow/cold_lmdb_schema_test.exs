defmodule Ferricstore.Flow.ColdLMDBSchemaTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.{LMDB, Locator}

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
        fencing_token: "fence",
        retention_at_ms: 999_999,
        value_refs_digest: <<1, 2, 3>>,
        checksum: <<4, 5, 6>>
      )

    assert {:ok, fields} = LMDB.decode_cold_park(encoded)
    assert fields.locator == locator
    assert fields.due_at_ms == 123_456
    assert fields.type == "email"
    assert fields.state == "waiting"
    assert fields.partition_key == "tenant-1"
    assert fields.state_key == "flow/state/tenant-1/flow-1"
    assert fields.priority == 3
    assert fields.fencing_token == "fence"
  end

  test "cold park decoder rejects invalid blobs and invalid locators" do
    assert :not_cold_park = LMDB.decode_cold_park(:erlang.term_to_binary({:other, 1}))
    assert :error = LMDB.decode_cold_park(<<1, 2, 3>>)

    invalid =
      :erlang.term_to_binary({:flow_cold_park, 1, %{locator: %{flow_id: "flow-1"}}})

    assert :not_cold_park = LMDB.decode_cold_park(invalid)
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

  test "reverse segment keys include physical location and logical generation" do
    original = state_locator(version: 1, raft_index: 10, file_id: {:flow_state, 0}, offset: 10)
    relocated = Locator.relocate!(original, file_id: {:flow_state, 1}, offset: 20)

    original_key = LMDB.cold_by_segment_key(original)
    relocated_key = LMDB.cold_by_segment_key(relocated)

    assert original_key != relocated_key
    assert original_key =~ "flow:cold:by-segment:v1"
    assert relocated_key =~ "flow:cold:by-segment:v1"
    assert String.starts_with?(original_key, LMDB.cold_by_segment_prefix({:flow_state, 0}))
  end

  test "cold value locator rows round-trip owner generation and locator" do
    locator =
      Locator.new!(
        flow_id: "flow-1",
        kind: :value,
        version: 4,
        raft_index: 40,
        file_id: {:flow_value, 0},
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

  defp state_locator(overrides) do
    defaults = [
      flow_id: "flow-1",
      kind: :state,
      version: 1,
      raft_index: 1,
      file_id: {:flow_state, 0},
      offset: 0,
      value_size: 1,
      checksum: <<0>>
    ]

    defaults
    |> Keyword.merge(overrides)
    |> Locator.new!()
  end
end
