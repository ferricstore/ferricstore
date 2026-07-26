defmodule Ferricstore.Flow.ProjectionLocatorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, Locator, ProjectionLocator, StorageScope}

  test "builds the canonical locator for Bitcask and WARaft sources" do
    state_key = Keys.state_key("run-1", "tenant-a")
    encoded = Ferricstore.Flow.encode_record(record())
    checksum = :crypto.hash(:sha256, encoded)

    assert {:ok, decoded,
            %Locator{
              flow_id: "run-1",
              kind: :state,
              version: 7,
              raft_index: 7,
              file_id: 3,
              offset: 128,
              value_size: 512,
              checksum: ^checksum,
              expire_at_ms: 9_000
            }} = ProjectionLocator.decode_source(state_key, encoded, 9_000, {3, 128, 512})

    assert decoded.id == "run-1"
    assert decoded.version == 7
    assert decoded.partition_key == "tenant-a"

    assert {:ok, _decoded,
            %Locator{
              raft_index: 99,
              file_id: {:waraft_apply_projection, 99},
              checksum: ^checksum
            }} =
             ProjectionLocator.decode_source(
               state_key,
               encoded,
               0,
               {{:waraft_apply_projection, 99}, 0, 512}
             )
  end

  test "rejects mismatched identities and non-durable source locations" do
    state_key = Keys.state_key("run-1", "tenant-a")
    encoded = Ferricstore.Flow.encode_record(record())

    assert {:error, :source_flow_identity_mismatch} =
             ProjectionLocator.decode_source(
               Keys.state_key("run-1", "tenant-b"),
               encoded,
               0,
               {3, 0, 512}
             )

    assert {:error, {:source_location_unavailable, "run-1"}} =
             ProjectionLocator.decode_source(state_key, encoded, 0, {:memory, 0, 512})

    assert {:error, {:source_location_unavailable, "run-1"}} =
             ProjectionLocator.decode_source(state_key, encoded, 0, {3, 0, 0})

    assert {:stale, 7} =
             ProjectionLocator.decode_source_at_least(
               state_key,
               encoded,
               8,
               0,
               {:memory, 0, 512}
             )

    assert {:error, :invalid_source_flow_record} =
             ProjectionLocator.decode_source(state_key, "not-a-record", 0, {3, 0, 512})
  end

  test "binds shared records through sealed physical scope metadata" do
    logical_partition = "tenant-a"
    correct_metadata = scope_metadata(42)
    forged_metadata = scope_metadata(99)

    assert {:ok, physical_partition} =
             StorageScope.physical_partition_key(
               logical_partition,
               <<42::unsigned-big-64>>
             )

    state_key = Keys.state_key("run-shared", physical_partition)

    record =
      record("run-shared", physical_partition)
      |> Map.put(:system_metadata, correct_metadata)

    encoded = Ferricstore.Flow.encode_record(record)

    assert {:ok, decoded, %Locator{flow_id: "run-shared", version: 7}} =
             ProjectionLocator.decode_source(state_key, encoded, 0, {3, 128, 512})

    assert decoded.system_metadata == correct_metadata

    assert {:error, :source_flow_identity_mismatch} =
             ProjectionLocator.decode_source(
               state_key,
               Ferricstore.Flow.encode_record(%{record | system_metadata: forged_metadata}),
               0,
               {3, 128, 512}
             )
  end

  defp record(id \\ "run-1", partition_key \\ "tenant-a") do
    %{
      id: id,
      version: 7,
      type: "job",
      state: "queued",
      partition_key: partition_key,
      created_at_ms: 100,
      updated_at_ms: 101,
      next_run_at_ms: 1_000,
      priority: 0,
      attempts: 0,
      payload_ref: nil,
      result_ref: nil,
      error_ref: nil
    }
  end

  defp scope_metadata(value), do: %{0x8001 => {1, :uint64, :isolation_scope, value}}
end
