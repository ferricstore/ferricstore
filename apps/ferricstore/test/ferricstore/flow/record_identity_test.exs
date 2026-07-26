defmodule Ferricstore.Flow.RecordIdentityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, RecordIdentity, StorageScope}

  test "accepts unscoped and sealed shared state ownership" do
    unscoped = %{id: "plain", partition_key: "tenant-a"}
    assert RecordIdentity.owns_state_key?(unscoped, Keys.state_key("plain", "tenant-a"))

    scope = <<42::unsigned-big-64>>
    assert {:ok, physical_partition} = StorageScope.physical_partition_key("tenant-a", scope)

    shared = %{
      id: "shared",
      partition_key: physical_partition,
      system_metadata: scope_metadata(42)
    }

    assert RecordIdentity.owns_state_key?(
             shared,
             Keys.state_key("shared", physical_partition)
           )
  end

  test "rejects foreign sealed scope and malformed identity without raising" do
    scope = <<42::unsigned-big-64>>
    assert {:ok, physical_partition} = StorageScope.physical_partition_key("tenant-a", scope)
    state_key = Keys.state_key("shared", physical_partition)

    forged = %{
      id: "shared",
      partition_key: physical_partition,
      system_metadata: scope_metadata(99)
    }

    refute RecordIdentity.owns_state_key?(forged, state_key)
    refute RecordIdentity.owns_state_key?(%{id: "shared", partition_key: %{}}, state_key)
    refute RecordIdentity.owns_state_key?(%{partition_key: "tenant-a"}, state_key)
    refute RecordIdentity.owns_state_key?(forged, :not_a_key)
  end

  defp scope_metadata(value), do: %{0x8001 => {1, :uint64, :isolation_scope, value}}
end
