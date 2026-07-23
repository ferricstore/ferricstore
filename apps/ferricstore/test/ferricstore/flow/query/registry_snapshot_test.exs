defmodule Ferricstore.Flow.Query.RegistrySnapshotTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{IndexDefinition, RegisteredIndex, RegistrySnapshot}

  test "holds a full catalog generation alongside its retiring generation" do
    indexes =
      Enum.flat_map(1..16, fn ordinal ->
        [
          registered(ordinal, 2, :active),
          registered(ordinal, 1, :retiring)
        ]
      end)

    assert {:ok, %RegistrySnapshot{indexes: ^indexes}} =
             RegistrySnapshot.new(epoch: 2, catalog_version: 2, indexes: indexes)
  end

  test "rejects forged registered definitions and unbounded generation counters" do
    valid = registered(1, 1, :active)

    forged_definition = %{
      valid.definition
      | fields: [{:state, :asc, :hashed} | tl(valid.definition.fields)]
    }

    forged = %{valid | definition: forged_definition}

    assert {:error, :invalid_query_index_snapshot} =
             RegistrySnapshot.new(epoch: 1, catalog_version: 1, indexes: [forged])

    assert {:error, :invalid_query_index_snapshot} =
             RegistrySnapshot.new(
               epoch: 0x1_0000_0000_0000_0000,
               catalog_version: 1,
               indexes: []
             )
  end

  test "registered-index construction rejects a forged definition" do
    valid = registered(1, 1, :active)
    forged = %{valid.definition | fingerprint: <<0::256>>}

    assert {:error, :invalid_registered_index} = RegisteredIndex.new(forged, :active)
  end

  defp registered(ordinal, version, state) do
    definition =
      IndexDefinition.new!(%{
        id: "index-#{ordinal}",
        version: version,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}]
      })

    RegisteredIndex.new!(definition, state)
  end
end
