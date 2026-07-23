defmodule Ferricstore.Flow.Query.IndexDefinitionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.IndexDefinition

  test "validates a tenant-prefixed composite definition and gives it a stable fingerprint" do
    attrs = %{
      id: "runs_by_type_state_updated",
      version: 3,
      workloads: ["WF-LIST-001", "WF-SERVICE-API-001"],
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    }

    assert {:ok, definition} = IndexDefinition.new(attrs)
    assert definition.source == :runs
    assert definition.id == attrs.id
    assert definition.workloads == attrs.workloads
    assert definition.scope_bytes == 0
    assert definition.count_prefixes == []

    assert definition.fields == [
             {:partition_key, :asc, :hashed},
             {:type, :asc, :hashed},
             {:state, :asc, :hashed},
             {:updated_at_ms, :desc, :ordered}
           ]

    assert byte_size(definition.fingerprint) == 32
    assert {:ok, same} = IndexDefinition.new(attrs)
    assert same.fingerprint == definition.fingerprint

    assert {:ok, changed} =
             IndexDefinition.new(%{
               attrs
               | fields: List.replace_at(attrs.fields, 3, {:updated_at_ms, :asc})
             })

    refute changed.fingerprint == definition.fingerprint

    assert {:ok, shared} = IndexDefinition.new(Map.put(attrs, :scope_bytes, 8))
    assert shared.scope_bytes == 8
    refute shared.fingerprint == definition.fingerprint
  end

  test "validates canonical exact-counter prefixes as part of the physical fingerprint" do
    attrs = %{
      id: "runs_by_type_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc},
        {:type, :asc},
        {:state, :asc},
        {:updated_at_ms, :desc}
      ]
    }

    assert {:ok, counted} = IndexDefinition.new(Map.put(attrs, :count_prefixes, [3, 1, 2]))
    assert counted.count_prefixes == [1, 2, 3]

    assert {:ok, uncounted} = IndexDefinition.new(attrs)
    refute counted.fingerprint == uncounted.fingerprint

    for invalid <- [[0], [4], [5], [1, 1], ["2"], :all] do
      assert {:error, :invalid_index_count_prefixes} =
               IndexDefinition.new(Map.put(attrs, :count_prefixes, invalid))
    end

    assert {:error, :invalid_index_count_prefixes} =
             IndexDefinition.new(Map.put(attrs, :count_prefixes, [4]))
  end

  test "validates bounded workload identifiers without changing the physical key fingerprint" do
    base = %{
      id: "runs_by_state",
      version: 1,
      fields: [{:partition_key, :asc}, {:state, :asc}]
    }

    assert {:ok, first} = IndexDefinition.new(Map.put(base, :workloads, ["WF-LIST-001"]))

    assert {:ok, second} =
             IndexDefinition.new(Map.put(base, :workloads, ["WF-SERVICE-API-001"]))

    assert first.fingerprint == second.fingerprint

    assert {:error, :duplicate_index_workload} =
             IndexDefinition.new(Map.put(base, :workloads, ["WF-LIST-001", "WF-LIST-001"]))

    assert {:error, :invalid_index_workload} =
             IndexDefinition.new(Map.put(base, :workloads, ["bad workload"]))
  end

  test "requires tenant scope as the leading ascending field" do
    assert {:error, :tenant_field_must_lead} =
             IndexDefinition.new(%{
               id: "unsafe",
               version: 1,
               fields: [{:state, :asc}, {:updated_at_ms, :desc}]
             })

    assert {:error, :tenant_field_must_lead} =
             IndexDefinition.new(%{
               id: "unsafe_desc",
               version: 1,
               fields: [{:partition_key, :desc}, {:state, :asc}]
             })
  end

  test "bounds definitions and rejects duplicate or unsafe fields" do
    assert {:error, :duplicate_index_field} =
             IndexDefinition.new(%{
               id: "duplicate",
               version: 1,
               fields: [{:partition_key, :asc}, {:state, :asc}, {:state, :desc}]
             })

    assert {:error, :too_many_multivalue_fields} =
             IndexDefinition.new(%{
               id: "fanout",
               version: 1,
               fields: [
                 {:partition_key, :asc},
                 {{:attribute, "region"}, :asc},
                 {{:attribute, "tags"}, :asc}
               ]
             })

    assert {:error, :invalid_index_id} =
             IndexDefinition.new(%{
               id: String.duplicate("x", 65),
               version: 1,
               fields: [{:partition_key, :asc}, {:state, :asc}]
             })

    assert {:error, :invalid_index_version} =
             IndexDefinition.new(%{
               id: "bad_version",
               version: 0,
               fields: [{:partition_key, :asc}, {:state, :asc}]
             })
  end

  test "requires hash encoding for unbounded keyword fields and ascending hash components" do
    assert {:error, :unbounded_ordered_index_field} =
             IndexDefinition.new(%{
               id: "ordered_type",
               version: 1,
               fields: [
                 {:partition_key, :asc, :hashed},
                 {:type, :asc, :ordered}
               ]
             })

    assert {:error, :invalid_hashed_index_direction} =
             IndexDefinition.new(%{
               id: "descending_hash",
               version: 1,
               fields: [
                 {:partition_key, :asc, :hashed},
                 {:state, :desc, :hashed}
               ]
             })
  end

  test "shares exact identity validation with management projections" do
    assert IndexDefinition.valid_id?("index-a:v1.test")
    assert IndexDefinition.valid_id?(String.duplicate("x", 64))

    refute IndexDefinition.valid_id?("")
    refute IndexDefinition.valid_id?(String.duplicate("x", 65))
    refute IndexDefinition.valid_id?("contains space")
    refute IndexDefinition.valid_id?("contains/slash")
    refute IndexDefinition.valid_id?(<<0>>)

    assert IndexDefinition.valid_version?(1)
    assert IndexDefinition.valid_version?(0xFFFF_FFFF_FFFF_FFFF)
    refute IndexDefinition.valid_version?(0)
    refute IndexDefinition.valid_version?(0x1_0000_0000_0000_0000)
  end

  test "allowed definitions have a proven worst-case LMDB key below 512 bytes" do
    assert {:ok, definition} =
             IndexDefinition.new(%{
               id: String.duplicate("i", 64),
               version: 1,
               fields: [
                 {:partition_key, :asc, :hashed},
                 {{:attribute, "large"}, :asc, :hashed},
                 {:type, :asc, :hashed},
                 {:state, :asc, :hashed},
                 {:run_state, :asc, :hashed},
                 {:correlation_id, :asc, :hashed},
                 {:parent_flow_id, :asc, :hashed},
                 {:root_flow_id, :asc, :hashed}
               ]
             })

    assert IndexDefinition.max_entry_key_bytes(definition) <= 511

    assert {:ok, scoped_definition} =
             definition
             |> Map.from_struct()
             |> Map.put(:scope_bytes, 8)
             |> IndexDefinition.new()

    assert IndexDefinition.max_entry_key_bytes(scoped_definition) ==
             IndexDefinition.max_entry_key_bytes(definition) + 11
  end

  test "rejects invalid hidden scope widths before computing a key layout" do
    attrs = %{
      id: "runs_by_state",
      version: 1,
      fields: [{:partition_key, :asc}, {:state, :asc}]
    }

    for invalid <- [-1, 257, "8"] do
      assert {:error, :invalid_index_scope_bytes} =
               attrs |> Map.put(:scope_bytes, invalid) |> IndexDefinition.new()
    end
  end

  test "definition storage prefix is versioned, fixed-width, and isolated from identifier delimiters" do
    assert {:ok, first} =
             IndexDefinition.new(%{
               id: "a:b",
               version: 1,
               fields: [{:partition_key, :asc}, {:state, :asc}]
             })

    assert {:ok, second} =
             IndexDefinition.new(%{
               id: "a",
               version: 1,
               fields: [{:partition_key, :asc}, {:state, :asc}]
             })

    assert byte_size(IndexDefinition.storage_prefix(first)) ==
             byte_size(IndexDefinition.storage_prefix(second))

    refute IndexDefinition.storage_prefix(first) == IndexDefinition.storage_prefix(second)
  end
end
