defmodule Ferricstore.Flow.Query.PageMemoryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Keys

  alias Ferricstore.Flow.Query.{
    CompositeIndex,
    CoveringCodec,
    IndexDefinition,
    Limits,
    MemoryBudget,
    PageMemory
  }

  test "retained page accounting covers decoded maps instead of trusting storage bytes" do
    covering_record =
      Map.new(1..14, fn field -> {String.to_atom("field_#{field}"), "value-#{field}"} end)

    entry = %{
      id: "run-1",
      state_key: "flow:state:tenant:run-1",
      record_version: 1,
      expire_at_ms: 0,
      storage_key: :binary.copy("k", 96),
      storage_bytes: 384,
      covering_record: Map.merge(covering_record, %{id: "run-1", version: 1})
    }

    retained = PageMemory.retained_bytes([entry], entry.storage_bytes)

    assert retained >= MemoryBudget.term_bytes([entry])
    assert retained > entry.storage_bytes + 192
  end

  test "planner estimate includes covering schema expansion" do
    plain = PageMemory.estimated_bytes(512, 256, 0)
    covering = PageMemory.estimated_bytes(512, 256, 16)

    assert covering > plain
    assert covering - plain == 512 * (256 + 16 * 96 + 64)
  end

  test "fused prefetch estimate reserves a QueryRow reference for every possible entry" do
    page = PageMemory.estimated_bytes(512, 256, 16)
    prefetched = PageMemory.estimated_prefetched_bytes(512, 256, 16)

    assert prefetched - page == 512 * 64
  end

  test "constant-time accounting bounds a full cover with the longest valid identity" do
    id = :binary.copy("i", Limits.max_run_id_bytes())

    definition =
      IndexDefinition.new!(%{
        id: "page-memory-full-cover",
        version: 1,
        fields: [{:partition_key, :asc}, {:updated_at_ms, :desc}],
        covering_fields: CoveringCodec.supported_fields()
      })

    record = %{
      id: id,
      version: 1,
      partition_key: "tenant",
      type: "invoice",
      state: "queued",
      priority: 1,
      created_at_ms: 2,
      updated_at_ms: 3,
      next_run_at_ms: 4,
      lease_deadline_ms: 5,
      attempts: 6,
      run_state: "ready",
      max_active_ms: 7,
      parent_flow_id: "parent",
      root_flow_id: "root",
      correlation_id: "correlation"
    }

    state_key = Keys.state_key(id, record.partition_key)
    assert {:ok, [encoded]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert {:ok, decoded} = CompositeIndex.decode_entry_value(encoded.value)

    entry =
      decoded
      |> Map.put(:storage_key, encoded.key)
      |> Map.put(:storage_bytes, byte_size(encoded.key) + byte_size(encoded.value))

    retained = PageMemory.retained_bytes([entry], entry.storage_bytes)

    estimated =
      PageMemory.estimated_bytes(1, entry.storage_bytes, length(definition.covering_fields))

    assert retained >= MemoryBudget.term_bytes([entry])
    assert estimated >= retained
  end
end
