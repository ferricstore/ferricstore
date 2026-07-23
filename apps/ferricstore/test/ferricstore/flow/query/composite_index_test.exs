defmodule Ferricstore.Flow.Query.CompositeIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, StorageScope, SystemMetadata}
  alias Ferricstore.Flow.Query.{CompositeIndex, Field, IndexDefinition, Limits}
  alias Ferricstore.TermCodec

  @max_exact_integer 9_007_199_254_740_991

  setup do
    {:ok, definition} =
      IndexDefinition.new(%{
        id: "runs_by_type_state_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {:type, :asc},
          {:state, :asc},
          {:updated_at_ms, :desc}
        ]
      })

    %{definition: definition}
  end

  test "projects a tenant-isolated, ordered, bounded LMDB entry", %{definition: definition} do
    record = record("run-1", "tenant-secret", 100)
    state_key = Keys.state_key("run-1", "tenant-secret")

    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 5_000)
    assert byte_size(entry.key) <= 511
    refute entry.key =~ "tenant-secret"
    refute entry.key =~ "invoice"
    assert String.starts_with?(entry.key, IndexDefinition.storage_prefix(definition))

    assert {:ok, value} = CompositeIndex.decode_entry_value(entry.value)
    assert value.id == "run-1"
    assert value.state_key == state_key
    assert value.record_version == 3
    assert value.expire_at_ms == 5_000
    assert CompositeIndex.entry_key_matches_record?(definition, record, state_key, entry.key)

    assert CompositeIndex.entry_key_matches_record_validated?(
             definition,
             record,
             state_key,
             entry.key
           )

    assert {:ok, matcher} =
             CompositeIndex.prepare_record_matcher_validated(
               definition,
               nil,
               "tenant-secret"
             )

    assert CompositeIndex.entry_key_matches_record_validated?(matcher, record, entry.key)

    refute CompositeIndex.entry_key_matches_record?(
             definition,
             record("run-1", "tenant-secret", 99),
             state_key,
             entry.key
           )

    refute CompositeIndex.entry_key_matches_record_validated?(
             definition,
             record("run-1", "tenant-secret", 99),
             state_key,
             entry.key
           )

    refute CompositeIndex.entry_key_matches_record_validated?(
             matcher,
             record("run-1", "tenant-secret", 99),
             entry.key
           )
  end

  test "entry values use the bounded compact codec and reject the pre-beta ETF shape", %{
    definition: definition
  } do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, [entry]} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               state_key,
               5_000
             )

    assert entry.value ==
             <<1, 5::unsigned-big-32, 3::unsigned-big-64, 5_000::unsigned-big-64, "run-1",
               state_key::binary>>

    old_etf =
      TermCodec.encode({:flow_composite_entry, 1, "run-1", state_key, 3, 5_000})

    assert :error = CompositeIndex.decode_entry_value(old_etf)

    for invalid <- [
          <<1>>,
          <<1, 0::unsigned-big-32, 3::unsigned-big-64, 0::unsigned-big-64, state_key::binary>>,
          <<1, 5::unsigned-big-32, 3::unsigned-big-64, 0::unsigned-big-64, "run-1">>,
          <<2, 5::unsigned-big-32, 3::unsigned-big-64, 0::unsigned-big-64, "run-1",
            state_key::binary>>
        ] do
      assert :error = CompositeIndex.decode_entry_value(invalid)
    end
  end

  test "shared layout derives its hidden prefix from sealed metadata, not index values", %{
    definition: dedicated_definition
  } do
    shared_definition =
      dedicated_definition
      |> Map.from_struct()
      |> Map.put(:scope_bytes, 8)
      |> IndexDefinition.new!()

    metadata = %{0x8001 => {1, :uint64, :isolation_scope, 11}}

    logical =
      record("run-1", "same-logical-partition", 100)
      |> SystemMetadata.put_record(metadata)

    assert {:ok, physical_partition} = StorageScope.physical_partition_key(logical)
    scoped_record = Map.put(logical, :partition_key, physical_partition)
    state_key = Keys.state_key("run-1", physical_partition)

    assert {:ok, [entry]} =
             CompositeIndex.entries(shared_definition, scoped_record, state_key, 0)

    assert {:ok, hidden_prefix} =
             CompositeIndex.encode_prefix(
               shared_definition,
               <<11::unsigned-big-64>>,
               ["same-logical-partition"]
             )

    assert String.starts_with?(entry.key, hidden_prefix)
    refute entry.key =~ "same-logical-partition"

    assert {:error, :invalid_composite_scope} =
             CompositeIndex.encode_prefix(shared_definition, ["same-logical-partition"])

    assert {:error, :invalid_composite_scope} =
             CompositeIndex.entries(
               dedicated_definition,
               scoped_record,
               state_key,
               0
             )

    assert {:error, :invalid_composite_scope} =
             CompositeIndex.entries(
               shared_definition,
               record("run-1", "same-logical-partition", 100),
               Keys.state_key("run-1", "same-logical-partition"),
               0
             )
  end

  test "supports valid long Flow IDs without expanding physical index keys", %{
    definition: definition
  } do
    id = String.duplicate("r", 60_000)
    state_key = Keys.state_key(id, "tenant-a")

    assert byte_size(state_key) > 511

    assert {:ok, [entry]} =
             CompositeIndex.entries(definition, record(id, "tenant-a", 100), state_key, 0)

    assert byte_size(entry.key) <= 511

    assert {:ok, %{id: ^id, state_key: ^state_key}} =
             CompositeIndex.decode_entry_value(entry.value)
  end

  test "rejects a state locator that does not own the projected record", %{
    definition: definition
  } do
    foreign_state_key = Keys.state_key("run-2", "tenant-a")

    assert {:error, :invalid_composite_record} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               foreign_state_key,
               0
             )
  end

  test "descending ordered fields sort newer records first", %{definition: definition} do
    assert {:ok, [older]} =
             CompositeIndex.entries(
               definition,
               record("older", "tenant-a", 100),
               Keys.state_key("older", "tenant-a"),
               0
             )

    assert {:ok, [newer]} =
             CompositeIndex.entries(
               definition,
               record("newer", "tenant-a", 200),
               Keys.state_key("newer", "tenant-a"),
               0
             )

    assert newer.key < older.key
  end

  test "a multivalue attribute emits one deduplicated entry per member" do
    {:ok, definition} =
      IndexDefinition.new(%{
        id: "runs_by_tag_updated",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tags"}, :asc, :hashed},
          {:updated_at_ms, :desc}
        ]
      })

    record =
      record("run-1", "tenant-a", 100)
      |> Map.put(:attributes, %{"tags" => ["urgent", "finance", "urgent"]})

    assert {:ok, entries} =
             CompositeIndex.entries(
               definition,
               record,
               Keys.state_key("run-1", "tenant-a"),
               0
             )

    assert length(entries) == 2
    assert length(Enum.uniq_by(entries, & &1.key)) == 2

    assert Enum.all?(entries, fn entry ->
             CompositeIndex.entry_key_matches_record?(
               definition,
               record,
               Keys.state_key("run-1", "tenant-a"),
               entry.key
             )
           end)
  end

  test "rejects too many multi-value members before materializing an oversized projection" do
    definition =
      IndexDefinition.new!(%{
        id: "runs_by_tag",
        version: 1,
        fields: [
          {:partition_key, :asc},
          {{:attribute, "tags"}, :asc, :hashed},
          {:updated_at_ms, :desc}
        ]
      })

    record =
      record("run-1", "tenant-a", 100)
      |> Map.put(:attributes, %{
        "tags" => Enum.map(1..129, &"tag-#{&1}")
      })

    assert {:error, :too_many_composite_entries} =
             CompositeIndex.entries(
               definition,
               record,
               Keys.state_key("run-1", "tenant-a"),
               0
             )
  end

  test "publishes the projection cardinality ceiling for bounded lifecycle planning" do
    assert CompositeIndex.max_entries_per_record() == 128
  end

  test "missing and explicit null components are distinct" do
    {:ok, definition} =
      IndexDefinition.new(%{
        id: "runs_by_priority",
        version: 1,
        fields: [{:partition_key, :asc}, {:priority, :asc}]
      })

    missing = record("missing", "tenant-a", 100) |> Map.delete(:priority)
    null = record("null", "tenant-a", 100) |> Map.put(:priority, nil)

    assert {:ok, [missing_entry]} =
             CompositeIndex.entries(
               definition,
               missing,
               Keys.state_key("missing", "tenant-a"),
               0
             )

    assert {:ok, [null_entry]} =
             CompositeIndex.entries(
               definition,
               null,
               Keys.state_key("null", "tenant-a"),
               0
             )

    refute missing_entry.key == null_entry.key

    assert {:ok, missing_prefix} =
             CompositeIndex.encode_prefix(definition, ["tenant-a", Field.missing()])

    assert String.starts_with?(missing_entry.key, missing_prefix)
  end

  test "rejects values that do not match a built-in field type", %{definition: definition} do
    malformed = record("run-1", "tenant-a", 100) |> Map.put(:updated_at_ms, "100")

    assert {:error, :invalid_index_value_type} =
             CompositeIndex.entries(
               definition,
               malformed,
               Keys.state_key("run-1", "tenant-a"),
               0
             )

    assert {:error, :invalid_index_value_type} =
             CompositeIndex.encode_prefix(
               definition,
               ["tenant-a", "invoice", "failed", "100"]
             )
  end

  test "rejects forged definitions before deriving physical keys", %{definition: definition} do
    forged = %{definition | fingerprint: <<0::256>>}
    malformed = %{definition | fingerprint: <<0>>}
    record = record("run-1", "tenant-a", 100)

    for invalid <- [forged, malformed] do
      assert {:error, :invalid_index_definition} =
               CompositeIndex.entries(
                 invalid,
                 record,
                 Keys.state_key("run-1", "tenant-a"),
                 0
               )

      assert {:error, :invalid_index_definition} =
               CompositeIndex.encode_prefix(invalid, ["tenant-a"])
    end
  end

  test "entry identity binds the projected row to its key", %{definition: definition} do
    assert {:ok, [entry]} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               Keys.state_key("run-1", "tenant-a"),
               0
             )

    assert CompositeIndex.entry_key_matches_id?(entry.key, "run-1")
    refute CompositeIndex.entry_key_matches_id?(entry.key, "run-2")
  end

  test "rejects record versions outside the authoritative record codec domain", %{
    definition: definition
  } do
    oversized = record("run-1", "tenant-a", 100) |> Map.put(:version, @max_exact_integer + 1)

    assert {:error, :invalid_composite_record} =
             CompositeIndex.entries(
               definition,
               oversized,
               Keys.state_key("run-1", "tenant-a"),
               0
             )

    state_key = Keys.state_key("run-1", "tenant-a")

    encoded =
      TermCodec.encode({
        :flow_composite_entry,
        1,
        "run-1",
        state_key,
        @max_exact_integer + 1,
        0
      })

    assert :error = CompositeIndex.decode_entry_value(encoded)
  end

  test "rejects oversized entry identities before encoding or persisted lookup", %{
    definition: definition
  } do
    oversized_id = String.duplicate("r", Limits.max_run_id_bytes() + 1)
    oversized_state_key = String.duplicate("k", Limits.max_state_key_bytes() + 1)

    assert {:error, :invalid_composite_record} =
             CompositeIndex.entries(
               definition,
               record(oversized_id, "tenant-a", 100),
               Keys.state_key(oversized_id, "tenant-a"),
               0
             )

    assert {:error, :invalid_composite_record} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               oversized_state_key,
               0
             )

    for {id, state_key} <- [
          {oversized_id, Keys.state_key(oversized_id, "tenant-a")},
          {"run-1", oversized_state_key}
        ] do
      encoded = TermCodec.encode({:flow_composite_entry, 1, id, state_key, 1, 0})
      assert :error = CompositeIndex.decode_entry_value(encoded)
    end

    reverse_key =
      CompositeIndex.reverse_prefix() <> :crypto.hash(:sha256, oversized_state_key)

    reverse_value =
      TermCodec.encode({
        :flow_composite_reverse,
        1,
        oversized_state_key,
        [IndexDefinition.global_storage_prefix() <> "entry"]
      })

    assert :error = CompositeIndex.decode_reverse_row(reverse_key, reverse_value)
  end

  test "unscoped or malformed records fail without creating a shared tenant range", %{
    definition: definition
  } do
    assert {:error, :unscoped_record} =
             CompositeIndex.entries(
               definition,
               record("run-1", nil, 100),
               Keys.state_key("run-1"),
               0
             )

    assert {:error, :unscoped_record} =
             CompositeIndex.entries(definition, %{id: "run-1"}, Keys.state_key("run-1"), 0)
  end

  test "reverse metadata is bounded and validates ownership", %{definition: definition} do
    state_key = Keys.state_key("run-1", "tenant-a")

    assert {:ok, entries} =
             CompositeIndex.entries(
               definition,
               record("run-1", "tenant-a", 100),
               state_key,
               0
             )

    keys = Enum.map(entries, & &1.key)
    reverse_key = CompositeIndex.reverse_key(state_key)
    reverse_value = CompositeIndex.encode_reverse_value(state_key, keys, 5_000)
    other_state_key = Keys.state_key("run-2", "tenant-a")

    assert byte_size(reverse_key) == byte_size(CompositeIndex.reverse_key(other_state_key))
    assert {:ok, ^keys} = CompositeIndex.decode_reverse_value(reverse_value, state_key)

    assert {:ok, %{keys: ^keys, expire_at_ms: 5_000}} =
             CompositeIndex.decode_reverse_state(reverse_value, state_key)

    assert {:ok, {^state_key, ^keys, 5_000}} =
             CompositeIndex.decode_reverse_row(reverse_key, reverse_value)

    assert :error =
             CompositeIndex.decode_reverse_row(
               CompositeIndex.reverse_key(other_state_key),
               reverse_value
             )

    assert :error = CompositeIndex.decode_reverse_value(reverse_value, other_state_key)
    assert :error = CompositeIndex.decode_reverse_value("corrupt", state_key)

    assert {:ok, foreign_entries} =
             CompositeIndex.entries(
               definition,
               record("run-2", "tenant-a", 100),
               other_state_key,
               0
             )

    foreign_keys = Enum.map(foreign_entries, & &1.key)

    assert_raise ArgumentError, fn ->
      CompositeIndex.encode_reverse_value(state_key, foreign_keys)
    end

    forged_reverse =
      TermCodec.encode({:flow_composite_reverse, 1, state_key, foreign_keys})

    assert :error = CompositeIndex.decode_reverse_value(forged_reverse, state_key)
  end

  test "rejects oversized persisted terms before external-term allocation" do
    oversized = TermCodec.encode(Enum.to_list(1..200_000))
    state_key = Keys.state_key("run-1", "tenant-a")

    for decode <- [
          fn -> CompositeIndex.decode_entry_value(oversized) end,
          fn -> CompositeIndex.decode_reverse_value(oversized, state_key) end,
          fn ->
            CompositeIndex.decode_reverse_row(CompositeIndex.reverse_key(state_key), oversized)
          end
        ] do
      parent = self()

      {_pid, monitor} =
        :erlang.spawn_opt(
          fn -> send(parent, {:decoded_oversized_term, self(), decode.()}) end,
          [:monitor, {:max_heap_size, %{size: 50_000, kill: true, error_logger: false}}]
        )

      assert_receive {:decoded_oversized_term, child, :error}, 1_000
      assert_receive {:DOWN, ^monitor, :process, ^child, :normal}, 1_000
    end
  end

  defp record(id, partition_key, updated_at_ms) do
    %{
      id: id,
      partition_key: partition_key,
      type: "invoice",
      state: "failed",
      version: 3,
      priority: 1,
      updated_at_ms: updated_at_ms
    }
  end
end
