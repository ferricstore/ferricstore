defmodule Ferricstore.Flow.Query.IndexValidationTest do
  use ExUnit.Case, async: true

  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    BackfillSource,
    CompositeCounter,
    CompositeIndex,
    CompositeProjection,
    IndexDefinition,
    SourceCatalog
  }

  alias Ferricstore.Flow.Query.IndexValidation

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_index_validation_#{suffix}")
    keydir = :ets.new(:query_validation_keydir, [:set, :public])

    ctx = %{
      name: :validation_test,
      data_dir: data_dir,
      shard_count: 1,
      keydir_refs: {keydir},
      max_value_size: 1_048_576
    }

    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{ctx: ctx, keydir: keydir, definition: definition()}
  end

  test "proves source coverage and physical ownership in bounded resumable passes", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "validation-build"
    {state_key, record} = project_record!(ctx, keydir, build_id, definition)

    source_done =
      finish_source_validation!(
        ctx,
        build_id,
        definition,
        IndexValidation.empty_checkpoint()
      )

    assert source_done.phase == :index
    assert source_done.checked_records == 1
    assert source_done.checked_entries == 1
    assert source_done.mismatches == 0

    assert {:ok, index_done} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               source_done,
               8,
               2 * 1_024 * 1_024
             )

    assert index_done.phase == :counter
    assert index_done.checked_entries == 2
    assert index_done.mismatches == 0

    assert {:ok, validation_done} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               index_done,
               8,
               2 * 1_024 * 1_024
             )

    assert validation_done.phase == :cleanup
    assert validation_done.checked_entries == 2

    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    expected_value = entry.value
    assert {:ok, ^expected_value} = LMDB.get(lmdb_path(ctx), entry.key)
  end

  test "validates each definition once per index page instead of once per row", context do
    %{ctx: ctx, keydir: keydir} = context
    definition = count_definition()
    build_id = "definition-validation-count"

    Enum.each(1..3, fn index ->
      record = record("run-#{index}", "tenant-secret", index)
      state_key = Keys.state_key(record.id, record.partition_key)
      :ets.insert(keydir, {state_key, <<>>, 0, 0, 0, 0, 0})
      catalog_key = Keys.type_catalog_member_key(record.type, state_key)
      assert {:ok, catalog_op} = SourceCatalog.put_op(catalog_key, state_key)
      assert :ok = LMDB.write_batch(lmdb_path(ctx), [catalog_op])
      put_projected_record!(ctx, definition, record)
    end)

    snapshot_all!(ctx, build_id)

    assert {:ok, source_done} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024
             )

    test_pid = self()

    observe_definition = fn candidate ->
      send(test_pid, {:definition_validated, candidate.id})
      :ok
    end

    assert {:ok, index_page} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               source_done,
               8,
               2 * 1_024 * 1_024,
               definition_validation_observer_fun: observe_definition
             )

    assert index_page.checked_entries > source_done.checked_entries
    definition_id = definition.id
    assert_receive {:definition_validated, ^definition_id}
    refute_receive {:definition_validated, _index_id}
  end

  test "rejects a corrupt exact counter before activating its index", context do
    %{ctx: ctx, keydir: keydir} = context
    definition = count_definition()
    build_id = "corrupt-counter-build"
    {state_key, record} = project_record!(ctx, keydir, build_id, definition)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)

    assert {:ok, prefixes} = CompositeCounter.prefixes_for_keys([definition], [entry.key])
    [{^definition, counter_prefix}] = MapSet.to_list(prefixes)
    counter_key = CompositeCounter.key(definition, counter_prefix)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, counter_key, CompositeCounter.encode_value(counter_prefix, 2)}
             ])

    assert {:ok, source_done} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024
             )

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               source_done,
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :composite_counter_physical_mismatch
    assert evidence.mismatches == 1
  end

  test "resumes exact counter aggregation without rescanning prior index pages", context do
    %{ctx: ctx} = context
    definition = count_definition()

    Enum.each(["run-a", "run-b"], fn id ->
      put_projected_record!(ctx, definition, record(id, "tenant-secret", 4))
    end)

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:ok, first_page} =
             IndexValidation.step(
               ctx,
               0,
               "resumable-counter-build",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024
             )

    assert first_page.phase == :index
    assert [%{count: 1, expected_count: 2}] = first_page.counter_runs

    assert {:ok, index_done} =
             IndexValidation.step(
               ctx,
               0,
               "resumable-counter-build",
               [definition],
               first_page,
               1,
               2 * 1_024 * 1_024
             )

    assert index_done.phase == :counter
    assert index_done.counter_runs == []
  end

  test "counts one record across trailing multivalue index fanout when validation resumes",
       context do
    %{ctx: ctx} = context

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_tenant_tag_count",
        version: 1,
        count_prefixes: [1, 2],
        fields: [
          {:partition_key, :asc, :hashed},
          {{:attribute, "tags"}, :asc, :hashed},
          {:updated_at_ms, :desc, :ordered}
        ]
      })

    record =
      record("run-multivalue", "tenant-secret", 4)
      |> Map.put(:attributes, %{"tags" => ["blue", "green"]})

    put_projected_record!(ctx, definition, record)
    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:ok, first_page} =
             IndexValidation.step(
               ctx,
               0,
               "multivalue-counter-build",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024
             )

    assert first_page.phase == :index

    assert {:ok, index_done} =
             IndexValidation.step(
               ctx,
               0,
               "multivalue-counter-build",
               [definition],
               first_page,
               1,
               2 * 1_024 * 1_024
             )

    assert index_done.phase == :counter
    assert index_done.counter_runs == []
  end

  test "accepts trailing multivalue counter growth behind a resumable validation cursor",
       context do
    %{ctx: ctx} = context

    definition =
      IndexDefinition.new!(%{
        id: "runs_by_tenant_count_with_tag_fanout",
        version: 1,
        count_prefixes: [1],
        fields: [
          {:partition_key, :asc, :hashed},
          {{:attribute, "tags"}, :asc, :hashed},
          {:updated_at_ms, :desc, :ordered}
        ]
      })

    initial_record =
      record("run-initial", "tenant-secret", 4)
      |> Map.put(:attributes, %{"tags" => ["blue", "green"]})

    put_projected_record!(ctx, definition, initial_record)
    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:ok, first_page} =
             IndexValidation.step(
               ctx,
               0,
               "concurrent-multivalue-counter-build",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024
             )

    cursor = first_page.cursor

    concurrent_record =
      Enum.find_value(1..10_000, fn suffix ->
        candidate =
          record("concurrent-multivalue-#{suffix}", "tenant-secret", 4)
          |> Map.put(:attributes, %{"tags" => ["blue", "green"]})

        state_key = Keys.state_key(candidate.id, candidate.partition_key)
        assert {:ok, entries} = CompositeIndex.entries(definition, candidate, state_key, 0)

        if Enum.min_by(entries, & &1.key).key < cursor, do: candidate
      end)

    assert is_map(concurrent_record)
    put_projected_record!(ctx, definition, concurrent_record)

    assert {:restart, :query_index_validation_concurrent_change} =
             finish_index_validation(
               ctx,
               "concurrent-multivalue-counter-build",
               definition,
               first_page,
               1
             )

    fresh = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:ok, %{phase: :counter, counter_runs: []}} =
             finish_index_validation(
               ctx,
               "concurrent-multivalue-counter-build",
               definition,
               fresh,
               1
             )
  end

  test "rejects a corrupt physical counter even when logical membership agrees", context do
    %{ctx: ctx} = context
    definition = count_definition()
    record = record("run-physical-counter-corruption", "tenant-secret", 4)
    put_projected_record!(ctx, definition, record)
    state_key = Keys.state_key(record.id, record.partition_key)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert {:ok, [prefix]} = CompositeCounter.prefixes_for_key(definition, entry.key)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, CompositeCounter.key(definition, prefix),
                CompositeCounter.encode_value(prefix, 1, 0, 2)}
             ])

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               "physical-counter-corruption",
               [definition],
               checkpoint,
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :composite_counter_physical_mismatch
    assert evidence.mismatches == 1
  end

  test "rejects a counter that omits expiring index membership", context do
    %{ctx: ctx} = context
    definition = count_definition()
    record = record("run-expiring", "tenant-secret", 4)
    state_key = Keys.state_key(record.id, record.partition_key)
    path = lmdb_path(ctx)
    expire_at_ms = 5_000

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               expire_at_ms,
               [definition],
               CompositeProjection.new_cache()
             )

    state_blob = record |> Ferricstore.Flow.encode_record() |> LMDB.encode_value(expire_at_ms)
    assert :ok = LMDB.write_batch(path, [{:put, state_key, state_blob} | ops])
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, expire_at_ms)
    assert {:ok, [prefix]} = CompositeCounter.prefixes_for_key(definition, entry.key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, CompositeCounter.key(definition, prefix),
                CompositeCounter.encode_value(prefix, 1, 0)}
             ])

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               "expiring-counter-build",
               [definition],
               checkpoint,
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :composite_counter_expiry_mismatch
    assert evidence.mismatches == 1
  end

  test "accepts consistent counter growth behind a resumable validation cursor", context do
    %{ctx: ctx} = context
    definition = count_definition()

    Enum.each(["run-a", "run-b"], fn id ->
      put_projected_record!(ctx, definition, record(id, "tenant-secret", 4))
    end)

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:ok, first_page} =
             IndexValidation.step(
               ctx,
               0,
               "concurrent-counter-build",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024
             )

    cursor = first_page.cursor

    concurrent_record =
      Enum.find_value(1..1_000, fn suffix ->
        candidate = record("concurrent-#{suffix}", "tenant-secret", 4)
        state_key = Keys.state_key(candidate.id, candidate.partition_key)
        assert {:ok, [entry]} = CompositeIndex.entries(definition, candidate, state_key, 0)
        if entry.key < cursor, do: candidate
      end)

    assert is_map(concurrent_record)
    put_projected_record!(ctx, definition, concurrent_record)

    assert {:ok, index_done} =
             IndexValidation.step(
               ctx,
               0,
               "concurrent-counter-build",
               [definition],
               first_page,
               1,
               2 * 1_024 * 1_024
             )

    assert index_done.phase == :counter
    assert index_done.counter_runs == []
  end

  test "rejects an orphan exact counter during the bounded counter inventory pass", context do
    %{ctx: ctx} = context
    definition = count_definition()
    assert {:ok, prefix} = CompositeIndex.encode_prefix(definition, ["tenant-orphan", "failed"])
    counter_key = CompositeCounter.key(definition, prefix)

    assert :ok =
             LMDB.write_batch(lmdb_path(ctx), [
               {:put, counter_key, CompositeCounter.encode_value(prefix, 1)}
             ])

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :counter}

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               "orphan-counter-build",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :orphan_composite_counter
    assert evidence.mismatches == 1
  end

  test "retries when a counter and its final index row disappear between inventory reads",
       context do
    %{ctx: ctx} = context
    definition = count_definition()
    record = record("run-concurrent-delete", "tenant-secret", 4)
    put_projected_record!(ctx, definition, record)

    state_key = Keys.state_key(record.id, record.partition_key)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert {:ok, [prefix]} = CompositeCounter.prefixes_for_key(definition, entry.key)
    counter_key = CompositeCounter.key(definition, prefix)
    path = lmdb_path(ctx)
    calls = :atomics.new(1, signed: false)

    range_entries = fn read_path, read_prefix, cursor, upper, limit, max_bytes ->
      if :atomics.add_get(calls, 1, 1) == 2 do
        assert :ok = LMDB.write_batch(path, [{:delete, entry.key}, {:delete, counter_key}])
      end

      LMDB.range_entries_bounded(read_path, read_prefix, cursor, upper, limit, max_bytes)
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :counter}

    assert {:retry, :query_index_validation_concurrent_change} =
             IndexValidation.step(
               ctx,
               0,
               "concurrent-counter-inventory",
               [definition],
               checkpoint,
               1,
               2 * 1_024 * 1_024,
               range_entries_fun: range_entries
             )
  end

  test "reports a missing expected row without exposing record or tenant values", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "missing-row-build"
    {state_key, record} = project_record!(ctx, keydir, build_id, definition)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:delete, entry.key}])

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :missing_index_entry
    assert evidence.mismatches == 1
    encoded = inspect(evidence)
    refute encoded =~ "tenant-secret"
    refute encoded =~ "run-secret"
  end

  test "rejects self-consistent projections owned by the wrong state key", context do
    %{ctx: ctx, definition: definition} = context
    build_id = "wrong-owner-build"
    record = record("run-secret", "tenant-secret", 4)
    wrong_state_key = Keys.state_key("other-run", record.partition_key)
    path = lmdb_path(ctx)
    catalog_key = Keys.type_catalog_member_key(record.type, wrong_state_key)
    assert {:ok, source_catalog_op} = SourceCatalog.put_op(catalog_key, wrong_state_key)
    assert :ok = LMDB.write_batch(path, [source_catalog_op])
    snapshot_all!(ctx, build_id)

    state_blob = record |> Ferricstore.Flow.encode_record() |> LMDB.encode_value(0)

    assert {:error, :invalid_composite_record} =
             CompositeProjection.reconcile(
               path,
               wrong_state_key,
               record,
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, [{:put, wrong_state_key, state_blob}])

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :state_key_identity_mismatch
    refute inspect(evidence) =~ "tenant-secret"
    refute inspect(evidence) =~ "run-secret"
  end

  test "reports an orphan physical row during the index-to-source pass", context do
    %{ctx: ctx, definition: definition} = context
    record = record("run-orphan", "tenant-secret", 7)
    state_key = Keys.state_key("run-orphan", "tenant-secret")
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [{:put, entry.key, entry.value}])

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:mismatch, evidence} =
             IndexValidation.step(
               ctx,
               0,
               "orphan-build",
               [definition],
               checkpoint,
               8,
               2 * 1_024 * 1_024
             )

    assert evidence.reason == :orphan_index_entry
    assert evidence.mismatches == 1
  end

  test "retries rather than misclassifying a state changed during validation", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "concurrent-build"
    {state_key, _record} = project_record!(ctx, keydir, build_id, definition)
    path = lmdb_path(ctx)
    calls = :atomics.new(1, signed: false)

    get_many = fn read_path, keys, max_bytes ->
      call = :atomics.add_get(calls, 1, 1)

      if call == 3 do
        assert :ok = LMDB.write_batch(path, [{:delete, state_key}])
      end

      LMDB.get_many_bounded(read_path, keys, max_bytes)
    end

    assert {:retry, :query_index_validation_concurrent_change} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024,
               get_many_fun: get_many
             )
  end

  test "retries when a physical row changes after its range snapshot", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "concurrent-index-build"
    {state_key, record} = project_record!(ctx, keydir, build_id, definition)
    path = lmdb_path(ctx)
    assert {:ok, [entry]} = CompositeIndex.entries(definition, record, state_key, 0)
    calls = :atomics.new(1, signed: false)

    get_many = fn read_path, keys, max_bytes ->
      if :atomics.add_get(calls, 1, 1) == 1 do
        assert :ok = LMDB.write_batch(path, [{:delete, entry.key}])
      end

      LMDB.get_many_bounded(read_path, keys, max_bytes)
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:retry, :query_index_validation_concurrent_change} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               checkpoint,
               8,
               2 * 1_024 * 1_024,
               get_many_fun: get_many
             )
  end

  test "rejects an oversized index-page key before validation reads", context do
    %{ctx: ctx, definition: definition} = context
    prefix = IndexDefinition.storage_prefix(definition)
    key = prefix <> :binary.copy(<<0>>, 512 - byte_size(prefix))
    value = "invalid-entry"
    parent = self()

    range_entries = fn _path, ^prefix, "", "", 1, _max_bytes ->
      {:ok, [{key, value}], true, byte_size(key) + byte_size(value)}
    end

    get_many = fn _path, keys, _max_bytes ->
      send(parent, {:unexpected_validation_read, keys})
      values = Enum.map(keys, fn ^key -> {:ok, value} end)
      {:ok, values, length(values) * byte_size(value)}
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:error, :invalid_query_index_validation_page} =
             IndexValidation.step(
               ctx,
               0,
               "oversized-index-key",
               [definition],
               checkpoint,
               1,
               2_048,
               range_entries_fun: range_entries,
               get_many_fun: get_many
             )

    refute_receive {:unexpected_validation_read, _keys}
  end

  test "passes the byte ceiling into bounded primary validation reads", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "bounded-read-build"
    project_record!(ctx, keydir, build_id, definition)
    parent = self()

    get_many = fn path, keys, max_bytes ->
      send(parent, {:bounded_validation_read, length(keys), max_bytes})
      LMDB.get_many_bounded(path, keys, max_bytes)
    end

    assert {:ok, %{phase: :index}} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               8,
               2 * 1_024 * 1_024,
               get_many_fun: get_many
             )

    assert_receive {:bounded_validation_read, 2, 2_097_152}
    assert_receive {:bounded_validation_read, 1, 2_097_152}
    assert_receive {:bounded_validation_read, 2, 2_097_152}
  end

  test "rejects a forged index definition before validation work", context do
    %{ctx: ctx, definition: definition} = context
    forged = %{definition | fingerprint: :crypto.strong_rand_bytes(32)}
    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :cleanup}

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(ctx, 0, "forged-build", [forged], checkpoint, 1, 1_024)
  end

  test "validation instrumentation cannot authorize a forged definition", context do
    %{ctx: ctx, definition: definition} = context
    forged = %{definition | fingerprint: :crypto.strong_rand_bytes(32)}

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "forged-instrumented-build",
               [forged],
               IndexValidation.empty_checkpoint(),
               1,
               1_024,
               definition_validation_observer_fun: fn _candidate -> :ok end
             )
  end

  test "rejects a cleanup checkpoint that skipped index validation", context do
    %{ctx: ctx, definition: definition} = context
    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :cleanup}

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "premature-cleanup",
               [definition],
               checkpoint,
               1,
               1_024
             )
  end

  test "rejects an unbounded primary-read callback", context do
    %{ctx: ctx, definition: definition} = context
    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :cleanup}

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "unbounded-read-build",
               [definition],
               checkpoint,
               1,
               1_024,
               get_many_fun: fn _path, _keys -> {:ok, []} end
             )
  end

  test "rejects malformed or unknown validation options as an invalid request", context do
    %{ctx: ctx, definition: definition} = context

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "invalid-definition-validator",
               [definition],
               IndexValidation.empty_checkpoint(),
               1,
               1_024,
               definition_validation_observer_fun: :not_a_function
             )

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "unknown-validation-option",
               [definition],
               IndexValidation.empty_checkpoint(),
               1,
               1_024,
               misspelled_page_fun: fn -> :ok end
             )
  end

  test "rejects duplicate definition identities before reading validation pages", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()

    staging_page = fn _ctx, _shard, _build, _cursor, _items, _bytes ->
      send(parent, :unexpected_validation_page_read)

      {:ok,
       %{
         state_keys: [],
         cursor: "",
         done?: true,
         scanned_entries: 0,
         staging_bytes: 0
       }}
    end

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(
               ctx,
               0,
               "duplicate-definition",
               [definition, definition],
               IndexValidation.empty_checkpoint(),
               1,
               1_024,
               staging_page_fun: staging_page
             )

    refute_receive :unexpected_validation_page_read
  end

  test "caps source pages by worst-case composite read cardinality", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()

    definitions =
      for ordinal <- 1..16 do
        IndexDefinition.new!(
          id: "bounded_validation_#{ordinal}",
          version: 1,
          fields: definition.fields
        )
      end

    staging_page = fn _ctx, 0, "cardinality-build", "", max_items, max_bytes ->
      send(parent, {:validation_page_budget, max_items, max_bytes})

      {:ok,
       %{
         state_keys: [],
         cursor: "",
         done?: true,
         scanned_entries: 0,
         staging_bytes: 0
       }}
    end

    assert {:ok, %{phase: :index}} =
             IndexValidation.step(
               ctx,
               0,
               "cardinality-build",
               definitions,
               IndexValidation.empty_checkpoint(),
               16,
               16 * 1_024 * 1_024,
               staging_page_fun: staging_page
             )

    assert_receive {:validation_page_budget, 1, 16_777_216}
  end

  test "rejects validation checkpoint counters outside the durable u64 domain", context do
    %{ctx: ctx, definition: definition} = context

    checkpoint = %{
      IndexValidation.empty_checkpoint()
      | phase: :cleanup,
        checked_records: @max_u64 + 1
    }

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(ctx, 0, "overflow-build", [definition], checkpoint, 1, 1_024)
  end

  test "rejects a validation cursor larger than an LMDB key", context do
    %{ctx: ctx, definition: definition} = context

    checkpoint = %{
      IndexValidation.empty_checkpoint()
      | phase: :cleanup,
        cursor: :binary.copy(<<0>>, 512)
    }

    assert {:error, :invalid_query_index_validation_request} =
             IndexValidation.step(ctx, 0, "oversized-cursor", [definition], checkpoint, 1, 1_024)
  end

  test "rejects a source counter overflow before primary validation reads", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()
    build_id = "source-counter-overflow"
    state_key = Keys.state_key("run-overflow", "tenant-overflow")
    staging_prefix = BackfillSource.staging_prefix(build_id)
    staging_bytes = byte_size(staging_prefix) + 32 + byte_size(state_key)
    staging_cursor = staging_prefix <> :crypto.hash(:sha256, state_key)

    staging_page = fn _ctx, 0, ^build_id, "", 1, _max_bytes ->
      {:ok,
       %{
         state_keys: [state_key],
         cursor: staging_cursor,
         done?: true,
         scanned_entries: 1,
         staging_bytes: staging_bytes
       }}
    end

    get_many = fn _path, keys, _max_bytes ->
      send(parent, :unexpected_validation_read)
      {:ok, Enum.map(keys, fn _key -> :not_found end), 0}
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | checked_records: @max_u64}

    assert {:error, :query_index_validation_counter_overflow} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               checkpoint,
               1,
               1_024,
               staging_page_fun: staging_page,
               get_many_fun: get_many
             )

    refute_receive :unexpected_validation_read
  end

  test "rejects an index counter overflow before primary validation reads", context do
    %{ctx: ctx, keydir: keydir, definition: definition} = context
    build_id = "index-counter-overflow"
    project_record!(ctx, keydir, build_id, definition)
    parent = self()

    get_many = fn path, keys, max_bytes ->
      send(parent, :unexpected_validation_read)
      LMDB.get_many_bounded(path, keys, max_bytes)
    end

    checkpoint = %{
      IndexValidation.empty_checkpoint()
      | phase: :index,
        checked_entries: @max_u64
    }

    assert {:error, :query_index_validation_counter_overflow} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               checkpoint,
               8,
               2 * 1_024 * 1_024,
               get_many_fun: get_many
             )

    refute_receive :unexpected_validation_read
  end

  test "rejects false staging byte accounting before primary reads", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()
    build_id = "staging-byte-underreport"
    state_key = Keys.state_key("run-underreported", "tenant-underreported")
    staging_cursor = BackfillSource.staging_prefix(build_id) <> :crypto.hash(:sha256, state_key)

    staging_page = fn _ctx, 0, ^build_id, "", 1, _max_bytes ->
      {:ok,
       %{
         state_keys: [state_key],
         cursor: staging_cursor,
         done?: true,
         scanned_entries: 1,
         staging_bytes: 0
       }}
    end

    get_many = fn _path, keys, _max_bytes ->
      send(parent, :unexpected_validation_read)
      {:ok, Enum.map(keys, fn _key -> :not_found end), 0}
    end

    assert {:error, :invalid_query_index_validation_page} =
             IndexValidation.step(
               ctx,
               0,
               build_id,
               [definition],
               IndexValidation.empty_checkpoint(),
               1,
               1_024,
               staging_page_fun: staging_page,
               get_many_fun: get_many
             )

    refute_receive :unexpected_validation_read
  end

  test "rejects false index byte accounting before primary reads", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()
    prefix = IndexDefinition.storage_prefix(definition)

    range_entries = fn _path, ^prefix, "", "", 1, _max_bytes ->
      {:ok, [{prefix <> "underreported", "value"}], true, 0}
    end

    get_many = fn _path, _keys, _max_bytes ->
      send(parent, :unexpected_validation_read)
      {:ok, [], 0}
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index}

    assert {:error, :invalid_query_index_validation_page} =
             IndexValidation.step(
               ctx,
               0,
               "index-byte-underreport",
               [definition],
               checkpoint,
               1,
               1_024,
               range_entries_fun: range_entries,
               get_many_fun: get_many
             )

    refute_receive :unexpected_validation_read
  end

  test "rejects an index page that regresses its persisted cursor", context do
    %{ctx: ctx, definition: definition} = context
    parent = self()
    prefix = IndexDefinition.storage_prefix(definition)
    cursor = prefix <> "z"
    row = {prefix <> "a", "value"}
    row_bytes = row |> Tuple.to_list() |> Enum.reduce(0, &(byte_size(&1) + &2))

    range_entries = fn _path, ^prefix, ^cursor, "", 1, _max_bytes ->
      {:ok, [row], false, row_bytes}
    end

    get_many = fn _path, _keys, _max_bytes ->
      send(parent, :unexpected_validation_read)
      {:ok, [], 0}
    end

    checkpoint = %{IndexValidation.empty_checkpoint() | phase: :index, cursor: cursor}

    assert {:error, :invalid_query_index_validation_page} =
             IndexValidation.step(
               ctx,
               0,
               "index-cursor-regression",
               [definition],
               checkpoint,
               1,
               1_024,
               range_entries_fun: range_entries,
               get_many_fun: get_many
             )

    refute_receive :unexpected_validation_read
  end

  defp project_record!(ctx, keydir, build_id, definition) do
    record = record("run-secret", "tenant-secret", 4)
    state_key = Keys.state_key("run-secret", "tenant-secret")
    :ets.insert(keydir, {state_key, <<>>, 0, 0, 0, 0, 0})
    catalog_key = Keys.type_catalog_member_key(record.type, state_key)
    assert {:ok, source_catalog_op} = SourceCatalog.put_op(catalog_key, state_key)
    assert :ok = LMDB.write_batch(lmdb_path(ctx), [source_catalog_op])
    snapshot_all!(ctx, build_id)

    path = lmdb_path(ctx)
    state_blob = record |> Ferricstore.Flow.encode_record() |> LMDB.encode_value(0)

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, [{:put, state_key, state_blob} | ops])
    {state_key, record}
  end

  defp snapshot_all!(ctx, build_id) do
    case BackfillSource.snapshot_page(ctx, 0, build_id, 8, 1_024 * 1_024) do
      {:ok, %{done?: true}} -> :ok
      {:ok, %{done?: false}} -> snapshot_all!(ctx, build_id)
    end
  end

  defp finish_source_validation!(ctx, build_id, definition, checkpoint) do
    case IndexValidation.step(
           ctx,
           0,
           build_id,
           [definition],
           checkpoint,
           8,
           2 * 1_024 * 1_024
         ) do
      {:ok, %{phase: :source} = next} ->
        finish_source_validation!(ctx, build_id, definition, next)

      {:ok, %{phase: :index} = next} ->
        next
    end
  end

  defp finish_index_validation(ctx, build_id, definition, checkpoint, max_items) do
    case IndexValidation.step(
           ctx,
           0,
           build_id,
           [definition],
           checkpoint,
           max_items,
           2 * 1_024 * 1_024
         ) do
      {:ok, %{phase: :index} = next} ->
        finish_index_validation(ctx, build_id, definition, next, max_items)

      {:ok, %{phase: :counter} = next} ->
        {:ok, next}

      other ->
        other
    end
  end

  defp put_projected_record!(ctx, definition, record) do
    state_key = Keys.state_key(record.id, record.partition_key)
    path = lmdb_path(ctx)
    state_blob = record |> Ferricstore.Flow.encode_record() |> LMDB.encode_value(0)

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               0,
               [definition],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, [{:put, state_key, state_blob} | ops])
  end

  defp definition do
    IndexDefinition.new!(
      id: "runs_by_tenant_state_updated",
      version: 1,
      fields: [
        {:partition_key, :asc, :hashed},
        {:state, :asc, :hashed},
        {:updated_at_ms, :desc, :ordered}
      ]
    )
  end

  defp count_definition do
    IndexDefinition.new!(
      id: "runs_by_tenant_state_count",
      version: 1,
      count_prefixes: [2],
      fields: [
        {:partition_key, :asc, :hashed},
        {:state, :asc, :hashed},
        {:updated_at_ms, :desc, :ordered}
      ]
    )
  end

  defp record(id, tenant, version) do
    %{
      id: id,
      type: "invoice",
      state: "failed",
      version: version,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: version,
      updated_at_ms: version,
      next_run_at_ms: version,
      priority: 0,
      ttl_ms: nil,
      history_hot_max_events: nil,
      history_max_events: nil,
      retention_ttl_ms: nil,
      max_active_ms: nil,
      terminal_retention_until_ms: nil,
      partition_key: tenant,
      payload_ref: nil,
      parent_flow_id: nil,
      parent_partition_key: nil,
      root_flow_id: id,
      correlation_id: nil,
      result_ref: nil,
      error_ref: nil,
      lease_owner: "",
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      state_enter_seq: version,
      child_groups: %{}
    }
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
