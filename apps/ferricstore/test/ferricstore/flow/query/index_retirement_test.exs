defmodule Ferricstore.Flow.Query.IndexRetirementTest do
  use ExUnit.Case, async: true

  @max_u64 0xFFFF_FFFF_FFFF_FFFF

  alias Ferricstore.Flow.{Keys, LMDB}

  alias Ferricstore.Flow.Query.{
    CompositeCounter,
    CompositeIndex,
    CompositeProjection,
    IndexDefinition
  }

  alias Ferricstore.Flow.Query.IndexRetirement

  setup do
    suffix = System.unique_integer([:positive, :monotonic])
    data_dir = Path.join(System.tmp_dir!(), "ferricstore_index_retirement_#{suffix}")
    ctx = %{name: :retirement_test, data_dir: data_dir, shard_count: 1}
    on_exit(fn -> File.rm_rf!(data_dir) end)

    %{ctx: ctx, retired: definition("retired", 1), active: definition("active", 1)}
  end

  test "deletes one bounded physical page then scrubs only retired reverse keys", context do
    %{ctx: ctx, retired: retired, active: active} = context
    path = lmdb_path(ctx)

    state_keys =
      for number <- 1..3 do
        record = record("run-#{number}", number)
        state_key = Keys.state_key(record.id, record.partition_key)

        assert {:ok, ops, _cache} =
                 CompositeProjection.reconcile(
                   path,
                   state_key,
                   record,
                   0,
                   [retired, active],
                   CompositeProjection.new_cache()
                 )

        assert :ok = LMDB.write_batch(path, ops)
        state_key
      end

    checkpoint = IndexRetirement.empty_checkpoint()

    assert {:ok, first} = IndexRetirement.step(ctx, 0, retired, checkpoint, 1, 1_024 * 1_024)
    assert first.phase == :index
    assert first.deleted_entries == 1
    assert first.cursor != ""

    {status, reverse_done} = retire_all!(ctx, retired, first)
    assert status == :complete
    assert reverse_done.deleted_entries == 4
    assert reverse_done.rewritten_reverse_rows == 3

    assert {:ok, []} =
             LMDB.prefix_entries(path, IndexDefinition.storage_prefix(retired), 10)

    assert {:ok, []} =
             LMDB.prefix_entries(path, CompositeCounter.definition_storage_prefix(retired), 10)

    assert {:ok, 3} = CompositeCounter.read(path, active, nil, ["tenant-a"])

    assert {:ok, active_rows} =
             LMDB.prefix_entries(path, IndexDefinition.storage_prefix(active), 10)

    assert length(active_rows) == 3

    Enum.each(state_keys, fn state_key ->
      assert {:ok, reverse_blob} = LMDB.get(path, CompositeIndex.reverse_key(state_key))
      assert {:ok, keys} = CompositeIndex.decode_reverse_value(reverse_blob, state_key)
      assert length(keys) == 1
      assert String.starts_with?(hd(keys), IndexDefinition.storage_prefix(active))
    end)
  end

  test "replaying a page after its write is idempotent", context do
    %{ctx: ctx, retired: retired} = context
    record = record("run-replay", 1)
    state_key = Keys.state_key(record.id, record.partition_key)
    path = lmdb_path(ctx)

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               0,
               [retired],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, ops)
    checkpoint = IndexRetirement.empty_checkpoint()

    assert {:ok, after_delete} =
             IndexRetirement.step(ctx, 0, retired, checkpoint, 8, 1_024 * 1_024)

    assert after_delete.phase == :counter

    assert {:ok, replayed} = IndexRetirement.step(ctx, 0, retired, checkpoint, 8, 1_024 * 1_024)
    assert replayed.phase == :counter
    assert replayed.deleted_entries == 0

    assert {:ok, counter_done} =
             IndexRetirement.step(ctx, 0, retired, replayed, 8, 1_024 * 1_024)

    assert counter_done.phase == :reverse

    assert {:complete, _done} =
             IndexRetirement.step(ctx, 0, retired, counter_done, 8, 1_024 * 1_024)

    assert :not_found = LMDB.get(path, CompositeIndex.reverse_key(state_key))
  end

  test "retries a reverse rewrite lost to a concurrent projection", context do
    %{ctx: ctx, retired: retired, active: active} = context
    record = record("run-race", 1)
    state_key = Keys.state_key(record.id, record.partition_key)
    path = lmdb_path(ctx)

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               0,
               [retired, active],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, ops)
    reverse_key = CompositeIndex.reverse_key(state_key)
    assert {:ok, original} = LMDB.get(path, reverse_key)

    assert {:ok, active_entries} = CompositeIndex.entries(active, record, state_key, 0)

    concurrent =
      CompositeIndex.encode_reverse_value(state_key, Enum.map(active_entries, & &1.key))

    calls = :atomics.new(1, signed: false)

    write_batch = fn write_path, write_ops ->
      if :atomics.add_get(calls, 1, 1) == 1 do
        assert :ok = LMDB.write_batch(path, [{:put, reverse_key, concurrent}])
      end

      LMDB.write_batch(write_path, write_ops)
    end

    checkpoint = %{IndexRetirement.empty_checkpoint() | phase: :reverse}

    assert {:retry, :query_index_retirement_concurrent_change} =
             IndexRetirement.step(ctx, 0, retired, checkpoint, 8, 1_024 * 1_024,
               write_batch_fun: write_batch
             )

    assert original != concurrent
    assert {:ok, ^concurrent} = LMDB.get(path, reverse_key)
  end

  test "rejects a forged index definition before retirement work", context do
    %{ctx: ctx, retired: retired} = context
    forged = %{retired | fingerprint: :crypto.strong_rand_bytes(32)}

    assert {:error, :invalid_query_index_retirement_request} =
             IndexRetirement.step(
               ctx,
               0,
               forged,
               IndexRetirement.empty_checkpoint(),
               1,
               1_024
             )
  end

  test "rejects retirement checkpoint counters outside the durable u64 domain", context do
    %{ctx: ctx, retired: retired} = context

    checkpoint = %{
      IndexRetirement.empty_checkpoint()
      | deleted_entries: @max_u64 + 1
    }

    assert {:error, :invalid_query_index_retirement_request} =
             IndexRetirement.step(ctx, 0, retired, checkpoint, 1, 1_024)
  end

  test "rejects a retirement cursor larger than an LMDB key", context do
    %{ctx: ctx, retired: retired} = context
    checkpoint = %{IndexRetirement.empty_checkpoint() | cursor: :binary.copy(<<0>>, 512)}

    assert {:error, :invalid_query_index_retirement_request} =
             IndexRetirement.step(ctx, 0, retired, checkpoint, 1, 1_024)
  end

  test "rejects retirement counter overflow before deleting an index page", context do
    %{ctx: ctx, retired: retired} = context
    record = record("run-overflow", 1)
    state_key = Keys.state_key(record.id, record.partition_key)
    path = lmdb_path(ctx)

    assert {:ok, ops, _cache} =
             CompositeProjection.reconcile(
               path,
               state_key,
               record,
               0,
               [retired],
               CompositeProjection.new_cache()
             )

    assert :ok = LMDB.write_batch(path, ops)
    assert {:ok, [entry]} = CompositeIndex.entries(retired, record, state_key, 0)

    checkpoint = %{
      IndexRetirement.empty_checkpoint()
      | deleted_entries: @max_u64,
        deleted_bytes: @max_u64
    }

    assert {:error, :query_index_retirement_counter_overflow} =
             IndexRetirement.step(ctx, 0, retired, checkpoint, 1, 1_024 * 1_024)

    assert {:ok, entry.value} == LMDB.get(path, entry.key)
  end

  test "rejects false page byte accounting before retirement writes", context do
    %{ctx: ctx, retired: retired} = context
    parent = self()
    prefix = IndexDefinition.storage_prefix(retired)

    range_entries = fn _path, ^prefix, "", "", 1, _max_bytes ->
      {:ok, [{prefix <> "underreported", "value"}], true, 0}
    end

    write_batch = fn _path, _ops ->
      send(parent, :unexpected_retirement_write)
      :ok
    end

    assert {:error, :invalid_query_index_retirement_page} =
             IndexRetirement.step(
               ctx,
               0,
               retired,
               IndexRetirement.empty_checkpoint(),
               1,
               1_024,
               range_entries_fun: range_entries,
               write_batch_fun: write_batch
             )

    refute_receive :unexpected_retirement_write
  end

  test "rejects an oversized retirement page key before writes", context do
    %{ctx: ctx, retired: retired} = context
    parent = self()
    prefix = IndexDefinition.storage_prefix(retired)
    key = prefix <> :binary.copy(<<0>>, 512 - byte_size(prefix))
    row = {key, "value"}
    read_bytes = byte_size(key) + byte_size(elem(row, 1))

    range_entries = fn _path, ^prefix, "", "", 1, _max_bytes ->
      {:ok, [row], true, read_bytes}
    end

    write_batch = fn _path, _ops ->
      send(parent, :unexpected_retirement_write)
      :ok
    end

    assert {:error, :invalid_query_index_retirement_page} =
             IndexRetirement.step(
               ctx,
               0,
               retired,
               IndexRetirement.empty_checkpoint(),
               1,
               1_024,
               range_entries_fun: range_entries,
               write_batch_fun: write_batch
             )

    refute_receive :unexpected_retirement_write
  end

  test "rejects a retirement page that regresses its persisted cursor", context do
    %{ctx: ctx, retired: retired} = context
    parent = self()
    prefix = IndexDefinition.storage_prefix(retired)
    cursor = prefix <> "z"
    row = {prefix <> "a", "value"}
    row_bytes = row |> Tuple.to_list() |> Enum.reduce(0, &(byte_size(&1) + &2))

    range_entries = fn _path, ^prefix, ^cursor, "", 1, _max_bytes ->
      {:ok, [row], false, row_bytes}
    end

    write_batch = fn _path, _ops ->
      send(parent, :unexpected_retirement_write)
      :ok
    end

    checkpoint = %{IndexRetirement.empty_checkpoint() | cursor: cursor}

    assert {:error, :invalid_query_index_retirement_page} =
             IndexRetirement.step(
               ctx,
               0,
               retired,
               checkpoint,
               1,
               1_024,
               range_entries_fun: range_entries,
               write_batch_fun: write_batch
             )

    refute_receive :unexpected_retirement_write
  end

  defp retire_all!(ctx, definition, checkpoint) do
    case IndexRetirement.step(ctx, 0, definition, checkpoint, 2, 1_024 * 1_024) do
      {:ok, next} -> retire_all!(ctx, definition, next)
      {:complete, done} -> {:complete, done}
    end
  end

  defp definition(id, version) do
    IndexDefinition.new!(
      id: id,
      version: version,
      count_prefixes: [1],
      fields: [
        {:partition_key, :asc, :hashed},
        {:updated_at_ms, :desc, :ordered}
      ]
    )
  end

  defp record(id, version) do
    %{
      id: id,
      partition_key: "tenant-a",
      state: "failed",
      type: "invoice",
      version: version,
      updated_at_ms: version
    }
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
