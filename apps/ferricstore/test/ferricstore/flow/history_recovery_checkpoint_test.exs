defmodule Ferricstore.Flow.HistoryRecoveryCheckpointTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF

  alias Ferricstore.Flow.{
    HistoryProjector,
    HistoryRecoveryCheckpoint,
    LMDB,
    NativeOrderedIndex,
    OrderedIndex
  }

  setup do
    id = System.unique_integer([:positive])
    dir = Path.join(System.tmp_dir!(), "ferricstore-history-recovery-checkpoint-#{id}")
    File.mkdir_p!(dir)

    name = :"history_recovery_checkpoint_#{id}"
    keydir = :ets.new(:history_recovery_checkpoint_keydir, [:public, :set])
    {flow_index, flow_lookup} = OrderedIndex.table_names(name, 0)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

    previous_hot = Application.get_env(:ferricstore, :flow_default_history_hot_max_events)
    previous_async = Application.get_env(:ferricstore, :flow_async_history)
    previous_hook = Application.get_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)
    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 0)
    Application.put_env(:ferricstore, :flow_async_history, true)
    Application.delete_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

    on_exit(fn ->
      restore_env(:flow_default_history_hot_max_events, previous_hot)
      restore_env(:flow_async_history, previous_async)
      restore_env(:flow_history_projector_lmdb_publish_hook, previous_hook)
      File.rm_rf!(dir)
    end)

    ctx = %{
      name: name,
      data_dir: Path.dirname(dir),
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    %{ctx: ctx, dir: dir, keydir: keydir, indexes: {flow_index, flow_lookup}}
  end

  test "a verified durable history prefix avoids republishing old LMDB entries", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir, indexes: {flow_index, flow_lookup}} = fixture
    {entry, lmdb_key} = history_entry("checkpoint", "3001-1", 3001)

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), lmdb_key)

    NativeOrderedIndex.reset(flow_index, flow_lookup)
    :ets.delete_all_objects(keydir)
    :atomics.put(ctx.flow_history_projected_index, 1, 0)

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _, _, _ ->
      {:error, :old_history_was_republished}
    end)

    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), lmdb_key)
  end

  test "a checkpoint replays an appended history tail without republishing its prefix", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {first, first_lmdb_key} = history_entry("tail", "3001-1", 3001)
    {second, second_lmdb_key} = history_entry("tail", "3002-2", 3002)

    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [first], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    assert {:ok, [_location]} =
             NIF.v2_append_batch_nosync(HistoryProjector.history_file_path(dir, 0), [
               {second.key, second.value, 0}
             ])

    assert :ok = NIF.v2_fsync(HistoryProjector.history_file_path(dir, 0))
    assert :not_found = LMDB.get(LMDB.path(dir), second_lmdb_key)

    :ets.delete_all_objects(keydir)
    :atomics.put(ctx.flow_history_projected_index, 1, 0)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), first_lmdb_key)
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), second_lmdb_key)
  end

  test "a checkpoint replays an appended tombstone against the saved prefix", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, lmdb_key} = history_entry("deleted", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    history_path = HistoryProjector.history_file_path(dir, 0)
    assert {:ok, _location} = NIF.v2_append_tombstone(history_path, entry.key)
    assert :ok = NIF.v2_fsync(history_path)

    :ets.delete_all_objects(keydir)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert :not_found = LMDB.get(LMDB.path(dir), lmdb_key)
  end

  test "a missing checkpoint falls back to the authoritative full log scan", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, lmdb_key} = history_entry("missing", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    assert :ok =
             LMDB.write_batch(LMDB.path(dir), [{:delete, HistoryRecoveryCheckpoint.key()}])

    parent = self()

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _, _, _ ->
      send(parent, :history_full_scan_published)
      :ok
    end)

    :ets.delete_all_objects(keydir)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert_receive :history_full_scan_published
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), lmdb_key)
  end

  test "a checkpoint refuses a different valid history file instead of leaving stale LMDB rows",
       fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, old_lmdb_key} = history_entry("origin", "3001-1", 3001)
    {replacement, _new_lmdb_key} = history_entry("change", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    history_path = HistoryProjector.history_file_path(dir, 0)
    replacement_path = Path.join(dir, "replacement.log")

    assert {:ok, [_location]} =
             NIF.v2_append_batch_nosync(replacement_path, [
               {replacement.key, replacement.value, 0}
             ])

    File.write!(history_path, File.read!(replacement_path))
    assert File.stat!(history_path).size == File.stat!(replacement_path).size
    :ets.delete_all_objects(keydir)

    assert {:error, {:history_recovery_checkpoint_source_changed, _reason}} =
             HistoryProjector.recover(ctx, 0, dir, keydir)

    assert {:ok, _old_value} = LMDB.get(LMDB.path(dir), old_lmdb_key)
  end

  test "a checkpoint refuses a truncated authoritative history log", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, _lmdb_key} = history_entry("truncated", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    File.write!(HistoryProjector.history_file_path(dir, 0), "")
    :ets.delete_all_objects(keydir)

    assert {:error, {:history_recovery_checkpoint_source_changed, _reason}} =
             HistoryProjector.recover(ctx, 0, dir, keydir)
  end

  test "changing the hot-history policy forces a full recovery", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, _lmdb_key} = history_entry("hot-policy", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    Application.put_env(:ferricstore, :flow_default_history_hot_max_events, 2)
    :ets.delete_all_objects(keydir)
    parent = self()

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _, _, _ ->
      send(parent, :history_policy_rebuilt)
      :ok
    end)

    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert_receive :history_policy_rebuilt
    assert :ets.member(keydir, entry.key)
  end

  test "synchronous history mode does not reuse an async-history checkpoint", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, _lmdb_key} = history_entry("sync-mode", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    Application.put_env(:ferricstore, :flow_async_history, false)
    :ets.delete_all_objects(keydir)
    parent = self()

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _, _, _ ->
      send(parent, :synchronous_history_rebuilt)
      :ok
    end)

    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert_receive :synchronous_history_rebuilt
  end

  test "an incomplete uncommitted tail keeps the previous tolerant recovery behavior", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, lmdb_key} = history_entry("torn-tail", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)

    File.write!(HistoryProjector.history_file_path(dir, 0), <<0xAA, 0xBB>>, [:append])
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)
    assert {:ok, _value} = LMDB.get(LMDB.path(dir), lmdb_key)
    assert :not_found = LMDB.get(LMDB.path(dir), HistoryRecoveryCheckpoint.key())
  end

  test "a corrupted committed tail still fails closed after checkpoint reuse", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {first, first_lmdb_key} = history_entry("corrupt-tail", "3001-1", 3001)
    {second, _second_lmdb_key} = history_entry("corrupt-tail", "3002-2", 3002)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [first], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    path = HistoryProjector.history_file_path(dir, 0)

    assert {:ok, [{tail_offset, _size}]} =
             NIF.v2_append_batch_nosync(path, [{second.key, second.value, 0}])

    assert :ok = NIF.v2_fsync(path)
    assert {:ok, fd} = :file.open(to_charlist(path), [:read, :write, :binary, :raw])
    assert {:ok, <<old_byte>>} = :file.pread(fd, tail_offset + 4, 1)
    assert :ok = :file.pwrite(fd, tail_offset + 4, <<Bitwise.bxor(old_byte, 255)>>)
    assert :ok = :file.close(fd)
    :ets.delete_all_objects(keydir)

    assert {:error, {:history_scan_failed, _reason}} =
             HistoryProjector.recover(ctx, 0, dir, keydir)

    assert {:ok, _first_value} = LMDB.get(LMDB.path(dir), first_lmdb_key)
  end

  test "an invalid committed checkpoint cannot silently select stale LMDB rows", fixture do
    %{ctx: ctx, dir: dir, keydir: keydir} = fixture
    {entry, _lmdb_key} = history_entry("bad-marker", "3001-1", 3001)
    assert :ok = HistoryProjector.write_entries_sync(ctx, 0, dir, [entry], 301)
    assert :ok = HistoryProjector.recover(ctx, 0, dir, keydir)

    assert :ok =
             LMDB.write_batch(LMDB.path(dir), [
               {:put, HistoryRecoveryCheckpoint.key(), <<0, 1, 2>>}
             ])

    :ets.delete_all_objects(keydir)

    assert {:error, {:history_recovery_checkpoint_source_changed, _reason}} =
             HistoryProjector.recover(ctx, 0, dir, keydir)
  end

  defp history_entry(id, event_id, event_ms) do
    history_key = Ferricstore.Flow.Keys.history_key(id)
    key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, nil)

    {%{
       key: key,
       expire_at_ms: 0,
       history_key: history_key,
       event_id: event_id,
       event_ms: event_ms,
       version: 1,
       value: "saved-event-#{event_id}",
       history_hot_max_events: 0,
       ra_index: event_ms
     }, LMDB.history_index_key(history_key, event_id, event_ms)}
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
