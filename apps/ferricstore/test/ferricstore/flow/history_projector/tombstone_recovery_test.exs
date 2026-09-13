defmodule Ferricstore.Flow.HistoryProjector.TombstoneRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{Keys, LMDB, NativeOrderedIndex}
  alias Ferricstore.Flow.HistoryProjector.{KeyCodec, Recovery, Storage}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "history-tombstone-#{System.pid()}-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(root, "history"))
    log = Path.join([root, "history", "00000.log"])
    File.touch!(log)
    keydir = :ets.new(:history_tombstone_recovery, [:set, :public])
    instance = %{name: :history_tombstone_recovery, data_dir: root}
    native = NativeOrderedIndex.reset_shard(instance.name, 0)

    on_exit(fn ->
      NativeOrderedIndex.unregister_all(instance.name, 1)
      LMDB.release(LMDB.path(root))
      File.rm_rf!(root)
    end)

    %{root: root, log: log, keydir: keydir, instance: instance, native: native}
  end

  test "durable trim tombstone removes stale cold and hot history on repeated recovery", ctx do
    deleted = entry("trimmed", "1000-1", 10_000)
    survivor = entry("survivor", "2000-1", 0)
    persist(ctx, [deleted, survivor])
    assert {:ok, _} = NIF.v2_append_ops_batch(ctx.log, [{:delete, deleted.key}])
    sync_log(ctx.log)

    for _ <- 1..2 do
      assert :ok = Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)
      assert LMDB.get(LMDB.path(ctx.root), index_key(deleted)) == :not_found

      assert LMDB.get(
               LMDB.path(ctx.root),
               LMDB.history_expire_key(deleted.expire_at_ms, index_key(deleted))
             ) == :not_found

      assert :ets.lookup(ctx.keydir, deleted.key) == []
      assert NativeOrderedIndex.count_all(ctx.native, deleted.history_key) == 0
      assert {:ok, _} = LMDB.get(LMDB.path(ctx.root), index_key(survivor))
    end
  end

  test "later live record wins over repeated tombstones for the same event", ctx do
    event = entry("rewritten", "1000-1", 0)
    persist(ctx, [event])

    assert {:ok, _} =
             NIF.v2_append_ops_batch(ctx.log, [{:delete, event.key}, {:delete, event.key}])

    assert {:ok, {new_offset, _}} = NIF.v2_append_record(ctx.log, event.key, "new-event", 0)
    sync_log(ctx.log)

    assert :ok = Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)
    assert {:ok, encoded} = LMDB.get(LMDB.path(ctx.root), index_key(event))

    assert encoded ==
             LMDB.encode_history_index_value(
               event.event_id,
               event.event_ms,
               event.key,
               0,
               {:flow_history, 0},
               new_offset,
               byte_size("new-event")
             )

    assert {:ok, "new-event"} = NIF.v2_pread_at(ctx.log, new_offset)
  end

  test "more than one deletion batch recovers without retaining trimmed events", ctx do
    events = for index <- 1..4_097, do: entry("many", "#{index}-1", 0)
    persist(ctx, events)
    assert {:ok, _} = NIF.v2_append_ops_batch(ctx.log, Enum.map(events, &{:delete, &1.key}))
    sync_log(ctx.log)

    assert :ok = Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)
    assert NativeOrderedIndex.count_all(ctx.native, hd(events).history_key) == 0
    assert :ets.info(ctx.keydir, :size) == 0

    for event <- events do
      assert LMDB.get(LMDB.path(ctx.root), index_key(event)) == :not_found
    end
  end

  test "recovery can retry after live-index publication fails following tombstone cleanup", ctx do
    deleted = entry("retry-deleted", "1000-1", 0)
    survivor = entry("retry-live", "2000-1", 0)
    persist(ctx, [deleted, survivor])
    assert {:ok, _} = NIF.v2_append_ops_batch(ctx.log, [{:delete, deleted.key}])
    sync_log(ctx.log)
    original_log = File.read!(ctx.log)
    old_hook = Application.fetch_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)

    Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, fn _path,
                                                                                    _file_id,
                                                                                    _entries ->
      {:error, :injected_publish_failure}
    end)

    try do
      assert {:error, :injected_publish_failure} =
               Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)

      assert LMDB.get(LMDB.path(ctx.root), index_key(deleted)) == :not_found
      assert File.read!(ctx.log) == original_log
    after
      case old_hook do
        {:ok, hook} ->
          Application.put_env(:ferricstore, :flow_history_projector_lmdb_publish_hook, hook)

        :error ->
          Application.delete_env(:ferricstore, :flow_history_projector_lmdb_publish_hook)
      end
    end

    assert :ok = Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)
    assert LMDB.get(LMDB.path(ctx.root), index_key(deleted)) == :not_found
    assert {:ok, _} = LMDB.get(LMDB.path(ctx.root), index_key(survivor))
    assert File.read!(ctx.log) == original_log
  end

  test "a failed tombstone index delete aborts recovery without hiding the error", ctx do
    event = entry("invalid-index", "1000-1", 0)
    persist(ctx, [event])
    path = LMDB.path(ctx.root)
    index_key = index_key(event)
    assert {:ok, original_index} = LMDB.get(path, index_key)
    assert {:ok, _} = NIF.v2_append_ops_batch(ctx.log, [{:delete, event.key}])
    sync_log(ctx.log)
    original_log = File.read!(ctx.log)
    assert :ok = LMDB.write_batch(path, [{:put, index_key, "invalid-index"}])

    assert {:error, :invalid_history_index_value} =
             Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)

    assert File.read!(ctx.log) == original_log
    assert [{_, _, _, _, _, _, _}] = :ets.lookup(ctx.keydir, event.key)

    assert :ok = LMDB.write_batch(path, [{:put, index_key, original_index}])
    assert :ok = Recovery.recover_history_log(ctx.instance, 0, ctx.root, ctx.keydir)
    assert LMDB.get(path, index_key) == :not_found
    assert :ets.lookup(ctx.keydir, event.key) == []
  end

  defp entry(id, event_id, expire_at_ms) do
    history_key = Keys.history_key(id)
    {:ok, event_ms} = KeyCodec.parse_event_ms(event_id)

    %{
      key: KeyCodec.history_entry_key(history_key, event_id),
      history_key: history_key,
      event_id: event_id,
      event_ms: event_ms,
      version: 1,
      expire_at_ms: expire_at_ms,
      history_hot_max_events: 10_000,
      value: "old-event"
    }
  end

  defp persist(ctx, entries) do
    assert {:ok, locations} =
             NIF.v2_append_batch(ctx.log, Enum.map(entries, &{&1.key, &1.value, &1.expire_at_ms}))

    sync_log(ctx.log)
    assert :ok = Storage.publish_lmdb_history_locations(ctx.root, 0, entries, locations)

    assert :ok =
             Storage.publish_keydir_entries(ctx.instance, 0, ctx.keydir, 0, entries, locations)

    assert :ok = Storage.publish_history_index(ctx.instance, 0, entries)
  end

  defp index_key(entry),
    do: LMDB.history_index_key(entry.history_key, entry.event_id, entry.event_ms)

  defp sync_log(path) do
    {:ok, file} = :file.open(String.to_charlist(path), [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.sync(file)
    after
      :file.close(file)
    end
  end
end
