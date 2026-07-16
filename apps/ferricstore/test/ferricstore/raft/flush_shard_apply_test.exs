defmodule Ferricstore.Raft.FlushShardApplyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Commands.Stream.{Index, Meta, Tables, Waiters}

  alias Ferricstore.Flow.{
    HistoryProjectedIndex,
    LMDB,
    LMDBReplaySafeIndex,
    NativeOrderedIndex
  }

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.{BitcaskWriter, CompoundKey, Promotion}
  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  test "replicated shard flush streams durable tombstones and preserves server catalog rows" do
    %{state: state, keydir: keydir, path: path} = start_state()

    keys = Enum.map(1..1_025, &"flush-shard:key:#{&1}")

    Enum.each(keys, fn key ->
      true = :ets.insert(keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})
    end)

    catalog_key = ServerCatalog.entry_key("acl", "default")
    watermark_key = Keys.shared_value_ref_backfill_key(state.shard_index)
    progress_key = SharedRefBackfill.progress_key(state.shard_index)

    for key <- [catalog_key, watermark_key, progress_key] do
      true = :ets.insert(keydir, {key, "control", 0, LFU.initial(), 0, 0, 7})
    end

    assert {new_state, {:ok, 1_027}} =
             StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

    assert Enum.all?(keys, &(:ets.lookup(keydir, &1) == []))
    assert [{^catalog_key, "control", 0, _, 0, 0, 7}] = :ets.lookup(keydir, catalog_key)
    assert [{^watermark_key, <<1>>, 0, _, _, _, 1}] = :ets.lookup(keydir, watermark_key)
    assert [{^progress_key, _progress, 0, _, _, _, _}] = :ets.lookup(keydir, progress_key)

    assert {:ok, tombstones} = NIF.v2_scan_tombstones(path)
    assert length(tombstones) == length(keys) + 2
    assert new_state.file_stats == ShardFlush.compute_file_stats(path |> Path.dirname(), keydir)

    active_file_size = new_state.active_file_size

    assert {^active_file_size, _dead_bytes} =
             Map.fetch!(new_state.file_stats, new_state.active_file_id)
  end

  test "replicated shard flush clears fetch-or-compute locks before deleting rows" do
    %{state: state, keydir: keydir} = start_state()
    key = "flush-shard:locked"
    true = :ets.insert(keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})

    locked_state = %{
      state
      | fetch_or_compute_locks: %{key => {make_ref(), Ferricstore.HLC.now_ms() + 60_000}}
    }

    assert {new_state, {:ok, 1}} =
             StateMachine.apply(%{}, {:flush_shard, {1, 0}}, locked_state)

    assert new_state.applied_count == locked_state.applied_count + 1
    assert new_state.fetch_or_compute_locks == %{}
    assert [] == :ets.lookup(keydir, key)
  end

  test "replicated shard flush clears replica-local derived state without Ops post-cleanup" do
    instance_name = :"flush_replay_#{System.unique_integer([:positive])}"
    %{state: state, keydir: keydir} = start_state(instance_name: instance_name)
    stream_key = "flush-replay-stream"
    type_key = CompoundKey.type_key(stream_key)
    flow_key = "flow-replay-index"
    scope = %{cache_scope: instance_name}
    degraded = :atomics.new(state.shard_index + 1, signed: false)
    replay_safe = :atomics.new(state.shard_index + 1, signed: false)
    replay_requested = :atomics.new(state.shard_index + 1, signed: false)
    history_projected = :atomics.new(state.shard_index + 1, signed: false)
    checkpoint_flags = :atomics.new(state.shard_index + 1, signed: false)
    disk_pressure = :atomics.new(state.shard_index + 1, signed: false)

    instance_ctx = %{
      name: instance_name,
      checkpoint_flags: checkpoint_flags,
      disk_pressure: disk_pressure,
      flow_lmdb_mirror_degraded: degraded,
      flow_lmdb_replay_safe_index: replay_safe,
      flow_lmdb_replay_safe_requested_index: replay_requested,
      flow_history_projected_index: history_projected
    }

    state = %{state | instance_ctx: instance_ctx}
    :atomics.put(degraded, state.shard_index + 1, 1)

    true = :ets.insert(keydir, {type_key, "stream", 0, LFU.initial(), 0, 0, 6})

    watermark_key = Keys.shared_value_ref_backfill_key(state.shard_index)
    progress_key = SharedRefBackfill.progress_key(state.shard_index)
    true = :ets.insert(keydir, {watermark_key, <<1>>, 0, LFU.initial(), 0, 0, 1})
    true = :ets.insert(keydir, {progress_key, "complete", 0, LFU.initial(), 0, 0, 8})

    :persistent_term.put(
      {SharedRefBackfill, :verified_complete, instance_name, state.shard_index},
      true
    )

    Tables.ensure_all()
    true = Meta.put_local(stream_key, 1, "1-0", "1-0", 1, 0, scope)
    true = Index.mark_ready(stream_key, scope)

    true =
      Index.insert_entry(
        stream_key,
        "1-0",
        CompoundKey.stream_prefix(stream_key) <> "1-0",
        scope
      )

    :ok = Waiters.register(stream_key, self(), "0-0", scope)

    native = NativeOrderedIndex.get(state.flow_index_name, state.flow_lookup_name)
    :ok = NativeOrderedIndex.put_member(native, flow_key, "member", 1)
    assert 1 == NativeOrderedIndex.count_all(native, flow_key)

    assert :ok =
             LMDB.write_batch(state.flow_lmdb_path, [
               {:put, "stale-flow-row", "value"},
               {:put, SharedRefBackfill.completion_key(state.shard_index), "stale-proof"}
             ])

    assert {:ok, "value"} = LMDB.get(state.flow_lmdb_path, "stale-flow-row")
    assert :ok = LMDBReplaySafeIndex.persist(state.shard_data_path, 99)
    assert :ok = HistoryProjectedIndex.persist(state.shard_data_path, 99)

    history_dir = Ferricstore.Flow.HistoryProjector.history_dir(state.shard_data_path)
    File.mkdir_p!(history_dir)
    File.write!(Path.join(history_dir, "stale.history"), "stale")

    assert {new_state, {:applied_at, 37, {:ok, 3}}, _effects} =
             StateMachine.apply(%{index: 37}, {:flush_shard, {1, 0}}, state)

    assert [] == :ets.lookup(keydir, type_key)
    assert [{^watermark_key, <<1>>, 0, _, _, _, 1}] = :ets.lookup(keydir, watermark_key)
    assert [{^progress_key, progress, 0, _, _, _, _}] = :ets.lookup(keydir, progress_key)

    assert {:shared_ref_backfill_progress, 2, _run_id, :complete, <<>>, 0} =
             :erlang.binary_to_term(progress, [:safe])

    assert [] == :ets.lookup(Ferricstore.Stream.Meta, {instance_name, stream_key})
    refute Index.ready?(stream_key, scope)
    assert 0 == Waiters.count(stream_key, scope)

    reset_native =
      NativeOrderedIndex.get(new_state.flow_index_name, new_state.flow_lookup_name)

    assert 0 == NativeOrderedIndex.count_all(reset_native, flow_key)
    assert :not_found == LMDB.get(state.flow_lmdb_path, "stale-flow-row")

    assert {:ok, certificate} =
             LMDB.get(
               state.flow_lmdb_path,
               SharedRefBackfill.completion_key(state.shard_index)
             )

    shard_index = state.shard_index

    assert {:shared_ref_backfill_complete, 2, ^shard_index, _run_id} =
             :erlang.binary_to_term(certificate, [:safe])

    refute File.exists?(history_dir)
    assert 37 == LMDBReplaySafeIndex.read(state.shard_data_path)
    assert 37 == HistoryProjectedIndex.read(state.shard_data_path)
    assert SharedRefBackfill.verified_complete?(instance_name, state.shard_index)
    assert 0 == :atomics.get(degraded, state.shard_index + 1)
  end

  test "derived cleanup failure keeps dedicated storage and succeeds on replay" do
    %{state: state, keydir: keydir} = start_state()
    key = "flush-derived-failure"
    true = :ets.insert(keydir, {key, "value", 0, LFU.initial(), 0, 0, 5})

    dedicated_root =
      Path.join([
        state.data_dir,
        "dedicated",
        "shard_#{state.shard_index}",
        "hash:retained"
      ])

    File.mkdir_p!(dedicated_root)
    File.write!(Path.join(dedicated_root, "00000.log"), "retained")

    Application.put_env(
      :ferricstore,
      :flush_derived_lmdb_clear_hook,
      fn _path -> {:error, :forced_lmdb_clear_failure} end
    )

    try do
      assert {_old_state,
              {:applied_at, 41,
               {:error,
                {:flush_shard_apply_failed,
                 {:flush_derived_state_cleanup_failed,
                  {:lmdb_clear_failed, :forced_lmdb_clear_failure}}}}}, _effects} =
               StateMachine.apply(%{index: 41}, {:flush_shard, {1, 0}}, state)

      assert [] == :ets.lookup(keydir, key)
      assert File.dir?(dedicated_root)
    after
      Application.delete_env(:ferricstore, :flush_derived_lmdb_clear_hook)
    end

    assert {_new_state, {:applied_at, 41, {:ok, 0}}, _effects} =
             StateMachine.apply(%{index: 41}, {:flush_shard, {1, 0}}, state)

    refute File.exists?(dedicated_root)
  end

  test "stream cache cleanup failure leaves the page durable rows replayable" do
    instance_name = :"flush_stream_retry_#{System.unique_integer([:positive])}"
    %{state: state, keydir: keydir} = start_state(instance_name: instance_name)
    stream_key = "flush-stream-retry"
    type_key = CompoundKey.type_key(stream_key)
    scope = %{cache_scope: instance_name}
    parent = self()

    true = :ets.insert(keydir, {type_key, "stream", 0, LFU.initial(), 0, 0, 6})
    Tables.ensure_all()
    true = Meta.put_local(stream_key, 1, "1-0", "1-0", 1, 0, scope)

    Process.put(:ferricstore_flush_stream_cleanup_hook, fn _state, roots ->
      send(parent, {:stream_cleanup_attempted, roots})
      {:error, :forced_stream_cleanup_failure}
    end)

    try do
      assert {_old_state,
              {:error,
               {:flush_shard_apply_failed,
                {:flush_shard_delete_failed,
                 {:stream_cache_cleanup_failed, :forced_stream_cleanup_failure}}}}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

      assert_received {:stream_cleanup_attempted, [^stream_key]}
      assert [{^type_key, "stream", 0, _, 0, 0, 6}] = :ets.lookup(keydir, type_key)

      assert [{_, _, _, _, _, _}] =
               :ets.lookup(Ferricstore.Stream.Meta, {instance_name, stream_key})
    after
      Process.delete(:ferricstore_flush_stream_cleanup_hook)
    end

    assert {_new_state, {:ok, 1}} =
             StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

    assert [] == :ets.lookup(keydir, type_key)
    assert [] == :ets.lookup(Ferricstore.Stream.Meta, {instance_name, stream_key})
  end

  test "dedicated parent fsync failure is storage-classified and replayable" do
    %{state: state} = start_state()
    dedicated_parent = Path.join(state.data_dir, "dedicated")
    dedicated_root = Path.join(dedicated_parent, "shard_#{state.shard_index}")
    File.mkdir_p!(dedicated_root)
    File.write!(Path.join(dedicated_root, "00000.log"), "retained")

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      assert path == dedicated_parent

      if Process.get(:flush_dedicated_fsync_failed, false) do
        :ok
      else
        Process.put(:flush_dedicated_fsync_failed, true)
        {:error, :forced_dedicated_parent_fsync_failure}
      end
    end)

    try do
      assert {_old_state,
              {:error,
               {:flush_shard_apply_failed,
                {:fsync_dedicated_parent_failed, ^dedicated_parent,
                 :forced_dedicated_parent_fsync_failure}}}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

      refute File.exists?(dedicated_root)

      assert {_new_state, {:ok, replay_deleted}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

      assert replay_deleted >= 0
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
      Process.delete(:flush_dedicated_fsync_failed)
    end
  end

  test "replayed cleanup fsyncs an existing dedicated parent when the shard root is absent" do
    %{state: state} = start_state()
    dedicated_parent = Path.join(state.data_dir, "dedicated")
    File.mkdir_p!(dedicated_parent)
    parent = self()

    Process.put(:ferricstore_promotion_fsync_dir_hook, fn path ->
      send(parent, {:dedicated_parent_fsynced, path})
      :ok
    end)

    try do
      assert {_new_state, {:ok, 0}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)
    after
      Process.delete(:ferricstore_promotion_fsync_dir_hook)
    end

    assert_received {:dedicated_parent_fsynced, ^dedicated_parent}
  end

  test "promotion marker retry reconciles partial append bytes before rotation accounting" do
    %{state: state, keydir: keydir, path: path} = start_state()
    marker_key = Promotion.marker_key("flush-marker-partial")
    true = :ets.insert(keydir, {marker_key, "hash", 0, LFU.initial(), 0, 0, 4})
    {:ok, %{size: initial_size}} = File.stat(path)

    Process.put(:ferricstore_promotion_marker_append_hook, fn active_path, ops ->
      case Process.get(:flush_marker_partial_injected, false) do
        false ->
          Process.put(:flush_marker_partial_injected, true)
          assert {:ok, [_location]} = NIF.v2_append_ops_batch(active_path, [hd(ops)])
          {:error, :forced_after_partial_append}

        true ->
          :passthrough
      end
    end)

    try do
      assert {_old_state,
              {:error,
               {:bitcask_append_failed,
                {:flush_promoted_cleanup_failed,
                 {:append_promotion_marker_tombstones_failed, :forced_after_partial_append}}}}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

      {:ok, %{size: partial_size}} = File.stat(path)
      assert partial_size > initial_size

      assert {new_state, {:ok, 1}} =
               StateMachine.apply(%{}, {:flush_shard, {1, 0}}, state)

      {:ok, %{size: final_size}} = File.stat(new_state.active_file_path)
      assert final_size == new_state.active_file_size

      assert {^final_size, _dead_bytes} =
               Map.fetch!(new_state.file_stats, new_state.active_file_id)
    after
      Process.delete(:ferricstore_promotion_marker_append_hook)
      Process.delete(:flush_marker_partial_injected)
    end
  end

  defp start_state(opts \\ []) do
    shard_index = 20_000 + System.unique_integer([:positive])
    root = Path.join(System.tmp_dir!(), "flush-shard-#{shard_index}")
    shard_path = Ferricstore.DataDir.shard_data_path(root, shard_index)
    path = Path.join(shard_path, "00000.log")
    File.mkdir_p!(shard_path)
    File.touch!(path)

    keydir = :ets.new(:flush_shard_apply, [:set, :public])

    state =
      StateMachine.init(%{
        shard_index: shard_index,
        shard_data_path: shard_path,
        active_file_id: 0,
        active_file_path: path,
        ets: keydir,
        instance_name: Keyword.get(opts, :instance_name, :default)
      })

    {:ok, writer} = BitcaskWriter.start_link(shard_index: shard_index)

    on_exit(fn ->
      if Process.alive?(writer) do
        try do
          GenServer.stop(writer)
        catch
          :exit, _reason -> :ok
        end
      end

      if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
      SharedRefBackfill.invalidate_verified_shard!(state.instance_name, state.shard_index)
      _ = LMDB.release(state.flow_lmdb_path)
      File.rm_rf!(root)
    end)

    %{state: state, keydir: keydir, path: path}
  end
end
