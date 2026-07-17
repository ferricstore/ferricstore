defmodule Ferricstore.Store.Shard.NativeOpsTest do
  use ExUnit.Case, async: true

  @native_ops_path Path.expand("../../../../lib/ferricstore/store/shard/native_ops.ex", __DIR__)

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.NativeOps

  describe "direct atomic persistence failures" do
    test "CAS reports the append failure and restores the previous row" do
      keydir = new_keydir("cas_failure")
      state = direct_state(keydir)
      put_persisted(state, "key", "old", 0)

      try do
        assert {:reply, {:error, _reason}, failed_state} =
                 NativeOps.handle_cas("key", "old", "new", nil, state)

        assert {:hit, "old", 0} = ShardETS.ets_lookup(state, "key")
        assert failed_state.pending == []
        assert failed_state.pending_count == 0
        assert failed_state.write_version == state.write_version
      after
        :ets.delete(keydir)
      end
    end

    test "LOCK reports the append failure and removes a newly inserted row" do
      keydir = new_keydir("lock_failure")
      state = direct_state(keydir)

      try do
        assert {:reply, {:error, _reason}, failed_state} =
                 NativeOps.handle_lock("lock", "owner", 5_000, state)

        assert [] == :ets.lookup(keydir, "lock")
        assert failed_state.pending == []
        assert failed_state.write_version == state.write_version
      after
        :ets.delete(keydir)
      end
    end

    test "EXTEND reports the append failure and restores the old expiry" do
      keydir = new_keydir("extend_failure")
      state = direct_state(keydir)
      old_expiry = Ferricstore.HLC.now_ms() + 60_000
      put_persisted(state, "lock", "owner", old_expiry)

      try do
        assert {:reply, {:error, _reason}, failed_state} =
                 NativeOps.handle_extend("lock", "owner", 120_000, state)

        assert {:hit, "owner", ^old_expiry} = ShardETS.ets_lookup(state, "lock")
        assert failed_state.pending == []
        assert failed_state.write_version == state.write_version
      after
        :ets.delete(keydir)
      end
    end

    test "rate-limit reports the append failure without exposing uncommitted state" do
      keydir = new_keydir("ratelimit_failure")
      state = direct_state(keydir)

      try do
        assert {:reply, {:error, _reason}, failed_state} =
                 NativeOps.handle_ratelimit_add_direct("limit", 10_000, 10, 1, state)

        assert [] == :ets.lookup(keydir, "limit")
        assert failed_state.pending == []
        assert failed_state.pending_count == 0
      after
        :ets.delete(keydir)
      end
    end

    test "UNLOCK does not account a live record as dead when the tombstone append fails" do
      keydir = new_keydir("unlock_failure")
      state = direct_state(keydir)
      put_persisted(state, "lock", "owner", 0)

      try do
        assert {:reply, {:error, _reason}, failed_state} =
                 NativeOps.handle_unlock("lock", "owner", state)

        assert {:hit, "owner", 0} = ShardETS.ets_lookup(state, "lock")
        assert failed_state.file_stats == state.file_stats
        assert failed_state.write_version == state.write_version
      after
        :ets.delete(keydir)
      end
    end
  end

  test "direct native handlers preserve Router-provided absolute expiry deadlines" do
    keydir = new_keydir("absolute_expiry")
    state = %{direct_state(keydir) | flush_in_flight: :in_flight}
    cas_deadline = Ferricstore.HLC.now_ms() + 60_000
    lock_deadline = cas_deadline + 60_000
    extend_deadline = lock_deadline + 60_000
    put_persisted(state, "cas", "old", 0)
    put_persisted(state, "lock", "owner", cas_deadline)

    try do
      assert {:reply, 1, _cas_state} =
               NativeOps.handle_cas("cas", "old", "new", cas_deadline, state)

      assert {:hit, "new", ^cas_deadline} = ShardETS.ets_lookup(state, "cas")

      assert {:reply, :ok, lock_state} =
               NativeOps.handle_lock("lock", "owner", lock_deadline, state)

      assert {:hit, "owner", ^lock_deadline} = ShardETS.ets_lookup(state, "lock")

      assert {:reply, 1, _extended_state} =
               NativeOps.handle_extend("lock", "owner", extend_deadline, lock_state)

      assert {:hit, "owner", ^extend_deadline} = ShardETS.ets_lookup(state, "lock")
    after
      :ets.delete(keydir)
    end
  end

  test "rate-limit rollover preserves counters above float integer precision" do
    dir =
      Path.join(
        System.tmp_dir!(),
        "native_ops_ratelimit_i64_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(dir)
    keydir = new_keydir("ratelimit_i64")
    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)
    previous_count = 9_007_199_254_740_993
    window_ms = 60_000

    instance_ctx = %{
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      hot_cache_max_value_size: 65_536,
      keydir_binary_bytes: :atomics.new(1, signed: true),
      name: :native_ops_ratelimit_i64
    }

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      instance_ctx: instance_ctx,
      keydir: keydir,
      index: 0,
      max_active_file_size: 64 * 1024 * 1024,
      merge_config: %{dead_bytes_threshold: 1_048_576, fragmentation_threshold: 0.5},
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: dir,
      write_version: 0
    }

    encoded =
      Ferricstore.Store.ValueCodec.encode_ratelimit(
        previous_count,
        Ferricstore.HLC.now_ms() - window_ms,
        0
      )

    put_persisted(state, "limit", encoded, 0)

    try do
      assert {:reply, ["denied", ^previous_count, 0, ttl_ms], new_state} =
               NativeOps.handle_ratelimit_add_direct(
                 "limit",
                 window_ms,
                 previous_count,
                 1,
                 state
               )

      assert ttl_ms > 0
      assert new_state.write_version == 1
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "forwarded compound result returns unknown outcome when local apply barrier times out" do
    shard_index = 0

    assert ErrorReasons.write_timeout_unknown() ==
             NativeOps.__barrier_forwarded_result__(
               shard_index,
               {:remote_applied_at, 1_000_000_000, :ok},
               25
             )
  end

  test "forwarded compound result returns leader result after local apply barrier passes" do
    shard_index = 0

    assert :ok ==
             NativeOps.__barrier_forwarded_result__(
               shard_index,
               {:remote_applied_at, 0, :ok},
               25
             )
  end

  test "raft list batch replies require exact per-command cardinality" do
    unknown = ErrorReasons.write_timeout_unknown()

    assert :ok == NativeOps.__normalize_batch_write_result_for_test__({:ok, [:ok, :ok]}, 2)

    assert ^unknown =
             NativeOps.__normalize_batch_write_result_for_test__({:ok, [:ok]}, 2)

    assert ^unknown =
             NativeOps.__normalize_batch_write_result_for_test__({:ok, [:ok, :ok, :ok]}, 2)
  end

  test "raft LMOVE submits one atomic state-machine command" do
    source = File.read!(@native_ops_path)
    body = function_body(source, "handle_list_op_lmove_raft")

    # LMOVE mutates source element, source metadata, destination element, and
    # destination metadata. In raft mode it must be one replicated command so
    # apply/replay sees one atomic pending-write batch instead of several
    # independently committed compound writes.
    assert body =~ "forced_quorum_call(state.index, {:list_op_lmove"
    refute body =~ "checked_lmove("
  end

  test "direct list compound_put does not update ETS when Bitcask append fails" do
    keydir = :ets.new(:"native_ops_test_#{System.unique_integer([:positive])}", [:set, :public])
    compound_key = CompoundKey.list_element("list", 0)

    state = %{
      active_file_path: Path.join(System.tmp_dir!(), "missing/native_ops.log"),
      active_file_id: 0,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      shard_data_path: System.tmp_dir!()
    }

    try do
      store = NativeOps.build_list_compound_store_direct("list", state)

      assert {:error, _reason} = store.compound_put.("list", compound_key, "value", 0)
      assert [] == :ets.lookup(keydir, compound_key)
    after
      :ets.delete(keydir)
    end
  end

  test "direct list writes update active file accounting" do
    dir =
      Path.join(System.tmp_dir!(), "native_ops_accounting_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"native_ops_accounting_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: dir,
      write_version: 0
    }

    try do
      {:reply, 1, new_state} = NativeOps.handle_list_op("list", {:rpush, ["value"]}, state)

      assert new_state.active_file_size > 0
      assert new_state.write_version == state.write_version + 1
      assert {total_bytes, 0} = Map.fetch!(new_state.file_stats, 0)
      assert total_bytes == new_state.active_file_size
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "direct list reads preserve the shard write version without touching the log" do
    keydir = new_keydir("list_read")
    state = direct_state(keydir)

    try do
      assert {:reply, 0, ^state} = NativeOps.handle_list_read("missing", :llen, state)
    after
      :ets.delete(keydir)
    end
  end

  test "direct list writes reject a malformed live plain-string row" do
    keydir = new_keydir("list_invalid_plain_row")
    state = direct_state(keydir)
    key = "list:invalid-plain-row"
    row = {key, nil, 0, 0, 0, :invalid_offset, 5}
    :ets.insert(keydir, row)

    try do
      assert {:reply, {:error, message}, ^state} =
               NativeOps.handle_list_op(key, {:rpush, ["value"]}, state)

      assert message =~ "WRONGTYPE"
      assert [^row] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "direct list batch put externalizes large values with one blob segment fsync" do
    dir =
      Path.join(System.tmp_dir!(), "native_ops_blob_batch_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"native_ops_blob_batch_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      data_dir: dir,
      instance_ctx: %{
        blob_side_channel_threshold_bytes: 128,
        hot_cache_max_value_size: 4096
      },
      keydir: keydir,
      index: 0,
      shard_data_path: dir
    }

    parent = self()

    Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
      send(parent, {:blob_fsync_file, path})
      Ferricstore.Bitcask.NIF.v2_fsync(path)
    end)

    try do
      store = NativeOps.build_list_compound_store_direct("list", state)
      first_key = CompoundKey.list_element("list", 0)
      second_key = CompoundKey.list_element("list", 1_000_000_000)
      first_payload = :binary.copy("A", 1024)
      second_payload = :binary.copy("B", 1024)

      assert :ok =
               store.compound_batch_put.("list", [
                 {first_key, first_payload, 0},
                 {second_key, second_payload, 0}
               ])

      assert {:hit, ^first_payload, 0} = ShardETS.ets_lookup(state, first_key)
      assert {:hit, ^second_payload, 0} = ShardETS.ets_lookup(state, second_key)

      assert_receive {:blob_fsync_file, first_path}, 1000
      refute_receive {:blob_fsync_file, _second_path}, 100
      assert String.ends_with?(first_path, ".bloblog")
    after
      Process.delete(:ferricstore_blob_store_fsync_file_hook)
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "direct list deletes update dead-byte accounting" do
    dir =
      Path.join(System.tmp_dir!(), "native_ops_dead_bytes_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"native_ops_dead_bytes_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_file_path = Path.join(dir, "00000.log")
    File.touch!(active_file_path)

    state = %{
      active_file_path: active_file_path,
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      instance_ctx: nil,
      keydir: keydir,
      index: 0,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: dir,
      write_version: 0
    }

    try do
      {:reply, 1, state} = NativeOps.handle_list_op("list", {:rpush, ["value"]}, state)
      {:reply, "value", state} = NativeOps.handle_list_op("list", {:lpop, 1}, state)

      assert {_total_bytes, dead_bytes} = Map.fetch!(state.file_stats, 0)
      assert dead_bytes > 0
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "defp #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n  end\n", parts: 2)
    body
  end

  defp new_keydir(name) do
    :ets.new(:"native_ops_#{name}_#{System.unique_integer([:positive])}", [:set, :public])
  end

  defp direct_state(keydir) do
    instance_ctx = %{
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      hot_cache_max_value_size: 65_536,
      keydir_binary_bytes: :atomics.new(1, signed: true)
    }

    %{
      active_file_path:
        Path.join(System.tmp_dir!(), "missing/native_ops_#{System.unique_integer()}.log"),
      active_file_id: 0,
      active_file_size: 0,
      file_stats: %{0 => {0, 0}},
      flush_in_flight: nil,
      index: 0,
      instance_ctx: instance_ctx,
      keydir: keydir,
      max_active_file_size: 64 * 1024 * 1024,
      pending: [],
      pending_count: 0,
      raft?: false,
      shard_data_path: System.tmp_dir!(),
      write_version: 7
    }
  end

  defp put_persisted(state, key, value, expire_at_ms) do
    ShardETS.ets_insert_with_location(
      state,
      key,
      value,
      expire_at_ms,
      0,
      0,
      byte_size(value)
    )
  end
end
