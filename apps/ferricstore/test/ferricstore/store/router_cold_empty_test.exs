defmodule Ferricstore.Store.RouterColdEmptyTest do
  @moduledoc false
  use ExUnit.Case, async: false

  @record_header_size 26

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Stats
  alias Ferricstore.Test.IsolatedInstance

  setup do
    ctx = IsolatedInstance.checkout(shard_count: 1, hot_cache_max_value_size: 1024)
    shard = Process.whereis(elem(ctx.shard_names, 0))
    keydir = elem(ctx.keydir_refs, 0)

    on_exit(fn -> IsolatedInstance.checkin(ctx) end)

    %{ctx: ctx, shard: shard, keydir: keydir}
  end

  test "get_with_file_ref treats cold empty values as valid file refs", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    key = "cold_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    :ok = GenServer.call(shard, {:put, key, "", 0})
    :ok = GenServer.call(shard, :flush)

    assert [{^key, "", exp, lfu, fid, off, 0}] = :ets.lookup(keydir, key)
    :ets.insert(keydir, {key, nil, exp, lfu, fid, off, 0})

    assert {:cold_ref, path, value_offset, 0} = Router.get_with_file_ref(ctx, key)
    assert File.exists?(path)
    assert is_integer(value_offset)
  end

  test "get_with_file_ref falls back on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert :miss == Router.get_with_file_ref(ctx, key)
  end

  test "get_with_file_ref retries when compaction changes the cold row after validation misses",
       %{
         ctx: ctx,
         keydir: keydir
       } do
    key = "cold_sendfile_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    value = "compacted-value"
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    stale_path = Path.join(shard_path, "00000.log")
    current_path = Path.join(shard_path, "00001.log")

    {:ok, {stale_offset, _stale_record_size}} =
      NIF.v2_append_record(stale_path, other_key, "wrong", 0)

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, stale_offset, value_size})

    Process.put(:ferricstore_router_validate_file_ref_miss_hook, fn ->
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, value_size})
    end)

    try do
      assert {:cold_ref, ^current_path, value_offset, ^value_size} =
               Router.get_with_file_ref(ctx, key)

      assert is_integer(value_offset)
    after
      Process.delete(:ferricstore_router_validate_file_ref_miss_hook)
    end
  end

  test "direct cold reads do not crash on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_get:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.get(ctx, key)
    assert nil == Router.get_meta(ctx, key)
  end

  test "failed direct cold GET increments keyspace misses", %{ctx: ctx, keydir: keydir} do
    key = "cold_missing_get_stats:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 99, 0, 5})

    before_misses = Stats.keyspace_misses(ctx)

    assert nil == Router.get(ctx, key)
    assert Stats.keyspace_misses(ctx) == before_misses + 1
  end

  test "failed direct cold GET_META increments misses without cold-read success", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_missing_get_meta_stats:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 99, 0, 5})

    before_misses = Stats.keyspace_misses(ctx)
    before_cold_reads = Stats.total_cold_reads(ctx)

    assert nil == Router.get_meta(ctx, key)
    assert Stats.keyspace_misses(ctx) == before_misses + 1
    assert Stats.total_cold_reads(ctx) == before_cold_reads
  end

  test "direct cold read retries when compaction replaces file before ETS offset update", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_compaction_gap:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "compacted-gap-value"
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, {_dead_offset, _dead_record_size}} = NIF.v2_append_record(path, "dead", "old", 0)
    {:ok, {old_offset, _old_record_size}} = NIF.v2_append_record(path, key, value, 0)

    File.rm!(path)
    {:ok, {new_offset, _new_record_size}} = NIF.v2_append_record(path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, old_offset, value_size})

    Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, new_offset, value_size})
    end)

    try do
      assert ^value = Router.get(ctx, key)
    after
      Process.delete(:ferricstore_router_cold_location_miss_hook)
    end
  end

  test "direct cold read waits through a delayed compaction ETS update", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_compaction_delayed:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "delayed-compacted-value"
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, {_dead_offset, _dead_record_size}} = NIF.v2_append_record(path, "dead", "old", 0)
    {:ok, {old_offset, _old_record_size}} = NIF.v2_append_record(path, key, value, 0)

    File.rm!(path)
    {:ok, {new_offset, _new_record_size}} = NIF.v2_append_record(path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, old_offset, value_size})

    Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
      misses = Process.get(:delayed_compaction_misses, 0) + 1
      Process.put(:delayed_compaction_misses, misses)

      if misses == 2 do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, new_offset, value_size})
      end
    end)

    try do
      assert ^value = Router.get(ctx, key)
    after
      Process.delete(:ferricstore_router_cold_location_miss_hook)
      Process.delete(:delayed_compaction_misses)
    end
  end

  test "batch cold reads do not crash on cold rows with invalid offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_invalid_batch:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert [nil] == Router.batch_get(ctx, [key])
  end

  test "failed batch cold GET increments keyspace misses", %{ctx: ctx, keydir: keydir} do
    key = "cold_batch_missing_stats:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 9, 0, 5})

    before_misses = Stats.keyspace_misses(ctx)
    before_cold_reads = Stats.total_cold_reads(ctx)

    assert [nil] == Router.batch_get(ctx, [key])
    assert Stats.keyspace_misses(ctx) == before_misses + 1
    assert Stats.total_cold_reads(ctx) == before_cold_reads
  end

  test "batch cold read top-level errors preserve reason for telemetry" do
    source = File.read!(Path.expand("../../../lib/ferricstore/store/router.ex", __DIR__))
    [_before, section] = String.split(source, "{:error, _reason} ->", parts: 2)
    [branch | _after] = String.split(section, "    end\n\n    entry_values", parts: 2)

    assert branch =~ "List.duplicate({:error, reason}, length(entries))",
           "top-level batch pread errors must preserve the reason instead of becoming nil_from_cold_location"
  end

  test "batch cold read length mismatch emits explicit telemetry reason", %{
    ctx: ctx,
    keydir: keydir
  } do
    key1 = "cold_batch_mismatch_1:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    key2 = "cold_batch_mismatch_2:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    :ets.insert(keydir, {key1, nil, 0, LFU.initial(), 0, 0, 5})
    :ets.insert(keydir, {key2, nil, 0, LFU.initial(), 0, 10, 5})

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    attach_pread_corrupt_handler()
    Process.put(:ferricstore_router_pread_batch_keyed_result, {:ok, ["only-one-value"]})

    try do
      assert [nil, nil] == Router.batch_get(ctx, [key1, key2])

      assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 2},
                      %{path: ^path, reason: :batch_result_length_mismatch}}
    after
      Process.delete(:ferricstore_router_pread_batch_keyed_result)
    end
  end

  test "direct cold reads do not return a value from a mismatched key offset", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_stale_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
      NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

    assert nil == Router.get(ctx, key)
    assert nil == Router.get_meta(ctx, key)
  end

  test "batch cold reads do not return values from mismatched key offsets", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_batch_stale_offset:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, [{other_offset, _}, {_key_offset, value_size}]} =
      NIF.v2_append_batch(path, [{other_key, "wrong-value", 0}, {key, "right-value", 0}])

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, other_offset, value_size})

    assert [nil] == Router.batch_get(ctx, [key])
  end

  test "direct cold reads emit telemetry when a cold record cannot be decoded", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_direct_corrupt:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "stable-value"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, {offset, _record_size}} = NIF.v2_append_record(path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, offset, byte_size(value)})

    {:ok, fd} = :file.open(path, [:read, :write, :binary])
    corrupt_at = offset + @record_header_size + byte_size(key)
    :ok = :file.pwrite(fd, corrupt_at, <<"X">>)
    :ok = :file.close(fd)

    attach_pread_corrupt_handler()

    assert nil == Router.get(ctx, key)

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^path, reason: :corrupt_record}}
  end

  test "direct cold reads emit telemetry when a cold file is missing", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_direct_missing_file:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    missing_path = Path.join(shard_path, "00009.log")

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 9, 0, 5})

    attach_pread_corrupt_handler()

    assert nil == Router.get(ctx, key)

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^missing_path, reason: :missing_file}}
  end

  test "direct cold reads retry when ETS changes after a cold read miss", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_direct_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "compacted-direct-value"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    current_path = Path.join(shard_path, "00001.log")
    missing_path = Path.join(shard_path, "00009.log")

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 9, 0, byte_size(value)})

    attach_pread_corrupt_handler(fn ->
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, byte_size(value)})
    end)

    assert ^value = Router.get(ctx, key)

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^missing_path, reason: :missing_file}}
  end

  test "direct cold meta reads retry when ETS changes after a cold read miss", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_meta_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "compacted-meta-value"
    expire_at_ms = System.system_time(:millisecond) + 60_000
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    current_path = Path.join(shard_path, "00001.log")
    missing_path = Path.join(shard_path, "00009.log")

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, expire_at_ms)

    :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), 9, 0, byte_size(value)})

    attach_pread_corrupt_handler(fn ->
      :ets.insert(
        keydir,
        {key, nil, expire_at_ms, LFU.initial(), 1, current_offset, byte_size(value)}
      )
    end)

    assert {^value, ^expire_at_ms} = Router.get_meta(ctx, key)

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^missing_path, reason: :missing_file}}
  end

  test "batch cold reads emit telemetry when a cold record cannot be decoded", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_batch_corrupt:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    value = "stable-value"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = Path.join(shard_path, "00000.log")

    {:ok, {offset, _record_size}} = NIF.v2_append_record(path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, offset, byte_size(value)})

    {:ok, fd} = :file.open(path, [:read, :write, :binary])
    corrupt_at = offset + @record_header_size + byte_size(key)
    :ok = :file.pwrite(fd, corrupt_at, <<"X">>)
    :ok = :file.close(fd)

    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :pread_corrupt],
        fn event, measurements, metadata, _config ->
          send(parent, {:pread_corrupt, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert [nil] == Router.batch_get(ctx, [key])

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^path, reason: :corrupt_record}}
  end

  test "batch cold reads emit telemetry when a cold file is missing", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_batch_missing_file:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    missing_path = Path.join(shard_path, "00009.log")

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 9, 0, 5})

    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :pread_corrupt],
        fn event, measurements, metadata, _config ->
          send(parent, {:pread_corrupt, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert [nil] == Router.batch_get(ctx, [key])

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^missing_path, reason: :missing_file}}
  end

  test "batch cold reads retry when ETS changes after a cold read miss", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_batch_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    good_key = key <> ":good"
    value = "compacted-batch-value"
    good_value = "batch-good-value"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    current_path = Path.join(shard_path, "00001.log")
    good_path = Path.join(shard_path, "00002.log")
    missing_path = Path.join(shard_path, "00009.log")

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, 0)

    {:ok, {good_offset, _good_record_size}} =
      NIF.v2_append_record(good_path, good_key, good_value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 9, 0, byte_size(value)})
    :ets.insert(keydir, {good_key, nil, 0, LFU.initial(), 2, good_offset, byte_size(good_value)})

    attach_pread_corrupt_handler(fn ->
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, byte_size(value)})
    end)

    assert [^value, ^good_value] = Router.batch_get(ctx, [key, good_key])

    assert_receive {:pread_corrupt, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: ^missing_path, reason: :missing_file}}
  end

  test "batch cold read corruption in one file does not hide valid keys from another file", %{
    ctx: ctx,
    keydir: keydir
  } do
    bad_key = "cold_batch_bad_file:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    same_file_good_key =
      "cold_batch_same_file_good:" <> Integer.to_string(:erlang.unique_integer([:positive]))

    good_key = "cold_batch_good_file:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    same_file_good_value = "same-file-readable"
    good_value = "still-readable"
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    bad_path = Path.join(shard_path, "00000.log")
    good_path = Path.join(shard_path, "00001.log")

    {:ok, {same_file_good_offset, _same_file_good_record_size}} =
      NIF.v2_append_record(bad_path, same_file_good_key, same_file_good_value, 0)

    {:ok, {bad_offset, _bad_record_size}} = NIF.v2_append_record(bad_path, bad_key, "bad", 0)

    {:ok, {good_offset, _good_record_size}} =
      NIF.v2_append_record(good_path, good_key, good_value, 0)

    :ets.insert(
      keydir,
      {same_file_good_key, nil, 0, LFU.initial(), 0, same_file_good_offset,
       byte_size(same_file_good_value)}
    )

    :ets.insert(keydir, {bad_key, nil, 0, LFU.initial(), 0, bad_offset, 3})
    :ets.insert(keydir, {good_key, nil, 0, LFU.initial(), 1, good_offset, byte_size(good_value)})

    {:ok, fd} = :file.open(bad_path, [:read, :write, :binary])
    corrupt_at = bad_offset + @record_header_size + byte_size(bad_key)
    :ok = :file.pwrite(fd, corrupt_at, <<"X">>)
    :ok = :file.close(fd)

    assert [^same_file_good_value, nil, ^good_value] =
             Router.batch_get(ctx, [same_file_good_key, bad_key, good_key])
  end

  test "batch_get preserves mixed cold result order including empty values", %{
    ctx: ctx,
    shard: shard,
    keydir: keydir
  } do
    cold_empty = "cold_batch_empty:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    cold_large = "cold_batch_large:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    hot = "cold_batch_hot:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    large_value = :binary.copy("x", 2_048)

    :ok = GenServer.call(shard, {:put, cold_empty, "", 0})
    :ok = GenServer.call(shard, {:put, cold_large, large_value, 0})
    :ok = GenServer.call(shard, {:put, hot, "hot", 0})
    :ok = GenServer.call(shard, :flush)

    assert [{^cold_empty, "", exp_empty, lfu_empty, fid_empty, off_empty, 0}] =
             :ets.lookup(keydir, cold_empty)

    :ets.insert(keydir, {cold_empty, nil, exp_empty, lfu_empty, fid_empty, off_empty, 0})

    assert [{^cold_large, _stored, exp_large, lfu_large, fid_large, off_large, vsize_large}] =
             :ets.lookup(keydir, cold_large)

    :ets.insert(
      keydir,
      {cold_large, nil, exp_large, lfu_large, fid_large, off_large, vsize_large}
    )

    assert Router.batch_get(ctx, [cold_large, "missing", hot, cold_empty, cold_large]) ==
             [large_value, nil, "hot", "", large_value]
  end

  test "get_file_ref rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_sendfile:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.get_file_ref(ctx, key)
  end

  test "get_file_ref retries when compaction changes the cold row after validation misses", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_file_ref_compacted:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    value = "compacted-file-ref"
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    stale_path = Path.join(shard_path, "00000.log")
    current_path = Path.join(shard_path, "00001.log")

    {:ok, {stale_offset, _stale_record_size}} =
      NIF.v2_append_record(stale_path, other_key, "wrong", 0)

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, stale_offset, value_size})

    Process.put(:ferricstore_router_validate_file_ref_miss_hook, fn ->
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, value_size})
    end)

    try do
      assert {^current_path, value_offset, ^value_size} = Router.get_file_ref(ctx, key)
      assert is_integer(value_offset)
    after
      Process.delete(:ferricstore_router_validate_file_ref_miss_hook)
    end
  end

  test "get_file_ref waits through a delayed compaction ETS update", %{
    ctx: ctx,
    keydir: keydir
  } do
    key = "cold_file_ref_delayed:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    other_key = key <> ":other"
    value = "delayed-file-ref"
    value_size = byte_size(value)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    stale_path = Path.join(shard_path, "00000.log")
    current_path = Path.join(shard_path, "00001.log")

    {:ok, {stale_offset, _stale_record_size}} =
      NIF.v2_append_record(stale_path, other_key, "wrong", 0)

    {:ok, {current_offset, _current_record_size}} =
      NIF.v2_append_record(current_path, key, value, 0)

    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, stale_offset, value_size})

    Process.put(:ferricstore_router_cold_location_miss_hook, fn ->
      misses = Process.get(:delayed_file_ref_misses, 0) + 1
      Process.put(:delayed_file_ref_misses, misses)

      if misses == 2 do
        :ets.insert(keydir, {key, nil, 0, LFU.initial(), 1, current_offset, value_size})
      end
    end)

    try do
      assert {^current_path, value_offset, ^value_size} = Router.get_file_ref(ctx, key)
      assert is_integer(value_offset)
    after
      Process.delete(:ferricstore_router_cold_location_miss_hook)
      Process.delete(:delayed_file_ref_misses)
    end
  end

  test "get_keydir_file_ref rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_file_ref:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert :miss == Router.get_keydir_file_ref(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "value_size rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_value_size:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.value_size(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "value_size preserves live pending cold rows", %{ctx: ctx, keydir: keydir} do
    key = "cold_pending_value_size:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), :pending, 0, 8192})

    assert 8192 == Router.value_size(ctx, key)
    assert [{^key, nil, 0, _lfu, :pending, 0, 8192}] = :ets.lookup(keydir, key)
  end

  test "exists rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_exists:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    refute Router.exists?(ctx, key)
    refute Router.exists_fast?(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "expire_at rejects cold rows with invalid offsets", %{ctx: ctx, keydir: keydir} do
    key = "cold_invalid_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    :ets.insert(keydir, {key, nil, 0, LFU.initial(), 0, :pending_offset, 5})

    assert nil == Router.expire_at_ms(ctx, key)
    assert [] == :ets.lookup(keydir, key)
  end

  test "expire_at_ms preserves live pending cold rows", %{ctx: ctx, keydir: keydir} do
    key = "cold_pending_expire_at:" <> Integer.to_string(:erlang.unique_integer([:positive]))
    expire_at_ms = System.system_time(:millisecond) + 60_000
    :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), :pending, 0, 8192})

    assert expire_at_ms == Router.expire_at_ms(ctx, key)
    assert [{^key, nil, ^expire_at_ms, _lfu, :pending, 0, 8192}] = :ets.lookup(keydir, key)
  end

  defp attach_pread_corrupt_handler(callback \\ fn -> :ok end) do
    parent = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :pread_corrupt],
        fn event, measurements, metadata, _config ->
          callback.()
          send(parent, {:pread_corrupt, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
