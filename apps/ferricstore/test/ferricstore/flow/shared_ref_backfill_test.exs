defmodule Ferricstore.Flow.SharedRefBackfillTest do
  use ExUnit.Case, async: false

  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Flow.RetentionGuard
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  setup do
    release_lmdb!()

    old_values =
      Map.new(
        [
          :flow_shared_ref_backfill_lmdb_hook,
          :flow_shared_ref_backfill_phase_hook,
          :flow_shared_ref_backfill_write_hook
        ],
        &{&1, Application.get_env(:ferricstore, &1)}
      )

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_shared_ref_backfill_test_#{System.unique_integer([:positive])}"
      )

    instance_name = :"shared_ref_backfill_test_#{System.unique_integer([:positive])}"
    keydir = :ets.new(:shared_ref_backfill_test_keydir, [:set, :public])
    shard_index = 0

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
    active_file_path = ShardETS.file_path(shard_path, 0)
    File.touch!(active_file_path)

    ctx = %{
      name: instance_name,
      data_dir: data_dir,
      shard_count: 1,
      keydir_refs: {keydir},
      blob_side_channel_threshold_bytes: 0
    }

    {flow_index, flow_lookup} = NativeOrderedIndex.table_names(instance_name, shard_index)
    NativeOrderedIndex.reset(flow_index, flow_lookup)

    on_exit(fn ->
      Enum.each(old_values, fn
        {key, nil} -> Application.delete_env(:ferricstore, key)
        {key, value} -> Application.put_env(:ferricstore, key, value)
      end)

      release_lmdb!()
      if :ets.info(keydir) != :undefined, do: :ets.delete(keydir)
      File.rm_rf!(data_dir)
    end)

    {:ok,
     data_dir: data_dir,
     shard_path: shard_path,
     active_file_path: active_file_path,
     shard_index: shard_index,
     keydir: keydir,
     ctx: ctx,
     flow_index: flow_index,
     flow_lookup: flow_lookup}
  end

  test "corrupt keydir state fails closed without publishing the watermark", test_ctx do
    state_key = Keys.state_key("corrupt-primary", "tenant")
    append_primary!(test_ctx, [{state_key, <<"not-a-flow-record">>}])

    assert_raise RuntimeError, ~r/shared-ref backfill.*decode/i, fn ->
      run!(test_ctx)
    end

    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "corrupt LMDB-only state fails closed without publishing the watermark", test_ctx do
    state_key = Keys.state_key("corrupt-lmdb", "tenant")

    assert :ok =
             LMDB.write_batch(LMDB.path(test_ctx.shard_path), [
               {:put, state_key, LMDB.encode_value(<<"not-a-flow-record">>, 0)}
             ])

    assert_raise RuntimeError, ~r/shared-ref backfill.*decode/i, fn ->
      run!(test_ctx)
    end

    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "LMDB scan errors do not advance to the final watermark", test_ctx do
    insert_record!(test_ctx, record("lmdb-error", payload_ref: shared_ref("external")))

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_lmdb_hook, fn
      :prefix_entries_after, _args -> {:error, :injected_lmdb_read_failure}
      _operation, _args -> :passthrough
    end)

    assert_raise RuntimeError, ~r/injected_lmdb_read_failure/, fn ->
      run!(test_ctx)
    end

    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "bounded pages resume idempotently after an interruption", test_ctx do
    Enum.each(1..7, fn index ->
      insert_record!(
        test_ctx,
        record("consumer-#{index}", payload_ref: shared_ref("external-#{index}"))
      )
    end)

    test_pid = self()

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn event ->
      send(test_pid, {:backfill_event, event})

      case event do
        {:page_persisted, :scan_work, %{processed: processed}} when processed >= 2 ->
          raise "injected migration interruption"

        _other ->
          :ok
      end
    end)

    assert_raise RuntimeError, ~r/injected migration interruption/, fn ->
      run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    end

    progress_key = SharedRefBackfill.progress_key(0)

    assert [{^progress_key, progress, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, progress_key)

    assert {:shared_ref_backfill_progress, 2, _run_id, :scan_work, cursor, processed} =
             :erlang.binary_to_term(progress, [:safe])

    assert is_binary(cursor) and cursor != ""
    assert processed >= 2
    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))

    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_phase_hook)
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    assert :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))

    events = drain_events([])

    assert Enum.all?(events, fn
             {:batch, _phase, %{items: items, bytes: bytes}} ->
               items <= 2 and bytes <= 2_048

             {:read_batch, _phase, %{items: items, bytes: bytes}} ->
               items <= 2 and (bytes <= 2_048 or items == 1)

             _other ->
               true
           end)

    Enum.each(1..7, fn index ->
      registry_key = Keys.shared_value_ref_registry_key("consumer-#{index}", "tenant")

      [{^registry_key, registry, 0, _lfu, _fid, _offset, _size}] =
        :ets.lookup(test_ctx.keydir, registry_key)

      assert :erlang.binary_to_term(registry, [:safe]) == [shared_ref("external-#{index}")]
    end)
  end

  test "migration preserves a newer guard and unions an existing registry", test_ctx do
    old_ref = shared_ref("old")
    new_ref = shared_ref("new")
    source_record = record("live-delta", version: 1, state_enter_seq: 1, payload_ref: old_ref)
    live_record = record("live-delta", version: 9, state_enter_seq: 99, payload_ref: new_ref)

    insert_record!(test_ctx, source_record)

    guard_key = Keys.retention_guard_key(source_record.id, source_record.partition_key)

    registry_key =
      Keys.shared_value_ref_registry_key(source_record.id, source_record.partition_key)

    count_key = Keys.shared_value_ref_count_key(new_ref, 0)

    append_primary!(test_ctx, [
      {guard_key, RetentionGuard.encode(live_record)},
      {registry_key, :erlang.term_to_binary([new_ref])},
      {count_key, :erlang.term_to_binary(1)}
    ])

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    assert [{^guard_key, guard, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, guard_key)

    assert guard == RetentionGuard.encode(live_record)

    assert [{^registry_key, registry, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, registry_key)

    assert :erlang.binary_to_term(registry, [:safe]) == Enum.sort([new_ref, old_ref])

    assert [{^count_key, count, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, count_key)

    assert :erlang.binary_to_term(count, [:safe]) == 1
  end

  test "owner resolution chooses the longest existing flow id", test_ctx do
    short = record("order")
    long = record("order:child")
    insert_record!(test_ctx, short)
    insert_record!(test_ctx, long)

    long_owned_key = Keys.governance_effect_key(long.id, "effect", long.partition_key)

    long_owned_value =
      :erlang.term_to_binary(
        {:flow_governance_effect_v1,
         %{flow_id: long.id, partition_key: long.partition_key, effect_key: "effect"}}
      )

    append_primary!(test_ctx, [{long_owned_key, long_owned_value}])

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    short_index = Keys.retention_cleanup_index_key(short.id, short.partition_key)
    long_index = Keys.retention_cleanup_index_key(long.id, long.partition_key)
    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)

    assert [] =
             NativeOrderedIndex.range_slice(native, short_index, :neg_inf, :inf, false, 0, :all)

    assert [{_member_key, +0.0}] =
             NativeOrderedIndex.range_slice(
               native,
               long_index,
               :neg_inf,
               :inf,
               false,
               0,
               :all
             )
  end

  test "raw user keys that resemble internal suffixes are never read", test_ctx do
    :ets.insert(test_ctx.keydir, {
      ":v:p:raw-user:1",
      nil,
      0,
      0,
      99_999,
      0,
      10
    })

    :ets.insert(test_ctx.keydir, {"X:user-stream", nil, 0, 0, 99_999, 0, 10})
    :ets.insert(test_ctx.keydir, {"abc}:svr:user", nil, 0, 0, 99_999, 0, 10})
    :ets.insert(test_ctx.keydir, {"f:{bogus}:s:user", nil, 0, 0, 99_999, 0, 10})
    :ets.insert(test_ctx.keydir, {"f:{bogus}:svr:user", nil, 0, 0, 99_999, 0, 10})

    :ets.insert(
      test_ctx.keydir,
      {"X:f:{f}:h:user" <> <<0>> <> "not-an-event", nil, 0, 0, 99_999, 0, 10}
    )

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    assert :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "malformed keydir rows fail closed", test_ctx do
    :ets.insert(test_ctx.keydir, {Keys.state_key("wrong-arity", "tenant"), <<"bad">>})

    assert_raise RuntimeError, ~r/invalid keydir/i, fn ->
      run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    end

    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "older guards are repaired and equal-version conflicts fail closed", test_ctx do
    current = record("guard-repair", version: 5, state_enter_seq: 50)
    stale = record("guard-repair", version: 4, state_enter_seq: 40)
    insert_record!(test_ctx, current)

    guard_key = Keys.retention_guard_key(current.id, current.partition_key)
    append_primary!(test_ctx, [{guard_key, RetentionGuard.encode(stale)}])

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    assert [{^guard_key, repaired, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, guard_key)

    assert repaired == RetentionGuard.encode(current)

    :ets.delete(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
    :ets.delete(test_ctx.keydir, SharedRefBackfill.progress_key(0))

    conflicting = record("guard-repair", version: 5, state_enter_seq: 51)
    append_primary!(test_ctx, [{guard_key, RetentionGuard.encode(conflicting)}])

    assert_raise RuntimeError, ~r/conflicting retention guard/i, fn ->
      run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    end
  end

  test "orphan counts are deleted and rebuilt overcounts can decrease", test_ctx do
    ref = shared_ref("shared-count")
    first = record("count-first", payload_ref: ref)
    second = record("count-second", payload_ref: ref)
    insert_record!(test_ctx, first)
    insert_record!(test_ctx, second)

    orphan_ref = shared_ref("orphan-count")
    orphan_count_key = Keys.shared_value_ref_count_key(orphan_ref, 0)
    append_primary!(test_ctx, [{orphan_count_key, :erlang.term_to_binary(9)}])

    mutated = :atomics.new(1, signed: false)

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn
      {:page_persisted, :count_refs, _metadata} ->
        if :atomics.exchange(mutated, 1, 1) == 0 do
          :ets.delete(test_ctx.keydir, Keys.state_key(second.id, second.partition_key))

          :ets.delete(
            test_ctx.keydir,
            Keys.shared_value_ref_registry_key(second.id, second.partition_key)
          )
        end

      _event ->
        :ok
    end)

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    count_key = Keys.shared_value_ref_count_key(ref, 0)

    assert [{^count_key, count, 0, _lfu, _fid, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, count_key)

    assert :erlang.binary_to_term(count, [:safe]) == 1
    refute :ets.member(test_ctx.keydir, orphan_count_key)
  end

  test "resume rebuilds native cleanup members after index reset", test_ctx do
    owned_ref = Keys.value_key("native-resume", :result, 1, "tenant")
    rec = record("native-resume", result_ref: owned_ref)
    insert_record!(test_ctx, rec)
    append_primary!(test_ctx, [{owned_ref, Flow.encode_value("result")}])

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn
      {:page_persisted, :cleanup_staging, _metadata} ->
        raise "interrupt before final native rebuild"

      _event ->
        :ok
    end)

    assert_raise RuntimeError, ~r/interrupt before final native rebuild/, fn ->
      run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    end

    NativeOrderedIndex.reset(test_ctx.flow_index, test_ctx.flow_lookup)
    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_phase_hook)
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)
    index_key = Keys.retention_cleanup_index_key(rec.id, rec.partition_key)

    assert [{_member_key, +0.0}] =
             NativeOrderedIndex.range_slice(native, index_key, :neg_inf, :inf, false, 0, :all)
  end

  test "governance owner comes from the encoded record, not an ambiguous key prefix", test_ctx do
    short = record("order")
    shadow = record("order:child")
    insert_record!(test_ctx, short)
    insert_record!(test_ctx, shadow)

    effect_key = Keys.governance_effect_key(short.id, "child:effect", short.partition_key)

    encoded_effect =
      :erlang.term_to_binary(
        {:flow_governance_effect_v1,
         %{
           flow_id: short.id,
           partition_key: short.partition_key,
           effect_key: "child:effect"
         }}
      )

    append_primary!(test_ctx, [{effect_key, encoded_effect}])
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)
    short_index = Keys.retention_cleanup_index_key(short.id, short.partition_key)
    shadow_index = Keys.retention_cleanup_index_key(shadow.id, shadow.partition_key)

    assert [{_member_key, +0.0}] =
             NativeOrderedIndex.range_slice(native, short_index, :neg_inf, :inf, false, 0, :all)

    assert [] =
             NativeOrderedIndex.range_slice(native, shadow_index, :neg_inf, :inf, false, 0, :all)
  end

  test "shared-link owner resolution skips a shadow flow that fails exact link validation",
       test_ctx do
    owner = record("link-owner")
    shadow = record("link-owner:name")
    insert_record!(test_ctx, owner)
    insert_record!(test_ctx, shadow)

    value_ref = Keys.value_key("link-owner:name", :shared, 1, owner.partition_key)
    link_key = Keys.shared_value_link_prefix(owner.id, owner.partition_key) <> "name:1"
    append_primary!(test_ctx, [{link_key, value_ref}, {value_ref, Flow.encode_value("value")}])

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)
    owner_index = Keys.retention_cleanup_index_key(owner.id, owner.partition_key)
    shadow_index = Keys.retention_cleanup_index_key(shadow.id, shadow.partition_key)

    assert 2 == NativeOrderedIndex.count_all(native, owner_index)
    assert 0 == NativeOrderedIndex.count_all(native, shadow_index)
  end

  test "LMDB-only state is migrated by bounded pages", test_ctx do
    ref = shared_ref("lmdb-only")
    rec = record("lmdb-only-consumer", payload_ref: ref)
    state_key = Keys.state_key(rec.id, rec.partition_key)

    assert :ok =
             LMDB.write_batch(LMDB.path(test_ctx.shard_path), [
               {:put, state_key, LMDB.encode_value(Flow.encode_record(rec), 0)}
             ])

    assert :ok = run!(test_ctx, batch_size: 4, batch_bytes: 2_048)
    registry_key = Keys.shared_value_ref_registry_key(rec.id, rec.partition_key)
    assert :ets.member(test_ctx.keydir, registry_key)
    refute :ets.member(test_ctx.keydir, state_key)
  end

  test "fast-path cleanup rebuild decodes an LMDB-only owner state", test_ctx do
    owned_ref = Keys.value_key("lmdb-member-owner", :result, 1, "tenant")
    rec = record("lmdb-member-owner", result_ref: owned_ref)
    state_key = Keys.state_key(rec.id, rec.partition_key)

    assert :ok =
             LMDB.write_batch(LMDB.path(test_ctx.shard_path), [
               {:put, state_key, LMDB.encode_value(Flow.encode_record(rec), 0)}
             ])

    append_primary!(test_ctx, [{owned_ref, Flow.encode_value("result")}])
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    NativeOrderedIndex.reset(test_ctx.flow_index, test_ctx.flow_lookup)
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)
    index_key = Keys.retention_cleanup_index_key(rec.id, rec.partition_key)
    assert 1 == NativeOrderedIndex.count_all(native, index_key)
  end

  test "LMDB write and manifest read failures never publish the watermark", test_ctx do
    insert_record!(test_ctx, record("lmdb-write-failure"))

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_lmdb_hook, fn
      :write_batch, _args -> {:error, :injected_lmdb_write_failure}
      _operation, _args -> :passthrough
    end)

    assert_raise RuntimeError, ~r/injected_lmdb_write_failure/, fn -> run!(test_ctx) end
    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))

    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_lmdb_hook)

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn
      {:page_persisted, :snapshot_keydir, _metadata} -> raise "leave resumable manifest"
      _event -> :ok
    end)

    assert_raise RuntimeError, ~r/leave resumable manifest/, fn ->
      run!(test_ctx, batch_size: 2)
    end

    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_phase_hook)

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_lmdb_hook, fn
      :get, _args -> {:error, :injected_manifest_read_failure}
      _operation, _args -> :passthrough
    end)

    assert_raise RuntimeError, ~r/injected_manifest_read_failure/, fn -> run!(test_ctx) end
    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
  end

  test "invalid append locations and corrupt final watermarks fail closed", test_ctx do
    Application.put_env(:ferricstore, :flow_shared_ref_backfill_write_hook, fn _path, rows ->
      {:ok, Enum.map(rows, fn _row -> {:invalid_offset, :invalid_size} end)}
    end)

    assert_raise RuntimeError, ~r/invalid locations/i, fn -> run!(test_ctx) end
    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_write_hook)

    append_primary!(test_ctx, [{Keys.shared_value_ref_backfill_key(0), <<2>>}])
    assert_raise RuntimeError, ~r/corrupt final watermark/i, fn -> run!(test_ctx) end
  end

  test "final native rebuild removes cleanup members whose owner state is gone", test_ctx do
    owner_id = "retained-owner"
    owned_ref = Keys.value_key(owner_id, :result, 1, "tenant")
    index_key = Keys.retention_cleanup_index_key(owner_id, "tenant")
    member_key = Keys.retention_cleanup_member_key(owner_id, owned_ref, "tenant")
    member = Ferricstore.Flow.RetentionCleanupMember.encode(index_key, owned_ref)
    native = NativeOrderedIndex.get(test_ctx.flow_index, test_ctx.flow_lookup)

    append_primary!(test_ctx, [{member_key, member}])
    assert :ok = NativeOrderedIndex.put_member(native, index_key, member_key, 0)

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    refute :ets.member(test_ctx.keydir, member_key)

    assert [] =
             NativeOrderedIndex.range_slice(native, index_key, :neg_inf, :inf, false, 0, :all)
  end

  test "final native rebuild rejects forged cleanup member ownership for a live owner",
       test_ctx do
    rec = record("member-owner")
    foreign = record("foreign-owner")
    owned_ref = Keys.value_key(rec.id, :result, 1, rec.partition_key)
    insert_record!(test_ctx, rec)
    insert_record!(test_ctx, foreign)
    append_primary!(test_ctx, [{owned_ref, Flow.encode_value("result")}])

    member_key = Keys.retention_cleanup_member_key(rec.id, owned_ref, rec.partition_key)
    foreign_index = Keys.retention_cleanup_index_key("foreign-owner", rec.partition_key)

    forged =
      Ferricstore.Flow.RetentionCleanupMember.encode(foreign_index, owned_ref)

    append_primary!(test_ctx, [
      {member_key, forged},
      {Keys.shared_value_ref_backfill_key(0), <<1>>}
    ])

    assert_raise RuntimeError, ~r/cleanup member.*owner|forged cleanup member/i, fn ->
      run!(test_ctx)
    end
  end

  test "a final marker without its LMDB certificate is repaired instead of trusted", test_ctx do
    ref = shared_ref("certificate-ref")
    rec = record("certificate-consumer", payload_ref: ref)
    insert_record!(test_ctx, rec)
    append_primary!(test_ctx, [{Keys.shared_value_ref_backfill_key(0), <<1>>}])

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    registry_key = Keys.shared_value_ref_registry_key(rec.id, rec.partition_key)
    assert :ets.member(test_ctx.keydir, registry_key)

    assert {:ok, certificate} =
             LMDB.get(
               LMDB.path(test_ctx.shard_path),
               SharedRefBackfill.completion_key(0)
             )

    assert {:shared_ref_backfill_complete, 2, 0, run_id} =
             :erlang.binary_to_term(certificate, [:safe])

    assert is_binary(run_id) and run_id != ""
    assert SharedRefBackfill.verified_complete?(test_ctx.ctx.name, 0)
  end

  test "forged finalize progress cannot manufacture a completion certificate", test_ctx do
    rec = record("forged-finalize")
    insert_record!(test_ctx, rec)

    forged_progress =
      :erlang.term_to_binary(
        {:shared_ref_backfill_progress, 2, "client-controlled-run", :finalize, <<>>, 0}
      )

    append_primary!(test_ctx, [{SharedRefBackfill.progress_key(0), forged_progress}])
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    assert :ets.member(
             test_ctx.keydir,
             Keys.retention_guard_key(rec.id, rec.partition_key)
           )

    assert {:ok, certificate} =
             LMDB.get(LMDB.path(test_ctx.shard_path), SharedRefBackfill.completion_key(0))

    assert {:shared_ref_backfill_complete, 2, 0, run_id} =
             :erlang.binary_to_term(certificate, [:safe])

    refute run_id == "client-controlled-run"
  end

  test "forged mid-run phase and cursor cannot skip staged state work", test_ctx do
    Enum.each(1..5, fn index ->
      insert_record!(test_ctx, record("forged-cursor-#{index}"))
    end)

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn
      {:page_persisted, :snapshot_keydir, %{processed: processed}} when processed >= 2 ->
        raise "interrupt with an authoritative LMDB cursor"

      _event ->
        :ok
    end)

    assert_raise RuntimeError, ~r/authoritative LMDB cursor/, fn ->
      run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    end

    progress_key = SharedRefBackfill.progress_key(0)

    [{^progress_key, encoded, 0, _lfu, _fid, _offset, _size}] =
      :ets.lookup(test_ctx.keydir, progress_key)

    {:shared_ref_backfill_progress, 2, run_id, _phase, _cursor, processed} =
      :erlang.binary_to_term(encoded, [:safe])

    forged =
      :erlang.term_to_binary(
        {:shared_ref_backfill_progress, 2, run_id, :scan_work, <<255>>, processed}
      )

    append_primary!(test_ctx, [{progress_key, forged}])
    Application.delete_env(:ferricstore, :flow_shared_ref_backfill_phase_hook)
    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)

    Enum.each(1..5, fn index ->
      assert :ets.member(
               test_ctx.keydir,
               Keys.retention_guard_key("forged-cursor-#{index}", "tenant")
             )
    end)
  end

  test "backfill primary persistence never calls blocking sync disk NIFs" do
    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/shared_ref_backfill.ex", __DIR__))

    refute source =~ "NIF.v2_append_batch("
    refute source =~ "NIF.v2_append_tombstone("
    refute source =~ "NIF.v2_fsync("
    assert source =~ "NIF.v2_append_batch_nosync("
    assert source =~ "NIF.v2_append_ops_batch_nosync("
    assert source =~ "NIF.v2_fsync_async("
  end

  test "keydir safe fixation is released before LMDB state pages", test_ctx do
    rec = record("fixed-table-release")
    state_key = Keys.state_key(rec.id, rec.partition_key)

    assert :ok =
             LMDB.write_batch(LMDB.path(test_ctx.shard_path), [
               {:put, state_key, LMDB.encode_value(Flow.encode_record(rec), 0)}
             ])

    test_pid = self()

    Application.put_env(:ferricstore, :flow_shared_ref_backfill_phase_hook, fn
      {:read_batch, :scan_lmdb_states, _metadata} ->
        send(test_pid, {:safe_fixed_during_lmdb, :ets.info(test_ctx.keydir, :safe_fixed)})

      _event ->
        :ok
    end)

    assert :ok = run!(test_ctx, batch_size: 2, batch_bytes: 2_048)
    assert_receive {:safe_fixed_during_lmdb, false}
  end

  test "empty-shard finalization republishes the proof chain after flush", test_ctx do
    assert :ok =
             SharedRefBackfill.finalize_empty_shard!(
               test_ctx.shard_path,
               test_ctx.keydir,
               test_ctx.shard_index,
               test_ctx.ctx,
               active_file_id: 0,
               active_file_path: test_ctx.active_file_path
             )

    assert [{watermark_key, <<1>>, 0, _lfu, 0, _offset, _size}] =
             :ets.lookup(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))

    assert watermark_key == Keys.shared_value_ref_backfill_key(0)
    assert SharedRefBackfill.verified_complete?(test_ctx.ctx.name, 0)

    assert {:ok, certificate} =
             LMDB.get(LMDB.path(test_ctx.shard_path), SharedRefBackfill.completion_key(0))

    assert {:shared_ref_backfill_complete, 2, 0, run_id} =
             :erlang.binary_to_term(certificate, [:safe])

    assert is_binary(run_id) and run_id != ""
  end

  test "empty-shard finalization refuses a nonempty keydir", test_ctx do
    :ets.insert(test_ctx.keydir, {"user-key", "value", 0, 0, 0, 0, 5})

    assert_raise RuntimeError, ~r/requires an empty keydir/, fn ->
      SharedRefBackfill.finalize_empty_shard!(
        test_ctx.shard_path,
        test_ctx.keydir,
        test_ctx.shard_index,
        test_ctx.ctx,
        active_file_id: 0,
        active_file_path: test_ctx.active_file_path
      )
    end

    refute :ets.member(test_ctx.keydir, Keys.shared_value_ref_backfill_key(0))
    refute SharedRefBackfill.verified_complete?(test_ctx.ctx.name, 0)
  end

  test "empty-shard finalization accepts only preserved local migration metadata", test_ctx do
    stale_progress =
      :erlang.term_to_binary(
        {:shared_ref_backfill_progress, 2, "pre-flush-run", :complete, <<>>, 9}
      )

    append_primary!(test_ctx, [
      {Keys.shared_value_ref_backfill_key(0), <<1>>},
      {SharedRefBackfill.progress_key(0), stale_progress}
    ])

    assert :ok =
             SharedRefBackfill.finalize_empty_shard!(
               test_ctx.shard_path,
               test_ctx.keydir,
               0,
               test_ctx.ctx,
               active_file_id: 0,
               active_file_path: test_ctx.active_file_path
             )

    assert :ets.info(test_ctx.keydir, :size) == 2
    assert SharedRefBackfill.verified_complete?(test_ctx.ctx.name, 0)

    [{progress_key, current_progress, 0, _lfu, 0, _offset, _size}] =
      :ets.lookup(test_ctx.keydir, SharedRefBackfill.progress_key(0))

    assert progress_key == SharedRefBackfill.progress_key(0)

    assert {:shared_ref_backfill_progress, 2, run_id, :complete, <<>>, 0} =
             :erlang.binary_to_term(current_progress, [:safe])

    refute run_id == "pre-flush-run"
  end

  defp run!(test_ctx, opts \\ []) do
    SharedRefBackfill.run!(
      test_ctx.shard_path,
      test_ctx.keydir,
      test_ctx.shard_index,
      test_ctx.ctx,
      test_ctx.flow_index,
      test_ctx.flow_lookup,
      Keyword.merge(
        [active_file_id: 0, active_file_path: test_ctx.active_file_path],
        opts
      )
    )
  end

  defp record(id, overrides \\ []) do
    Map.merge(
      %{
        id: id,
        type: "backfill-test",
        state: "queued",
        version: 1,
        attempts: 0,
        fencing_token: 0,
        created_at_ms: 1,
        updated_at_ms: 2,
        next_run_at_ms: 3,
        priority: 0,
        partition_key: "tenant",
        root_flow_id: id,
        state_enter_seq: 1
      },
      Map.new(overrides)
    )
  end

  defp shared_ref(id), do: Keys.value_key(id, :shared, 1, "shared-owner")

  defp insert_record!(test_ctx, record) do
    append_primary!(test_ctx, [
      {Keys.state_key(record.id, record.partition_key), Flow.encode_record(record)}
    ])
  end

  defp append_primary!(test_ctx, entries) do
    rows = Enum.map(entries, fn {key, value} -> {key, value, 0} end)
    assert {:ok, locations} = NIF.v2_append_batch(test_ctx.active_file_path, rows)

    Enum.zip(entries, locations)
    |> Enum.each(fn {{key, value}, {offset, _record_size}} ->
      :ets.insert(test_ctx.keydir, {key, value, 0, 0, 0, offset, byte_size(value)})
    end)
  end

  defp drain_events(acc) do
    receive do
      {:backfill_event, event} -> drain_events([event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp release_lmdb! do
    case LMDB.release_all() do
      :ok -> :ok
      {:ok, _released} -> :ok
    end
  end
end
