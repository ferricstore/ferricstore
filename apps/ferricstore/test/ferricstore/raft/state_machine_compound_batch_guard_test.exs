defmodule Ferricstore.Raft.StateMachineCompoundBatchGuardTest do
  @moduledoc false
  use ExUnit.Case, async: true
  @moduletag :raft

  test "state-machine command stores expose compound batch reads" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    # Hash/set/zset command handlers call Ops.compound_batch_get/3 for HMGET,
    # SMISMEMBER, ZMSCORE, and friends. During Raft apply the command store is
    # a map, so missing batch callbacks make Ops fall back to one cold read per
    # field. Keep the callback explicit so cold compound reads stay batched.
    assert source =~ "compound_batch_get:",
           "state-machine command store must provide compound_batch_get"

    assert source =~ "compound_batch_get_meta:",
           "state-machine command store must provide compound_batch_get_meta"
  end

  test "state-machine command stores expose compound batch writes" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()
    cross_shard_store = function_body(source, "build_cross_shard_store")

    # List push/pop and similar data-primitive mutations can touch many
    # compound keys. During Raft apply those writes already share one pending
    # NIF flush, so the command store must keep the batch callback explicit
    # and avoid regressing to per-element Ops fallback calls.
    assert source =~ "compound_batch_put:",
           "state-machine command store must provide compound_batch_put"

    assert source =~ "compound_batch_delete:",
           "state-machine command store must provide compound_batch_delete"

    assert cross_shard_store =~ "compound_batch_put:",
           "cross-shard state-machine store must provide compound_batch_put"

    assert cross_shard_store =~ "compound_batch_delete:",
           "cross-shard state-machine store must provide compound_batch_delete"
  end

  test "state-machine command stores expose plain batch reads" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    # MGET, PFCOUNT, PFMERGE, and BITOP call Ops.batch_get/2.
    # During Raft apply the store is a map, so missing batch_get callbacks
    # make Ops fall back to one closure call and one possible cold-read waiter
    # per key. Keep this explicit to preserve batched cold reads in apply.
    assert source =~ "batch_get: fn keys ->" and
             (source =~ "cross_shard_batch_read(ctx, keys)" or
                source =~ "cross_shard_routed_batch_read(keys, ctx_for_key)"),
           "state-machine command store must provide batch_get for plain multi-key reads"
  end

  test "transaction apply stores do not expose process-global Router callbacks" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    refute source =~ "keys: fn -> Router.keys(instance_ctx) end"
    refute source =~ "Router.dbsize(instance_ctx)"
    refute source =~ "Router.list_op(instance_ctx, key, op)"
    refute source =~ "prob_write: prob_write"

    refute source =~
             "sm_tx_pending_meta(key) != nil or cross_shard_ets_exists?(ctx, key)\n            sm_tx_pending_meta(key) != nil or cross_shard_ets_exists?(ctx, key)"
  end

  test "state-machine compound batch metadata reads use keyed batched cold path" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()
    body = function_body(source, "cross_shard_read_cold_meta_bitcask_batch")

    # HGETEX/HEXPIRE-style logic reads value+TTL for many fields. If this
    # helper falls back to one pread per field, large hashes/sets/zsets create
    # one waiter per cold member. Keep the promoted-aware batched reader.
    assert body =~ "ColdRead.pread_batch_keyed",
           "state-machine compound batch metadata reads must use the keyed batched cold reader"
  end

  test "state-machine pop commands use bounded ordered indexes" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [_before_spop, spop_and_after] = String.split(source, "defp do_spop(state", parts: 2)
    [spop, zpop_and_after] = String.split(spop_and_after, "defp do_zpop(", parts: 2)

    [zpop, _after_zpop] =
      String.split(zpop_and_after, "defp deterministic_member_cursor", parts: 2)

    assert spop =~ "CompoundMemberIndex.member_slice("
    refute spop =~ "prefix_scan_entries("
    refute spop =~ "Enum.sort"
    refute spop =~ "right ++ left"

    assert zpop =~ "ZSetIndex.rank_range("
    assert zpop =~ "ensure_zpop_score_index(state, redis_key, prefix)"
    refute zpop =~ "prefix_scan_entries("
    refute zpop =~ "Enum.sort"
    refute zpop =~ "Enum.reverse"

    assert Regex.match?(
             ~r/maybe_delete_empty_compound_type\(\s*state,\s*redis_key,\s*type_key,\s*:set,\s*selected_delete_keys\s*\)/,
             source
           )

    assert Regex.match?(
             ~r/maybe_delete_empty_compound_type\(\s*state,\s*redis_key,\s*type_key,\s*:zset,\s*selected_delete_keys\s*\)/,
             source
           )

    refute source =~
             "Enum.each(selected, fn member ->\n        do_compound_delete(state, redis_key, CompoundKey.set_member(redis_key, member))"

    refute source =~
             "do_compound_delete(state, redis_key, CompoundKey.zset_member(redis_key, member))"
  end

  test "state-machine applies compact compound batch terms directly" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    assert source =~ "def apply(meta, {:compound_batch_put, redis_key, entries}, state)",
           "compound batch writes should not replay through generic {:batch, compound_put...}"

    assert source =~ "def apply(meta, {:compound_batch_delete, redis_key, compound_keys}, state)",
           "compound batch deletes should not replay through generic {:batch, compound_delete...}"

    assert source =~ "defp do_shared_compound_batch_put_fast",
           "shared compound puts should stage records and publish after append success"

    assert source =~ "defp do_shared_compound_batch_delete_fast",
           "shared compound deletes should stage tombstones and publish after append success"
  end

  test "shard raft path submits compact compound batch terms" do
    source = Ferricstore.Test.SourceFiles.shard_compound_source()
    put_body = function_body(source, "handle_compound_batch_put_raft")
    delete_body = function_body(source, "handle_compound_batch_delete_raft")

    assert put_body =~ "CompoundCommand.batch_put(redis_key, entries)"
    refute put_body =~ "Enum.map(entries"
    refute put_body =~ "{:batch, commands}"

    assert delete_body =~ "CompoundCommand.batch_delete(redis_key, compound_keys)"
    refute delete_body =~ "Enum.map(compound_keys"
    refute delete_body =~ "{:batch, commands}"
  end

  test "state-machine promoted compound batch tombstones keep sync durability" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()
    body = function_body(source, "do_promoted_compound_batch_delete")

    # The old single promoted tombstone path used v2_append_tombstone/2, which
    # fsyncs. The batched replacement must keep that ack boundary because
    # promoted files bypass the shared pending-write checkpointer.
    assert body =~ "NIF.v2_append_ops_batch(active, ops)"
    refute body =~ "NIF.v2_append_ops_batch_nosync(active, ops)"
    refute body =~ "NIF.v2_fsync(active)"
  end

  test "WARaft standalone staged apply keeps Bitcask sync boundary until unified segments exist" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()
    dispatcher = function_body(source, "append_pending_batch")

    sync_body =
      source
      |> function_body("do_append_pending_batch_sync")
      |> String.split("\n      defp append_pending_batch_nosync", parts: 2)
      |> hd()

    # WARaft currently uses the standalone staged apply path. It already has a
    # durable WARaft segment log, but Bitcask is still a separate physical log,
    # so storage apply must not publish a replay position after no-sync bytes.
    # When WARaft/Bitcask become one segment file, replace this guard with a
    # crash test that proves the unified record is durable before metadata moves.
    assert dispatcher =~ "standalone_staged_apply?()"
    assert dispatcher =~ "append_pending_batch_sync(file_path, batch, has_delete?)"

    assert sync_body =~ "NIF.v2_append_batch(file_path, puts)"

    assert sync_body =~ "NIF.v2_append_ops_batch(file_path, ops)"
    refute sync_body =~ "NIF.v2_append_ops_batch_nosync(file_path, ops)"
    refute sync_body =~ "NIF.v2_fsync(file_path)"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "defp #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n  end\n", parts: 2)
    body
  end
end
