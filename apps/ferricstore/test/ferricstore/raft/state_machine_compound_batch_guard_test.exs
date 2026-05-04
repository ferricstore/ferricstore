defmodule Ferricstore.Raft.StateMachineCompoundBatchGuardTest do
  @moduledoc false
  use ExUnit.Case, async: true

  @state_machine_path Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__)

  test "state-machine command stores expose compound batch reads" do
    source = File.read!(@state_machine_path)

    # Hash/set/zset command handlers call Ops.compound_batch_get/3 for HMGET,
    # SMISMEMBER, ZMSCORE, and friends. During Raft apply the command store is
    # a map, so missing batch callbacks make Ops fall back to one cold read per
    # field. Keep the callback explicit so cold compound reads stay batched.
    assert source =~ "compound_batch_get:",
           "state-machine command store must provide compound_batch_get"

    assert source =~ "compound_batch_get_meta:",
           "state-machine command store must provide compound_batch_get_meta"
  end

  test "state-machine command stores expose plain batch reads" do
    source = File.read!(@state_machine_path)

    # MGET, JSON.MGET, PFCOUNT, PFMERGE, and BITOP call Ops.batch_get/2.
    # During Raft apply the store is a map, so missing batch_get callbacks
    # make Ops fall back to one closure call and one possible cold-read waiter
    # per key. Keep this explicit to preserve batched cold reads in apply.
    assert source =~ ~r/batch_get: fn keys -> cross_shard_batch_read\(ctx, keys\) end/,
           "state-machine command store must provide batch_get for plain multi-key reads"
  end

  test "state-machine compound batch metadata reads use keyed batched cold path" do
    source = File.read!(@state_machine_path)
    body = function_body(source, "cross_shard_read_cold_meta_batch")

    # HGETEX/HEXPIRE-style logic reads value+TTL for many fields. If this
    # helper falls back to one pread per field, large hashes/sets/zsets create
    # one waiter per cold member. Keep the promoted-aware batched reader.
    assert body =~ "ColdRead.pread_batch_keyed",
           "state-machine compound batch metadata reads must use the keyed batched cold reader"
  end

  defp function_body(source, function) do
    [_before, rest] = String.split(source, "defp #{function}", parts: 2)
    [body, _after] = String.split(rest, "\n  end\n", parts: 2)
    body
  end
end
