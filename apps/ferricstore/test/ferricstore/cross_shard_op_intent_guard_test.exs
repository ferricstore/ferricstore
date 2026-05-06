defmodule Ferricstore.CrossShardOpIntentGuardTest do
  use ExUnit.Case, async: true

  @source_path Path.expand("../../lib/ferricstore/cross_shard_op.ex", __DIR__)

  test "cross-shard execution handles intent write failures before running user writes" do
    source = File.read!(@source_path)

    assert source =~ "case write_intent(coordinator_shard, owner_ref, full_intent) do",
           "CrossShardOp must branch on write_intent/3 before executing locked writes"

    assert source =~
             "try do\n          case write_intent(coordinator_shard, owner_ref, full_intent) do",
           "CrossShardOp must wrap intent writing in cleanup scope so raised failures release locks"

    refute Regex.match?(
             ~r/^\s*write_intent\(coordinator_shard, owner_ref, full_intent\)\s*$/m,
             source
           ),
           "CrossShardOp must not ignore write_intent/3 result; otherwise a failed intent write can still execute without recovery metadata"
  end
end
