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
end
