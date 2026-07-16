defmodule Ferricstore.Raft.StateMachineColdScanGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  test "cross-shard prefix scans batch cold Bitcask reads" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    section =
      Ferricstore.Test.SourceFiles.private_function_source!(
        source,
        "cross_shard_read_cold_bitcask_values",
        "pread_batch_keyed"
      )

    assert section =~ "ColdRead.pread_batch_keyed",
           "cross_shard_prefix_scan_from_path/3 should use ColdRead.pread_batch_keyed/2; " <>
             "per-entry pread_at/3 creates one async waiter per cold collection member"
  end
end
