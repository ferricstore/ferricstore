defmodule Ferricstore.Raft.StateMachineColdScanGuardTest do
  use ExUnit.Case, async: true
  @moduletag :raft

  test "cross-shard prefix scans batch cold Bitcask reads" do
    source = Ferricstore.Test.SourceFiles.state_machine_source()

    [_, section] =
      String.split(source, "defp cross_shard_read_cold_bitcask_values(acc, entries) do", parts: 2)

    section =
      section |> String.split("defp cross_shard_read_cold_waraft_values", parts: 2) |> hd()

    assert section =~ "ColdRead.pread_batch_keyed",
           "cross_shard_prefix_scan_from_path/3 should use ColdRead.pread_batch_keyed/2; " <>
             "per-entry pread_at/3 creates one async waiter per cold collection member"
  end
end
