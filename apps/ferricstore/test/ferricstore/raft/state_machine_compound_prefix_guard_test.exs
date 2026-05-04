defmodule Ferricstore.Raft.StateMachineCompoundPrefixGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__)

  test "cross-shard prefix keys are filtered in the ETS match spec" do
    source = File.read!(@state_machine_path)

    [_, section] = String.split(source, "defp cross_shard_prefix_scan_from_path", parts: 2)
    section = section |> String.split("defp cross_shard_read_cold_batch", parts: 2) |> hd()

    assert section =~ ":binary_part",
           "state-machine compound prefix reads must not allocate all ETS keys before filtering"

    refute section =~ "Enum.filter",
           "state-machine compound prefix reads should keep prefix filtering inside :ets.select/2"
  end
end
