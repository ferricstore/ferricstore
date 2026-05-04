defmodule Ferricstore.Raft.StateMachineAsyncReadGuardTest do
  use ExUnit.Case, async: true

  @state_machine_path Path.expand("../../../lib/ferricstore/raft/state_machine.ex", __DIR__)

  test "Raft apply cold-read fallbacks use async pread" do
    source = File.read!(@state_machine_path)

    # Cross-shard command apply can read cold large values. Keep the disk I/O
    # behind the ColdRead async helper while still waiting synchronously for
    # deterministic apply results. Do not call the blocking pread NIF here.
    assert source =~ "ColdRead.pread_at",
           "expected Raft state-machine cold reads to use ColdRead.pread_at/*"

    refute Regex.match?(~r/NIF\.v2_pread_at\(/, source),
           "expected Raft state-machine cold reads to avoid blocking v2_pread_at/2"
  end
end
