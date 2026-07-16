defmodule Ferricstore.Raft.LmdbProjectionCommandContractTest do
  use ExUnit.Case, async: true

  @source_path Path.expand(
                 "../../../lib/ferricstore/raft/state_machine/sections/lmdb_projection.ex",
                 __DIR__
               )

  test "hibernation after-flush command uses the canonical beta contract" do
    source = File.read!(@source_path)

    refute source =~ ":hibernate_flow_evict_hot_v1"
    assert source =~ "{:hibernate_flow_evict_hot,"
  end
end
