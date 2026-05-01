defmodule Ferricstore.Raft.WalChecksumGuardTest do
  use ExUnit.Case, async: true

  @moduletag :guard

  @source Path.expand("../../../lib/ferricstore/raft/cluster.ex", __DIR__)

  test "Ra WAL checksums stay enabled" do
    source = File.read!(@source)

    # Ra can discard/stop on corrupt WAL entries only when entry checksums are
    # written. Do not trade this away silently for throughput; benchmark first.
    refute source =~ ~r/wal_compute_checksums:\s*false/,
           "Ra WAL checksums must stay enabled; false weakens corrupt/torn-tail detection"

    assert source =~ ~r/wal_compute_checksums:\s*true/
  end
end
