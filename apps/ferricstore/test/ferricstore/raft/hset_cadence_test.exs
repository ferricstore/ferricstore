defmodule Ferricstore.Raft.HsetCadenceTest do
  use ExUnit.Case, async: true
  alias Ferricstore.Raft.WARaftBackend.HsetCadence

  test "paced requests remain direct even with a negative monotonic origin" do
    origin = -576_460_000_000
    cadence = HsetCadence.new(1, origin)
    for ms <- 0..2_000//25, do: refute(HsetCadence.busy?(cadence, 0, origin + ms))
  end

  test "sustained requests become busy across window boundaries and decay after idle" do
    cadence = HsetCadence.new(2, -1_000)
    flags = for ms <- 0..1_000//10, do: HsetCadence.busy?(cadence, 0, -1_000 + ms)
    assert Enum.all?(Enum.drop(flags, 6))
    refute HsetCadence.busy?(cadence, 1, 0)
    refute HsetCadence.busy?(cadence, 0, 1_000)
  end

  test "sparse synchronized client waves do not lock the heuristic into coalescing" do
    cadence = HsetCadence.new(1, 0)
    for ms <- 0..2_000//100, _client <- 1..4, do: refute(HsetCadence.busy?(cadence, 0, ms))
  end

  test "a delayed timestamp cannot reset a newer busy window" do
    cadence = HsetCadence.new(1, 0)
    for _ <- 1..5, do: HsetCadence.busy?(cadence, 0, 200)
    assert HsetCadence.busy?(cadence, 0, 99)
    assert HsetCadence.busy?(cadence, 0, 300)
    refute HsetCadence.busy?(cadence, 0, 500)
  end

  test "long-lived VMs and saturated counters do not overflow the atomic word" do
    cadence = HsetCadence.new(1, 0)
    twenty_years_ms = 20 * 366 * 24 * 60 * 60 * 1_000
    refute HsetCadence.busy?(cadence, 0, twenty_years_ms)
    for _ <- 1..1_000, do: HsetCadence.busy?(cadence, 0, twenty_years_ms)
    assert HsetCadence.busy?(cadence, 0, twenty_years_ms + 100)
    refute HsetCadence.busy?(cadence, 0, twenty_years_ms + 300)
    refute HsetCadence.busy?(cadence, -1, twenty_years_ms)
    refute HsetCadence.busy?(cadence, 1, twenty_years_ms)
  end
end
