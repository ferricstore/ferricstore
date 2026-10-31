defmodule Ferricstore.Commands.LatencyTest do
  @moduledoc "LATENCY, as Redis answers it."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.LatencyMonitor

  setup do
    ctx = FerricStore.Instance.get(:default)
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end

    reset = fn ->
      :ok = d.("CONFIG", ["SET", "latency-monitor-threshold", "0"])
      :ok = d.("CONFIG", ["SET", "latency-tracking", "no"])
      d.("LATENCY", ["RESET"])
      LatencyMonitor.reset_histograms()
    end

    reset.()
    on_exit(reset)
    %{d: d}
  end

  test "with monitoring off nothing is sampled", %{d: d} do
    LatencyMonitor.record("SET", 50_000)
    assert [] == d.("LATENCY", ["LATEST"])
    assert [] == d.("LATENCY", ["HISTORY", "command"])
    assert d.("LATENCY", ["DOCTOR"]) =~ "Latency monitoring is disabled"
  end

  test "spikes over the threshold are sampled per event", %{d: d} do
    :ok = d.("CONFIG", ["SET", "latency-monitor-threshold", "2"])

    assert ["latency-monitor-threshold", "2"] ==
             d.("CONFIG", ["GET", "latency-monitor-threshold"])

    now = System.os_time(:second)
    # Under the threshold: not sampled.
    LatencyMonitor.record("SET", 1_000, now - 20)
    # Spikes in one second keep that second's worst, as Redis's samples do.
    LatencyMonitor.record("SET", 5_000, now - 10)
    LatencyMonitor.record("GET", 3_000, now - 10)
    LatencyMonitor.record("GET", 3_000, now)
    LatencyMonitor.sync()

    assert [["command", ^now, 3, 5]] = d.("LATENCY", ["LATEST"])
    assert [[now - 10, 5], [now, 3]] == d.("LATENCY", ["HISTORY", "command"])

    assert d.("LATENCY", ["GRAPH", "command"]) =~ "command - high 5 ms, low 3 ms"
    assert d.("LATENCY", ["DOCTOR"]) =~ "command"

    assert {:error, "ERR No samples available for event 'nope'"} ==
             d.("LATENCY", ["GRAPH", "nope"])

    assert 0 == d.("LATENCY", ["RESET", "nope"])
    assert 1 == d.("LATENCY", ["RESET", "command"])
    assert [] == d.("LATENCY", ["LATEST"])
  end

  test "HISTOGRAM answers cumulative counts per power-of-two bucket", %{d: d} do
    assert %{} == d.("LATENCY", ["HISTOGRAM"])
    :ok = d.("CONFIG", ["SET", "latency-tracking", "yes"])

    LatencyMonitor.record("GET", 3)
    LatencyMonitor.record("GET", 4)
    LatencyMonitor.record("GET", 100)
    LatencyMonitor.record("SET", 1)

    # (The CONFIG SET that turned tracking on counts too, as in Redis.)
    assert %{
             "get" => %{"calls" => 3, "histogram_usec" => %{4 => 2, 128 => 3}},
             "set" => %{"calls" => 1, "histogram_usec" => %{1 => 1}}
           } == Map.take(d.("LATENCY", ["HISTOGRAM"]), ["get", "set"])

    assert %{"set" => _} = d.("LATENCY", ["HISTOGRAM", "set", "nope"])
    assert map_size(d.("LATENCY", ["HISTOGRAM", "set", "nope"])) == 1
  end

  test "errors and HELP", %{d: d} do
    assert {:error, "ERR unknown subcommand 'NOPE'. Try LATENCY HELP."} ==
             d.("LATENCY", ["NOPE"])

    assert {:error, "ERR wrong number of arguments for 'latency|history' command"} ==
             d.("LATENCY", ["HISTORY"])

    assert {:error, "ERR wrong number of arguments for 'latency' command"} == d.("LATENCY", [])
    assert [_ | _] = d.("LATENCY", ["HELP"])

    assert {:error, "ERR Invalid argument '-1' for CONFIG SET 'latency-monitor-threshold'"} ==
             d.("CONFIG", ["SET", "latency-monitor-threshold", "-1"])

    assert {:error, "ERR Invalid argument 'maybe' for CONFIG SET 'latency-tracking'"} ==
             d.("CONFIG", ["SET", "latency-tracking", "maybe"])
  end
end
