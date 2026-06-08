defmodule Ferricstore.Store.ActiveFileBenchTest do
  use ExUnit.Case, async: false

  @moduletag :bench

  alias Ferricstore.Store.ActiveFile

  test "ActiveFile.get hot path timing" do
    # Warm the process dictionary cache
    ActiveFile.get(0)

    iterations = 1_000_000

    {elapsed_us, _} =
      :timer.tc(fn ->
        for _ <- 1..iterations, do: ActiveFile.get(0)
      end)

    ns_per_call = elapsed_us * 1000 / iterations

    IO.puts(
      "\n  ActiveFile.get (cached): #{Float.round(ns_per_call, 1)} ns/call (#{iterations} iterations)"
    )

    # Should be under 50ns (atomics.get + Process.get)
    assert ns_per_call < 200, "Expected <200ns, got #{Float.round(ns_per_call, 1)}ns"
  end

  test "ActiveFile.get cold path (after publish) timing" do
    {_, _, data_path} = ActiveFile.get(0)

    iterations = 100_000

    {elapsed_us, _} =
      :timer.tc(fn ->
        for i <- 1..iterations do
          # Bump generation to force ETS re-read every time
          ActiveFile.publish(0, i, Path.join(data_path, "#{i}.log"), data_path)
          ActiveFile.get(0)
        end
      end)

    ns_per_call = elapsed_us * 1000 / iterations

    IO.puts(
      "  ActiveFile.get (cold):   #{Float.round(ns_per_call, 1)} ns/call (#{iterations} iterations)"
    )

    # Cold path includes ETS lookup — should be under 500ns
    assert ns_per_call < 2000, "Expected <2000ns, got #{Float.round(ns_per_call, 1)}ns"
  end
end
