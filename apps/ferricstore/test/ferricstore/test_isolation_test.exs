defmodule Ferricstore.TestIsolationTest do
  use ExUnit.Case, async: true

  # The Flow digest format is node-global (a persistent_term), and Flow keys
  # are built from it. A test that switches it while async tests run changes
  # the keys those tests build and check: CompositeBackfillTest then saw
  # records it had built itself as invalid. Tests that switch it run alone.
  @switches_format ~r/\b(Digest|DataFormat)\.(put_format|put_current)\(/
  @async_false ~r/use ExUnit\.Case,\s*async: false/

  test "tests that switch the data format do not run async" do
    umbrella = Path.expand("../../../..", __DIR__)

    offenders =
      umbrella
      |> Path.join("apps/*/test/**/*.exs")
      |> Path.wildcard()
      |> Enum.filter(fn path ->
        source = File.read!(path)
        Regex.match?(@switches_format, source) and not Regex.match?(@async_false, source)
      end)
      |> Enum.map(&Path.relative_to(&1, umbrella))

    assert offenders == []
  end
end
