defmodule Ferricstore.Commands.SortedSetRandomComplexityTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.SortedSet.Helpers

  test "negative-count sampling uses bounded output with replacement" do
    assert ["member", "member", "member"] ==
             Helpers.select_random_members([{"member", "1"}], -3, false)

    assert {:error, "ERR count exceeds maximum allowed response size"} ==
             Helpers.select_random_members([{"member", "1"}], -10_001, false)
  end

  test "replacement sampling does not walk the member list for every result" do
    source =
      "../../../lib/ferricstore/commands/sorted_set/helpers.ex"
      |> Path.expand(__DIR__)
      |> File.read!()

    refute source =~ "Enum.random(pairs)"
  end
end
