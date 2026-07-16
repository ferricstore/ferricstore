defmodule Ferricstore.TDigest.CoreCorrectnessTest do
  use ExUnit.Case, async: true

  alias Ferricstore.TDigest.Core

  test "new rejects compression values that cannot produce a valid digest" do
    for compression <- [0, -1, 1.5, 1_001] do
      assert_raise ArgumentError, ~r/compression/, fn -> Core.new(compression) end
    end
  end

  test "merge_many validates the result compression even for empty inputs" do
    for digests <- [[], [Core.new() |> Core.add(1.0)]] do
      assert_raise ArgumentError, ~r/compression/, fn -> Core.merge_many(digests, 0) end
    end
  end

  test "compression respects the scale-function weight bound at the actual quantile" do
    digest = Core.new(10) |> Core.add_many(Enum.to_list(1..600)) |> Core.compress()

    Enum.reduce(digest.centroids, 0.0, fn {_mean, weight}, weight_before ->
      q = (weight_before + weight / 2.0) / digest.count
      max_weight = 4.0 * digest.count * q * (1.0 - q) / digest.compression

      # A single observation cannot be split even when the tail bound is below one.
      assert weight == 1.0 or weight <= max_weight + 1.0e-9
      weight_before + weight
    end)
  end
end
