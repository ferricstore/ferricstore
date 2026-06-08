defmodule Ferricstore.Bugs.LinsertNegativeIndexTest do
  @moduledoc """
  Regression test for Bug #1: LINSERT position calculation issues.

  The LINSERT BEFORE/AFTER implementation computes new positions as the
  midpoint between adjacent elements. When many elements are inserted at
  the same position (e.g., repeatedly LINSERT BEFORE the same pivot),
  the midpoint positions converge toward each other and eventually collide
  due to float64 precision limits (related to Bug #5: position encoding
  precision loss).

  The position calculation at list_ops.ex:316 also has a latent bug where
  `Enum.at(sorted, idx - 1)` with idx=0 would use Elixir negative indexing
  to access the last element. Currently guarded by `if idx == 0`, but the
  midpoint-based position scheme breaks down under sustained insertions.

  File: apps/ferricstore/lib/ferricstore/store/list_ops.ex:316
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag timeout: 30_000

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    :ok
  end

  describe "LINSERT repeated insertions at same position" do
    test "many LINSERT BEFORE same pivot preserves all elements" do
      # Build a 2-element list: [a, b]
      FerricStore.rpush("linbug:many", ["a", "b"])

      # Insert 55 elements before "b" — each time computing the midpoint
      # between the previous insert and "b". After enough iterations,
      # the midpoint positions collide and elements overwrite each other.
      count = 55

      for i <- 1..count do
        FerricStore.linsert("linbug:many", :before, "b", "m#{i}")
      end

      {:ok, result} = FerricStore.lrange("linbug:many", 0, -1)

      # Expected: ["a", "m1", "m2", ..., "m55", "b"] = 57 total elements
      expected_len = count + 2

      assert length(result) == expected_len,
             "Expected #{expected_len} elements after #{count} LINSERT operations, " <>
               "but got #{length(result)}. Position collisions caused element loss. " <>
               "List: #{inspect(Enum.take(result, 5))} ... #{inspect(Enum.take(result, -5))}"
    end

    test "LINSERT BEFORE first element repeatedly preserves all elements" do
      FerricStore.rpush("linbug:prepend", ["anchor"])

      # Repeatedly insert before the current first element
      count = 55

      for i <- 1..count do
        {:ok, [first | _]} = FerricStore.lrange("linbug:prepend", 0, 0)
        FerricStore.linsert("linbug:prepend", :before, first, "p#{i}")
      end

      {:ok, result} = FerricStore.lrange("linbug:prepend", 0, -1)

      expected_len = count + 1

      assert length(result) == expected_len,
             "Expected #{expected_len} elements after #{count} LINSERT BEFORE first operations, " <>
               "but got #{length(result)}. Position collisions caused element loss."

      # "anchor" should still be the last element
      assert List.last(result) == "anchor",
             "The original anchor element should remain at the end of the list"
    end
  end
end
