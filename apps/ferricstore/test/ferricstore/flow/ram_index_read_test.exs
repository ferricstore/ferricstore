defmodule Ferricstore.Flow.RAMIndexReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.RAMIndexRead

  test "bounds map nil to open score ranges and integers to inclusive ranges" do
    assert RAMIndexRead.min_bound(nil) == :neg_inf
    assert RAMIndexRead.max_bound(nil) == :pos_inf
    assert RAMIndexRead.min_bound(10) == {:inclusive, 10}
    assert RAMIndexRead.max_bound(20) == {:inclusive, 20}
  end

  test "reverse helpers preserve previous query behavior" do
    assert RAMIndexRead.reverse?(%{rev?: true})
    refute RAMIndexRead.reverse?(%{rev?: false})
    refute RAMIndexRead.reverse?(nil)

    assert RAMIndexRead.maybe_reverse([1, 2, 3], true) == [3, 2, 1]
    assert RAMIndexRead.maybe_reverse([1, 2, 3], false) == [1, 2, 3]
  end
end
