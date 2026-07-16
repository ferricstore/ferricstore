defmodule Ferricstore.MemoryGuard.SystemMemoryTest do
  use ExUnit.Case, async: true

  alias Ferricstore.MemoryGuard.SystemMemory

  test "selects the smallest positive memory bound" do
    assert SystemMemory.__select_limit_for_test__([nil, 8_000, 2_000, 0, -1]) == 2_000
  end

  test "uses a conservative fallback when no source is valid" do
    assert SystemMemory.__select_limit_for_test__([nil, 0, -1, :unknown]) == 1_073_741_824
  end
end
