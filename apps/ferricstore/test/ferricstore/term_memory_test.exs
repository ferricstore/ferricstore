defmodule Ferricstore.TermMemoryTest do
  use ExUnit.Case, async: true

  import Bitwise

  alias Ferricstore.TermMemory

  test "large integer estimates scale with their stored magnitude" do
    integer = 1 <<< 8_192

    assert TermMemory.bytes(integer) >= :erlang.external_size(integer, minor_version: 2)
    assert TermMemory.bytes(integer) > TermMemory.bytes(1 <<< 64)
  end
end
