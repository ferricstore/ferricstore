defmodule Ferricstore.Commands.BloomReadStatusSafetyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Bloom

  test "metadata lookup exceptions return a command error instead of crashing" do
    store = %{exists?: fn _key -> raise "metadata unavailable" end}

    assert {:error, "ERR bloom metadata lookup failed"} ==
             Bloom.handle("BF.CARD", ["filter"], store)
  end
end
