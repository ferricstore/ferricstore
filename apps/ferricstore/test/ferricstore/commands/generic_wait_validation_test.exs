defmodule Ferricstore.Commands.GenericWaitValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Generic
  alias Ferricstore.Test.MockStore

  @integer_error {:error, "ERR value is not an integer or out of range"}

  test "WAIT rejects malformed and negative numeric arguments" do
    store = MockStore.make()

    assert @integer_error == Generic.handle("WAIT", ["not-a-number", "0"], store)
    assert @integer_error == Generic.handle("WAIT", ["1", "-1"], store)
  end

  test "prepared WAIT ASTs preserve parser errors and numeric invariants" do
    assert @integer_error ==
             Generic.handle_ast({:wait, @integer_error, 0}, MockStore.make())

    assert @integer_error ==
             Generic.handle_ast({:wait, 1, @integer_error}, MockStore.make())

    assert @integer_error == Generic.handle_ast({:wait, -1, 0}, MockStore.make())
  end
end
