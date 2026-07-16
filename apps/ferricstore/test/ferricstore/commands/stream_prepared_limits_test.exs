defmodule Ferricstore.Commands.StreamPreparedLimitsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.MockStore

  @integer_error {:error, "ERR value is not an integer or out of range"}

  test "prepared range and read counts reject negative values" do
    store = MockStore.make()

    assert @integer_error == Stream.handle_ast({:xrange, "stream", :min, :max, -1}, store)
    assert @integer_error == Stream.handle_ast({:xrevrange, "stream", :min, :max, -1}, store)
    assert @integer_error == Stream.handle_ast({:xread, -1, :no_block, [{"stream", "0"}]}, store)

    assert @integer_error ==
             Stream.handle_ast(
               {:xreadgroup, "group", "consumer", {-1, :no_block, [{"stream", ">"}]}},
               store
             )
  end

  test "prepared blocking reads reject timeouts above the VM timer ceiling" do
    store = MockStore.make()
    timeout = 0xFFFFFFFF + 1

    assert @integer_error ==
             Stream.handle_ast({:xread, 1, {:block, timeout}, [{"stream", "0"}]}, store)

    assert @integer_error ==
             Stream.handle_ast(
               {:xreadgroup, "group", "consumer",
                {1, {:block, timeout}, [{"stream", ">"}]}},
               store
             )
  end
end
