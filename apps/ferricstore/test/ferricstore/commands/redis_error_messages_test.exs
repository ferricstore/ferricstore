defmodule Ferricstore.Commands.RedisErrorMessagesTest do
  @moduledoc "Error replies that clients match on read as Redis words them."
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  test "LPOS RANK 0 answers Redis's message" do
    ctx = FerricStore.Instance.get(:default)
    key = "{lpos:#{System.unique_integer([:positive])}}"
    assert 1 == Dispatcher.dispatch("RPUSH", [key, "a"], ctx)

    assert {:error,
            "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"} ==
             Dispatcher.dispatch("LPOS", [key, "a", "RANK", "0"], ctx)
  end
end
