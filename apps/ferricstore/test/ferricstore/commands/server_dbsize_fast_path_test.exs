defmodule Ferricstore.Commands.ServerDbsizeFastPathTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server

  test "DBSIZE uses the store count primitive without materializing keys" do
    store = %{
      dbsize: fn -> 42 end,
      keys: fn -> flunk("DBSIZE must not materialize the keyspace") end
    }

    assert 42 == Server.handle("DBSIZE", [], store)
  end
end
