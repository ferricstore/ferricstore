defmodule Ferricstore.Commands.ListCleanupFailureTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.List
  alias Ferricstore.Test.MockStore

  test "pop restores the removed value when empty-list cleanup cannot read the length" do
    base = MockStore.make()
    assert 1 == List.handle("LPUSH", ["list", "value"], base)
    parent = self()

    store =
      Map.put(base, :list_op, fn
        "list", {:lpop, 1} ->
          "value"

        "list", :llen ->
          {:error, :disk_read_failed}

        "list", {:rpush, ["value"]} ->
          send(parent, :restored)
          1
      end)

    assert {:error, :disk_read_failed} = List.handle("LPOP", ["list"], store)
    assert_receive :restored
  end
end
