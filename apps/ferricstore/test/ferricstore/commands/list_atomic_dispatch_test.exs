defmodule Ferricstore.Commands.ListAtomicDispatchTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.List
  alias Ferricstore.Store.CompoundKey

  test "LINSERT uses the store's atomic list operation capability" do
    type_key = CompoundKey.type_key("jobs")
    parent = self()

    store = %{
      compound_get: fn
        "jobs", ^type_key -> CompoundKey.encode_type(:list)
        _redis_key, _compound_key -> nil
      end,
      list_op: fn key, operation ->
        send(parent, {:list_op, key, operation})
        4
      end
    }

    assert 4 == List.handle_ast({:linsert, "jobs", :before, "pivot", "new"}, store)
    assert_received {:list_op, "jobs", {:linsert, :before, "pivot", "new"}}
  end

  test "list range and point mutations use the indexed list operation capability" do
    type_key = CompoundKey.type_key("jobs")
    parent = self()

    store = %{
      compound_get: fn
        "jobs", ^type_key -> CompoundKey.encode_type(:list)
        _redis_key, _compound_key -> nil
      end,
      list_op: fn key, operation ->
        send(parent, {:list_op, key, operation})

        case operation do
          {:lrange, 10, 12} -> ["ten", "eleven", "twelve"]
          {:lset, 11, "updated"} -> :ok
        end
      end
    }

    assert ["ten", "eleven", "twelve"] ==
             List.handle_ast({:lrange, "jobs", 10, 12}, store)

    assert :ok == List.handle_ast({:lset, "jobs", 11, "updated"}, store)
    assert_received {:list_op, "jobs", {:lrange, 10, 12}}
    assert_received {:list_op, "jobs", {:lset, 11, "updated"}}
  end
end
