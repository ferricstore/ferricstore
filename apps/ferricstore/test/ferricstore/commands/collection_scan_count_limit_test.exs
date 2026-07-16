defmodule Ferricstore.Commands.CollectionScanCountLimitTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Hash, Set, SortedSet}
  alias Ferricstore.Store.CompoundKey

  @count_error {:error, "ERR value is not an integer or out of range"}

  test "collection scans reject work counts above the bounded page budget" do
    hash_type_key = CompoundKey.type_key("hash")
    set_type_key = CompoundKey.type_key("set")
    zset_type_key = CompoundKey.type_key("zset")

    store = %{
      compound_get: fn
        "hash", ^hash_type_key -> "hash"
        "set", ^set_type_key -> "set"
        "zset", ^zset_type_key -> "zset"
      end,
      compound_scan_page: fn _key, _prefix, _cursor, _count, _match, _fields_only ->
        flunk("an invalid COUNT must fail before paging storage")
      end
    }

    assert @count_error == Hash.handle("HSCAN", ["hash", "0", "COUNT", "10001"], store)
    assert @count_error == Set.handle("SSCAN", ["set", "0", "COUNT", "10001"], store)
    assert @count_error == SortedSet.handle("ZSCAN", ["zset", "0", "COUNT", "10001"], store)

    assert @count_error == Hash.handle_ast({:hscan, "hash", 0, count: 10_001}, store)
    assert @count_error == Set.handle_ast({:sscan, "set", 0, count: 10_001}, store)
    assert @count_error == SortedSet.handle_ast({:zscan, "zset", 0, count: 10_001}, store)
  end
end
