defmodule Ferricstore.Commands.CompoundScanPageTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{CollectionScan, Hash, Set, SortedSet}
  alias Ferricstore.Store.CompoundKey

  test "collection SCAN commands use the bounded page capability" do
    hash_type_key = CompoundKey.type_key("hash")
    set_type_key = CompoundKey.type_key("set")
    zset_type_key = CompoundKey.type_key("zset")
    hash_prefix = CompoundKey.hash_prefix("hash")
    set_prefix = CompoundKey.set_prefix("set")
    zset_prefix = CompoundKey.zset_prefix("zset")

    store = %{
      compound_get: fn
        "hash", ^hash_type_key -> "hash"
        "set", ^set_type_key -> "set"
        "zset", ^zset_type_key -> "zset"
      end,
      compound_scan_page: fn
        "hash", ^hash_prefix, 0, 2, nil, false ->
          {:ok, {{:after, "field-b"}, [{"field-a", "value-a"}, {"field-b", "value-b"}]}}

        "set", ^set_prefix, 0, 2, nil, true ->
          {:ok, {{:after, "member-b"}, [{"member-a", nil}, {"member-b", nil}]}}

        "zset", ^zset_prefix, 0, 2, nil, false ->
          {:ok, {{:after, "member-b"}, [{"member-a", "1.0"}, {"member-b", "2.0"}]}}
      end,
      compound_scan: fn _key, _prefix ->
        flunk("collection SCAN must not materialize the full collection")
      end
    }

    assert ["~ZmllbGQtYg", ["field-a", "value-a", "field-b", "value-b"]] =
             Hash.handle("HSCAN", ["hash", "0", "COUNT", "2"], store)

    assert ["~bWVtYmVyLWI", ["member-a", "member-b"]] =
             Set.handle("SSCAN", ["set", "0", "COUNT", "2"], store)

    assert ["~bWVtYmVyLWI", ["member-a", "1.0", "member-b", "2.0"]] =
             SortedSet.handle("ZSCAN", ["zset", "0", "COUNT", "2"], store)
  end

  test "collection cursors reject decoded boundaries above the compound-key limit" do
    oversized = :binary.copy("m", Ferricstore.Store.Router.max_key_size() + 1)
    token = "~" <> Base.url_encode64(oversized, padding: false)

    assert {:error, "ERR invalid cursor"} = CollectionScan.parse_cursor(token)

    store = %{
      compound_scan_page: fn "set", "prefix", 0, 1, nil, true ->
        {:ok, {{:after, oversized}, []}}
      end
    }

    assert {:error, "ERR storage read failed"} =
             CollectionScan.page(store, "set", "prefix", 0, 1, nil, true)
  end

  test "collection paging requires a bounded store capability" do
    test_pid = self()

    store = %{
      compound_scan: fn _key, _prefix ->
        send(test_pid, :materialized_full_collection)
        []
      end
    }

    assert {:error, "ERR storage read failed"} =
             CollectionScan.page(store, "set", "prefix", 0, 1, nil, true)

    refute_received :materialized_full_collection
  end
end
