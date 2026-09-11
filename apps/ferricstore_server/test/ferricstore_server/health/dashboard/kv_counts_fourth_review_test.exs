defmodule FerricstoreServer.Health.Dashboard.KVCountsFourthReviewTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Data.KV
  alias FerricstoreServer.Acl

  test "returned counts are consistent and restricted viewers never receive raw scan counts" do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    prefix = "fourth-kv-#{System.unique_integer([:positive])}:"
    keys = Enum.map(1..3, &(prefix <> to_string(&1)))
    :ets.insert(:keydir_0, Enum.map(keys, &{&1, "value", 0, 0, 0, 0, 5}))
    on_exit(fn -> Enum.each(keys, &:ets.delete(:keydir_0, &1)) end)
    open = KV.collect_keyspace_page(%{"prefix" => prefix, "limit" => "2"})
    assert open.total_sampled == length(open.rows)
    assert open.scanned_count >= length(open.rows)

    actor = prefix <> "reader"
    :ok = Acl.set_user(actor, ["on", "nopass", "-@all", "+GET", "+SCAN", "%R~#{hd(keys)}"])
    on_exit(fn -> Acl.del_user(actor) end)

    restricted =
      KV.collect_keyspace_page(%{"prefix" => prefix, "limit" => "2", "acl_username" => actor})

    assert restricted.total_sampled == 1
    assert restricted.scanned_count == nil
    assert Enum.map(restricted.rows, & &1.key) == [hd(keys)]
  end
end
