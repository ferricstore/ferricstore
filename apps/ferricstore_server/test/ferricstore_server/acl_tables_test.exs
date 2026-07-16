defmodule FerricstoreServer.Acl.TablesTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Tables
  alias FerricstoreServer.Management.ACL

  @tag :acl_snapshot_generations
  test "rapid snapshot swaps retain every captured generation for the grace period" do
    store = FerricStore.Instance.get(:default)
    default_user = Acl.get_user("default")
    revision = max(Acl.catalog_projection_revision(), 0)

    try do
      captured_tables =
        Enum.map(1..12, fn _offset ->
          table = Tables.active_table()

          assert :ok =
                   Acl.replace_catalog_snapshot(
                     [{"default", default_user, revision}],
                     revision
                   )

          table
        end)

      assert Enum.all?(captured_tables, &(:ets.info(&1) != :undefined))
    after
      :ok = ACL.reconcile_catalog(store)
    end
  end
end
