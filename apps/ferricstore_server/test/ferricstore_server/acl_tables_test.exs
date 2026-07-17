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

  @tag :acl_snapshot_generations
  test "captured lookup, list, and count reads retry after generation reclamation" do
    store = FerricStore.Instance.get(:default)
    default_user = Acl.get_user("default")
    revision = max(Acl.catalog_projection_revision(), 0)
    parent = self()

    readers = [
      lookup: fn table -> :ets.lookup(table, "default") end,
      list: &:ets.tab2list/1,
      count: fn table -> :ets.info(table, :size) end
    ]

    try do
      for {name, reader} <- readers do
        captured = Tables.active_table()

        task =
          Task.async(fn ->
            Tables.read(fn table ->
              unless Process.get(:acl_generation_captured) do
                Process.put(:acl_generation_captured, true)
                send(parent, {:acl_generation_captured, name, table})

                receive do
                  {:resume_acl_generation_read, ^name} -> :ok
                end
              end

              reader.(table)
            end)
          end)

        assert_receive {:acl_generation_captured, ^name, ^captured}

        assert :ok =
                 Acl.replace_catalog_snapshot(
                   [{"default", default_user, revision}],
                   revision
                 )

        assert :ok = Tables.cleanup_retired_table(captured)
        assert :ets.info(captured) == :undefined
        send(task.pid, {:resume_acl_generation_read, name})

        result = Task.await(task)

        case name do
          :lookup -> assert [{"default", ^default_user}] = result
          :list -> assert {"default", default_user} in result
          :count -> assert result >= 1
        end
      end
    after
      :ok = ACL.reconcile_catalog(store)
    end
  end
end
