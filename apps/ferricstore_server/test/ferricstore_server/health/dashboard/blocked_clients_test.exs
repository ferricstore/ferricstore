defmodule FerricstoreServer.Health.Dashboard.BlockedClientsTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Data.Operational

  test "a client waiting on several keys counts as one blocked client" do
    before = Operational.collect_connections().blocked
    client = spawn(fn -> Process.sleep(:infinity) end)
    entries = for key <- ["blocked-a", "blocked-b", "blocked-c"], do: {key, client, 0, 0}

    :ets.insert(:ferricstore_waiters, entries)

    on_exit(fn ->
      Enum.each(entries, &:ets.delete_object(:ferricstore_waiters, &1))
      Process.exit(client, :kill)
    end)

    assert Operational.collect_connections().blocked == before + 1
  end
end
