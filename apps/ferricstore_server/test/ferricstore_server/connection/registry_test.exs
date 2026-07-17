defmodule FerricstoreServer.Connection.RegistryTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Connection.Registry

  test "snapshot bounds rows while retaining exact live aggregates" do
    base = System.unique_integer([:positive]) * 10
    now = System.monotonic_time(:millisecond)
    pids = for _ <- 1..3, do: spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Enum.each(pids, &Process.exit(&1, :kill))
      Enum.each(1..3, &Registry.unregister(base + &1, Enum.at(pids, &1 - 1)))
    end)

    Registry.register(base + 1, Enum.at(pids, 0), %{created_at_ms: now - 3_000, flags: "S"})
    Registry.register(base + 2, Enum.at(pids, 1), %{created_at_ms: now - 2_000, flags: "M"})
    Registry.register(base + 3, Enum.at(pids, 2), %{created_at_ms: now - 1_000, flags: ""})

    snapshot = Registry.snapshot(2)
    ids = MapSet.new(Enum.map(snapshot.clients, & &1.client_id))

    assert MapSet.subset?(ids, MapSet.new([base + 1, base + 2, base + 3]))
    assert length(snapshot.clients) == 2
    assert snapshot.registered_count >= 3
    assert snapshot.pubsub_count >= 1
    assert snapshot.transaction_count >= 1
    assert snapshot.oldest_created_at_ms <= now - 3_000
    assert Registry.snapshot(0).clients == []

    source =
      File.read!(Path.expand("../../../lib/ferricstore_server/connection/registry.ex", __DIR__))

    refute source =~ ":ets.tab2list"
  end

  test "ACL membership lookup and replacement are indexed by username" do
    base = System.unique_integer([:positive]) * 10
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Process.exit(first, :kill)
      Process.exit(second, :kill)
      Registry.unregister(base + 1, first)
      Registry.unregister(base + 2, second)
    end)

    assert :ok = Registry.register(base + 1, first, %{username: "alice"})
    assert :ok = Registry.register(base + 2, second, %{username: "bob"})

    assert Registry.acl_user_pids("alice") == [first]
    assert Registry.acl_user_pids("bob") == [second]

    assert :ok = Registry.replace_acl_user(base + 2, second, "bob", "alice")
    assert MapSet.new(Registry.acl_user_pids("alice")) == MapSet.new([first, second])
    assert Registry.acl_user_pids("bob") == []

    assert :ok = Registry.unregister(base + 1, first)
    assert Registry.acl_user_pids("alice") == [second]
  end
end
