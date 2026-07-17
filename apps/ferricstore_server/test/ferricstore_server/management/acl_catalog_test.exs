defmodule FerricstoreServer.Management.ACLCatalogTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Management.ACL

  setup do
    Acl.reset!()

    on_exit(fn ->
      Acl.reset!()
      Application.delete_env(:ferricstore, :max_acl_users)
    end)

    :ok
  end

  test "prepared SETUSER catalog values never contain the plaintext password" do
    secret = "catalog-secret-that-must-not-enter-raft"

    assert {:ok, value} = ACL.prepare_catalog_value(nil, "alice", ["on", ">" <> secret, "+@all"])
    refute :binary.match(value, secret) != :nomatch

    assert {:ok, %{value: ^value}} =
             1
             |> ServerCatalog.encode_entry(value)
             |> ServerCatalog.decode_entry()

    assert {:ok, user} = ACL.decode_catalog_value(value)

    assert is_binary(user.password)
    refute user.password == secret
  end

  test "prepared SETUSER values reject usernames that cannot authenticate" do
    assert {:error, utf8_reason} =
             ACL.prepare_catalog_value(nil, <<0xFF>>, ["on", "nopass"])

    assert utf8_reason =~ "valid UTF-8"

    assert {:error, size_reason} =
             ACL.prepare_catalog_value(nil, String.duplicate("u", 1_025), ["on", "nopass"])

    assert size_reason =~ "exceeds 1024 bytes"
  end

  test "prepared SETUSER values reject invalid and excessive patterns before compilation" do
    assert {:error, utf8_reason} =
             ACL.prepare_catalog_value(nil, "alice", ["~" <> <<0xFF>>])

    assert utf8_reason =~ "valid UTF-8"

    assert {:error, size_reason} =
             ACL.prepare_catalog_value(nil, "alice", ["~" <> String.duplicate("x", 4_097)])

    assert size_reason =~ "exceeds 4096 bytes"

    patterns = for index <- 1..4_097, do: "~tenant:#{index}:*"
    assert {:error, count_reason} = ACL.prepare_catalog_value(nil, "alice", patterns)
    assert count_reason =~ "more than 4096 key patterns"
  end

  test "preparing the same canonical value does not hash again during projection" do
    assert {:ok, value} = ACL.prepare_catalog_value(nil, "alice", ["on", ">secret"])
    assert {:ok, projected} = ACL.decode_catalog_value(value)

    assert projected.password != nil
    assert {:ok, ^projected} = ACL.decode_catalog_value(value)
  end

  test "catalog values persist declarative ACL data instead of runtime collection and regex terms" do
    assert {:ok, value} =
             ACL.prepare_catalog_value(nil, "alice", [
               "on",
               ">secret",
               "+@all",
               "%R~tenant:*",
               "&events:*"
             ])

    assert {:ferricstore_acl_user, true, password, :all, [], [{"tenant:*", :read}], ["events:*"]} =
             :erlang.binary_to_term(value, [:safe])

    assert is_binary(password)

    assert {:ok, user} = ACL.decode_catalog_value(value)
    assert [{"tenant:*", :read, %Regex{}}] = user.keys
    assert [{"events:*", %Regex{}}] = user.channels
  end

  test "catalog decoder rejects malformed ACL user shapes" do
    malformed =
      :erlang.term_to_binary(
        {:ferricstore_acl_user, true, nil, "all", [], :all, :all},
        [:deterministic]
      )

    assert {:error, :invalid_acl_catalog_value} = ACL.decode_catalog_value(malformed)
  end

  test "catalog decoder rejects compressed and trailing external-term values" do
    pattern = String.duplicate("tenant:", 256) <> "*"

    assert {:ok, value} =
             ACL.prepare_catalog_value(nil, "alice", ["on", "nopass", "~" <> pattern])

    canonical = :erlang.binary_to_term(value, [:safe])
    compressed = :erlang.term_to_binary(canonical, compressed: 9)
    assert <<131, 80, _rest::binary>> = compressed

    for non_canonical <- [compressed, value <> <<0>>] do
      assert {:error, :invalid_acl_catalog_value} = ACL.decode_catalog_value(non_canonical)
    end
  end

  test "SETUSER stores canonical bytes and DELUSER physically removes the catalog entry" do
    store = FerricStore.Instance.get(:default)
    username = "catalog-user-#{System.unique_integer([:positive])}"
    secret = "plaintext-must-not-be-replicated"

    assert :ok = ACL.set_user(username, ["on", ">" <> secret, "+@all"], store: store)
    assert {:ok, encoded} = Router.server_catalog_entry(store, "acl", username)
    assert :binary.match(encoded, secret) == :nomatch
    assert {:ok, %{value: canonical}} = ServerCatalog.decode_entry(encoded)
    assert {:ok, projected} = ACL.decode_catalog_value(canonical)
    assert Acl.get_user(username).password == projected.password

    assert {:ok, 1} = ACL.del_user(username, store: store)
    assert Acl.get_user(username) == nil
    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", username)
  end

  test "public ACL mutations cannot bypass the replicated catalog" do
    store = FerricStore.Instance.get(:default)
    username = "public-catalog-user-#{System.unique_integer([:positive])}"

    assert :ok = Acl.set_user(username, ["on", "nopass", "+@all"])
    assert {:ok, encoded} = Router.server_catalog_entry(store, "acl", username)
    assert is_binary(encoded)

    assert :ok = Acl.del_user(username)
    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", username)
  end

  @tag :acl_mutation_projection_barrier
  test "public ACL mutation waits until the local catalog projector observes its revision" do
    username = "projection-barrier-#{System.unique_integer([:positive])}"
    projector = Process.whereis(CatalogProjector)
    assert is_pid(projector)
    assert :ok = :sys.suspend(projector)

    task = Task.async(fn -> Acl.set_user(username, ["on", "nopass", "+@all"]) end)

    try do
      assert Task.yield(task, 50) == nil
    after
      assert :ok = :sys.resume(projector)
    end

    assert :ok = Task.await(task, 5_000)

    assert %{ready: true, revision: revision} = CatalogProjector.status()
    assert revision == Acl.catalog_projection_revision()
  end

  test "public ACL load atomically replaces the replicated catalog" do
    store = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive])
    removed = "load-removed-#{suffix}"
    imported = "load-imported-#{suffix}"

    assert :ok = Acl.set_user(removed, ["on", "nopass", "+@all"])

    contents = """
    user default on nopass ~* &* +@all
    user #{imported} on nopass ~* &* +get
    """

    assert :ok = Acl.load_contents(contents)
    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", removed)
    assert {:ok, imported_entry} = Router.server_catalog_entry(store, "acl", imported)
    assert is_binary(imported_entry)
    assert Acl.get_user(removed) == nil
    assert Acl.get_user(imported) != nil
  end

  test "public multi-user deletion removes every catalog entry atomically" do
    store = FerricStore.Instance.get(:default)
    suffix = System.unique_integer([:positive])
    first = "delete-first-#{suffix}"
    second = "delete-second-#{suffix}"

    assert :ok = Acl.set_user(first, ["on", "nopass"])
    assert :ok = Acl.set_user(second, ["on", "nopass"])
    assert :ok = Acl.del_users([first, second])

    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", first)
    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", second)
  end

  test "catalog reconciliation atomically restores durable users and removes local-only users" do
    store = FerricStore.Instance.get(:default)
    username = "reconciled-user-#{System.unique_integer([:positive])}"
    local_only = "local-only-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(username, ["on", ">secret", "+@all"], store: store)
    expected_password = Acl.get_user(username).password

    assert :ok = Acl.reset_projection!()

    assert {:ok, local_value} =
             ACL.prepare_catalog_value(nil, local_only, ["on", "nopass", "+@all"])

    assert :ok =
             Acl.project_catalog_entry(local_only, ServerCatalog.encode_entry(0, local_value))

    assert Acl.get_user(username) == nil
    assert Acl.get_user(local_only) != nil

    assert :ok = ACL.reconcile_catalog(store)
    assert Acl.get_user(username).password == expected_password
    assert Acl.get_user(local_only) == nil

    assert {:ok, 1} = ACL.del_user(username, store: store)
  end

  test "a delayed pre-snapshot projection cannot resurrect a deleted user" do
    store = FerricStore.Instance.get(:default)
    username = "delayed-projection-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(username, ["on", "nopass", "+@all"], store: store)
    assert {:ok, old_create} = Router.server_catalog_entry(store, "acl", username)
    assert {:ok, 1} = ACL.del_user(username, store: store)
    assert :ok = ACL.reconcile_catalog(store)
    assert Acl.get_user(username) == nil

    assert :ok = Acl.project_catalog_entry(username, old_create)
    assert Acl.get_user(username) == nil
  end

  test "single-entry catalog projection advances the authoritative revision watermark" do
    store = FerricStore.Instance.get(:default)
    username = "watermark-user-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(username, ["on", "nopass", "+@all"], store: store)
    assert {:ok, encoded_revision} = Router.server_catalog_revision(store, "acl")
    assert {:ok, revision} = ServerCatalog.decode_revision(encoded_revision)
    assert Acl.catalog_projection_revision() == revision

    assert {:ok, 1} = ACL.del_user(username, store: store)
  end

  test "stale catalog projection is observable and denies cached full-access sessions" do
    on_exit(fn -> CatalogProjector.mark_ready() end)

    assert :ok = CatalogProjector.mark_stale(:injected_projection_failure)
    assert CatalogProjector.readiness() == {:stale, :injected_projection_failure}
    assert {:error, message} = ConnAuth.check_command_cached(:full_access, "GET")
    assert message =~ "ACL catalog projection unavailable"
    assert {:error, auth_message} = Acl.authenticate("default", "anything")
    assert auth_message =~ "ACL catalog projection unavailable"

    assert :ok = CatalogProjector.mark_ready()
    assert CatalogProjector.readiness() == :ready
    assert :ok = ConnAuth.check_command_cached(:full_access, "GET")
  end

  test "projector skips a full reconciliation when direct projection reached the revision" do
    store = FerricStore.Instance.get(:default)
    username = "incremental-projector-#{System.unique_integer([:positive])}"

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    before = CatalogProjector.status(projector)
    assert before.reconciliations == 1

    assert :ok = ACL.set_user(username, ["on", "nopass"], store: store)
    after_poll = CatalogProjector.poll_now(projector)

    assert after_poll.ready
    assert after_poll.revision == Acl.catalog_projection_revision()
    assert after_poll.reconciliations == before.reconciliations

    assert {:ok, 1} = ACL.del_user(username, store: store)
  end

  @tag :acl_revision_fence
  test "projector fences authorization until a required catalog revision is visible" do
    store = FerricStore.Instance.get(:default)

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    %{revision: revision, ready: true} = CatalogProjector.status(projector)
    required_revision = revision + 1

    assert %{ready: false, target_revision: ^required_revision} =
             CatalogProjector.require_revision(required_revision, projector)

    assert %{ready: false, target_revision: ^required_revision} =
             CatalogProjector.status(projector)
  end

  @tag :acl_revision_fence
  test "repeated fences at the projected revision do not reread the catalog" do
    store = FerricStore.Instance.get(:default)

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    %{revision: revision, ready: true} = CatalogProjector.status(projector)
    :erlang.trace_pattern({Router, :server_catalog_revision, 2}, true, [])
    :erlang.trace(projector, true, [:call])

    try do
      assert %{revision: ^revision, ready: true} =
               CatalogProjector.require_revision(revision, projector)

      assert %{revision: ^revision, ready: true} =
               CatalogProjector.require_revision(revision, projector)

      refute_receive {:trace, ^projector, :call,
                      {Router, :server_catalog_revision, [_store, "acl"]}},
                     50
    after
      :erlang.trace(projector, false, [:call])
      :erlang.trace_pattern({Router, :server_catalog_revision, 2}, false, [])
    end
  end

  @tag :acl_revision_fence
  test "revision fences revalidate a locally reset ACL projection" do
    store = FerricStore.Instance.get(:default)
    supervised_projector = Process.whereis(CatalogProjector)
    :ok = :sys.suspend(supervised_projector)

    on_exit(fn ->
      if Process.alive?(supervised_projector), do: :sys.resume(supervised_projector)
    end)

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    %{revision: revision, ready: true} = CatalogProjector.status(projector)
    assert :ok = Acl.reset_projection!()
    assert Acl.catalog_projection_revision() == -1

    assert %{ready: true, revision: ^revision} =
             CatalogProjector.require_revision(revision, projector)

    assert Acl.catalog_projection_revision() == revision
    assert Acl.get_user("default") != nil
  end

  @tag :acl_revision_invalidation
  test "catalog projections invalidate sessions with the committed revision" do
    store = FerricStore.Instance.get(:default)
    username = "revision-invalidation-#{System.unique_integer([:positive])}"
    client_id = System.unique_integer([:positive])
    :ok = ConnRegistry.register(client_id, self(), %{username: username})

    assert :ok = ACL.set_user(username, ["on", "nopass"], store: store)
    revision = Acl.catalog_projection_revision()

    assert_receive {:acl_invalidate, ^username, ^revision}
    assert {:ok, 1} = ACL.del_user(username, store: store)
  end

  @tag :acl_revision_invalidation
  test "durable user mutations advertise ordered changes to projectors" do
    store = FerricStore.Instance.get(:default)
    username = "projector-change-#{System.unique_integer([:positive])}"
    observer = start_projector_observer()

    previous_revision = durable_catalog_revision(store)
    assert :ok = ACL.set_user(username, ["on", "nopass"], store: store)
    revision = durable_catalog_revision(store)

    assert_receive {:projector_observer, ^observer,
                    {:acl_catalog_changed, :upsert, ^username, ^previous_revision, ^revision}}

    assert {:ok, 1} = ACL.del_user(username, store: store)
    delete_revision = durable_catalog_revision(store)

    assert_receive {:projector_observer, ^observer,
                    {:acl_catalog_changed, :delete, ^username, ^revision, ^delete_revision}}
  end

  @tag :acl_projector_membership
  test "supervised catalog projector joins the projector-only ACL group" do
    projector = Process.whereis(CatalogProjector)
    scope = ConnAuth.acl_projector_scope()
    group = ConnAuth.acl_projector_group()

    assert is_pid(projector)
    assert projector in :pg.get_members(scope, group)
  end

  @tag :acl_targeted_projection
  test "projector applies stable user upserts and deletes without a full catalog scan" do
    store = FerricStore.Instance.get(:default)
    username = "targeted-projection-#{System.unique_integer([:positive])}"
    supervised_projector = Process.whereis(CatalogProjector)
    :ok = :sys.suspend(supervised_projector)

    on_exit(fn ->
      if Process.alive?(supervised_projector), do: :sys.resume(supervised_projector)
    end)

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    table = FerricstoreServer.Acl.Tables.active_table()
    {:ok, value} = ACL.prepare_catalog_value(nil, username, ["on", "nopass", "+@all"])
    {_encoded, previous_revision, revision} = direct_catalog_mutation(store, username, value)

    trace_catalog_scans(projector)
    on_exit(&stop_catalog_scan_trace/0)

    send(
      projector,
      {:acl_catalog_changed, :upsert, username, previous_revision, revision}
    )

    assert %{ready: true, revision: ^revision} =
             CatalogProjector.require_revision(revision, projector)

    assert FerricstoreServer.Acl.Tables.active_table() == table
    assert Acl.get_user(username).enabled
    refute_catalog_scan(projector)

    {_tombstone, delete_previous, delete_revision} =
      direct_catalog_mutation(store, username, :deleted)

    send(
      projector,
      {:acl_catalog_changed, :delete, username, delete_previous, delete_revision}
    )

    assert %{ready: true, revision: ^delete_revision} =
             CatalogProjector.require_revision(delete_revision, projector)

    assert FerricstoreServer.Acl.Tables.active_table() == table
    assert Acl.get_user(username) == nil
    refute_catalog_scan(projector)
  end

  @tag :acl_targeted_projection
  test "projector reconciles exactly once when the catalog advances during notification" do
    store = FerricStore.Instance.get(:default)
    first = "projection-gap-first-#{System.unique_integer([:positive])}"
    second = "projection-gap-second-#{System.unique_integer([:positive])}"
    supervised_projector = Process.whereis(CatalogProjector)
    :ok = :sys.suspend(supervised_projector)

    on_exit(fn ->
      if Process.alive?(supervised_projector), do: :sys.resume(supervised_projector)
    end)

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 60_000, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    initial_table = FerricstoreServer.Acl.Tables.active_table()
    {:ok, first_value} = ACL.prepare_catalog_value(nil, first, ["on", "nopass"])

    {_first_encoded, initial_revision, first_revision} =
      direct_catalog_mutation(store, first, first_value)

    {:ok, second_value} = ACL.prepare_catalog_value(nil, second, ["on", "nopass"])

    {_second_encoded, ^first_revision, second_revision} =
      direct_catalog_mutation(store, second, second_value)

    trace_catalog_scans(projector)
    on_exit(&stop_catalog_scan_trace/0)
    send(projector, {:acl_catalog_changed, :upsert, first, initial_revision, first_revision})

    assert %{ready: true, revision: ^second_revision} =
             CatalogProjector.require_revision(second_revision, projector)

    assert Acl.get_user(first) != nil
    assert Acl.get_user(second) != nil
    refute FerricstoreServer.Acl.Tables.active_table() == initial_table

    assert_receive {:trace, ^projector, :call, {Router, :server_catalog_entries, [_store, "acl"]}}

    refute_catalog_scan(projector)
  end

  test "explicit reconciliation projects an authoritative full snapshot" do
    store = FerricStore.Instance.get(:default)
    first = "incremental-first-#{System.unique_integer([:positive])}"
    second = "incremental-second-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(first, ["on", ">first-secret", "+@all"], store: store)
    assert :ok = ACL.set_user(second, ["on", ">second-secret", "+@all"], store: store)
    assert :ok = Acl.reset_projection!()

    assert :ok = ACL.reconcile_catalog(store)
    assert Acl.get_user(first) != nil
    assert Acl.get_user(second) != nil

    assert {:ok, 1} = ACL.del_user(first, store: store)
    assert {:ok, 1} = ACL.del_user(second, store: store)
  end

  test "catalog projector completes reconciliation before start_link returns" do
    store = FerricStore.Instance.get(:default)
    username = "projector-user-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(username, ["on", ">secret"], store: store)
    expected_password = Acl.get_user(username).password
    assert :ok = Acl.reset_projection!()

    {:ok, projector} =
      CatalogProjector.start_link(store: store, poll_interval_ms: 10, name: nil)

    on_exit(fn ->
      if Process.alive?(projector), do: GenServer.stop(projector)
    end)

    assert Acl.get_user(username).password == expected_password
    assert {:ok, 1} = ACL.del_user(username, store: store)
  end

  test "concurrent creators cannot overrun the replicated live-user limit" do
    store = FerricStore.Instance.get(:default)
    first = "limit-first-#{System.unique_integer([:positive])}"
    second = "limit-second-#{System.unique_integer([:positive])}"

    assert {:ok, entries} = ACL.catalog_entries(store)
    max_users = length(entries) + 1
    Application.put_env(:ferricstore, :max_acl_users, max_users)

    tasks =
      for username <- [first, second] do
        Task.async(fn -> ACL.set_user(username, ["on", "nopass"], store: store) end)
      end

    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &(&1 == :ok)) == 1

    assert Enum.count(results, fn
             {:error, message} when is_binary(message) ->
               String.contains?(message, "max ACL users reached")

             _other ->
               false
           end) == 1

    for username <- [first, second], Acl.get_user(username) != nil do
      assert {:ok, 1} = ACL.del_user(username, store: store)
    end
  end

  test "deletion remains available when the configured user limit is invalid" do
    store = FerricStore.Instance.get(:default)
    username = "delete-with-invalid-limit-#{System.unique_integer([:positive])}"

    assert :ok = ACL.set_user(username, ["on", "nopass"], store: store)
    Application.put_env(:ferricstore, :max_acl_users, 0)

    assert {:ok, 1} = ACL.del_user(username, store: store)
    assert {:ok, nil} = Router.server_catalog_entry(store, "acl", username)
  end

  defp direct_catalog_mutation(store, username, value) do
    {:ok, expected} = Router.server_catalog_entry(store, "acl", username)
    {:ok, encoded_previous_revision} = Router.server_catalog_revision(store, "acl")

    previous_revision =
      case encoded_previous_revision do
        nil ->
          -1

        encoded ->
          {:ok, revision} = ServerCatalog.decode_revision(encoded)
          revision
      end

    assert {:ok, encoded} =
             Router.server_catalog_mutate(
               store,
               "acl",
               username,
               expected,
               encoded_previous_revision,
               value,
               10_000
             )

    assert {:ok, %{version: revision}} = ServerCatalog.decode_entry(encoded)
    {encoded, previous_revision, revision}
  end

  defp start_projector_observer do
    parent = self()
    scope = ConnAuth.acl_projector_scope()
    group = ConnAuth.acl_projector_group()

    observer =
      spawn(fn ->
        :ok = :pg.join(scope, group, self())
        send(parent, {:projector_observer_ready, self()})
        relay_projector_events(parent)
      end)

    assert_receive {:projector_observer_ready, ^observer}
    on_exit(fn -> Process.exit(observer, :kill) end)
    observer
  end

  defp relay_projector_events(parent) do
    receive do
      message ->
        send(parent, {:projector_observer, self(), message})
        relay_projector_events(parent)
    end
  end

  defp durable_catalog_revision(store) do
    assert {:ok, encoded} = Router.server_catalog_revision(store, "acl")
    assert {:ok, revision} = ServerCatalog.decode_revision(encoded)
    revision
  end

  defp trace_catalog_scans(projector) do
    :erlang.trace_pattern({Router, :server_catalog_entries, 2}, true, [])
    :erlang.trace(projector, true, [:call])
  end

  defp refute_catalog_scan(projector) do
    refute_receive {:trace, ^projector, :call,
                    {Router, :server_catalog_entries, [_store, "acl"]}},
                   50
  end

  defp stop_catalog_scan_trace do
    :erlang.trace_pattern({Router, :server_catalog_entries, 2}, false, [])
  end
end
