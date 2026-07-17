defmodule FerricstoreServer.Connection.AuthInvalidationTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Connection.Auth
  alias FerricstoreServer.Connection.Registry

  test "local ACL invalidation targets matching user sessions" do
    alice_sessions = Enum.map(1..2, fn _ -> start_session("alice") end)
    other_sessions = Enum.map(1..3, fn _ -> start_session("bob") end)
    sessions = alice_sessions ++ other_sessions

    on_exit(fn -> Enum.each(sessions, &Process.exit(&1, :kill)) end)

    assert :ok = Auth.broadcast_local_acl_invalidation("alice", 41)

    assert_messages(alice_sessions, {:acl_invalidate, "alice", 41})
    refute_session_messages(other_sessions)

    assert :ok = Auth.broadcast_local_acl_invalidation(:all, 42)
    assert_messages(sessions, {:acl_invalidate, :all, 42})
  end

  test "authenticated sessions move between targeted invalidation groups" do
    session = start_session("default")
    on_exit(fn -> Process.exit(session, :kill) end)

    send(session, {:move_user, "default", "alice", self()})
    assert_receive {:moved_user, ^session}

    assert :ok = Auth.broadcast_local_acl_invalidation("default", 51)
    refute_session_messages([session])

    assert :ok = Auth.broadcast_local_acl_invalidation("alice", 52)
    assert_messages([session], {:acl_invalidate, "alice", 52})
  end

  test "authentication group transition fails closed across an ACL epoch change" do
    username = "auth-group-race-#{System.unique_integer([:positive])}"
    assert :ok = FerricstoreServer.Acl.set_user(username, ["on", ">old-secret", "+@all", "~*"])

    initial_epoch = FerricstoreServer.Acl.get_user(username).auth_epoch
    client_id = System.unique_integer([:positive])
    :ok = Registry.register(client_id, self(), %{username: "default"})

    assert :ok = FerricstoreServer.Acl.set_user(username, [">new-secret"])

    state = %{
      client_id: client_id,
      username: "default",
      authenticated: false,
      require_auth: true,
      acl_cache: :denied
    }

    assert {:error, :acl_changed_during_authentication} =
             Auth.activate_authenticated_user(state, username, initial_epoch)

    assert :ok = Auth.broadcast_local_acl_invalidation(username, initial_epoch + 1)
    refute_receive {:acl_invalidate, ^username, _revision}, 50

    assert :ok = Auth.broadcast_local_acl_invalidation("default", initial_epoch + 1)
    assert_receive {:acl_invalidate, "default", _revision}

    current_epoch = FerricstoreServer.Acl.get_user(username).auth_epoch

    assert {:ok, authenticated} =
             Auth.activate_authenticated_user(state, username, current_epoch)

    assert authenticated.username == username
    assert authenticated.authenticated
    refute authenticated.require_auth

    assert :ok = Auth.broadcast_local_acl_invalidation(username, current_epoch)
    assert_receive {:acl_invalidate, ^username, ^current_epoch}
  end

  test "AUTH provisionally indexes the target user before verification" do
    client_id = System.unique_integer([:positive])
    :ok = Registry.register(client_id, self(), %{username: "default"})

    state = %{client_id: client_id, username: "default"}

    assert :ok = Auth.begin_acl_authentication(state, "alice")
    assert :ok = Auth.broadcast_local_acl_invalidation("alice", 61)
    assert_receive {:acl_invalidate, "alice", 61}

    assert :ok = Auth.cancel_acl_authentication(state, "alice")
    assert :ok = Auth.broadcast_local_acl_invalidation("alice", 62)
    refute_receive {:acl_invalidate, "alice", 62}, 50

    assert :ok = Auth.broadcast_local_acl_invalidation("default", 63)
    assert_receive {:acl_invalidate, "default", 63}
  end

  test "durable catalog changes are sent only to projectors" do
    parent = self()
    scope = Auth.acl_projector_scope()
    projector_group = Auth.acl_projector_group()

    projector =
      spawn(fn ->
        :ok = :pg.join(scope, projector_group, self())
        send(parent, {:projector_ready, self()})

        receive do
          message -> send(parent, {:projector_message, self(), message})
        end
      end)

    client_id = System.unique_integer([:positive])
    :ok = Registry.register(client_id, self(), %{username: "alice"})
    assert_receive {:projector_ready, ^projector}
    revision = FerricstoreServer.Acl.catalog_projection_revision()
    previous_revision = max(revision - 1, -1)

    assert :ok =
             Auth.broadcast_acl_catalog_change(
               :upsert,
               "alice",
               previous_revision,
               revision
             )

    assert_receive {:projector_message, ^projector,
                    {:acl_catalog_changed, :upsert, "alice", ^previous_revision, ^revision}}

    refute_receive {:acl_invalidate, "alice", ^revision}, 50
  end

  test "native connections do not re-fence projected ACL revisions per session" do
    source =
      File.read!(
        Path.expand(
          "../../../lib/ferricstore_server/native/connection.ex",
          __DIR__
        )
      )

    refute source =~ "CatalogProjector.require_revision"
  end

  defp start_session(username) do
    parent = self()
    client_id = System.unique_integer([:positive])

    pid =
      spawn(fn ->
        :ok = Registry.register(client_id, self(), %{username: username})
        send(parent, {:session_ready, self()})
        relay_session_messages(parent, client_id)
      end)

    assert_receive {:session_ready, ^pid}
    pid
  end

  defp relay_session_messages(parent, client_id) do
    receive do
      {:move_user, previous, current, caller} ->
        :ok = Auth.move_acl_invalidation_group(client_id, previous, current)
        send(caller, {:moved_user, self()})
        relay_session_messages(parent, client_id)

      message ->
        send(parent, {:session_message, self(), message})
        relay_session_messages(parent, client_id)
    end
  end

  defp assert_messages(sessions, expected) do
    Enum.each(sessions, fn session ->
      assert_receive {:session_message, ^session, ^expected}
    end)
  end

  defp refute_session_messages(sessions) do
    Enum.each(sessions, fn session ->
      refute_receive {:session_message, ^session, _message}, 50
    end)
  end
end
