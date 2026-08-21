defmodule FerricstoreServer.CommandGatewayTest do
  use Ferricstore.Test.AclCase

  @moduletag :global_state

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Acl.CatalogProjector
  alias FerricstoreServer.AuthenticationGateway
  alias FerricstoreServer.AuthenticationGateway.Session
  alias FerricstoreServer.CommandGateway

  setup do
    {:ok, _apps} = Application.ensure_all_started(:ferricstore_server)
    :ok = CatalogProjector.mark_ready()
    :ok = FerricstoreServer.AuthRateLimiter.reset()
    :ok = Ferricstore.Config.set("requirepass", "")
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
    end)

    :ok
  end

  test "authenticates an ACL credential without retaining the password" do
    put_http_user("http-user", "secret")

    assert {:ok, %Session{username: "http-user"} = session} =
             AuthenticationGateway.authenticate("http-user", "secret", peer: peer())

    refute inspect(session) =~ "secret"

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("http-user", "wrong", peer: peer())
  end

  test "supports the legacy default requirepass credential" do
    assert :ok = Ferricstore.Config.set("requirepass", "legacy-secret")

    assert {:ok, %Session{username: "default"} = session} =
             AuthenticationGateway.authenticate("default", "legacy-secret", peer: peer())

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("default", "wrong", peer: peer())

    assert :ok = Ferricstore.Config.set("requirepass", "replacement-secret")

    assert {:error, :reauthentication_required} =
             CommandGateway.execute_batch(session, [["PING"]])
  end

  test "uses the shared authentication rate limiter" do
    put_http_user("limited", "secret")
    Application.put_env(:ferricstore, :auth_rate_limit_max_attempts, 1)

    on_exit(fn ->
      Application.delete_env(:ferricstore, :auth_rate_limit_max_attempts)
    end)

    assert {:error, :unauthenticated} =
             AuthenticationGateway.authenticate("limited", "wrong", peer: peer())

    assert {:error, {:rate_limited, retry_after_ms}} =
             AuthenticationGateway.authenticate("limited", "secret", peer: peer())

    assert retry_after_ms > 0
  end

  test "executes an ordered stateless command batch through the canonical command path" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())

    key = allowed_key("allowed")

    assert {:ok,
            [
              %{status: :ok, value: "OK"},
              %{status: :ok, value: "value"}
            ]} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               ["GET", key]
             ])
  end

  test "returns command and key ACL failures in their original batch positions" do
    put_http_user("reader", "secret", ["+GET"])
    assert {:ok, session} = AuthenticationGateway.authenticate("reader", "secret", peer: peer())

    denied_key = denied_key("denied")
    allowed_key = allowed_key("allowed")

    assert {:ok,
            [
              %{status: :noperm, value: denied},
              %{status: :ok, value: nil},
              %{status: :noperm, value: command_denied}
            ]} =
             CommandGateway.execute_batch(session, [
               ["GET", denied_key],
               ["GET", allowed_key],
               ["SET", allowed_key, "value"]
             ])

    assert denied =~ "NOPERM"
    assert command_denied =~ "NOPERM"
  end

  test "invalidates a cached session when its credential epoch changes" do
    put_http_user("rotating", "old-secret")

    assert {:ok, session} =
             AuthenticationGateway.authenticate("rotating", "old-secret", peer: peer())

    assert :ok = FerricstoreServer.Acl.set_user("rotating", [">new-secret"])

    assert {:error, :reauthentication_required} =
             CommandGateway.execute_batch(session, [["PING"]])
  end

  test "prevalidates the complete batch and rejects connection-scoped commands" do
    put_http_user("writer", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("writer", "secret", peer: peer())
    key = allowed_key("not-written")

    assert {:error, {:unsupported_command, 1, "MULTI"}} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               ["MULTI"]
             ])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil

    assert {:error, {:malformed_command, 1}} =
             CommandGateway.execute_batch(session, [
               ["SET", key, "value"],
               []
             ])

    assert Router.get(FerricStore.Instance.get(:default), key) == nil
  end

  test "rejects every stateful command family at the transport-neutral boundary" do
    put_http_user("worker", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("worker", "secret", peer: peer())

    for command <- ["AUTH", "MULTI", "BLPOP"] do
      assert {:error, {:unsupported_command, 0, ^command}} =
               CommandGateway.execute_batch(session, [[command]])
    end
  end

  test "bounds batches and propagates absolute deadlines" do
    put_http_user("worker", "secret")
    assert {:ok, session} = AuthenticationGateway.authenticate("worker", "secret", peer: peer())

    assert {:error, {:too_many_commands, 1}} =
             CommandGateway.execute_batch(session, [["PING"], ["PING"]], max_commands: 1)

    expired = System.system_time(:millisecond) - 1

    assert {:ok, [%{status: :error, value: %{"code" => "deadline_exceeded"}}]} =
             CommandGateway.execute_batch(session, [["PING"]], deadline_ms: expired)
  end

  defp put_http_user(username, password, commands \\ ["+GET", "+SET"]) do
    assert :ok =
             FerricstoreServer.Acl.set_user(
               username,
               ["on", ">#{password}", "-@all" | commands] ++ ["~tenant:a:*"]
             )
  end

  defp peer, do: {{127, 0, 0, 1}, 12_345}

  defp allowed_key(suffix) do
    "tenant:a:gateway:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end

  defp denied_key(suffix) do
    "tenant:b:gateway:#{suffix}:#{System.unique_integer([:positive, :monotonic])}"
  end
end
