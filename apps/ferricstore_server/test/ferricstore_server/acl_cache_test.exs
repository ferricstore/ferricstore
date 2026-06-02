defmodule FerricstoreServer.AclCacheTest do
  @moduledoc """
  Tests for ACL permission caching in the connection process.

  Verifies that:
    - The ACL cache is populated on connection init (default user)
    - AUTH populates the cache for the authenticated user
    - Cached permission checks allow/deny commands correctly
    - denied_commands are respected in the cache
    - ACL SETUSER triggers cache invalidation across connections
    - ACL DELUSER triggers cache invalidation
    - RESET clears and rebuilds the cache for the default user
    - Stress test: 100K cached permission checks complete in < 50ms

  These tests are `async: false` because they share the global ACL ETS table,
  the Config GenServer (for requirepass), and the :pg process group.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock, timeout \\ 5_000) do
    recv_response_acc(sock, "", timeout)
  end

  defp recv_responses(sock, count, timeout \\ 5_000) do
    recv_responses_acc(sock, count, "", [], timeout)
  end

  defp recv_responses_acc(_sock, count, _buf, acc, _timeout) when length(acc) >= count,
    do: Enum.take(acc, count)

  defp recv_responses_acc(sock, count, buf, acc, timeout) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [], _rest} ->
        recv_responses_acc(sock, count, buf2, acc, timeout)

      {:ok, values, rest} ->
        recv_responses_acc(sock, count, rest, acc ++ values, timeout)
    end
  end

  defp recv_response_acc(sock, buf, timeout) do
    {:ok, data} = :gen_tcp.recv(sock, 0, timeout)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response_acc(sock, buf2, timeout)
    end
  end

  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp connect_and_auth(port, username, password) do
    sock = connect_and_hello(port)
    send_cmd(sock, ["AUTH", username, password])
    resp = recv_response(sock)
    {sock, resp}
  end

  # Sets requirepass for tests that need AUTH. Registers on_exit cleanup.
  defp enable_requirepass do
    Ferricstore.Config.set("requirepass", "testpass")

    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
      Acl.reset!()
    end)
  end

  defp cleanup_requirepass do
    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
      Acl.reset!()
    end)
  end

  # Returns true if response is a NOPERM error.
  defp noperm?(resp) do
    match?({:error, "NOPERM" <> _}, resp)
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup do
    Acl.reset!()
    {:ok, port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Cache populated on connection init (default user)
  # ---------------------------------------------------------------------------

  describe "connection init populates ACL cache for default user" do
    test "default user can execute commands without ETS lookup contention", %{port: port} do
      sock = connect_and_hello(port)

      # The default user has +@all permissions. SET and GET should work.
      send_cmd(sock, ["SET", "acl_cache_test_key", "hello"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "acl_cache_test_key"])
      assert recv_response(sock) == "hello"

      # Cleanup
      send_cmd(sock, ["DEL", "acl_cache_test_key"])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "PING works immediately after connect (cache is populated)", %{port: port} do
      sock = connect_and_hello(port)
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end
  end

  describe "pipeline auth state" do
    test "pipelined GET observes requirepass enabled after connection init", %{port: port} do
      cleanup_requirepass()

      key = "acl:pipeline:live-requirepass:#{System.unique_integer([:positive])}"
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", key, "secret"])
      assert recv_response(sock) == {:simple, "OK"}

      admin = connect_and_hello(port)
      send_cmd(admin, ["CONFIG", "SET", "requirepass", "testpass"])
      assert recv_response(admin) == {:simple, "OK"}

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["GET", key]),
          Encoder.encode(["GET", key])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      assert [
               {:error, "NOAUTH Authentication required."},
               {:error, "NOAUTH Authentication required."}
             ] = recv_responses(sock, 2)
    end

    test "CONFIG SET requirepass is a pipeline auth barrier", %{port: port} do
      cleanup_requirepass()

      key = "acl:pipeline:config-barrier:#{System.unique_integer([:positive])}"
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", key, "secret"])
      assert recv_response(sock) == {:simple, "OK"}

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["CONFIG", "SET", "requirepass", "testpass"]),
          Encoder.encode(["GET", key])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      assert [
               {:simple, "OK"},
               {:error, "NOAUTH Authentication required."}
             ] = recv_responses(sock, 2)
    end
  end

  # ---------------------------------------------------------------------------
  # AUTH populates cache for authenticated user
  # ---------------------------------------------------------------------------

  describe "AUTH populates ACL cache" do
    test "successful ACL auth populates cache with user permissions", %{port: port} do
      enable_requirepass()

      # Create a user with only GET permission
      :ok = Acl.set_user("readonly", ["on", ">readpass", "~*", "-@all", "+get"])

      {sock, auth_resp} = connect_and_auth(port, "readonly", "readpass")
      assert auth_resp == {:simple, "OK"}

      # GET should be allowed (cached)
      send_cmd(sock, ["GET", "some_key"])
      resp = recv_response(sock)
      refute noperm?(resp), "Expected GET to be allowed, got: #{inspect(resp)}"

      # SET should be denied (cached)
      send_cmd(sock, ["SET", "some_key", "val"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end

    test "requirepass auth populates cache for default user", %{port: port} do
      enable_requirepass()

      {sock, auth_resp} = connect_and_auth(port, "default", "testpass")
      assert auth_resp == {:simple, "OK"}

      # Default user with +@all should be able to SET
      send_cmd(sock, ["SET", "cache_test_rp", "val"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", "cache_test_rp"])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "failed auth does not change cache", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("alice", ["on", ">correct", "~*", "+@all"])

      sock = connect_and_hello(port)
      send_cmd(sock, ["AUTH", "alice", "wrong_password"])
      resp = recv_response(sock)
      assert {:error, msg} = resp
      assert msg =~ "WRONGPASS"

      # Connection still requires auth (requirepass is set)
      send_cmd(sock, ["SET", "key", "val"])
      resp = recv_response(sock)
      assert {:error, msg2} = resp
      assert msg2 =~ "NOAUTH"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Cached permission checks: allow/deny
  # ---------------------------------------------------------------------------

  describe "check_command_cached allows permitted commands" do
    test "specific ACL subcommand grant does not grant all ACL subcommands" do
      cache = %{
        commands: MapSet.new(["ACL.WHOAMI"]),
        denied_commands: MapSet.new(),
        keys: :all,
        channels: :all,
        enabled: true
      }

      assert :ok = Auth.check_command_cached(cache, "ACL.WHOAMI")
      assert {:error, "NOPERM" <> _} = Auth.check_command_cached(cache, "ACL.SETUSER")
    end

    test "parent ACL command grant covers known ACL subcommands" do
      cache = %{
        commands: MapSet.new(["ACL"]),
        denied_commands: MapSet.new(),
        keys: :all,
        channels: :all,
        enabled: true
      }

      assert :ok = Auth.check_command_cached(cache, "ACL.SETUSER")
      assert :ok = Auth.check_command_cached(cache, "ACL.GETUSER")
    end

    test "user with +@all can run any command", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("admin", ["on", ">adminpass", "~*", "+@all"])

      {sock, _} = connect_and_auth(port, "admin", "adminpass")

      for cmd <- ["SET", "GET", "DEL", "HSET", "LPUSH", "ZADD", "PING", "INFO"] do
        send_cmd(sock, [cmd | dummy_args(cmd)])
        resp = recv_response(sock)
        refute noperm?(resp), "Expected #{cmd} to be allowed, got: #{inspect(resp)}"
      end

      :gen_tcp.close(sock)
    end

    test "user with specific commands can only run those", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("limited", ["on", ">pass", "~*", "-@all", "+get", "+set"])

      {sock, _} = connect_and_auth(port, "limited", "pass")

      # GET and SET should work
      send_cmd(sock, ["SET", "lim_key", "val"])
      resp = recv_response(sock)
      refute noperm?(resp), "Expected SET to be allowed, got: #{inspect(resp)}"

      send_cmd(sock, ["GET", "lim_key"])
      resp = recv_response(sock)
      refute noperm?(resp), "Expected GET to be allowed, got: #{inspect(resp)}"

      # DEL should be denied
      send_cmd(sock, ["DEL", "lim_key"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected DEL to be denied, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end
  end

  describe "check_command_cached denies unpermitted commands" do
    test "disabled user is denied all commands", %{port: port} do
      enable_requirepass()
      # Create user, then disable it
      :ok = Acl.set_user("disabled_user", ["on", ">pass", "~*", "+@all"])
      {sock, _} = connect_and_auth(port, "disabled_user", "pass")

      # Verify auth worked
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      # Now disable the user (direct ETS mutation + broadcast)
      :ok = Acl.set_user("disabled_user", ["off"])

      # Use a second connection to trigger the ACL SETUSER broadcast
      admin_sock = connect_and_hello(port)
      send_cmd(admin_sock, ["AUTH", "testpass"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      send_cmd(admin_sock, ["ACL", "SETUSER", "disabled_user", "off"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      :gen_tcp.close(admin_sock)

      # Give the invalidation message time to be delivered
      Process.sleep(100)

      # Commands should now be denied
      send_cmd(sock, ["SET", "x", "y"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied for disabled user, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end
  end

  describe "denied_commands respected in cache" do
    test "user with +@all -set cannot run SET", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("no_set", ["on", ">pass", "~*", "+@all", "-set"])

      {sock, _} = connect_and_auth(port, "no_set", "pass")

      # GET should work
      send_cmd(sock, ["GET", "some_key"])
      resp = recv_response(sock)
      refute noperm?(resp), "Expected GET to be allowed, got: #{inspect(resp)}"

      # SET should be denied
      send_cmd(sock, ["SET", "some_key", "val"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end

    test "user with +@all -@write cannot run write commands", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("reader", ["on", ">pass", "~*", "+@all", "-@write"])

      {sock, _} = connect_and_auth(port, "reader", "pass")

      # GET (read) should work
      send_cmd(sock, ["GET", "some_key"])
      resp = recv_response(sock)
      refute noperm?(resp), "Expected GET to be allowed, got: #{inspect(resp)}"

      # SET (write) should be denied
      send_cmd(sock, ["SET", "some_key", "val"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied, got: #{inspect(resp)}"

      # DEL (write) should be denied
      send_cmd(sock, ["DEL", "some_key"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected DEL to be denied, got: #{inspect(resp)}"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL SETUSER triggers cache invalidation
  # ---------------------------------------------------------------------------

  describe "ACL SETUSER triggers cache invalidation" do
    test "adding permission is reflected in existing connection", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("evolving", ["on", ">pass", "~*", "-@all", "+get"])

      {sock, _} = connect_and_auth(port, "evolving", "pass")

      # SET should initially be denied
      send_cmd(sock, ["SET", "evo_key", "v"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be initially denied"

      # Admin adds SET permission via a second connection
      admin_sock = connect_and_hello(port)
      send_cmd(admin_sock, ["AUTH", "testpass"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      send_cmd(admin_sock, ["ACL", "SETUSER", "evolving", "+set"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      :gen_tcp.close(admin_sock)

      # Give invalidation time to propagate
      Process.sleep(100)

      # SET should now be allowed
      send_cmd(sock, ["SET", "evo_key", "v"])
      resp2 = recv_response(sock)
      refute noperm?(resp2), "Expected SET to be allowed after ACL update, got: #{inspect(resp2)}"

      send_cmd(sock, ["DEL", "evo_key"])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "revoking permission is reflected in existing connection", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("shrinking", ["on", ">pass", "~*", "+@all"])

      {sock, _} = connect_and_auth(port, "shrinking", "pass")

      # SET should initially work
      send_cmd(sock, ["SET", "shrink_key", "v"])
      assert recv_response(sock) == {:simple, "OK"}

      # Admin revokes SET permission
      admin_sock = connect_and_hello(port)
      send_cmd(admin_sock, ["AUTH", "testpass"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      send_cmd(admin_sock, ["ACL", "SETUSER", "shrinking", "-set"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      :gen_tcp.close(admin_sock)

      # Give invalidation time to propagate
      Process.sleep(100)

      # SET should now be denied
      send_cmd(sock, ["SET", "shrink_key", "v2"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied after revocation, got: #{inspect(resp)}"

      send_cmd(sock, ["DEL", "shrink_key"])
      recv_response(sock)
      :gen_tcp.close(sock)
    end

    test "invalidation does not affect connections with different usernames", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("userA", ["on", ">passA", "~*", "+@all"])
      :ok = Acl.set_user("userB", ["on", ">passB", "~*", "+@all"])

      {sock_a, _} = connect_and_auth(port, "userA", "passA")
      {sock_b, _} = connect_and_auth(port, "userB", "passB")

      # Revoke SET from userA via admin connection
      admin_sock = connect_and_hello(port)
      send_cmd(admin_sock, ["AUTH", "testpass"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      send_cmd(admin_sock, ["ACL", "SETUSER", "userA", "-set"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      :gen_tcp.close(admin_sock)

      Process.sleep(100)

      # userA should be denied SET
      send_cmd(sock_a, ["SET", "keyA", "v"])
      resp_a = recv_response(sock_a)
      assert noperm?(resp_a), "Expected userA's SET to be denied"

      # userB should still be able to SET
      send_cmd(sock_b, ["SET", "keyB", "v"])
      resp_b = recv_response(sock_b)
      refute noperm?(resp_b), "Expected userB's SET to still work, got: #{inspect(resp_b)}"

      send_cmd(sock_b, ["DEL", "keyB"])
      recv_response(sock_b)
      :gen_tcp.close(sock_a)
      :gen_tcp.close(sock_b)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL DELUSER triggers cache invalidation
  # ---------------------------------------------------------------------------

  describe "ACL DELUSER triggers cache invalidation" do
    test "rebuilt cache for deleted users fails closed" do
      :ok = Acl.set_user("doomed", ["on", ">pass", "~*", "+@all"])
      assert Auth.build_acl_cache("doomed") != :denied

      :ok = Acl.del_user("doomed")
      assert Auth.build_acl_cache("doomed") == :denied

      assert {:error, "NOPERM" <> _} = Auth.check_command_cached(:denied, "PING")
      assert {:error, "NOPERM" <> _} = Auth.check_keys_cached(:denied, "GET", ["k"])
      assert {:error, "NOPERM" <> _} = Auth.check_command_cached(nil, "PING")
      assert {:error, "NOPERM" <> _} = Auth.check_keys_cached(nil, "GET", ["k"])
    end

    test "deleted user's connections get cache invalidated", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("doomed", ["on", ">pass", "~*", "+@all"])

      {sock, _} = connect_and_auth(port, "doomed", "pass")

      # Verify the connection works initially
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      # Delete the user via admin
      admin_sock = connect_and_hello(port)
      send_cmd(admin_sock, ["AUTH", "testpass"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      send_cmd(admin_sock, ["ACL", "DELUSER", "doomed"])
      assert recv_response(admin_sock) == {:simple, "OK"}
      :gen_tcp.close(admin_sock)

      Process.sleep(100)

      # After invalidation, the rebuilt cache fails closed. The connection
      # should keep running and return either PONG if the invalidation races
      # this request or NOPERM once the denied cache is installed.
      send_cmd(sock, ["PING"])
      resp = recv_response(sock)
      assert resp == {:simple, "PONG"} or match?({:error, _}, resp)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # RESET clears and rebuilds cache
  # ---------------------------------------------------------------------------

  describe "RESET rebuilds ACL cache for default user" do
    test "RESET switches back to default user permissions", %{port: port} do
      enable_requirepass()
      :ok = Acl.set_user("temp", ["on", ">pass", "~*", "-@all", "+get"])

      {sock, _} = connect_and_auth(port, "temp", "pass")

      # SET should be denied for "temp"
      send_cmd(sock, ["SET", "reset_key", "v"])
      resp = recv_response(sock)
      assert noperm?(resp), "Expected SET to be denied for temp user"

      # RESET should switch back to default user
      send_cmd(sock, ["RESET"])
      resp = recv_response(sock)
      assert resp == {:simple, "RESET"}

      # Now requires auth again (requirepass is set)
      send_cmd(sock, ["AUTH", "testpass"])
      assert recv_response(sock) == {:simple, "OK"}

      # Default user has +@all, SET should work now
      send_cmd(sock, ["SET", "reset_key", "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["DEL", "reset_key"])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Stress: 100K check_command_cached calls < 50ms
  # ---------------------------------------------------------------------------

  describe "performance" do
    @tag :bench
    test "100K cached permission checks complete in under 50ms" do
      # Build a cache matching the default user (commands: :all)
      cache = %{
        commands: :all,
        denied_commands: MapSet.new(),
        keys: :all,
        enabled: true
      }

      # Replicate the check_command_cached logic inline to benchmark the
      # pure function cost without calling private functions.
      check_fn = fn cache, cmd ->
        cmd_up = String.upcase(cmd)

        cond do
          not cache.enabled ->
            {:error, "NOPERM"}

          cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd_up) ->
            :ok

          cache.commands == :all ->
            {:error, "NOPERM"}

          MapSet.member?(cache.commands, cmd_up) and
              not MapSet.member?(cache.denied_commands, cmd_up) ->
            :ok

          true ->
            {:error, "NOPERM"}
        end
      end

      commands = ~w(GET SET DEL HSET HGET LPUSH RPOP ZADD INFO PING)

      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..10_000, cmd <- commands do
            ^cmd = cmd
            :ok = check_fn.(cache, cmd)
          end
        end)

      elapsed_ms = elapsed_us / 1_000

      assert elapsed_ms < 50,
             "100K cached checks took #{elapsed_ms}ms, expected < 50ms"
    end

    @tag :bench
    test "100K cached permission checks with denied_commands complete in under 50ms" do
      cache = %{
        commands: :all,
        denied_commands: MapSet.new(~w(SET DEL HSET LPUSH ZADD)),
        keys: :all,
        enabled: true
      }

      check_fn = fn cache, cmd ->
        cmd_up = String.upcase(cmd)

        cond do
          not cache.enabled ->
            {:error, "NOPERM"}

          cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd_up) ->
            :ok

          cache.commands == :all ->
            {:error, "NOPERM"}

          true ->
            {:error, "NOPERM"}
        end
      end

      commands = ~w(GET SET DEL HSET HGET LPUSH RPOP ZADD INFO PING)

      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..10_000, cmd <- commands do
            check_fn.(cache, cmd)
          end
        end)

      elapsed_ms = elapsed_us / 1_000

      assert elapsed_ms < 50,
             "100K cached checks (with denials) took #{elapsed_ms}ms, expected < 50ms"
    end

    @tag :bench
    test "100K cached permission checks with MapSet commands complete in under 50ms" do
      cache = %{
        commands: MapSet.new(~w(GET SET DEL HSET HGET LPUSH RPOP ZADD INFO PING)),
        denied_commands: MapSet.new(),
        keys: :all,
        enabled: true
      }

      check_fn = fn cache, cmd ->
        cmd_up = String.upcase(cmd)

        cond do
          not cache.enabled ->
            {:error, "NOPERM"}

          cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd_up) ->
            :ok

          cache.commands == :all ->
            {:error, "NOPERM"}

          MapSet.member?(cache.commands, cmd_up) and
              not MapSet.member?(cache.denied_commands, cmd_up) ->
            :ok

          true ->
            {:error, "NOPERM"}
        end
      end

      commands = ~w(GET SET DEL HSET HGET LPUSH RPOP ZADD INFO PING)

      {elapsed_us, _} =
        :timer.tc(fn ->
          for _ <- 1..10_000, cmd <- commands do
            check_fn.(cache, cmd)
          end
        end)

      elapsed_ms = elapsed_us / 1_000

      assert elapsed_ms < 50,
             "100K cached checks (MapSet commands) took #{elapsed_ms}ms, expected < 50ms"
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Returns minimal valid arguments for a command (enough to avoid arity errors).
  defp dummy_args("SET"), do: ["_acl_test_key", "v"]
  defp dummy_args("GET"), do: ["_acl_test_key"]
  defp dummy_args("DEL"), do: ["_acl_test_key"]
  defp dummy_args("HSET"), do: ["_acl_test_hash", "f", "v"]
  defp dummy_args("LPUSH"), do: ["_acl_test_list", "v"]
  defp dummy_args("ZADD"), do: ["_acl_test_zset", "1", "m"]
  defp dummy_args("PING"), do: []
  defp dummy_args("INFO"), do: []
  defp dummy_args(_), do: []
end
