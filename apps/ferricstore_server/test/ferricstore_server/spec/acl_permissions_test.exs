defmodule FerricstoreServer.Spec.AclPermissionsTest do
  @moduledoc """
  Tests for ACL command-level permission enforcement.

  Verifies that the `FerricstoreServer.Acl.check_command/2` function correctly
  enforces command-level restrictions based on user ACL rules:

    - `+@all` grants access to every command
    - `-@all` denies access to every command
    - `+command` grants access to a specific command
    - `-command` denies access to a specific command
    - `+@category` / `-@category` grants/denies categories of commands

  Also verifies that `FerricstoreServer.Connection` integrates the check
  at dispatch time, returning NOPERM errors over TCP when a user is denied.

  These tests are `async: false` because they share the global ACL ETS table
  and the Config GenServer (for requirepass).
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
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

  defp parsed_command_keys(wire) do
    assert {:ok, [{:command, cmd, _args, _ast, keys}], ""} = Parser.parse_commands(wire)
    {cmd, keys}
  end

  defp parser_supported_commands do
    parser_path =
      Path.expand(
        "../../../../ferricstore/native/resp_parser_nif/src/lib.rs",
        __DIR__
      )

    parser_source = File.read!(parser_path)

    parser_ast_commands =
      parser_source
      |> parser_match_block!(
        ~r/fn classify_command_ast\(cmd: &\[u8\], arity: usize\).*?match cmd \{(.*?)\n\s*_ => CommandAstKind::Unknown,/s
      )
      |> parser_command_literals()

    parser_tag_commands =
      parser_source
      |> parser_match_block!(
        ~r/fn command_tag_name\(cmd: &\[u8\]\).*?match cmd \{(.*?)\n\s*}\n}/s
      )
      |> parser_command_literals()

    MapSet.union(parser_ast_commands, parser_tag_commands)
  end

  defp parser_match_block!(source, pattern) do
    [_all, command_tag_block] =
      Regex.run(
        pattern,
        source
      )

    command_tag_block
  end

  defp parser_command_literals(block) do
    ~r/b"([A-Z0-9_.]+)"\s*=>\s*Some/
    |> Regex.scan(block, capture: :all_but_first)
    |> then(fn tag_commands ->
      ast_commands =
        ~r/b"([A-Z0-9_.]+)"(?:\s+if\s+.*?)?\s*=>\s*CommandAstKind::/
        |> Regex.scan(block, capture: :all_but_first)

      tag_commands ++ ast_commands
    end)
    |> List.flatten()
    |> MapSet.new()
  end

  # Sets requirepass for tests that need AUTH. Registers on_exit cleanup.
  defp enable_requirepass do
    Ferricstore.Config.set("requirepass", "testpass")

    on_exit(fn ->
      Ferricstore.Config.set("requirepass", "")
      Acl.reset!()
    end)
  end

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    Acl.reset!()
    :ok
  end

  # ---------------------------------------------------------------------------
  # Unit tests: Acl.check_command/2
  # ---------------------------------------------------------------------------

  describe "Acl.check_command/2 with +@all" do
    test "user with +@all (commands: :all) can run any command" do
      assert :ok = Acl.check_command("default", "GET")
      assert :ok = Acl.check_command("default", "SET")
      assert :ok = Acl.check_command("default", "FLUSHDB")
      assert :ok = Acl.check_command("default", "CONFIG")
      assert :ok = Acl.check_command("default", "ACL")
    end

    test "explicitly created user with +@all can run any command" do
      :ok = Acl.set_user("admin", ["on", ">pass", "+@all"])
      assert :ok = Acl.check_command("admin", "GET")
      assert :ok = Acl.check_command("admin", "FLUSHDB")
      assert :ok = Acl.check_command("admin", "DEL")
    end
  end

  describe "Acl.check_command/2 with -@all" do
    test "user with -@all gets NOPERM on any command" do
      :ok = Acl.set_user("noone", ["on", ">pass", "-@all"])

      assert {:error, msg} = Acl.check_command("noone", "GET")
      assert msg =~ "NOPERM"
      assert msg =~ "get"

      assert {:error, _} = Acl.check_command("noone", "SET")
      assert {:error, _} = Acl.check_command("noone", "FLUSHDB")
      assert {:error, _} = Acl.check_command("noone", "CONFIG")
    end
  end

  describe "Acl.check_command/2 with specific commands" do
    test "user with +get +set can run GET and SET" do
      :ok = Acl.set_user("limited", ["on", ">pass", "-@all", "+get", "+set"])

      assert :ok = Acl.check_command("limited", "GET")
      assert :ok = Acl.check_command("limited", "SET")
    end

    test "user with +get +set cannot run DEL" do
      :ok = Acl.set_user("limited", ["on", ">pass", "-@all", "+get", "+set"])

      assert {:error, msg} = Acl.check_command("limited", "DEL")
      assert msg =~ "NOPERM"
      assert msg =~ "del"
    end

    test "user with +get +set cannot run FLUSHDB" do
      :ok = Acl.set_user("limited", ["on", ">pass", "-@all", "+get", "+set"])

      assert {:error, msg} = Acl.check_command("limited", "FLUSHDB")
      assert msg =~ "NOPERM"
    end

    test "check_command is case-insensitive for command name" do
      :ok = Acl.set_user("limited", ["on", ">pass", "-@all", "+get"])

      assert :ok = Acl.check_command("limited", "get")
      assert :ok = Acl.check_command("limited", "GET")
      assert :ok = Acl.check_command("limited", "Get")
    end

    test "user with specific commands cannot run unlisted commands" do
      :ok = Acl.set_user("safe2", ["on", ">pass", "-@all", "+get", "+set", "+del"])

      assert :ok = Acl.check_command("safe2", "GET")
      assert :ok = Acl.check_command("safe2", "SET")
      assert :ok = Acl.check_command("safe2", "DEL")
      assert {:error, _} = Acl.check_command("safe2", "FLUSHDB")
    end
  end

  describe "Acl.check_command/2 with non-existent or disabled users" do
    test "non-existent user gets NOPERM" do
      assert {:error, msg} = Acl.check_command("ghost", "GET")
      assert msg =~ "NOPERM"
    end

    test "disabled user gets NOPERM" do
      :ok = Acl.set_user("disabled_user", ["off", ">pass", "+@all"])
      assert {:error, msg} = Acl.check_command("disabled_user", "GET")
      assert msg =~ "NOPERM"
    end
  end

  # ---------------------------------------------------------------------------
  # Unit tests: Acl.check_command/2 with categories
  # ---------------------------------------------------------------------------

  describe "Acl.check_command/2 with @read category" do
    test "+@read allows GET, MGET, HGET and other read commands" do
      :ok = Acl.set_user("reader", ["on", ">pass", "-@all", "+@read"])

      for cmd <- ~w(GET MGET HGET HGETALL HKEYS HVALS LRANGE LLEN
                     SMEMBERS SISMEMBER SCARD ZSCORE ZRANGE ZCARD
                     EXISTS TTL PTTL TYPE STRLEN GETRANGE) do
        assert :ok = Acl.check_command("reader", cmd),
               "Expected #{cmd} to be allowed for @read user"
      end
    end

    test "+@read does NOT allow SET, DEL, or other write commands" do
      :ok = Acl.set_user("reader", ["on", ">pass", "-@all", "+@read"])

      for cmd <- ~w(SET DEL HSET LPUSH SADD ZADD) do
        assert {:error, _} = Acl.check_command("reader", cmd),
               "Expected #{cmd} to be denied for @read user"
      end
    end
  end

  describe "Acl.check_command/2 with @write category" do
    test "+@write allows SET, DEL, HSET and other write commands" do
      :ok = Acl.set_user("writer", ["on", ">pass", "-@all", "+@write"])

      for cmd <- ~w(SET DEL HSET LPUSH RPUSH SADD ZADD SREM LPOP RPOP
                     INCR DECR INCRBY DECRBY APPEND
                     EXPIRE PERSIST SETRANGE SETNX MSET) do
        assert :ok = Acl.check_command("writer", cmd),
               "Expected #{cmd} to be allowed for @write user"
      end
    end

    test "+@write does NOT allow GET or other read-only commands" do
      :ok = Acl.set_user("writer", ["on", ">pass", "-@all", "+@write"])

      for cmd <- ~w(GET MGET HGET LRANGE SMEMBERS ZSCORE) do
        assert {:error, _} = Acl.check_command("writer", cmd),
               "Expected #{cmd} to be denied for @write user"
      end
    end
  end

  describe "Acl.check_command/2 with @admin category" do
    test "+@admin allows CONFIG, ACL, DEBUG, and other admin commands" do
      :ok = Acl.set_user("admin", ["on", ">pass", "-@all", "+@admin"])

      for cmd <- ~w(CONFIG ACL DEBUG SLOWLOG SAVE BGSAVE FLUSHDB FLUSHALL) do
        assert :ok = Acl.check_command("admin", cmd),
               "Expected #{cmd} to be allowed for @admin user"
      end
    end

    test "+@admin does NOT allow GET, SET" do
      :ok = Acl.set_user("admin", ["on", ">pass", "-@all", "+@admin"])

      for cmd <- ~w(GET SET DEL) do
        assert {:error, _} = Acl.check_command("admin", cmd),
               "Expected #{cmd} to be denied for @admin-only user"
      end
    end
  end

  describe "Acl.check_command/2 with @dangerous category" do
    test "+@dangerous allows FLUSHDB, FLUSHALL, DEBUG, CONFIG, and KEYS" do
      :ok = Acl.set_user("danger", ["on", ">pass", "-@all", "+@dangerous"])

      for cmd <- ~w(FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN) do
        assert :ok = Acl.check_command("danger", cmd),
               "Expected #{cmd} to be allowed for @dangerous user"
      end
    end
  end

  describe "Acl.check_command/2 with combined categories" do
    test "+@read +@write allows both read and write commands" do
      :ok = Acl.set_user("readwrite", ["on", ">pass", "-@all", "+@read", "+@write"])

      assert :ok = Acl.check_command("readwrite", "GET")
      assert :ok = Acl.check_command("readwrite", "SET")
      assert :ok = Acl.check_command("readwrite", "DEL")
      assert :ok = Acl.check_command("readwrite", "HGET")
    end

    test "+@read +@write does NOT allow admin commands" do
      :ok = Acl.set_user("readwrite", ["on", ">pass", "-@all", "+@read", "+@write"])

      assert {:error, _} = Acl.check_command("readwrite", "CONFIG")
      assert {:error, _} = Acl.check_command("readwrite", "DEBUG")
    end

    test "multiple categories combine correctly" do
      :ok = Acl.set_user("safe", ["on", ">pass", "-@all", "+@read", "+@write", "+@admin"])

      assert :ok = Acl.check_command("safe", "GET")
      assert :ok = Acl.check_command("safe", "CONFIG")
    end
  end

  # ---------------------------------------------------------------------------
  # TCP integration tests: NOPERM enforcement over the wire
  # ---------------------------------------------------------------------------

  describe "TCP: command permission enforcement" do
    setup do
      enable_requirepass()
      :ok
    end

    test "user with +get +set can SET and GET over TCP", %{port: port} do
      :ok =
        Acl.set_user("limited", [
          "on",
          ">limitpass",
          "-@all",
          "+get",
          "+set",
          "+auth",
          "+hello",
          "+ping",
          "+command",
          "~*"
        ])

      {sock, resp} = connect_and_auth(port, "limited", "limitpass")
      assert resp == {:simple, "OK"}

      send_cmd(sock, ["SET", "mykey", "myval"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "mykey"])
      assert recv_response(sock) == "myval"

      :gen_tcp.close(sock)
    end

    test "user with +get +set gets NOPERM on DEL over TCP", %{port: port} do
      :ok =
        Acl.set_user("limited", [
          "on",
          ">limitpass",
          "-@all",
          "+get",
          "+set",
          "+auth",
          "+hello",
          "+ping",
          "+command"
        ])

      {sock, _} = connect_and_auth(port, "limited", "limitpass")

      send_cmd(sock, ["DEL", "somekey"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      :gen_tcp.close(sock)
    end

    test "EXEC rechecks permissions for queued commands after ACL changes", %{port: port} do
      key = "acl:tx:#{System.unique_integer([:positive])}"

      :ok =
        Acl.set_user("tx_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+multi",
          "+exec",
          "+discard",
          "+get",
          "+set",
          "~*"
        ])

      {sock, _} = connect_and_auth(port, "tx_user", "txpass")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "must_not_write"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      :ok =
        Acl.set_user("tx_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+multi",
          "+exec",
          "+discard",
          "+get",
          "~*"
        ])

      ConnAuth.broadcast_acl_invalidation("tx_user")
      Process.sleep(50)

      send_cmd(sock, ["EXEC"])
      assert {:error, "NOPERM" <> _} = recv_response(sock)

      :ok =
        Acl.set_user("tx_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+get",
          "~*"
        ])

      ConnAuth.broadcast_acl_invalidation("tx_user")
      Process.sleep(50)

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "EXEC uses fresh ACL even before invalidation message is processed", %{port: port} do
      key = "acl:tx:fresh:#{System.unique_integer([:positive])}"

      :ok =
        Acl.set_user("tx_fresh_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+multi",
          "+exec",
          "+discard",
          "+get",
          "+set",
          "~*"
        ])

      {sock, _} = connect_and_auth(port, "tx_fresh_user", "txpass")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "must_not_write"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      :ok =
        Acl.set_user("tx_fresh_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+multi",
          "+exec",
          "+discard",
          "+get",
          "~*"
        ])

      send_cmd(sock, ["EXEC"])
      assert {:error, "NOPERM" <> _} = recv_response(sock)

      :ok =
        Acl.set_user("tx_fresh_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+get",
          "~*"
        ])

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "ACL denial inside MULTI aborts the whole transaction", %{port: port} do
      key = "acl:tx:queue-denied:#{System.unique_integer([:positive])}"

      :ok =
        Acl.set_user("tx_queue_denied_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+multi",
          "+exec",
          "+discard",
          "+get",
          "+set",
          "~*"
        ])

      {sock, _} = connect_and_auth(port, "tx_queue_denied_user", "txpass")

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", key, "must_not_write"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      send_cmd(sock, ["DEL", key])
      assert {:error, "NOPERM" <> _} = recv_response(sock)

      send_cmd(sock, ["EXEC"])
      assert {:error, "EXECABORT" <> _} = recv_response(sock)

      :ok =
        Acl.set_user("tx_queue_denied_user", [
          "on",
          ">txpass",
          "-@all",
          "+auth",
          "+hello",
          "+get",
          "~*"
        ])

      send_cmd(sock, ["GET", key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "WATCH requires read access to watched keys, not write access", %{port: port} do
      key = "tenant:watch:#{System.unique_integer([:positive])}"

      :ok =
        Acl.set_user("watch_reader_user", [
          "on",
          ">watchpass",
          "-@all",
          "+auth",
          "+hello",
          "+watch",
          "%R~tenant:watch:*"
        ])

      {sock, _} = connect_and_auth(port, "watch_reader_user", "watchpass")

      send_cmd(sock, ["WATCH", key])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "EXEC rechecks WATCH permissions after ACL changes", %{port: port} do
      watched_key = "tenant:watch:#{System.unique_integer([:positive])}"
      write_key = "tenant:tx:#{System.unique_integer([:positive])}"

      :ok =
        Acl.set_user("watch_reauth_user", [
          "on",
          ">watchpass",
          "-@all",
          "+auth",
          "+hello",
          "+watch",
          "+multi",
          "+exec",
          "+discard",
          "+set",
          "+get",
          "%R~tenant:watch:*",
          "%W~tenant:tx:*",
          "%R~tenant:tx:*"
        ])

      {sock, _} = connect_and_auth(port, "watch_reauth_user", "watchpass")

      send_cmd(sock, ["WATCH", watched_key])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["MULTI"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", write_key, "must_not_commit"])
      assert recv_response(sock) == {:simple, "QUEUED"}

      :ok =
        Acl.set_user("watch_reauth_user", [
          "on",
          ">watchpass",
          "-@all",
          "+auth",
          "+hello",
          "+watch",
          "+multi",
          "+exec",
          "+discard",
          "+set",
          "+get",
          "resetkeys",
          "%R~tenant:other:*",
          "%W~tenant:tx:*",
          "%R~tenant:tx:*"
        ])

      send_cmd(sock, ["EXEC"])
      assert {:error, "NOPERM" <> _} = recv_response(sock)

      send_cmd(sock, ["GET", write_key])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "user with -@all gets NOPERM on every command over TCP", %{port: port} do
      :ok = Acl.set_user("blocked", ["on", ">blockedpass", "-@all", "+auth", "+hello"])

      {sock, _} = connect_and_auth(port, "blocked", "blockedpass")

      send_cmd(sock, ["GET", "key"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      send_cmd(sock, ["SET", "key", "val"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      send_cmd(sock, ["PING"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      :gen_tcp.close(sock)
    end

    test "default user with +@all can run any command over TCP", %{port: port} do
      :ok = Acl.set_user("default", [">testpass"])

      {sock, _} = connect_and_auth(port, "default", "testpass")

      send_cmd(sock, ["SET", "k", "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "k"])
      assert recv_response(sock) == "v"

      send_cmd(sock, ["DEL", "k"])
      assert recv_response(sock) == 1

      :gen_tcp.close(sock)
    end

    test "AUTH/HELLO/QUIT/RESET always bypass ACL check", %{port: port} do
      :ok = Acl.set_user("minimal", ["on", ">minpass", "-@all"])

      {sock, resp} = connect_and_auth(port, "minimal", "minpass")
      assert resp == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "disabled authenticated user cannot continue using ACL WHOAMI", %{port: port} do
      :ok =
        Acl.set_user("whoami_disabled", [
          "on",
          ">pass",
          "-@all",
          "+auth",
          "+hello",
          "+acl|whoami"
        ])

      {sock, resp} = connect_and_auth(port, "whoami_disabled", "pass")
      assert resp == {:simple, "OK"}

      send_cmd(sock, ["ACL", "WHOAMI"])
      assert recv_response(sock) == "whoami_disabled"

      :ok =
        Acl.set_user("whoami_disabled", [
          "off",
          ">pass",
          "-@all",
          "+auth",
          "+hello",
          "+acl|whoami"
        ])

      ConnAuth.broadcast_acl_invalidation("whoami_disabled")
      Process.sleep(50)

      send_cmd(sock, ["ACL", "WHOAMI"])
      assert {:error, "NOPERM" <> _} = recv_response(sock)

      :gen_tcp.close(sock)
    end
  end

  describe "TCP: no-auth ACL enforcement" do
    test "default user password requires AUTH even when requirepass is empty", %{port: port} do
      :ok = Acl.set_user("default", ["on", ">testpass", "+@all", "~*"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "acl:default-password"])
      assert match?({:error, "NOAUTH" <> _}, recv_response(sock))

      send_cmd(sock, ["AUTH", "default", "testpass"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      :gen_tcp.close(sock)
    end

    test "default user denied commands are enforced without requirepass", %{port: port} do
      :ok = Acl.set_user("default", ["on", "nopass", "+@all", "~*", "-ping"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      :gen_tcp.close(sock)
    end

    test "disabled default user is enforced without requirepass", %{port: port} do
      :ok = Acl.set_user("default", ["off", "nopass", "+@all", "~*"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      :gen_tcp.close(sock)
    end
  end

  describe "Flow partition ACL key extraction" do
    test "explicit partition keys are enforced for single-flow commands" do
      :ok = Acl.set_user("flow_tenant_a", ["on", ">pass", "+@all", "~tenant-a"])
      cache = ConnAuth.build_acl_cache("flow_tenant_a")

      assert {"FLOW.GET", ["tenant-a"]} =
               parsed_command_keys("flow.get flow-1 PARTITION tenant-a\r\n")

      assert :ok = ConnAuth.check_keys_cached(cache, "FLOW.GET", ["tenant-a"])

      assert {"FLOW.GET", ["tenant-b"]} =
               parsed_command_keys("flow.get flow-1 PARTITION tenant-b\r\n")

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "FLOW.GET", ["tenant-b"])
    end

    test "unpartitioned Flow commands fall back to their visible id or type" do
      assert {"FLOW.GET", ["flow-1"]} = parsed_command_keys("flow.get flow-1\r\n")
      assert {"FLOW.LIST", ["checkout"]} = parsed_command_keys("flow.list checkout\r\n")
    end

    test "explicit partition keys are enforced for Flow index commands" do
      :ok = Acl.set_user("flow_tenant_a", ["on", ">pass", "+@all", "~tenant-a"])
      cache = ConnAuth.build_acl_cache("flow_tenant_a")

      assert {"FLOW.CLAIM_DUE", ["tenant-a"]} =
               parsed_command_keys(
                 "flow.claim_due checkout WORKER worker-a PARTITION tenant-a\r\n"
               )

      assert :ok = ConnAuth.check_keys_cached(cache, "FLOW.CLAIM_DUE", ["tenant-a"])

      assert {"FLOW.INFO", ["tenant-b"]} =
               parsed_command_keys("flow.info checkout PARTITION tenant-b\r\n")

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "FLOW.INFO", ["tenant-b"])
    end
  end

  describe "PubSub channel ACL key extraction" do
    test "pubsub channels and patterns are enforced as ACL keys" do
      :ok = Acl.set_user("tenant_pubsub", ["on", ">pass", "+@pubsub", "~tenant:*"])
      cache = ConnAuth.build_acl_cache("tenant_pubsub")

      assert {"PUBLISH", ["tenant:a"]} = parsed_command_keys("publish tenant:a msg\r\n")
      assert :ok = ConnAuth.check_keys_cached(cache, "PUBLISH", ["tenant:a"])

      assert {"PUBLISH", ["other:a"]} = parsed_command_keys("publish other:a msg\r\n")

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "PUBLISH", ["other:a"])

      assert {"SUBSCRIBE", ["tenant:a", "tenant:b"]} =
               parsed_command_keys("subscribe tenant:a tenant:b\r\n")

      assert {"PSUBSCRIBE", ["tenant:*"]} = parsed_command_keys("psubscribe tenant:*\r\n")
      assert {"PUBSUB", ["tenant:a"]} = parsed_command_keys("pubsub numsub tenant:a\r\n")
    end
  end

  describe "mixed key access ACL enforcement" do
    test "BITOP requires write on destination and read on sources" do
      :ok =
        Acl.set_user("bitop_mixed_access", [
          "on",
          ">pass",
          "+@all",
          "%W~tenant:dst:*",
          "%R~tenant:src:*"
        ])

      cache = ConnAuth.build_acl_cache("bitop_mixed_access")

      assert {"BITOP", ["tenant:dst:bits", "tenant:src:a", "tenant:src:b"]} =
               parsed_command_keys("bitop AND tenant:dst:bits tenant:src:a tenant:src:b\r\n")

      assert :ok =
               ConnAuth.check_keys_cached(cache, "BITOP", [
                 "tenant:dst:bits",
                 "tenant:src:a",
                 "tenant:src:b"
               ])

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "BITOP", [
                 "tenant:src:not-destination",
                 "tenant:src:a"
               ])

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "BITOP", [
                 "tenant:dst:bits",
                 "tenant:dst:not-readable-source"
               ])
    end

    test "copy and store commands require read sources and write destination" do
      :ok =
        Acl.set_user("store_mixed_access", [
          "on",
          ">pass",
          "+@all",
          "%W~tenant:dst:*",
          "%R~tenant:src:*"
        ])

      cache = ConnAuth.build_acl_cache("store_mixed_access")

      for {command, wire, keys} <- [
            {"COPY", "copy tenant:src:a tenant:dst:a\r\n", ["tenant:src:a", "tenant:dst:a"]},
            {"PFMERGE", "pfmerge tenant:dst:h tenant:src:h1 tenant:src:h2\r\n",
             ["tenant:dst:h", "tenant:src:h1", "tenant:src:h2"]},
            {"SDIFFSTORE", "sdiffstore tenant:dst:s tenant:src:s1 tenant:src:s2\r\n",
             ["tenant:dst:s", "tenant:src:s1", "tenant:src:s2"]},
            {"SINTERSTORE", "sinterstore tenant:dst:s tenant:src:s1 tenant:src:s2\r\n",
             ["tenant:dst:s", "tenant:src:s1", "tenant:src:s2"]},
            {"SUNIONSTORE", "sunionstore tenant:dst:s tenant:src:s1 tenant:src:s2\r\n",
             ["tenant:dst:s", "tenant:src:s1", "tenant:src:s2"]},
            {"GEOSEARCHSTORE",
             "geosearchstore tenant:dst:g tenant:src:g FROMMEMBER Palermo BYRADIUS 10 km\r\n",
             ["tenant:dst:g", "tenant:src:g"]}
          ] do
        assert {^command, ^keys} = parsed_command_keys(wire)
        assert :ok = ConnAuth.check_keys_cached(cache, command, keys)
      end

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "COPY", [
                 "tenant:dst:not-readable",
                 "tenant:dst:a"
               ])

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "PFMERGE", [
                 "tenant:src:not-writable",
                 "tenant:src:h1"
               ])
    end

    test "move commands require read/write source and write destination" do
      :ok =
        Acl.set_user("move_mixed_access", [
          "on",
          ">pass",
          "+@all",
          "%R~tenant:rw-src:*",
          "%W~tenant:rw-src:*",
          "%R~tenant:read-only-src:*",
          "%W~tenant:dst:*"
        ])

      cache = ConnAuth.build_acl_cache("move_mixed_access")

      for {command, wire, keys} <- [
            {"RENAME", "rename tenant:rw-src:a tenant:dst:a\r\n",
             ["tenant:rw-src:a", "tenant:dst:a"]},
            {"RENAMENX", "renamenx tenant:rw-src:a tenant:dst:a\r\n",
             ["tenant:rw-src:a", "tenant:dst:a"]},
            {"LMOVE", "lmove tenant:rw-src:list tenant:dst:list LEFT RIGHT\r\n",
             ["tenant:rw-src:list", "tenant:dst:list"]},
            {"BLMOVE", "blmove tenant:rw-src:list tenant:dst:list LEFT RIGHT 1\r\n",
             ["tenant:rw-src:list", "tenant:dst:list"]},
            {"RPOPLPUSH", "rpoplpush tenant:rw-src:list tenant:dst:list\r\n",
             ["tenant:rw-src:list", "tenant:dst:list"]},
            {"SMOVE", "smove tenant:rw-src:set tenant:dst:set member\r\n",
             ["tenant:rw-src:set", "tenant:dst:set"]}
          ] do
        assert {^command, ^keys} = parsed_command_keys(wire)
        assert :ok = ConnAuth.check_keys_cached(cache, command, keys)
      end

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "RENAME", [
                 "tenant:read-only-src:a",
                 "tenant:dst:a"
               ])

      assert {:error, "NOPERM" <> _} =
               ConnAuth.check_keys_cached(cache, "SMOVE", [
                 "tenant:rw-src:set",
                 "tenant:read-only-src:dest"
               ])
    end

    test "value-returning mutation commands require read and write on mutated keys" do
      :ok =
        Acl.set_user("pop_write_only", [
          "on",
          ">pass",
          "+@all",
          "%W~tenant:pop:*"
        ])

      :ok =
        Acl.set_user("pop_read_write", [
          "on",
          ">pass",
          "+@all",
          "%R~tenant:pop:*",
          "%W~tenant:pop:*"
        ])

      write_only = ConnAuth.build_acl_cache("pop_write_only")
      read_write = ConnAuth.build_acl_cache("pop_read_write")

      for {command, keys} <- [
            {"LPOP", ["tenant:pop:list"]},
            {"RPOP", ["tenant:pop:list"]},
            {"BLPOP", ["tenant:pop:list"]},
            {"BRPOP", ["tenant:pop:list"]},
            {"BLMPOP", ["tenant:pop:list"]},
            {"SPOP", ["tenant:pop:set"]},
            {"ZPOPMIN", ["tenant:pop:zset"]},
            {"ZPOPMAX", ["tenant:pop:zset"]},
            {"HGETDEL", ["tenant:pop:hash"]},
            {"HGETEX", ["tenant:pop:hash"]},
            {"XREADGROUP", ["tenant:pop:stream"]}
          ] do
        assert {:error, "NOPERM" <> _} = ConnAuth.check_keys_cached(write_only, command, keys)
        assert :ok = ConnAuth.check_keys_cached(read_write, command, keys)
      end
    end
  end

  describe "TCP: global keyspace enumeration with restricted key patterns" do
    test "global keyspace enumeration commands are denied for restricted users", %{port: port} do
      :ok = Acl.set_user("tenant_reader", ["on", ">pass", "+@read", "~tenant-a:*"])

      {sock, resp} = connect_and_auth(port, "tenant_reader", "pass")
      assert resp == {:simple, "OK"}

      for cmd <- [["KEYS", "*"], ["SCAN", "0"], ["RANDOMKEY"], ["DBSIZE"]] do
        send_cmd(sock, cmd)
        assert match?({:error, "NOPERM" <> _}, recv_response(sock))
      end

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge case: AUTH changes permissions mid-connection
  # ---------------------------------------------------------------------------

  describe "TCP: AUTH changes permissions mid-connection" do
    setup do
      enable_requirepass()
      :ok
    end

    test "switching user via AUTH changes command permissions", %{port: port} do
      :ok =
        Acl.set_user("reader", [
          "on",
          ">readpass",
          "-@all",
          "+get",
          "+auth",
          "+hello",
          "+command",
          "~*"
        ])

      :ok =
        Acl.set_user("writer", [
          "on",
          ">writepass",
          "-@all",
          "+set",
          "+auth",
          "+hello",
          "+command",
          "~*"
        ])

      {sock, _} = connect_and_auth(port, "reader", "readpass")

      send_cmd(sock, ["GET", "k"])
      _resp = recv_response(sock)

      send_cmd(sock, ["SET", "k", "v"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      send_cmd(sock, ["AUTH", "writer", "writepass"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["SET", "k", "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "k"])
      assert match?({:error, "NOPERM" <> _}, recv_response(sock))

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Stress test: 1000 commands with permission checking
  # ---------------------------------------------------------------------------

  describe "stress: permission checking under load" do
    setup do
      enable_requirepass()
      :ok
    end

    test "1000 sequential commands with permission check do not degrade", %{port: port} do
      :ok =
        Acl.set_user("stressuser", [
          "on",
          ">stresspass",
          "-@all",
          "+get",
          "+set",
          "+auth",
          "+hello",
          "+del",
          "+command",
          "~*"
        ])

      {sock, _} = connect_and_auth(port, "stressuser", "stresspass")

      start_time = System.monotonic_time(:millisecond)

      for i <- 1..500 do
        key = "stress_key_#{i}"
        send_cmd(sock, ["SET", key, "value_#{i}"])
        assert recv_response(sock) == {:simple, "OK"}
      end

      for i <- 1..500 do
        key = "stress_key_#{i}"
        send_cmd(sock, ["GET", key])
        assert recv_response(sock) == "value_#{i}"
      end

      elapsed = System.monotonic_time(:millisecond) - start_time

      assert elapsed < 30_000,
             "1000 commands with ACL checks took #{elapsed}ms, expected < 30000ms"

      :gen_tcp.close(sock)
    end

    test "1000 denied commands return NOPERM without crashing", %{port: port} do
      :ok = Acl.set_user("denied", ["on", ">deniedpass", "-@all", "+auth", "+hello"])

      {sock, _} = connect_and_auth(port, "denied", "deniedpass")

      for _i <- 1..1000 do
        send_cmd(sock, ["SET", "k", "v"])
        assert match?({:error, "NOPERM" <> _}, recv_response(sock))
      end

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # Unit tests: category definitions
  # ---------------------------------------------------------------------------

  describe "category membership" do
    test "ACL categories cover every RESP command known by the parser" do
      parser_commands = parser_supported_commands()
      acl_commands = FerricstoreServer.Acl.CommandCategories.acl_supported_commands()

      assert MapSet.difference(parser_commands, acl_commands) == MapSet.new()
    end

    test "@flow category covers every FLOW command known by the parser" do
      parser_flow_commands =
        parser_supported_commands()
        |> Enum.filter(&String.starts_with?(&1, "FLOW."))
        |> MapSet.new()

      {:ok, acl_flow_commands} = FerricstoreServer.Acl.CommandCategories.category_commands("FLOW")

      assert MapSet.difference(parser_flow_commands, acl_flow_commands) == MapSet.new()
    end

    test "-@write denies every mutating command family, including newer modules" do
      :ok = Acl.set_user("no_writes", ["on", ">pass", "+@all", "-@write"])

      denied =
        ~w(
          SET GETSET GETDEL GETEX HSET HGETDEL HGETEX LPUSH BLPOP SADD ZADD XGROUP XACK
          JSON.SET BF.ADD CF.ADD CMS.INCRBY TOPK.ADD TDIGEST.ADD FLOW.CREATE
          FLOW.SPAWN_CHILDREN RATELIMIT.ADD FETCH_OR_COMPUTE
        )

      for cmd <- denied do
        assert {:error, _} = Acl.check_command("no_writes", cmd),
               "-@write should deny #{cmd}"
      end
    end

    test "-@dangerous denies broad destructive maintenance commands" do
      :ok = Acl.set_user("no_danger", ["on", ">pass", "+@all", "-@dangerous"])

      for cmd <- ~w(FLUSHDB CLUSTER.JOIN FLOW.RETENTION_CLEANUP) do
        assert {:error, _} = Acl.check_command("no_danger", cmd),
               "-@dangerous should deny #{cmd}"
      end
    end

    test "key access classification covers newer read and write command families" do
      read_cmds =
        ~w(
          BF.EXISTS CF.EXISTS CMS.QUERY TOPK.QUERY TDIGEST.INFO FLOW.GET FLOW.HISTORY
          FLOW.TERMINALS FLOW.FAILURES ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE
          HTTL HPTTL HEXPIRETIME
        )

      write_cmds =
        ~w(
          BF.ADD CF.DEL CMS.MERGE TOPK.INCRBY TDIGEST.MERGE FLOW.COMPLETE
          FLOW.TRANSITION_MANY FLOW.RETENTION_CLEANUP HEXPIRE HSETEX
        )

      read_write_cmds =
        ~w(
          GETEX GETDEL GETSET HGETEX HGETDEL CAS
          LPOP RPOP BLPOP BRPOP BLMPOP SPOP ZPOPMIN ZPOPMAX XREADGROUP
        )

      for cmd <- read_cmds do
        assert FerricstoreServer.Connection.Auth.command_access_type(cmd) == :read
      end

      for cmd <- write_cmds do
        assert FerricstoreServer.Connection.Auth.command_access_type(cmd) == :write
      end

      for cmd <- read_write_cmds do
        assert FerricstoreServer.Connection.Auth.command_access_type(cmd) == :rw
      end
    end

    test "@read category includes standard read commands" do
      :ok = Acl.set_user("cat_reader", ["on", ">pass", "-@all", "+@read"])

      read_cmds = ~w(GET MGET HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS
                      LRANGE LLEN LINDEX SMEMBERS SISMEMBER SCARD SRANDMEMBER
                      ZSCORE ZRANK ZRANGE ZCARD ZCOUNT EXISTS TTL PTTL TYPE
                      STRLEN GETRANGE DBSIZE)

      for cmd <- read_cmds do
        assert :ok = Acl.check_command("cat_reader", cmd),
               "@read should include #{cmd}"
      end
    end

    test "@write category includes standard write commands" do
      :ok = Acl.set_user("cat_writer", ["on", ">pass", "-@all", "+@write"])

      write_cmds = ~w(SET DEL HSET HDEL LPUSH RPUSH LPOP RPOP SADD SREM
                       ZADD ZREM INCR DECR INCRBY DECRBY APPEND
                       EXPIRE PERSIST SETRANGE SETNX MSET)

      for cmd <- write_cmds do
        assert :ok = Acl.check_command("cat_writer", cmd),
               "@write should include #{cmd}"
      end
    end

    test "@admin category includes admin/server commands" do
      :ok = Acl.set_user("cat_admin", ["on", ">pass", "-@all", "+@admin"])

      admin_cmds = ~w(CONFIG ACL DEBUG SLOWLOG SAVE BGSAVE FLUSHDB FLUSHALL INFO COMMAND)

      for cmd <- admin_cmds do
        assert :ok = Acl.check_command("cat_admin", cmd),
               "@admin should include #{cmd}"
      end
    end

    test "@dangerous category includes dangerous commands" do
      :ok = Acl.set_user("cat_danger", ["on", ">pass", "-@all", "+@dangerous"])

      dangerous_cmds = ~w(FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN CLIENT.KILL)

      for cmd <- dangerous_cmds do
        assert :ok = Acl.check_command("cat_danger", cmd),
               "@dangerous should include #{cmd}"
      end
    end

    test "@connection does not grant destructive CLIENT subcommands" do
      :ok = Acl.set_user("cat_connection", ["on", ">pass", "-@all", "+@connection"])

      assert :ok = Acl.check_command("cat_connection", "CLIENT.ID")
      assert :ok = Acl.check_command("cat_connection", "CLIENT.SETNAME")
      assert {:error, _} = Acl.check_command("cat_connection", "CLIENT.KILL")
      assert {:error, _} = Acl.check_command("cat_connection", "CLIENT.LIST")

      assert {:error, _} =
               ConnAuth.acl_command_name("CLIENT", ["KILL", "ID", "1"], {:client, "KILL", []})
               |> then(&Acl.check_command("cat_connection", &1))
    end
  end

  # ---------------------------------------------------------------------------
  # NOPERM error message format
  # ---------------------------------------------------------------------------

  describe "NOPERM error message format" do
    test "includes the command name in lowercase" do
      :ok = Acl.set_user("nope", ["on", ">pass", "-@all"])

      {:error, msg} = Acl.check_command("nope", "FLUSHDB")
      assert msg =~ "NOPERM"
      assert msg =~ "flushdb"
      assert msg =~ "no permissions to run"
    end

    test "includes the command name for various commands" do
      :ok = Acl.set_user("nope", ["on", ">pass", "-@all"])

      for cmd <- ~w(GET SET DEL HSET CONFIG ACL) do
        {:error, msg} = Acl.check_command("nope", cmd)
        assert msg =~ String.downcase(cmd)
      end
    end
  end
end
