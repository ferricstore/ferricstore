defmodule FerricstoreServer.Spec.AclPermissionsTest.Sections.Part02 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Acl
      alias FerricstoreServer.Connection.Auth, as: ConnAuth
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener

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
  end
end
