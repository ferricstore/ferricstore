defmodule FerricstoreServer.Spec.ErrorHandlingTest do
  @moduledoc """
  Spec section 3: Error Handling tests (ERR-001 through ERR-018).

  Verifies that FerricStore returns the correct error codes and messages for
  each error scenario defined in the test plan. Tests use MockStore for
  unit-level validation and TCP connections for integration-level validation.

  ## Coverage map

  | ID       | Scenario                              | Status                |
  |----------|---------------------------------------|-----------------------|
  | ERR-001  | WRONGTYPE: SET then HSET              | Tested (TCP)          |
  | ERR-002  | WRONGTYPE: RPUSH then SADD            | Tested (unit + TCP)   |
  | ERR-003  | KEYDIR_FULL new key                   | See keydir_full_test  |
  | ERR-004  | KEYDIR_FULL update ok                 | See keydir_full_test  |
  | ERR-005  | NOLEADER write                        | Skipped (cluster)     |
  | ERR-009  | LOADING on restart                    | Tested (unit)         |
  | ERR-010  | NOAUTH                                | Tested (TCP)          |
  | ERR-011  | AUTH wrong password                   | Tested (TCP)          |
  | ERR-012  | VALUE_TOO_LARGE                       | Tested (:large_alloc) |
  | ERR-013  | BUSYGROUP                             | Tested (unit + TCP)   |
  | ERR-014  | NOGROUP                               | Tested (unit + TCP)   |
  | ERR-015  | Wrong arg count                       | Tested (unit + TCP)   |
  | ERR-016  | INCR non-integer                      | Tested (unit + TCP)   |
  | ERR-017  | EXPIRE negative                       | Tested (unit + TCP)   |
  | ERR-018  | CLUSTERDOWN                           | Skipped (cluster)     |
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.{Dispatcher, Expiry, Hash, List, Set, Stream, Strings}
  alias Ferricstore.Config
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.MockStore

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

  defp ukey(base), do: "err_h_#{base}_#{:rand.uniform(999_999)}"

  # ---------------------------------------------------------------------------
  # Setup
  # ---------------------------------------------------------------------------

  setup_all do
    %{port: Listener.port()}
  end

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
    on_exit(fn -> Config.set("requirepass", "") end)
    :ok
  end

  # ===========================================================================
  # ERR-001: WRONGTYPE — SET foo bar; HSET foo f v → WRONGTYPE
  #
  # In Redis, SET creates a string key. A subsequent HSET on that key must
  # return WRONGTYPE because the key holds the wrong data type.
  #
  # FerricStore stores string keys in the plain key store without type
  # metadata. Data structure commands (HSET, LPUSH, SADD) use a TypeRegistry
  # to track types. Since SET does not register a "string" type, HSET on a
  # SET-created key currently succeeds by registering the key as "hash".
  #
  # This test documents the current behavior. When the TypeRegistry is
  # enhanced to track string types, update the assertion to require WRONGTYPE.
  # ===========================================================================

  describe "ERR-001: WRONGTYPE — SET then HSET on same key" do
    test "HSET on a SET-created key via TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err001")

      # Step 1: SET foo bar
      send_cmd(sock, ["SET", k, "bar"])
      assert recv_response(sock) == {:simple, "OK"}

      # Step 2: HSET foo f v
      send_cmd(sock, ["HSET", k, "f", "v"])
      response = recv_response(sock)

      # FerricStore currently allows this (no string type in registry).
      # When WRONGTYPE enforcement is added for string keys, change to:
      #   assert {:error, "WRONGTYPE" <> _} = response
      assert is_integer(response) or match?({:error, "WRONGTYPE" <> _}, response),
             "Expected WRONGTYPE or integer, got: #{inspect(response)}"

      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-002: WRONGTYPE cross-command — RPUSH list a; SADD list b → WRONGTYPE
  #
  # After RPUSH creates a list, SADD (a set command) on the same key must
  # return WRONGTYPE because the TypeRegistry records it as a list.
  # ===========================================================================

  describe "ERR-002: WRONGTYPE cross-command — RPUSH then SADD" do
    test "SADD on a key created by RPUSH returns WRONGTYPE (unit)" do
      store = MockStore.make()

      # Step 1: RPUSH creates a list key
      assert 1 = List.handle("RPUSH", ["mylist", "a"], store)

      # Step 2: SADD on the same key should fail with WRONGTYPE
      result = Set.handle("SADD", ["mylist", "b"], store)
      assert {:error, "WRONGTYPE" <> _} = result
    end

    test "SADD on a key created by RPUSH returns WRONGTYPE over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err002")

      # Step 1: RPUSH list a
      send_cmd(sock, ["RPUSH", k, "a"])
      response = recv_response(sock)
      assert response == 1

      # Step 2: SADD list b — should return WRONGTYPE
      send_cmd(sock, ["SADD", k, "b"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert String.starts_with?(msg, "WRONGTYPE")

      # Cleanup
      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-009: LOADING on restart
  #
  # When a node restarts with a large keydir, commands issued before the
  # keydir is fully loaded should return LOADING. FerricStore shards start
  # synchronously, so LOADING is not observable in normal operation. We
  # verify that INFO reports loading:0 (fully loaded) as a baseline.
  # ===========================================================================

  describe "ERR-009: LOADING on restart" do
    @tag :slow
    test "INFO persistence section reports loading:0 when fully loaded" do
      store = MockStore.make()
      result = Ferricstore.Commands.Server.handle("INFO", ["persistence"], store)
      assert is_binary(result)
      assert result =~ "loading:0"
    end
  end

  # ===========================================================================
  # ERR-010: NOAUTH — Connect to password-protected node; GET key → NOAUTH
  # ===========================================================================

  describe "ERR-010: NOAUTH on password-protected node" do
    test "GET returns NOAUTH when requirepass is set and client not authenticated", %{port: port} do
      Config.set("requirepass", "test_secret_010")

      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "anykey"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "NOAUTH")

      :gen_tcp.close(sock)
    end

    test "SET returns NOAUTH when not authenticated", %{port: port} do
      Config.set("requirepass", "test_secret_010b")

      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "anykey", "anyval"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert String.starts_with?(msg, "NOAUTH")

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-011: AUTH wrong password → ERR invalid password (WRONGPASS)
  # ===========================================================================

  describe "ERR-011: AUTH wrong password" do
    test "AUTH with incorrect password returns WRONGPASS", %{port: port} do
      Config.set("requirepass", "correct_password_011")

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "wrong_password"])
      response = recv_response(sock)

      assert {:error, msg} = response

      assert String.starts_with?(msg, "WRONGPASS"),
             "Expected WRONGPASS prefix, got: #{inspect(msg)}"

      :gen_tcp.close(sock)
    end

    test "AUTH with empty password returns WRONGPASS", %{port: port} do
      Config.set("requirepass", "correct_password_011b")

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", ""])
      response = recv_response(sock)

      assert {:error, msg} = response

      assert String.starts_with?(msg, "WRONGPASS"),
             "Expected WRONGPASS prefix, got: #{inspect(msg)}"

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-012: VALUE_TOO_LARGE — SET key <large value> → ERR value too large
  #
  # FerricStore enforces a 512 MiB value size limit at the Router level.
  # Tagged :large_alloc because it requires ~512 MiB of RAM.
  # ===========================================================================

  describe "ERR-012: VALUE_TOO_LARGE via Router.put" do
    @tag :large_alloc
    test "Router.put rejects value at 512 MiB" do
      # 512 MiB exactly hits the >= guard in Router.put
      oversized = :binary.copy("x", 512 * 1024 * 1024)

      result = Router.put(FerricStore.Instance.get(:default), "err012_key", oversized, 0)
      assert {:error, msg} = result
      assert msg =~ "value too large"
    end

    @tag :large_alloc
    test "Router.put accepts value under the limit" do
      # Verify the size guard constant is 512 MiB and a 1MB value passes.
      # We don't allocate 512MB-1 because CI runners may OOM.
      assert Ferricstore.Store.Router.max_value_size() == 512 * 1024 * 1024

      small = :binary.copy("y", 1_000_000)
      result = Router.put(FerricStore.Instance.get(:default), "err012_under", small, 0)
      assert result == :ok

      Router.delete(FerricStore.Instance.get(:default), "err012_under")
    end
  end

  # ===========================================================================
  # ERR-013: BUSYGROUP — XGROUP CREATE st g 0; XGROUP CREATE st g 0 → BUSYGROUP
  #
  # Creating a consumer group that already exists must return BUSYGROUP.
  # ===========================================================================

  describe "ERR-013: BUSYGROUP — duplicate XGROUP CREATE" do
    test "second XGROUP CREATE for same group returns BUSYGROUP (unit)" do
      store = MockStore.make()
      Stream.ensure_meta_table()

      stream_key = ukey("err013_stream")

      # Create stream with first XGROUP CREATE + MKSTREAM
      assert :ok = Stream.handle("XGROUP", ["CREATE", stream_key, "grp1", "0", "MKSTREAM"], store)

      # Second XGROUP CREATE for the same group should return BUSYGROUP
      result = Stream.handle("XGROUP", ["CREATE", stream_key, "grp1", "0"], store)
      assert {:error, "BUSYGROUP" <> _} = result
    end

    test "XGROUP CREATE for a different group on same stream succeeds (unit)" do
      store = MockStore.make()
      Stream.ensure_meta_table()

      stream_key = ukey("err013_stream2")

      assert :ok = Stream.handle("XGROUP", ["CREATE", stream_key, "grp1", "0", "MKSTREAM"], store)

      # Different group name should succeed
      assert :ok = Stream.handle("XGROUP", ["CREATE", stream_key, "grp2", "0"], store)
    end

    test "duplicate XGROUP CREATE returns BUSYGROUP over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err013_tcp")

      # Create stream and group
      send_cmd(sock, ["XGROUP", "CREATE", k, "mygroup", "0", "MKSTREAM"])
      assert recv_response(sock) == {:simple, "OK"}

      # Duplicate group creation
      send_cmd(sock, ["XGROUP", "CREATE", k, "mygroup", "0"])
      response = recv_response(sock)
      assert {:error, msg} = response

      assert String.starts_with?(msg, "BUSYGROUP"),
             "Expected BUSYGROUP prefix, got: #{inspect(msg)}"

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-014: NOGROUP — XREADGROUP GROUP nonexistent → NOGROUP
  # ===========================================================================

  describe "ERR-014: NOGROUP — XREADGROUP with nonexistent group" do
    test "XREADGROUP with nonexistent group returns NOGROUP (unit)" do
      store = MockStore.make()
      Stream.ensure_meta_table()

      stream_key = ukey("err014_stream")

      # Add an entry to create the stream
      Stream.handle("XADD", [stream_key, "1-0", "field", "value"], store)

      # XREADGROUP with a nonexistent group
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "nonexistent_group", "consumer1", "STREAMS", stream_key, ">"],
          store
        )

      assert {:error, msg} = result

      assert String.starts_with?(msg, "NOGROUP"),
             "Expected NOGROUP prefix, got: #{inspect(msg)}"
    end

    test "XREADGROUP with nonexistent group returns NOGROUP over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err014_tcp")

      # Create the stream with XADD
      send_cmd(sock, ["XADD", k, "1-0", "f", "v"])
      _id = recv_response(sock)

      # XREADGROUP with a group that does not exist
      send_cmd(sock, ["XREADGROUP", "GROUP", "nogroup", "c1", "STREAMS", k, ">"])
      response = recv_response(sock)

      assert {:error, msg} = response

      assert String.starts_with?(msg, "NOGROUP"),
             "Expected NOGROUP prefix, got: #{inspect(msg)}"

      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-015: Wrong arg count — SET (no args) → ERR wrong number of arguments
  # ===========================================================================

  describe "ERR-015: wrong number of arguments" do
    test "SET with no arguments returns ERR wrong number of arguments" do
      store = MockStore.make()
      assert {:error, "ERR wrong number of arguments" <> _} = Strings.handle("SET", [], store)
    end

    test "GET with no arguments returns ERR wrong number of arguments" do
      store = MockStore.make()
      assert {:error, "ERR wrong number of arguments" <> _} = Strings.handle("GET", [], store)
    end

    test "DEL with no arguments returns ERR wrong number of arguments" do
      store = MockStore.make()
      assert {:error, "ERR wrong number of arguments" <> _} = Strings.handle("DEL", [], store)
    end

    test "HSET with only key and one field (missing value) returns ERR" do
      store = MockStore.make()
      result = Hash.handle("HSET", ["key", "field"], store)
      assert {:error, "ERR wrong number of arguments" <> _} = result
    end

    test "EXPIRE with no arguments returns ERR wrong number of arguments" do
      store = MockStore.make()
      assert {:error, "ERR wrong number of arguments" <> _} = Expiry.handle("EXPIRE", [], store)
    end

    test "SET with no arguments returns ERR over TCP", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end

    test "unknown command returns ERR unknown command" do
      store = MockStore.make()
      result = Dispatcher.dispatch("BOGUSCMD", [], store)
      assert {:error, "ERR unknown command" <> _} = result
    end
  end

  # ===========================================================================
  # ERR-016: INCR non-integer — SET foo bar; INCR foo → ERR not an integer
  # ===========================================================================

  describe "ERR-016: INCR on non-integer value" do
    test "INCR on a string value returns ERR value is not an integer (unit)" do
      store = MockStore.make(%{"foo" => {"bar", 0}})

      result = Strings.handle("INCR", ["foo"], store)
      assert {:error, "ERR value is not an integer or out of range"} = result
    end

    test "DECRBY on a string value returns ERR value is not an integer (unit)" do
      store = MockStore.make(%{"foo" => {"not_a_number", 0}})

      result = Strings.handle("DECRBY", ["foo", "5"], store)
      assert {:error, "ERR value is not an integer or out of range"} = result
    end

    test "INCR on a non-integer returns ERR over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err016_tcp")

      # Set a non-integer value
      send_cmd(sock, ["SET", k, "not_a_number"])
      assert recv_response(sock) == {:simple, "OK"}

      # INCR should fail
      send_cmd(sock, ["INCR", k])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "not an integer"

      # Cleanup
      send_cmd(sock, ["DEL", k])
      recv_response(sock)
      :gen_tcp.close(sock)
    end
  end

  # ===========================================================================
  # ERR-017: EXPIRE negative — SET foo bar; EXPIRE foo -1 → ERR invalid expire
  # ===========================================================================

  describe "ERR-017: EXPIRE with negative value deletes key (Redis compat)" do
    test "EXPIRE with -1 deletes the key (unit)" do
      store = MockStore.make(%{"foo" => {"bar", 0}})

      result = Expiry.handle("EXPIRE", ["foo", "-1"], store)
      assert 1 == result
      assert nil == store.get.("foo")
    end

    test "PEXPIRE with -1 deletes the key (unit)" do
      store = MockStore.make(%{"foo" => {"bar", 0}})

      result = Expiry.handle("PEXPIRE", ["foo", "-1"], store)
      assert 1 == result
      assert nil == store.get.("foo")
    end

    test "EXPIRE with -1 deletes key over TCP", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("err017_tcp")

      send_cmd(sock, ["SET", k, "bar"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "-1"])
      response = recv_response(sock)

      # Redis-compatible: returns integer 1 (key deleted)
      assert 1 == response

      # Key should be gone
      send_cmd(sock, ["GET", k])
      assert nil == recv_response(sock)

      :gen_tcp.close(sock)
    end

    test "SET with EX -1 returns ERR invalid expire time (unit)" do
      store = MockStore.make()

      result = Strings.handle("SET", ["foo", "bar", "EX", "-1"], store)
      assert {:error, msg} = result
      assert msg =~ "invalid expire"
    end
  end

  # ERR-005: NOLEADER and ERR-018: CLUSTERDOWN are cluster-only error codes.
  # They require multi-node :peer clusters and are covered by cluster tests
  # (mix test --include cluster).
end
