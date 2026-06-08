defmodule FerricstoreServer.Spec.CommandEdgeCasesComprehensiveTest.Sections.WrongtypeCrossTypeAccess do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.{Encoder, Parser}
      alias FerricstoreServer.Listener
      alias Ferricstore.Test.ShardHelpers

  describe "WRONGTYPE cross-type access" do
    test "LPUSH to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_list")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["LPUSH", k, "elem"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "HSET on a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_hash")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["HSET", k, "field", "value"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "SADD to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_set")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["SADD", k, "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "ZADD to a key that holds a string returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_str_zset")
      cmd(sock, ["SET", k, "string_value"])
      result = cmd(sock, ["ZADD", k, "1.0", "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_str")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a list key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_list_str")
      cmd(sock, ["LPUSH", k, "elem"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_set_str")
      cmd(sock, ["SADD", k, "member"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "GET on a sorted set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_zset_str")
      cmd(sock, ["ZADD", k, "1.0", "member"])
      result = cmd(sock, ["GET", k])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "HSET on a list key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_list_hash")
      cmd(sock, ["LPUSH", k, "elem"])
      result = cmd(sock, ["HSET", k, "field", "value"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "LPUSH on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_list")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["LPUSH", k, "elem"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "SADD on a hash key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_hash_set")
      cmd(sock, ["HSET", k, "field", "value"])
      result = cmd(sock, ["SADD", k, "member"])
      assert_error_contains(result, "WRONGTYPE")
    end

    test "ZADD on a set key returns WRONGTYPE", %{sock: sock} do
      k = ukey("wrongtype_set_zset")
      cmd(sock, ["SADD", k, "member"])
      result = cmd(sock, ["ZADD", k, "1.0", "member"])
      assert_error_contains(result, "WRONGTYPE")
    end
  end
  describe "TTL / Expiry edge cases" do
    test "TTL on non-existent key returns -2", %{sock: sock} do
      k = ukey("ttl_nil")
      assert cmd(sock, ["TTL", k]) == -2
    end

    test "TTL on key without expiry returns -1", %{sock: sock} do
      k = ukey("ttl_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "PTTL on non-existent key returns -2", %{sock: sock} do
      k = ukey("pttl_nil")
      assert cmd(sock, ["PTTL", k]) == -2
    end

    test "PTTL on key without expiry returns -1", %{sock: sock} do
      k = ukey("pttl_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PTTL", k]) == -1
    end

    test "PERSIST on non-existent key returns 0", %{sock: sock} do
      k = ukey("persist_nil")
      assert cmd(sock, ["PERSIST", k]) == 0
    end

    test "PERSIST on key without expiry returns 0", %{sock: sock} do
      k = ukey("persist_no_exp")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PERSIST", k]) == 0
    end

    test "PERSIST removes TTL", %{sock: sock} do
      k = ukey("persist_ok")
      cmd(sock, ["SET", k, "val", "EX", "1000"])
      assert cmd(sock, ["TTL", k]) > 0
      assert cmd(sock, ["PERSIST", k]) == 1
      assert cmd(sock, ["TTL", k]) == -1
    end

    test "EXPIRE on non-existent key returns 0", %{sock: sock} do
      k = ukey("expire_nil")
      assert cmd(sock, ["EXPIRE", k, "100"]) == 0
    end

    test "EXPIRE with zero or negative deletes the key", %{sock: sock} do
      k = ukey("expire_zero")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXPIRE", k, "0"]) == 1
      assert cmd(sock, ["GET", k]) == nil
    end

    test "EXPIRE with non-integer returns error", %{sock: sock} do
      k = ukey("expire_nan")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["EXPIRE", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "PEXPIRE with non-integer returns error", %{sock: sock} do
      k = ukey("pexpire_nan")
      cmd(sock, ["SET", k, "val"])
      result = cmd(sock, ["PEXPIRE", k, "abc"])
      assert_error_contains(result, "not an integer")
    end

    test "EXPIREAT with past timestamp deletes key", %{sock: sock} do
      k = ukey("expireat_past")
      cmd(sock, ["SET", k, "val"])
      # Timestamp 0 (epoch) is in the past
      cmd(sock, ["EXPIREAT", k, "1"])
      # Key should be gone or have expired
      assert cmd(sock, ["GET", k]) == nil
    end

    test "EXPIRETIME on non-existent key returns -2", %{sock: sock} do
      k = ukey("expiretime_nil")
      assert cmd(sock, ["EXPIRETIME", k]) == -2
    end

    test "EXPIRETIME on persistent key returns -1", %{sock: sock} do
      k = ukey("expiretime_perm")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXPIRETIME", k]) == -1
    end

    test "PEXPIRETIME on persistent key returns -1", %{sock: sock} do
      k = ukey("pexpiretime_perm")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["PEXPIRETIME", k]) == -1
    end

    test "TTL wrong arity", %{sock: sock} do
      result = cmd(sock, ["TTL"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXPIRE wrong arity", %{sock: sock} do
      result = cmd(sock, ["EXPIRE", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end
  describe "Generic command edge cases" do
    test "DEL multiple keys, some exist some don't", %{sock: sock} do
      k1 = ukey("del_multi_a")
      k2 = ukey("del_multi_b")
      k3 = ukey("del_multi_c")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k3, "v3"])
      assert cmd(sock, ["DEL", k1, k2, k3]) == 2
    end

    test "DEL with no args returns error", %{sock: sock} do
      result = cmd(sock, ["DEL"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXISTS with multiple keys returns count", %{sock: sock} do
      k1 = ukey("exists_multi_a")
      k2 = ukey("exists_multi_b")
      k3 = ukey("exists_multi_c")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k3, "v3"])
      assert cmd(sock, ["EXISTS", k1, k2, k3]) == 2
    end

    test "EXISTS with no args returns error", %{sock: sock} do
      result = cmd(sock, ["EXISTS"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "EXISTS counts duplicate key occurrences", %{sock: sock} do
      k = ukey("exists_dup")
      cmd(sock, ["SET", k, "v"])
      # Redis counts each occurrence, so EXISTS k k returns 2
      assert cmd(sock, ["EXISTS", k, k]) == 2
    end

    test "TYPE on non-existent key returns 'none'", %{sock: sock} do
      k = ukey("type_nil")
      assert cmd(sock, ["TYPE", k]) == {:simple, "none"}
    end

    test "TYPE on string key returns 'string'", %{sock: sock} do
      k = ukey("type_str")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "string"}
    end

    test "TYPE on hash key returns 'hash'", %{sock: sock} do
      k = ukey("type_hash")
      cmd(sock, ["HSET", k, "f", "v"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "hash"}
    end

    test "TYPE on list key returns 'list'", %{sock: sock} do
      k = ukey("type_list")
      cmd(sock, ["RPUSH", k, "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "list"}
    end

    test "TYPE on set key returns 'set'", %{sock: sock} do
      k = ukey("type_set")
      cmd(sock, ["SADD", k, "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "set"}
    end

    test "TYPE on zset key returns 'zset'", %{sock: sock} do
      k = ukey("type_zset")
      cmd(sock, ["ZADD", k, "1.0", "a"])
      assert cmd(sock, ["TYPE", k]) == {:simple, "zset"}
    end

    test "RENAME non-existent key returns error", %{sock: sock} do
      k1 = ukey("rename_nil_src")
      k2 = ukey("rename_nil_dst")
      result = cmd(sock, ["RENAME", k1, k2])
      assert_error_contains(result, "no such key")
    end

    test "RENAME to same key returns OK", %{sock: sock} do
      k = ukey("rename_self")
      cmd(sock, ["SET", k, "val"])
      assert_ok(cmd(sock, ["RENAME", k, k]))
      assert cmd(sock, ["GET", k]) == "val"
    end

    test "RENAME overwrites destination", %{sock: sock} do
      k1 = ukey("rename_src")
      k2 = ukey("rename_dst")
      cmd(sock, ["SET", k1, "src_val"])
      cmd(sock, ["SET", k2, "dst_val"])
      assert_ok(cmd(sock, ["RENAME", k1, k2]))
      assert cmd(sock, ["GET", k2]) == "src_val"
      assert cmd(sock, ["GET", k1]) == nil
    end

    test "RENAMENX when dest exists returns 0", %{sock: sock} do
      k1 = ukey("renamenx_src")
      k2 = ukey("renamenx_dst")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k2, "v2"])
      assert cmd(sock, ["RENAMENX", k1, k2]) == 0
      # Source should still exist
      assert cmd(sock, ["GET", k1]) == "v1"
      # Dest should be unchanged
      assert cmd(sock, ["GET", k2]) == "v2"
    end

    test "RENAMENX when dest does not exist returns 1", %{sock: sock} do
      k1 = ukey("renamenx_ok_src")
      k2 = ukey("renamenx_ok_dst")
      cmd(sock, ["SET", k1, "val"])
      assert cmd(sock, ["RENAMENX", k1, k2]) == 1
      assert cmd(sock, ["GET", k2]) == "val"
      assert cmd(sock, ["GET", k1]) == nil
    end

    test "RENAMENX to same key returns 0", %{sock: sock} do
      k = ukey("renamenx_self")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["RENAMENX", k, k]) == 0
    end

    test "COPY non-existent source returns 0", %{sock: sock} do
      k1 = ukey("copy_nil")
      k2 = ukey("copy_dst")
      assert cmd(sock, ["COPY", k1, k2]) == 0
    end

    test "COPY to existing dest without REPLACE returns 0", %{sock: sock} do
      k1 = ukey("copy_src")
      k2 = ukey("copy_dst")
      cmd(sock, ["SET", k1, "v1"])
      cmd(sock, ["SET", k2, "v2"])
      assert cmd(sock, ["COPY", k1, k2]) == 0
    end

    test "COPY with REPLACE to existing dest succeeds", %{sock: sock} do
      k1 = ukey("copy_replace_src")
      k2 = ukey("copy_replace_dst")
      cmd(sock, ["SET", k1, "new_val"])
      cmd(sock, ["SET", k2, "old_val"])
      assert cmd(sock, ["COPY", k1, k2, "REPLACE"]) == 1
      assert cmd(sock, ["GET", k2]) == "new_val"
      # Source still exists
      assert cmd(sock, ["GET", k1]) == "new_val"
    end

    test "UNLINK works like DEL", %{sock: sock} do
      k = ukey("unlink")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["UNLINK", k]) == 1
      assert cmd(sock, ["GET", k]) == nil
    end

    test "OBJECT ENCODING on non-existent key returns error", %{sock: sock} do
      k = ukey("obj_nil")
      result = cmd(sock, ["OBJECT", "ENCODING", k])
      assert_error_contains(result, "no such key")
    end

    test "OBJECT HELP returns list", %{sock: sock} do
      result = cmd(sock, ["OBJECT", "HELP"])
      assert is_list(result)
    end

    test "WAIT returns 0 (no replication)", %{sock: sock} do
      assert cmd(sock, ["WAIT", "1", "0"]) == 0
    end

    test "RENAME wrong arity", %{sock: sock} do
      result = cmd(sock, ["RENAME", "k"])
      assert_error_contains(result, "wrong number of arguments")
    end
  end
  describe "Server command edge cases" do
    test "PING with argument echoes it", %{sock: sock} do
      assert cmd(sock, ["PING", "hello world"]) == "hello world"
    end

    test "PING with too many args returns error", %{sock: sock} do
      result = cmd(sock, ["PING", "a", "b"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "ECHO returns the argument", %{sock: sock} do
      assert cmd(sock, ["ECHO", "ferricstore"]) == "ferricstore"
    end

    test "ECHO with no args returns error", %{sock: sock} do
      result = cmd(sock, ["ECHO"])
      assert_error_contains(result, "wrong number of arguments")
    end

    test "SELECT returns error (not supported)", %{sock: sock} do
      result = cmd(sock, ["SELECT", "1"])
      assert_error_contains(result, "SELECT not supported")
    end

    test "INFO with unknown section returns empty string", %{sock: sock} do
      result = cmd(sock, ["INFO", "nonexistent_section"])
      assert result == ""
    end

    test "INFO with valid section returns non-empty", %{sock: sock} do
      result = cmd(sock, ["INFO", "server"])
      assert is_binary(result)
      assert String.contains?(result, "ferricstore_version")
    end

    test "DBSIZE returns non-negative integer", %{sock: sock} do
      result = cmd(sock, ["DBSIZE"])
      assert is_integer(result) and result >= 0
    end

    test "COMMAND COUNT returns positive integer", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "COUNT"])
      assert is_integer(result) and result > 0
    end

    test "COMMAND LIST returns list of strings", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "LIST"])
      assert is_list(result) and result != []
    end

    test "COMMAND INFO with unknown command returns nil in list", %{sock: sock} do
      result = cmd(sock, ["COMMAND", "INFO", "NOTACOMMAND"])
      assert is_list(result)
      assert Enum.at(result, 0) == nil
    end

    test "CONFIG GET with glob pattern", %{sock: sock} do
      result = cmd(sock, ["CONFIG", "GET", "*"])
      assert is_list(result)
    end

    test "LOLWUT returns ASCII art", %{sock: sock} do
      result = cmd(sock, ["LOLWUT"])
      assert is_binary(result)
      # The ASCII art contains "v0.1.0" version string
      assert String.contains?(result, "v0.1.0")
    end

    test "unknown command returns error", %{sock: sock} do
      result = cmd(sock, ["TOTALLYUNKNOWNCOMMAND"])
      assert_error_contains(result, "unknown command")
    end

    test "FLUSHDB clears all keys", %{sock: sock} do
      k = ukey("flushdb_test")
      cmd(sock, ["SET", k, "val"])
      assert cmd(sock, ["EXISTS", k]) == 1
      assert_ok(cmd(sock, ["FLUSHDB"]))
      assert cmd(sock, ["DBSIZE"]) == 0
    end
  end
  describe "Transaction edge cases" do
    test "EXEC without MULTI returns error", %{sock: sock} do
      result = cmd(sock, ["EXEC"])
      assert_error_contains(result, "EXEC without MULTI")
    end

    test "DISCARD without MULTI returns error", %{sock: sock} do
      result = cmd(sock, ["DISCARD"])
      assert_error_contains(result, "DISCARD without MULTI")
    end

    test "empty MULTI/EXEC returns empty list", %{sock: sock} do
      assert_ok(cmd(sock, ["MULTI"]))
      result = cmd(sock, ["EXEC"])
      assert result == []
    end

    test "MULTI inside MULTI returns error (nested MULTI)", %{sock: sock} do
      assert_ok(cmd(sock, ["MULTI"]))
      result = cmd(sock, ["MULTI"])
      assert_error_contains(result, "MULTI calls can not be nested")
      # Clean up: discard the transaction
      cmd(sock, ["DISCARD"])
    end

    test "commands that error inside MULTI still queue, errors appear in EXEC result", %{
      sock: sock
    } do
      k = ukey("txn_err_inside")
      cmd(sock, ["SET", k, "hello"])

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["INCR", k]) == {:simple, "QUEUED"}
      assert cmd(sock, ["SET", k, "world"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      assert is_list(result)
      assert length(result) == 2
      # INCR on "hello" should fail at EXEC time
      assert {:error, _} = Enum.at(result, 0)
      # SET should succeed
      assert Enum.at(result, 1) == {:simple, "OK"}
    end

    test "WATCH key, modify from same connection, EXEC aborts", %{sock: sock, port: port} do
      k = ukey("watch_abort")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))

      # Modify from another connection (use cmd_direct to avoid polluting process dict)
      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "modified"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn_value"]) == {:simple, "QUEUED"}
      assert cmd(sock, ["EXEC"]) == nil

      # Value should be the concurrent modification
      assert cmd(sock, ["GET", k]) == "modified"
    end

    test "WATCH without concurrent modification succeeds", %{sock: sock} do
      k = ukey("watch_ok")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))
      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "updated"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      assert is_list(result)
      assert cmd(sock, ["GET", k]) == "updated"
    end

    test "UNWATCH clears watches", %{sock: sock, port: port} do
      k = ukey("unwatch_ok")
      cmd(sock, ["SET", k, "initial"])

      assert_ok(cmd(sock, ["WATCH", k]))
      assert_ok(cmd(sock, ["UNWATCH"]))

      # Modify from another connection (use cmd_direct to avoid polluting process dict)
      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "modified"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn_value"]) == {:simple, "QUEUED"}
      result = cmd(sock, ["EXEC"])
      # Should succeed because we unwatched
      assert is_list(result)
      assert cmd(sock, ["GET", k]) == "txn_value"
    end

    test "DISCARD clears queued commands and watch state", %{sock: sock} do
      k = ukey("discard_clear")
      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "should_not_exist"]) == {:simple, "QUEUED"}
      assert_ok(cmd(sock, ["DISCARD"]))
      assert cmd(sock, ["GET", k]) == nil
      # Should be back in normal mode
      assert_ok(cmd(sock, ["SET", k, "normal"]))
      assert cmd(sock, ["GET", k]) == "normal"
    end

    test "after aborted EXEC, connection returns to normal mode", %{sock: sock, port: port} do
      k = ukey("abort_recovery")
      cmd(sock, ["SET", k, "before"])

      assert_ok(cmd(sock, ["WATCH", k]))

      sock2 = connect_and_hello(port)
      cmd_direct(sock2, ["SET", k, "concurrent"])
      :gen_tcp.close(sock2)

      assert_ok(cmd(sock, ["MULTI"]))
      assert cmd(sock, ["SET", k, "txn"]) == {:simple, "QUEUED"}
      assert cmd(sock, ["EXEC"]) == nil

      # Should work normally after abort
      assert_ok(cmd(sock, ["SET", k, "after_abort"]))
      assert cmd(sock, ["GET", k]) == "after_abort"
    end
  end
  describe "Pipeline edge cases" do
    test "1000 commands in one pipeline", %{sock: sock} do
      prefix = ukey("pipe1000")

      # Send 1000 SET commands as a single TCP write
      pipeline =
        for i <- 1..1000 do
          Encoder.encode(["SET", "#{prefix}_#{i}", "val#{i}"])
        end

      :ok = :gen_tcp.send(sock, IO.iodata_to_binary(pipeline))

      # Drain all 1000 responses
      for _ <- 1..1000 do
        assert recv_response(sock) == {:simple, "OK"}
      end

      # Spot-check
      assert cmd(sock, ["GET", "#{prefix}_1"]) == "val1"
      assert cmd(sock, ["GET", "#{prefix}_500"]) == "val500"
      assert cmd(sock, ["GET", "#{prefix}_1000"]) == "val1000"
    end

    test "mixed valid + invalid commands in pipeline", %{sock: sock} do
      k = ukey("pipe_mix")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "hello"]),
          # will fail: "hello" is not integer
          Encoder.encode(["INCR", k]),
          Encoder.encode(["GET", k]),
          # unknown command
          Encoder.encode(["NOTACOMMAND"]),
          # should still work
          Encoder.encode(["GET", k])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      r1 = recv_response(sock)
      r2 = recv_response(sock)
      r3 = recv_response(sock)
      r4 = recv_response(sock)
      r5 = recv_response(sock)

      assert_ok(r1)
      assert {:error, _} = r2
      assert r3 == "hello"
      assert {:error, _} = r4
      assert r5 == "hello"
    end

    test "pipeline with MULTI/EXEC embedded", %{sock: sock} do
      k = ukey("pipe_txn")

      pipeline =
        IO.iodata_to_binary([
          Encoder.encode(["SET", k, "before"]),
          Encoder.encode(["MULTI"]),
          Encoder.encode(["SET", k, "inside_txn"]),
          Encoder.encode(["GET", k]),
          Encoder.encode(["EXEC"])
        ])

      :ok = :gen_tcp.send(sock, pipeline)

      # SET
      r1 = recv_response(sock)
      # MULTI
      r2 = recv_response(sock)
      # SET (QUEUED)
      r3 = recv_response(sock)
      # GET (QUEUED)
      r4 = recv_response(sock)
      # EXEC
      r5 = recv_response(sock)

      assert_ok(r1)
      assert_ok(r2)
      assert r3 == {:simple, "QUEUED"}
      assert r4 == {:simple, "QUEUED"}
      assert is_list(r5)
      assert length(r5) == 2
    end
  end
  describe "SCAN edge cases" do
    test "SCAN on empty DB returns cursor 0 and empty list", %{sock: sock} do
      result = cmd(sock, ["SCAN", "0"])
      assert is_list(result)
      assert length(result) == 2
      [cursor, keys] = result
      assert cursor == "0"
      assert keys == []
    end

    test "SCAN with MATCH pattern filters results", %{sock: sock} do
      prefix = "scan_match_#{:rand.uniform(9_999_999)}"
      cmd(sock, ["SET", "#{prefix}_a", "1"])
      cmd(sock, ["SET", "#{prefix}_b", "2"])
      cmd(sock, ["SET", ukey("other"), "3"])

      # Collect all results from SCAN
      keys = scan_all(sock, "MATCH", "#{prefix}_*")
      assert "#{prefix}_a" in keys
      assert "#{prefix}_b" in keys
    end

    test "SCAN with TYPE filter", %{sock: sock} do
      str_key = ukey("scan_type_str")
      hash_key = ukey("scan_type_hash")
      cmd(sock, ["SET", str_key, "val"])
      cmd(sock, ["HSET", hash_key, "f", "v"])

      string_keys = scan_all(sock, "TYPE", "string")
      assert str_key in string_keys
      refute hash_key in string_keys
    end

    test "SCAN with COUNT hint", %{sock: sock} do
      for i <- 1..5 do
        cmd(sock, ["SET", ukey("scan_count_#{i}"), "v"])
      end

      result = cmd(sock, ["SCAN", "0", "COUNT", "2"])
      assert is_list(result)
      assert length(result) == 2
    end

    test "HSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("hscan_nil")
      result = cmd(sock, ["HSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end

    test "SSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("sscan_nil")
      result = cmd(sock, ["SSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end

    test "ZSCAN on non-existent key returns cursor 0 and empty list", %{sock: sock} do
      k = ukey("zscan_nil")
      result = cmd(sock, ["ZSCAN", k, "0"])
      [cursor, elements] = result
      assert cursor == "0"
      assert elements == []
    end
  end
  describe "Bitmap edge cases" do
    test "SETBIT on non-existent key creates it", %{sock: sock} do
      k = ukey("setbit_nil")
      assert cmd(sock, ["SETBIT", k, "7", "1"]) == 0
      assert cmd(sock, ["GETBIT", k, "7"]) == 1
    end

    test "GETBIT on non-existent key returns 0", %{sock: sock} do
      k = ukey("getbit_nil")
      assert cmd(sock, ["GETBIT", k, "100"]) == 0
    end

    test "SETBIT with value not 0 or 1 returns error", %{sock: sock} do
      k = ukey("setbit_bad")
      result = cmd(sock, ["SETBIT", k, "0", "2"])
      assert_error_contains(result, "bit")
    end

    test "BITCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("bitcount_nil")
      assert cmd(sock, ["BITCOUNT", k]) == 0
    end

    test "BITCOUNT on key with value", %{sock: sock} do
      k = ukey("bitcount_val")
      cmd(sock, ["SET", k, "foobar"])
      result = cmd(sock, ["BITCOUNT", k])
      assert is_integer(result) and result > 0
    end
  end
  describe "HyperLogLog edge cases" do
    test "PFADD creates HLL on non-existent key", %{sock: sock} do
      k = ukey("pfadd_nil")
      assert cmd(sock, ["PFADD", k, "a", "b", "c"]) == 1
    end

    test "PFCOUNT on non-existent key returns 0", %{sock: sock} do
      k = ukey("pfcount_nil")
      assert cmd(sock, ["PFCOUNT", k]) == 0
    end

    test "PFCOUNT returns approximate cardinality", %{sock: sock} do
      k = ukey("pfcount_approx")
      cmd(sock, ["PFADD", k, "a", "b", "c", "d", "e"])
      count = cmd(sock, ["PFCOUNT", k])
      assert is_integer(count) and count >= 3 and count <= 7
    end

    test "PFADD with duplicate elements returns 0 (no change)", %{sock: sock} do
      k = ukey("pfadd_dup")
      cmd(sock, ["PFADD", k, "a", "b"])
      assert cmd(sock, ["PFADD", k, "a", "b"]) == 0
    end
  end
  describe "Connection edge cases" do
    test "HELLO 2 returns NOPROTO error", %{port: port} do
      {:ok, sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_cmd(sock, ["HELLO", "2"])
      result = recv_direct(sock, "", 5_000)
      assert {:error, msg} = result
      assert String.contains?(msg, "NOPROTO")
      :gen_tcp.close(sock)
    end

    test "HELLO without version returns server info", %{port: port} do
      {:ok, sock} =
        :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

      send_cmd(sock, ["HELLO"])
      result = recv_direct(sock, "", 5_000)
      assert is_map(result)
      assert result["server"] == "ferricstore"
      :gen_tcp.close(sock)
    end

    test "QUIT closes the connection gracefully", %{port: port} do
      sock = connect_and_hello(port)
      assert cmd_direct(sock, ["QUIT"]) == {:simple, "OK"}
      assert :gen_tcp.recv(sock, 0, 1000) in [{:error, :closed}, {:error, :econnreset}]
      :gen_tcp.close(sock)
    end

    test "RESET returns RESET status", %{port: port} do
      sock = connect_and_hello(port)
      assert cmd_direct(sock, ["RESET"]) == {:simple, "RESET"}
      :gen_tcp.close(sock)
    end
  end
  describe "Binary safety edge cases" do
    test "key with null bytes round-trips correctly", %{sock: sock} do
      k = "key\x00with\x00nulls_#{:rand.uniform(9_999_999)}"
      v = "value\x00also\x00null"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "key with newlines and tabs", %{sock: sock} do
      k = "key\n\twith\r\nstuff_#{:rand.uniform(9_999_999)}"
      v = "val\n\tval"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "unicode key and value", %{sock: sock} do
      k = ukey("unicode_key")
      v = "Hello World"
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end

    test "value with all byte values 0-255", %{sock: sock} do
      k = ukey("all_bytes")
      v = :binary.list_to_bin(Enum.to_list(0..255))
      assert_ok(cmd(sock, ["SET", k, v]))
      assert cmd(sock, ["GET", k]) == v
    end
  end
    end
  end
end
