defmodule FerricstoreServer.Resp.ParserTest.Sections.ServerCommandParserPart1 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser

  describe "server command parser part 1" do
    test "parses RESP array commands into normalized command tuples with AST" do
      input = "*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"

      assert {:ok, [{:command, "SET", ["key", "value"], {:set, "key", "value"}, ["key"]}], ""} =
               Parser.parse_commands(input)
    end

    test "parses inline commands into normalized command tuples with AST" do
      assert {:ok, [{:command, "PING", ["hello"], {:ping, "hello"}, []}], ""} =
               Parser.parse_commands("ping hello\r\n")
    end

    test "parses pipelined commands and preserves partial tail" do
      input = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$"

      assert {:ok, [{:command, "PING", [], :ping, []}], "*2\r\n$3\r\nGET\r\n$"} =
               Parser.parse_commands(input)
    end

    test "copies partial command tail after a large parsed pipeline" do
      complete = String.duplicate("*1\r\n$4\r\nPING\r\n", 8_192)
      partial = "*2\r\n$3\r\nGET\r\n$"

      assert {:ok, commands, rest} = Parser.parse_commands(complete <> partial)
      assert rest == partial
      assert length(commands) == 8_192
      assert :binary.referenced_byte_size(rest) <= byte_size(rest) + 64
    end

    test "copies partial RESP tail after a large parsed buffer" do
      complete = String.duplicate("+OK\r\n", 16_384)
      partial = "$5\r\nhe"

      assert {:ok, values, rest} = Parser.parse(complete <> partial)
      assert rest == partial
      assert length(values) == 16_384
      assert :binary.referenced_byte_size(rest) <= byte_size(rest) + 64
    end

    test "rejects non bulk-string array arguments for server command mode" do
      assert {:error, :invalid_command_argument} =
               Parser.parse_commands("*2\r\n$3\r\nGET\r\n:1\r\n")
    end

    test "rejects non-array RESP values for server command mode" do
      assert {:error, :invalid_command_format} = Parser.parse_commands("+OK\r\n")
    end

    test "guards command argument payload size" do
      assert {:error, {:value_too_large, 5, 4}} =
               Parser.parse_commands("*1\r\n$5\r\nhello\r\n", 4)
    end

    test "allows empty binary arguments and keeps empty command names dispatchable" do
      assert {:ok, [{:command, "SET", ["k", ""], {:set, "k", ""}, ["k"]}], ""} =
               Parser.parse_commands("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$0\r\n\r\n")

      assert {:ok, [{:command, "", [], {:unknown, "", []}, []}], ""} =
               Parser.parse_commands("*1\r\n$0\r\n\r\n")
    end

    test "skips blank inline commands without poisoning the connection buffer" do
      assert {:ok, [], ""} = Parser.parse_commands("\r\n")

      assert {:ok, [{:command, "PING", [], :ping, []}], ""} =
               Parser.parse_commands("\r\nPING\r\n")
    end

    test "rejects empty RESP command arrays" do
      assert {:error, error} = Parser.parse_commands("*0\r\n")
      assert error in [:empty_command_array, "ERR protocol error: empty command array"]
    end

    test "preserves embedded NUL bytes in command args, AST, and key extraction" do
      key = <<"k", 0, "1">>
      value = <<"v", 0, "2">>
      input = "*3\r\n$3\r\nSET\r\n$3\r\n" <> key <> "\r\n$3\r\n" <> value <> "\r\n"

      assert {:ok, [{:command, "SET", [^key, ^value], {:set, ^key, ^value}, [^key]}], ""} =
               Parser.parse_commands(input)
    end

    test "malformed command arity does not become executable single-key AST" do
      input = "*3\r\n$3\r\nGET\r\n$1\r\na\r\n$1\r\nb\r\n"

      assert {:ok, [{:command, "GET", ["a", "b"], ast, ["a"]}], ""} =
               Parser.parse_commands(input)

      assert ast == {:get, ["a", "b"]}
      refute match?({:get, key} when is_binary(key), ast)
    end

    test "rejects null bulk and RESP3 non-bulk command arguments" do
      assert {:error, {:invalid_bulk_length, -1}} = Parser.parse_commands("*1\r\n$-1\r\n")

      assert {:error, :invalid_command_argument} =
               Parser.parse_commands("*2\r\n$3\r\nGET\r\n_\r\n")
    end

    test "parses SET EX/NX/GET options into Rust AST" do
      input =
        "*7\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$2\r\n10\r\n$2\r\nNX\r\n$3\r\nGET\r\n"

      assert {:ok,
              [
                {:command, "SET", ["k", "v", "EX", "10", "NX", "GET"],
                 {:set, "k", "v", [{:ex, 10}, :nx, :get]}, ["k"]}
              ], ""} = Parser.parse_commands(input)
    end

    test "parses valid SET option combinations into Rust AST" do
      input =
        "*7\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nPX\r\n$3\r\n500\r\n$2\r\nXX\r\n$3\r\nGET\r\n"

      assert {:ok,
              [
                {:command, "SET", ["k", "v", "PX", "500", "XX", "GET"],
                 {:set, "k", "v", [{:px, 500}, :xx, :get]}, ["k"]}
              ], ""} = Parser.parse_commands(input)
    end

    test "keeps SET option syntax errors inside AST without rejecting frame" do
      input = "*6\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nNX\r\n$2\r\nXX\r\n$3\r\nGET\r\n"

      assert {:ok,
              [
                {:command, "SET", ["k", "v", "NX", "XX", "GET"],
                 {:set, "k", "v",
                  {:error, "ERR XX and NX options at the same time are not compatible"}}, ["k"]}
              ], ""} = Parser.parse_commands(input)
    end

    test "parses string numeric arguments into typed Rust AST" do
      assert {:ok,
              [
                {:command, "INCRBY", ["k", "42"], {:incrby, "k", 42}, ["k"]},
                {:command, "DECRBY", ["k", "-7"], {:decrby, "k", -7}, ["k"]},
                {:command, "INCRBYFLOAT", ["k", "1.25"], {:incrbyfloat, "k", 1.25}, ["k"]},
                {:command, "SETEX", ["k", "10", "v"], {:setex, "k", 10, "v"}, ["k"]},
                {:command, "PSETEX", ["k", "250", "v"], {:psetex, "k", 250, "v"}, ["k"]},
                {:command, "GETRANGE", ["k", "-2", "4"], {:getrange, "k", -2, 4}, ["k"]},
                {:command, "SETRANGE", ["k", "3", "v"], {:setrange, "k", 3, "v"}, ["k"]}
              ], ""} =
               Parser.parse_commands(
                 "incrby k 42\r\n" <>
                   "decrby k -7\r\n" <>
                   "incrbyfloat k 1.25\r\n" <>
                   "setex k 10 v\r\n" <>
                   "psetex k 250 v\r\n" <>
                   "getrange k -2 4\r\n" <>
                   "setrange k 3 v\r\n"
               )
    end

    test "keeps string numeric parse errors inside AST" do
      assert {:ok,
              [
                {:command, "INCRBY", ["k", "1.5"],
                 {:incrby, "k", {:error, "ERR value is not an integer or out of range"}}, ["k"]},
                {:command, "INCRBYFLOAT", ["k", "nan"],
                 {:incrbyfloat, "k", {:error, "ERR value is not a valid float"}}, ["k"]},
                {:command, "SETRANGE", ["k", "-1", "v"], {:setrange, "k", -1, "v"}, ["k"]}
              ], ""} =
               Parser.parse_commands("incrby k 1.5\r\nincrbyfloat k nan\r\nsetrange k -1 v\r\n")
    end

    test "parses GETEX options into typed Rust AST" do
      assert {:ok,
              [
                {:command, "GETEX", ["k"], {:getex, "k"}, ["k"]},
                {:command, "GETEX", ["k", "persist"], {:getex, "k", :persist}, ["k"]},
                {:command, "GETEX", ["k", "EX", "10"], {:getex, "k", {:ex, 10}}, ["k"]},
                {:command, "GETEX", ["k", "PX", "25"], {:getex, "k", {:px, 25}}, ["k"]},
                {:command, "GETEX", ["k", "EXAT", "999"], {:getex, "k", {:exat, 999}}, ["k"]},
                {:command, "GETEX", ["k", "PXAT", "9999"], {:getex, "k", {:pxat, 9999}}, ["k"]}
              ], ""} =
               Parser.parse_commands(
                 "getex k\r\n" <>
                   "getex k persist\r\n" <>
                   "getex k EX 10\r\n" <>
                   "getex k PX 25\r\n" <>
                   "getex k EXAT 999\r\n" <>
                   "getex k PXAT 9999\r\n"
               )
    end

    test "keeps GETEX option parse errors inside AST" do
      assert {:ok,
              [
                {:command, "GETEX", ["k", "EX", "0"],
                 {:getex, "k", {:error, "ERR invalid expire time in 'getex' command"}}, ["k"]},
                {:command, "GETEX", ["k", "EX", "abc"],
                 {:getex, "k", {:error, "ERR value is not an integer or out of range"}}, ["k"]},
                {:command, "GETEX", ["k", "BAD"], {:getex, "k", {:error, "ERR syntax error"}},
                 ["k"]}
              ], ""} =
               Parser.parse_commands("getex k EX 0\r\ngetex k EX abc\r\ngetex k BAD\r\n")
    end

    test "parses blocking command arguments into typed Rust AST" do
      assert {:ok,
              [
                {:command, "BLPOP", ["a", "b", "1.5"], {:blpop, ["a", "b"], 1500}, ["a", "b"]},
                {:command, "BRPOP", ["a", "0"], {:brpop, ["a"], 0}, ["a"]},
                {:command, "BLMOVE", ["src", "dst", "LEFT", "right", "0.25"],
                 {:blmove, "src", "dst", :left, :right, 250}, ["src", "dst"]},
                {:command, "BLMPOP", ["2.5", "2", "a", "b", "RIGHT", "COUNT", "3"],
                 {:blmpop, ["a", "b"], :right, 3, 2500}, ["a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "blpop a b 1.5\r\n" <>
                   "brpop a 0\r\n" <>
                   "blmove src dst LEFT right 0.25\r\n" <>
                   "blmpop 2.5 2 a b RIGHT COUNT 3\r\n"
               )
    end

    test "keeps blocking command parse errors inside AST" do
      assert {:ok,
              [
                {:command, "BLPOP", ["a", "-1"], {:blpop, {:error, "ERR timeout is negative"}},
                 ["a"]},
                {:command, "BLMOVE", ["src", "dst", "UP", "RIGHT", "1"],
                 {:blmove, {:error, "ERR syntax error"}}, ["src", "dst"]},
                {:command, "BLMPOP", ["1", "2", "a", "LEFT"],
                 {:blmpop, {:error, "ERR syntax error"}}, ["a", "LEFT"]}
              ], ""} =
               Parser.parse_commands(
                 "blpop a -1\r\n" <>
                   "blmove src dst UP RIGHT 1\r\n" <>
                   "blmpop 1 2 a LEFT\r\n"
               )
    end

    test "parses connection/admin command arguments into Rust AST" do
      assert {:ok,
              [
                {:command, "HELLO", ["3"], {:hello, 3}, []},
                {:command, "HELLO", [], :hello, []},
                {:command, "AUTH", ["secret"], {:auth, "default", "secret"}, []},
                {:command, "AUTH", ["alice", "secret"], {:auth, "alice", "secret"}, []},
                {:command, "ACL", ["whoami"], {:acl, "WHOAMI", []}, []},
                {:command, "CLIENT", ["setname", "c1"], {:client, "SETNAME", ["c1"]}, []},
                {:command, "CLIENT", ["hello", "3"], {:hello, 3}, []},
                {:command, "SANDBOX", ["join", "tok"], {:sandbox, "JOIN", ["tok"]}, []}
              ], ""} =
               Parser.parse_commands(
                 "hello 3\r\n" <>
                   "hello\r\n" <>
                   "auth secret\r\n" <>
                   "auth alice secret\r\n" <>
                   "acl whoami\r\n" <>
                   "client setname c1\r\n" <>
                   "client hello 3\r\n" <>
                   "sandbox join tok\r\n"
               )
    end

    test "parses pubsub and transaction command arguments into Rust AST" do
      assert {:ok,
              [
                {:command, "SUBSCRIBE", ["a", "b"], {:subscribe, ["a", "b"]}, ["a", "b"]},
                {:command, "UNSUBSCRIBE", [], {:unsubscribe, []}, []},
                {:command, "PSUBSCRIBE", ["p*"], {:psubscribe, ["p*"]}, ["p*"]},
                {:command, "PUNSUBSCRIBE", [], {:punsubscribe, []}, []},
                {:command, "MULTI", [], :multi, []},
                {:command, "EXEC", [], :exec, []},
                {:command, "DISCARD", [], :discard, []},
                {:command, "WATCH", ["k1", "k2"], {:watch, ["k1", "k2"]}, ["k1", "k2"]},
                {:command, "UNWATCH", [], :unwatch, []},
                {:command, "RESET", [], :reset, []},
                {:command, "QUIT", [], :quit, []}
              ], ""} =
               Parser.parse_commands(
                 "subscribe a b\r\n" <>
                   "unsubscribe\r\n" <>
                   "psubscribe p*\r\n" <>
                   "punsubscribe\r\n" <>
                   "multi\r\n" <>
                   "exec\r\n" <>
                   "discard\r\n" <>
                   "watch k1 k2\r\n" <>
                   "unwatch\r\n" <>
                   "reset\r\n" <>
                   "quit\r\n"
               )
    end

    test "keeps connection/admin parse errors inside AST" do
      assert {:ok,
              [
                {:command, "HELLO", ["2"],
                 {:hello,
                  {:error, "NOPROTO this server does not support the requested protocol version"}},
                 []},
                {:command, "AUTH", [],
                 {:auth, {:error, "ERR wrong number of arguments for 'auth' command"}}, []},
                {:command, "ACL", [],
                 {:acl, {:error, "ERR wrong number of arguments for 'acl' command"}}, []},
                {:command, "CLIENT", [],
                 {:client, {:error, "ERR wrong number of arguments for 'client' command"}}, []},
                {:command, "SUBSCRIBE", [],
                 {:subscribe, {:error, "ERR wrong number of arguments for 'subscribe' command"}},
                 []},
                {:command, "WATCH", [],
                 {:watch, {:error, "ERR wrong number of arguments for 'watch' command"}}, []}
              ], ""} =
               Parser.parse_commands(
                 "hello 2\r\n" <>
                   "auth\r\n" <>
                   "acl\r\n" <>
                   "client\r\n" <>
                   "subscribe\r\n" <>
                   "watch\r\n"
               )
    end

    test "uppercases server/admin subcommands in Rust AST" do
      assert {:ok,
              [
                {:command, "COMMAND", ["count"], {:command, ["COUNT"]}, []},
                {:command, "DEBUG", ["sleep", "0"], {:debug, ["SLEEP", "0"]}, []},
                {:command, "CONFIG", ["get", "*"], {:config, ["GET", "*"]}, []},
                {:command, "MEMORY", ["usage", "k"], {:memory, ["USAGE", "k"]}, ["k"]},
                {:command, "FERRICSTORE.CONFIG", ["set", "p", "ttl", "1"],
                 {:ferricstore_config, ["SET", "p", "ttl", "1"]}, []},
                {:command, "FERRICSTORE.DOCTOR", ["CHECK", "SCOPE", "FLOW_LMDB"],
                 {:ferricstore_doctor, ["CHECK", "SCOPE", "FLOW_LMDB"]}, []},
                {:command, "PUBSUB", ["channels"], {:pubsub, ["CHANNELS"]}, []}
              ], ""} =
               Parser.parse_commands(
                 "command count\r\n" <>
                   "debug sleep 0\r\n" <>
                   "config get *\r\n" <>
                   "memory usage k\r\n" <>
                   "ferricstore.config set p ttl 1\r\n" <>
                   "ferricstore.doctor CHECK SCOPE FLOW_LMDB\r\n" <>
                   "pubsub channels\r\n"
               )
    end

    test "extracts pubsub channels and patterns as ACL keys" do
      assert {:ok,
              [
                {:command, "PUBLISH", ["tenant:a", "msg"], {:publish, ["tenant:a", "msg"]},
                 ["tenant:a"]},
                {:command, "SUBSCRIBE", ["tenant:a", "tenant:b"],
                 {:subscribe, ["tenant:a", "tenant:b"]}, ["tenant:a", "tenant:b"]},
                {:command, "PSUBSCRIBE", ["tenant:*"], {:psubscribe, ["tenant:*"]}, ["tenant:*"]},
                {:command, "PUBSUB", ["numsub", "tenant:a"], {:pubsub, ["NUMSUB", "tenant:a"]},
                 ["tenant:a"]},
                {:command, "PUBSUB", ["channels", "tenant:*"],
                 {:pubsub, ["CHANNELS", "tenant:*"]}, ["tenant:*"]}
              ], ""} =
               Parser.parse_commands(
                 "publish tenant:a msg\r\n" <>
                   "subscribe tenant:a tenant:b\r\n" <>
                   "psubscribe tenant:*\r\n" <>
                   "pubsub numsub tenant:a\r\n" <>
                   "pubsub channels tenant:*\r\n"
               )
    end

    test "parses native numeric command arguments in Rust AST" do
      assert {:ok,
              [
                {:command, "CAS", ["k", "old", "new", "EX", "5"], {:cas, "k", "old", "new", 5000},
                 ["k"]},
                {:command, "LOCK", ["k", "owner", "100"], {:lock, "k", "owner", 100}, ["k"]},
                {:command, "EXTEND", ["k", "owner", "250"], {:extend, "k", "owner", 250}, ["k"]},
                {:command, "RATELIMIT.ADD", ["rl", "1000", "10", "2"],
                 {:ratelimit_add, "rl", 1000, 10, 2}, ["rl"]},
                {:command, "FETCH_OR_COMPUTE", ["k", "1000", "hint"],
                 {:fetch_or_compute, "k", 1000, "hint"}, ["k"]},
                {:command, "FETCH_OR_COMPUTE_RESULT", ["k", "v", "1000"],
                 {:fetch_or_compute_result, "k", "v", 1000}, ["k"]}
              ], ""} =
               Parser.parse_commands(
                 "cas k old new EX 5\r\n" <>
                   "lock k owner 100\r\n" <>
                   "extend k owner 250\r\n" <>
                   "ratelimit.add rl 1000 10 2\r\n" <>
                   "fetch_or_compute k 1000 hint\r\n" <>
                   "fetch_or_compute_result k v 1000\r\n"
               )
    end

    test "parses list numeric and direction arguments into typed Rust AST" do
      assert {:ok,
              [
                {:command, "LPOP", ["l"], {:lpop, "l"}, ["l"]},
                {:command, "RPOP", ["l", "2"], {:rpop, "l", 2}, ["l"]},
                {:command, "LRANGE", ["l", "-2", "4"], {:lrange, "l", -2, 4}, ["l"]},
                {:command, "LINDEX", ["l", "1"], {:lindex, "l", 1}, ["l"]},
                {:command, "LSET", ["l", "1", "x"], {:lset, "l", 1, "x"}, ["l"]},
                {:command, "LREM", ["l", "-1", "x"], {:lrem, "l", -1, "x"}, ["l"]},
                {:command, "LTRIM", ["l", "0", "2"], {:ltrim, "l", 0, 2}, ["l"]},
                {:command, "LINSERT", ["l", "before", "p", "x"],
                 {:linsert, "l", :before, "p", "x"}, ["l"]},
                {:command, "LMOVE", ["a", "b", "LEFT", "right"],
                 {:lmove, "a", "b", :left, :right}, ["a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "lpop l\r\n" <>
                   "rpop l 2\r\n" <>
                   "lrange l -2 4\r\n" <>
                   "lindex l 1\r\n" <>
                   "lset l 1 x\r\n" <>
                   "lrem l -1 x\r\n" <>
                   "ltrim l 0 2\r\n" <>
                   "linsert l before p x\r\n" <>
                   "lmove a b LEFT right\r\n"
               )
    end

    test "keeps list semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "LPOP", ["l", "abc"],
                 {:lpop, "l", {:error, "ERR value is not an integer or out of range"}}, ["l"]},
                {:command, "LINSERT", ["l", "sideways", "p", "x"],
                 {:linsert, "l", {:error, "ERR syntax error"}, "p", "x"}, ["l"]},
                {:command, "LMOVE", ["a", "b", "MIDDLE", "LEFT"],
                 {:lmove, "a", "b", {:error, "ERR syntax error"}, :left}, ["a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "lpop l abc\r\nlinsert l sideways p x\r\nlmove a b MIDDLE LEFT\r\n"
               )
    end

    test "parses hash numeric and field-count arguments into typed Rust AST" do
      assert {:ok,
              [
                {:command, "HINCRBY", ["h", "f", "5"], {:hincrby, "h", "f", 5}, ["h"]},
                {:command, "HINCRBYFLOAT", ["h", "f", "1.25"], {:hincrbyfloat, "h", "f", 1.25},
                 ["h"]},
                {:command, "HRANDFIELD", ["h", "-2", "WITHVALUES"],
                 {:hrandfield, "h", -2, :withvalues}, ["h"]},
                {:command, "HSCAN", ["h", "0", "MATCH", "f*", "COUNT", "5"],
                 {:hscan, "h", 0, [match: "f*", count: 5]}, ["h"]},
                {:command, "HEXPIRE", ["h", "10", "FIELDS", "2", "f1", "f2"],
                 {:hexpire, "h", 10, ["f1", "f2"]}, ["h"]},
                {:command, "HPTTL", ["h", "FIELDS", "1", "f1"], {:hpttl, "h", ["f1"]}, ["h"]},
                {:command, "HGETEX", ["h", "EX", "10", "FIELDS", "1", "f1"],
                 {:hgetex, "h", {:ex, 10}, ["f1"]}, ["h"]},
                {:command, "HSETEX", ["h", "60", "f", "v"], {:hsetex, "h", 60, ["f", "v"]}, ["h"]}
              ], ""} =
               Parser.parse_commands(
                 "hincrby h f 5\r\n" <>
                   "hincrbyfloat h f 1.25\r\n" <>
                   "hrandfield h -2 WITHVALUES\r\n" <>
                   "hscan h 0 MATCH f* COUNT 5\r\n" <>
                   "hexpire h 10 FIELDS 2 f1 f2\r\n" <>
                   "hpttl h FIELDS 1 f1\r\n" <>
                   "hgetex h EX 10 FIELDS 1 f1\r\n" <>
                   "hsetex h 60 f v\r\n"
               )
    end

    test "keeps hash semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "HINCRBY", ["h", "f", "bad"],
                 {:hincrby, "h", "f", {:error, "ERR value is not an integer or out of range"}},
                 ["h"]},
                {:command, "HRANDFIELD", ["h", "2", "BAD"],
                 {:hrandfield, "h", 2, {:error, "ERR syntax error"}}, ["h"]},
                {:command, "HEXPIRE", ["h", "0", "FIELDS", "1", "f"],
                 {:hexpire, "h", {:error, "ERR seconds is not a positive integer"}, ["f"]},
                 ["h"]},
                {:command, "HGETDEL", ["h", "FIELDS", "2", "f1"],
                 {:hgetdel, "h",
                  {:error, "ERR number of fields does not match the count argument"}}, ["h"]}
              ], ""} =
               Parser.parse_commands(
                 "hincrby h f bad\r\n" <>
                   "hrandfield h 2 BAD\r\n" <>
                   "hexpire h 0 FIELDS 1 f\r\n" <>
                   "hgetdel h FIELDS 2 f1\r\n"
               )
    end

    test "parses set counts and options into typed Rust AST" do
      assert {:ok,
              [
                {:command, "SRANDMEMBER", ["s", "-3"], {:srandmember, "s", -3}, ["s"]},
                {:command, "SPOP", ["s", "2"], {:spop, "s", 2}, ["s"]},
                {:command, "SSCAN", ["s", "0", "MATCH", "a*", "COUNT", "5"],
                 {:sscan, "s", 0, [match: "a*", count: 5]}, ["s"]},
                {:command, "SINTERCARD", ["2", "s1", "s2", "LIMIT", "7"],
                 {:sintercard, ["s1", "s2"], 7}, ["s1", "s2"]}
              ], ""} =
               Parser.parse_commands(
                 "srandmember s -3\r\n" <>
                   "spop s 2\r\n" <>
                   "sscan s 0 MATCH a* COUNT 5\r\n" <>
                   "sintercard 2 s1 s2 LIMIT 7\r\n"
               )
    end

    test "keeps set semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "SRANDMEMBER", ["s", "bad"],
                 {:srandmember, "s", {:error, "ERR value is not an integer or out of range"}},
                 ["s"]},
                {:command, "SPOP", ["s", "-1"],
                 {:spop, "s", {:error, "ERR value is not an integer or out of range"}}, ["s"]},
                {:command, "SSCAN", ["s", "nope"], {:sscan, "s", {:error, "ERR invalid cursor"}},
                 ["s"]},
                {:command, "SINTERCARD", ["3", "s1", "s2"],
                 {:sintercard,
                  {:error, "ERR Number of keys can't be greater than number of args"}},
                 ["s1", "s2"]}
              ], ""} =
               Parser.parse_commands(
                 "srandmember s bad\r\n" <>
                   "spop s -1\r\n" <>
                   "sscan s nope\r\n" <>
                   "sintercard 3 s1 s2\r\n"
               )
    end

    test "parses zset scores, counts, bounds, and options into typed Rust AST" do
      assert {:ok,
              [
                {:command, "ZADD", ["z", "NX", "CH", "1.5", "a", "2", "b"],
                 {:zadd, "z", [:nx, :ch], [{1.5, "a"}, {2.0, "b"}]}, ["z"]},
                {:command, "ZINCRBY", ["z", "2.25", "a"], {:zincrby, "z", 2.25, "a"}, ["z"]},
                {:command, "ZRANGE", ["z", "0", "-1", "WITHSCORES"], {:zrange, "z", 0, -1, true},
                 ["z"]},
                {:command, "ZPOPMIN", ["z", "2"], {:zpopmin, "z", 2}, ["z"]},
                {:command, "ZRANDMEMBER", ["z", "-2", "WITHSCORES"],
                 {:zrandmember, "z", -2, true}, ["z"]},
                {:command, "ZSCAN", ["z", "0", "MATCH", "a*", "COUNT", "5"],
                 {:zscan, "z", 0, [match: "a*", count: 5]}, ["z"]},
                {:command, "ZRANGEBYSCORE", ["z", "(1", "+inf", "WITHSCORES", "LIMIT", "1", "2"],
                 {:zrangebyscore, "z", {:exclusive, 1.0}, :inf,
                  [withscores: true, limit: {1, 2}]}, ["z"]},
                {:command, "ZREVRANGEBYSCORE", ["z", "+inf", "-inf"],
                 {:zrevrangebyscore, "z", :inf, :neg_inf, []}, ["z"]}
              ], ""} =
               Parser.parse_commands(
                 "zadd z NX CH 1.5 a 2 b\r\n" <>
                   "zincrby z 2.25 a\r\n" <>
                   "zrange z 0 -1 WITHSCORES\r\n" <>
                   "zpopmin z 2\r\n" <>
                   "zrandmember z -2 WITHSCORES\r\n" <>
                   "zscan z 0 MATCH a* COUNT 5\r\n" <>
                   "zrangebyscore z (1 +inf WITHSCORES LIMIT 1 2\r\n" <>
                   "zrevrangebyscore z +inf -inf\r\n"
               )
    end

    test "keeps zset semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "ZADD", ["z", "bad", "a"],
                 {:zadd, "z", {:error, "ERR value is not a valid float"}}, ["z"]},
                {:command, "ZINCRBY", ["z", "bad", "a"],
                 {:zincrby, "z", {:error, "ERR value is not a valid float"}, "a"}, ["z"]},
                {:command, "ZRANGE", ["z", "0", "bad"],
                 {:zrange, "z", {:error, "ERR value is not an integer or out of range"}, "bad"},
                 ["z"]},
                {:command, "ZPOPMAX", ["z", "-1"],
                 {:zpopmax, "z", {:error, "ERR value is not an integer or out of range"}}, ["z"]},
                {:command, "ZRANDMEMBER", ["z", "2", "BAD"],
                 {:zrandmember, "z", 2, {:error, "ERR syntax error"}}, ["z"]},
                {:command, "ZSCAN", ["z", "-1"], {:zscan, "z", {:error, "ERR invalid cursor"}},
                 ["z"]},
                {:command, "ZRANGEBYSCORE", ["z", "bad", "+inf"],
                 {:zrangebyscore, "z", {:error, "ERR min or max is not a float"}}, ["z"]}
              ], ""} =
               Parser.parse_commands(
                 "zadd z bad a\r\n" <>
                   "zincrby z bad a\r\n" <>
                   "zrange z 0 bad\r\n" <>
                   "zpopmax z -1\r\n" <>
                   "zrandmember z 2 BAD\r\n" <>
                   "zscan z -1\r\n" <>
                   "zrangebyscore z bad +inf\r\n"
               )
    end

    test "parses bitmap offsets, modes, and operations into typed Rust AST" do
      assert {:ok,
              [
                {:command, "SETBIT", ["bits", "7", "1"], {:setbit, "bits", 7, 1}, ["bits"]},
                {:command, "GETBIT", ["bits", "7"], {:getbit, "bits", 7}, ["bits"]},
                {:command, "BITCOUNT", ["bits"], {:bitcount, "bits"}, ["bits"]},
                {:command, "BITCOUNT", ["bits", "0", "-1", "BIT"],
                 {:bitcount, "bits", {0, -1, :bit}}, ["bits"]},
                {:command, "BITPOS", ["bits", "1"], {:bitpos, "bits", 1, :all}, ["bits"]},
                {:command, "BITPOS", ["bits", "1", "0", "7", "BYTE"],
                 {:bitpos, "bits", 1, {0, 7, :byte}}, ["bits"]},
                {:command, "BITOP", ["AND", "dst", "a", "b"], {:bitop, :band, "dst", ["a", "b"]},
                 ["dst", "a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "setbit bits 7 1\r\n" <>
                   "getbit bits 7\r\n" <>
                   "bitcount bits\r\n" <>
                   "bitcount bits 0 -1 BIT\r\n" <>
                   "bitpos bits 1\r\n" <>
                   "bitpos bits 1 0 7 BYTE\r\n" <>
                   "bitop AND dst a b\r\n"
               )
    end

    test "keeps bitmap semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "SETBIT", ["bits", "bad", "1"],
                 {:setbit, "bits", {:error, "ERR bit offset is not an integer or out of range"},
                  "1"}, ["bits"]},
                {:command, "SETBIT", ["bits", "0", "2"],
                 {:setbit, "bits", 0, {:error, "ERR bit is not an integer or out of range"}},
                 ["bits"]},
                {:command, "BITCOUNT", ["bits", "0", "1", "BAD"],
                 {:bitcount, "bits", {:error, "ERR syntax error"}}, ["bits"]},
                {:command, "BITPOS", ["bits", "2"],
                 {:bitpos, "bits", {:error, "ERR bit is not an integer or out of range"}},
                 ["bits"]},
                {:command, "BITOP", ["NOT", "dst", "a", "b"],
                 {:bitop, {:error, "ERR BITOP NOT requires one and only one key"}},
                 ["dst", "a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "setbit bits bad 1\r\n" <>
                   "setbit bits 0 2\r\n" <>
                   "bitcount bits 0 1 BAD\r\n" <>
                   "bitpos bits 2\r\n" <>
                   "bitop NOT dst a b\r\n"
               )
    end

    test "parses generic key command options into typed Rust AST" do
      assert {:ok,
              [
                {:command, "TYPE", ["k"], {:type, "k"}, ["k"]},
                {:command, "UNLINK", ["a", "b"], {:unlink, ["a", "b"]}, ["a", "b"]},
                {:command, "RENAME", ["a", "b"], {:rename, "a", "b"}, ["a", "b"]},
                {:command, "RENAMENX", ["a", "b"], {:renamenx, "a", "b"}, ["a", "b"]},
                {:command, "COPY", ["a", "b", "REPLACE"], {:copy, "a", "b", true}, ["a", "b"]},
                {:command, "RANDOMKEY", [], {:randomkey, []}, []},
                {:command, "SCAN", ["0", "MATCH", "user:*", "COUNT", "5", "TYPE", "STRING"],
                 {:scan, "0", [match: "user:*", count: 5, type: "string"]}, []},
                {:command, "EXPIRETIME", ["k"], {:expiretime, "k"}, ["k"]},
                {:command, "PEXPIRETIME", ["k"], {:pexpiretime, "k"}, ["k"]},
                {:command, "OBJECT", ["encoding", "k"], {:object, :encoding, "k"}, ["k"]},
                {:command, "OBJECT", ["help"], {:object, :help}, []},
                {:command, "WAIT", ["abc", "xyz"], {:wait, "abc", "xyz"}, []}
              ], ""} =
               Parser.parse_commands(
                 "type k\r\n" <>
                   "unlink a b\r\n" <>
                   "rename a b\r\n" <>
                   "renamenx a b\r\n" <>
                   "copy a b REPLACE\r\n" <>
                   "randomkey\r\n" <>
                   "scan 0 MATCH user:* COUNT 5 TYPE STRING\r\n" <>
                   "expiretime k\r\n" <>
                   "pexpiretime k\r\n" <>
                   "object encoding k\r\n" <>
                   "object help\r\n" <>
                   "wait abc xyz\r\n"
               )
    end

    test "keeps generic semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "COPY", ["a", "b", "BAD"],
                 {:copy, "a", "b", {:error, "ERR syntax error"}}, ["a", "b"]},
                {:command, "SCAN", ["0", "COUNT", "0"],
                 {:scan, "0", {:error, "ERR value is not an integer or out of range"}}, []},
                {:command, "SCAN", ["0", "MATCH"], {:scan, "0", {:error, "ERR syntax error"}},
                 []},
                {:command, "OBJECT", ["bogus", "k"],
                 {:object,
                  {:error,
                   "ERR unknown subcommand or wrong number of arguments for 'bogus' command"}},
                 ["k"]}
              ], ""} =
               Parser.parse_commands(
                 "copy a b BAD\r\n" <>
                   "scan 0 COUNT 0\r\n" <>
                   "scan 0 MATCH\r\n" <>
                   "object bogus k\r\n"
               )
    end

  end
    end
  end
end
