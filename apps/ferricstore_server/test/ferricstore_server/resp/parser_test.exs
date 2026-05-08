defmodule FerricstoreServer.Resp.ParserTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.Parser

  # ---------------------------------------------------------------------------
  # Simple string (+)
  # ---------------------------------------------------------------------------

  describe "simple string" do
    test "parses a simple string" do
      assert {:ok, [{:simple, "OK"}], ""} = Parser.parse("+OK\r\n")
    end

    test "parses an empty simple string" do
      assert {:ok, [{:simple, ""}], ""} = Parser.parse("+\r\n")
    end

    test "parses a simple string with spaces" do
      assert {:ok, [{:simple, "hello world"}], ""} = Parser.parse("+hello world\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Simple error (-)
  # ---------------------------------------------------------------------------

  describe "simple error" do
    test "parses a simple error" do
      assert {:ok, [{:error, "ERR unknown command"}], ""} =
               Parser.parse("-ERR unknown command\r\n")
    end

    test "parses WRONGTYPE error" do
      msg = "WRONGTYPE Operation against a key holding the wrong kind of value"

      assert {:ok, [{:error, ^msg}], ""} =
               Parser.parse("-#{msg}\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Integer (:)
  # ---------------------------------------------------------------------------

  describe "integer" do
    test "parses a positive integer" do
      assert {:ok, [42], ""} = Parser.parse(":42\r\n")
    end

    test "parses zero" do
      assert {:ok, [0], ""} = Parser.parse(":0\r\n")
    end

    test "parses a negative integer" do
      assert {:ok, [-100], ""} = Parser.parse(":-100\r\n")
    end

    test "parses a large integer" do
      assert {:ok, [999_999_999_999], ""} = Parser.parse(":999999999999\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Bulk string ($)
  # ---------------------------------------------------------------------------

  describe "bulk string" do
    test "parses a bulk string" do
      assert {:ok, ["hello"], ""} = Parser.parse("$5\r\nhello\r\n")
    end

    test "parses an empty bulk string" do
      assert {:ok, [""], ""} = Parser.parse("$0\r\n\r\n")
    end

    test "parses nil bulk string ($-1)" do
      assert {:ok, [nil], ""} = Parser.parse("$-1\r\n")
    end

    test "parses bulk string containing CRLF" do
      assert {:ok, ["he\r\nlo"], ""} = Parser.parse("$6\r\nhe\r\nlo\r\n")
    end

    test "parses binary data in bulk string" do
      data = <<0, 1, 2, 255, 254>>
      input = "$5\r\n" <> data <> "\r\n"
      assert {:ok, [^data], ""} = Parser.parse(input)
    end

    test "returns error when bulk crlf terminator is missing" do
      # Header says 5 bytes but payload has no \r\n at position 5
      assert {:error, :bulk_crlf_missing} = Parser.parse("$5\r\nhelloXXXXX")
    end

    test "rejects bulk string whose declared length exceeds hard cap (64 MB)" do
      hard_cap = Parser.hard_cap_bytes()
      over = hard_cap + 1
      # Use parse/2 with a max above the hard cap to isolate the hard cap check
      assert {:error, {:value_too_large, ^over, ^hard_cap}} =
               Parser.parse("$#{over}\r\n", hard_cap * 2)
    end

    test "rejects bulk string whose declared length exceeds max_value_size" do
      # With default 1 MB limit, a 1 MB + 1 byte bulk string is rejected
      max = Parser.default_max_value_size()
      over = max + 1

      assert {:error, {:value_too_large, ^over, ^max}} =
               Parser.parse("$#{over}\r\n")
    end

    test "accepts bulk string at exactly max_value_size (incomplete data returns :ok [])" do
      max = Parser.default_max_value_size()
      # Exactly at limit -- allowed; no body bytes present so incomplete
      assert {:ok, [], _rest} = Parser.parse("$#{max}\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Array (*)
  # ---------------------------------------------------------------------------

  describe "array" do
    test "parses an array of integers" do
      input = "*3\r\n:1\r\n:2\r\n:3\r\n"
      assert {:ok, [[1, 2, 3]], ""} = Parser.parse(input)
    end

    test "parses an empty array" do
      assert {:ok, [[]], ""} = Parser.parse("*0\r\n")
    end

    test "parses nil array (*-1)" do
      assert {:ok, [nil], ""} = Parser.parse("*-1\r\n")
    end

    test "parses a mixed-type array" do
      input = "*3\r\n:1\r\n$5\r\nhello\r\n+OK\r\n"
      assert {:ok, [[1, "hello", {:simple, "OK"}]], ""} = Parser.parse(input)
    end

    test "parses nested arrays" do
      input = "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"
      assert {:ok, [[[1, 2], [3, 4]]], ""} = Parser.parse(input)
    end

    test "parses array with nil elements" do
      input = "*3\r\n$3\r\nfoo\r\n_\r\n$3\r\nbar\r\n"
      assert {:ok, [["foo", nil, "bar"]], ""} = Parser.parse(input)
    end

    test "rejects oversized arrays with the public error contract" do
      over_limit = 1_048_577

      assert {:error, "ERR protocol error: array too large"} =
               Parser.parse("*#{over_limit}\r\n")

      assert {:error, "ERR protocol error: array too large"} =
               Parser.parse_commands("*#{over_limit}\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Null (_)
  # ---------------------------------------------------------------------------

  describe "null" do
    test "parses null" do
      assert {:ok, [nil], ""} = Parser.parse("_\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean (#)
  # ---------------------------------------------------------------------------

  describe "boolean" do
    test "parses true" do
      assert {:ok, [true], ""} = Parser.parse("#t\r\n")
    end

    test "parses false" do
      assert {:ok, [false], ""} = Parser.parse("#f\r\n")
    end

    test "returns error for invalid boolean" do
      assert {:error, {:invalid_boolean, "x"}} = Parser.parse("#x\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Double (,)
  # ---------------------------------------------------------------------------

  describe "double" do
    test "parses a positive double" do
      assert {:ok, [3.14], ""} = Parser.parse(",3.14\r\n")
    end

    test "parses a negative double" do
      assert {:ok, [-1.5], ""} = Parser.parse(",-1.5\r\n")
    end

    test "parses zero as double" do
      {:ok, [result], ""} = Parser.parse(",0\r\n")
      assert result == 0.0
    end

    test "parses an integer-like double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "parses inf double" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
    end

    test "parses -inf double" do
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Big number (()
  # ---------------------------------------------------------------------------

  describe "big number" do
    test "parses a big positive number" do
      assert {:ok, [3_492_890_328_409_238_509_324_850_943_850_943_825_024_385], ""} =
               Parser.parse("(3492890328409238509324850943850943825024385\r\n")
    end

    test "parses a big negative number" do
      assert {:ok, [-99_999_999_999_999_999_999], ""} =
               Parser.parse("(-99999999999999999999\r\n")
    end

    test "parses big number zero" do
      assert {:ok, [0], ""} = Parser.parse("(0\r\n")
    end

    test "parses large negative big number" do
      assert {:ok, [-12_345_678_901_234_567_890], ""} =
               Parser.parse("(-12345678901234567890\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Blob error (!)
  # ---------------------------------------------------------------------------

  describe "blob error" do
    test "parses a blob error" do
      assert {:ok, [{:error, "SYNTAX invalid syntax"}], ""} =
               Parser.parse("!21\r\nSYNTAX invalid syntax\r\n")
    end

    test "parses an empty blob error" do
      assert {:ok, [{:error, ""}], ""} = Parser.parse("!0\r\n\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Verbatim string (=)
  # ---------------------------------------------------------------------------

  describe "verbatim string" do
    test "parses a verbatim text string" do
      assert {:ok, [{:verbatim, "txt", "Some string"}], ""} =
               Parser.parse("=15\r\ntxt:Some string\r\n")
    end

    test "parses a verbatim markdown string" do
      assert {:ok, [{:verbatim, "mkd", "# Hello"}], ""} =
               Parser.parse("=11\r\nmkd:# Hello\r\n")
    end

    test "parses verbatim string with empty data" do
      assert {:ok, [{:verbatim, "txt", ""}], ""} = Parser.parse("=4\r\ntxt:\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Map (%)
  # ---------------------------------------------------------------------------

  describe "map" do
    test "parses a simple map" do
      input = "%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n"
      assert {:ok, [%{"foo" => 1, "bar" => 2}], ""} = Parser.parse(input)
    end

    test "parses an empty map" do
      assert {:ok, [%{}], ""} = Parser.parse("%0\r\n")
    end

    test "parses a map with mixed value types" do
      input = "%2\r\n$4\r\nname\r\n$5\r\nAlice\r\n$3\r\nage\r\n:30\r\n"
      assert {:ok, [%{"name" => "Alice", "age" => 30}], ""} = Parser.parse(input)
    end

    test "map with duplicate keys — last value wins" do
      input = "%2\r\n+k\r\n:1\r\n+k\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == %{{:simple, "k"} => 2}
    end
  end

  # ---------------------------------------------------------------------------
  # Set (~)
  # ---------------------------------------------------------------------------

  describe "set" do
    test "parses a set" do
      input = "~3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == MapSet.new(["foo", "bar", "baz"])
    end

    test "parses an empty set" do
      assert {:ok, [result], ""} = Parser.parse("~0\r\n")
      assert result == MapSet.new()
    end

    test "set with duplicate elements deduplicates" do
      input = "~3\r\n+a\r\n+a\r\n+b\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert MapSet.size(result) == 2
      assert MapSet.member?(result, {:simple, "a"})
      assert MapSet.member?(result, {:simple, "b"})
    end
  end

  # ---------------------------------------------------------------------------
  # Push (>)
  # ---------------------------------------------------------------------------

  describe "push" do
    test "parses a push message" do
      input = ">3\r\n$7\r\nmessage\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
      assert {:ok, [{:push, ["message", "hello", "world"]}], ""} = Parser.parse(input)
    end

    test "parses an empty push" do
      assert {:ok, [{:push, []}], ""} = Parser.parse(">0\r\n")
    end

    test "push type with single element" do
      assert {:ok, [{:push, [{:simple, "msg"}]}], ""} = Parser.parse(">1\r\n+msg\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Inline commands
  # ---------------------------------------------------------------------------

  describe "inline commands" do
    test "parses PING inline command" do
      assert {:ok, [{:inline, ["PING"]}], ""} = Parser.parse("PING\r\n")
    end

    test "parses SET inline command with args" do
      assert {:ok, [{:inline, ["SET", "foo", "bar"]}], ""} = Parser.parse("SET foo bar\r\n")
    end

    test "parses CLIENT HELLO 3 inline command" do
      assert {:ok, [{:inline, ["CLIENT", "HELLO", "3"]}], ""} =
               Parser.parse("CLIENT HELLO 3\r\n")
    end

    test "parses inline command with extra whitespace" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET  key\r\n")
    end

    test "parses empty inline command (just CRLF)" do
      assert {:ok, [{:inline, []}], ""} = Parser.parse("\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Server command parser
  # ---------------------------------------------------------------------------

  describe "server command parser" do
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

    test "skips true empty commands without poisoning the connection buffer" do
      assert {:ok, [], ""} = Parser.parse_commands("\r\n")
      assert {:ok, [], ""} = Parser.parse_commands("*0\r\n")

      assert {:ok, [{:command, "PING", [], :ping, []}], ""} =
               Parser.parse_commands("\r\nPING\r\n")
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
                {:command, "SUBSCRIBE", ["a", "b"], {:subscribe, ["a", "b"]}, []},
                {:command, "UNSUBSCRIBE", [], {:unsubscribe, []}, []},
                {:command, "PSUBSCRIBE", ["p*"], {:psubscribe, ["p*"]}, []},
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
                {:command, "PUBSUB", ["channels"], {:pubsub, ["CHANNELS"]}, []}
              ], ""} =
               Parser.parse_commands(
                 "command count\r\n" <>
                   "debug sleep 0\r\n" <>
                   "config get *\r\n" <>
                   "memory usage k\r\n" <>
                   "ferricstore.config set p ttl 1\r\n" <>
                   "pubsub channels\r\n"
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

    test "parses expiry commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "EXPIRE", ["k", "10", "nx"], {:expire, "k", 10, :nx}, ["k"]},
                {:command, "PEXPIRE", ["k", "250"], {:pexpire, "k", 250}, ["k"]},
                {:command, "EXPIREAT", ["k", "9999999999", "GT"],
                 {:expireat, "k", 9_999_999_999, :gt}, ["k"]},
                {:command, "PEXPIREAT", ["k", "9999999999999"],
                 {:pexpireat, "k", 9_999_999_999_999}, ["k"]},
                {:command, "TTL", ["k"], {:ttl, "k"}, ["k"]},
                {:command, "PTTL", ["k"], {:pttl, "k"}, ["k"]},
                {:command, "PERSIST", ["k"], {:persist, "k"}, ["k"]}
              ], ""} =
               Parser.parse_commands(
                 "expire k 10 nx\r\n" <>
                   "pexpire k 250\r\n" <>
                   "expireat k 9999999999 GT\r\n" <>
                   "pexpireat k 9999999999999\r\n" <>
                   "ttl k\r\n" <>
                   "pttl k\r\n" <>
                   "persist k\r\n"
               )
    end

    test "keeps expiry semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "EXPIRE", ["k", "1.5"],
                 {:expire, "k", {:error, "ERR value is not an integer or out of range"}}, ["k"]},
                {:command, "PEXPIRE", ["k", "10", "bad"],
                 {:pexpire, "k", 10, {:error, "ERR Unsupported option bad"}}, ["k"]}
              ], ""} =
               Parser.parse_commands("expire k 1.5\r\npexpire k 10 bad\r\n")
    end

    test "parses stream IDs, ranges, and read options into typed Rust AST" do
      assert {:ok,
              [
                {:command, "XADD", ["s", "NOMKSTREAM", "MAXLEN", "~", "10", "*", "f", "v"],
                 {:xadd, "s", {:auto, ["f", "v"], {:maxlen, true, 10}, true}}, ["s"]},
                {:command, "XRANGE", ["s", "-", "+", "COUNT", "2"], {:xrange, "s", :min, :max, 2},
                 ["s"]},
                {:command, "XREVRANGE", ["s", "9-0", "1-0"],
                 {:xrevrange, "s", {1, 0}, {9, 0}, :infinity}, ["s"]},
                {:command, "XREAD", ["COUNT", "2", "BLOCK", "0", "STREAMS", "s1", "s2", "0", "$"],
                 {:xread, 2, {:block, 0}, [{"s1", "0"}, {"s2", "$"}]}, ["s1", "s2"]},
                {:command, "XTRIM", ["s", "MINID", "1-0"], {:xtrim, "s", {:minid, false, "1-0"}},
                 ["s"]},
                {:command, "XGROUP", ["CREATE", "s", "g", "0", "MKSTREAM"],
                 {:xgroup_create, "s", "g", "0", true}, ["s"]},
                {:command, "XREADGROUP", ["GROUP", "g", "c", "COUNT", "1", "STREAMS", "s", ">"],
                 {:xreadgroup, "g", "c", {1, :no_block, [{"s", ">"}]}}, ["s"]},
                {:command, "XACK", ["s", "g", "1-0"], {:xack, "s", "g", ["1-0"]}, ["s"]}
              ], ""} =
               Parser.parse_commands(
                 "xadd s NOMKSTREAM MAXLEN ~ 10 * f v\r\n" <>
                   "xrange s - + COUNT 2\r\n" <>
                   "xrevrange s 9-0 1-0\r\n" <>
                   "xread COUNT 2 BLOCK 0 STREAMS s1 s2 0 $\r\n" <>
                   "xtrim s MINID 1-0\r\n" <>
                   "xgroup CREATE s g 0 MKSTREAM\r\n" <>
                   "xreadgroup GROUP g c COUNT 1 STREAMS s >\r\n" <>
                   "xack s g 1-0\r\n"
               )
    end

    test "keeps stream semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "XADD", ["s", "bad-id", "f", "v"],
                 {:xadd, {:error, "ERR Invalid stream ID specified as stream command argument"}},
                 ["s"]},
                {:command, "XRANGE", ["s", "bad", "+"],
                 {:xrange, "s",
                  {:error, "ERR Invalid stream ID specified as stream command argument"}}, ["s"]},
                {:command, "XTRIM", ["s", "MAXLEN", "-1"],
                 {:xtrim, "s", {:error, "ERR value is not an integer or out of range"}}, ["s"]},
                {:command, "XREAD", ["COUNT", "bad", "STREAMS", "s", "0"],
                 {:xread, {:error, "ERR value is not an integer or out of range"}}, ["s"]},
                {:command, "XGROUP", ["BAD", "s", "g", "0"],
                 {:xgroup, {:error, "ERR syntax error"}}, ["s"]}
              ], ""} =
               Parser.parse_commands(
                 "xadd s bad-id f v\r\n" <>
                   "xrange s bad +\r\n" <>
                   "xtrim s MAXLEN -1\r\n" <>
                   "xread COUNT bad STREAMS s 0\r\n" <>
                   "xgroup BAD s g 0\r\n"
               )
    end

    test "parses JSON command grammar into typed Rust AST" do
      assert {:ok,
              [
                {:command, "JSON.SET", ["doc", "$.a[0]", ~s({"x":1}), "NX"],
                 {:json_set, "doc", ["a", 0], ~s({"x":1}), [:nx]}, ["doc"]},
                {:command, "JSON.GET", ["doc", "$.a", "$['b']"],
                 {:json_get, "doc", [{"$.a", ["a"]}, {"$['b']", ["b"]}]}, ["doc"]},
                {:command, "JSON.NUMINCRBY", ["doc", "$.n", "1.5"],
                 {:json_numincrby, "doc", ["n"], 1.5}, ["doc"]},
                {:command, "JSON.ARRAPPEND", ["doc", "$.arr", "1", "x"],
                 {:json_arrappend, "doc", ["arr"], ["1", "x"]}, ["doc"]},
                {:command, "JSON.MGET", ["a", "b", "$.name"], {:json_mget, ["a", "b"], ["name"]},
                 ["a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "json.set doc $.a[0] {\"x\":1} NX\r\n" <>
                   "json.get doc $.a $['b']\r\n" <>
                   "json.numincrby doc $.n 1.5\r\n" <>
                   "json.arrappend doc $.arr 1 \"x\"\r\n" <>
                   "json.mget a b $.name\r\n"
               )
    end

    test "keeps JSON semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "JSON.SET", ["doc", "$.a", "1", "BAD"],
                 {:json_set, {:error, "ERR syntax error, option 'BAD' not recognized"}}, ["doc"]},
                {:command, "JSON.GET", ["doc", "$["],
                 {:json_get, "doc", {:error, "ERR invalid JSONPath syntax"}}, ["doc"]},
                {:command, "JSON.NUMINCRBY", ["doc", "$.n", "nan"],
                 {:json_numincrby, "doc", {:error, "ERR value is not a number"}}, ["doc"]},
                {:command, "JSON.MGET", ["a"],
                 {:json_mget, {:error, "ERR wrong number of arguments for 'json.mget' command"}},
                 []}
              ], ""} =
               Parser.parse_commands(
                 "json.set doc $.a 1 BAD\r\n" <>
                   "json.get doc $[\r\n" <>
                   "json.numincrby doc $.n nan\r\n" <>
                   "json.mget a\r\n"
               )
    end

    test "parses Geo command grammar into typed Rust AST" do
      assert {:ok,
              [
                {:command, "GEOADD", ["g", "NX", "CH", "13.0", "38.0", "Palermo"],
                 {:geoadd, "g", [:nx, :ch], [{13.0, 38.0, "Palermo"}]}, ["g"]},
                {:command, "GEODIST", ["g", "Palermo", "Catania", "km"],
                 {:geodist, "g", "Palermo", "Catania", "KM"}, ["g"]},
                {:command, "GEOSEARCH",
                 [
                   "g",
                   "FROMLONLAT",
                   "13.0",
                   "38.0",
                   "BYRADIUS",
                   "100",
                   "km",
                   "ASC",
                   "COUNT",
                   "2",
                   "ANY",
                   "WITHDIST"
                 ],
                 {:geosearch, "g",
                  [
                    center: {:lonlat, 13.0, 38.0},
                    shape: {:radius, 100_000.0},
                    unit: "KM",
                    raw_radius: 100.0,
                    sort: :asc,
                    count: 2,
                    any: true,
                    withdist: true
                  ]}, ["g"]},
                {:command, "GEOSEARCHSTORE",
                 ["dst", "src", "FROMMEMBER", "Palermo", "BYBOX", "10", "20", "m"],
                 {:geosearchstore, "dst", "src",
                  [
                    center: {:member, "Palermo"},
                    shape: {:box, 10.0, 20.0},
                    unit: "M"
                  ]}, ["dst", "src"]}
              ], ""} =
               Parser.parse_commands(
                 "geoadd g NX CH 13.0 38.0 Palermo\r\n" <>
                   "geodist g Palermo Catania km\r\n" <>
                   "geosearch g FROMLONLAT 13.0 38.0 BYRADIUS 100 km ASC COUNT 2 ANY WITHDIST\r\n" <>
                   "geosearchstore dst src FROMMEMBER Palermo BYBOX 10 20 m\r\n"
               )
    end

    test "keeps Geo semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "GEOADD", ["g", "NX", "XX", "13", "38", "p"],
                 {:geoadd, {:error, "ERR XX and NX options at the same time are not compatible"}},
                 ["g"]},
                {:command, "GEOADD", ["g", "200", "38", "p"],
                 {:geoadd, {:error, "ERR invalid longitude,latitude pair 200,38"}}, ["g"]},
                {:command, "GEODIST", ["g", "a", "b", "parsecs"],
                 {:geodist, "g", "a", "b",
                  {:error, "ERR unsupported unit provided. please use M, KM, FT, MI"}}, ["g"]},
                {:command, "GEOSEARCH", ["g", "BYRADIUS", "100", "KM"],
                 {:geosearch, "g",
                  {:error, "ERR exactly one of FROMMEMBER or FROMLONLAT must be provided"}},
                 ["g"]}
              ], ""} =
               Parser.parse_commands(
                 "geoadd g NX XX 13 38 p\r\n" <>
                   "geoadd g 200 38 p\r\n" <>
                   "geodist g a b parsecs\r\n" <>
                   "geosearch g BYRADIUS 100 KM\r\n"
               )
    end

    test "parses HyperLogLog commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "PFADD", ["h", "a", "b"], {:pfadd, ["h", "a", "b"]}, ["h"]},
                {:command, "PFCOUNT", ["h1", "h2"], {:pfcount, ["h1", "h2"]}, ["h1", "h2"]},
                {:command, "PFMERGE", ["dst", "h1", "h2"], {:pfmerge, ["dst", "h1", "h2"]},
                 ["dst", "h1", "h2"]},
                {:command, "PFCOUNT", [],
                 {:pfcount, {:error, "ERR wrong number of arguments for 'pfcount' command"}}, []},
                {:command, "PFMERGE", ["dst"],
                 {:pfmerge, {:error, "ERR wrong number of arguments for 'pfmerge' command"}},
                 ["dst"]}
              ], ""} =
               Parser.parse_commands(
                 "pfadd h a b\r\n" <>
                   "pfcount h1 h2\r\n" <>
                   "pfmerge dst h1 h2\r\n" <>
                   "pfcount\r\n" <>
                   "pfmerge dst\r\n"
               )
    end

    test "parses probabilistic commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "BF.RESERVE", ["bf", "0.01", "100"], {:bf_reserve, "bf", 0.01, 100},
                 ["bf"]},
                {:command, "CF.RESERVE", ["cf", "1000"], {:cf_reserve, "cf", 1000}, ["cf"]},
                {:command, "CMS.INITBYDIM", ["cms", "100", "5"], {:cms_initbydim, "cms", 100, 5},
                 ["cms"]},
                {:command, "CMS.INCRBY", ["cms", "a", "2", "b", "3"],
                 {:cms_incrby, "cms", [{"a", 2}, {"b", 3}]}, ["cms"]},
                {:command, "CMS.MERGE", ["dst", "2", "a", "b", "WEIGHTS", "2", "3"],
                 {:cms_merge, "dst", ["a", "b"], [2, 3]}, ["dst", "a", "b"]},
                {:command, "TOPK.RESERVE", ["tk", "10", "8", "7", "0.9"],
                 {:topk_reserve, "tk", 10, 8, 7, 0.9}, ["tk"]},
                {:command, "TOPK.INCRBY", ["tk", "a", "2"], {:topk_incrby, "tk", [{"a", 2}]},
                 ["tk"]},
                {:command, "TOPK.LIST", ["tk", "WITHCOUNT"], {:topk_list, "tk", true}, ["tk"]},
                {:command, "TDIGEST.CREATE", ["td", "COMPRESSION", "200"],
                 {:tdigest_create, "td", 200}, ["td"]},
                {:command, "TDIGEST.ADD", ["td", "1.5", "2"], {:tdigest_add, "td", [1.5, 2.0]},
                 ["td"]},
                {:command, "TDIGEST.MERGE",
                 ["dst", "2", "a", "b", "COMPRESSION", "200", "OVERRIDE"],
                 {:tdigest_merge, "dst", ["a", "b"], [compression: 200, override: true]},
                 ["dst", "a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "bf.reserve bf 0.01 100\r\n" <>
                   "cf.reserve cf 1000\r\n" <>
                   "cms.initbydim cms 100 5\r\n" <>
                   "cms.incrby cms a 2 b 3\r\n" <>
                   "cms.merge dst 2 a b WEIGHTS 2 3\r\n" <>
                   "topk.reserve tk 10 8 7 0.9\r\n" <>
                   "topk.incrby tk a 2\r\n" <>
                   "topk.list tk WITHCOUNT\r\n" <>
                   "tdigest.create td COMPRESSION 200\r\n" <>
                   "tdigest.add td 1.5 2\r\n" <>
                   "tdigest.merge dst 2 a b COMPRESSION 200 OVERRIDE\r\n"
               )
    end

    test "keeps probabilistic semantic parse errors inside AST" do
      assert {:ok,
              [
                {:command, "BF.RESERVE", ["bf", "bad", "100"],
                 {:bf_reserve, "bf", {:error, "ERR error_rate is not a valid float"}}, ["bf"]},
                {:command, "CMS.INCRBY", ["cms", "a"],
                 {:cms_incrby, "cms",
                  {:error, "ERR wrong number of arguments for 'cms.incrby' command"}}, ["cms"]},
                {:command, "TOPK.LIST", ["tk", "BAD"],
                 {:topk_list, "tk", {:error, "ERR syntax error"}}, ["tk"]},
                {:command, "TDIGEST.TRIMMED_MEAN", ["td", "0.9", "0.1"],
                 {:tdigest_trimmed_mean, "td",
                  {:error, "ERR TDIGEST: low_quantile must be less than high_quantile"}}, ["td"]}
              ], ""} =
               Parser.parse_commands(
                 "bf.reserve bf bad 100\r\n" <>
                   "cms.incrby cms a\r\n" <>
                   "topk.list tk BAD\r\n" <>
                   "tdigest.trimmed_mean td 0.9 0.1\r\n"
               )
    end

    test "emits command-specific AST atom for catalog commands not yet semantically specialized" do
      assert {:ok,
              [
                {:command, "BF.ADD", ["bf", "v"], {:bf_add, ["bf", "v"]}, ["bf"]}
              ], ""} =
               Parser.parse_commands("*3\r\n$6\r\nBF.ADD\r\n$2\r\nbf\r\n$1\r\nv\r\n")
    end

    test "extracts ACL/tracking keys in Rust command tuple" do
      assert {:ok,
              [
                {:command, "MSET", ["a", "1", "b", "2"], {:mset, ["a", "1", "b", "2"]},
                 ["a", "b"]},
                {:command, "BITOP", ["AND", "dst", "a", "b"], {:bitop, :band, "dst", ["a", "b"]},
                 ["dst", "a", "b"]},
                {:command, "JSON.MGET", ["a", "b", "$"], {:json_mget, ["a", "b"], []}, ["a", "b"]}
              ], ""} =
               Parser.parse_commands(
                 "*5\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n" <>
                   "*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$3\r\ndst\r\n$1\r\na\r\n$1\r\nb\r\n" <>
                   "*4\r\n$9\r\nJSON.MGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\n$\r\n"
               )
    end

    test "extracts XREAD stream keys in Rust command tuple" do
      input =
        "*8\r\n$5\r\nXREAD\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n$7\r\nSTREAMS\r\n$2\r\ns1\r\n$2\r\ns2\r\n$1\r\n0\r\n$1\r\n0\r\n"

      assert {:ok,
              [
                {:command, "XREAD", ["COUNT", "2", "STREAMS", "s1", "s2", "0", "0"],
                 {:xread, 2, :no_block, [{"s1", "0"}, {"s2", "0"}]}, ["s1", "s2"]}
              ], ""} = Parser.parse_commands(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Partial reads (incomplete data)
  # ---------------------------------------------------------------------------

  describe "partial reads" do
    test "returns empty list and full buffer for partial simple string" do
      assert {:ok, [], "+OK\r"} = Parser.parse("+OK\r")
    end

    test "returns empty list for partial bulk string header" do
      assert {:ok, [], "$5\r"} = Parser.parse("$5\r")
    end

    test "returns empty list for partial bulk string data" do
      assert {:ok, [], "$5\r\nhel"} = Parser.parse("$5\r\nhel")
    end

    test "returns empty list for partial bulk string missing trailing CRLF" do
      assert {:ok, [], "$5\r\nhello"} = Parser.parse("$5\r\nhello")
    end

    test "returns empty list for partial array" do
      assert {:ok, [], "*3\r\n:1\r\n:2\r\n"} = Parser.parse("*3\r\n:1\r\n:2\r\n")
    end

    test "returns empty list for partial integer" do
      assert {:ok, [], ":42"} = Parser.parse(":42")
    end

    test "returns empty list for empty input" do
      assert {:ok, [], ""} = Parser.parse("")
    end

    test "returns empty list for partial inline" do
      assert {:ok, [], "PING"} = Parser.parse("PING")
    end

    test "returns empty list for partial map" do
      # Map header says 2 entries but only 1 key provided
      assert {:ok, [], "%2\r\n$3\r\nfoo\r\n"} = Parser.parse("%2\r\n$3\r\nfoo\r\n")
    end

    test "server command parser preserves partial command frames at every split" do
      wire = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"

      for split <- 0..(byte_size(wire) - 1) do
        prefix = binary_part(wire, 0, split)
        assert {:ok, [], ^prefix} = Parser.parse_commands(prefix)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Pipelining
  # ---------------------------------------------------------------------------

  describe "pipelining" do
    test "parses multiple complete commands" do
      input = "+OK\r\n:42\r\n$5\r\nhello\r\n"
      assert {:ok, [{:simple, "OK"}, 42, "hello"], ""} = Parser.parse(input)
    end

    test "parses complete commands with trailing partial" do
      input = ":1\r\n:2\r\n:3\r"
      assert {:ok, [1, 2], ":3\r"} = Parser.parse(input)
    end

    test "parses multiple inline commands" do
      input = "PING\r\nSET foo bar\r\n"

      assert {:ok, [{:inline, ["PING"]}, {:inline, ["SET", "foo", "bar"]}], ""} =
               Parser.parse(input)
    end

    test "parses mixed typed and inline commands" do
      input = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n+OK\r\n"
      assert {:ok, [["SET", "foo", "bar"], {:simple, "OK"}], ""} = Parser.parse(input)
    end

    test "returns all complete from a large pipeline" do
      input = Enum.map_join(1..100, "", fn i -> ":#{i}\r\n" end)
      assert {:ok, values, ""} = Parser.parse(input)
      assert values == Enum.to_list(1..100)
    end

    test "handles pipeline where first message is complete and second is partial" do
      assert {:ok, [{:simple, "OK"}], "+HE"} = Parser.parse("+OK\r\n+HE")
    end
  end

  # ---------------------------------------------------------------------------
  # Nested types
  # ---------------------------------------------------------------------------

  describe "nested types" do
    test "array of maps" do
      input = "*2\r\n%1\r\n$1\r\na\r\n:1\r\n%1\r\n$1\r\nb\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == [%{"a" => 1}, %{"b" => 2}]
    end

    test "map of arrays" do
      input = "%1\r\n$4\r\nkeys\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
      assert {:ok, [%{"keys" => ["a", "b"]}], ""} = Parser.parse(input)
    end

    test "array containing sets" do
      input = "*1\r\n~2\r\n:1\r\n:2\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert result == [MapSet.new([1, 2])]
    end

    test "deeply nested structure" do
      input = "*1\r\n*1\r\n*1\r\n:42\r\n"
      assert {:ok, [[[[42]]]], ""} = Parser.parse(input)
    end

    test "map with nested map values" do
      input = "%1\r\n$5\r\nouter\r\n%1\r\n$5\r\ninner\r\n:1\r\n"
      assert {:ok, [%{"outer" => %{"inner" => 1}}], ""} = Parser.parse(input)
    end

    test "push containing a map" do
      input = ">2\r\n$7\r\nmessage\r\n%1\r\n$3\r\nfoo\r\n:1\r\n"
      assert {:ok, [{:push, ["message", %{"foo" => 1}]}], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "bulk string with length zero" do
      assert {:ok, [""], ""} = Parser.parse("$0\r\n\r\n")
    end

    test "array with single element" do
      assert {:ok, [[{:simple, "OK"}]], ""} = Parser.parse("*1\r\n+OK\r\n")
    end

    test "multiple nil values" do
      input = "_\r\n$-1\r\n*-1\r\n"
      assert {:ok, [nil, nil, nil], ""} = Parser.parse(input)
    end

    test "simple string that looks like a number" do
      assert {:ok, [{:simple, "42"}], ""} = Parser.parse("+42\r\n")
    end

    test "bulk string with unicode" do
      # "hello" in Japanese: 3 bytes each = 15 bytes
      str = "helloworld"
      len = byte_size(str)
      input = "$#{len}\r\n#{str}\r\n"
      assert {:ok, [^str], ""} = Parser.parse(input)
    end
  end

  # ---------------------------------------------------------------------------
  # Attribute type (|)
  # ---------------------------------------------------------------------------

  describe "attribute type" do
    test "parses an attribute type" do
      # |1 = one key-value pair: key=simple "key", value=integer 42
      input = "|1\r\n+key\r\n:42\r\n"
      assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}], ""} = Parser.parse(input)
    end

    test "parses attribute followed by a value" do
      input = "|1\r\n+key\r\n:42\r\n+OK\r\n"

      assert {:ok, [{:attribute, %{{:simple, "key"} => 42}}, {:simple, "OK"}], ""} =
               Parser.parse(input)
    end

    test "parses empty attribute" do
      assert {:ok, [{:attribute, %{}}], ""} = Parser.parse("|0\r\n")
    end

    test "rejects malformed attributes and command-mode attributes" do
      assert {:error, {:invalid_map_count, "-1"}} = Parser.parse("|-1\r\n")
      assert {:ok, [], "|1\r\n$1\r\nk\r\n"} = Parser.parse("|1\r\n$1\r\nk\r\n")

      assert {:error, :invalid_command_format} =
               Parser.parse_commands("|0\r\n*1\r\n$4\r\nPING\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Blob error — additional edge cases
  # ---------------------------------------------------------------------------

  describe "blob error edge cases" do
    test "blob error with negative length returns error" do
      assert {:error, {:invalid_blob_error_length, -1}} = Parser.parse("!-1\r\n\r\n")
    end

    test "blob error with length mismatch is incomplete (not enough data)" do
      # Header says 5 bytes but payload has 11 — the parser reads only 5 bytes
      # then expects CRLF at position 5. "hello world" has " " at position 5, not CRLF.
      assert {:error, :bulk_crlf_missing} = Parser.parse("!5\r\nhello world\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Verbatim string — additional edge cases
  # ---------------------------------------------------------------------------

  describe "verbatim string edge cases" do
    test "verbatim string with length < 4 returns error" do
      assert {:error, {:invalid_verbatim_length, 3}} = Parser.parse("=3\r\nABC\r\n")
    end

    test "verbatim string with missing colon separator returns error" do
      # Length 4, payload "ABCD" — no colon after 3-byte encoding
      assert {:error, :invalid_verbatim_payload} = Parser.parse("=4\r\nABCD\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Null — additional edge cases
  # ---------------------------------------------------------------------------

  describe "null edge cases" do
    test "null with non-empty content returns error" do
      assert {:error, {:invalid_null, "garbage"}} = Parser.parse("_garbage\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Double — additional edge cases
  # ---------------------------------------------------------------------------

  describe "double edge cases" do
    test "parses NaN double (lowercase)" do
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end

    test "parses NaN double (mixed case)" do
      assert {:ok, [:nan], ""} = Parser.parse(",NaN\r\n")
    end

    test "parses NaN double (uppercase)" do
      assert {:ok, [:nan], ""} = Parser.parse(",NAN\r\n")
    end

    test "parses scientific notation double" do
      assert {:ok, [1.5e10], ""} = Parser.parse(",1.5e10\r\n")
    end

    test "parses scientific notation without decimal point" do
      assert {:ok, [1.0e5], ""} = Parser.parse(",1e5\r\n")
    end

    test "parses integer-form double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "invalid double returns error" do
      assert {:error, {:invalid_double, "notafloat"}} = Parser.parse(",notafloat\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Integer — additional edge cases
  # ---------------------------------------------------------------------------

  describe "integer edge cases" do
    test "invalid integer returns error" do
      assert {:error, {:invalid_integer, "abc"}} = Parser.parse(":abc\r\n")
    end

    test "float-like value in integer position returns error" do
      assert {:error, {:invalid_integer, "3.14"}} = Parser.parse(":3.14\r\n")
    end
  end

  describe "Flow command AST" do
    test "parses Flow write commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "FLOW.CREATE",
                 [
                   "flow-1",
                   "TYPE",
                   "checkout",
                   "STATE",
                   "queued",
                   "RUN_AT",
                   "1000",
                   "PRIORITY",
                   "2",
                   "PARTITION",
                   "tenant-a",
                   "RETENTION_TTL",
                   "5000",
                   "HISTORY_MAX_EVENTS",
                   "10",
                   "IDEMPOTENT",
                   "true"
                 ],
                 {:flow_create, "flow-1",
                  [
                    type: "checkout",
                    state: "queued",
                    run_at_ms: 1000,
                    priority: 2,
                    partition_key: "tenant-a",
                    retention_ttl_ms: 5000,
                    history_max_events: 10,
                    idempotent: true
                  ]}, ["tenant-a"]},
                {:command, "FLOW.CLAIM_DUE",
                 [
                   "checkout",
                   "WORKER",
                   "worker-a",
                   "LEASE_MS",
                   "30000",
                   "LIMIT",
                   "100",
                   "NOW",
                   "1000"
                 ],
                 {:flow_claim_due, "checkout",
                  [worker: "worker-a", lease_ms: 30000, limit: 100, now_ms: 1000]}, ["checkout"]},
                {:command, "FLOW.RECLAIM",
                 [
                   "checkout",
                   "WORKER",
                   "worker-b",
                   "LEASE_MS",
                   "30000",
                   "LIMIT",
                   "10",
                   "NOW",
                   "2000"
                 ],
                 {:flow_reclaim, "checkout",
                  [worker: "worker-b", lease_ms: 30000, limit: 10, now_ms: 2000]}, ["checkout"]},
                {:command, "FLOW.COMPLETE",
                 ["flow-1", "lease-1", "FENCING", "1", "RESULT", "result-1"],
                 {:flow_complete, "flow-1", "lease-1", [fencing_token: 1, result: "result-1"]},
                 ["flow-1"]},
                {:command, "FLOW.TRANSITION",
                 ["flow-1", "queued", "running", "FENCING", "1", "LEASE_TOKEN", "lease-1"],
                 {:flow_transition, "flow-1", "queued", "running",
                  [fencing_token: 1, lease_token: "lease-1"]}, ["flow-1"]},
                {:command, "FLOW.TRANSITION_MANY",
                 [
                   "tenant-a",
                   "queued",
                   "running",
                   "RUN_AT",
                   "2000",
                   "NOW",
                   "1000",
                   "ITEMS",
                   "flow-1",
                   "1",
                   "-",
                   "flow-2",
                   "2",
                   "lease-2"
                 ],
                 {:flow_transition_many, "tenant-a", "queued", "running",
                  [
                    {:id, "flow-1", :fencing_token, 1, :lease_token, nil},
                    {:id, "flow-2", :fencing_token, 2, :lease_token, "lease-2"}
                  ], [run_at_ms: 2000, now_ms: 1000]}, ["tenant-a"]},
                {:command, "FLOW.RETRY", ["flow-1", "lease-1", "FENCING", "1", "RUN_AT", "2000"],
                 {:flow_retry, "flow-1", "lease-1", [fencing_token: 1, run_at_ms: 2000]},
                 ["flow-1"]},
                {:command, "FLOW.FAIL", ["flow-1", "lease-1", "FENCING", "1", "ERROR", "err-1"],
                 {:flow_fail, "flow-1", "lease-1", [fencing_token: 1, error: "err-1"]},
                 ["flow-1"]},
                {:command, "FLOW.CANCEL", ["flow-1", "FENCING", "1", "REASON_REF", "reason-1"],
                 {:flow_cancel, "flow-1", [fencing_token: 1, reason_ref: "reason-1"]},
                 ["flow-1"]},
                {:command, "FLOW.REWIND",
                 [
                   "flow-1",
                   "TO_EVENT",
                   "1000-1",
                   "RUN_AT",
                   "5000",
                   "EXPECT_STATE",
                   "completed",
                   "REASON_REF",
                   "manual"
                 ],
                 {:flow_rewind, "flow-1",
                  [
                    to_event: "1000-1",
                    run_at_ms: 5000,
                    expect_state: "completed",
                    reason_ref: "manual"
                  ]}, ["flow-1"]}
              ], ""} =
               Parser.parse_commands(
                 "flow.create flow-1 TYPE checkout STATE queued RUN_AT 1000 PRIORITY 2 PARTITION tenant-a RETENTION_TTL 5000 HISTORY_MAX_EVENTS 10 IDEMPOTENT true\r\n" <>
                   "flow.claim_due checkout WORKER worker-a LEASE_MS 30000 LIMIT 100 NOW 1000\r\n" <>
                   "flow.reclaim checkout WORKER worker-b LEASE_MS 30000 LIMIT 10 NOW 2000\r\n" <>
                   "flow.complete flow-1 lease-1 FENCING 1 RESULT result-1\r\n" <>
                   "flow.transition flow-1 queued running FENCING 1 LEASE_TOKEN lease-1\r\n" <>
                   "flow.transition_many tenant-a queued running RUN_AT 2000 NOW 1000 ITEMS flow-1 1 - flow-2 2 lease-2\r\n" <>
                   "flow.retry flow-1 lease-1 FENCING 1 RUN_AT 2000\r\n" <>
                   "flow.fail flow-1 lease-1 FENCING 1 ERROR err-1\r\n" <>
                   "flow.cancel flow-1 FENCING 1 REASON_REF reason-1\r\n" <>
                   "flow.rewind flow-1 TO_EVENT 1000-1 RUN_AT 5000 EXPECT_STATE completed REASON_REF manual\r\n"
               )
    end

    test "parses Flow read commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "FLOW.GET", ["flow-1", "PARTITION", "GLOBAL"],
                 {:flow_get, "flow-1", []}, ["GLOBAL"]},
                {:command, "FLOW.GET", ["flow-1", "NOPAYLOAD"],
                 {:flow_get, "flow-1", [payload: false]}, ["flow-1"]},
                {:command, "FLOW.GET", ["flow-1", "PAYLOAD", "MAXBYTES", "4096"],
                 {:flow_get, "flow-1", [payload: true, payload_max_bytes: 4096]}, ["flow-1"]},
                {:command, "FLOW.LIST", ["checkout", "STATE", "queued", "COUNT", "25"],
                 {:flow_list, "checkout", [state: "queued", count: 25]}, ["checkout"]},
                {:command, "FLOW.CLAIM_DUE",
                 ["checkout", "WORKER", "worker-a", "LIMIT", "10", "PAYLOAD", "MAXBYTES", "2048"],
                 {:flow_claim_due, "checkout",
                  [worker: "worker-a", limit: 10, payload: true, payload_max_bytes: 2048]},
                 ["checkout"]},
                {:command, "FLOW.INFO", ["checkout", "PARTITION", "tenant-a"],
                 {:flow_info, "checkout", [partition_key: "tenant-a"]}, ["tenant-a"]},
                {:command, "FLOW.STUCK", ["checkout", "OLDER_THAN", "1000", "COUNT", "10"],
                 {:flow_stuck, "checkout", [older_than_ms: 1000, count: 10]}, ["checkout"]},
                {:command, "FLOW.HISTORY", ["flow-1", "COUNT", "10"],
                 {:flow_history, "flow-1", [count: 10]}, ["flow-1"]}
              ], ""} =
               Parser.parse_commands(
                 "flow.get flow-1 PARTITION GLOBAL\r\n" <>
                   "flow.get flow-1 NOPAYLOAD\r\n" <>
                   "flow.get flow-1 PAYLOAD MAXBYTES 4096\r\n" <>
                   "flow.list checkout STATE queued COUNT 25\r\n" <>
                   "flow.claim_due checkout WORKER worker-a LIMIT 10 PAYLOAD MAXBYTES 2048\r\n" <>
                   "flow.info checkout PARTITION tenant-a\r\n" <>
                   "flow.stuck checkout OLDER_THAN 1000 COUNT 10\r\n" <>
                   "flow.history flow-1 COUNT 10\r\n"
               )
    end

    test "parses mixed-partition Flow many commands into typed Rust AST" do
      assert {:ok,
              [
                {:command, "FLOW.CREATE_MANY", ["tenant-a", "ITEMS", "flow-min", "payload-min"],
                 {:flow_create_many, "tenant-a", [{:id, "flow-min", :payload, "payload-min"}],
                  []}, ["tenant-a"]},
                {:command, "FLOW.CREATE_MANY",
                 [
                   "MIXED",
                   "TYPE",
                   "iot",
                   "RUN_AT",
                   "1000",
                   "IDEMPOTENT",
                   "true",
                   "ITEMS",
                   "flow-1",
                   "device-a",
                   "payload-1",
                   "flow-2",
                   "device-b",
                   "payload-2"
                 ],
                 {:flow_create_many, nil,
                  [
                    {"flow-1", [partition_key: "device-a", payload: "payload-1"]},
                    {"flow-2", [partition_key: "device-b", payload: "payload-2"]}
                  ], [type: "iot", run_at_ms: 1000, idempotent: true]}, ["device-a", "device-b"]},
                {:command, "FLOW.TRANSITION_MANY",
                 [
                   "MIXED",
                   "queued",
                   "ready",
                   "RUN_AT",
                   "2000",
                   "ITEMS",
                   "flow-1",
                   "device-a",
                   "1",
                   "-",
                   "flow-2",
                   "device-b",
                   "2",
                   "lease-2"
                 ],
                 {:flow_transition_many, nil, "queued", "ready",
                  [
                    {"flow-1", [partition_key: "device-a", fencing_token: 1]},
                    {"flow-2",
                     [partition_key: "device-b", fencing_token: 2, lease_token: "lease-2"]}
                  ], [run_at_ms: 2000]}, ["device-a", "device-b"]}
              ], ""} =
               Parser.parse_commands(
                 "flow.create_many tenant-a ITEMS flow-min payload-min\r\n" <>
                   "flow.create_many MIXED TYPE iot RUN_AT 1000 IDEMPOTENT true ITEMS flow-1 device-a payload-1 flow-2 device-b payload-2\r\n" <>
                   "flow.transition_many MIXED queued ready RUN_AT 2000 ITEMS flow-1 device-a 1 - flow-2 device-b 2 lease-2\r\n"
               )
    end

    test "parses mixed-partition Flow spawn_children into typed Rust AST" do
      assert {:ok,
              [
                {:command, "FLOW.SPAWN_CHILDREN",
                 [
                   "parent-1",
                   "GROUP",
                   "fanout",
                   "PARTITION",
                   "parent-p",
                   "FENCING",
                   "1",
                   "ITEMS",
                   "MIXED",
                   "child-a",
                   "device-a",
                   "child",
                   "payload-a",
                   "child-b",
                   "device-b",
                   "child",
                   "payload-b"
                 ],
                 {:flow_spawn_children, "parent-1",
                  [
                    {"child-a", [partition_key: "device-a", type: "child", payload: "payload-a"]},
                    {"child-b", [partition_key: "device-b", type: "child", payload: "payload-b"]}
                  ], [group_id: "fanout", partition_key: "parent-p", fencing_token: 1]},
                 ["parent-p", "device-a", "device-b"]}
              ], ""} =
               Parser.parse_commands(
                 "flow.spawn_children parent-1 GROUP fanout PARTITION parent-p FENCING 1 ITEMS MIXED child-a device-a child payload-a child-b device-b child payload-b\r\n"
               )
    end

    test "keeps Flow option parse errors inside AST" do
      huge_ref = String.duplicate("p", 4_097)

      assert {:ok,
              [
                {:command, "FLOW.CREATE", ["f", "TYPE", "t", "PRIORITY", "x"],
                 {:flow_create, "f", {:error, "ERR value is not an integer or out of range"}},
                 ["f"]},
                {:command, "FLOW.CREATE", ["f", "TYPE", "t", "PAYLOAD_REF", ^huge_ref],
                 {:flow_create, "f", {:error, "ERR syntax error"}}, ["f"]},
                {:command, "FLOW.CLAIM_DUE", ["t", "WORKER", "w", "LIMIT", "0"],
                 {:flow_claim_due, "t", {:error, "ERR flow limit must be a positive integer"}},
                 ["t"]},
                {:command, "FLOW.COMPLETE", ["f", "l"],
                 {:flow_complete,
                  {:error, "ERR wrong number of arguments for 'flow.complete' command"}}, ["f"]},
                {:command, "FLOW.REWIND", ["f", "RUN_AT", "1"],
                 {:flow_rewind, "f", {:error, "ERR flow to_event is required"}}, ["f"]}
              ], ""} =
               Parser.parse_commands(
                 "flow.create f TYPE t PRIORITY x\r\n" <>
                   "flow.create f TYPE t PAYLOAD_REF #{huge_ref}\r\n" <>
                   "flow.claim_due t WORKER w LIMIT 0\r\n" <>
                   "flow.complete f l\r\n" <>
                   "flow.rewind f RUN_AT 1\r\n"
               )
    end
  end

  describe "cluster command AST parsing" do
    test "parses CLUSTER.ENABLE through the Rust command catalog" do
      assert {:ok,
              [
                {:command, "CLUSTER.ENABLE", [], {:cluster_enable, []}, []},
                {:command, "CLUSTER.ENABLE", ["dryrun"], {:cluster_enable, ["dryrun"]}, []}
              ], ""} =
               Parser.parse_commands("cluster.enable\r\ncluster.enable dryrun\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean — additional edge cases (already one test, adding invalid)
  # ---------------------------------------------------------------------------

  describe "boolean edge cases" do
    test "invalid boolean value returns error" do
      assert {:error, {:invalid_boolean, "1"}} = Parser.parse("#1\r\n")
    end

    test "empty boolean returns error" do
      assert {:error, {:invalid_boolean, ""}} = Parser.parse("#\r\n")
    end
  end
end
