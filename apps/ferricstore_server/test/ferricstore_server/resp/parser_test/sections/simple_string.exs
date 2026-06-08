defmodule FerricstoreServer.Resp.ParserTest.Sections.SimpleString do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser

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
  describe "null" do
    test "parses null" do
      assert {:ok, [nil], ""} = Parser.parse("_\r\n")
    end
  end
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
  describe "blob error" do
    test "parses a blob error" do
      assert {:ok, [{:error, "SYNTAX invalid syntax"}], ""} =
               Parser.parse("!21\r\nSYNTAX invalid syntax\r\n")
    end

    test "parses an empty blob error" do
      assert {:ok, [{:error, ""}], ""} = Parser.parse("!0\r\n\r\n")
    end
  end
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
    end
  end
end
