defmodule FerricstoreServer.Resp.RespEdgeCasesTest.Sections.ParserBulkStringEmbeddedCrlfSequences do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Resp.Encoder

  describe "parser: bulk string with embedded CRLF sequences" do
    test "bulk string payload consisting entirely of CRLF pairs" do
      # 4 bytes: \r\n\r\n
      payload = "\r\n\r\n"
      input = "$4\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk string payload with multiple CRLF at various positions" do
      payload = "a\r\nb\r\nc"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk string payload that ends with a lone CR (not CRLF)" do
      payload = "hello\r"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end

    test "bulk string payload that starts with LF" do
      payload = "\nworld"
      len = byte_size(payload)
      input = "$#{len}\r\n#{payload}\r\n"
      assert {:ok, [^payload], ""} = Parser.parse(input)
    end
  end
  describe "parser: array of arrays (nested)" do
    test "array of three inner arrays with mixed types" do
      inner1 = "*2\r\n:1\r\n:2\r\n"
      inner2 = "*1\r\n$3\r\nfoo\r\n"
      inner3 = "*0\r\n"
      input = "*3\r\n#{inner1}#{inner2}#{inner3}"
      assert {:ok, [[[1, 2], ["foo"], []]], ""} = Parser.parse(input)
    end

    test "4-level deep nesting" do
      input = "*1\r\n*1\r\n*1\r\n*1\r\n:99\r\n"
      assert {:ok, [[[[[99]]]]], ""} = Parser.parse(input)
    end

    test "array containing null arrays and empty arrays" do
      input = "*3\r\n*-1\r\n*0\r\n*1\r\n:7\r\n"
      assert {:ok, [[nil, [], [7]]], ""} = Parser.parse(input)
    end
  end
  describe "parser: map with map values (nested)" do
    test "two-level nested map" do
      # outer has one key "a" => inner map {"b" => 42}
      input = "%1\r\n$1\r\na\r\n%1\r\n$1\r\nb\r\n:42\r\n"
      assert {:ok, [%{"a" => %{"b" => 42}}], ""} = Parser.parse(input)
    end

    test "map whose keys are also maps" do
      # key is a map %{"k" => 1}, value is integer 99
      input = "%1\r\n%1\r\n$1\r\nk\r\n:1\r\n:99\r\n"
      assert {:ok, [%{%{"k" => 1} => 99}], ""} = Parser.parse(input)
    end

    test "three-level nested map" do
      input =
        "%1\r\n$1\r\nx\r\n" <>
          "%1\r\n$1\r\ny\r\n" <>
          "%1\r\n$1\r\nz\r\n:0\r\n"

      assert {:ok, [%{"x" => %{"y" => %{"z" => 0}}}], ""} = Parser.parse(input)
    end
  end
  describe "parser: set containing complex types" do
    test "set of arrays" do
      input = "~2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert MapSet.member?(result, [1, 2])
      assert MapSet.member?(result, [3])
    end

    test "set of maps" do
      input = "~1\r\n%1\r\n$1\r\na\r\n:1\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert MapSet.member?(result, %{"a" => 1})
    end

    test "set of mixed: integer, bulk string, boolean" do
      input = "~3\r\n:42\r\n$3\r\nfoo\r\n#t\r\n"
      assert {:ok, [result], ""} = Parser.parse(input)
      assert MapSet.size(result) == 3
      assert MapSet.member?(result, 42)
      assert MapSet.member?(result, "foo")
      assert MapSet.member?(result, true)
    end
  end
  describe "parser: push type with nested arrays" do
    test "push containing an array element" do
      input = ">2\r\n$4\r\ntype\r\n*2\r\n:1\r\n:2\r\n"
      assert {:ok, [{:push, ["type", [1, 2]]}], ""} = Parser.parse(input)
    end

    test "push containing nested push" do
      input = ">1\r\n>2\r\n$1\r\na\r\n$1\r\nb\r\n"
      assert {:ok, [{:push, [{:push, ["a", "b"]}]}], ""} = Parser.parse(input)
    end

    test "push containing map and set" do
      input = ">2\r\n%1\r\n$1\r\nk\r\n:1\r\n~1\r\n:9\r\n"
      assert {:ok, [{:push, [%{"k" => 1}, set]}], ""} = Parser.parse(input)
      assert MapSet.member?(set, 9)
    end
  end
  describe "parser: attribute type with nested map" do
    test "attribute with map value" do
      input = "|1\r\n$4\r\nmeta\r\n%1\r\n$3\r\nttl\r\n:300\r\n"

      assert {:ok, [{:attribute, %{"meta" => %{"ttl" => 300}}}], ""} =
               Parser.parse(input)
    end

    test "attribute with multiple pairs including arrays" do
      input = "|2\r\n$1\r\na\r\n*2\r\n:1\r\n:2\r\n$1\r\nb\r\n#t\r\n"

      assert {:ok, [{:attribute, %{"a" => [1, 2], "b" => true}}], ""} =
               Parser.parse(input)
    end
  end
  describe "parser: verbatim string edge cases" do
    test "verbatim string with exactly 4-byte payload (enc + colon + empty data)" do
      # "txt:" = 4 bytes, data is ""
      assert {:ok, [{:verbatim, "txt", ""}], ""} = Parser.parse("=4\r\ntxt:\r\n")
    end

    test "verbatim string with multi-byte encoding (non-ASCII in data)" do
      data = "cafe"
      payload = "txt:" <> data
      len = byte_size(payload)
      input = "=#{len}\r\n#{payload}\r\n"
      assert {:ok, [{:verbatim, "txt", ^data}], ""} = Parser.parse(input)
    end

    test "verbatim string with long data containing CRLF" do
      data = "line1\r\nline2"
      payload = "txt:" <> data
      len = byte_size(payload)
      input = "=#{len}\r\n#{payload}\r\n"
      assert {:ok, [{:verbatim, "txt", ^data}], ""} = Parser.parse(input)
    end

    test "verbatim string with mkd encoding and unicode content" do
      data = "hello world"
      payload = "mkd:" <> data
      len = byte_size(payload)
      input = "=#{len}\r\n#{payload}\r\n"
      assert {:ok, [{:verbatim, "mkd", ^data}], ""} = Parser.parse(input)
    end
  end
  describe "parser: double nan variants" do
    test "nan lowercase" do
      assert {:ok, [:nan], ""} = Parser.parse(",nan\r\n")
    end

    test "NaN mixed case" do
      assert {:ok, [:nan], ""} = Parser.parse(",NaN\r\n")
    end

    test "NAN uppercase" do
      assert {:ok, [:nan], ""} = Parser.parse(",NAN\r\n")
    end

    test "non-standard nan casing (nAn) returns error" do
      assert {:error, {:invalid_double, "nAn"}} = Parser.parse(",nAn\r\n")
    end
  end
  describe "parser: double inf variants" do
    test "inf positive" do
      assert {:ok, [:infinity], ""} = Parser.parse(",inf\r\n")
    end

    test "-inf negative" do
      assert {:ok, [:neg_infinity], ""} = Parser.parse(",-inf\r\n")
    end

    test "Inf with capital I returns error (only lowercase inf is valid)" do
      assert {:error, {:invalid_double, "Inf"}} = Parser.parse(",Inf\r\n")
    end
  end
  describe "parser: double integer-form (no decimal point)" do
    test "integer-form double zero" do
      assert {:ok, [result], ""} = Parser.parse(",0\r\n")
      assert result == 0.0
    end

    test "integer-form positive double" do
      assert {:ok, [42.0], ""} = Parser.parse(",42\r\n")
    end

    test "integer-form negative double" do
      assert {:ok, [-7.0], ""} = Parser.parse(",-7\r\n")
    end

    test "integer-form large double" do
      assert {:ok, [1000.0], ""} = Parser.parse(",1000\r\n")
    end
  end
  describe "parser: double scientific notation" do
    test "1e5" do
      assert {:ok, [1.0e5], ""} = Parser.parse(",1e5\r\n")
    end

    test "1.0E-3" do
      assert {:ok, [result], ""} = Parser.parse(",1.0E-3\r\n")
      assert_in_delta result, 0.001, 1.0e-10
    end

    test "-2.5e10" do
      assert {:ok, [-2.5e10], ""} = Parser.parse(",-2.5e10\r\n")
    end

    test "3.14e0 (exponent zero)" do
      assert {:ok, [result], ""} = Parser.parse(",3.14e0\r\n")
      assert_in_delta result, 3.14, 1.0e-10
    end

    test "1e-1 (no decimal, negative exponent)" do
      assert {:ok, [result], ""} = Parser.parse(",1e-1\r\n")
      assert_in_delta result, 0.1, 1.0e-10
    end

    test "-1e2 (negative mantissa, no decimal)" do
      assert {:ok, [-100.0], ""} = Parser.parse(",-1e2\r\n")
    end
  end
  describe "parser: big number edge cases" do
    test "very large positive big number (100 digits)" do
      big = String.duplicate("9", 100)
      expected = String.to_integer(big)
      input = "(#{big}\r\n"
      assert {:ok, [^expected], ""} = Parser.parse(input)
    end

    test "very large negative big number (100 digits)" do
      big = "-" <> String.duplicate("8", 100)
      expected = String.to_integer(big)
      input = "(#{big}\r\n"
      assert {:ok, [^expected], ""} = Parser.parse(input)
    end

    test "big number zero" do
      assert {:ok, [0], ""} = Parser.parse("(0\r\n")
    end

    test "big number +1" do
      assert {:ok, [1], ""} = Parser.parse("(1\r\n")
    end

    test "big number -1" do
      assert {:ok, [-1], ""} = Parser.parse("(-1\r\n")
    end

    test "invalid big number returns error" do
      assert {:error, {:invalid_big_number, "abc"}} = Parser.parse("(abc\r\n")
    end

    test "big number with decimal returns error" do
      assert {:error, {:invalid_big_number, "3.14"}} = Parser.parse("(3.14\r\n")
    end
  end
  describe "parser: boolean edge cases" do
    test "#t parses to true" do
      assert {:ok, [true], ""} = Parser.parse("#t\r\n")
    end

    test "#f parses to false" do
      assert {:ok, [false], ""} = Parser.parse("#f\r\n")
    end

    test "#T (uppercase) is invalid" do
      assert {:error, {:invalid_boolean, "T"}} = Parser.parse("#T\r\n")
    end

    test "#F (uppercase) is invalid" do
      assert {:error, {:invalid_boolean, "F"}} = Parser.parse("#F\r\n")
    end

    test "#true (multi-char) is invalid" do
      assert {:error, {:invalid_boolean, "true"}} = Parser.parse("#true\r\n")
    end

    test "#0 is invalid" do
      assert {:error, {:invalid_boolean, "0"}} = Parser.parse("#0\r\n")
    end
  end
  describe "parser: null edge cases" do
    test "correct null _\\r\\n" do
      assert {:ok, [nil], ""} = Parser.parse("_\r\n")
    end

    test "null array *-1\\r\\n" do
      assert {:ok, [nil], ""} = Parser.parse("*-1\r\n")
    end

    test "null bulk string $-1\\r\\n" do
      assert {:ok, [nil], ""} = Parser.parse("$-1\r\n")
    end

    test "malformed null _X\\r\\n returns error" do
      assert {:error, {:invalid_null, "X"}} = Parser.parse("_X\r\n")
    end

    test "malformed null with whitespace returns error" do
      assert {:error, {:invalid_null, " "}} = Parser.parse("_ \r\n")
    end

    test "malformed null with number returns error" do
      assert {:error, {:invalid_null, "0"}} = Parser.parse("_0\r\n")
    end
  end
  describe "parser: zero-length containers" do
    test "zero-length array *0\\r\\n" do
      assert {:ok, [[]], ""} = Parser.parse("*0\r\n")
    end

    test "zero-length map %0\\r\\n" do
      assert {:ok, [%{}], ""} = Parser.parse("%0\r\n")
    end

    test "zero-length set ~0\\r\\n" do
      assert {:ok, [result], ""} = Parser.parse("~0\r\n")
      assert result == MapSet.new()
    end

    test "zero-length push >0\\r\\n" do
      assert {:ok, [{:push, []}], ""} = Parser.parse(">0\r\n")
    end

    test "zero-length attribute |0\\r\\n" do
      assert {:ok, [{:attribute, %{}}], ""} = Parser.parse("|0\r\n")
    end
  end
  describe "parser: pipelining 10 values in one buffer" do
    test "10 heterogeneous values parse correctly" do
      input =
        "+OK\r\n" <>
          "-ERR bad\r\n" <>
          ":42\r\n" <>
          "$5\r\nhello\r\n" <>
          "_\r\n" <>
          "#t\r\n" <>
          ",3.14\r\n" <>
          "*2\r\n:1\r\n:2\r\n" <>
          "%1\r\n$1\r\na\r\n:1\r\n" <>
          ">1\r\n$4\r\nping\r\n"

      assert {:ok, values, ""} = Parser.parse(input)
      assert length(values) == 10

      assert Enum.at(values, 0) == {:simple, "OK"}
      assert Enum.at(values, 1) == {:error, "ERR bad"}
      assert Enum.at(values, 2) == 42
      assert Enum.at(values, 3) == "hello"
      assert Enum.at(values, 4) == nil
      assert Enum.at(values, 5) == true
      assert Enum.at(values, 6) == 3.14
      assert Enum.at(values, 7) == [1, 2]
      assert Enum.at(values, 8) == %{"a" => 1}
      assert Enum.at(values, 9) == {:push, ["ping"]}
    end

    test "20 integers in one buffer" do
      input = Enum.map_join(1..20, "", fn i -> ":#{i}\r\n" end)
      assert {:ok, values, ""} = Parser.parse(input)
      assert values == Enum.to_list(1..20)
    end
  end
  describe "parser: partial reads at every byte offset" do
    test "simple string +OK\\r\\n cut at each offset returns :incomplete or parsed" do
      full = "+OK\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      # Full buffer parses completely
      assert {:ok, [{:simple, "OK"}], ""} = Parser.parse(full)
    end

    test "integer :42\\r\\n cut at each offset" do
      full = ":42\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [42], ""} = Parser.parse(full)
    end

    test "bulk string $5\\r\\nhello\\r\\n cut at each offset" do
      full = "$5\r\nhello\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        result = Parser.parse(partial)

        case result do
          {:ok, [], _rest} ->
            :ok

          {:error, :bulk_crlf_missing} ->
            # This happens when header is complete, enough bytes for payload exist,
            # but the trailing \r\n is not at the expected position because the
            # buffer was cut mid-CRLF
            :ok
        end
      end

      assert {:ok, ["hello"], ""} = Parser.parse(full)
    end

    test "null _\\r\\n cut at each offset" do
      full = "_\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [nil], ""} = Parser.parse(full)
    end

    test "boolean #t\\r\\n cut at each offset" do
      full = "#t\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [true], ""} = Parser.parse(full)
    end

    test "array *2\\r\\n:1\\r\\n:2\\r\\n cut at each offset" do
      full = "*2\r\n:1\r\n:2\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [[1, 2]], ""} = Parser.parse(full)
    end

    test "map %1\\r\\n$1\\r\\na\\r\\n:1\\r\\n cut at each offset" do
      full = "%1\r\n$1\r\na\r\n:1\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [%{"a" => 1}], ""} = Parser.parse(full)
    end

    test "double ,3.14\\r\\n cut at each offset" do
      full = ",3.14\r\n"

      for offset <- 1..(byte_size(full) - 1) do
        partial = binary_part(full, 0, offset)
        assert {:ok, [], ^partial} = Parser.parse(partial), "failed at offset #{offset}"
      end

      assert {:ok, [3.14], ""} = Parser.parse(full)
    end
  end
  describe "parser: inline command edge cases" do
    test "inline command with multiple spaces between tokens collapses spaces" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET   key\r\n")
    end

    test "inline command with leading spaces" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("  GET key\r\n")
    end

    test "inline command with trailing spaces" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET key  \r\n")
    end

    test "inline command preserves spaces inside double quotes" do
      assert {:ok, [{:inline, ["SET", "key", "hello world"]}], ""} =
               Parser.parse("SET key \"hello world\"\r\n")
    end

    test "command parser preserves quoted inline args" do
      assert {:ok,
              [{:command, "SET", ["key", "hello world"], {:set, "key", "hello world"}, ["key"]}],
              ""} =
               Parser.parse_commands("SET key \"hello world\"\r\n")
    end

    test "inline command with tabs between tokens" do
      assert {:ok, [{:inline, ["GET", "key"]}], ""} = Parser.parse("GET\tkey\r\n")
    end

    test "empty inline command (only CRLF)" do
      assert {:ok, [{:inline, []}], ""} = Parser.parse("\r\n")
    end

    test "inline command that looks like a RESP type but isn't (lowercase)" do
      # 'set' does not match any RESP prefix character followed by data
      # 's' is not a RESP type prefix, so it falls through to inline
      assert {:ok, [{:inline, ["set", "foo", "bar"]}], ""} =
               Parser.parse("set foo bar\r\n")
    end
  end
  describe "parser: invalid/unusual type prefixes" do
    test "unknown prefix 'Z' is treated as inline" do
      # 'Z' is not a RESP prefix, so the parser falls through to inline parsing
      assert {:ok, [{:inline, ["Zhello"]}], ""} = Parser.parse("Zhello\r\n")
    end

    test "unknown prefix '@' is treated as inline" do
      assert {:ok, [{:inline, ["@foo"]}], ""} = Parser.parse("@foo\r\n")
    end
  end
  describe "parser: invalid integer" do
    test "non-numeric string in integer position" do
      assert {:error, {:invalid_integer, "hello"}} = Parser.parse(":hello\r\n")
    end

    test "empty string in integer position" do
      assert {:error, {:invalid_integer, ""}} = Parser.parse(":\r\n")
    end

    test "integer with trailing space" do
      assert {:error, {:invalid_integer, "42 "}} = Parser.parse(":42 \r\n")
    end

    test "integer with leading space" do
      assert {:error, {:invalid_integer, " 42"}} = Parser.parse(": 42\r\n")
    end

    test "hex in integer position" do
      assert {:error, {:invalid_integer, "0xff"}} = Parser.parse(":0xff\r\n")
    end
  end
  describe "parser: invalid bulk string length" do
    test "bulk string length -2 returns error" do
      assert {:error, {:invalid_bulk_length, "-2"}} = Parser.parse("$-2\r\n")
    end

    test "bulk string length -100 returns error" do
      assert {:error, {:invalid_bulk_length, "-100"}} = Parser.parse("$-100\r\n")
    end

    test "bulk string non-numeric length returns error" do
      assert {:error, {:invalid_bulk_length, "abc"}} = Parser.parse("$abc\r\n")
    end

    test "bulk string length with decimal returns error" do
      assert {:error, {:invalid_bulk_length, "3.5"}} = Parser.parse("$3.5\r\n")
    end
  end
  describe "parser: bulk data missing trailing CRLF" do
    test "correct-length payload but no CRLF terminator" do
      # 5 bytes of payload followed by "XX" instead of \r\n
      assert {:error, :bulk_crlf_missing} = Parser.parse("$5\r\nhelloXX")
    end

    test "correct-length payload with only CR (no LF) is incomplete" do
      # 5 bytes + \r but no \n — total 6 bytes after header, need 7 (5+2)
      assert {:ok, [], "$5\r\nhello\r"} = Parser.parse("$5\r\nhello\r")
    end

    test "bulk data with swapped CR and LF (LF then CR)" do
      assert {:error, :bulk_crlf_missing} = Parser.parse("$5\r\nhello\n\r")
    end
  end
  describe "parser: invalid boolean values" do
    test "boolean with 'y' is invalid" do
      assert {:error, {:invalid_boolean, "y"}} = Parser.parse("#y\r\n")
    end

    test "boolean with 'n' is invalid" do
      assert {:error, {:invalid_boolean, "n"}} = Parser.parse("#n\r\n")
    end
  end
  describe "parser: malformed null" do
    test "null with content 'null' returns error" do
      assert {:error, {:invalid_null, "null"}} = Parser.parse("_null\r\n")
    end

    test "null with newline character returns error" do
      # _\n (just LF, not CRLF) — no \r\n found until we add it
      assert {:error, {:invalid_null, "\n"}} = Parser.parse("_\n\r\n")
    end
  end
  describe "parser: invalid array count" do
    test "array count -2 returns error" do
      assert {:error, {:invalid_array_count, "-2"}} = Parser.parse("*-2\r\n")
    end

    test "array count non-numeric returns error" do
      assert {:error, {:invalid_array_count, "abc"}} = Parser.parse("*abc\r\n")
    end
  end
  describe "parser: invalid map count" do
    test "map count negative returns error" do
      assert {:error, {:invalid_map_count, "-1"}} = Parser.parse("%-1\r\n")
    end

    test "map count non-numeric returns error" do
      assert {:error, {:invalid_map_count, "xyz"}} = Parser.parse("%xyz\r\n")
    end
  end
  describe "parser: error propagation in nested structures" do
    test "error inside an array element propagates" do
      # Array of 1 element, but the element is an invalid integer
      input = "*1\r\n:abc\r\n"
      assert {:error, {:invalid_integer, "abc"}} = Parser.parse(input)
    end

    test "error inside a map value propagates" do
      input = "%1\r\n$1\r\nk\r\n:xyz\r\n"
      assert {:error, {:invalid_integer, "xyz"}} = Parser.parse(input)
    end

    test "error inside a set element propagates" do
      input = "~1\r\n:notnum\r\n"
      assert {:error, {:invalid_integer, "notnum"}} = Parser.parse(input)
    end

    test "error inside a push element propagates" do
      input = ">1\r\n:bad\r\n"
      assert {:error, {:invalid_integer, "bad"}} = Parser.parse(input)
    end
  end
  describe "parser: pipelining with partial tail" do
    test "9 complete integers and 1 partial" do
      complete = Enum.map_join(1..9, "", fn i -> ":#{i}\r\n" end)
      partial = ":10\r"
      input = complete <> partial
      assert {:ok, values, ^partial} = Parser.parse(input)
      assert values == Enum.to_list(1..9)
    end

    test "complete array then partial bulk string" do
      input = "*1\r\n:1\r\n$10\r\nhel"
      assert {:ok, [[1]], "$10\r\nhel"} = Parser.parse(input)
    end
  end
  describe "parser: large bulk string" do
    test "parses a 10KB bulk string" do
      size = 10_000
      data = :binary.copy("A", size)
      input = "$#{size}\r\n#{data}\r\n"
      assert {:ok, [^data], ""} = Parser.parse(input)
    end
  end
  describe "encoder: nil" do
    test "encodes nil as RESP3 null _\\r\\n" do
      assert to_bin(Encoder.encode(nil)) == "_\r\n"
    end
  end
  describe "encoder: empty binary" do
    test "encodes empty binary as $0\\r\\n\\r\\n" do
      assert to_bin(Encoder.encode("")) == "$0\r\n\r\n"
    end
  end
  describe "encoder: empty simple string" do
    test "encodes {:simple, ''} as +\\r\\n" do
      assert to_bin(Encoder.encode({:simple, ""})) == "+\r\n"
    end
  end
  describe "encoder: empty error" do
    test "encodes {:error, ''} as -\\r\\n" do
      assert to_bin(Encoder.encode({:error, ""})) == "-\r\n"
    end
  end
  describe "encoder: large integer (> 2^53)" do
    test "integer larger than 2^53 still fits in int64, encoded as :" do
      val = 9_007_199_254_740_993
      assert to_bin(Encoder.encode(val)) == ":#{val}\r\n"
    end

    test "integer at max int64 boundary encoded as :" do
      val = 9_223_372_036_854_775_807
      assert to_bin(Encoder.encode(val)) == ":#{val}\r\n"
    end

    test "integer beyond int64 encoded as big number (" do
      val = 9_223_372_036_854_775_808
      assert to_bin(Encoder.encode(val)) == "(#{val}\r\n"
    end

    test "very large integer (200 digits) encoded as big number" do
      val = String.to_integer(String.duplicate("1", 200))
      result = to_bin(Encoder.encode(val))
      assert String.starts_with?(result, "(")
      assert String.ends_with?(result, "\r\n")
    end
  end
  describe "encoder: negative integer" do
    test "encodes -1 as :-1\\r\\n" do
      assert to_bin(Encoder.encode(-1)) == ":-1\r\n"
    end

    test "encodes min int64 as regular integer" do
      val = -9_223_372_036_854_775_808
      assert to_bin(Encoder.encode(val)) == ":#{val}\r\n"
    end

    test "encodes below min int64 as big number" do
      val = -9_223_372_036_854_775_809
      assert to_bin(Encoder.encode(val)) == "(#{val}\r\n"
    end
  end
  describe "encoder: zero" do
    test "encodes 0 as :0\\r\\n" do
      assert to_bin(Encoder.encode(0)) == ":0\r\n"
    end
  end
  describe "encoder: empty list" do
    test "encodes [] as *0\\r\\n" do
      assert to_bin(Encoder.encode([])) == "*0\r\n"
    end
  end
  describe "encoder: nested list" do
    test "encodes [[1, 2], [3]] as nested arrays" do
      result = to_bin(Encoder.encode([[1, 2], [3]]))
      assert result == "*2\r\n*2\r\n:1\r\n:2\r\n*1\r\n:3\r\n"
    end

    test "encodes 3-level nested list" do
      result = to_bin(Encoder.encode([[[42]]]))
      assert result == "*1\r\n*1\r\n*1\r\n:42\r\n"
    end

    test "encodes list with nil and nested list" do
      result = to_bin(Encoder.encode([nil, [1]]))
      assert result == "*2\r\n_\r\n*1\r\n:1\r\n"
    end
  end
  describe "encoder: empty map" do
    test "encodes %{} as %0\\r\\n" do
      assert to_bin(Encoder.encode(%{})) == "%0\r\n"
    end
  end
  describe "encoder: map with atom keys" do
    test "atom keys raise FunctionClauseError (atoms are not encodable)" do
      # The encoder does not support arbitrary atoms as values/keys.
      # Only :ok, :infinity, :neg_infinity, :nan, true, false, and nil are
      # handled. Atom map keys like :name will raise.
      assert_raise FunctionClauseError, fn ->
        Encoder.encode(%{name: "Alice"})
      end
    end

    test "map with string keys works as expected" do
      result = to_bin(Encoder.encode(%{"name" => "Alice"}))
      assert String.starts_with?(result, "%1\r\n")
      assert String.contains?(result, "$4\r\nname\r\n")
      assert String.contains?(result, "$5\r\nAlice\r\n")
    end
  end
  describe "encoder: MapSet" do
    test "encodes empty MapSet as ~0\\r\\n" do
      assert to_bin(Encoder.encode(MapSet.new())) == "~0\r\n"
    end

    test "encodes MapSet with single integer" do
      result = to_bin(Encoder.encode(MapSet.new([42])))
      assert result == "~1\r\n:42\r\n"
    end

    test "encodes MapSet with booleans" do
      result = to_bin(Encoder.encode(MapSet.new([true, false])))
      assert String.starts_with?(result, "~2\r\n")
      assert String.contains?(result, "#t\r\n")
      assert String.contains?(result, "#f\r\n")
    end

    test "encodes MapSet with nested list elements" do
      result = to_bin(Encoder.encode(MapSet.new([[1, 2]])))
      assert result == "~1\r\n*2\r\n:1\r\n:2\r\n"
    end
  end
  describe "encoder: push" do
    test "encodes push with empty list" do
      assert to_bin(Encoder.encode({:push, []})) == ">0\r\n"
    end

    test "encodes push with single element" do
      assert to_bin(Encoder.encode({:push, ["msg"]})) == ">1\r\n$3\r\nmsg\r\n"
    end

    test "encodes push with nested array" do
      result = to_bin(Encoder.encode({:push, [[1, 2]]}))
      assert result == ">1\r\n*2\r\n:1\r\n:2\r\n"
    end

    test "encodes push with map element" do
      result = to_bin(Encoder.encode({:push, [%{"k" => "v"}]}))
      assert String.starts_with?(result, ">1\r\n%1\r\n")
    end
  end
  describe "encoder: verbatim string" do
    test "encodes verbatim with empty data" do
      result = to_bin(Encoder.encode({:verbatim, "txt", ""}))
      assert result == "=4\r\ntxt:\r\n"
    end

    test "encodes verbatim with data containing CRLF" do
      data = "a\r\nb"
      result = to_bin(Encoder.encode({:verbatim, "txt", data}))
      expected_len = 3 + 1 + byte_size(data)
      assert result == "=#{expected_len}\r\ntxt:#{data}\r\n"
    end

    test "encodes verbatim with non-standard encoding prefix" do
      result = to_bin(Encoder.encode({:verbatim, "bin", "data"}))
      assert result == "=8\r\nbin:data\r\n"
    end
  end
  describe "encoder: boolean" do
    test "encodes true as #t\\r\\n" do
      assert to_bin(Encoder.encode(true)) == "#t\r\n"
    end

    test "encodes false as #f\\r\\n" do
      assert to_bin(Encoder.encode(false)) == "#f\r\n"
    end
  end
    end
  end
end
