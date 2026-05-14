defmodule FerricstoreServer.Resp.EncoderTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.Encoder
  alias FerricstoreServer.Resp.Parser

  # Helper to flatten iodata for assertion
  defp to_binary(iodata), do: IO.iodata_to_binary(iodata)

  # ---------------------------------------------------------------------------
  # :ok atom
  # ---------------------------------------------------------------------------

  describe "encode :ok" do
    test "encodes :ok as simple string OK" do
      assert to_binary(Encoder.encode(:ok)) == "+OK\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Simple string
  # ---------------------------------------------------------------------------

  describe "encode simple string" do
    test "encodes a simple string" do
      assert to_binary(Encoder.encode({:simple, "PONG"})) == "+PONG\r\n"
    end

    test "encodes an empty simple string" do
      assert to_binary(Encoder.encode({:simple, ""})) == "+\r\n"
    end

    test "raises on simple string containing CRLF" do
      assert_raise ArgumentError, ~r/simple string must not contain/, fn ->
        Encoder.encode({:simple, "OK\r\nINJECTED"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Simple error
  # ---------------------------------------------------------------------------

  describe "encode error" do
    test "encodes an error" do
      assert to_binary(Encoder.encode({:error, "ERR unknown"})) == "-ERR unknown\r\n"
    end

    test "encodes unknown outcome write timeout as Redis error text" do
      assert to_binary(Encoder.encode({:error, {:timeout, :unknown_outcome}})) ==
               "-ERR write timeout; outcome unknown\r\n"
    end

    test "encodes an empty error" do
      assert to_binary(Encoder.encode({:error, ""})) == "-\r\n"
    end

    test "raises on simple error containing CRLF" do
      assert_raise ArgumentError, ~r/simple error must not contain/, fn ->
        Encoder.encode({:error, "ERR\r\nINJECTED"})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Blob error
  # ---------------------------------------------------------------------------

  describe "encode blob error" do
    test "encodes a blob error" do
      assert to_binary(Encoder.encode({:blob_error, "ERR something"})) ==
               "!13\r\nERR something\r\n"
    end

    test "encodes an empty blob error" do
      assert to_binary(Encoder.encode({:blob_error, ""})) == "!0\r\n\r\n"
    end

    test "encodes a blob error containing CRLF (allowed in blob errors)" do
      msg = "ERR\r\ndetails"
      result = to_binary(Encoder.encode({:blob_error, msg}))
      assert result == "!12\r\nERR\r\ndetails\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Integer
  # ---------------------------------------------------------------------------

  describe "encode integer" do
    test "encodes a positive integer" do
      assert to_binary(Encoder.encode(42)) == ":42\r\n"
    end

    test "encodes zero" do
      assert to_binary(Encoder.encode(0)) == ":0\r\n"
    end

    test "encodes a negative integer" do
      assert to_binary(Encoder.encode(-99)) == ":-99\r\n"
    end

    test "encodes a large integer" do
      assert to_binary(Encoder.encode(999_999_999)) == ":999999999\r\n"
    end

    test "encodes integer boundary: one" do
      assert to_binary(Encoder.encode(1)) == ":1\r\n"
    end

    test "encodes integer boundary: negative one" do
      assert to_binary(Encoder.encode(-1)) == ":-1\r\n"
    end

    test "encodes integer boundary: max JS safe integer" do
      assert to_binary(Encoder.encode(9_007_199_254_740_992)) == ":9007199254740992\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Big number
  # ---------------------------------------------------------------------------

  describe "encode big number" do
    test "encodes a large positive integer as big number" do
      assert to_binary(Encoder.encode(99_999_999_999_999_999_999)) ==
               "(99999999999999999999\r\n"
    end

    test "encodes a large negative integer as big number" do
      assert to_binary(Encoder.encode(-99_999_999_999_999_999_999)) ==
               "(-99999999999999999999\r\n"
    end

    test "encodes max int64 as regular integer" do
      assert to_binary(Encoder.encode(9_223_372_036_854_775_807)) ==
               ":9223372036854775807\r\n"
    end

    test "encodes min int64 as regular integer" do
      assert to_binary(Encoder.encode(-9_223_372_036_854_775_808)) ==
               ":-9223372036854775808\r\n"
    end

    test "encodes max int64 + 1 as big number" do
      assert to_binary(Encoder.encode(9_223_372_036_854_775_808)) ==
               "(9223372036854775808\r\n"
    end

    test "encodes min int64 - 1 as big number" do
      assert to_binary(Encoder.encode(-9_223_372_036_854_775_809)) ==
               "(-9223372036854775809\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Bulk string (binary)
  # ---------------------------------------------------------------------------

  describe "encode binary" do
    test "encodes a binary string" do
      assert to_binary(Encoder.encode("hello")) == "$5\r\nhello\r\n"
    end

    test "encodes an empty binary" do
      assert to_binary(Encoder.encode("")) == "$0\r\n\r\n"
    end

    test "encodes binary with CRLF inside" do
      assert to_binary(Encoder.encode("a\r\nb")) == "$4\r\na\r\nb\r\n"
    end

    test "encodes binary with null bytes" do
      data = <<0, 1, 2>>
      assert to_binary(Encoder.encode(data)) == "$3\r\n\x00\x01\x02\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Nil
  # ---------------------------------------------------------------------------

  describe "encode nil" do
    test "encodes nil as RESP3 null" do
      assert to_binary(Encoder.encode(nil)) == "_\r\n"
    end
  end

  describe "encode bulk string result batches" do
    test "encodes GET-style binary and nil values in order" do
      values = ["hello", nil, "", "a\r\nb", <<0, 1, 2>>]

      assert to_binary(Encoder.encode_bulk_strings_or_nulls(values)) ==
               "$5\r\nhello\r\n_\r\n$0\r\n\r\n$4\r\na\r\nb\r\n$3\r\n\x00\x01\x02\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean
  # ---------------------------------------------------------------------------

  describe "encode boolean" do
    test "encodes true" do
      assert to_binary(Encoder.encode(true)) == "#t\r\n"
    end

    test "encodes false" do
      assert to_binary(Encoder.encode(false)) == "#f\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Float
  # ---------------------------------------------------------------------------

  describe "encode float" do
    test "encodes a positive float" do
      result = to_binary(Encoder.encode(3.14))
      assert result == ",3.14\r\n"
    end

    test "encodes a negative float" do
      result = to_binary(Encoder.encode(-1.5))
      assert result == ",-1.5\r\n"
    end

    test "encodes zero as float" do
      result = to_binary(Encoder.encode(0.0))
      assert result == ",0.0\r\n"
    end

    test "encodes a very large float" do
      result = to_binary(Encoder.encode(1.0e308))
      assert String.starts_with?(result, ",")
      assert String.ends_with?(result, "\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # List (Array)
  # ---------------------------------------------------------------------------

  describe "encode list" do
    test "encodes a list of integers" do
      result = to_binary(Encoder.encode([1, 2, 3]))
      assert result == "*3\r\n:1\r\n:2\r\n:3\r\n"
    end

    test "encodes an empty list" do
      assert to_binary(Encoder.encode([])) == "*0\r\n"
    end

    test "encodes a mixed-type list" do
      result = to_binary(Encoder.encode([1, "hello", nil]))
      assert result == "*3\r\n:1\r\n$5\r\nhello\r\n_\r\n"
    end

    test "encodes nested lists" do
      result = to_binary(Encoder.encode([[1, 2], [3, 4]]))
      assert result == "*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Map
  # ---------------------------------------------------------------------------

  describe "encode map" do
    test "encodes an empty map" do
      assert to_binary(Encoder.encode(%{})) == "%0\r\n"
    end

    test "encodes a single-entry map" do
      result = to_binary(Encoder.encode(%{"key" => "val"}))
      assert result == "%1\r\n$3\r\nkey\r\n$3\r\nval\r\n"
    end

    test "encodes a map with integer values" do
      result = to_binary(Encoder.encode(%{"count" => 42}))
      assert result == "%1\r\n$5\r\ncount\r\n:42\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Set (MapSet)
  # ---------------------------------------------------------------------------

  describe "encode set" do
    test "encodes a MapSet with string elements" do
      set = MapSet.new(["a", "b"])
      result = to_binary(Encoder.encode(set))

      # Set element order is non-deterministic, so verify structure
      assert String.starts_with?(result, "~2\r\n")

      # Both bulk strings must appear
      assert String.contains?(result, "$1\r\na\r\n")
      assert String.contains?(result, "$1\r\nb\r\n")
    end

    test "encodes an empty MapSet" do
      assert to_binary(Encoder.encode(MapSet.new())) == "~0\r\n"
    end

    test "encodes a MapSet with integer elements" do
      set = MapSet.new([1, 2, 3])
      result = to_binary(Encoder.encode(set))
      assert String.starts_with?(result, "~3\r\n")
      assert String.contains?(result, ":1\r\n")
      assert String.contains?(result, ":2\r\n")
      assert String.contains?(result, ":3\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Push
  # ---------------------------------------------------------------------------

  describe "encode push" do
    test "encodes a push message" do
      result = to_binary(Encoder.encode({:push, ["message", "channel", "data"]}))
      assert result == ">3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$4\r\ndata\r\n"
    end

    test "encodes an empty push" do
      assert to_binary(Encoder.encode({:push, []})) == ">0\r\n"
    end

    test "encodes push with mixed types" do
      result = to_binary(Encoder.encode({:push, ["subscribe", 42]}))
      assert String.starts_with?(result, ">")
      assert String.contains?(result, "$9\r\nsubscribe\r\n")
      assert String.contains?(result, ":42\r\n")
    end
  end

  # ---------------------------------------------------------------------------
  # Verbatim string
  # ---------------------------------------------------------------------------

  describe "encode verbatim string" do
    test "encodes a verbatim text string" do
      result = to_binary(Encoder.encode({:verbatim, "txt", "Some string"}))
      assert result == "=15\r\ntxt:Some string\r\n"
    end

    test "encodes a verbatim markdown string" do
      result = to_binary(Encoder.encode({:verbatim, "mkd", "# Hello"}))
      assert result == "=11\r\nmkd:# Hello\r\n"
    end

    test "encodes a verbatim string with empty data" do
      result = to_binary(Encoder.encode({:verbatim, "txt", ""}))
      # 3 (encoding) + 1 (colon) + 0 (data) = 4
      assert result == "=4\r\ntxt:\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # iodata output
  # ---------------------------------------------------------------------------

  describe "iodata output" do
    test "returns iodata, not a flat binary" do
      result = Encoder.encode(:ok)
      assert is_list(result)
    end

    test "returns iodata for a list" do
      result = Encoder.encode([1, 2])
      assert is_list(result)
    end

    test "iodata can be sent directly to IO" do
      result = Encoder.encode("test")
      assert is_list(result)
      assert IO.iodata_to_binary(result) == "$4\r\ntest\r\n"
    end

    test "encoder returns iodata for large nested structure" do
      input = [%{"a" => 1}, %{"b" => 2}, %{"c" => 3}]
      result = Encoder.encode(input)
      assert is_list(result)
    end
  end

  # ---------------------------------------------------------------------------
  # Round-trip: encode then parse
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Special float atoms
  # ---------------------------------------------------------------------------

  describe "encode special float atoms" do
    test "encodes :infinity as +inf" do
      assert to_binary(Encoder.encode(:infinity)) == ",inf\r\n"
    end

    test "encodes :neg_infinity as -inf" do
      assert to_binary(Encoder.encode(:neg_infinity)) == ",-inf\r\n"
    end

    test "encodes :nan" do
      assert to_binary(Encoder.encode(:nan)) == ",nan\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Additional edge cases
  # ---------------------------------------------------------------------------

  describe "encode edge cases" do
    test "encodes verbatim string with binary data containing null bytes" do
      data = <<0, 1, 2, 3>>
      result = to_binary(Encoder.encode({:verbatim, "bin", data}))
      # Payload length: 3 (encoding) + 1 (colon) + 4 (data) = 8
      assert result == "=8\r\nbin:" <> data <> "\r\n"
    end

    test "encodes push message with all RESP3 types" do
      push =
        {:push, [nil, 42, %{"k" => "v"}, MapSet.new([1])]}

      result = to_binary(Encoder.encode(push))
      assert String.starts_with?(result, ">4\r\n")
      # nil
      assert String.contains?(result, "_\r\n")
      # integer
      assert String.contains?(result, ":42\r\n")
    end

    test "encodes deeply nested structure (5 levels)" do
      nested = [[[[[42]]]]]
      result = to_binary(Encoder.encode(nested))
      # 5 nested array headers: *1 each, then the integer
      expected = "*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n:42\r\n"
      assert result == expected
    end

    test "simple string with lone LF does not raise (only CRLF is rejected)" do
      # The encoder only rejects \r\n (CRLF pair), not standalone \n
      result = to_binary(Encoder.encode({:simple, "OK\nINJECTED"}))
      assert result == "+OK\nINJECTED\r\n"
    end

    test "simple string with lone CR does not raise (only CRLF is rejected)" do
      result = to_binary(Encoder.encode({:simple, "OK\rSTILL"}))
      assert result == "+OK\rSTILL\r\n"
    end

    test "encodes empty map with no pairs" do
      assert to_binary(Encoder.encode(%{})) == "%0\r\n"
    end

    test "encodes MapSet with single element" do
      result = to_binary(Encoder.encode(MapSet.new([42])))
      assert result == "~1\r\n:42\r\n"
    end

    test "encodes blob error with CRLF in message" do
      msg = "line1\r\nline2"
      result = to_binary(Encoder.encode({:blob_error, msg}))
      assert result == "!12\r\nline1\r\nline2\r\n"
    end
  end

  describe "round-trip through parser" do
    test "simple string round-trips" do
      original = {:simple, "OK"}
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "error round-trips" do
      original = {:error, "ERR bad"}
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "integer round-trips" do
      encoded = to_binary(Encoder.encode(42))
      assert {:ok, [42], ""} = Parser.parse(encoded)
    end

    test "negative integer round-trips" do
      encoded = to_binary(Encoder.encode(-7))
      assert {:ok, [-7], ""} = Parser.parse(encoded)
    end

    test "binary round-trips" do
      encoded = to_binary(Encoder.encode("hello"))
      assert {:ok, ["hello"], ""} = Parser.parse(encoded)
    end

    test "nil round-trips" do
      encoded = to_binary(Encoder.encode(nil))
      assert {:ok, [nil], ""} = Parser.parse(encoded)
    end

    test "true round-trips" do
      encoded = to_binary(Encoder.encode(true))
      assert {:ok, [true], ""} = Parser.parse(encoded)
    end

    test "false round-trips" do
      encoded = to_binary(Encoder.encode(false))
      assert {:ok, [false], ""} = Parser.parse(encoded)
    end

    test "float round-trips" do
      encoded = to_binary(Encoder.encode(3.14))
      assert {:ok, [3.14], ""} = Parser.parse(encoded)
    end

    test "list round-trips" do
      original = [1, "two", nil]
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "empty list round-trips" do
      encoded = to_binary(Encoder.encode([]))
      assert {:ok, [[]], ""} = Parser.parse(encoded)
    end

    test "map round-trips" do
      original = %{"key" => "val"}
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "push round-trips" do
      original = {:push, ["message", "chan", "data"]}
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "verbatim string round-trips" do
      original = {:verbatim, "txt", "Some text"}
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test "nested list of maps round-trips" do
      original = [%{"a" => 1}, %{"b" => 2}]
      encoded = to_binary(Encoder.encode(original))
      assert {:ok, [^original], ""} = Parser.parse(encoded)
    end

    test ":ok round-trips as simple string" do
      encoded = to_binary(Encoder.encode(:ok))
      assert {:ok, [{:simple, "OK"}], ""} = Parser.parse(encoded)
    end
  end
end
