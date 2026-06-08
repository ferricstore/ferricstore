defmodule FerricstoreServer.Resp.RespEdgeCasesTest.Sections.EncoderFloatEdgeCases do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Parser
      alias FerricstoreServer.Resp.Encoder

      describe "encoder: float edge cases" do
        test "encodes 1.5" do
          assert to_bin(Encoder.encode(1.5)) == ",1.5\r\n"
        end

        test "encodes -0.0" do
          result = to_bin(Encoder.encode(-0.0))
          # Elixir/Erlang does not distinguish -0.0 from 0.0
          assert result == ",0.0\r\n" or result == ",-0.0\r\n"
        end

        test "encodes :infinity as ,inf\\r\\n" do
          assert to_bin(Encoder.encode(:infinity)) == ",inf\r\n"
        end

        test "encodes :neg_infinity as ,-inf\\r\\n" do
          assert to_bin(Encoder.encode(:neg_infinity)) == ",-inf\r\n"
        end

        test "encodes :nan as ,nan\\r\\n" do
          assert to_bin(Encoder.encode(:nan)) == ",nan\r\n"
        end

        test "encodes very small float" do
          result = to_bin(Encoder.encode(1.0e-300))
          assert String.starts_with?(result, ",")
          assert String.ends_with?(result, "\r\n")
        end

        test "encodes very large float" do
          # credo:disable-for-next-line Credo.Check.Readability.LargeNumbers
          result = to_bin(Encoder.encode(1.7976931348623157e308))
          assert String.starts_with?(result, ",")
          assert String.ends_with?(result, "\r\n")
        end
      end

      describe "encoder: blob error" do
        test "encodes blob error with empty message" do
          assert to_bin(Encoder.encode({:blob_error, ""})) == "!0\r\n\r\n"
        end

        test "encodes blob error with CRLF in message" do
          result = to_bin(Encoder.encode({:blob_error, "a\r\nb"}))
          assert result == "!4\r\na\r\nb\r\n"
        end
      end

      describe "round-trip: encode then parse gives back original value" do
        test "nil round-trips" do
          encoded = to_bin(Encoder.encode(nil))
          assert {:ok, [nil], ""} = Parser.parse(encoded)
        end

        test "true round-trips" do
          encoded = to_bin(Encoder.encode(true))
          assert {:ok, [true], ""} = Parser.parse(encoded)
        end

        test "false round-trips" do
          encoded = to_bin(Encoder.encode(false))
          assert {:ok, [false], ""} = Parser.parse(encoded)
        end

        test ":infinity round-trips" do
          encoded = to_bin(Encoder.encode(:infinity))
          assert {:ok, [:infinity], ""} = Parser.parse(encoded)
        end

        test ":neg_infinity round-trips" do
          encoded = to_bin(Encoder.encode(:neg_infinity))
          assert {:ok, [:neg_infinity], ""} = Parser.parse(encoded)
        end

        test ":nan round-trips" do
          encoded = to_bin(Encoder.encode(:nan))
          assert {:ok, [:nan], ""} = Parser.parse(encoded)
        end

        test "empty binary round-trips" do
          encoded = to_bin(Encoder.encode(""))
          assert {:ok, [""], ""} = Parser.parse(encoded)
        end

        test "binary with CRLF round-trips" do
          val = "hello\r\nworld"
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "empty list round-trips" do
          encoded = to_bin(Encoder.encode([]))
          assert {:ok, [[]], ""} = Parser.parse(encoded)
        end

        test "nested list round-trips" do
          val = [[1, 2], [3, 4]]
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "empty map round-trips" do
          encoded = to_bin(Encoder.encode(%{}))
          assert {:ok, [%{}], ""} = Parser.parse(encoded)
        end

        test "map with integer values round-trips" do
          val = %{"count" => 42, "limit" => 100}
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "MapSet round-trips" do
          val = MapSet.new([1, 2, 3])
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [result], ""} = Parser.parse(encoded)
          assert result == val
        end

        test "push round-trips" do
          val = {:push, ["subscribe", "channel", 1]}
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "verbatim string round-trips" do
          val = {:verbatim, "txt", "hello world"}
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "blob error round-trips" do
          val = {:blob_error, "ERR something went wrong"}
          encoded = to_bin(Encoder.encode(val))
          # blob error parses to {:error, msg}
          assert {:ok, [{:error, "ERR something went wrong"}], ""} = Parser.parse(encoded)
        end

        test "big number round-trips through parser big number type" do
          val = 99_999_999_999_999_999_999
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "negative big number round-trips" do
          val = -99_999_999_999_999_999_999
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end

        test "zero integer round-trips" do
          encoded = to_bin(Encoder.encode(0))
          assert {:ok, [0], ""} = Parser.parse(encoded)
        end

        test "float round-trips" do
          val = 3.14159
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [result], ""} = Parser.parse(encoded)
          assert_in_delta result, val, 1.0e-10
        end

        test "negative float round-trips" do
          val = -273.15
          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [result], ""} = Parser.parse(encoded)
          assert_in_delta result, val, 1.0e-10
        end

        test "complex nested structure round-trips" do
          val = [
            %{"users" => [1, 2, 3]},
            nil,
            true,
            "hello",
            42
          ]

          encoded = to_bin(Encoder.encode(val))
          assert {:ok, [^val], ""} = Parser.parse(encoded)
        end
      end

      describe "round-trip: multiple encoded values in one buffer" do
        test "three different types concatenated" do
          buf =
            to_bin(Encoder.encode(42)) <>
              to_bin(Encoder.encode("hello")) <>
              to_bin(Encoder.encode(nil))

          assert {:ok, [42, "hello", nil], ""} = Parser.parse(buf)
        end

        test "10 integers concatenated" do
          buf = Enum.map_join(1..10, "", fn i -> to_bin(Encoder.encode(i)) end)
          assert {:ok, values, ""} = Parser.parse(buf)
          assert values == Enum.to_list(1..10)
        end
      end
    end
  end
end
