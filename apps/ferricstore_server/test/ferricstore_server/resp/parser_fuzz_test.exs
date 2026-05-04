defmodule FerricstoreServer.Resp.ParserFuzzTest do
  @moduledoc """
  Deterministic fuzz-style guards for RESP3 parsing.

  These are not a replacement for coverage-guided fuzzing. They give CI a
  cheap regression net for parser crashes, nested framing, hostile bytes, and
  encoder/parser round-trips.
  """

  use ExUnit.Case, async: true

  alias FerricstoreServer.Resp.{Encoder, Parser}

  @iterations 300

  test "random hostile byte buffers never crash parser" do
    seed_rand()

    for _ <- 1..@iterations do
      input = random_binary(:rand.uniform(256) - 1)

      assert_no_crash("Parser.parse/1 crashed for #{inspect(input, limit: 50)}", fn ->
        assert_parser_result(Parser.parse(input))
      end)
    end
  end

  test "encoded generated RESP values round-trip through parser" do
    seed_rand()

    for _ <- 1..@iterations do
      value = random_resp_value(0)
      wire = IO.iodata_to_binary(Encoder.encode(value))

      assert {:ok, [parsed], ""} = Parser.parse(wire)
      assert parsed == normalize_round_trip(value)
    end
  end

  test "valid generated frames parse the same across every TCP split point" do
    seed_rand()

    for _ <- 1..80 do
      value = random_resp_value(0)
      wire = IO.iodata_to_binary(Encoder.encode(value))
      expected = normalize_round_trip(value)

      for split <- 0..(byte_size(wire) - 1) do
        <<prefix::binary-size(split), suffix::binary>> = wire
        assert {:ok, [], ^prefix} = Parser.parse(prefix)
        assert {:ok, [parsed], ""} = Parser.parse(prefix <> suffix)
        assert parsed == expected
      end
    end
  end

  test "nested arrays parse without crashing up to bounded audit depth" do
    for depth <- 1..64 do
      input = nested_array_wire(depth)

      assert_no_crash("Parser.parse/1 crashed at nested depth #{depth}", fn ->
        assert {:ok, [_parsed], ""} = Parser.parse(input)
      end)
    end
  end

  test "excessive RESP nesting is rejected before recursive stack growth" do
    assert {:error, :nesting_too_deep} = Parser.parse(nested_array_wire(129))
    assert {:error, :nesting_too_deep} = Parser.parse(nested_map_wire(129))
    assert {:error, :nesting_too_deep} = Parser.parse(nested_set_wire(129))
    assert {:error, :nesting_too_deep} = Parser.parse(nested_push_wire(129))
    assert {:error, :nesting_too_deep} = Parser.parse(nested_attribute_wire(129))
  end

  test "malicious protocol framing returns structured result, not exception" do
    seed_rand()

    malformed =
      [
        "$999999999999999999999999999999999\r\n",
        "*999999999999999999999999999999999\r\n",
        "%999999999999999999999999999999999\r\n",
        "$-999999999999999999999999999999999\r\n",
        "*-999999999999999999999999999999999\r\n",
        "$5\r\nabc",
        "$5\r\nabc\r",
        "$5\r\nabc\r\ntrailing",
        "*2\r\n$3\r\nGET\r\n$",
        "%1\r\n$3\r\nkey\r\n$5\r\nvalue",
        "#x\r\n",
        "_x\r\n",
        ",not-a-float\r\n"
      ]

    random_lengths =
      for _ <- 1..100 do
        prefix = Enum.random(["$", "*", "%", "~", ">"])
        len = random_ascii_digits(:rand.uniform(40))
        prefix <> len <> "\r\n" <> random_binary(:rand.uniform(32))
      end

    for input <- malformed ++ random_lengths do
      assert_no_crash("Parser.parse/1 crashed for #{inspect(input, limit: 80)}", fn ->
        assert_parser_result(Parser.parse(input))
      end)
    end
  end

  test "adversarial command-mode frames preserve AST tuple contract or error cleanly" do
    key_with_nul = <<"k", 0, "1">>
    value_with_nul = <<"v", 0, "2">>

    frames = [
      {"non-array RESP3 attribute", "|0\r\n*1\r\n$4\r\nPING\r\n"},
      {"malformed RESP3 attribute", "|1\r\n$1\r\nk\r\n"},
      {"nested array argument", "*2\r\n$3\r\nGET\r\n*1\r\n$1\r\nk\r\n"},
      {"integer argument", "*2\r\n$3\r\nGET\r\n:1\r\n"},
      {"null argument", "*2\r\n$3\r\nGET\r\n_\r\n"},
      {"oversized array", "*1048577\r\n"},
      {"negative command array", "*-1\r\n"},
      {"invalid count", "*abc\r\n"},
      {"partial command name", "*1\r\n$4\r\nPI"},
      {"partial argument", "*2\r\n$3\r\nGET\r\n$3\r\nab"},
      {"embedded nul command", "*2\r\n$3\r\nGET\r\n$3\r\n" <> key_with_nul <> "\r\n"},
      {"embedded nul set",
       "*3\r\n$3\r\nSET\r\n$3\r\n" <>
         key_with_nul <> "\r\n$3\r\n" <> value_with_nul <> "\r\n"}
    ]

    for {label, frame} <- frames do
      assert_no_crash("Parser.parse_commands/1 crashed for #{label}", fn ->
        assert_command_parser_result(Parser.parse_commands(frame))
      end)
    end
  end

  test "generated command frames keep command tuple shape stable" do
    seed_rand()

    command_names = ["GET", "SET", "MGET", "MSET", "DEL", "EXISTS", "JSON.GET", "PFADD", ""]

    for _ <- 1..@iterations do
      command = Enum.random(command_names)
      args = random_list(:rand.uniform(6) - 1, fn -> random_binary(:rand.uniform(16) - 1) end)
      frame = command_frame([command | args])

      assert_no_crash("Parser.parse_commands/1 crashed for #{inspect(frame, limit: 80)}", fn ->
        assert_command_parser_result(Parser.parse_commands(frame))
      end)
    end
  end

  defp seed_rand do
    :rand.seed(:exsss, {0xF3, 0xE2, 0xD1})
  end

  defp random_resp_value(depth) when depth >= 3 do
    random_scalar()
  end

  defp random_resp_value(depth) do
    case :rand.uniform(8) do
      1 -> random_scalar()
      2 -> random_binary(:rand.uniform(64) - 1)
      3 -> random_list(:rand.uniform(5) - 1, fn -> random_resp_value(depth + 1) end)
      4 -> {:simple, printable_binary(:rand.uniform(32) - 1)}
      5 -> {:error, "ERR " <> printable_binary(:rand.uniform(24) - 1)}
      6 -> nil
      7 -> :rand.uniform(2) == 1
      8 -> (:rand.uniform(2_000_001) - 1_000_001) / 10
    end
  end

  defp random_scalar do
    case :rand.uniform(5) do
      1 -> :rand.uniform(2_000_001) - 1_000_001
      2 -> random_binary(:rand.uniform(64) - 1)
      3 -> nil
      4 -> :rand.uniform(2) == 1
      5 -> Enum.random([:infinity, :neg_infinity, :nan])
    end
  end

  defp random_list(count, _fun) when count <= 0, do: []
  defp random_list(count, fun), do: Enum.map(1..count, fn _ -> fun.() end)

  defp normalize_round_trip(:ok), do: {:simple, "OK"}
  defp normalize_round_trip(:infinity), do: :infinity
  defp normalize_round_trip(:neg_infinity), do: :neg_infinity
  defp normalize_round_trip(:nan), do: :nan

  defp normalize_round_trip(value) when is_list(value),
    do: Enum.map(value, &normalize_round_trip/1)

  defp normalize_round_trip(value), do: value

  defp nested_array_wire(0), do: ":1\r\n"
  defp nested_array_wire(depth), do: "*1\r\n" <> nested_array_wire(depth - 1)

  defp nested_map_wire(0), do: "$1\r\nv\r\n"
  defp nested_map_wire(depth), do: "%1\r\n$1\r\nk\r\n" <> nested_map_wire(depth - 1)

  defp nested_set_wire(0), do: "$1\r\nv\r\n"
  defp nested_set_wire(depth), do: "~1\r\n" <> nested_set_wire(depth - 1)

  defp nested_push_wire(0), do: "$1\r\nv\r\n"
  defp nested_push_wire(depth), do: ">1\r\n" <> nested_push_wire(depth - 1)

  defp nested_attribute_wire(0), do: "$1\r\nv\r\n"
  defp nested_attribute_wire(depth), do: "|1\r\n$1\r\nk\r\n" <> nested_attribute_wire(depth - 1)

  defp random_ascii_digits(count) do
    if count <= 0, do: "", else: for(_ <- 1..count, into: "", do: <<Enum.random(?0..?9)>>)
  end

  defp printable_binary(count) do
    if count <= 0, do: "", else: for(_ <- 1..count, into: "", do: <<Enum.random(32..126)>>)
  end

  defp random_binary(count) do
    if count <= 0, do: <<>>, else: for(_ <- 1..count, into: <<>>, do: <<Enum.random(0..255)>>)
  end

  defp command_frame(parts) do
    IO.iodata_to_binary([
      "*",
      Integer.to_string(length(parts)),
      "\r\n",
      Enum.map(parts, fn part ->
        ["$", Integer.to_string(byte_size(part)), "\r\n", part, "\r\n"]
      end)
    ])
  end

  defp assert_parser_result({:ok, values, rest}) when is_list(values) and is_binary(rest), do: :ok
  defp assert_parser_result({:error, _reason}), do: :ok

  defp assert_parser_result(other) do
    flunk("unexpected parser result: #{inspect(other)}")
  end

  defp assert_command_parser_result({:ok, commands, rest})
       when is_list(commands) and is_binary(rest) do
    Enum.each(commands, fn
      {:command, cmd, args, ast, keys}
      when is_binary(cmd) and is_list(args) and is_list(keys) ->
        assert Enum.all?(args, &is_binary/1)
        assert Enum.all?(keys, &is_binary/1)
        assert is_tuple(ast) or is_atom(ast)

      other ->
        flunk("unexpected command tuple: #{inspect(other)}")
    end)
  end

  defp assert_command_parser_result({:error, _reason}), do: :ok

  defp assert_command_parser_result(other) do
    flunk("unexpected command parser result: #{inspect(other)}")
  end

  defp assert_no_crash(message, fun) do
    fun.()
  rescue
    exception ->
      flunk("#{message}: #{Exception.format(:error, exception, __STACKTRACE__)}")
  catch
    kind, reason ->
      flunk("#{message}: #{inspect({kind, reason})}")
  end
end
