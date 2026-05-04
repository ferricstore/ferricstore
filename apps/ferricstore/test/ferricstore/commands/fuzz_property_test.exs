defmodule Ferricstore.Commands.FuzzPropertyTest do
  @moduledoc """
  Deterministic fuzz-style guards for command argument parsers.

  This keeps audit coverage in-tree without adding a property-test dependency.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Bitmap, Json, SortedSet, Stream}
  alias Ferricstore.GlobMatcher
  alias Ferricstore.Test.MockStore

  @iterations 250

  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    :ok
  end

  test "glob matcher handles generated subjects and patterns without exceptions" do
    seed_rand()

    for _ <- 1..@iterations do
      subject = random_printable(:rand.uniform(80) - 1)
      pattern = random_glob_pattern(:rand.uniform(80) - 1)

      assert_no_crash("GlobMatcher crashed for #{inspect({subject, pattern})}", fn ->
        assert is_boolean(GlobMatcher.match?(subject, pattern))
      end)
    end
  end

  test "escaped literal glob patterns match exactly their source subject" do
    seed_rand()

    for _ <- 1..@iterations do
      subject = random_printable(:rand.uniform(80) - 1)
      pattern = escape_glob_literal(subject)

      assert GlobMatcher.match?(subject, pattern)
    end
  end

  test "stream IDs and range arguments are parser-safe for generated inputs" do
    seed_rand()
    store = MockStore.make()
    key = unique_key("stream")

    assert is_binary(Stream.handle("XADD", [key, "1-0", "f", "v"], store))

    for _ <- 1..@iterations do
      id = random_stream_id()

      assert_no_crash("Stream command crashed for id #{inspect(id)}", fn ->
        assert_command_result(Stream.handle("XADD", [key, id, "f", "v"], store))
        assert_command_result(Stream.handle("XRANGE", [key, id, id], store))
        assert_command_result(Stream.handle("XREVRANGE", [key, id, id], store))
      end)
    end
  end

  test "stream explicit IDs stay strictly ordered and failed XADD does not mutate" do
    seed_rand()
    store = MockStore.make()
    key = unique_key("stream_order")

    ids =
      for i <- 1..160 do
        ms = i * 10 + rem(i * 17, 7)
        seq = rem(i * 31, 5)
        id = "#{ms}-#{seq}"

        assert ^id = Stream.handle("XADD", [key, id, "i", Integer.to_string(i)], store)
        id
      end

    entries = Stream.handle("XRANGE", [key, "-", "+"], store)
    ranged_ids = Enum.map(entries, &stream_entry_id/1)

    assert ranged_ids == ids
    assert ranged_ids == Enum.sort_by(ranged_ids, &parse_stream_id!/1)
    assert Stream.handle("XLEN", [key], store) == length(ids)

    before_range = Stream.handle("XRANGE", [key, "-", "+"], store)
    before_len = Stream.handle("XLEN", [key], store)

    for bad_id <- malformed_or_stale_stream_ids(ids) do
      assert {:error, _} = Stream.handle("XADD", [key, bad_id, "bad", "value"], store)
      assert Stream.handle("XLEN", [key], store) == before_len
      assert Stream.handle("XRANGE", [key, "-", "+"], store) == before_range
    end
  end

  test "JSON command paths and payloads return errors or values, not crashes" do
    seed_rand()

    for _ <- 1..@iterations do
      store = MockStore.make()
      key = unique_key("json")
      path = random_json_path()
      payload = random_json_payload()

      assert_no_crash("JSON command crashed for #{inspect({path, payload}, limit: 80)}", fn ->
        assert_command_result(Json.handle("JSON.SET", [key, path, payload], store))
        assert_command_result(Json.handle("JSON.GET", [key, path], store))
        assert_command_result(Json.handle("JSON.DEL", [key, path], store))
      end)
    end
  end

  test "invalid JSONPath mutations return syntax errors and do not mutate documents" do
    mutation_commands = [
      {"JSON.SET", fn key, path -> [key, path, "9"] end},
      {"JSON.DEL", fn key, path -> [key, path] end},
      {"JSON.NUMINCRBY", fn key, path -> [key, path, "1"] end},
      {"JSON.ARRAPPEND", fn key, path -> [key, path, "1"] end},
      {"JSON.TOGGLE", fn key, path -> [key, path] end},
      {"JSON.CLEAR", fn key, path -> [key, path] end}
    ]

    for path <- invalid_json_paths(), {cmd, build_args} <- mutation_commands do
      store = MockStore.make()
      key = unique_key("json_invalid_path")
      doc = ~s({"arr":[1,2,3],"flag":true,"n":1,"obj":{"a":1}})

      assert :ok = Json.handle("JSON.SET", [key, "$", doc], store)
      before_doc = Json.handle("JSON.GET", [key], store)

      assert {:error, msg} = Json.handle(cmd, build_args.(key, path), store)
      assert msg =~ "invalid JSONPath"
      assert Json.handle("JSON.GET", [key], store) == before_doc
    end
  end

  test "bitmap numeric parsers are safe for generated offsets and modes" do
    seed_rand()
    store = MockStore.make()
    key = unique_key("bitmap")

    for _ <- 1..@iterations do
      offset = random_numberish()
      bit = Enum.random(["0", "1", "-1", "2", "x", "", random_printable(8)])
      mode = Enum.random(["BYTE", "BIT", "byte", "bit", "BORK", "", random_printable(5)])

      assert_no_crash("Bitmap command crashed for #{inspect({offset, bit, mode})}", fn ->
        assert_command_result(Bitmap.handle("GETBIT", [key, offset], store))
        assert_command_result(Bitmap.handle("SETBIT", [key, bounded_offset(offset), bit], store))
        assert_command_result(Bitmap.handle("BITCOUNT", [key, offset, offset, mode], store))
        assert_command_result(Bitmap.handle("BITPOS", [key, bit, offset, offset, mode], store))
      end)
    end
  end

  test "sorted set score and option parsers are safe for generated inputs" do
    seed_rand()
    store = MockStore.make()
    key = unique_key("zset")

    for _ <- 1..@iterations do
      score = random_score()
      member = random_printable(:rand.uniform(20) - 1)

      option =
        Enum.random([
          "NX",
          "XX",
          "GT",
          "LT",
          "CH",
          "WITHSCORES",
          "withscores",
          "",
          random_printable(8)
        ])

      index = random_numberish()

      assert_no_crash(
        "SortedSet command crashed for #{inspect({score, member, option, index})}",
        fn ->
          assert_command_result(SortedSet.handle("ZADD", [key, option, score, member], store))
          assert_command_result(SortedSet.handle("ZSCORE", [key, member], store))
          assert_command_result(SortedSet.handle("ZRANGE", [key, index, index, option], store))

          assert_command_result(
            SortedSet.handle("ZRANGEBYSCORE", [key, score, score, option], store)
          )
        end
      )
    end
  end

  defp seed_rand do
    :rand.seed(:exsss, {0xA11D, 0xF022, 0xC0DE})
  end

  defp assert_command_result({:error, msg}) when is_binary(msg), do: :ok
  defp assert_command_result({:ok, _}), do: :ok

  defp assert_command_result(value) when is_binary(value) or is_integer(value) or is_float(value),
    do: :ok

  defp assert_command_result(value)
       when is_list(value) or is_nil(value) or value in [:ok, true, false], do: :ok

  defp assert_command_result(other) do
    flunk("unexpected command result shape: #{inspect(other)}")
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

  defp random_glob_pattern(count) do
    alphabet = Enum.concat([?a..?z, ?A..?Z, ?0..?9]) ++ ~c"*?[]!^\\:-_"
    if count <= 0, do: "", else: for(_ <- 1..count, into: "", do: <<Enum.random(alphabet)>>)
  end

  defp escape_glob_literal(binary) do
    binary
    |> String.graphemes()
    |> Enum.map(fn
      "*" -> "\\*"
      "?" -> "\\?"
      "[" -> "\\["
      "]" -> "\\]"
      "\\" -> "\\\\"
      ch -> ch
    end)
    |> IO.iodata_to_binary()
  end

  defp random_stream_id do
    Enum.random([
      "*",
      "-",
      "+",
      random_numberish(),
      random_numberish() <> "-" <> random_numberish(),
      random_numberish() <> "-*",
      random_printable(:rand.uniform(24) - 1),
      "#{:rand.uniform(1_000)}-#{:rand.uniform(1_000)}"
    ])
  end

  defp malformed_or_stale_stream_ids(ids) do
    [
      "",
      "-",
      "+",
      "abc",
      "1--1",
      "1-",
      "-1-0",
      "0-0",
      hd(ids),
      Enum.at(ids, div(length(ids), 2)),
      List.last(ids)
    ]
  end

  defp parse_stream_id!(id) do
    [ms, seq] = String.split(id, "-", parts: 2)
    {String.to_integer(ms), String.to_integer(seq)}
  end

  defp stream_entry_id({id, _fields}), do: id
  defp stream_entry_id([id | _fields]), do: id

  defp random_json_path do
    Enum.random([
      "$",
      "$." <> random_printable(:rand.uniform(12) - 1),
      "$[" <> random_numberish() <> "]",
      "$['" <> random_printable(:rand.uniform(12) - 1) <> "']",
      "$." <> random_printable(4) <> "[" <> random_numberish() <> "]",
      random_printable(:rand.uniform(24) - 1)
    ])
  end

  defp invalid_json_paths do
    [
      "",
      "arr[0]",
      "$.",
      "$..a",
      "$.arr[",
      "$.arr[]",
      "$.arr[abc]",
      "$.arr[1",
      "$.arr[1]junk",
      "$['unterminated]",
      "$[\"unterminated]",
      "$.arr..x",
      "$.arr[1].",
      "$.arr[1]#"
    ]
  end

  defp random_json_payload do
    Enum.random([
      "null",
      "true",
      "false",
      Integer.to_string(:rand.uniform(10_000) - 5_000),
      Jason.encode!(%{"v" => random_printable(:rand.uniform(20) - 1)}),
      Jason.encode!(random_list(:rand.uniform(6) - 1, fn -> random_printable(4) end)),
      random_printable(:rand.uniform(40) - 1)
    ])
  end

  defp bounded_offset(offset) do
    case Integer.parse(offset) do
      {n, ""} when n >= 0 and n <= 1024 -> offset
      _ -> Enum.random(["0", "1", "7", "8", "63", "1024", offset])
    end
  end

  defp random_score do
    Enum.random([
      Float.to_string((:rand.uniform(200_001) - 100_001) / 10),
      "inf",
      "-inf",
      "+inf",
      "nan",
      random_numberish(),
      random_printable(:rand.uniform(16) - 1)
    ])
  end

  defp random_numberish do
    Enum.random([
      Integer.to_string(:rand.uniform(10_000) - 5_000),
      "",
      "-",
      "+",
      "00" <> Integer.to_string(:rand.uniform(1000)),
      for(_ <- 1..:rand.uniform(40), into: "", do: <<Enum.random(?0..?9)>>),
      random_printable(:rand.uniform(16) - 1)
    ])
  end

  defp random_list(count, _fun) when count <= 0, do: []
  defp random_list(count, fun), do: Enum.map(1..count, fn _ -> fun.() end)

  defp random_printable(count) do
    if count <= 0, do: "", else: for(_ <- 1..count, into: "", do: <<Enum.random(32..126)>>)
  end

  defp unique_key(prefix), do: "#{prefix}:fuzz:#{System.unique_integer([:positive])}"
end
