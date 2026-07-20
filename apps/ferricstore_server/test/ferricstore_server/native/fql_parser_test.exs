defmodule FerricstoreServer.Native.FQLParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.ReferenceParser
  alias FerricstoreServer.Native.FQLParser

  @golden_queries [
    "FROM runs WHERE run_id = 'run-auto' RETURN RECORD",
    "FROM events WHERE run_id = @run_id ORDER BY event_id ASC LIMIT 25 RETURN RECORDS",
    "FROM runs WHERE partition_key = @partition AND parent_flow_id = @parent " <>
      "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD",
    "from RUNS where RUN_ID = 'Run''''42' and PARTITION_KEY = 'partition' return record;",
    "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
    "FROM runs WHERE partition_key = @tenant AND state IN ('failed', 'completed') " <>
      "AND updated_at_ms FROM @from TO @until " <>
      "ORDER BY updated_at_ms DESC, run_id DESC LIMIT 25 RETURN RECORDS",
    "FROM runs WHERE partition_key = @tenant AND state = 'failed' " <>
      "ORDER BY updated_at_ms DESC, run_id DESC LIMIT 25 CURSOR @page RETURN RECORDS",
    "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND priority BETWEEN 1 AND 5 " <>
      "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS;",
    "FROM runs WHERE partition_key = 'tenant-a' AND attribute.region IS NULL " <>
      "ORDER BY run_id ASC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = 'tenant-a' AND attribute.region IS MISSING " <>
      "ORDER BY run_id ASC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = @partition AND type = 'payment' " <>
      "AND state = 'failed' RETURN COUNT",
    "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND state IN ('failed', 'completed') " <>
      "RETURN COUNT;"
  ]

  test "Rust parser matches the Elixir reference parser" do
    for query <- @golden_queries do
      assert FQLParser.parse(query) == ReferenceParser.parse(query)
    end
  end

  test "Rust parser matches bounded reference failures" do
    queries = [
      "",
      "FROM runs RETURN RECORDS",
      "FROM runs WHERE tenant_ref = 'forged' AND run_id = 'one' RETURN RECORD",
      "FROM runs WHERE partition_key = 'p' OR run_id = 'two' RETURN RECORD",
      "FROM events WHERE partition_key = 'p' AND run_id = 'run-123' RETURN RECORD",
      "FROM runs WHERE partition_key = 'p' AND state IN () ORDER BY run_id ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' AND state IN ('a',) ORDER BY run_id ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' AND priority BETWEEN 2 AND 1 ORDER BY run_id ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' ORDER BY run_id ASC LIMIT 101 RETURN RECORDS",
      "FROM events WHERE partition_key = 'p' RETURN COUNT",
      "FROM runs WHERE partition_key = 'p' LIMIT 1 RETURN COUNT",
      "FROM runs WHERE partition_key = 'p' CURSOR @page RETURN COUNT"
    ]

    for query <- queries do
      assert FQLParser.parse(query) == ReferenceParser.parse(query)
    end

    oversized =
      "FROM runs WHERE partition_key = 'p' AND run_id = '" <>
        String.duplicate("x", 16_385) <> "' RETURN RECORD"

    assert FQLParser.parse(oversized) == ReferenceParser.parse(oversized)
  end

  test "Rust parser matches the oracle across deterministic byte mutations" do
    query =
      "FROM runs WHERE partition_key = 'tenant-a' AND run_id = @flow_id RETURN RECORD;"

    for offset <- 0..(byte_size(query) - 1), byte <- [0, 9, 10, 13, 32, ?', ?;, ?@, ?_, 255] do
      <<prefix::binary-size(offset), _replaced, suffix::binary>> = query
      mutated = <<prefix::binary, byte, suffix::binary>>

      assert FQLParser.parse(mutated) == ReferenceParser.parse(mutated)
    end
  end

  test "Rust parser matches generated valid grammar combinations" do
    whitespace = [" ", "\t", "\n", "\r\n", " \t "]
    modes = ["", "EXPLAIN ", "explain\t"]

    predicate_orders = [
      {"partition_key", "'tenant-a'", "run_id", "@flow_id"},
      {"RUN_ID", "'Run''''42'", "PARTITION_KEY", "@partition"},
      {"partition_key", "@same", "run_id", "@same"}
    ]

    for mode <- modes,
        separator <- whitespace,
        {first_field, first_value, second_field, second_value} <- predicate_orders,
        terminator <- ["", ";"] do
      query =
        mode <>
          Enum.join(
            [
              "FROM",
              "runs",
              "WHERE",
              first_field,
              "=",
              first_value,
              "AND",
              second_field,
              "=",
              second_value,
              "RETURN",
              "RECORD"
            ],
            separator
          ) <> terminator

      assert {:ok, _request} = ReferenceParser.parse(query)
      assert FQLParser.parse(query) == ReferenceParser.parse(query)
    end
  end

  test "Rust parser matches the oracle at exact byte and token limits" do
    prefix = "FROM runs WHERE partition_key = 'tenant-a' AND run_id = '"
    suffix = "' RETURN RECORD"
    fill_size = 16 * 1024 - byte_size(prefix) - byte_size(suffix)
    exact_limit = prefix <> String.duplicate("x", fill_size) <> suffix

    assert byte_size(exact_limit) == 16 * 1024
    assert {:ok, _request} = ReferenceParser.parse(exact_limit)
    assert FQLParser.parse(exact_limit) == ReferenceParser.parse(exact_limit)

    over_limit = exact_limit <> " "
    assert {:error, :query_too_large} = ReferenceParser.parse(over_limit)
    assert FQLParser.parse(over_limit) == ReferenceParser.parse(over_limit)

    at_token_limit = Enum.join(List.duplicate("x", 256), " ")
    over_token_limit = at_token_limit <> " x"

    assert {:error, :invalid_syntax} = ReferenceParser.parse(at_token_limit)
    assert {:error, :unsupported_query_shape} = ReferenceParser.parse(over_token_limit)
    assert FQLParser.parse(at_token_limit) == ReferenceParser.parse(at_token_limit)
    assert FQLParser.parse(over_token_limit) == ReferenceParser.parse(over_token_limit)
  end

  test "Rust parser matches the oracle for every truncation of a valid query" do
    query =
      "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND run_id = @flow_id RETURN RECORD"

    for size <- 0..(byte_size(query) - 1) do
      truncated = binary_part(query, 0, size)
      refute match?({:ok, _request}, ReferenceParser.parse(truncated))
      assert FQLParser.parse(truncated) == ReferenceParser.parse(truncated)
    end
  end

  test "Rust parser matches the oracle for seeded arbitrary binary input" do
    :rand.seed(:exsss, {12_345, 67_890, 42_424})

    for _case <- 1..2_000 do
      size = :rand.uniform(257) - 1

      input =
        for _byte <- List.duplicate(nil, size), into: <<>> do
          <<:rand.uniform(256) - 1>>
        end

      assert FQLParser.parse(input) == ReferenceParser.parse(input)
    end
  end
end
