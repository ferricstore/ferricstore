defmodule FerricstoreServer.Native.FQLParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Error, Limits, ReferenceParser}
  alias FerricstoreServer.Native.{FQLParser, NIF}

  @golden_queries [
    "FROM runs WHERE run_id = 'run-auto' RETURN RECORD",
    "FROM runs WHERE run_id = 'run-auto' " <>
      "RETURN RECORD (run_id, state, attributes, state_meta, attribute['customer'])",
    "FROM events WHERE run_id = @run_id ORDER BY event_id ASC LIMIT 25 RETURN RECORDS",
    "FROM events WHERE run_id = @run_id ORDER BY event_id ASC LIMIT 25 " <>
      "RETURN RECORDS (event_id, fields['event'])",
    "FROM runs WHERE partition_key = @partition AND parent_flow_id = @parent " <>
      "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD",
    "from RUNS where RUN_ID = 'Run''''42' and PARTITION_KEY = 'partition' return record;",
    "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD",
    "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition AND state = 'failed' " <>
      "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = @tenant AND state IN ('failed', 'completed') " <>
      "AND updated_at_ms FROM @from TO @until " <>
      "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS",
    "FROM runs WHERE partition_key = @tenant AND state = 'failed' " <>
      "ORDER BY updated_at_ms DESC LIMIT 25 CURSOR @page RETURN RECORDS",
    "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND priority BETWEEN 1 AND 5 " <>
      "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS;",
    "FROM runs WHERE partition_key = 'tenant-a' AND attribute.region IS NULL " <>
      "ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = 'tenant-a' AND attribute.region IS MISSING " <>
      "ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = @tenant " <>
      "AND state_meta['review.v2']['ai.model'] = @model " <>
      "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = @tenant " <>
      "AND attribute['customer''s.region'] = @region " <>
      "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS",
    "FROM runs WHERE partition_key = @partition AND type = 'payment' " <>
      "AND state = 'failed' RETURN COUNT",
    "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND state IN ('failed', 'completed') " <>
      "RETURN COUNT;"
  ]

  test "Rust parser matches the Elixir reference parser" do
    for query <- @golden_queries do
      assert {:ok, _request} = reference = ReferenceParser.parse(query)
      assert FQLParser.parse(query) == reference
    end
  end

  test "Rust parser matches bounded reference failures" do
    queries = [
      "",
      "FROM runs RETURN RECORDS",
      "FROM runs WHERE tenant_ref = 'forged' AND run_id = 'one' RETURN RECORD",
      "FROM runs WHERE partition_key = 'p' OR run_id = 'two' RETURN RECORD",
      "FROM events WHERE partition_key = 'p' AND run_id = 'run-123' RETURN RECORD",
      "FROM runs WHERE partition_key = 'p' AND state IN () ORDER BY updated_at_ms ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' AND state IN ('a',) ORDER BY updated_at_ms ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' AND priority BETWEEN 2 AND 1 ORDER BY updated_at_ms ASC LIMIT 1 RETURN RECORDS",
      "FROM runs WHERE partition_key = 'p' ORDER BY updated_at_ms ASC LIMIT 101 RETURN RECORDS",
      "FROM events WHERE partition_key = 'p' RETURN COUNT",
      "FROM runs WHERE partition_key = 'p' LIMIT 1 RETURN COUNT",
      "FROM runs WHERE partition_key = 'p' CURSOR @page RETURN COUNT",
      "FROM runs WHERE run_id = 'one' RETURN RECORD (state, STATE)",
      "FROM runs WHERE run_id = 'one' RETURN RECORD (lease_token)",
      "FROM events WHERE run_id = 'one' ORDER BY event_id ASC LIMIT 1 RETURN RECORDS (state)",
      "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition " <>
        "ORDER BY updated_at_ms DESC LIMIT 10 CURSOR @page RETURN RECORDS"
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
    modes = ["", "EXPLAIN ", "explain\t", "EXPLAIN ANALYZE ", "explain\tanalyze\n"]

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

  test "Rust parser enforces the canonical predicate and IN value limits" do
    exact_predicates = predicate_query(Limits.max_predicates())
    assert {:ok, _, _, _, _, _, _, _, _} = NIF.parse_fql(exact_predicates)

    excess_predicates = predicate_query(Limits.max_predicates() + 1)
    excess_predicate_marker = "attribute.field#{Limits.max_predicates()}"

    assert {:error, :unsupported_query_shape, predicate_byte} =
             NIF.parse_fql(excess_predicates)

    assert predicate_byte == marker_byte(excess_predicates, excess_predicate_marker, 1)
    assert FQLParser.parse(excess_predicates) == ReferenceParser.parse(excess_predicates)

    exact_values = in_query(Limits.max_in_values())
    assert {:ok, _, _, _, _, _, _, _, _} = NIF.parse_fql(exact_values)

    excess_values = in_query(Limits.max_in_values() + 1)
    excess_value_marker = "'state#{Limits.max_in_values()}'"

    assert {:error, :unsupported_query_shape, value_byte} = NIF.parse_fql(excess_values)
    assert value_byte == marker_byte(excess_values, excess_value_marker, 1)
    assert FQLParser.parse(excess_values) == ReferenceParser.parse(excess_values)
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

  test "diagnostic parsing positions malformed sources, fields, lists, and terminators" do
    empty_in = "FROM runs WHERE partition_key = 'p' AND state IN () RETURN COUNT"
    trailing_in = "FROM runs WHERE partition_key = 'p' AND state IN ('failed',) RETURN COUNT"
    extra_terminator = "FROM runs WHERE partition_key = 'p' RETURN COUNT;;"
    unterminated = "FROM runs WHERE partition_key = 'unterminated RETURN COUNT"

    cases = [
      {"FROM unknown_source WHERE run_id = 'one' RETURN RECORD", "unknown_source"},
      {"FROM runs WHERE unknown_field = 'one' RETURN COUNT", "unknown_field"},
      {"FROM runs WHERE run_id = 'one' RETURN RECORD (lease_token)", "lease_token"},
      {"FROM runs WHERE run_id = 'one' RETURN RECORD (state, STATE)", "STATE"},
      {"EXPLAIN ANALYZE FROM runs RETURN RECORDS", "RETURN"},
      {empty_in, ")"},
      {trailing_in, ")"},
      {extra_terminator, ";", 2},
      {unterminated, "'"},
      {"FROM runs WHERE partition_key = 'p' OR run_id = 'two' RETURN RECORD", "OR"},
      {"FROM events WHERE partition_key = 'p' AND run_id = 'run-123' RETURN RECORD", "RECORD"},
      {"FROM events WHERE partition_key = 'p' RETURN COUNT", "COUNT"},
      {"FROM runs WHERE partition_key = 'p' ORDER BY updated_at_ms ASC LIMIT 101 RETURN RECORDS",
       "101"}
    ]

    for entry <- cases do
      {query, marker, occurrence} =
        case entry do
          {query, marker} -> {query, marker, 1}
          {query, marker, occurrence} -> {query, marker, occurrence}
        end

      assert {:error, reason} = FQLParser.parse(query)

      assert {:error,
              %Error{
                reason: ^reason,
                position: %{byte: byte, line: 1, column: byte}
              }} = FQLParser.parse_diagnostic(query)

      assert byte == marker_byte(query, marker, occurrence)
      assert ReferenceParser.parse_diagnostic(query) == FQLParser.parse_diagnostic(query)
    end
  end

  test "diagnostic positions do not leak across failures or successful parses" do
    first = "FROM runs\nWHERE @"
    valid = "FROM runs WHERE run_id = 'run-auto' RETURN RECORD"
    second = "FROM missing_source WHERE run_id = 'run-auto' RETURN RECORD"

    assert {:error, %Error{position: %{byte: 17, line: 2, column: 7}}} =
             FQLParser.parse_diagnostic(first)

    assert {:ok, _request} = FQLParser.parse_diagnostic(valid)

    assert {:error, %Error{position: %{byte: 6, line: 1, column: 6}}} =
             FQLParser.parse_diagnostic(second)
  end

  test "diagnostics preserve bracket fields and report a missing tail at end of input" do
    query =
      "FROM runs WHERE partition_key = 'p' AND " <>
        "attribute['customer.region'] = 'eu'"

    assert {:error,
            %Error{
              reason: :unsupported_query_shape,
              position: %{byte: byte, line: 1, column: byte}
            }} = FQLParser.parse_diagnostic(query)

    assert byte == byte_size(query) + 1
  end

  test "every truncated query diagnostic stays inside its source boundary" do
    query =
      "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND run_id = @flow_id RETURN RECORD"

    for size <- 0..(byte_size(query) - 1) do
      truncated = binary_part(query, 0, size)

      assert {:error, %Error{position: %{byte: byte, line: line, column: column}}} =
               FQLParser.parse_diagnostic(truncated)

      assert byte in 1..(size + 1)
      assert line > 0
      assert column > 0
    end
  end

  defp marker_byte(query, marker, occurrence) do
    query
    |> :binary.matches(marker)
    |> Enum.at(occurrence - 1)
    |> elem(0)
    |> Kernel.+(1)
  end

  defp predicate_query(count) do
    predicates =
      ["partition_key = @partition"] ++
        Enum.map(1..(count - 1), fn index ->
          "attribute.field#{index} = @value#{index}"
        end)

    "FROM runs WHERE " <>
      Enum.join(predicates, " AND ") <>
      " ORDER BY updated_at_ms ASC LIMIT 25 RETURN RECORDS"
  end

  defp in_query(count) do
    values = Enum.map_join(0..(count - 1), ",", &"'state#{&1}'")

    "FROM runs WHERE partition_key = @partition AND state IN (#{values}) " <>
      "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"
  end
end
