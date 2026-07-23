defmodule Ferricstore.Flow.Query.ErrorTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.Error

  test "exposes stable bounded-execution errors with retry semantics" do
    non_retryable = [
      :query_no_bounded_plan,
      :query_range_budget_exceeded,
      :query_scan_budget_exceeded,
      :query_scan_byte_budget_exceeded,
      :query_hydration_budget_exceeded,
      :query_result_budget_exceeded,
      :query_response_budget_exceeded,
      :query_memory_budget_exceeded,
      :query_deadline_exceeded,
      :query_concurrency_exceeded,
      :query_cursor_invalid,
      :query_cursor_expired,
      :query_cursor_too_large
    ]

    for reason <- non_retryable do
      assert Error.known?(reason)
      assert %{retryable: false, safe_to_retry: false} = atom_payload(reason)
    end

    assert Error.known?(:query_projection_changed)
    assert %{retryable: true, safe_to_retry: true} = atom_payload(:query_projection_changed)

    assert %{code: "query_cursor_invalid"} = atom_payload(:query_cursor_invalid)
    assert %{code: "query_cursor_expired"} = atom_payload(:query_cursor_expired)
  end

  test "maps only canonical wire messages back to their query reason" do
    for reason <- [:unexpected_parameter, :query_storage_unavailable, :unauthorized_scope] do
      assert {:ok, ^reason} = reason |> Error.message() |> Error.reason()
    end

    assert :error = Error.reason("ERR FQL1 received an unexpected parameter ")
    assert :error = Error.reason("unknown")
  end

  test "adds a parser-supplied source position and actionable guidance to syntax errors" do
    query = "FROM runs\nWHERE @"

    assert %Error{
             reason: :invalid_syntax,
             detail: detail,
             hint: hint,
             position: %{byte: 17, line: 2, column: 7}
           } = diagnostic = Error.diagnose(:invalid_syntax, query, 17)

    assert detail =~ "reported position"
    assert hint =~ "FROM runs"

    assert %{
             "code" => "invalid_syntax",
             "detail" => ^detail,
             "hint" => ^hint,
             "position" => %{"byte" => 17, "line" => 2, "column" => 7}
           } = Error.payload(diagnostic)
  end

  test "lists bounded supported fields without echoing the rejected query" do
    query =
      "FROM runs WHERE tenant_secret = 'value-secret' " <>
        "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

    diagnostic = Error.diagnose(:unsupported_field, query, 17)
    payload = Error.payload(diagnostic)

    assert %{
             "code" => "unsupported_field",
             "position" => %{"byte" => 17, "line" => 1, "column" => 17},
             "context" => %{"supported_fields" => fields}
           } = payload

    assert fields == Enum.sort(fields)
    assert "partition_key" in fields
    assert "updated_at_ms" in fields
    assert "attribute.<name>" in fields
    assert "state_meta.<state>.<name>" in fields
    refute inspect(payload) =~ "tenant_secret"
    refute inspect(payload) =~ "value-secret"

    formatted = Error.format(diagnostic)
    assert formatted =~ "Valid fields:"
    assert formatted =~ "partition_key"
  end

  test "renders diagnostics for the text command surface without changing canonical messages" do
    diagnostic = Error.diagnose(:invalid_syntax, "FROM runs\nWHERE @", 17)

    assert Error.message(:invalid_syntax) == "ERR FQL1 invalid syntax"
    assert Error.format(diagnostic) =~ "at line 2, column 7 (byte 17)"
    assert Error.format(diagnostic) =~ "HINT:"
  end

  test "reports human columns and byte offsets after UTF-8 literals" do
    diagnostic = Error.diagnose(:invalid_syntax, "FROM runs WHERE state = 'café' @", 33)

    assert diagnostic.position == %{byte: 33, line: 1, column: 32}
  end

  test "positions unsupported clause shapes without changing their stable code" do
    diagnostic = Error.diagnose(:unsupported_query_shape, "FROM runs RETURN RECORDS", 11)
    payload = Error.payload(diagnostic)

    assert payload["code"] == "unsupported_query_shape"
    assert payload["message"] == "ERR FQL1 query shape is not supported"
    assert payload["position"] == %{"byte" => 11, "line" => 1, "column" => 11}
    assert payload["hint"] =~ "Point reads require"
    assert payload["hint"] =~ "Collection reads require"
  end

  test "positions EXPLAIN ANALYZE failures after the full mode prefix" do
    query = "EXPLAIN ANALYZE FROM runs RETURN RECORDS"
    expected = marker_byte(query, "RETURN")
    diagnostic = Error.diagnose(:unsupported_query_shape, query, expected)

    assert diagnostic.position == %{byte: expected, line: 1, column: expected}
    assert Error.diagnose(:invalid_syntax, "EXPLAIN ANALYZE").hint =~ "EXPLAIN ANALYZE"
  end

  test "makes EXPLAIN ANALYZE cursor rejection actionable" do
    query =
      "EXPLAIN ANALYZE FROM runs WHERE partition_key = @tenant " <>
        "ORDER BY updated_at_ms DESC LIMIT 10 CURSOR @secret_page RETURN RECORDS"

    expected = marker_byte(query, "CURSOR")
    diagnostic = Error.diagnose(:query_cursor_invalid, query, expected)

    assert diagnostic.position == %{byte: expected, line: 1, column: expected}
    assert diagnostic.detail =~ "fresh query"
    assert diagnostic.hint =~ "Remove CURSOR"
    refute inspect(Error.payload(diagnostic)) =~ "secret_page"
  end

  test "points at the first unexpected clause token instead of the query end" do
    query = "FROM runs WHERE state = 'failed' BOGUS RETURN RECORDS"
    byte = elem(:binary.match(query, "BOGUS"), 0) + 1

    assert Error.diagnose(:unsupported_query_shape, query, byte).position == %{
             byte: byte,
             line: 1,
             column: byte
           }

    incomplete = "FROM runs WHERE state ="

    assert Error.diagnose(
             :unsupported_query_shape,
             incomplete,
             byte_size(incomplete) + 1
           ).position == %{
             byte: byte_size(incomplete) + 1,
             line: 1,
             column: byte_size(incomplete) + 1
           }
  end

  test "positions malformed grammar boundaries deterministically" do
    empty_in = "FROM runs WHERE state IN () RETURN COUNT"
    trailing_in = "FROM runs WHERE state IN ('failed',) RETURN COUNT"
    extra_terminator = "FROM runs WHERE state = 'failed' RETURN COUNT;;"
    unterminated = "FROM runs WHERE state = 'unterminated"

    cases = [
      {"", 1, 1, 1},
      {"FROM", 5, 1, 5},
      {"FROM runs\r\nWHERE", 17, 2, 6},
      {"FROM runs WHERE state =", 24, 1, 24},
      {empty_in, marker_byte(empty_in, ")"), 1, marker_byte(empty_in, ")")},
      {trailing_in, marker_byte(trailing_in, ")"), 1, marker_byte(trailing_in, ")")},
      {extra_terminator, marker_byte(extra_terminator, ";", 2), 1,
       marker_byte(extra_terminator, ";", 2)},
      {unterminated, marker_byte(unterminated, "'"), 1, marker_byte(unterminated, "'")}
    ]

    for {query, byte, line, column} <- cases do
      assert Error.diagnose(:invalid_syntax, query, byte).position == %{
               byte: byte,
               line: line,
               column: column
             }
    end
  end

  test "reports CRLF and multibyte positions in byte and character coordinates" do
    query = "FROM runs\r\nWHERE state = 'café'\r\n@"
    byte = marker_byte(query, "@")

    assert Error.diagnose(:invalid_syntax, query, byte).position == %{
             byte: byte,
             line: 3,
             column: 1
           }
  end

  test "reports unsupported sources without echoing query literals" do
    query = "FROM tenant_secret WHERE run_id = 'run-secret' RETURN RECORD"
    diagnostic = Error.diagnose(:unsupported_source, query, 6)
    payload = Error.payload(diagnostic)

    assert payload["code"] == "unsupported_source"
    assert payload["position"] == %{"byte" => 6, "line" => 1, "column" => 6}
    assert payload["context"] == %{"supported_sources" => ["events", "runs"]}
    refute inspect(payload) =~ "tenant_secret"
    refute inspect(payload) =~ "run-secret"
  end

  test "bounds optional diagnostic sections independently" do
    diagnostic =
      Error.new(:invalid_syntax,
        detail: String.duplicate("d", 1_025),
        hint: String.duplicate("h", 1_024),
        context: Map.new(1..17, &{Integer.to_string(&1), &1})
      )

    payload = Error.payload(diagnostic)
    refute Map.has_key?(payload, "detail")
    assert byte_size(payload["hint"]) == 1_024
    refute Map.has_key?(payload, "context")
  end

  test "drops source positions outside the supplied query boundary" do
    assert Error.diagnose(:invalid_syntax, "query", 0).position == nil
    assert Error.diagnose(:invalid_syntax, "query", 7).position == nil
  end

  test "does not fabricate a position when a parser supplies no source span" do
    diagnostic = Error.diagnose(:invalid_syntax, "FROM runs WHERE @")

    assert diagnostic.position == nil
    assert diagnostic.hint =~ "FROM runs"
  end

  test "accepts only bounded wire-safe structured diagnostics" do
    diagnostic =
      Error.new(:query_no_bounded_plan,
        context: %{
          "predicates" => [%{"field" => "state", "operator" => "eq"}],
          "bounds" => %{"scanned_entries" => 50_000}
        }
      )

    assert Error.valid?(diagnostic)

    refute Error.valid?(%Error{
             reason: :query_no_bounded_plan,
             detail: String.duplicate("x", 1_025)
           })

    forged = %Error{
      reason: :query_no_bounded_plan,
      context: %{"provider_state" => self()}
    }

    refute Error.valid?(forged)
    assert Error.payload(forged)["code"] == "query_engine_failure"
    assert Error.status(forged) == :error
    assert Error.format(forged) == "ERR Flow query engine failed"

    sanitized = Error.new(:query_no_bounded_plan, context: %{"provider_state" => self()})
    assert sanitized.context == %{}
    assert Error.valid?(sanitized)
  end

  test "diagnostic context rejects integers outside the signed native wire range" do
    diagnostic =
      Error.new(:query_no_bounded_plan,
        context: %{"counter" => 0x8000_0000_0000_0000}
      )

    assert diagnostic.context == %{}
    assert Error.valid?(diagnostic)
  end

  defp atom_payload(reason) do
    Error.payload(reason)
    |> Map.new(fn {key, value} -> {String.to_atom(key), value} end)
  end

  defp marker_byte(query, marker, occurrence \\ 1) do
    query
    |> :binary.matches(marker)
    |> Enum.at(occurrence - 1)
    |> elem(0)
    |> Kernel.+(1)
  end
end
