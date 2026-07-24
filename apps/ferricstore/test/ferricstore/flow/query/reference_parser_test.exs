defmodule Ferricstore.Flow.Query.ReferenceParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Binder, Error, Limits, ReferenceParser, Request}
  alias Ferricstore.Store.Router

  describe "parse/1" do
    test "parses bounded run and history return projections" do
      assert {:ok,
              %Request{
                source: :runs,
                projection: [
                  :run_id,
                  :state,
                  :attributes,
                  :state_meta,
                  {:attribute, "customer"},
                  {:state_meta, "review.v2", "owner"}
                ]
              }} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-1' " <>
                   "RETURN RECORD (run_id, state, attributes, state_meta, attribute['customer'], " <>
                   "state_meta['review.v2']['owner'])"
               )

      assert {:ok,
              %Request{
                source: :events,
                projection: [:event_id, {:event_field, "event"}, {:event_field, "worker"}]
              }} =
               ReferenceParser.parse(
                 "FROM events WHERE run_id = 'run-1' ORDER BY event_id DESC LIMIT 10 " <>
                   "RETURN RECORDS (event_id, fields['event'], fields['worker']);"
               )
    end

    test "rejects unsafe, duplicate, empty, and oversized return projections" do
      prefix =
        "FROM runs WHERE partition_key = 'tenant-a' ORDER BY updated_at_ms DESC LIMIT 10 " <>
          "RETURN RECORDS "

      assert {:error, :unsupported_field} = ReferenceParser.parse(prefix <> "(lease_token)")
      assert {:error, :unsupported_field} = ReferenceParser.parse(prefix <> "(event_id)")

      assert {:error, :duplicate_projection_field} =
               ReferenceParser.parse(prefix <> "(state, STATE)")

      assert {:error, :unsupported_query_shape} = ReferenceParser.parse(prefix <> "()")
      assert {:error, :unsupported_query_shape} = ReferenceParser.parse(prefix <> "(state,)")

      fields =
        1..(Limits.max_return_fields() + 1)
        |> Enum.map_join(", ", &"attribute['field-#{&1}']")

      assert {:error, :query_projection_limit_exceeded} =
               ReferenceParser.parse(prefix <> "(" <> fields <> ")")

      history =
        "FROM events WHERE run_id = 'run-1' ORDER BY event_id ASC LIMIT 10 RETURN RECORDS "

      assert {:error, :unsupported_field} = ReferenceParser.parse(history <> "(state)")

      assert {:error, :unsupported_field} =
               ReferenceParser.parse(history <> "(fields['__internal'])")
    end

    test "parses the bounded run point-read shape" do
      assert {:ok,
              %Request{
                version: 1,
                mode: :execute,
                source: :runs,
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
                     {:eq, :run_id, {:literal, :keyword, "run-123"}}
                   ]},
                order_by: [],
                limit: 1,
                return: :record
              }} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"
               )
    end

    test "parses a run-id point read that uses the canonical auto partition" do
      assert {:ok,
              %Request{
                source: :runs,
                predicate: {:and, [{:eq, :run_id, {:literal, :keyword, "run-auto"}}]},
                order_by: [],
                limit: 1,
                return: :record
              }} = ReferenceParser.parse("FROM runs WHERE run_id = 'run-auto' RETURN RECORD")
    end

    test "parses bounded direct event history" do
      query =
        "FROM events WHERE run_id = @run_id " <>
          "ORDER BY event_id DESC LIMIT 25 RETURN RECORDS"

      assert {:ok,
              %Request{
                source: :events,
                predicate: {:and, [{:eq, :run_id, {:parameter, :keyword, "run_id"}}]},
                order_by: [{:event_id, :desc}],
                limit: 25,
                return: :record
              }} = ReferenceParser.parse(query)
    end

    test "parses a partition-contained parent lineage query" do
      query =
        "FROM runs WHERE partition_key = @partition AND parent_flow_id = @parent " <>
          "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"

      assert {:ok,
              %Request{
                source: :runs,
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:parameter, :keyword, "partition"}},
                     {:eq, :parent_flow_id, {:parameter, :keyword, "parent"}}
                   ]},
                order_by: [{:updated_at_ms, :desc}],
                limit: 25,
                return: :record
              }} = ReferenceParser.parse(query)
    end

    test "normalizes keywords and supports a trailing semicolon" do
      assert {:ok,
              %Request{
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:literal, :keyword, "partition"}},
                     {:eq, :run_id, {:literal, :keyword, "Run''42"}}
                   ]}
              }} =
               ReferenceParser.parse(
                 "from RUNS where RUN_ID = 'Run''''42' and PARTITION_KEY = 'partition' return record;"
               )
    end

    test "parses EXPLAIN and named parameters without binding values" do
      assert {:ok,
              %Request{
                mode: :explain,
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:parameter, :keyword, "partition"}},
                     {:eq, :run_id, {:parameter, :keyword, "flow_id"}}
                   ]}
              }} =
               ReferenceParser.parse(
                 "EXPLAIN FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
               )
    end

    test "parses EXPLAIN ANALYZE as a distinct fresh-execution mode" do
      query =
        "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition " <>
          "AND state = 'failed' ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

      assert {:ok,
              %Request{
                mode: :analyze,
                cursor: nil,
                limit: 10,
                return: :record
              }} = ReferenceParser.parse(query)

      assert {:error, :invalid_syntax} =
               ReferenceParser.parse("ANALYZE FROM runs WHERE run_id = 'run-1' RETURN RECORD")

      assert {:error, :query_cursor_invalid} =
               ReferenceParser.parse(
                 "EXPLAIN ANALYZE FROM runs WHERE partition_key = @partition " <>
                   "ORDER BY updated_at_ms DESC LIMIT 10 CURSOR @page RETURN RECORDS"
               )
    end

    test "parses bounded collection equality, IN, time window, order, and limit" do
      query =
        "FROM runs WHERE partition_key = @tenant " <>
          "AND state IN ('failed', 'completed') " <>
          "AND updated_at_ms FROM @from TO @until " <>
          "ORDER BY updated_at_ms DESC LIMIT 25 RETURN RECORDS"

      assert {:ok,
              %Request{
                mode: :execute,
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:parameter, :keyword, "tenant"}},
                     {:in, :state,
                      [
                        {:literal, :keyword, "failed"},
                        {:literal, :keyword, "completed"}
                      ]},
                     {:time_window, :updated_at_ms, {:parameter, :integer, "from"},
                      {:parameter, :integer, "until"}}
                   ]},
                order_by: [{:updated_at_ms, :desc}],
                limit: 25,
                return: :record
              }} = ReferenceParser.parse(query)
    end

    test "parses an optional parameterized collection cursor" do
      query =
        "FROM runs WHERE partition_key = @tenant " <>
          "ORDER BY updated_at_ms DESC LIMIT 25 " <>
          "CURSOR @page RETURN RECORDS"

      assert {:ok,
              %Request{
                cursor: {:parameter, :keyword, "page"},
                limit: 25,
                order_by: [{:updated_at_ms, :desc}]
              }} = ReferenceParser.parse(query)

      assert {:error, :unsupported_query_shape} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = @tenant ORDER BY run_id ASC LIMIT 10 " <>
                   "CURSOR 'token-in-query-text' RETURN RECORDS"
               )

      assert {:error, :unsupported_query_shape} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = @tenant AND run_id = @id " <>
                   "CURSOR @page RETURN RECORD"
               )
    end

    test "parses inclusive ranges and explicit null or missing predicates" do
      for {syntax, expected} <- [
            {"priority BETWEEN 1 AND 5", {:range, :priority, integer(1), integer(5)}},
            {"attribute.region IS NULL", {:is, {:attribute, "region"}, :null}},
            {"attribute.region IS MISSING", {:is, {:attribute, "region"}, :missing}}
          ] do
        query =
          "EXPLAIN FROM runs WHERE partition_key = 'tenant-a' AND #{syntax} " <>
            "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS;"

        assert {:ok, %Request{mode: :explain, predicate: {:and, [_tenant, ^expected]}}} =
                 ReferenceParser.parse(query)
      end
    end

    test "parses safely quoted metadata field segments" do
      query =
        "FROM runs WHERE partition_key = @tenant " <>
          "AND state_meta['review.v2']['ai.model'] = @model " <>
          "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

      assert {:ok,
              %Request{
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:parameter, :keyword, "tenant"}},
                     {:eq, {:state_meta, "review.v2", "ai.model"},
                      {:parameter, :dynamic, "model"}}
                   ]}
              }} = ReferenceParser.parse(query)

      assert {:ok, _request} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = @tenant " <>
                   "AND attribute['customer''s.region'] = @region " <>
                   "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"
               )
    end

    test "parses a bounded scalar count without row ordering or pagination" do
      query =
        "EXPLAIN FROM runs WHERE partition_key = @partition " <>
          "AND type = 'payment' AND state = 'failed' RETURN COUNT;"

      assert {:ok,
              %Request{
                mode: :explain,
                source: :runs,
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:parameter, :keyword, "partition"}},
                     {:eq, :type, {:literal, :keyword, "payment"}},
                     {:eq, :state, {:literal, :keyword, "failed"}}
                   ]},
                order_by: [],
                limit: nil,
                cursor: nil,
                return: :count
              }} = ReferenceParser.parse(query)

      for invalid <- [
            "FROM events WHERE partition_key = 'p' RETURN COUNT",
            "FROM runs WHERE partition_key = 'p' ORDER BY updated_at_ms DESC LIMIT 1 RETURN COUNT",
            "FROM runs WHERE partition_key = 'p' LIMIT 1 RETURN COUNT",
            "FROM runs WHERE partition_key = 'p' CURSOR @page RETURN COUNT"
          ] do
        assert {:error, :unsupported_query_shape} = ReferenceParser.parse(invalid)
      end
    end

    test "rejects collection queries without explicit bounds or malformed IN lists" do
      for query <- [
            "FROM runs WHERE partition_key = 'tenant-a' RETURN RECORDS",
            "FROM runs WHERE partition_key = 'tenant-a' LIMIT 10 RETURN RECORDS",
            "FROM runs WHERE partition_key = 'tenant-a' AND state IN () LIMIT 10 RETURN RECORDS",
            "FROM runs WHERE partition_key = 'tenant-a' AND state IN ('a',) LIMIT 10 RETURN RECORDS"
          ] do
        assert {:error, :unsupported_query_shape} = ReferenceParser.parse(query)
      end
    end

    test "rejects unbounded and unsupported query shapes" do
      assert {:error, :unsupported_query_shape} =
               ReferenceParser.parse("FROM runs RETURN RECORDS")

      assert {:error, :unsupported_field} =
               ReferenceParser.parse(
                 "FROM runs WHERE tenant_ref = 'forged' AND run_id = 'one' RETURN RECORD"
               )

      assert {:error, :unsupported_query_shape} =
               ReferenceParser.parse(
                 "FROM runs WHERE partition_key = 'p' OR run_id = 'two' RETURN RECORD"
               )
    end

    test "rejects oversized input before tokenization" do
      query =
        "FROM runs WHERE partition_key = 'p' AND run_id = '" <>
          String.duplicate("x", 16_385) <> "' RETURN RECORD"

      assert {:error, :query_too_large} = ReferenceParser.parse(query)
    end

    test "rejects huge integer literals without constructing an unbounded integer" do
      query =
        "FROM runs WHERE partition_key = 'tenant-a' AND priority BETWEEN " <>
          String.duplicate("9", 8_000) <>
          " AND 9 ORDER BY run_id ASC LIMIT 1 RETURN RECORDS"

      assert {:error, :invalid_parameter_type} = ReferenceParser.parse(query)
    end
  end

  describe "parse_diagnostic/1" do
    test "reports the unsupported source and field tokens after optional mode prefixes" do
      field_query =
        "FROM runs WHERE tenant_secret = 'x' " <>
          "ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

      assert {:error, %Error{reason: :unsupported_field, position: %{byte: 17, column: 17}}} =
               ReferenceParser.parse_diagnostic(field_query)

      source_query =
        "EXPLAIN FROM jobs WHERE partition_key = 'p' " <>
          "ORDER BY updated_at_ms DESC LIMIT 1 RETURN RECORDS"

      assert {:error, %Error{reason: :unsupported_source, position: %{byte: 14, column: 14}}} =
               ReferenceParser.parse_diagnostic(source_query)
    end

    test "reports a later unsupported field after a valid metadata predicate" do
      query =
        "FROM runs WHERE state_meta['queued']['priority'] = 'high' " <>
          "AND tenant_secret = 'x' ORDER BY updated_at_ms DESC LIMIT 10 RETURN RECORDS"

      {byte_offset, _length} = :binary.match(query, "tenant_secret")

      assert {:error,
              %Error{
                reason: :unsupported_field,
                position: %{byte: byte, line: 1, column: column}
              }} = ReferenceParser.parse_diagnostic(query)

      assert byte == byte_offset + 1
      assert column == byte
    end

    test "reports unsupported and duplicate return projection fields at the rejected selector" do
      unsupported =
        "FROM runs WHERE run_id = 'run-1' RETURN RECORD (lease_token)"

      {unsupported_offset, _length} = :binary.match(unsupported, "lease_token")

      assert {:error,
              %Error{
                reason: :unsupported_field,
                position: %{byte: unsupported_byte, line: 1, column: unsupported_byte}
              }} = ReferenceParser.parse_diagnostic(unsupported)

      assert unsupported_byte == unsupported_offset + 1

      duplicate =
        "FROM runs WHERE run_id = 'run-1' RETURN RECORD (state, STATE)"

      {duplicate_offset, _length} = :binary.match(duplicate, "STATE")

      assert {:error,
              %Error{
                reason: :duplicate_projection_field,
                position: %{byte: duplicate_byte, line: 1, column: duplicate_byte}
              }} = ReferenceParser.parse_diagnostic(duplicate)

      assert duplicate_byte == duplicate_offset + 1
    end

    test "reports the clause that appears where WHERE is required" do
      assert {:error,
              %Error{
                reason: :unsupported_query_shape,
                position: %{byte: 19, line: 1, column: 19}
              }} = ReferenceParser.parse_diagnostic("EXPLAIN FROM runs RETURN RECORDS")
    end

    test "rejects oversized input without a second diagnostic scan" do
      oversized = :binary.copy("x", Limits.max_query_bytes() + 1)
      {:reductions, reductions_before} = :erlang.process_info(self(), :reductions)

      result = ReferenceParser.parse_diagnostic(oversized)

      {:reductions, reductions_after} = :erlang.process_info(self(), :reductions)

      assert {:error, %Error{reason: :query_too_large, position: nil}} = result

      assert reductions_after - reductions_before < 256,
             "oversized diagnostics must stay O(1) instead of scanning the rejected query"
    end
  end

  describe "bind/2" do
    test "binds a named keyword parameter" do
      {:ok, request} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
        )

      assert {:ok,
              %Request{
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:literal, :keyword, "tenant-a"}},
                     {:eq, :run_id, {:literal, :keyword, "run-123"}}
                   ]}
              }} =
               Binder.bind(request, %{"partition" => "tenant-a", "flow_id" => "run-123"})
    end

    test "binds repeated, mixed, and literal-only values exactly" do
      {:ok, repeated} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @id AND run_id = @id RETURN RECORD"
        )

      assert {:ok,
              %Request{
                predicate:
                  {:and,
                   [
                     {:eq, :partition_key, {:literal, :keyword, "same"}},
                     {:eq, :run_id, {:literal, :keyword, "same"}}
                   ]}
              }} = Binder.bind(repeated, %{"id" => "same"})

      {:ok, mixed} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = 'tenant-a' AND run_id = @id RETURN RECORD"
        )

      assert {:ok, _bound} = Binder.bind(mixed, %{"id" => "run-123"})

      {:ok, literals} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = 'tenant-a' AND run_id = 'run-123' RETURN RECORD"
        )

      assert {:ok, ^literals} = Binder.bind(literals, %{})
      assert {:error, :unexpected_parameter} = Binder.bind(literals, %{"unused" => "secret"})
    end

    test "fails closed on missing, mistyped, and unused parameters" do
      {:ok, request} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
        )

      assert {:error, :missing_parameter} = Binder.bind(request, %{})

      assert {:error, :invalid_parameter_type} =
               Binder.bind(request, %{"partition" => "tenant-a", "flow_id" => 123})

      assert {:error, :unexpected_parameter} =
               Binder.bind(request, %{
                 "partition" => "tenant-a",
                 "flow_id" => "run-123",
                 "unused" => "secret"
               })

      assert {:error, :invalid_parameters} =
               Binder.bind(request, %{partition: "tenant-a", flow_id: "run-123"})
    end

    test "rejects unsupported canonical predicates without raising" do
      request = %Request{
        mode: :execute,
        source: :runs,
        predicate: {:and, [{:eq, :run_id, {:parameter, :keyword, "flow_id"}}]},
        order_by: [],
        limit: 1,
        return: :payload
      }

      assert {:error, :unsupported_query_shape} =
               Binder.bind(request, %{"flow_id" => "run-123"})
    end

    test "rejects malformed canonical envelopes before binding" do
      {:ok, request} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
        )

      params = %{"partition" => "tenant-a", "flow_id" => "run-123"}

      assert {:error, :unsupported_query_shape} =
               Binder.bind(%{request | source: :events}, params)

      for malformed <- [
            %{request | mode: :invalid},
            %{request | order_by: [{:run_id, :asc}]},
            %{request | limit: 2},
            %{request | return: :payload}
          ] do
        assert {:error, :unsupported_query_shape} = Binder.bind(malformed, params)
      end
    end

    test "rejects malformed canonical values without raising" do
      request =
        Request.point_read(
          :execute,
          {:parameter, :keyword, 123},
          {:literal, :keyword, "run-123"}
        )

      assert {:error, :invalid_parameter_type} = Binder.bind(request, %{})
    end

    test "rejects oversized bound values before planning" do
      {:ok, request} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
        )

      oversized = String.duplicate("x", Router.max_key_size() + 1)

      assert {:error, :query_value_too_large} =
               Binder.bind(request, %{"partition" => oversized, "flow_id" => "run-123"})

      assert {:error, :query_value_too_large} =
               Binder.bind(request, %{"partition" => "tenant-a", "flow_id" => oversized})
    end

    test "accepts exact storage-key boundaries and rejects the next byte" do
      {:ok, request} =
        ReferenceParser.parse(
          "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"
        )

      state_key_overhead = byte_size("f:{f:" <> String.duplicate("x", 43) <> "}:s:")
      max_run_id_bytes = Router.max_key_size() - state_key_overhead
      max_partition = String.duplicate("p", Router.max_key_size())
      max_run_id = String.duplicate("r", max_run_id_bytes)

      assert {:ok, _bound} =
               Binder.bind(request, %{"partition" => max_partition, "flow_id" => "r"})

      assert {:ok, _bound} =
               Binder.bind(request, %{"partition" => "p", "flow_id" => max_run_id})

      assert {:error, :query_value_too_large} =
               Binder.bind(request, %{
                 "partition" => max_partition <> "p",
                 "flow_id" => "r"
               })

      assert {:error, :query_value_too_large} =
               Binder.bind(request, %{
                 "partition" => "p",
                 "flow_id" => max_run_id <> "r"
               })
    end
  end

  defp integer(value), do: {:literal, :integer, value}
end
