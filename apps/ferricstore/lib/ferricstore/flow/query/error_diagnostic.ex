defmodule Ferricstore.Flow.Query.ErrorDiagnostic do
  @moduledoc false

  alias Ferricstore.Flow.Query.{Error, Field}

  @syntax_detail "FQL1 could not parse the query near the reported position."
  @syntax_hint "Use FROM runs|events WHERE ..., optionally prefixed by EXPLAIN or EXPLAIN ANALYZE; collection reads require ORDER BY, LIMIT, and RETURN RECORDS."
  @field_detail "The field at the reported position is not available in FQL1."
  @field_hint "Use a supported built-in field or one of the documented metadata field forms."
  @source_detail "The source at the reported position is not available in FQL1."
  @source_hint "Use FROM runs or FROM events."
  @shape_detail "The query parsed, but its clauses do not match a supported FQL1 request shape."
  @shape_hint "Point reads require run_id, plus partition_key for explicitly partitioned runs, and RETURN RECORD. Collection reads require a partition_key predicate, integer ORDER BY, LIMIT, and RETURN RECORDS; counts use RETURN COUNT without ordering or limits. Event history requires run_id, optional partition_key, ORDER BY event_id, LIMIT, and RETURN RECORDS."
  @cursor_detail "EXPLAIN and EXPLAIN ANALYZE plan a fresh query and do not accept a pagination cursor."
  @cursor_hint "Remove CURSOR, or execute the query without an EXPLAIN prefix to resume a page."

  @spec build(atom(), binary()) :: Error.t()
  def build(reason, query) when is_atom(reason) and is_binary(query), do: build_error(reason, nil)

  def build(reason, _query) when is_atom(reason), do: Error.new(reason)

  @spec build(atom(), binary(), pos_integer()) :: Error.t()
  def build(reason, query, byte)
      when is_atom(reason) and is_binary(query) and is_integer(byte) do
    build_error(reason, source_position(query, byte))
  end

  def build(reason, _query, _byte) when is_atom(reason), do: Error.new(reason)

  defp build_error(reason, position) do
    case reason do
      :invalid_syntax ->
        Error.new(reason,
          detail: @syntax_detail,
          hint: @syntax_hint,
          position: position
        )

      :unsupported_field ->
        supported_fields = Field.supported_external_names()

        Error.new(reason,
          detail: @field_detail,
          hint: @field_hint <> " Valid fields: " <> Enum.join(supported_fields, ", ") <> ".",
          position: position,
          context: %{"supported_fields" => supported_fields}
        )

      :unsupported_source ->
        Error.new(reason,
          detail: @source_detail,
          hint: @source_hint,
          position: position,
          context: %{"supported_sources" => ["events", "runs"]}
        )

      :unsupported_query_shape ->
        Error.new(reason,
          detail: @shape_detail,
          hint: @shape_hint,
          position: position
        )

      :query_cursor_invalid ->
        Error.new(reason,
          detail: @cursor_detail,
          hint: @cursor_hint,
          position: position
        )

      _other ->
        Error.new(reason)
    end
  end

  defp source_position(query, byte) when byte > 0 and byte <= byte_size(query) + 1 do
    {line, column} = line_column(query, byte - 1, 1, 1)
    %{byte: byte, line: line, column: column}
  end

  defp source_position(_query, _byte), do: nil

  defp line_column(_query, 0, line, column), do: {line, column}

  defp line_column(<<?\n, rest::binary>>, remaining, line, _column),
    do: line_column(rest, remaining - 1, line + 1, 1)

  defp line_column(<<codepoint::utf8, rest::binary>>, remaining, line, column) do
    size = utf8_size(codepoint)

    if size <= remaining,
      do: line_column(rest, remaining - size, line, column + 1),
      else: {line, column + remaining}
  end

  defp line_column(<<_byte, rest::binary>>, remaining, line, column),
    do: line_column(rest, remaining - 1, line, column + 1)

  defp utf8_size(codepoint) when codepoint <= 0x7F, do: 1
  defp utf8_size(codepoint) when codepoint <= 0x7FF, do: 2
  defp utf8_size(codepoint) when codepoint <= 0xFFFF, do: 3
  defp utf8_size(_codepoint), do: 4
end
