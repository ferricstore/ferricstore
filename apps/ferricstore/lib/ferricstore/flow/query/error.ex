defmodule Ferricstore.Flow.Query.Error do
  @moduledoc false

  alias Ferricstore.Flow.Query.ErrorDiagnostic

  @max_text_bytes 1_024
  @max_context_entries 16
  @max_context_list_items 32
  @max_context_key_bytes 128
  @max_context_depth 6
  @max_context_nodes 512
  @min_context_integer -0x8000_0000_0000_0000
  @max_context_integer 0x7FFF_FFFF_FFFF_FFFF

  @enforce_keys [:reason]
  defstruct [:reason, :detail, :hint, :position, context: %{}]

  @type position :: %{byte: pos_integer(), line: pos_integer(), column: pos_integer()}
  @type t :: %__MODULE__{
          reason: atom(),
          detail: binary() | nil,
          hint: binary() | nil,
          position: position() | nil,
          context: map()
        }

  @errors %{
    duplicate_projection_field:
      {"duplicate_projection_field", "ERR FQL1 return projection contains a duplicate field"},
    invalid_parameters: {"invalid_parameters", "ERR FQL1 parameters must be an object"},
    invalid_parameter_type: {"invalid_parameter_type", "ERR FQL1 parameter has an invalid type"},
    invalid_syntax: {"invalid_syntax", "ERR FQL1 invalid syntax"},
    missing_parameter: {"missing_parameter", "ERR FQL1 parameter is missing"},
    query_concurrency_exceeded:
      {"query_concurrency_exceeded", "ERR Flow query concurrency limit exceeded"},
    query_cursor_expired: {"query_cursor_expired", "ERR Flow query cursor expired"},
    query_cursor_invalid: {"query_cursor_invalid", "ERR Flow query cursor is invalid"},
    query_cursor_too_large:
      {"query_cursor_too_large", "ERR Flow query cursor exceeds the byte limit"},
    query_deadline_exceeded: {"query_deadline_exceeded", "ERR Flow query deadline exceeded"},
    query_engine_failure: {"query_engine_failure", "ERR Flow query engine failed"},
    query_hydration_budget_exceeded:
      {"query_hydration_budget_exceeded", "ERR Flow query hydration budget exceeded"},
    query_memory_budget_exceeded:
      {"query_memory_budget_exceeded", "ERR Flow query memory budget exceeded"},
    query_no_bounded_plan:
      {"query_no_bounded_plan", "ERR Flow query has no bounded execution plan"},
    query_projection_changed:
      {"query_projection_changed", "ERR Flow visibility projection changed during the query"},
    query_projection_limit_exceeded:
      {"query_projection_limit_exceeded", "ERR FQL1 return projection exceeds the field limit"},
    query_range_budget_exceeded:
      {"query_range_budget_exceeded", "ERR Flow query range budget exceeded"},
    query_response_budget_exceeded:
      {"query_response_budget_exceeded", "ERR Flow query response budget exceeded"},
    query_result_budget_exceeded:
      {"query_result_budget_exceeded", "ERR Flow query result budget exceeded"},
    query_scan_budget_exceeded:
      {"query_scan_budget_exceeded", "ERR Flow query scan budget exceeded"},
    query_scan_byte_budget_exceeded:
      {"query_scan_byte_budget_exceeded", "ERR Flow query scan byte budget exceeded"},
    query_storage_inconsistent:
      {"query_storage_inconsistent", "ERR Flow query storage record is inconsistent"},
    query_storage_unavailable:
      {"query_storage_unavailable", "ERR Flow query storage is unavailable"},
    query_too_large: {"query_too_large", "ERR FQL1 query exceeds the byte limit"},
    query_value_too_large: {"query_value_too_large", "ERR FQL1 value exceeds the byte limit"},
    unexpected_parameter: {"unexpected_parameter", "ERR FQL1 received an unexpected parameter"},
    unsupported_field: {"unsupported_field", "ERR FQL1 field is not supported"},
    unsupported_query_shape: {"unsupported_query_shape", "ERR FQL1 query shape is not supported"},
    unsupported_query_version: {"unsupported_query_version", "ERR unsupported FQL version"},
    unsupported_source: {"unsupported_source", "ERR FQL1 source is not supported"},
    unauthorized_scope: {"unauthorized_scope", "NOPERM Flow query scope is not authorized"}
  }
  @reasons_by_message Map.new(@errors, fn {reason, {_code, message}} -> {message, reason} end)

  @spec new(atom(), keyword()) :: t()
  def new(reason, opts \\ []) when is_atom(reason) and is_list(opts) do
    reason = if known?(reason), do: reason, else: :invalid_syntax

    %__MODULE__{
      reason: reason,
      detail: bounded_text(Keyword.get(opts, :detail)),
      hint: bounded_text(Keyword.get(opts, :hint)),
      position: valid_position(Keyword.get(opts, :position)),
      context: bounded_context(Keyword.get(opts, :context, %{}))
    }
  end

  @spec diagnose(atom(), binary()) :: t()
  defdelegate diagnose(reason, query), to: ErrorDiagnostic, as: :build

  @spec diagnose(atom(), binary(), pos_integer()) :: t()
  defdelegate diagnose(reason, query, byte), to: ErrorDiagnostic, as: :build

  @spec message(atom() | t()) :: binary()
  def message(%__MODULE__{reason: reason} = diagnostic),
    do: if(valid?(diagnostic), do: message(reason), else: message(:query_engine_failure))

  def message(reason), do: reason |> error() |> elem(1)

  @spec payload(atom() | t()) :: map()
  def payload(%__MODULE__{} = diagnostic) do
    if valid?(diagnostic) do
      diagnostic.reason
      |> base_payload()
      |> put_optional("detail", diagnostic.detail)
      |> put_optional("hint", diagnostic.hint)
      |> put_optional("position", wire_position(diagnostic.position))
      |> put_optional("context", diagnostic.context)
    else
      base_payload(:query_engine_failure)
    end
  end

  def payload(reason) do
    base_payload(reason)
  end

  defp base_payload(reason) do
    {code, message} = error(reason)
    retryable = reason in [:query_projection_changed, :query_storage_unavailable]

    %{
      "code" => code,
      "message" => message,
      "retryable" => retryable,
      "safe_to_retry" => retryable,
      "retry_after_ms" => 0
    }
  end

  @spec known?(term()) :: boolean()
  def known?(reason), do: is_atom(reason) and Map.has_key?(@errors, reason)

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{} = diagnostic) do
    known?(diagnostic.reason) and valid_bounded_text?(diagnostic.detail) and
      valid_bounded_text?(diagnostic.hint) and valid_position?(diagnostic.position) and
      valid_context?(diagnostic.context)
  end

  def valid?(_diagnostic), do: false

  @spec reason(binary() | t()) :: {:ok, atom()} | :error
  def reason(%__MODULE__{reason: reason} = diagnostic),
    do: if(valid?(diagnostic), do: {:ok, reason}, else: :error)

  def reason(message) when is_binary(message), do: Map.fetch(@reasons_by_message, message)

  @spec status(atom() | t()) :: :bad_request | :error | :noperm
  def status(%__MODULE__{reason: reason} = diagnostic),
    do: if(valid?(diagnostic), do: status(reason), else: :error)

  def status(:unauthorized_scope), do: :noperm

  def status(reason)
      when reason in [
             :query_engine_failure,
             :query_projection_changed,
             :query_storage_inconsistent,
             :query_storage_unavailable
           ],
      do: :error

  def status(_reason), do: :bad_request

  @spec format(atom() | t()) :: binary()
  def format(reason) when is_atom(reason), do: message(reason)

  def format(%__MODULE__{} = diagnostic) do
    if valid?(diagnostic) do
      diagnostic
      |> format_position(message(diagnostic))
      |> append_section("DETAIL", diagnostic.detail)
      |> append_section("HINT", diagnostic.hint)
    else
      message(:query_engine_failure)
    end
  end

  defp wire_position(%{byte: byte, line: line, column: column}),
    do: %{"byte" => byte, "line" => line, "column" => column}

  defp wire_position(_position), do: nil

  defp valid_position(%{byte: byte, line: line, column: column})
       when is_integer(byte) and byte > 0 and is_integer(line) and line > 0 and
              is_integer(column) and column > 0,
       do: %{byte: byte, line: line, column: column}

  defp valid_position(_position), do: nil

  defp bounded_text(value) when is_binary(value) and byte_size(value) <= @max_text_bytes,
    do: value

  defp bounded_text(_value), do: nil

  defp bounded_context(context) do
    if valid_context?(context), do: context, else: %{}
  end

  defp valid_bounded_text?(nil), do: true

  defp valid_bounded_text?(value),
    do: is_binary(value) and byte_size(value) <= @max_text_bytes

  defp valid_position?(nil), do: true

  defp valid_position?(%{byte: byte, line: line, column: column} = position) do
    map_size(position) == 3 and is_integer(byte) and byte > 0 and is_integer(line) and line > 0 and
      is_integer(column) and column > 0
  end

  defp valid_position?(_position), do: false

  defp valid_context?(context)
       when is_map(context) and map_size(context) <= @max_context_entries do
    match?(
      {:ok, _remaining},
      validate_wire_value(context, @max_context_depth, @max_context_nodes)
    )
  end

  defp valid_context?(_context), do: false

  defp validate_wire_value(_value, _depth, 0), do: :error

  defp validate_wire_value(value, _depth, remaining)
       when is_binary(value) and byte_size(value) <= @max_text_bytes,
       do: {:ok, remaining - 1}

  defp validate_wire_value(value, _depth, remaining)
       when is_integer(value) and value >= @min_context_integer and value <= @max_context_integer,
       do: {:ok, remaining - 1}

  defp validate_wire_value(value, _depth, remaining) when is_boolean(value) or is_nil(value),
    do: {:ok, remaining - 1}

  defp validate_wire_value(value, depth, remaining)
       when is_map(value) and depth > 0 and map_size(value) <= @max_context_entries do
    Enum.reduce_while(value, {:ok, remaining - 1}, fn {key, item}, {:ok, nodes} ->
      if valid_context_key?(key) do
        case validate_wire_value(item, depth - 1, nodes) do
          {:ok, _remaining} = valid -> {:cont, valid}
          :error -> {:halt, :error}
        end
      else
        {:halt, :error}
      end
    end)
  end

  defp validate_wire_value(value, depth, remaining) when is_list(value) and depth > 0 do
    validate_wire_list(value, depth - 1, remaining - 1, @max_context_list_items)
  end

  defp validate_wire_value(_value, _depth, _remaining), do: :error

  defp validate_wire_list([], _depth, remaining, _items), do: {:ok, remaining}
  defp validate_wire_list(_values, _depth, _remaining, 0), do: :error

  defp validate_wire_list([value | rest], depth, remaining, items) do
    case validate_wire_value(value, depth, remaining) do
      {:ok, remaining} -> validate_wire_list(rest, depth, remaining, items - 1)
      :error -> :error
    end
  end

  defp validate_wire_list(_improper, _depth, _remaining, _items), do: :error

  defp valid_context_key?(key),
    do: is_binary(key) and key != "" and byte_size(key) <= @max_context_key_bytes

  defp put_optional(map, _key, nil), do: map
  defp put_optional(map, _key, context) when context == %{}, do: map
  defp put_optional(map, key, value), do: Map.put(map, key, value)

  defp format_position(%__MODULE__{position: nil}, message), do: message

  defp format_position(
         %__MODULE__{position: %{byte: byte, line: line, column: column}},
         message
       ),
       do: "#{message} at line #{line}, column #{column} (byte #{byte})"

  defp append_section(message, _label, nil), do: message
  defp append_section(message, label, value), do: "#{message}; #{label}: #{value}"

  defp error(reason), do: Map.get(@errors, reason, @errors.invalid_syntax)
end
