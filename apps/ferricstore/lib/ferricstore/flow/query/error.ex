defmodule Ferricstore.Flow.Query.Error do
  @moduledoc false

  @errors %{
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

  @spec message(atom()) :: binary()
  def message(reason), do: reason |> error() |> elem(1)

  @spec payload(atom()) :: map()
  def payload(reason) do
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

  @spec status(atom()) :: :bad_request | :error | :noperm
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

  defp error(reason), do: Map.get(@errors, reason, @errors.invalid_syntax)
end
