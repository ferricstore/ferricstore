defmodule Ferricstore.Flow.Query.RecordProjection do
  @moduledoc false

  # Query records are an allowlist so newly added storage/control fields never
  # become remotely visible without an explicit query-contract decision.
  @fields [
    :id,
    :type,
    :state,
    :version,
    :priority,
    :partition_key,
    :created_at_ms,
    :updated_at_ms,
    :next_run_at_ms,
    :lease_deadline_ms,
    :attempts,
    :run_state,
    :max_active_ms,
    :parent_flow_id,
    :root_flow_id,
    :correlation_id
  ]

  @spec project_result({:ok, map() | nil} | {:error, term()}) ::
          {:ok, map() | nil} | {:error, term()}
  def project_result({:ok, record}) when is_map(record),
    do: {:ok, :maps.with(@fields, record)}

  def project_result(result), do: result
end
