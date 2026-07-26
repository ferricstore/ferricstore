defmodule Ferricstore.Flow.Query.RunRecordFields do
  @moduledoc false

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
    :correlation_id,
    :attributes,
    :state_meta
  ]
  @builtins @fields -- [:attributes, :state_meta]

  @spec all() :: [atom()]
  def all, do: @fields

  @spec builtins() :: [atom()]
  def builtins, do: @builtins
end
