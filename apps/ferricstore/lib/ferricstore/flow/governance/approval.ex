defmodule Ferricstore.Flow.Governance.Approval do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  defstruct [
    :id,
    :flow_id,
    :scope,
    :reason,
    :requested_by,
    :requested_at_ms,
    :policy_hash,
    :policy_version,
    :expires_at_ms,
    :decided_by,
    :decided_at_ms,
    :decision_reason,
    assignees: [],
    status: :pending
  ]

  def request(id, opts) when is_binary(id) do
    %__MODULE__{
      id: id,
      flow_id: Keyword.fetch!(opts, :flow_id),
      scope: Keyword.fetch!(opts, :scope),
      reason: Keyword.get(opts, :reason),
      requested_by: Keyword.get(opts, :requested_by),
      requested_at_ms: Keyword.fetch!(opts, :now_ms),
      assignees: Keyword.get(opts, :assignees, []),
      policy_hash: Keyword.get(opts, :policy_hash),
      policy_version: Keyword.get(opts, :policy_version),
      expires_at_ms: Keyword.get(opts, :expires_at_ms)
    }
  end

  def approve(%__MODULE__{} = approval, opts), do: decide(approval, :approved, opts)
  def reject(%__MODULE__{} = approval, opts), do: decide(approval, :rejected, opts)

  defp decide(%__MODULE__{status: :pending} = approval, status, opts) do
    {:ok,
     %{
       approval
       | status: status,
         decided_by: Keyword.fetch!(opts, :approver),
         decided_at_ms: Keyword.fetch!(opts, :now_ms),
         decision_reason: Keyword.get(opts, :reason)
     }}
  end

  defp decide(%__MODULE__{} = approval, _status, _opts) do
    {:error,
     Decision.conflict(%{
       approval_id: approval.id,
       status: approval.status,
       message: "Approval #{approval.id} already has terminal status #{approval.status}"
     })}
  end
end
