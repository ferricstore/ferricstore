defmodule Ferricstore.Flow.Governance.Approval do
  @moduledoc false

  alias Ferricstore.Flow.Governance.Decision

  @max_assignees 1_000
  @max_dimension_bytes 65_535
  @max_field_bytes 262_144
  @max_record_bytes 900_000
  @max_exact_integer 9_007_199_254_740_991

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

  @doc false
  def valid?(%__MODULE__{} = approval) do
    required_dimension?(approval.id) and required_dimension?(approval.flow_id) and
      required_dimension?(approval.scope) and non_negative_integer?(approval.requested_at_ms) and
      optional_binary?(approval.reason) and optional_binary?(approval.requested_by) and
      optional_binary?(approval.policy_hash) and valid_policy_version?(approval.policy_version) and
      valid_expiry?(approval) and
      valid_assignees?(approval.assignees) and valid_decision?(approval) and
      :erlang.external_size(approval) <= @max_record_bytes
  end

  def valid?(_approval), do: false

  defp decide(%__MODULE__{status: :pending} = approval, status, opts) do
    now_ms = Keyword.fetch!(opts, :now_ms)

    cond do
      now_ms < approval.requested_at_ms ->
        {:error, "ERR flow approval now_ms cannot precede requested_at_ms"}

      expired?(approval, now_ms) ->
        expired = %{
          approval
          | status: :expired,
            decided_by: nil,
            decided_at_ms: now_ms,
            decision_reason: nil
        }

        {:error,
         Decision.conflict(%{
           approval_id: approval.id,
           status: :expired,
           message: "Approval #{approval.id} expired before it was decided"
         }), expired}

      true ->
        {:ok,
         %{
           approval
           | status: status,
             decided_by: Keyword.fetch!(opts, :approver),
             decided_at_ms: now_ms,
             decision_reason: Keyword.get(opts, :reason)
         }}
    end
  end

  defp decide(%__MODULE__{} = approval, _status, _opts) do
    {:error,
     Decision.conflict(%{
       approval_id: approval.id,
       status: approval.status,
       message: "Approval #{approval.id} already has terminal status #{approval.status}"
     })}
  end

  defp valid_decision?(%__MODULE__{status: :pending} = approval) do
    is_nil(approval.decided_by) and is_nil(approval.decided_at_ms) and
      is_nil(approval.decision_reason)
  end

  defp valid_decision?(%__MODULE__{status: status} = approval)
       when status in [:approved, :rejected] do
    required_binary?(approval.decided_by) and non_negative_integer?(approval.decided_at_ms) and
      approval.decided_at_ms >= approval.requested_at_ms and
      decision_precedes_expiry?(approval) and
      optional_binary?(approval.decision_reason)
  end

  defp valid_decision?(%__MODULE__{status: :expired} = approval) do
    is_integer(approval.expires_at_ms) and is_nil(approval.decided_by) and
      is_nil(approval.decision_reason) and
      non_negative_integer?(approval.decided_at_ms) and
      approval.decided_at_ms >= approval.requested_at_ms and
      approval.decided_at_ms >= approval.expires_at_ms
  end

  defp valid_decision?(_approval), do: false

  defp valid_assignees?(assignees) when is_list(assignees),
    do: valid_assignees?(assignees, MapSet.new(), 0)

  defp valid_assignees?(_assignees), do: false

  defp valid_assignees?([], _seen, _count), do: true

  defp valid_assignees?([assignee | rest], seen, count)
       when count < @max_assignees and is_binary(assignee) and assignee != "" and
              byte_size(assignee) <= @max_dimension_bytes do
    if MapSet.member?(seen, assignee) do
      false
    else
      valid_assignees?(rest, MapSet.put(seen, assignee), count + 1)
    end
  end

  defp valid_assignees?(_assignees, _seen, _count), do: false

  defp valid_policy_version?(nil), do: true

  defp valid_policy_version?(version) when is_binary(version),
    do: version != "" and byte_size(version) <= @max_field_bytes

  defp valid_policy_version?(version), do: non_negative_integer?(version)

  defp valid_expiry?(%__MODULE__{expires_at_ms: nil}), do: true

  defp valid_expiry?(%__MODULE__{} = approval) do
    non_negative_integer?(approval.expires_at_ms) and
      approval.expires_at_ms >= approval.requested_at_ms
  end

  defp expired?(%__MODULE__{expires_at_ms: expires_at_ms}, now_ms)
       when is_integer(expires_at_ms),
       do: now_ms >= expires_at_ms

  defp expired?(%__MODULE__{}, _now_ms), do: false

  defp decision_precedes_expiry?(%__MODULE__{expires_at_ms: nil}), do: true

  defp decision_precedes_expiry?(%__MODULE__{} = approval),
    do: approval.decided_at_ms < approval.expires_at_ms

  defp required_binary?(value), do: is_binary(value) and value != ""

  defp required_dimension?(value),
    do: required_binary?(value) and byte_size(value) <= @max_dimension_bytes

  defp optional_binary?(nil), do: true

  defp optional_binary?(value),
    do: required_binary?(value) and byte_size(value) <= @max_field_bytes

  defp non_negative_integer?(value),
    do: is_integer(value) and value >= 0 and value <= @max_exact_integer
end
