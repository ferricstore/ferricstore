defmodule FerricStore.Flow.Job do
  @moduledoc """
  Claimed Flow job returned by `FerricStore.Flow.Workflow` modules.

  `FerricStore.Flow.Job` wraps the raw Flow record returned by
  `flow_claim_due/2`. It keeps the raw `:record` map available while exposing
  the fields most handlers need directly:

    * `:id`
    * `:type`
    * `:state`
    * `:partition_key`
    * `:lease_token`
    * `:fencing_token`
    * `:payload`
    * `:payload_ref`

  SDK helpers use these guard fields automatically:

      BillingFlow.ok(job, result)
      BillingFlow.error(job, reason)
      BillingFlow.extend_lease(job, lease_ms: 60_000)

  `guard_opts/2` adds `:partition_key` and `:fencing_token` to command options.
  `lease_guard_opts/2` also adds `:lease_token`, used by guarded transitions.

  The job is not durable state by itself. Durable truth remains the Flow record
  in FerricStore.
  """

  @enforce_keys [:workflow, :record, :id, :type, :state]
  defstruct [
    :workflow,
    :record,
    :id,
    :type,
    :state,
    :partition_key,
    :lease_token,
    :fencing_token,
    :payload,
    :payload_ref,
    :payload_omitted,
    :payload_size
  ]

  @type t :: %__MODULE__{
          workflow: module(),
          record: map(),
          id: binary(),
          type: binary(),
          state: binary(),
          partition_key: binary() | nil,
          lease_token: binary() | nil,
          fencing_token: non_neg_integer() | nil,
          payload: term(),
          payload_ref: binary() | nil,
          payload_omitted: boolean() | nil,
          payload_size: non_neg_integer() | nil
        }

  @spec new(module(), map()) :: t()
  def new(workflow, record) when is_atom(workflow) and is_map(record) do
    %__MODULE__{
      workflow: workflow,
      record: record,
      id: field(record, :id),
      type: field(record, :type),
      state: field(record, :state),
      partition_key: field(record, :partition_key),
      lease_token: field(record, :lease_token),
      fencing_token: field(record, :fencing_token),
      payload: field(record, :payload),
      payload_ref: field(record, :payload_ref),
      payload_omitted: field(record, :payload_omitted),
      payload_size: field(record, :payload_size)
    }
  end

  @spec guard_opts(t(), keyword()) :: keyword()
  def guard_opts(%__MODULE__{} = job, opts \\ []) when is_list(opts) do
    opts
    |> maybe_put_new(:fencing_token, job.fencing_token)
    |> maybe_put_new(:partition_key, job.partition_key)
  end

  @spec lease_guard_opts(t(), keyword()) :: keyword()
  def lease_guard_opts(%__MODULE__{} = job, opts \\ []) when is_list(opts) do
    guard_opts(job, opts)
    |> maybe_put_new(:lease_token, job.lease_token)
  end

  defp field(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp maybe_put_new(opts, _key, nil), do: opts
  defp maybe_put_new(opts, key, value), do: Keyword.put_new(opts, key, value)
end
