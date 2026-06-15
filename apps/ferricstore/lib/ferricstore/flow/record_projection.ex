defmodule Ferricstore.Flow.RecordProjection do
  @moduledoc false

  @meta_keys [
    :id,
    :type,
    :state,
    :version,
    :priority,
    :partition_key,
    :payload_ref,
    :result_ref,
    :error_ref,
    :created_at_ms,
    :updated_at_ms,
    :next_run_at_ms,
    :lease_deadline_ms,
    :lease_owner,
    :lease_token,
    :fencing_token,
    :attempts,
    :run_state,
    :value_refs
  ]

  def meta(record) when is_map(record), do: :maps.with(@meta_keys, record)
  def meta(record), do: record

  def maybe_meta(record, opts) do
    if Keyword.get(opts, :return) == :meta, do: meta(record), else: record
  end

  def maybe_meta_result({:ok, record}, opts) when is_map(record) do
    if Keyword.get(opts, :return) == :meta do
      {:ok, meta(record)}
    else
      {:ok, record}
    end
  end

  def maybe_meta_result({:ok, records}, opts) when is_list(records) do
    if Keyword.get(opts, :return) == :meta do
      {:ok, Enum.map(records, &meta/1)}
    else
      {:ok, records}
    end
  end

  def maybe_meta_result(result, _opts), do: result
end
