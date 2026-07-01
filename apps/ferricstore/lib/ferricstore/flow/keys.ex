defmodule Ferricstore.Flow.Keys do
  @moduledoc false

  @global_tag "{f}"
  @partition_tag_prefix "{f:"
  @auto_partition_prefix "__flow_auto__:"
  @auto_partition_buckets 256
  @auto_partition_tags 0..(@auto_partition_buckets - 1)
                       |> Enum.map(&("{fa:" <> Integer.to_string(&1) <> "}"))
                       |> List.to_tuple()
  @auto_partition_keys 0..(@auto_partition_buckets - 1)
                       |> Enum.map(&(@auto_partition_prefix <> Integer.to_string(&1)))

  def state_key(id, partition_key \\ nil)

  def state_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":s:" <> id
  end

  def state_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":s:" <> id
  end

  def registry_key(id, partition_key \\ nil)

  def registry_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":r:" <> id
  end

  def registry_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":r:" <> id
  end

  def state_key_from_due_key(due_key, id) when is_binary(due_key) and is_binary(id) do
    case :binary.match(due_key, "}:d:") do
      {pos, _len} when pos >= 2 ->
        tag = binary_part(due_key, 2, pos + 1 - 2)
        {:ok, "f:" <> tag <> ":s:" <> id}

      :nomatch ->
        :error
    end
  end

  def history_key(id, partition_key \\ nil)

  def history_key(id, nil) when is_binary(id) do
    "f:" <> tag(auto_partition_key(id)) <> ":h:" <> id
  end

  def history_key(id, partition_key) do
    "f:" <> tag(partition_key) <> ":h:" <> id
  end

  def value_key(id, kind, version, partition_key \\ nil)

  def value_key(id, kind, version, nil)
      when kind in [:payload, :result, :error, :shared] and is_integer(version) and
             is_binary(id) do
    value_key(id, kind, version, auto_partition_key(id))
  end

  def value_key(id, kind, version, partition_key)
      when kind in [:payload, :result, :error, :shared] and is_integer(version) do
    "f:" <>
      tag(partition_key) <>
      ":v:" <> flow_value_kind(kind) <> ":" <> id <> ":" <> Integer.to_string(version)
  end

  def shared_value_link_prefix(owner_flow_id, partition_key \\ nil)
      when is_binary(owner_flow_id) do
    "f:" <> tag(partition_key) <> ":svl:" <> owner_flow_id <> ":"
  end

  def signal_idempotency_key(id, idempotency_key, partition_key \\ nil)
      when is_binary(id) and is_binary(idempotency_key) do
    "f:" <> tag(partition_key) <> ":sig:" <> id <> ":" <> idempotency_key
  end

  def policy_key(type) do
    "f:" <> @global_tag <> ":policy:" <> type
  end

  def policy_type("f:" <> @global_tag <> ":policy:" <> type) when type != "", do: {:ok, type}
  def policy_type(_key), do: :error

  def governance_effect_key(id, effect_key, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:e:" <> id <> ":" <> effect_key
  end

  def governance_effect_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:e:" <> id <> ":"
  end

  def governance_ledger_key(id, event_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:l:" <> id <> ":" <> event_id
  end

  def governance_ledger_key_prefix(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:l:" <> id <> ":"
  end

  def governance_ledger_index_key(id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":gov:li:" <> id
  end

  def governance_scope_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:scope:" <> scope
  end

  def governance_approval_key(id) when is_binary(id) do
    "f:" <> tag(id) <> ":gov:a:" <> id
  end

  def governance_approval_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:a:") != :nomatch

  def governance_approval_key?(_key), do: false

  def governance_circuit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:c:" <> scope
  end

  def governance_circuit_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:c:") != :nomatch

  def governance_circuit_key?(_key), do: false

  def governance_budget_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:b:" <> scope
  end

  def governance_budget_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:b:") != :nomatch

  def governance_budget_key?(_key), do: false

  def governance_limit_key(scope) when is_binary(scope) do
    "f:" <> tag(scope) <> ":gov:limit:" <> scope
  end

  def governance_limit_key?(<<"f:{", rest::binary>>),
    do: :binary.match(rest, "}:gov:limit:") != :nomatch

  def governance_limit_key?(_key), do: false

  def policy_key?(key) when is_binary(key),
    do: String.starts_with?(key, "f:" <> @global_tag <> ":policy:")

  def policy_key?(_key), do: false

  def value_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:v:") != :nomatch
  def value_key?(_key), do: false

  def history_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:h:") != :nomatch
  def history_key?(_key), do: false

  def due_key(type, state, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":d:" <> type <> ":" <> state <> ":p" <> Integer.to_string(priority)
  end

  def due_any_key(type, priority, partition_key \\ nil) do
    "f:" <>
      tag(partition_key) <>
      ":da:" <> type <> ":p" <> Integer.to_string(priority)
  end

  def state_index_key(type, state, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:s:" <> type <> ":" <> state
  end

  def inflight_index_key(type, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:r:" <> type
  end

  def worker_index_key(worker, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:w:" <> worker
  end

  def parent_index_key(parent_flow_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:p:" <> parent_flow_id
  end

  def root_index_key(root_flow_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:o:" <> root_flow_id
  end

  def correlation_index_key(correlation_id, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:c:" <> correlation_id
  end

  def attribute_index_key(type, state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:a:" <> type <> ":" <> state <> ":" <> name <> "=" <> value
  end

  def attribute_index_prefix(type, state, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:a:" <> type <> ":" <> state <> ":" <> name <> "="
  end

  def attribute_type_index_key(type, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:at:" <> type <> ":" <> name <> "=" <> value
  end

  def attribute_type_index_prefix(type, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:at:" <> type <> ":" <> name <> "="
  end

  def attribute_state_index_key(state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:as:" <> state <> ":" <> name <> "=" <> value
  end

  def attribute_state_index_prefix(state, name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:as:" <> state <> ":" <> name <> "="
  end

  def attribute_partition_index_key(name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:ap:" <> name <> "=" <> value
  end

  def attribute_partition_index_prefix(name, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:ap:" <> name <> "="
  end

  def state_meta_index_key(type, state, name, value, partition_key \\ nil) do
    "f:" <> tag(partition_key) <> ":i:sm:" <> type <> ":" <> state <> ":" <> name <> "=" <> value
  end

  def stream_entry_key(id, event_id, partition_key \\ nil) do
    stream_entry_key_from_history_key(history_key(id, partition_key), event_id)
  end

  def stream_entry_key_from_history_key(history_key, event_id)
      when is_binary(history_key) and is_binary(event_id) do
    "X:" <> history_key <> <<0>> <> event_id
  end

  def state_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:s:") != :nomatch

  def state_key?(_key), do: false

  def registry_key?(<<"f:{", rest::binary>>), do: :binary.match(rest, "}:r:") != :nomatch

  def registry_key?(_key), do: false

  def tag(nil), do: @global_tag
  def tag(:global), do: @global_tag

  def tag(<<@auto_partition_prefix, bucket::binary>> = partition_key) do
    case auto_partition_bucket(bucket) do
      {:ok, bucket_index} ->
        elem(@auto_partition_tags, bucket_index)

      :error ->
        hashed_partition_tag(partition_key)
    end
  end

  def tag(partition_key) when is_binary(partition_key), do: hashed_partition_tag(partition_key)

  defp hashed_partition_tag(partition_key) do
    @partition_tag_prefix <>
      Base.url_encode64(:crypto.hash(:sha256, partition_key), padding: false) <> "}"
  end

  def auto_partition_key(id) when is_binary(id) do
    bucket =
      id
      |> :erlang.crc32()
      |> rem(@auto_partition_buckets)

    @auto_partition_prefix <> Integer.to_string(bucket)
  end

  def auto_partition_keys do
    @auto_partition_keys
  end

  def auto_partition_key?(<<@auto_partition_prefix, bucket::binary>>) do
    match?({:ok, _bucket_index}, auto_partition_bucket(bucket))
  end

  def auto_partition_key?(_partition_key), do: false

  defp auto_partition_bucket(bucket), do: auto_partition_bucket(bucket, 0, false)

  defp auto_partition_bucket(<<>>, value, true) when value < @auto_partition_buckets,
    do: {:ok, value}

  defp auto_partition_bucket(<<digit, rest::binary>>, value, _seen?)
       when digit >= ?0 and digit <= ?9 do
    next = value * 10 + (digit - ?0)

    if next < @auto_partition_buckets do
      auto_partition_bucket(rest, next, true)
    else
      :error
    end
  end

  defp auto_partition_bucket(_bucket, _value, _seen?), do: :error

  defp flow_value_kind(:payload), do: "p"
  defp flow_value_kind(:result), do: "r"
  defp flow_value_kind(:error), do: "e"
  defp flow_value_kind(:shared), do: "s"
end
