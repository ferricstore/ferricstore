defmodule Ferricstore.Commands.Flow do
  @moduledoc """
  Command dispatch boundary for FerricFlow commands.

  The native AST parser builds Flow tuples and this module forwards them to
  the public `FerricStore.flow_*` APIs, normalizing replies into protocol-friendly
  values. It intentionally stays thin so command parsing, API validation, and
  Raft apply logic remain separate.

  ## Performance boundary

  Flow write commands can be batched by the connection pipeline before reaching
  here. Keep this dispatcher mechanical: no behaviours/protocols, no per-command
  dynamic lookup tables in hot writes, and no response hydration beyond API
  contracts.
  """

  @spec handle_ast(term(), map()) :: term()
  def handle_ast({tag, {:error, msg}}, _store) when is_atom(tag) and is_binary(msg),
    do: {:error, msg}

  def handle_ast({tag, _arg, {:error, msg}}, _store) when is_atom(tag) and is_binary(msg),
    do: {:error, msg}

  def handle_ast({tag, _arg1, _arg2, {:error, msg}}, _store)
      when is_atom(tag) and is_binary(msg),
      do: {:error, msg}

  def handle_ast({tag, _arg1, _arg2, _arg3, {:error, msg}}, _store)
      when is_atom(tag) and is_binary(msg),
      do: {:error, msg}

  def handle_ast({:flow_create, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_create(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_value_put, value, opts}, _store) when is_list(opts) do
    FerricStore.flow_value_put(value, opts) |> normalize_result()
  end

  def handle_ast({:flow_signal, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_signal(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_create_many, partition_key, items, opts}, _store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    FerricStore.flow_create_many(partition_key, items, opts) |> normalize_result()
  end

  def handle_ast({:flow_spawn_children, parent_id, children, opts}, _store)
      when is_binary(parent_id) and is_list(children) and is_list(opts) do
    FerricStore.flow_spawn_children(parent_id, children, opts) |> normalize_result()
  end

  def handle_ast({:flow_get, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_get(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_policy_set, type, opts}, _store)
      when is_binary(type) and is_list(opts) do
    FerricStore.flow_policy_set(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_policy_get, type, opts}, _store)
      when is_binary(type) and is_list(opts) do
    FerricStore.flow_policy_get(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_claim_due, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_claim_due(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_reclaim, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_reclaim(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_extend_lease, id, lease_token, opts}, _store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    FerricStore.flow_extend_lease(id, lease_token, opts) |> normalize_result()
  end

  def handle_ast({:flow_complete, id, lease_token, opts}, _store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    FerricStore.flow_complete(id, lease_token, opts) |> normalize_result()
  end

  def handle_ast({:flow_complete_many, partition_key, items, opts}, _store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    FerricStore.flow_complete_many(partition_key, items, opts) |> normalize_result()
  end

  def handle_ast({:flow_fail_many, partition_key, items, opts}, _store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    FerricStore.flow_fail_many(partition_key, items, opts) |> normalize_result()
  end

  def handle_ast({:flow_cancel_many, partition_key, items, opts}, _store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    FerricStore.flow_cancel_many(partition_key, items, opts) |> normalize_result()
  end

  def handle_ast({:flow_transition, id, from_state, to_state, opts}, _store)
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    FerricStore.flow_transition(id, from_state, to_state, opts) |> normalize_result()
  end

  def handle_ast(
        {:flow_transition_many, partition_key, from_state, to_state, items, opts},
        _store
      )
      when (is_binary(partition_key) or is_nil(partition_key)) and is_binary(from_state) and
             is_binary(to_state) and is_list(items) and is_list(opts) do
    FerricStore.flow_transition_many(partition_key, from_state, to_state, items, opts)
    |> normalize_result()
  end

  def handle_ast({:flow_retry, id, lease_token, opts}, _store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    FerricStore.flow_retry(id, lease_token, opts) |> normalize_result()
  end

  def handle_ast({:flow_retry_many, partition_key, items, opts}, _store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    FerricStore.flow_retry_many(partition_key, items, opts) |> normalize_result()
  end

  def handle_ast({:flow_fail, id, lease_token, opts}, _store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    FerricStore.flow_fail(id, lease_token, opts) |> normalize_result()
  end

  def handle_ast({:flow_cancel, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_cancel(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_rewind, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_rewind(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_list, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_list(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_search, opts}, _store) when is_list(opts) do
    FerricStore.flow_search(opts) |> normalize_result()
  end

  def handle_ast({:flow_attributes, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_attributes(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_attribute_values, type, attr_name, opts}, _store)
      when is_binary(type) and is_binary(attr_name) and is_list(opts) do
    FerricStore.flow_attribute_values(type, attr_name, opts) |> normalize_result()
  end

  def handle_ast({:flow_stats, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_stats(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_terminals, type, opts}, _store)
      when is_binary(type) and is_list(opts) do
    FerricStore.flow_terminals(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_failures, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_failures(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_by_parent, parent_flow_id, opts}, _store)
      when is_binary(parent_flow_id) and is_list(opts) do
    FerricStore.flow_by_parent(parent_flow_id, opts) |> normalize_result()
  end

  def handle_ast({:flow_by_root, root_flow_id, opts}, _store)
      when is_binary(root_flow_id) and is_list(opts) do
    FerricStore.flow_by_root(root_flow_id, opts) |> normalize_result()
  end

  def handle_ast({:flow_by_correlation, correlation_id, opts}, _store)
      when is_binary(correlation_id) and is_list(opts) do
    FerricStore.flow_by_correlation(correlation_id, opts) |> normalize_result()
  end

  def handle_ast({:flow_info, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_info(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_stuck, type, opts}, _store) when is_binary(type) and is_list(opts) do
    FerricStore.flow_stuck(type, opts) |> normalize_result()
  end

  def handle_ast({:flow_history, id, opts}, _store) when is_binary(id) and is_list(opts) do
    FerricStore.flow_history(id, opts) |> normalize_result()
  end

  def handle_ast({:flow_retention_cleanup, opts}, _store) when is_list(opts) do
    FerricStore.flow_retention_cleanup(opts) |> normalize_result()
  end

  def handle_ast({tag, _args}, _store) when is_atom(tag),
    do: {:error, "ERR wrong number of arguments for '#{command_name(tag)}' command"}

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported flow command AST"}

  @doc false
  def normalize_result(:ok), do: "OK"

  def normalize_result({:ok, records}) when is_list(records),
    do: Enum.map(records, &normalize_value/1)

  def normalize_result({:ok, value}), do: normalize_value(value)
  def normalize_result({:error, _} = error), do: error

  defp normalize_value(nil), do: nil
  defp normalize_value(:ok), do: "OK"

  defp normalize_value({event_id, fields}) when is_binary(event_id),
    do: [event_id, normalize_map(fields)]

  defp normalize_value(value) when is_map(value), do: normalize_map(value)
  defp normalize_value(value), do: value

  defp normalize_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), normalize_value(value)}
      {key, value} -> {key, normalize_value(value)}
    end)
  end

  defp command_name(tag) do
    tag
    |> Atom.to_string()
    |> String.replace("_", ".")
  end
end
