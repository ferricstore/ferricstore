defmodule Ferricstore.Commands.Flow do
  @moduledoc false

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

  def handle_ast({:flow_create, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.create(&1, id, opts))
  end

  def handle_ast({:flow_value_put, value, opts}, store) when is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.value_put(&1, value, opts))
  end

  def handle_ast({:flow_value_mget, refs}, store) when is_list(refs) do
    with_flow_ctx(store, &Ferricstore.Flow.value_mget(&1, refs))
  end

  def handle_ast({:flow_value_mget, refs, opts}, store) when is_list(refs) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.value_mget(&1, refs, opts))
  end

  def handle_ast({:flow_signal, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.signal(&1, id, opts))
  end

  def handle_ast({:flow_create_many, partition_key, items, opts}, store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.create_many(&1, partition_key, items, opts))
  end

  def handle_ast({:flow_spawn_children, parent_id, children, opts}, store)
      when is_binary(parent_id) and is_list(children) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.spawn_children(&1, parent_id, children, opts))
  end

  def handle_ast({:flow_get, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.get(&1, id, opts))
  end

  def handle_ast({:flow_policy_set, type, opts}, store)
      when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.policy_set(&1, type, opts))
  end

  def handle_ast({:flow_policy_get, type, opts}, store)
      when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.policy_get(&1, type, opts))
  end

  def handle_ast({:flow_claim_due, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.claim_due(&1, type, opts))
  end

  def handle_ast({:flow_reclaim, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.reclaim(&1, type, opts))
  end

  def handle_ast({:flow_extend_lease, id, lease_token, opts}, store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.extend_lease(&1, id, lease_token, opts))
  end

  def handle_ast({:flow_complete, id, lease_token, opts}, store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.complete(&1, id, lease_token, opts))
  end

  def handle_ast({:flow_complete_many, partition_key, items, opts}, store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.complete_many(&1, partition_key, items, opts))
  end

  def handle_ast({:flow_fail_many, partition_key, items, opts}, store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.fail_many(&1, partition_key, items, opts))
  end

  def handle_ast({:flow_cancel_many, partition_key, items, opts}, store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.cancel_many(&1, partition_key, items, opts))
  end

  def handle_ast({:flow_transition, id, from_state, to_state, opts}, store)
      when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.transition(&1, id, from_state, to_state, opts))
  end

  def handle_ast(
        {:flow_transition_many, partition_key, from_state, to_state, items, opts},
        store
      )
      when (is_binary(partition_key) or is_nil(partition_key)) and is_binary(from_state) and
             is_binary(to_state) and is_list(items) and is_list(opts) do
    with_flow_ctx(
      store,
      &Ferricstore.Flow.transition_many(&1, partition_key, from_state, to_state, items, opts)
    )
  end

  def handle_ast({:flow_retry, id, lease_token, opts}, store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.retry(&1, id, lease_token, opts))
  end

  def handle_ast({:flow_retry_many, partition_key, items, opts}, store)
      when (is_binary(partition_key) or is_nil(partition_key)) and is_list(items) and
             is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.retry_many(&1, partition_key, items, opts))
  end

  def handle_ast({:flow_fail, id, lease_token, opts}, store)
      when is_binary(id) and is_binary(lease_token) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.fail(&1, id, lease_token, opts))
  end

  def handle_ast({:flow_cancel, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.cancel(&1, id, opts))
  end

  def handle_ast({:flow_rewind, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.rewind(&1, id, opts))
  end

  def handle_ast({:flow_list, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.list(&1, type, opts))
  end

  def handle_ast({:flow_terminals, type, opts}, store)
      when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.terminals(&1, type, opts))
  end

  def handle_ast({:flow_failures, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.failures(&1, type, opts))
  end

  def handle_ast({:flow_by_parent, parent_flow_id, opts}, store)
      when is_binary(parent_flow_id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.by_parent(&1, parent_flow_id, opts))
  end

  def handle_ast({:flow_by_root, root_flow_id, opts}, store)
      when is_binary(root_flow_id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.by_root(&1, root_flow_id, opts))
  end

  def handle_ast({:flow_by_correlation, correlation_id, opts}, store)
      when is_binary(correlation_id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.by_correlation(&1, correlation_id, opts))
  end

  def handle_ast({:flow_info, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.info(&1, type, opts))
  end

  def handle_ast({:flow_stuck, type, opts}, store) when is_binary(type) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.stuck(&1, type, opts))
  end

  def handle_ast({:flow_history, id, opts}, store) when is_binary(id) and is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.history(&1, id, opts))
  end

  def handle_ast({:flow_retention_cleanup, opts}, store) when is_list(opts) do
    with_flow_ctx(store, &Ferricstore.Flow.retention_cleanup(&1, opts))
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

  defp with_flow_ctx(store, fun) when is_function(fun, 1) do
    case flow_ctx(store) do
      {:ok, ctx} -> fun.(ctx) |> normalize_result()
      {:error, _reason} = error -> error
    end
  end

  defp flow_ctx(%FerricStore.Instance{} = ctx), do: {:ok, ctx}

  defp flow_ctx(%{__sandbox_namespace__: namespace}) when is_binary(namespace),
    do: {:error, "ERR FLOW commands are not supported in sandbox mode"}

  defp flow_ctx(%{__instance_ctx__: %FerricStore.Instance{} = ctx}), do: {:ok, ctx}

  defp flow_ctx(%{__flow_default_instance__: true}),
    do: {:ok, FerricStore.Instance.get(:default)}

  defp flow_ctx(_store), do: {:error, "ERR FLOW commands require a FerricStore instance"}

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
