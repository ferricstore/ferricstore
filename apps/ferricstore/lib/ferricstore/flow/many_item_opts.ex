defmodule Ferricstore.Flow.ManyItemOpts do
  @moduledoc false

  def create(id) when is_binary(id), do: {:ok, id, []}

  def create(%{id: id} = item) when is_binary(id), do: {:ok, id, create_opts_from_map(item)}
  def create(%{"id" => id} = item) when is_binary(id), do: {:ok, id, create_opts_from_map(item)}

  def create({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    keyword_result(id, item_opts)
  end

  def create({:id, id, :payload_ref, payload_ref}) when is_binary(id) do
    {:ok, id, [payload_ref: payload_ref]}
  end

  def create({:id, id, :payload, payload}) when is_binary(id) do
    {:ok, id, [payload: payload]}
  end

  def create({:id, id, :partition_key, partition_key, :payload, payload})
      when is_binary(id) do
    {:ok, id, [partition_key: partition_key, payload: payload]}
  end

  def create({:id, id, :partition_key, partition_key, :payload_ref, payload_ref})
      when is_binary(id) do
    {:ok, id, [partition_key: partition_key, payload_ref: payload_ref]}
  end

  def create(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def complete(%{id: id, lease_token: lease_token, fencing_token: fencing_token} = item)
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       complete_result_ref(item) ++
       complete_result(item) ++ complete_payload(item) ++ partition_key(item)}
  end

  def complete(
        %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item
      )
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       complete_result_ref(item) ++
       complete_result(item) ++ complete_payload(item) ++ partition_key(item)}
  end

  def complete({id, lease_token, item_opts})
      when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    lease_keyword_result(id, lease_token, item_opts)
  end

  def complete({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    item_keyword_lease_result(id, item_opts)
  end

  def complete({:id, id, :lease_token, lease_token, :fencing_token, fencing_token})
      when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  def complete(
        {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
         fencing_token}
      )
      when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  def complete(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def retry(%{id: id, lease_token: lease_token, fencing_token: fencing_token} = item)
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_error_ref(item) ++
       retry_error(item) ++ retry_payload(item) ++ retry_policy(item) ++ partition_key(item)}
  end

  def retry(%{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item)
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_error_ref(item) ++
       retry_error(item) ++ retry_payload(item) ++ retry_policy(item) ++ partition_key(item)}
  end

  def retry({id, lease_token, item_opts})
      when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    lease_keyword_result(id, lease_token, item_opts)
  end

  def retry({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    item_keyword_lease_result(id, item_opts)
  end

  def retry({:id, id, :lease_token, lease_token, :fencing_token, fencing_token})
      when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  def retry(
        {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
         fencing_token}
      )
      when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  def retry(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def fail(%{id: id, lease_token: lease_token, fencing_token: fencing_token} = item)
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_error_ref(item) ++ retry_error(item) ++ retry_payload(item) ++ partition_key(item)}
  end

  def fail(%{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token} = item)
      when is_binary(id) do
    {:ok, id, lease_token,
     [fencing_token: fencing_token] ++
       retry_error_ref(item) ++ retry_error(item) ++ retry_payload(item) ++ partition_key(item)}
  end

  def fail({id, lease_token, item_opts})
      when is_binary(id) and is_binary(lease_token) and is_list(item_opts) do
    lease_keyword_result(id, lease_token, item_opts)
  end

  def fail({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    item_keyword_lease_result(id, item_opts)
  end

  def fail({:id, id, :lease_token, lease_token, :fencing_token, fencing_token})
      when is_binary(id) do
    {:ok, id, lease_token, [fencing_token: fencing_token]}
  end

  def fail(
        {:id, id, :partition_key, partition_key, :lease_token, lease_token, :fencing_token,
         fencing_token}
      )
      when is_binary(id) do
    {:ok, id, lease_token, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  def fail(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def cancel(%{id: id, fencing_token: fencing_token} = item) when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       lease_token(item) ++ cancel_reason_ref(item) ++ cancel_reason(item) ++ partition_key(item)}
  end

  def cancel(%{"id" => id, "fencing_token" => fencing_token} = item) when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       lease_token(item) ++ cancel_reason_ref(item) ++ cancel_reason(item) ++ partition_key(item)}
  end

  def cancel({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    keyword_result(id, item_opts)
  end

  def cancel({:id, id, :fencing_token, fencing_token}) when is_binary(id) do
    {:ok, id, [fencing_token: fencing_token]}
  end

  def cancel({:id, id, :partition_key, partition_key, :fencing_token, fencing_token})
      when is_binary(id) do
    {:ok, id, [partition_key: partition_key, fencing_token: fencing_token]}
  end

  def cancel(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def transition(%{id: id, fencing_token: fencing_token} = item) when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       transition_payload(item) ++ lease_token(item) ++ partition_key(item)}
  end

  def transition(%{"id" => id, "fencing_token" => fencing_token} = item) when is_binary(id) do
    {:ok, id,
     [fencing_token: fencing_token] ++
       transition_payload(item) ++ lease_token(item) ++ partition_key(item)}
  end

  def transition({id, item_opts}) when is_binary(id) and is_list(item_opts) do
    keyword_result(id, item_opts)
  end

  def transition({:id, id, :fencing_token, fencing_token, :lease_token, lease_token})
      when is_binary(id) do
    opts =
      if is_nil(lease_token),
        do: [fencing_token: fencing_token],
        else: [fencing_token: fencing_token, lease_token: lease_token]

    {:ok, id, opts}
  end

  def transition(
        {:id, id, :partition_key, partition_key, :fencing_token, fencing_token, :lease_token,
         lease_token}
      )
      when is_binary(id) do
    opts =
      if is_nil(lease_token),
        do: [partition_key: partition_key, fencing_token: fencing_token],
        else: [
          partition_key: partition_key,
          fencing_token: fencing_token,
          lease_token: lease_token
        ]

    {:ok, id, opts}
  end

  def transition(_item), do: {:error, "ERR flow id must be a non-empty string"}

  def merge(base_opts, [], partition_key),
    do: Keyword.put(base_opts, :partition_key, partition_key)

  def merge(base_opts, item_opts, partition_key) do
    base_opts
    |> Keyword.merge(Keyword.delete(item_opts, :partition_key))
    |> Keyword.put(:partition_key, partition_key)
  end

  defp create_opts_from_map(item) do
    []
    |> maybe_put(:type, item, :type, "type")
    |> maybe_put(:state, item, :state, "state")
    |> maybe_put(:run_at_ms, item, :run_at_ms, "run_at_ms")
    |> maybe_put(:priority, item, :priority, "priority")
    |> maybe_put(:payload, item, :payload, "payload")
    |> maybe_put(:payload_ref, item, :payload_ref, "payload_ref")
    |> maybe_put(:values, item, :values, "values")
    |> maybe_put(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put(:partition_key, item, :partition_key, "partition_key")
    |> maybe_put(:parent_flow_id, item, :parent_flow_id, "parent_flow_id")
    |> maybe_put(:root_flow_id, item, :root_flow_id, "root_flow_id")
    |> maybe_put(:correlation_id, item, :correlation_id, "correlation_id")
    |> maybe_put(:idempotent, item, :idempotent, "idempotent")
    |> maybe_put(:retention_ttl_ms, item, :retention_ttl_ms, "retention_ttl_ms")
    |> maybe_put(:history_hot_max_events, item, :history_hot_max_events, "history_hot_max_events")
    |> maybe_put(:history_max_events, item, :history_max_events, "history_max_events")
  end

  defp keyword_result(id, item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp lease_keyword_result(id, lease_token, item_opts) do
    if Keyword.keyword?(item_opts) do
      {:ok, id, lease_token, item_opts}
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp item_keyword_lease_result(id, item_opts) do
    cond do
      not Keyword.keyword?(item_opts) ->
        {:error, "ERR flow opts must be a keyword list"}

      not is_binary(Keyword.get(item_opts, :lease_token)) ->
        {:error, "ERR flow lease_token must be a non-empty string"}

      true ->
        {:ok, id, Keyword.fetch!(item_opts, :lease_token), item_opts}
    end
  end

  defp lease_token(item) do
    cond do
      Map.has_key?(item, :lease_token) -> [lease_token: Map.get(item, :lease_token)]
      Map.has_key?(item, "lease_token") -> [lease_token: Map.get(item, "lease_token")]
      true -> []
    end
  end

  defp partition_key(item),
    do: maybe_put([], :partition_key, item, :partition_key, "partition_key")

  defp transition_payload(item) do
    []
    |> maybe_put(:payload, item, :payload, "payload")
    |> maybe_put(:payload_ref, item, :payload_ref, "payload_ref")
    |> maybe_put(:values, item, :values, "values")
    |> maybe_put(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put(:override_values, item, :override_values, "override_values")
  end

  defp complete_result_ref(item), do: maybe_put([], :result_ref, item, :result_ref, "result_ref")
  defp complete_result(item), do: maybe_put([], :result, item, :result, "result")
  defp complete_payload(item), do: transition_payload(item)

  defp retry_error_ref(item), do: maybe_put([], :error_ref, item, :error_ref, "error_ref")
  defp retry_error(item), do: maybe_put([], :error, item, :error, "error")
  defp retry_payload(item), do: transition_payload(item)
  defp retry_policy(item), do: maybe_put([], :retry, item, :retry, "retry")

  defp cancel_reason_ref(item), do: maybe_put([], :reason_ref, item, :reason_ref, "reason_ref")

  defp cancel_reason(item) do
    []
    |> maybe_put(:reason, item, :reason, "reason")
    |> maybe_put(:values, item, :values, "values")
    |> maybe_put(:value_refs, item, :value_refs, "value_refs")
    |> maybe_put(:drop_values, item, :drop_values, "drop_values")
    |> maybe_put(:override_values, item, :override_values, "override_values")
  end

  defp maybe_put(opts, opt_key, item, atom_key, string_key) do
    cond do
      Map.has_key?(item, atom_key) -> Keyword.put(opts, opt_key, Map.get(item, atom_key))
      Map.has_key?(item, string_key) -> Keyword.put(opts, opt_key, Map.get(item, string_key))
      true -> opts
    end
  end
end
