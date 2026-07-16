defmodule Ferricstore.Flow.Governance.Ledger do
  @moduledoc false

  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.RetentionGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.TermCodec

  @default_max_events 1_000
  @max_index_bytes 900_000
  @max_exact_integer 9_007_199_254_740_991
  @event_kinds [
    :effect_reserved,
    :effect_confirmed,
    :effect_failed,
    :effect_compensated,
    :approval_required,
    :circuit_open,
    :effect_denied
  ]

  def append(ctx, record, kind, fields, now_ms) when is_map(record) and is_map(fields) do
    partition_key = Map.get(record, :partition_key)
    flow_id = Map.fetch!(record, :id)
    index_key = Keys.governance_ledger_index_key(flow_id, partition_key)
    retention_owner = retention_owner(record)

    with {:ok, event_id} <- resolve_event_id(fields, now_ms),
         event =
           fields
           |> Map.take([
             :effect_key,
             :effect_type,
             :status,
             :policy_hash,
             :policy_version,
             :code,
             :message,
             :policy
           ])
           |> Map.merge(%{
             id: event_id,
             flow_id: flow_id,
             partition_key: partition_key,
             kind: kind,
             at_ms: now_ms
           }),
         :ok <- validate_append_event(event),
         :ok <- run_before_append_hook(event) do
      AtomicRecord.mutate(
        ctx,
        index_key,
        &decode_index/1,
        &encode_index/1,
        fn -> {:ok, []} end,
        fn events ->
          case put_event(events, event, @default_max_events) do
            {:ok, updated} -> {:ok, updated, :ok}
            {:error, _reason} = error -> error
          end
        end,
        flow_retention_owner: retention_owner
      )
    end
  end

  def list(ctx, id, opts \\ [])

  def list(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, index_key} <- resolve_index_key(id, opts),
         {:ok, limit} <- optional_limit(opts),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, from_ms} <- optional_non_negative_integer(opts, :from_ms, 0),
         {:ok, to_ms} <- optional_non_negative_integer(opts, :to_ms, :infinity),
         {:ok, index_events} <- load_index(ctx, index_key) do
      events =
        index_events
        |> Enum.filter(&in_window?(&1, from_ms, to_ms))
        |> maybe_reverse(rev?)
        |> Enum.take(limit)

      {:ok, events}
    else
      false -> {:error, "ERR flow governance ledger opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def list(_ctx, _id, _opts),
    do: {:error, "ERR flow governance ledger opts must be a keyword list"}

  @doc false
  def resolve_index_key(id, opts) when is_binary(id) and is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- validate_required_binary(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         index_key =
           Keys.governance_ledger_index_key(id, partition_key || Keys.auto_partition_key(id)),
         :ok <- validate_key_size(index_key) do
      {:ok, index_key}
    else
      false -> {:error, "ERR flow governance ledger opts must be a keyword list"}
      {:error, _reason} = error -> error
    end
  end

  def resolve_index_key(_id, _opts),
    do: {:error, "ERR flow governance ledger opts must be a keyword list"}

  defp load_index(ctx, index_key) do
    case Router.get(ctx, index_key) do
      nil -> {:ok, []}
      value when is_binary(value) -> decode_index(value)
      _other -> {:error, "ERR flow governance ledger index is corrupt"}
    end
  end

  defp put_event(events, event, max_events) do
    case Enum.find(events, &(Map.get(&1, :id) == event.id)) do
      nil ->
        updated =
          events
          |> Kernel.++([event])
          |> Enum.sort_by(&Map.get(&1, :at_ms, 0))
          |> trim(max_events)
          |> trim_index_bytes()

        {:ok, updated}

      ^event ->
        {:ok, events}

      _conflicting ->
        {:error, "ERR flow governance ledger event id already exists"}
    end
  end

  defp trim(events, max_events) when length(events) > max_events,
    do: Enum.take(events, -max_events)

  defp trim(events, _max_events), do: events

  defp trim_index_bytes(events) do
    if index_value_size(events) <= @max_index_bytes do
      events
    else
      drop_oldest_until_fit(events, 1, length(events) - 1)
    end
  end

  defp drop_oldest_until_fit(events, low, high) when low >= high,
    do: Enum.drop(events, low)

  defp drop_oldest_until_fit(events, low, high) do
    midpoint = div(low + high, 2)

    if events |> Enum.drop(midpoint) |> index_value_size() <= @max_index_bytes do
      drop_oldest_until_fit(events, low, midpoint)
    else
      drop_oldest_until_fit(events, midpoint + 1, high)
    end
  end

  defp index_value_size(events), do: events |> encode_index() |> byte_size()

  defp retention_owner(record) do
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    %{
      id: id,
      partition_key: partition_key,
      state_key: Keys.state_key(id, partition_key),
      expected_guard: RetentionGuard.encode(record)
    }
  end

  defp in_window?(event, from_ms, to_ms) do
    at_ms = Map.get(event, :at_ms, 0)
    at_ms >= from_ms and (to_ms == :infinity or at_ms <= to_ms)
  end

  defp maybe_reverse(events, true), do: Enum.reverse(events)
  defp maybe_reverse(events, false), do: events

  defp encode_index(events),
    do: TermCodec.encode({:flow_governance_ledger_index_v1, events})

  defp validate_append_event(event) do
    cond do
      not valid_event?(event) ->
        {:error, "ERR flow governance ledger event is invalid"}

      index_value_size([event]) > @max_index_bytes ->
        {:error, "ERR flow governance ledger event exceeds the durable index byte budget"}

      true ->
        :ok
    end
  end

  defp decode_index(value) do
    case TermCodec.decode(value) do
      {:ok, {:flow_governance_ledger_index_v1, events}} when is_list(events) ->
        if valid_events?(events) do
          {:ok, events}
        else
          {:error, "ERR flow governance ledger index is corrupt"}
        end

      _other ->
        {:error, "ERR flow governance ledger index is corrupt"}
    end
  end

  defp valid_events?(events), do: valid_events?(events, -1, MapSet.new(), 0)

  defp valid_events?([], _previous_at_ms, _seen_ids, _count), do: true

  defp valid_events?([event | rest], previous_at_ms, seen_ids, count)
       when count < @default_max_events and is_map(event) do
    id = Map.get(event, :id)
    at_ms = Map.get(event, :at_ms)

    if valid_event?(event) and at_ms >= previous_at_ms and not MapSet.member?(seen_ids, id) do
      valid_events?(rest, at_ms, MapSet.put(seen_ids, id), count + 1)
    else
      false
    end
  end

  defp valid_events?(_events, _previous_at_ms, _seen_ids, _count), do: false

  defp valid_event?(event) do
    valid_required_binary(event, :id) and valid_required_binary(event, :flow_id) and
      valid_optional_binary(event, :partition_key) and Map.get(event, :kind) in @event_kinds and
      valid_timestamp?(Map.get(event, :at_ms)) and valid_optional_binary(event, :effect_key) and
      valid_optional_binary(event, :effect_type) and valid_optional_binary(event, :policy_hash) and
      valid_policy_version?(Map.get(event, :policy_version)) and
      valid_event_status?(Map.get(event, :kind), Map.get(event, :status)) and
      valid_optional_binary(event, :code) and valid_optional_binary(event, :message) and
      valid_optional_binary(event, :policy)
  end

  defp valid_event_status?(:effect_reserved, :reserved), do: true
  defp valid_event_status?(:effect_confirmed, :confirmed), do: true
  defp valid_event_status?(:effect_failed, :failed), do: true
  defp valid_event_status?(:effect_compensated, :compensated), do: true

  defp valid_event_status?(kind, :denied)
       when kind in [:approval_required, :circuit_open, :effect_denied],
       do: true

  defp valid_event_status?(_kind, _status), do: false

  defp valid_required_binary(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} when is_binary(value) ->
        byte_size(value) > 0 and byte_size(value) <= Router.max_key_size()

      _missing_or_invalid ->
        false
    end
  end

  defp valid_optional_binary(map, key) do
    case Map.fetch(map, key) do
      :error -> true
      {:ok, nil} -> true
      {:ok, value} when is_binary(value) -> byte_size(value) <= Router.max_key_size()
      _invalid -> false
    end
  end

  defp valid_policy_version?(nil), do: true

  defp valid_policy_version?(version) when is_binary(version),
    do: byte_size(version) > 0 and byte_size(version) <= Router.max_key_size()

  defp valid_policy_version?(version), do: valid_timestamp?(version)

  defp valid_timestamp?(value),
    do: is_integer(value) and value >= 0 and value <= @max_exact_integer

  defp event_id(now_ms) do
    Integer.to_string(now_ms) <>
      ":" <> Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
  end

  defp resolve_event_id(fields, now_ms) do
    case Map.get(fields, :event_id) do
      nil ->
        {:ok, event_id(now_ms)}

      value when is_binary(value) and value != "" ->
        if byte_size(value) <= Router.max_key_size(),
          do: {:ok, value},
          else: {:error, "ERR flow governance ledger event id is invalid"}

      _invalid ->
        {:error, "ERR flow governance ledger event id is invalid"}
    end
  end

  defp run_before_append_hook(event) do
    case Process.get(:ferricstore_governance_ledger_before_append_hook) do
      hook when is_function(hook, 1) -> hook.(event)
      _missing -> :ok
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      nil ->
        {:ok, nil}

      value when is_binary(value) ->
        if byte_size(value) <= Router.max_key_size(),
          do: {:ok, value},
          else: {:error, "ERR flow partition_key must be a string"}

      _other ->
        {:error, "ERR flow partition_key must be a string"}
    end
  end

  defp validate_required_binary(value) do
    if value != "" and byte_size(value) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR flow id must be a non-empty string"}
    end
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp optional_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, @default_max_events)}
      _other -> {:error, "ERR flow governance ledger limit must be a positive integer"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _other -> {:error, "ERR flow governance ledger #{key} must be boolean"}
    end
  end

  defp optional_non_negative_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      :infinity -> {:ok, :infinity}
      value when is_integer(value) and value >= 0 and value <= @max_exact_integer -> {:ok, value}
      _other -> {:error, "ERR flow governance ledger #{key} must be a non-negative integer"}
    end
  end
end
