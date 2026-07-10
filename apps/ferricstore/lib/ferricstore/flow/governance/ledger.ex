defmodule Ferricstore.Flow.Governance.Ledger do
  @moduledoc false

  alias Ferricstore.Flow.Governance.AtomicRecord
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.RetentionGuard
  alias Ferricstore.Store.Router

  @default_max_events 1_000

  def append(ctx, record, kind, fields, now_ms) when is_map(record) and is_map(fields) do
    partition_key = Map.get(record, :partition_key)
    flow_id = Map.fetch!(record, :id)
    event_id = event_id(now_ms)

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
      })

    event_key = Keys.governance_ledger_key(flow_id, event_id, partition_key)
    index_key = Keys.governance_ledger_index_key(flow_id, partition_key)
    retention_owner = retention_owner(record)

    event_opts = %{
      expire_at_ms: 0,
      nx: false,
      xx: false,
      get: false,
      keepttl: false,
      flow_retention_owner: retention_owner
    }

    with :ok <- Router.set(ctx, event_key, encode_event(event), event_opts) do
      AtomicRecord.mutate(
        ctx,
        index_key,
        &decode_index/1,
        &encode_index/1,
        fn -> {:ok, []} end,
        fn events -> {:ok, put_event(events, event, @default_max_events), :ok} end,
        flow_retention_owner: retention_owner
      )
    end
  end

  def list(ctx, id, opts \\ [])

  def list(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, limit} <- optional_limit(opts),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, from_ms} <- optional_non_negative_integer(opts, :from_ms, 0),
         {:ok, to_ms} <- optional_non_negative_integer(opts, :to_ms, :infinity),
         index_key = Keys.governance_ledger_index_key(id, partition_key),
         {:ok, index_events} <- load_index(ctx, index_key) do
      events =
        index_events
        |> Enum.filter(&in_window?(&1, from_ms, to_ms))
        |> maybe_reverse(rev?)
        |> Enum.take(limit)

      {:ok, events}
    end
  end

  def list(_ctx, _id, _opts),
    do: {:error, "ERR flow governance ledger opts must be a keyword list"}

  defp load_index(ctx, index_key) do
    case Router.get(ctx, index_key) do
      nil -> {:ok, []}
      value when is_binary(value) -> decode_index(value)
      _other -> {:error, "ERR flow governance ledger index is corrupt"}
    end
  end

  defp put_event(events, event, max_events) do
    events
    |> Enum.reject(&(Map.get(&1, :id) == event.id))
    |> Kernel.++([event])
    |> Enum.sort_by(&Map.get(&1, :at_ms, 0))
    |> trim(max_events)
  end

  defp trim(events, max_events) when length(events) > max_events,
    do: Enum.take(events, -max_events)

  defp trim(events, _max_events), do: events

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

  defp encode_event(event), do: :erlang.term_to_binary({:flow_governance_ledger_v1, event})

  defp encode_index(events),
    do: :erlang.term_to_binary({:flow_governance_ledger_index_v1, events})

  defp decode_index(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {:flow_governance_ledger_index_v1, events} when is_list(events) -> {:ok, events}
      _other -> {:error, "ERR flow governance ledger index is corrupt"}
    end
  rescue
    _ -> {:error, "ERR flow governance ledger index is corrupt"}
  end

  defp event_id(now_ms) do
    Integer.to_string(now_ms) <>
      ":" <> Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _other -> {:error, "ERR flow partition_key must be a string"}
    end
  end

  defp optional_limit(opts) do
    case Keyword.get(opts, :limit, 100) do
      value when is_integer(value) and value > 0 -> {:ok, value}
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
      value when is_integer(value) and value >= 0 -> {:ok, value}
      _other -> {:error, "ERR flow governance ledger #{key} must be a non-negative integer"}
    end
  end
end
