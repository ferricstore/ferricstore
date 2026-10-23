defmodule Ferricstore.Flow.RewindTarget do
  @moduledoc false

  # A rewind restores the fields of one of the Flow's history events. History is
  # per node: the history projector writes and trims it outside Raft, so whether
  # a node holds an event yet, or still, depends on how far its projector has
  # got, and apply must not look. From cluster feature level 5 the rewind reads
  # its target here, after projecting everything this node has applied, and
  # sends the stored entry through Raft. Apply decodes the entry against the
  # replicated record and refuses an event outside the record's history cap,
  # which every projector trims.

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow.{HistoryProjector, HistoryRead, Keys}
  alias Ferricstore.Store.{ReadResult, Router}

  @flush_timeout_ms 30_000
  @not_found {:error, "ERR flow rewind target event not found"}

  @doc """
  Adds the target event's stored entry to rewind `attrs` once feature level 5
  is active on the Flow's shard; below it, apply reads the event itself.
  """
  @spec attach(FerricStore.Instance.t(), map()) :: {:ok, map()} | {:error, binary()}
  def attach(ctx, %{id: id, to_event: event_id} = attrs) do
    partition_key = Map.get(attrs, :partition_key)
    shard_index = Router.shard_for(ctx, Keys.state_key(id, partition_key))

    if level_5?(ctx, shard_index) do
      with :ok <- flush_history(ctx, shard_index),
           {:ok, entry} <- read_entry(ctx, id, partition_key, event_id) do
        {:ok, Map.put(attrs, :target_entry, entry)}
      end
    else
      {:ok, attrs}
    end
  end

  @doc false
  # The stored entry of event `event_id` in this node's history of Flow `id`.
  @spec read_entry(FerricStore.Instance.t(), binary(), binary() | nil, binary()) ::
          {:ok, binary()} | {:error, binary()}
  def read_entry(ctx, id, partition_key, event_id) do
    history_key = Keys.history_key(id, partition_key)

    values =
      Router.compound_batch_get(ctx, history_key, [
        Keys.stream_entry_key(id, event_id, partition_key)
      ])

    with {:ok, hot} <- HistoryRead.hot_values_by_event([event_id], values),
         {:ok, cold} <- HistoryRead.cold_values_by_event(ctx, history_key, [event_id], hot) do
      case Map.get(hot, event_id) || Map.get(cold, event_id) do
        entry when is_binary(entry) -> {:ok, entry}
        _missing -> @not_found
      end
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp level_5?(ctx, shard_index),
    do:
      Router.read_shard_value(ctx, shard_index, FeatureLevel.key(5)) ==
        {:ok, FeatureLevel.marker()}

  defp flush_history(ctx, shard_index) do
    case HistoryProjector.flush(ctx, shard_index, @flush_timeout_ms) do
      :ok -> :ok
      {:error, :not_started} -> :ok
      {:error, _reason} -> {:error, "ERR flow history unavailable, retry the rewind"}
    end
  end
end
