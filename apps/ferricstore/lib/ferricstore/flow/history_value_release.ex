defmodule Ferricstore.Flow.HistoryValueRelease do
  @moduledoc false

  # A running Flow writes its payload, result and error under a new version each
  # time they change, with a retention cleanup member (a hot keydir row) for each
  # value. Members used to be read only when a finished Flow was pruned, so a Flow
  # that never finishes, such as a schedule, gained a value and a member with
  # every change. History trimming drops the events that referred to old values;
  # a release then deletes the values that no remaining event and not the record
  # refers to, together with their members.
  #
  # Remaining history is per node: the history projector trims it outside Raft.
  # So the release reads it here and sends what it found through Raft: the
  # version of the oldest remaining event and every ref the remaining events
  # hold. Apply deletes only values written before that version, so a value an
  # event this node has not projected yet may hold always stays. It needs
  # cluster feature level 2.

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Flow
  alias Ferricstore.Flow.{HistoryProjector, Internal, Keys}
  alias Ferricstore.Flow.HistoryValueRelease.Worker
  alias Ferricstore.Store.Router

  # A Flow keeping more history keeps its values: reading that much history for
  # each release would cost more than the values it frees.
  @max_history_events 1_000
  @default_page 512
  @max_pages 10_000
  @flush_timeout_ms 30_000

  @doc """
  Releases the values of Flow `id` that its remaining history no longer refers to.
  Returns `{:ok, %{released: count}}`, or `{:error, :feature_level_inactive}`
  before feature level 2 is active on the Flow's shard.
  """
  @spec release(FerricStore.Instance.t(), binary(), binary() | nil, keyword()) ::
          {:ok, %{released: non_neg_integer()}} | {:error, term()}
  def release(ctx, id, partition_key, opts \\ []) when is_binary(id) and is_list(opts) do
    shard_index = Router.shard_for(ctx, Keys.state_key(id, partition_key))

    with :ok <- require_feature_level(ctx, shard_index),
         :ok <- flush_history(ctx, shard_index) do
      release_flushed(ctx, id, partition_key, opts)
    end
  end

  @doc """
  Called from apply after a Flow's history put. Every quarter of the history
  window past the first trim, asks this node's worker to release the Flow's
  values. Only a hint: it changes no state, and a dropped one waits for the next.
  """
  @spec notify(map(), non_neg_integer(), map()) :: :ok
  def notify(
        %{name: instance_name},
        shard_index,
        %{id: id, version: version, history_max_events: max_events} = record
      )
      when is_atom(instance_name) and is_integer(shard_index) and is_binary(id) and
             is_integer(version) and is_integer(max_events) and max_events > 0 and
             max_events <= @max_history_events and version > max_events do
    if rem(version, max(1, div(max_events, 4))) == 0 do
      case Process.whereis(Worker.name(instance_name)) do
        pid when is_pid(pid) ->
          GenServer.cast(pid, {:trimmed, shard_index, id, Map.get(record, :partition_key)})

        nil ->
          :ok
      end
    end

    :ok
  end

  def notify(_instance_ctx, _shard_index, _record), do: :ok

  @doc false
  # The worker's batch: one level check and one history flush for the shard.
  @spec release_shard(FerricStore.Instance.t(), non_neg_integer(), [{binary(), binary() | nil}]) ::
          :ok | {:error, term()}
  def release_shard(ctx, shard_index, flows) do
    with :ok <- require_feature_level(ctx, shard_index),
         :ok <- flush_history(ctx, shard_index) do
      Enum.each(flows, fn {id, partition_key} -> release_flushed(ctx, id, partition_key, []) end)
    end
  end

  defp release_flushed(ctx, id, partition_key, opts) do
    with {:ok, oldest_version, keep_refs} <- remaining_history(ctx, id, partition_key) do
      release_pages(
        ctx,
        %{
          id: id,
          partition_key: partition_key,
          oldest_version: oldest_version,
          keep_refs: keep_refs,
          offset: 0,
          limit: Keyword.get(opts, :page, @default_page)
        },
        0,
        @max_pages
      )
    else
      :nothing_trimmed -> {:ok, %{released: 0}}
      {:error, _reason} = error -> error
    end
  end

  defp require_feature_level(ctx, shard_index) do
    if Router.read_shard_value(ctx, shard_index, FeatureLevel.key(2)) ==
         {:ok, FeatureLevel.marker()},
       do: :ok,
       else: {:error, :feature_level_inactive}
  end

  # Projects and trims every history entry this node has applied, so the history
  # read below is at least as new as the record the command will see.
  defp flush_history(ctx, shard_index) do
    case HistoryProjector.flush(ctx, shard_index, @flush_timeout_ms) do
      :ok -> :ok
      {:error, :not_started} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp remaining_history(ctx, id, partition_key) do
    opts = Internal.put(partition_key: partition_key, count: @max_history_events + 1)

    case Flow.history(ctx, id, opts) do
      {:ok, events} when length(events) > @max_history_events ->
        :nothing_trimmed

      {:ok, [_ | _] = events} ->
        case oldest_version(events) do
          version when is_integer(version) and version > 1 ->
            {:ok, version, events |> Enum.flat_map(&event_refs/1) |> Enum.uniq()}

          _untrimmed_or_unreadable ->
            :nothing_trimmed
        end

      {:ok, []} ->
        :nothing_trimmed

      {:error, _reason} = error ->
        error
    end
  end

  # nil when any event's version is unreadable: then nothing is released.
  defp oldest_version(events) do
    Enum.reduce_while(events, nil, fn {_event_id, fields}, oldest ->
      case Integer.parse(to_string(Map.get(fields, "version"))) do
        {version, ""} when version > 0 -> {:cont, min(oldest || version, version)}
        _unreadable -> {:halt, nil}
      end
    end)
  end

  defp event_refs({_event_id, fields}) do
    for field <- ["payload_ref", "result_ref", "error_ref"],
        ref = Map.get(fields, field),
        is_binary(ref) and ref != "",
        do: ref
  end

  defp release_pages(_ctx, _attrs, released, 0), do: {:ok, %{released: released}}

  defp release_pages(ctx, attrs, released, pages_left) do
    case Router.flow_history_release_values(ctx, attrs) do
      {:ok, %{released: count, complete: true}} ->
        {:ok, %{released: released + count}}

      {:ok, %{released: count, next_offset: next_offset}} ->
        release_pages(ctx, %{attrs | offset: next_offset}, released + count, pages_left - 1)

      {:error, _reason} = error ->
        error
    end
  end
end
