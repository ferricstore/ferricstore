defmodule Ferricstore.Flow.Query.StatisticsStore do
  @moduledoc false

  use GenServer

  alias Ferricstore.Flow.Query.{IndexDefinition, Limits}
  alias Ferricstore.Flow.Query.IndexStatistics

  @default_max_entries 4_096
  @maximum_entries 65_536
  @maximum_summary_indexes 32
  @maximum_scope_bytes Limits.max_partition_key_bytes()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    name = Keyword.get(opts, :name, server_name(ctx))
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec server_name(map() | atom()) :: atom()
  def server_name(%{name: name}), do: server_name(name)
  def server_name(:default), do: __MODULE__
  def server_name(name) when is_atom(name), do: :"#{name}.Flow.Query.StatisticsStore"

  @spec table_name(map() | atom()) :: atom()
  def table_name(%{name: name}), do: table_name(name)
  def table_name(:default), do: Ferricstore.Flow.Query.StatisticsStore.Cache
  def table_name(name) when is_atom(name), do: :"#{name}.Flow.Query.StatisticsStore.Cache"

  @spec put(GenServer.server(), IndexStatistics.t()) :: :ok | {:error, atom()}
  def put(server, %IndexStatistics{} = stat), do: GenServer.call(server, {:put, stat})
  def put(_server, _stat), do: {:error, :invalid_query_index_statistics}

  @spec lookup(map(), binary(), pos_integer(), binary()) ::
          {:ok, IndexStatistics.t()} | :not_found
  def lookup(ctx, index_id, index_version, scope)
      when is_binary(scope) and scope != "" and byte_size(scope) <= @maximum_scope_bytes do
    lookup_digest(ctx, index_id, index_version, IndexStatistics.scope_digest(scope))
  end

  def lookup(_ctx, _index_id, _index_version, _scope), do: :not_found

  @doc false
  @spec lookup_digest(map(), binary(), pos_integer(), <<_::256>>) ::
          {:ok, IndexStatistics.t()} | :not_found
  def lookup_digest(ctx, index_id, index_version, scope_digest)
      when is_binary(index_id) and is_integer(index_version) and index_version > 0 and
             is_binary(scope_digest) and byte_size(scope_digest) == 32 do
    key = {scope_digest, index_id, index_version}

    case :ets.lookup(table_name(ctx), key) do
      [{^key, %IndexStatistics{} = stat, _sequence}] -> {:ok, stat}
      _missing -> :not_found
    end
  rescue
    ArgumentError -> :not_found
  end

  def lookup_digest(_ctx, _index_id, _index_version, _scope_digest), do: :not_found

  @spec size(map()) :: non_neg_integer()
  def size(ctx) do
    case :ets.info(table_name(ctx), :size) do
      size when is_integer(size) and size >= 0 -> size
      _missing -> 0
    end
  rescue
    ArgumentError -> 0
  end

  @doc false
  @spec summaries(map(), MapSet.t(), non_neg_integer()) ::
          {:ok, %{{binary(), pos_integer()} => map()}}
          | {:error, :invalid_query_statistics_summary | :query_statistics_unavailable}
  def summaries(ctx, identities, now_ms)
      when is_struct(identities, MapSet) and is_integer(now_ms) and now_ms >= 0 do
    if valid_summary_identities?(identities) do
      summaries = Map.new(identities, &{&1, empty_summary()})

      :ets.foldl(
        &summarize_entry(&1, &2, identities, now_ms),
        summaries,
        table_name(ctx)
      )
      |> Map.new(fn {identity, summary} -> {identity, finalize_summary(summary, now_ms)} end)
      |> then(&{:ok, &1})
    else
      {:error, :invalid_query_statistics_summary}
    end
  rescue
    ArgumentError -> {:error, :query_statistics_unavailable}
  end

  def summaries(_ctx, _identities, _now_ms),
    do: {:error, :invalid_query_statistics_summary}

  @impl true
  def init(opts) do
    ctx = Keyword.fetch!(opts, :instance_ctx)
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)

    if valid_context?(ctx) and is_integer(max_entries) and max_entries > 0 and
         max_entries <= @maximum_entries do
      table =
        :ets.new(table_name(ctx), [
          :named_table,
          :set,
          :protected,
          read_concurrency: true
        ])

      {:ok,
       %{
         table: table,
         max_entries: max_entries,
         sequence: 0,
         eviction_order: :gb_trees.empty()
       }}
    else
      {:stop, :invalid_query_statistics_store_options}
    end
  rescue
    ArgumentError -> {:stop, :query_statistics_store_already_started}
  end

  @impl true
  def handle_call({:put, %IndexStatistics{} = stat}, _from, state) do
    with {:ok, validated} <- IndexStatistics.new(Map.from_struct(stat)),
         key <- key(validated),
         :ok <- monotonic?(state.table, key, validated) do
      state = reserve_slot(state, key)
      sequence = state.sequence + 1
      true = :ets.insert(state.table, {key, validated, sequence})

      eviction_order = :gb_trees.enter(sequence, key, state.eviction_order)
      {:reply, :ok, %{state | sequence: sequence, eviction_order: eviction_order}}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  defp key(stat), do: {stat.scope_digest, stat.index_id, stat.index_version}

  defp valid_summary_identities?(identities) do
    MapSet.size(identities) <= @maximum_summary_indexes and
      Enum.all?(identities, fn
        {id, version} ->
          IndexDefinition.valid_id?(id) and IndexDefinition.valid_version?(version)

        _invalid ->
          false
      end)
  end

  defp empty_summary do
    %{
      samples: 0,
      fresh_samples: 0,
      stale_samples: 0,
      future_samples: 0,
      oldest_collected_at_ms: nil,
      newest_collected_at_ms: nil
    }
  end

  defp summarize_entry(
         {{_scope_digest, index_id, index_version}, %IndexStatistics{} = stat, _sequence},
         summaries,
         identities,
         now_ms
       ) do
    identity = {index_id, index_version}

    if MapSet.member?(identities, identity) do
      Map.update!(summaries, identity, &add_stat(&1, stat, now_ms))
    else
      summaries
    end
  end

  defp add_stat(summary, stat, now_ms) do
    fresh? = IndexStatistics.fresh?(stat, now_ms)

    summary
    |> Map.update!(:samples, &(&1 + 1))
    |> Map.update!(if(fresh?, do: :fresh_samples, else: :stale_samples), &(&1 + 1))
    |> Map.update!(:future_samples, fn count ->
      if stat.collected_at_ms > now_ms, do: count + 1, else: count
    end)
    |> Map.update!(:oldest_collected_at_ms, &oldest(&1, stat.collected_at_ms))
    |> Map.update!(:newest_collected_at_ms, &newest(&1, stat.collected_at_ms))
  end

  defp oldest(nil, collected_at_ms), do: collected_at_ms
  defp oldest(previous, collected_at_ms), do: min(previous, collected_at_ms)
  defp newest(nil, collected_at_ms), do: collected_at_ms
  defp newest(previous, collected_at_ms), do: max(previous, collected_at_ms)

  defp finalize_summary(summary, now_ms) do
    summary
    |> Map.put(:status, summary_status(summary))
    |> Map.put(:oldest_age_ms, sample_age(now_ms, summary.oldest_collected_at_ms))
    |> Map.put(:newest_age_ms, sample_age(now_ms, summary.newest_collected_at_ms))
  end

  defp summary_status(%{samples: 0}), do: :missing
  defp summary_status(%{stale_samples: 0}), do: :fresh
  defp summary_status(%{fresh_samples: 0}), do: :stale
  defp summary_status(_summary), do: :mixed

  defp sample_age(_now_ms, nil), do: nil
  defp sample_age(now_ms, collected_at_ms), do: max(now_ms - collected_at_ms, 0)

  defp monotonic?(table, key, next) do
    case :ets.lookup(table, key) do
      [{^key, old, _sequence}]
      when next.collected_at_ms >= old.collected_at_ms and
             next.source_watermark >= old.source_watermark ->
        if prefix_observations_monotonic?(old, next),
          do: :ok,
          else: {:error, :non_monotonic_query_statistics}

      [] ->
        :ok

      _regression ->
        {:error, :non_monotonic_query_statistics}
    end
  end

  defp prefix_observations_monotonic?(old, next) do
    Enum.all?(next.prefix_observed_at_ms, fn {digest, observed_at_ms} ->
      case Map.fetch(old.prefix_observed_at_ms, digest) do
        {:ok, previous_at_ms} -> observed_at_ms >= previous_at_ms
        :error -> true
      end
    end)
  end

  defp reserve_slot(state, key) do
    case :ets.lookup(state.table, key) do
      [{^key, _stat, old_sequence}] ->
        %{state | eviction_order: :gb_trees.delete_any(old_sequence, state.eviction_order)}

      [] ->
        if :gb_trees.size(state.eviction_order) < state.max_entries do
          state
        else
          {_sequence, oldest_key, eviction_order} =
            :gb_trees.take_smallest(state.eviction_order)

          true = :ets.delete(state.table, oldest_key)
          %{state | eviction_order: eviction_order}
        end
    end
  end

  defp valid_context?(%{name: name}), do: is_atom(name)
  defp valid_context?(_ctx), do: false
end
