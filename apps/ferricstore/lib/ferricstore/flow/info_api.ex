defmodule Ferricstore.Flow.InfoAPI do
  @moduledoc false

  alias Ferricstore.Flow.InfoCountRead
  alias Ferricstore.Flow.InfoCounts
  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.IndexZSet
  alias Ferricstore.Store.Router

  @default_state "queued"
  @terminal_states ["completed", "failed", "cancelled"]
  @default_lmdb_query_scan_limit 10_000

  def info(ctx, type, opts \\ [])

  def info(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, counts, inflight} <-
           counts(
             ctx,
             type,
             partition_key,
             include_cold? or consistent_projection?,
             consistent_projection?
           ) do
      {:ok,
       counts
       |> Map.put(:type, type)
       |> Map.put(:partition_key, response_partition_key(partition_key))
       |> Map.put(:inflight, inflight)}
    end
  end

  def info(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def info(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp counts(ctx, type, :auto, include_cold?, consistent?) do
    zero_counts = InfoCounts.zero_counts(@default_state, @terminal_states)

    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, zero_counts, 0}, fn partition_key,
                                                   {:ok, counts_acc, inflight_acc} ->
      case counts(ctx, type, partition_key, include_cold?, consistent?) do
        {:ok, counts, inflight} ->
          {merged, inflight_total} =
            InfoCounts.merge_auto({counts_acc, inflight_acc}, counts, inflight)

          {:cont, {:ok, merged, inflight_total}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp counts(ctx, type, partition_key, include_cold?, consistent?) do
    state_keys =
      InfoCounts.state_keys(
        type,
        partition_key,
        @default_state,
        @terminal_states
      )

    inflight_key = InfoCounts.inflight_key(type, partition_key)
    all_keys = state_keys ++ [inflight_key]

    with :ok <- validate_index_keys(all_keys),
         {:ok, ram_counts} <-
           InfoCountRead.zset_count_many(
             ctx,
             Enum.map(all_keys, fn {_state, key} -> key end)
           ),
         {:ok, lmdb_counts} <-
           InfoCountRead.terminal_lmdb_counts(
             ctx,
             state_keys,
             partition_key,
             include_cold?,
             consistent?,
             @terminal_states
           ) do
      {state_ram_counts, [inflight]} = Enum.split(ram_counts, length(state_keys))

      state_keys
      |> Enum.zip(state_ram_counts)
      |> Enum.reduce_while({:ok, %{}}, fn {{state, key}, ram_count}, {:ok, acc} ->
        with {:ok, count} <-
               maybe_recount_overlapping_terminal(
                 ctx,
                 key,
                 state,
                 partition_key,
                 ram_count,
                 Map.get(lmdb_counts, key, 0)
               ) do
          {:cont, {:ok, Map.put(acc, String.to_atom(state), count)}}
        else
          {:error, _} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, counts} -> {:ok, counts, inflight}
        {:error, _reason} = error -> error
      end
    end
  end

  defp validate_index_keys(state_keys) do
    Enum.reduce_while(state_keys, :ok, fn {_state, key}, :ok ->
      case validate_key_size(key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp maybe_recount_overlapping_terminal(
         ctx,
         index_key,
         state,
         partition_key,
         ram_count,
         lmdb_count
       )
       when state in @terminal_states and lmdb_count > 0 do
    with {:ok, ram_ids} <- maybe_zrange_all(ctx, index_key, ram_count),
         {:ok, lmdb_ids} <-
           LMDBIndexRead.terminal_ids(
             ctx,
             index_key,
             state,
             partition_key,
             lmdb_count,
             true,
             false,
             nil,
             @terminal_states,
             @default_lmdb_query_scan_limit
           ) do
      count =
        ram_ids
        |> MapSet.new()
        |> MapSet.union(MapSet.new(lmdb_ids))
        |> MapSet.size()

      {:ok, count}
    end
  end

  defp maybe_recount_overlapping_terminal(
         _ctx,
         _index_key,
         _state,
         _partition_key,
         ram_count,
         lmdb_count
       ) do
    {:ok, ram_count + lmdb_count}
  end

  defp maybe_zrange_all(_ctx, _index_key, count) when count <= 0, do: {:ok, []}
  defp maybe_zrange_all(ctx, index_key, count), do: IndexZSet.range(ctx, index_key, 0, count - 1)

  defp response_partition_key(:auto), do: nil
  defp response_partition_key(partition_key), do: partition_key

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp optional_auto_partition_key(opts) do
    case Keyword.get(opts, :partition_key, :auto) do
      nil -> {:ok, nil}
      :auto -> {:ok, :auto}
      :any -> {:ok, :auto}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end
end
