defmodule Ferricstore.Flow.InfoCountRead do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.IndexZSet
  alias Ferricstore.Flow.InfoCounts
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBMirror
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Store.Router

  def zset_count_many(_ctx, []), do: {:ok, []}

  def zset_count_many(ctx, keys) do
    case Router.flow_index_count_all_many(ctx, keys) do
      {:ok, counts} -> {:ok, counts}
      :unavailable -> zcard_many_fallback(ctx, keys)
    end
  end

  def terminal_lmdb_counts(
        _ctx,
        _state_keys,
        _partition_key,
        false,
        _consistent?,
        _terminal_states
      ),
      do: {:ok, %{}}

  def terminal_lmdb_counts(ctx, state_keys, partition_key, true, consistent?, terminal_states) do
    terminal_keys = InfoCounts.terminal_keys(state_keys, terminal_states)

    case terminal_keys do
      [] ->
        {:ok, %{}}

      [first_key | _] ->
        with :ok <- maybe_flush_lmdb_for_index(ctx, first_key, partition_key, consistent?),
             :ok <- LMDBMirror.require_healthy(ctx, first_key, partition_key) do
          now_ms = CommandTime.now_ms()
          sweep_limit = terminal_lmdb_sweep_limit()

          ctx
          |> lmdb_paths_for_index(first_key, partition_key)
          |> Enum.reduce_while({:ok, Map.new(terminal_keys, &{&1, 0})}, fn path, {:ok, acc} ->
            with {:ok, counts} <- LMDB.terminal_counts(path, terminal_keys),
                 {:ok, counts} <-
                   maybe_sweep_terminal_lmdb_counts(
                     path,
                     terminal_keys,
                     counts,
                     now_ms,
                     sweep_limit
                   ) do
              merged = InfoCounts.merge_terminal_counts(acc, terminal_keys, counts)
              {:cont, {:ok, merged}}
            else
              {:error, _reason} = error -> {:halt, error}
            end
          end)
        end
    end
  end

  defp zcard_many_fallback(ctx, keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case IndexZSet.card(ctx, key) do
        {:ok, count} -> {:cont, {:ok, [count | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, counts} -> {:ok, Enum.reverse(counts)}
      {:error, _reason} = error -> error
    end
  end

  defp maybe_flush_lmdb_for_index(_ctx, _index_key, _partition_key, false), do: :ok

  defp maybe_flush_lmdb_for_index(ctx, index_key, partition_key, true) do
    case partition_key do
      nil ->
        LMDBWriter.flush_all(ctx.name, ctx.shard_count)

      partition_key when is_binary(partition_key) ->
        shard_index = Router.shard_for(ctx, index_key)
        LMDBWriter.flush(ctx.name, shard_index)
    end
  end

  defp maybe_sweep_terminal_lmdb_counts(path, terminal_keys, counts, now_ms, sweep_limit) do
    if Enum.any?(counts, &(&1 > 0)) do
      with {:ok, _swept} <- LMDB.sweep_expired_terminal(path, now_ms, sweep_limit) do
        LMDB.terminal_counts(path, terminal_keys)
      end
    else
      {:ok, counts}
    end
  end

  defp lmdb_paths_for_index(ctx, _index_key, nil) do
    LMDBMirror.paths_for_index(ctx, nil, nil)
  end

  defp lmdb_paths_for_index(ctx, index_key, partition_key) when is_binary(partition_key) do
    LMDBMirror.paths_for_index(ctx, index_key, partition_key)
  end

  defp terminal_lmdb_sweep_limit do
    Application.get_env(:ferricstore, :flow_lmdb_terminal_sweep_limit, 10_000)
  end
end
