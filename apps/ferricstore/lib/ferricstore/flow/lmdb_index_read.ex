defmodule Ferricstore.Flow.LMDBIndexRead do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.IndexMerge
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDBIndexDecode
  alias Ferricstore.Flow.LMDBMirror
  alias Ferricstore.Flow.LMDBQueryWindow
  alias Ferricstore.Flow.LMDBWriter
  alias Ferricstore.Flow.RAMIndexRead
  alias Ferricstore.Flow.RecordQuery
  alias Ferricstore.Store.Router

  def terminal_ids(
        ctx,
        index_key,
        state,
        partition_key,
        count,
        include_cold?,
        consistent?,
        query,
        terminal_states,
        default_scan_limit
      ) do
    with {:ok, entries} <-
           terminal_entries(
             ctx,
             index_key,
             state,
             partition_key,
             count,
             include_cold?,
             consistent?,
             query,
             terminal_states,
             default_scan_limit
           ) do
      ids =
        entries
        |> Enum.map(fn {id, _updated_at_ms} -> id end)
        |> Enum.take(count)

      {:ok, ids}
    end
  end

  def terminal_entries(
        ctx,
        index_key,
        state,
        partition_key,
        count,
        include_cold?,
        consistent?,
        query,
        terminal_states,
        default_scan_limit
      ) do
    cond do
      state not in terminal_states ->
        {:ok, []}

      count <= 0 ->
        {:ok, []}

      include_cold? ->
        lmdb_terminal_entries(
          ctx,
          index_key,
          partition_key,
          count,
          consistent?,
          query,
          default_scan_limit
        )

      true ->
        {:ok, []}
    end
  end

  def query_entries(
        _ctx,
        _index_key,
        _partition_key,
        count,
        _consistent?,
        _query,
        _default_scan_limit
      )
      when count <= 0,
      do: {:ok, []}

  def query_entries(ctx, index_key, partition_key, count, consistent?, query, default_scan_limit) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key, partition_key) do
      prefix = LMDB.query_index_prefix(index_key)
      now_ms = CommandTime.now_ms()
      scan_count = query_scan_count(count, default_scan_limit)

      ctx
      |> lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, entries} <- query_prefix_entries(path, prefix, scan_count, query) do
          {:cont,
           {:ok,
            RecordQuery.prepend_chunk(
              LMDBIndexDecode.query_entries(entries, path, now_ms),
              acc
            )}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, chunks} -> {:ok, IndexMerge.query_entries_from_chunks(chunks)}
        {:error, _reason} = error -> error
      end
    end
  end

  def query_count(ctx, index_key, partition_key, consistent?) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key, partition_key) do
      prefix = LMDB.query_index_prefix(index_key)

      count_lmdb_prefix(ctx, index_key, partition_key, prefix)
    end
  end

  def query_prefix_raw_entries(ctx, index_key_prefix, partition_key, count, consistent?)
      when is_integer(count) and count > 0 do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key_prefix, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key_prefix, partition_key) do
      prefix = LMDB.query_index_raw_prefix(index_key_prefix)

      ctx
      |> lmdb_paths_for_index(index_key_prefix, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        case LMDB.prefix_entries(path, prefix, count) do
          {:ok, entries} -> {:cont, {:ok, [{path, entries} | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
        {:error, _reason} = error -> error
      end
    end
  end

  def query_prefix_raw_entries(_ctx, _index_key_prefix, _partition_key, _count, _consistent?),
    do: {:ok, []}

  def query_prefix_count(ctx, index_key_prefix, partition_key, consistent?) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key_prefix, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key_prefix, partition_key) do
      prefix = LMDB.query_index_raw_prefix(index_key_prefix)

      count_lmdb_prefix(ctx, index_key_prefix, partition_key, prefix)
    end
  end

  def query_scan_count(count, default_scan_limit) when is_integer(count) and count > 0 do
    LMDBQueryWindow.query_scan_count(count, default_scan_limit)
  end

  defp count_lmdb_prefix(ctx, index_key, partition_key, prefix) do
    ctx
    |> lmdb_paths_for_index(index_key, partition_key)
    |> Enum.reduce_while({:ok, 0}, fn path, {:ok, acc} ->
      case LMDB.prefix_count(path, prefix) do
        {:ok, count} -> {:cont, {:ok, acc + count}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp lmdb_terminal_entries(
         ctx,
         index_key,
         partition_key,
         count,
         consistent?,
         query,
         default_scan_limit
       ) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key, partition_key) do
      prefix = LMDB.terminal_index_prefix(index_key)
      now_ms = CommandTime.now_ms()
      sweep_limit = terminal_lmdb_sweep_limit()
      scan_count = terminal_scan_count(count, query, default_scan_limit)

      ctx
      |> lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, _swept} <- LMDB.sweep_expired_terminal(path, now_ms, sweep_limit),
             {:ok, entries} <- terminal_prefix_entries(path, prefix, scan_count, query) do
          {:cont,
           {:ok,
            RecordQuery.prepend_chunk(
              LMDBIndexDecode.terminal_entries(entries, path, now_ms),
              acc
            )}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, chunks} ->
          entries =
            IndexMerge.terminal_entries_from_chunks(
              chunks,
              count,
              RAMIndexRead.reverse?(query)
            )

          {:ok, entries}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp terminal_scan_count(count, nil, _default_scan_limit), do: count

  defp terminal_scan_count(count, %{from_ms: nil, to_ms: nil, rev?: false}, _default_scan_limit),
    do: count

  defp terminal_scan_count(count, _query, default_scan_limit),
    do: query_scan_count(count, default_scan_limit)

  defp terminal_prefix_entries(path, prefix, limit, nil) do
    LMDB.prefix_entries(path, prefix, limit)
  end

  defp terminal_prefix_entries(path, prefix, limit, query) do
    query_prefix_entries(path, prefix, limit, query)
  end

  defp query_prefix_entries(path, prefix, limit, %{rev?: true, to_ms: to_ms})
       when is_integer(to_ms) and to_ms >= 0 do
    LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      LMDBQueryWindow.time_upper_seek_key(prefix, to_ms),
      limit
    )
  end

  defp query_prefix_entries(path, prefix, limit, %{rev?: true}) do
    LMDB.prefix_entries(path, prefix, limit, true)
  end

  defp query_prefix_entries(path, prefix, limit, %{from_ms: from_ms})
       when is_integer(from_ms) and from_ms >= 0 do
    LMDB.prefix_entries_after(
      path,
      prefix,
      LMDBQueryWindow.time_seek_key(prefix, from_ms),
      limit
    )
  end

  defp query_prefix_entries(path, prefix, limit, query) do
    LMDB.prefix_entries(path, prefix, limit, Map.get(query, :rev?, false))
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
