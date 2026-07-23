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

  @default_terminal_lmdb_sweep_limit 10_000
  @raw_prefix_merge_max_bytes 16 * 1_024 * 1_024

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

  def terminal_entries_window_with_count(
        _ctx,
        _index_key,
        _state,
        _partition_key,
        count,
        _include_cold?,
        _consistent?,
        _query,
        _terminal_states
      )
      when count <= 0,
      do: {:ok, [], true, 0}

  def terminal_entries_window_with_count(
        _ctx,
        _index_key,
        _state,
        _partition_key,
        _count,
        false,
        _consistent?,
        _query,
        _terminal_states
      ),
      do: {:ok, [], true, 0}

  def terminal_entries_window_with_count(
        ctx,
        index_key,
        state,
        partition_key,
        count,
        true,
        consistent?,
        query,
        terminal_states
      ) do
    if state in terminal_states do
      lmdb_terminal_entries_window(
        ctx,
        index_key,
        partition_key,
        count,
        consistent?,
        query
      )
    else
      {:ok, [], true, 0}
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
    scan_count = query_scan_count(count, default_scan_limit)

    query_entries_window(
      ctx,
      index_key,
      partition_key,
      scan_count,
      consistent?,
      query
    )
  end

  def query_entries_window(
        _ctx,
        _index_key,
        _partition_key,
        count,
        _consistent?,
        _query
      )
      when count <= 0,
      do: {:ok, []}

  def query_entries_window(ctx, index_key, partition_key, count, consistent?, query) do
    with {:ok, entries, _exhausted?, _scanned_count} <-
           query_entries_window_with_count(
             ctx,
             index_key,
             partition_key,
             count,
             consistent?,
             query
           ) do
      {:ok, entries}
    end
  end

  def query_entries_window_with_count(
        _ctx,
        _index_key,
        _partition_key,
        count,
        _consistent?,
        _query
      )
      when count <= 0,
      do: {:ok, [], true, 0}

  def query_entries_window_with_count(
        ctx,
        index_key,
        partition_key,
        count,
        consistent?,
        query
      ) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key, partition_key) do
      prefix = LMDB.query_index_prefix(index_key)
      now_ms = CommandTime.now_ms()
      probe_count = count + 1

      ctx
      |> lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        case query_prefix_entries(path, prefix, probe_count, query) do
          {:ok, entries} when is_list(entries) ->
            {bounded_entries, exhausted?} =
              bounded_query_entries(entries, prefix, probe_count, query)

            {:cont, {:ok, [{path, bounded_entries, exhausted?} | acc]}}

          {:error, _reason} = error ->
            {:halt, error}

          invalid ->
            {:halt, {:error, {:invalid_lmdb_query_window, invalid}}}
        end
      end)
      |> case do
        {:ok, path_windows} ->
          path_windows
          |> Enum.reverse()
          |> finalize_query_window(count, query, now_ms)

        {:error, _reason} = error ->
          error
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
      |> raw_prefix_path_chunks(prefix, count)
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

  defp bounded_query_entries(entries, prefix, probe_count, query) do
    {bounded_entries, outside_range} =
      Enum.split_while(entries, &query_entry_within_far_bound?(&1, prefix, query))

    exhausted? = outside_range != [] or length(entries) < probe_count
    {bounded_entries, exhausted?}
  end

  defp raw_prefix_path_chunks([], _prefix, _count), do: {:ok, []}

  defp raw_prefix_path_chunks([path], prefix, count) do
    case LMDB.prefix_entries(path, prefix, count) do
      {:ok, []} -> {:ok, []}
      {:ok, entries} -> {:ok, [{path, entries}]}
      {:error, _reason} = error -> error
    end
  end

  defp raw_prefix_path_chunks(paths, prefix, count) do
    with {:ok, rows, _scanned} <-
           LMDB.prefix_merge_entries(paths, prefix, count, @raw_prefix_merge_max_bytes) do
      entries_by_source =
        Enum.group_by(
          rows,
          fn {source, _key, _value} -> source end,
          fn {_source, key, value} -> {key, value} end
        )

      chunks =
        paths
        |> Enum.with_index()
        |> Enum.flat_map(fn {path, source} ->
          case Map.fetch(entries_by_source, source) do
            {:ok, entries} -> [{path, entries}]
            :error -> []
          end
        end)

      {:ok, chunks}
    end
  end

  defp query_entry_within_far_bound?({key, _value}, prefix, %{rev?: true, from_ms: from_ms})
       when is_binary(key) and is_integer(from_ms) and from_ms >= 0 do
    key >= LMDBQueryWindow.time_seek_key(prefix, from_ms)
  end

  defp query_entry_within_far_bound?({key, _value}, prefix, %{rev?: false, to_ms: to_ms})
       when is_binary(key) and is_integer(to_ms) and to_ms >= 0 do
    key < LMDBQueryWindow.time_upper_seek_key(prefix, to_ms)
  end

  defp query_entry_within_far_bound?(_entry, _prefix, _query), do: true

  defp finalize_query_window([{path, entries, path_exhausted?}], count, _query, now_ms) do
    selected_entries = Enum.take(entries, count)
    scanned_count = length(selected_entries)
    exhausted? = length(entries) <= count and path_exhausted?

    with {:ok, decoded_entries} <-
           LMDBIndexDecode.query_entries(selected_entries, path, now_ms) do
      {:ok, IndexMerge.query_entries_from_chunks([decoded_entries]), exhausted?, scanned_count}
    end
  end

  defp finalize_query_window(path_windows, count, query, now_ms) do
    raw_entries = ranked_raw_entries(path_windows, query)
    selected_entries = Enum.take(raw_entries, count)
    exhausted? = raw_window_exhausted?(path_windows, raw_entries, count)

    with {:ok, chunks} <- decode_query_window_entries(selected_entries, now_ms) do
      {:ok, IndexMerge.query_entries_from_chunks(chunks), exhausted?, length(selected_entries)}
    end
  end

  defp decode_query_window_entries(entries, now_ms) do
    entries
    |> Enum.group_by(
      fn {path, _entry} -> path end,
      fn {_path, entry} -> entry end
    )
    |> Enum.reduce_while({:ok, []}, fn {path, path_entries}, {:ok, chunks} ->
      case LMDBIndexDecode.query_entries(path_entries, path, now_ms) do
        {:ok, decoded_entries} ->
          {:cont, {:ok, RecordQuery.prepend_chunk(decoded_entries, chunks)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp ranked_raw_entries(path_windows, query) do
    path_windows
    |> Enum.flat_map(fn {path, entries, _exhausted?} ->
      Enum.map(entries, &{path, &1})
    end)
    |> Enum.sort_by(
      fn {_path, {key, _value}} -> key end,
      if(RAMIndexRead.reverse?(query), do: :desc, else: :asc)
    )
  end

  defp raw_window_exhausted?(path_windows, raw_entries, count) do
    length(raw_entries) <= count and
      Enum.all?(path_windows, fn {_path, _entries, exhausted?} -> exhausted? end)
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
    scan_count = terminal_scan_count(count, query, default_scan_limit)

    with {:ok, entries, _exhausted?, _scanned_count} <-
           lmdb_terminal_entries_window(
             ctx,
             index_key,
             partition_key,
             scan_count,
             consistent?,
             query
           ) do
      {:ok, Enum.take(entries, count)}
    end
  end

  defp lmdb_terminal_entries_window(
         ctx,
         index_key,
         partition_key,
         count,
         consistent?,
         query
       ) do
    with :ok <- maybe_flush_lmdb_for_index(ctx, index_key, partition_key, consistent?),
         :ok <- LMDBMirror.require_healthy(ctx, index_key, partition_key) do
      prefix = LMDB.terminal_index_prefix(index_key)
      now_ms = CommandTime.now_ms()
      probe_count = count + 1
      sweep_limit = terminal_lmdb_sweep_limit(probe_count)

      ctx
      |> lmdb_paths_for_index(index_key, partition_key)
      |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
        with {:ok, _swept} <- LMDB.sweep_expired_terminal(path, now_ms, sweep_limit),
             {:ok, entries} <- terminal_prefix_entries(path, prefix, probe_count, query) do
          {bounded_entries, exhausted?} =
            bounded_query_entries(entries, prefix, probe_count, query)

          {:cont, {:ok, [{path, bounded_entries, exhausted?} | acc]}}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, path_windows} ->
          path_windows
          |> Enum.reverse()
          |> finalize_terminal_window(count, query, now_ms)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp finalize_terminal_window([{path, entries, path_exhausted?}], count, query, now_ms) do
    selected_entries = Enum.take(entries, count)
    scanned_count = length(selected_entries)
    exhausted? = length(entries) <= count and path_exhausted?

    with {:ok, decoded_entries} <-
           LMDBIndexDecode.terminal_entries(selected_entries, path, now_ms) do
      {:ok,
       IndexMerge.terminal_entries_from_chunks(
         [decoded_entries],
         count,
         RAMIndexRead.reverse?(query)
       ), exhausted?, scanned_count}
    end
  end

  defp finalize_terminal_window(path_windows, count, query, now_ms) do
    raw_entries = ranked_raw_entries(path_windows, query)
    selected_entries = Enum.take(raw_entries, count)
    exhausted? = raw_window_exhausted?(path_windows, raw_entries, count)

    with {:ok, chunks} <- decode_terminal_window_entries(selected_entries, now_ms) do
      {:ok,
       IndexMerge.terminal_entries_from_chunks(
         chunks,
         count,
         RAMIndexRead.reverse?(query)
       ), exhausted?, length(selected_entries)}
    end
  end

  defp decode_terminal_window_entries(entries, now_ms) do
    entries
    |> Enum.group_by(
      fn {path, _entry} -> path end,
      fn {_path, entry} -> entry end
    )
    |> Enum.reduce_while({:ok, []}, fn {path, path_entries}, {:ok, chunks} ->
      case LMDBIndexDecode.terminal_entries(path_entries, path, now_ms) do
        {:ok, decoded_entries} ->
          {:cont, {:ok, RecordQuery.prepend_chunk(decoded_entries, chunks)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp terminal_scan_count(count, nil, _default_scan_limit), do: count

  defp terminal_scan_count(count, query, default_scan_limit) when is_map(query) do
    if Map.get(query, :from_ms) == nil and Map.get(query, :to_ms) == nil and
         Map.get(query, :after_id) == nil and Map.get(query, :before_id) == nil and
         Map.get(query, :rev?, false) == false,
       do: count,
       else: query_scan_count(count, default_scan_limit)
  end

  defp terminal_scan_count(count, _query, default_scan_limit),
    do: query_scan_count(count, default_scan_limit)

  defp terminal_prefix_entries(path, prefix, limit, nil) do
    LMDB.prefix_entries(path, prefix, limit)
  end

  defp terminal_prefix_entries(path, prefix, limit, query) do
    query_prefix_entries(path, prefix, limit, query)
  end

  defp query_prefix_entries(path, prefix, limit, %{
         rev?: true,
         to_ms: to_ms,
         before_id: before_id
       })
       when is_integer(to_ms) and to_ms >= 0 and is_binary(before_id) and before_id != "" do
    LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      LMDBQueryWindow.cursor_seek_key(prefix, to_ms, before_id),
      limit
    )
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

  defp query_prefix_entries(path, prefix, limit, %{
         rev?: false,
         from_ms: from_ms,
         after_id: after_id
       })
       when is_integer(from_ms) and from_ms >= 0 and is_binary(after_id) and after_id != "" do
    LMDB.prefix_entries_after(
      path,
      prefix,
      LMDBQueryWindow.cursor_seek_key(prefix, from_ms, after_id),
      limit
    )
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
    case Application.get_env(
           :ferricstore,
           :flow_lmdb_terminal_sweep_limit,
           @default_terminal_lmdb_sweep_limit
         ) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @default_terminal_lmdb_sweep_limit
    end
  end

  defp terminal_lmdb_sweep_limit(request_limit)
       when is_integer(request_limit) and request_limit > 0 do
    min(terminal_lmdb_sweep_limit(), request_limit)
  end

  @doc false
  def __terminal_lmdb_sweep_limit_for_test__, do: terminal_lmdb_sweep_limit()

  @doc false
  def __terminal_lmdb_sweep_limit_for_test__(request_limit),
    do: terminal_lmdb_sweep_limit(request_limit)
end
