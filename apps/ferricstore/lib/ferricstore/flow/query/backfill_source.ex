defmodule Ferricstore.Flow.Query.BackfillSource do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.Query.SourceCatalog
  alias Ferricstore.Store.Router

  @root "flow-query-backfill:1:"
  @snapshot_progress_suffix ":snapshot-progress"
  @snapshot_complete_suffix ":snapshot-complete"
  @staging_suffix ":state:"
  @max_build_id_bytes 128
  @max_page_items 256
  @max_page_bytes 16 * 1_024 * 1_024
  @cleanup_page_bytes 2 * 1_024 * 1_024

  @spec staging_prefix(binary()) :: binary()
  def staging_prefix(build_id) when is_binary(build_id) and build_id != "" do
    root(build_id) <> @staging_suffix
  end

  @spec snapshot_page(map(), non_neg_integer(), binary(), pos_integer(), pos_integer()) ::
          {:ok,
           %{done?: boolean(), scanned_keys: non_neg_integer(), staged_states: non_neg_integer()}}
          | {:error, term()}
  def snapshot_page(ctx, shard_index, build_id, max_items, max_bytes)
      when is_integer(max_items) and max_items > 0 and max_items <= @max_page_items and
             is_integer(max_bytes) and max_bytes > 0 and max_bytes <= @max_page_bytes do
    with :ok <- validate_context(ctx, shard_index, build_id),
         path <- lmdb_path(ctx, shard_index),
         false <- snapshot_complete?(path, build_id) do
      bootstrap_or_snapshot(ctx, shard_index, path, build_id, max_items, max_bytes)
    else
      true -> {:ok, %{done?: true, scanned_keys: 0, staged_states: 0}}
      {:error, _reason} = error -> error
    end
  rescue
    error in [ArgumentError] -> {:error, {:query_backfill_snapshot_failed, error}}
  end

  def snapshot_page(_ctx, _shard_index, _build_id, _max_items, _max_bytes),
    do: {:error, :invalid_query_backfill_snapshot_request}

  @spec staging_page(
          map(),
          non_neg_integer(),
          binary(),
          binary(),
          pos_integer(),
          pos_integer()
        ) ::
          {:ok,
           %{
             state_keys: [binary()],
             cursor: binary(),
             done?: boolean(),
             scanned_entries: non_neg_integer(),
             staging_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def staging_page(ctx, shard_index, build_id, cursor, max_items, max_bytes)
      when is_binary(cursor) and is_integer(max_items) and max_items > 0 and
             max_items <= @max_page_items and is_integer(max_bytes) and max_bytes > 0 and
             max_bytes <= @max_page_bytes do
    with :ok <- validate_context(ctx, shard_index, build_id),
         prefix <- staging_prefix(build_id),
         :ok <- validate_cursor(prefix, cursor),
         {:ok, rows, exhausted, staging_bytes} <-
           LMDB.range_entries_bounded(
             lmdb_path(ctx, shard_index),
             prefix,
             cursor,
             "",
             max_items,
             max_bytes
           ),
         {:ok, state_keys} <- decode_staging_rows(prefix, rows) do
      {:ok,
       %{
         state_keys: state_keys,
         cursor: next_cursor(rows, cursor),
         done?: exhausted,
         scanned_entries: length(rows),
         staging_bytes: staging_bytes
       }}
    end
  end

  def staging_page(_ctx, _shard_index, _build_id, _cursor, _max_items, _max_bytes),
    do: {:error, :invalid_query_backfill_staging_page_request}

  @spec page(
          map(),
          non_neg_integer(),
          binary(),
          binary(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) ::
          {:ok,
           %{
             records: [map()],
             cursor: binary(),
             done?: boolean(),
             scanned_entries: non_neg_integer(),
             hydrated_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def page(ctx, shard_index, build_id, cursor, max_items, max_bytes, opts \\ [])

  def page(ctx, shard_index, build_id, cursor, max_items, max_bytes, opts)
      when is_binary(cursor) and is_integer(max_items) and max_items > 0 and
             max_items <= @max_page_items and is_integer(max_bytes) and max_bytes > 0 and
             max_bytes <= @max_page_bytes and is_list(opts) do
    with :ok <- validate_context(ctx, shard_index, build_id),
         effective_items <- effective_page_items(ctx, max_items, max_bytes),
         {:ok, staged} <-
           staging_page(ctx, shard_index, build_id, cursor, effective_items, max_bytes),
         {:ok, records, hydrated_bytes} <-
           hydrate_records(ctx, shard_index, staged.state_keys, max_bytes, opts) do
      {:ok,
       %{
         records: records,
         cursor: staged.cursor,
         done?: staged.done?,
         scanned_entries: staged.scanned_entries,
         hydrated_bytes: hydrated_bytes
       }}
    end
  end

  def page(_ctx, _shard_index, _build_id, _cursor, _max_items, _max_bytes, _opts),
    do: {:error, :invalid_query_backfill_page_request}

  @spec cleanup(map(), non_neg_integer(), binary()) ::
          :ok | {:ok, :progress} | {:error, term()}
  def cleanup(ctx, shard_index, build_id) do
    with :ok <- validate_context(ctx, shard_index, build_id),
         path <- lmdb_path(ctx, shard_index),
         {:ok, status} <- cleanup_staging_page(path, staging_prefix(build_id)) do
      case status do
        :complete ->
          LMDB.write_batch(path, [
            {:delete, snapshot_progress_key(build_id)},
            {:delete, snapshot_complete_key(build_id)}
          ])

        :progress ->
          {:ok, :progress}
      end
    end
  end

  defp validate_context(
         %{data_dir: data_dir, shard_count: shard_count},
         shard_index,
         build_id
       )
       when is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and
              shard_count > 0 and is_integer(shard_index) and shard_index >= 0 and
              shard_index < shard_count and is_binary(build_id) and build_id != "" and
              byte_size(build_id) <= @max_build_id_bytes,
       do: :ok

  defp validate_context(_ctx, _shard_index, _build_id),
    do: {:error, :invalid_query_backfill_context}

  defp bootstrap_or_snapshot(ctx, shard_index, path, build_id, max_items, max_bytes) do
    case SourceCatalog.bootstrap_page(ctx, shard_index, max_items, max_bytes) do
      {:ok, %{done?: true}} ->
        snapshot_catalog_page(path, build_id, max_items, max_bytes)

      {:ok, %{done?: false, scanned_keys: scanned_keys}} when scanned_keys > 0 ->
        {:ok, %{done?: false, scanned_keys: scanned_keys, staged_states: 0}}

      {:error, :query_source_catalog_page_too_large} ->
        {:error, :query_backfill_snapshot_page_too_large}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_query_source_catalog_bootstrap_page}
    end
  end

  defp snapshot_catalog_page(path, build_id, max_items, max_bytes) do
    with {:ok, cursor} <- load_snapshot_cursor(path, build_id),
         {:ok, page} <- SourceCatalog.page(path, cursor, max_items, max_bytes),
         :ok <-
           persist_snapshot_page(
             path,
             build_id,
             page.state_keys,
             page.cursor,
             page.done?,
             max_bytes
           ) do
      {:ok,
       %{
         done?: page.done?,
         scanned_keys: page.scanned_entries,
         staged_states: length(page.state_keys)
       }}
    else
      {:error, :query_source_catalog_page_too_large} ->
        {:error, :query_backfill_snapshot_page_too_large}

      {:error, _reason} = error ->
        error
    end
  end

  defp load_snapshot_cursor(path, build_id) do
    case LMDB.get(path, snapshot_progress_key(build_id)) do
      :not_found ->
        {:ok, ""}

      {:ok, cursor}
      when is_binary(cursor) and cursor != "" and byte_size(cursor) <= 511 ->
        {:ok, cursor}

      {:ok, _invalid} ->
        {:error, :invalid_query_backfill_snapshot_cursor}

      {:error, _reason} = error ->
        error
    end
  end

  defp persist_snapshot_page(
         path,
         build_id,
         state_keys,
         next_cursor,
         done?,
         max_bytes
       ) do
    prefix = staging_prefix(build_id)
    staged = Enum.map(state_keys, &{prefix <> digest(&1), &1})

    bytes =
      Enum.reduce(staged, 0, fn {key, value}, total ->
        total + byte_size(key) + byte_size(value)
      end)

    if bytes <= max_bytes do
      keys = Enum.map(staged, &elem(&1, 0))

      with {:ok, current, _read_bytes} <- bounded_staging_values(path, keys, max_bytes),
           {:ok, puts} <- collision_safe_puts(staged, current) do
        tail_ops = snapshot_tail_ops(build_id, next_cursor, done?)
        LMDB.write_batch(path, puts ++ tail_ops)
      end
    else
      {:error, :query_backfill_snapshot_page_too_large}
    end
  end

  defp collision_safe_puts(staged, current) when length(staged) == length(current) do
    Enum.zip(staged, current)
    |> Enum.reduce_while({:ok, []}, fn
      {{key, state_key}, :not_found}, {:ok, acc} ->
        {:cont, {:ok, [{:put, key, state_key} | acc]}}

      {{_key, state_key}, {:ok, state_key}}, {:ok, acc} ->
        {:cont, {:ok, acc}}

      {{_key, _state_key}, {:ok, _other}}, _acc ->
        {:halt, {:error, :query_backfill_staging_hash_collision}}

      {_entry, {:error, _reason} = error}, _acc ->
        {:halt, error}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp collision_safe_puts(_staged, _current),
    do: {:error, :invalid_query_backfill_staging_read}

  defp bounded_staging_values(path, keys, max_bytes) do
    case LMDB.get_many_bounded(path, keys, max_bytes) do
      {:ok, values, bytes} ->
        {:ok, values, bytes}

      {:error, reason}
      when reason in [:batch_value_budget_exceeded, :batch_key_budget_exceeded] ->
        {:error, :query_backfill_snapshot_page_too_large}

      {:error, _reason} = error ->
        error
    end
  end

  defp snapshot_tail_ops(build_id, _cursor, true) do
    [
      {:put, snapshot_complete_key(build_id), build_id},
      {:delete, snapshot_progress_key(build_id)}
    ]
  end

  defp snapshot_tail_ops(build_id, cursor, false) do
    [
      {:put, snapshot_progress_key(build_id), cursor}
    ]
  end

  defp snapshot_complete?(path, build_id),
    do: LMDB.get(path, snapshot_complete_key(build_id)) == {:ok, build_id}

  defp decode_staging_rows(prefix, rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {key, state_key}, {:ok, acc} when is_binary(key) and is_binary(state_key) ->
        if key == prefix <> digest(state_key) and Keys.state_key?(state_key),
          do: {:cont, {:ok, [state_key | acc]}},
          else: {:halt, {:error, :corrupt_query_backfill_staging_entry}}

      _invalid, _acc ->
        {:halt, {:error, :corrupt_query_backfill_staging_entry}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_records(ctx, shard_index, state_keys, max_bytes, opts) do
    read_values = Keyword.get(opts, :read_values_fun, &Router.read_shard_values/3)
    decode_record = Keyword.get(opts, :decode_record_fun, &decode_record/1)

    if is_function(read_values, 3) and is_function(decode_record, 1) do
      case read_values.(ctx, shard_index, state_keys) do
        {:ok, values} when is_list(values) and length(values) == length(state_keys) ->
          decode_current_records(state_keys, values, decode_record, max_bytes)

        :unavailable ->
          {:error, :query_backfill_primary_unavailable}

        {:error, _reason} = error ->
          error

        _invalid ->
          {:error, :invalid_query_backfill_primary_read}
      end
    else
      {:error, :invalid_query_backfill_reader}
    end
  end

  defp decode_current_records(state_keys, values, decode_record, max_bytes) do
    Enum.zip(state_keys, values)
    |> Enum.reduce_while({:ok, [], 0}, fn
      {state_key, nil}, {:ok, acc, bytes} ->
        tombstone = %{state_key: state_key, record: nil, expire_at_ms: 0}
        {:cont, {:ok, [tombstone | acc], bytes}}

      {state_key, encoded}, {:ok, acc, bytes} when is_binary(encoded) ->
        next_bytes = bytes + byte_size(encoded)

        if next_bytes > max_bytes do
          {:halt, {:error, :query_backfill_hydration_budget_exceeded}}
        else
          case decode_record.(encoded) do
            {:ok, record} when is_map(record) ->
              if valid_record_owner?(record, state_key) do
                projected = %{
                  state_key: state_key,
                  record: record,
                  expire_at_ms: record_expiry(record)
                }

                {:cont, {:ok, [projected | acc], next_bytes}}
              else
                {:halt, {:error, :corrupt_query_backfill_record}}
              end

            _invalid ->
              {:halt, {:error, :corrupt_query_backfill_record}}
          end
        end

      {_state_key, _invalid}, _acc ->
        {:halt, {:error, :invalid_query_backfill_primary_read}}
    end)
    |> case do
      {:ok, reversed, bytes} -> {:ok, Enum.reverse(reversed), bytes}
      {:error, _reason} = error -> error
    end
  end

  defp decode_record(encoded) do
    {:ok, Ferricstore.Flow.decode_record(encoded)}
  rescue
    _error -> {:error, :invalid_record}
  end

  defp valid_record_owner?(record, state_key) do
    case {Map.get(record, :id), Map.get(record, :partition_key)} do
      {id, partition_key}
      when is_binary(id) and (is_nil(partition_key) or is_binary(partition_key)) ->
        Keys.state_key(id, partition_key) == state_key

      _invalid ->
        false
    end
  end

  defp record_expiry(%{terminal_retention_until_ms: expiry})
       when is_integer(expiry) and expiry > 0,
       do: expiry

  defp record_expiry(_record), do: 0

  defp effective_page_items(ctx, max_items, max_bytes) do
    max_value_size =
      case Map.get(ctx, :max_value_size, 1_048_576) do
        value when is_integer(value) and value > 0 -> value
        _invalid -> 1_048_576
      end

    min(max_items, max(div(max_bytes, max_value_size), 1))
  end

  defp validate_cursor(_prefix, ""), do: :ok

  defp validate_cursor(prefix, cursor) do
    if byte_size(cursor) == byte_size(prefix) + 32 and String.starts_with?(cursor, prefix),
      do: :ok,
      else: {:error, :invalid_query_backfill_cursor}
  end

  defp next_cursor([], cursor), do: cursor
  defp next_cursor(rows, _cursor), do: rows |> List.last() |> elem(0)

  defp cleanup_staging_page(path, prefix) do
    case LMDB.range_entries_bounded(path, prefix, "", "", @max_page_items, @cleanup_page_bytes) do
      {:ok, [], true, _bytes} ->
        {:ok, :complete}

      {:ok, rows, exhausted, _bytes} ->
        with :ok <- LMDB.write_batch(path, Enum.map(rows, fn {key, _value} -> {:delete, key} end)) do
          {:ok, if(exhausted, do: :complete, else: :progress)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end

  defp root(build_id), do: @root <> digest(build_id)
  defp snapshot_progress_key(build_id), do: root(build_id) <> @snapshot_progress_suffix
  defp snapshot_complete_key(build_id), do: root(build_id) <> @snapshot_complete_suffix
  defp digest(value), do: :crypto.hash(:sha256, value)
end
