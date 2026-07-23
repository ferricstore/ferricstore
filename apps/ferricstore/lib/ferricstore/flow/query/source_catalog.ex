defmodule Ferricstore.Flow.Query.SourceCatalog do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB, PolicyMigration}

  @entry_prefix <<0, "fqsc:e:">>
  @bootstrap_progress_key <<0, "fqsc:p">>
  @bootstrap_complete_key <<0, "fqsc:c">>
  @bootstrap_complete_value <<"complete">>
  @max_lmdb_key_bytes 511
  @max_state_key_bytes 65_535
  @max_page_items 256
  @max_page_bytes 16 * 1_024 * 1_024

  @spec entry_prefix() :: binary()
  def entry_prefix, do: @entry_prefix

  @spec put_op(binary(), binary()) :: {:ok, {:put, binary(), binary()}} | {:error, atom()}
  def put_op(catalog_key, state_key) do
    with :ok <- validate_owner(catalog_key, state_key),
         entry_key <- entry_key(catalog_key),
         true <- byte_size(entry_key) <= @max_lmdb_key_bytes do
      {:ok, {:put, entry_key, state_key}}
    else
      _invalid -> {:error, :invalid_query_source_catalog_entry}
    end
  end

  @spec delete_op(binary()) :: {:ok, {:delete, binary()}} | {:error, atom()}
  def delete_op(catalog_key) when is_binary(catalog_key) do
    entry_key = entry_key(catalog_key)

    if Keys.type_catalog_member_key?(catalog_key) and
         byte_size(entry_key) <= @max_lmdb_key_bytes do
      {:ok, {:delete, entry_key}}
    else
      {:error, :invalid_query_source_catalog_entry}
    end
  end

  def delete_op(_catalog_key), do: {:error, :invalid_query_source_catalog_entry}

  @spec decode_entry(binary(), binary()) ::
          {:ok, binary(), binary()} | {:error, atom()}
  def decode_entry(entry_key, state_key)
      when is_binary(entry_key) and is_binary(state_key) do
    case entry_key do
      <<@entry_prefix::binary, catalog_key::binary>> when catalog_key != "" ->
        case validate_owner(catalog_key, state_key) do
          :ok -> {:ok, catalog_key, state_key}
          {:error, _reason} = error -> error
        end

      _invalid ->
        {:error, :invalid_query_source_catalog_entry}
    end
  end

  def decode_entry(_entry_key, _state_key),
    do: {:error, :invalid_query_source_catalog_entry}

  @spec bootstrap_page(map(), non_neg_integer(), pos_integer(), pos_integer()) ::
          {:ok,
           %{
             done?: boolean(),
             scanned_keys: non_neg_integer(),
             catalog_entries: non_neg_integer()
           }}
          | {:error, term()}
  def bootstrap_page(ctx, shard_index, max_items, max_bytes),
    do: bootstrap_page(ctx, shard_index, max_items, max_bytes, [])

  @doc false
  @spec bootstrap_page(map(), non_neg_integer(), pos_integer(), pos_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def bootstrap_page(ctx, shard_index, max_items, max_bytes, opts)
      when is_integer(max_items) and max_items > 0 and max_items <= @max_page_items and
             is_integer(max_bytes) and max_bytes > 0 and max_bytes <= @max_page_bytes and
             is_list(opts) do
    with {:ok, write_batch} <- write_batch_fun(opts),
         :ok <- validate_context(ctx, shard_index),
         path <- lmdb_path(ctx, shard_index),
         {:ok, false} <- bootstrap_complete?(path),
         {:ok, cursor} <- load_bootstrap_cursor(path),
         {:ok, rows, exhausted, _range_bytes} <-
           LMDB.range_entries_bounded(
             path,
             Keys.policy_catalog_projection_global_prefix(),
             cursor,
             "",
             max_items,
             max_bytes
           ),
         {:ok, candidates} <- decode_projection_rows(rows),
         {:ok, entries} <- hydrate_catalog_entries(path, candidates, max_bytes),
         :ok <- persist_bootstrap_page(path, rows, entries, exhausted, max_bytes, write_batch) do
      {:ok,
       %{
         done?: exhausted,
         scanned_keys: length(rows),
         catalog_entries: length(entries)
       }}
    else
      {:ok, true} -> {:ok, %{done?: true, scanned_keys: 0, catalog_entries: 0}}
      {:error, :range_entry_too_large} -> {:error, :query_source_catalog_page_too_large}
      {:error, _reason} = error -> error
    end
  end

  def bootstrap_page(_ctx, _shard_index, _max_items, _max_bytes, _opts),
    do: {:error, :invalid_query_source_catalog_bootstrap_request}

  @spec page(binary(), binary(), pos_integer(), pos_integer()) ::
          {:ok,
           %{
             state_keys: [binary()],
             cursor: binary(),
             done?: boolean(),
             scanned_entries: non_neg_integer(),
             catalog_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def page(path, cursor, max_items, max_bytes)
      when is_binary(path) and is_binary(cursor) and is_integer(max_items) and max_items > 0 and
             max_items <= @max_page_items and is_integer(max_bytes) and max_bytes > 0 and
             max_bytes <= @max_page_bytes do
    with :ok <- validate_cursor(cursor),
         {:ok, rows, exhausted, catalog_bytes} <-
           LMDB.range_entries_bounded(
             path,
             @entry_prefix,
             cursor,
             "",
             max_items,
             max_bytes
           ),
         {:ok, state_keys} <- decode_source_rows(rows) do
      {:ok,
       %{
         state_keys: state_keys,
         cursor: next_cursor(rows, cursor),
         done?: exhausted,
         scanned_entries: length(rows),
         catalog_bytes: catalog_bytes
       }}
    else
      {:error, :range_entry_too_large} -> {:error, :query_source_catalog_page_too_large}
      {:error, _reason} = error -> error
    end
  end

  def page(_path, _cursor, _max_items, _max_bytes),
    do: {:error, :invalid_query_source_catalog_page_request}

  defp decode_projection_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {key, <<1>>}, {:ok, acc} ->
        case Keys.decode_policy_catalog_projection_key(key) do
          {:ok, candidate} -> {:cont, {:ok, [candidate | acc]}}
          :error -> {:halt, {:error, :corrupt_policy_catalog_projection}}
        end

      _invalid, _acc ->
        {:halt, {:error, :corrupt_policy_catalog_projection}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_catalog_entries(_path, [], _max_bytes), do: {:ok, []}

  defp hydrate_catalog_entries(path, candidates, max_bytes) do
    catalog_keys = Enum.map(candidates, & &1.catalog_key)

    case LMDB.get_many_bounded(path, catalog_keys, max_bytes) do
      {:ok, values, _value_bytes} -> decode_catalog_values(candidates, values)
      {:error, :batch_value_budget_exceeded} -> {:error, :query_source_catalog_page_too_large}
      {:error, _reason} = error -> error
    end
  end

  defp decode_catalog_values(candidates, values) when length(candidates) == length(values) do
    Enum.zip(candidates, values)
    |> Enum.reduce_while({:ok, []}, fn
      {_candidate, :not_found}, {:ok, acc} ->
        # A concurrent membership delete removes the primary and stable projections together.
        {:cont, {:ok, acc}}

      {candidate, {:ok, wrapped}}, {:ok, acc} when is_binary(wrapped) ->
        with {:ok, encoded} <- decode_mirror_value(wrapped),
             {:ok, catalog} <- PolicyMigration.decode_catalog(encoded),
             true <- catalog.migration_generation >= candidate.migration_generation,
             :ok <- validate_owner(candidate.catalog_key, catalog.state_key),
             {:ok, {:put, entry_key, state_key}} <-
               put_op(candidate.catalog_key, catalog.state_key) do
          entry = %{
            catalog_key: candidate.catalog_key,
            primary_value: wrapped,
            entry_key: entry_key,
            state_key: state_key
          }

          {:cont, {:ok, [entry | acc]}}
        else
          _invalid -> {:halt, {:error, :corrupt_query_source_catalog_primary}}
        end

      _invalid, _acc ->
        {:halt, {:error, :corrupt_query_source_catalog_primary}}
    end)
    |> case do
      {:ok, reversed} ->
        {:ok, reversed |> Enum.reverse() |> Enum.uniq_by(& &1.entry_key)}

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_catalog_values(_candidates, _values),
    do: {:error, :invalid_query_source_catalog_primary_read}

  defp decode_mirror_value(wrapped) do
    case LMDB.decode_value(wrapped, System.system_time(:millisecond)) do
      {:ok, encoded} when is_binary(encoded) -> {:ok, encoded}
      _invalid -> {:error, :corrupt_query_source_catalog_primary}
    end
  end

  defp persist_bootstrap_page(path, rows, entries, exhausted, max_bytes, write_batch) do
    stable_keys = Enum.map(entries, & &1.entry_key)

    with {:ok, current, _read_bytes} <- LMDB.get_many_bounded(path, stable_keys, max_bytes),
         :ok <- validate_current_entries(entries, current) do
      guarded_puts =
        entries
        |> Enum.zip(current)
        |> Enum.flat_map(fn {entry, stable_result} ->
          [
            {:compare, entry.catalog_key, entry.primary_value},
            stable_guard_op(entry.entry_key, stable_result),
            {:put, entry.entry_key, entry.state_key}
          ]
        end)

      tail =
        if exhausted do
          [
            {:put, @bootstrap_complete_key, @bootstrap_complete_value},
            {:delete, @bootstrap_progress_key}
          ]
        else
          [{:put, @bootstrap_progress_key, rows |> List.last() |> elem(0)}]
        end

      ops = guarded_puts ++ tail

      if operation_bytes(ops) <= max_bytes,
        do: write_batch.(path, ops),
        else: {:error, :query_source_catalog_page_too_large}
    else
      {:error, :batch_value_budget_exceeded} -> {:error, :query_source_catalog_page_too_large}
      {:error, _reason} = error -> error
    end
  end

  defp validate_current_entries(entries, current) when length(entries) == length(current) do
    Enum.zip(entries, current)
    |> Enum.reduce_while(:ok, fn
      {%{state_key: _state_key}, :not_found}, :ok -> {:cont, :ok}
      {%{state_key: state_key}, {:ok, state_key}}, :ok -> {:cont, :ok}
      _conflict, :ok -> {:halt, {:error, :query_source_catalog_conflict}}
    end)
  end

  defp validate_current_entries(_entries, _current),
    do: {:error, :invalid_query_source_catalog_read}

  defp operation_bytes(ops) do
    Enum.reduce(ops, 0, fn
      {:put, key, value}, total -> total + byte_size(key) + byte_size(value)
      {:compare, key, value}, total -> total + byte_size(key) + byte_size(value)
      {:compare_missing, key}, total -> total + byte_size(key)
      {:delete, key}, total -> total + byte_size(key)
    end)
  end

  defp decode_source_rows(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn
      {entry_key, state_key}, {:ok, acc} ->
        case decode_entry(entry_key, state_key) do
          {:ok, _catalog_key, ^state_key} -> {:cont, {:ok, [state_key | acc]}}
          {:error, _reason} -> {:halt, {:error, :corrupt_query_source_catalog_entry}}
        end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp stable_guard_op(entry_key, :not_found), do: {:compare_missing, entry_key}
  defp stable_guard_op(entry_key, {:ok, state_key}), do: {:compare, entry_key, state_key}

  defp validate_owner(catalog_key, state_key)
       when is_binary(catalog_key) and is_binary(state_key) and state_key != "" and
              byte_size(state_key) <= @max_state_key_bytes do
    if Keys.state_key?(state_key) and Keys.type_catalog_member_key?(catalog_key) and
         Keys.type_catalog_member_owns_state_key?(catalog_key, state_key),
       do: :ok,
       else: {:error, :invalid_query_source_catalog_entry}
  end

  defp validate_owner(_catalog_key, _state_key),
    do: {:error, :invalid_query_source_catalog_entry}

  defp validate_context(%{data_dir: data_dir, shard_count: shard_count}, shard_index)
       when is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and
              shard_count > 0 and is_integer(shard_index) and shard_index >= 0 and
              shard_index < shard_count,
       do: :ok

  defp validate_context(_ctx, _shard_index), do: {:error, :invalid_query_source_catalog_context}

  defp load_bootstrap_cursor(path) do
    case LMDB.get(path, @bootstrap_progress_key) do
      :not_found ->
        {:ok, ""}

      {:ok, cursor} ->
        if(validate_projection_cursor(cursor), do: {:ok, cursor}, else: cursor_error())

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_projection_cursor(cursor) when is_binary(cursor) do
    cursor != "" and byte_size(cursor) <= @max_lmdb_key_bytes and
      String.starts_with?(cursor, Keys.policy_catalog_projection_global_prefix())
  end

  defp validate_projection_cursor(_cursor), do: false
  defp cursor_error, do: {:error, :corrupt_query_source_catalog_progress}

  defp validate_cursor(""), do: :ok

  defp validate_cursor(cursor) when is_binary(cursor) do
    if byte_size(cursor) <= @max_lmdb_key_bytes and String.starts_with?(cursor, @entry_prefix),
      do: :ok,
      else: {:error, :invalid_query_source_catalog_cursor}
  end

  defp next_cursor([], cursor), do: cursor
  defp next_cursor(rows, _cursor), do: rows |> List.last() |> elem(0)

  defp bootstrap_complete?(path) do
    case LMDB.get(path, @bootstrap_complete_key) do
      :not_found -> {:ok, false}
      {:ok, @bootstrap_complete_value} -> {:ok, true}
      {:ok, _invalid} -> {:error, :corrupt_query_source_catalog_complete}
      {:error, _reason} = error -> error
    end
  end

  defp write_batch_fun(opts) do
    case Keyword.get(opts, :write_batch_fun, &LMDB.write_batch/2) do
      fun when is_function(fun, 2) -> {:ok, fun}
      _invalid -> {:error, :invalid_query_source_catalog_writer}
    end
  end

  defp entry_key(catalog_key), do: @entry_prefix <> catalog_key

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
