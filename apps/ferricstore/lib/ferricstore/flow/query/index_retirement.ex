defmodule Ferricstore.Flow.Query.IndexRetirement do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.{CompositeCounter, CompositeIndex, IndexDefinition}

  @max_items 16
  @max_bytes 16 * 1_024 * 1_024
  @max_u64 0xFFFF_FFFF_FFFF_FFFF
  @max_storage_key_bytes 511

  @type checkpoint :: %{
          phase: :index | :counter | :reverse,
          cursor: binary(),
          deleted_entries: non_neg_integer(),
          deleted_bytes: non_neg_integer(),
          rewritten_reverse_rows: non_neg_integer()
        }

  @spec empty_checkpoint() :: checkpoint()
  def empty_checkpoint do
    %{
      phase: :index,
      cursor: "",
      deleted_entries: 0,
      deleted_bytes: 0,
      rewritten_reverse_rows: 0
    }
  end

  @spec step(
          map(),
          non_neg_integer(),
          IndexDefinition.t(),
          checkpoint(),
          pos_integer(),
          pos_integer(),
          keyword()
        ) ::
          {:ok, checkpoint()}
          | {:complete, checkpoint()}
          | {:retry, :query_index_retirement_concurrent_change}
          | {:error, term()}
  def step(ctx, shard_index, definition, checkpoint, max_items, max_bytes, opts \\ []) do
    with :ok <-
           validate_request(
             ctx,
             shard_index,
             definition,
             checkpoint,
             max_items,
             max_bytes,
             opts
           ) do
      do_step(ctx, shard_index, definition, checkpoint, max_items, max_bytes, opts)
    end
  rescue
    _error -> {:error, :query_index_retirement_failed}
  catch
    :exit, _reason -> {:error, :query_index_retirement_dependency_unavailable}
    _kind, _reason -> {:error, :query_index_retirement_failed}
  end

  defp do_step(
         ctx,
         shard_index,
         definition,
         %{phase: :index} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    next_phase = if definition.count_prefixes == [], do: :reverse, else: :counter

    delete_prefix_page(
      ctx,
      shard_index,
      checkpoint,
      IndexDefinition.storage_prefix(definition),
      next_phase,
      max_items,
      max_bytes,
      opts
    )
  end

  defp do_step(
         ctx,
         shard_index,
         definition,
         %{phase: :counter} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    delete_prefix_page(
      ctx,
      shard_index,
      checkpoint,
      CompositeCounter.definition_storage_prefix(definition),
      :reverse,
      max_items,
      max_bytes,
      opts
    )
  end

  defp do_step(
         ctx,
         shard_index,
         definition,
         %{phase: :reverse} = checkpoint,
         max_items,
         max_bytes,
         opts
       ) do
    path = lmdb_path(ctx, shard_index)
    prefix = CompositeIndex.reverse_prefix()
    retired_prefix = IndexDefinition.storage_prefix(definition)
    range_entries = Keyword.get(opts, :range_entries_fun, &LMDB.range_entries_bounded/6)
    write_batch = Keyword.get(opts, :write_batch_fun, &LMDB.write_batch/2)

    with {:ok, rows, exhausted, read_bytes} <-
           range_entries.(path, prefix, checkpoint.cursor, "", max_items, max_bytes),
         :ok <-
           validate_page(
             rows,
             exhausted,
             read_bytes,
             prefix,
             checkpoint.cursor,
             max_items,
             max_bytes
           ),
         {:ok, ops, rewritten_rows} <- reverse_rewrite_ops(rows, retired_prefix),
         {:ok, total_rewritten_rows} <-
           checked_add(checkpoint.rewritten_reverse_rows, rewritten_rows),
         :ok <- write_reverse_ops(write_batch, path, ops) do
      next = %{
        checkpoint
        | cursor: if(exhausted, do: "", else: last_key(rows)),
          rewritten_reverse_rows: total_rewritten_rows
      }

      if exhausted, do: {:complete, next}, else: {:ok, next}
    else
      {:error, {:compare_failed, _key}} ->
        {:retry, :query_index_retirement_concurrent_change}

      {:error, _reason} = error ->
        error
    end
  end

  defp do_step(_ctx, _shard_index, _definition, _checkpoint, _items, _bytes, _opts),
    do: {:error, :invalid_query_index_retirement_checkpoint}

  defp delete_prefix_page(
         ctx,
         shard_index,
         checkpoint,
         prefix,
         next_phase,
         max_items,
         max_bytes,
         opts
       ) do
    path = lmdb_path(ctx, shard_index)
    range_entries = Keyword.get(opts, :range_entries_fun, &LMDB.range_entries_bounded/6)
    write_batch = Keyword.get(opts, :write_batch_fun, &LMDB.write_batch/2)

    with {:ok, rows, exhausted, read_bytes} <-
           range_entries.(path, prefix, checkpoint.cursor, "", max_items, max_bytes),
         :ok <-
           validate_page(
             rows,
             exhausted,
             read_bytes,
             prefix,
             checkpoint.cursor,
             max_items,
             max_bytes
           ),
         deleted_bytes <- rows_bytes(rows),
         {:ok, total_deleted_entries} <-
           checked_add(checkpoint.deleted_entries, length(rows)),
         {:ok, total_deleted_bytes} <- checked_add(checkpoint.deleted_bytes, deleted_bytes),
         :ok <- write_deletes(write_batch, path, rows) do
      next = %{
        checkpoint
        | phase: if(exhausted, do: next_phase, else: checkpoint.phase),
          cursor: if(exhausted, do: "", else: last_key(rows)),
          deleted_entries: total_deleted_entries,
          deleted_bytes: total_deleted_bytes
      }

      {:ok, next}
    end
  end

  defp write_deletes(_write_batch, _path, []), do: :ok

  defp write_deletes(write_batch, path, rows) do
    write_batch.(path, Enum.map(rows, fn {key, _value} -> {:delete, key} end))
  end

  defp reverse_rewrite_ops(rows, retired_prefix) do
    Enum.reduce_while(rows, {:ok, [], 0}, fn {key, blob}, {:ok, acc, rewritten} ->
      case CompositeIndex.decode_reverse_row(key, blob) do
        {:ok, {state_key, keys, expire_at_ms}} ->
          retained = Enum.reject(keys, &String.starts_with?(&1, retired_prefix))

          if retained == keys do
            {:cont, {:ok, acc, rewritten}}
          else
            rewrite =
              case retained do
                [] ->
                  [{:compare, key, blob}, {:delete, key}]

                retained ->
                  [
                    {:compare, key, blob},
                    {:put, key,
                     CompositeIndex.encode_reverse_value(state_key, retained, expire_at_ms)}
                  ]
              end

            {:cont, {:ok, :lists.reverse(rewrite, acc), rewritten + 1}}
          end

        :error ->
          {:halt, {:error, :invalid_composite_reverse}}
      end
    end)
    |> case do
      {:ok, reversed, rewritten} -> {:ok, Enum.reverse(reversed), rewritten}
      {:error, _reason} = error -> error
    end
  end

  defp write_reverse_ops(_write_batch, _path, []), do: :ok
  defp write_reverse_ops(write_batch, path, ops), do: write_batch.(path, ops)

  defp validate_request(
         %{data_dir: data_dir, shard_count: shard_count},
         shard_index,
         %IndexDefinition{} = definition,
         checkpoint,
         max_items,
         max_bytes,
         opts
       )
       when is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and shard_count > 0 and
              is_integer(shard_index) and shard_index >= 0 and shard_index < shard_count and
              is_integer(max_items) and max_items > 0 and max_items <= @max_items and
              is_integer(max_bytes) and max_bytes > 0 and max_bytes <= @max_bytes and
              is_list(opts) do
    if IndexDefinition.validate(definition) == :ok and valid_checkpoint?(checkpoint) and
         valid_functions?(opts),
       do: :ok,
       else: {:error, :invalid_query_index_retirement_request}
  end

  defp validate_request(_ctx, _shard, _definition, _checkpoint, _items, _bytes, _opts),
    do: {:error, :invalid_query_index_retirement_request}

  defp valid_checkpoint?(%{
         phase: phase,
         cursor: cursor,
         deleted_entries: deleted_entries,
         deleted_bytes: deleted_bytes,
         rewritten_reverse_rows: rewritten_reverse_rows
       }) do
    phase in [:index, :counter, :reverse] and is_binary(cursor) and
      byte_size(cursor) <= @max_storage_key_bytes and
      nonnegative_u64?(deleted_entries) and nonnegative_u64?(deleted_bytes) and
      nonnegative_u64?(rewritten_reverse_rows)
  end

  defp valid_checkpoint?(_checkpoint), do: false

  defp valid_functions?(opts) do
    function_opt?(opts, :range_entries_fun, 6) and function_opt?(opts, :write_batch_fun, 2)
  end

  defp function_opt?(opts, key, arity) do
    case Keyword.fetch(opts, key) do
      :error -> true
      {:ok, value} -> is_function(value, arity)
    end
  end

  defp validate_page(
         rows,
         exhausted,
         read_bytes,
         prefix,
         previous_cursor,
         max_items,
         max_bytes
       )
       when is_list(rows) and is_boolean(exhausted) and is_integer(read_bytes) and read_bytes >= 0 and
              is_binary(prefix) and is_binary(previous_cursor) do
    valid_rows? = valid_page_rows?(rows, prefix, previous_cursor)
    actual_bytes = if valid_rows?, do: rows_bytes(rows), else: 0

    cond do
      length(rows) > max_items ->
        {:error, :invalid_query_index_retirement_page}

      not exhausted and rows == [] ->
        {:error, :query_index_retirement_made_no_progress}

      not valid_rows? ->
        {:error, :invalid_query_index_retirement_page}

      read_bytes > max_bytes or actual_bytes > max_bytes ->
        {:error, :query_index_retirement_read_budget_exceeded}

      read_bytes != actual_bytes ->
        {:error, :invalid_query_index_retirement_page}

      true ->
        :ok
    end
  end

  defp validate_page(
         _rows,
         _exhausted,
         _read_bytes,
         _prefix,
         _previous_cursor,
         _items,
         _bytes
       ),
       do: {:error, :invalid_query_index_retirement_page}

  defp valid_page_rows?([], _prefix, _previous_cursor), do: true

  defp valid_page_rows?([{key, value} | rows], prefix, previous_cursor)
       when is_binary(key) and is_binary(value) do
    byte_size(key) <= @max_storage_key_bytes and String.starts_with?(key, prefix) and
      key > previous_cursor and
      valid_page_rows?(rows, prefix, key)
  end

  defp valid_page_rows?(_rows, _prefix, _previous_cursor), do: false

  defp rows_bytes(rows) do
    Enum.reduce(rows, 0, fn {key, value}, total ->
      total + byte_size(key) + byte_size(value)
    end)
  end

  defp last_key([]), do: ""
  defp last_key(rows), do: rows |> List.last() |> elem(0)

  defp checked_add(left, right)
       when is_integer(left) and left >= 0 and left <= @max_u64 and is_integer(right) and
              right >= 0 and right <= @max_u64 and left <= @max_u64 - right,
       do: {:ok, left + right}

  defp checked_add(_left, _right), do: {:error, :query_index_retirement_counter_overflow}

  defp nonnegative_u64?(value),
    do: is_integer(value) and value >= 0 and value <= @max_u64

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
