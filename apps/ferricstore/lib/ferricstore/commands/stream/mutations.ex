defmodule Ferricstore.Commands.Stream.Mutations do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, ID, Index, Meta, Tables}
  alias Ferricstore.Store.{Ops, ReadResult}

  @spec trim(binary(), term(), map()) :: non_neg_integer() | {:error, term()}
  def trim(key, trim_opts, store) do
    Tables.ensure_all()

    with :ok <- validate_trim_opts(trim_opts),
         :ok <- Meta.ensure_read_type(key, store) do
      case Meta.entries(key, store) do
        [] -> 0
        [{^key, _len, _first, _last, _ms, _seq}] -> apply_trim(key, trim_opts, store)
        {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
        {:error, _reason} = error -> error
      end
    end
  end

  @type prepared_trim ::
          :none
          | {:maxlen, non_neg_integer(), :indexed | {:scanned, [binary()]}}
          | {:minid, ID.stream_id(), :indexed | {:scanned, [binary()]}}

  @spec prepare_trim(binary(), binary(), term() | nil, map()) ::
          {:ok, prepared_trim()} | {:error, term()}
  def prepare_trim(_key, _new_id, nil, _store), do: {:ok, :none}

  def prepare_trim(key, new_id, {:maxlen, _approx, max_len}, store) do
    prepare_trim_source(key, new_id, {:maxlen, max_len}, store)
  end

  def prepare_trim(key, new_id, {:minid, _approx, min_id_str}, store) do
    with {:ok, min_id} <- ID.parse_full_id(min_id_str) do
      prepare_trim_source(key, new_id, {:minid, min_id}, store)
    end
  end

  @spec maybe_trim(binary(), prepared_trim(), map()) :: :ok | {:error, term()}
  def maybe_trim(_key, :none, _store), do: :ok

  def maybe_trim(key, {:maxlen, max_len, :indexed}, store) do
    key
    |> apply_trim_maxlen_indexed(max_len, store)
    |> normalize_trim_result()
  end

  def maybe_trim(key, {:maxlen, max_len, {:scanned, all_ids}}, store) do
    key
    |> apply_trim_maxlen_scanned(max_len, all_ids, store)
    |> normalize_trim_result()
  end

  def maybe_trim(key, {:minid, min_id, :indexed}, store) do
    key
    |> apply_trim_minid_indexed(min_id, store)
    |> normalize_trim_result()
  end

  def maybe_trim(key, {:minid, min_id, {:scanned, all_ids}}, store) do
    key
    |> apply_trim_minid_scanned(min_id, all_ids, store)
    |> normalize_trim_result()
  end

  @spec xdel(binary(), [binary()], map()) :: non_neg_integer() | {:error, term()}
  def xdel(key, ids, store) do
    Tables.ensure_all()

    unique_ids = Enum.uniq(ids)
    compound_keys = Entries.delete_keys(key, unique_ids)
    raw_values = Entries.batch_get(store, key, compound_keys)

    with nil <- ReadResult.first_failure(raw_values),
         :ok <- prepare_meta_for_mutation(key, store) do
      existing_ids = Entries.existing_ids(unique_ids, raw_values, [])

      delete_result =
        case delete_stream_ids(key, existing_ids, store) do
          {:ok, deleted} -> deleted
          {:error, reason, _deleted_count} -> {:error, reason}
        end

      case delete_result do
        {:error, _} = error ->
          error

        0 ->
          0

        deleted ->
          with :ok <- update_meta_after_xdel(key, deleted, store) do
            deleted
          end
      end
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp apply_trim(key, {:maxlen, _approx, max_len}, store) do
    if Ops.has_compound?(store) do
      apply_trim_maxlen_indexed(key, max_len, store)
    else
      apply_trim_maxlen_scanned(key, max_len, store)
    end
  end

  defp apply_trim(key, {:minid, _approx, min_id_str}, store) do
    case ID.parse_full_id(min_id_str) do
      {:error, _} = err -> err
      {:ok, min_id} -> apply_trim_minid(key, min_id, store)
    end
  end

  defp apply_trim_maxlen_scanned(key, max_len, store) do
    with {:ok, all_ids} <- sorted_ids(key, store) do
      apply_trim_maxlen_scanned(key, max_len, all_ids, store)
    end
  end

  defp apply_trim_maxlen_scanned(key, max_len, all_ids, store) do
    current_len = length(all_ids)

    if current_len > max_len do
      to_remove = Enum.take(all_ids, current_len - max_len)

      case delete_stream_ids(key, to_remove, store) do
        {:ok, deleted_count} ->
          remaining = Enum.drop(all_ids, deleted_count)

          with :ok <- update_meta_after_trim(key, remaining, store) do
            deleted_count
          end

        {:error, reason, deleted_count} ->
          reconcile_partial_trim(key, all_ids, deleted_count, store, reason)
      end
    else
      0
    end
  end

  defp apply_trim_minid(key, min_id, store) do
    if Ops.has_compound?(store) do
      apply_trim_minid_indexed(key, min_id, store)
    else
      apply_trim_minid_scanned(key, min_id, store)
    end
  end

  defp apply_trim_minid_scanned(key, min_id, store) do
    with {:ok, all_ids} <- sorted_ids(key, store) do
      apply_trim_minid_scanned(key, min_id, all_ids, store)
    end
  end

  defp apply_trim_minid_scanned(key, min_id, all_ids, store) do
    {to_remove, _keep} =
      Enum.split_while(all_ids, fn id_str ->
        ID.compare(ID.parse_id!(id_str), min_id) == :lt
      end)

    case delete_stream_ids(key, to_remove, store) do
      {:ok, 0} ->
        0

      {:ok, deleted_count} ->
        remaining = Enum.drop(all_ids, deleted_count)

        with :ok <- update_meta_after_trim(key, remaining, store) do
          deleted_count
        end

      {:error, reason, deleted_count} ->
        reconcile_partial_trim(key, all_ids, deleted_count, store, reason)
    end
  end

  defp apply_trim_maxlen_indexed(key, max_len, store) do
    with :ok <- Index.ensure(key, store) do
      case Meta.entries(key, store) do
        [{^key, len, _first, last, ms, seq}] when len > max_len ->
          delete_count = len - max_len
          ids_to_remove = Index.ids(key, delete_count, store)

          case delete_stream_ids(key, ids_to_remove, store) do
            {:ok, ^delete_count} ->
              with :ok <-
                     update_meta_after_index_mutation(key, max_len, last, ms, seq, store) do
                delete_count
              end

            {:error, reason, deleted_count} ->
              reconcile_partial_indexed_trim(
                key,
                len,
                deleted_count,
                last,
                ms,
                seq,
                store,
                reason
              )
          end

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        {:error, _reason} = error ->
          error

        _ ->
          0
      end
    end
  end

  defp apply_trim_minid_indexed(key, min_id, store) do
    with :ok <- Index.ensure(key, store) do
      case Meta.entries(key, store) do
        [{^key, len, _first, last, ms, seq}] ->
          to_remove =
            key
            |> Index.slice(:min, exclusive_upper_bound(min_id), :infinity, false, store)
            |> Enum.map(fn {id_str, _compound_key} -> id_str end)

          case delete_stream_ids(key, to_remove, store) do
            {:ok, 0} ->
              0

            {:ok, deleted_count} ->
              with :ok <-
                     update_meta_after_index_mutation(
                       key,
                       len - deleted_count,
                       last,
                       ms,
                       seq,
                       store
                     ) do
                deleted_count
              end

            {:error, reason, deleted_count} ->
              reconcile_partial_indexed_trim(
                key,
                len,
                deleted_count,
                last,
                ms,
                seq,
                store,
                reason
              )
          end

        [] ->
          0

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp delete_stream_ids(_key, [], _store), do: {:ok, 0}

  defp delete_stream_ids(key, ids, store) do
    compound_keys = Entries.delete_keys(key, ids)

    case Entries.delete(store, key, compound_keys) do
      :ok ->
        Index.delete_ids(key, ids, store)
        {:ok, length(compound_keys)}

      {:error, reason} ->
        {:error, reason, 0}
    end
  end

  defp update_meta_after_trim(key, [], store) do
    # Preserve metadata with length=0 instead of deleting, so that
    # the stream's last_id is kept for future XADD ordering.
    case Meta.entries(key, store) do
      [{^key, _len, _first, last, ms, seq}] ->
        Meta.put(key, 0, "0-0", last, ms, seq, store)

      [] ->
        case Meta.durable_entry(key, store) do
          {_, _, last, ms, seq} -> Meta.put(key, 0, "0-0", last, ms, seq, store)
          nil -> :ok
          {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
          {:error, _reason} = error -> error
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp update_meta_after_trim(key, remaining_ids, store) do
    first_str = List.first(remaining_ids)
    last_str = List.last(remaining_ids)
    {last_ms, last_seq} = ID.parse_id!(last_str)
    Meta.put(key, length(remaining_ids), first_str, last_str, last_ms, last_seq, store)
  end

  defp update_meta_after_index_mutation(key, remaining_len, old_last, old_ms, old_seq, store)
       when remaining_len <= 0 do
    if Ops.has_compound?(store) do
      Meta.put(key, 0, "0-0", old_last, old_ms, old_seq, store)
    else
      update_meta_after_trim(key, [], store)
    end
  end

  defp update_meta_after_index_mutation(key, remaining_len, _old_last, _old_ms, _old_seq, store) do
    if Ops.has_compound?(store) do
      case Index.first_last(key, store) do
        {first_str, last_str} ->
          {last_ms, last_seq} = ID.parse_id!(last_str)
          Meta.put(key, remaining_len, first_str, last_str, last_ms, last_seq, store)

        nil ->
          with {:ok, remaining_ids} <- sorted_ids(key, store) do
            update_meta_after_trim(key, remaining_ids, store)
          end
      end
    else
      with {:ok, remaining_ids} <- sorted_ids(key, store) do
        update_meta_after_trim(key, remaining_ids, store)
      end
    end
  end

  defp update_meta_after_xdel(key, deleted, store) do
    case Meta.entries(key, store) do
      [{^key, len, _first, last, ms, seq}] ->
        update_meta_after_index_mutation(key, max(len - deleted, 0), last, ms, seq, store)

      [] ->
        with {:ok, remaining_ids} <- sorted_ids(key, store) do
          update_meta_after_trim(key, remaining_ids, store)
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_meta_for_mutation(key, store) do
    case Meta.entries(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
      _entries -> :ok
    end
  end

  defp prepare_trim_source(key, new_id, operation, store) do
    if Ops.has_compound?(store) do
      with :ok <- Index.ensure(key, store) do
        {:ok, prepared_trim(operation, :indexed)}
      end
    else
      with {:ok, ids} <- sorted_ids(key, store) do
        {:ok, prepared_trim(operation, {:scanned, ids ++ [new_id]})}
      end
    end
  end

  defp prepared_trim({operation, threshold}, source), do: {operation, threshold, source}

  defp normalize_trim_result(result) when is_integer(result), do: :ok
  defp normalize_trim_result({:error, _reason} = error), do: error

  defp validate_trim_opts({:maxlen, approximate?, max_len})
       when is_boolean(approximate?) and is_integer(max_len) and max_len >= 0,
       do: :ok

  defp validate_trim_opts({:maxlen, _approximate?, _max_len}),
    do: {:error, "ERR value is not an integer or out of range"}

  defp validate_trim_opts({:minid, approximate?, min_id})
       when is_boolean(approximate?) and is_binary(min_id) do
    case ID.parse_full_id(min_id) do
      {:ok, _id} -> :ok
      {:error, _message} = error -> error
    end
  end

  defp validate_trim_opts(_trim_opts), do: {:error, "ERR syntax error"}

  defp reconcile_partial_trim(_key, _all_ids, 0, _store, reason), do: {:error, reason}

  defp reconcile_partial_trim(key, all_ids, deleted_count, store, reason) do
    remaining = Enum.drop(all_ids, deleted_count)
    merge_trim_errors(reason, update_meta_after_trim(key, remaining, store))
  end

  defp reconcile_partial_indexed_trim(
         _key,
         _len,
         0,
         _last,
         _ms,
         _seq,
         _store,
         reason
       ),
       do: {:error, reason}

  defp reconcile_partial_indexed_trim(
         key,
         len,
         deleted_count,
         last,
         ms,
         seq,
         store,
         reason
       ) do
    metadata_result =
      update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)

    merge_trim_errors(reason, metadata_result)
  end

  defp merge_trim_errors(reason, :ok), do: {:error, reason}

  defp merge_trim_errors(reason, {:error, metadata_reason}) do
    {:error, {:trim_delete_and_metadata_failed, reason, metadata_reason}}
  end

  defp sorted_ids(key, store) do
    case Entries.ids_for(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      ids when is_list(ids) ->
        {:ok, Enum.sort_by(ids, &ID.parse_id!/1)}
    end
  end

  defp exclusive_upper_bound({ms, seq}), do: {ms, seq - 1}
end
