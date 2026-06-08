defmodule Ferricstore.Commands.Stream.Mutations do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, ID, Index, Meta, Tables}
  alias Ferricstore.Store.Ops

  @meta_table Ferricstore.Stream.Meta

  @spec trim(binary(), term(), map()) :: non_neg_integer() | {:error, term()}
  def trim(key, trim_opts, store) do
    Tables.ensure_all()

    case :ets.lookup(@meta_table, key) do
      [] -> 0
      [{^key, _len, _first, _last, _ms, _seq}] -> apply_trim(key, trim_opts, store)
    end
  end

  @spec maybe_trim(binary(), term() | nil, map()) :: :ok
  def maybe_trim(_key, nil, _store), do: :ok

  def maybe_trim(key, trim_opts, store) do
    apply_trim(key, trim_opts, store)
    :ok
  end

  @spec xdel(binary(), [binary()], map()) :: non_neg_integer() | {:error, term()}
  def xdel(key, ids, store) do
    Tables.ensure_all()

    unique_ids = Enum.uniq(ids)
    compound_keys = Entries.delete_keys(key, unique_ids)
    raw_values = Entries.batch_get(store, key, compound_keys)

    existing_ids = Entries.existing_ids(unique_ids, raw_values, [])

    delete_result =
      case delete_stream_ids(key, existing_ids, store) do
        {:ok, deleted} -> deleted
        {:error, reason, _deleted_count} -> {:error, reason}
      end

    case delete_result do
      {:error, _} = error ->
        error

      deleted ->
        if deleted > 0 do
          update_meta_after_xdel(key, deleted, store)
        end

        deleted
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
    all_ids =
      store
      |> Entries.ids_for(key)
      |> Enum.sort_by(&ID.parse_id!/1)

    current_len = length(all_ids)

    if current_len > max_len do
      to_remove = Enum.take(all_ids, current_len - max_len)

      case delete_stream_ids(key, to_remove, store) do
        {:ok, deleted_count} ->
          remaining = Enum.drop(all_ids, deleted_count)
          update_meta_after_trim(key, remaining, store)
          deleted_count

        {:error, reason, deleted_count} ->
          if deleted_count > 0 do
            remaining = Enum.drop(all_ids, deleted_count)
            update_meta_after_trim(key, remaining, store)
          end

          {:error, reason}
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
    all_ids =
      store
      |> Entries.ids_for(key)
      |> Enum.sort_by(&ID.parse_id!/1)

    {to_remove, _keep} =
      Enum.split_with(all_ids, fn id_str ->
        ID.compare(ID.parse_id!(id_str), min_id) == :lt
      end)

    case delete_stream_ids(key, to_remove, store) do
      {:ok, deleted_count} ->
        if deleted_count > 0 do
          remaining = all_ids -- to_remove
          update_meta_after_trim(key, remaining, store)
        end

        deleted_count

      {:error, reason, deleted_count} ->
        if deleted_count > 0 do
          deleted_ids = Enum.take(to_remove, deleted_count)
          remaining = all_ids -- deleted_ids
          update_meta_after_trim(key, remaining, store)
        end

        {:error, reason}
    end
  end

  defp apply_trim_maxlen_indexed(key, max_len, store) do
    Index.ensure(key, store)

    case Meta.entries(key, store) do
      [{^key, len, _first, last, ms, seq}] when len > max_len ->
        delete_count = len - max_len
        ids_to_remove = Index.ids(key, delete_count)

        case delete_stream_ids(key, ids_to_remove, store) do
          {:ok, ^delete_count} ->
            update_meta_after_index_mutation(key, max_len, last, ms, seq, store)
            delete_count

          {:error, reason, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            {:error, reason}
        end

      _ ->
        0
    end
  end

  defp apply_trim_minid_indexed(key, min_id, store) do
    Index.ensure(key, store)

    case Meta.entries(key, store) do
      [{^key, len, _first, last, ms, seq}] ->
        to_remove =
          key
          |> Index.slice(:min, exclusive_upper_bound(min_id), :infinity, false)
          |> Enum.map(fn {id_str, _compound_key} -> id_str end)

        case delete_stream_ids(key, to_remove, store) do
          {:ok, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            deleted_count

          {:error, reason, deleted_count} ->
            if deleted_count > 0 do
              update_meta_after_index_mutation(key, len - deleted_count, last, ms, seq, store)
            end

            {:error, reason}
        end

      [] ->
        0
    end
  end

  defp delete_stream_ids(_key, [], _store), do: {:ok, 0}

  defp delete_stream_ids(key, ids, store) do
    compound_keys = Entries.delete_keys(key, ids)

    case Entries.delete(store, key, compound_keys) do
      :ok ->
        Index.delete_ids(key, ids)
        {:ok, length(compound_keys)}

      {:error, reason} ->
        {:error, reason, 0}
    end
  end

  defp update_meta_after_trim(key, [], store) do
    # Preserve metadata with length=0 instead of deleting, so that
    # the stream's last_id is kept for future XADD ordering.
    case :ets.lookup(@meta_table, key) do
      [{^key, _len, _first, last, ms, seq}] ->
        Meta.put(key, 0, "0-0", last, ms, seq, store)

      [] ->
        case Meta.durable_entry(key, store) do
          {_, _, last, ms, seq} -> Meta.put(key, 0, "0-0", last, ms, seq, store)
          nil -> :ok
        end
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
      case Index.first_last(key) do
        {first_str, last_str} ->
          {last_ms, last_seq} = ID.parse_id!(last_str)
          Meta.put(key, remaining_len, first_str, last_str, last_ms, last_seq, store)

        nil ->
          remaining_ids =
            store
            |> Entries.ids_for(key)
            |> Enum.sort_by(&ID.parse_id!/1)

          update_meta_after_trim(key, remaining_ids, store)
      end
    else
      remaining_ids =
        store
        |> Entries.ids_for(key)
        |> Enum.sort_by(&ID.parse_id!/1)

      update_meta_after_trim(key, remaining_ids, store)
    end
  end

  defp update_meta_after_xdel(key, deleted, store) do
    case :ets.lookup(@meta_table, key) do
      [{^key, len, _first, last, ms, seq}] ->
        update_meta_after_index_mutation(key, max(len - deleted, 0), last, ms, seq, store)

      [] ->
        remaining_ids =
          store
          |> Entries.ids_for(key)
          |> Enum.sort_by(&ID.parse_id!/1)

        update_meta_after_trim(key, remaining_ids, store)
    end
  end

  defp exclusive_upper_bound({ms, seq}), do: {ms, seq - 1}
end
