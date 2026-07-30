defmodule Ferricstore.Commands.Stream.AtomicMutation do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, Entries, ID, Index, Meta}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @spec run(binary(), {:trim, term()} | {:delete, [binary()]}, map()) :: term()
  def run(key, {operation, _arg1, _arg2, _arg3} = group_operation, store)
      when operation in [:create, :read] do
    Ferricstore.Commands.Stream.AtomicGroup.run(key, group_operation, store)
  end

  def run(key, {:ack, _group, _ids} = group_operation, store) do
    Ferricstore.Commands.Stream.AtomicGroup.run(key, group_operation, store)
  end

  def run(key, operation, store) do
    with {:ok, marker_present?} <-
           TypeRegistry.check_type_status(key, :stream, CompoundKey.type_key(key), store) do
      if marker_present? do
        mutate_existing(key, operation, store)
      else
        0
      end
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp mutate_existing(key, operation, store) do
    case Meta.serialized_entry(key, store) do
      {len, first, last, ms, seq} ->
        mutate(key, operation, {len, first, last, ms, seq}, store)

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp mutate(key, {:trim, {:maxlen, approximate?, max_len}}, current, store)
       when is_boolean(approximate?) and is_integer(max_len) and max_len >= 0 do
    {len, _first, _last, _ms, _seq} = current

    if len <= max_len do
      0
    else
      indexed_maxlen_trim(key, current, max_len, store)
    end
  end

  defp mutate(key, {:trim, {:minid, approximate?, min_id_str}}, current, store)
       when is_boolean(approximate?) and is_binary(min_id_str) do
    with {:ok, min_id} <- ID.parse_full_id(min_id_str) do
      trim_minid(key, current, min_id, store)
    end
  end

  defp mutate(_key, {:trim, _invalid}, _current, _store),
    do: {:error, "ERR syntax error"}

  defp mutate(key, {:delete, ids}, current, store) when is_list(ids) do
    unique_ids = Enum.uniq(ids)
    compound_keys = Entries.delete_keys(key, unique_ids)
    raw_values = Entries.batch_get(store, key, compound_keys)

    case ReadResult.first_failure(raw_values) do
      nil ->
        existing_ids = Entries.existing_ids(unique_ids, raw_values, [])
        delete_existing(key, current, existing_ids, store)

      failure ->
        ReadResult.command_error(failure)
    end
  end

  defp trim_minid(_key, {0, _first, _last, _ms, _seq}, _min_id, _store), do: 0

  defp trim_minid(key, {_len, first, _last, _ms, _seq} = current, min_id, store) do
    if ID.compare(ID.parse_id!(first), min_id) == :lt do
      indexed_minid_trim(key, current, min_id, store)
    else
      0
    end
  end

  defp delete_existing(_key, _current, [], _store), do: 0

  defp delete_existing(
         key,
         {len, first, last, _generated_ms, _generated_seq} = current,
         delete_ids,
         store
       ) do
    deleted = MapSet.new(delete_ids)

    if MapSet.member?(deleted, first) or MapSet.member?(deleted, last) do
      with :ok <- Index.ensure(key, store) do
        {new_first, new_last} = Index.first_last_excluding(key, deleted, store) || {"0-0", "0-0"}

        persist_known_bounds(
          key,
          current,
          delete_ids,
          max(len - length(delete_ids), 0),
          new_first,
          new_last,
          store
        )
      end
    else
      persist_known_bounds(
        key,
        current,
        delete_ids,
        max(len - length(delete_ids), 0),
        first,
        last,
        store
      )
    end
  end

  defp indexed_maxlen_trim(
         key,
         {len, _first, last, _generated_ms, _generated_seq} = current,
         max_len,
         store
       ) do
    delete_count = len - max_len

    with :ok <- Index.ensure(key, store) do
      indexed = Index.slice(key, :min, :max, delete_count + 1, false, store)
      delete_ids = indexed |> Enum.take(delete_count) |> Enum.map(&elem(&1, 0))

      first =
        case Enum.at(indexed, delete_count) do
          {id, _compound_key} -> id
          nil -> "0-0"
        end

      retained_last = if max_len == 0, do: "0-0", else: last
      persist_known_bounds(key, current, delete_ids, max_len, first, retained_last, store)
    end
  end

  defp indexed_minid_trim(key, {len, _first, last, _ms, _seq} = current, min_id, store) do
    with :ok <- Index.ensure(key, store) do
      delete_ids = Index.ids_before(key, min_id, store)

      case Index.slice(key, min_id, :max, 1, false, store) do
        [{first, _compound_key}] ->
          persist_known_bounds(
            key,
            current,
            delete_ids,
            max(len - length(delete_ids), 0),
            first,
            last,
            store
          )

        [] ->
          persist_known_bounds(key, current, delete_ids, 0, "0-0", "0-0", store)
      end
    end
  end

  defp persist_known_bounds(
         key,
         {_old_len, _old_first, _old_last, generated_ms, generated_seq},
         delete_ids,
         new_len,
         first,
         last,
         store
       ) do
    meta_entry =
      Meta.serialized_put_entry(key, new_len, first, last, generated_ms, generated_seq)

    with :ok <- Ops.compound_batch_delete(store, key, Entries.delete_keys(key, delete_ids)),
         :ok <- Ops.compound_batch_put(store, key, [meta_entry]),
         :ok <-
           defer_cache_update(
             key,
             {:mutate, new_len, first, last, generated_ms, generated_seq, delete_ids},
             store
           ) do
      length(delete_ids)
    end
  end

  defp defer_cache_update(key, update, %{defer_stream_append: defer} = store)
       when is_function(defer, 2) do
    defer.(CacheKey.build(store, key), update)
  end

  defp defer_cache_update(key, _update, %{defer_stream_cleanup: defer} = store)
       when is_function(defer, 1) do
    defer.(CacheKey.build(store, key))
  end

  defp defer_cache_update(_key, _update, _store), do: :ok
end
