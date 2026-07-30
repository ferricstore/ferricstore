defmodule Ferricstore.Commands.Stream.AtomicMutation do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, Entries, ID, Index, Meta}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @invalid_stream_id "ERR Invalid stream ID specified as stream command argument"

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
    if binary_ids?(ids) do
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
    else
      {:error, @invalid_stream_id}
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
      with {:ok, bounds} <- current_first_last_excluding(key, deleted, store) do
        {new_first, new_last} = bounds || {"0-0", "0-0"}

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

    with {:ok, delete_ids, first} <- current_maxlen_plan(key, delete_count, store) do
      retained_last = if max_len == 0, do: "0-0", else: last
      persist_known_bounds(key, current, delete_ids, max_len, first, retained_last, store)
    end
  end

  defp indexed_minid_trim(key, {len, _first, last, _ms, _seq} = current, min_id, store) do
    with {:ok, delete_ids, first} <- current_minid_plan(key, min_id, store) do
      case first do
        first when is_binary(first) ->
          persist_known_bounds(
            key,
            current,
            delete_ids,
            max(len - length(delete_ids), 0),
            first,
            last,
            store
          )

        nil ->
          persist_known_bounds(key, current, delete_ids, 0, "0-0", "0-0", store)
      end
    end
  end

  # An earlier Stream command in the same pending WAL batch is visible through
  # the pending keydir but must not be published into the shared derived index
  # before durability. In that uncommon dependent-command case, derive bounds
  # from the pending-aware store snapshot. Ordinary mutations retain the hot
  # ordered-index path.
  defp current_first_last_excluding(key, excluded, store) do
    case Index.pending_ids(key, store) do
      {:ok, ids} ->
        included = Enum.reject(ids, &MapSet.member?(excluded, &1))

        case included do
          [] -> {:ok, nil}
          _ -> {:ok, {hd(included), List.last(included)}}
        end

      :clean ->
        with :ok <- Index.ensure(key, store) do
          {:ok, Index.first_last_excluding(key, excluded, store)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp current_maxlen_plan(key, delete_count, store) do
    case Index.pending_ids(key, store) do
      {:ok, ids} ->
        indexed = Enum.take(ids, delete_count + 1)
        delete_ids = Enum.take(indexed, delete_count)
        first = Enum.at(indexed, delete_count) || "0-0"
        {:ok, delete_ids, first}

      :clean ->
        with :ok <- Index.ensure(key, store) do
          indexed = Index.slice(key, :min, :max, delete_count + 1, false, store)
          delete_ids = indexed |> Enum.take(delete_count) |> Enum.map(&elem(&1, 0))
          first = indexed |> Enum.at(delete_count) |> indexed_id_or_zero()
          {:ok, delete_ids, first}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp current_minid_plan(key, min_id, store) do
    case Index.pending_ids(key, store) do
      {:ok, ids} ->
        {deleted, retained} =
          Enum.split_while(ids, fn id_str -> ID.compare(ID.parse_id!(id_str), min_id) == :lt end)

        {:ok, deleted, List.first(retained)}

      :clean ->
        with :ok <- Index.ensure(key, store) do
          delete_ids = Index.ids_before(key, min_id, store)

          first =
            case Index.slice(key, min_id, :max, 1, false, store) do
              [{id, _compound_key}] -> id
              [] -> nil
            end

          {:ok, delete_ids, first}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp indexed_id_or_zero(nil), do: "0-0"
  defp indexed_id_or_zero({id, _compound_key}), do: id

  defp binary_ids?([]), do: true
  defp binary_ids?([id | ids]) when is_binary(id), do: binary_ids?(ids)
  defp binary_ids?(_invalid), do: false

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
