defmodule Ferricstore.Commands.Stream.AtomicGroup do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Stream.{CacheKey, Entries, Groups, ID, Index, Meta}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}

  @invalid_stream_id "ERR Invalid stream ID specified as stream command argument"

  @spec run(binary(), term(), map()) :: term()
  def run(key, {:create, group, id_str, mkstream}, store)
      when is_binary(group) and is_binary(id_str) and is_boolean(mkstream) do
    create(key, group, id_str, mkstream, store)
  end

  def run(key, {:read, group, consumer, count}, store)
      when is_binary(group) and is_binary(consumer) and
             (count == :infinity or (is_integer(count) and count >= 0)) do
    read_new(key, group, consumer, count, store)
  end

  def run(key, {:ack, group, ids}, store) when is_binary(group) and is_list(ids) do
    if binary_ids?(ids), do: ack(key, group, ids, store), else: {:error, @invalid_stream_id}
  end

  def run(_key, _operation, _store), do: {:error, "ERR invalid stream group operation"}

  defp create(key, group, id_str, mkstream, store) do
    with {:ok, marker_present?} <-
           TypeRegistry.check_type_status(key, :stream, CompoundKey.type_key(key), store),
         group_state <- Groups.serialized_state(store, key, group) do
      create_checked(key, group, id_str, mkstream, marker_present?, group_state, store)
    else
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      {:error, _reason} = error -> error
    end
  end

  defp create_checked(_key, _group, _id, _mkstream, _marker, {:v1, _, _, _}, _store),
    do: {:error, "BUSYGROUP Consumer Group name already exists"}

  defp create_checked(_key, _group, _id, _mkstream, _marker, {:v2, _}, _store),
    do: {:error, "BUSYGROUP Consumer Group name already exists"}

  defp create_checked(_key, _group, _id_str, false, false, :missing, _store) do
    {:error,
     "ERR The XGROUP subcommand requires the key to exist. " <>
       "Note that for CREATE you may want to use the MKSTREAM option to create " <>
       "an empty stream automatically."}
  end

  defp create_checked(key, group, id_str, mkstream, marker_present?, :missing, store) do
    with {:ok, meta, meta_entry} <- group_create_meta(key, marker_present?, mkstream, store),
         {:ok, last_delivered} <- group_start_id(id_str, meta),
         group_entry = Groups.serialized_v2_put_entry(key, group, last_delivered),
         put_entries = maybe_add_type_marker(key, marker_present?, [group_entry | meta_entry]),
         :ok <- Ops.compound_batch_put(store, key, put_entries),
         :ok <- maybe_defer_empty_meta(key, marker_present?, meta, store),
         :ok <- defer_group_update(key, group, last_delivered, %{}, %{}, store) do
      :ok
    end
  end

  defp create_checked(_key, _group, _id, _mkstream, _marker, {:error, reason}, _store),
    do: ReadResult.command_error(ReadResult.failure(reason))

  defp group_create_meta(key, true, _mkstream, store) do
    case Meta.serialized_entry(key, store) do
      {len, first, last, ms, seq} -> {:ok, {len, first, last, ms, seq}, []}
      {:error, _reason} = error -> error
    end
  end

  defp group_create_meta(key, false, true, _store) do
    meta = {0, "0-0", "0-0", 0, 0}
    {:ok, meta, [Meta.serialized_put_entry(key, 0, "0-0", "0-0", 0, 0)]}
  end

  defp group_create_meta(_key, false, false, _store), do: {:error, :stream_missing}

  defp group_start_id("$", {_len, _first, last, _ms, _seq}), do: {:ok, last}

  defp group_start_id(id_str, _meta) do
    case ID.parse_full_id(id_str) do
      {:ok, {ms, seq}} -> {:ok, "#{ms}-#{seq}"}
      {:error, _message} = error -> error
    end
  end

  defp maybe_add_type_marker(_key, true, entries), do: entries

  defp maybe_add_type_marker(key, false, entries),
    do: [{CompoundKey.type_key(key), "stream", 0} | entries]

  defp maybe_defer_empty_meta(_key, true, _meta, _store), do: :ok

  defp maybe_defer_empty_meta(key, false, {len, first, last, ms, seq}, store) do
    defer_stream_update(key, {:mutate, len, first, last, ms, seq, []}, store)
  end

  defp read_new(key, group, consumer, count, store) do
    case Groups.serialized_state(store, key, group) do
      :missing ->
        {:error, "NOGROUP No such consumer group '#{group}' for key name '#{key}'"}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error

      {:v1, last_delivered, consumers, pending} ->
        deliver_new(
          key,
          group,
          consumer,
          count,
          {:v1, last_delivered, consumers, pending},
          store
        )

      {:v2, last_delivered} ->
        deliver_new(key, group, consumer, count, {:v2, last_delivered}, store)
    end
  end

  defp deliver_new(key, group, consumer, count, state, store) do
    last_delivered = group_last_delivered(state)
    read_count = delivery_read_count(state, count, store)

    with {:ok, exclusive_start} <- ID.parse_exclusive_start(last_delivered),
         entries when is_list(entries) <-
           delivery_entries(key, exclusive_start, read_count, state, store) do
      persist_delivery(key, group, consumer, entries, state, store)
    else
      {:error, _reason} = error -> error
    end
  end

  defp group_last_delivered({:v1, last_delivered, _consumers, _pending}), do: last_delivered
  defp group_last_delivered({:v2, last_delivered}), do: last_delivered

  defp delivery_entries(_key, _start, 0, _state, _store), do: []

  defp delivery_entries(
         key,
         start,
         count,
         {:v2, _last_delivered},
         %{stream_range_pages: true} = store
       ) do
    Entries.range_page(store, key, start, :max, count)
  end

  defp delivery_entries(key, start, count, _state, store) do
    with :ok <- Index.ensure(key, store) do
      key
      |> Index.slice(start, :max, count, false, store)
      |> Entries.decode_indexed(key, store)
    end
  end

  defp delivery_read_count(
         {:v2, _last_delivered},
         count,
         %{compound_write_budget: budget}
       )
       when is_integer(budget) and budget > 0 do
    max_entries = max(budget - 2, 0)

    case count do
      :infinity -> max_entries
      requested when is_integer(requested) -> min(requested, max_entries)
    end
  end

  defp delivery_read_count(_state, count, _store), do: count

  defp persist_delivery(_key, _group, _consumer, [], _state, _store), do: nil

  defp persist_delivery(
         key,
         group,
         consumer,
         entries,
         {:v1, _old_last, consumers, pending},
         store
       ) do
    last_delivered = entries |> List.last() |> hd()
    now_ms = CommandTime.now_ms()

    new_pending =
      Enum.reduce(entries, pending, fn [id | _fields], acc ->
        Map.put(acc, id, {consumer, now_ms})
      end)

    new_consumers = Map.put(consumers, consumer, now_ms)
    entry = Groups.serialized_put_entry(key, group, last_delivered, new_consumers, new_pending)

    with :ok <- Ops.compound_batch_put(store, key, [entry]),
         :ok <-
           defer_group_update(
             key,
             group,
             last_delivered,
             new_consumers,
             new_pending,
             store
           ) do
      [key, entries]
    end
  end

  defp persist_delivery(key, group, consumer, entries, {:v2, _old_last}, store) do
    last_delivered = entries |> List.last() |> hd()
    now_ms = CommandTime.now_ms()

    put_entries =
      [Groups.serialized_v2_put_entry(key, group, last_delivered)] ++
        Groups.serialized_delivery_entries(key, group, consumer, entries, now_ms)

    with :ok <- Ops.compound_batch_put(store, key, put_entries),
         :ok <- defer_group_invalidation(key, group, store) do
      [key, entries]
    end
  end

  defp ack(key, group, ids, store) do
    case Groups.serialized_state(store, key, group) do
      :missing ->
        0

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error

      {:v1, last_delivered, consumers, pending} ->
        persist_ack(key, group, ids, last_delivered, consumers, pending, store)

      {:v2, _last_delivered} ->
        persist_split_ack(key, group, ids, store)
    end
  end

  defp persist_ack(key, group, ids, last_delivered, consumers, pending, store) do
    {new_pending, acked} =
      Enum.reduce(ids, {pending, 0}, fn id, {current, count} ->
        if Map.has_key?(current, id) do
          {Map.delete(current, id), count + 1}
        else
          {current, count}
        end
      end)

    if acked == 0 do
      0
    else
      entry = Groups.serialized_put_entry(key, group, last_delivered, consumers, new_pending)

      with :ok <- Ops.compound_batch_put(store, key, [entry]),
           :ok <-
             defer_group_update(
               key,
               group,
               last_delivered,
               consumers,
               new_pending,
               store
             ) do
        acked
      end
    end
  end

  defp persist_split_ack(key, group, ids, store) do
    case Groups.existing_pending_ids(store, key, group, ids) do
      {:ok, []} ->
        0

      {:ok, existing_ids} ->
        with :ok <-
               Ops.compound_batch_delete(
                 store,
                 key,
                 Groups.pending_keys(key, group, existing_ids)
               ),
             :ok <- defer_group_invalidation(key, group, store) do
          length(existing_ids)
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp defer_group_update(
         key,
         group,
         last_delivered,
         consumers,
         pending,
         %{defer_stream_group: defer} = store
       )
       when is_function(defer, 2) do
    defer.(
      CacheKey.build(store, key),
      {group, last_delivered, consumers, pending}
    )
  end

  defp defer_group_update(key, _group, _last, _consumers, _pending, store) do
    defer_stream_cleanup(key, store)
  end

  defp defer_group_invalidation(
         key,
         group,
         %{defer_stream_group: defer} = store
       )
       when is_function(defer, 2) do
    defer.(CacheKey.build(store, key), {:invalidate, group})
  end

  defp defer_group_invalidation(key, _group, store), do: defer_stream_cleanup(key, store)

  defp defer_stream_update(
         key,
         update,
         %{defer_stream_append: defer} = store
       )
       when is_function(defer, 2),
       do: defer.(CacheKey.build(store, key), update)

  defp defer_stream_update(key, _update, store), do: defer_stream_cleanup(key, store)

  defp defer_stream_cleanup(key, %{defer_stream_cleanup: defer} = store)
       when is_function(defer, 1),
       do: defer.(CacheKey.build(store, key))

  defp defer_stream_cleanup(_key, _store), do: :ok

  defp binary_ids?([]), do: true
  defp binary_ids?([id | ids]) when is_binary(id), do: binary_ids?(ids)
  defp binary_ids?(_invalid), do: false
end
