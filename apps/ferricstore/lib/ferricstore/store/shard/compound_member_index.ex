defmodule Ferricstore.Store.Shard.CompoundMemberIndex do
  @moduledoc false

  alias Ferricstore.ExpiryContext
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @separator <<0>>
  @ready_key :"$ferricstore_compound_member_index_ready"
  @count_tag :"$ferricstore_compound_member_count"
  @expiry_tag :"$ferricstore_compound_member_expiry"
  @member_expiry_tag :"$ferricstore_compound_member_expiry_lookup"
  @stream_select_min_count 128
  @stream_select_batch_max 4096

  @waraft_location_tags [:waraft_segment, :waraft_projection, :waraft_apply_projection]

  @type table_ref :: atom() | :ets.tid() | nil
  @type scan_cursor ::
          0 | {:after, binary() | {non_neg_integer(), non_neg_integer()}}

  defguardp valid_scan_cursor(cursor)
            when cursor == 0 or
                   (is_tuple(cursor) and tuple_size(cursor) == 2 and
                      elem(cursor, 0) == :after and
                      (is_binary(elem(cursor, 1)) or
                         (is_tuple(elem(cursor, 1)) and tuple_size(elem(cursor, 1)) == 2 and
                            is_integer(elem(elem(cursor, 1), 0)) and
                            elem(elem(cursor, 1), 0) >= 0 and
                            is_integer(elem(elem(cursor, 1), 1)) and
                            elem(elem(cursor, 1), 1) >= 0)))

  @spec table_name(atom() | binary(), non_neg_integer()) :: atom()
  def table_name(instance_name, shard_index),
    do: :"ferricstore_compound_member_index_#{instance_name}_#{shard_index}"

  @spec ensure_table!(atom()) :: atom()
  def ensure_table!(table_name) when is_atom(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [
          :ordered_set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      _tid ->
        table_name
    end
  end

  @spec reset(table_ref()) :: :ok
  def reset(nil), do: :ok

  def reset(table) do
    case table_ref(table) do
      :undefined ->
        :ok

      tid ->
        :ets.delete_all_objects(tid)
        :ets.insert(tid, {@ready_key, true})
        :ok
    end
  end

  @spec rebuild(table_ref(), :ets.tid() | atom()) :: :ok
  def rebuild(table, keydir) do
    case table_ref(table) do
      :undefined ->
        :ok

      index ->
        :ets.delete_all_objects(index)

        now =
          ExpiryContext.capture()
          |> ExpiryContext.safe_expiry_cutoff_ms()

        :ets.foldl(
          fn
            {key, value, expire_at_ms, _lfu, file_id, offset, value_size}, :ok
            when is_binary(key) ->
              if live_keydir_entry?(
                   value,
                   expire_at_ms,
                   file_id,
                   offset,
                   value_size,
                   now
                 ) do
                put(index, key, expire_at_ms)
              end

              :ok

            _row, :ok ->
              :ok
          end,
          :ok,
          keydir
        )

        :ets.insert(index, {@ready_key, true})
        :ok
    end
  end

  @doc false
  @spec ready?(table_ref()) :: boolean()
  def ready?(table) do
    ready_table_ref(table) != :undefined
  rescue
    ArgumentError -> false
  end

  @doc false
  @spec supports_prefix?(term()) :: boolean()
  def supports_prefix?(<<"H:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"L:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"S:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"Z:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"X:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"XG:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"XP:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"XC:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(<<"XM:", _rest::binary>> = prefix), do: member_prefix?(prefix)
  def supports_prefix?(_prefix), do: false

  @spec put(table_ref(), binary()) :: :ok
  def put(table, compound_key), do: put(table, compound_key, 0)

  @spec put(table_ref(), binary(), non_neg_integer()) :: :ok
  def put(nil, _compound_key, _expire_at_ms), do: :ok

  def put(table, <<"H:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"L:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"S:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"Z:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"XG:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"XP:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"XC:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"XM:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(table, <<"X:", _rest::binary>> = compound_key, expire_at_ms),
    do: put_compound_member(table, compound_key, expire_at_ms)

  def put(_table, _compound_key, _expire_at_ms), do: :ok

  @spec put_many(table_ref(), [{binary(), non_neg_integer()}]) :: :ok
  def put_many(nil, _entries), do: :ok
  def put_many(_table, []), do: :ok

  def put_many(table, entries) when is_list(entries) do
    case writable_table_ref(table) do
      :undefined ->
        :ok

      tid ->
        {stream_groups, remaining} = partition_new_stream_members(entries)

        Enum.each(stream_groups, fn {prefix, reversed_objects} ->
          insert_new_stream_objects(tid, prefix, Enum.reverse(reversed_objects))
        end)

        remaining
        |> Enum.reverse()
        |> Enum.each(fn {compound_key, expire_at_ms} ->
          put_compound_member_resolved(tid, compound_key, expire_at_ms)
        end)
    end

    :ok
  end

  @doc false
  @spec put_many(table_ref(), [{binary(), non_neg_integer()}], binary()) :: :ok
  def put_many(nil, _entries, _stream_prefix), do: :ok
  def put_many(_table, [], _stream_prefix), do: :ok

  def put_many(table, entries, <<"X:", _rest::binary>> = stream_prefix)
      when is_list(entries) do
    case writable_table_ref(table) do
      :undefined ->
        :ok

      tid ->
        {stream_objects, remaining} =
          partition_known_stream_members(entries, stream_prefix, [], [])

        insert_new_stream_objects(tid, stream_prefix, Enum.reverse(stream_objects))

        remaining
        |> Enum.reverse()
        |> Enum.each(fn {compound_key, expire_at_ms} ->
          put_compound_member_resolved(tid, compound_key, expire_at_ms)
        end)
    end

    :ok
  end

  def put_many(table, entries, _stream_prefix), do: put_many(table, entries)

  @doc false
  @spec put_many(
          table_ref(),
          [{binary(), non_neg_integer()}],
          binary(),
          [{non_neg_integer(), non_neg_integer(), binary()}] | nil
        ) :: :ok
  def put_many(nil, _entries, _stream_prefix, _parsed_ids), do: :ok
  def put_many(_table, [], _stream_prefix, _parsed_ids), do: :ok

  def put_many(
        table,
        entries,
        <<"X:", _rest::binary>> = stream_prefix,
        parsed_ids
      )
      when is_list(entries) and is_list(parsed_ids) do
    case writable_table_ref(table) do
      :undefined ->
        :ok

      tid ->
        case partition_known_stream_members(
               entries,
               stream_prefix,
               parsed_ids,
               [],
               []
             ) do
          {:ok, stream_objects, remaining} ->
            insert_new_stream_objects(tid, stream_prefix, Enum.reverse(stream_objects))

            remaining
            |> Enum.reverse()
            |> Enum.each(fn {compound_key, expire_at_ms} ->
              put_compound_member_resolved(tid, compound_key, expire_at_ms)
            end)

          :fallback ->
            put_many(tid, entries, stream_prefix)
        end
    end

    :ok
  end

  def put_many(table, entries, stream_prefix, _parsed_ids),
    do: put_many(table, entries, stream_prefix)

  @doc """
  Publishes the already-planned member rows for a committed Stream append.

  Auto-generated Stream IDs are unique and `member_count` comes from the
  committed metadata row, so both writes are idempotent during recovery. This
  avoids an `insert_new` membership probe for every append while retaining an
  exact prefix count.
  """
  @spec publish_stream_append(
          table_ref(),
          binary(),
          [
            {{binary(), {non_neg_integer(), non_neg_integer()}}, binary()}
          ],
          non_neg_integer()
        ) :: :ok
  def publish_stream_append(nil, _stream_prefix, _objects, _member_count), do: :ok

  def publish_stream_append(
        table,
        <<"X:", _rest::binary>> = stream_prefix,
        objects,
        member_count
      )
      when is_list(objects) and is_integer(member_count) and member_count >= 0 do
    case writable_table_ref(table) do
      :undefined ->
        :ok

      tid ->
        :ets.insert(tid, [{{@count_tag, stream_prefix}, member_count} | objects])
    end

    :ok
  end

  def publish_stream_append(_table, _stream_prefix, _objects, _member_count), do: :ok

  @doc """
  Publishes planned member rows and exact counts for several committed Streams
  in one ETS operation.
  """
  @spec publish_stream_appends(
          table_ref(),
          [{{binary(), {non_neg_integer(), non_neg_integer()}}, binary()}],
          [{binary(), non_neg_integer()}]
        ) :: :ok
  def publish_stream_appends(nil, _objects, _stream_counts), do: :ok

  def publish_stream_appends(table, objects, stream_counts)
      when is_list(objects) and is_list(stream_counts) do
    with tid when tid != :undefined <- writable_table_ref(table),
         {:ok, rows} <- prepend_stream_count_rows(stream_counts, objects) do
      if rows != [], do: :ets.insert(tid, rows)
    end

    :ok
  end

  def publish_stream_appends(_table, _objects, _stream_counts), do: :ok

  defp prepend_stream_count_rows([], objects), do: {:ok, objects}

  defp prepend_stream_count_rows(
         [{<<"X:", _rest::binary>> = prefix, count} | stream_counts],
         objects
       )
       when is_integer(count) and count >= 0 do
    prepend_stream_count_rows(stream_counts, [{{@count_tag, prefix}, count} | objects])
  end

  defp prepend_stream_count_rows(_invalid, _objects), do: :error

  defp partition_known_stream_members([], _prefix, [], objects, remaining),
    do: {:ok, objects, remaining}

  defp partition_known_stream_members([], _prefix, _unconsumed_ids, _objects, _remaining),
    do: :fallback

  defp partition_known_stream_members(
         [{compound_key, 0} = entry | rest],
         prefix,
         parsed_ids,
         objects,
         remaining
       )
       when is_binary(compound_key) do
    prefix_size = byte_size(prefix)

    if byte_size(compound_key) > prefix_size and
         binary_part(compound_key, 0, prefix_size) == prefix do
      member = binary_part(compound_key, prefix_size, byte_size(compound_key) - prefix_size)

      case parsed_ids do
        [{ms, seq, ^member} | remaining_ids]
        when is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 ->
          object = {{prefix, {ms, seq}}, compound_key}

          partition_known_stream_members(
            rest,
            prefix,
            remaining_ids,
            [object | objects],
            remaining
          )

        _mismatch ->
          :fallback
      end
    else
      partition_known_stream_members(rest, prefix, parsed_ids, objects, [entry | remaining])
    end
  end

  defp partition_known_stream_members(
         [entry | rest],
         prefix,
         parsed_ids,
         objects,
         remaining
       ) do
    partition_known_stream_members(rest, prefix, parsed_ids, objects, [entry | remaining])
  end

  defp partition_known_stream_members([], _prefix, objects, remaining),
    do: {objects, remaining}

  defp partition_known_stream_members(
         [{compound_key, 0} = entry | rest],
         prefix,
         objects,
         remaining
       )
       when is_binary(compound_key) do
    prefix_size = byte_size(prefix)

    if byte_size(compound_key) > prefix_size and
         binary_part(compound_key, 0, prefix_size) == prefix do
      member = binary_part(compound_key, prefix_size, byte_size(compound_key) - prefix_size)
      object = {{prefix, index_member(prefix, member)}, compound_key}
      partition_known_stream_members(rest, prefix, [object | objects], remaining)
    else
      partition_known_stream_members(rest, prefix, objects, [entry | remaining])
    end
  end

  defp partition_known_stream_members([entry | rest], prefix, objects, remaining) do
    partition_known_stream_members(rest, prefix, objects, [entry | remaining])
  end

  defp insert_new_stream_objects(_tid, _prefix, []), do: :ok

  defp insert_new_stream_objects(tid, prefix, objects) do
    if :ets.insert_new(tid, objects) do
      increment_prefix_count(tid, prefix, length(objects))
    else
      Enum.each(objects, fn {{_prefix, _member}, compound_key} ->
        put_compound_member_resolved(tid, compound_key, 0)
      end)
    end

    :ok
  end

  defp partition_new_stream_members(entries) do
    Enum.reduce(entries, {%{}, []}, fn
      {<<"X:", _rest::binary>> = compound_key, 0} = entry, {groups, remaining} ->
        case split_at_separator(compound_key) do
          {:ok, prefix, member} ->
            object = {{prefix, index_member(prefix, member)}, compound_key}
            {Map.update(groups, prefix, [object], &[object | &1]), remaining}

          :ignore ->
            {groups, [entry | remaining]}
        end

      {compound_key, expire_at_ms} = entry, {groups, remaining}
      when is_binary(compound_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        {groups, [entry | remaining]}

      _invalid, acc ->
        acc
    end)
  end

  defp put_compound_member(table, compound_key, expire_at_ms)
       when is_integer(expire_at_ms) and expire_at_ms >= 0 do
    with tid when tid != :undefined <- writable_table_ref(table) do
      put_compound_member_resolved(tid, compound_key, expire_at_ms)
    end

    :ok
  end

  defp put_compound_member(_table, _compound_key, _expire_at_ms), do: :ok

  defp put_compound_member_resolved(tid, compound_key, expire_at_ms) do
    with {:ok, prefix, member} <- split_at_separator(compound_key) do
      index_key = {prefix, index_member(prefix, member)}
      new_member? = :ets.insert_new(tid, {index_key, compound_key})

      unless new_member? do
        :ets.insert(tid, {index_key, compound_key})
      end

      replace_member_expiry(tid, prefix, compound_key, expire_at_ms)

      if new_member? do
        increment_prefix_count(tid, prefix, 1)
      end
    end

    :ok
  end

  @spec delete(table_ref(), binary()) :: :ok
  def delete(nil, _compound_key), do: :ok

  def delete(table, <<"H:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"L:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"S:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"Z:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XG:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XP:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XC:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"XM:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(table, <<"X:", _rest::binary>> = compound_key),
    do: delete_compound_member(table, compound_key)

  def delete(_table, _compound_key), do: :ok

  defp delete_compound_member(table, compound_key) do
    with tid when tid != :undefined <- table_ref(table),
         {:ok, prefix, member} <- split_at_separator(compound_key) do
      index_member = index_member(prefix, member)

      case :ets.take(tid, {prefix, index_member}) do
        [{{^prefix, ^index_member}, _compound_key}] ->
          delete_member_expiry(tid, prefix, compound_key)
          increment_prefix_count(tid, prefix, -1)

        [] ->
          delete_member_expiry(tid, prefix, compound_key)
      end
    end

    :ok
  end

  @spec delete_prefix(table_ref(), binary()) :: :ok
  def delete_prefix(nil, _prefix), do: :ok

  def delete_prefix(table, prefix) when is_binary(prefix) do
    with tid when tid != :undefined <- table_ref(table) do
      delete_index_prefix(tid, prefix, first_key(tid, prefix))
      delete_expiry_prefix(tid, prefix, first_expiry_key(tid, prefix))
      :ets.delete(tid, {@count_tag, prefix})
    end

    :ok
  end

  def delete_prefix(_table, _prefix), do: :ok

  @spec any_live?(table_ref(), map(), binary()) :: boolean() | :unavailable
  def any_live?(table, state, prefix) when is_binary(prefix) do
    any_live?(table, state, prefix, %{})
  end

  def any_live?(_table, _state, _prefix), do: :unavailable

  @spec any_live?(table_ref(), map(), binary(), map() | MapSet.t()) :: boolean() | :unavailable
  def any_live?(table, state, prefix, ignored_keys) when is_binary(prefix) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        do_any_live?(
          tid,
          lookup_state(state),
          prefix,
          first_key(tid, prefix),
          ignored_keys,
          ExpiryContext.capture()
        )
    end
  end

  def any_live?(_table, _state, _prefix, _ignored_keys), do: :unavailable

  @spec count_live(table_ref(), map(), binary()) ::
          {:ok, non_neg_integer()} | {:error, term()} | :unavailable
  def count_live(table, state, prefix) when is_binary(prefix) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        do_count_live(
          tid,
          lookup_state(state),
          prefix,
          first_key(tid, prefix),
          0,
          ExpiryContext.capture()
        )
    end
  rescue
    ArgumentError -> :unavailable
  end

  def count_live(_table, _state, _prefix), do: :unavailable

  @spec count_live_indexed(table_ref(), map(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer(), non_neg_integer()}
          | {:error, :limit_exceeded | term()}
          | :unavailable
  def count_live_indexed(table, state, prefix, cleanup_budget)
      when is_binary(prefix) and is_integer(cleanup_budget) and cleanup_budget >= 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        expiry_context = ExpiryContext.capture()

        case cleanup_due_expiries(
               tid,
               lookup_state(state),
               prefix,
               first_expiry_key(tid, prefix),
               cleanup_budget,
               0,
               expiry_context
             ) do
          {:ok, inspected} -> {:ok, prefix_count(tid, prefix), inspected}
          {:error, _reason} = error -> error
        end
    end
  rescue
    ArgumentError -> :unavailable
  end

  def count_live_indexed(_table, _state, _prefix, _cleanup_budget), do: :unavailable

  @spec scan_entries(table_ref(), map(), binary()) :: {:ok, [{binary(), binary()}]} | :unavailable
  def scan_entries(table, state, prefix) when is_binary(prefix) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        lookup_state = lookup_state(state)
        expiry_context = ExpiryContext.capture()

        entries =
          tid
          |> scan_keys(prefix)
          |> Enum.reduce_while([], fn compound_key, acc ->
            case ShardETS.ets_lookup(lookup_state, compound_key, expiry_context) do
              {:hit, value, _expire_at_ms} when is_binary(value) ->
                {:cont, [{member_from_key(compound_key, prefix), value} | acc]}

              {:hit, value, _expire_at_ms} ->
                {:cont, [{member_from_key(compound_key, prefix), to_string(value)} | acc]}

              {:cold, _fid, _off, _vsize, _expire_at_ms} ->
                {:halt, :unavailable}

              {:error, :invalid_keydir_entry} ->
                {:halt, :unavailable}

              {:error, {:storage_read_failed, _reason}} ->
                {:halt, :unavailable}

              :expired ->
                delete_stale_member(tid, lookup_state, compound_key)
                {:cont, acc}

              :miss ->
                delete_stale_member(tid, lookup_state, compound_key)
                {:cont, acc}
            end
          end)

        case entries do
          :unavailable -> :unavailable
          entries -> {:ok, Enum.reverse(entries)}
        end
    end
  end

  def scan_entries(_table, _state, _prefix), do: :unavailable

  @doc false
  @spec keys_for_prefix(table_ref(), binary()) :: {:ok, [binary()]} | :unavailable
  def keys_for_prefix(table, prefix) when is_binary(prefix) do
    case ready_table_ref(table) do
      :undefined -> :unavailable
      tid -> {:ok, scan_keys(tid, prefix)}
    end
  rescue
    ArgumentError -> :unavailable
  end

  def keys_for_prefix(_table, _prefix), do: :unavailable

  @doc false
  @spec keys_for_prefix(table_ref(), binary(), non_neg_integer()) ::
          {:ok, [binary()]} | {:error, :limit_exceeded} | :unavailable
  def keys_for_prefix(table, prefix, limit)
      when is_binary(prefix) and is_integer(limit) and limit >= 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        do_scan_keys_bounded(tid, prefix, first_key(tid, prefix), limit, [])
    end
  rescue
    ArgumentError -> :unavailable
  end

  def keys_for_prefix(_table, _prefix, _limit), do: :unavailable

  @spec live_keys_for_prefix(table_ref(), map(), binary(), non_neg_integer()) ::
          {:ok, [binary()], non_neg_integer(), :complete | :more}
          | {:error, term()}
          | :unavailable
  def live_keys_for_prefix(table, state, prefix, limit)
      when is_binary(prefix) and is_integer(limit) and limit >= 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        do_live_keys_for_prefix(
          tid,
          lookup_state(state),
          prefix,
          first_key(tid, prefix),
          limit,
          [],
          0,
          ExpiryContext.capture()
        )
    end
  rescue
    ArgumentError -> :unavailable
  end

  def live_keys_for_prefix(_table, _state, _prefix, _limit), do: :unavailable

  @doc false
  @spec scan_rows(table_ref(), map(), binary()) ::
          {:ok, [tuple()]} | {:error, term()} | :unavailable
  def scan_rows(table, state, prefix) when is_binary(prefix) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        case reduce_rows_while(tid, state, prefix, [], fn row, acc -> {:cont, [row | acc]} end) do
          {:ok, rows} -> {:ok, Enum.reverse(rows)}
          other -> other
        end
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def scan_rows(_table, _state, _prefix), do: :unavailable

  @doc false
  @spec reduce_rows_while(table_ref(), map(), binary(), term(), function()) ::
          {:ok, term()} | {:halt, term()} | {:error, term()} | :unavailable
  def reduce_rows_while(table, state, prefix, acc, reducer)
      when is_binary(prefix) and is_function(reducer, 2) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        keydir = state |> lookup_state() |> Map.fetch!(:keydir)
        do_reduce_rows_while(tid, keydir, prefix, first_key(tid, prefix), acc, reducer)
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def reduce_rows_while(_table, _state, _prefix, _acc, _reducer), do: :unavailable

  @spec member_slice(
          table_ref(),
          map(),
          binary(),
          binary(),
          non_neg_integer(),
          ExpiryContext.t(),
          map() | MapSet.t()
        ) :: {:ok, [binary()]} | {:error, term()} | :unavailable
  def member_slice(_table, _state, _prefix, _start_member, 0, _expiry_context, _pending_values),
    do: {:ok, []}

  def member_slice(table, state, prefix, start_member, count, expiry_context, pending_values)
      when is_binary(prefix) and is_binary(start_member) and is_integer(count) and count > 0 and
             is_tuple(expiry_context) do
    expiry_context = ExpiryContext.normalize(expiry_context)

    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        start_key = {prefix, index_member(prefix, start_member)}
        first = first_key_at_or_after(tid, prefix, start_member)

        with {:ok, acc, remaining} <-
               collect_member_slice(
                 tid,
                 state,
                 prefix,
                 first,
                 count,
                 expiry_context,
                 pending_values,
                 :to_end,
                 []
               ),
             {:ok, acc, _remaining} <-
               collect_member_slice(
                 tid,
                 state,
                 prefix,
                 first_key(tid, prefix),
                 remaining,
                 expiry_context,
                 pending_values,
                 {:before, start_key},
                 acc
               ) do
          {:ok, Enum.reverse(acc)}
        end
    end
  rescue
    ArgumentError -> :unavailable
  end

  def member_slice(_table, _state, _prefix, _start_member, _count, _now_ms, _pending_values),
    do: {:error, :invalid_member_slice}

  @doc false
  @spec row_slice(
          table_ref(),
          map(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: {:ok, [tuple()]} | {:error, term()} | :unavailable
  def row_slice(_table, _state, _prefix, _start, 0, _total), do: {:ok, []}

  def row_slice(table, state, prefix, start, count, total)
      when is_binary(prefix) and is_integer(start) and start >= 0 and is_integer(count) and
             count > 0 and is_integer(total) and total >= 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        requested = min(count, max(total - start, 0))

        if requested == 0 do
          {:ok, []}
        else
          keydir = state |> lookup_state() |> Map.fetch!(:keydir)
          expiry_context = ExpiryContext.capture()
          tail = max(total - start - requested, 0)

          if start <= tail do
            case collect_row_slice(
                   tid,
                   keydir,
                   prefix,
                   first_key(tid, prefix),
                   :forward,
                   start,
                   requested,
                   expiry_context,
                   []
                 ) do
              {:ok, rows} -> {:ok, Enum.reverse(rows)}
              other -> other
            end
          else
            collect_row_slice(
              tid,
              keydir,
              prefix,
              last_key(tid, prefix),
              :backward,
              tail,
              requested,
              expiry_context,
              []
            )
          end
        end
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def row_slice(_table, _state, _prefix, _start, _count, _total),
    do: {:error, :invalid_row_slice}

  @doc false
  @spec hot_stream_slice(
          table_ref(),
          atom() | reference(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) ::
          {:ok, [{binary(), binary()}]}
          | :fallback
          | :unavailable
  def hot_stream_slice(
        table,
        keydir,
        <<"X:", _rest::binary>> = prefix,
        start,
        count,
        total
      )
      when is_integer(start) and start >= 0 and is_integer(count) and count >= 0 and
             is_integer(total) and total >= 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      _tid when count == 0 ->
        {:ok, []}

      tid ->
        requested = min(count, max(total - start, 0))

        if requested == 0 do
          {:ok, []}
        else
          tail = max(total - start - requested, 0)

          cond do
            requested >= @stream_select_min_count and start == 0 ->
              select_hot_stream_page_once(tid, keydir, prefix, 0, requested)

            requested >= @stream_select_min_count and tail == 0 ->
              select_hot_stream_reverse_tail(tid, keydir, prefix, requested)

            start <= tail ->
              case collect_hot_stream_slice(
                     tid,
                     keydir,
                     prefix,
                     :ets.next_lookup(tid, {prefix, {-1, -1}}),
                     :forward,
                     start,
                     requested,
                     []
                   ) do
                {:ok, entries} ->
                  {:ok, Enum.reverse(entries)}

                :fallback ->
                  :fallback
              end

            true ->
              case collect_hot_stream_slice(
                     tid,
                     keydir,
                     prefix,
                     :ets.prev_lookup(tid, {prefix <> <<0>>, <<>>}),
                     :backward,
                     tail,
                     requested,
                     []
                   ) do
                {:ok, entries} -> {:ok, entries}
                :fallback -> :fallback
              end
          end
        end
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def hot_stream_slice(_table, _state, _prefix, _start, _count, _total), do: :fallback

  @doc false
  @spec scan_page(
          table_ref(),
          map(),
          binary(),
          scan_cursor(),
          pos_integer(),
          binary() | nil
        ) :: {:ok, {0 | {:after, binary()}, [binary()]}} | {:error, term()} | :unavailable
  def scan_page(table, state, prefix, cursor, count, match_pattern)
      when is_binary(prefix) and valid_scan_cursor(cursor) and is_integer(count) and count > 0 and
             (is_binary(match_pattern) or is_nil(match_pattern)) do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        first = scan_page_start_key(tid, prefix, cursor)

        result =
          if stream_entry_prefix?(prefix) do
            # Stream entries never carry per-member expiry. Their values are
            # fetched immediately after this catalog walk, which also filters
            # a stale row if recovery or a concurrent lifecycle operation left
            # one behind. Avoiding a redundant keydir lookup keeps bounded
            # XRANGE on par with the old stream-specific ETS index.
            collect_stream_scan_page(tid, prefix, first, count, match_pattern, [], nil)
          else
            collect_scan_page(
              tid,
              lookup_state(state),
              prefix,
              first,
              count,
              match_pattern,
              ExpiryContext.capture(),
              [],
              nil
            )
          end

        case result do
          {:ok, members, last_inspected, next_key} ->
            next_cursor = scan_page_next_cursor(prefix, last_inspected, next_key)
            {:ok, {next_cursor, Enum.reverse(members)}}

          {:error, _reason} = error ->
            error
        end
    end
  rescue
    ArgumentError -> :unavailable
  end

  def scan_page(_table, _state, _prefix, _cursor, _count, _match_pattern),
    do: {:error, :invalid_scan_page}

  @doc false
  @spec hot_stream_page(
          table_ref(),
          atom() | reference(),
          binary(),
          scan_cursor(),
          pos_integer()
        ) ::
          {:ok, {0 | {:after, binary()}, [{binary(), binary()}]}}
          | :fallback
          | :unavailable
  def hot_stream_page(table, keydir, <<"X:", _rest::binary>> = prefix, cursor, count)
      when valid_scan_cursor(cursor) and is_integer(count) and count > 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        first = stream_page_start_lookup(tid, prefix, cursor)

        case collect_hot_stream_page(tid, keydir, prefix, first, count, [], nil) do
          {:ok, entries, last_inspected, next_key} ->
            next_cursor = scan_page_next_cursor(prefix, last_inspected, next_key)
            {:ok, {next_cursor, Enum.reverse(entries)}}

          :fallback ->
            :fallback
        end
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def hot_stream_page(_table, _state, _prefix, _cursor, _count), do: :fallback

  @doc false
  @spec hot_stream_page_once(
          table_ref(),
          atom() | reference(),
          binary(),
          scan_cursor(),
          pos_integer()
        ) ::
          {:ok, [{binary(), binary()}]}
          | :fallback
          | :unavailable
  def hot_stream_page_once(table, keydir, <<"X:", _rest::binary>> = prefix, cursor, count)
      when valid_scan_cursor(cursor) and is_integer(count) and count > 0 do
    case ready_table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        if count >= @stream_select_min_count do
          select_hot_stream_page_once(tid, keydir, prefix, cursor, count)
        else
          first = stream_page_start_lookup(tid, prefix, cursor)

          case collect_hot_stream_page_once(tid, keydir, prefix, first, count, []) do
            {:ok, entries} -> {:ok, Enum.reverse(entries)}
            :fallback -> :fallback
          end
        end
    end
  rescue
    ArgumentError -> :unavailable
    KeyError -> :unavailable
  end

  def hot_stream_page_once(_table, _state, _prefix, _cursor, _count), do: :fallback

  defp stream_page_start_lookup(tid, prefix, 0),
    do: :ets.next_lookup(tid, {prefix, {-1, -1}})

  defp stream_page_start_lookup(tid, prefix, {:after, {ms, seq}}),
    do: :ets.next_lookup(tid, {prefix, {ms, seq}})

  defp select_hot_stream_page_once(table, keydir, prefix, cursor, count) do
    case stream_select_match_spec(prefix, cursor) do
      :fallback ->
        :fallback

      match_spec ->
        table
        |> :ets.select(match_spec, min(count, @stream_select_batch_max))
        |> collect_selected_hot_stream_rows(keydir, prefix, count, [], :input)
    end
  end

  defp select_hot_stream_reverse_tail(table, keydir, prefix, count) do
    result =
      :ets.select_reverse(
        table,
        stream_select_match_spec(prefix, 0),
        min(count, @stream_select_batch_max)
      )

    collect_selected_hot_stream_rows(result, keydir, prefix, count, [], :reverse_input)
  end

  defp stream_select_match_spec(prefix, 0) do
    [{{{prefix, :"$1"}, :"$2"}, [], [:"$2"]}]
  end

  defp stream_select_match_spec(prefix, {:after, {ms, seq}}) do
    [
      {{{prefix, :"$1"}, :"$2"}, [{:>, :"$1", {:const, {ms, seq}}}], [:"$2"]}
    ]
  end

  defp stream_select_match_spec(_prefix, _cursor), do: :fallback

  defp collect_selected_hot_stream_rows(
         :"$end_of_table",
         _keydir,
         _prefix,
         _remaining,
         acc,
         output_order
       ),
       do: {:ok, order_selected_hot_stream_rows(acc, output_order)}

  defp collect_selected_hot_stream_rows(
         {rows, continuation},
         keydir,
         prefix,
         remaining,
         acc,
         output_order
       ) do
    case collect_selected_hot_stream_batch(rows, keydir, prefix, remaining, acc) do
      {:done, acc} ->
        {:ok, order_selected_hot_stream_rows(acc, output_order)}

      {:more, remaining, acc} ->
        continuation
        |> :ets.select()
        |> collect_selected_hot_stream_rows(
          keydir,
          prefix,
          remaining,
          acc,
          output_order
        )

      :fallback ->
        :fallback
    end
  end

  defp collect_selected_hot_stream_batch(_rows, _keydir, _prefix, 0, acc),
    do: {:done, acc}

  defp collect_selected_hot_stream_batch([], _keydir, _prefix, remaining, acc),
    do: {:more, remaining, acc}

  defp collect_selected_hot_stream_batch(
         [compound_key | rows],
         keydir,
         prefix,
         remaining,
         acc
       ) do
    case hot_stream_value(keydir, compound_key) do
      {:ok, value} ->
        entry = {member_from_key(compound_key, prefix), value}

        collect_selected_hot_stream_batch(
          rows,
          keydir,
          prefix,
          remaining - 1,
          [entry | acc]
        )

      :miss ->
        collect_selected_hot_stream_batch(rows, keydir, prefix, remaining, acc)

      :fallback ->
        :fallback
    end
  end

  defp order_selected_hot_stream_rows(acc, :input), do: Enum.reverse(acc)
  defp order_selected_hot_stream_rows(acc, :reverse_input), do: acc

  defp scan_page_start_key(tid, prefix, 0), do: first_key(tid, prefix)

  defp scan_page_start_key(
         tid,
         <<"X:", _rest::binary>> = prefix,
         {:after, {ms, seq}}
       ),
       do: :ets.next(tid, {prefix, {ms, seq}})

  defp scan_page_start_key(tid, prefix, {:after, member}),
    do: :ets.next(tid, {prefix, index_member(prefix, member)})

  defp scan_page_next_cursor(prefix, last_inspected, {prefix, _next_member})
       when is_binary(last_inspected),
       do: {:after, last_inspected}

  defp scan_page_next_cursor(_prefix, _last_inspected, _next_key), do: 0

  defp stream_entry_prefix?(<<"X:", _rest::binary>>), do: true
  defp stream_entry_prefix?(_prefix), do: false

  defp table_ref(nil), do: :undefined
  defp table_ref(table) when is_reference(table), do: table

  defp table_ref(table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined -> :undefined
      tid -> tid
    end
  end

  defp ready_table_ref(table) do
    case table_ref(table) do
      :undefined ->
        :undefined

      tid ->
        if :ets.lookup(tid, @ready_key) == [{@ready_key, true}], do: tid, else: :undefined
    end
  rescue
    ArgumentError -> :undefined
  end

  defp writable_table_ref(table) when is_atom(table) do
    ensure_table!(table)
    table_ref(table)
  end

  defp writable_table_ref(table), do: table_ref(table)

  defp member_prefix?(prefix), do: :binary.last(prefix) == 0

  defp live_keydir_entry?(_value, expire_at_ms, _file_id, _offset, _value_size, now)
       when is_integer(expire_at_ms) and expire_at_ms != 0 and expire_at_ms <= now,
       do: false

  defp live_keydir_entry?(value, expire_at_ms, _file_id, _offset, _value_size, _now)
       when value != nil and is_integer(expire_at_ms) and expire_at_ms >= 0,
       do: true

  defp live_keydir_entry?(nil, expire_at_ms, file_id, offset, value_size, _now)
       when is_integer(expire_at_ms) and expire_at_ms >= 0,
       do: valid_cold_location?(file_id, offset, value_size)

  defp live_keydir_entry?(_value, _expire_at_ms, _file_id, _offset, _value_size, _now),
    do: false

  defp split_at_separator(key) do
    case :binary.match(key, @separator) do
      {pos, 1} ->
        prefix = binary_part(key, 0, pos + 1)
        member = binary_part(key, pos + 1, byte_size(key) - pos - 1)
        {:ok, prefix, member}

      :nomatch ->
        :ignore
    end
  end

  defp replace_member_expiry(table, prefix, compound_key, expire_at_ms) do
    delete_member_expiry(table, prefix, compound_key)

    if expire_at_ms > 0 do
      :ets.insert(table, {
        {@member_expiry_tag, compound_key},
        {prefix, expire_at_ms}
      })

      :ets.insert(table, {
        {@expiry_tag, prefix, expire_at_ms, compound_key},
        true
      })
    end
  end

  defp delete_member_expiry(table, _prefix, compound_key) do
    case :ets.take(table, {@member_expiry_tag, compound_key}) do
      [{{@member_expiry_tag, ^compound_key}, {stored_prefix, expire_at_ms}}] ->
        :ets.delete(table, {@expiry_tag, stored_prefix, expire_at_ms, compound_key})

      [] ->
        :ok
    end
  end

  defp increment_prefix_count(table, prefix, delta) do
    count =
      :ets.update_counter(
        table,
        {@count_tag, prefix},
        {2, delta},
        {{@count_tag, prefix}, 0}
      )

    if count <= 0 do
      run_after_zero_count_hook(prefix)
      :ets.delete_object(table, {{@count_tag, prefix}, count})
    end

    :ok
  end

  defp run_after_zero_count_hook(prefix) do
    case Process.get(:ferricstore_compound_member_after_zero_count_hook) do
      hook when is_function(hook, 1) -> hook.(prefix)
      _missing -> :ok
    end
  end

  defp prefix_count(table, prefix) do
    case :ets.lookup(table, {@count_tag, prefix}) do
      [{{@count_tag, ^prefix}, count}] when is_integer(count) and count > 0 -> count
      _empty_or_invalid -> 0
    end
  end

  defp first_expiry_key(table, prefix) do
    :ets.next(table, {@expiry_tag, prefix, -1, <<>>})
  end

  defp cleanup_due_expiries(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         _remaining,
         inspected,
         _expiry_context
       ),
       do: {:ok, inspected}

  defp cleanup_due_expiries(
         table,
         state,
         prefix,
         {@expiry_tag, prefix, expire_at_ms, compound_key} = expiry_key,
         remaining,
         inspected,
         expiry_context
       ) do
    case ExpiryContext.classify(expiry_context, expire_at_ms) do
      :live ->
        {:ok, inspected}

      {:unsafe, reason} ->
        {:error, reason}

      :expired when remaining == 0 ->
        {:error, :limit_exceeded}

      :expired ->
        next_key = :ets.next(table, expiry_key)

        case keydir_member_row_status(state, compound_key, expiry_context) do
          :stale ->
            delete_stale_member(table, state, compound_key, expiry_context)

            cleanup_due_expiries(
              table,
              state,
              prefix,
              next_key,
              remaining - 1,
              inspected + 1,
              expiry_context
            )

          {:live,
           {^compound_key, _value, current_expire_at_ms, _lfu, _file_id, _offset, _value_size}} ->
            replace_member_expiry(table, prefix, compound_key, current_expire_at_ms)

            cleanup_due_expiries(
              table,
              state,
              prefix,
              next_key,
              remaining - 1,
              inspected + 1,
              expiry_context
            )

          {:error, reason} ->
            {:error, {:invalid_indexed_member, compound_key, reason}}
        end
    end
  end

  defp cleanup_due_expiries(
         _table,
         _state,
         _prefix,
         _other_key,
         _remaining,
         inspected,
         _expiry_context
       ),
       do: {:ok, inspected}

  defp scan_keys(table, prefix) do
    first = first_key(table, prefix)
    do_scan_keys(table, prefix, first, [])
  end

  defp first_key(table, <<"X:", _rest::binary>> = prefix),
    do: :ets.next(table, {prefix, {-1, -1}})

  defp first_key(table, prefix) do
    case :ets.lookup(table, {prefix, <<>>}) do
      [{{^prefix, <<>>}, _compound_key}] -> {prefix, <<>>}
      _ -> :ets.next(table, {prefix, <<>>})
    end
  end

  defp first_key_at_or_after(table, prefix, member) do
    member = index_member(prefix, member)

    case :ets.lookup(table, {prefix, member}) do
      [{{^prefix, ^member}, _compound_key}] -> {prefix, member}
      _ -> :ets.next(table, {prefix, member})
    end
  end

  defp last_key(table, prefix), do: :ets.prev(table, {prefix <> <<0>>, <<>>})

  defp collect_hot_stream_slice(
         _table,
         _keydir,
         _prefix,
         _next_lookup,
         _direction,
         _skip,
         0,
         entries
       ),
       do: {:ok, entries}

  defp collect_hot_stream_slice(
         _table,
         _keydir,
         _prefix,
         :"$end_of_table",
         _direction,
         _skip,
         _remaining,
         entries
       ),
       do: {:ok, entries}

  defp collect_hot_stream_slice(
         table,
         keydir,
         prefix,
         {{prefix, _member} = index_key, [{index_key, compound_key}]},
         direction,
         skip,
         remaining,
         entries
       ) do
    case hot_stream_value(keydir, compound_key) do
      {:ok, value} ->
        if skip > 0 do
          next_lookup = row_slice_next_lookup(table, index_key, direction)

          collect_hot_stream_slice(
            table,
            keydir,
            prefix,
            next_lookup,
            direction,
            skip - 1,
            remaining,
            entries
          )
        else
          member = member_from_key(compound_key, prefix)

          if remaining == 1 do
            {:ok, [{member, value} | entries]}
          else
            next_lookup = row_slice_next_lookup(table, index_key, direction)

            collect_hot_stream_slice(
              table,
              keydir,
              prefix,
              next_lookup,
              direction,
              0,
              remaining - 1,
              [{member, value} | entries]
            )
          end
        end

      :miss ->
        next_lookup = row_slice_next_lookup(table, index_key, direction)

        collect_hot_stream_slice(
          table,
          keydir,
          prefix,
          next_lookup,
          direction,
          skip,
          remaining,
          entries
        )

      :fallback ->
        :fallback
    end
  end

  defp collect_hot_stream_slice(
         _table,
         _keydir,
         _prefix,
         {_other_key, _objects},
         _direction,
         _skip,
         _remaining,
         entries
       ),
       do: {:ok, entries}

  defp row_slice_next_lookup(table, key, :forward), do: :ets.next_lookup(table, key)
  defp row_slice_next_lookup(table, key, :backward), do: :ets.prev_lookup(table, key)

  defp collect_row_slice(
         _table,
         _keydir,
         _prefix,
         _index_key,
         _direction,
         _skip,
         0,
         _expiry_context,
         acc
       ),
       do: {:ok, acc}

  defp collect_row_slice(
         _table,
         _keydir,
         _prefix,
         :"$end_of_table",
         _direction,
         _skip,
         _remaining,
         _expiry_context,
         acc
       ),
       do: {:ok, acc}

  defp collect_row_slice(
         table,
         keydir,
         prefix,
         {prefix, _member} = index_key,
         direction,
         skip,
         remaining,
         expiry_context,
         acc
       ) do
    next_key = row_slice_next_key(table, index_key, direction)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        case keydir_member_row_status(%{keydir: keydir}, compound_key, expiry_context) do
          {:live, _row} when skip > 0 ->
            collect_row_slice(
              table,
              keydir,
              prefix,
              next_key,
              direction,
              skip - 1,
              remaining,
              expiry_context,
              acc
            )

          {:live, row} ->
            collect_row_slice(
              table,
              keydir,
              prefix,
              next_key,
              direction,
              0,
              remaining - 1,
              expiry_context,
              [row | acc]
            )

          :stale ->
            delete_stale_member(table, %{keydir: keydir}, compound_key, expiry_context)

            collect_row_slice(
              table,
              keydir,
              prefix,
              next_key,
              direction,
              skip,
              remaining,
              expiry_context,
              acc
            )

          {:error, _reason} = error ->
            error
        end

      _missing ->
        collect_row_slice(
          table,
          keydir,
          prefix,
          next_key,
          direction,
          skip,
          remaining,
          expiry_context,
          acc
        )
    end
  end

  defp collect_row_slice(
         _table,
         _keydir,
         _prefix,
         _other_key,
         _direction,
         _skip,
         _remaining,
         _expiry_context,
         acc
       ),
       do: {:ok, acc}

  defp row_slice_next_key(table, index_key, :forward), do: :ets.next(table, index_key)
  defp row_slice_next_key(table, index_key, :backward), do: :ets.prev(table, index_key)

  defp collect_member_slice(
         _table,
         _state,
         _prefix,
         _index_key,
         0,
         _expiry_context,
         _pending_values,
         _boundary,
         acc
       ),
       do: {:ok, acc, 0}

  defp collect_member_slice(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         remaining,
         _expiry_context,
         _pending_values,
         _boundary,
         acc
       ),
       do: {:ok, acc, remaining}

  defp collect_member_slice(
         table,
         state,
         prefix,
         {prefix, _member} = index_key,
         remaining,
         expiry_context,
         pending_values,
         boundary,
         acc
       ) do
    if member_slice_boundary?(index_key, boundary) do
      next_key = :ets.next(table, index_key)

      case :ets.lookup(table, index_key) do
        [{^index_key, compound_key}] ->
          case indexed_member_status(state, compound_key, expiry_context, pending_values) do
            :live ->
              collect_member_slice(
                table,
                state,
                prefix,
                next_key,
                remaining - 1,
                expiry_context,
                pending_values,
                boundary,
                [member_from_key(compound_key, prefix) | acc]
              )

            :pending_skip ->
              collect_member_slice(
                table,
                state,
                prefix,
                next_key,
                remaining,
                expiry_context,
                pending_values,
                boundary,
                acc
              )

            :stale ->
              delete_stale_member(table, state, compound_key, expiry_context)

              collect_member_slice(
                table,
                state,
                prefix,
                next_key,
                remaining,
                expiry_context,
                pending_values,
                boundary,
                acc
              )

            {:error, _reason} = error ->
              error
          end

        _missing ->
          collect_member_slice(
            table,
            state,
            prefix,
            next_key,
            remaining,
            expiry_context,
            pending_values,
            boundary,
            acc
          )
      end
    else
      {:ok, acc, remaining}
    end
  end

  defp collect_member_slice(
         _table,
         _state,
         _prefix,
         _other_key,
         remaining,
         _expiry_context,
         _pending_values,
         _boundary,
         acc
       ),
       do: {:ok, acc, remaining}

  defp member_slice_boundary?(_index_key, :to_end), do: true
  defp member_slice_boundary?(index_key, {:before, start_key}), do: index_key < start_key

  defp collect_hot_stream_page(
         _table,
         _keydir,
         _prefix,
         next_lookup,
         0,
         entries,
         last_inspected
       ),
       do: {:ok, entries, last_inspected, lookup_key(next_lookup)}

  defp collect_hot_stream_page(
         _table,
         _keydir,
         _prefix,
         :"$end_of_table",
         _remaining,
         entries,
         last_inspected
       ),
       do: {:ok, entries, last_inspected, :"$end_of_table"}

  defp collect_hot_stream_page(
         table,
         keydir,
         prefix,
         {{prefix, _member} = index_key, [{index_key, compound_key}]},
         remaining,
         entries,
         _last_inspected
       ) do
    next_lookup = :ets.next_lookup(table, index_key)
    external_member = member_from_key(compound_key, prefix)

    case hot_stream_value(keydir, compound_key) do
      {:ok, value} ->
        collect_hot_stream_page(
          table,
          keydir,
          prefix,
          next_lookup,
          remaining - 1,
          [{external_member, value} | entries],
          external_member
        )

      :miss ->
        collect_hot_stream_page(
          table,
          keydir,
          prefix,
          next_lookup,
          remaining,
          entries,
          external_member
        )

      :fallback ->
        :fallback
    end
  end

  defp collect_hot_stream_page(
         _table,
         _keydir,
         _prefix,
         {other_key, _objects},
         _remaining,
         entries,
         last_inspected
       ),
       do: {:ok, entries, last_inspected, other_key}

  defp collect_hot_stream_page_once(
         _table,
         _keydir,
         _prefix,
         _next_lookup,
         0,
         entries
       ),
       do: {:ok, entries}

  defp collect_hot_stream_page_once(
         _table,
         _keydir,
         _prefix,
         :"$end_of_table",
         _remaining,
         entries
       ),
       do: {:ok, entries}

  defp collect_hot_stream_page_once(
         table,
         keydir,
         prefix,
         {{prefix, _member} = index_key, [{index_key, compound_key}]},
         remaining,
         entries
       ) do
    case hot_stream_value(keydir, compound_key) do
      {:ok, value} ->
        external_member = member_from_key(compound_key, prefix)
        entries = [{external_member, value} | entries]

        if remaining == 1 do
          {:ok, entries}
        else
          collect_hot_stream_page_once(
            table,
            keydir,
            prefix,
            :ets.next_lookup(table, index_key),
            remaining - 1,
            entries
          )
        end

      :miss ->
        collect_hot_stream_page_once(
          table,
          keydir,
          prefix,
          :ets.next_lookup(table, index_key),
          remaining,
          entries
        )

      :fallback ->
        :fallback
    end
  end

  defp collect_hot_stream_page_once(
         _table,
         _keydir,
         _prefix,
         {_other_key, _objects},
         _remaining,
         entries
       ),
       do: {:ok, entries}

  # Stream entries are persisted as binary field blobs without member TTLs.
  # Reading only the value avoids copying the seven-element keydir row for
  # every range item; nil still identifies a cold value and preserves fallback.
  defp hot_stream_value(keydir, compound_key) do
    case :ets.lookup_element(keydir, compound_key, 2, :missing) do
      :missing -> :miss
      nil -> :fallback
      value -> {:ok, value}
    end
  end

  defp lookup_key(:"$end_of_table"), do: :"$end_of_table"
  defp lookup_key({key, _objects}), do: key

  defp collect_stream_scan_page(
         _table,
         _prefix,
         index_key,
         0,
         _match_pattern,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, index_key}

  defp collect_stream_scan_page(
         _table,
         _prefix,
         :"$end_of_table",
         _remaining,
         _match_pattern,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, :"$end_of_table"}

  defp collect_stream_scan_page(
         table,
         prefix,
         {prefix, _member} = index_key,
         remaining,
         match_pattern,
         acc,
         last_inspected
       ) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        external_member = member_from_key(compound_key, prefix)

        acc =
          if scan_member_matches?(external_member, match_pattern),
            do: [external_member | acc],
            else: acc

        collect_stream_scan_page(
          table,
          prefix,
          next_key,
          remaining - 1,
          match_pattern,
          acc,
          external_member
        )

      _missing ->
        collect_stream_scan_page(
          table,
          prefix,
          next_key,
          remaining,
          match_pattern,
          acc,
          last_inspected
        )
    end
  end

  defp collect_stream_scan_page(
         _table,
         _prefix,
         other_key,
         _remaining,
         _match_pattern,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, other_key}

  defp collect_scan_page(
         _table,
         _state,
         _prefix,
         index_key,
         0,
         _match_pattern,
         _expiry_context,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, index_key}

  defp collect_scan_page(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         _remaining,
         _match_pattern,
         _expiry_context,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, :"$end_of_table"}

  defp collect_scan_page(
         table,
         state,
         prefix,
         {prefix, _member} = index_key,
         remaining,
         match_pattern,
         expiry_context,
         acc,
         last_inspected
       ) do
    next_key = :ets.next(table, index_key)
    remaining = remaining - 1

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        external_member = member_from_key(compound_key, prefix)

        case keydir_member_status(state, compound_key, expiry_context) do
          :live ->
            cond do
              not scan_member_matches?(external_member, match_pattern) ->
                collect_scan_page(
                  table,
                  state,
                  prefix,
                  next_key,
                  remaining,
                  match_pattern,
                  expiry_context,
                  acc,
                  external_member
                )

              true ->
                collect_scan_page(
                  table,
                  state,
                  prefix,
                  next_key,
                  remaining,
                  match_pattern,
                  expiry_context,
                  [external_member | acc],
                  external_member
                )
            end

          :stale ->
            delete_stale_member(table, state, compound_key, expiry_context)

            collect_scan_page(
              table,
              state,
              prefix,
              next_key,
              remaining,
              match_pattern,
              expiry_context,
              acc,
              external_member
            )

          {:error, _reason} = error ->
            error
        end

      _missing ->
        collect_scan_page(
          table,
          state,
          prefix,
          next_key,
          remaining,
          match_pattern,
          expiry_context,
          acc,
          last_inspected
        )
    end
  end

  defp collect_scan_page(
         _table,
         _state,
         _prefix,
         other_key,
         _remaining,
         _match_pattern,
         _expiry_context,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, other_key}

  defp scan_member_matches?(_member, nil), do: true
  defp scan_member_matches?(member, pattern), do: Ferricstore.GlobMatcher.match?(member, pattern)

  defp indexed_member_status(state, compound_key, expiry_context, pending_values) do
    case pending_member_status(pending_values, compound_key, expiry_context) do
      :not_pending -> keydir_member_status(state, compound_key, expiry_context)
      status -> status
    end
  end

  defp pending_member_status(%MapSet{} = pending_values, compound_key, _expiry_context) do
    if MapSet.member?(pending_values, compound_key), do: :pending_skip, else: :not_pending
  end

  defp pending_member_status(pending_values, compound_key, expiry_context)
       when is_map(pending_values) do
    case Map.fetch(pending_values, compound_key) do
      {:ok, :deleted} ->
        :pending_skip

      {:ok, {_value, expire_at_ms}} ->
        case ExpiryContext.classify(expiry_context, expire_at_ms) do
          :live -> :live
          :expired -> :pending_skip
          {:unsafe, reason} -> {:error, reason}
        end

      {:ok, invalid} ->
        {:error, {:invalid_pending_value, compound_key, invalid}}

      :error ->
        :not_pending
    end
  end

  defp pending_member_status(_pending_values, _compound_key, _expiry_context), do: :not_pending

  defp keydir_member_status(state, compound_key, expiry_context) do
    case keydir_member_row_status(state, compound_key, expiry_context) do
      {:live, _row} -> :live
      status -> status
    end
  end

  defp keydir_member_row_status(state, compound_key, expiry_context) do
    keydir = Map.get(state, :keydir) || Map.get(state, :ets)

    case :ets.lookup(keydir, compound_key) do
      [{^compound_key, value, expire_at_ms, _lfu, file_id, offset, value_size} = row]
      when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        case ExpiryContext.classify(expiry_context, expire_at_ms) do
          :expired ->
            :stale

          {:unsafe, reason} ->
            {:error, reason}

          :live ->
            cond do
              value != nil ->
                {:live, row}

              valid_cold_location?(file_id, offset, value_size) ->
                {:live, row}

              true ->
                {:error, {:invalid_cold_location, compound_key, {file_id, offset, value_size}}}
            end
        end

      [] ->
        :stale

      invalid ->
        {:error, {:invalid_keydir_entry, compound_key, invalid}}
    end
  rescue
    ArgumentError -> {:error, :keydir_unavailable}
  end

  defp delete_stale_member(table, state, compound_key, expiry_context \\ ExpiryContext.capture()) do
    previous_expire_at_ms = member_expire_at_ms(table, compound_key)
    delete(table, compound_key)
    run_after_stale_delete_hook(compound_key)

    case keydir_member_row_status(lookup_state(state), compound_key, expiry_context) do
      {:live, {^compound_key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}} ->
        put(table, compound_key, expire_at_ms)

      {:error, _reason} ->
        put(table, compound_key, previous_expire_at_ms)

      _missing_stale_or_invalid ->
        :ok
    end
  end

  defp member_expire_at_ms(table, compound_key) do
    case :ets.lookup(table, {@member_expiry_tag, compound_key}) do
      [{{@member_expiry_tag, ^compound_key}, {_prefix, expire_at_ms}}]
      when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        expire_at_ms

      _missing_or_invalid ->
        0
    end
  end

  defp run_after_stale_delete_hook(compound_key) do
    case Process.get(:ferricstore_compound_member_after_stale_delete_hook) do
      hook when is_function(hook, 1) -> hook.(compound_key)
      _missing -> :ok
    end
  end

  defp valid_cold_location?(file_id, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp valid_cold_location?({tag, file_id}, offset, value_size)
       when tag in @waraft_location_tags and is_integer(file_id) and file_id > 0 and
              is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0,
       do: true

  defp valid_cold_location?(_file_id, _offset, _value_size), do: false

  defp do_reduce_rows_while(_table, _keydir, _prefix, :"$end_of_table", acc, _reducer),
    do: {:ok, acc}

  defp do_reduce_rows_while(table, keydir, prefix, {prefix, _member} = index_key, acc, reducer) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        case :ets.lookup(keydir, compound_key) do
          [row] ->
            case reducer.(row, acc) do
              {:cont, next_acc} ->
                do_reduce_rows_while(table, keydir, prefix, next_key, next_acc, reducer)

              {:halt, result} ->
                {:halt, result}

              invalid ->
                {:error, {:invalid_row_reducer_result, invalid}}
            end

          [] ->
            delete_stale_member(table, %{keydir: keydir}, compound_key)
            do_reduce_rows_while(table, keydir, prefix, next_key, acc, reducer)

          malformed ->
            {:error, {:invalid_indexed_member, compound_key, malformed}}
        end

      _missing ->
        do_reduce_rows_while(table, keydir, prefix, next_key, acc, reducer)
    end
  end

  defp do_reduce_rows_while(_table, _keydir, _prefix, _other_key, acc, _reducer),
    do: {:ok, acc}

  defp do_scan_keys(_table, _prefix, :"$end_of_table", acc), do: Enum.reverse(acc)

  defp do_scan_keys(table, prefix, {prefix, _member} = index_key, acc) do
    acc =
      case :ets.lookup(table, index_key) do
        [{^index_key, compound_key}] -> [compound_key | acc]
        _missing -> acc
      end

    do_scan_keys(table, prefix, :ets.next(table, index_key), acc)
  end

  defp do_scan_keys(_table, _prefix, _other_key, acc), do: Enum.reverse(acc)

  defp do_scan_keys_bounded(_table, _prefix, :"$end_of_table", _remaining, acc),
    do: {:ok, Enum.reverse(acc)}

  defp do_scan_keys_bounded(_table, prefix, {prefix, _member}, 0, _acc),
    do: {:error, :limit_exceeded}

  defp do_scan_keys_bounded(table, prefix, {prefix, _member} = index_key, remaining, acc) do
    {remaining, acc} =
      case :ets.lookup(table, index_key) do
        [{^index_key, compound_key}] -> {remaining - 1, [compound_key | acc]}
        _missing -> {remaining, acc}
      end

    do_scan_keys_bounded(
      table,
      prefix,
      :ets.next(table, index_key),
      remaining,
      acc
    )
  end

  defp do_scan_keys_bounded(_table, _prefix, _other_key, _remaining, acc),
    do: {:ok, Enum.reverse(acc)}

  defp do_live_keys_for_prefix(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         _remaining,
         acc,
         inspected,
         _expiry_context
       ),
       do: {:ok, Enum.reverse(acc), inspected, :complete}

  defp do_live_keys_for_prefix(
         _table,
         _state,
         prefix,
         {prefix, _member},
         0,
         acc,
         inspected,
         _expiry_context
       ),
       do: {:ok, Enum.reverse(acc), inspected, :more}

  defp do_live_keys_for_prefix(
         table,
         state,
         prefix,
         {prefix, _member} = index_key,
         remaining,
         acc,
         inspected,
         expiry_context
       ) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        case keydir_member_row_status(state, compound_key, expiry_context) do
          {:live, _row} ->
            do_live_keys_for_prefix(
              table,
              state,
              prefix,
              next_key,
              remaining - 1,
              [compound_key | acc],
              inspected + 1,
              expiry_context
            )

          :stale ->
            delete_stale_member(table, state, compound_key, expiry_context)

            do_live_keys_for_prefix(
              table,
              state,
              prefix,
              next_key,
              remaining - 1,
              acc,
              inspected + 1,
              expiry_context
            )

          {:error, reason} ->
            {:error, reason}
        end

      [] ->
        do_live_keys_for_prefix(
          table,
          state,
          prefix,
          next_key,
          remaining - 1,
          acc,
          inspected + 1,
          expiry_context
        )
    end
  end

  defp do_live_keys_for_prefix(
         _table,
         _state,
         _prefix,
         _other_key,
         _remaining,
         acc,
         inspected,
         _expiry_context
       ),
       do: {:ok, Enum.reverse(acc), inspected, :complete}

  defp delete_index_prefix(_table, _prefix, :"$end_of_table"), do: :ok

  defp delete_index_prefix(table, prefix, {prefix, _member} = index_key) do
    next_key = :ets.next(table, index_key)

    case :ets.take(table, index_key) do
      [{^index_key, compound_key}] -> delete_member_expiry(table, prefix, compound_key)
      [] -> :ok
    end

    delete_index_prefix(table, prefix, next_key)
  end

  defp delete_index_prefix(_table, _prefix, _other_key), do: :ok

  defp delete_expiry_prefix(_table, _prefix, :"$end_of_table"), do: :ok

  defp delete_expiry_prefix(
         table,
         prefix,
         {@expiry_tag, prefix, _expire_at_ms, compound_key} = expiry_key
       ) do
    next_key = :ets.next(table, expiry_key)
    :ets.delete(table, expiry_key)

    case :ets.lookup(table, {@member_expiry_tag, compound_key}) do
      [{{@member_expiry_tag, ^compound_key}, {^prefix, _current_expire_at_ms}}] ->
        :ets.delete(table, {@member_expiry_tag, compound_key})

      _missing_or_other_prefix ->
        :ok
    end

    delete_expiry_prefix(table, prefix, next_key)
  end

  defp delete_expiry_prefix(_table, _prefix, _other_key), do: :ok

  defp do_any_live?(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         _ignored_keys,
         _expiry_context
       ),
       do: false

  defp do_any_live?(
         table,
         state,
         prefix,
         {prefix, _member} = index_key,
         ignored_keys,
         expiry_context
       ) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        if ignored_key?(ignored_keys, compound_key) do
          do_any_live?(table, state, prefix, next_key, ignored_keys, expiry_context)
        else
          case ShardETS.ets_lookup_metadata(state, compound_key, expiry_context) do
            {:live, _entry, _location} ->
              true

            :expired ->
              delete_stale_member(table, state, compound_key, expiry_context)
              do_any_live?(table, state, prefix, next_key, ignored_keys, expiry_context)

            :miss ->
              delete_stale_member(table, state, compound_key, expiry_context)
              do_any_live?(table, state, prefix, next_key, ignored_keys, expiry_context)

            {:error, {:storage_read_failed, _reason}} ->
              :unavailable

            {:error, _reason} ->
              :unavailable
          end
        end

      _missing ->
        do_any_live?(table, state, prefix, next_key, ignored_keys, expiry_context)
    end
  end

  defp do_any_live?(
         _table,
         _state,
         _prefix,
         _other_key,
         _ignored_keys,
         _expiry_context
       ),
       do: false

  defp do_count_live(
         _table,
         _state,
         _prefix,
         :"$end_of_table",
         count,
         _expiry_context
       ),
       do: {:ok, count}

  defp do_count_live(
         table,
         state,
         prefix,
         {prefix, _member} = index_key,
         count,
         expiry_context
       ) do
    next_key = :ets.next(table, index_key)

    case :ets.lookup(table, index_key) do
      [{^index_key, compound_key}] ->
        case ShardETS.ets_lookup_metadata(state, compound_key, expiry_context) do
          {:live, _entry, _location} ->
            do_count_live(table, state, prefix, next_key, count + 1, expiry_context)

          status when status in [:expired, :miss] ->
            delete_stale_member(table, state, compound_key, expiry_context)
            do_count_live(table, state, prefix, next_key, count, expiry_context)

          {:error, {:storage_read_failed, _reason}} = failure ->
            failure

          {:error, reason} ->
            {:error, {:invalid_indexed_member, compound_key, reason}}
        end

      _missing ->
        do_count_live(table, state, prefix, next_key, count, expiry_context)
    end
  end

  defp do_count_live(
         _table,
         _state,
         _prefix,
         _other_key,
         count,
         _expiry_context
       ),
       do: {:ok, count}

  defp ignored_key?(%MapSet{} = ignored_keys, compound_key),
    do: MapSet.member?(ignored_keys, compound_key)

  defp ignored_key?(ignored_keys, compound_key) when is_map(ignored_keys),
    do: Map.get(ignored_keys, compound_key) == :deleted

  defp ignored_key?(_ignored_keys, _compound_key), do: false

  defp member_from_key(compound_key, prefix) do
    prefix_len = byte_size(prefix)
    binary_part(compound_key, prefix_len, byte_size(compound_key) - prefix_len)
  end

  defp index_member(<<"X:", _rest::binary>>, member),
    do: CompoundKey.stream_index_member(member)

  defp index_member(_prefix, member), do: member

  defp lookup_state(%{keydir: _keydir} = state), do: state
  defp lookup_state(%{ets: ets} = state), do: Map.put(state, :keydir, ets)
end
