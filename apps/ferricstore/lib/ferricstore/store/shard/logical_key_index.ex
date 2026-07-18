defmodule Ferricstore.Store.Shard.LogicalKeyIndex do
  @moduledoc false

  alias Ferricstore.ExpiryContext
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.HLC
  alias Ferricstore.Store.CompoundKey

  @ready_key :"$ferricstore_logical_key_index_ready"
  @slot_count_key :"$ferricstore_logical_key_index_slot_count"
  @expiry_count_key :"$ferricstore_logical_key_index_expiry_count"
  @version_key :"$ferricstore_logical_key_index_version"
  @write_lock_key :"$ferricstore_logical_key_index_write_lock"
  @expiry_key :"$ferricstore_logical_key_expiry"
  @metadata_rows 3
  @read_retry_limit 10_000
  @type_warm_batch_size 4_096
  @expiry_purge_budget 256
  @random_stale_cleanup_budget 64
  @types ~w(hash list set zset stream bloom cms cuckoo topk)

  @type table_ref :: atom() | :ets.tid() | nil
  @type cursor :: 0 | {:after, binary()}

  @spec table_names(atom() | binary(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index) do
    {
      :"ferricstore_logical_keys_#{instance_name}_#{shard_index}",
      :"ferricstore_logical_key_slots_#{instance_name}_#{shard_index}"
    }
  end

  @spec ensure_tables!(atom(), atom()) :: {atom(), atom()}
  def ensure_tables!(ordered, slots) when is_atom(ordered) and is_atom(slots) do
    ensure_table!(ordered, :ordered_set)
    ensure_table!(slots, :set)
    initialize_slot_metadata(slots)
    {ordered, slots}
  end

  @spec reset(table_ref(), table_ref()) :: :ok
  def reset(ordered, slots) do
    with {:ok, ordered_tid} <- fetch_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots) do
      :ets.delete_all_objects(ordered_tid)
      :ets.delete_all_objects(slots_tid)
      initialize_slot_metadata(slots_tid)
      :ets.insert(ordered_tid, {@ready_key, true})
      :ok
    else
      _missing -> :ok
    end
  end

  @spec ready?(table_ref()) :: boolean()
  def ready?(ordered) do
    with {:ok, tid} <- fetch_table(ordered) do
      :ets.lookup(tid, @ready_key) == [{@ready_key, true}]
    else
      _missing -> false
    end
  rescue
    ArgumentError -> false
  end

  @spec rebuild(table_ref(), table_ref(), table_ref(), binary() | nil) ::
          :ok | {:error, term()}
  def rebuild(ordered, slots, keydir, shard_path \\ nil) do
    expiry_cutoff_ms =
      ExpiryContext.capture()
      |> ExpiryContext.safe_expiry_cutoff_ms()

    with {:ok, ordered_tid} <- fetch_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots),
         {:ok, keydir_tid} <- fetch_table(keydir) do
      :ets.delete_all_objects(ordered_tid)
      :ets.delete_all_objects(slots_tid)
      initialize_slot_metadata(slots_tid)

      result =
        rebuild_projection_rows(
          keydir_tid,
          ordered_tid,
          slots_tid,
          shard_path,
          expiry_cutoff_ms
        )

      case result do
        :ok ->
          :ets.insert(ordered_tid, {@ready_key, true})
          :ok

        {:error, _reason} = error ->
          :ets.delete(ordered_tid, @ready_key)
          error
      end
    else
      _missing -> {:error, :logical_key_index_unavailable}
    end
  rescue
    ArgumentError -> {:error, :logical_key_index_unavailable}
  end

  defp rebuild_projection_rows(keydir, ordered, slots, shard_path, expiry_cutoff_ms) do
    result =
      :ets.foldl(
        fn
          _row, {:error, _reason} = error ->
            error

          {storage_key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size} = row,
          {:ok, pending, pending_count}
          when is_binary(storage_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 ->
            observe_rebuild_row(storage_key)

            project_rebuild_row(
              row,
              keydir,
              ordered,
              slots,
              shard_path,
              expiry_cutoff_ms,
              {pending, pending_count}
            )

          row, {:ok, _pending, _pending_count} ->
            {:error, {:invalid_keydir_row, row}}
        end,
        {:ok, [], 0},
        keydir
      )

    case result do
      {:ok, [], 0} ->
        :ok

      {:ok, pending, _pending_count} ->
        warm_type_batch(keydir, ordered, slots, shard_path, pending)

      {:error, _reason} = error ->
        error
    end
  end

  defp project_rebuild_row(
         {<<"T:", _rest::binary>> = key, nil, expire_at_ms, _lfu, file_id, offset, _value_size},
         keydir,
         ordered,
         slots,
         shard_path,
         expiry_cutoff_ms,
         {pending, pending_count}
       ) do
    cond do
      not is_integer(file_id) or file_id < 0 or not is_integer(offset) or offset < 0 ->
        {:error, {:invalid_type_metadata_location, key, file_id, offset}}

      not live_expiration?(expire_at_ms, expiry_cutoff_ms) ->
        {:ok, pending, pending_count}

      not is_binary(shard_path) ->
        {:error, {:invalid_type_metadata, key, nil}}

      true ->
        pending = [{key, file_id, offset} | pending]
        pending_count = pending_count + 1

        if pending_count >= @type_warm_batch_size do
          case warm_type_batch(keydir, ordered, slots, shard_path, pending) do
            :ok -> {:ok, [], 0}
            {:error, _reason} = error -> error
          end
        else
          {:ok, pending, pending_count}
        end
    end
  end

  defp project_rebuild_row(
         {storage_key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size},
         _keydir,
         ordered,
         slots,
         _shard_path,
         expiry_cutoff_ms,
         {pending, pending_count}
       ) do
    if live_expiration?(expire_at_ms, expiry_cutoff_ms) do
      case put_projection_unlocked(ordered, slots, storage_key, value, expire_at_ms) do
        :ok -> {:ok, pending, pending_count}
        {:error, _reason} = error -> error
      end
    else
      {:ok, pending, pending_count}
    end
  end

  if Mix.env() == :test do
    defp observe_rebuild_row(storage_key) do
      case Process.get(:ferricstore_logical_key_rebuild_visit_hook) do
        hook when is_function(hook, 1) -> hook.(storage_key)
        _missing -> :ok
      end
    end
  else
    defp observe_rebuild_row(_storage_key), do: :ok
  end

  defp warm_type_batch(keydir, ordered, slots, shard_path, pending) do
    pending
    |> Enum.group_by(fn {_key, file_id, _offset} -> file_id end)
    |> Enum.reduce_while(:ok, fn {file_id, rows}, :ok ->
      rows = Enum.reverse(rows)
      offsets = Enum.map(rows, &elem(&1, 2))
      file_path = Ferricstore.Store.Shard.ETS.file_path(shard_path, file_id)

      case Ferricstore.Bitcask.NIF.v2_pread_batch(file_path, offsets) do
        {:ok, values} when length(values) == length(rows) ->
          if Enum.all?(values, &(CompoundKey.type_name(&1) in @types)) do
            result =
              Enum.zip(rows, values)
              |> Enum.reduce_while(:ok, fn {{key, _file_id, _offset}, value}, :ok ->
                true = :ets.update_element(keydir, key, {2, value})

                case put_projection_unlocked(ordered, slots, key, value, 0) do
                  :ok -> {:cont, :ok}
                  {:error, _reason} = error -> {:halt, error}
                end
              end)

            case result do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          else
            {:halt, {:error, :invalid_type_metadata_values}}
          end

        {:ok, _wrong_length} ->
          {:halt, {:error, :invalid_type_metadata_batch_length}}

        {:error, reason} ->
          {:halt, {:error, {:type_metadata_read_failed, file_id, reason}}}

        other ->
          {:halt, {:error, {:unexpected_type_metadata_read, file_id, other}}}
      end
    end)
  end

  @spec put(table_ref(), table_ref(), binary(), term(), non_neg_integer()) ::
          :ok | {:error, term()}
  def put(ordered, slots, storage_key, value, expire_at_ms)
      when is_binary(storage_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 do
    with {:ok, ordered_tid} <- fetch_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots),
         {:ok, logical_key, type} <- logical_projection(storage_key, value) do
      with_write_lock(slots_tid, fn ->
        upsert_logical(ordered_tid, slots_tid, logical_key, type, expire_at_ms, storage_key)
      end)
    else
      :ignore -> :ok
      :missing -> :ok
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> {:error, :logical_key_index_unavailable}
  end

  def put(_ordered, _slots, storage_key, _value, expire_at_ms),
    do: {:error, {:invalid_logical_key_projection, storage_key, expire_at_ms}}

  @spec delete(table_ref(), table_ref(), binary()) :: :ok
  def delete(ordered, slots, storage_key) when is_binary(storage_key) do
    with {:ok, ordered_tid} <- fetch_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots),
         {:ok, logical_key} <- logical_key_for_delete(storage_key) do
      with_write_lock(slots_tid, fn -> delete_logical(ordered_tid, slots_tid, logical_key) end)
    else
      _ignored_or_missing -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  def delete(_ordered, _slots, _storage_key), do: :ok

  @spec scan_page(
          table_ref(),
          table_ref(),
          cursor(),
          pos_integer(),
          binary() | nil,
          binary() | nil,
          non_neg_integer()
        ) :: {:ok, {cursor(), [binary()]}} | {:error, term()} | :unavailable
  def scan_page(ordered, keydir, cursor, count, match_pattern, type_filter, now_ms)
      when (cursor == 0 or
              (is_tuple(cursor) and tuple_size(cursor) == 2 and elem(cursor, 0) == :after and
                 is_binary(elem(cursor, 1)))) and is_integer(count) and count > 0 and
             (is_binary(match_pattern) or is_nil(match_pattern)) and
             (is_binary(type_filter) or is_nil(type_filter)) and is_integer(now_ms) and
             now_ms >= 0 do
    with {:ok, ordered_tid} <- fetch_ready_table(ordered),
         {:ok, keydir_tid} <- fetch_table(keydir) do
      first = scan_start(ordered_tid, cursor)

      case collect_page(
             ordered_tid,
             keydir_tid,
             first,
             count,
             match_pattern,
             type_filter,
             now_ms,
             [],
             nil
           ) do
        {:ok, keys, last_inspected, next_key} ->
          next_cursor = page_cursor(last_inspected, next_key)
          {:ok, {next_cursor, Enum.reverse(keys)}}

        {:error, _reason} = error ->
          error
      end
    else
      :unavailable -> :unavailable
      :missing -> :unavailable
    end
  rescue
    ArgumentError -> :unavailable
  end

  def scan_page(_ordered, _keydir, _cursor, _count, _match_pattern, _type_filter, _now_ms),
    do: {:error, :invalid_logical_scan}

  @spec all_live(table_ref(), table_ref(), non_neg_integer(), pos_integer()) ::
          {:ok, [binary()]} | {:error, term()} | :unavailable
  def all_live(ordered, keydir, now_ms, page_size \\ 10_000)

  def all_live(ordered, keydir, now_ms, page_size)
      when is_integer(now_ms) and now_ms >= 0 and is_integer(page_size) and page_size > 0 do
    collect_all_live(ordered, keydir, now_ms, page_size, 0, [])
  end

  def all_live(_ordered, _keydir, _now_ms, _page_size),
    do: {:error, :invalid_logical_key_collection}

  @spec random_key(table_ref(), table_ref(), table_ref()) ::
          {:ok, binary() | nil} | {:error, term()} | :unavailable
  def random_key(ordered, slots, keydir) do
    with {:ok, ordered_tid} <- fetch_ready_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots),
         {:ok, keydir_tid} <- fetch_table(keydir),
         {:ok, _inspected} <-
           purge_expired(
             ordered_tid,
             slots_tid,
             keydir_tid,
             HLC.now_ms(),
             fn _expired_entry -> :ok end
           ) do
      sample_random_key(ordered_tid, slots_tid, keydir_tid)
    else
      :unavailable -> :unavailable
      :missing -> :unavailable
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> :unavailable
  end

  @spec count_live(table_ref(), table_ref(), table_ref(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, term()} | :unavailable
  def count_live(ordered, slots, keydir, now_ms),
    do: count_live(ordered, slots, keydir, now_ms, fn _expired_entry -> :ok end)

  @spec count_live(table_ref(), table_ref(), table_ref(), non_neg_integer(), (tuple() -> term())) ::
          {:ok, non_neg_integer()} | {:error, term()} | :unavailable
  def count_live(ordered, slots, keydir, now_ms, on_expired)
      when is_integer(now_ms) and now_ms >= 0 and is_function(on_expired, 1) do
    with {:ok, ordered_tid} <- fetch_ready_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots),
         {:ok, keydir_tid} <- fetch_table(keydir),
         {:ok, _inspected} <-
           purge_expired(ordered_tid, slots_tid, keydir_tid, now_ms, on_expired) do
      case consistent_read(slots_tid, fn ->
             with count when is_integer(count) and count >= 0 <-
                    validated_slot_count(ordered_tid, slots_tid),
                  {:ok, due_count} <- due_expiry_count(ordered_tid, now_ms),
                  true <- due_count <= count do
               {:ok, count - due_count}
             else
               false -> {:error, :inconsistent_logical_key_expiry_count}
               {:error, _reason} = error -> error
             end
           end) do
        {:ok, count} when is_integer(count) and count >= 0 -> {:ok, count}
        {:error, _reason} = error -> error
      end
    else
      :unavailable -> :unavailable
      :missing -> :unavailable
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> :unavailable
  end

  def count_live(_ordered, _slots, _keydir, _now_ms, _on_expired),
    do: {:error, :invalid_logical_key_count}

  @spec slot_count(table_ref(), table_ref()) ::
          non_neg_integer() | :unavailable | {:error, term()}
  def slot_count(ordered, slots) do
    with {:ok, ordered_tid} <- fetch_ready_table(ordered),
         {:ok, slots_tid} <- fetch_table(slots) do
      consistent_read(slots_tid, fn -> validated_slot_count(ordered_tid, slots_tid) end)
    else
      :unavailable -> :unavailable
      :missing -> :unavailable
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp ensure_table!(name, type) do
    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [
          type,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto}
        ])

      _tid ->
        name
    end
  end

  defp initialize_slot_metadata(slots) do
    :ets.insert_new(slots, {@slot_count_key, 0})
    :ets.insert_new(slots, {@expiry_count_key, 0})
    :ets.insert_new(slots, {@version_key, 0})
    :ok
  end

  defp with_write_lock(slots, fun) when is_function(fun, 0) do
    case acquire_write_lock(slots, @read_retry_limit) do
      :ok ->
        try do
          result = fun.()
          :ets.update_counter(slots, @version_key, {2, 1}, {@version_key, 0})
          result
        after
          :ets.delete(slots, @write_lock_key)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp acquire_write_lock(_slots, 0), do: {:error, :logical_key_index_busy}

  defp acquire_write_lock(slots, attempts) do
    if :ets.insert_new(slots, {@write_lock_key, self()}) do
      :ok
    else
      case :ets.lookup(slots, @write_lock_key) do
        [{@write_lock_key, owner}] when is_pid(owner) ->
          if Process.alive?(owner) do
            :erlang.yield()
            acquire_write_lock(slots, attempts - 1)
          else
            :ets.delete_object(slots, {@write_lock_key, owner})
            acquire_write_lock(slots, attempts - 1)
          end

        invalid ->
          {:error, {:invalid_logical_key_index_writer, invalid}}
      end
    end
  end

  defp consistent_read(slots, fun) when is_function(fun, 0) do
    do_consistent_read(slots, fun, @read_retry_limit)
  end

  defp do_consistent_read(_slots, _fun, 0), do: {:error, :logical_key_index_busy}

  defp do_consistent_read(slots, fun, attempts) do
    case {:ets.lookup(slots, @write_lock_key), version_value(slots)} do
      {[], {:ok, version}} ->
        result = fun.()

        if :ets.lookup(slots, @write_lock_key) == [] and version_value(slots) == {:ok, version} do
          result
        else
          :erlang.yield()
          do_consistent_read(slots, fun, attempts - 1)
        end

      {[{@write_lock_key, owner}], {:ok, _version}} when is_pid(owner) ->
        if Process.alive?(owner) do
          :erlang.yield()
          do_consistent_read(slots, fun, attempts - 1)
        else
          :ets.delete_object(slots, {@write_lock_key, owner})
          do_consistent_read(slots, fun, attempts - 1)
        end

      {_lock, {:error, _reason} = error} ->
        error

      {invalid, _version} ->
        {:error, {:invalid_logical_key_index_writer, invalid}}
    end
  end

  defp validated_slot_count(ordered, slots) do
    with {:ok, count} <- metadata_count(slots, @slot_count_key),
         {:ok, expiry_count} <- metadata_count(slots, @expiry_count_key) do
      if :ets.info(ordered, :size) == count + expiry_count + 1 and
           :ets.info(slots, :size) == count + @metadata_rows do
        count
      else
        catalog_inconsistency(ordered, slots)
      end
    end
  end

  defp catalog_inconsistency(ordered, _slots) do
    malformed =
      :ets.foldl(
        fn
          {@ready_key, true}, nil ->
            nil

          {{@expiry_key, expire_at_ms, logical_key}, storage_key}, nil
          when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(logical_key) and
                 is_binary(storage_key) ->
            nil

          {logical_key, type, expire_at_ms, storage_key, slot}, nil
          when is_binary(logical_key) and is_binary(type) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 and is_binary(storage_key) and is_integer(slot) and slot > 0 ->
            nil

          {logical_key, _rest} = row, nil when is_binary(logical_key) ->
            {:invalid_logical_key_entry, logical_key, [row]}

          _row, reason ->
            reason
        end,
        nil,
        ordered
      )

    case malformed do
      nil -> {:error, :inconsistent_logical_key_slots}
      reason -> {:error, reason}
    end
  rescue
    ArgumentError -> {:error, :logical_key_index_unavailable}
  end

  defp metadata_count(slots, key) do
    case :ets.lookup(slots, key) do
      [{^key, value}] when is_integer(value) and value >= 0 -> {:ok, value}
      invalid -> {:error, {:invalid_logical_key_index_metadata, key, invalid}}
    end
  end

  defp version_value(slots) do
    case :ets.lookup(slots, @version_key) do
      [{@version_key, version}] when is_integer(version) and version >= 0 -> {:ok, version}
      invalid -> {:error, {:invalid_logical_key_index_version, invalid}}
    end
  end

  defp fetch_ready_table(table) do
    with {:ok, tid} <- fetch_table(table) do
      if :ets.lookup(tid, @ready_key) == [{@ready_key, true}], do: {:ok, tid}, else: :unavailable
    end
  end

  defp fetch_table(nil), do: :missing
  defp fetch_table(table) when is_reference(table), do: {:ok, table}

  defp fetch_table(table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined -> :missing
      tid -> {:ok, tid}
    end
  end

  defp logical_projection(<<"T:", _rest::binary>> = storage_key, value) do
    type = CompoundKey.type_name(value)

    if type in @types do
      {:ok, CompoundKey.extract_redis_key(storage_key), type}
    else
      {:error, {:invalid_type_metadata, storage_key, value}}
    end
  end

  defp logical_projection(storage_key, _value) do
    cond do
      InternalKey.internal?(storage_key) -> :ignore
      CompoundKey.internal_key?(storage_key) -> :ignore
      true -> {:ok, storage_key, "string"}
    end
  end

  defp put_projection_unlocked(ordered, slots, storage_key, value, expire_at_ms) do
    case logical_projection(storage_key, value) do
      {:ok, logical_key, type} ->
        if lower_priority_value_projection?(ordered, logical_key, storage_key) do
          :ok
        else
          upsert_logical(ordered, slots, logical_key, type, expire_at_ms, storage_key)
        end

      :ignore ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp lower_priority_value_projection?(_ordered, _logical_key, <<"T:", _rest::binary>>),
    do: false

  defp lower_priority_value_projection?(ordered, logical_key, _storage_key) do
    case :ets.lookup(ordered, logical_key) do
      [{^logical_key, _type, _expire_at_ms, <<"T:", _rest::binary>>, _slot}] -> true
      _missing_or_value_projection -> false
    end
  end

  defp logical_key_for_delete(<<"T:", _rest::binary>> = storage_key),
    do: {:ok, CompoundKey.extract_redis_key(storage_key)}

  defp logical_key_for_delete(storage_key) do
    cond do
      InternalKey.internal?(storage_key) -> :ignore
      CompoundKey.internal_key?(storage_key) -> :ignore
      true -> {:ok, storage_key}
    end
  end

  defp upsert_logical(ordered, slots, logical_key, type, expire_at_ms, storage_key) do
    {slot, previous_expire_at_ms} =
      case :ets.lookup(ordered, logical_key) do
        [{^logical_key, _old_type, old_expire_at_ms, _old_storage_key, existing_slot}]
        when is_integer(existing_slot) and existing_slot > 0 ->
          {existing_slot, old_expire_at_ms}

        [] ->
          next_slot =
            :ets.update_counter(slots, @slot_count_key, {2, 1}, {@slot_count_key, 0})

          :ets.insert(slots, {next_slot, logical_key})
          {next_slot, 0}

        invalid ->
          throw({:invalid_logical_key_entry, logical_key, invalid})
      end

    if previous_expire_at_ms != expire_at_ms do
      delete_expiry_entry(ordered, slots, previous_expire_at_ms, logical_key)
    end

    :ets.insert(ordered, {logical_key, type, expire_at_ms, storage_key, slot})
    put_expiry_entry(ordered, slots, expire_at_ms, logical_key, storage_key)
    :ok
  catch
    {:invalid_logical_key_entry, _key, _invalid} = reason -> {:error, reason}
  end

  defp delete_logical(ordered, slots, logical_key) do
    case :ets.lookup(ordered, logical_key) do
      [{^logical_key, _type, expire_at_ms, _storage_key, slot}]
      when is_integer(expire_at_ms) and expire_at_ms >= 0 and is_integer(slot) and slot > 0 ->
        last_slot = slot_count_value(slots)

        if last_slot < slot do
          raise "logical key slot invariant violated for #{inspect(logical_key)}"
        end

        if slot < last_slot do
          move_last_slot!(ordered, slots, last_slot, slot)
        end

        delete_expiry_entry(ordered, slots, expire_at_ms, logical_key)
        :ets.delete(ordered, logical_key)
        :ets.delete(slots, last_slot)
        :ets.update_counter(slots, @slot_count_key, {2, -1})

      [] ->
        :ok

      invalid ->
        raise "invalid logical key entry for #{inspect(logical_key)}: #{inspect(invalid)}"
    end

    :ok
  end

  defp put_expiry_entry(_ordered, _slots, 0, _logical_key, _storage_key), do: :ok

  defp put_expiry_entry(ordered, slots, expire_at_ms, logical_key, storage_key) do
    expiry_key = {@expiry_key, expire_at_ms, logical_key}

    if :ets.insert_new(ordered, {expiry_key, storage_key}) do
      :ets.update_counter(slots, @expiry_count_key, {2, 1}, {@expiry_count_key, 0})
    else
      :ets.insert(ordered, {expiry_key, storage_key})
    end

    :ok
  end

  defp delete_expiry_entry(_ordered, _slots, 0, _logical_key), do: :ok

  defp delete_expiry_entry(ordered, slots, expire_at_ms, logical_key) do
    case :ets.take(ordered, {@expiry_key, expire_at_ms, logical_key}) do
      [] ->
        :ok

      [_entry] ->
        :ets.update_counter(slots, @expiry_count_key, {2, -1})
        :ok
    end
  end

  defp move_last_slot!(ordered, slots, last_slot, destination_slot) do
    case :ets.lookup(slots, last_slot) do
      [{^last_slot, moved_key}] when is_binary(moved_key) ->
        case :ets.lookup(ordered, moved_key) do
          [{^moved_key, _type, _expire_at_ms, _storage_key, ^last_slot}] ->
            true = :ets.update_element(ordered, moved_key, {5, destination_slot})
            true = :ets.insert(slots, {destination_slot, moved_key})

          invalid ->
            raise "logical key slot points to invalid row: #{inspect({last_slot, invalid})}"
        end

      invalid ->
        raise "missing logical key slot #{last_slot}: #{inspect(invalid)}"
    end
  end

  defp scan_start(table, 0), do: :ets.first(table)
  defp scan_start(table, {:after, key}), do: :ets.next(table, key)

  defp collect_page(
         _ordered,
         _keydir,
         :"$end_of_table",
         _remaining,
         _match_pattern,
         _type_filter,
         _now_ms,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, :"$end_of_table"}

  defp collect_page(
         ordered,
         keydir,
         @ready_key,
         remaining,
         match_pattern,
         type_filter,
         now_ms,
         acc,
         last_inspected
       ) do
    collect_page(
      ordered,
      keydir,
      :ets.next(ordered, @ready_key),
      remaining,
      match_pattern,
      type_filter,
      now_ms,
      acc,
      last_inspected
    )
  end

  defp collect_page(
         ordered,
         keydir,
         {@expiry_key, expire_at_ms, logical_key} = expiry_key,
         remaining,
         match_pattern,
         type_filter,
         now_ms,
         acc,
         last_inspected
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(logical_key) do
    collect_page(
      ordered,
      keydir,
      :ets.next(ordered, expiry_key),
      remaining,
      match_pattern,
      type_filter,
      now_ms,
      acc,
      last_inspected
    )
  end

  defp collect_page(
         _ordered,
         _keydir,
         current,
         0,
         _match_pattern,
         _type_filter,
         _now_ms,
         acc,
         last_inspected
       ),
       do: {:ok, acc, last_inspected, current}

  defp collect_page(
         ordered,
         keydir,
         logical_key,
         remaining,
         match_pattern,
         type_filter,
         now_ms,
         acc,
         _last_inspected
       )
       when is_binary(logical_key) do
    next_key = :ets.next(ordered, logical_key)

    case :ets.lookup(ordered, logical_key) do
      [{^logical_key, type, expire_at_ms, storage_key, _slot}]
      when is_binary(type) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             is_binary(storage_key) ->
        case catalog_entry_live?(keydir, storage_key, now_ms) do
          true ->
            acc =
              if matches_page_filters?(logical_key, type, match_pattern, type_filter),
                do: [logical_key | acc],
                else: acc

            collect_page(
              ordered,
              keydir,
              next_key,
              remaining - 1,
              match_pattern,
              type_filter,
              now_ms,
              acc,
              logical_key
            )

          false ->
            collect_page(
              ordered,
              keydir,
              next_key,
              remaining - 1,
              match_pattern,
              type_filter,
              now_ms,
              acc,
              logical_key
            )

          {:error, _reason} = error ->
            error
        end

      invalid ->
        {:error, {:invalid_logical_key_entry, logical_key, invalid}}
    end
  end

  defp collect_page(
         _ordered,
         _keydir,
         other,
         _remaining,
         _match_pattern,
         _type_filter,
         _now_ms,
         _acc,
         _last_inspected
       ),
       do: {:error, {:invalid_logical_key, other}}

  defp page_cursor(_last_inspected, :"$end_of_table"), do: 0
  defp page_cursor(nil, _next_key), do: 0
  defp page_cursor(last_inspected, _next_key), do: {:after, last_inspected}

  defp matches_page_filters?(key, type, match_pattern, type_filter) do
    (is_nil(type_filter) or type == type_filter) and
      (is_nil(match_pattern) or Ferricstore.GlobMatcher.match?(key, match_pattern))
  end

  defp collect_all_live(ordered, keydir, now_ms, page_size, cursor, chunks) do
    case scan_page(ordered, keydir, cursor, page_size, nil, nil, now_ms) do
      {:ok, {0, keys}} ->
        {:ok, :lists.append(Enum.reverse([keys | chunks]))}

      {:ok, {next_cursor, keys}} when next_cursor != cursor ->
        collect_all_live(
          ordered,
          keydir,
          now_ms,
          page_size,
          next_cursor,
          [keys | chunks]
        )

      {:ok, {^cursor, _keys}} ->
        {:error, {:non_advancing_logical_scan, cursor}}

      :unavailable ->
        :unavailable

      {:error, _reason} = error ->
        error
    end
  end

  defp catalog_entry_live?(keydir, storage_key, now_ms) do
    case catalog_expiration(keydir, storage_key) do
      {:ok, expire_at_ms} -> live_expiration?(expire_at_ms, now_ms)
      :missing -> false
      {:error, _reason} = error -> error
    end
  end

  defp catalog_expiration(keydir, storage_key) do
    case catalog_entry(keydir, storage_key) do
      {:ok, {_key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}} ->
        {:ok, expire_at_ms}

      :missing ->
        :missing

      {:error, _reason} = error ->
        error
    end
  end

  defp catalog_entry(keydir, storage_key) do
    case :ets.lookup(keydir, storage_key) do
      [{^storage_key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size} = entry]
      when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        {:ok, entry}

      [] ->
        :missing

      invalid ->
        {:error, {:invalid_keydir_entry, storage_key, invalid}}
    end
  end

  defp purge_expired(ordered, slots, keydir, now_ms, on_expired) do
    case first_expiry_key(ordered) do
      {@expiry_key, expire_at_ms, logical_key}
      when is_integer(expire_at_ms) and expire_at_ms <= now_ms and is_binary(logical_key) ->
        with_write_lock(slots, fn ->
          do_purge_expired(
            ordered,
            slots,
            keydir,
            first_expiry_key(ordered),
            now_ms,
            on_expired,
            @expiry_purge_budget,
            0
          )
        end)

      _none_due ->
        {:ok, 0}
    end
  end

  defp first_expiry_key(ordered), do: :ets.next(ordered, @ready_key)

  defp due_expiry_count(ordered, now_ms) do
    match_spec = [
      {{{@expiry_key, :"$1", :_}, :_}, [{:is_integer, :"$1"}, {:"=<", :"$1", now_ms}], [true]}
    ]

    {:ok, :ets.select_count(ordered, match_spec)}
  rescue
    ArgumentError -> {:error, :logical_key_index_unavailable}
  end

  defp do_purge_expired(
         _ordered,
         _slots,
         _keydir,
         _expiry_key,
         _now_ms,
         _on_expired,
         0,
         inspected
       ),
       do: {:ok, inspected}

  defp do_purge_expired(
         _ordered,
         _slots,
         _keydir,
         {@expiry_key, expire_at_ms, _logical_key},
         now_ms,
         _on_expired,
         _remaining,
         inspected
       )
       when expire_at_ms > now_ms,
       do: {:ok, inspected}

  defp do_purge_expired(
         ordered,
         slots,
         keydir,
         {@expiry_key, expire_at_ms, logical_key} = expiry_key,
         now_ms,
         on_expired,
         remaining,
         inspected
       )
       when is_integer(expire_at_ms) and expire_at_ms > 0 and is_binary(logical_key) do
    next_key = :ets.next(ordered, expiry_key)

    result =
      case {:ets.lookup(ordered, expiry_key), :ets.lookup(ordered, logical_key)} do
        {[{^expiry_key, expiry_storage_key}],
         [{^logical_key, type, ^expire_at_ms, logical_storage_key, slot}]}
        when expiry_storage_key == logical_storage_key and is_binary(logical_storage_key) and
               is_binary(type) and is_integer(slot) and slot > 0 ->
          storage_key = logical_storage_key

          case catalog_entry(keydir, storage_key) do
            {:ok,
             {_key, _value, keydir_expire_at_ms, _lfu, _file_id, _offset, _value_size} =
                 keydir_entry} ->
              if live_expiration?(keydir_expire_at_ms, now_ms) do
                delete_expiry_entry(ordered, slots, expire_at_ms, logical_key)

                :ets.insert(
                  ordered,
                  {logical_key, type, keydir_expire_at_ms, storage_key, slot}
                )

                put_expiry_entry(
                  ordered,
                  slots,
                  keydir_expire_at_ms,
                  logical_key,
                  storage_key
                )
              else
                _cleanup_result = on_expired.(keydir_entry)

                reconcile_expired_logical_entry(
                  ordered,
                  slots,
                  keydir,
                  logical_key,
                  type,
                  expire_at_ms,
                  storage_key,
                  slot,
                  now_ms
                )
              end

            :missing ->
              delete_logical(ordered, slots, logical_key)

            {:error, _reason} = error ->
              error
          end

        {[{^expiry_key, _storage_key}], [{^logical_key, _type, _other_exp, _storage, _slot}]} ->
          delete_expiry_entry(ordered, slots, expire_at_ms, logical_key)

        {[{^expiry_key, _storage_key}], []} ->
          delete_expiry_entry(ordered, slots, expire_at_ms, logical_key)

        {[], _logical_row} ->
          :ok

        invalid ->
          {:error, {:invalid_logical_expiry_entry, expiry_key, invalid}}
      end

    case result do
      :ok ->
        do_purge_expired(
          ordered,
          slots,
          keydir,
          next_key,
          now_ms,
          on_expired,
          remaining - 1,
          inspected + 1
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp do_purge_expired(
         _ordered,
         _slots,
         _keydir,
         _not_expiry,
         _now_ms,
         _on_expired,
         _remaining,
         inspected
       ),
       do: {:ok, inspected}

  defp reconcile_expired_logical_entry(
         ordered,
         slots,
         keydir,
         logical_key,
         type,
         old_expire_at_ms,
         storage_key,
         slot,
         now_ms
       ) do
    case catalog_entry(keydir, storage_key) do
      {:ok, {_key, _value, current_expire_at_ms, _lfu, _file_id, _offset, _value_size}} ->
        if live_expiration?(current_expire_at_ms, now_ms) do
          delete_expiry_entry(ordered, slots, old_expire_at_ms, logical_key)

          :ets.insert(
            ordered,
            {logical_key, type, current_expire_at_ms, storage_key, slot}
          )

          put_expiry_entry(
            ordered,
            slots,
            current_expire_at_ms,
            logical_key,
            storage_key
          )
        else
          delete_logical(ordered, slots, logical_key)
        end

      :missing ->
        delete_logical(ordered, slots, logical_key)

      {:error, _reason} = error ->
        error
    end
  end

  defp sample_random_key(ordered, slots, keydir),
    do: sample_random_key(ordered, slots, keydir, @random_stale_cleanup_budget)

  defp sample_random_key(_ordered, _slots, _keydir, 0),
    do: {:error, :logical_key_expiry_backlog}

  defp sample_random_key(ordered, slots, keydir, remaining) do
    now_ms = HLC.now_ms()

    result =
      consistent_read(slots, fn ->
        case validated_slot_count(ordered, slots) do
          0 ->
            {:ok, nil}

          max_slot when is_integer(max_slot) and max_slot > 0 ->
            sample_slot(ordered, slots, keydir, :rand.uniform(max_slot), now_ms)

          {:error, _reason} = error ->
            error
        end
      end)

    case result do
      {:stale, logical_key} ->
        case remove_stale_logical_key(ordered, slots, keydir, logical_key, now_ms) do
          :ok -> sample_random_key(ordered, slots, keydir, remaining - 1)
          {:error, _reason} = error -> error
        end

      other ->
        other
    end
  end

  defp sample_slot(ordered, slots, keydir, slot, now_ms) do
    case :ets.lookup(slots, slot) do
      [{^slot, logical_key}] when is_binary(logical_key) ->
        case live_logical_key(ordered, keydir, logical_key, now_ms) do
          {:ok, true} -> {:ok, logical_key}
          {:ok, false} -> {:stale, logical_key}
          {:error, _reason} = error -> error
        end

      invalid ->
        {:error, {:invalid_logical_key_slot, slot, invalid}}
    end
  end

  defp remove_stale_logical_key(ordered, slots, keydir, logical_key, now_ms) do
    with_write_lock(slots, fn ->
      case live_logical_key(ordered, keydir, logical_key, now_ms) do
        {:ok, true} -> :ok
        {:ok, false} -> delete_logical(ordered, slots, logical_key)
        {:error, _reason} = error -> error
      end
    end)
  end

  defp live_logical_key(ordered, keydir, logical_key, now_ms) do
    case :ets.lookup(ordered, logical_key) do
      [{^logical_key, type, expire_at_ms, storage_key, slot}]
      when is_binary(type) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             is_binary(storage_key) and is_integer(slot) and slot > 0 ->
        case catalog_entry_live?(keydir, storage_key, now_ms) do
          true ->
            {:ok, true}

          false ->
            {:ok, false}

          {:error, _reason} = error ->
            error
        end

      [] ->
        {:ok, false}

      invalid ->
        {:error, {:invalid_logical_key_entry, logical_key, invalid}}
    end
  end

  defp slot_count_value(slots) do
    case metadata_count(slots, @slot_count_key) do
      {:ok, value} -> value
      {:error, reason} -> raise "invalid logical key slot count: #{inspect(reason)}"
    end
  end

  defp live_expiration?(0, _now_ms), do: true
  defp live_expiration?(expire_at_ms, now_ms), do: expire_at_ms > now_ms
end
