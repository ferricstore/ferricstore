defmodule Ferricstore.Flow.LMDBWriter.ProjectionOps do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.WARaftSegmentReader

  @default_source_pending_retries 100
  @default_source_pending_sleep_ms 1
  @terminal_states ["completed", "failed", "cancelled"]

  def expand_ops(state, []) do
    {:ok, [], Map.put(state, :terminal_count_cache, empty_terminal_count_cache())}
  end

  def expand_ops(state, ops) do
    initial = %{
      ops: [],
      counts: %{},
      terminal_values: %{},
      active_reverse_values: %{},
      terminal_count_inits: state.terminal_count_inits,
      terminal_count_cache: empty_terminal_count_cache()
    }

    Enum.reduce_while(ops, {:ok, initial}, fn op, {:ok, acc} ->
      case expand_op(state, op, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok,
       %{
         ops: expanded,
         terminal_count_inits: terminal_count_inits,
         terminal_count_cache: terminal_count_cache
       }} ->
        state =
          state
          |> Map.put(:terminal_count_inits, terminal_count_inits)
          |> Map.put(:terminal_count_cache, terminal_count_cache)

        {:ok, Enum.reverse(expanded), state}

      {:error, _reason} = error ->
        error
    end
  end

  def empty_terminal_count_cache, do: %{puts: %{}, refresh: MapSet.new()}

  def expand_op(state, {:project_kv_from_source, key}, acc) when is_binary(key) do
    case read_source_value(state, key) do
      {:ok, value, expire_at_ms} ->
        expand_path_op(
          state.path,
          {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)},
          acc
        )

      :not_found ->
        {:ok, prepend_ops(acc, [{:delete, key}])}

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(state, {:project_flow_state_from_source, key}, acc) when is_binary(key) do
    case read_source_value(state, key) do
      {:ok, value, expire_at_ms} ->
        expand_flow_state_value(state.path, key, value, expire_at_ms, acc)

      :not_found ->
        expand_missing_flow_state_projection(state.path, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(state, {:project_flow_state, key, value, expire_at_ms}, acc)
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) do
    expand_flow_state_value(state.path, key, value, expire_at_ms, acc)
  end

  def expand_op(%{path: path}, op, acc), do: expand_path_op(path, op, acc)

  def expand_flow_state_value(path, key, value, expire_at_ms, acc) do
    wrapper = Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)

    with {:ok, acc} <- expand_path_op(path, {:put, key, wrapper}, acc) do
      case decode_flow_record_value(wrapper) do
        {:ok, record} ->
          projection_expire_at_ms = flow_state_projection_expire_at(record, expire_at_ms)
          expand_flow_state_projection(path, key, projection_expire_at_ms, record, acc)

        :error ->
          {:ok, acc}
      end
    end
  end

  def expand_flow_state_projection(path, state_key, expire_at_ms, record, acc) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      with {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
        expand_path_op(
          path,
          {:terminal_project, Map.fetch!(record, :id), Map.fetch!(record, :type),
           Map.fetch!(record, :state), Map.get(record, :partition_key),
           Map.get(record, :updated_at_ms, 0), state_key, expire_at_ms,
           Map.get(record, :parent_flow_id), Map.get(record, :root_flow_id),
           Map.get(record, :correlation_id), Ferricstore.Flow.Attributes.record(record),
           Ferricstore.Flow.Attributes.indexed_names(record)},
          acc
        )
      end
    else
      with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
           {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
        {active_ops, reverse_value} =
          Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
            state_key,
            record,
            expire_at_ms
          )

        acc =
          acc
          |> put_in([:active_reverse_values, state_key], reverse_value)
          |> prepend_ops(
            stale_attribute_query_delete_ops(path, state_key, record) ++
              active_ops ++ flow_attribute_query_ops(record, expire_at_ms, state_key)
          )

        {:ok, acc}
      end
    end
  end

  def flow_state_projection_expire_at(record, fallback_expire_at_ms) when is_map(record) do
    case Map.get(record, :terminal_retention_until_ms) do
      expire_at_ms when is_integer(expire_at_ms) and expire_at_ms > 0 -> expire_at_ms
      _other -> fallback_expire_at_ms
    end
  end

  def expand_missing_flow_state_projection(path, state_key, acc) do
    with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
         {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
      {:ok, prepend_ops(acc, [{:delete, state_key}])}
    end
  end

  def maybe_expand_stale_terminal_delete(path, state_key, acc) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    with {:ok, terminal_key} when is_binary(terminal_key) <-
           Ferricstore.Flow.LMDB.get(path, reverse_key),
         {:ok, terminal_value} <- Ferricstore.Flow.LMDB.get(path, terminal_key),
         {:ok, count_key} <- Ferricstore.Flow.LMDB.terminal_index_count_key(terminal_value) do
      expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
    else
      _ -> {:ok, acc}
    end
  end

  def maybe_expand_stale_active_delete(path, state_key, acc) do
    with {:ok, reverse_value, acc} <- active_reverse_value(path, state_key, acc),
         true <- is_binary(reverse_value) do
      acc =
        acc
        |> put_in([:active_reverse_values, state_key], nil)
        |> prepend_ops(
          Ferricstore.Flow.LMDB.active_index_delete_ops_from_reverse(state_key, reverse_value)
        )

      {:ok, acc}
    else
      false -> {:ok, acc}
      :not_found -> {:ok, acc}
      {:error, _reason} = error -> error
      _ -> {:ok, acc}
    end
  end

  def read_source_value(state, key) do
    read_source_value(state, key, source_pending_retries())
  end

  def read_source_value(state, key, pending_retries) do
    with keydir when not is_nil(keydir) <- source_keydir(state),
         [{^key, cached_value, expire_at_ms, _lfu, file_id, offset, _value_size}] <-
           :ets.lookup(keydir, key) do
      case file_id do
        :pending when pending_retries > 0 ->
          Process.sleep(source_pending_sleep_ms())
          read_source_value(state, key, pending_retries - 1)

        :pending ->
          {:error, {:source_pending, key}}

        :deleted ->
          :not_found

        file_id when is_integer(file_id) and file_id >= 0 ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        {:waraft_segment, _index} = file_id ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        {kind, _index} = file_id when kind in [:waraft_projection, :waraft_apply_projection] ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)

        _ when is_binary(cached_value) ->
          source_read_result({:ok, cached_value}, expire_at_ms)

        _ ->
          read_source_location(state, key, cached_value, expire_at_ms, file_id, offset)
      end
    else
      [] -> :not_found
      nil -> {:error, :source_keydir_unavailable}
      _ -> {:error, :source_keydir_bad_entry}
    end
  rescue
    error -> {:error, {:source_read_failed, error}}
  end

  def source_pending_retries do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_retries, @default_source_pending_retries)
    |> normalize_non_negative_integer(@default_source_pending_retries)
  end

  def source_pending_sleep_ms do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_sleep_ms, @default_source_pending_sleep_ms)
    |> normalize_non_negative_integer(@default_source_pending_sleep_ms)
  end

  def source_keydir(%{instance_ctx: ctx, shard_index: shard_index})
      when is_map(ctx) and is_integer(shard_index) do
    case Map.get(ctx, :keydir_refs) do
      refs when is_tuple(refs) and shard_index >= 0 and shard_index < tuple_size(refs) ->
        elem(refs, shard_index)

      _ ->
        nil
    end
  end

  def source_keydir(_state), do: nil

  def read_source_location(state, _key, _cached_value, expire_at_ms, file_id, offset)
      when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    state.shard_data_path
    |> bitcask_file_path(file_id)
    |> NIF.v2_pread_at(offset)
    |> source_read_result(expire_at_ms)
  end

  def read_source_location(_state, _key, _cached_value, _expire_at_ms, :deleted, _offset),
    do: :not_found

  def read_source_location(
        %{instance_ctx: ctx, shard_index: shard_index},
        key,
        _cached_value,
        expire_at_ms,
        {:waraft_segment, _index} = file_id,
        _offset
      ) do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> source_segment_read_result(expire_at_ms)
  end

  def read_source_location(
        %{instance_ctx: ctx, shard_index: shard_index},
        key,
        _cached_value,
        expire_at_ms,
        {kind, _index} = file_id,
        _offset
      )
      when kind in [:waraft_projection, :waraft_apply_projection] do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> source_segment_read_result(expire_at_ms)
  end

  def read_source_location(_state, _key, _cached_value, _expire_at_ms, file_id, _offset),
    do: {:error, {:source_location_unavailable, file_id}}

  def source_read_result({:ok, value}, expire_at_ms) when is_binary(value),
    do: {:ok, value, expire_at_ms}

  def source_read_result({:error, reason}, _expire_at_ms), do: {:error, reason}
  def source_read_result(other, _expire_at_ms), do: {:error, {:bad_source_read, other}}

  def source_segment_read_result({:ok, value}, expire_at_ms) when is_binary(value),
    do: {:ok, value, expire_at_ms}

  def source_segment_read_result(:not_found, _expire_at_ms), do: :not_found
  def source_segment_read_result({:error, reason}, _expire_at_ms), do: {:error, reason}
  def source_segment_read_result(other, _expire_at_ms), do: {:error, {:bad_source_read, other}}

  def bitcask_file_path(shard_data_path, file_id) do
    Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  def expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc)
      when is_binary(terminal_key) and is_binary(value) and is_binary(state_key) and
             is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)
      count = if existed?, do: count, else: count + 1
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      ops =
        [
          {:put, terminal_key, value},
          {:put, reverse_key, terminal_key},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, state_key, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(path, {:terminal_put, terminal_key, value, nil, count_key}, acc)
      when is_binary(terminal_key) and is_binary(value) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)
      count = if existed?, do: count, else: count + 1

      ops =
        [
          {:put, terminal_key, value},
          {:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}
        ]
        |> maybe_put_expire_key(terminal_key, value, nil, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], value)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(
        path,
        {:terminal_project, id, type, terminal_state, partition_key, updated_at_ms, state_key,
         expire_at_ms, parent_flow_id, root_flow_id, correlation_id, attributes,
         indexed_attributes},
        acc
      )
      when is_binary(id) and is_binary(type) and is_binary(terminal_state) and
             is_integer(updated_at_ms) and is_binary(state_key) and is_integer(expire_at_ms) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, id, updated_at_ms)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key,
        count_key
      )

    with {:ok, acc} <-
           expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc) do
      {:ok,
       prepend_ops(
         acc,
         stale_attribute_query_delete_ops(path, state_key, %{
           id: id,
           type: type,
           state: terminal_state,
           partition_key: partition_key,
           updated_at_ms: updated_at_ms,
           attributes: attributes,
           indexed_attributes: indexed_attributes
         }) ++
           terminal_project_metadata_ops(
             id,
             partition_key,
             updated_at_ms,
             expire_at_ms,
             state_key,
             parent_flow_id,
             root_flow_id,
             correlation_id,
             type,
             terminal_state,
             attributes,
             indexed_attributes
           )
       )}
    end
  end

  def expand_path_op(_path, {:query_put, query_key, value}, acc)
      when is_binary(query_key) and is_binary(value) do
    {:ok, prepend_ops(acc, [{:put, query_key, value}])}
  end

  def expand_path_op(
        path,
        {:history_project_from_index, flow_index, flow_lookup, id, partition_key, history_key,
         expire_at_ms},
        acc
      )
      when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) do
    entries = history_project_from_index_entries(flow_index, flow_lookup, history_key)

    {:ok,
     prepend_ops(
       acc,
       history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries)
     )}
  end

  def expand_path_op(
        path,
        {:history_put_many, id, partition_key, history_key, expire_at_ms, entries},
        acc
      )
      when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) and
             is_list(entries) do
    {:ok,
     prepend_ops(
       acc,
       history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries)
     )}
  end

  def expand_path_op(_path, {:query_delete, query_key}, acc) when is_binary(query_key) do
    {:ok, prepend_ops(acc, [{:delete, query_key}])}
  end

  def expand_path_op(path, {:history_delete, history_index_key}, acc)
      when is_binary(history_index_key) do
    {:ok,
     prepend_ops(acc, Ferricstore.Flow.LMDB.history_index_delete_ops(path, history_index_key))}
  end

  def expand_path_op(_path, {put_mode, key, value} = op, acc)
      when put_mode in [:put, :put_new] and is_binary(key) and is_binary(value) do
    acc = maybe_init_terminal_counts_for_active_record(value, acc)
    {:ok, prepend_ops(acc, [op])}
  end

  def expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
      when is_binary(terminal_key) and is_binary(state_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)

      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key}, {:delete, reverse_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(path, {:terminal_delete, terminal_key, nil, count_key}, acc)
      when is_binary(terminal_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         {:ok, count, acc} <- terminal_count(path, count_key, acc) do
      existed? = is_binary(old_value)
      {:ok, count, acc} = repair_terminal_count_if_missing(path, count_key, count, existed?, acc)

      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      ops =
        [{:delete, terminal_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value)

      acc =
        acc
        |> put_in([:counts, count_key], count)
        |> put_in([:terminal_values, terminal_key], nil)
        |> put_in([:terminal_count_cache, :puts, count_key], count)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(_path, op, acc), do: {:ok, prepend_ops(acc, [op])}

  def maybe_init_terminal_counts_for_active_record(value, acc) do
    with {:ok, record} <- decode_flow_record_value(value),
         state when is_binary(state) <- Map.get(record, :state),
         false <- Ferricstore.Flow.LMDB.terminal_state?(state),
         type when is_binary(type) <- Map.get(record, :type) do
      partition_key = Map.get(record, :partition_key)
      init_key = {type, partition_key}

      if MapSet.member?(acc.terminal_count_inits, init_key) do
        acc
      else
        count_ops =
          Enum.map(@terminal_states, fn terminal_state ->
            state_index_key =
              Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

            count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

            {:put_new, count_key, Ferricstore.Flow.LMDB.encode_count(0)}
          end)

        acc
        |> Map.update!(:terminal_count_inits, &MapSet.put(&1, init_key))
        |> update_in([:terminal_count_cache, :refresh], fn refresh ->
          Enum.reduce(count_ops, refresh, fn {:put_new, count_key, _value}, refresh ->
            MapSet.put(refresh, count_key)
          end)
        end)
        |> prepend_ops(count_ops)
      end
    else
      _ -> acc
    end
  end

  def decode_flow_record_value(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {_expire_at_ms, encoded_record} when is_binary(encoded_record) ->
        {:ok, flow_call(:decode_record, [encoded_record])}

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  def flow_call(function, args) do
    apply(Ferricstore.Flow, function, args)
  end

  def terminal_value(path, terminal_key, acc) do
    case Map.fetch(acc.terminal_values, terminal_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, terminal_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:terminal_values, terminal_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:terminal_values, terminal_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  def terminal_count(path, count_key, acc) do
    case Map.fetch(acc.counts, count_key) do
      {:ok, count} ->
        {:ok, count, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, count_key) do
          {:ok, value} ->
            case Ferricstore.Flow.LMDB.decode_count(value) do
              {:ok, count} -> {:ok, count, put_in(acc, [:counts, count_key], count)}
              :error -> {:ok, 0, put_in(acc, [:counts, count_key], 0)}
            end

          :not_found ->
            {:ok, 0, put_in(acc, [:counts, count_key], 0)}

          {:error, _reason} = error ->
            error
        end
    end
  end

  def repair_terminal_count_if_missing(path, count_key, 0, true, acc) do
    case terminal_state_index_key_from_count_key(count_key) do
      {:ok, state_index_key} ->
        prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(state_index_key)

        case Ferricstore.Flow.LMDB.prefix_count(path, prefix) do
          {:ok, count} when count > 0 ->
            {:ok, count, put_in(acc, [:counts, count_key], count)}

          _missing_or_error ->
            {:ok, 1, put_in(acc, [:counts, count_key], 1)}
        end

      :error ->
        {:ok, 1, put_in(acc, [:counts, count_key], 1)}
    end
  end

  def repair_terminal_count_if_missing(_path, _count_key, count, _existed?, acc),
    do: {:ok, count, acc}

  def terminal_state_index_key_from_count_key(count_key) when is_binary(count_key) do
    prefix = Ferricstore.Flow.LMDB.terminal_count_prefix()

    if String.starts_with?(count_key, prefix) and byte_size(count_key) > byte_size(prefix) do
      state_index_key =
        binary_part(count_key, byte_size(prefix), byte_size(count_key) - byte_size(prefix))

      case state_index_key do
        <<>> -> :error
        _ -> {:ok, state_index_key}
      end
    else
      :error
    end
  end

  def terminal_state_index_key_from_count_key(_count_key), do: :error

  def active_reverse_value(path, state_key, acc) do
    case Map.fetch(acc.active_reverse_values, state_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(state_key)

        case Ferricstore.Flow.LMDB.get(path, reverse_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:active_reverse_values, state_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:active_reverse_values, state_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  def cache_terminal_counts(path, %{puts: puts, refresh: refresh}) do
    Enum.each(puts, fn {count_key, count} ->
      Ferricstore.Flow.LMDB.put_cached_terminal_count_key(path, count_key, count)
    end)

    Enum.each(refresh, fn count_key ->
      case Map.has_key?(puts, count_key) do
        true -> :ok
        false -> Ferricstore.Flow.LMDB.refresh_terminal_count_key(path, count_key)
      end
    end)

    :ok
  end

  def prepend_ops(acc, ops), do: %{acc | ops: :lists.reverse(ops, acc.ops)}

  def history_project_from_index_entries(nil, _flow_lookup, _history_key), do: []
  def history_project_from_index_entries(_flow_index, nil, _history_key), do: []

  def history_project_from_index_entries(flow_index, flow_lookup, history_key) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> []
      native -> history_project_from_native_entries(native, history_key)
    end
  rescue
    ArgumentError -> []
  end

  def history_project_from_native_entries(native, history_key) do
    native
    |> Ferricstore.Flow.NativeOrderedIndex.range_slice(
      history_key,
      :neg_inf,
      :inf,
      false,
      0,
      :all
    )
    |> history_project_normalize_entries()
  rescue
    ArgumentError -> []
  end

  def history_project_normalize_entries(entries) do
    Enum.map(entries, fn {event_id, event_ms} -> {event_id, trunc(event_ms)} end)
  end

  def terminal_project_metadata_ops(
        id,
        partition_key,
        score,
        expire_at_ms,
        state_key,
        parent_flow_id,
        root_flow_id,
        correlation_id,
        type,
        terminal_state,
        attributes \\ %{},
        indexed_attributes \\ []
      ) do
    []
    |> terminal_project_metadata_op(
      :parent,
      parent_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :root,
      root_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :correlation,
      correlation_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_attribute_ops(
      attributes,
      type,
      terminal_state,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key,
      indexed_attributes
    )
  end

  def terminal_project_attribute_ops(
        ops,
        attributes,
        type,
        terminal_state,
        partition_key,
        id,
        score,
        expire_at_ms,
        state_key,
        indexed_attributes
      ) do
    record = %{
      id: id,
      type: type,
      state: terminal_state,
      partition_key: partition_key,
      updated_at_ms: score,
      attributes: attributes,
      indexed_attributes: indexed_attributes
    }

    flow_attribute_query_ops(record, expire_at_ms, state_key) ++ ops
  end

  def flow_attribute_query_ops(record, expire_at_ms, state_key) do
    id = Map.get(record, :id)

    record
    |> Ferricstore.Flow.Attributes.index_entries()
    |> Enum.map(fn {index_key, _id, score} ->
      query_key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
      value = Ferricstore.Flow.LMDB.encode_query_index_value(id, score, expire_at_ms, state_key)
      {:put, query_key, value}
    end)
  end

  def stale_attribute_query_delete_ops(path, state_key, next_record) do
    next_keys =
      next_record
      |> flow_attribute_query_keys()
      |> MapSet.new()

    path
    |> old_projected_flow_record(state_key)
    |> flow_attribute_query_keys()
    |> MapSet.new()
    |> MapSet.difference(next_keys)
    |> Enum.map(&{:delete, &1})
  end

  def old_projected_flow_record(path, state_key) do
    case Ferricstore.Flow.LMDB.get(path, state_key) do
      {:ok, value} ->
        case decode_flow_record_value(value) do
          {:ok, record} -> record
          :error -> nil
        end

      _ ->
        nil
    end
  end

  def flow_attribute_query_keys(nil), do: []

  def flow_attribute_query_keys(record) do
    id = Map.get(record, :id)

    record
    |> Ferricstore.Flow.Attributes.index_entries()
    |> Enum.map(fn {index_key, _id, score} ->
      Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
    end)
  end

  def terminal_project_metadata_op(
        ops,
        :root,
        nil,
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        :root,
        "",
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        :root,
        id,
        _partition_key,
        id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        kind,
        value,
        partition_key,
        id,
        score,
        expire_at_ms,
        state_key
      )
      when is_binary(value) and value != "" do
    index_key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    query_key = Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
    value = Ferricstore.Flow.LMDB.encode_query_index_value(id, score, expire_at_ms, state_key)

    [{:put, query_key, value} | ops]
  end

  def terminal_project_metadata_op(
        ops,
        _kind,
        _value,
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_index_keys(
        id,
        partition_key,
        parent_flow_id,
        root_flow_id,
        correlation_id
      ) do
    []
    |> terminal_project_metadata_index_key(:parent, parent_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:root, root_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:correlation, correlation_id, partition_key, id)
  end

  def terminal_project_metadata_index_key(keys, :root, nil, _partition_key, _id), do: keys
  def terminal_project_metadata_index_key(keys, :root, "", _partition_key, _id), do: keys
  def terminal_project_metadata_index_key(keys, :root, id, _partition_key, id), do: keys

  def terminal_project_metadata_index_key(keys, kind, value, partition_key, _id)
      when is_binary(value) and value != "" do
    key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    [key | keys]
  end

  def terminal_project_metadata_index_key(keys, _kind, _value, _partition_key, _id), do: keys

  def history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries) do
    entries
    |> Enum.flat_map(fn {event_id, event_ms} ->
      compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition_key)
      history_index_key = Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)

      value =
        history_index_value(
          path,
          history_index_key,
          event_id,
          event_ms,
          compound_key,
          expire_at_ms
        )

      [
        {:put, history_index_key, value}
      ]
      |> maybe_history_expire_put(expire_at_ms, history_index_key)
      |> Enum.reverse()
    end)
    |> maybe_history_flow_expire_put(expire_at_ms, history_key)
  end

  def history_index_value(
        path,
        history_index_key,
        event_id,
        event_ms,
        compound_key,
        expire_at_ms
      ) do
    case existing_history_index_location(path, history_index_key) do
      {{:flow_history, _file_id} = file_ref, offset, value_size} ->
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          event_ms,
          compound_key,
          expire_at_ms,
          file_ref,
          offset,
          value_size
        )

      nil ->
        Ferricstore.Flow.LMDB.encode_history_index_value(
          event_id,
          event_ms,
          compound_key,
          expire_at_ms
        )
    end
  end

  def existing_history_index_location(path, history_index_key) do
    with {:ok, value} <- Ferricstore.Flow.LMDB.get(path, history_index_key),
         {:ok,
          {_event_id, _event_ms, _expire_at_ms, _compound_key,
           {:flow_history, _file_id} = file_ref, offset, value_size}} <-
           Ferricstore.Flow.LMDB.decode_history_index_location(value),
         true <- is_integer(offset) and offset >= 0,
         true <- is_integer(value_size) and value_size >= 0 do
      {file_ref, offset, value_size}
    else
      _ -> nil
    end
  rescue
    _ -> nil
  end

  def maybe_history_expire_put(ops, expire_at_ms, history_index_key) do
    case Ferricstore.Flow.LMDB.history_expire_key(expire_at_ms, history_index_key) do
      nil ->
        ops

      expire_key ->
        [
          {:put, expire_key, Ferricstore.Flow.LMDB.encode_history_expire_value(history_index_key)}
          | ops
        ]
    end
  end

  def maybe_history_flow_expire_put(ops, expire_at_ms, history_key) do
    case Ferricstore.Flow.LMDB.history_flow_expire_key(expire_at_ms, history_key) do
      nil ->
        ops

      expire_key ->
        ops ++
          [
            {:put, expire_key,
             Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
          ]
    end
  end

  def maybe_put_expire_key(ops, terminal_key, value, state_key, count_key) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        [{:put, expire_key, expire_value} | ops]

      _ ->
        ops
    end
  end

  def maybe_delete_old_expire_key(ops, _terminal_key, nil), do: ops

  def maybe_delete_old_expire_key(ops, terminal_key, old_value) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(old_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)
        [{:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

  defp normalize_non_negative_integer(value, default) do
    case Integer.parse(to_string(value)) do
      {integer, ""} when integer >= 0 -> integer
      _ -> default
    end
  rescue
    _ -> default
  end
end
