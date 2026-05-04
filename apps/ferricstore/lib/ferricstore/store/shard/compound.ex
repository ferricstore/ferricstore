defmodule Ferricstore.Store.Shard.Compound do
  @moduledoc "Compound-key CRUD, prefix scan/count, promoted-collection dedicated storage, and automatic compaction."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{ColdRead, LFU, Promotion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Record header size for dead byte accounting (same as @bitcask_header_size).
  @record_header_size 26

  # Promoted (dedicated) compaction thresholds.
  @promoted_frag_threshold 0.5
  @promoted_dead_bytes_min 1_048_576
  @promoted_compaction_cooldown_ms 30_000
  @cold_batch_read_timeout_ms 10_000

  # -------------------------------------------------------------------
  # Compound key handle_call handlers
  # -------------------------------------------------------------------

  @spec handle_compound_get(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get(redis_key, compound_key, state) do
    {value, state} = compound_get_value(redis_key, compound_key, state)
    {:reply, value, state}
  end

  @spec handle_compound_batch_get(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get(redis_key, compound_keys, state) do
    {values, state} =
      case promoted_store(state, redis_key) do
        nil ->
          compound_batch_get_shared(compound_keys, state)

        dedicated_path ->
          if Enum.any?(compound_keys, &shared_log_compound_key?/1) do
            compound_batch_get_mixed(redis_key, compound_keys, state)
          else
            compound_batch_get_dedicated(dedicated_path, compound_keys, state)
          end
      end

    {:reply, values, state}
  end

  defp compound_batch_get_mixed(redis_key, compound_keys, state) do
    Enum.map_reduce(compound_keys, state, fn compound_key, acc_state ->
      compound_get_value(redis_key, compound_key, acc_state)
    end)
  end

  defp compound_batch_get_shared(compound_keys, state) do
    {results, {state, cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {state, [], 0}, fn compound_key,
                                                        {state, cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, _exp} ->
            {{:value, value}, {state, cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = ShardETS.file_path(state.shard_data_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {state, [entry | cold_entries], cold_count + 1}}

          :expired ->
            {{:value, nil}, {state, cold_entries, cold_count}}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)

              {{:value, ShardETS.warm_from_store(state, compound_key)},
               {state, cold_entries, cold_count}}
            else
              {{:value, nil}, {state, cold_entries, cold_count}}
            end
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_shared_cold_batch_async()
      |> List.to_tuple()

    values =
      Enum.map(results, fn
        {:value, value} -> value
        {:cold, index} -> elem(cold_values, index)
      end)

    {values, state}
  end

  defp compound_batch_get_dedicated(dedicated_path, compound_keys, state) do
    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {[], 0}, fn compound_key, {cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, _exp} ->
            {{:value, value}, {cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = dedicated_file_path(dedicated_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          :expired ->
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            {{:value, nil}, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_compound_cold_batch_async()
      |> List.to_tuple()

    values =
      Enum.map(results, fn
        {:value, value} -> value
        {:cold, index} -> elem(cold_values, index)
      end)

    {values, state}
  end

  defp read_shared_cold_batch_async(entries), do: read_compound_cold_batch_async(entries)

  defp read_compound_cold_batch_async(entries) do
    if entries == [] do
      []
    else
      {unique_entries, value_indexes} = dedupe_compound_cold_batch_entries(entries)
      unique_values = read_unique_compound_cold_batch_async(unique_entries) |> List.to_tuple()

      Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
    end
  end

  defp dedupe_compound_cold_batch_entries(entries) do
    {unique_entries, _index_by_location, value_indexes} =
      Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
        location = compound_cold_batch_entry_location(entry)

        case Map.fetch(index_acc, location) do
          {:ok, index} ->
            {unique_acc, index_acc, [index | value_index_acc]}

          :error ->
            index = map_size(index_acc)
            {[entry | unique_acc], Map.put(index_acc, location, index), [index | value_index_acc]}
        end
      end)

    {Enum.reverse(unique_entries), Enum.reverse(value_indexes)}
  end

  defp compound_cold_batch_entry_location(
         {_state, compound_key, file_path, _fid, off, _vsize, _exp}
       ) do
    {file_path, off, compound_key}
  end

  defp read_unique_compound_cold_batch_async(entries) do
    locations =
      Enum.map(entries, fn {_state, compound_key, file_path, _fid, off, _vsize, _exp} ->
        {file_path, off, compound_key}
      end)

    values =
      case Ferricstore.Store.ColdRead.pread_batch_keyed(
             locations,
             @cold_batch_read_timeout_ms
           ) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    emit_compound_batch_cold_read_errors(entries, values)

    Enum.zip(entries, values)
    |> Enum.map(fn
      {{state, compound_key, _file_path, fid, off, vsize, exp}, value} when is_binary(value) ->
        ShardETS.cold_read_warm_ets(state, compound_key, value, exp, fid, off, vsize)
        value

      {_entry, _value} ->
        nil
    end)
  end

  defp emit_compound_batch_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_state, _compound_key, file_path, _fid, _off, _vsize, _exp}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  defp compound_get_value(redis_key, compound_key, state) do
    case promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        case ShardETS.ets_lookup_warm(state, compound_key) do
          {:hit, value, _exp} ->
            {value, state}

          :expired ->
            {nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {ShardETS.warm_from_store(state, compound_key), state}
            else
              {nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, _exp} ->
            {value, state}

          :expired ->
            {nil, state}

          _cold_or_miss ->
            case promoted_read(dedicated_path, compound_key, state) do
              {:ok, nil} ->
                {nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {value, state}

              {:ok, value, exp, fid, off, vsize} ->
                ShardETS.cold_read_warm_ets(state, compound_key, value, exp, fid, off, vsize)
                {value, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {value, state}

              _error ->
                {nil, state}
            end
        end
    end
  end

  @spec handle_compound_get_meta(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get_meta(redis_key, compound_key, state) do
    {meta, state} = compound_get_meta_value(redis_key, compound_key, state)
    {:reply, meta, state}
  end

  @spec handle_compound_batch_get_meta(binary(), [binary()], map()) :: {:reply, [term()], map()}
  @doc false
  def handle_compound_batch_get_meta(redis_key, compound_keys, state) do
    {metas, state} =
      case promoted_store(state, redis_key) do
        nil ->
          compound_batch_get_meta_shared(redis_key, compound_keys, state)

        dedicated_path ->
          compound_batch_get_meta_dedicated(dedicated_path, compound_keys, state)
      end

    {:reply, metas, state}
  end

  defp compound_batch_get_meta_shared(redis_key, compound_keys, state) do
    Enum.map_reduce(compound_keys, state, fn compound_key, state ->
      compound_get_meta_value(redis_key, compound_key, state)
    end)
  end

  defp compound_batch_get_meta_dedicated(dedicated_path, compound_keys, state) do
    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(compound_keys, {[], 0}, fn compound_key, {cold_entries, cold_count} ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {{:value, {value, expire_at_ms}}, {cold_entries, cold_count}}

          {:cold, fid, off, vsize, exp} ->
            file_path = dedicated_file_path(dedicated_path, fid)
            entry = {state, compound_key, file_path, fid, off, vsize, exp}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          :expired ->
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            {{:value, nil}, {cold_entries, cold_count}}
        end
      end)

    cold_entries = Enum.reverse(cold_entries)

    cold_metas =
      read_compound_cold_batch_async(cold_entries)
      |> Enum.zip(cold_entries)
      |> Enum.map(fn
        {nil, _entry} ->
          nil

        {value, {_state, _compound_key, _file_path, _fid, _off, _vsize, exp}} ->
          {value, exp}
      end)
      |> List.to_tuple()

    metas =
      Enum.map(results, fn
        {:value, meta} -> meta
        {:cold, index} -> elem(cold_metas, index)
      end)

    {metas, state}
  end

  defp compound_get_meta_value(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
      nil ->
        case ShardETS.ets_lookup_warm(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {{value, expire_at_ms}, state}

          :expired ->
            {nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {ShardETS.warm_meta_from_store(state, compound_key), state}
            else
              {nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {{value, expire_at_ms}, state}

          :expired ->
            {nil, state}

          _cold_or_miss ->
            case promoted_read(dedicated_path, compound_key, state) do
              {:ok, nil} ->
                {nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {{value, exp}, state}

              {:ok, value, exp, fid, off, vsize} ->
                ShardETS.cold_read_warm_ets(state, compound_key, value, exp, fid, off, vsize)
                {{value, exp}, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {{value, 0}, state}

              _error ->
                {nil, state}
            end
        end
    end
  end

  @spec handle_compound_put(binary(), binary(), binary(), non_neg_integer(), map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_put(redis_key, compound_key, value, expire_at_ms, state) do
    if state.raft? do
      handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state)
    else
      handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state)
    end
  end

  @spec handle_compound_delete(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_delete(redis_key, compound_key, state) do
    if state.raft? do
      handle_compound_delete_raft(redis_key, compound_key, state)
    else
      handle_compound_delete_direct(redis_key, compound_key, state)
    end
  end

  @spec handle_compound_scan(binary(), binary(), map()) :: {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_compound_scan(redis_key, prefix, state) do
    case promoted_store(state, redis_key) do
      nil ->
        state =
          if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
            ShardFlush.flush_pending_for_read(state)
          else
            state
          end

        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}

      dedicated_path ->
        results = ShardETS.prefix_scan_entries(state, prefix, dedicated_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
    end
  end

  @spec handle_compound_count(binary(), binary(), map()) :: {:reply, non_neg_integer(), map()}
  @doc false
  def handle_compound_count(redis_key, prefix, state) do
    case promoted_store(state, redis_key) do
      nil ->
        {:reply, ShardETS.prefix_count_entries(state, prefix), state}

      _dedicated_path ->
        {:reply, ShardETS.prefix_count_entries(state, prefix), state}
    end
  end

  @spec handle_compound_delete_prefix(binary(), binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_compound_delete_prefix(redis_key, prefix, state) do
    if state.raft? do
      handle_compound_delete_prefix_raft(redis_key, prefix, state)
    else
      handle_compound_delete_prefix_direct(redis_key, prefix, state)
    end
  end

  # -------------------------------------------------------------------
  # Raft / direct write helpers
  # -------------------------------------------------------------------

  defp handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state) do
    tracked_state =
      case promoted_store_for_compound(state, redis_key, compound_key) do
        nil ->
          state

        _dedicated_path ->
          track_promoted_dead_bytes(
            state,
            redis_key,
            compound_key,
            promoted_record_size(compound_key, value)
          )
      end

    result =
      Ferricstore.Raft.Batcher.write(
        tracked_state.index,
        {:compound_put, compound_key, value, expire_at_ms}
      )

    new_version = tracked_state.write_version + 1

    case result do
      :ok ->
        new_state = %{tracked_state | write_version: new_version}

        new_state =
          case promoted_store_for_compound(new_state, redis_key, compound_key) do
            nil -> maybe_promote(new_state, redis_key, compound_key)
            _dedicated_path -> bump_promoted_writes(new_state, redis_key)
          end

        {:reply, :ok, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp promoted_record_size(compound_key, value) when is_binary(value) do
    @record_header_size + byte_size(compound_key) + byte_size(value)
  end

  defp handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state) do
    case promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        ShardETS.ets_insert(state, compound_key, value, expire_at_ms)
        new_pending = [{compound_key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        new_state = maybe_promote(new_state, redis_key, compound_key)

        {:reply, :ok, new_state}

      dedicated_path ->
        case promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
          {:ok, {fid, offset, record_size}} ->
            state = track_promoted_dead_bytes(state, redis_key, compound_key, record_size)
            value_size = byte_size(value)

            ShardETS.ets_insert_with_location(
              state,
              compound_key,
              value,
              expire_at_ms,
              fid,
              offset,
              value_size
            )

            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_delete_raft(redis_key, compound_key, state) do
    tracked_state =
      if promoted_store_for_compound(state, redis_key, compound_key) do
        track_promoted_delete_bytes(state, redis_key, compound_key)
      else
        state
      end

    result = Ferricstore.Raft.Batcher.write(tracked_state.index, {:compound_delete, compound_key})
    new_version = tracked_state.write_version + 1

    case result do
      :ok ->
        new_state =
          if promoted_store_for_compound(tracked_state, redis_key, compound_key) do
            bump_promoted_writes(tracked_state, redis_key)
          else
            tracked_state
          end

        {:reply, :ok, %{new_state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_direct(redis_key, compound_key, state) do
    case promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)
        state = ShardFlush.track_delete_dead_bytes(state, compound_key)

        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, compound_key)

            new_pending =
              case state.pending do
                [] -> []
                pending -> Enum.reject(pending, fn {k, _, _} -> k == compound_key end)
              end

            new_version = state.write_version + 1
            {:reply, :ok, %{state | pending: new_pending, write_version: new_version}}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for compound_delete: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end

      dedicated_path ->
        state = track_promoted_delete_bytes(state, redis_key, compound_key)

        case promoted_tombstone(dedicated_path, compound_key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, compound_key)
            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted tombstone failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_delete_prefix_raft(redis_key, prefix, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, {:compound_delete_prefix, prefix})
    new_version = state.write_version + 1

    case result do
      :ok ->
        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted, write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_prefix_direct(redis_key, prefix, state) do
    case promoted_store(state, redis_key) do
      nil ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case tombstone_and_delete_keys(state, keys_to_delete) do
          {:ok, new_state} ->
            {:reply, :ok, %{new_state | write_version: new_state.write_version + 1}}

          {{:error, reason}, new_state} ->
            Logger.error(
              "Shard #{state.index}: compound_delete_prefix tombstone failed: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, new_state}
        end

      _dedicated ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ShardETS.ets_delete_key(state, key) end)

        Promotion.cleanup_promoted!(
          redis_key,
          state.shard_data_path,
          state.keydir,
          state.data_dir,
          state.index,
          state.instance_ctx
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted}}
    end
  end

  # -------------------------------------------------------------------
  # Promotion helpers
  # -------------------------------------------------------------------

  @spec promoted_store(map(), binary()) :: binary() | nil
  @doc false
  def promoted_store(state, redis_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{path: path} -> path
      path when is_binary(path) -> path
      nil -> nil
    end
  end

  defp promoted_store_for_compound(state, redis_key, compound_key) do
    if shared_log_compound_key?(compound_key) do
      nil
    else
      promoted_store(state, redis_key)
    end
  end

  # Type and promotion metadata are authoritative in the shared shard log.
  # Promoted H/S/Z data rows use dedicated Bitcask files, but metadata rows
  # keep shared-log offsets and must not be read through the dedicated path.
  defp shared_log_compound_key?(<<"T:", _rest::binary>>), do: true
  defp shared_log_compound_key?(<<"PM:", _rest::binary>>), do: true
  defp shared_log_compound_key?(_key), do: false

  defp tombstone_and_delete_keys(state, keys) do
    Enum.reduce_while(keys, {:ok, state}, fn key, {:ok, acc_state} ->
      next_state = ShardFlush.track_delete_dead_bytes(acc_state, key)

      case NIF.v2_append_tombstone(next_state.active_file_path, key) do
        {:ok, _} ->
          ShardETS.ets_delete_key(next_state, key)
          {:cont, {:ok, next_state}}

        {:error, reason} ->
          {:halt, {{:error, reason}, next_state}}
      end
    end)
  end

  @spec promoted_read(binary(), binary(), map()) ::
          {:ok, binary() | nil}
          | {:ok, binary(), non_neg_integer()}
          | {:ok, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
             non_neg_integer()}
          | {:error, term()}
  @doc false
  def promoted_read(dedicated_path, compound_key, %{keydir: keydir} = state) do
    now = HLC.now_ms()

    case :ets.lookup(keydir, compound_key) do
      [{^compound_key, value, exp, _lfu, _fid, _offset, _vsize}]
      when value != nil and (exp == 0 or exp > now) ->
        {:ok, value, exp}

      [{^compound_key, nil, exp, _lfu, fid, offset, vsize}]
      when (exp == 0 or exp > now) and is_integer(fid) and fid >= 0 and is_integer(offset) and
             offset >= 0 and is_integer(vsize) and vsize >= 0 ->
        file_path = dedicated_file_path(dedicated_path, fid)

        case read_cold_async(file_path, offset, compound_key) do
          {:ok, value} -> {:ok, value, exp, fid, offset, vsize}
          other -> other
        end

      [{^compound_key, _value, _exp, _lfu, _fid, _offset, _vsize}] ->
        ShardETS.ets_delete_key(state, compound_key)
        {:ok, nil}

      _ ->
        {:ok, nil}
    end
  end

  @spec promoted_write(binary(), binary(), binary(), non_neg_integer()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | {:error, term()}
  @doc false
  def promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    case NIF.v2_append_record(active, compound_key, value, expire_at_ms) do
      {:ok, {offset, record_size}} -> {:ok, {fid, offset, record_size}}
      {:error, _} = err -> err
    end
  end

  @spec promoted_tombstone(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc false
  def promoted_tombstone(dedicated_path, compound_key) do
    active = Promotion.find_active(dedicated_path)
    NIF.v2_append_tombstone(active, compound_key)
  end

  @spec parse_fid_from_path(binary()) :: non_neg_integer()
  @doc false
  def parse_fid_from_path(path) do
    path |> Path.basename() |> String.trim_trailing(".log") |> String.to_integer()
  end

  @spec dedicated_file_path(binary(), non_neg_integer()) :: binary()
  @doc false
  def dedicated_file_path(dedicated_path, file_id) do
    Path.join(dedicated_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  @spec bump_promoted_writes(map(), binary()) :: map()
  @doc false
  def bump_promoted_writes(state, redis_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{path: path, total_bytes: total, dead_bytes: dead, last_compacted_at: last} = info ->
        frag = if total > 0, do: dead / total, else: 0.0

        cooldown_ok =
          last == nil or
            System.monotonic_time(:millisecond) - last >= @promoted_compaction_cooldown_ms

        if frag >= @promoted_frag_threshold and dead >= @promoted_dead_bytes_min and cooldown_ok do
          case compact_dedicated_result(state, redis_key, path) do
            {:ok, state} ->
              new_total = promoted_dir_size(path)

              new_info = %{
                info
                | dead_bytes: 0,
                  total_bytes: new_total,
                  last_compacted_at: System.monotonic_time(:millisecond)
              }

              new_promoted = Map.put(state.promoted_instances, redis_key, new_info)
              %{state | promoted_instances: new_promoted}

            {:error, state} ->
              %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, info)}
          end
        else
          %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, info)}
        end

      %{path: path, writes: _writes} = info ->
        new_info =
          Map.merge(info, %{
            total_bytes: promoted_dir_size(path),
            dead_bytes: 0,
            last_compacted_at: nil
          })

        new_promoted = Map.put(state.promoted_instances, redis_key, new_info)
        %{state | promoted_instances: new_promoted}

      _ ->
        state
    end
  end

  @spec promoted_dir_size(binary()) :: non_neg_integer()
  @doc false
  def promoted_dir_size(dir_path) do
    case Ferricstore.FS.ls(dir_path) do
      {:ok, files} ->
        files
        |> Enum.filter(&String.ends_with?(&1, ".log"))
        |> Enum.reduce(0, fn name, acc ->
          case File.stat(Path.join(dir_path, name)) do
            {:ok, %{size: s}} -> acc + s
            _ -> acc
          end
        end)

      _ ->
        0
    end
  end

  @spec track_promoted_dead_bytes(map(), binary(), binary(), non_neg_integer()) :: map()
  @doc false
  def track_promoted_dead_bytes(state, redis_key, compound_key, new_record_size) do
    case Map.get(state.promoted_instances, redis_key) do
      %{total_bytes: total, dead_bytes: dead} = info ->
        old_record_size =
          case :ets.lookup(state.keydir, compound_key) do
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}]
            when is_integer(old_vsize) and old_vsize >= 0 ->
              @record_header_size + byte_size(compound_key) + old_vsize

            _ ->
              0
          end

        new_info = %{
          info
          | dead_bytes: dead + old_record_size,
            total_bytes: total + new_record_size
        }

        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  @spec track_promoted_delete_bytes(map(), binary(), binary()) :: map()
  @doc false
  def track_promoted_delete_bytes(state, redis_key, compound_key) do
    case Map.get(state.promoted_instances, redis_key) do
      %{dead_bytes: dead} = info ->
        old_record_size =
          case :ets.lookup(state.keydir, compound_key) do
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}]
            when is_integer(old_vsize) and old_vsize >= 0 ->
              @record_header_size + byte_size(compound_key) + old_vsize

            _ ->
              0
          end

        new_info = %{info | dead_bytes: dead + old_record_size}
        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  @spec compact_dedicated(map(), binary(), binary()) :: map()
  @doc false
  def compact_dedicated(state, redis_key, dedicated_path) do
    {_status, state} = compact_dedicated_result(state, redis_key, dedicated_path)
    state
  end

  defp compact_dedicated_result(state, redis_key, dedicated_path) do
    Promotion.with_compaction_latch(state, redis_key, fn ->
      do_compact_dedicated(state, redis_key, dedicated_path)
    end)
  end

  defp do_compact_dedicated(state, redis_key, dedicated_path) do
    alias Ferricstore.Store.CompoundKey

    prefix = promoted_prefix_for(state, redis_key)

    if prefix == nil do
      Logger.warning(
        "Shard #{state.index}: cannot determine prefix for promoted key #{inspect(redis_key)}, skipping compaction"
      )

      {:error, state}
    else
      active = Promotion.find_active(dedicated_path)
      # Sync outgoing active before we stop writing to it, so any last
      # pre-compaction bytes are durable regardless of when the page
      # cache writes back.
      old_fid = parse_fid_from_path(active)
      new_fid = old_fid + 1
      new_file = dedicated_file_path(dedicated_path, new_fid)

      case dedicated_fsync_file(state, active, :sync_old_active) do
        :ok ->
          Ferricstore.FS.touch!(new_file)

          case dedicated_fsync_dir(state, dedicated_path, :create_active) do
            :ok ->
              now = HLC.now_ms()

              case collect_promoted_live_entries(state, dedicated_path, prefix, now) do
                {:ok, live_entries} ->
                  maybe_run_promoted_compaction_after_collect_hook(redis_key, live_entries)

                  compact_promoted_live_entries(
                    state,
                    redis_key,
                    dedicated_path,
                    new_file,
                    old_fid,
                    new_fid,
                    live_entries
                  )

                {:error, reason} ->
                  Logger.error(
                    "Shard #{state.index}: dedicated compaction read failed: #{inspect(reason)}"
                  )

                  _ = Ferricstore.FS.rm(new_file)
                  _ = dedicated_fsync_dir(state, dedicated_path, :rollback_new_active)
                  {:error, state}
              end

            {:error, _reason} ->
              _ = Ferricstore.FS.rm(new_file)
              {:error, state}
          end

        {:error, _reason} ->
          {:error, state}
      end
    end
  end

  defp compact_promoted_live_entries(
         state,
         redis_key,
         dedicated_path,
         new_file,
         old_fid,
         new_fid,
         live_entries
       ) do
    if live_entries == [] do
      # No live promoted members remain. Keep the newly touched empty
      # active file so future writes have a valid target, and remove old
      # dedicated logs so accounting does not reset while bytes remain.
      with :ok <- remove_dedicated_logs_before(state, dedicated_path, new_fid),
           :ok <- dedicated_fsync_dir(state, dedicated_path, :remove_old_logs) do
        {:ok, state}
      else
        {:error, _reason} -> {:error, state}
      end
    else
      batch = Enum.map(live_entries, fn {k, v, exp} -> {k, v, exp} end)

      case NIF.v2_append_batch(new_file, batch) do
        {:ok, results} when length(results) == length(live_entries) ->
          ref = keydir_binary_ref(state)

          live_entries
          |> Enum.zip(results)
          |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
            value_for_ets = ShardETS.value_for_ets(value, ShardETS.hot_cache_threshold(state))
            track_binary_insert(ref, state, key, value_for_ets)

            :ets.insert(
              state.keydir,
              {key, value_for_ets, expire_at_ms, LFU.initial(), new_fid, offset, value_size}
            )
          end)

          with :ok <- remove_dedicated_logs_before(state, dedicated_path, new_fid),
               :ok <- dedicated_fsync_dir(state, dedicated_path, :remove_old_logs) do
            Logger.debug(
              "Shard #{state.index}: compacted dedicated #{inspect(redis_key)} " <>
                "(#{length(live_entries)} live entries, fid #{old_fid} -> #{new_fid})"
            )

            :telemetry.execute(
              [:ferricstore, :dedicated, :compaction],
              %{live_entries: length(live_entries), old_fid: old_fid, new_fid: new_fid},
              %{shard_index: state.index, redis_key: redis_key}
            )

            {:ok, state}
          else
            {:error, _reason} ->
              {:error, state}
          end

        {:ok, results} ->
          Logger.error(
            "Shard #{state.index}: dedicated compaction append result mismatch: expected #{length(live_entries)}, got #{length(results)}"
          )

          rollback_new_active_file(state, dedicated_path, new_file)
          {:error, state}

        {:error, reason} ->
          Logger.error(
            "Shard #{state.index}: dedicated compaction write failed: #{inspect(reason)}"
          )

          # Roll back the `touch!(new_file)` on write error. Fsync
          # so the rollback survives a subsequent crash.
          rollback_new_active_file(state, dedicated_path, new_file)
          {:error, state}
      end
    end
  end

  defp rollback_new_active_file(state, dedicated_path, new_file) do
    case Ferricstore.FS.rm(new_file) do
      :ok ->
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "Shard #{state.index}: dedicated compaction rollback failed to remove new active file #{new_file}: #{inspect(reason)}"
        )
    end

    _ = dedicated_fsync_dir(state, dedicated_path, :rollback_new_active)
    :ok
  end

  defp dedicated_fsync_dir(state, dedicated_path, phase) do
    result =
      case Process.get(:ferricstore_promoted_compaction_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(dedicated_path)
        _ -> NIF.v2_fsync_dir(dedicated_path)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{state.index}: dedicated compaction directory fsync failed during #{phase} for #{dedicated_path}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp dedicated_fsync_file(state, path, phase) do
    result =
      case Process.get(:ferricstore_promoted_compaction_fsync_file_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _ -> NIF.v2_fsync(path)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Shard #{state.index}: dedicated compaction file fsync failed during #{phase} for #{path}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp remove_dedicated_logs_before(state, dedicated_path, new_fid) do
    case Ferricstore.FS.ls(dedicated_path) do
      {:ok, files} ->
        Enum.reduce_while(files, :ok, fn name, :ok ->
          if String.ends_with?(name, ".log") do
            fid = name |> String.trim_trailing(".log") |> String.to_integer()

            if fid < new_fid do
              path = Path.join(dedicated_path, name)

              case Ferricstore.FS.rm(path) do
                :ok ->
                  {:cont, :ok}

                {:error, reason} ->
                  Logger.error(
                    "Shard #{state.index}: dedicated compaction failed to remove old log #{path}: #{inspect(reason)}"
                  )

                  {:halt, {:error, {:remove_old_log_failed, path, reason}}}
              end
            else
              {:cont, :ok}
            end
          else
            {:cont, :ok}
          end
        end)

      _ ->
        :ok
    end
  end

  @spec promoted_prefix_for(map(), binary()) :: binary() | nil
  @doc false
  def promoted_prefix_for(state, redis_key) do
    mk = Promotion.marker_key(redis_key)

    case :ets.lookup(state.keydir, mk) do
      [{^mk, "hash", _, _, _, _, _}] -> "H:" <> redis_key <> <<0>>
      [{^mk, "set", _, _, _, _, _}] -> "S:" <> redis_key <> <<0>>
      [{^mk, "zset", _, _, _, _, _}] -> "Z:" <> redis_key <> <<0>>
      _ -> nil
    end
  end

  defp collect_promoted_live_entries(state, dedicated_path, prefix, now) do
    {tokens, cold_entries, _cold_count} =
      :ets.foldl(
        fn {key, value, exp, _lfu, fid, off, vsize}, {tokens, cold_entries, cold_count} ->
          cond do
            not is_binary(key) or not String.starts_with?(key, prefix) ->
              {tokens, cold_entries, cold_count}

            exp != 0 and exp <= now ->
              {tokens, cold_entries, cold_count}

            value != nil ->
              {[{:value, {key, value, exp}} | tokens], cold_entries, cold_count}

            valid_promoted_cold_location?(fid, off, vsize) ->
              file_path = dedicated_file_path(dedicated_path, fid)
              entry = {key, exp, file_path, off}
              {[{:cold, cold_count} | tokens], [entry | cold_entries], cold_count + 1}

            true ->
              {tokens, cold_entries, cold_count}
          end
        end,
        {[], [], 0},
        state.keydir
      )

    case read_promoted_cold_batch(Enum.reverse(cold_entries)) do
      {:ok, cold_values} ->
        cold_values = List.to_tuple(cold_values)

        live_entries =
          Enum.flat_map(tokens, fn
            {:value, entry} ->
              [entry]

            {:cold, index} ->
              [elem(cold_values, index)]
          end)

        {:ok, live_entries}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_promoted_cold_batch([]), do: {:ok, []}

  defp read_promoted_cold_batch(entries) do
    locations = Enum.map(entries, fn {key, _exp, file_path, off} -> {file_path, off, key} end)

    values =
      case Ferricstore.Store.ColdRead.pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) and length(values) == length(entries) ->
          values

        {:ok, _bad_values} ->
          List.duplicate({:error, :batch_result_length_mismatch}, length(entries))

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    emit_promoted_cold_read_errors(entries, values)

    {live_entries, errors} =
      Enum.zip(entries, values)
      |> Enum.reduce({[], []}, fn
        {{key, exp, _file_path, _off}, value}, {live_entries, errors} when is_binary(value) ->
          {[{key, value, exp} | live_entries], errors}

        {{key, _exp, file_path, off}, {:error, reason}}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, reason} | errors]}

        {{key, _exp, file_path, off}, nil}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, :missing_live_cold_entry} | errors]}

        {{key, _exp, file_path, off}, value}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, {:unexpected_cold_value, value}} | errors]}
      end)

    case errors do
      [] -> {:ok, Enum.reverse(live_entries)}
      [_ | _] -> {:error, {:cold_read_failed, Enum.reverse(errors)}}
    end
  end

  defp maybe_run_promoted_compaction_after_collect_hook(redis_key, live_entries) do
    case Process.get(:ferricstore_promoted_compaction_after_collect_hook) do
      fun when is_function(fun, 2) -> fun.(redis_key, live_entries)
      _ -> :ok
    end
  end

  defp emit_promoted_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_key, _exp, file_path, _off}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {{_key, _exp, file_path, _off}, nil}, acc ->
        Map.update(acc, {file_path, :missing_live_cold_entry}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  defp valid_promoted_cold_location?(fid, off, vsize) do
    is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 and is_integer(vsize) and
      vsize >= 0
  end

  @spec maybe_promote(map(), binary(), binary()) :: map()
  @doc false
  def maybe_promote(state, redis_key, compound_key) do
    alias Ferricstore.Store.CompoundKey

    threshold = Promotion.threshold()

    if threshold == 0 or Map.has_key?(state.promoted_instances, redis_key) do
      state
    else
      case detect_compound_type(redis_key, compound_key) do
        nil ->
          state

        {type, prefix} ->
          count = ShardETS.prefix_count_entries(state, prefix)

          if count > threshold do
            state = ShardFlush.await_in_flight(state)
            state = ShardFlush.flush_pending_sync(state)

            case Promotion.promote_collection!(
                   type,
                   redis_key,
                   state.shard_data_path,
                   state.keydir,
                   state.data_dir,
                   state.index,
                   state.instance_ctx
                 ) do
              {:ok, dedicated_store} ->
                total_bytes = promoted_dir_size(dedicated_store)

                new_promoted =
                  Map.put(state.promoted_instances, redis_key, %{
                    path: dedicated_store,
                    writes: 0,
                    total_bytes: total_bytes,
                    dead_bytes: 0,
                    last_compacted_at: nil
                  })

                %{state | promoted_instances: new_promoted}
            end
          else
            state
          end
      end
    end
  end

  @spec detect_compound_type(binary(), binary()) :: {atom(), binary()} | nil
  @doc false
  def detect_compound_type(redis_key, compound_key) do
    alias Ferricstore.Store.CompoundKey

    cond do
      String.starts_with?(compound_key, CompoundKey.hash_prefix(redis_key)) ->
        {:hash, CompoundKey.hash_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.set_prefix(redis_key)) ->
        {:set, CompoundKey.set_prefix(redis_key)}

      String.starts_with?(compound_key, CompoundKey.zset_prefix(redis_key)) ->
        {:zset, CompoundKey.zset_prefix(redis_key)}

      true ->
        nil
    end
  end

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_batch_read_timeout_ms)
  end

  # -- Off-heap binary byte tracking --

  defp keydir_binary_ref(%{instance_ctx: %{keydir_binary_bytes: ref, shard_count: count}} = state)
       when ref != nil do
    index = Map.fetch!(state, :index)
    if index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(%{instance_name: name} = state) when is_atom(name) do
    keydir_binary_ref_for_instance(name, Map.fetch!(state, :index))
  end

  defp keydir_binary_ref(state) do
    keydir_binary_ref_for_instance(:default, Map.fetch!(state, :index))
  end

  defp keydir_binary_ref_for_instance(name, index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  defp track_binary_insert(nil, _, _, _), do: :ok

  defp track_binary_insert(ref, state, key, new_val) do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case :ets.lookup(state.keydir, key) do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, state.index + 1, delta)
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
