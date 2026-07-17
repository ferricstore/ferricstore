defmodule Ferricstore.Store.Shard.Compound.Promoted do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{ColdRead, CompoundKey, Promotion}
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Compound.Support

  require Logger

  @record_header_size 26
  @promoted_frag_threshold 0.5
  @promoted_dead_bytes_min 1_048_576
  @promoted_compaction_cooldown_ms 30_000
  @cold_batch_read_timeout_ms 10_000
  @promoted_compaction_page_size 256

  defp promoted_record_size(compound_key, value) when is_binary(value) do
    @record_header_size + byte_size(compound_key) + byte_size(value)
  end

  @spec promoted_store(map(), binary()) :: binary() | nil
  @doc false
  def promoted_store(state, redis_key) do
    Promotion.await_compaction_latch(state, redis_key)

    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      %{path: path} -> if(promotion_marker_live?(state, redis_key), do: path)
      path when is_binary(path) -> if(promotion_marker_live?(state, redis_key), do: path)
      nil -> marker_promoted_store(state, redis_key)
    end
  end

  defp promotion_marker_live?(state, redis_key) do
    marker_key = Promotion.marker_key(redis_key)
    now = HLC.now_ms()

    case :ets.lookup(Map.get(state, :ets), marker_key) do
      [{^marker_key, type_str, expire_at_ms, _lfu, _fid, _offset, _value_size}]
      when expire_at_ms == 0 or expire_at_ms > now ->
        decode_promoted_marker_type(type_str) != nil

      _other ->
        false
    end
  end

  defp marker_promoted_store(state, redis_key) do
    marker_key = Promotion.marker_key(redis_key)
    now = HLC.now_ms()

    case :ets.lookup(Map.get(state, :ets), marker_key) do
      [{^marker_key, type_str, expire_at_ms, _lfu, _fid, _offset, _value_size}]
      when expire_at_ms == 0 or expire_at_ms > now ->
        case decode_promoted_marker_type(type_str) do
          nil ->
            nil

          type ->
            path = Promotion.dedicated_path(state.data_dir, state.index, type, redis_key)
            if Ferricstore.FS.dir?(path), do: path, else: nil
        end

      _other ->
        nil
    end
  end

  defp decode_promoted_marker_type(type_str) do
    case CompoundKey.decode_type(type_str) do
      type when type in [:hash, :set, :zset] -> type
      _other -> nil
    end
  rescue
    _ -> nil
  end

  def promoted_store_for_compound(state, redis_key, compound_key) do
    if shared_log_compound_key?(compound_key) do
      nil
    else
      promoted_store(state, redis_key)
    end
  end

  # The marker remains in the shared log so recovery can discover the dedicated
  # collection. Type metadata is co-located with members so logical batches use
  # one append target.
  def shared_log_compound_key?(<<"PM:", _rest::binary>>), do: true
  def shared_log_compound_key?(_key), do: false

  def tombstone_and_delete_keys(state, []), do: {:ok, state}

  def tombstone_and_delete_keys(state, keys) do
    case append_tombstone_batch_sync(state.active_file_path, keys) do
      {:ok, _locations} ->
        next_state =
          Enum.reduce(keys, state, fn key, acc_state ->
            ShardFlush.track_delete_dead_bytes(acc_state, key)
          end)

        Enum.each(keys, fn key -> ShardETS.ets_delete_key(next_state, key) end)
        {:ok, next_state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  @spec promoted_read(binary(), binary(), map()) ::
          {:ok, binary() | nil}
          | {:ok, binary(), non_neg_integer()}
          | {:ok, binary(), non_neg_integer(), non_neg_integer(), non_neg_integer(),
             non_neg_integer()}
          | {:error, term()}
  @doc false
  def promoted_read(dedicated_path, compound_key, state) do
    case ShardETS.ets_lookup_metadata(state, compound_key) do
      {:live, {^compound_key, value, exp, _lfu, _fid, _offset, _vsize}, :hot} ->
        {:ok, value, exp}

      {:live, {^compound_key, nil, exp, _lfu, fid, offset, vsize}, :cold}
      when is_integer(fid) and fid >= 0 and is_integer(offset) and offset >= 0 and
             is_integer(vsize) and vsize >= 0 ->
        file_path = dedicated_file_path(dedicated_path, fid)

        case Support.read_cold_async(state, file_path, offset, compound_key) do
          {:ok, value} -> {:ok, value, exp, fid, offset, vsize}
          other -> other
        end

      {:live, _entry, :pending} ->
        {:error, :pending_cold_write}

      {:live, _entry, _invalid_or_unsupported_location} ->
        {:error, :invalid_keydir_entry}

      {:error, :invalid_keydir_entry} ->
        {:error, :invalid_keydir_entry}

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      result when result in [:expired, :miss] ->
        {:ok, nil}
    end
  rescue
    ArgumentError -> {:error, :keydir_unavailable}
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

  def promoted_write_value(state, dedicated_path, compound_key, value, expire_at_ms) do
    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    with {:ok, persisted_value} <- Support.persisted_disk_value(state, value) do
      case NIF.v2_append_record(active, compound_key, persisted_value, expire_at_ms) do
        {:ok, {offset, _record_size}} ->
          value_size = byte_size(persisted_value)
          record_size = promoted_record_size(compound_key, persisted_value)
          {:ok, {fid, offset, value_size, record_size}}

        {:error, _} = err ->
          err
      end
    end
  end

  def promoted_write_batch_values(_state, _dedicated_path, []), do: {:ok, []}

  def promoted_write_batch_values(state, dedicated_path, entries) do
    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    with {:ok, persisted_entries} <- Support.persisted_disk_entries(state, entries) do
      case NIF.v2_append_batch(active, persisted_entries) do
        {:ok, locations} when length(locations) == length(entries) ->
          results =
            persisted_entries
            |> Enum.zip(locations)
            |> Enum.map(fn {{compound_key, persisted_value, _expire_at_ms}, {offset, value_size}} ->
              {fid, offset, value_size, promoted_record_size(compound_key, persisted_value)}
            end)

          {:ok, results}

        {:ok, locations} ->
          {:error, {:batch_result_mismatch, length(entries), locations}}

        {:error, _} = err ->
          err
      end
    end
  end

  @spec promoted_tombstone(binary(), binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  @doc false
  def promoted_tombstone(dedicated_path, compound_key) do
    active = Promotion.find_active(dedicated_path)
    NIF.v2_append_tombstone(active, compound_key)
  end

  @spec promoted_tombstone_batch(binary(), [binary()]) :: {:ok, list()} | {:error, term()}
  @doc false
  def promoted_tombstone_batch(_dedicated_path, []), do: {:ok, []}

  def promoted_tombstone_batch(dedicated_path, compound_keys) do
    active = Promotion.find_active(dedicated_path)
    append_tombstone_batch_sync(active, compound_keys)
  end

  defp append_tombstone_batch_sync(path, keys) do
    ops = Enum.map(keys, &{:delete, &1})

    case NIF.v2_append_ops_batch(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(keys)) do
          {:ok, locations}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_tombstone_locations(locations, expected_count)
       when length(locations) == expected_count do
    if Enum.all?(locations, &valid_tombstone_location?/1) do
      :ok
    else
      {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_tombstone_locations(locations, expected_count),
    do: {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}

  defp valid_tombstone_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_tombstone_location?(_location), do: false

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
    if promoted_compaction_due?(state, redis_key) do
      send(self(), {:maybe_compact_promoted, redis_key})
    end

    state
  end

  @spec apply_promoted_maintenance(map(), binary(), map()) :: map()
  @doc false
  def apply_promoted_maintenance(
        state,
        redis_key,
        %{appended_bytes: appended, reclaimable_bytes: reclaimable, writes: writes}
      )
      when is_integer(appended) and appended >= 0 and is_integer(reclaimable) and
             reclaimable >= 0 and is_integer(writes) and writes >= 0 do
    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      info when is_map(info) ->
        updated =
          info
          |> Map.update(:total_bytes, appended, &(&1 + appended))
          |> Map.update(:dead_bytes, reclaimable, &(&1 + reclaimable))
          |> Map.update(:writes, writes, &(&1 + writes))
          |> Map.put_new(:last_compacted_at, nil)

        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, updated)}

      _missing ->
        state
    end
  end

  def apply_promoted_maintenance(state, _redis_key, _invalid), do: state

  @spec promoted_compaction_due?(map(), binary(), integer()) :: boolean()
  @doc false
  def promoted_compaction_due?(
        state,
        redis_key,
        now_ms \\ System.monotonic_time(:millisecond)
      ) do
    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      %{total_bytes: total, dead_bytes: dead, last_compacted_at: last}
      when is_integer(total) and total > 0 and is_integer(dead) and dead >= 0 ->
        dead >= @promoted_dead_bytes_min and dead / total >= @promoted_frag_threshold and
          (last == nil or now_ms - last >= @promoted_compaction_cooldown_ms)

      _other ->
        false
    end
  end

  @spec promoted_dir_size(binary()) :: non_neg_integer()
  @doc false
  def promoted_dir_size(dir_path) do
    case Ferricstore.FS.ls(dir_path) do
      {:ok, files} ->
        files
        |> Enum.reduce(0, fn name, acc ->
          case dedicated_log_file_id(name) do
            {:ok, _fid} ->
              case File.lstat(Path.join(dir_path, name)) do
                {:ok, %File.Stat{type: :regular, size: size}} -> acc + size
                _ -> acc
              end

            :skip ->
              acc
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
      %{total_bytes: total, dead_bytes: dead} = info ->
        old_record_size =
          case :ets.lookup(state.keydir, compound_key) do
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}]
            when is_integer(old_vsize) and old_vsize >= 0 ->
              @record_header_size + byte_size(compound_key) + old_vsize

            _ ->
              0
          end

        tombstone_size = @record_header_size + byte_size(compound_key)

        new_info = %{
          info
          | total_bytes: total + tombstone_size,
            dead_bytes: dead + old_record_size + tombstone_size
        }

        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  @doc false
  def track_promoted_delete_bytes_entry(
        state,
        redis_key,
        {compound_key, _value, _expire_at_ms, _lfu, _file_id, _offset, old_vsize}
      )
      when is_integer(old_vsize) and old_vsize >= 0 do
    case Map.get(state.promoted_instances, redis_key) do
      %{dead_bytes: dead} = info ->
        old_record_size = @record_header_size + byte_size(compound_key) + old_vsize
        new_info = %{info | dead_bytes: dead + old_record_size}
        %{state | promoted_instances: Map.put(state.promoted_instances, redis_key, new_info)}

      _ ->
        state
    end
  end

  def track_promoted_delete_bytes_entry(state, _redis_key, _entry), do: state

  @spec compact_dedicated(map(), binary(), binary()) :: map()
  @doc false
  def compact_dedicated(state, redis_key, dedicated_path) do
    {_status, state} = compact_dedicated_result(state, redis_key, dedicated_path)
    state
  end

  @doc false
  def compact_dedicated_result(state, redis_key, dedicated_path) do
    Promotion.with_compaction_latch(state, redis_key, fn ->
      do_compact_dedicated(state, redis_key, dedicated_path)
    end)
  end

  @doc false
  def compact_dedicated_result_latched(state, redis_key, dedicated_path) do
    do_compact_dedicated(state, redis_key, dedicated_path)
  end

  defp do_compact_dedicated(state, redis_key, dedicated_path) do
    alias Ferricstore.Store.CompoundKey

    prefix = promoted_prefix_for(state, redis_key)

    if prefix == nil do
      Logger.warning(
        "Shard #{state.index}: cannot determine prefix for promoted key #{inspect(redis_key)}, skipping compaction"
      )

      fail_dedicated_compaction(state, redis_key, dedicated_path, :prefix, :missing_prefix)
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

              compact_promoted_catalog_pages(
                state,
                redis_key,
                dedicated_path,
                prefix,
                new_file,
                old_fid,
                new_fid,
                now
              )

            {:error, reason} ->
              rollback_new_active_file(state, dedicated_path, new_file)
              fail_dedicated_compaction(state, redis_key, dedicated_path, :create_active, reason)
          end

        {:error, reason} ->
          fail_dedicated_compaction(state, redis_key, dedicated_path, :sync_old_active, reason)
      end
    end
  end

  defp compact_promoted_catalog_pages(
         state,
         redis_key,
         dedicated_path,
         prefix,
         new_file,
         old_fid,
         new_fid,
         now_ms
       ) do
    member_index =
      Map.get(state, :compound_member_index) || Map.get(state, :compound_member_index_name)

    metadata_key = CompoundKey.type_key(redis_key)

    page_result =
      with {:ok, metadata_entries} <-
             collect_promoted_exact_page(state, dedicated_path, [metadata_key], now_ms),
           :ok <- maybe_run_promoted_compaction_after_collect_hook(redis_key, metadata_entries),
           {:ok, metadata_written?} <-
             append_promoted_compaction_page(state, new_file, new_fid, metadata_entries) do
        compact_promoted_page(
          state,
          redis_key,
          dedicated_path,
          prefix,
          member_index,
          new_file,
          new_fid,
          now_ms,
          0,
          length(metadata_entries),
          metadata_written?
        )
      else
        {:error, {:cold_read_failed, _errors} = reason} ->
          {:error, :collect_metadata, reason, false}

        {:error, reason} ->
          {:error, :append_metadata, reason, false}
      end

    case page_result do
      {:ok, live_count, _published?} ->
        with :ok <- remove_dedicated_logs_before(state, dedicated_path, new_fid),
             :ok <- dedicated_fsync_dir(state, dedicated_path, :remove_old_logs) do
          Logger.debug(
            "Shard #{state.index}: compacted dedicated #{inspect(redis_key)} " <>
              "(#{live_count} live entries, fid #{old_fid} -> #{new_fid})"
          )

          :telemetry.execute(
            [:ferricstore, :dedicated, :compaction],
            %{live_entries: live_count, old_fid: old_fid, new_fid: new_fid},
            %{shard_index: state.index, redis_key: redis_key}
          )

          {:ok, state}
        else
          {:error, reason} ->
            fail_dedicated_compaction(
              state,
              redis_key,
              dedicated_path,
              :remove_old_logs,
              reason
            )
        end

      {:error, phase, reason, published?} ->
        log_promoted_compaction_page_failure(state, phase, reason)

        unless published? do
          rollback_new_active_file(state, dedicated_path, new_file)
        end

        fail_dedicated_compaction(state, redis_key, dedicated_path, phase, reason)
    end
  end

  defp compact_promoted_page(
         state,
         redis_key,
         dedicated_path,
         prefix,
         member_index,
         new_file,
         new_fid,
         now_ms,
         cursor,
         live_count,
         published?
       ) do
    case CompoundMemberIndex.scan_page(
           member_index,
           state,
           prefix,
           cursor,
           @promoted_compaction_page_size,
           nil
         ) do
      {:ok, {next_cursor, members}} ->
        with {:ok, live_entries} <-
               collect_promoted_live_page(state, dedicated_path, prefix, members, now_ms),
             :ok <- maybe_run_promoted_compaction_after_collect_hook(redis_key, live_entries),
             {:ok, wrote?} <-
               append_promoted_compaction_page(state, new_file, new_fid, live_entries) do
          live_count = live_count + length(live_entries)
          published? = published? or wrote?

          if next_cursor == 0 do
            {:ok, live_count, published?}
          else
            compact_promoted_page(
              state,
              redis_key,
              dedicated_path,
              prefix,
              member_index,
              new_file,
              new_fid,
              now_ms,
              next_cursor,
              live_count,
              published?
            )
          end
        else
          {:error, {:cold_read_failed, _errors} = reason} ->
            {:error, :collect_live_entries, reason, published?}

          {:error, reason} ->
            {:error, :append, reason, published?}
        end

      :unavailable ->
        {:error, :member_catalog, :compound_member_index_unavailable, published?}

      {:error, reason} ->
        {:error, :member_catalog, reason, published?}
    end
  end

  defp append_promoted_compaction_page(_state, _new_file, _new_fid, []),
    do: {:ok, false}

  defp append_promoted_compaction_page(state, new_file, new_fid, live_entries) do
    batch =
      Enum.map(live_entries, fn {key, value, expire_at_ms, _old_row} ->
        {key, value, expire_at_ms}
      end)

    case NIF.v2_append_batch(new_file, batch) do
      {:ok, results} when length(results) == length(live_entries) ->
        publish_promoted_compaction_page(state, new_fid, live_entries, results)
        {:ok, true}

      {:ok, results} ->
        {:error, {:append_result_mismatch, length(live_entries), length(results)}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp publish_promoted_compaction_page(state, new_fid, live_entries, results) do
    ref = Support.keydir_binary_ref(state)
    hot_cache_threshold = ShardETS.hot_cache_threshold(state)

    live_entries
    |> Enum.zip(results)
    |> Enum.each(fn
      {{key, value, expire_at_ms, old_row}, {offset, value_size}} ->
        case :ets.lookup(state.keydir, key) do
          [^old_row] ->
            value_for_ets = ShardETS.value_for_ets(value, hot_cache_threshold)
            Support.track_binary_insert(ref, state, key, value_for_ets)
            old_lfu = elem(old_row, 3)

            :ets.insert(
              state.keydir,
              {key, value_for_ets, expire_at_ms, old_lfu, new_fid, offset, value_size}
            )

          _changed_or_deleted ->
            :ok
        end
    end)

    :ok
  end

  defp log_promoted_compaction_page_failure(state, :append, reason) do
    Logger.error("Shard #{state.index}: dedicated compaction write failed: #{inspect(reason)}")
  end

  defp log_promoted_compaction_page_failure(state, phase, reason) do
    Logger.error("Shard #{state.index}: dedicated compaction #{phase} failed: #{inspect(reason)}")
  end

  defp fail_dedicated_compaction(state, redis_key, dedicated_path, phase, reason) do
    :telemetry.execute(
      [:ferricstore, :dedicated, :compaction_failed],
      %{count: 1, error_count: dedicated_compaction_error_count(reason)},
      %{
        shard_index: state.index,
        phase: phase,
        reason: dedicated_compaction_failure_reason(reason),
        path: dedicated_path,
        redis_key_hash: :erlang.phash2(redis_key)
      }
    )

    {:error, state}
  end

  defp dedicated_compaction_error_count({:cold_read_failed, errors}) when is_list(errors),
    do: length(errors)

  defp dedicated_compaction_error_count(_reason), do: 1

  defp dedicated_compaction_failure_reason({:cold_read_failed, _errors}), do: :cold_read_failed

  defp dedicated_compaction_failure_reason({:append_result_mismatch, _expected, _got}),
    do: :append_result_mismatch

  defp dedicated_compaction_failure_reason({:remove_old_log_failed, _path, _reason}),
    do: :remove_old_log_failed

  defp dedicated_compaction_failure_reason(reason) when is_atom(reason), do: reason
  defp dedicated_compaction_failure_reason({reason, _detail}) when is_atom(reason), do: reason
  defp dedicated_compaction_failure_reason(_reason), do: :error

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
    case list_dedicated_logs(dedicated_path) do
      {:ok, files} ->
        Enum.reduce_while(files, :ok, fn name, :ok ->
          case dedicated_log_file_id(name) do
            {:ok, fid} when fid < new_fid ->
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

            {:ok, _fid} ->
              {:cont, :ok}

            :skip ->
              {:cont, :ok}
          end
        end)

      {:error, reason} ->
        Logger.error(
          "Shard #{state.index}: dedicated compaction failed to list old logs under " <>
            "#{dedicated_path}: #{inspect(reason)}"
        )

        {:error, {:list_old_logs_failed, {dedicated_path, reason}}}

      other ->
        Logger.error(
          "Shard #{state.index}: dedicated compaction received an invalid old-log listing for " <>
            "#{dedicated_path}: #{inspect(other)}"
        )

        {:error, {:list_old_logs_failed, {dedicated_path, {:unexpected_result, other}}}}
    end
  end

  defp list_dedicated_logs(dedicated_path) do
    case Process.get(:ferricstore_promoted_compaction_list_hook) do
      fun when is_function(fun, 1) -> fun.(dedicated_path)
      _missing -> Ferricstore.FS.ls(dedicated_path)
    end
  end

  defp dedicated_log_file_id(name) do
    with true <- String.ends_with?(name, ".log"),
         false <- String.starts_with?(name, "compact_"),
         stem <- String.trim_trailing(name, ".log"),
         {fid, ""} <- Integer.parse(stem),
         true <- fid >= 0 do
      {:ok, fid}
    else
      _ -> :skip
    end
  end

  @spec promoted_prefix_for(map(), binary()) :: binary() | nil
  @doc false
  def promoted_prefix_for(state, redis_key) do
    mk = Promotion.marker_key(redis_key)

    case :ets.lookup(state.keydir, mk) do
      [{^mk, "hash", _, _, _, _, _}] -> CompoundKey.hash_prefix(redis_key)
      [{^mk, "set", _, _, _, _, _}] -> CompoundKey.set_prefix(redis_key)
      [{^mk, "zset", _, _, _, _, _}] -> CompoundKey.zset_prefix(redis_key)
      _ -> nil
    end
  end

  defp collect_promoted_live_page(state, dedicated_path, prefix, members, now) do
    keys = Enum.map(members, &(prefix <> &1))
    collect_promoted_exact_page(state, dedicated_path, keys, now)
  end

  defp collect_promoted_exact_page(state, dedicated_path, keys, now) do
    {tokens, cold_entries, _cold_count} =
      Enum.reduce(
        keys,
        {[], [], 0},
        fn key, {tokens, cold_entries, cold_count} ->
          case :ets.lookup(state.keydir, key) do
            [{^key, _value, exp, _lfu, _fid, _off, _vsize}]
            when exp != 0 and exp <= now ->
              {tokens, cold_entries, cold_count}

            [{^key, value, exp, _lfu, _fid, _off, _vsize} = row] when is_binary(value) ->
              {[{:value, {key, value, exp, row}} | tokens], cold_entries, cold_count}

            [{^key, nil, exp, _lfu, fid, off, vsize} = row]
            when is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 and
                   is_integer(vsize) and vsize >= 0 ->
              file_path = dedicated_file_path(dedicated_path, fid)
              entry = {key, exp, file_path, off, row}
              {[{:cold, cold_count} | tokens], [entry | cold_entries], cold_count + 1}

            [] ->
              {tokens, cold_entries, cold_count}

            [invalid] ->
              {[{:error, {:invalid_promoted_keydir_entry, invalid}} | tokens], cold_entries,
               cold_count}
          end
        end
      )

    case Enum.find(tokens, &match?({:error, _reason}, &1)) do
      {:error, reason} ->
        {:error, reason}

      nil ->
        materialize_promoted_live_page(tokens, cold_entries)
    end
  rescue
    ArgumentError -> {:error, :compound_keydir_unavailable}
  end

  defp materialize_promoted_live_page(tokens, cold_entries) do
    case read_promoted_cold_batch(Enum.reverse(cold_entries)) do
      {:ok, cold_values} ->
        cold_values = List.to_tuple(cold_values)

        live_entries =
          tokens
          |> Enum.reverse()
          |> Enum.flat_map(fn
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
    locations =
      Enum.map(entries, fn {key, _exp, file_path, off, _row} -> {file_path, off, key} end)

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
        {{key, exp, _file_path, _off, row}, value}, {live_entries, errors}
        when is_binary(value) ->
          {[{key, value, exp, row} | live_entries], errors}

        {{key, _exp, file_path, off, _row}, {:error, reason}}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, reason} | errors]}

        {{key, _exp, file_path, off, _row}, nil}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, :missing_live_cold_entry} | errors]}

        {{key, _exp, file_path, off, _row}, value}, {live_entries, errors} ->
          {live_entries, [{key, file_path, off, {:unexpected_cold_value, value}} | errors]}
      end)

    case errors do
      [] -> {:ok, Enum.reverse(live_entries)}
      [_ | _] -> {:error, {:cold_read_failed, Enum.reverse(errors)}}
    end
  end

  defp maybe_run_promoted_compaction_after_collect_hook(_redis_key, []), do: :ok

  defp maybe_run_promoted_compaction_after_collect_hook(redis_key, live_entries) do
    case Process.get(:ferricstore_promoted_compaction_after_collect_hook) do
      fun when is_function(fun, 2) -> fun.(redis_key, live_entries)
      _ -> :ok
    end

    :ok
  end

  defp emit_promoted_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_key, _exp, file_path, _off, _row}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {{_key, _exp, file_path, _off, _row}, nil}, acc ->
        Map.update(acc, {file_path, :missing_live_cold_entry}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  @spec maybe_promote(map(), binary(), binary()) :: map()
  @doc false
  def maybe_promote(state, redis_key, compound_key) do
    threshold = Map.fetch!(state, :apply_context).promotion_threshold
    maybe_promote(state, redis_key, compound_key, threshold)
  end

  @spec maybe_promote(map(), binary(), binary(), non_neg_integer()) :: map()
  @doc false
  def maybe_promote(state, redis_key, compound_key, threshold)
      when is_integer(threshold) and threshold >= 0 do
    alias Ferricstore.Store.CompoundKey

    if threshold == 0 or Map.has_key?(state.promoted_instances, redis_key) do
      state
    else
      case detect_compound_type(redis_key, compound_key) do
        nil ->
          state

        {type, prefix} ->
          if promotion_type_metadata_ready?(state, redis_key, type) do
            count = ShardETS.prefix_count_entries(state, prefix)

            if is_integer(count) and count > threshold do
              start_compound_promotion(state, redis_key, type)
            else
              state
            end
          else
            state
          end
      end
    end
  end

  defp promotion_type_metadata_ready?(state, redis_key, type) do
    type_key = CompoundKey.type_key(redis_key)
    expected_type = CompoundKey.encode_type(type)
    now = HLC.now_ms()

    case :ets.lookup(state.keydir, type_key) do
      [{^type_key, value, expire_at_ms, _lfu, _fid, _offset, _value_size}]
      when (expire_at_ms == 0 or expire_at_ms > now) and
             (value == nil or value == expected_type) ->
        true

      _missing_expired_or_mismatched ->
        false
    end
  rescue
    ArgumentError -> false
  end

  defp start_compound_promotion(state, redis_key, type) do
    pending = Map.get(state, :compound_promotion_pending, %{})
    worker = Map.get(state, :compound_promotion_worker)

    if Map.has_key?(pending, redis_key) or
         match?(%{redis_key: ^redis_key}, worker) do
      state
    else
      send(self(), {:start_compound_promotion, redis_key, type})
      Map.put(state, :compound_promotion_pending, Map.put(pending, redis_key, type))
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
end
