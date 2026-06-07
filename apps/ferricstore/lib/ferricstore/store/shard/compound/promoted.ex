defmodule Ferricstore.Store.Shard.Compound.Promoted do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{ColdRead, CompoundKey, LFU, Promotion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Compound.Support

  require Logger

  @record_header_size 26
  @promoted_frag_threshold 0.5
  @promoted_dead_bytes_min 1_048_576
  @promoted_compaction_cooldown_ms 30_000
  @cold_batch_read_timeout_ms 10_000

  defp promoted_record_size(compound_key, value) when is_binary(value) do
    @record_header_size + byte_size(compound_key) + byte_size(value)
  end

  @spec promoted_store(map(), binary()) :: binary() | nil
  @doc false
  def promoted_store(state, redis_key) do
    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      %{path: path} -> path
      path when is_binary(path) -> path
      nil -> marker_promoted_store(state, redis_key)
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

  # Type and promotion metadata are authoritative in the shared shard log.
  # Promoted H/S/Z data rows use dedicated Bitcask files, but metadata rows
  # keep shared-log offsets and must not be read through the dedicated path.
  def shared_log_compound_key?(<<"T:", _rest::binary>>), do: true
  def shared_log_compound_key?(<<"PM:", _rest::binary>>), do: true
  def shared_log_compound_key?(_key), do: false

  def tombstone_and_delete_keys(state, []), do: {:ok, state}

  def tombstone_and_delete_keys(state, keys) do
    next_state =
      Enum.reduce(keys, state, fn key, acc_state ->
        ShardFlush.track_delete_dead_bytes(acc_state, key)
      end)

    case append_tombstone_batch_sync(next_state.active_file_path, keys) do
      {:ok, _locations} ->
        Enum.each(keys, fn key -> ShardETS.ets_delete_key(next_state, key) end)
        {:ok, next_state}

      {:error, reason} ->
        {{:error, reason}, next_state}
    end
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

        case Support.read_cold_async(state, file_path, offset, compound_key) do
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

    case NIF.v2_append_ops_batch_nosync(path, ops) do
      {:ok, locations} ->
        with :ok <- validate_tombstone_locations(locations, length(keys)),
             :ok <- NIF.v2_fsync(path) do
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
        |> Enum.reduce(0, fn name, acc ->
          case dedicated_log_file_id(name) do
            {:ok, _fid} ->
              case File.stat(Path.join(dir_path, name)) do
                {:ok, %{size: s}} -> acc + s
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

                  rollback_new_active_file(state, dedicated_path, new_file)

                  fail_dedicated_compaction(
                    state,
                    redis_key,
                    dedicated_path,
                    :collect_live_entries,
                    reason
                  )
              end

            {:error, reason} ->
              rollback_new_active_file(state, dedicated_path, new_file)
              fail_dedicated_compaction(state, redis_key, dedicated_path, :create_active, reason)
          end

        {:error, reason} ->
          fail_dedicated_compaction(state, redis_key, dedicated_path, :sync_old_active, reason)
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
        {:error, reason} ->
          fail_dedicated_compaction(state, redis_key, dedicated_path, :remove_old_logs, reason)
      end
    else
      batch = Enum.map(live_entries, fn {k, v, exp} -> {k, v, exp} end)

      case NIF.v2_append_batch(new_file, batch) do
        {:ok, results} when length(results) == length(live_entries) ->
          ref = Support.keydir_binary_ref(state)

          live_entries
          |> Enum.zip(results)
          |> Enum.each(fn {{key, value, expire_at_ms}, {offset, value_size}} ->
            value_for_ets = ShardETS.value_for_ets(value, ShardETS.hot_cache_threshold(state))
            Support.track_binary_insert(ref, state, key, value_for_ets)

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
            {:error, reason} ->
              fail_dedicated_compaction(
                state,
                redis_key,
                dedicated_path,
                :remove_old_logs,
                reason
              )
          end

        {:ok, results} ->
          Logger.error(
            "Shard #{state.index}: dedicated compaction append result mismatch: expected #{length(live_entries)}, got #{length(results)}"
          )

          rollback_new_active_file(state, dedicated_path, new_file)

          fail_dedicated_compaction(
            state,
            redis_key,
            dedicated_path,
            :append,
            {:append_result_mismatch, length(live_entries), length(results)}
          )

        {:error, reason} ->
          Logger.error(
            "Shard #{state.index}: dedicated compaction write failed: #{inspect(reason)}"
          )

          # Roll back the `touch!(new_file)` on write error. Fsync
          # so the rollback survives a subsequent crash.
          rollback_new_active_file(state, dedicated_path, new_file)
          fail_dedicated_compaction(state, redis_key, dedicated_path, :append, reason)
      end
    end
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
    case Ferricstore.FS.ls(dedicated_path) do
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

      _ ->
        :ok
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

    # Promotion is a one-time structural migration per collection. Keeping it
    # inline preserves the current crash-safe semantics, but it can add a cold
    # create latency spike when a large hash/set/zset first crosses the
    # threshold. If that p99 path becomes important, move promotion to a
    # background job that keeps reads on shared compound keys until the dedicated
    # copy and marker are fully durable. Do not prioritize that over steady-state
    # score-index work for long-lived hot sorted sets.
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
end
