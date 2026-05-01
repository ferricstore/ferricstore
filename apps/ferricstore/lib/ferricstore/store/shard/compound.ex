defmodule Ferricstore.Store.Shard.Compound do
  @moduledoc "Compound-key CRUD, prefix scan/count, promoted-collection dedicated storage, and automatic compaction."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{LFU, Promotion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  require Logger

  # Record header size for dead byte accounting (same as @bitcask_header_size).
  @record_header_size 26

  # Promoted (dedicated) compaction thresholds.
  @promoted_frag_threshold 0.5
  @promoted_dead_bytes_min 1_048_576
  @promoted_compaction_cooldown_ms 30_000

  # -------------------------------------------------------------------
  # Compound key handle_call handlers
  # -------------------------------------------------------------------

  @spec handle_compound_get(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
      nil ->
        case ShardETS.ets_lookup_warm(state, compound_key) do
          {:hit, value, _exp} ->
            {:reply, value, state}

          :expired ->
            {:reply, nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {:reply, ShardETS.warm_from_store(state, compound_key), state}
            else
              {:reply, nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, _exp} ->
            {:reply, value, state}

          :expired ->
            {:reply, nil, state}

          _cold_or_miss ->
            case promoted_read(dedicated_path, compound_key, state.keydir) do
              {:ok, nil} ->
                {:reply, nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {:reply, value, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {:reply, value, state}

              _error ->
                {:reply, nil, state}
            end
        end
    end
  end

  @spec handle_compound_get_meta(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_get_meta(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
      nil ->
        case ShardETS.ets_lookup_warm(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {:reply, {value, expire_at_ms}, state}

          :expired ->
            {:reply, nil, state}

          :miss ->
            if ShardETS.pending_cold?(state, compound_key) do
              state = ShardFlush.flush_pending_for_read(state)
              {:reply, ShardETS.warm_meta_from_store(state, compound_key), state}
            else
              {:reply, nil, state}
            end
        end

      dedicated_path ->
        case ShardETS.ets_lookup(state, compound_key) do
          {:hit, value, expire_at_ms} ->
            {:reply, {value, expire_at_ms}, state}

          :expired ->
            {:reply, nil, state}

          _cold_or_miss ->
            case promoted_read(dedicated_path, compound_key, state.keydir) do
              {:ok, nil} ->
                {:reply, nil, state}

              {:ok, value, exp} ->
                ShardETS.ets_insert(state, compound_key, value, exp)
                {:reply, {value, exp}, state}

              {:ok, value} ->
                ShardETS.ets_insert(state, compound_key, value, 0)
                {:reply, {value, 0}, state}

              _error ->
                {:reply, nil, state}
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

        results = ShardETS.prefix_scan_entries(state.keydir, prefix, state.shard_data_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}

      dedicated_path ->
        results = ShardETS.prefix_scan_entries(state.keydir, prefix, dedicated_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
    end
  end

  @spec handle_compound_count(binary(), binary(), map()) :: {:reply, non_neg_integer(), map()}
  @doc false
  def handle_compound_count(redis_key, prefix, state) do
    case promoted_store(state, redis_key) do
      nil ->
        {:reply, ShardETS.prefix_count_entries(state.keydir, prefix), state}

      _dedicated_path ->
        {:reply, ShardETS.prefix_count_entries(state.keydir, prefix), state}
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
    case promoted_store(state, redis_key) do
      nil ->
        result =
          Ferricstore.Raft.Batcher.write(state.index, {:put, compound_key, value, expire_at_ms})

        new_version = state.write_version + 1

        case result do
          :ok ->
            new_state = %{state | write_version: new_version}
            new_state = maybe_promote(new_state, redis_key, compound_key)
            {:reply, :ok, new_state}

          {:error, _} = err ->
            {:reply, err, state}
        end

      dedicated_path ->
        case promoted_write(dedicated_path, compound_key, value, expire_at_ms) do
          {:ok, {fid, offset, record_size}} ->
            state = track_promoted_dead_bytes(state, redis_key, compound_key, record_size)

            ShardETS.ets_insert_with_location(
              state,
              compound_key,
              value,
              expire_at_ms,
              fid,
              offset,
              record_size
            )

            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state) do
    case promoted_store(state, redis_key) do
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

            ShardETS.ets_insert_with_location(
              state,
              compound_key,
              value,
              expire_at_ms,
              fid,
              offset,
              record_size
            )

            {:reply, :ok, bump_promoted_writes(state, redis_key)}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_delete_raft(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
      nil ->
        result = Ferricstore.Raft.Batcher.write(state.index, {:delete, compound_key})
        new_version = state.write_version + 1

        case result do
          :ok -> {:reply, :ok, %{state | write_version: new_version}}
          {:error, _} = err -> {:reply, err, state}
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

  defp handle_compound_delete_direct(redis_key, compound_key, state) do
    case promoted_store(state, redis_key) do
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
    case promoted_store(state, redis_key) do
      nil ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key ->
          Ferricstore.Raft.Batcher.write(state.index, {:delete, key})
        end)

        new_version = state.write_version + 1
        {:reply, :ok, %{state | write_version: new_version}}

      _dedicated ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ShardETS.ets_delete_key(state, key) end)

        Promotion.cleanup_promoted!(
          redis_key,
          state.shard_data_path,
          state.keydir,
          state.data_dir,
          state.index
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)
        {:reply, :ok, %{state | promoted_instances: new_promoted}}
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
          state.index
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

  @spec promoted_read(binary(), binary(), :ets.tid()) ::
          {:ok, binary() | nil} | {:ok, binary(), non_neg_integer()} | {:error, term()}
  @doc false
  def promoted_read(dedicated_path, compound_key, keydir) do
    now = HLC.now_ms()

    case :ets.lookup(keydir, compound_key) do
      [{^compound_key, _value, exp, _lfu, fid, offset, _vsize}]
      when (exp == 0 or exp > now) and is_integer(fid) and is_integer(offset) and offset >= 0 ->
        file_path = dedicated_file_path(dedicated_path, fid)

        case NIF.v2_pread_at(file_path, offset) do
          {:ok, value} -> {:ok, value, exp}
          other -> other
        end

      [{^compound_key, _value, _exp, _lfu, _fid, _offset, _vsize}] ->
        :ets.delete(keydir, compound_key)
        {:ok, nil}

      _ ->
        active = Promotion.find_active(dedicated_path)

        case NIF.v2_scan_file(active) do
          {:ok, records} ->
            last_entry =
              records
              |> Enum.filter(fn {k, _off, _vsize, _exp, _is_tomb} -> k == compound_key end)
              |> List.last()

            case last_entry do
              nil ->
                {:ok, nil}

              {_key, _offset, _vsize, _exp, true} ->
                {:ok, nil}

              {_key, offset, _vsize, exp, false} ->
                if exp == 0 or exp > now do
                  case NIF.v2_pread_at(active, offset) do
                    {:ok, value} -> {:ok, value, exp}
                    other -> other
                  end
                else
                  {:ok, nil}
                end
            end

          {:error, reason} ->
            {:error, reason}
        end
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
            System.system_time(:millisecond) - last >= @promoted_compaction_cooldown_ms

        if frag >= @promoted_frag_threshold and dead >= @promoted_dead_bytes_min and cooldown_ok do
          state = compact_dedicated(state, redis_key, path)
          new_total = promoted_dir_size(path)

          new_info = %{
            info
            | dead_bytes: 0,
              total_bytes: new_total,
              last_compacted_at: System.system_time(:millisecond)
          }

          new_promoted = Map.put(state.promoted_instances, redis_key, new_info)
          %{state | promoted_instances: new_promoted}
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
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}] when old_vsize > 0 ->
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
            [{^compound_key, _v, _exp, _lfu, _fid, _off, old_vsize}] when old_vsize > 0 ->
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
    alias Ferricstore.Store.CompoundKey

    prefix = promoted_prefix_for(state, redis_key)

    if prefix == nil do
      Logger.warning(
        "Shard #{state.index}: cannot determine prefix for promoted key #{inspect(redis_key)}, skipping compaction"
      )

      state
    else
      active = Promotion.find_active(dedicated_path)
      # Sync outgoing active before we stop writing to it, so any last
      # pre-compaction bytes are durable regardless of when the page
      # cache writes back.
      _ = NIF.v2_fsync(active)

      old_fid = parse_fid_from_path(active)
      new_fid = old_fid + 1
      new_file = dedicated_file_path(dedicated_path, new_fid)
      Ferricstore.FS.touch!(new_file)
      _ = NIF.v2_fsync_dir(dedicated_path)

      now = HLC.now_ms()

      live_entries =
        :ets.foldl(
          fn {key, value, exp, _lfu, _fid, _off, _vsize}, acc ->
            if is_binary(key) and value != nil and String.starts_with?(key, prefix) and
                 (exp == 0 or exp > now) do
              [{key, value, exp} | acc]
            else
              acc
            end
          end,
          [],
          state.keydir
        )

      if live_entries == [] do
        # Roll back the `touch!(new_file)` above: remove the empty
        # placeholder and fsync the dir so no zombie empty file
        # remains after a crash.
        _ = Ferricstore.FS.rm(new_file)
        _ = NIF.v2_fsync_dir(dedicated_path)
        state
      else
        batch = Enum.map(live_entries, fn {k, v, exp} -> {k, v, exp} end)

        case NIF.v2_append_batch(new_file, batch) do
          {:ok, results} ->
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

            case Ferricstore.FS.ls(dedicated_path) do
              {:ok, files} ->
                Enum.each(files, fn name ->
                  if String.ends_with?(name, ".log") do
                    fid = name |> String.trim_trailing(".log") |> String.to_integer()

                    if fid < new_fid do
                      _ = Ferricstore.FS.rm(Path.join(dedicated_path, name))
                    end
                  end
                end)

                _ = NIF.v2_fsync_dir(dedicated_path)

              _ ->
                :ok
            end

            Logger.debug(
              "Shard #{state.index}: compacted dedicated #{inspect(redis_key)} " <>
                "(#{length(live_entries)} live entries, fid #{old_fid} -> #{new_fid})"
            )

            :telemetry.execute(
              [:ferricstore, :dedicated, :compaction],
              %{live_entries: length(live_entries), old_fid: old_fid, new_fid: new_fid},
              %{shard_index: state.index, redis_key: redis_key}
            )

            state

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: dedicated compaction write failed: #{inspect(reason)}"
            )

            # Roll back the `touch!(new_file)` on write error. Fsync
            # so the rollback survives a subsequent crash.
            _ = Ferricstore.FS.rm(new_file)
            _ = NIF.v2_fsync_dir(dedicated_path)
            state
        end
      end
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
          count = ShardETS.prefix_count_entries(state.keydir, prefix)

          if count > threshold do
            state = ShardFlush.await_in_flight(state)
            state = ShardFlush.flush_pending_sync(state)

            case Promotion.promote_collection!(
                   type,
                   redis_key,
                   state.shard_data_path,
                   state.keydir,
                   state.data_dir,
                   state.index
                 ) do
              {:ok, dedicated_store} ->
                new_promoted =
                  Map.put(state.promoted_instances, redis_key, %{
                    path: dedicated_store,
                    writes: 0,
                    total_bytes: 0,
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

  # -- Off-heap binary byte tracking --

  defp keydir_binary_ref(%{instance_ctx: %{keydir_binary_bytes: ref}}) when ref != nil, do: ref

  defp keydir_binary_ref(_) do
    try do
      ctx = FerricStore.Instance.get(:default)
      ctx && ctx.keydir_binary_bytes
    rescue
      _ -> nil
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
