defmodule Ferricstore.Store.Shard.Compaction do
  @moduledoc """
  Promoted Bitcask compaction helpers injected into `Ferricstore.Store.Shard`.

  Promoted hashes/sets/zsets keep large collection entries in dedicated Bitcask
  logs. These helpers compact those dedicated logs, preserve live compound
  entries, and remove obsolete/tombstone-only files when safe.

  ## Performance boundary

  Compaction is cold/control-plane compared with GET/SET/Flow writes, but it
  runs inside shard state and touches keydir metadata. Keep correctness first:
  do not add request-path calls here, and keep compaction scheduling separate
  from per-command write latency.
  """

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.LMDB
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Raft.Backend, as: RaftBackend
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Writes, as: ShardWrites
      alias Ferricstore.Store.Shard.ZSetIndex
      require Logger

      defp tombstone_file?(path) do
        case NIF.v2_scan_tombstones(path) do
          {:ok, [_ | _]} -> true
          _ -> false
        end
      end

      defp remove_compacted_source(state, source) do
        case Ferricstore.FS.rm(source) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: compaction failed to remove source #{source}: #{inspect(reason)}"
            )

            {:error, reason}
        end
      end

      # A tombstone-only file protects deleted keys only when older log history
      # still contains a live version of one of those keys. Keeping the file just
      # because any lower fid exists leaks tombstone-only logs for unrelated keys.
      defp tombstone_file_still_needed?(shard_path, fid, tombstone_path) do
        with {:ok, tombstones} <- NIF.v2_scan_tombstones(tombstone_path),
             masked_keys =
               MapSet.new(tombstones, fn {key, _offset, _record_size, _expire_at_ms} -> key end),
             false <- MapSet.size(masked_keys) == 0,
             {:ok, files} <- Ferricstore.FS.ls(shard_path),
             {:ok, states} <- scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
          Enum.any?(masked_keys, fn key -> Map.get(states, key) == :live end)
        else
          true -> false
          _ -> true
        end
      end

      defp scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
        candidate_files =
          files
          |> Enum.flat_map(fn name ->
            with true <- String.ends_with?(name, ".log"),
                 false <- String.starts_with?(name, "compact_"),
                 {other_fid, ""} <- Integer.parse(String.trim_trailing(name, ".log")),
                 true <- other_fid < fid do
              [{other_fid, Path.join(shard_path, name)}]
            else
              _ -> []
            end
          end)
          |> Enum.sort_by(fn {other_fid, _path} -> -other_fid end)

        started_at = System.monotonic_time()
        masked_key_count = MapSet.size(masked_keys)

        now_ms = Ferricstore.HLC.now_ms()

        result =
          candidate_files
          |> Enum.reduce_while({:ok, %{}, masked_keys, 0}, fn {_other_fid, path},
                                                              {:ok, states, unresolved_keys,
                                                               files_scanned} ->
            next_files_scanned = files_scanned + 1

            case NIF.v2_scan_key_states(path, MapSet.to_list(unresolved_keys)) do
              {:ok, records} ->
                file_states =
                  Enum.reduce(records, %{}, fn {key, expire_at_ms, tombstone?}, acc ->
                    Map.put(
                      acc,
                      key,
                      tombstone_dependency_state(tombstone?, expire_at_ms, now_ms)
                    )
                  end)

                next_states = Map.merge(states, file_states)

                next_unresolved_keys =
                  Enum.reduce(Map.keys(file_states), unresolved_keys, &MapSet.delete(&2, &1))

                if MapSet.size(next_unresolved_keys) == 0 do
                  {:halt, {:ok, next_states, next_unresolved_keys, next_files_scanned}}
                else
                  {:cont, {:ok, next_states, next_unresolved_keys, next_files_scanned}}
                end

              {:error, reason} ->
                {:halt, {:error, reason, next_files_scanned}}
            end
          end)

        case result do
          {:ok, states, unresolved_keys, files_scanned} ->
            emit_tombstone_dependency_scan(
              shard_path,
              fid,
              :ok,
              started_at,
              length(candidate_files),
              files_scanned,
              masked_key_count,
              masked_key_count - MapSet.size(unresolved_keys)
            )

            {:ok, states}

          {:error, reason, files_scanned} ->
            emit_tombstone_dependency_scan(
              shard_path,
              fid,
              :error,
              started_at,
              length(candidate_files),
              files_scanned,
              masked_key_count,
              0,
              reason
            )

            {:error, reason}
        end
      end

      defp emit_tombstone_dependency_scan(
             shard_path,
             fid,
             status,
             started_at,
             candidate_files,
             files_scanned,
             masked_keys,
             resolved_keys,
             reason \\ nil
           ) do
        metadata = %{
          shard_path: shard_path,
          fid: fid,
          status: status
        }

        metadata =
          if reason == nil do
            metadata
          else
            Map.put(metadata, :reason, reason)
          end

        :telemetry.execute(
          [:ferricstore, :bitcask, :tombstone_dependency_scan],
          %{
            candidate_files: candidate_files,
            files_scanned: files_scanned,
            masked_keys: masked_keys,
            resolved_keys: resolved_keys,
            duration_us:
              System.convert_time_unit(
                System.monotonic_time() - started_at,
                :native,
                :microsecond
              )
          },
          metadata
        )
      end

      defp tombstone_offsets(path) do
        case NIF.v2_scan_tombstones(path) do
          {:ok, tombstones} ->
            Enum.map(tombstones, fn {_key, offset, _record_size, _expire_at_ms} -> offset end)

          _ ->
            []
        end
      end

      defp needed_tombstone_offsets(shard_path, fid, path) do
        with {:ok, tombstones} <- NIF.v2_scan_tombstones(path),
             false <- tombstones == [],
             tombstone_by_key =
               Map.new(tombstones, fn {key, offset, _record_size, _expire_at_ms} ->
                 {key, offset}
               end),
             masked_keys = Map.keys(tombstone_by_key) |> MapSet.new(),
             {:ok, files} <- Ferricstore.FS.ls(shard_path),
             {:ok, states} <- scan_lower_tombstone_key_states(shard_path, files, fid, masked_keys) do
          tombstone_by_key
          |> Enum.filter(fn {key, _offset} -> Map.get(states, key) == :live end)
          |> Enum.map(fn {_key, offset} -> offset end)
        else
          true ->
            []

          _ ->
            tombstone_offsets(path)
        end
      end

      defp remove_hint_for_file(shard_path, fid) do
        # Compaction rewrites or invalidates offsets in the paired log file.
        # Dropping every numeric alias for this fid forces startup to scan the log
        # instead of trusting stale offsets that can hide or resurrect keys.
        with {:ok, files} <- Ferricstore.FS.ls(shard_path),
             hint_names = hint_names_for_file(files, fid),
             :ok <- remove_hint_files(shard_path, hint_names) do
          :ok
        else
          {:error, reason} = error ->
            Logger.warning(
              "failed to remove stale compaction hint file(s) for fid #{fid} under #{shard_path}: #{inspect(reason)}"
            )

            error
        end
      end

      defp hint_names_for_file(files, fid) do
        files
        |> Enum.filter(&(hint_file_id(&1) == fid))
        |> case do
          [] -> ["#{String.pad_leading(Integer.to_string(fid), 5, "0")}.hint"]
          names -> names
        end
      end

      defp hint_file_id(name) do
        with true <- String.ends_with?(name, ".hint"),
             false <- String.starts_with?(name, "compact_"),
             stem <- String.trim_trailing(name, ".hint"),
             {parsed, ""} <- Integer.parse(stem),
             true <- parsed >= 0 do
          parsed
        else
          _ -> nil
        end
      end

      defp remove_hint_files(shard_path, hint_names) do
        Enum.reduce_while(hint_names, :ok, fn hint_name, :ok ->
          hint_path = Path.join(shard_path, hint_name)

          case Ferricstore.FS.rm(hint_path) do
            :ok ->
              {:cont, :ok}

            {:error, {:not_found, _}} ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {hint_path, reason}}}
          end
        end)
      end

      defp remove_compaction_temp(state, path) do
        case Ferricstore.FS.rm(path) do
          :ok ->
            :ok

          {:error, {:not_found, _}} ->
            :ok

          {:error, reason} ->
            Logger.warning(
              "Shard #{state.index}: failed to remove compaction temp file #{path}: #{inspect(reason)}"
            )
        end
      end

      defp prepare_compaction_temp(path) do
        case Ferricstore.FS.rm(path) do
          :ok -> :ok
          {:error, {:not_found, _}} -> :ok
          {:error, reason} -> {:error, reason}
        end
      end

      defp group_compaction_live_entries(_state, []), do: %{}

      defp group_compaction_live_entries(state, file_ids) do
        target_fids = MapSet.new(file_ids)
        now_ms = Ferricstore.HLC.now_ms()

        hot_groups =
          :ets.foldl(
            fn
              {key, _value, expire_at_ms, _lfu, fid, off, _vsize}, acc
              when expire_at_ms == 0 or expire_at_ms > now_ms ->
                if MapSet.member?(target_fids, fid) and fid != state.active_file_id and
                     shared_compaction_entry?(state, key, fid, fid) do
                  Map.update(acc, fid, [{key, off}], &[{key, off} | &1])
                else
                  acc
                end

              {_key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, acc ->
                acc
            end,
            %{},
            state.keydir
          )

        merge_compaction_entry_groups(
          hot_groups,
          group_compaction_cold_flow_entries(state, file_ids)
        )
      end

      defp group_compaction_cold_flow_entries(state, file_ids) do
        if Hibernation.enabled?() do
          hot_keys = compaction_hot_key_set(state)
          path = LMDB.path(state.shard_data_path)

          Enum.reduce(file_ids, %{}, fn fid, acc ->
            if fid == state.active_file_id do
              acc
            else
              prefix = LMDB.cold_by_segment_prefix(fid)

              case LMDB.prefix_entries(path, prefix, 100_000) do
                {:ok, entries} ->
                  entries
                  |> Enum.reduce(acc, fn {_reverse_key, park_key}, inner ->
                    case compaction_cold_flow_entry(state, path, fid, park_key, hot_keys) do
                      {:ok, entry} -> Map.update(inner, fid, [entry], &[entry | &1])
                      :skip -> inner
                    end
                  end)

                _ ->
                  acc
              end
            end
          end)
        else
          %{}
        end
      end

      defp compaction_hot_key_set(state) do
        :ets.foldl(
          fn {key, _value, _expire_at_ms, _lfu, _fid, _off, _vsize}, acc ->
            MapSet.put(acc, key)
          end,
          MapSet.new(),
          state.keydir
        )
      end

      defp compaction_cold_flow_entry(_state, _path, _fid, park_key, _hot_keys)
           when not is_binary(park_key),
           do: :skip

      defp compaction_cold_flow_entry(_state, path, fid, park_key, hot_keys) do
        with {:ok, park_blob} <- LMDB.get(path, park_key),
             {:ok,
              %{locator: %Locator{kind: :state, file_id: ^fid} = locator, state_key: state_key} =
                park} <-
               LMDB.decode_cold_park(park_blob),
             true <- is_binary(state_key),
             false <- MapSet.member?(hot_keys, state_key) do
          {:ok, {state_key, locator.offset, {:cold_flow, park_key, park}}}
        else
          _ -> :skip
        end
      end

      defp merge_compaction_entry_groups(left, right) do
        Map.merge(left, right, fn _fid, left_entries, right_entries ->
          right_entries ++ left_entries
        end)
      end

      defp compaction_entry_offset({_key, offset}) when is_integer(offset), do: offset
      defp compaction_entry_offset({_key, offset, _meta}) when is_integer(offset), do: offset

      defp tombstone_dependency_state(true, _expire_at_ms, _now_ms), do: :tombstone
      defp tombstone_dependency_state(false, 0, _now_ms), do: :live

      defp tombstone_dependency_state(false, expire_at_ms, now_ms) when expire_at_ms > now_ms,
        do: :live

      defp tombstone_dependency_state(false, _expire_at_ms, _now_ms), do: :expired

      defp shared_compaction_entry?(state, key, fid, target_fid) do
        fid == target_fid and not promoted_data_compound_entry?(state, key)
      end

      # Promoted collection data is stored in dedicated Bitcask dirs but reuses the
      # same ETS location tuple shape. Shared-log compaction must not interpret
      # those file ids and offsets as shared-log locations.
      defp promoted_data_compound_entry?(state, <<"H:", _rest::binary>> = key),
        do: promoted_parent?(state, key)

      defp promoted_data_compound_entry?(state, <<"S:", _rest::binary>> = key),
        do: promoted_parent?(state, key)

      defp promoted_data_compound_entry?(state, <<"Z:", _rest::binary>> = key),
        do: promoted_parent?(state, key)

      defp promoted_data_compound_entry?(_state, _key), do: false

      defp promoted_parent?(state, compound_key) do
        redis_key = CompoundKey.extract_redis_key(compound_key)
        Map.has_key?(state.promoted_instances, redis_key)
      end

      defp update_compacted_ets_locations(keydir, fid, live_entries, results) do
        Enum.zip(live_entries, results)
        |> Enum.each(fn
          {{key, old_offset}, {new_offset, _new_size}} ->
            update_compacted_ets_location(keydir, fid, key, old_offset, new_offset)

          {{key, old_offset, _meta}, {new_offset, _new_size}} ->
            update_compacted_ets_location(keydir, fid, key, old_offset, new_offset)
        end)
      end

      defp update_compacted_ets_location(keydir, fid, key, old_offset, new_offset) do
        case :ets.lookup(keydir, key) do
          [{^key, _value, _exp, _lfu, ^fid, ^old_offset, _vsize}] ->
            :ets.update_element(keydir, key, {6, new_offset})

          _ ->
            :ok
        end
      end

      defp update_compacted_flow_cold_locations(state, live_entries, results) do
        {ops, errors} =
          live_entries
          |> Enum.zip(results)
          |> Enum.reduce({[], []}, fn
            {{_state_key, _old_offset,
              {:cold_flow, park_key, %{locator: %Locator{} = locator} = park}},
             {new_offset, new_size}},
            {ops_acc, errors_acc} ->
              old_row = %{locator: locator, park: park, park_key: park_key}

              with {:ok, new_row} <-
                     Hibernation.relocate_cold_row(old_row,
                       offset: new_offset,
                       value_size: new_size
                     ),
                   {:ok, row_ops} <- Hibernation.cold_compaction_ops(old_row, new_row) do
                {row_ops ++ ops_acc, errors_acc}
              else
                error -> {ops_acc, [{park_key, error} | errors_acc]}
              end

            _other, acc ->
              acc
          end)

        case {ops, errors} do
          {_ops, [_ | _]} ->
            {:error, {:cold_flow_compaction_relocation_failed, Enum.reverse(errors)}}

          {[], []} ->
            :ok

          {[_ | _], []} ->
            LMDB.write_batch(LMDB.path(state.shard_data_path), Enum.reverse(ops))
        end
      end

      # -------------------------------------------------------------------
      # handle_info
      # -------------------------------------------------------------------

      @impl true
      # Handle pending writes from tx_execute. These are queued via send/2
      # during transaction execution to persist ETS-only writes to Bitcask.
    end
  end
end
