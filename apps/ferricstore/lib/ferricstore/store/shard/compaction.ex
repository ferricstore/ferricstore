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
      alias Ferricstore.Store.CompactionJournal
      alias Ferricstore.Store.CompactionPlan
      alias Ferricstore.Store.CompactionTombstoneCatalog
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.HintMetadata
      alias Ferricstore.Store.Keydir
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SegmentLock
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
              remove_hint_metadata(hint_path)

            {:error, {:not_found, _}} ->
              remove_hint_metadata(hint_path)

            {:error, reason} ->
              {:halt, {:error, {hint_path, reason}}}
          end
        end)
      end

      defp remove_hint_metadata(hint_path) do
        case HintMetadata.remove(hint_path) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {HintMetadata.metadata_path(hint_path), reason}}}
        end
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

      @compaction_source_page_records 4_096
      @compaction_plan_page_records 512

      defp build_compaction_plan(state, fid, source, dest) do
        with :ok <- prepare_compaction_temp(dest),
             {:ok, catalog} <- CompactionTombstoneCatalog.open(state.shard_data_path, fid) do
          result = build_compaction_plan_with_catalog(state, fid, source, dest, catalog)
          close_result = CompactionTombstoneCatalog.close(catalog)

          case {result, close_result} do
            {{:ok, _plan} = ok, :ok} ->
              ok

            {{:ok, %{plan_path: plan_path}}, {:error, reason}} ->
              _ = CompactionPlan.remove(plan_path)
              remove_compaction_temp(state, dest)
              {:error, {:tombstone_catalog_close_failed, reason}}

            {{:error, _reason} = error, :ok} ->
              error

            {{:error, reason}, {:error, close_reason}} ->
              {:error, {reason, {:tombstone_catalog_close_failed, close_reason}}}
          end
        end
      end

      defp build_compaction_plan_with_catalog(state, fid, source, dest, catalog) do
        with {:ok, tombstone_keys} <-
               catalog_source_tombstones(state, source, catalog, 0, 0),
             :ok <- resolve_tombstone_dependencies(state, fid, catalog, tombstone_keys),
             {:ok, writer} <- CompactionPlan.create(state.shard_data_path, fid) do
          case stream_compaction_source(
                 state,
                 fid,
                 source,
                 dest,
                 catalog,
                 writer,
                 0,
                 0,
                 0
               ) do
            {:ok, live_count, tombstone_count} ->
              case CompactionPlan.finish(writer) do
                {:ok, plan_path} ->
                  {:ok,
                   %{
                     plan_path: plan_path,
                     live_count: live_count,
                     tombstone_count: tombstone_count
                   }}

                {:error, reason} ->
                  remove_compaction_temp(state, dest)
                  {:error, {:plan_finish_failed, reason}}
              end

            {:error, reason} ->
              CompactionPlan.abort(writer)
              remove_compaction_temp(state, dest)
              {:error, reason}
          end
        end
      end

      defp catalog_source_tombstones(state, source, catalog, start_offset, candidate_count) do
        case compaction_scan_source_page(state, source, start_offset) do
          {:ok, records, next_offset, done?}
          when is_list(records) and is_integer(next_offset) and next_offset >= start_offset and
                 is_boolean(done?) ->
            with :ok <-
                   validate_advancing_compaction_scan(records, start_offset, next_offset, done?),
                 {:ok, new_candidates} <-
                   CompactionTombstoneCatalog.record_source_page_count(catalog, records) do
              next_count = candidate_count + new_candidates

              if done? do
                case validate_compaction_scan_end(source, next_offset) do
                  :ok -> {:ok, next_count}
                  {:error, reason} -> {:error, {:copy_failed, reason}}
                end
              else
                catalog_source_tombstones(state, source, catalog, next_offset, next_count)
              end
            end

          {:error, reason} ->
            {:error, {:source_scan_failed, reason}}

          invalid ->
            {:error, {:invalid_source_scan_result, invalid}}
        end
      end

      defp resolve_tombstone_dependencies(_state, _fid, _catalog, 0), do: :ok

      defp resolve_tombstone_dependencies(state, fid, catalog, candidate_count) do
        with {:ok, lower_files} <- lower_compaction_files(state.shard_data_path, fid) do
          started_at = System.monotonic_time()

          result =
            Enum.reduce_while(lower_files, {:ok, 0, 0}, fn
              _file, {:ok, resolved, scanned} when resolved >= candidate_count ->
                {:halt, {:ok, resolved, scanned}}

              {lower_fid, path}, {:ok, resolved, scanned} ->
                case catalog_lower_file(path, lower_fid, catalog, 0, 0) do
                  {:ok, newly_resolved} ->
                    {:cont, {:ok, resolved + newly_resolved, scanned + 1}}

                  {:error, reason} ->
                    {:halt, {:error, reason, resolved, scanned + 1}}
                end
            end)

          emit_streaming_tombstone_scan(
            state,
            fid,
            result,
            started_at,
            length(lower_files),
            candidate_count
          )
        end
      end

      defp lower_compaction_files(shard_path, fid) do
        with {:ok, files} <- Ferricstore.FS.ls(shard_path) do
          lower_files =
            files
            |> Enum.flat_map(fn name ->
              with true <- String.ends_with?(name, ".log"),
                   false <- String.starts_with?(name, "compact_"),
                   {lower_fid, ""} <- Integer.parse(String.trim_trailing(name, ".log")),
                   true <- lower_fid < fid do
                [{lower_fid, Path.join(shard_path, name)}]
              else
                _ -> []
              end
            end)
            |> Enum.sort_by(fn {lower_fid, _path} -> -lower_fid end)

          {:ok, lower_files}
        end
      end

      defp catalog_lower_file(path, fid, catalog, start_offset, resolved_count) do
        case NIF.v2_scan_file_page(path, start_offset, @compaction_source_page_records) do
          {:ok, records, next_offset, done?}
          when is_list(records) and is_integer(next_offset) and next_offset >= start_offset and
                 is_boolean(done?) ->
            with :ok <-
                   validate_advancing_compaction_scan(records, start_offset, next_offset, done?),
                 {:ok, newly_resolved} <-
                   CompactionTombstoneCatalog.observe_lower_page_count(catalog, records, fid) do
              next_count = resolved_count + newly_resolved

              if done? do
                case validate_compaction_scan_end(path, next_offset) do
                  :ok -> {:ok, next_count}
                  {:error, reason} -> {:error, {:lower_segment_scan_failed, fid, reason}}
                end
              else
                catalog_lower_file(path, fid, catalog, next_offset, next_count)
              end
            end

          {:error, reason} ->
            {:error, {:lower_segment_scan_failed, fid, reason}}

          invalid ->
            {:error, {:invalid_lower_segment_scan_result, fid, invalid}}
        end
      end

      defp emit_streaming_tombstone_scan(
             state,
             fid,
             result,
             started_at,
             candidate_files,
             candidate_count
           ) do
        {status, resolved, files_scanned, reason} =
          case result do
            {:ok, resolved, files_scanned} ->
              {:ok, resolved, files_scanned, nil}

            {:error, reason, resolved, files_scanned} ->
              {:error, resolved, files_scanned, reason}
          end

        metadata = %{shard_path: state.shard_data_path, fid: fid, status: status}
        metadata = if reason == nil, do: metadata, else: Map.put(metadata, :reason, reason)

        :telemetry.execute(
          [:ferricstore, :bitcask, :tombstone_dependency_scan],
          %{
            candidate_files: candidate_files,
            files_scanned: files_scanned,
            masked_keys: candidate_count,
            resolved_keys: resolved,
            duration_us:
              System.convert_time_unit(
                System.monotonic_time() - started_at,
                :native,
                :microsecond
              )
          },
          metadata
        )

        case result do
          {:ok, _resolved, _files_scanned} -> :ok
          {:error, reason, _resolved, _files_scanned} -> {:error, reason}
        end
      end

      defp validate_advancing_compaction_scan([], start_offset, next_offset, false),
        do: {:error, {:non_advancing_source_scan, start_offset, next_offset}}

      defp validate_advancing_compaction_scan(_records, start_offset, start_offset, false),
        do: {:error, {:non_advancing_source_scan, start_offset, start_offset}}

      defp validate_advancing_compaction_scan(_records, _start_offset, _next_offset, _done?),
        do: :ok

      defp validate_compaction_scan_end(path, next_offset) do
        case File.lstat(path) do
          {:ok, %File.Stat{type: :regular, size: ^next_offset}} ->
            :ok

          {:ok, %File.Stat{type: :regular, size: size}} when next_offset < size ->
            case NIF.v2_pread_at(path, next_offset) do
              {:error, reason} -> {:error, reason}
              other -> {:error, {:source_scan_stopped_before_eof, next_offset, size, other}}
            end

          {:ok, %File.Stat{type: :regular, size: size}} ->
            {:error, {:source_scan_past_eof, next_offset, size}}

          {:ok, %File.Stat{type: type}} ->
            {:error, {:unsafe_source_file_type, type}}

          {:error, reason} ->
            {:error, {:source_stat_failed, reason}}
        end
      end

      defp stream_compaction_source(
             state,
             fid,
             source,
             dest,
             catalog,
             writer,
             start_offset,
             live_count,
             tombstone_count
           ) do
        case compaction_scan_source_page(state, source, start_offset) do
          {:ok, records, next_offset, done?}
          when is_list(records) and is_integer(next_offset) and next_offset >= start_offset and
                 is_boolean(done?) ->
            with :ok <-
                   validate_advancing_compaction_scan(records, start_offset, next_offset, done?),
                 {:ok, live_entries, tombstone_offsets} <-
                   resolve_compaction_page(state, fid, catalog, records),
                 {:ok, plan_entries} <-
                   copy_compaction_page(
                     state,
                     source,
                     dest,
                     live_entries,
                     tombstone_offsets
                   ),
                 :ok <- CompactionPlan.append(writer, plan_entries) do
              next_live_count = live_count + length(plan_entries)
              next_tombstone_count = tombstone_count + length(tombstone_offsets)

              if done? do
                case validate_compaction_scan_end(source, next_offset) do
                  :ok -> {:ok, next_live_count, next_tombstone_count}
                  {:error, reason} -> {:error, {:copy_failed, reason}}
                end
              else
                stream_compaction_source(
                  state,
                  fid,
                  source,
                  dest,
                  catalog,
                  writer,
                  next_offset,
                  next_live_count,
                  next_tombstone_count
                )
              end
            end

          {:error, reason} ->
            {:error, {:source_scan_failed, reason}}

          invalid ->
            {:error, {:invalid_source_scan_result, invalid}}
        end
      end

      defp compaction_scan_source_page(state, source, start_offset) do
        case Map.get(state, :compaction_scan_page_fun) do
          fun when is_function(fun, 3) ->
            fun.(source, start_offset, @compaction_source_page_records)

          _ ->
            NIF.v2_scan_file_page(source, start_offset, @compaction_source_page_records)
        end
      end

      defp resolve_compaction_page(state, fid, catalog, records) do
        now_ms = Ferricstore.HLC.now_ms()

        with {:ok, markers, _all_tombstones, cold_candidates} <-
               classify_compaction_records(state, fid, records, now_ms),
             {:ok, cold_entries} <- resolve_compaction_cold_entries(state, fid, cold_candidates),
             {:ok, tombstone_offsets} <-
               CompactionTombstoneCatalog.needed_offsets(catalog, records) do
          live_entries =
            markers
            |> Enum.flat_map(fn
              {:hot, _key, _offset} = entry ->
                [entry]

              {:cold, candidate_index} ->
                case Map.fetch(cold_entries, candidate_index) do
                  {:ok, entry} -> [entry]
                  :error -> []
                end

              :dead ->
                []
            end)

          {:ok, live_entries, tombstone_offsets}
        end
      end

      defp classify_compaction_records(state, fid, records, now_ms) do
        records
        |> Enum.reduce_while({:ok, [], [], [], 0}, fn
          {key, offset, _value_size, _expire_at_ms, true},
          {:ok, markers, tombstones, candidates, candidate_index}
          when is_binary(key) and is_integer(offset) and offset >= 0 ->
            {:cont, {:ok, [:dead | markers], [offset | tombstones], candidates, candidate_index}}

          {key, offset, _value_size, _expire_at_ms, false},
          {:ok, markers, tombstones, candidates, candidate_index}
          when is_binary(key) and is_integer(offset) and offset >= 0 ->
            case current_hot_compaction_entry(state, key, fid, offset, now_ms) do
              {:ok, entry} ->
                {:cont, {:ok, [entry | markers], tombstones, candidates, candidate_index}}

              :not_hot ->
                if Hibernation.enabled?() and not :ets.member(state.keydir, key) do
                  candidate = {candidate_index, key, offset}

                  {:cont,
                   {:ok, [{:cold, candidate_index} | markers], tombstones,
                    [candidate | candidates], candidate_index + 1}}
                else
                  {:cont, {:ok, [:dead | markers], tombstones, candidates, candidate_index}}
                end
            end

          invalid, _acc ->
            {:halt, {:error, {:invalid_source_record, invalid}}}
        end)
        |> case do
          {:ok, markers, tombstones, candidates, _candidate_count} ->
            {:ok, Enum.reverse(markers), Enum.reverse(tombstones), Enum.reverse(candidates)}

          {:error, _reason} = error ->
            error
        end
      end

      defp current_hot_compaction_entry(state, key, fid, offset, now_ms) do
        case :ets.lookup(state.keydir, key) do
          [{^key, _value, expire_at_ms, _lfu, ^fid, ^offset, _value_size}]
          when expire_at_ms == 0 or expire_at_ms > now_ms ->
            if shared_compaction_entry?(state, key, fid, fid) do
              {:ok, {:hot, key, offset}}
            else
              :not_hot
            end

          _other ->
            :not_hot
        end
      end

      defp resolve_compaction_cold_entries(_state, _fid, []), do: {:ok, %{}}

      defp resolve_compaction_cold_entries(state, fid, candidates) do
        path = LMDB.path(state.shard_data_path)

        reverse_keys =
          Enum.map(candidates, fn {_index, _key, offset} ->
            LMDB.cold_by_segment_key(fid, offset)
          end)

        with {:ok, reverse_values} <- compaction_lmdb_get_many(state, path, reverse_keys),
             true <- length(reverse_values) == length(candidates),
             {:ok, found} <- collect_compaction_reverse_rows(candidates, reverse_values),
             park_keys <- Enum.map(found, fn {_candidate, park_key} -> park_key end),
             {:ok, park_values} <- compaction_lmdb_get_many(state, path, park_keys),
             true <- length(park_values) == length(found) do
          decode_compaction_cold_rows(fid, found, park_values)
        else
          false -> {:error, :cold_catalog_result_count_mismatch}
          {:error, reason} -> {:error, {:cold_catalog_read_failed, reason}}
          invalid -> {:error, {:invalid_cold_catalog_result, invalid}}
        end
      end

      defp collect_compaction_reverse_rows(candidates, reverse_values) do
        candidates
        |> Enum.zip(reverse_values)
        |> Enum.reduce_while({:ok, []}, fn
          {candidate, {:ok, park_key}}, {:ok, acc} when is_binary(park_key) ->
            {:cont, {:ok, [{candidate, park_key} | acc]}}

          {_candidate, :not_found}, {:ok, acc} ->
            {:cont, {:ok, acc}}

          {candidate, invalid}, _acc ->
            {:halt, {:error, {:invalid_reverse_catalog_row, candidate, invalid}}}
        end)
        |> case do
          {:ok, found} -> {:ok, Enum.reverse(found)}
          {:error, _reason} = error -> error
        end
      end

      defp decode_compaction_cold_rows(fid, found, park_values) do
        found
        |> Enum.zip(park_values)
        |> Enum.reduce_while({:ok, %{}}, fn
          {{{index, key, offset}, park_key}, {:ok, park_blob}}, {:ok, acc}
          when is_binary(park_key) and is_binary(park_blob) ->
            case LMDB.decode_cold_park(park_blob) do
              {:ok,
               %{locator: %Locator{kind: :state, file_id: ^fid, offset: ^offset}, state_key: ^key} =
                   park} ->
                {:cont, {:ok, Map.put(acc, index, {:cold, key, offset, park_key, park})}}

              invalid ->
                {:halt, {:error, {:invalid_cold_park, park_key, invalid}}}
            end

          {{{_index, _key, _offset}, park_key}, :not_found}, _acc ->
            {:halt, {:error, {:cold_park_missing, park_key}}}

          row, _acc ->
            {:halt, {:error, {:invalid_cold_catalog_row, row}}}
        end)
      end

      defp compaction_lmdb_get_many(state, path, keys) do
        case Map.get(state, :compaction_cold_get_many_fun) do
          fun when is_function(fun, 2) -> fun.(path, keys)
          _ -> LMDB.get_many(path, keys)
        end
      end

      defp copy_compaction_page(_state, _source, _dest, [], []), do: {:ok, []}

      defp copy_compaction_page(state, source, dest, live_entries, tombstone_offsets) do
        offsets = Enum.map(live_entries, &compaction_live_offset/1)

        case compaction_copy_records(state, source, dest, offsets, tombstone_offsets) do
          {:ok, results} when length(results) == length(live_entries) ->
            {:ok, Enum.zip_with(live_entries, results, &compaction_plan_entry/2)}

          {:ok, results} ->
            {:error, {:copy_result_mismatch, length(live_entries), length(results)}}

          {:error, reason} ->
            {:error, {:copy_failed, reason}}
        end
      end

      defp compaction_live_offset({:hot, _key, offset}), do: offset
      defp compaction_live_offset({:cold, _key, offset, _park_key, _park}), do: offset

      defp compaction_plan_entry({:hot, key, old_offset}, {new_offset, new_size}),
        do: {:hot, key, old_offset, new_offset, new_size}

      defp compaction_plan_entry(
             {:cold, key, old_offset, park_key, park},
             {new_offset, new_size}
           ),
           do: {:cold, key, old_offset, new_offset, new_size, park_key, park}

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

      defp update_compacted_ets_locations(keydir, fid, plan_path, direction) do
        case CompactionPlan.reduce_pages(
               plan_path,
               @compaction_plan_page_records,
               :ok,
               fn page, :ok ->
                 Enum.each(page, fn
                   {:hot, key, old_offset, new_offset, _new_size} ->
                     {expected_offset, target_offset} =
                       case direction do
                         :forward -> {old_offset, new_offset}
                         :reverse -> {new_offset, old_offset}
                       end

                     _relocated? =
                       Keydir.relocate_exact(
                         keydir,
                         key,
                         fid,
                         expected_offset,
                         target_offset
                       )

                   _cold_entry ->
                     :ok
                 end)

                 :ok
               end
             ) do
          {:ok, :ok} -> :ok
          {:error, _reason} = error -> error
        end
      end

      defp publish_compacted_segment(state, fid, source, dest, plan_path) do
        SegmentLock.with_lock(source, fn ->
          with :ok <- remove_hint_for_file(state.shard_data_path, fid) do
            do_publish_compacted_segment(state, fid, source, dest, plan_path)
          end
        end)
      end

      defp do_publish_compacted_segment(state, fid, source, dest, plan_path) do
        case CompactionJournal.begin(state.shard_data_path, fid, plan_path) do
          {:ok, transaction} ->
            publish_compacted_segment_with_journal(
              state,
              transaction,
              source,
              dest
            )

          {:error, reason} ->
            {:error, {:compaction_journal_begin_failed, reason}}
        end
      end

      defp publish_compacted_segment_with_journal(
             state,
             transaction,
             source,
             dest
           ) do
        case compaction_rename(source, transaction.backup) do
          :ok ->
            publish_compacted_segment_after_backup(
              state,
              transaction,
              source,
              dest
            )

          {:error, reason} ->
            case CompactionJournal.abort_before_swap(transaction) do
              :ok ->
                {:error, reason}

              {:error, abort_reason} ->
                {:error, {:compaction_journal_abort_failed, reason, abort_reason}}
            end
        end
      end

      defp publish_compacted_segment_after_backup(
             state,
             transaction,
             source,
             dest
           ) do
        with :ok <- compaction_rename(dest, source),
             :ok <- CompactionJournal.sync_swap(transaction) do
          case commit_compacted_locations(state, transaction) do
            :ok ->
              case CompactionJournal.complete(transaction) do
                :ok -> :ok
                {:error, reason} -> {:committed_error, {:compaction_finalize_failed, reason}}
              end

            {:error, reason} ->
              rollback_compacted_segment(state, transaction, reason)
          end
        else
          {:error, reason} ->
            rollback_compacted_segment(state, transaction, reason)
        end
      end

      defp commit_compacted_locations(state, transaction) do
        with :ok <- relocate_compacted_cold(state, transaction.plan, :forward),
             :ok <-
               update_compacted_ets_locations(
                 state.keydir,
                 transaction.fid,
                 transaction.plan,
                 :forward
               ),
             :ok <-
               write_compacted_flow_cold_locations(state, [
                 CompactionJournal.marker_op(transaction)
               ]) do
          :ok
        end
      end

      defp rollback_compacted_segment(state, transaction, reason) do
        hot_rollback =
          update_compacted_ets_locations(
            state.keydir,
            transaction.fid,
            transaction.plan,
            :reverse
          )

        journal_rollback = CompactionJournal.rollback(transaction)

        case {hot_rollback, journal_rollback} do
          {:ok, :ok} ->
            {:error, reason}

          {hot_error, :ok} ->
            {:error, {:compaction_hot_rollback_failed, reason, hot_error}}

          {:ok, journal_error} ->
            {:error, {:compaction_rollback_failed, reason, journal_error}}

          {hot_error, journal_error} ->
            {:error,
             {:compaction_rollback_failed, reason, {:hot, hot_error, :journal, journal_error}}}
        end
      end

      defp compaction_rename(from, to) do
        case Ferricstore.FS.rename(from, to) do
          :ok -> :ok
          {:error, reason} -> {:error, {:compaction_rename_failed, from, to, reason}}
        end
      end

      defp relocate_compacted_cold(state, plan_path, direction) do
        CompactionPlan.relocate_cold(
          plan_path,
          LMDB.path(state.shard_data_path),
          direction,
          get_many_fun: fn path, keys -> compaction_lmdb_get_many(state, path, keys) end,
          write_fun: fn
            _path, [] -> :ok
            path, ops -> write_compacted_flow_cold_locations(state, path, ops)
          end
        )
      end

      defp write_compacted_flow_cold_locations(state, ops) do
        write_compacted_flow_cold_locations(state, LMDB.path(state.shard_data_path), ops)
      end

      defp write_compacted_flow_cold_locations(state, path, ops) do
        case Map.get(state, :compaction_cold_write_fun) do
          fun when is_function(fun, 2) ->
            fun.(path, ops)

          _ ->
            LMDB.write_batch(path, ops)
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
