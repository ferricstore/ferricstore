defmodule Ferricstore.Store.Shard.Startup do
  @moduledoc false

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
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
      alias Ferricstore.Store.Shard.Reads, as: ShardReads
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Writes, as: ShardWrites
      alias Ferricstore.Store.Shard.ZSetIndex
      require Logger
      @impl true

      def init(opts) do
        # Supervised shutdown reaches terminate/2 only when the GenServer traps
        # exits. That path drains pending writes and writes the active hint file.
        Process.flag(:trap_exit, true)

        try do
          index = Keyword.fetch!(opts, :index)
          data_dir = Keyword.fetch!(opts, :data_dir)
          flush_ms = Keyword.get(opts, :flush_interval_ms, @flush_interval_ms)
          ctx = Keyword.get(opts, :instance_ctx)
          fsync_dir_fun = Keyword.get(opts, :fsync_dir_fun, &NIF.v2_fsync_dir/1)

          apply_context =
            case ctx do
              %{apply_context: %Ferricstore.Raft.ApplyContext{} = context} ->
                context

              _missing_context ->
                Ferricstore.Raft.ApplyContext.from_runtime()
            end

          apply_context_encoded = Ferricstore.Raft.ApplyContext.encode(apply_context)

          release_cursor_interval =
            Keyword.get_lazy(opts, :release_cursor_interval, fn ->
              Application.get_env(:ferricstore, :release_cursor_interval, 200_000)
            end)

          flow_async_history =
            Keyword.get_lazy(opts, :flow_async_history, &flow_async_history_enabled?/0)

          flow_shared_ref_backfill? = Keyword.get(opts, :flow_shared_ref_backfill?, true)

          if ctx && !Ferricstore.ReplicationMode.raft?() do
            :ok = Ferricstore.Store.StandaloneTxLog.recover_once(data_dir)
          end

          path = Ferricstore.DataDir.shard_data_path(data_dir, index)

          {active_file_id, active_file_size, active_file_path} =
            ensure_initial_files!(path, index, fsync_dir_fun)

          # Create/clear named ETS tables.
          # Use instance-scoped names from ctx if available, else default naming.
          keydir_name =
            if ctx, do: elem(ctx.keydir_refs, index), else: :"keydir_#{index}"

          keydir = prepare_startup_keydir(keydir_name, ctx, index)

          # Remove any leftover hot_cache table from a previous run.
          case :ets.whereis(:"hot_cache_#{index}") do
            :undefined -> :ok
            _ref -> :ets.delete(:"hot_cache_#{index}")
          end

          instance_name = if ctx, do: ctx.name, else: :default
          compound_member_index = CompoundMemberIndex.table_name(instance_name, index)
          CompoundMemberIndex.ensure_table!(compound_member_index)
          {zset_score_index, zset_score_lookup} = ZSetIndex.table_names(instance_name, index)
          ensure_zset_index_table!(zset_score_index, :ordered_set)
          ensure_zset_index_table!(zset_score_lookup, :set)
          {flow_index, flow_lookup} = NativeFlowIndex.table_names(instance_name, index)

          # v2: recover ETS keydir from hint files or by scanning log files BEFORE
          # starting Raft. This ensures cold entries ({key, nil, ..., fid, off, vsize})
          # are in ETS when ra replays WAL entries via apply/3. Without this, replayed
          # read-modify-write commands (INCR, APPEND, etc.) see ETS misses during
          # replay and start from nil instead of the correct prior value.
          # 7-tuple format: {key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}
          # Must run BEFORE recover_promoted so PM: markers are in ETS.
          profile_startup_phase(index, :recover_keydir, fn ->
            unless raft_projection_owner?(ctx) do
              ShardLifecycle.recover_keydir(path, keydir, index, ctx)
            end

            :ok
          end)

          profile_startup_phase(index, :compound_member_index_rebuild, fn ->
            unless raft_projection_owner?(ctx) do
              CompoundMemberIndex.rebuild(compound_member_index, keydir)
            end

            :ok
          end)

          profile_startup_phase(index, :flow_native_index_init, fn ->
            unless raft_projection_owner?(ctx) do
              NativeFlowIndex.reset(flow_index, flow_lookup)
            end

            :ok
          end)

          profile_startup_phase(index, :flow_history_projector_recover, fn ->
            :ok = Ferricstore.Flow.HistoryProjector.recover(ctx, index, path, keydir)
          end)

          profile_startup_phase(index, :flow_lmdb_rebuild, fn ->
            unless raft_projection_owner?(ctx) do
              Ferricstore.Flow.LMDBRebuilder.reconcile_startup_shard(
                path,
                keydir,
                index,
                ctx,
                zset_score_index,
                zset_score_lookup,
                flow_index,
                flow_lookup,
                active_file_id: active_file_id,
                active_file_path: active_file_path,
                shared_ref_backfill?: flow_shared_ref_backfill?
              )
            end

            :ok
          end)

          active_file_size = startup_file_size(active_file_path)

          keydir = publish_startup_keydir(keydir, keydir_name, ctx)

          # Default-instance replication is owned by WARaftBackend. Shard GenServers
          # still own local keydir/read/recovery state.
          raft? = false

          # Recover promoted collection instances
          promoted =
            profile_startup_phase(index, :recover_promoted, fn ->
              Ferricstore.Store.Promotion.recover_promoted(path, keydir, data_dir, index, ctx)
            end)

          # Migrate existing prob files: scan prob dir for files without
          # corresponding metadata markers in the keydir. Write markers so
          # DEL can clean up prob files and BF.INFO/CMS.INFO can recover metadata.
          profile_startup_phase(index, :migrate_prob_files, fn ->
            ShardLifecycle.migrate_prob_files(path, keydir, index, ctx)
          end)

          # Publish active file metadata to ActiveFile registry
          Ferricstore.Store.ActiveFile.publish(ctx, index, active_file_id, active_file_path, path)

          # Compute per-file dead bytes stats from disk sizes + ETS live data.
          file_stats =
            profile_startup_phase(index, :compute_file_stats, fn ->
              compute_file_stats(path, keydir)
            end)

          # Read merge config for fragmentation thresholds
          merge_config_overrides = Keyword.get(opts, :merge_config, %{})

          merge_config = %{
            fragmentation_threshold:
              Map.get(
                merge_config_overrides,
                :fragmentation_threshold,
                @default_fragmentation_threshold
              ),
            dead_bytes_threshold:
              Map.get(
                merge_config_overrides,
                :dead_bytes_threshold,
                @default_dead_bytes_threshold
              )
          }

          schedule_drain_pending(flush_ms)
          ShardLifecycle.schedule_expiry_sweep()
          ShardLifecycle.schedule_frag_check()

          max_file_size =
            if ctx, do: ctx.max_active_file_size, else: @default_max_active_file_size

          {:ok,
           %__MODULE__{
             ets: keydir,
             keydir: keydir,
             index: index,
             data_dir: data_dir,
             shard_data_path: path,
             instance_ctx: ctx,
             apply_context: apply_context,
             apply_context_encoded: apply_context_encoded,
             release_cursor_interval: release_cursor_interval,
             flow_async_history: flow_async_history,
             active_file_id: active_file_id,
             active_file_path: active_file_path,
             active_file_size: active_file_size,
             pending: [],
             flush_in_flight: nil,
             promoted_instances: promoted,
             file_stats: file_stats,
             merge_config: merge_config,
             raft?: raft?,
             max_active_file_size: max_file_size,
             compound_member_index: compound_member_index,
             zset_score_index: zset_score_index,
             zset_score_lookup: zset_score_lookup,
             flow_index: flow_index,
             flow_lookup: flow_lookup,
             zset_index_ready: MapSet.new()
           }, {:continue, {:flush_interval, flush_ms}}}
        catch
          {:shard_init_failed, reason} -> {:stop, reason}
        end
      end

      defp file_path(shard_path, file_id), do: ShardETS.file_path(shard_path, file_id)

      defp startup_file_size(path) do
        case File.stat(path) do
          {:ok, %{size: size}} when is_integer(size) and size >= 0 -> size
          _missing -> 0
        end
      end

      defp ensure_zset_index_table!(table_name, table_type) do
        case :ets.whereis(table_name) do
          :undefined ->
            :ets.new(table_name, [
              table_type,
              :public,
              :named_table,
              {:read_concurrency, true},
              {:write_concurrency, :auto}
            ])

          _tid ->
            :ets.delete_all_objects(table_name)
            table_name
        end
      end

      defp ensure_initial_files!(path, index, fsync_dir_fun) do
        dir_created? = not Ferricstore.FS.dir?(path)
        Ferricstore.FS.mkdir_p!(path)

        maybe_fsync_startup_dir!(
          dir_created?,
          Path.dirname(path),
          :create_shard_dir,
          index,
          fsync_dir_fun
        )

        # v2: scan data_dir for existing .log files, find highest file_id
        {active_file_id, active_file_size} = ShardLifecycle.discover_active_file(path)
        active_file_path = file_path(path, active_file_id)

        # Ensure the active file exists (touch it)
        # credo:disable-for-next-line Credo.Check.Refactor.UnlessWithElse
        file_created? =
          unless Ferricstore.FS.exists?(active_file_path) do
            Ferricstore.FS.touch!(active_file_path)
            true
          else
            false
          end

        maybe_fsync_startup_dir!(
          dir_created? or file_created?,
          path,
          :create_active_file,
          index,
          fsync_dir_fun
        )

        {active_file_id, active_file_size, active_file_path}
      end

      defp maybe_fsync_startup_dir!(false, _path, _phase, _index, _fsync_dir_fun), do: :ok

      defp maybe_fsync_startup_dir!(true, path, phase, index, fsync_dir_fun) do
        case fsync_dir_fun.(path) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error(
              "Shard #{index} startup failed to fsync #{phase} directory #{inspect(path)}: #{inspect(reason)}"
            )

            throw({:shard_init_failed, {:fsync_dir_failed, phase, reason}})
        end
      end

      defp prepare_rebuilding_keydir(keydir_name, ctx, index) do
        # Do not expose an empty/partial final keydir during startup recovery.
        # Router reads treat an existing table miss as authoritative, so rebuild
        # into a private startup table and publish it only after recovery finishes.
        delete_keydir_table(keydir_name)
        reset_keydir_binary_counter(ctx, index)

        temp_name = rebuilding_keydir_name(keydir_name)
        delete_keydir_table(temp_name)

        :ets.new(temp_name, keydir_table_options())
      end

      defp prepare_startup_keydir(keydir_name, ctx, index) do
        if raft_projection_owner?(ctx) do
          ensure_live_keydir(ctx, keydir_name)
        else
          prepare_rebuilding_keydir(keydir_name, ctx, index)
        end
      end

      defp publish_startup_keydir(keydir, keydir_name, ctx) do
        if raft_projection_owner?(ctx) do
          keydir
        else
          publish_rebuilt_keydir(keydir, keydir_name)
        end
      end

      defp ensure_live_keydir(ctx, keydir_name) do
        if is_map(ctx) do
          Ferricstore.Store.KeydirTableOwner.ensure_tables(ctx)
        end

        case :ets.whereis(keydir_name) do
          :undefined -> :ets.new(keydir_name, keydir_table_options())
          _tid -> keydir_name
        end
      rescue
        _ ->
          case :ets.whereis(keydir_name) do
            :undefined -> :ets.new(keydir_name, keydir_table_options())
            _tid -> keydir_name
          end
      end

      defp publish_rebuilt_keydir(temp_name, keydir_name) do
        delete_keydir_table(keydir_name)
        :ets.rename(temp_name, keydir_name)
        keydir_name
      end

      defp rebuilding_keydir_name(keydir_name), do: :"#{keydir_name}.__rebuilding__"

      defp keydir_table_options do
        [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto},
          {:decentralized_counters, true}
        ]
      end

      defp delete_keydir_table(name) do
        case :ets.whereis(name) do
          :undefined -> :ok
          _ref -> :ets.delete(name)
        end
      end

      defp reset_keydir_binary_counter(nil, _index), do: :ok
      defp reset_keydir_binary_counter(%{keydir_binary_bytes: nil}, _index), do: :ok

      defp reset_keydir_binary_counter(%{keydir_binary_bytes: keydir_binary_bytes}, index) do
        :atomics.put(keydir_binary_bytes, index + 1, 0)
      end

      defp raft_projection_owner?(%{name: name}) when name not in [nil, :default], do: false

      defp raft_projection_owner?(_ctx) do
        Ferricstore.ReplicationMode.raft?()
      rescue
        _ -> false
      end

      defp profile_startup_phase(index, phase, fun) when is_function(fun, 0) do
        {duration_us, result} = :timer.tc(fun)

        :telemetry.execute(
          [:ferricstore, :shard, :startup_phase],
          %{duration_us: duration_us},
          %{shard_index: index, phase: phase}
        )

        result
      end

      # -------------------------------------------------------------------
      # handle_continue
      # -------------------------------------------------------------------
    end
  end
end
