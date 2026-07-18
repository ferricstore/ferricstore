defmodule Ferricstore.Raft.WARaftBackend.Sections.PublicApi do
  @moduledoc false

  # Extracted from WARaftBackend: is_storage_unknown_outcome_reason .. do_peer_ready
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.ErrorReasons
      alias Ferricstore.NamespaceConfig
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Raft.MembershipGate
      alias Ferricstore.Raft.WARaftBackend.Batcher, as: NamespaceBatcher
      alias Ferricstore.Raft.WARaftBackend.BatcherSupervisor, as: NamespaceBatcherSupervisor
      alias Ferricstore.Raft.WARaftBackend.RuntimeSupervisor
      alias Ferricstore.Raft.WARaftBackend.SyncGate

      defguardp is_storage_unknown_outcome_reason(reason)
                when reason == :active_file_unavailable or
                       reason == :storage_apply_timeout or
                       reason == :commit_unreachable_after_submit or
                       reason == :not_leader_after_submit or
                       (is_tuple(reason) and tuple_size(reason) == 2 and
                          elem(reason, 0) in [
                            :bitcask_append_failed,
                            :bitcask_append_result_mismatch,
                            :bitcask_writer_flush_failed,
                            :blob_externalize_failed,
                            :blob_ref_unavailable,
                            :cross_shard_compensation_failed,
                            :flow_history_projection_failed,
                            :waraft_projection_failed,
                            :state_read_failed,
                            :delete_prob_file_failed,
                            :storage_blocked,
                            :commit_call_failed_after_submit
                          ]) or
                       (is_tuple(reason) and tuple_size(reason) == 3 and
                          elem(reason, 0) in [
                            :batch_result_mismatch,
                            :fsync_file,
                            :fsync_dir,
                            :fsync_dir_failed,
                            :unsafe_metadata_path,
                            :tombstone_batch_result_mismatch
                          ])

      defguardp is_write_timeout_unknown_reason(reason)
                when reason == :write_timeout_unknown or
                       (is_tuple(reason) and tuple_size(reason) == 2 and
                          elem(reason, 0) == :timeout and elem(reason, 1) == :unknown_outcome)

      defguardp valid_shard_index_shape(shard_index)
                when is_integer(shard_index) and shard_index >= 0

      defguardp invalid_shard_index_shape(shard_index)
                when not is_integer(shard_index) or shard_index < 0

      defguardp invalid_redirects_left_shape(redirects_left)
                when not is_integer(redirects_left) or redirects_left < 0

      @doc false
      @spec default_log_module() :: module()
      def default_log_module, do: @default_log_module

      @doc false
      @spec default_commit_batch_interval_ms() :: non_neg_integer()
      def default_commit_batch_interval_ms do
        case Application.fetch_env(:ferricstore, :waraft_commit_batch_interval_ms) do
          {:ok, value} ->
            non_negative_integer_option!(:waraft_commit_batch_interval_ms, value)

          :error ->
            wal_commit_delay_floor_ms()
        end
      end

      @doc false
      @spec default_commit_batch_max() :: pos_integer()
      def default_commit_batch_max do
        value =
          Application.get_env(:ferricstore, :waraft_commit_batch_max, @default_commit_batch_max)

        positive_integer_option!(:waraft_commit_batch_max, value)
      end

      @spec start(FerricStore.Instance.t(), keyword()) :: :ok | {:error, term()}
      def start(%FerricStore.Instance{} = ctx, opts \\ []) do
        Ferricstore.Raft.WARaftStorage.validate_supported_apply_mode!()
        :ok = Ferricstore.Flow.LMDBRebuilder.init_startup_active_rebuild_limiter()

        config = backend_config!(opts)

        profile_startup_phase(:ensure_waraft_app_started, %{shard_count: ctx.shard_count}, fn ->
          :ok = ensure_started()
        end)

        with_startup_write_fence(fn ->
          profile_startup_phase(:stop_existing_backend, %{shard_count: ctx.shard_count}, fn ->
            _ = stop()
            :ok
          end)

          case profile_startup_phase(
                 :ensure_storage_runtime,
                 %{shard_count: ctx.shard_count},
                 fn -> RuntimeSupervisor.ensure_started() end
               ) do
            :ok -> start_after_storage_runtime(ctx, opts, config)
            {:error, _reason} = error -> cleanup_failed_start(error)
          end
        end)
      end

      @doc false
      @spec starting?() :: boolean()
      def starting?, do: :persistent_term.get(@starting_key, false) == true

      defp with_startup_write_fence(fun) when is_function(fun, 0) do
        :persistent_term.put(@starting_key, true)

        try do
          fun.()
        after
          :persistent_term.erase(@starting_key)
        end
      end

      defp start_after_storage_runtime(ctx, opts, config) do
        case profile_startup_phase(
               :ensure_data_layout,
               %{shard_count: ctx.shard_count},
               fn -> ensure_backend_data_layout(ctx) end
             ) do
          :ok -> start_after_data_layout(ctx, opts, config)
          {:error, _reason} = error -> cleanup_failed_start(error)
        end
      end

      defp start_after_data_layout(ctx, opts, config) do
        profile_startup_phase(:active_file_init, %{shard_count: ctx.shard_count}, fn ->
          Ferricstore.Store.ActiveFile.init(ctx.shard_count)
        end)

        profile_startup_phase(:configure, %{shard_count: ctx.shard_count}, fn ->
          configure(ctx, config)
        end)

        :persistent_term.put({@context_key, @table}, ctx)

        specs =
          for partition <- 1..ctx.shard_count do
            partition_spec(partition, config)
          end

        spec =
          @app
          |> :wa_raft_sup.child_spec([], %{config_search_apps: [@app, :ferricstore]})
          |> Map.put(:id, @sup_id)

        start_result =
          profile_startup_phase(:start_backend_supervisor, %{shard_count: ctx.shard_count}, fn ->
            Supervisor.start_child(:kernel_sup, spec)
          end)

        case start_result do
          {:ok, _pid} ->
            start_partitions_then_finish(specs, ctx.shard_count, opts)

          {:ok, _pid, _info} ->
            start_partitions_then_finish(specs, ctx.shard_count, opts)

          {:error, {:already_started, _pid}} ->
            start_partitions_then_finish(specs, ctx.shard_count, opts)

          {:error, :already_present} ->
            restart_present_child(ctx, opts)

          {:error, reason} ->
            cleanup_failed_start({:error, reason})
        end
      end

      defp ensure_backend_data_layout(ctx) do
        with :ok <- validate_waraft_segment_log_dirs(ctx) do
          Ferricstore.DataDir.ensure_layout!(ctx.data_dir, ctx.shard_count)
        end
      rescue
        error in File.Error ->
          {:error, {:data_layout_failed, error.reason}}
      end

      defp validate_waraft_segment_log_dirs(ctx) do
        Enum.reduce_while(1..ctx.shard_count, :ok, fn partition, :ok ->
          path =
            Path.join([
              ctx.data_dir,
              "waraft",
              "#{@table}.#{partition}",
              "segment_log"
            ])

          case File.lstat(path) do
            {:ok, %{type: :directory}} ->
              {:cont, :ok}

            {:ok, %{type: :symlink}} ->
              {:halt, {:error, {:unsafe_segment_log_dir, path}}}

            {:ok, %{type: type}} ->
              {:halt, {:error, {:invalid_segment_log_dir, path, type}}}

            {:error, :enoent} ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {:segment_log_dir_stat_failed, path, reason}}}
          end
        end)
      end

      @spec stop() :: :ok
      def stop do
        shard_count = registered_partition_count()
        stop_namespace_batchers(shard_count)
        flush_storage_before_stop(shard_count)
        _ = Supervisor.terminate_child(:kernel_sup, @sup_id)
        _ = Supervisor.delete_child(:kernel_sup, @sup_id)
        _ = stop_orphaned_waraft_sup()
        wait_down(registered_names(shard_count), 100)
        erase_waraft_option_cache(shard_count)
        :persistent_term.erase({@context_key, @table})
        :persistent_term.erase(@inflight_bytes_key)
        :persistent_term.erase(@max_inflight_bytes_key)
        SyncGate.clear_shards(shard_count)
        erase_cached_voter_nodes(shard_count)
        :persistent_term.erase(@shard_count_key)
        RuntimeSupervisor.stop()
        :ok
      end

      defp flush_storage_before_stop(shard_count)
           when is_integer(shard_count) and shard_count > 0 do
        Enum.each(0..(shard_count - 1), fn shard_index ->
          _ = create_snapshot(shard_index)
          :ok
        end)
      catch
        _kind, _reason -> :ok
      end

      defp flush_storage_before_stop(_shard_count), do: :ok

      @doc false
      @spec __registered_names_for_test__(pos_integer()) :: [atom()]
      def __registered_names_for_test__(shard_count)
          when is_integer(shard_count) and shard_count > 0 do
        registered_names(shard_count)
      end

      @doc false
      def __redirect_write_failure_for_test__(kind, reason) do
        redirect_write_failure(kind, reason)
      end

      @doc false
      def __normalize_commit_result_for_test__(result) do
        normalize_commit_result(result)
      end

      @doc false
      def __redirectable_write_error_for_test__(error) do
        redirectable_write_error?(error)
      end

      @doc false
      def __redirect_membership_failure_for_test__(kind, reason) do
        redirect_membership_failure(kind, reason)
      end

      @doc false
      def __redirect_transfer_failure_for_test__(kind, reason) do
        redirect_transfer_failure(kind, reason)
      end

      @doc false
      def __peer_node_for_test__(peer) do
        peer_node(peer)
      end

      @spec context!(atom()) :: FerricStore.Instance.t()
      def context!(table) do
        :persistent_term.get({@context_key, table})
      end

      defp context(table) do
        {:ok, context!(table)}
      catch
        :error, :badarg -> backend_unavailable_error()
      end

      @spec write(non_neg_integer(), tuple()) :: term()
      def write(shard_index, command) when valid_shard_index_shape(shard_index) do
        with_sync_write(shard_index, fn ->
          case maybe_namespace_window_write(shard_index, command) do
            :direct -> commit_or_redirect(shard_index, command, 2)
            result -> result
          end
        end)
      end

      def write(shard_index, _command), do: invalid_shard_index_error(shard_index)

      @spec write_async(non_neg_integer(), tuple(), GenServer.from()) :: :ok | {:direct, term()}
      def write_async(shard_index, _command, from) when invalid_shard_index_shape(shard_index) do
        GenServer.reply(from, invalid_shard_index_error(shard_index))
        :ok
      end

      def write_async(shard_index, command, from) when is_tuple(command) do
        case SyncGate.enter(shard_index) do
          {:ok, token} ->
            try do
              case NamespaceBatcher.write_single_async(shard_index, command, from, token) do
                :ok ->
                  :ok

                {:direct, result} ->
                  SyncGate.leave(token)
                  {:direct, result}
              end
            catch
              kind, reason ->
                SyncGate.leave(token)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          {:error, _reason} = error ->
            GenServer.reply(from, error)
            :ok
        end
      end

      def write_async(_shard_index, command, from) do
        GenServer.reply(from, {:error, {:invalid_command, command}})
        :ok
      end

      @spec write_many([{non_neg_integer(), tuple()}]) :: [term()]
      def write_many([]), do: []

      def write_many(shard_commands) when is_list(shard_commands) do
        with_sync_write_many(shard_commands, fn ->
          shard_commands
          |> Enum.map(&submit_write_many_entry/1)
          |> Enum.map(&await_write_many_entry/1)
        end)
      end

      def write_many(shard_commands), do: {:error, {:invalid_write_many_entries, shard_commands}}

      @doc false
      @spec write_redirected(non_neg_integer(), tuple(), non_neg_integer()) :: term()
      def write_redirected(shard_index, _command, _redirects_left)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def write_redirected(_shard_index, _command, redirects_left)
          when invalid_redirects_left_shape(redirects_left),
          do: invalid_redirects_left_error(redirects_left)

      def write_redirected(shard_index, command, redirects_left)
          when is_integer(redirects_left) and redirects_left >= 0 do
        with_sync_write(shard_index, fn ->
          commit_or_redirect_with_position(shard_index, command, redirects_left)
        end)
      end

      @spec write_batch(non_neg_integer(), [tuple()]) :: term()
      def write_batch(shard_index, _commands) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def write_batch(_shard_index, []), do: {:ok, []}

      def write_batch(shard_index, commands) when is_list(commands) do
        with_sync_write(shard_index, fn ->
          NamespaceBatcher.write_batch(shard_index, commands)
        end)
      end

      def write_batch(_shard_index, commands),
        do: {:error, {:invalid_command_batch, commands}}

      @spec write_batch_async(non_neg_integer(), [tuple()], GenServer.from()) ::
              :ok | {:direct, term()}
      def write_batch_async(shard_index, _commands, from)
          when invalid_shard_index_shape(shard_index) do
        GenServer.reply(from, invalid_shard_index_error(shard_index))
        :ok
      end

      def write_batch_async(_shard_index, [], from) do
        GenServer.reply(from, {:ok, []})
        :ok
      end

      def write_batch_async(shard_index, commands, from) when is_list(commands) do
        case SyncGate.enter(shard_index) do
          {:ok, token} ->
            try do
              case NamespaceBatcher.write_batch_async(shard_index, commands, from, token) do
                :ok ->
                  :ok

                {:direct, result} ->
                  SyncGate.leave(token)
                  {:direct, result}
              end
            catch
              kind, reason ->
                SyncGate.leave(token)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          {:error, _reason} = error ->
            GenServer.reply(from, error)
            :ok
        end
      end

      def write_batch_async(_shard_index, commands, from) do
        GenServer.reply(from, {:error, {:invalid_command_batch, commands}})
        :ok
      end

      @doc false
      @spec __commit_batch_direct__(non_neg_integer(), [tuple()]) :: term()
      def __commit_batch_direct__(shard_index, commands) when is_list(commands) do
        commit_or_redirect(shard_index, {:batch, commands}, 2)
      end

      @doc false
      @spec __commit_single_direct__(non_neg_integer(), tuple()) :: term()
      def __commit_single_direct__(shard_index, command) when is_tuple(command) do
        commit_or_redirect(shard_index, command, 2)
      end

      @doc false
      @spec __commit_single_batch_direct__(non_neg_integer(), tuple()) :: term()
      def __commit_single_batch_direct__(shard_index, command) when is_tuple(command) do
        case commit_or_redirect(shard_index, command, 2) do
          {:error, _reason} = error -> error
          result -> {:ok, [result]}
        end
      end

      @spec write_put_batch(non_neg_integer(), [{binary(), binary(), non_neg_integer()}]) ::
              term()
      def write_put_batch(shard_index, _entries) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def write_put_batch(_shard_index, []), do: {:ok, []}

      def write_put_batch(shard_index, entries) when is_list(entries) do
        with_sync_write(shard_index, fn ->
          NamespaceBatcher.write_put_batch(shard_index, entries)
        end)
      end

      def write_put_batch(_shard_index, entries),
        do: {:error, {:invalid_put_batch, entries}}

      @spec write_put_batch_async(
              non_neg_integer(),
              [{binary(), binary(), non_neg_integer()}],
              GenServer.from()
            ) :: :ok | {:direct, term()}
      def write_put_batch_async(shard_index, _entries, from)
          when invalid_shard_index_shape(shard_index) do
        GenServer.reply(from, invalid_shard_index_error(shard_index))
        :ok
      end

      def write_put_batch_async(_shard_index, [], from) do
        GenServer.reply(from, {:ok, []})
        :ok
      end

      def write_put_batch_async(shard_index, entries, from) when is_list(entries) do
        case SyncGate.enter(shard_index) do
          {:ok, token} ->
            try do
              case NamespaceBatcher.write_put_batch_async(shard_index, entries, from, token) do
                :ok ->
                  :ok

                {:direct, result} ->
                  SyncGate.leave(token)
                  {:direct, result}
              end
            catch
              kind, reason ->
                SyncGate.leave(token)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          {:error, _reason} = error ->
            GenServer.reply(from, error)
            :ok
        end
      end

      def write_put_batch_async(_shard_index, entries, from) do
        GenServer.reply(from, {:error, {:invalid_put_batch, entries}})
        :ok
      end

      @spec write_delete_batch(non_neg_integer(), [binary()]) :: term()
      def write_delete_batch(shard_index, _keys) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def write_delete_batch(_shard_index, []), do: {:ok, []}

      def write_delete_batch(shard_index, keys) when is_list(keys) do
        with_sync_write(shard_index, fn ->
          NamespaceBatcher.write_delete_batch(shard_index, keys)
        end)
      end

      def write_delete_batch(_shard_index, keys),
        do: {:error, {:invalid_delete_batch, keys}}

      @spec write_delete_batch_async(non_neg_integer(), [binary()], GenServer.from()) ::
              :ok | {:direct, term()}
      def write_delete_batch_async(shard_index, _keys, from)
          when invalid_shard_index_shape(shard_index) do
        GenServer.reply(from, invalid_shard_index_error(shard_index))
        :ok
      end

      def write_delete_batch_async(_shard_index, [], from) do
        GenServer.reply(from, {:ok, []})
        :ok
      end

      def write_delete_batch_async(shard_index, keys, from) when is_list(keys) do
        case SyncGate.enter(shard_index) do
          {:ok, token} ->
            try do
              case NamespaceBatcher.write_delete_batch_async(shard_index, keys, from, token) do
                :ok ->
                  :ok

                {:direct, result} ->
                  SyncGate.leave(token)
                  {:direct, result}
              end
            catch
              kind, reason ->
                SyncGate.leave(token)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end

          {:error, _reason} = error ->
            GenServer.reply(from, error)
            :ok
        end
      end

      def write_delete_batch_async(_shard_index, keys, from) do
        GenServer.reply(from, {:error, {:invalid_delete_batch, keys}})
        :ok
      end

      @spec pause_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
      def pause_writes_for_sync(shard_index, timeout \\ 30_000)

      def pause_writes_for_sync(shard_index, timeout) when valid_shard_index_shape(shard_index) do
        with {:ok, gate_pid} <- SyncGate.pause(shard_index),
             :ok <- NamespaceBatcher.flush(shard_index, timeout),
             :ok <- SyncGate.await_drained(gate_pid, timeout),
             :ok <- NamespaceBatcher.flush(shard_index, timeout),
             :ok <- sync_pause_barrier(shard_index) do
          :ok
        else
          {:error, reason} = error ->
            _ = resume_writes_for_sync(shard_index, 5_000)
            emit_sync_pause_failed(shard_index, reason)
            error
        end
      end

      def pause_writes_for_sync(shard_index, _timeout), do: invalid_shard_index_error(shard_index)

      @spec pause_writes_for_sync(
              non_neg_integer(),
              SyncGate.pause_lease(),
              timeout()
            ) :: :ok | {:error, term()}
      def pause_writes_for_sync(
            shard_index,
            {owner_pid, lease_ref} = pause_lease,
            timeout
          )
          when valid_shard_index_shape(shard_index) and is_pid(owner_pid) and
                 is_reference(lease_ref) do
        with {:ok, gate_pid} <- SyncGate.pause(shard_index, pause_lease),
             :ok <- NamespaceBatcher.flush(shard_index, timeout),
             :ok <- SyncGate.await_drained(gate_pid, timeout),
             :ok <- NamespaceBatcher.flush(shard_index, timeout),
             :ok <- sync_pause_barrier(shard_index) do
          :ok
        else
          {:error, reason} = error ->
            _ = SyncGate.resume(shard_index, pause_lease, 5_000)
            emit_sync_pause_failed(shard_index, reason)
            error
        end
      end

      def pause_writes_for_sync(shard_index, _pause_lease, _timeout),
        do: invalid_shard_index_error(shard_index)

      @spec resume_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
      def resume_writes_for_sync(shard_index, timeout \\ 5_000)

      def resume_writes_for_sync(shard_index, timeout)
          when valid_shard_index_shape(shard_index) do
        SyncGate.resume(shard_index, timeout)
      end

      def resume_writes_for_sync(shard_index, _timeout),
        do: invalid_shard_index_error(shard_index)

      @spec resume_writes_for_sync(
              non_neg_integer(),
              SyncGate.pause_lease(),
              timeout()
            ) :: :ok | {:error, term()}
      def resume_writes_for_sync(
            shard_index,
            {owner_pid, lease_ref} = pause_lease,
            timeout
          )
          when valid_shard_index_shape(shard_index) and is_pid(owner_pid) and
                 is_reference(lease_ref) do
        SyncGate.resume(shard_index, pause_lease, timeout)
      end

      def resume_writes_for_sync(shard_index, _pause_lease, _timeout),
        do: invalid_shard_index_error(shard_index)

      @spec pause_writes_for_sync_all(non_neg_integer(), timeout()) :: :ok | {:error, term()}
      def pause_writes_for_sync_all(shard_count, timeout \\ 30_000)

      def pause_writes_for_sync_all(0, _timeout), do: :ok

      def pause_writes_for_sync_all(shard_count, timeout)
          when is_integer(shard_count) and shard_count > 0 do
        shard_indexes = Enum.to_list(0..(shard_count - 1))
        deadline = sync_pause_deadline(timeout)

        with {:ok, pauses} <- SyncGate.pause_many(shard_indexes),
             :ok <- flush_sync_pause_batchers(shard_indexes, deadline),
             :ok <- SyncGate.await_many_drained(pauses, sync_pause_remaining(deadline)),
             :ok <- flush_sync_pause_batchers(shard_indexes, deadline),
             :ok <- run_sync_pause_barriers(shard_indexes) do
          :ok
        else
          {:error, reason} = error ->
            _ = SyncGate.resume_many(shard_indexes, 5_000)
            emit_sync_pause_failed(:all, reason)
            error
        end
      end

      def pause_writes_for_sync_all(shard_count, _timeout),
        do: {:error, {:invalid_shard_count, shard_count}}

      @spec pause_writes_for_sync_all(
              non_neg_integer(),
              SyncGate.pause_lease(),
              timeout()
            ) :: :ok | {:error, term()}
      def pause_writes_for_sync_all(0, {_owner_pid, _lease_ref}, _timeout), do: :ok

      def pause_writes_for_sync_all(
            shard_count,
            {owner_pid, lease_ref} = pause_lease,
            timeout
          )
          when is_integer(shard_count) and shard_count > 0 and is_pid(owner_pid) and
                 is_reference(lease_ref) do
        shard_indexes = Enum.to_list(0..(shard_count - 1))
        deadline = sync_pause_deadline(timeout)

        with {:ok, pauses} <- SyncGate.pause_many(shard_indexes, pause_lease),
             :ok <- flush_sync_pause_batchers(shard_indexes, deadline),
             :ok <- SyncGate.await_many_drained(pauses, sync_pause_remaining(deadline)),
             :ok <- flush_sync_pause_batchers(shard_indexes, deadline),
             :ok <- run_sync_pause_barriers(shard_indexes) do
          :ok
        else
          {:error, reason} = error ->
            _ = SyncGate.resume_many(shard_indexes, pause_lease, 5_000)
            emit_sync_pause_failed(:all, reason)
            error
        end
      end

      def pause_writes_for_sync_all(shard_count, _pause_lease, _timeout),
        do: {:error, {:invalid_shard_count, shard_count}}

      @spec resume_writes_for_sync_all(non_neg_integer(), timeout()) :: :ok | {:error, term()}
      def resume_writes_for_sync_all(shard_count, timeout \\ 5_000)

      def resume_writes_for_sync_all(0, _timeout), do: :ok

      def resume_writes_for_sync_all(shard_count, timeout)
          when is_integer(shard_count) and shard_count > 0 do
        SyncGate.resume_many(Enum.to_list(0..(shard_count - 1)), timeout)
      end

      def resume_writes_for_sync_all(shard_count, _timeout),
        do: {:error, {:invalid_shard_count, shard_count}}

      @spec resume_writes_for_sync_all(
              non_neg_integer(),
              SyncGate.pause_lease(),
              timeout()
            ) :: :ok | {:error, term()}
      def resume_writes_for_sync_all(0, {_owner_pid, _lease_ref}, _timeout), do: :ok

      def resume_writes_for_sync_all(
            shard_count,
            {owner_pid, lease_ref} = pause_lease,
            timeout
          )
          when is_integer(shard_count) and shard_count > 0 and is_pid(owner_pid) and
                 is_reference(lease_ref) do
        SyncGate.resume_many(Enum.to_list(0..(shard_count - 1)), pause_lease, timeout)
      end

      def resume_writes_for_sync_all(shard_count, _pause_lease, _timeout),
        do: {:error, {:invalid_shard_count, shard_count}}

      @doc false
      @spec write_flush_shard_paused(non_neg_integer(), {non_neg_integer(), non_neg_integer()}) ::
              term()
      def write_flush_shard_paused(shard_index, {physical_ms, logical} = flush_epoch)
          when valid_shard_index_shape(shard_index) and is_integer(physical_ms) and
                 physical_ms >= 0 and is_integer(logical) and logical >= 0 do
        if SyncGate.paused?(shard_index) do
          commit_or_redirect(shard_index, {:flush_shard, flush_epoch}, 2)
        else
          {:error, :flush_shard_requires_paused_writes}
        end
      end

      def write_flush_shard_paused(shard_index, _flush_epoch),
        do: invalid_shard_index_error(shard_index)

      defp flush_sync_pause_batchers(shard_indexes, deadline) do
        Enum.reduce_while(shard_indexes, :ok, fn shard_index, :ok ->
          case NamespaceBatcher.flush(shard_index, sync_pause_remaining(deadline)) do
            :ok ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {:sync_pause_batcher_flush_failed, shard_index, reason}}}
          end
        end)
      end

      defp run_sync_pause_barriers(shard_indexes) do
        Enum.reduce_while(shard_indexes, :ok, fn shard_index, :ok ->
          case sync_pause_barrier(shard_index) do
            :ok ->
              {:cont, :ok}

            {:error, reason} ->
              {:halt, {:error, {:sync_pause_barrier_failed, shard_index, reason}}}
          end
        end)
      end

      defp sync_pause_deadline(:infinity), do: :infinity

      defp sync_pause_deadline(timeout) when is_integer(timeout) and timeout >= 0,
        do: System.monotonic_time(:millisecond) + timeout

      defp sync_pause_remaining(:infinity), do: :infinity

      defp sync_pause_remaining(deadline),
        do: max(deadline - System.monotonic_time(:millisecond), 0)

      @doc false
      @spec __commit_put_batch_direct__(
              non_neg_integer(),
              [{binary(), binary(), non_neg_integer()}]
            ) :: term()
      def __commit_put_batch_direct__(shard_index, entries) when is_list(entries) do
        commit_or_redirect(shard_index, {:put_batch, entries}, 2)
      end

      @doc false
      @spec __commit_delete_batch_direct__(non_neg_integer(), [binary()]) :: term()
      def __commit_delete_batch_direct__(shard_index, keys) when is_list(keys) do
        commit_or_redirect(shard_index, {:delete_batch, keys}, 2)
      end

      @spec inflight_commit_bytes(non_neg_integer()) :: non_neg_integer()
      def inflight_commit_bytes(shard_index) when invalid_shard_index_shape(shard_index), do: 0

      def inflight_commit_bytes(shard_index) do
        case :persistent_term.get(@inflight_bytes_key, nil) do
          ref when is_reference(ref) ->
            idx = partition(shard_index)

            if idx <= :atomics.info(ref).size do
              :atomics.get(ref, idx)
            else
              0
            end

          _other ->
            0
        end
      end

      @spec segment_log_memory_status(non_neg_integer()) :: map()
      def segment_log_memory_status(shard_index) when invalid_shard_index_shape(shard_index),
        do: empty_segment_log_memory_status()

      def segment_log_memory_status(shard_index) do
        partition = partition(shard_index)
        log_name = :wa_raft_log.registered_name(@table, partition)
        log = {:raft_log, log_name, @app, @table, partition, @default_log_module}

        case @default_log_module.memory_status(log) do
          status when is_map(status) -> status
          _other -> empty_segment_log_memory_status()
        end
      rescue
        _ -> empty_segment_log_memory_status()
      catch
        _, _ -> empty_segment_log_memory_status()
      end

      @spec storage_position(non_neg_integer()) :: {:ok, tuple()} | {:error, term()}
      def storage_position(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def storage_position(shard_index) do
        storage = :wa_raft_storage.registered_name(@table, partition(shard_index))

        backend_call(fn ->
          {:ok, :wa_raft_storage.position(storage)}
        end)
      end

      @doc false
      @spec storage_status(non_neg_integer()) :: {:ok, keyword()} | {:error, term()}
      def storage_status(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def storage_status(shard_index) do
        storage = :wa_raft_storage.registered_name(@table, partition(shard_index))

        backend_call(fn ->
          {:ok, :wa_raft_storage.status(storage)}
        end)
      end

      @doc false
      @spec flush_storage_replay_dependencies(non_neg_integer(), non_neg_integer()) ::
              :ok | {:error, term()}
      def flush_storage_replay_dependencies(shard_index, _timeout_ms)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def flush_storage_replay_dependencies(_shard_index, timeout_ms)
          when not is_integer(timeout_ms) or timeout_ms < 0,
          do: {:error, {:invalid_timeout, timeout_ms}}

      def flush_storage_replay_dependencies(shard_index, timeout_ms) do
        storage = :wa_raft_storage.registered_name(@table, partition(shard_index))
        deadline = System.monotonic_time(:millisecond) + timeout_ms
        ref = make_ref()

        case Process.whereis(storage) do
          pid when is_pid(pid) ->
            send(pid, {:ferricstore_waraft_flush_replay_dependencies, self(), ref})
            await_storage_replay_flush_request(shard_index, ref, deadline)

          nil ->
            backend_unavailable_error()
        end
      end

      defp await_storage_replay_flush_request(shard_index, ref, deadline) do
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)

        receive do
          {:ferricstore_waraft_flush_replay_dependencies, ^ref, {:ok, target_position}} ->
            await_storage_replay_flush_position(shard_index, target_position, deadline)

          {:ferricstore_waraft_flush_replay_dependencies, ^ref, {:error, reason}} ->
            {:error, {:storage_blocked, reason}}
        after
          remaining -> {:error, :storage_durability_timeout}
        end
      end

      defp await_storage_replay_flush_position(shard_index, target_position, deadline) do
        case storage_status(shard_index) do
          {:ok, status} when is_list(status) ->
            cond do
              reason = Keyword.get(status, :blocked_error) ->
                {:error, {:storage_blocked, reason}}

              storage_replay_position_reached?(
                Keyword.get(status, :durable_position),
                target_position
              ) ->
                :ok

              System.monotonic_time(:millisecond) >= deadline ->
                {:error, :storage_durability_timeout}

              true ->
                Process.sleep(10)
                await_storage_replay_flush_position(shard_index, target_position, deadline)
            end

          {:error, _reason} = error ->
            error

          _invalid ->
            {:error, :missing_waraft_storage_metrics}
        end
      end

      defp storage_replay_position_reached?(
             {:raft_log_pos, durable_index, _durable_term},
             {:raft_log_pos, target_index, _target_term}
           )
           when is_integer(durable_index) and is_integer(target_index),
           do: durable_index >= target_index

      defp storage_replay_position_reached?(_durable_position, _target_position), do: false

      @spec create_snapshot(non_neg_integer()) :: {:ok, tuple()} | {:error, term()}
      def create_snapshot(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def create_snapshot(shard_index) do
        storage = :wa_raft_storage.registered_name(@table, partition(shard_index))
        backend_call(fn -> :wa_raft_storage.create_snapshot(storage) end)
      end

      @spec install_snapshot(non_neg_integer(), charlist() | binary(), tuple()) ::
              :ok | {:error, term()}
      def install_snapshot(shard_index, _snapshot_path, _position)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def install_snapshot(shard_index, snapshot_path, position) do
        with {:ok, snapshot_path_chars} <- snapshot_path_charlist(snapshot_path),
             :ok <- validate_snapshot_position(position) do
          server = :wa_raft_server.registered_name(@table, partition(shard_index))

          backend_call(fn ->
            :wa_raft_server.snapshot_available(server, snapshot_path_chars, position)
          end)
        end
      end

      @spec bootstrap_cluster([node()]) :: :ok | {:error, term()}
      def bootstrap_cluster(nodes) when is_list(nodes) do
        with :ok <- validate_bootstrap_nodes(nodes),
             {:ok, ctx} <- context(@table) do
          bootstrap_cluster_partitions(ctx.shard_count, nodes)
        end
      end

      def bootstrap_cluster(_nodes), do: {:error, :invalid_cluster_nodes}

      defp bootstrap_cluster_partitions(shard_count, nodes) do
        0..(shard_count - 1)
        |> Enum.reduce_while(:ok, fn shard_index, :ok ->
          case bootstrap_cluster_partition(shard_index, nodes) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      @spec trigger_election(non_neg_integer()) :: :ok | term()
      def trigger_election(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def trigger_election(shard_index) do
        server =
          shard_index
          |> partition()
          |> server_name()

        case backend_call(fn -> :wa_raft_server.trigger_election(server) end) do
          :ok ->
            with :ok <- wait_known_leader(shard_index, server, startup_wait_attempts()) do
              rollout_apply_context_shard(shard_index)
            end

          other ->
            other
        end
      end

      @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
      def transfer_leadership(shard_index, _target_node)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def transfer_leadership(shard_index, target_node) do
        with :ok <- validate_node_name(target_node) do
          transfer_leadership_or_redirect(shard_index, target_node, 2)
        end
      end

      @doc false
      @spec transfer_leadership_redirected(non_neg_integer(), node(), non_neg_integer()) ::
              :ok | {:error, term()}
      def transfer_leadership_redirected(shard_index, _target_node, _redirects_left)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def transfer_leadership_redirected(_shard_index, _target_node, redirects_left)
          when invalid_redirects_left_shape(redirects_left),
          do: invalid_redirects_left_error(redirects_left)

      def transfer_leadership_redirected(shard_index, target_node, redirects_left)
          when is_integer(redirects_left) and redirects_left >= 0 do
        with :ok <- validate_node_name(target_node) do
          transfer_leadership_or_redirect(shard_index, target_node, redirects_left)
        end
      end

      @spec status(non_neg_integer()) :: keyword() | term()
      def status(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def status(shard_index) do
        maybe_status_hook(shard_index)

        server =
          shard_index
          |> partition()
          |> server_name()

        backend_call(fn -> :wa_raft_server.status(server) end)
      end

      defp maybe_status_hook(shard_index) do
        case Process.get(:ferricstore_waraft_backend_status_hook) do
          fun when is_function(fun, 1) -> fun.(shard_index)
          _other -> :ok
        end
      end

      @doc false
      @spec cache_config(non_neg_integer(), term()) :: :ok
      def cache_config(shard_index, {_position, config}) when is_integer(shard_index),
        do: cache_config(shard_index, config)

      def cache_config(shard_index, config) when is_integer(shard_index) and is_map(config) do
        :persistent_term.put({@voter_nodes_key, shard_index}, config_voter_nodes(config))
        :ok
      end

      def cache_config(_shard_index, _config), do: :ok

      @spec membership(non_neg_integer()) :: term()
      def membership(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def membership(shard_index) do
        result =
          backend_call(fn ->
            shard_index
            |> partition()
            |> server_name()
            |> :wa_raft_server.membership()
          end)

        case result do
          identities when is_list(identities) -> Enum.map(identities, &normalize_identity/1)
          other -> other
        end
      end

      @doc false
      @spec cached_members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
      def cached_members(shard_index) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def cached_members(shard_index) do
        case cached_voter_nodes(shard_index) do
          {:ok, [_ | _] = voters} ->
            partition = partition(shard_index)
            server = server_name(partition)

            leader_node = :wa_raft_info.get_leader(@table, partition)
            leader = if valid_node_name?(leader_node), do: {server, leader_node}, else: nil

            {:ok, Enum.map(voters, &{server, &1}), leader}

          {:ok, []} ->
            {:error, :unknown_cached_membership}

          :unknown ->
            {:error, :unknown_cached_membership}
        end
      catch
        :exit, reason -> backend_exit_error(reason)
        kind, reason -> {:error, {kind, reason}}
      end

      @spec adjust_membership(non_neg_integer(), atom(), node()) ::
              {:ok, tuple()} | {:error, term()}
      def adjust_membership(shard_index, _action, _node_name)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def adjust_membership(_shard_index, action, _node_name)
          when action not in @membership_actions,
          do: invalid_membership_action_error(action)

      def adjust_membership(shard_index, action, node_name) when action in @membership_actions do
        with :ok <- validate_node_name(node_name) do
          commit_config_or_redirect(shard_index, action, node_name, @timeout, @config_redirects)
        end
      end

      @doc false
      @spec adjust_membership_redirected(
              non_neg_integer(),
              atom(),
              node(),
              non_neg_integer(),
              non_neg_integer()
            ) ::
              {:ok, tuple()} | {:error, term()}
      def adjust_membership_redirected(
            shard_index,
            _action,
            _node_name,
            _timeout_ms,
            _redirects_left
          )
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def adjust_membership_redirected(
            _shard_index,
            action,
            _node_name,
            _timeout_ms,
            _redirects_left
          )
          when action not in @membership_actions,
          do: invalid_membership_action_error(action)

      def adjust_membership_redirected(
            _shard_index,
            _action,
            _node_name,
            _timeout_ms,
            redirects_left
          )
          when invalid_redirects_left_shape(redirects_left),
          do: invalid_redirects_left_error(redirects_left)

      def adjust_membership_redirected(shard_index, action, node_name, timeout_ms, redirects_left)
          when action in @membership_actions and is_integer(redirects_left) and
                 redirects_left >= 0 do
        with :ok <- validate_node_name(node_name),
             {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
          commit_config_or_redirect(shard_index, action, node_name, timeout_ms, redirects_left)
        end
      end

      @spec add_member(non_neg_integer(), node(), keyword()) :: {:ok, tuple()} | {:error, term()}
      def add_member(shard_index, node_name, opts \\ [])

      def add_member(shard_index, _node_name, _opts) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def add_member(shard_index, node_name, opts) do
        with :ok <- validate_node_name(node_name),
             {:ok, timeout_ms} <-
               validate_membership_timeout_ms(Keyword.get(opts, :timeout_ms, 10_000)) do
          add_member_or_redirect(shard_index, node_name, timeout_ms, @config_redirects)
        end
      end

      @doc false
      @spec add_member_redirected(non_neg_integer(), node(), non_neg_integer(), non_neg_integer()) ::
              {:ok, tuple()} | {:error, term()}
      def add_member_redirected(shard_index, _node_name, _timeout_ms, _redirects_left)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def add_member_redirected(_shard_index, _node_name, _timeout_ms, redirects_left)
          when invalid_redirects_left_shape(redirects_left),
          do: invalid_redirects_left_error(redirects_left)

      def add_member_redirected(shard_index, node_name, timeout_ms, redirects_left)
          when is_integer(redirects_left) and redirects_left >= 0 do
        with :ok <- validate_node_name(node_name),
             {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
          add_member_or_redirect(shard_index, node_name, timeout_ms, redirects_left)
        end
      end

      @spec add_participant(non_neg_integer(), node(), keyword()) ::
              {:ok, tuple()} | {:error, term()}
      def add_participant(shard_index, node_name, opts \\ [])

      def add_participant(shard_index, _node_name, _opts)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def add_participant(shard_index, node_name, opts) do
        with :ok <- validate_node_name(node_name),
             {:ok, timeout_ms} <-
               validate_membership_timeout_ms(Keyword.get(opts, :timeout_ms, 10_000)) do
          add_participant_or_redirect(shard_index, node_name, timeout_ms, @config_redirects)
        end
      end

      @doc false
      @spec add_participant_redirected(
              non_neg_integer(),
              node(),
              non_neg_integer(),
              non_neg_integer()
            ) ::
              {:ok, tuple()} | {:error, term()}
      def add_participant_redirected(shard_index, _node_name, _timeout_ms, _redirects_left)
          when invalid_shard_index_shape(shard_index),
          do: invalid_shard_index_error(shard_index)

      def add_participant_redirected(_shard_index, _node_name, _timeout_ms, redirects_left)
          when invalid_redirects_left_shape(redirects_left),
          do: invalid_redirects_left_error(redirects_left)

      def add_participant_redirected(shard_index, node_name, timeout_ms, redirects_left)
          when is_integer(redirects_left) and redirects_left >= 0 do
        with :ok <- validate_node_name(node_name),
             {:ok, timeout_ms} <- validate_membership_timeout_ms(timeout_ms) do
          add_participant_or_redirect(shard_index, node_name, timeout_ms, redirects_left)
        end
      end

      defp add_member_or_redirect(shard_index, node_name, timeout_ms, redirects_left) do
        case workflow_redirect_node(shard_index, redirects_left) do
          leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
            redirect_add_member(leader_node, shard_index, node_name, timeout_ms, redirects_left)

          _other ->
            MembershipGate.with_membership_change(fn ->
              add_member_local(shard_index, node_name, timeout_ms)
            end)
        end
      end

      defp add_member_local(shard_index, node_name, timeout_ms) do
        with :ok <- ensure_participant(shard_index, node_name, timeout_ms),
             {:ok, snapshot_position, snapshot_path} <- create_transfer_snapshot(shard_index),
             {:ok, _transport_id} <-
               transfer_snapshot(
                 shard_index,
                 node_name,
                 snapshot_position,
                 snapshot_path,
                 timeout_ms
               ),
             :ok <- wait_peer_ready(shard_index, node_name, timeout_ms),
             {:ok, position} <- promote_participant(shard_index, node_name, timeout_ms) do
          {:ok, position}
        else
          :already_member ->
            current_member_position(shard_index, node_name, timeout_ms)

          {:error, :already_member} ->
            current_member_position(shard_index, node_name, timeout_ms)

          other ->
            other
        end
      end

      defp add_participant_or_redirect(shard_index, node_name, timeout_ms, redirects_left) do
        case workflow_redirect_node(shard_index, redirects_left) do
          leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
            redirect_add_participant(
              leader_node,
              shard_index,
              node_name,
              timeout_ms,
              redirects_left
            )

          _other ->
            MembershipGate.with_membership_change(fn ->
              add_participant_local(shard_index, node_name, timeout_ms)
            end)
        end
      end

      defp add_participant_local(shard_index, node_name, timeout_ms) do
        with :ok <- ensure_participant(shard_index, node_name, timeout_ms),
             {:ok, snapshot_position, snapshot_path} <- create_transfer_snapshot(shard_index),
             {:ok, _transport_id} <-
               transfer_snapshot(
                 shard_index,
                 node_name,
                 snapshot_position,
                 snapshot_path,
                 timeout_ms
               ),
             :ok <- wait_peer_ready(shard_index, node_name, timeout_ms) do
          {:ok, snapshot_position}
        else
          :already_member ->
            {:error, :already_member}

          other ->
            other
        end
      end

      @spec peer_ready(non_neg_integer(), node()) :: :ok | {:error, term()}
      def peer_ready(shard_index, _node_name) when invalid_shard_index_shape(shard_index),
        do: invalid_shard_index_error(shard_index)

      def peer_ready(shard_index, node_name) do
        with :ok <- validate_node_name(node_name) do
          do_peer_ready(shard_index, node_name)
        end
      end

      defp do_peer_ready(shard_index, node_name) do
        partition = partition(shard_index)
        server = server_name(partition)
        backend_call(fn -> :wa_raft_server.is_peer_ready(server, {server, node_name}) end)
      end
    end
  end
end
