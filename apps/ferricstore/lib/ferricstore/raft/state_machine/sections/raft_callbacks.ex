defmodule Ferricstore.Raft.StateMachine.Sections.RaftCallbacks do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.CommandTime
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Flow
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.HLC

      alias Ferricstore.Store.{
        BitcaskWriter,
        BlobRef,
        BlobStore,
        BlobValue,
        ColdRead,
        CompoundKey,
        ExpiryTracker,
        LFU,
        ListOps,
        Promotion,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp apply_cross_shard_tx(meta, shard_batches, watched_keys, state) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            if transaction_watches_clean?(watched_keys, state) do
              with_cross_shard_pending_writes(state, fn ->
                with {:ok, ordered_entries} <-
                       shard_batches
                       |> cross_shard_ordered_entries()
                       |> prepare_cross_shard_policy_entries(state) do
                  Process.put(:tx_deleted_keys, MapSet.new())
                  Process.put(:tx_pending_values, %{})

                  try do
                    case Enum.reduce_while(ordered_entries, {:ok, %{}, %{}}, fn
                           {_orig_idx, shard_idx, pos, entry, sandbox_namespace},
                           {:ok, results, stores} ->
                             {store, stores} =
                               case Map.fetch(stores, shard_idx) do
                                 {:ok, cached} ->
                                   {cached, stores}

                                 :error ->
                                   store = build_cross_shard_store(shard_idx, state)
                                   {store, Map.put(stores, shard_idx, store)}
                               end

                             result =
                               dispatch_cross_shard_entry(entry, sandbox_namespace, store, state)

                             case cross_shard_fatal_entry_error(entry, result) do
                               {:error, _reason} = error ->
                                 {:halt, error}

                               nil ->
                                 results =
                                   Map.update(
                                     results,
                                     shard_idx,
                                     %{pos => result},
                                     fn shard_results ->
                                       Map.put(shard_results, pos, result)
                                     end
                                   )

                                 {:cont, {:ok, results, stores}}
                             end
                         end) do
                      {:ok, results_by_position, _stores} ->
                        cross_shard_results_by_batch_position(shard_batches, results_by_position)

                      {:error, _reason} = error ->
                        error
                    end
                  after
                    Process.delete(:tx_deleted_keys)
                    Process.delete(:tx_pending_values)
                  end
                end
              end)
            else
              nil
            end

          case write_result do
            {:error, _reason} = error ->
              new_state = %{state | applied_count: old_count + 1}
              maybe_release_cursor(meta, old_count, new_state, error)

            {:error, reason, flushed_state} ->
              new_state = %{flushed_state | applied_count: old_count + 1}
              maybe_release_cursor(meta, old_count, new_state, {:error, reason})

            {shard_results, flushed_state} ->
              new_state = %{flushed_state | applied_count: old_count + 1}
              maybe_release_cursor(meta, old_count, new_state, shard_results)

            nil ->
              new_state = %{state | applied_count: old_count + 1}
              maybe_release_cursor(meta, old_count, new_state, nil)
          end
        end)
      end

      defp normalize_generic_batch_commands(commands) when is_list(commands) do
        Enum.flat_map(commands, &expand_generic_batch_command/1)
      end

      defp expand_generic_batch_command({:batch, commands}) when is_list(commands) do
        normalize_generic_batch_commands(commands)
      end

      defp expand_generic_batch_command({:put_batch, entries}) when is_list(entries) do
        Enum.map(entries, fn
          {key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          invalid -> {:invalid_put_batch_entry, invalid}
        end)
      end

      defp expand_generic_batch_command({:put_blob_batch, entries}) when is_list(entries) do
        Enum.map(entries, fn
          {key, value, expire_at_ms, :value} ->
            {:put, key, value, expire_at_ms}

          {key, encoded_ref, expire_at_ms, :blob_ref} ->
            {:put_blob_ref, key, encoded_ref, expire_at_ms}

          invalid ->
            {:invalid_put_blob_batch_entry, invalid}
        end)
      end

      defp expand_generic_batch_command({:delete_batch, keys}) when is_list(keys) do
        Enum.map(keys, &{:delete, &1})
      end

      defp expand_generic_batch_command(command), do: [command]

      defp finish_hot_batch_apply(
             meta,
             old_count,
             state,
             applied_increment,
             {:error, _reason} = error
           ) do
        new_state = %{state | applied_count: old_count + applied_increment}
        maybe_release_cursor(meta, old_count, new_state, error)
      end

      defp finish_hot_batch_apply(meta, old_count, state, applied_increment, results) do
        new_state = %{state | applied_count: old_count + applied_increment}
        maybe_release_cursor(meta, old_count, new_state, {:ok, results})
      end

      @doc """
      Lifecycle hook called when the Raft node transitions roles.

      When becoming leader, generates a fresh HLC timestamp via `HLC.now/0` to
      ensure the leader's clock is up to date before it starts stamping commands.
      This is a side-effect only -- it does not affect the deterministic state
      machine output.

      In single-node mode, the node is always the leader. In multi-node clusters,
      this can be used to start/stop leader-only processes (e.g., merge scheduler,
      active expiry sweeper).

      Returns a list of effects (currently empty).
      """
      def state_enter(:leader, _state) do
        # Ensure the leader's HLC is freshly advanced. In multi-node clusters,
        # this guarantees the new leader's clock is at least at wall-clock time
        # before it begins stamping commands for followers to merge.
        HLC.now()
        []
      end

      def state_enter(:follower, _state), do: []
      def state_enter(:candidate, _state), do: []
      def state_enter(:await_condition, _state), do: []
      def state_enter(:delete_and_terminate, _state), do: []
      def state_enter(:receive_snapshot, _state), do: []
      def state_enter(_role, _state), do: []

      @doc """
      Periodic tick callback. Returns a list of effects (currently empty).
      """
      def tick(_time_ms, _state) do
        []
      end

      @doc """
      Initializes non-replicated auxiliary state.

      Aux state is local to each node and not replicated via Raft. Used for
      tracking hot-key statistics and other node-local metadata.
      """
      def init_aux(_name) do
        %{hot_keys: %{}}
      end

      @doc """
      Handles non-replicated auxiliary commands (5-arity new API).

      The `int_state` parameter is ra's internal state and must be passed back
      unchanged in the return tuple.

      Currently supports:
        * `{:cast, {:key_written, key}}` -- Increments a local hot-key counter.
      """
      # Cap hot_keys map to prevent unbounded memory growth. When the map exceeds
      # 10,000 entries, reset it to prevent the ra process heap from growing
      # indefinitely with unique keys. This bounds memory to ~1MB worst case.
      @hot_keys_max_size 10_000

      def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
        hot = aux.hot_keys

        if map_size(hot) >= @hot_keys_max_size do
          # Reset to prevent unbounded growth; start fresh with just this key.
          {:no_reply, %{aux | hot_keys: %{key => 1}}, int_state}
        else
          count = Map.get(hot, key, 0)
          {:no_reply, %{aux | hot_keys: Map.put(hot, key, count + 1)}, int_state}
        end
      end

      def handle_aux(_raft_state, _type, _cmd, aux, int_state) do
        {:no_reply, aux, int_state}
      end

      @doc """
      Returns a summary map for debugging and monitoring.

      Includes the shard index, ETS keydir size, total applied command count,
      and the release_cursor interval.
      """
      def overview(state) do
        ets_size =
          try do
            :ets.info(state.ets, :size)
          rescue
            ArgumentError -> 0
          end

        %{
          shard_index: state.shard_index,
          keydir_size: ets_size,
          applied_count: state.applied_count,
          release_cursor_interval: state.release_cursor_interval
        }
      end

      @doc false
      def __validate_pending_locations__(batch, locations) do
        validate_pending_locations(batch, locations)
      end

      @doc false
      def __apply_pending_locations_for_test__(state, file_id, batch, locations) do
        apply_pending_locations(state, file_id, batch, locations)
      end

      @doc false
      def apply_standalone_batch(commands, state) when is_list(commands) do
        commands = flatten_standalone_batch_commands(commands)

        apply_standalone(fn ->
          case commands do
            [{:cross_shard_tx, _shard_batches} = command] -> apply(%{}, command, state)
            _other -> apply(%{}, {:batch, commands}, state)
          end
        end)
      end

      defp flatten_standalone_batch_commands(commands) do
        Enum.flat_map(commands, fn
          {:batch, inner_commands} when is_list(inner_commands) -> inner_commands
          command -> [command]
        end)
      end

      @doc false
      def apply_standalone_command(command, state) do
        apply_standalone_command(command, %{}, state)
      end

      @doc false
      def apply_standalone_command(command, meta, state) when is_map(meta) do
        apply_standalone(fn -> apply(meta, command, state) end)
      end

      @doc false
      def apply_waraft_storage_command(command, meta, state) when is_map(meta) do
        apply(meta, command, state)
      end

      @doc false
      def apply_waraft_segment_command(command, meta, state, projection_writer)
          when is_map(meta) and is_function(projection_writer, 1) do
        with_waraft_projection_writer(projection_writer, fn ->
          apply_standalone(fn -> apply(meta, command, state) end)
        end)
      end

      @doc false
      def consume_waraft_replay_dependencies do
        apply_state_pop(:waraft_replay_dependencies, %{history: %{}, apply_projection: %{}})
      end

      defp apply_standalone(fun) when is_function(fun, 0) do
        previous = Process.get(@sm_standalone_staged_key, :undefined)
        Process.put(@sm_standalone_staged_key, true)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(@sm_standalone_staged_key)
            value -> Process.put(@sm_standalone_staged_key, value)
          end
        end
      end

      defp with_waraft_projection_writer(projection_writer, fun)
           when is_function(projection_writer, 1) and is_function(fun, 0) do
        previous = Process.get(@sm_waraft_projection_writer_key, :undefined)
        Process.put(@sm_waraft_projection_writer_key, projection_writer)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(@sm_waraft_projection_writer_key)
            value -> Process.put(@sm_waraft_projection_writer_key, value)
          end
        end
      end

      @doc false
      def __compensate_cross_shard_partial_writes_for_test__(state, successful_groups, originals) do
        compensate_cross_shard_partial_writes(state, successful_groups, originals)
      end

      @doc false
      def __append_pending_batch_sync_for_test__(file_path, batch) do
        append_pending_batch_sync(file_path, batch, batch_contains_delete?(batch))
      end

      @doc false
      def __compensate_cross_shard_partial_writes_for_test__(
            state,
            successful_groups,
            originals,
            opts
          ) do
        if Keyword.get(opts, :standalone_staged?, false) do
          apply_standalone(fn ->
            compensate_cross_shard_partial_writes(state, successful_groups, originals)
          end)
        else
          compensate_cross_shard_partial_writes(state, successful_groups, originals)
        end
      end

      # ---------------------------------------------------------------------------
      # Private: release_cursor compaction
      # ---------------------------------------------------------------------------

      # Checks whether the applied_count crossed an interval boundary AND the
      # ra meta contains a valid index. If both conditions are met, emits
      # checkpoint/release_cursor effects so ra can compact the log up to this
      # point.
      #
      # For single commands (put/delete), old_count + 1 == new applied_count,
      # so `div(old, interval) != div(new, interval)` is equivalent to
      # `rem(new, interval) == 0`.
      #
      # For batches, the applied_count may jump by N, potentially crossing one
      # or more interval boundaries. We emit a single release_cursor at the
      # batch's ra index when any boundary was crossed.
      #
      # When meta has no :index (e.g. unit tests calling apply/3 directly with
      # an empty map), the 2-tuple `{state, result}` is returned and no effect
      # is emitted.
      @spec maybe_release_cursor(map(), non_neg_integer(), shard_state(), term()) ::
              {shard_state(), term()} | {shard_state(), term(), list()}
      defp maybe_release_cursor(%{index: ra_index}, old_count, state, result) do
        state = consume_pending_state(state)
        checkpoint_clean_before_write? = apply_state_pop(:checkpoint_clean_before_write) == true
        release_cursor_blocked? = apply_state_pop(:release_cursor_blocked) == true

        checkpoint_dependencies_clean_before_write? =
          apply_state_pop(:checkpoint_dependencies_clean_before_write) == true

        dirty_checkpoint_indices = consume_checkpoint_dirty_indices()
        previous_pending_release_index = Map.get(state, :pending_release_cursor_index)

        previous_pending_checkpoint_indices =
          Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

        record_cursor_metric(state, :last_applied_index, ra_index)

        # Wrap every reply with the ra_index it was applied at so the originating
        # Batcher can wait for the LOCAL state machine to also reach that index
        # before replying to the user. Otherwise a writer on a follower can see
        # the leader's :applied event (replied) before the local follower's state
        # machine has applied — read-your-write violation in cluster mode.
        wrapped_result = {:applied_at, ra_index, result}

        # Notify the local Batcher for this shard that ra_index was applied
        # *locally*. The `:local` send_msg option causes ra to fire this on every
        # node that has a local member (i.e., every voter), so each node's Batcher
        # tracks its OWN last_local_applied_idx independently.
        batcher_name = Ferricstore.Raft.Batcher.batcher_name(state.shard_index)
        notify_effect = {:send_msg, batcher_name, {:locally_applied, ra_index}, [:local]}

        interval = state.release_cursor_interval
        crossed_interval? = div(old_count, interval) != div(state.applied_count, interval)

        checkpoint_clean_now? =
          checkpoint_clean?(state) and checkpoint_indices_clean?(state, dirty_checkpoint_indices)

        {state, checkpoint_effects} =
          if crossed_interval? do
            checkpoint_state =
              state
              |> Map.put(:pending_release_cursor_index, ra_index)
              |> Map.put(
                :pending_release_cursor_checkpoint_indices,
                MapSet.union(previous_pending_checkpoint_indices, dirty_checkpoint_indices)
              )

            if checkpoint_clean_now? do
              {checkpoint_state, []}
            else
              {checkpoint_state, [{:checkpoint, ra_index, checkpoint_state}]}
            end
          else
            {state, []}
          end

        record_pending_checkpoint_count(state)

        pending_checkpoint_indices =
          Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

        pending_checkpoint_clean? = checkpoint_indices_clean?(state, pending_checkpoint_indices)

        release_index =
          cond do
            checkpoint_clean_now? and pending_checkpoint_clean? ->
              Map.get(state, :pending_release_cursor_index)

            checkpoint_dependencies_clean_before_write? ->
              previous_pending_release_index

            checkpoint_clean_before_write? and pending_checkpoint_clean? ->
              previous_pending_release_index

            true ->
              nil
          end

        record_release_cursor_blocked_apply(state, release_cursor_blocked?)

        if release_cursor_blocked? do
          {state, wrapped_result, [notify_effect]}
        else
          {state, release_effects} =
            release_cursor_effects(
              state,
              release_index,
              ra_index,
              crossed_interval?,
              checkpoint_effects
            )

          {state, wrapped_result, [notify_effect | release_effects]}
        end
      end

      defp maybe_release_cursor(_meta, _old_count, state, result) do
        state = consume_pending_state(state)
        Process.delete(@sm_apply_state_key)

        # No meta (e.g. cross-shard sub-apply) — pass through untouched.
        {state, result}
      end

      defp release_cursor_effects(
             state,
             release_index,
             ra_index,
             crossed_interval?,
             checkpoint_effects
           )
           when is_integer(release_index) and release_index > 0 do
        if release_index > cursor_metric(state, :last_released_cursor_index) do
          case ensure_replay_safe_index(state, release_index) do
            {:ready, state} ->
              record_cursor_metric(state, :last_released_cursor_index, release_index)

              state =
                if Map.get(state, :pending_release_cursor_index) == release_index do
                  state
                  |> Map.put(:pending_release_cursor_index, nil)
                  |> Map.put(:pending_replay_safe_marker_index, nil)
                  |> Map.put(:pending_release_cursor_checkpoint_indices, MapSet.new())
                else
                  state
                end

              record_pending_checkpoint_count(state)

              checkpoint_effects =
                if crossed_interval? and release_index == ra_index do
                  [{:checkpoint, ra_index, state} | checkpoint_effects]
                else
                  checkpoint_effects
                end

              {state, Enum.reverse(checkpoint_effects) ++ [{:release_cursor, release_index}]}

            {:pending, state} ->
              {state, checkpoint_effects}
          end
        else
          {state, checkpoint_effects}
        end
      end

      defp release_cursor_effects(
             state,
             _release_index,
             _ra_index,
             _crossed_interval?,
             checkpoint_effects
           ),
           do: {state, checkpoint_effects}

      defp ensure_replay_safe_index(state, release_index) do
        instance_ctx = checkpoint_ctx_for_state(state)

        bitcask_ready? =
          Ferricstore.Raft.ReplaySafeIndexWriter.durable?(
            instance_ctx,
            state.shard_index,
            state.shard_data_path,
            release_index
          )

        lmdb_ready? = flow_lmdb_replay_safe?(state, instance_ctx, release_index)
        history_ready? = flow_history_projector_replay_safe?(state, instance_ctx, release_index)

        if bitcask_ready? and lmdb_ready? and history_ready? do
          {:ready, state}
        else
          case request_replay_safe_indexes(
                 state,
                 instance_ctx,
                 release_index,
                 bitcask_ready?,
                 lmdb_ready?,
                 history_ready?
               ) do
            {:ready, state} -> {:ready, state}
            state -> {:pending, state}
          end
        end
      end

      defp flow_lmdb_replay_safe?(state, instance_ctx, release_index) do
        Ferricstore.Flow.LMDBWriter.durable?(
          instance_ctx,
          state.shard_index,
          state.shard_data_path,
          release_index
        )
      end

      defp flow_history_projector_replay_safe?(state, instance_ctx, release_index) do
        HistoryProjector.durable?(
          instance_ctx,
          state.shard_index,
          state.shard_data_path,
          release_index
        )
      end

      defp request_replay_safe_indexes(
             state,
             instance_ctx,
             release_index,
             bitcask_ready?,
             lmdb_ready?,
             history_ready?
           ) do
        bitcask_status =
          cond do
            bitcask_ready? ->
              :durable

            Map.get(state, :pending_replay_safe_marker_index) == release_index ->
              :requested

            true ->
              Ferricstore.Raft.ReplaySafeIndexWriter.request(
                instance_ctx,
                state.shard_index,
                state.shard_data_path,
                release_index
              )
          end

        lmdb_status =
          cond do
            lmdb_ready? ->
              :durable

            true ->
              Ferricstore.Flow.LMDBWriter.request(
                instance_ctx,
                state.shard_index,
                state.shard_data_path,
                release_index
              )
          end

        history_status =
          cond do
            history_ready? ->
              :durable

            true ->
              HistoryProjector.request(
                instance_ctx,
                state.shard_index,
                state.shard_data_path,
                release_index
              )
          end

        if bitcask_status == :durable and lmdb_status == :durable and history_status == :durable do
          {:ready, Map.put(state, :pending_replay_safe_marker_index, nil)}
        else
          Map.put(state, :pending_replay_safe_marker_index, release_index)
        end
      end

      defp block_release_cursor_for_apply do
        apply_state_put(:release_cursor_blocked, true)
      end

      defp record_release_cursor_blocked_apply(state, true) do
        count = cursor_metric(state, :release_cursor_blocked_apply_count) + 1
        record_cursor_metric(state, :release_cursor_blocked_apply_count, count)

        :telemetry.execute(
          [:ferricstore, :raft, :release_cursor, :blocked],
          %{count: 1, consecutive_count: count},
          %{shard_index: Map.get(state, :shard_index)}
        )
      rescue
        _ -> :ok
      end

      defp record_release_cursor_blocked_apply(state, false) do
        if cursor_metric(state, :release_cursor_blocked_apply_count) != 0 do
          record_cursor_metric(state, :release_cursor_blocked_apply_count, 0)
        end
      rescue
        _ -> :ok
      end

      defp consume_checkpoint_dirty_indices do
        case apply_state_pop(:checkpoint_dirty_indices) do
          %MapSet{} = indices -> indices
          indices when is_list(indices) -> MapSet.new(indices)
          _ -> MapSet.new()
        end
      end

      defp record_checkpoint_dirty_index(shard_index) when is_integer(shard_index) do
        indices = apply_state_get(:checkpoint_dirty_indices, MapSet.new())
        apply_state_put(:checkpoint_dirty_indices, MapSet.put(indices, shard_index))
      end

      defp record_checkpoint_dirty_index(_shard_index), do: :ok

      defp remember_checkpoint_dependencies_clean_before_write(state) do
        if apply_state_get(:checkpoint_dependencies_clean_before_write) != true do
          indices = Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

          if Enum.any?(indices) and checkpoint_indices_clean?(state, indices) do
            apply_state_put(:checkpoint_dependencies_clean_before_write, true)
          end
        end
      rescue
        _ -> :ok
      end

      defp checkpoint_indices_clean?(state, indices) do
        Enum.all?(indices, fn shard_index -> checkpoint_index_clean?(state, shard_index) end)
      end

      defp checkpoint_index_clean?(state, shard_index) do
        case checkpoint_ctx_for_state(state) do
          nil ->
            true

          ctx ->
            flag_idx = shard_index + 1

            checkpoint_ref_clean?(Map.get(ctx, :checkpoint_flags), flag_idx) and
              checkpoint_ref_clean?(Map.get(ctx, :checkpoint_in_flight), flag_idx)
        end
      rescue
        _ -> false
      end

      defp checkpoint_ref_clean?(nil, _flag_idx), do: true

      defp checkpoint_ref_clean?(ref, flag_idx) do
        flag_idx > :atomics.info(ref).size or :atomics.get(ref, flag_idx) == 0
      end

      defp consume_pending_state(state) do
        case apply_state_pop(:pending_state) do
          nil ->
            state

          pending_state ->
            state
            |> Map.merge(%{
              active_file_id: pending_state.active_file_id,
              active_file_path: pending_state.active_file_path,
              active_file_size: pending_state.active_file_size,
              file_stats: pending_state.file_stats
            })
            |> Map.put(
              :promoted_instances,
              Map.get(
                pending_state,
                :promoted_instances,
                Map.get(state, :promoted_instances, %{})
              )
            )
        end
      end

      defp apply_state_get(field, default \\ nil) do
        @sm_apply_state_key
        |> Process.get(%{})
        |> Map.get(field, default)
      end

      defp apply_state_put(field, value) do
        state = Process.get(@sm_apply_state_key, %{})
        Process.put(@sm_apply_state_key, Map.put(state, field, value))
      end

      defp apply_state_pop(field, default \\ nil) do
        state = Process.get(@sm_apply_state_key, %{})
        {value, state} = Map.pop(state, field, default)

        if map_size(state) == 0 do
          Process.delete(@sm_apply_state_key)
        else
          Process.put(@sm_apply_state_key, state)
        end

        value
      end

      defp record_cursor_metric(%{shard_index: shard_index} = state, field, index)
           when is_atom(field) and is_integer(index) and index >= 0 do
        instance_ctx =
          case Map.get(state, :instance_ctx) do
            ctx when is_map(ctx) -> ctx
            _ -> instance_ctx_by_name(Map.get(state, :instance_name, :default))
          end

        case Map.get(instance_ctx, field) do
          ref when is_reference(ref) ->
            size = :atomics.info(ref).size
            if shard_index < size, do: :atomics.put(ref, shard_index + 1, index)

          _ ->
            :ok
        end
      rescue
        _ -> :ok
      end

      defp record_cursor_metric(_state, _field, _index), do: :ok

      defp record_pending_checkpoint_count(state) do
        count =
          state
          |> Map.get(:pending_release_cursor_checkpoint_indices, MapSet.new())
          |> MapSet.size()

        record_cursor_metric(state, :pending_release_cursor_checkpoint_count, count)
      rescue
        _ -> :ok
      end

      defp cursor_metric(%{shard_index: shard_index} = state, field) when is_atom(field) do
        case checkpoint_ctx_for_state(state) |> metric_ref(field) do
          ref when is_reference(ref) ->
            size = :atomics.info(ref).size
            if shard_index < size, do: :atomics.get(ref, shard_index + 1), else: 0

          _ ->
            0
        end
      rescue
        _ -> 0
      end

      defp cursor_metric(_state, _field), do: 0

      defp metric_ref(nil, _field), do: nil
      defp metric_ref(ctx, field), do: Map.get(ctx, field)

      # A release_cursor lets ra compact log entries. For the Bitcask-backed state
      # machine, the log must not be released past writes that are still only in
      # the OS page cache; the checkpoint flag is cleared only after fsync succeeds.
      defp checkpoint_clean?(
             %{instance_ctx: nil, instance_name: name, shard_index: shard_index} = state
           )
           when is_atom(name) do
        case checkpoint_ctx_for_state(state) do
          %FerricStore.Instance{} = instance_ctx ->
            checkpoint_clean?(%{instance_ctx: instance_ctx, shard_index: shard_index})

          _ ->
            name == :default
        end
      end

      # Only legacy/default state-machine callers may release without an Instance.
      # Any unresolved custom or malformed state fails closed because checkpoint
      # atomics are instance-owned and releasing Ra early can discard the only
      # durable copy of un-fsynced Bitcask writes.
      defp checkpoint_clean?(%{instance_ctx: nil, instance_name: :default}), do: true
      defp checkpoint_clean?(%{instance_ctx: nil}), do: false

      defp checkpoint_clean?(%{instance_ctx: instance_ctx, shard_index: shard_index})
           when is_map(instance_ctx) do
        flag_idx = shard_index + 1

        checkpoint_flag_clean? =
          case Map.get(instance_ctx, :checkpoint_flags) do
            nil ->
              true

            checkpoint_flags ->
              flag_idx > :atomics.info(checkpoint_flags).size or
                :atomics.get(checkpoint_flags, flag_idx) == 0
          end

        checkpoint_idle? =
          case Map.get(instance_ctx, :checkpoint_in_flight) do
            nil ->
              true

            checkpoint_in_flight ->
              flag_idx > :atomics.info(checkpoint_in_flight).size or
                :atomics.get(checkpoint_in_flight, flag_idx) == 0
          end

        checkpoint_flag_clean? and checkpoint_idle?
      end

      defp checkpoint_clean?(_state), do: true

      defp with_apply_time(%{system_time: now_ms}, fun) when is_integer(now_ms) do
        clear_stale_pending_state()
        CommandTime.with_now_ms(now_ms, fun)
      end

      defp with_apply_time(_meta, fun) do
        clear_stale_pending_state()
        fun.()
      end

      defp clear_stale_pending_state do
        Process.delete(@sm_apply_state_key)
        :ok
      end

      defp apply_now_ms do
        CommandTime.now_ms()
      end

      defp raft_apply_context(%{apply_context: %Ferricstore.Raft.ApplyContext{} = context}),
        do: context

      defp raft_apply_context(_legacy_state), do: Ferricstore.Raft.ApplyContext.default()
    end
  end
end
