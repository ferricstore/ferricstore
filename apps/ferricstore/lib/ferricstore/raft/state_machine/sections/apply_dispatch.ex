defmodule Ferricstore.Raft.StateMachine.Sections.ApplyDispatch do
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
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Flow
      alias Ferricstore.Flow.Hibernation
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.RetryPolicy
      alias Ferricstore.Flow.SharedRefBackfill
      alias Ferricstore.HLC
      alias Ferricstore.ServerCatalog

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
        TypeRegistry,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      @flush_shard_page_size 512

      def apply(%{index: idx} = meta, _command, %{skip_below_index: skip} = state)
          when skip > 0 and idx <= skip do
        old_count = state.applied_count
        new_state = %{state | applied_count: old_count + 1}

        # Clear skip_below_index once we've passed it — no need to check on every apply
        new_state =
          if idx == skip, do: %{new_state | skip_below_index: 0}, else: new_state

        maybe_release_cursor(meta, old_count, new_state, :ok)
      end

      def apply(%{index: idx} = meta, command, %{skip_below_index: skip} = state)
          when skip > 0 and idx > skip do
        __MODULE__.apply(meta, command, %{state | skip_below_index: 0})
      end

      # Unwrap pre-serialized commands produced by the write Batcher.
      def apply(meta, {:ttb, binary}, state) when is_binary(binary) do
        case CommandStamp.decode_ttb(binary) do
          {:ok, decoded} -> __MODULE__.apply(meta, decoded, state)
          {:error, :invalid_preencoded_command} = error -> {state, error}
        end
      end

      def apply(meta, {:ferricstore_latency_trace, inner_command}, state) do
        previous_trace = Ferricstore.LatencyTrace.start(%{})

        try do
          result =
            Ferricstore.LatencyTrace.span("server_apply_us", fn ->
              __MODULE__.apply(meta, inner_command, state)
            end)

          trace = Ferricstore.LatencyTrace.finish(previous_trace)
          wrap_latency_trace_apply_result(result, trace)
        rescue
          error ->
            _ = Ferricstore.LatencyTrace.finish(previous_trace)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            _ = Ferricstore.LatencyTrace.finish(previous_trace)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      # Async commands. Router on the origin node has already persisted the write
      # Async single-command path. Delegates to apply_single which handles
      # origin-skip via the embedded origin node tag.
      def apply(meta, {:async, _origin, _inner_cmd} = cmd, state) do
        apply_pending_with_time(meta, state, fn -> apply_single(state, cmd) end)
      end

      def apply(meta, {:put, key, value, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:flow_policy_put, key, value, expire_at_ms}, state) do
        apply_flow_pending_with_time(meta, state, :flow_policy_put, %{items: 1}, fn ->
          do_flow_policy_put(state, key, value, expire_at_ms)
        end)
      end

      def apply(meta, {:flow_policy_allocate, key, value, expire_at_ms}, state) do
        apply_flow_pending_with_time(meta, state, :flow_policy_allocate, %{items: 1}, fn ->
          do_flow_policy_allocate(state, key, value, expire_at_ms)
        end)
      end

      def apply(meta, {:flow_policy_fence, installs, command}, state)
          when is_list(installs) and is_tuple(command) do
        apply_pending_with_time(meta, state, fn ->
          apply_flow_policy_fence(state, installs, command)
        end)
      end

      def apply(meta, {:flow_policy_attribute_catalog_repair_request, key, name}, state) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_policy_attribute_catalog_repair_request,
          %{items: 1},
          fn -> do_flow_policy_attribute_catalog_repair_request(state, key, name) end
        )
      end

      def apply(meta, {:flow_policy_attribute_catalog_repair, attrs}, state)
          when is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_policy_attribute_catalog_repair,
          %{items: 1},
          fn -> do_flow_policy_attribute_catalog_repair(state, attrs) end
        )
      end

      def apply(meta, {:flow_policy_migration_step, plan}, state) when is_map(plan) do
        item_count = telemetry_list_size(Map.get(plan, :catalog_entries, []))

        apply_flow_pending_with_time(
          meta,
          state,
          :flow_policy_migration_step,
          %{items: item_count},
          fn -> do_flow_policy_migration_step(state, plan) end
        )
      end

      def apply(meta, {:flow_governance_limit_mutate, key, attrs}, state)
          when is_binary(key) and is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_governance_limit_mutate,
          %{items: Map.get(attrs, :amount, 1)},
          fn -> do_flow_governance_limit_mutate(state, key, attrs) end
        )
      end

      def apply(meta, command, state)
          when is_tuple(command) and tuple_size(command) > 0 and
                 elem(command, 0) == :flow_governance_limit_mutate do
        with_apply_time(meta, fn ->
          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}

          maybe_release_cursor(
            meta,
            old_count,
            new_state,
            {:error, "ERR invalid flow limit mutation"}
          )
        end)
      end

      def apply(
            meta,
            {:flow_governance_limit_catalog_outbox_ack, _key, shard_index, expected_head, up_to},
            state
          )
          when is_integer(shard_index) and shard_index >= 0 and is_integer(expected_head) and
                 expected_head > 0 and is_integer(up_to) and up_to >= expected_head and
                 up_to - expected_head < 256 do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_governance_limit_catalog_outbox_ack,
          %{items: up_to - expected_head + 1},
          fn ->
            do_flow_governance_limit_catalog_outbox_ack(
              state,
              shard_index,
              expected_head,
              up_to
            )
          end
        )
      end

      def apply(
            meta,
            {:flow_governance_limit_catalog_outbox_ack, _key, _shard_index, _expected_head,
             _up_to},
            state
          ) do
        with_apply_time(meta, fn ->
          bump_applied(
            meta,
            state,
            {:error, "ERR invalid flow limit catalog publication acknowledgement"}
          )
        end)
      end

      def apply(meta, {:flow_policy_catalog_backfill_step, request}, state)
          when is_map(request) do
        item_count = telemetry_list_size(Map.get(request, :candidates, []))

        apply_flow_pending_with_time(
          meta,
          state,
          :flow_policy_catalog_backfill_step,
          %{items: item_count},
          fn -> do_flow_policy_catalog_backfill_step(state, request) end
        )
      end

      def apply(
            meta,
            {:flow_governance_release_outbox_ack, _key, shard_index, expected_head, up_to},
            state
          )
          when is_integer(shard_index) and shard_index >= 0 and is_integer(expected_head) and
                 expected_head > 0 and is_integer(up_to) and up_to >= expected_head and
                 up_to - expected_head < 256 do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_governance_release_outbox_ack,
          %{items: max(up_to - expected_head + 1, 0)},
          fn ->
            do_flow_governance_release_outbox_ack(
              state,
              shard_index,
              expected_head,
              up_to
            )
          end
        )
      end

      def apply(
            meta,
            {:flow_governance_release_outbox_mark_completed, _key, shard_index, sequences},
            state
          )
          when is_integer(shard_index) and shard_index >= 0 and is_list(sequences) and
                 sequences != [] and length(sequences) <= 256 do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_governance_release_outbox_mark_completed,
          %{items: length(sequences)},
          fn ->
            do_flow_governance_release_outbox_mark_completed(
              state,
              shard_index,
              sequences
            )
          end
        )
      end

      def apply(
            meta,
            {:flow_governance_release_outbox_mark_completed, _key, _shard_index, _sequences},
            state
          ) do
        with_apply_time(meta, fn ->
          bump_applied(
            meta,
            state,
            {:error, "ERR invalid flow governance release outbox completion"}
          )
        end)
      end

      def apply(
            meta,
            {:flow_governance_release_outbox_ack, _key, _shard_index, _expected_head, _up_to},
            state
          ) do
        with_apply_time(meta, fn ->
          bump_applied(
            meta,
            state,
            {:error, "ERR invalid flow governance release outbox acknowledgement"}
          )
        end)
      end

      def apply(meta, {:put_blob_ref, key, encoded_ref, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms)
        end)
      end

      def apply(meta, {:set, key, value, expire_at_ms, opts}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_set(state, key, value, expire_at_ms, opts) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:set_blob_ref, key, encoded_ref, expire_at_ms, opts}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
        end)
      end

      def apply(meta, {:delete, key}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn -> do_delete(state, key) end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:expire_if_batch, entries}, state) when is_list(entries) do
        apply_pending_with_time(meta, state, fn ->
          do_expire_if_batch(state, entries, apply_now_ms())
        end)
      end

      defp do_expire_if_batch(state, entries, now_ms) do
        matched_keys =
          Enum.reduce(entries, MapSet.new(), fn
            {key, expected_expire_at_ms}, acc
            when is_binary(key) and is_integer(expected_expire_at_ms) and
                   expected_expire_at_ms > 0 and expected_expire_at_ms <= now_ms ->
              redis_key = CompoundKey.extract_redis_key(key)

              case {:ets.lookup(state.ets, key), check_key_lock(state, redis_key, nil)} do
                {[{^key, _value, ^expected_expire_at_ms, _lfu, _file_id, _offset, _value_size}],
                 :ok} ->
                  MapSet.put(acc, key)

                _stale_or_locked ->
                  acc
              end

            _invalid, acc ->
              acc
          end)

        with :ok <- expire_matching_plain_keys(state, matched_keys),
             :ok <- expire_matching_compound_keys(state, matched_keys) do
          Enum.map(entries, fn {key, _expire_at_ms} -> MapSet.member?(matched_keys, key) end)
        end
      end

      defp expire_matching_plain_keys(state, matched_keys) do
        Enum.reduce_while(matched_keys, :ok, fn key, :ok ->
          if CompoundKey.internal_key?(key) do
            {:cont, :ok}
          else
            case do_delete(state, key) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end
        end)
      end

      defp expire_matching_compound_keys(state, matched_keys) do
        matched_keys
        |> Enum.filter(&CompoundKey.internal_key?/1)
        |> Enum.group_by(&CompoundKey.extract_redis_key/1)
        |> Enum.reduce_while(:ok, fn {redis_key, keys}, :ok ->
          case do_compound_batch_delete(state, redis_key, keys) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      def apply(meta, {:put_batch, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_put_batch_entries(state, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:mset, entries}, state) when is_list(entries) do
        apply_pending_with_time(meta, state, fn ->
          apply_atomic_string_batch(state, entries, :mset, :plain)
        end)
      end

      def apply(meta, {:mset_blob_batch, entries}, state) when is_list(entries) do
        apply_pending_with_time(meta, state, fn ->
          apply_atomic_string_batch(state, entries, :mset, :blob)
        end)
      end

      def apply(meta, {:msetnx, entries}, state) when is_list(entries) do
        apply_pending_with_time(meta, state, fn ->
          apply_atomic_string_batch(state, entries, :msetnx, :plain)
        end)
      end

      def apply(meta, {:msetnx_blob_batch, entries}, state) when is_list(entries) do
        apply_pending_with_time(meta, state, fn ->
          apply_atomic_string_batch(state, entries, :msetnx, :blob)
        end)
      end

      def apply(meta, {:put_blob_batch, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_put_blob_batch_entries(state, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:flush_shard, {physical_ms, logical}}, state)
          when is_integer(physical_ms) and physical_ms >= 0 and is_integer(logical) and
                 logical >= 0 do
        with_apply_time(meta, fn ->
          old_count = state.applied_count
          flush_state = abort_flush_shard_transactions(state)
          result = apply_flush_shard(flush_state)

          new_state =
            case result do
              {:ok, _deleted} -> flush_state
              {:error, _reason} -> state
            end
            |> Map.put(:applied_count, old_count + 1)

          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:delete_batch, keys}, state) when is_list(keys) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_delete_batch_keys(state, keys)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(keys), write_result)
        end)
      end

      defp apply_flush_shard(state) do
        try do
          :ets.safe_fixtable(state.ets, true)

          try do
            match_spec = [{{:"$1", :_, :_, :_, :_, :_, :_}, [{:is_binary, :"$1"}], [:"$1"]}]

            case apply_flush_shard_pages(
                   state,
                   :ets.select(state.ets, match_spec, @flush_shard_page_size),
                   0
                 ) do
              {:ok, deleted} ->
                _reset = CompoundMemberIndex.reset(Map.get(state, :compound_member_index_name))
                _state = ZSetIndex.reset(state)
                apply_state_put(:flow_due_catalog, Ferricstore.Flow.DueCatalog.new())
                {:ok, deleted}

              {:error, _reason} = error ->
                error
            end
          after
            :ets.safe_fixtable(state.ets, false)
          end
        rescue
          error in ArgumentError -> {:error, {:flush_shard_keydir_unavailable, error}}
        end
      end

      defp abort_flush_shard_transactions(state) do
        state
        |> Map.put(:cross_shard_locks, %{})
        |> Map.put(:cross_shard_intents, %{})
        |> Map.put(:cross_shard_lock_expiries, {0, nil})
      end

      defp apply_flush_shard_pages(_state, :"$end_of_table", deleted), do: {:ok, deleted}

      defp apply_flush_shard_pages(state, {keys, continuation}, deleted) do
        delete_keys = Enum.reject(keys, &flush_shard_preserved_key?(state, &1))

        with {:ok, page_deleted} <- apply_flush_shard_page(state, delete_keys) do
          apply_flush_shard_pages(state, :ets.select(continuation), deleted + page_deleted)
        end
      end

      defp apply_flush_shard_page(_state, []), do: {:ok, 0}

      defp apply_flush_shard_page(state, keys) do
        case with_pending_writes(state, fn -> apply_delete_batch_keys(state, keys) end) do
          results when is_list(results) ->
            if length(results) == length(keys) and Enum.all?(results, &(&1 == :ok)) do
              {:ok, length(keys)}
            else
              {:error, {:flush_shard_delete_failed, results}}
            end

          {:error, _reason} = error ->
            error

          other ->
            {:error, {:flush_shard_delete_failed, other}}
        end
      end

      defp flush_shard_preserved_key?(state, key) do
        ServerCatalog.internal_key?(key) or
          key == FlowKeys.shared_value_ref_backfill_key(state.shard_index) or
          key == SharedRefBackfill.progress_key(state.shard_index)
      end

      def apply(meta, {:delete_prefix, prefix}, state) when is_binary(prefix) do
        with_apply_time(meta, fn ->
          result = with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:batch, commands}, state) when is_list(commands) do
        with_apply_time(meta, fn ->
          commands = normalize_generic_batch_commands(commands)
          old_count = state.applied_count
          applied_increment = length(commands)

          # All commands in a batch share one pending-writes buffer so they
          # are flushed in a single v2_append_batch_nosync NIF call.
          write_result =
            case generic_batch_barrier_kind(commands) do
              nil ->
                case prepare_apply_blob_command(state, {:batch, commands}) do
                  {:ok, {:batch, prepared_commands}} ->
                    with_pending_writes(state, fn ->
                      Enum.map_reduce(prepared_commands, old_count, fn cmd, count ->
                        result = apply_single(state, cmd)
                        {result, count + 1}
                      end)
                    end)

                  {:ok, prepared_command} ->
                    with_pending_writes(state, fn ->
                      result = apply_single(state, prepared_command)
                      {List.wrap(result), old_count + applied_increment}
                    end)

                  {:error, _reason} = error ->
                    error
                end

              barrier_kind ->
                {:error, {:batch_barrier_command, barrier_kind}}
            end

          case write_result do
            {:error, _reason} = error ->
              new_state = %{state | applied_count: old_count + applied_increment}
              maybe_release_cursor(meta, old_count, new_state, error)

            {results, new_count} ->
              new_state = %{state | applied_count: new_count}
              maybe_release_cursor(meta, old_count, new_state, {:ok, results})
          end
        end)
      end

      defp generic_batch_barrier_kind(commands) do
        Enum.find_value(commands, &Ferricstore.Raft.CommandBatching.barrier_kind/1)
      end

      defp telemetry_list_size(items) when is_list(items), do: length(items)
      defp telemetry_list_size(_invalid), do: 0

      def apply(meta, {:cross_shard_tx, shard_batches}, state) when is_list(shard_batches) do
        apply_cross_shard_tx(meta, shard_batches, %{}, state)
      end

      defp wrap_latency_trace_apply_result({state, result}, trace) do
        {state, Ferricstore.LatencyTrace.wrap_result(result, trace)}
      end

      defp wrap_latency_trace_apply_result({state, result, effects}, trace) do
        {state, Ferricstore.LatencyTrace.wrap_result(result, trace), effects}
      end

      # Router.list_op/3 submits this canonical Raft command. Apply translates
      # the logical operation into compound list rows inside one pending-write scope.
      def apply(meta, {:list_op, key, operation}, state) do
        with_apply_time(meta, fn ->
          result = with_pending_writes(state, fn -> do_checked_list_op(state, key, operation) end)

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:list_op_lmove, src_key, dst_key, from_dir, to_dir}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_lmove(state, src_key, dst_key, from_dir, to_dir)
        end)
      end

      def apply(meta, {:hset_single, key, field, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:hset_single, key, field, value})
        end)
      end

      def apply(meta, {:lpush_single, key, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:lpush_single, key, value})
        end)
      end

      def apply(meta, {:rpush_single, key, value}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:rpush_single, key, value})
        end)
      end

      def apply(meta, {:sadd_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:sadd_single, key, member})
        end)
      end

      def apply(meta, {:srem_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:srem_single, key, member})
        end)
      end

      def apply(meta, {:zadd_single, key, score, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:zadd_single, key, score, member})
        end)
      end

      def apply(meta, {:zadd_many_single, entries}, state) when is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              {apply_zadd_many_single_entries(state, entries), old_count + length(entries)}
            end)

          case write_result do
            {:error, _reason} = error ->
              new_state = %{state | applied_count: old_count + length(entries)}
              maybe_release_cursor(meta, old_count, new_state, error)

            {results, new_count} ->
              new_state = %{state | applied_count: new_count}
              maybe_release_cursor(meta, old_count, new_state, {:ok, results})
          end
        end)
      end

      def apply(meta, {:zrem_single, key, member}, state) do
        apply_pending_with_time(meta, state, fn ->
          apply_single(state, {:zrem_single, key, member})
        end)
      end

      def apply(meta, {:compound_type_claim, redis_key, type}, state)
          when is_binary(redis_key) and type in [:hash, :list, :set, :zset, :stream] do
        with_apply_time(meta, fn ->
          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  TypeRegistry.serialized_claim_status(
                    redis_key,
                    type,
                    build_compound_store(state)
                  )
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:compound_put, compound_key, value, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms)
        end)
      end

      def apply(meta, {:compound_batch_put, redis_key, entries}, state)
          when is_binary(redis_key) and is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_batch_put_entries(state, redis_key, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:compound_blob_batch_put, redis_key, entries}, state)
          when is_binary(redis_key) and is_list(entries) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_blob_batch_put_entries(state, redis_key, entries)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
        end)
      end

      def apply(meta, {:compound_delete, compound_key}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_delete(state, redis_key, compound_key)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:compound_batch_delete, redis_key, compound_keys}, state)
          when is_binary(redis_key) and is_list(compound_keys) do
        with_apply_time(meta, fn ->
          old_count = state.applied_count

          write_result =
            with_pending_writes(state, fn ->
              apply_compound_batch_delete_keys(state, redis_key, compound_keys)
            end)

          finish_hot_batch_apply(meta, old_count, state, length(compound_keys), write_result)
        end)
      end

      def apply(meta, {:compound_delete_prefix, prefix}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

          result =
            case check_key_lock(state, redis_key, nil) do
              :ok ->
                with_pending_writes(state, fn ->
                  do_compound_delete_prefix(state, redis_key, prefix)
                end)

              {:error, :key_locked} ->
                {:error, :key_locked}
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:incr, key, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_incr(state, key, delta) end)
      end

      def apply(meta, {:incr_float, key, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_incr_float(state, key, delta) end)
      end

      def apply(meta, {:append, key, suffix}, state) do
        apply_pending_with_time(meta, state, fn -> do_append(state, key, suffix) end)
      end

      def apply(meta, {:append_blob_ref, key, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn -> do_append_blob_ref(state, key, encoded_ref) end)
      end

      def apply(meta, {:getset, key, new_value}, state) do
        apply_pending_with_time(meta, state, fn -> do_getset(state, key, new_value) end)
      end

      def apply(meta, {:getset_blob_ref, key, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn -> do_getset_blob_ref(state, key, encoded_ref) end)
      end

      def apply(meta, {:getdel, key}, state) do
        apply_pending_with_time(meta, state, fn -> do_getdel(state, key) end)
      end

      def apply(meta, {:getex, key, expire_at_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_getex(state, key, expire_at_ms) end)
      end

      def apply(meta, {:setrange, key, offset, value}, state) do
        apply_pending_with_time(meta, state, fn -> do_setrange(state, key, offset, value) end)
      end

      def apply(meta, {:setrange_blob_ref, key, offset, encoded_ref}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_setrange_blob_ref(state, key, offset, encoded_ref)
        end)
      end

      # Atomic SETBIT — read bitmap blob, mutate one bit, write back. Previously
      # the read+compute+write ran in the caller process (FerricStore.setbit/3),
      # losing updates under concurrent writes on the same key.
      def apply(meta, {:setbit, key, offset, bit_val}, state) do
        apply_pending_with_time(meta, state, fn -> do_setbit(state, key, offset, bit_val) end)
      end

      # Atomic HINCRBY / HINCRBYFLOAT — read compound field, add delta, write back.
      # Previously ran in caller process and lost updates under concurrent hincrby
      # on the same field.
      def apply(meta, {:hincrby, key, field, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_hincrby(state, key, field, delta) end)
      end

      def apply(meta, {:hincrbyfloat, key, field, delta}, state) do
        apply_pending_with_time(meta, state, fn -> do_hincrbyfloat(state, key, field, delta) end)
      end

      # Atomic ZINCRBY — read zset member's score, add increment, write back.
      # Also sets the type metadata atomically if absent (first write to the key).
      def apply(meta, {:zincrby, key, increment, member}, state) do
        apply_pending_with_time(meta, state, fn -> do_zincrby(state, key, increment, member) end)
      end

      def apply(meta, {:pfadd, key, elements}, state) do
        apply_pending_with_time(meta, state, fn ->
          HyperLogLog.handle_ast({:pfadd, [key | elements]}, build_string_value_store(state))
        end)
      end

      def apply(meta, {:pfmerge, dest_key, source_sketches}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_pfmerge(state, dest_key, source_sketches)
        end)
      end

      def apply(meta, {:pfmerge, dest_key, _source_keys, source_sketches}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_pfmerge(state, dest_key, source_sketches)
        end)
      end

      def apply(meta, {:spop, key, count}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_spop(state, key, count, Map.get(meta, :index, 0))
        end)
      end

      def apply(meta, {:zpop, key, count, direction}, state) do
        apply_pending_with_time(meta, state, fn -> do_zpop(state, key, count, direction) end)
      end

      def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_cas(state, key, expected, new_value, ttl_ms)
        end)
      end

      def apply(meta, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_cas_blob_ref(state, key, expected, encoded_ref, ttl_ms)
        end)
      end

      def apply(meta, {:lock, key, owner, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_lock(state, key, owner, ttl_ms) end)
      end

      def apply(meta, {:unlock, key, owner}, state) do
        apply_pending_with_time(meta, state, fn -> do_unlock(state, key, owner) end)
      end

      def apply(meta, {:extend, key, owner, ttl_ms}, state) do
        apply_pending_with_time(meta, state, fn -> do_extend(state, key, owner, ttl_ms) end)
      end

      def apply(meta, {:ratelimit_add, key, window_ms, max, count}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_ratelimit_add(state, key, window_ms, max, count)
        end)
      end

      # ---------------------------------------------------------------------------
      # Cross-shard operation commands (mini-percolator)
      #
      # These commands support the CrossShardOp protocol: per-key locking through
      # Raft consensus, intent records for crash recovery, and locked writes.
      # ---------------------------------------------------------------------------

      def apply(meta, {:lock_keys, keys, owner_ref, expire_at_ms}, state) do
        with_apply_time(meta, fn ->
          {new_state, result} = do_lock_keys(state, keys, owner_ref, expire_at_ms)
          old_count = state.applied_count
          new_state = %{new_state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:renew_key_locks, keys, owner_ref, expire_at_ms}, state) do
        apply_control_with_time(meta, state, fn ->
          do_renew_key_locks(state, keys, owner_ref, expire_at_ms)
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_lock, key, outcome_key, owner_ref, expire_at_ms},
            state
          )
          when is_binary(key) and is_binary(outcome_key) and is_binary(owner_ref) and
                 byte_size(owner_ref) <= 512 and is_integer(expire_at_ms) and expire_at_ms > 0 do
        apply_control_with_time(meta, state, fn ->
          do_fetch_or_compute_lock(state, key, outcome_key, owner_ref, expire_at_ms)
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_lock, _key, _outcome_key, _owner_ref, _expire_at_ms},
            state
          ) do
        apply_control_with_time(meta, state, fn ->
          {state, {:error, "ERR invalid fetch_or_compute lock command"}}
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_fail, key, outcome_key, encoded_error, outcome_expire_at_ms,
             owner_ref},
            state
          )
          when is_binary(key) and is_binary(outcome_key) and is_binary(encoded_error) and
                 byte_size(encoded_error) <= 65_541 and is_integer(outcome_expire_at_ms) and
                 outcome_expire_at_ms > 0 and is_binary(owner_ref) and byte_size(owner_ref) <= 512 do
        apply_control_with_time(meta, state, fn ->
          do_fetch_or_compute_fail(
            state,
            key,
            outcome_key,
            encoded_error,
            outcome_expire_at_ms,
            owner_ref
          )
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_fail, _key, _outcome_key, _encoded_error, _outcome_expire_at_ms,
             _owner_ref},
            state
          ) do
        apply_control_with_time(meta, state, fn ->
          {state, {:error, "ERR invalid fetch_or_compute failure command"}}
        end)
      end

      def apply(meta, {:fetch_or_compute_publish, key, value, expire_at_ms, owner_ref}, state)
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 and is_binary(owner_ref) and byte_size(owner_ref) <= 512 do
        apply_control_with_time(meta, state, fn ->
          do_fetch_or_compute_publish(state, key, value, expire_at_ms, owner_ref)
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_publish, _key, _value, _expire_at_ms, _owner_ref},
            state
          ) do
        apply_control_with_time(meta, state, fn ->
          {state, {:error, "ERR invalid fetch_or_compute publish command"}}
        end)
      end

      def apply(
            meta,
            {:fetch_or_compute_publish_blob_ref, key, encoded_ref, expire_at_ms, owner_ref},
            state
          )
          when is_binary(key) and is_binary(encoded_ref) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 and is_binary(owner_ref) and byte_size(owner_ref) <= 512 do
        apply_control_with_time(meta, state, fn ->
          do_fetch_or_compute_publish_blob_ref(
            state,
            key,
            encoded_ref,
            expire_at_ms,
            owner_ref
          )
        end)
      end

      def apply(meta, {:unlock_keys, keys, owner_ref}, state) do
        apply_control_with_time(meta, state, fn -> do_unlock_keys(state, keys, owner_ref) end)
      end

      def apply(meta, {:unlock_keys_owned, keys, owner_ref}, state) do
        apply_control_with_time(meta, state, fn ->
          do_unlock_keys_owned(state, keys, owner_ref)
        end)
      end

      def apply(meta, {:cross_shard_intent, owner_ref, intent_map}, state) do
        apply_control_with_time(meta, state, fn ->
          do_write_intent(state, owner_ref, intent_map)
        end)
      end

      def apply(meta, {:delete_intent, owner_ref}, state) do
        apply_control_with_time(meta, state, fn -> do_delete_intent(state, owner_ref) end)
      end

      def apply(meta, {:get_intents}, state) do
        apply_control_with_time(meta, state, fn -> {state, do_get_intents(state)} end)
      end

      def apply(meta, {:get_lock_count}, state) do
        apply_control_with_time(meta, state, fn ->
          {state, map_size(Map.get(state, :cross_shard_locks, %{}))}
        end)
      end

      def apply(meta, {:clear_locks}, state) do
        apply_control_with_time(meta, state, fn ->
          {
            state
            |> Map.put(:cross_shard_locks, %{})
            |> Map.put(:cross_shard_lock_expiries, :gb_trees.empty())
            |> Map.put(:cross_shard_intents, %{}),
            :ok
          }
        end)
      end

      def apply(meta, {:locked_put, key, value, expire_at_ms, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}, state) do
        apply_pending_with_time(meta, state, fn ->
          do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref)
        end)
      end

      def apply(meta, {:locked_delete, key, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_delete(state, key) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      def apply(meta, {:locked_delete_prefix, prefix, owner_ref}, state) do
        with_apply_time(meta, fn ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

          result =
            case check_key_lock(state, redis_key, owner_ref) do
              :ok ->
                with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

              {:error, _} = err ->
                err
            end

          old_count = state.applied_count
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      # ---------------------------------------------------------------------------
      # Probabilistic data structure commands (bloom, CMS, cuckoo, TopK)
      #
      # These commands replicate prob mutations through Raft so that followers
      # apply the same NIF writes to their local prob files. Read commands
      # (BF.EXISTS, CMS.QUERY, etc.) bypass Raft and go directly to the local
      # stateless pread NIF.
      # ---------------------------------------------------------------------------

      # -- Bloom --

      def apply(meta, {:bloom_create, key, num_bits, num_hashes, prob_meta}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
        end)
      end

      def apply(meta, {:bloom_add, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:bloom_madd, key, elements, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "bloom")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.bloom_file_madd(path, elements)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      # -- CMS --

      def apply(meta, {:cms_create, key, width, depth}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_cms_metadata(state, key, width, depth)
        end)
      end

      def apply(meta, {:cms_incrby, key, items}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cms")
          NIF.cms_file_incrby(path, items)
        end)
      end

      def apply(meta, {:cms_merge, dst_key, src_keys, weights, create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          dst_path = prob_path(state, dst_key, "cms")
          src_paths = cms_source_paths(state, src_keys)

          with :ok <- ensure_prob_dir(state) do
            case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
              :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      # -- Cuckoo --

      def apply(meta, {:cuckoo_create, key, capacity, bucket_size}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_cuckoo_metadata(state, key, capacity, bucket_size)
        end)
      end

      def apply(meta, {:cuckoo_add, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_add(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:cuckoo_addnx, key, element, auto_create_params}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")

          with :ok <- ensure_prob_dir(state) do
            case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
              :ok -> NIF.cuckoo_file_addnx(path, element)
              {:error, _reason} = error -> error
            end
          end
        end)
      end

      def apply(meta, {:cuckoo_del, key, element}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "cuckoo")
          NIF.cuckoo_file_del(path, element)
        end)
      end

      # -- TopK --

      def apply(meta, {:topk_create, key, k, width, depth}, state) do
        apply_prob_with_time(meta, state, fn ->
          create_topk_metadata(state, key, k, width, depth)
        end)
      end

      def apply(meta, {:topk_add, key, elements}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_add_v2(path, elements)
        end)
      end

      def apply(meta, {:topk_incrby, key, pairs}, state) do
        apply_prob_with_time(meta, state, fn ->
          path = prob_path(state, key, "topk")
          NIF.topk_file_incrby_v2(path, pairs)
        end)
      end

      def apply(meta, {:tx_execute, queue, sandbox_namespace}, state) when is_list(queue) do
        apply_tx_execute(meta, queue, sandbox_namespace, %{}, state)
      end

      def apply(meta, {:tx_execute, queue, sandbox_namespace, watched_keys}, state)
          when is_list(queue) and is_map(watched_keys) do
        apply_tx_execute(meta, queue, sandbox_namespace, watched_keys, state)
      end

      def apply(meta, {:watch_token, key}, state) when is_binary(key) do
        apply_pending_with_time(meta, state, fn -> transaction_watch_token(state, key) end)
      end

      def apply(meta, {:watch_tokens, keys}, state) when is_list(keys) do
        apply_pending_with_time(meta, state, fn -> transaction_watch_tokens(state, keys) end)
      end

      # -- Flow --

      def apply(meta, {:flow_create, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create, attrs, fn ->
          do_flow_create(state, attrs)
        end)
      end

      def apply(meta, {:flow_create_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create_many, attrs, fn ->
          do_flow_create_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_create_pipeline_batch, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_create_pipeline_batch, attrs, fn ->
          do_flow_create_pipeline_batch(state, attrs)
        end)
      end

      def apply(meta, {:flow_start_and_claim_pipeline_batch, _key, attrs}, state)
          when is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_start_and_claim_pipeline_batch,
          attrs,
          fn ->
            do_flow_start_and_claim_pipeline_batch(state, attrs)
          end
        )
      end

      def apply(meta, {:flow_named_value_put, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_named_value_put, attrs, fn ->
          do_flow_named_value_put(state, attrs)
        end)
      end

      def apply(meta, {:flow_named_value_put_pipeline_batch, _key, attrs}, state)
          when is_map(attrs) do
        apply_flow_pending_with_time(
          meta,
          state,
          :flow_named_value_put_pipeline_batch,
          attrs,
          fn ->
            do_flow_named_value_put_pipeline_batch(state, attrs)
          end
        )
      end

      def apply(meta, {:flow_signal, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_signal, attrs, fn ->
          do_flow_signal(state, attrs)
        end)
      end

      def apply(meta, {:flow_signal_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_signal_many, attrs, fn ->
          do_flow_signal_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_spawn_children, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_spawn_children, attrs, fn ->
          do_flow_spawn_children(state, attrs)
        end)
      end

      def apply(meta, {:flow_claim_due, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_claim_due, attrs, fn ->
          do_flow_claim_due(state, attrs)
        end)
      end

      def apply(meta, {:flow_extend_lease, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_extend_lease, attrs, fn ->
          do_flow_extend_lease(state, attrs)
        end)
      end

      def apply(meta, {:flow_complete, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_complete, attrs, fn ->
          do_flow_complete(state, attrs)
        end)
      end

      def apply(meta, {:flow_complete_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_complete_many, attrs, fn ->
          do_flow_complete_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_terminal_pipeline_batch, op, _key, attrs}, state)
          when op in [:complete, :retry, :fail, :cancel] and is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_terminal_pipeline_batch, attrs, fn ->
          do_flow_terminal_pipeline_batch(state, op, attrs)
        end)
      end

      def apply(meta, {:flow_transition, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_transition, attrs, fn ->
          do_flow_transition(state, attrs)
        end)
      end

      def apply(meta, {:flow_reschedule, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_reschedule, attrs, fn ->
          do_flow_reschedule(state, attrs)
        end)
      end

      def apply(meta, {:flow_schedule_replace, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_schedule_replace, attrs, fn ->
          do_flow_schedule_replace(state, attrs)
        end)
      end

      def apply(meta, {:flow_start_and_claim, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_start_and_claim, attrs, fn ->
          do_flow_start_and_claim(state, attrs)
        end)
      end

      def apply(meta, {:flow_run_steps_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_run_steps_many, attrs, fn ->
          do_flow_run_steps_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_step_continue, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_step_continue, attrs, fn ->
          do_flow_step_continue(state, attrs)
        end)
      end

      def apply(meta, {:flow_step_continue_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_step_continue_many, attrs, fn ->
          do_flow_step_continue_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_transition_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_transition_many, attrs, fn ->
          do_flow_transition_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_retry, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retry, attrs, fn ->
          do_flow_retry(state, attrs)
        end)
      end

      def apply(meta, {:flow_retry_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retry_many, attrs, fn ->
          do_flow_retry_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_fail, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_fail, attrs, fn ->
          do_flow_fail(state, attrs)
        end)
      end

      def apply(meta, {:flow_fail_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_fail_many, attrs, fn ->
          do_flow_fail_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_cancel, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_cancel, attrs, fn ->
          do_flow_cancel(state, attrs)
        end)
      end

      def apply(meta, {:flow_cancel_many, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_cancel_many, attrs, fn ->
          do_flow_cancel_many(state, attrs)
        end)
      end

      def apply(meta, {:flow_retention_cleanup, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_retention_cleanup, attrs, fn ->
          do_flow_retention_cleanup(state, attrs)
        end)
      end

      def apply(meta, {:flow_rewind, _key, attrs}, state) when is_map(attrs) do
        apply_flow_pending_with_time(meta, state, :flow_rewind, attrs, fn ->
          do_flow_rewind(state, attrs)
        end)
      end

      # ---------------------------------------------------------------------------
      # HLC-wrapped commands (spec 2G.6)
      #
      # Raft submit paths stamp commands before they enter the log. During apply,
      # the stamped physical HLC time is installed through `CommandTime`, so command
      # modules compute relative expiries and other time-derived values from the
      # same log-entry timestamp on every replica.
      # ---------------------------------------------------------------------------

      def apply(meta, {:ferricstore_apply_context_barrier, expected_encoded}, state) do
        with_apply_time(meta, fn ->
          if Map.get(state, :apply_context_encoded) == expected_encoded do
            bump_applied(meta, state, {:ok, expected_encoded})
          else
            bump_applied(meta, state, {:error, "ERR replicated apply context mismatch"})
          end
        end)
      end

      def apply(meta, {:ferricstore_apply_context, encoded, inner_command}, state)
          when is_tuple(inner_command) do
        if Map.get(state, :apply_context_encoded) == encoded do
          __MODULE__.apply(meta, inner_command, state)
        else
          case Ferricstore.Raft.ApplyContext.decode(encoded) do
            {:ok, context} ->
              state =
                state
                |> Map.put(:apply_context, context)
                |> Map.put(:apply_context_encoded, encoded)

              __MODULE__.apply(meta, inner_command, state)

            {:error, :invalid_apply_context} ->
              with_apply_time(meta, fn ->
                bump_applied(meta, state, {:error, "ERR invalid replicated apply context"})
              end)
          end
        end
      end

      def apply(meta, {:ferricstore_apply_context, _encoded, _invalid_inner}, state) do
        with_apply_time(meta, fn ->
          bump_applied(meta, state, {:error, "ERR invalid replicated apply context command"})
        end)
      end

      def apply(
            meta,
            {:server_catalog_mutate, namespace, subject, expected_encoded, expected_revision,
             value, max_live_entries},
            state
          ) do
        with_apply_time(meta, fn ->
          result =
            apply_server_catalog_mutation(
              meta,
              state,
              namespace,
              subject,
              expected_encoded,
              expected_revision,
              value,
              max_live_entries
            )

          bump_applied(meta, state, result)
        end)
      end

      def apply(
            meta,
            {:server_catalog_replace, namespace, expected_revision, mutations,
             expected_live_count, max_live_entries},
            state
          ) do
        with_apply_time(meta, fn ->
          result =
            apply_server_catalog_replacement(
              meta,
              state,
              namespace,
              expected_revision,
              mutations,
              expected_live_count,
              max_live_entries
            )

          bump_applied(meta, state, result)
        end)
      end

      defp apply_server_catalog_mutation(
             meta,
             %{shard_index: 0} = state,
             namespace,
             subject,
             expected_encoded,
             expected_revision,
             value,
             max_live_entries
           )
           when is_binary(namespace) and is_binary(subject) and
                  (is_nil(expected_encoded) or is_binary(expected_encoded)) and
                  (is_nil(expected_revision) or is_binary(expected_revision)) and
                  (is_binary(value) or value == :deleted) and
                  is_integer(max_live_entries) and max_live_entries >= 0 do
        with :ok <- validate_server_catalog_expected(expected_encoded),
             :ok <- validate_server_catalog_revision(expected_revision),
             {:ok, key, revision_key, count_key, encoded, revision} <-
               encode_server_catalog_mutation(meta, state, namespace, subject, value) do
          with_pending_writes(state, fn ->
            apply_server_catalog_cas(
              state,
              key,
              revision_key,
              count_key,
              expected_encoded,
              expected_revision,
              encoded,
              revision,
              value,
              max_live_entries
            )
          end)
        end
      end

      defp apply_server_catalog_mutation(
             _meta,
             _state,
             _namespace,
             _subject,
             _expected_encoded,
             _expected_revision,
             _value,
             _max_live_entries
           ),
           do: {:error, :invalid_server_catalog_mutation}

      defp apply_server_catalog_replacement(
             meta,
             %{shard_index: 0} = state,
             namespace,
             expected_revision,
             mutations,
             expected_live_count,
             max_live_entries
           )
           when is_binary(namespace) and
                  (is_nil(expected_revision) or is_binary(expected_revision)) and
                  is_list(mutations) and is_integer(expected_live_count) and
                  expected_live_count >= 0 and is_integer(max_live_entries) and
                  max_live_entries >= 0 do
        with :ok <- validate_server_catalog_revision(expected_revision) do
          with_pending_writes(state, fn ->
            apply_server_catalog_replacement_cas(
              meta,
              state,
              namespace,
              expected_revision,
              mutations,
              expected_live_count,
              max_live_entries
            )
          end)
        end
      end

      defp apply_server_catalog_replacement(
             _meta,
             _state,
             _namespace,
             _expected_revision,
             _mutations,
             _expected_live_count,
             _max_live_entries
           ),
           do: {:error, :invalid_server_catalog_replacement}

      defp apply_server_catalog_replacement_cas(
             meta,
             state,
             namespace,
             expected_revision,
             mutations,
             expected_live_count,
             max_live_entries
           ) do
        revision_key = Ferricstore.ServerCatalog.revision_key(namespace)
        count_key = Ferricstore.ServerCatalog.live_count_key(namespace)
        current_revision = do_get(state, revision_key)

        if current_revision != expected_revision do
          {:error, :stale_server_catalog_revision}
        else
          version = server_catalog_version(meta, state)
          revision = Ferricstore.ServerCatalog.encode_revision(version)

          with {:ok, current_count} <-
                 decode_server_catalog_live_count(do_get(state, count_key), current_revision),
               {:ok, prepared, next_count} <-
                 prepare_server_catalog_replacements(
                   state,
                   namespace,
                   mutations,
                   current_count,
                   version
                 ),
               :ok <-
                 validate_server_catalog_replacement_count(
                   next_count,
                   expected_live_count,
                   max_live_entries
                 ),
               :ok <- write_server_catalog_replacements(state, prepared),
               :ok <- raw_put(state, revision_key, revision, 0),
               :ok <-
                 raw_put(
                   state,
                   count_key,
                   Ferricstore.ServerCatalog.encode_live_count(next_count),
                   0
                 ) do
            {:ok, revision}
          end
        end
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_replacement}
      end

      defp prepare_server_catalog_replacements(
             state,
             namespace,
             mutations,
             current_count,
             version
           ) do
        mutations
        |> Enum.reduce_while({:ok, [], current_count, MapSet.new()}, fn
          {subject, value}, {:ok, prepared, count, seen}
          when is_binary(subject) and (is_binary(value) or value == :deleted) ->
            if MapSet.member?(seen, subject) do
              {:halt, {:error, :invalid_server_catalog_replacement}}
            else
              key = Ferricstore.ServerCatalog.entry_key(namespace, subject)

              with {:ok, current_live?} <- server_catalog_entry_live?(do_get(state, key)) do
                next_live? = is_binary(value)
                next_count = count + live_count_delta(current_live?, next_live?)

                if next_count < 0 do
                  {:halt, {:error, :invalid_server_catalog_state}}
                else
                  encoded =
                    if next_live?,
                      do: Ferricstore.ServerCatalog.encode_entry(version, value),
                      else: nil

                  {:cont,
                   {:ok, [{key, encoded, value} | prepared], next_count,
                    MapSet.put(seen, subject)}}
                end
              else
                {:error, _reason} = error -> {:halt, error}
              end
            end

          _invalid, _acc ->
            {:halt, {:error, :invalid_server_catalog_replacement}}
        end)
        |> case do
          {:ok, prepared, next_count, _seen} ->
            {:ok, Enum.reverse(prepared), next_count}

          {:error, _reason} = error ->
            error
        end
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_replacement}
      end

      defp validate_server_catalog_replacement_count(
             next_count,
             expected_live_count,
             max_live_entries
           ) do
        cond do
          next_count != expected_live_count ->
            {:error, :invalid_server_catalog_replacement}

          next_count > max_live_entries ->
            {:error, {:server_catalog_limit_reached, max_live_entries}}

          true ->
            :ok
        end
      end

      defp write_server_catalog_replacements(state, prepared) do
        Enum.reduce_while(prepared, :ok, fn {key, encoded, value}, :ok ->
          case write_server_catalog_entry(state, key, encoded, value) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp apply_server_catalog_cas(
             state,
             key,
             revision_key,
             count_key,
             expected_encoded,
             expected_revision,
             encoded,
             revision,
             value,
             max_live_entries
           ) do
        current_revision = do_get(state, revision_key)
        current_encoded = do_get(state, key)

        cond do
          current_revision != expected_revision ->
            {:error, :stale_server_catalog_revision}

          current_encoded != expected_encoded ->
            {:error, :stale_server_catalog_entry}

          true ->
            with {:ok, current_count} <-
                   decode_server_catalog_live_count(do_get(state, count_key), current_revision),
                 {:ok, current_live?} <- server_catalog_entry_live?(current_encoded),
                 {:ok, next_count} <-
                   next_server_catalog_live_count(
                     current_count,
                     current_live?,
                     is_binary(value),
                     max_live_entries
                   ),
                 :ok <- write_server_catalog_entry(state, key, encoded, value),
                 :ok <- raw_put(state, revision_key, revision, 0),
                 :ok <-
                   raw_put(
                     state,
                     count_key,
                     Ferricstore.ServerCatalog.encode_live_count(next_count),
                     0
                   ) do
              {:ok, encoded}
            end
        end
      end

      defp decode_server_catalog_live_count(nil, nil), do: {:ok, 0}

      defp decode_server_catalog_live_count(encoded, _revision) when is_binary(encoded) do
        case Ferricstore.ServerCatalog.decode_live_count(encoded) do
          {:ok, count} -> {:ok, count}
          {:error, _invalid} -> {:error, :invalid_server_catalog_state}
        end
      end

      defp decode_server_catalog_live_count(_encoded, _revision),
        do: {:error, :invalid_server_catalog_state}

      defp server_catalog_entry_live?(nil), do: {:ok, false}

      defp server_catalog_entry_live?(encoded) when is_binary(encoded) do
        case Ferricstore.ServerCatalog.decode_entry(encoded) do
          {:ok, %{value: :deleted}} -> {:ok, false}
          {:ok, %{value: value}} when is_binary(value) -> {:ok, true}
          _invalid -> {:error, :invalid_server_catalog_state}
        end
      end

      defp next_server_catalog_live_count(count, current_live?, next_live?, max_live_entries) do
        next_count = count + live_count_delta(current_live?, next_live?)

        cond do
          next_count < 0 ->
            {:error, :invalid_server_catalog_state}

          not current_live? and next_live? and next_count > max_live_entries ->
            {:error, {:server_catalog_limit_reached, max_live_entries}}

          true ->
            {:ok, next_count}
        end
      end

      defp live_count_delta(false, true), do: 1
      defp live_count_delta(true, false), do: -1
      defp live_count_delta(_current_live?, _next_live?), do: 0

      defp write_server_catalog_entry(state, key, _encoded, :deleted), do: do_delete(state, key)

      defp write_server_catalog_entry(state, key, encoded, _value),
        do: raw_put(state, key, encoded, 0)

      defp validate_server_catalog_expected(nil), do: :ok

      defp validate_server_catalog_expected(encoded) do
        case Ferricstore.ServerCatalog.decode_entry(encoded) do
          {:ok, _entry} -> :ok
          {:error, :invalid_server_catalog_entry} -> {:error, :invalid_server_catalog_mutation}
        end
      end

      defp validate_server_catalog_revision(nil), do: :ok

      defp validate_server_catalog_revision(encoded) do
        case Ferricstore.ServerCatalog.decode_revision(encoded) do
          {:ok, _revision} -> :ok
          {:error, _invalid} -> {:error, :invalid_server_catalog_mutation}
        end
      end

      defp encode_server_catalog_mutation(meta, state, namespace, subject, value) do
        version = server_catalog_version(meta, state)

        {:ok, Ferricstore.ServerCatalog.entry_key(namespace, subject),
         Ferricstore.ServerCatalog.revision_key(namespace),
         Ferricstore.ServerCatalog.live_count_key(namespace),
         Ferricstore.ServerCatalog.encode_entry(version, value),
         Ferricstore.ServerCatalog.encode_revision(version)}
      rescue
        ArgumentError -> {:error, :invalid_server_catalog_mutation}
      end

      defp server_catalog_version(meta, state) do
        case Map.get(meta, :index) do
          index when is_integer(index) and index >= 0 -> index
          _missing -> Map.get(state, :applied_count, 0) + 1
        end
      end

      def apply(meta, {inner_command, %{hlc_ts: {physical_ms, _logical} = remote_ts}}, state)
          when is_tuple(inner_command) and is_integer(physical_ms) do
        merge_hlc(remote_ts)

        __MODULE__.apply(
          Map.put(meta, :system_time, physical_ms),
          inner_command,
          state
        )
      end

      # Catch-all: unknown commands should not crash the ra state machine.
      # Log the unrecognized command and return an error result so the caller
      # gets a meaningful error instead of ra crashing with FunctionClauseError.
      def apply(_meta, unknown_command, state) do
        require Logger
        Logger.error("StateMachine: unrecognized command: #{inspect(unknown_command)}")
        {state, {:error, {:unknown_command, unknown_command}}}
      end
    end
  end
end
