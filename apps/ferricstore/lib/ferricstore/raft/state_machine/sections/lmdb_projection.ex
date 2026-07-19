defmodule Ferricstore.Raft.StateMachine.Sections.LmdbProjection do
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

      @flow_lmdb_max_u64 18_446_744_073_709_551_615
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp queue_pending_lmdb_flow_state_projection(state_key, value, expire_at_ms)
           when is_binary(state_key) and is_binary(value) and is_integer(expire_at_ms) do
        queue_pending_lmdb_mirror_op({:project_flow_state, state_key, value, expire_at_ms})
        :ok
      end

      defp queue_pending_lmdb_flow_state_projection(state_key, _value, _expire_at_ms)
           when is_binary(state_key) do
        queue_pending_lmdb_flow_state_projection_from_source(state_key)
      end

      defp queue_pending_lmdb_flow_state_projection_from_source(state_key)
           when is_binary(state_key) do
        queue_pending_lmdb_mirror_op({:project_flow_state_from_source, state_key})
        :ok
      end

      defp queue_pending_lmdb_projection_outbox(state_key, version)
           when is_binary(state_key) and is_integer(version) do
        pending = Process.get(:sm_pending_lmdb_projection_outbox, [])

        item =
          case Process.get(:sm_pending_lmdb_mirror_shard) do
            shard_index when is_integer(shard_index) and shard_index >= 0 ->
              {:lmdb_shard, shard_index, {state_key, version}}

            _other ->
              {state_key, version}
          end

        Process.put(:sm_pending_lmdb_projection_outbox, [item | pending])
        :ok
      end

      defp queue_pending_lmdb_projection_dirty do
        pending = Process.get(:sm_pending_lmdb_projection_dirty_shards, MapSet.new())

        shard_index =
          case Process.get(:sm_pending_lmdb_mirror_shard) do
            shard_index when is_integer(shard_index) and shard_index >= 0 ->
              shard_index

            _other ->
              Process.get(:sm_pending_lmdb_mirror_default_shard, 0)
          end

        Process.put(:sm_pending_lmdb_projection_dirty_shards, MapSet.put(pending, shard_index))
        :ok
      end

      defp maybe_queue_terminal_lmdb_index_delete(state, record) do
        with_lmdb_mirror_shard(state, fn ->
          if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
            partition_key = Map.get(record, :partition_key)
            state_index_key = FlowKeys.state_index_key(record.type, record.state, partition_key)
            updated_at_ms = Map.get(record, :updated_at_ms, 0)

            terminal_key =
              Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, record.id, updated_at_ms)

            terminal_key
            |> queue_pending_lmdb_mirror_terminal_delete(
              FlowKeys.state_key(record.id, Map.get(record, :partition_key)),
              Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
            )

            maybe_queue_terminal_lmdb_expire_delete(record, terminal_key)
            maybe_queue_terminal_lmdb_history_expire_delete(record)
          end
        end)

        :ok
      end

      defp maybe_queue_terminal_lmdb_expire_delete(record, terminal_key) do
        case Map.get(record, :terminal_retention_until_ms) do
          expire_at_ms
          when is_integer(expire_at_ms) and expire_at_ms > 0 and
                 expire_at_ms <= @flow_lmdb_max_u64 ->
            expire_at_ms
            |> Ferricstore.Flow.LMDB.terminal_expire_key(terminal_key)
            |> queue_pending_lmdb_mirror_delete()

          _missing_or_invalid ->
            # :terminal_delete derives the exact marker from the persisted index value.
            :ok
        end
      end

      defp maybe_queue_terminal_lmdb_history_expire_delete(record) do
        partition_key = Map.get(record, :partition_key)
        history_key = FlowKeys.history_key(Map.fetch!(record, :id), partition_key)

        case Map.get(record, :terminal_retention_until_ms) do
          expire_at_ms
          when is_integer(expire_at_ms) and expire_at_ms > 0 and
                 expire_at_ms <= @flow_lmdb_max_u64 ->
            expire_at_ms
            |> Ferricstore.Flow.LMDB.history_flow_expire_key(history_key)
            |> queue_pending_lmdb_mirror_delete()

          _missing_or_invalid ->
            :ok
        end
      end

      defp queue_lmdb_metadata_index_deletes(state, record) do
        with_lmdb_mirror_shard(state, fn ->
          metadata_query_keys =
            record
            |> flow_metadata_index_entries()
            |> Enum.map(fn {index_key, id, score} ->
              Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
            end)

          attribute_query_keys =
            record
            |> Ferricstore.Flow.Attributes.index_entries()
            |> Enum.map(fn {index_key, id, score} ->
              Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
            end)

          state_meta_query_keys =
            record
            |> Ferricstore.Flow.StateMeta.index_entries()
            |> Enum.map(fn {index_key, id, score} ->
              Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
            end)

          (metadata_query_keys ++ attribute_query_keys ++ state_meta_query_keys)
          |> Enum.each(fn query_key ->
            queue_pending_lmdb_mirror_query_delete(query_key)
          end)
        end)

        :ok
      end

      defp queue_lmdb_history_indexes_project_from_index(state, record, history_key) do
        queue_pending_lmdb_mirror_op(
          {:history_project_from_index, Map.get(state, :flow_index_name),
           Map.get(state, :flow_lookup_name), Map.fetch!(record, :id),
           Map.get(record, :partition_key), history_key, flow_record_expire_at(record)}
        )

        :ok
      end

      defp queue_lmdb_history_index_delete(_record, history_key, event_id, event_ms) do
        history_key
        |> Ferricstore.Flow.LMDB.history_index_key(event_id, event_ms)
        |> queue_pending_lmdb_mirror_history_delete()
      end

      defp queue_pending_lmdb_mirror_history_delete(history_index_key) do
        queue_pending_lmdb_mirror_op({:history_delete, history_index_key})
        :ok
      end

      defp queue_pending_lmdb_mirror_query_delete(query_key) do
        queue_pending_lmdb_mirror_op({:query_delete, query_key})
        :ok
      end

      defp queue_pending_lmdb_mirror_delete(key) do
        queue_pending_lmdb_mirror_op({:delete, key})
        :ok
      end

      defp queue_pending_lmdb_mirror_terminal_delete(terminal_key, state_key, count_key) do
        op = {:terminal_delete, terminal_key, state_key, count_key}
        queue_pending_lmdb_mirror_op(op)
        :ok
      end

      defp queue_pending_lmdb_mirror_after_flush(action) do
        pending = Process.get(:sm_pending_lmdb_mirror_after_flush, [])

        item =
          case Process.get(:sm_pending_lmdb_mirror_shard) do
            shard_index when is_integer(shard_index) and shard_index >= 0 ->
              {:lmdb_shard, shard_index, action}

            _other ->
              action
          end

        Process.put(:sm_pending_lmdb_mirror_after_flush, [item | pending])
        :ok
      end

      defp queue_pending_lmdb_mirror_op(op) do
        pending = Process.get(:sm_pending_lmdb_mirror_ops, [])

        item =
          case Process.get(:sm_pending_lmdb_mirror_shard) do
            shard_index when is_integer(shard_index) and shard_index >= 0 ->
              {:lmdb_shard, shard_index, op}

            _other ->
              op
          end

        Process.put(:sm_pending_lmdb_mirror_ops, [item | pending])
      end

      defp with_lmdb_mirror_shard(state, fun) when is_function(fun, 0) do
        shard_index = Map.get(state, :shard_index, 0)

        case Process.get(:sm_pending_lmdb_mirror_default_shard, shard_index) do
          ^shard_index ->
            fun.()

          _other ->
            with_tagged_lmdb_mirror_shard(shard_index, fun)
        end
      end

      defp with_tagged_lmdb_mirror_shard(shard_index, fun) do
        previous = Process.get(:sm_pending_lmdb_mirror_shard, :undefined)
        Process.put(:sm_pending_lmdb_mirror_shard, shard_index)
        Process.put(:sm_pending_lmdb_mirror_tagged, true)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(:sm_pending_lmdb_mirror_shard)
            value -> Process.put(:sm_pending_lmdb_mirror_shard, value)
          end
        end
      end

      defp enqueue_pending_lmdb_mirror(state) do
        dirty_projection_shards =
          case Process.put(:sm_pending_lmdb_projection_dirty_shards, MapSet.new()) do
            %MapSet{} = pending -> MapSet.to_list(pending)
            pending when is_list(pending) -> pending
            _ -> []
          end

        projection_outbox_entries =
          case Process.put(:sm_pending_lmdb_projection_outbox, []) do
            pending when is_list(pending) -> Enum.reverse(pending)
            _ -> []
          end

        after_flush =
          case Process.put(:sm_pending_lmdb_mirror_after_flush, []) do
            pending when is_list(pending) -> Enum.reverse(pending)
            _ -> []
          end

        pending_ops =
          case Process.put(:sm_pending_lmdb_mirror_ops, []) do
            pending when is_list(pending) -> Enum.reverse(pending)
            _ -> []
          end

        with {:ok, {hibernation_ops, hibernation_after_flush}} <-
               pending_flow_hibernation_mirror_items(state, pending_ops),
             ops = pending_ops ++ hibernation_ops,
             after_flush = after_flush ++ hibernation_after_flush,
             :ok <- enqueue_lmdb_projection_dirty_groups(state, dirty_projection_shards),
             :ok <- enqueue_lmdb_projection_outbox_groups(state, projection_outbox_entries) do
          case ops do
            [] ->
              :ok

            [_ | _] ->
              enqueue_lmdb_mirror_groups(state, ops, after_flush)
          end
        end
      end

      defp pending_flow_hibernation_mirror_items(state, pending_ops) do
        pending_projections =
          pending_flow_state_projections(pending_ops, Map.get(state, :shard_index, 0))

        case Process.put(:sm_pending_flow_hibernation_candidates, []) do
          pending when is_list(pending) ->
            pending
            |> coalesce_pending_flow_hibernation_candidates()
            |> Enum.reduce_while({:ok, {[], []}}, fn
              {key, record, state_value}, {:ok, acc} ->
                reduce_flow_hibernation_candidate(
                  state,
                  key,
                  record,
                  state_value,
                  pending_projections,
                  acc
                )

              {key, record}, {:ok, acc} ->
                reduce_flow_hibernation_candidate(
                  state,
                  key,
                  record,
                  nil,
                  pending_projections,
                  acc
                )
            end)
            |> case do
              {:ok, {reversed_ops, reversed_after_flush}} ->
                {:ok, {Enum.reverse(reversed_ops), Enum.reverse(reversed_after_flush)}}

              {:error, _reason} = error ->
                error
            end

          _ ->
            {:ok, {[], []}}
        end
      end

      @doc false
      def __coalesce_pending_flow_hibernation_candidates_for_test__(pending)
          when is_list(pending),
          do: coalesce_pending_flow_hibernation_candidates(pending)

      defp coalesce_pending_flow_hibernation_candidates(pending) do
        pending
        |> Enum.reduce({MapSet.new(), []}, fn
          {key, _record, _state_value} = candidate, {seen, selected} when is_binary(key) ->
            select_latest_hibernation_candidate(key, candidate, seen, selected)

          {key, _record} = candidate, {seen, selected} when is_binary(key) ->
            select_latest_hibernation_candidate(key, candidate, seen, selected)
        end)
        |> elem(1)
      end

      defp select_latest_hibernation_candidate(key, candidate, seen, selected) do
        if MapSet.member?(seen, key) do
          {seen, selected}
        else
          {MapSet.put(seen, key), [candidate | selected]}
        end
      end

      defp reduce_flow_hibernation_candidate(
             state,
             key,
             record,
             state_value,
             pending_projections,
             {ops_acc, after_acc}
           ) do
        case flow_hibernation_candidate_items(
               state,
               key,
               record,
               state_value,
               pending_projections
             ) do
          {:ok, {ops, after_flush}} ->
            {:cont, {:ok, {Enum.reverse(ops, ops_acc), Enum.reverse(after_flush, after_acc)}}}

          :skip ->
            {:cont, {:ok, {ops_acc, after_acc}}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end

      defp flow_hibernation_candidate_items(
             state,
             key,
             record,
             state_value,
             pending_projections
           ) do
        with {:ok, locator} <-
               flow_hibernation_locator_from_hot(state, key, record, state_value),
             {:ok, active_index_reverse_value} <-
               flow_hibernation_active_index_reverse(state, key, record, pending_projections),
             {:ok, ops} <-
               Hibernation.demotion_ops_result(%{
                 locator: locator,
                 record: Map.put(record, :state_key, key),
                 state_value: state_value,
                 active_index_reverse_value: active_index_reverse_value
               }) do
          candidate_record = Map.put(record, :state_key, key)

          action =
            {:hibernate_flow_evict_hot,
             %{
               data_dir: Map.get(state, :data_dir),
               shard_index: Map.get(state, :shard_index),
               ets: state.ets,
               zset_index: Map.get(state, :zset_score_index_name),
               zset_lookup: Map.get(state, :zset_score_lookup_name),
               flow_index: Map.get(state, :flow_index_name),
               flow_lookup: Map.get(state, :flow_lookup_name),
               state_key: key,
               record: flow_hibernation_eviction_record(candidate_record),
               locator: locator
             }}

          {:ok,
           {
             Enum.map(ops, &{:lmdb_shard, state.shard_index, &1}),
             [{:lmdb_shard, state.shard_index, action}]
           }}
        else
          {:error, :not_found} -> :skip
          {:error, reason} -> {:error, {:flow_hibernation_demotion_failed, reason}}
          _invalid_locator -> :skip
        end
      end

      defp flow_hibernation_active_index_reverse(state, key, record, pending_projections) do
        case Map.get(pending_projections, {state.shard_index, key}, :none) do
          {:ok, projected_record} ->
            flow_hibernation_projected_active_reverse(key, projected_record)

          :from_source ->
            flow_hibernation_projected_active_reverse(key, record)

          {:error, _reason} = error ->
            error

          :none ->
            case Ferricstore.Flow.LMDB.get(
                   flow_lmdb_record_path(state),
                   Ferricstore.Flow.LMDB.active_by_state_key_key(key)
                 ) do
              {:ok, reverse_value} when is_binary(reverse_value) -> {:ok, reverse_value}
              :not_found -> {:ok, nil}
              {:error, _reason} = error -> error
              other -> {:error, {:invalid_active_index_reverse_read, other}}
            end
        end
      end

      defp pending_flow_state_projections(pending_ops, default_shard_index) do
        Enum.reduce(pending_ops, %{}, fn
          {:project_flow_state, key, value, _expire_at_ms}, acc when is_binary(key) ->
            Map.put(
              acc,
              {default_shard_index, key},
              decode_pending_flow_projection(value)
            )

          {:project_flow_state_from_source, key}, acc when is_binary(key) ->
            Map.put(acc, {default_shard_index, key}, :from_source)

          {:lmdb_shard, shard_index, {:project_flow_state, key, value, _expire_at_ms}}, acc
          when is_integer(shard_index) and shard_index >= 0 and is_binary(key) ->
            Map.put(acc, {shard_index, key}, decode_pending_flow_projection(value))

          {:lmdb_shard, shard_index, {:project_flow_state_from_source, key}}, acc
          when is_integer(shard_index) and shard_index >= 0 and is_binary(key) ->
            Map.put(acc, {shard_index, key}, :from_source)

          _other, acc ->
            acc
        end)
      end

      defp decode_pending_flow_projection(value) when is_binary(value) do
        case Flow.decode_record(value) do
          record when is_map(record) -> {:ok, record}
          _invalid -> {:error, :invalid_pending_flow_projection}
        end
      rescue
        _error -> {:error, :invalid_pending_flow_projection}
      end

      defp flow_hibernation_projected_active_reverse(key, record) when is_map(record) do
        {_ops, reverse_value} =
          Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(key, record, 0)

        {:ok, reverse_value}
      rescue
        _error -> {:error, :invalid_pending_active_index_projection}
      end

      defp flow_hibernation_locator_from_hot(state, key, record, state_value) do
        version = Map.get(record, :version, 0)
        ra_index = current_ra_index() || version

        case :ets.lookup(state.ets, key) do
          [{^key, current_value, expire_at_ms, _lfu, file_id, offset, value_size}]
          when valid_cold_location(file_id, offset, value_size) or
                 valid_waraft_segment_location(file_id, offset, value_size) ->
            if flow_hibernation_candidate_current?(current_value, state_value, record) do
              Locator.new(
                flow_id: Map.fetch!(record, :id),
                kind: :state,
                version: version,
                raft_index: ra_index,
                file_id: file_id,
                offset: offset,
                value_size: value_size,
                expire_at_ms: expire_at_ms
              )
            else
              :skip
            end

          _ ->
            :skip
        end
      rescue
        ArgumentError -> :skip
        KeyError -> :skip
      end

      @doc false
      def __flow_hibernation_candidate_current_for_test__(current_value, state_value, record),
        do: flow_hibernation_candidate_current?(current_value, state_value, record)

      defp flow_hibernation_candidate_current?(current_value, state_value, _record)
           when is_binary(current_value) and is_binary(state_value),
           do: current_value == state_value

      defp flow_hibernation_candidate_current?(current_value, nil, record)
           when is_binary(current_value) and is_map(record) do
        case Flow.decode_record(current_value) do
          ^record -> true
          _stale_or_invalid -> false
        end
      rescue
        _error -> false
      end

      defp flow_hibernation_candidate_current?(_current_value, _state_value, _record), do: false

      defp flow_hibernation_eviction_record(record) do
        Map.take(record, [
          :id,
          :type,
          :state,
          :partition_key,
          :priority,
          :next_run_at_ms,
          :parent_flow_id,
          :root_flow_id,
          :correlation_id,
          :lease_owner
        ])
      end

      defp enqueue_lmdb_projection_outbox_groups(_state, []), do: :ok

      defp enqueue_lmdb_projection_outbox_groups(state, entries) do
        entries
        |> group_lmdb_mirror_items(state.shard_index)
        |> Enum.reduce_while(:ok, fn {shard_index, shard_entries}, :ok ->
          case Ferricstore.Flow.LMDBWriter.enqueue_projection_outbox(
                 state.instance_name,
                 shard_index,
                 shard_entries
               ) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:lmdb_shard, shard_index, reason}}}
            other -> {:halt, {:error, {:lmdb_shard, shard_index, other}}}
          end
        end)
      end

      defp enqueue_lmdb_projection_dirty_groups(_state, []), do: :ok

      defp enqueue_lmdb_projection_dirty_groups(state, dirty_shards) do
        dirty_shards
        |> Enum.map(fn
          shard_index when is_integer(shard_index) and shard_index >= 0 -> shard_index
          _other -> state.shard_index
        end)
        |> Enum.uniq()
        |> Enum.reduce_while(:ok, fn shard_index, :ok ->
          case Ferricstore.Flow.LMDBWriter.mark_projection_dirty(state.instance_name, shard_index) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:lmdb_shard, shard_index, reason}}}
            other -> {:halt, {:error, {:lmdb_shard, shard_index, other}}}
          end
        end)
      end

      defp enqueue_lmdb_mirror_groups(state, ops, after_flush) do
        if Process.get(:sm_pending_lmdb_mirror_tagged, false) or lmdb_mirror_tagged_items?(ops) or
             lmdb_mirror_tagged_items?(after_flush) do
          enqueue_tagged_lmdb_mirror_groups(state, ops, after_flush)
        else
          enqueue_lmdb_mirror_group(state, state.shard_index, ops, after_flush)
        end
      end

      defp lmdb_mirror_tagged_items?(items) do
        Enum.any?(items, fn
          {:lmdb_shard, shard_index, _item} when is_integer(shard_index) and shard_index >= 0 ->
            true

          _ ->
            false
        end)
      end

      defp enqueue_tagged_lmdb_mirror_groups(state, ops, after_flush) do
        op_groups = group_lmdb_mirror_items(ops, state.shard_index)
        after_flush_groups = group_lmdb_mirror_items(after_flush, state.shard_index)

        (Map.keys(op_groups) ++ Map.keys(after_flush_groups))
        |> Enum.uniq()
        |> Enum.reduce_while(:ok, fn shard_index, :ok ->
          shard_ops = Map.get(op_groups, shard_index, [])
          shard_after_flush = Map.get(after_flush_groups, shard_index, [])

          case enqueue_lmdb_mirror_group(state, shard_index, shard_ops, shard_after_flush) do
            :ok -> {:cont, :ok}
            {:error, reason} -> {:halt, {:error, {:lmdb_shard, shard_index, reason}}}
            other -> {:halt, {:error, {:lmdb_shard, shard_index, other}}}
          end
        end)
      end

      defp enqueue_lmdb_mirror_group(state, shard_index, shard_ops, shard_after_flush) do
        case Ferricstore.Flow.LMDBWriter.enqueue_async(
               state.instance_name,
               shard_index,
               shard_ops,
               shard_after_flush
             ) do
          :ok -> :ok
          {:error, _reason} = error -> error
          other -> {:error, other}
        end
      end

      defp group_lmdb_mirror_items(items, default_shard) do
        items
        |> Enum.reduce(%{}, fn
          {:lmdb_shard, shard_index, item}, acc
          when is_integer(shard_index) and shard_index >= 0 ->
            Map.update(acc, shard_index, [item], &[item | &1])

          item, acc ->
            Map.update(acc, default_shard, [item], &[item | &1])
        end)
        |> Map.new(fn {shard_index, shard_items} -> {shard_index, Enum.reverse(shard_items)} end)
      end

      defp observe_pending_lmdb_mirror_enqueue(_state, :ok), do: :ok

      defp observe_pending_lmdb_mirror_enqueue(
             state,
             {:error, {:lmdb_shard, shard_index, reason}}
           )
           when is_integer(shard_index) and shard_index >= 0 do
        mark_flow_lmdb_mirror_degraded(state, shard_index, reason)
        :ok
      end

      defp observe_pending_lmdb_mirror_enqueue(state, {:error, reason}) do
        mark_flow_lmdb_mirror_degraded(state, reason)
        :ok
      end

      defp observe_pending_lmdb_mirror_enqueue(_state, _other), do: :ok

      defp mark_flow_lmdb_mirror_degraded(state, reason) do
        mark_flow_lmdb_mirror_degraded(state, Map.get(state, :shard_index, 0), reason)
      end

      defp mark_flow_lmdb_mirror_degraded(state, shard_index, reason) do
        ctx = Map.get(state, :instance_ctx)
        flag_idx = shard_index + 1

        flow_lmdb_safe_atomic_update(
          Map.get(ctx || %{}, :flow_lmdb_mirror_enqueue_failures),
          flag_idx,
          fn ref, idx -> :atomics.add(ref, idx, 1) end
        )

        flow_lmdb_safe_atomic_update(
          Map.get(ctx || %{}, :flow_lmdb_mirror_degraded),
          flag_idx,
          fn ref, idx -> :atomics.put(ref, idx, 1) end
        )

        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_mirror, :degraded],
          %{count: 1},
          %{
            instance_name: Map.get(state, :instance_name, :default),
            shard_index: shard_index,
            reason: reason
          }
        )
      end

      defp flow_lmdb_safe_atomic_update(ref, flag_idx, fun)
           when is_reference(ref) and is_integer(flag_idx) and flag_idx > 0 and
                  is_function(fun, 2) do
        if flag_idx <= :atomics.info(ref).size do
          fun.(ref, flag_idx)
        else
          :ok
        end
      rescue
        _ -> :ok
      end

      defp flow_lmdb_safe_atomic_update(_ref, _flag_idx, _fun), do: :ok

      defp flush_pending_lmdb(_state), do: :ok

      defp rollback_pending_lmdb(_state), do: :ok

      defp rollback_pending_writes(state) do
        rollback_pending_lmdb(state)
        rollback_pending_prob_creates(state)

        Process.get(:sm_pending_originals, %{})
        |> Enum.each(fn
          {key, {:entry, entry}} ->
            track_keydir_binary_restore(state, key, entry)
            safe_ets_insert(state.ets, entry)

          {key, :missing} ->
            track_keydir_binary_restore(state, key, nil)
            safe_ets_delete(state.ets, key)
        end)

        rollback_pending_flow_indexes(state)
      end

      defp rollback_pending_prob_creates(state) do
        :sm_pending_prob_paths
        |> Process.get(%{by_final: %{}})
        |> Map.get(:by_final, %{})
        |> Enum.each(fn {_final_path, create_path} ->
          cleanup_created_prob_file(state, create_path)
          cleanup_created_prob_file(state, prob_mutation_receipt_path(create_path))
        end)
      end

      defp track_keydir_binary_restore(state, key, original_entry) do
        ref = keydir_binary_ref(state)
        current = safe_ets_lookup(state.ets, key)
        original = if(original_entry, do: [original_entry], else: [])

        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          state.shard_index,
          ExpiryTracker.entry_expire_at(current),
          ExpiryTracker.entry_expire_at(original)
        )

        if ref do
          current_bytes = keydir_entry_binary_bytes(key, current)
          original_bytes = keydir_entry_binary_bytes(key, original)
          delta = original_bytes - current_bytes
          if delta != 0, do: :atomics.add(ref, state.shard_index + 1, delta)
        end
      end

      defp safe_ets_lookup(table, key) do
        :ets.lookup(table, key)
      rescue
        ArgumentError -> []
      end

      defp safe_ets_select(table, match_spec) do
        :ets.select(table, match_spec)
      rescue
        ArgumentError -> []
      end

      defp safe_ets_select_page(_table, _match_spec, limit) when limit <= 0, do: {[], false}

      defp safe_ets_select_page(table, match_spec, limit) do
        case :ets.select(table, match_spec, limit) do
          :"$end_of_table" -> {[], true}
          {matches, :"$end_of_table"} -> {matches, true}
          {matches, _continuation} -> {matches, false}
        end
      rescue
        ArgumentError -> {[], false}
      end

      defp safe_ets_insert(table, entry) do
        :ets.insert(table, entry)
        :ok
      rescue
        ArgumentError -> :ok
      end

      defp safe_ets_delete(table, key) do
        :ets.delete(table, key)
        :ok
      rescue
        ArgumentError -> :ok
      end

      defp keydir_entry_binary_bytes(key, [{entry_key, value, _, _, _, _, _}])
           when entry_key == key and is_binary(value),
           do: binary_byte_size(key) + binary_byte_size(value)

      defp keydir_entry_binary_bytes(_key, _entry), do: 0

      # Returns {path, file_id} for the active Bitcask log file. Prefer the live
      # ActiveFile registry so state-machine writes follow shard rotations even
      # when the init-time path still exists. Falls back to ra state for isolated
      # tests/recovery where the registry has not been published yet.
      defp resolve_active_file(state) do
        case live_active_file(state) do
          {file_path, file_id} ->
            {file_path, file_id}

          :stale ->
            if Ferricstore.FS.exists?(state.active_file_path) do
              {state.active_file_path, state.active_file_id}
            else
              :stale
            end
        end
      end

      defp live_active_file(state) do
        try do
          case active_file_instance_ctx(state) do
            {:ok, ctx} ->
              {file_id, file_path, shard_data_path} =
                Ferricstore.Store.ActiveFile.get(ctx, state.shard_index)

              if active_file_shard_path?(state, shard_data_path) and
                   Ferricstore.FS.exists?(file_path) do
                {file_path, file_id}
              else
                :stale
              end

            :stale ->
              :stale
          end
        rescue
          _ -> :stale
        end
      end

      defp active_file_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx}),
        do: {:ok, ctx}

      defp active_file_instance_ctx(%{instance_ctx: ctx}) when is_map(ctx), do: {:ok, ctx}

      defp active_file_instance_ctx(%{instance_name: :default} = state),
        do: {:ok, instance_ctx_for_state(state)}

      defp active_file_instance_ctx(state) do
        case instance_ctx_for_state(state) do
          %FerricStore.Instance{} = ctx -> {:ok, ctx}
          _unresolved -> :stale
        end
      end

      defp active_file_shard_path?(%{shard_data_path: shard_data_path}, shard_data_path),
        do: true

      defp active_file_shard_path?(
             %{shard_data_path_expanded: expected_path},
             shard_data_path
           )
           when is_binary(expected_path) and is_binary(shard_data_path) do
        Path.expand(shard_data_path) == expected_path
      end

      defp active_file_shard_path?(_state, _shard_data_path), do: false

      defp do_delete(state, key) do
        # If the key has a pending background write, flush the BitcaskWriter
        # first to ensure the PUT record lands on disk BEFORE the tombstone.
        # Without this, a background PUT arriving after the tombstone would
        # resurrect the key on recovery (Bitcask last-record-wins semantics).
        with :ok <- flush_pending_for_key(state, key) do
          prob_type =
            if CompoundKey.internal_key?(key), do: nil, else: prob_type_marker(state, key)

          prob_path = prob_file_path_for_delete(state, key)

          case resolve_active_file(state) do
            :stale ->
              set_disk_pressure(state)
              {:error, :active_file_unavailable}

            {_file_path, _file_id} ->
              with :ok <- maybe_delete_prob_type_marker(state, key, prob_type) do
                if cross_shard_pending_active?() do
                  ctx = cross_shard_pending_ctx(state)
                  record_cross_shard_pending_original(ctx, key)

                  track_keydir_binary_delta_for_keydir(
                    state,
                    ctx.keydir,
                    ctx.index,
                    key,
                    nil,
                    0
                  )

                  :ets.delete(ctx.keydir, key)
                  queue_cross_shard_pending_delete(ctx, key)
                  maybe_queue_lmdb_state_delete_after_publish(state, key)
                else
                  record_pending_original(state, key)
                  queue_pending_delete(key, prob_path)

                  unless standalone_staged_apply?() do
                    track_keydir_binary_remove(state, key)
                    :ets.delete(state.ets, key)
                    maybe_queue_lmdb_state_delete(state, key)
                  end
                end

                queue_compound_promotion_removal_after_flush(key)
                :ok
              end
          end
        end
      end

      defp maybe_delete_prob_type_marker(state, key, type)
           when type in [:bloom, :cms, :cuckoo, :topk] do
        do_delete(state, CompoundKey.type_key(key))
      end

      defp maybe_delete_prob_type_marker(_state, _key, _type), do: :ok

      defp queue_compound_promotion_removal_after_flush(<<"PM:", _::binary>> = marker_key) do
        redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(marker_key)

        case Process.get(:sm_pending_compound_promotion_removals) do
          %MapSet{} = pending ->
            Process.put(
              :sm_pending_compound_promotion_removals,
              MapSet.put(pending, redis_key)
            )

          _not_in_apply ->
            :ok
        end

        :ok
      end

      defp queue_compound_promotion_removal_after_flush(_key), do: :ok

      defp maybe_queue_lmdb_state_delete(state, key) when is_binary(key) do
        cond do
          flow_state_key?(key) ->
            :ets.insert(state.ets, {key, nil, 0, :flow_state_deleted, :deleted, 0, 0})
            queue_lmdb_state_delete_projection(state, key)

          flow_owned_value_ref?(key) or FlowKeys.policy_key?(key) or
              FlowKeys.policy_indexed_attribute_catalog_key?(key) ->
            with_lmdb_mirror_shard(state, fn ->
              queue_pending_lmdb_mirror_delete(key)
            end)

          true ->
            :ok
        end

        :ok
      end

      defp maybe_queue_lmdb_state_delete(_state, _key), do: :ok

      defp maybe_queue_lmdb_state_delete_after_publish(state, key) when is_binary(key) do
        cond do
          flow_state_key?(key) ->
            queue_lmdb_state_delete_projection(state, key)

          flow_owned_value_ref?(key) or FlowKeys.policy_key?(key) or
              FlowKeys.policy_indexed_attribute_catalog_key?(key) ->
            with_lmdb_mirror_shard(state, fn ->
              queue_pending_lmdb_mirror_delete(key)
            end)

          true ->
            :ok
        end

        :ok
      end

      defp maybe_queue_lmdb_state_delete_after_publish(_state, _key), do: :ok

      defp queue_lmdb_state_delete_projection(state, key) do
        with_lmdb_mirror_shard(state, fn ->
          queue_pending_lmdb_mirror_delete(key)
          queue_pending_lmdb_mirror_after_flush({:delete_flow_tombstone, state.ets, key})
        end)
      end

      defp maybe_queue_lmdb_policy_put(key, value, expire_at_ms) do
        if FlowKeys.policy_key?(key) or FlowKeys.policy_indexed_attribute_catalog_key?(key) do
          queue_pending_lmdb_mirror_put(key, value, expire_at_ms)
        end

        :ok
      end

      defp maybe_queue_lmdb_flow_blob_value_put(_state, key, encoded_ref, _expire_at_ms)
           when is_binary(key) and is_binary(encoded_ref) do
        # Prepared Flow blob values are already durable through the Bitcask/blob
        # row. The async history projector publishes cold LMDB locators later, so
        # enqueueing a direct LMDB value op here would put cold projection back on
        # the apply hot path.
        :ok
      end

      defp maybe_queue_lmdb_flow_blob_value_put(_state, _key, _encoded_ref, _expire_at_ms),
        do: :ok

      defp flow_state_key?(key) when is_binary(key) do
        FlowKeys.state_key?(key)
      end

      # Flushes the BitcaskWriter if the key has a pending background write.
      # Called before tombstone writes and delete_prefix operations to ensure
      # correct disk ordering (PUT before TOMBSTONE).
      defp flush_pending_for_key(state, key) do
        case :ets.lookup(state.ets, key) do
          [{^key, _v, _e, _lfu, :pending, _off, _vs}] ->
            try do
              case BitcaskWriter.flush(state.instance_ctx, state.shard_index) do
                :ok ->
                  :ok

                {:error, reason} ->
                  Logger.warning(
                    "Shard #{state.shard_index}: pending write flush failed before tombstone for #{inspect(key)}: #{inspect(reason)}"
                  )

                  {:error, {:bitcask_writer_flush_failed, reason}}
              end
            rescue
              error ->
                {:error, {:bitcask_writer_flush_failed, {:exception, error}}}
            catch
              :exit, reason ->
                {:error, {:bitcask_writer_flush_failed, {:exit, reason}}}
            end

          _ ->
            :ok
        end
      end

      # Returns nil for values exceeding the hot cache max value size threshold,
      # or the value itself if it fits. Prevents large values from being stored
      # in ETS, avoiding expensive binary copies on every :ets.lookup.
      @compile {:inline, value_for_ets: 2}
      defp value_for_ets(nil, _threshold), do: nil
      defp value_for_ets(value, _threshold) when is_integer(value), do: Integer.to_string(value)
      defp value_for_ets(value, _threshold) when is_float(value), do: Float.to_string(value)

      defp value_for_ets(value, threshold) when is_binary(value) do
        if byte_size(value) > threshold do
          nil
        else
          value
        end
      end

      # Catch-all for non-primitive values (e.g. tuples like {:topk_path, path}
      # stored via Ops.put). Serialize to binary for ETS storage.
      defp value_for_ets(value, _threshold), do: :erlang.term_to_binary(value)

      @compile {:inline, hot_cache_threshold: 1}
      defp hot_cache_threshold(%{instance_ctx: ctx}) when ctx != nil,
        do: Map.get(ctx, :hot_cache_max_value_size, 65_536)

      defp hot_cache_threshold(_state), do: 65_536

      defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
      defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
      defp to_disk_binary(v) when is_binary(v), do: v
      defp to_disk_binary(v), do: :erlang.term_to_binary(v)

      # ---------------------------------------------------------------------------
      # Private: string mutation operations
      # ---------------------------------------------------------------------------

      # Atomic INCR/DECR/INCRBY/DECRBY: reads current value, parses as integer,
      # adds delta, writes back. Preserves existing expire_at_ms.
      # Returns {:ok, new_integer} or {:error, reason}.
      # Enforces int64 bounds [-2^63, 2^63-1] to match Redis behavior.
      @int64_max 9_223_372_036_854_775_807
      @int64_min -9_223_372_036_854_775_808

      defp do_incr(state, key, delta) do
        with :ok <- ensure_string_key(state, key) do
          case do_get_meta(state, key) do
            nil ->
              if delta > @int64_max or delta < @int64_min do
                {:error, "ERR increment or decrement would overflow"}
              else
                do_put(state, key, delta, 0)
                {:ok, delta}
              end

            {value, expire_at_ms} ->
              case coerce_integer(value) do
                {:ok, int_val} ->
                  new_val = int_val + delta

                  if new_val > @int64_max or new_val < @int64_min do
                    {:error, "ERR increment or decrement would overflow"}
                  else
                    do_put(state, key, new_val, expire_at_ms)
                    {:ok, new_val}
                  end

                :error ->
                  {:error, "ERR value is not an integer or out of range"}
              end
          end
        end
      end

      # Parses a binary as an integer. Returns `{:ok, integer}` or `:error`.
      defp parse_integer(str) when is_binary(str) do
        case Integer.parse(str) do
          {val, ""} -> {:ok, val}
          _ -> :error
        end
      end

      # Coerces a value (integer, float, or binary) to integer.
      defp coerce_integer(v) when is_integer(v), do: {:ok, v}
      defp coerce_integer(v) when is_float(v), do: :error
      defp coerce_integer(v) when is_binary(v), do: parse_integer(v)

      # Coerces a value (integer, float, or binary) to float.
      defp coerce_float(v) when is_float(v), do: {:ok, v}
      defp coerce_float(v) when is_integer(v), do: ValueCodec.number_to_float(v)
      defp coerce_float(v) when is_binary(v), do: parse_float(v)

      # Atomic INCRBYFLOAT: reads current value, parses as float, adds delta,
      # formats result, writes back. Preserves existing expire_at_ms.
      defp do_incr_float(state, key, delta) do
        with :ok <- ensure_string_key(state, key) do
          case do_get_meta(state, key) do
            nil ->
              do_increment_float_value(state, key, 0.0, delta, 0)

            {value, expire_at_ms} ->
              case coerce_float(value) do
                {:ok, float_val} ->
                  do_increment_float_value(state, key, float_val, delta, expire_at_ms)

                :error ->
                  {:error, "ERR value is not a valid float"}
              end
          end
        end
      end

      defp do_increment_float_value(state, key, current, delta, expire_at_ms) do
        case ValueCodec.checked_float_add(current, delta) do
          {:ok, new_val} ->
            case do_put(state, key, new_val, expire_at_ms) do
              :ok -> {:ok, new_val}
              {:error, _reason} = error -> error
            end

          :overflow ->
            {:error, "ERR increment would produce NaN or Infinity"}

          :error ->
            {:error, "ERR value is not a valid float"}
        end
      end

      # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
      defp parse_float(str), do: ValueCodec.parse_float(str)

      # Atomic APPEND: reads current value (or ""), concatenates suffix, writes
      # back. Preserves the existing expire_at_ms on the key.
      defp do_append(state, key, suffix) do
        with :ok <- ensure_string_key(state, key) do
          {old_val, expire_at_ms} =
            case do_get_meta(state, key) do
              nil -> {"", 0}
              {v, exp} -> {to_disk_binary(v), exp}
            end

          new_size =
            Ferricstore.Raft.ApplyLimits.append_size(byte_size(old_val), byte_size(suffix))

          with :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, new_size),
               new_val = old_val <> suffix,
               :ok <- do_put(state, key, new_val, expire_at_ms) do
            {:ok, new_size}
          end
        end
      end

      # Atomic GETSET: reads old value, writes new value with no expiry, returns
      # old value directly (not wrapped in {:ok, ...}).
    end
  end
end
