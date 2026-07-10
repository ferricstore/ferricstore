defmodule Ferricstore.Raft.StateMachine.Sections.CrossShardDispatch do
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

      defp with_current_ra_index(%{index: ra_index}, fun) when is_integer(ra_index) do
        previous = Process.get(:sm_current_ra_index, :undefined)
        Process.put(:sm_current_ra_index, ra_index)

        try do
          fun.()
        after
          case previous do
            :undefined -> Process.delete(:sm_current_ra_index)
            value -> Process.put(:sm_current_ra_index, value)
          end
        end
      end

      defp with_current_ra_index(_meta, fun), do: fun.()

      defp current_ra_index do
        case Process.get(:sm_current_ra_index) do
          idx when is_integer(idx) and idx >= 0 -> idx
          _ -> nil
        end
      end

      defp raft_apply_hook(%{instance_name: name} = state) when is_atom(name) do
        case current_raft_apply_hook(name) do
          {:ok, hook} -> hook
          :error -> raft_apply_hook_from_ctx(state)
        end
      end

      defp raft_apply_hook(%{instance_ctx: %{name: name}} = state) when is_atom(name) do
        case current_raft_apply_hook(name) do
          {:ok, hook} -> hook
          :error -> raft_apply_hook_from_ctx(state)
        end
      end

      defp raft_apply_hook(state) do
        raft_apply_hook_from_ctx(state) || raft_apply_hook_for_instance(:default)
      end

      defp raft_apply_hook_from_ctx(%{instance_ctx: %{raft_apply_hook: fun}})
           when is_function(fun),
           do: fun

      defp raft_apply_hook_from_ctx(_state), do: nil

      defp current_raft_apply_hook(name) do
        case FerricStore.Instance.get(name) do
          %{raft_apply_hook: fun} when is_function(fun) -> {:ok, fun}
          %{} -> {:ok, nil}
          _ -> :error
        end
      rescue
        _ -> :error
      catch
        :exit, _ -> :error
      end

      defp raft_apply_hook_for_instance(name) do
        try do
          case FerricStore.Instance.get(name) do
            %{raft_apply_hook: fun} when is_function(fun) -> fun
            _ -> nil
          end
        rescue
          _ -> nil
        catch
          :exit, _ -> nil
        end
      end

      defp apply_pending_with_time(meta, state, fun) do
        with_apply_time(meta, fn ->
          with_current_ra_index(meta, fn ->
            result = with_pending_writes(state, fun)
            bump_applied(meta, state, result)
          end)
        end)
      end

      defp apply_flow_pending_with_time(meta, state, command_shape, attrs, fun) do
        with_apply_time(meta, fn ->
          with_current_ra_index(meta, fn ->
            item_count = flow_apply_item_count(attrs)
            started_at = System.monotonic_time()

            result =
              with_flow_policy_snapshots(attrs, fn ->
                with_pending_writes(state, fun)
              end)

            emit_flow_apply_telemetry(
              state,
              command_shape,
              started_at,
              item_count,
              result
            )

            bump_applied(meta, state, result)
          end)
        end)
      end

      defp apply_flow_single_with_telemetry(state, command_shape, attrs, fun) do
        item_count = flow_apply_item_count(attrs)
        started_at = System.monotonic_time()
        result = with_flow_policy_snapshots(attrs, fun)

        emit_flow_apply_telemetry(
          state,
          command_shape,
          started_at,
          item_count,
          result
        )

        result
      end

      defp with_flow_policy_snapshots(attrs, fun) when is_function(fun, 0) do
        with {:ok, {captured?, snapshots}} <- flow_policy_snapshots(attrs) do
          if not captured? do
            fun.()
          else
            with {:ok, merged} <- merge_flow_policy_snapshots(snapshots) do
              previous = Process.get(:sm_flow_policy_snapshots, :undefined)
              Process.put(:sm_flow_policy_snapshots, merged)

              try do
                fun.()
              after
                case previous do
                  :undefined -> Process.delete(:sm_flow_policy_snapshots)
                  value -> Process.put(:sm_flow_policy_snapshots, value)
                end
              end
            end
          end
        end
      end

      defp flow_policy_snapshots(attrs) do
        with {:ok, snapshots} <- collect_flow_policy_snapshots(attrs, %{}),
             :ok <-
               snapshots
               |> Map.values()
               |> Enum.map(& &1.policy)
               |> RetryPolicy.validate_flow_policy_snapshots_size(),
             {:ok, captured?} <- flow_policy_snapshot_captured?(attrs, snapshots) do
          {:ok, {captured?, snapshots}}
        end
      end

      defp flow_policy_snapshot_captured?(attrs, snapshots) when is_list(attrs),
        do: {:ok, map_size(snapshots) > 0}

      defp flow_policy_snapshot_captured?(attrs, snapshots) when is_map(attrs) do
        case Map.fetch(attrs, :policy_snapshot_captured) do
          :error -> {:ok, map_size(snapshots) > 0}
          {:ok, true} -> {:ok, true}
          {:ok, _invalid} -> {:error, "ERR invalid flow policy snapshot"}
        end
      end

      defp collect_flow_policy_snapshots(attrs, snapshots) when is_map(attrs) do
        with {:ok, snapshots} <- collect_flow_policy_snapshot(attrs, snapshots),
             {:ok, snapshots} <- collect_flow_policy_snapshot_map(attrs, snapshots) do
          Enum.reduce_while([:records, :children], {:ok, snapshots}, fn key, {:ok, acc} ->
            case Map.get(attrs, key) do
              entries when is_list(entries) ->
                case collect_flow_policy_snapshots(entries, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  {:error, _reason} = error -> {:halt, error}
                end

              _other ->
                {:cont, {:ok, acc}}
            end
          end)
        end
      end

      defp collect_flow_policy_snapshot_map(attrs, snapshots) do
        case Map.fetch(attrs, :policy_snapshots) do
          :error ->
            {:ok, snapshots}

          {:ok, entries} when is_map(entries) ->
            Enum.reduce_while(entries, {:ok, snapshots}, fn
              {type, %{generation: generation, policy: policy}}, {:ok, acc}
              when is_binary(type) and type != "" ->
                snapshot_attrs = %{
                  type: type,
                  policy_generation: generation,
                  policy_snapshot: policy
                }

                case collect_flow_policy_snapshot(snapshot_attrs, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  {:error, _reason} = error -> {:halt, error}
                end

              _invalid, _acc ->
                {:halt, {:error, "ERR invalid flow policy snapshot"}}
            end)

          {:ok, _invalid} ->
            {:error, "ERR invalid flow policy snapshot"}
        end
      end

      defp collect_flow_policy_snapshots(entries, snapshots) when is_list(entries) do
        Enum.reduce_while(entries, {:ok, snapshots}, fn entry, {:ok, acc} ->
          case collect_flow_policy_snapshots(entry, acc) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp collect_flow_policy_snapshots(_other, snapshots), do: {:ok, snapshots}

      defp collect_flow_policy_snapshot(attrs, snapshots) do
        case {Map.fetch(attrs, :policy_generation), Map.fetch(attrs, :policy_snapshot)} do
          {:error, :error} ->
            {:ok, snapshots}

          {{:ok, generation}, {:ok, %{type: type} = policy}}
          when is_integer(generation) and generation >= 0 and is_binary(type) and type != "" ->
            if generation <= RetryPolicy.max_policy_generation() and
                 flow_policy_snapshot_matches_attrs?(attrs, type) do
              snapshot = %{generation: generation, policy: policy}

              case Map.fetch(snapshots, type) do
                :error -> {:ok, Map.put(snapshots, type, snapshot)}
                {:ok, ^snapshot} -> {:ok, snapshots}
                {:ok, _conflict} -> {:error, "ERR conflicting flow policy snapshots"}
              end
            else
              {:error, "ERR invalid flow policy snapshot"}
            end

          _invalid ->
            {:error, "ERR invalid flow policy snapshot"}
        end
      end

      defp flow_policy_snapshot_matches_attrs?(%{type: attrs_type}, snapshot_type)
           when is_binary(attrs_type) and attrs_type != "",
           do: attrs_type == snapshot_type

      defp flow_policy_snapshot_matches_attrs?(_attrs, _snapshot_type), do: true

      defp merge_flow_policy_snapshots(snapshots) do
        current = Process.get(:sm_flow_policy_snapshots, %{})

        Enum.reduce_while(snapshots, {:ok, current}, fn {type, snapshot}, {:ok, acc} ->
          case Map.fetch(acc, type) do
            :error -> {:cont, {:ok, Map.put(acc, type, snapshot)}}
            {:ok, ^snapshot} -> {:cont, {:ok, acc}}
            {:ok, _conflict} -> {:halt, {:error, "ERR conflicting flow policy snapshots"}}
          end
        end)
      end

      defp emit_flow_apply_telemetry(state, command_shape, started_at, item_count, result) do
        :telemetry.execute(
          [:ferricstore, :flow, :apply],
          %{
            duration_us: duration_us(started_at),
            item_count: item_count,
            result_count: flow_apply_result_count(result, item_count)
          },
          %{
            shard_index: Map.get(state, :shard_index),
            command_shape: command_shape,
            result: flow_apply_result_class(result)
          }
        )
      end

      defp flow_apply_item_count(%{records: records}) when is_list(records), do: length(records)

      defp flow_apply_item_count(%{children: children}) when is_list(children),
        do: length(children)

      defp flow_apply_item_count(_attrs), do: 1

      defp flow_apply_result_count({:ok, records}, _item_count) when is_list(records),
        do: length(records)

      defp flow_apply_result_count(results, _item_count) when is_list(results),
        do: length(results)

      defp flow_apply_result_count(result, item_count) when result in [:ok, nil], do: item_count
      defp flow_apply_result_count({:ok, _value}, item_count), do: item_count
      defp flow_apply_result_count({:error, _reason}, _item_count), do: 0
      defp flow_apply_result_count(_result, _item_count), do: 0

      defp flow_apply_result_class({:error, _reason}), do: :error
      defp flow_apply_result_class({:ok, _value}), do: :ok
      defp flow_apply_result_class(result) when result in [:ok, nil], do: :ok

      defp flow_apply_result_class(results) when is_list(results) do
        if Enum.any?(results, &flow_apply_error_result?/1), do: :partial, else: :ok
      end

      defp flow_apply_result_class(_result), do: :error

      defp flow_apply_error_result?({:error, _reason}), do: true
      defp flow_apply_error_result?(_result), do: false

      defp apply_control_with_time(meta, state, fun) do
        with_apply_time(meta, fn ->
          {new_state, result} = fun.()
          old_count = state.applied_count
          new_state = %{new_state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, result)
        end)
      end

      defp apply_prob_with_time(meta, state, fun) do
        with_apply_time(meta, fn ->
          result = do_prob_command(state, fun)
          bump_applied(meta, state, result)
        end)
      end

      defp cross_shard_ordered_entries(shard_batches) do
        {_next_legacy_index, entries} =
          Enum.reduce(shard_batches, {0, []}, fn {shard_idx, queue, sandbox_namespace},
                                                 {next_legacy_index, acc} ->
            {next_legacy_index, batch_entries} =
              queue
              |> Enum.with_index()
              |> Enum.reduce({next_legacy_index, []}, fn
                {{orig_idx, entry}, pos}, {next, inner} when is_integer(orig_idx) ->
                  {max(next, orig_idx + 1),
                   [{orig_idx, shard_idx, pos, entry, sandbox_namespace} | inner]}

                {entry, pos}, {next, inner} ->
                  {next + 1, [{next, shard_idx, pos, entry, sandbox_namespace} | inner]}
              end)

            {next_legacy_index, batch_entries ++ acc}
          end)

        Enum.sort_by(entries, fn {orig_idx, _shard_idx, _pos, _entry, _sandbox_namespace} ->
          orig_idx
        end)
      end

      defp prepare_cross_shard_policy_entries(entries, state) do
        with {:ok, policy_targets} <- cross_shard_policy_target_indices(entries, state) do
          entries
          |> Enum.reduce_while({:ok, [], %{}}, fn entry, {:ok, prepared, policies} ->
            case prepare_cross_shard_policy_entry(entry, state, policies, policy_targets) do
              {:ok, next_entry, next_policies} ->
                {:cont, {:ok, [next_entry | prepared], next_policies}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
          |> case do
            {:ok, prepared, _policies} -> {:ok, Enum.reverse(prepared)}
            {:error, _reason} = error -> error
          end
        end
      end

      defp cross_shard_policy_target_indices(entries, state) do
        entries
        |> Enum.reduce_while({:ok, %{}}, fn
          {_orig_idx, shard_idx, _pos,
           {:flow_cross_policy_put, target_idx, key, _value, _expire_at_ms}, _namespace},
          {:ok, targets}
          when is_integer(target_idx) and target_idx >= 0 and shard_idx == target_idx and
                 is_binary(key) ->
            case Map.fetch(targets, key) do
              {:ok, indices} ->
                if MapSet.member?(indices, target_idx) do
                  {:halt, {:error, "ERR invalid flow policy target set"}}
                else
                  {:cont, {:ok, Map.put(targets, key, MapSet.put(indices, target_idx))}}
                end

              :error ->
                {:cont, {:ok, Map.put(targets, key, MapSet.new([target_idx]))}}
            end

          {_orig_idx, _shard_idx, _pos,
           {:flow_cross_policy_put, _target_idx, _key, _value, _expire_at_ms}, _namespace},
          _acc ->
            {:halt, {:error, "ERR invalid flow policy target shard"}}

          _entry, {:ok, targets} ->
            {:cont, {:ok, targets}}
        end)
        |> case do
          {:ok, targets} ->
            with :ok <- validate_cross_shard_policy_target_sets(state, targets) do
              {:ok,
               Map.new(targets, fn {key, indices} ->
                 {key, indices |> Enum.to_list() |> Enum.sort()}
               end)}
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp validate_cross_shard_policy_target_sets(state, targets) do
        case cross_shard_instance_ctx(state) do
          %{shard_count: shard_count} when is_integer(shard_count) and shard_count > 0 ->
            expected = MapSet.new(0..(shard_count - 1))

            if Enum.all?(targets, fn {_key, indices} -> MapSet.equal?(indices, expected) end) do
              :ok
            else
              {:error, "ERR invalid flow policy target set"}
            end

          _unknown_instance ->
            :ok
        end
      end

      defp prepare_cross_shard_policy_entry(
             {orig_idx, shard_idx, pos,
              {:flow_cross_policy_put, target_idx, key, value, expire_at_ms}, namespace},
             state,
             policies,
             policy_targets
           ) do
        case Map.fetch(policies, key) do
          {:ok, {^value, versioned_value}} ->
            entry =
              {orig_idx, shard_idx, pos,
               {:flow_cross_policy_put, target_idx, key, versioned_value, expire_at_ms},
               namespace}

            {:ok, entry, policies}

          {:ok, {_other_value, _versioned_value}} ->
            {:error, "ERR conflicting flow policy values in transaction"}

          :error ->
            with {:ok, target_indices} <- Map.fetch(policy_targets, key),
                 {:ok, versioned_value} <-
                   next_flow_policy_value(state, key, value, target_indices) do
              entry =
                {orig_idx, shard_idx, pos,
                 {:flow_cross_policy_put, target_idx, key, versioned_value, expire_at_ms},
                 namespace}

              {:ok, entry, Map.put(policies, key, {value, versioned_value})}
            end
        end
      end

      defp prepare_cross_shard_policy_entry(entry, _state, policies, _policy_targets),
        do: {:ok, entry, policies}

      defp next_flow_policy_value(state, key, value, target_indices) do
        case FlowKeys.policy_type(key) do
          {:ok, type} ->
            case RetryPolicy.decode_flow_policy_entry(value) do
              {:ok, {_input_generation, %{type: ^type} = policy}} ->
                with {:ok, high_water} <-
                       current_flow_policy_generation(state, key, target_indices),
                     {:ok, generation} <-
                       Ferricstore.Flow.PolicyMigration.next_generation(high_water) do
                  {:ok, RetryPolicy.encode_flow_policy(policy, generation)}
                end

              _invalid ->
                {:error, "ERR invalid flow policy value"}
            end

          :error ->
            {:error, "ERR invalid flow policy key"}
        end
      end

      defp current_flow_policy_generation(state, key, target_indices) do
        with {:ok, type} <- FlowKeys.policy_type(key) do
          Enum.reduce_while(target_indices, {:ok, 0}, fn target_idx, {:ok, high_water} ->
            case flow_policy_target_state(state, target_idx) do
              {:ok, target_state} ->
                {stored_generation, _stored_value} =
                  flow_stored_policy_generation(target_state, key)

                case flow_policy_strict_migration_high_water(target_state, type) do
                  {:ok, migration_generation} ->
                    target_high_water = max(stored_generation, migration_generation)
                    {:cont, {:ok, max(high_water, target_high_water)}}

                  {:error, _reason} = error ->
                    {:halt, error}
                end

              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
        else
          :error -> {:error, "ERR invalid flow policy key"}
        end
      end

      defp flow_policy_strict_migration_high_water(state, type) do
        [
          {FlowKeys.policy_migration_job_key(type), :active},
          {FlowKeys.policy_migration_marker_key(type), :done}
        ]
        |> Enum.reduce_while({:ok, 0}, fn {key, expected_status}, {:ok, high_water} ->
          case do_get(state, key) do
            nil ->
              {:cont, {:ok, high_water}}

            value when is_binary(value) ->
              case Ferricstore.Flow.PolicyMigration.decode_job(value) do
                {:ok,
                 %{
                   type: ^type,
                   status: ^expected_status,
                   migration_generation: generation
                 }} ->
                  {:cont, {:ok, max(high_water, generation)}}

                _invalid ->
                  {:halt, {:error, "ERR corrupt flow policy migration high-water"}}
              end

            _invalid ->
              {:halt, {:error, "ERR corrupt flow policy migration high-water"}}
          end
        end)
      end

      defp flow_policy_target_state(state, target_idx) do
        instance_ctx = cross_shard_instance_ctx(state)

        target_in_range? =
          target_idx == state.shard_index or
            (is_map(instance_ctx) and is_integer(Map.get(instance_ctx, :shard_count)) and
               target_idx < Map.fetch!(instance_ctx, :shard_count) and
               is_tuple(Map.get(instance_ctx, :keydir_refs)) and
               target_idx < tuple_size(Map.fetch!(instance_ctx, :keydir_refs)))

        if target_in_range? do
          try do
            target_state = cross_shard_state_for_index(state, target_idx)

            if :ets.info(target_state.ets, :type) == :undefined do
              {:error, "ERR flow policy target shard not available"}
            else
              {:ok, target_state}
            end
          rescue
            _error -> {:error, "ERR flow policy target shard not available"}
          catch
            _kind, _reason -> {:error, "ERR flow policy target shard not available"}
          end
        else
          {:error, "ERR flow policy target shard not available"}
        end
      end

      defp transaction_watches_clean?(watched_keys, _state) when map_size(watched_keys) == 0,
        do: true

      defp transaction_watches_clean?(watched_keys, state) when is_map(watched_keys) do
        ctx = state.instance_ctx || FerricStore.Instance.get(:default)

        Enum.all?(watched_keys, fn {key, saved_token} ->
          try do
            Router.watch_token(ctx, key) == saved_token
          rescue
            _ -> false
          catch
            :exit, _ -> false
          end
        end)
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_spawn_children, attrs},
             _sandbox_namespace,
             _store,
             state
           ) do
        with_flow_policy_snapshots(attrs, fn -> do_flow_cross_spawn_children(state, attrs) end)
      end

      defp dispatch_cross_shard_entry(
             {:flow_shared_ref_write, shard_index, command},
             _sandbox_namespace,
             _store,
             state
           )
           when is_integer(shard_index) and shard_index >= 0 and is_tuple(command) do
        state
        |> cross_shard_state_for_index(shard_index)
        |> apply_single(command)
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_policy_put, shard_index, key, value, expire_at_ms},
             _sandbox_namespace,
             %{shard_index: shard_index},
             state
           )
           when is_integer(shard_index) and shard_index >= 0 and is_binary(key) and
                  is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0 do
        target_state = cross_shard_state_for_index(state, shard_index)

        with_lmdb_mirror_shard(target_state, fn ->
          do_flow_policy_put(target_state, key, value, expire_at_ms)
        end)
      end

      defp dispatch_cross_shard_entry(
             {orig_idx, entry},
             sandbox_namespace,
             store,
             state
           )
           when is_integer(orig_idx) and is_tuple(entry) do
        dispatch_cross_shard_entry(entry, sandbox_namespace, store, state)
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_terminal, op, attrs},
             _sandbox_namespace,
             _store,
             state
           ) do
        if op in [:complete, :retry, :fail, :cancel] do
          with_flow_policy_snapshots(attrs, fn -> do_flow_cross_terminal(state, op, attrs) end)
        else
          {:error, "ERR invalid flow cross-shard terminal op"}
        end
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_terminal_many, op, attrs_list},
             _sandbox_namespace,
             _store,
             state
           ) do
        if op in [:complete, :retry, :fail, :cancel] do
          with_flow_policy_snapshots(attrs_list, fn ->
            do_flow_cross_terminal_many(state, op, attrs_list)
          end)
        else
          {:error, "ERR invalid flow cross-shard terminal op"}
        end
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_retention_cleanup, attrs},
             _sandbox_namespace,
             _store,
             state
           ) do
        with_flow_policy_snapshots(attrs, fn -> do_flow_cross_retention_cleanup(state, attrs) end)
      end

      defp dispatch_cross_shard_entry(entry, sandbox_namespace, store, _state) do
        with {:ok, ast} <- normalize_cross_shard_entry_ast(entry, sandbox_namespace) do
          try do
            Dispatcher.dispatch_ast(ast, store)
          catch
            :exit, {:noproc, _} ->
              {:error, "ERR server not ready, shard process unavailable"}

            :exit, {reason, _} ->
              {:error, "ERR internal error: #{inspect(reason)}"}
          end
        end
      end

      defp normalize_cross_shard_entry_ast(entry, sandbox_namespace) do
        {:ok,
         entry
         |> TxAst.command_ast()
         |> TxAst.namespace_first_key(sandbox_namespace)}
      rescue
        _error in [ArgumentError, FunctionClauseError] ->
          {:error, "ERR invalid cross-shard transaction entry"}
      end

      defp cross_shard_fatal_entry_error({orig_idx, entry}, result)
           when is_integer(orig_idx) and is_tuple(entry) do
        cross_shard_fatal_entry_error(entry, result)
      end

      defp cross_shard_fatal_entry_error(
             _entry,
             {:error, {:blob_externalize_failed, _reason}} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_cross_spawn_children, _attrs},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_cross_terminal, _op, _attrs},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_cross_terminal_many, _op, _attrs_list},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_cross_retention_cleanup, _attrs},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_shared_ref_write, _shard_index, _command},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(
             {:flow_cross_policy_put, _shard_index, _key, _value, _expire_at_ms},
             {:error, _reason} = error
           ),
           do: error

      defp cross_shard_fatal_entry_error(_entry, _result), do: nil

      defp cross_shard_results_by_batch_position(shard_batches, results_by_position) do
        Map.new(shard_batches, fn {shard_idx, queue, _sandbox_namespace} ->
          shard_positions = Map.get(results_by_position, shard_idx, %{})

          results =
            queue
            |> Enum.with_index()
            |> Enum.map(fn {_entry, pos} -> Map.fetch!(shard_positions, pos) end)

          {shard_idx, results}
        end)
      end

      # ---------------------------------------------------------------------------
      # Private: cross-shard transaction store builder
      # ---------------------------------------------------------------------------

      # Builds a store map for a given shard_idx, usable by Dispatcher.dispatch.
      # For the anchor shard (matching state.shard_index), uses state directly.
      # For remote shards, reads active file info from persistent_term.
      defp build_cross_shard_store(shard_idx, anchor_state) do
        instance_ctx = cross_shard_instance_ctx(anchor_state)

        data_dir =
          if instance_ctx do
            instance_ctx.data_dir
          else
            anchor_state.data_dir
          end

        default_ctx = cross_shard_ctx(anchor_state, shard_idx, data_dir, instance_ctx)

        ctx_for_key =
          if instance_ctx do
            ctx_by_shard =
              0..(instance_ctx.shard_count - 1)
              |> Map.new(fn idx ->
                {idx, cross_shard_ctx(anchor_state, idx, data_dir, instance_ctx)}
              end)

            fn key ->
              Map.fetch!(ctx_by_shard, cross_shard_route_key(instance_ctx, key, shard_idx))
            end
          else
            fn _key -> default_ctx end
          end

        tx_binary_ref = keydir_binary_ref(anchor_state)

        put_in_ctx = fn ctx, key, value, expire_at_ms ->
          case maybe_externalize_cross_shard_value(anchor_state, ctx, value) do
            {:ok, value_for, disk_val, pending_value} ->
              record_cross_shard_pending_original(ctx, key)

              unless standalone_staged_apply?() do
                if tx_binary_ref do
                  new_bytes = binary_byte_size(key) + binary_byte_size(value_for)

                  old_bytes =
                    case :ets.lookup(ctx.keydir, key) do
                      [{^key, old_val, _, _, _, _, _}] ->
                        binary_byte_size(key) + binary_byte_size(old_val)

                      _ ->
                        0
                    end

                  delta = new_bytes - old_bytes
                  if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
                end

                :ets.insert(
                  ctx.keydir,
                  {key, value_for, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
                )
              end

              sm_tx_put_pending(key, pending_value, expire_at_ms)
              deleted = Process.get(:tx_deleted_keys, MapSet.new())

              if MapSet.member?(deleted, key) do
                Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
              end

              queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, value_for)

              :ok

            {:error, _reason} = error ->
              error
          end
        end

        local_put = fn key, value, expire_at_ms ->
          put_in_ctx.(ctx_for_key.(key), key, value, expire_at_ms)
        end

        delete_in_ctx = fn ctx, key ->
          record_cross_shard_pending_original(ctx, key)

          unless standalone_staged_apply?() do
            if tx_binary_ref do
              bytes =
                case :ets.lookup(ctx.keydir, key) do
                  [{^key, val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(val)
                  _ -> 0
                end

              if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
            end

            :ets.delete(ctx.keydir, key)
          end

          sm_tx_drop_pending(key)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())
          Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
          queue_cross_shard_pending_delete(ctx, key)
          :ok
        end

        local_delete = fn key ->
          delete_in_ctx.(ctx_for_key.(key), key)
        end

        local_get = fn key ->
          ctx = ctx_for_key.(key)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, key) do
            nil
          else
            case sm_tx_pending_meta(key) do
              {value, _exp} -> normalize_get_value(value)
              nil -> ctx |> cross_shard_ets_read(key) |> normalize_get_value()
            end
          end
        end

        local_get_meta = fn key ->
          ctx = ctx_for_key.(key)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, key) do
            nil
          else
            case sm_tx_pending_meta(key) do
              nil -> cross_shard_ets_read_meta(ctx, key)
              meta -> meta
            end
          end
        end

        local_exists = fn key ->
          ctx = ctx_for_key.(key)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, key) do
            false
          else
            sm_tx_pending_meta(key) != nil or cross_shard_ets_exists?(ctx, key)
          end
        end

        local_incr = fn key, delta ->
          case local_get_meta.(key) do
            nil ->
              local_put.(key, delta, 0)
              {:ok, delta}

            {value, expire_at_ms} ->
              case coerce_integer(value) do
                {:ok, int_val} ->
                  new_val = int_val + delta
                  local_put.(key, new_val, expire_at_ms)
                  {:ok, new_val}

                :error ->
                  {:error, "ERR value is not an integer or out of range"}
              end
          end
        end

        local_incr_float = fn key, delta ->
          case local_get_meta.(key) do
            nil ->
              new_val = delta * 1.0
              local_put.(key, new_val, 0)
              {:ok, new_val}

            {value, expire_at_ms} ->
              case coerce_float(value) do
                {:ok, float_val} ->
                  new_val = float_val + delta
                  local_put.(key, new_val, expire_at_ms)
                  {:ok, new_val}

                :error ->
                  {:error, "ERR value is not a valid float"}
              end
          end
        end

        local_append = fn key, suffix ->
          {current, expire_at_ms} =
            case local_get_meta.(key) do
              nil -> {"", 0}
              {v, exp} when is_integer(v) -> {Integer.to_string(v), exp}
              {v, exp} when is_float(v) -> {Float.to_string(v), exp}
              {v, exp} -> {v, exp}
            end

          new_val = current <> suffix
          local_put.(key, new_val, expire_at_ms)
          {:ok, byte_size(new_val)}
        end

        local_getset = fn key, new_value ->
          old = local_get.(key)
          local_put.(key, new_value, 0)
          old
        end

        local_getdel = fn key ->
          old = local_get.(key)
          if old, do: local_delete.(key)
          old
        end

        local_getex = fn key, expire_at_ms ->
          value = local_get.(key)
          if value, do: local_put.(key, value, expire_at_ms)
          value
        end

        local_setrange = fn key, offset, value ->
          {old, expire_at_ms} =
            case local_get_meta.(key) do
              nil -> {"", 0}
              {v, exp} when is_integer(v) -> {Integer.to_string(v), exp}
              {v, exp} when is_float(v) -> {Float.to_string(v), exp}
              {v, exp} -> {v, exp}
            end

          new_val = sm_apply_setrange(old, offset, value)
          local_put.(key, new_val, expire_at_ms)
          {:ok, byte_size(new_val)}
        end

        promoted_put = fn redis_key, compound_key, value, expire_at_ms, dedicated_path ->
          ctx = ctx_for_key.(redis_key)
          Promotion.await_compaction_latch(anchor_state, redis_key)

          value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
          disk_val = to_disk_binary(value)
          active = Promotion.find_active(dedicated_path)
          fid = parse_fid_from_path(active)

          case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
            {:ok, {offset, _record_size}} ->
              value_size = byte_size(disk_val)

              if tx_binary_ref do
                new_bytes = binary_byte_size(compound_key) + binary_byte_size(value_for)

                old_bytes =
                  case :ets.lookup(ctx.keydir, compound_key) do
                    [{^compound_key, old_val, _, _, _, _, _}] ->
                      binary_byte_size(compound_key) + binary_byte_size(old_val)

                    _ ->
                      0
                  end

                delta = new_bytes - old_bytes
                if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
              end

              :ets.insert(
                ctx.keydir,
                {compound_key, value_for, expire_at_ms, LFU.initial(), fid, offset, value_size}
              )

              sm_tx_put_pending(compound_key, value, expire_at_ms)
              deleted = Process.get(:tx_deleted_keys, MapSet.new())

              if MapSet.member?(deleted, compound_key) do
                Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
              end

              :ok

            {:error, _reason} = err ->
              err
          end
        end

        promoted_delete = fn redis_key, compound_key, dedicated_path ->
          ctx = ctx_for_key.(redis_key)
          Promotion.await_compaction_latch(anchor_state, redis_key)

          if tx_binary_ref do
            bytes =
              case :ets.lookup(ctx.keydir, compound_key) do
                [{^compound_key, val, _, _, _, _, _}] ->
                  binary_byte_size(compound_key) + binary_byte_size(val)

                _ ->
                  0
              end

            if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
          end

          active = Promotion.find_active(dedicated_path)

          case NIF.v2_append_tombstone(active, compound_key) do
            {:ok, _offset} ->
              :ets.delete(ctx.keydir, compound_key)
              sm_tx_drop_pending(compound_key)
              deleted = Process.get(:tx_deleted_keys, MapSet.new())
              Process.put(:tx_deleted_keys, MapSet.put(deleted, compound_key))
              :ok

            {:error, _reason} = err ->
              err
          end
        end

        promoted_put_batch = fn redis_key, entries, dedicated_path ->
          ctx = ctx_for_key.(redis_key)
          Promotion.await_compaction_latch(anchor_state, redis_key)

          active = Promotion.find_active(dedicated_path)
          fid = parse_fid_from_path(active)

          prepared =
            Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
              value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
              disk_val = to_disk_binary(value)
              {compound_key, value, value_for, disk_val, expire_at_ms}
            end)

          batch =
            Enum.map(prepared, fn {compound_key, _value, _value_for, disk_val, expire_at_ms} ->
              {compound_key, disk_val, expire_at_ms}
            end)

          case NIF.v2_append_batch(active, batch) do
            {:ok, locations} when length(locations) == length(prepared) ->
              deleted =
                Enum.zip(prepared, locations)
                |> Enum.reduce(Process.get(:tx_deleted_keys, MapSet.new()), fn
                  {{compound_key, value, value_for, disk_val, expire_at_ms},
                   {offset, _value_size}},
                  deleted_acc ->
                    if tx_binary_ref do
                      new_bytes = binary_byte_size(compound_key) + binary_byte_size(value_for)

                      old_bytes =
                        case :ets.lookup(ctx.keydir, compound_key) do
                          [{^compound_key, old_val, _, _, _, _, _}] ->
                            binary_byte_size(compound_key) + binary_byte_size(old_val)

                          _ ->
                            0
                        end

                      delta = new_bytes - old_bytes
                      if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
                    end

                    :ets.insert(
                      ctx.keydir,
                      {
                        compound_key,
                        value_for,
                        expire_at_ms,
                        LFU.initial(),
                        fid,
                        offset,
                        byte_size(disk_val)
                      }
                    )

                    sm_tx_put_pending(compound_key, value, expire_at_ms)
                    MapSet.delete(deleted_acc, compound_key)
                end)

              Process.put(:tx_deleted_keys, deleted)
              :ok

            {:ok, locations} ->
              {:error, {:batch_result_mismatch, length(prepared), locations}}

            {:error, _reason} = err ->
              err
          end
        end

        promoted_delete_batch = fn redis_key, compound_keys, dedicated_path ->
          ctx = ctx_for_key.(redis_key)
          Promotion.await_compaction_latch(anchor_state, redis_key)

          active = Promotion.find_active(dedicated_path)
          ops = Enum.map(compound_keys, &{:delete, &1})

          case NIF.v2_append_ops_batch_nosync(active, ops) do
            {:ok, locations} ->
              with :ok <- validate_promoted_tombstone_batch(locations, length(compound_keys)),
                   :ok <- NIF.v2_fsync(active) do
                deleted =
                  Enum.reduce(compound_keys, Process.get(:tx_deleted_keys, MapSet.new()), fn
                    compound_key, acc ->
                      if tx_binary_ref do
                        bytes =
                          case :ets.lookup(ctx.keydir, compound_key) do
                            [{^compound_key, val, _, _, _, _, _}] ->
                              binary_byte_size(compound_key) + binary_byte_size(val)

                            _ ->
                              0
                          end

                        if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
                      end

                      :ets.delete(ctx.keydir, compound_key)
                      sm_tx_drop_pending(compound_key)
                      MapSet.put(acc, compound_key)
                  end)

                Process.put(:tx_deleted_keys, deleted)
                :ok
              end

            {:error, _reason} = err ->
              err
          end
        end

        %{
          get: local_get,
          get_meta: local_get_meta,
          batch_get: fn keys -> cross_shard_routed_batch_read(keys, ctx_for_key) end,
          put: local_put,
          delete: local_delete,
          exists?: local_exists,
          keys: fn -> Router.keys(instance_ctx) end,
          flush: fn ->
            Enum.each(Router.keys(instance_ctx), fn k -> Router.delete(instance_ctx, k) end)
            :ok
          end,
          dbsize: fn -> Router.dbsize(instance_ctx) end,
          incr: local_incr,
          incr_float: local_incr_float,
          append: local_append,
          getset: local_getset,
          getdel: local_getdel,
          getex: local_getex,
          setrange: local_setrange,
          cas: fn key, expected, new_value, ttl_ms ->
            Router.cas(instance_ctx, key, expected, new_value, ttl_ms)
          end,
          lock: fn key, owner, ttl_ms -> Router.lock(instance_ctx, key, owner, ttl_ms) end,
          unlock: fn key, owner -> Router.unlock(instance_ctx, key, owner) end,
          extend: fn key, owner, ttl_ms -> Router.extend(instance_ctx, key, owner, ttl_ms) end,
          ratelimit_add: fn key, window_ms, max, count ->
            Router.ratelimit_add(instance_ctx, key, window_ms, max, count)
          end,
          list_op: fn key, op -> Router.list_op(instance_ctx, key, op) end,
          compound_get: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_read(ctx, redis_key, compound_key)
          end,
          compound_get_meta: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_read_meta(ctx, redis_key, compound_key)
          end,
          compound_batch_get: fn redis_key, compound_keys ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_batch_read(ctx, redis_key, compound_keys)
          end,
          compound_batch_get_meta: fn redis_key, compound_keys ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys)
          end,
          compound_put: fn redis_key, compound_key, value, expire_at_ms ->
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil ->
                put_in_ctx.(ctx, compound_key, value, expire_at_ms)

              dedicated_path ->
                promoted_put.(redis_key, compound_key, value, expire_at_ms, dedicated_path)
            end
          end,
          compound_batch_put: fn redis_key, entries ->
            ctx = ctx_for_key.(redis_key)

            entries
            |> Enum.chunk_by(fn {compound_key, _value, _expire_at_ms} ->
              promoted_compound_path(ctx, redis_key, compound_key)
            end)
            |> Enum.reduce_while(:ok, fn entries, :ok ->
              result =
                case promoted_compound_path(ctx, redis_key, elem(hd(entries), 0)) do
                  nil ->
                    Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
                      put_in_ctx.(ctx, compound_key, value, expire_at_ms)
                    end)

                    :ok

                  dedicated_path ->
                    promoted_put_batch.(redis_key, entries, dedicated_path)
                end

              case result do
                :ok -> {:cont, :ok}
                {:error, _} = error -> {:halt, error}
              end
            end)
          end,
          compound_delete: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil -> delete_in_ctx.(ctx, compound_key)
              dedicated_path -> promoted_delete.(redis_key, compound_key, dedicated_path)
            end
          end,
          compound_batch_delete: fn redis_key, compound_keys ->
            ctx = ctx_for_key.(redis_key)

            compound_keys
            |> Enum.chunk_by(fn compound_key ->
              promoted_compound_path(ctx, redis_key, compound_key)
            end)
            |> Enum.reduce_while(:ok, fn compound_keys, :ok ->
              result =
                case promoted_compound_path(ctx, redis_key, hd(compound_keys)) do
                  nil ->
                    Enum.each(compound_keys, fn compound_key ->
                      delete_in_ctx.(ctx, compound_key)
                    end)

                    :ok

                  dedicated_path ->
                    promoted_delete_batch.(redis_key, compound_keys, dedicated_path)
                end

              case result do
                :ok -> {:cont, :ok}
                {:error, _} = error -> {:halt, error}
              end
            end)
          end,
          compound_scan: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_scan(ctx, redis_key, prefix)
          end,
          compound_count: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_prefix_count(ctx, prefix)
          end,
          compound_delete_prefix: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_delete_prefix(ctx, prefix, fn key -> delete_in_ctx.(ctx, key) end)
          end,
          zset_score_range: fn redis_key, min_bound, max_bound, reverse? ->
            ctx = ctx_for_key.(redis_key)

            cross_shard_zset_index_read(ctx, redis_key, fn state ->
              {:ok,
               ZSetIndex.range(state.zset_score_index, redis_key, min_bound, max_bound, reverse?)}
            end)
          end,
          zset_score_range_slice: fn redis_key, min_bound, max_bound, reverse?, offset, count ->
            ctx = ctx_for_key.(redis_key)

            cross_shard_zset_index_read(ctx, redis_key, fn state ->
              {:ok,
               ZSetIndex.range_slice(
                 state.zset_score_index,
                 redis_key,
                 min_bound,
                 max_bound,
                 reverse?,
                 offset,
                 count
               )}
            end)
          end,
          zset_score_count: fn redis_key, min_bound, max_bound ->
            ctx = ctx_for_key.(redis_key)

            cross_shard_zset_index_read(ctx, redis_key, fn state ->
              {:ok,
               ZSetIndex.count(
                 state.zset_score_index,
                 state.zset_score_lookup,
                 redis_key,
                 min_bound,
                 max_bound
               )}
            end)
          end,
          zset_rank_range: fn redis_key, start_idx, stop_idx, reverse? ->
            ctx = ctx_for_key.(redis_key)

            cross_shard_zset_index_read(ctx, redis_key, fn state ->
              {:ok,
               ZSetIndex.rank_range(
                 state.zset_score_index,
                 redis_key,
                 start_idx,
                 stop_idx,
                 reverse?
               )}
            end)
          end,
          zset_member_rank: fn redis_key, member, reverse? ->
            ctx = ctx_for_key.(redis_key)

            cross_shard_zset_index_read(ctx, redis_key, fn state ->
              {:ok,
               ZSetIndex.member_rank(
                 state.zset_score_index,
                 state.zset_score_lookup,
                 redis_key,
                 member,
                 reverse?
               )}
            end)
          end,
          prob_dir: fn ->
            Path.join(default_ctx.shard_data_path, "prob")
          end,
          prob_write: fn command ->
            # Within cross-shard tx, prob writes are applied directly
            # (the state machine is already applying through Raft)
            apply_prob_locally(instance_ctx, command)
          end,
          shard_index: default_ctx.index,
          data_dir: data_dir
        }
      end
    end
  end
end
