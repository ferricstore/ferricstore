defmodule Ferricstore.Raft.StateMachine.Sections.CrossShardDispatch do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
      import Kernel, except: [apply: 3]
      import Bitwise

      require Logger

      alias Ferricstore.CommandTime
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.HyperLogLog
      alias Ferricstore.ExpiryContext
      alias Ferricstore.Raft.BlobCommand
      alias Ferricstore.Raft.ApplyLimits
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
        RateLimit,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}

      @transaction_watch_max_entries 10_000
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp with_current_ra_index(%{index: ra_index}, fun)
           when is_integer(ra_index) and ra_index >= 0 do
        case Process.get(:sm_current_ra_index, :undefined) do
          ^ra_index ->
            fun.()

          previous ->
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
      end

      defp with_current_ra_index(_meta, fun), do: fun.()

      defp current_ra_index do
        case Process.get(:sm_current_ra_index) do
          idx when is_integer(idx) and idx >= 0 -> idx
          _ -> nil
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
              with :ok <- ApplyLimits.validate_flow_batch(state, attrs),
                   :ok <- ApplyLimits.validate_flow_time(attrs, apply_now_ms()) do
                with_flow_policy_references(state, command_shape, attrs, fn ->
                  with_pending_writes(state, fun)
                end)
              end

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

        result =
          with :ok <- ApplyLimits.validate_flow_batch(state, attrs),
               :ok <- ApplyLimits.validate_flow_time(attrs, apply_now_ms()) do
            with_flow_policy_references(state, command_shape, attrs, fun)
          end

        emit_flow_apply_telemetry(
          state,
          command_shape,
          started_at,
          item_count,
          result
        )

        result
      end

      defp apply_flow_policy_fence(state, installs, command) do
        with :ok <- validate_flow_policy_fence(installs),
             :ok <- install_flow_policy_fence(state, installs) do
          apply_single(state, command)
        end
      end

      defp validate_flow_policy_fence(installs) when is_list(installs) do
        with :ok <-
               installs
               |> :erlang.external_size()
               |> RetryPolicy.validate_flow_policy_snapshot_batch_size() do
          installs
          |> Enum.reduce_while({:ok, MapSet.new()}, fn
            {key, encoded, 0}, {:ok, seen} when is_binary(key) and is_binary(encoded) ->
              with {:ok, type} <- FlowKeys.policy_type(key),
                   {:ok, {_generation, %{type: ^type}}} <-
                     RetryPolicy.decode_flow_policy_entry(encoded),
                   false <- MapSet.member?(seen, key) do
                {:cont, {:ok, MapSet.put(seen, key)}}
              else
                _invalid -> {:halt, {:error, "ERR invalid flow policy fence"}}
              end

            _invalid, _acc ->
              {:halt, {:error, "ERR invalid flow policy fence"}}
          end)
          |> case do
            {:ok, _seen} -> :ok
            {:error, _reason} = error -> error
          end
        end
      end

      defp install_flow_policy_fence(state, installs) do
        Enum.reduce_while(installs, :ok, fn {key, encoded, expire_at_ms}, :ok ->
          case do_flow_policy_put(state, key, encoded, expire_at_ms) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp with_flow_policy_references(state, command_shape, attrs, fun)
           when is_function(fun, 0) do
        if Ferricstore.Flow.PolicyCommand.policy_sensitive_op?(command_shape) do
          with :ok <- validate_flow_policy_guards(state, attrs),
               {:ok, refs} <- flow_policy_refs(attrs),
               {:ok, snapshots} <- resolve_flow_policy_refs(state, refs),
               {:ok, merged} <- merge_flow_policy_snapshots(snapshots) do
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
        else
          fun.()
        end
      end

      defp flow_policy_refs(attrs) do
        with {:ok, refs} <- collect_flow_policy_refs(attrs, %{}),
             :ok <-
               refs
               |> Map.values()
               |> RetryPolicy.validate_flow_policy_snapshots_size(),
             :ok <- require_flow_policy_reference_marker(attrs) do
          {:ok, refs}
        end
      end

      defp require_flow_policy_reference_marker(attrs) when is_map(attrs) do
        case Map.fetch(attrs, :policy_reference_captured) do
          {:ok, true} -> :ok
          :error -> {:error, "ERR flow policy reference is required"}
          {:ok, _invalid} -> {:error, "ERR invalid flow policy reference marker"}
        end
      end

      defp require_flow_policy_reference_marker(_attrs),
        do: {:error, "ERR flow policy reference is required"}

      defp collect_flow_policy_refs(attrs, refs) when is_map(attrs) do
        with {:ok, refs} <- collect_flow_policy_ref(attrs, refs),
             {:ok, refs} <- collect_flow_policy_ref_map(attrs, refs) do
          Enum.reduce_while([:records, :children], {:ok, refs}, fn key, {:ok, acc} ->
            case Map.get(attrs, key) do
              entries when is_list(entries) ->
                case collect_flow_policy_refs(entries, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  {:error, _reason} = error -> {:halt, error}
                end

              _other ->
                {:cont, {:ok, acc}}
            end
          end)
        end
      end

      defp collect_flow_policy_ref_map(attrs, refs) do
        case Map.fetch(attrs, :policy_refs) do
          :error ->
            {:ok, refs}

          {:ok, entries} when is_map(entries) ->
            Enum.reduce_while(entries, {:ok, refs}, fn
              {type, %{type: type, generation: generation, digest: digest} = policy_ref},
              {:ok, acc}
              when is_binary(type) and type != "" ->
                ref_attrs = %{type: type, policy_ref: policy_ref}

                case collect_flow_policy_ref(ref_attrs, acc) do
                  {:ok, next} -> {:cont, {:ok, next}}
                  {:error, _reason} = error -> {:halt, error}
                end

              _invalid, _acc ->
                {:halt, {:error, "ERR invalid flow policy reference"}}
            end)

          {:ok, _invalid} ->
            {:error, "ERR invalid flow policy reference"}
        end
      end

      defp collect_flow_policy_refs(entries, refs) when is_list(entries) do
        Enum.reduce_while(entries, {:ok, refs}, fn entry, {:ok, acc} ->
          case collect_flow_policy_refs(entry, acc) do
            {:ok, next} -> {:cont, {:ok, next}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp collect_flow_policy_refs(_other, refs), do: {:ok, refs}

      defp collect_flow_policy_ref(attrs, refs) do
        case Map.fetch(attrs, :policy_ref) do
          :error ->
            {:ok, refs}

          {:ok, %{type: type, generation: generation, digest: digest} = policy_ref}
          when is_integer(generation) and generation >= 0 and is_binary(type) and type != "" and
                 is_binary(digest) and byte_size(digest) == 32 ->
            if generation <= RetryPolicy.max_policy_generation() and
                 flow_policy_ref_matches_attrs?(attrs, type) do
              case Map.fetch(refs, type) do
                :error -> {:ok, Map.put(refs, type, policy_ref)}
                {:ok, ^policy_ref} -> {:ok, refs}
                {:ok, _conflict} -> {:error, "ERR conflicting flow policy references"}
              end
            else
              {:error, "ERR invalid flow policy reference"}
            end

          _invalid ->
            {:error, "ERR invalid flow policy reference"}
        end
      end

      defp flow_policy_ref_matches_attrs?(%{type: attrs_type}, ref_type)
           when is_binary(attrs_type) and attrs_type != "",
           do: attrs_type == ref_type

      defp flow_policy_ref_matches_attrs?(_attrs, _ref_type), do: true

      defp resolve_flow_policy_refs(state, refs) do
        refs
        |> Enum.reduce_while({:ok, %{}}, fn {type, policy_ref}, {:ok, snapshots} ->
          case resolve_flow_policy_ref(state, type, policy_ref) do
            {:ok, snapshot} -> {:cont, {:ok, Map.put(snapshots, type, snapshot)}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, snapshots} ->
            with :ok <-
                   snapshots
                   |> Map.values()
                   |> Enum.map(& &1.policy)
                   |> RetryPolicy.validate_flow_policy_snapshots_size() do
              {:ok, snapshots}
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp resolve_flow_policy_ref(
             state,
             type,
             %{generation: expected_generation, digest: expected_digest}
           ) do
        value = do_get(state, FlowKeys.policy_key(type))

        case decode_replicated_flow_policy(type, value) do
          {:ok, {local_generation, policy, encoded}} ->
            cond do
              local_generation < expected_generation ->
                {:error, "ERR flow policy generation is not applied"}

              local_generation > expected_generation ->
                {:error, "ERR stale flow policy generation"}

              not :crypto.hash_equals(:crypto.hash(:sha256, encoded), expected_digest) ->
                {:error, "ERR conflicting flow policy generation"}

              true ->
                {:ok, %{generation: local_generation, policy: policy}}
            end

          {:error, _reason} = error ->
            error
        end
      end

      defp decode_replicated_flow_policy(type, nil) do
        policy = %{type: type}
        {:ok, {0, policy, RetryPolicy.encode_flow_policy(policy, 0)}}
      end

      defp decode_replicated_flow_policy(type, encoded) when is_binary(encoded) do
        case RetryPolicy.decode_flow_policy_entry(encoded) do
          {:ok, {generation, %{type: ^type} = policy}} ->
            {:ok, {generation, policy, encoded}}

          _invalid ->
            {:error, "ERR replicated flow policy is corrupt"}
        end
      end

      defp decode_replicated_flow_policy(_type, _invalid),
        do: {:error, "ERR replicated flow policy is corrupt"}

      defp validate_flow_policy_guards(state, attrs) when is_map(attrs) do
        with :ok <- validate_flow_policy_guard(state, Map.get(attrs, :policy_guard)) do
          Enum.reduce_while([:records, :children], :ok, fn key, :ok ->
            case Map.get(attrs, key) do
              entries when is_list(entries) ->
                case validate_flow_policy_guards(state, entries) do
                  :ok -> {:cont, :ok}
                  {:error, _reason} = error -> {:halt, error}
                end

              _other ->
                {:cont, :ok}
            end
          end)
        end
      end

      defp validate_flow_policy_guards(state, entries) when is_list(entries) do
        Enum.reduce_while(entries, :ok, fn entry, :ok ->
          case validate_flow_policy_guards(state, entry) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp validate_flow_policy_guards(_state, _other), do: :ok

      defp validate_flow_policy_guard(_state, nil), do: :ok

      defp validate_flow_policy_guard(
             state,
             %{state_key: state_key, type: expected_type, incarnation: expected_incarnation}
           )
           when is_binary(state_key) and is_binary(expected_type) and expected_type != "" and
                  is_integer(expected_incarnation) and expected_incarnation >= 0 do
        target_state = cross_shard_state_for_key(state, state_key)

        case flow_read_record_by_key(target_state, state_key) do
          %{type: ^expected_type, incarnation: ^expected_incarnation} -> :ok
          _missing_or_recreated -> {:error, "ERR stale flow policy target"}
        end
      end

      defp validate_flow_policy_guard(_state, _invalid),
        do: {:error, "ERR invalid flow policy target"}

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
          previous_failure =
            Process.get(:sm_state_read_failure, :__ferricstore_control_read_failure_unset__)

          Process.put(:sm_state_read_failure, nil)

          try do
            {candidate_state, candidate_result} = fun.()

            {new_state, result} =
              case Process.get(:sm_state_read_failure) do
                nil ->
                  {candidate_state, candidate_result}

                reason ->
                  {state, {:error, {:state_read_failed, reason}}}
              end

            old_count = state.applied_count
            new_state = %{new_state | applied_count: old_count + 1}
            maybe_release_cursor(meta, old_count, new_state, result)
          after
            case previous_failure do
              :__ferricstore_control_read_failure_unset__ ->
                Process.delete(:sm_state_read_failure)

              failure ->
                Process.put(:sm_state_read_failure, failure)
            end
          end
        end)
      end

      defp apply_prob_with_time(meta, state, fun) do
        with_apply_time(meta, fn ->
          result = do_prob_command(state, fun)
          bump_applied(meta, state, result)
        end)
      end

      defp cross_shard_ordered_entries(shard_batches) do
        {_next_generated_index, entries} =
          Enum.reduce(shard_batches, {0, []}, fn {shard_idx, queue, sandbox_namespace},
                                                 {next_generated_index, acc} ->
            {next_generated_index, batch_entries} =
              queue
              |> Enum.with_index()
              |> Enum.reduce({next_generated_index, []}, fn
                {{orig_idx, entry}, pos}, {next, inner} when is_integer(orig_idx) ->
                  {max(next, orig_idx + 1),
                   [{orig_idx, shard_idx, pos, entry, sandbox_namespace} | inner]}

                {entry, pos}, {next, inner} ->
                  {next + 1, [{next, shard_idx, pos, entry, sandbox_namespace} | inner]}
              end)

            {next_generated_index, batch_entries ++ acc}
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
        do: :clean

      defp transaction_watches_clean?(watched_keys, state) when is_map(watched_keys) do
        watched_keys
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce_while(@transaction_watch_max_entries, fn {key, saved_token}, remaining ->
          if match?({:error, _reason}, saved_token) do
            {:halt, :changed}
          else
            case transaction_watch_token_with_budget(state, key, remaining) do
              {:ok, ^saved_token, cost} -> {:cont, remaining - cost}
              {:ok, _changed_token, _cost} -> {:halt, :changed}
              {:error, reason} -> {:halt, {:error, reason}}
            end
          end
        end)
        |> case do
          remaining when is_integer(remaining) -> :clean
          result -> result
        end
      end

      defp transaction_watch_token(state, key) when is_binary(key) do
        case transaction_watch_token_with_budget(state, key, @transaction_watch_max_entries) do
          {:ok, token, _cost} -> token
          {:error, _reason} = error -> error
        end
      end

      defp transaction_watch_token_with_budget(state, key, remaining)
           when is_binary(key) and is_integer(remaining) and remaining >= 0 do
        case :ets.lookup(state.ets, key) do
          [] ->
            transaction_compound_watch_token(state, key, remaining)

          entry ->
            case transaction_storage_watch_token(state, key, entry) do
              {:error, _reason} = error -> error
              token -> {:ok, token, 0}
            end
        end
      rescue
        ArgumentError -> {:error, :watch_state_unavailable}
      end

      defp transaction_watch_tokens(state, keys) when is_list(keys) do
        Enum.reduce_while(keys, {:ok, %{}, @transaction_watch_max_entries}, fn
          key, {:ok, tokens, remaining} when is_binary(key) ->
            case transaction_watch_token_with_budget(state, key, remaining) do
              {:error, _reason} = error -> {:halt, error}
              {:ok, token, cost} -> {:cont, {:ok, Map.put(tokens, key, token), remaining - cost}}
            end

          _invalid_key, _acc ->
            {:halt, {:error, :invalid_watch_key}}
        end)
        |> case do
          {:ok, tokens, _remaining} -> tokens
          {:error, _reason} = error -> error
        end
      end

      defp transaction_compound_watch_token(state, key, remaining) do
        {metadata_keys, prefixes} = transaction_compound_watch_layout(state, key)

        case transaction_compound_watch_keys(state, metadata_keys, prefixes, remaining) do
          {:error, _reason} = error ->
            error

          keys ->
            case transaction_compound_watch_entries(state, key, keys) do
              {:error, _reason} = error -> error
              token -> {:ok, token, length(keys)}
            end
        end
      end

      defp transaction_compound_watch_keys(state, metadata_keys, prefixes, max_entries) do
        initial = MapSet.new(metadata_keys)

        if MapSet.size(initial) > max_entries do
          {:error, watch_budget_error(max_entries)}
        else
          Enum.reduce_while(prefixes, initial, fn prefix, keys ->
            remaining = max(max_entries - MapSet.size(keys), 0)

            case CompoundMemberIndex.keys_for_prefix(
                   Map.get(state, :compound_member_index_name),
                   prefix,
                   remaining
                 ) do
              {:ok, prefix_keys} ->
                {:cont, Enum.reduce(prefix_keys, keys, &MapSet.put(&2, &1))}

              {:error, :limit_exceeded} ->
                {:halt, {:error, watch_budget_error(max_entries)}}

              :unavailable ->
                {:halt, {:error, :watch_compound_index_unavailable}}
            end
          end)
          |> case do
            %MapSet{} = keys -> keys |> MapSet.to_list() |> Enum.sort()
            {:error, _reason} = error -> error
          end
        end
      end

      defp watch_budget_error(@transaction_watch_max_entries),
        do: :watch_collection_too_large

      defp watch_budget_error(_remaining), do: :watch_scan_budget_exceeded

      defp transaction_compound_watch_layout(state, key) do
        type_key = CompoundKey.type_key(key)
        list_meta_key = CompoundKey.list_meta_key(key)

        case sm_store_compound_get(state, key, type_key) do
          "hash" ->
            {[type_key], [CompoundKey.hash_prefix(key)]}

          "list" ->
            {[type_key, list_meta_key], [CompoundKey.list_prefix(key)]}

          "set" ->
            {[type_key], [CompoundKey.set_prefix(key)]}

          "zset" ->
            {[type_key], [CompoundKey.zset_prefix(key)]}

          "stream" ->
            {[type_key, CompoundKey.stream_meta_key(key)],
             [CompoundKey.stream_prefix(key), CompoundKey.stream_group_prefix(key)]}

          nil ->
            if do_get(state, list_meta_key) == nil do
              {[], []}
            else
              {[list_meta_key], [CompoundKey.list_prefix(key)]}
            end

          _other_type ->
            {[type_key], []}
        end
      end

      defp transaction_compound_watch_entries(_state, _redis_key, []), do: :missing

      defp transaction_compound_watch_entries(state, _redis_key, keys)
           when length(keys) > @transaction_watch_max_entries do
        _ = state
        {:error, :watch_collection_too_large}
      end

      defp transaction_compound_watch_entries(state, redis_key, keys) do
        keys
        |> Enum.reduce_while({:ok, []}, fn storage_key, {:ok, acc} ->
          entry = :ets.lookup(state.ets, storage_key)

          token =
            transaction_compound_storage_watch_token(state, redis_key, storage_key, entry)

          case token do
            :missing -> {:cont, {:ok, acc}}
            {:error, _reason} = error -> {:halt, error}
            _token -> {:cont, {:ok, [{storage_key, token} | acc]}}
          end
        end)
        |> case do
          {:ok, []} ->
            :missing

          {:ok, entries} ->
            digest =
              entries
              |> Enum.reverse()
              |> :erlang.term_to_binary([:deterministic])
              |> then(&:crypto.hash(:sha256, &1))

            {:watch, {:compound_sha256, digest}}

          {:error, _reason} = error ->
            error
        end
      end

      defp transaction_compound_storage_watch_token(
             state,
             redis_key,
             storage_key,
             [
               {entry_key, _value, expire_at_ms, _lfu, file_id, offset, value_size}
             ] = entry
           )
           when entry_key == storage_key and is_integer(expire_at_ms) and is_integer(file_id) and
                  file_id >= 0 and
                  is_integer(offset) and offset >= 0 and is_integer(value_size) and
                  value_size >= 0 do
        if promoted_compound_path(state, redis_key, storage_key) == nil do
          transaction_storage_watch_token(state, storage_key, entry)
        else
          case ExpiryContext.classify(ExpiryContext.capture(), expire_at_ms) do
            :expired ->
              :missing

            {:unsafe, reason} ->
              {:error, reason}

            :live ->
              case Ferricstore.Store.Shard.CompoundRevisionIndex.revision_token(
                     Map.get(state, :compound_revision_index_name),
                     storage_key
                   ) do
                {:ok, {epoch, revision}} ->
                  {:watch, {:dedicated_revision, epoch, revision}, expire_at_ms}

                :missing ->
                  transaction_storage_watch_token(state, storage_key, entry)

                :unavailable ->
                  {:error, :watch_state_unavailable}
              end
          end
        end
      end

      defp transaction_compound_storage_watch_token(state, redis_key, storage_key, entry) do
        Ferricstore.Transaction.WatchToken.from_entry(entry, ExpiryContext.capture(), fn ->
          sm_store_compound_get(state, redis_key, storage_key)
        end)
      end

      defp transaction_storage_watch_token(state, storage_key, entry) do
        Ferricstore.Transaction.WatchToken.from_entry(entry, ExpiryContext.capture(), fn ->
          do_get(state, storage_key)
        end)
      end

      defp dispatch_cross_shard_entry(
             {:flow_cross_spawn_children, attrs},
             _sandbox_namespace,
             _store,
             state
           ) do
        with_flow_policy_references(state, :flow_cross_spawn_children, attrs, fn ->
          do_flow_cross_spawn_children(state, attrs)
        end)
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
          with_flow_policy_references(state, :flow_cross_terminal, attrs, fn ->
            do_flow_cross_terminal(state, op, attrs)
          end)
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
          with_flow_policy_references(state, :flow_cross_terminal_many, attrs_list, fn ->
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
        with_flow_policy_references(state, :flow_cross_retention_cleanup, attrs, fn ->
          do_flow_cross_retention_cleanup(state, attrs)
        end)
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
         |> TxAst.namespace_ast_keys(sandbox_namespace)}
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

      defp hold_transaction_promotion_latch(ctx, redis_key) do
        latch_key = {ctx.index, redis_key}
        held = Process.get(:sm_tx_promoted_latches, %{})

        unless Map.has_key?(held, latch_key) do
          token = Promotion.acquire_compaction_latch(ctx, redis_key)
          Process.put(:sm_tx_promoted_latches, Map.put(held, latch_key, token))
        end

        :ok
      end

      defp transaction_compound_prefix_keys(ctx, prefix, member_budget) do
        case CompoundMemberIndex.live_keys_for_prefix(
               ctx.compound_member_index_name,
               %{keydir: ctx.keydir},
               prefix,
               member_budget
             ) do
          {:ok, _catalog_keys, _inspected, :more} ->
            {:error, :limit_exceeded}

          {:ok, catalog_keys, inspected, :complete} ->
            deleted = Process.get(:tx_deleted_keys, MapSet.new())

            base_keys =
              catalog_keys
              |> MapSet.new()
              |> MapSet.difference(deleted)

            :tx_pending_compound_keys
            |> sm_tx_compound_keys_for_prefix(prefix)
            |> Enum.reduce_while({:ok, base_keys, inspected}, fn
              _key, {:ok, _keys, work} when work >= member_budget ->
                {:halt, {:error, :limit_exceeded}}

              key, {:ok, keys, work} ->
                keys =
                  if MapSet.member?(deleted, key) do
                    keys
                  else
                    MapSet.put(keys, key)
                  end

                {:cont, {:ok, keys, work + 1}}
            end)
            |> case do
              {:ok, keys, work} ->
                {:ok, keys |> MapSet.to_list() |> Enum.sort(), work}

              {:error, :limit_exceeded} = error ->
                error
            end

          {:error, :limit_exceeded} ->
            {:error, :limit_exceeded}

          :unavailable ->
            {:error, :compound_member_index_unavailable}
        end
      end

      defp transaction_compound_member_budget_remaining(anchor_state) do
        max(
          anchor_state.apply_context.compound_member_apply_budget -
            Process.get(:tx_compound_member_work_used, 0),
          0
        )
      end

      defp charge_transaction_compound_member_work!(count)
           when is_integer(count) and count >= 0 do
        used = Process.get(:tx_compound_member_work_used, 0)
        Process.put(:tx_compound_member_work_used, used + count)
        :ok
      end

      defp admit_transaction_compound_batch_read_work!(items, anchor_state) do
        remaining = transaction_compound_member_budget_remaining(anchor_state)

        case compound_batch_read_work_up_to(items, remaining, 0, MapSet.new()) do
          {:ok, count, read_keys} ->
            charge_transaction_compound_member_work!(count)

            Process.put(
              :tx_current_command_compound_reads,
              MapSet.union(
                Process.get(:tx_current_command_compound_reads, MapSet.new()),
                read_keys
              )
            )

            :ok

          :limit_exceeded ->
            throw({
              :transaction_store_failure,
              :transaction_compound_read_budget_exceeded
            })

          :invalid ->
            throw({:transaction_store_failure, :invalid_compound_read_batch})
        end
      end

      defp compound_batch_read_work_up_to([], _remaining, count, read_keys),
        do: {:ok, count, read_keys}

      defp compound_batch_read_work_up_to([_item | _rest], 0, _count, _read_keys),
        do: :limit_exceeded

      defp compound_batch_read_work_up_to(
             [key | rest],
             remaining,
             count,
             read_keys
           )
           when is_binary(key) do
        compound_batch_read_work_up_to(
          rest,
          remaining - 1,
          count + 1,
          MapSet.put(read_keys, key)
        )
      end

      defp compound_batch_read_work_up_to(
             _invalid,
             _remaining,
             _count,
             _read_keys
           ),
           do: :invalid

      defp admit_transaction_compound_batch_work!(items, anchor_state) do
        remaining = transaction_compound_member_budget_remaining(anchor_state)
        read_keys = Process.get(:tx_current_command_compound_reads, MapSet.new())

        case compound_mutation_batch_work_up_to(items, read_keys, remaining, 0) do
          {:ok, count} ->
            charge_transaction_compound_member_work!(count)

          :limit_exceeded ->
            throw({
              :transaction_store_failure,
              :transaction_compound_mutation_budget_exceeded
            })

          :invalid ->
            throw({:transaction_store_failure, :invalid_compound_mutation_batch})
        end
      end

      defp compound_mutation_batch_work_up_to([], _read_keys, _remaining, count),
        do: {:ok, count}

      defp compound_mutation_batch_work_up_to(
             [item | rest],
             read_keys,
             remaining,
             count
           ) do
        case compound_mutation_key(item) do
          {:ok, key} ->
            if MapSet.member?(read_keys, key) do
              compound_mutation_batch_work_up_to(rest, read_keys, remaining, count)
            else
              if remaining == 0 do
                :limit_exceeded
              else
                compound_mutation_batch_work_up_to(
                  rest,
                  read_keys,
                  remaining - 1,
                  count + 1
                )
              end
            end

          :error ->
            :invalid
        end
      end

      defp compound_mutation_batch_work_up_to(
             _improper_tail,
             _read_keys,
             _remaining,
             _count
           ),
           do: :invalid

      defp compound_mutation_key(key) when is_binary(key), do: {:ok, key}

      defp compound_mutation_key({key, _value, _expire_at_ms}) when is_binary(key),
        do: {:ok, key}

      defp compound_mutation_key(_invalid), do: :error

      defp transaction_result_byte_budget_remaining(anchor_state) do
        max(
          anchor_state.apply_context.transaction_result_byte_budget -
            Process.get(:tx_result_bytes_used, 0),
          0
        )
      end

      defp charge_transaction_result_bytes!(bytes)
           when is_integer(bytes) and bytes >= 0 do
        used = Process.get(:tx_result_bytes_used, 0)
        Process.put(:tx_result_bytes_used, used + bytes)

        precharged = Process.get(:tx_current_command_precharged_bytes, 0)
        Process.put(:tx_current_command_precharged_bytes, precharged + bytes)
        :ok
      end

      defp transaction_compound_scan(
             ctx,
             redis_key,
             prefix,
             member_budget,
             byte_budget,
             projection
           ) do
        with {:ok, compound_keys, member_work} <-
               transaction_compound_prefix_keys(ctx, prefix, member_budget) do
          if projection == :fields do
            transaction_compound_field_entries(
              ctx,
              compound_keys,
              prefix,
              byte_budget,
              member_work
            )
          else
            transaction_compound_value_entries(
              ctx,
              redis_key,
              compound_keys,
              prefix,
              byte_budget,
              projection,
              member_work
            )
          end
        end
      end

      defp transaction_compound_value_entries(
             ctx,
             redis_key,
             compound_keys,
             prefix,
             byte_budget,
             projection,
             member_work
           ) do
        with {:ok, preflight_bytes} <-
               transaction_compound_scan_preflight_bytes(
                 ctx,
                 compound_keys,
                 prefix,
                 projection
               ) do
          if preflight_bytes <= byte_budget do
            previous_reserved = Process.get(:tx_materialization_bytes_reserved, 0)
            previous_precharged = Process.get(:tx_blob_ref_storage_precharged, :undefined)

            Process.put(
              :tx_materialization_bytes_reserved,
              previous_reserved + preflight_bytes
            )

            Process.put(:tx_blob_ref_storage_precharged, true)

            try do
              values = cross_shard_compound_batch_read(ctx, redis_key, compound_keys)
              prefix_size = byte_size(prefix)

              {entries, bytes} =
                Enum.zip_reduce(compound_keys, values, {[], 0}, fn
                  _compound_key, nil, acc ->
                    acc

                  compound_key, value, {entries, bytes} ->
                    field =
                      binary_part(
                        compound_key,
                        prefix_size,
                        byte_size(compound_key) - prefix_size
                      )

                    {
                      [{field, value} | entries],
                      bytes +
                        projected_compound_field_bytes(projection, byte_size(field)) +
                        transaction_result_value_size(value)
                    }
                end)

              if bytes <= byte_budget do
                {:ok, Enum.reverse(entries), member_work, bytes}
              else
                {:error, :byte_limit_exceeded}
              end
            after
              Process.put(:tx_materialization_bytes_reserved, previous_reserved)

              case previous_precharged do
                :undefined -> Process.delete(:tx_blob_ref_storage_precharged)
                value -> Process.put(:tx_blob_ref_storage_precharged, value)
              end
            end
          else
            {:error, :byte_limit_exceeded}
          end
        end
      end

      defp transaction_compound_field_entries(
             ctx,
             compound_keys,
             prefix,
             byte_budget,
             member_work
           ) do
        pending_keys = sm_tx_compound_keys_for_prefix(:tx_pending_compound_keys, prefix)
        pending_values = Process.get(:tx_pending_values, %{})
        expiry_context = ExpiryContext.capture()
        prefix_size = byte_size(prefix)

        compound_keys
        |> Enum.reduce_while({:ok, [], 0}, fn compound_key, {:ok, entries, bytes} ->
          live? =
            if MapSet.member?(pending_keys, compound_key) do
              transaction_pending_member_live?(
                pending_keys,
                pending_values,
                compound_key,
                expiry_context
              )
            else
              cross_shard_ets_exists?(ctx, compound_key, expiry_context)
            end

          if live? do
            field_size = byte_size(compound_key) - prefix_size
            next_bytes = bytes + field_size

            if next_bytes <= byte_budget do
              field = binary_part(compound_key, prefix_size, field_size)
              {:cont, {:ok, [{field, nil} | entries], next_bytes}}
            else
              {:halt, {:error, :byte_limit_exceeded}}
            end
          else
            {:cont, {:ok, entries, bytes}}
          end
        end)
        |> case do
          {:ok, entries, bytes} ->
            {:ok, Enum.reverse(entries), member_work, bytes}

          {:error, _reason} = error ->
            error
        end
      end

      defp transaction_compound_scan_preflight_bytes(
             ctx,
             compound_keys,
             prefix,
             projection
           ) do
        pending_values = Process.get(:tx_pending_values, %{})
        expiry_context = ExpiryContext.capture()
        prefix_size = byte_size(prefix)

        Enum.reduce_while(compound_keys, {:ok, 0}, fn compound_key, {:ok, bytes} ->
          field_bytes =
            projected_compound_field_bytes(
              projection,
              byte_size(compound_key) - prefix_size
            )

          case Map.fetch(pending_values, compound_key) do
            {:ok, {value, expire_at_ms}}
            when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
              case ExpiryContext.classify(expiry_context, expire_at_ms) do
                :live ->
                  {:cont, {:ok, bytes + field_bytes + transaction_result_value_size(value)}}

                :expired ->
                  {:cont, {:ok, bytes}}

                {:unsafe, reason} ->
                  {:halt, {:error, reason}}
              end

            {:ok, invalid} ->
              {:halt, {:error, {:invalid_pending_value, compound_key, invalid}}}

            :error ->
              transaction_compound_scan_preflight_keydir_bytes(
                ctx,
                compound_key,
                field_bytes,
                bytes,
                expiry_context
              )
          end
        end)
      end

      defp transaction_compound_scan_preflight_keydir_bytes(
             ctx,
             compound_key,
             field_bytes,
             bytes,
             expiry_context
           ) do
        case :ets.lookup(ctx.keydir, compound_key) do
          [
            {^compound_key, value, expire_at_ms, _lfu, file_id, offset, value_size}
          ]
          when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
            case ExpiryContext.classify(expiry_context, expire_at_ms) do
              :live ->
                cond do
                  value != nil ->
                    {:cont, {:ok, bytes + field_bytes + transaction_result_value_size(value)}}

                  valid_cross_shard_cold_location_value?(file_id, offset, value_size) ->
                    {:cont, {:ok, bytes + field_bytes + value_size}}

                  true ->
                    {:halt,
                     {:error,
                      {:invalid_cold_location, compound_key, {file_id, offset, value_size}}}}
                end

              :expired ->
                {:cont, {:ok, bytes}}

              {:unsafe, reason} ->
                {:halt, {:error, reason}}
            end

          [] ->
            {:cont, {:ok, bytes}}

          invalid ->
            {:halt, {:error, {:invalid_keydir_entry, compound_key, invalid}}}
        end
      rescue
        ArgumentError -> {:halt, {:error, :keydir_unavailable}}
      end

      defp projected_compound_field_bytes(:values, _field_bytes), do: 0
      defp projected_compound_field_bytes(_projection, field_bytes), do: field_bytes

      defp transaction_result_value_size(value) when is_binary(value), do: byte_size(value)

      defp transaction_result_value_size(value) when is_integer(value),
        do: value |> to_string() |> byte_size()

      defp transaction_result_value_size(value) when is_float(value),
        do: value |> ValueCodec.format_float() |> byte_size()

      defp transaction_result_value_size(value),
        do: value |> :erlang.term_to_binary() |> byte_size()

      defp transaction_compound_count(ctx, prefix, member_budget) do
        case CompoundMemberIndex.count_live_indexed(
               ctx.compound_member_index_name,
               %{keydir: ctx.keydir},
               prefix,
               member_budget
             ) do
          {:ok, base_count, inspected} ->
            pending_keys = sm_tx_compound_keys_for_prefix(:tx_pending_compound_keys, prefix)
            deleted_keys = sm_tx_compound_keys_for_prefix(:tx_deleted_compound_keys, prefix)
            changed_keys = MapSet.union(pending_keys, deleted_keys)

            if MapSet.size(changed_keys) > member_budget - inspected do
              {:error, :limit_exceeded}
            else
              pending_values = Process.get(:tx_pending_values, %{})
              expiry_context = ExpiryContext.capture()

              delta =
                Enum.reduce(changed_keys, 0, fn key, acc ->
                  base_live? = cross_shard_ets_exists?(ctx, key, expiry_context)

                  current_live? =
                    transaction_pending_member_live?(
                      pending_keys,
                      pending_values,
                      key,
                      expiry_context
                    )

                  cond do
                    current_live? and not base_live? -> acc + 1
                    base_live? and not current_live? -> acc - 1
                    true -> acc
                  end
                end)

              {:ok, max(base_count + delta, 0), inspected + MapSet.size(changed_keys)}
            end

          {:error, :limit_exceeded} ->
            {:error, :limit_exceeded}

          {:error, reason} ->
            {:error, reason}

          :unavailable ->
            {:error, :compound_member_index_unavailable}
        end
      end

      defp transaction_pending_member_live?(
             pending_keys,
             pending_values,
             key,
             expiry_context
           ) do
        if MapSet.member?(pending_keys, key) do
          case Map.fetch(pending_values, key) do
            {:ok, {_value, expire_at_ms}}
            when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
              case ExpiryContext.classify(expiry_context, expire_at_ms) do
                :live ->
                  true

                :expired ->
                  false

                {:unsafe, reason} ->
                  record_state_read_failure(reason)
                  false
              end

            {:ok, invalid} ->
              record_state_read_failure({:invalid_pending_value, key, invalid})
              false

            :error ->
              record_state_read_failure({:missing_pending_value, key})
              false
          end
        else
          false
        end
      end

      # Builds a store map for a given shard_idx, usable by Dispatcher.dispatch.
      # For the anchor shard (matching state.shard_index), uses state directly.
      # For remote shards, reads active file info from persistent_term.
      defp build_cross_shard_store(shard_idx, anchor_state) do
        build_transaction_store(shard_idx, anchor_state, :routed)
      end

      defp build_local_raft_tx_store(anchor_state) do
        build_transaction_store(anchor_state.shard_index, anchor_state, :local)
      end

      defp build_transaction_store(shard_idx, anchor_state, routing_mode) do
        instance_ctx = cross_shard_instance_ctx(anchor_state)

        cache_scope =
          case instance_ctx do
            %{name: name} -> name
            _missing_instance_ctx -> :default
          end

        data_dir =
          if instance_ctx do
            instance_ctx.data_dir
          else
            anchor_state.data_dir
          end

        default_ctx = cross_shard_ctx(anchor_state, shard_idx, data_dir, instance_ctx)

        ctx_for_key =
          if routing_mode == :routed and instance_ctx do
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

        validate_value! = fn value ->
          case Ferricstore.Raft.ApplyLimits.validate_value(anchor_state, value) do
            :ok -> :ok
            {:error, reason} -> throw({:transaction_store_failure, reason})
          end
        end

        validate_value_size! = fn size ->
          case Ferricstore.Raft.ApplyLimits.validate_value_size(anchor_state, size) do
            :ok -> :ok
            {:error, reason} -> throw({:transaction_store_failure, reason})
          end
        end

        stage_put_in_ctx = fn ctx, key, value_for, disk_val, pending_value, expire_at_ms ->
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

          queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, value_for)
          cross_shard_transaction_hook({:staged_put, ctx.index, key})
          :ok
        end

        put_in_ctx = fn ctx, key, value, expire_at_ms ->
          :ok = validate_value!.(value)

          case maybe_externalize_cross_shard_value(anchor_state, ctx, value) do
            {:ok, value_for, disk_val, pending_value} ->
              stage_put_in_ctx.(ctx, key, value_for, disk_val, pending_value, expire_at_ms)

            {:error, reason} ->
              throw({:transaction_store_failure, reason})
          end
        end

        batch_put_in_ctx = fn ctx, entries ->
          Enum.each(entries, fn {_key, value, _expire_at_ms} ->
            :ok = validate_value!.(value)
          end)

          case maybe_externalize_cross_shard_entries(anchor_state, ctx, entries) do
            {:ok, prepared} ->
              Enum.each(prepared, fn {key, value_for, disk_val, pending_value, expire_at_ms} ->
                :ok =
                  stage_put_in_ctx.(
                    ctx,
                    key,
                    value_for,
                    disk_val,
                    pending_value,
                    expire_at_ms
                  )
              end)

              :ok

            {:error, reason} ->
              throw({:transaction_store_failure, reason})
          end
        end

        local_put = fn key, value, expire_at_ms ->
          put_in_ctx.(ctx_for_key.(key), key, value, expire_at_ms)
        end

        local_batch_put = fn kv_pairs ->
          kv_pairs
          |> Enum.reduce(%{}, fn {key, value}, groups ->
            ctx = ctx_for_key.(key)
            entry = {key, value, 0}

            Map.update(groups, ctx.index, {ctx, [entry]}, fn {existing_ctx, entries} ->
              {existing_ctx, [entry | entries]}
            end)
          end)
          |> Map.values()
          |> Enum.reduce_while(:ok, fn {ctx, reversed_entries}, :ok ->
            case batch_put_in_ctx.(ctx, Enum.reverse(reversed_entries)) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
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

          sm_tx_mark_deleted(key)
          queue_cross_shard_pending_delete(ctx, key)
          cross_shard_transaction_hook({:staged_delete, ctx.index, key})
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
              :tx_deleted -> nil
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
              :tx_deleted -> nil
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
            case sm_tx_pending_meta(key) do
              {_value, _expire_at_ms} -> true
              :tx_deleted -> false
              nil -> cross_shard_ets_exists?(ctx, key)
            end
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
          {current, expire_at_ms} =
            case local_get_meta.(key) do
              nil -> {0.0, 0}
              {value, expiry} -> {value, expiry}
            end

          with {:ok, float_val} <- coerce_float(current),
               {:ok, new_val} <- ValueCodec.checked_float_add(float_val, delta) do
            case local_put.(key, new_val, expire_at_ms) do
              :ok -> {:ok, new_val}
              {:error, _reason} = error -> error
            end
          else
            :overflow ->
              {:error, "ERR increment would produce NaN or Infinity"}

            :error ->
              {:error, "ERR value is not a valid float"}
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

          new_size =
            Ferricstore.Raft.ApplyLimits.append_size(byte_size(current), byte_size(suffix))

          :ok = validate_value_size!.(new_size)
          new_val = current <> suffix
          local_put.(key, new_val, expire_at_ms)
          {:ok, new_size}
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

          new_size =
            Ferricstore.Raft.ApplyLimits.setrange_size(
              byte_size(old),
              offset,
              byte_size(value)
            )

          :ok = validate_value_size!.(new_size)
          new_val = sm_apply_setrange(old, offset, value)
          local_put.(key, new_val, expire_at_ms)
          {:ok, new_size}
        end

        local_cas = fn key, expected, new_value, ttl_ms ->
          case local_get_meta.(key) do
            nil ->
              nil

            {current, old_expire_at_ms} ->
              if normalize_get_value(current) == expected do
                expire_at_ms =
                  if is_integer(ttl_ms), do: apply_now_ms() + ttl_ms, else: old_expire_at_ms

                case local_put.(key, new_value, expire_at_ms) do
                  :ok -> 1
                  {:error, _reason} = error -> error
                end
              else
                0
              end
          end
        end

        local_lock = fn key, owner, ttl_ms ->
          expire_at_ms = apply_now_ms() + ttl_ms

          case local_get_meta.(key) do
            nil -> local_put.(key, owner, expire_at_ms)
            {^owner, _old_expire_at_ms} -> local_put.(key, owner, expire_at_ms)
            {_other, _old_expire_at_ms} -> {:error, "DISTLOCK lock is held by another owner"}
          end
        end

        local_unlock = fn key, owner ->
          case local_get_meta.(key) do
            nil ->
              1

            {^owner, _expire_at_ms} ->
              :ok = local_delete.(key)
              1

            {_other, _expire_at_ms} ->
              {:error, "DISTLOCK caller is not the lock owner"}
          end
        end

        local_extend = fn key, owner, ttl_ms ->
          case local_get_meta.(key) do
            {^owner, _old_expire_at_ms} ->
              case local_put.(key, owner, apply_now_ms() + ttl_ms) do
                :ok -> 1
                {:error, _reason} = error -> error
              end

            nil ->
              {:error, "DISTLOCK lock does not exist or has expired"}

            {_other, _old_expire_at_ms} ->
              {:error, "DISTLOCK caller is not the lock owner"}
          end
        end

        local_ratelimit_add = fn key, window_ms, limit, count ->
          now_ms = apply_now_ms()

          {current_count, current_start_ms, previous_count} =
            case local_get_meta.(key) do
              {value, _expire_at_ms} -> ValueCodec.decode_ratelimit(value, now_ms)
              nil -> {0, now_ms, 0}
            end

          {current_count, current_start_ms, previous_count} =
            cond do
              now_ms - current_start_ms >= window_ms * 2 -> {0, now_ms, 0}
              now_ms - current_start_ms >= window_ms -> {0, now_ms, current_count}
              true -> {current_count, current_start_ms, previous_count}
            end

          elapsed_ms = now_ms - current_start_ms

          effective_count =
            RateLimit.effective_count(current_count, previous_count, elapsed_ms, window_ms)

          expire_at_ms = current_start_ms + window_ms * 2

          {status, final_count, remaining, stored_count} =
            if effective_count + count > limit do
              {"denied", effective_count, max(0, limit - effective_count), current_count}
            else
              new_count = current_count + count

              {"allowed", effective_count + count, max(0, limit - effective_count - count),
               new_count}
            end

          encoded =
            ValueCodec.encode_ratelimit(stored_count, current_start_ms, previous_count)

          case local_put.(key, encoded, expire_at_ms) do
            :ok ->
              [status, final_count, remaining, max(0, current_start_ms + window_ms - now_ms)]

            {:error, _reason} = error ->
              error
          end
        end

        promoted_target_ctx = fn redis_key, dedicated_path ->
          ctx = ctx_for_key.(redis_key)
          hold_transaction_promotion_latch(ctx, redis_key)
          active = Promotion.find_active(dedicated_path)

          Map.merge(ctx, %{
            active_file_path: active,
            active_file_id: parse_fid_from_path(active)
          })
        end

        promoted_put = fn redis_key, compound_key, value, expire_at_ms, dedicated_path ->
          ctx = promoted_target_ctx.(redis_key, dedicated_path)

          with :ok <- put_in_ctx.(ctx, compound_key, value, expire_at_ms) do
            with :ok <-
                   queue_compound_indexes_put_after_flush(
                     ctx,
                     redis_key,
                     compound_key,
                     value,
                     expire_at_ms
                   ) do
              queue_promoted_revision_put_after_flush(
                ctx.compound_revision_index_name,
                compound_key
              )
            end
          end
        end

        promoted_delete = fn redis_key, compound_key, dedicated_path ->
          ctx = promoted_target_ctx.(redis_key, dedicated_path)

          with :ok <- delete_in_ctx.(ctx, compound_key) do
            with :ok <-
                   queue_compound_indexes_delete_after_flush(ctx, redis_key, compound_key) do
              queue_promoted_revision_delete_after_flush(
                ctx.compound_revision_index_name,
                compound_key
              )
            end
          end
        end

        promoted_put_batch = fn redis_key, entries, dedicated_path ->
          ctx = promoted_target_ctx.(redis_key, dedicated_path)

          with :ok <- batch_put_in_ctx.(ctx, entries) do
            Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
              :ok =
                queue_compound_indexes_put_after_flush(
                  ctx,
                  redis_key,
                  compound_key,
                  value,
                  expire_at_ms
                )

              :ok =
                queue_promoted_revision_put_after_flush(
                  ctx.compound_revision_index_name,
                  compound_key
                )
            end)

            :ok
          end
        end

        promoted_delete_batch = fn redis_key, compound_keys, dedicated_path ->
          ctx = promoted_target_ctx.(redis_key, dedicated_path)

          Enum.each(compound_keys, fn compound_key ->
            :ok = delete_in_ctx.(ctx, compound_key)
            :ok = queue_compound_indexes_delete_after_flush(ctx, redis_key, compound_key)

            :ok =
              queue_promoted_revision_delete_after_flush(
                ctx.compound_revision_index_name,
                compound_key
              )
          end)

          :ok
        end

        stage_promoted_collection_removal = fn
          ctx, redis_key, collection_type, dedicated_path
          when collection_type in ["hash", "set", "zset"] ->
            marker_key = Promotion.marker_key(redis_key)

            type =
              case collection_type do
                "hash" -> :hash
                "set" -> :set
                "zset" -> :zset
              end

            with :ok <- delete_in_ctx.(ctx, marker_key) do
              queue_compound_promotion_removal_after_flush(marker_key)
              queue_promoted_storage_cleanup_after_flush(redis_key, type, dedicated_path)
            end

          _ctx, _redis_key, _collection_type, _dedicated_path ->
            {:error, :invalid_promoted_type_marker}
        end

        %{
          get: local_get,
          cache_scope: cache_scope,
          get_meta: local_get_meta,
          batch_get: fn keys -> cross_shard_routed_batch_read(keys, ctx_for_key) end,
          put: local_put,
          batch_put: local_batch_put,
          delete: local_delete,
          exists?: local_exists,
          incr: local_incr,
          incr_float: local_incr_float,
          append: local_append,
          getset: local_getset,
          getdel: local_getdel,
          getex: local_getex,
          setrange: local_setrange,
          defer_stream_cleanup: &queue_stream_cache_cleanup/1,
          cas: local_cas,
          lock: local_lock,
          unlock: local_unlock,
          extend: local_extend,
          ratelimit_add: local_ratelimit_add,
          compound_get: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_read(ctx, redis_key, compound_key)
          end,
          compound_get_meta: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_read_meta(ctx, redis_key, compound_key)
          end,
          compound_batch_get: fn redis_key, compound_keys ->
            :ok = admit_transaction_compound_batch_read_work!(compound_keys, anchor_state)
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_batch_read(ctx, redis_key, compound_keys)
          end,
          compound_batch_get_meta: fn redis_key, compound_keys ->
            :ok = admit_transaction_compound_batch_read_work!(compound_keys, anchor_state)
            ctx = ctx_for_key.(redis_key)
            cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys)
          end,
          compound_put: fn redis_key, compound_key, value, expire_at_ms ->
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil ->
                with :ok <- put_in_ctx.(ctx, compound_key, value, expire_at_ms) do
                  queue_compound_indexes_put_after_flush(
                    ctx,
                    redis_key,
                    compound_key,
                    value,
                    expire_at_ms
                  )
                end

              dedicated_path ->
                promoted_put.(redis_key, compound_key, value, expire_at_ms, dedicated_path)
            end
          end,
          compound_batch_put: fn redis_key, entries ->
            :ok = admit_transaction_compound_batch_work!(entries, anchor_state)
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_batch_path(ctx, redis_key, entries) do
              :mixed ->
                {:error, :mixed_compound_batch_targets}

              nil ->
                :ok = batch_put_in_ctx.(ctx, entries)

                Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
                  :ok =
                    queue_compound_indexes_put_after_flush(
                      ctx,
                      redis_key,
                      compound_key,
                      value,
                      expire_at_ms
                    )
                end)

                :ok

              dedicated_path when is_binary(dedicated_path) ->
                promoted_put_batch.(redis_key, entries, dedicated_path)
            end
          end,
          compound_delete: fn redis_key, compound_key ->
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil ->
                with :ok <- delete_in_ctx.(ctx, compound_key) do
                  queue_compound_indexes_delete_after_flush(ctx, redis_key, compound_key)
                end

              dedicated_path ->
                collection_type =
                  if compound_key == CompoundKey.type_key(redis_key) do
                    cross_shard_compound_read(ctx, redis_key, compound_key)
                  end

                with :ok <- promoted_delete.(redis_key, compound_key, dedicated_path) do
                  if is_nil(collection_type) do
                    :ok
                  else
                    stage_promoted_collection_removal.(
                      ctx,
                      redis_key,
                      collection_type,
                      dedicated_path
                    )
                  end
                end
            end
          end,
          compound_batch_delete: fn redis_key, compound_keys ->
            :ok = admit_transaction_compound_batch_work!(compound_keys, anchor_state)
            ctx = ctx_for_key.(redis_key)

            case promoted_compound_batch_path(ctx, redis_key, compound_keys) do
              :mixed ->
                {:error, :mixed_compound_batch_targets}

              nil ->
                Enum.each(compound_keys, fn compound_key ->
                  :ok = delete_in_ctx.(ctx, compound_key)
                  :ok = queue_compound_indexes_delete_after_flush(ctx, redis_key, compound_key)
                end)

                :ok

              dedicated_path when is_binary(dedicated_path) ->
                promoted_delete_batch.(redis_key, compound_keys, dedicated_path)
            end
          end,
          compound_scan: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            member_budget = transaction_compound_member_budget_remaining(anchor_state)
            byte_budget = transaction_result_byte_budget_remaining(anchor_state)
            projection = Process.get(:tx_compound_scan_projection, :pairs)

            case transaction_compound_scan(
                   ctx,
                   redis_key,
                   prefix,
                   member_budget,
                   byte_budget,
                   projection
                 ) do
              {:ok, entries, member_work, result_bytes} ->
                charge_transaction_compound_member_work!(member_work)
                charge_transaction_result_bytes!(result_bytes)
                entries

              {:error, :limit_exceeded} ->
                throw({
                  :transaction_store_failure,
                  :transaction_compound_read_budget_exceeded
                })

              {:error, :byte_limit_exceeded} ->
                throw({
                  :transaction_store_failure,
                  :transaction_result_byte_budget_exceeded
                })

              {:error, reason} ->
                throw({:transaction_store_failure, reason})
            end
          end,
          compound_count: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            member_budget = transaction_compound_member_budget_remaining(anchor_state)

            case transaction_compound_count(ctx, prefix, member_budget) do
              {:ok, count, work} ->
                charge_transaction_compound_member_work!(work)
                count

              {:error, :limit_exceeded} ->
                throw({
                  :transaction_store_failure,
                  :transaction_compound_read_budget_exceeded
                })

              {:error, reason} ->
                throw({:transaction_store_failure, reason})
            end
          end,
          compound_delete_prefix: fn redis_key, prefix ->
            ctx = ctx_for_key.(redis_key)
            member_budget = transaction_compound_member_budget_remaining(anchor_state)

            case transaction_compound_prefix_keys(ctx, prefix, member_budget) do
              {:ok, compound_keys, member_work} ->
                charge_transaction_compound_member_work!(member_work)

                case promoted_compound_path(ctx, redis_key, prefix) do
                  nil ->
                    Enum.each(compound_keys, fn key ->
                      :ok = delete_in_ctx.(ctx, key)
                      :ok = queue_compound_indexes_delete_after_flush(ctx, redis_key, key)
                    end)

                    :ok

                  dedicated_path ->
                    promoted_delete_batch.(redis_key, compound_keys, dedicated_path)
                end

              {:error, :limit_exceeded} ->
                throw({:transaction_store_failure, :compound_delete_budget_exceeded})

              {:error, reason} ->
                throw({:transaction_store_failure, reason})
            end
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
          shard_index: default_ctx.index,
          data_dir: data_dir,
          compound_member_apply_budget: anchor_state.apply_context.compound_member_apply_budget,
          transaction_command_budget: anchor_state.apply_context.transaction_command_budget,
          transaction_key_apply_budget: anchor_state.apply_context.transaction_key_apply_budget,
          transaction_result_byte_budget:
            anchor_state.apply_context.transaction_result_byte_budget
        }
      end

      if Mix.env() == :test do
        defp cross_shard_transaction_hook(event) do
          case Application.get_env(:ferricstore, :cross_shard_transaction_hook) do
            hook when is_function(hook, 1) -> hook.(event)
            _missing -> :ok
          end
        end
      else
        defp cross_shard_transaction_hook(_event), do: :ok
      end

      defp queue_promoted_storage_cleanup_after_flush(redis_key, type, dedicated_path)
           when is_binary(redis_key) and type in [:hash, :set, :zset] and
                  is_binary(dedicated_path) do
        pending = Process.get(:sm_pending_promoted_storage_cleanups, %{})

        Process.put(
          :sm_pending_promoted_storage_cleanups,
          Map.put(pending, redis_key, {type, dedicated_path})
        )

        :ok
      end
    end
  end
end
