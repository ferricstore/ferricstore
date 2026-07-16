defmodule Ferricstore.Raft.StateMachine.Sections.FlowValues do
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

      defp flow_after_history_fast_record?(record), do: flow_history_trim_skippable?(record)

      defp flow_after_history_put(state, record) do
        with :ok <- flow_history_trim(state, record) do
          maybe_queue_terminal_lmdb_history_indexes(state, record)
        end
      end

      defp flow_history_trim_oldest(state, record, id, partition_key, history_key, delete_count) do
        events = flow_index_rank_range(state, history_key, 0, delete_count - 1, false)

        with :ok <-
               flow_history_delete_oldest_events(
                 state,
                 record,
                 id,
                 partition_key,
                 history_key,
                 events
               ) do
          events
          |> Enum.map(fn {event_id, _event_ms} -> event_id end)
          |> then(&flow_index_delete_members(state, history_key, &1))
        end
      end

      defp flow_history_delete_oldest_events(
             _state,
             _record,
             _id,
             _partition_key,
             _history_key,
             []
           ),
           do: :ok

      defp flow_history_delete_oldest_events(
             state,
             record,
             id,
             partition_key,
             history_key,
             events
           ) do
        Enum.reduce_while(events, :ok, fn {event_id, event_ms}, :ok ->
          compound_key = FlowKeys.stream_entry_key(id, event_id, partition_key)

          case do_delete(state, compound_key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp maybe_queue_terminal_lmdb_history_indexes(_state, _record), do: :ok

      defp flow_record_expire_at(%{terminal_retention_until_ms: expire_at_ms})
           when is_integer(expire_at_ms) and expire_at_ms > 0,
           do: expire_at_ms

      defp flow_record_expire_at(_record), do: 0

      defp flow_state_record_expire_at(_record), do: 0

      defp flow_encode(record) when is_map(record) do
        record
        |> flow_record_for_storage()
        |> Flow.encode_record()
      end

      defp flow_record_for_storage(%{state: "running"} = record), do: record
      defp flow_record_for_storage(record), do: Map.delete(record, :governance_limit)

      defp flow_governance_release_result(record) when is_map(record) do
        case Map.get(record, :governance_limit) do
          %{
            scope: scope,
            shard_id: shard_id,
            reservation_id: reservation_id,
            enforcement: enforcement
          } = reservation
          when is_binary(scope) and scope != "" and is_integer(shard_id) and shard_id >= 0 and
                 is_binary(reservation_id) and reservation_id != "" and
                 enforcement in [:strict_global, :approximate_global] ->
            {:flow_governance_release, reservation}

          nil ->
            :ok

          _invalid ->
            {:error, "ERR invalid flow governance limit reservation"}
        end
      end

      defp flow_governance_release_results(plans) when is_list(plans) do
        plans
        |> Enum.reduce_while({:ok, []}, fn plan, {:ok, acc} ->
          {record, _next} = flow_claim_plan_pair(plan)

          case flow_governance_release_result(record) do
            {:flow_governance_release, reservation} -> {:cont, {:ok, [reservation | acc]}}
            :ok -> {:cont, {:ok, acc}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
        |> case do
          {:ok, []} -> :ok
          {:ok, releases} -> {:flow_governance_releases, Enum.reverse(releases)}
          {:error, _reason} = error -> error
        end
      end

      defp do_put(state, key, value, expire_at_ms) do
        case Ferricstore.Raft.ApplyLimits.validate_value(state, value) do
          :ok ->
            with :ok <- maybe_clear_compound_data_structure_for_string_put(state, key) do
              case maybe_externalize_apply_value(state, value) do
                {:ok, :value, value} ->
                  raw_put(state, key, value, expire_at_ms)

                {:ok, :blob_ref, encoded_ref, materialized_value} ->
                  raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

                {:error, reason} = error ->
                  record_state_write_failure(reason)
                  error
              end
            end

          {:error, reason} = error ->
            record_state_write_failure(reason)
            error
        end
      end

      defp prepare_apply_blob_command(state, command) do
        ctx = blob_apply_ctx(state)

        if BlobCommand.side_channel_candidate?(ctx, command) do
          case BlobCommand.prepare(ctx, state.shard_index, command, single_member?: true) do
            {:ok, prepared_command} -> {:ok, prepared_command}
            {:error, reason} -> {:error, {:blob_externalize_failed, reason}}
          end
        else
          {:ok, command}
        end
      end

      defp blob_apply_ctx(%{instance_ctx: %{data_dir: data_dir} = ctx}) when is_binary(data_dir),
        do: ctx

      defp blob_apply_ctx(%{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold})
           when is_binary(data_dir) do
        %{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold}
      end

      defp blob_apply_ctx(_state), do: %{blob_side_channel_threshold_bytes: 0}

      defp maybe_externalize_apply_value(state, value) when is_binary(value) do
        ctx = blob_apply_ctx(state)
        threshold = BlobValue.threshold(ctx)

        if flow_inline_blob_value?(threshold, value) do
          {:ok, :value, value}
        else
          case BlobValue.maybe_externalize(
                 Map.get(ctx, :data_dir),
                 state.shard_index,
                 threshold,
                 value
               ) do
            {:ok, ^value} ->
              {:ok, :value, value}

            {:ok, encoded_ref} ->
              {:ok, :blob_ref, encoded_ref, value}

            {:error, reason} ->
              {:error, {:blob_externalize_failed, reason}}
          end
        end
      end

      defp maybe_externalize_apply_value(_state, value), do: {:ok, :value, value}

      defp maybe_externalize_cross_shard_value(anchor_state, ctx, value) when is_binary(value) do
        instance_ctx = Map.get(ctx, :instance_ctx) || Map.get(anchor_state, :instance_ctx)
        threshold = BlobValue.threshold(instance_ctx)

        if flow_inline_blob_value?(threshold, value) do
          {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value),
           value}
        else
          case BlobValue.maybe_externalize(ctx.data_dir, ctx.index, threshold, value) do
            {:ok, ^value} ->
              {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)),
               to_disk_binary(value), value}

            {:ok, encoded_ref} ->
              {:ok, nil, to_disk_binary(encoded_ref), value}

            {:error, reason} ->
              {:error, {:blob_externalize_failed, reason}}
          end
        end
      end

      defp maybe_externalize_cross_shard_value(anchor_state, _ctx, value) do
        {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value),
         value}
      end

      defp maybe_externalize_cross_shard_entries(anchor_state, ctx, entries)
           when is_list(entries) do
        if Enum.all?(entries, fn {_key, value, _expire_at_ms} -> is_binary(value) end) do
          instance_ctx = Map.get(ctx, :instance_ctx) || Map.get(anchor_state, :instance_ctx)
          threshold = BlobValue.threshold(instance_ctx)
          values = Enum.map(entries, fn {_key, value, _expire_at_ms} -> value end)

          case BlobValue.maybe_externalize_many(ctx.data_dir, ctx.index, threshold, values) do
            {:ok, stored_values} ->
              hot_threshold = hot_cache_threshold(anchor_state)

              prepared =
                entries
                |> Enum.zip(stored_values)
                |> Enum.map(fn {{key, value, expire_at_ms}, stored_value} ->
                  value_for =
                    if stored_value == value, do: value_for_ets(value, hot_threshold), else: nil

                  {key, value_for, to_disk_binary(stored_value), value, expire_at_ms}
                end)

              {:ok, prepared}

            {:error, reason} ->
              {:error, {:blob_externalize_failed, reason}}
          end
        else
          Enum.reduce_while(entries, {:ok, []}, fn {key, value, expire_at_ms}, {:ok, acc} ->
            case maybe_externalize_cross_shard_value(anchor_state, ctx, value) do
              {:ok, value_for, disk_value, pending_value} ->
                {:cont, {:ok, [{key, value_for, disk_value, pending_value, expire_at_ms} | acc]}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
          |> case do
            {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
            {:error, _reason} = error -> error
          end
        end
      end

      defp flow_inline_blob_value?(threshold, value) when is_binary(value) do
        size = byte_size(value)
        threshold <= 0 or (size < threshold and not BlobRef.encoded_size?(size))
      end

      defp flow_put_record_values(state, record, attrs) do
        do_flow_put_record_values(state, record, attrs)
      end

      defp do_flow_put_record_values(state, record, attrs) do
        with :ok <- flow_maybe_put_record_value(state, record, attrs, :payload),
             :ok <- flow_maybe_put_record_value(state, record, attrs, :result),
             :ok <- flow_maybe_put_record_value(state, record, attrs, :error) do
          flow_put_named_record_values(state, record, attrs)
        end
      end

      defp flow_maybe_put_record_value(state, record, attrs, kind) do
        if Map.has_key?(attrs, kind) do
          key = Map.fetch!(record, flow_value_ref_field(kind))
          value = Map.fetch!(attrs, kind)

          case BlobCommand.flow_blob_value_ref(value) do
            {:ok, encoded_ref} ->
              flow_put_record_blob_value(state, record, key, encoded_ref)

            :error ->
              with :ok <- flow_validate_key_size(key) do
                raw_put_cold(
                  state,
                  key,
                  Flow.encode_value(value),
                  flow_record_expire_at(record)
                )
              end
          end
        else
          :ok
        end
      end

      defp flow_put_named_record_values(state, record, attrs) do
        values = flow_named_values(Map.get(attrs, :values))

        if map_size(values) == 0 do
          :ok
        else
          refs = flow_record_value_refs(record)

          Enum.reduce_while(values, :ok, fn {name, value}, :ok ->
            case Map.get(refs, name) do
              %{ref: key} when is_binary(key) and key != "" ->
                link_key = flow_shared_value_link_key(record, name, Map.get(refs, name))

                with :ok <- flow_put_named_record_value(state, record, key, value),
                     :ok <- flow_maybe_put_shared_value_link(state, link_key, key, record) do
                  {:cont, :ok}
                else
                  {:error, _reason} = error -> {:halt, error}
                end

              _missing ->
                {:halt, {:error, "ERR flow value #{name} missing ref"}}
            end
          end)
        end
      end

      defp flow_put_named_record_value(state, record, key, value) do
        case BlobCommand.flow_blob_value_ref(value) do
          {:ok, encoded_ref} ->
            flow_put_record_blob_value(state, record, key, encoded_ref)

          :error ->
            with :ok <- flow_validate_key_size(key) do
              raw_put_cold(
                state,
                key,
                Flow.encode_value(value),
                flow_record_expire_at(record)
              )
            end
        end
      end

      defp flow_shared_value_link_key(record, name, %{version: version})
           when is_binary(name) and is_integer(version) do
        FlowKeys.shared_value_link_key(
          Map.fetch!(record, :id),
          name,
          version,
          Map.get(record, :partition_key)
        )
      end

      defp flow_shared_value_link_key(_record, _name, _entry), do: nil

      defp flow_maybe_put_shared_value_link(_state, nil, _ref, _record), do: :ok

      defp flow_maybe_put_shared_value_link(state, link_key, ref, record)
           when is_binary(link_key) and is_binary(ref) do
        with :ok <- flow_validate_key_size(link_key) do
          with :ok <- raw_put_cold(state, link_key, ref, flow_record_expire_at(record)) do
            flow_track_retention_cleanup_key(state, record, link_key)
          end
        end
      end

      defp flow_refresh_terminal_value_expirations(state, record, attrs) do
        flow_refresh_terminal_value_expirations_without_materializing(state, record, attrs)
      end

      defp flow_refresh_terminal_value_expirations_without_materializing(_state, _record, _attrs) do
        # Payload/result/error bytes are separate value/blob records. Terminal state
        # writes must not read those bytes just to refresh TTL: large payloads would
        # turn a metadata transition into a hidden cold-read/materialize path. Newly
        # supplied values are already written above with the terminal record expiry;
        # existing refs keep their original value-retention policy.
        :ok
      end

      defp flow_refresh_record_value_expirations(state, record, attrs) do
        refs =
          [:payload, :result, :error]
          |> Enum.reject(&Map.has_key?(attrs, &1))
          |> Enum.map(fn kind -> Map.get(record, flow_value_ref_field(kind)) end)
          |> Enum.filter(&flow_owned_value_ref?/1)

        values = sm_store_batch_get(state, refs, &sm_file_path/2)
        expire_at_ms = flow_record_expire_at(record)

        refs
        |> Enum.zip(values)
        |> Enum.reduce_while(:ok, fn
          {_key, nil}, :ok ->
            {:cont, :ok}

          {key, value}, :ok when is_binary(value) ->
            with :ok <- flow_validate_key_size(key),
                 :ok <- raw_put_cold(state, key, value, expire_at_ms) do
              {:cont, :ok}
            else
              {:error, _reason} = error -> {:halt, error}
            end

          {_key, _value}, :ok ->
            {:cont, :ok}
        end)
      end

      defp flow_create_put_record_values(state, plans) do
        if flow_create_plans_have_record_values?(plans) do
          Enum.reduce_while(plans, :ok, fn {record, attrs}, :ok ->
            case flow_put_record_values(state, record, attrs) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end)
        else
          :ok
        end
      end

      defp flow_many_put_record_values(state, plans) do
        flow_many_put_record_values(state, plans, :unknown)
      end

      defp flow_many_put_record_values(_state, _plans, false), do: :ok

      defp flow_many_put_record_values(state, plans, true) do
        flow_many_put_record_values_nonempty(state, plans)
      end

      defp flow_many_put_record_values(state, plans, :unknown) do
        if flow_many_plans_have_record_values?(plans) do
          flow_many_put_record_values_nonempty(state, plans)
        else
          :ok
        end
      end

      defp flow_many_put_record_values_nonempty(state, plans) do
        Enum.reduce_while(plans, :ok, fn
          {_record, next, attrs}, :ok ->
            case flow_put_record_values(state, next, attrs) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end

          {_record, next, _history_meta, attrs}, :ok ->
            case flow_put_record_values(state, next, attrs) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
        end)
      end

      defp flow_create_plans_have_record_values?(plans) do
        Enum.any?(plans, fn {_record, attrs} -> flow_attrs_have_record_values?(attrs) end)
      end

      defp flow_many_plans_have_record_values?(plans) do
        Enum.any?(plans, fn
          {_record, _next, attrs} -> flow_attrs_have_record_values?(attrs)
          {_record, _next, _history_meta, attrs} -> flow_attrs_have_record_values?(attrs)
        end)
      end

      defp flow_attrs_have_record_values?(attrs) do
        Map.has_key?(attrs, :payload) or Map.has_key?(attrs, :result) or
          Map.has_key?(attrs, :error) or
          map_size(flow_named_values(Map.get(attrs, :values))) > 0
      end

      defp flow_attrs_record_value_mode(attrs) do
        has_payload? = Map.has_key?(attrs, :payload)
        has_result? = Map.has_key?(attrs, :result)
        has_error? = Map.has_key?(attrs, :error)
        has_named? = map_size(flow_named_values(Map.get(attrs, :values))) > 0

        cond do
          has_payload? and not has_result? and not has_error? and not has_named? -> :payload_only
          has_payload? or has_result? or has_error? or has_named? -> :mixed
          true -> :none
        end
      end

      defp flow_merge_record_value_mode(:mixed, _mode), do: :mixed
      defp flow_merge_record_value_mode(_mode, :mixed), do: :mixed
      defp flow_merge_record_value_mode(:empty, mode), do: mode
      defp flow_merge_record_value_mode(:none, :none), do: :none
      defp flow_merge_record_value_mode(:none, :payload_only), do: :mixed
      defp flow_merge_record_value_mode(:payload_only, :none), do: :mixed
      defp flow_merge_record_value_mode(:none, mode), do: mode
      defp flow_merge_record_value_mode(mode, :none), do: mode
      defp flow_merge_record_value_mode(:payload_only, :payload_only), do: :payload_only

      defp flow_finalize_record_value_mode(:empty), do: :none
      defp flow_finalize_record_value_mode(mode), do: mode

      defp flow_put_state_record(state, key, record) when is_map(record) do
        with :ok <-
               flow_put_state_record_encoded(
                 state,
                 key,
                 flow_encode(record),
                 flow_state_record_expire_at(record),
                 record
               ) do
          flow_enqueue_governance_release_intents(state, [{key, record}])
        end
      end

      defp flow_put_new_state_record(state, key, record) when is_map(record) do
        with :ok <-
               flow_put_state_record_encoded(
                 state,
                 key,
                 flow_encode(record),
                 flow_state_record_expire_at(record),
                 record
               ) do
          flow_enqueue_governance_release_intents(state, [{key, record}])
        end
      end

      defp flow_put_state_record_encoded(state, key, value, expire_at_ms, record) do
        result =
          cond do
            flow_record_has_indexed_attributes?(record) ->
              with :ok <- flow_put_hot(state, key, value, expire_at_ms) do
                maybe_queue_lmdb_indexes_for_state_record(state, key, value, expire_at_ms, record)
                maybe_queue_flow_hibernation_candidate(state, key, record, value)
              end

            Ferricstore.Flow.LMDB.mode() == :lagged and
                Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
              with :ok <- flow_put_hot(state, key, value, expire_at_ms) do
                queue_pending_lmdb_projection_dirty()
              end

            true ->
              with :ok <- flow_put_hot(state, key, value, expire_at_ms) do
                maybe_queue_flow_hibernation_candidate(state, key, record, value)
              end
          end

        with :ok <- result,
             :ok <- flow_put_type_catalog_member(state, key, record),
             :ok <- flow_put_retention_guard(state, record),
             :ok <- flow_track_retention_cleanup_keys(state, record) do
          flow_track_shared_value_refs(state, record)
        end
      end

      defp flow_put_retention_guard(state, %{id: id} = record) when is_binary(id) do
        guard_key = FlowKeys.retention_guard_key(id, Map.get(record, :partition_key))
        guard = Ferricstore.Flow.RetentionGuard.encode(record)
        flow_put_hot_value(state, guard_key, guard, 0)
      end

      defp flow_track_retention_cleanup_keys(state, record) do
        record
        |> flow_record_shared_and_private_value_refs()
        |> Enum.filter(&flow_retention_owned_value_ref?(&1, record))
        |> Enum.uniq()
        |> Enum.reduce_while(:ok, fn owned_key, :ok ->
          case flow_track_retention_cleanup_key(state, record, owned_key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_record_shared_and_private_value_refs(record) do
        direct_refs =
          Enum.map([:payload_ref, :result_ref, :error_ref], &Map.get(record, &1))

        named_refs =
          record
          |> flow_record_value_refs()
          |> Map.values()
          |> Enum.flat_map(fn
            %{ref: ref} when is_binary(ref) -> [ref]
            _entry -> []
          end)

        Enum.filter(direct_refs ++ named_refs, &is_binary/1)
      end

      defp flow_track_retention_cleanup_key(state, %{id: id} = record, owned_key)
           when is_binary(id) and is_binary(owned_key) do
        partition_key = Map.get(record, :partition_key)
        index_key = FlowKeys.retention_cleanup_index_key(id, partition_key)
        member_key = FlowKeys.retention_cleanup_member_key(id, owned_key, partition_key)

        case flow_index_score_of(state, index_key, member_key) do
          :miss ->
            member = Ferricstore.Flow.RetentionCleanupMember.encode(index_key, owned_key)

            with :ok <- flow_put_hot_value(state, member_key, member, 0) do
              flow_index_put_new_members(state, index_key, [{member_key, 0}])
            end

          _present ->
            :ok
        end
      end

      defp flow_track_state_retention_metadata_batch(state, key_records) do
        Enum.reduce_while(key_records, :ok, fn {key, record}, :ok ->
          with :ok <- flow_put_type_catalog_member(state, key, record),
               :ok <- flow_put_retention_guard(state, record),
               :ok <- flow_track_retention_cleanup_keys(state, record),
               :ok <- flow_track_shared_value_refs(state, record) do
            {:cont, :ok}
          else
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_track_shared_value_refs(state, record) do
        refs =
          record
          |> flow_record_shared_value_refs()
          |> Enum.filter(&FlowKeys.shared_value_ref?/1)
          |> Enum.uniq()
          |> Enum.sort()

        case refs do
          [] ->
            :ok

          refs ->
            with :ok <- flow_validate_shared_value_ref_locality(record, refs) do
              registry_key =
                FlowKeys.shared_value_ref_registry_key(
                  Map.fetch!(record, :id),
                  Map.get(record, :partition_key)
                )

              with {:ok, existing} <- flow_shared_value_ref_registry(state, registry_key),
                   :ok <- flow_increment_shared_value_ref_counts(state, refs -- existing) do
                registry = Enum.sort(Enum.uniq(existing ++ refs))

                flow_put_hot_value(
                  state,
                  registry_key,
                  Ferricstore.TermCodec.encode(registry),
                  0
                )
              end
            end
        end
      end

      defp flow_validate_shared_value_ref_locality(record, refs) do
        refs = Enum.filter(refs, &FlowKeys.shared_value_ref?/1)

        state_key =
          FlowKeys.state_key(Map.fetch!(record, :id), Map.get(record, :partition_key))

        state_tag = Router.extract_hash_tag(state_key)

        if Enum.all?(refs, &(Router.extract_hash_tag(&1) == state_tag)) do
          :ok
        else
          {:error, "CROSSSLOT Flow shared value refs must hash to the Flow shard"}
        end
      end

      defp flow_record_shared_value_refs(record) do
        flow_record_shared_and_private_value_refs(record)
      end

      defp flow_shared_value_ref_registry(state, registry_key) do
        case sm_store_batch_get(state, [registry_key], &sm_file_path/2) do
          [value] when is_binary(value) ->
            flow_decode_shared_value_ref_registry(state, registry_key, value)

          _missing ->
            {:ok, []}
        end
      end

      defp flow_decode_shared_value_ref_registry(state, registry_key, value) do
        max_bytes = raft_apply_context(state).max_value_size

        with true <- byte_size(value) <= max_bytes,
             {:ok, refs} when is_list(refs) <- Ferricstore.TermCodec.decode(value) do
          normalized =
            refs
            |> Enum.filter(&FlowKeys.shared_value_ref?/1)
            |> Enum.uniq()
            |> Enum.sort()

          if refs == normalized do
            {:ok, refs}
          else
            {:error, {:invalid_flow_shared_value_ref_registry, registry_key}}
          end
        else
          _invalid -> {:error, {:invalid_flow_shared_value_ref_registry, registry_key}}
        end
      end

      defp flow_increment_shared_value_ref_counts(_state, []), do: :ok

      defp flow_increment_shared_value_ref_counts(state, refs) do
        Enum.reduce_while(refs, :ok, fn ref, :ok ->
          count_key = FlowKeys.shared_value_ref_count_key(ref, state.shard_index)

          with {:ok, count} <- flow_shared_value_ref_count(state, count_key),
               :ok <-
                 flow_put_hot_value(
                   state,
                   count_key,
                   Ferricstore.TermCodec.encode(count + 1),
                   0
                 ) do
            {:cont, :ok}
          else
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_shared_value_ref_count(state, count_key) do
        case sm_store_batch_get(state, [count_key], &sm_file_path/2) do
          [value] when is_binary(value) ->
            if byte_size(value) <= 64 do
              case Ferricstore.TermCodec.decode(value) do
                {:ok, count}
                when is_integer(count) and count > 0 and count < 9_223_372_036_854_775_807 ->
                  {:ok, count}

                _invalid ->
                  {:error, {:invalid_flow_shared_value_ref_count, count_key}}
              end
            else
              {:error, {:invalid_flow_shared_value_ref_count, count_key}}
            end

          _missing ->
            {:ok, 0}
        end
      end

      defp flow_put_hot(state, key, value, expire_at_ms) do
        case maybe_externalize_apply_value(state, value) do
          {:ok, :value, value} ->
            flow_put_hot_value(state, key, value, expire_at_ms)

          {:ok, :blob_ref, encoded_ref, materialized_value} ->
            raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

          {:error, _reason} = error ->
            error
        end
      end

      defp flow_put_hot_value(state, key, value, expire_at_ms) do
        disk_val = to_disk_binary(value)

        if cross_shard_pending_active?() do
          cross_shard_raw_put(state, key, value, disk_val, expire_at_ms, LFU.initial())
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          :ok
        else
          materialize_pending_fast_deletes(state)
          record_pending_original(state, key)

          unless standalone_staged_apply?() do
            track_keydir_binary_delta(state, key, value, expire_at_ms)

            safe_ets_insert(
              state.ets,
              {key, value, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
            )
          end

          queue_pending_put(key, disk_val, expire_at_ms)
          Process.put(:sm_pending_fast_staged_put_batch, true)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

          :ok
        end
      end

      defp raw_put_cold(state, key, value, expire_at_ms) do
        raw_put_cold(state, key, value, expire_at_ms, flow_cold_lfu(value))
      end

      defp raw_put_cold(state, key, value, expire_at_ms, lfu) do
        case maybe_externalize_apply_value(state, value) do
          {:ok, :value, value} ->
            raw_put_cold_value(state, key, value, expire_at_ms, lfu)

          {:ok, :blob_ref, encoded_ref, materialized_value} ->
            raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, lfu)

          {:error, _reason} = error ->
            error
        end
      end

      defp raw_put_cold_value(state, key, value, expire_at_ms, lfu) do
        disk_val = to_disk_binary(value)

        if cross_shard_pending_active?() do
          ets_val = nil
          cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu)
        else
          materialize_pending_fast_deletes(state)
          record_pending_original(state, key)

          unless standalone_staged_apply?() do
            track_keydir_binary_delta(state, key, nil, expire_at_ms)

            safe_ets_insert(
              state.ets,
              {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
            )
          end

          queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
          Process.put(:sm_pending_fast_staged_put_batch, true)
          :ok
        end
      end

      defp cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu) do
        ctx = cross_shard_pending_ctx(state)
        record_cross_shard_pending_original(ctx, key)

        track_keydir_binary_delta_for_keydir(
          state,
          ctx.keydir,
          ctx.index,
          key,
          ets_val,
          expire_at_ms
        )

        :ets.insert(
          ctx.keydir,
          {key, ets_val, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        )

        queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, ets_val)
        :ok
      end

      defp cross_shard_pending_ctx(state) do
        %{
          keydir: state.ets,
          index: state.shard_index,
          active_file_path: state.active_file_path,
          active_file_id: state.active_file_id
        }
      end

      defp cross_shard_pending_active? do
        is_list(Process.get(:sm_cross_shard_pending_writes, :undefined))
      end

      defp track_keydir_binary_delta_for_keydir(
             state,
             keydir,
             shard_index,
             key,
             new_value,
             new_expire_at_ms
           ) do
        ref = keydir_binary_ref(state)
        previous = :ets.lookup(keydir, key)

        ExpiryTracker.adjust(
          expiry_instance_ctx(state),
          shard_index,
          ExpiryTracker.entry_expire_at(previous),
          new_expire_at_ms
        )

        if ref do
          new_bytes = binary_byte_size(key) + binary_byte_size(new_value)

          old_bytes =
            case previous do
              [{^key, old_val, _, _, _, _, _}] ->
                binary_byte_size(key) + binary_byte_size(old_val)

              _ ->
                0
            end

          delta = new_bytes - old_bytes
          if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
        end
      end

      defp flow_record_lfu(%{version: version}, _value) when is_integer(version) do
        {:flow_state_version, version, LFU.initial()}
      end

      defp flow_record_lfu(_record, value), do: flow_cold_lfu(value)

      defp flow_cold_lfu(value) when is_binary(value) do
        if Flow.record_blob?(value) do
          case flow_decode_record_blob(value) do
            {:ok, %{version: version}} when is_integer(version) ->
              {:flow_state_version, version, LFU.initial()}

            _ ->
              LFU.initial()
          end
        else
          LFU.initial()
        end
      end

      defp flow_cold_lfu(_value), do: LFU.initial()

      defp raw_put(state, key, value, expire_at_ms) do
        ets_val = value_for_ets(value, hot_cache_threshold(state))
        disk_val = to_disk_binary(value)

        if cross_shard_pending_active?() do
          cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, LFU.initial())
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          :ok
        else
          materialize_pending_fast_deletes(state)
          record_pending_original(state, key)

          unless standalone_staged_apply?() do
            # Track binary memory: subtract old entry's bytes, add new entry's bytes.
            # This gives MemoryGuard accurate off-heap binary accounting.
            track_keydir_binary_delta(state, key, ets_val, expire_at_ms)

            # Insert into ETS immediately so subsequent read-modify-write commands
            # (INCR, APPEND, etc.) in the same batch see the correct value.
            # The file_id is :pending — flush_pending_writes will update it with
            # the real offset after the batch NIF call.
            :ets.insert(
              state.ets,
              {key, ets_val, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
            )
          end

          # Accumulate for one storage append, then publish real locations before
          # the replicated apply returns.
          queue_pending_put(key, disk_val, expire_at_ms)
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

          :ok
        end
      end

      defp do_set(state, key, value, expire_at_ms, opts) do
        with {:ok, owner_state, owner_record} <-
               flow_validate_retention_owned_write(state, key, opts) do
          do_set_validated(
            state,
            key,
            value,
            expire_at_ms,
            opts,
            owner_state,
            owner_record
          )
        end
      end

      defp do_set_validated(
             state,
             key,
             value,
             expire_at_ms,
             opts,
             owner_state,
             owner_record
           ) do
        compound_data_structure? = compound_data_structure_key?(state, key)
        get? = Map.get(opts, :get, false)
        current = set_current_meta(state, key, get?)
        exists? = current != nil or compound_data_structure?

        {old_value, old_expire_at_ms} =
          case current do
            nil -> {nil, expire_at_ms}
            {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
          end

        skip? =
          cond do
            Map.get(opts, :nx, false) and exists? -> true
            Map.get(opts, :xx, false) and not exists? -> true
            true -> false
          end

        cond do
          compound_data_structure? and get? ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

          skip? and get? ->
            old_value

          skip? ->
            nil

          true ->
            effective_expire_at_ms =
              if Map.get(opts, :keepttl, false) and exists? do
                old_expire_at_ms
              else
                expire_at_ms
              end

            with :ok <- do_put(state, key, value, effective_expire_at_ms),
                 :ok <-
                   flow_maybe_track_retention_owned_key(
                     owner_state,
                     owner_record,
                     key
                   ) do
              if get?, do: old_value, else: :ok
            end
        end
      end

      defp flow_validate_retention_owned_write(state, key, opts) do
        case Map.get(opts, :flow_retention_owner) do
          nil ->
            {:ok, nil, nil}

          %{
            id: id,
            partition_key: partition_key,
            state_key: state_key,
            expected_guard: expected_guard
          }
          when is_binary(id) and (is_binary(partition_key) or is_nil(partition_key)) and
                 is_binary(state_key) and is_binary(expected_guard) ->
            if Router.extract_hash_tag(key) == Router.extract_hash_tag(state_key) do
              guard_key = FlowKeys.retention_guard_key(id, partition_key)

              if flow_retention_current_guard(state, guard_key) == expected_guard do
                {:ok, state, %{id: id, partition_key: partition_key}}
              else
                {:error, "ERR stale flow governance owner"}
              end
            else
              {:error, "CROSSSLOT Flow-owned keys must hash to the owner shard"}
            end

          _invalid_owner ->
            {:error, "ERR invalid flow retention owner"}
        end
      end

      defp flow_maybe_track_retention_owned_key(nil, nil, _key), do: :ok

      defp flow_maybe_track_retention_owned_key(owner_state, owner_record, key) do
        flow_track_retention_cleanup_key(owner_state, owner_record, key)
      end

      defp set_current_meta(state, key, true), do: do_get_meta(state, key)

      defp set_current_meta(state, key, false) do
        case plain_expire_at_ms(state, key) do
          nil -> nil
          expire_at_ms -> {nil, expire_at_ms}
        end
      end

      defp plain_expire_at_ms(state, key) do
        now = apply_now_ms()

        case :ets.lookup(state.ets, key) do
          [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
            0

          [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            0

          [{^key, nil, 0, _lfu, fid, off, vsize}]
          when valid_waraft_segment_location(fid, off, vsize) ->
            0

          [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
            exp

          [{^key, nil, exp, _lfu, fid, off, vsize}]
          when exp > now and valid_cold_location(fid, off, vsize) ->
            exp

          [{^key, nil, exp, _lfu, fid, off, vsize}]
          when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
            exp

          [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
            track_keydir_binary_remove_known(state, key, value)
            :ets.delete(state.ets, key)
            nil

          [] ->
            nil
        end
      end

      defp do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
        redis_key = CompoundKey.extract_redis_key(key)

        case check_fetch_or_compute_lock(state, redis_key, nil) do
          :ok -> do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp do_checked_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms) do
        redis_key = CompoundKey.extract_redis_key(key)

        case check_fetch_or_compute_lock(state, redis_key, nil) do
          :ok -> do_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp do_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
        with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref),
             :ok <- maybe_clear_compound_data_structure_for_string_put(state, key) do
          raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized)
        end
      end

      defp do_put_blob_ref_ref_only(state, key, encoded_ref, expire_at_ms) do
        with {:ok, ref} <- decode_blob_ref(encoded_ref),
             :ok <- Ferricstore.Raft.ApplyLimits.validate_value_size(state, ref.size),
             :ok <- verify_blob_refs_for_apply(state, [ref]) do
          do_put_blob_ref_ref_only_validated(state, key, encoded_ref, expire_at_ms)
        end
      end

      defp do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) do
        redis_key = CompoundKey.extract_redis_key(key)

        case check_fetch_or_compute_lock(state, redis_key, nil) do
          :ok -> do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
          {:error, :key_locked} -> {:error, :key_locked}
        end
      end

      defp do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) when is_map(opts) do
        compound_data_structure? = compound_data_structure_key?(state, key)
        get? = Map.get(opts, :get, false)
        current = set_current_meta(state, key, get?)
        exists? = current != nil or compound_data_structure?

        {old_value, old_expire_at_ms} =
          case current do
            nil -> {nil, expire_at_ms}
            {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
          end

        skip? =
          cond do
            Map.get(opts, :nx, false) and exists? -> true
            Map.get(opts, :xx, false) and not exists? -> true
            true -> false
          end

        cond do
          compound_data_structure? and get? ->
            {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

          skip? and get? ->
            old_value

          skip? ->
            nil

          true ->
            effective_expire_at_ms =
              if Map.get(opts, :keepttl, false) and exists? do
                old_expire_at_ms
              else
                expire_at_ms
              end

            case do_put_blob_ref(state, key, encoded_ref, effective_expire_at_ms) do
              :ok -> if get?, do: old_value, else: :ok
              {:error, _reason} = error -> error
            end
        end
      end

      defp do_getset_blob_ref(state, key, encoded_ref) do
        with :ok <- ensure_string_key(state, key),
             {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
          old = do_get(state, key)
          raw_put_blob_ref(state, key, encoded_ref, 0, materialized)
          old
        end
      end

      defp do_append_blob_ref(state, key, encoded_ref) do
        with {:ok, suffix} <- materialize_blob_ref(state, encoded_ref) do
          do_append(state, key, suffix)
        end
      end

      defp do_setrange_blob_ref(state, key, offset, encoded_ref) do
        with {:ok, value} <- materialize_blob_ref(state, encoded_ref) do
          do_setrange(state, key, offset, value)
        end
      end

      defp do_cas_blob_ref(state, key, expected, encoded_ref, expire_at_ms) do
        case ets_lookup(state, key) do
          {:hit, ^expected, old_exp} ->
            with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
              expire = if expire_at_ms, do: expire_at_ms, else: old_exp
              raw_put_blob_ref(state, key, encoded_ref, expire, materialized)
              1
            end

          {:hit, _other, _exp} ->
            0

          :expired ->
            nil

          :miss ->
            nil
        end
      end

      defp do_fetch_or_compute_publish_blob_ref(
             state,
             key,
             encoded_ref,
             expire_at_ms,
             owner_ref
           ) do
        with :ok <- check_fetch_or_compute_lock(state, key, owner_ref) do
          case with_pending_writes(state, fn ->
                 do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
               end) do
            :ok -> do_release_fetch_or_compute_locks(state, [key], owner_ref)
            {:error, _reason} = error -> {state, error}
            other -> {state, {:error, {:invalid_fetch_or_compute_write_result, other}}}
          end
        else
          {:error, _reason} = error -> {state, error}
        end
      end

      defp do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms) do
        redis_key = CompoundKey.extract_redis_key(compound_key)

        case check_fetch_or_compute_lock(state, redis_key, nil) do
          :ok ->
            do_compound_put_blob_ref(state, redis_key, compound_key, encoded_ref, expire_at_ms)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end
      end
    end
  end
end
