defmodule Ferricstore.Raft.StateMachine.Sections.CrossShardPending do
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
      alias Ferricstore.Store.Shard.LogicalKeyIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp apply_cross_shard_pending_locations(keydir, file_id, entries, locations) do
        Enum.zip(entries, locations)
        |> only_latest_cross_shard_entries()
        |> Enum.each(fn
          {{:put, _idx, ^keydir, _file_path, ^file_id, key, ets_value, _disk_value, exp},
           {:put, offset, value_size}} ->
            try do
              :ets.select_replace(keydir, [
                {
                  {key, ets_value, exp, :"$1", :pending, :_, :_},
                  [],
                  [{{key, ets_value, exp, :"$1", file_id, offset, value_size}}]
                }
              ])
            rescue
              ArgumentError -> :ok
            end

          {{:delete, _idx, ^keydir, _file_path, ^file_id, _key}, {:delete, _offset, _record_size}} ->
            :ok
        end)
      end

      defp publish_cross_shard_pending_groups(state, successful_groups) do
        ref = keydir_binary_ref(state)

        Enum.each(successful_groups, fn {_idx, _file_path, file_id, keydir, entries, locations} ->
          publish_cross_shard_pending_locations(
            state,
            ref,
            keydir,
            file_id,
            entries,
            locations
          )
        end)
      end

      defp publish_cross_shard_pending_locations(
             state,
             ref,
             keydir,
             file_id,
             entries,
             locations
           ) do
        Enum.zip(entries, locations)
        |> only_latest_cross_shard_entries()
        |> Enum.each(fn
          {{:put, idx, ^keydir, _file_path, ^file_id, key, ets_value, _disk_value, exp},
           {:put, offset, value_size}} ->
            track_cross_shard_keydir_binary_publish(ref, keydir, idx, key, ets_value)
            :ets.insert(keydir, {key, ets_value, exp, LFU.initial(), file_id, offset, value_size})
            {logical_keys, logical_slots} = logical_key_index_tables(state, idx)
            :ok = LogicalKeyIndex.put(logical_keys, logical_slots, key, ets_value, exp)

          {{:delete, idx, ^keydir, _file_path, ^file_id, key}, {:delete, _offset, _record_size}} ->
            track_cross_shard_keydir_binary_publish(ref, keydir, idx, key, nil)
            :ets.delete(keydir, key)
            {logical_keys, logical_slots} = logical_key_index_tables(state, idx)
            :ok = LogicalKeyIndex.delete(logical_keys, logical_slots, key)
        end)
      end

      defp logical_key_index_tables(state, shard_index) do
        LogicalKeyIndex.table_names(Map.get(state, :instance_name, :default), shard_index)
      end

      defp track_cross_shard_keydir_binary_publish(nil, _keydir, _shard_index, _key, _new_value),
        do: :ok

      defp track_cross_shard_keydir_binary_publish(ref, keydir, shard_index, key, new_value) do
        current_bytes =
          case :ets.lookup(keydir, key) do
            [{^key, value, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(value)
            _ -> 0
          end

        new_bytes =
          if is_nil(new_value) do
            0
          else
            binary_byte_size(key) + binary_byte_size(new_value)
          end

        delta = new_bytes - current_bytes
        if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
      end

      defp only_latest_cross_shard_entries(entry_locations) do
        latest =
          entry_locations
          |> Enum.with_index()
          |> Enum.reduce(%{}, fn {{entry, _location}, idx}, acc ->
            Map.put(acc, cross_shard_entry_identity(entry), idx)
          end)

        entry_locations
        |> Enum.with_index()
        |> Enum.flat_map(fn {{entry, location}, idx} ->
          if Map.fetch!(latest, cross_shard_entry_identity(entry)) == idx do
            [{entry, location}]
          else
            []
          end
        end)
      end

      defp cross_shard_entry_identity(
             {:put, _idx, keydir, _file_path, _file_id, key, _ets_value, _disk_value,
              _expire_at_ms}
           ),
           do: {keydir, key}

      defp cross_shard_entry_identity({:delete, _idx, keydir, _file_path, _file_id, key}),
        do: {keydir, key}

      defp compensate_cross_shard_partial_writes(state, successful_groups, originals) do
        Enum.reduce_while(successful_groups, {:ok, state}, fn group, {:ok, acc_state} ->
          {idx, file_path, file_id, keydir, entries} = cross_shard_successful_group_parts(group)

          case cross_shard_compensation_batch(
                 acc_state,
                 idx,
                 keydir,
                 file_path,
                 entries,
                 originals
               ) do
            {:ok, []} ->
              {:cont, {:ok, acc_state}}

            {:ok, compensation_batch} ->
              append_result =
                file_path
                |> append_pending_batch(
                  compensation_batch,
                  batch_contains_delete?(compensation_batch)
                )
                |> then(&validate_append_result(compensation_batch, &1))

              case append_result do
                {:ok, _locations} ->
                  compensated_state =
                    acc_state
                    |> track_cross_shard_append_bytes(
                      idx,
                      file_path,
                      file_id,
                      bitcask_record_bytes(compensation_batch)
                    )
                    |> mark_cross_shard_checkpoint_dirty(idx)

                  {:cont, {:ok, compensated_state}}

                {:error, reason} ->
                  compensated_state = mark_cross_shard_checkpoint_dirty(acc_state, idx)
                  {:halt, {:error, {:compensation_append_failed, reason}, compensated_state}}
              end

            {:error, reason} ->
              compensated_state = mark_cross_shard_checkpoint_dirty(acc_state, idx)
              {:halt, {:error, reason, compensated_state}}
          end
        end)
      end

      defp cross_shard_successful_group_parts({idx, file_path, file_id, keydir, entries}),
        do: {idx, file_path, file_id, keydir, entries}

      defp cross_shard_successful_group_parts(
             {idx, file_path, file_id, keydir, entries, _locations}
           ),
           do: {idx, file_path, file_id, keydir, entries}

      defp cross_shard_compensation_batch(state, idx, keydir, file_path, entries, originals) do
        entries
        |> Enum.map(&cross_shard_pending_key/1)
        |> Enum.uniq()
        |> Enum.reduce_while({:ok, []}, fn key, {:ok, acc} ->
          original = Map.get(originals, {keydir, key}, {idx, :missing})

          case cross_shard_compensation_batch_entry(state, key, original, file_path) do
            {:ok, batch_entries} -> {:cont, {:ok, [batch_entries | acc]}}
            {:error, reason} -> {:halt, {:error, reason}}
          end
        end)
        |> case do
          {:ok, batches} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
          {:error, reason} -> {:error, reason}
        end
      end

      defp cross_shard_pending_key(
             {:put, _idx, _keydir, _file_path, _file_id, key, _ets, _disk, _exp}
           ),
           do: key

      defp cross_shard_pending_key({:delete, _idx, _keydir, _file_path, _file_id, key}), do: key

      defp cross_shard_compensation_batch_entry(_state, key, {_idx, :missing}, _file_path) do
        {:ok, [{:delete, key, nil}]}
      end

      defp cross_shard_compensation_batch_entry(
             _state,
             key,
             {_idx,
              {:entry, {original_key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}}},
             _file_path
           )
           when original_key == key and is_binary(value) do
        {:ok, [{:put, key, value, expire_at_ms}]}
      end

      defp cross_shard_compensation_batch_entry(
             state,
             key,
             {idx,
              {:entry, {original_key, nil, expire_at_ms, _lfu, file_id, offset, _value_size}}},
             file_path
           )
           when original_key == key do
        case cross_shard_compensation_read(state, idx, file_path, file_id, offset, key) do
          {:ok, value} when is_binary(value) ->
            {:ok, [{:put, key, value, expire_at_ms}]}

          {:error, reason} ->
            {:error, {:compensation_read_failed, key, reason}}

          other ->
            {:error, {:compensation_read_failed, key, other}}
        end
      end

      defp cross_shard_compensation_batch_entry(_state, key, original, _file_path),
        do: {:error, {:compensation_original_mismatch, key, original}}

      defp cross_shard_compensation_read(_state, _idx, file_path, file_id, offset, key)
           when is_integer(file_id) and file_id >= 0 do
        shard_data_path = Path.dirname(file_path)
        old_path = sm_file_path_from_path(shard_data_path, file_id)
        ColdRead.pread_keyed(old_path, offset, key, @cold_read_timeout_ms)
      end

      defp cross_shard_compensation_read(state, idx, _file_path, file_id, _offset, key)
           when is_tuple(file_id) do
        ctx = cross_shard_compensation_reader_ctx(state)

        Ferricstore.Raft.WARaftSegmentReader.read_value_from_location_including_expired(
          ctx,
          idx,
          file_id,
          key
        )
      end

      defp cross_shard_compensation_read(_state, _idx, _file_path, file_id, _offset, _key),
        do: {:error, {:invalid_file_id, file_id}}

      defp cross_shard_compensation_reader_ctx(%{instance_ctx: %{data_dir: data_dir}})
           when is_binary(data_dir),
           do: %{data_dir: data_dir}

      defp cross_shard_compensation_reader_ctx(%{data_dir: data_dir}) when is_binary(data_dir),
        do: %{data_dir: data_dir}

      defp record_cross_shard_pending_original(ctx, key) do
        originals = Process.get(:sm_cross_shard_pending_originals, %{})
        original_key = {ctx.keydir, key}

        if Map.has_key?(originals, original_key) do
          :ok
        else
          original =
            case :ets.lookup(ctx.keydir, key) do
              [entry] -> {:entry, entry}
              [] -> :missing
            end

          Process.put(
            :sm_cross_shard_pending_originals,
            Map.put(originals, original_key, {ctx.index, original})
          )
        end
      end

      defp queue_cross_shard_pending_put(ctx, key, disk_value, expire_at_ms, ets_value) do
        pending = Process.get(:sm_cross_shard_pending_writes, [])

        Process.put(:sm_cross_shard_pending_writes, [
          {:put, ctx.index, ctx.keydir, ctx.active_file_path, ctx.active_file_id, key, ets_value,
           disk_value, expire_at_ms}
          | pending
        ])
      end

      defp queue_cross_shard_pending_delete(ctx, key) do
        pending = Process.get(:sm_cross_shard_pending_writes, [])
        Process.put(:sm_pending_has_delete, true)

        Process.put(:sm_cross_shard_pending_writes, [
          {:delete, ctx.index, ctx.keydir, ctx.active_file_path, ctx.active_file_id, key}
          | pending
        ])
      end

      defp rollback_cross_shard_pending_writes(state) do
        ref = keydir_binary_ref(state)

        Process.get(:sm_cross_shard_pending_originals, %{})
        |> Enum.each(fn
          {{keydir, key}, {shard_index, {:entry, entry}}} ->
            track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, entry)
            safe_ets_insert(keydir, entry)

          {{keydir, key}, {shard_index, :missing}} ->
            track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, nil)
            safe_ets_delete(keydir, key)
        end)
      end

      defp track_cross_shard_keydir_binary_restore(nil, _keydir, _shard_index, _key, _entry),
        do: :ok

      defp track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, original_entry) do
        current_bytes = keydir_entry_binary_bytes(key, safe_ets_lookup(keydir, key))

        original_bytes =
          keydir_entry_binary_bytes(key, if(original_entry, do: [original_entry], else: []))

        delta = original_bytes - current_bytes
        if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
      end

      # Wraps a block of state machine operations with batched disk writes.
      # Initializes the pending-writes buffer, runs the block, then flushes
      # all accumulated writes in one no-sync NIF call.
      # If the append fails, restores any ETS entries that were replaced with
      # :pending locations and returns the disk error instead of acknowledging
      # success to the caller.
      defp with_pending_writes(state, fun) do
        init_pending_write_process_state(state)
        started_at = System.monotonic_time()

        try do
          command_result = fun.()

          result = state_storage_failure_result(command_result)

          if pending_write_error_result?(result) do
            rollback_pending_writes(state)
            emit_raft_apply_telemetry(state, started_at, result, :rolled_back)
            result
          else
            flush_result = flush_pending_writes(state)
            emit_raft_apply_telemetry(state, started_at, result, flush_result)

            case flush_result do
              :ok ->
                publish_pending_compound_revisions(state)
                dispatch_pending_compound_promotions(state)

                case publish_pending_flow_history_projections(state) do
                  :ok ->
                    result

                  {:error, reason} ->
                    handle_flow_history_projection_publish_failure(state, reason)
                    result
                end

              {:error, _reason} = error ->
                error
            end
          end
        rescue
          error ->
            rollback_pending_writes(state)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            rollback_pending_writes(state)
            :erlang.raise(kind, reason, __STACKTRACE__)
        after
          clear_pending_write_process_state()
        end
      end

      defp init_pending_write_process_state(state) do
        Enum.each(@sm_pending_write_initial_values, fn {key, value} ->
          Process.put(key, value)
        end)

        Process.put(:sm_pending_lmdb_mirror_default_shard, Map.get(state, :shard_index, 0))
      end

      defp clear_pending_write_process_state do
        Enum.each(@sm_pending_write_keys, &Process.delete/1)
      end

      defp dispatch_pending_compound_promotions(state) do
        case promotion_shard_pid(state) do
          pid when is_pid(pid) ->
            Process.get(:sm_pending_promoted_maintenance, %{})
            |> Enum.each(fn {redis_key, maintenance} ->
              send(pid, {:promoted_maintenance_after_commit, redis_key, maintenance})
            end)

            Process.get(:sm_pending_compound_promotion_removals, MapSet.new())
            |> Enum.each(fn redis_key ->
              send(pid, {:remove_promoted_after_commit, redis_key})
            end)

            Process.get(:sm_pending_compound_promotions, %{})
            |> Enum.each(fn {redis_key, {compound_key, threshold}} ->
              send(pid, {:maybe_promote_after_commit, redis_key, compound_key, threshold})
            end)

          nil ->
            :ok
        end

        :ok
      end

      defp publish_pending_compound_revisions(state) do
        table = Map.get(state, :compound_revision_index_name)

        Process.get(:sm_pending_compound_revision_ops, [])
        |> Enum.reverse()
        |> Enum.each(fn
          {:put, key, revision} ->
            Ferricstore.Store.Shard.CompoundRevisionIndex.put(table, key, revision)

          {:delete, key} ->
            Ferricstore.Store.Shard.CompoundRevisionIndex.delete(table, key)
        end)

        :ok
      end

      defp promotion_shard_pid(state) do
        ctx =
          case Map.get(state, :instance_ctx) do
            %FerricStore.Instance{} = instance_ctx ->
              instance_ctx

            _missing ->
              FerricStore.Instance.get(Map.get(state, :instance_name, :default))
          end

        shard_index = Map.get(state, :shard_index)

        case ctx do
          %FerricStore.Instance{shard_names: shard_names}
          when is_tuple(shard_names) and is_integer(shard_index) and shard_index >= 0 and
                 shard_index < tuple_size(shard_names) ->
            case Router.shard_name(ctx, shard_index) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: pid
              name when is_atom(name) -> Process.whereis(name)
              _invalid_name -> nil
            end

          _invalid_context ->
            nil
        end
      rescue
        _error -> nil
      catch
        :exit, _reason -> nil
      end

      defp pending_write_error_result?({:error, _reason}), do: true
      defp pending_write_error_result?({:error, _reason, _state}), do: true
      defp pending_write_error_result?(_result), do: false

      defp record_state_read_failure(reason) do
        case Process.get(:sm_state_read_failure, :outside_apply) do
          nil -> Process.put(:sm_state_read_failure, reason)
          _first_failure_or_outside_apply -> :ok
        end

        :miss
      end

      defp record_state_write_failure(reason) do
        case Process.get(:sm_state_write_failure, :outside_apply) do
          nil -> Process.put(:sm_state_write_failure, reason)
          _first_failure_or_outside_apply -> :ok
        end

        :ok
      end

      defp state_storage_failure_result(command_result) do
        case Process.get(:sm_state_read_failure) do
          nil ->
            case Process.get(:sm_state_write_failure) do
              nil -> command_result
              reason -> {:error, reason}
            end

          reason ->
            {:error, {:state_read_failed, reason}}
        end
      end

      defp do_flow_create(state, %{id: id} = attrs) do
        partition_key = Map.get(attrs, :partition_key)
        state_key = FlowKeys.state_key(id, partition_key)

        case flow_create_existing_state(state, attrs, state_key) do
          nil ->
            flow_create_apply_new_record(state, attrs, state_key)

          existing ->
            case flow_create_duplicate_result(state, existing, attrs) do
              {:ok, _existing} -> :ok
              {:error, _reason} = error -> error
            end
        end
      end

      defp do_flow_start_and_claim(state, %{id: id} = attrs) do
        partition_key = Map.get(attrs, :partition_key)
        state_key = FlowKeys.state_key(id, partition_key)

        case flow_create_existing_state(state, attrs, state_key) do
          nil -> flow_start_and_claim_apply_new_record(state, attrs, state_key)
          _existing -> {:error, "ERR flow already exists"}
        end
      end

      defp do_flow_named_value_put(
             state,
             %{id: id, name: name, value: value, partition_key: partition_key} = attrs
           )
           when is_binary(name) and name != "" do
        now_ms = flow_attrs_now_ms(attrs)

        with {:ok, record} <- flow_require_record(state, id, partition_key) do
          refs = flow_record_value_refs(record)
          encoded_value = flow_named_value_put_encoded_value(value)
          digest = flow_named_value_put_digest(value, encoded_value)
          existing = Map.get(refs, name)
          override? = Map.get(attrs, :override, false)

          cond do
            flow_named_value_same_digest?(existing, digest) ->
              flow_named_value_put_result(attrs, %{
                ref: Map.fetch!(existing, :ref),
                partition_key: partition_key,
                owner_flow_id: id,
                name: name,
                version: Map.get(existing, :version),
                created: false,
                stored: false
              })

            not is_nil(existing) and not override? ->
              {:error,
               "ERR flow value #{name} already exists with different digest; use OVERRIDE true"}

            true ->
              version = Map.fetch!(record, :version) + 1

              flow_named_value_put_store(
                state,
                record,
                attrs,
                refs,
                existing,
                id,
                name,
                value,
                encoded_value,
                digest,
                partition_key,
                now_ms,
                version,
                override?
              )
          end
        end
      end

      defp flow_named_value_put_encoded_value(value) do
        case BlobCommand.flow_blob_value_ref(value) do
          {:ok, _encoded_ref} -> :generic
          :error -> {:ok, Flow.encode_value(value)}
        end
      end

      defp flow_named_value_put_digest(_value, {:ok, encoded_value}),
        do: flow_value_digest_encoded(encoded_value)

      defp flow_named_value_put_digest(value, :generic), do: flow_value_digest(value)

      defp flow_named_value_put_store(
             state,
             record,
             attrs,
             refs,
             existing,
             id,
             name,
             _value,
             {:ok, encoded_value},
             digest,
             partition_key,
             now_ms,
             version,
             _override?
           ) do
        value_version = flow_named_value_next_version(existing)
        ref = FlowKeys.named_shared_value_key(id, name, value_version, partition_key)
        entry = %{ref: ref, version: value_version, digest: digest}
        value_refs = Map.put(refs, name, entry)

        next =
          record
          |> Map.put(:version, version)
          |> Map.put(:updated_at_ms, now_ms)
          |> flow_put_record_value_refs(value_refs)

        link_key = flow_shared_value_link_key(next, name, entry)

        with :ok <- flow_validate_record_keys(next),
             :ok <- flow_validate_key_size(ref),
             :ok <- raw_put_cold(state, ref, encoded_value, flow_record_expire_at(next)),
             :ok <- flow_maybe_put_shared_value_link(state, link_key, ref, next),
             :ok <- flow_put_state_record(state, FlowKeys.state_key(id, partition_key), next),
             :ok <- flow_history_put_planned(state, record, next, "value_put", now_ms),
             :ok <- flow_after_history_put(state, next) do
          flow_named_value_put_result(attrs, %{
            ref: ref,
            partition_key: partition_key,
            owner_flow_id: id,
            name: name,
            version: value_version,
            created: is_nil(existing),
            stored: true
          })
        end
      end

      defp flow_named_value_put_store(
             state,
             record,
             attrs,
             _refs,
             existing,
             id,
             name,
             value,
             :generic,
             _digest,
             partition_key,
             now_ms,
             version,
             override?
           ) do
        with {:ok, value_refs} <-
               flow_named_value_refs(
                 record,
                 %{
                   values: %{name => value},
                   override_values: if(override?, do: [name], else: [])
                 },
                 id,
                 version,
                 partition_key
               ) do
          next =
            record
            |> Map.put(:version, version)
            |> Map.put(:updated_at_ms, now_ms)
            |> flow_put_record_value_refs(value_refs)

          with :ok <- flow_validate_record_keys(next),
               :ok <- flow_put_named_record_values(state, next, %{values: %{name => value}}),
               :ok <- flow_put_state_record(state, FlowKeys.state_key(id, partition_key), next),
               :ok <- flow_history_put_planned(state, record, next, "value_put", now_ms),
               :ok <- flow_after_history_put(state, next) do
            entry = Map.fetch!(value_refs, name)

            flow_named_value_put_result(attrs, %{
              ref: Map.fetch!(entry, :ref),
              partition_key: partition_key,
              owner_flow_id: id,
              name: name,
              version: Map.get(entry, :version),
              created: is_nil(existing),
              stored: true
            })
          end
        end
      end

      defp flow_named_value_put_result(%{return: :ok_on_success}, _result), do: {:ok, :ok}

      defp flow_named_value_put_result(_attrs, result), do: {:ok, result}

      defp do_flow_named_value_put(_state, _attrs),
        do: {:error, "ERR flow value name must be a non-empty string"}

      defp do_flow_named_value_put_pipeline_batch(state, %{records: [_ | _] = attrs_list}) do
        Enum.map(attrs_list, fn attrs -> do_flow_named_value_put(state, attrs) end)
      end

      defp do_flow_named_value_put_pipeline_batch(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp do_flow_signal(state, %{id: id, signal: signal} = attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        partition_key = Map.get(attrs, :partition_key)

        with {:ok, record} <- flow_require_record(state, id, partition_key),
             :ok <- flow_signal_idempotency_check(state, record, attrs),
             :ok <- flow_signal_transition_allowed?(record, attrs),
             {:ok, next} <- flow_signal_next_record(record, attrs, now_ms),
             next = flow_stamp_state_enter_seq_on_change(state, record, next),
             next = flow_refresh_indexed_attributes(state, next),
             :ok <- flow_require_fifo_entry(state, attrs, next, true),
             :ok <- flow_validate_record_keys(next),
             :ok <- flow_put_record_values(state, next, attrs),
             :ok <- flow_transition_move_indexes(state, [{record, next}]),
             :ok <- flow_put_state_record(state, FlowKeys.state_key(id, partition_key), next),
             :ok <- flow_signal_idempotency_put(state, next, attrs),
             :ok <-
               flow_history_put_planned(state, record, next, "signaled", now_ms, %{
                 "signal" => signal
               }),
             :ok <- flow_after_history_put(state, next) do
          :ok
        end
      end

      defp do_flow_signal_many(state, %{records: [_ | _] = records} = attrs) do
        records
        |> flow_expand_shared_attrs(Map.get(attrs, :shared))
        |> Enum.map(&do_flow_signal(state, &1))
      end

      defp do_flow_signal_many(_state, _attrs),
        do: {:error, "ERR flow signal_many requires records"}

      defp flow_signal_next_record(record, attrs, now_ms) do
        version = Map.fetch!(record, :version) + 1
        id = Map.fetch!(record, :id)
        partition_key = Map.get(record, :partition_key)

        with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
          transition_to = Map.get(attrs, :transition_to)

          next =
            record
            |> Map.merge(%{
              version: version,
              updated_at_ms: now_ms,
              ttl_ms: nil,
              retention_ttl_ms: Map.get(record, :retention_ttl_ms),
              history_hot_max_events: Map.get(record, :history_hot_max_events),
              history_max_events: Map.get(record, :history_max_events)
            })
            |> flow_put_record_value_refs(value_refs)

          next =
            if is_binary(transition_to) and transition_to != "" do
              Map.merge(next, %{
                state: transition_to,
                next_run_at_ms: Map.get(attrs, :run_at_ms, now_ms),
                lease_owner: nil,
                lease_token: nil,
                lease_deadline_ms: 0
              })
            else
              next
            end

          {:ok, flow_stamp_terminal_retention(next, now_ms)}
        end
      end

      defp flow_signal_transition_allowed?(record, attrs) do
        case Map.get(attrs, :transition_to) do
          nil ->
            :ok

          transition_to when is_binary(transition_to) and transition_to != "" ->
            cond do
              Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
                {:error, "ERR flow is terminal; use FLOW.REWIND"}

              is_nil(Map.get(attrs, :if_state)) ->
                {:error, "ERR flow signal transition requires if_state"}

              not flow_signal_if_state_match?(Map.get(attrs, :if_state), Map.get(record, :state)) ->
                {:error, "ERR flow state mismatch"}

              true ->
                with :ok <- flow_reject_running_transition(transition_to) do
                  flow_reject_terminal_transition(transition_to)
                end
            end
        end
      end

      defp flow_signal_if_state_match?(states, state) when is_list(states), do: state in states
      defp flow_signal_if_state_match?(state, state), do: true
      defp flow_signal_if_state_match?(_expected, _state), do: false

      defp flow_signal_idempotency_check(state, record, attrs) do
        case Map.get(attrs, :idempotency_key) do
          key when is_binary(key) and key != "" ->
            idem_key =
              FlowKeys.signal_idempotency_key(
                Map.fetch!(record, :id),
                key,
                Map.get(record, :partition_key)
              )

            digest = flow_signal_digest(attrs)

            case sm_store_batch_get(state, [idem_key], &sm_file_path/2) do
              [^digest] -> :ok
              [nil] -> :ok
              [_other] -> {:error, "ERR flow signal idempotency conflict"}
            end

          _ ->
            :ok
        end
      end

      defp flow_signal_idempotency_put(state, record, attrs) do
        case Map.get(attrs, :idempotency_key) do
          key when is_binary(key) and key != "" ->
            idem_key =
              FlowKeys.signal_idempotency_key(
                Map.fetch!(record, :id),
                key,
                Map.get(record, :partition_key)
              )

            raw_put_cold(
              state,
              idem_key,
              flow_signal_digest(attrs),
              flow_record_expire_at(record)
            )

          _ ->
            :ok
        end
      end

      defp flow_signal_digest(attrs) do
        attrs
        |> Map.take([
          :signal,
          :if_state,
          :transition_to,
          :run_at_ms,
          :values,
          :value_refs,
          :drop_values,
          :override_values
        ])
        |> :erlang.term_to_binary()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)
      end

      defp flow_create_apply_new_record(state, attrs, state_key) do
        with :ok <- flow_validate_create_attrs(attrs) do
          key_info = flow_create_fast_key_info(attrs, state_key)
          record = flow_create_record(state, attrs)

          with :ok <- flow_require_fifo_entry(state, attrs, record, true),
               false <- Map.get(attrs, :idempotent, false),
               plan = flow_create_fast_plan(record, attrs, key_info),
               :ok <- flow_many_same_state_machine_shard_by_keys?(state, [key_info]),
               :ok <- flow_validate_create_fast_plan_keys(plan) do
            flow_create_many_fast_apply(state, [plan])
          else
            {:error, _reason} = error -> error
            _ -> flow_create_apply_new_record_slow(state, attrs, state_key, record)
          end
        end
      end

      defp flow_create_apply_new_record_slow(state, attrs, state_key, record) do
        with :ok <- flow_validate_record_keys(record),
             :ok <- flow_put_record_values(state, record, attrs),
             :ok <-
               flow_put_new_state_record(
                 state,
                 state_key,
                 record
               ),
             :ok <- flow_create_put_registry_marker(state, record),
             :ok <- flow_due_put(state, record),
             :ok <- flow_index_put(state, record),
             :ok <- flow_history_put(state, record, "created", Map.get(record, :created_at_ms)),
             :ok <- flow_history_trim(state, record) do
          :ok
        end
      end

      defp flow_start_and_claim_apply_new_record(state, attrs, state_key) do
        with :ok <- flow_validate_start_and_claim_attrs(attrs) do
          key_info = flow_create_fast_key_info(attrs, state_key)

          record =
            state
            |> flow_create_record(attrs)
            |> flow_start_and_claim_record(attrs)

          with :ok <- flow_require_fifo_entry(state, attrs, record, true),
               plan = flow_create_fast_plan(record, attrs, key_info),
               :ok <- flow_many_same_state_machine_shard_by_keys?(state, [key_info]),
               :ok <- flow_validate_create_fast_plan_keys(plan),
               :ok <- flow_create_many_fast_apply(state, [plan]) do
            {:ok, record}
          else
            {:error, _reason} = error ->
              error

            _ ->
              flow_start_and_claim_apply_new_record_slow(state, attrs, state_key, record)
          end
        end
      end

      defp flow_start_and_claim_apply_new_record_slow(state, attrs, state_key, record) do
        with :ok <- flow_validate_record_keys(record),
             :ok <- flow_put_record_values(state, record, attrs),
             :ok <- flow_put_new_state_record(state, state_key, record),
             :ok <- flow_create_put_registry_marker(state, record),
             :ok <- flow_due_put(state, record),
             :ok <- flow_index_put(state, record),
             :ok <- flow_history_put(state, record, "created", Map.get(record, :created_at_ms)),
             :ok <- flow_history_trim(state, record) do
          {:ok, record}
        end
      end

      defp flow_start_and_claim_record(record, attrs) do
        now_ms = flow_attrs_now_ms(attrs)
        lease_ms = Map.fetch!(attrs, :lease_ms)
        fencing_token = 1
        worker = Map.fetch!(attrs, :worker)
        deadline_ms = now_ms + lease_ms

        token =
          worker <>
            ":" <>
            Integer.to_string(now_ms) <> ":" <> Integer.to_string(fencing_token)

        %{
          record
          | state: "running",
            run_state: Map.fetch!(attrs, :run_state),
            attempts: 1,
            fencing_token: fencing_token,
            updated_at_ms: now_ms,
            ttl_ms: nil,
            terminal_retention_until_ms: nil,
            lease_owner: worker,
            lease_token: token,
            lease_deadline_ms: deadline_ms,
            next_run_at_ms: deadline_ms
        }
      end

      defp do_flow_create_many(state, %{records: [_ | _] = attrs_list} = attrs) do
        stamped_shard = Map.get(attrs, @flow_shard_marker)

        case flow_create_pipeline_batch_fast_prepare(state, attrs_list, stamped_shard) do
          {:ok, plans} ->
            flow_create_many_fast_apply(state, plans)

          :fallback ->
            with :ok <- flow_many_partitions_valid?(state, attrs_list, stamped_shard),
                 :ok <- flow_create_many_unique?(attrs_list),
                 {:ok, _records, new_plans} <- flow_create_many_prepare(state, attrs_list),
                 :ok <- flow_create_many_apply(state, new_plans) do
              :ok
            end
        end
      end

      defp do_flow_create_many(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp do_flow_create_pipeline_batch(state, %{records: [_ | _] = attrs_list} = attrs) do
        case flow_create_pipeline_batch_fast_prepare(
               state,
               attrs_list,
               Map.get(attrs, @flow_shard_marker)
             ) do
          {:ok, plans} ->
            case flow_create_many_fast_apply(state, plans) do
              :ok -> List.duplicate(:ok, length(attrs_list))
              {:error, _reason} = error -> List.duplicate(error, length(attrs_list))
            end

          :fallback ->
            {results, plans} = flow_create_pipeline_batch_prepare(state, attrs_list)

            if plans == [] do
              results
            else
              case flow_create_many_apply(state, plans) do
                :ok ->
                  results

                {:error, _reason} = error ->
                  Enum.map(results, fn
                    :ok -> error
                    other -> other
                  end)
              end
            end
        end
      end

      defp do_flow_create_pipeline_batch(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp do_flow_start_and_claim_pipeline_batch(
             state,
             %{records: [_ | _] = attrs_list} = attrs
           ) do
        case flow_start_and_claim_pipeline_batch_fast_prepare(
               state,
               attrs_list,
               Map.get(attrs, @flow_shard_marker)
             ) do
          {:ok, plans, records} ->
            case flow_create_many_fast_apply(state, plans) do
              :ok -> Enum.map(records, &{:ok, &1})
              {:error, _reason} = error -> List.duplicate(error, length(attrs_list))
            end

          :fallback ->
            flow_start_and_claim_pipeline_batch_prepare(state, attrs_list)
        end
      end

      defp do_flow_start_and_claim_pipeline_batch(_state, _attrs),
        do: {:error, "ERR flow items must be a non-empty list"}

      defp flow_create_pipeline_batch_fast_prepare(state, attrs_list, stamped_shard) do
        if Enum.any?(attrs_list, &Map.get(&1, :idempotent, false)) do
          :fallback
        else
          with :ok <- flow_validate_create_attrs_list(attrs_list),
               :ok <- flow_many_partition_keys_present?(attrs_list),
               key_infos = flow_create_fast_key_infos(attrs_list, stamped_shard),
               :ok <- flow_many_same_state_machine_shard_by_keys?(state, key_infos),
               :ok <- flow_create_many_unique?(attrs_list),
               {:ok, plans} <-
                 flow_create_non_idempotent_many_prepare(state, attrs_list, key_infos) do
            {:ok, plans}
          else
            _ -> :fallback
          end
        end
      end

      defp flow_start_and_claim_pipeline_batch_fast_prepare(state, attrs_list, stamped_shard) do
        with :ok <- flow_validate_start_and_claim_attrs_list(attrs_list),
             :ok <- flow_many_partition_keys_present?(attrs_list),
             key_infos = flow_create_fast_key_infos(attrs_list, stamped_shard),
             :ok <- flow_many_same_state_machine_shard_by_keys?(state, key_infos),
             :ok <- flow_create_many_unique?(attrs_list),
             {:ok, plans, records} <-
               flow_start_and_claim_non_idempotent_many_prepare(state, attrs_list, key_infos) do
          {:ok, plans, records}
        else
          _ -> :fallback
        end
      end

      defp flow_validate_start_and_claim_attrs_list(attrs_list) do
        Enum.reduce_while(attrs_list, :ok, fn attrs, :ok ->
          case flow_validate_start_and_claim_attrs(attrs) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_fast_key_infos(attrs_list, stamped_shard) do
        Enum.map(attrs_list, fn attrs ->
          id = Map.fetch!(attrs, :id)
          partition_key = Map.get(attrs, :partition_key)
          tag = FlowKeys.tag(partition_key)

          %{
            partition_key: partition_key,
            tag: tag,
            state_key: flow_state_key_with_tag(tag, id),
            registry_key: flow_registry_key_with_tag(tag, id),
            shard_index: stamped_shard || Map.get(attrs, @flow_shard_marker)
          }
        end)
      end

      defp flow_create_fast_key_info(attrs, state_key) do
        partition_key = Map.get(attrs, :partition_key)
        tag = FlowKeys.tag(partition_key)

        %{
          partition_key: partition_key,
          tag: tag,
          state_key: state_key,
          registry_key: flow_registry_key_with_tag(tag, Map.fetch!(attrs, :id)),
          shard_index: Map.get(attrs, @flow_shard_marker)
        }
      end

      defp flow_many_same_state_machine_shard_by_keys?(
             %{instance_ctx: ctx, shard_index: shard_index},
             key_infos
           )
           when is_map(ctx) do
        if flow_key_infos_same_stamped_shard?(key_infos, shard_index) or
             Enum.all?(key_infos, fn %{state_key: key} ->
               Router.shard_for(ctx, key) == shard_index
             end) do
          :ok
        else
          {:error, "ERR flow batch crosses shards"}
        end
      rescue
        _ -> :ok
      end

      defp flow_many_same_state_machine_shard_by_keys?(_state, _key_infos), do: :ok

      defp flow_key_infos_same_stamped_shard?([_ | _] = key_infos, shard_index) do
        Enum.all?(key_infos, &(Map.get(&1, :shard_index) == shard_index))
      end

      defp flow_key_infos_same_stamped_shard?(_key_infos, _shard_index), do: false

      defp flow_create_non_idempotent_many_prepare(state, attrs_list, key_infos) do
        keys = Enum.map(key_infos, & &1.state_key)
        registry_keys = Enum.map(key_infos, & &1.registry_key)

        if Enum.any?(flow_registry_keys_present_hot_only(state, registry_keys), & &1) or
             Enum.any?(flow_state_keys_present_hot_only(state, keys), & &1) do
          {:error, "ERR flow already exists"}
        else
          attrs_list
          |> Enum.zip(key_infos)
          |> Enum.reduce_while({:ok, []}, fn {%{id: _id} = attrs, key_info}, {:ok, acc} ->
            record = flow_create_record(state, attrs)

            with :ok <- flow_require_fifo_entry(state, attrs, record, true),
                 plan = flow_create_fast_plan(record, attrs, key_info),
                 :ok <- flow_validate_create_fast_plan_keys(plan) do
              {:cont, {:ok, [plan | acc]}}
            else
              {:error, _reason} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, plans} -> {:ok, Enum.reverse(plans)}
            {:error, _reason} = error -> error
          end
        end
      end

      defp flow_start_and_claim_non_idempotent_many_prepare(state, attrs_list, key_infos) do
        keys = Enum.map(key_infos, & &1.state_key)
        registry_keys = Enum.map(key_infos, & &1.registry_key)

        if Enum.any?(flow_registry_keys_present_hot_only(state, registry_keys), & &1) or
             Enum.any?(flow_state_keys_present_hot_only(state, keys), & &1) do
          {:error, "ERR flow already exists"}
        else
          attrs_list
          |> Enum.zip(key_infos)
          |> Enum.reduce_while({:ok, [], []}, fn {%{id: _id} = attrs, key_info},
                                                 {:ok, plans, records} ->
            record =
              state
              |> flow_create_record(attrs)
              |> flow_start_and_claim_record(attrs)

            with :ok <- flow_require_fifo_entry(state, attrs, record, true),
                 plan = flow_create_fast_plan(record, attrs, key_info),
                 :ok <- flow_validate_create_fast_plan_keys(plan) do
              {:cont, {:ok, [plan | plans], [record | records]}}
            else
              {:error, _reason} = error -> {:halt, error}
            end
          end)
          |> case do
            {:ok, plans, records} -> {:ok, Enum.reverse(plans), Enum.reverse(records)}
            {:error, _reason} = error -> error
          end
        end
      end

      defp flow_start_and_claim_pipeline_batch_prepare(state, attrs_list) do
        {results, _seen} =
          Enum.reduce(attrs_list, {[], MapSet.new()}, fn attrs, {results, seen} ->
            case flow_validate_start_and_claim_attrs(attrs) do
              :ok ->
                key = FlowKeys.state_key(Map.fetch!(attrs, :id), Map.get(attrs, :partition_key))

                if MapSet.member?(seen, key) do
                  {[{:error, "ERR flow already exists"} | results], seen}
                else
                  result = do_flow_start_and_claim(state, attrs)
                  {[result | results], MapSet.put(seen, key)}
                end

              {:error, _reason} = error ->
                {[error | results], seen}
            end
          end)

        Enum.reverse(results)
      end

      defp flow_create_fast_plan(record, attrs, %{
             tag: tag,
             state_key: state_key,
             registry_key: registry_key
           }) do
        partition_key = Map.get(record, :partition_key)
        id = Map.fetch!(record, :id)
        type = Map.fetch!(record, :type)
        flow_state = Map.fetch!(record, :state)
        score = Map.get(record, :updated_at_ms, 0)
        history_key = flow_history_key_with_tag(tag, id)

        %{
          record: record,
          attrs: attrs,
          partition_key: partition_key,
          tag: tag,
          state_key: state_key,
          registry_key: registry_key,
          shard_index: Map.get(attrs, @flow_shard_marker),
          history_key: history_key,
          state_index_key: flow_state_index_key_with_tag(tag, type, flow_state),
          state_index_score: score,
          due_key: flow_create_fast_due_key(record, tag, type, flow_state),
          due_any_key: flow_create_fast_due_any_key(record, tag, type),
          running_index_entries: flow_create_fast_running_index_entries(record, tag, type),
          metadata_index_entries: flow_metadata_index_entries_with_tag(record, tag)
        }
      end

      defp flow_create_fast_due_key(%{next_run_at_ms: nil}, _tag, _type, _flow_state), do: nil

      defp flow_create_fast_due_key(%{priority: priority}, tag, type, flow_state) do
        flow_due_key_with_tag(tag, type, flow_state, priority)
      end

      defp flow_create_fast_due_any_key(%{next_run_at_ms: nil}, _tag, _type), do: nil

      defp flow_create_fast_due_any_key(%{priority: priority}, tag, type) do
        if flow_due_any_index_enabled?() do
          flow_due_any_key_with_tag(tag, type, priority)
        else
          nil
        end
      end

      defp flow_create_fast_running_index_entries(%{state: "running"} = record, tag, type) do
        lease_score = Map.get(record, :lease_deadline_ms, 0)

        [
          {flow_inflight_index_key_with_tag(tag, type), lease_score},
          {flow_worker_index_key_with_tag(tag, Map.get(record, :lease_owner, "")), lease_score}
        ]
      end

      defp flow_create_fast_running_index_entries(_record, _tag, _type), do: []

      defp flow_validate_create_fast_plan_keys(%{
             record: record,
             state_key: state_key,
             history_key: history_key,
             state_index_key: state_index_key,
             due_key: due_key,
             due_any_key: due_any_key,
             running_index_entries: running_index_entries,
             metadata_index_entries: metadata_index_entries
           }) do
        with :ok <-
               flow_validate_shared_value_ref_locality(
                 record,
                 flow_record_shared_value_refs(record)
               ),
             :ok <- flow_validate_key_size(state_key),
             :ok <- flow_validate_key_size(history_key),
             :ok <- flow_validate_key_size(state_index_key),
             :ok <-
               flow_validate_key_size(
                 FlowKeys.stream_entry_key_from_history_key(
                   history_key,
                   "18446744073709551615-18446744073709551615"
                 )
               ),
             :ok <- flow_validate_create_fast_due_key(due_key),
             :ok <- flow_validate_create_fast_due_key(due_any_key),
             :ok <- flow_validate_create_fast_index_entries(running_index_entries),
             :ok <- flow_validate_create_fast_metadata_entries(metadata_index_entries) do
          if byte_size(Map.fetch!(record, :id)) <= @flow_max_key_size do
            :ok
          else
            {:error, "ERR key too large (max #{@flow_max_key_size} bytes)"}
          end
        end
      end

      defp flow_validate_create_fast_due_key(nil), do: :ok
      defp flow_validate_create_fast_due_key(key), do: flow_validate_key_size(key)

      defp flow_validate_create_fast_index_entries(entries) do
        Enum.reduce_while(entries, :ok, fn {key, _score}, :ok ->
          case flow_validate_key_size(key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_validate_create_fast_metadata_entries(entries) do
        Enum.reduce_while(entries, :ok, fn {key, _id, _score}, :ok ->
          case flow_validate_key_size(key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp flow_create_pipeline_batch_prepare(state, attrs_list) do
        {results, plans, _seen} =
          Enum.reduce(attrs_list, {[], [], MapSet.new()}, fn attrs, {results, plans, seen} ->
            case flow_validate_create_attrs(attrs) do
              :ok ->
                key = FlowKeys.state_key(Map.fetch!(attrs, :id), Map.get(attrs, :partition_key))

                if MapSet.member?(seen, key) do
                  {[{:error, "ERR flow already exists"} | results], plans, seen}
                else
                  case flow_create_many_prepare(state, [attrs]) do
                    {:ok, _records, new_plans} ->
                      {[:ok | results], Enum.reverse(new_plans) ++ plans, MapSet.put(seen, key)}

                    {:error, _reason} = error ->
                      {[error | results], plans, seen}
                  end
                end

              {:error, _reason} = error ->
                {[error | results], plans, seen}
            end
          end)

        {Enum.reverse(results), Enum.reverse(plans)}
      end
    end
  end
end
