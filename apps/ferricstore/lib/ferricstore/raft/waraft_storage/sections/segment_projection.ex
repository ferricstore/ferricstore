defmodule Ferricstore.Raft.WARaftStorage.Sections.SegmentProjection do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Flow.SharedRefBackfill
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.ServerCatalog
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.LogicalKeyIndex
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      @segment_flush_page_size 512

      defp prepare_segment_blob_batch_entries(entries) do
        entries
        |> Enum.reduce_while({:ok, [], []}, fn
          {key, value, expire_at_ms, :value}, {:ok, prepared, encoded_refs}
          when is_binary(key) and is_binary(value) ->
            if non_neg_integer?(expire_at_ms) do
              {:cont, {:ok, [{:value, key, value, expire_at_ms} | prepared], encoded_refs}}
            else
              {:halt, {:unsupported, :invalid_blob_batch_entry}}
            end

          {key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, prepared, encoded_refs}
          when is_binary(key) and is_binary(encoded_ref) ->
            if non_neg_integer?(expire_at_ms) do
              {:cont,
               {:ok, [{:blob_ref, key, encoded_ref, expire_at_ms} | prepared],
                [encoded_ref | encoded_refs]}}
            else
              {:halt, {:unsupported, :invalid_blob_batch_entry}}
            end

          _entry, {:ok, _prepared, _encoded_refs} ->
            {:halt, {:unsupported, :invalid_blob_batch_entry}}
        end)
        |> case do
          {:ok, prepared, encoded_refs} ->
            {:ok, Enum.reverse(prepared), Enum.reverse(encoded_refs)}

          other ->
            other
        end
      end

      defp prepare_segment_compound_blob_batch_entries(redis_key, entries) do
        entries
        |> Enum.reduce_while({:ok, [], []}, fn
          {compound_key, value, expire_at_ms, :value}, {:ok, prepared, encoded_refs}
          when is_binary(compound_key) and is_binary(value) ->
            if compound_key_for_redis_key?(redis_key, compound_key) and
                 non_neg_integer?(expire_at_ms) do
              {:cont,
               {:ok, [{:value, compound_key, value, expire_at_ms} | prepared], encoded_refs}}
            else
              {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
            end

          {compound_key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, prepared, encoded_refs}
          when is_binary(compound_key) and is_binary(encoded_ref) ->
            if compound_key_for_redis_key?(redis_key, compound_key) and
                 non_neg_integer?(expire_at_ms) do
              {:cont,
               {:ok, [{:blob_ref, compound_key, encoded_ref, expire_at_ms} | prepared],
                [encoded_ref | encoded_refs]}}
            else
              {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
            end

          _entry, {:ok, _prepared, _encoded_refs} ->
            {:halt, {:unsupported, :invalid_compound_blob_batch_entry}}
        end)
        |> case do
          {:ok, prepared, encoded_refs} ->
            {:ok, Enum.reverse(prepared), Enum.reverse(encoded_refs)}

          other ->
            other
        end
      end

      defp verify_segment_blob_refs(_sm_state, []), do: :ok

      defp verify_segment_blob_refs(sm_state, encoded_refs) do
        with {:ok, refs} <- decode_segment_blob_refs(encoded_refs),
             :ok <- BlobStore.verify_many(sm_state.data_dir, sm_state.shard_index, refs) do
          :ok
        else
          {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
        end
      end

      defp decode_segment_blob_refs(encoded_refs) do
        Enum.reduce_while(encoded_refs, {:ok, []}, fn encoded_ref, {:ok, refs} ->
          case BlobRef.decode(encoded_ref) do
            {:ok, ref} -> {:cont, {:ok, [ref | refs]}}
            :error -> {:halt, {:error, :invalid_blob_ref}}
          end
        end)
        |> case do
          {:ok, refs} -> {:ok, Enum.reverse(refs)}
          {:error, _reason} = error -> error
        end
      end

      defp segment_project_put(sm_state, key, value, expire_at_ms, position) do
        file_id = {:waraft_segment, position_index(position)}
        offset = segment_record_offset(sm_state, position)
        segment_project_put_at_location(sm_state, key, value, expire_at_ms, file_id, offset)
      end

      defp segment_project_put_at_location(sm_state, key, value, expire_at_ms, file_id, offset) do
        shard_state = shard_ets_state_from_sm(sm_state)
        threshold = segment_project_hot_cache_threshold(shard_state, key)

        segment_project_put_at_location(
          sm_state,
          shard_state,
          threshold,
          key,
          value,
          expire_at_ms,
          file_id,
          offset
        )
      end

      defp segment_project_put_at_location(
             sm_state,
             shard_state,
             threshold,
             key,
             value,
             expire_at_ms,
             file_id,
             offset
           ) do
        previous = :ets.lookup(shard_state.keydir, key)
        sm_state = segment_project_clear_compound_for_string_put(sm_state, key, previous)

        true =
          ShardETS.ets_insert_with_location(
            shard_state,
            key,
            value,
            expire_at_ms,
            file_id,
            offset,
            byte_size(value),
            previous,
            threshold
          )

        sm_state
      end

      defp segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position) do
        shard_state = shard_ets_state_from_sm(sm_state)
        previous = :ets.lookup(shard_state.keydir, key)
        sm_state = segment_project_clear_compound_for_string_put(sm_state, key, previous)
        file_id = {:waraft_segment, position_index(position)}
        offset = segment_record_offset(sm_state, position)
        value_size = blob_ref_logical_size(encoded_ref)

        true =
          ShardETS.ets_insert_with_location(
            shard_state,
            key,
            nil,
            expire_at_ms,
            file_id,
            offset,
            value_size,
            previous
          )

        sm_state
      end

      defp blob_ref_logical_size(encoded_ref) do
        case BlobRef.decode(encoded_ref) do
          {:ok, %BlobRef{size: size}} -> size
          :error -> byte_size(encoded_ref)
        end
      end

      defp segment_project_clear_compound_for_string_put(sm_state, key, previous)
           when is_binary(key) do
        cond do
          CompoundKey.internal_key?(key) ->
            sm_state

          # Existing plain string row means this SET cannot be overwriting a compound value.
          # Reuse the lookup needed for ETS accounting and skip the marker probe.
          match?([{^key, _value, _expire_at_ms, _lfu, _fid, _offset, _value_size}], previous) ->
            sm_state

          true ->
            segment_project_clear_compound_for_string_put(sm_state, key)
        end
      end

      defp segment_project_clear_compound_for_string_put(sm_state, _key, _previous), do: sm_state

      defp segment_project_clear_compound_for_string_put(sm_state, key) when is_binary(key) do
        if CompoundKey.internal_key?(key) do
          sm_state
        else
          marker_key = CompoundKey.type_key(key)

          case segment_project_live_value(sm_state, marker_key) do
            "hash" ->
              sm_state
              |> segment_project_delete_prefix_for_string_put(key, CompoundKey.hash_prefix(key))
              |> segment_project_delete(marker_key)

            "list" ->
              sm_state
              |> segment_project_delete_prefix_for_string_put(key, CompoundKey.list_prefix(key))
              |> segment_project_delete(CompoundKey.list_meta_key(key))
              |> segment_project_delete(marker_key)

            "set" ->
              sm_state
              |> segment_project_delete_prefix_for_string_put(key, CompoundKey.set_prefix(key))
              |> segment_project_delete(marker_key)

            "zset" ->
              sm_state
              |> segment_project_delete_prefix_for_string_put(key, CompoundKey.zset_prefix(key))
              |> segment_project_delete(marker_key)

            _none_or_unknown ->
              sm_state
          end
        end
      end

      defp segment_project_clear_compound_for_string_put(sm_state, _key), do: sm_state

      defp segment_project_live_value(
             %{ets: keydir, instance_ctx: ctx, shard_index: shard_index},
             key
           )
           when is_binary(key) do
        now = storage_expiry_cutoff_ms()

        case :ets.lookup(keydir, key) do
          [{^key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
          when is_binary(value) ->
            if live_expire_at?(expire_at_ms, now), do: value, else: nil

          [
            {^key, nil, expire_at_ms, _lfu, file_id, _offset, _value_size}
          ]
          when valid_segment_backed_file_id(file_id) ->
            if live_expire_at?(expire_at_ms, now) do
              case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                     ctx,
                     shard_index,
                     file_id,
                     key
                   ) do
                {:ok, value} when is_binary(value) -> value
                _other -> nil
              end
            else
              nil
            end

          _other ->
            nil
        end
      end

      defp segment_project_delete(sm_state, key) do
        true = ShardETS.ets_delete_key(shard_ets_state_from_sm(sm_state), key)
        sm_state
      end

      defp segment_project_delete_prefix(sm_state, redis_key, prefix) do
        index = Map.get(sm_state, :compound_member_index_name)
        budget = sm_state.apply_context.compound_delete_member_budget

        case CompoundMemberIndex.keys_for_prefix(index, prefix, budget) do
          {:ok, compound_keys} ->
            next_state =
              compound_keys
              |> Enum.reduce(sm_state, fn key, acc -> segment_project_delete(acc, key) end)
              |> ZSetIndex.clear_ready_key(redis_key)

            {:ok, next_state}

          {:error, :limit_exceeded} ->
            {:error, :compound_delete_budget_exceeded}

          :unavailable ->
            {:error, :compound_member_index_unavailable}
        end
      end

      defp segment_project_delete_prefix_for_string_put(sm_state, redis_key, prefix) do
        case segment_project_delete_prefix(sm_state, redis_key, prefix) do
          {:ok, next_state} ->
            next_state

          {:error, reason} ->
            raise "WARaft projected string overwrite violated compound cleanup preflight: #{inspect(reason)}"
        end
      end

      defp segment_project_zset_put(sm_state, redis_key, compound_key, value),
        do: ZSetIndex.apply_put(sm_state, redis_key, compound_key, value)

      defp segment_project_zset_delete(sm_state, redis_key, compound_key),
        do: ZSetIndex.apply_delete(sm_state, redis_key, compound_key)

      defp apply_segment_projection_entries(sm_state, _position, []), do: sm_state

      defp apply_segment_projection_entries(sm_state, projection_root, entries)
           when is_binary(projection_root) do
        now = storage_expiry_cutoff_ms()

        entries
        |> Enum.with_index(1)
        |> Enum.reduce(sm_state, fn {{key, value, expire_at_ms}, projection_index}, acc ->
          if live_expire_at?(expire_at_ms, now) do
            segment_project_recovered_projection_entry(
              acc,
              projection_root,
              projection_index,
              key,
              value,
              expire_at_ms
            )
          else
            acc
          end
        end)
      end

      defp apply_segment_projection_entries(sm_state, position, entries) do
        now = storage_expiry_cutoff_ms()

        Enum.reduce(entries, sm_state, fn {key, value, expire_at_ms}, acc ->
          if live_expire_at?(expire_at_ms, now) do
            segment_project_recovered_entry(acc, key, value, expire_at_ms, position)
          else
            acc
          end
        end)
      end

      if Mix.env() == :test do
        @doc false
        def __apply_segment_projection_entries_for_test__(sm_state, position, entries) do
          apply_segment_projection_entries(sm_state, position, entries)
        end
      end

      defp segment_project_recovered_projection_entry(
             sm_state,
             projection_root,
             projection_index,
             key,
             value,
             expire_at_ms
           ) do
        offset = projection_record_offset(projection_root, projection_index)
        sm_state = segment_project_clear_compound_for_string_put(sm_state, key)
        shard_state = shard_ets_state_from_sm(sm_state)
        threshold = segment_project_hot_cache_threshold(shard_state, key)
        previous = :ets.lookup(shard_state.keydir, key)

        if segment_blob_ref_value?(value) do
          true =
            ShardETS.ets_insert_with_location(
              shard_state,
              key,
              nil,
              expire_at_ms,
              {:waraft_projection, projection_index},
              offset,
              segment_projected_value_size(value),
              previous
            )
        else
          true =
            ShardETS.ets_insert_with_location(
              shard_state,
              key,
              value,
              expire_at_ms,
              {:waraft_projection, projection_index},
              offset,
              byte_size(value),
              previous,
              threshold
            )
        end

        sm_state
      end

      defp segment_project_recovered_entry(sm_state, key, value, expire_at_ms, position) do
        if segment_blob_ref_value?(value) do
          segment_project_put_blob_ref(sm_state, key, value, expire_at_ms, position)
        else
          segment_project_put(sm_state, key, value, expire_at_ms, position)
        end
      end

      defp segment_blob_ref_value?(value) when is_binary(value) do
        BlobRef.encoded_size?(byte_size(value)) and BlobRef.ref?(value)
      end

      defp segment_blob_ref_value?(_value), do: false

      defp segment_projected_value_size(value) when is_binary(value) do
        if segment_blob_ref_value?(value),
          do: blob_ref_logical_size(value),
          else: byte_size(value)
      end

      defp segment_projected_value_size(_value), do: 0

      defp single_segment_project_result({:ok, [result]}), do: result
      defp single_segment_project_result(result), do: result

      defp bump_segment_projected_applied_count(sm_state, 0), do: sm_state

      defp bump_segment_projected_applied_count(sm_state, count)
           when is_integer(count) and count > 0 do
        Map.update(sm_state, :applied_count, count, &(&1 + count))
      end

      defp shard_ets_state_from_sm(sm_state) do
        %{
          keydir: sm_state.ets,
          index: sm_state.shard_index,
          instance_ctx: sm_state.instance_ctx,
          compound_member_index: Map.get(sm_state, :compound_member_index_name),
          logical_key_index: Map.get(sm_state, :logical_key_index_name),
          logical_key_slots: Map.get(sm_state, :logical_key_slots_name)
        }
      end

      defp position_index({:raft_log_pos, index, _term}) when is_integer(index), do: index
      defp position_index(_position), do: 0

      defp segment_record_offset(
             %{data_dir: data_dir, shard_index: shard_index},
             {:raft_log_pos, index, _term}
           )
           when is_integer(index) and index > 0 do
        root = Path.join([data_dir, "waraft", "#{@storage_root}.#{shard_index + 1}"])

        case :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), index) do
          {:ok, {_ordinal, offset, _encoded_size}} when is_integer(offset) and offset >= 0 ->
            offset

          _missing_or_error ->
            0
        end
      end

      defp segment_record_offset(_sm_state, _position), do: 0

      defp projection_record_offset(projection_root, projection_index)
           when is_binary(projection_root) and is_integer(projection_index) and
                  projection_index > 0 do
        case projection_record_location(projection_root, projection_index) do
          {:ok, offset} -> offset
          {:error, _reason} -> 0
        end
      end

      defp projection_record_offset(_projection_root, _projection_index), do: 0

      defp projection_record_location(projection_root, projection_index)
           when is_binary(projection_root) and is_integer(projection_index) and
                  projection_index > 0 do
        case :ferricstore_waraft_spike_segment_log.location_for_index(
               to_charlist(projection_root),
               projection_index
             ) do
          {:ok, {_ordinal, offset, _encoded_size}} when is_integer(offset) and offset >= 0 ->
            {:ok, offset}

          :not_found ->
            {:error, {:missing_segment_projection_offset, projection_index}}

          {:error, reason} ->
            {:error, {:segment_projection_offset_failed, projection_index, reason}}
        end
      end

      defp projection_record_location(_projection_root, projection_index),
        do: {:error, {:bad_segment_projection_index, projection_index}}

      defp apply_state_machine_command(command, position, sm_state) do
        meta = meta_from_position(position)

        StateMachine.apply_waraft_segment_command(command, meta, sm_state, fn batch ->
          write_apply_projection_batch(sm_state, position, batch)
        end)
      end

      defp write_apply_projection_batch(sm_state, position, batch) do
        index = position_index(position)

        if index > 0 do
          file_id = {:waraft_apply_projection, index}

          :ok = cache_apply_projection_batch(sm_state, index, batch)

          :ok =
            Ferricstore.FaultInjection.maybe_pause(:after_waraft_apply_projection_write, %{
              shard_index: sm_state.shard_index,
              index: index,
              entry_count: length(batch)
            })

          {:ok, file_id, apply_projection_locations(batch, 0)}
        else
          {:error, {:bad_waraft_projection_position, position}}
        end
      end

      defp cache_apply_projection_batch(sm_state, index, batch) do
        WARaftSegmentReader.put_apply_projection(
          sm_state.instance_ctx.data_dir,
          sm_state.shard_index,
          index,
          apply_projection_entries(batch)
        )
      end

      defp recover_apply_projection_value_locators!(sm_state, root_dir) do
        projection_root = apply_projection_root(root_dir)

        case :ferricstore_waraft_spike_segment_log.fold_disk(
               to_charlist(projection_root),
               &recover_apply_projection_value_locator_record/3,
               %{sm_state: sm_state, error: nil}
             ) do
          {:ok, %{error: nil, sm_state: recovered_sm_state}} ->
            recovered_sm_state

          {:ok, %{error: reason}} ->
            raise "failed to recover WARaft apply projection value locators: #{inspect(reason)}"

          {:error, :enoent} ->
            sm_state

          {:error, reason} ->
            raise "failed to recover WARaft apply projection value locators: #{inspect(reason)}"
        end
      end

      defp recover_apply_projection_value_locator_record(
             _log_index,
             _entry,
             %{error: reason} = acc
           )
           when not is_nil(reason),
           do: acc

      defp recover_apply_projection_value_locator_record(
             _log_index,
             {0, {:ferricstore_segment_apply_projection_batch, position, entries}},
             acc
           )
           when is_list(entries) do
        case position_index(position) do
          index when is_integer(index) and index > 0 ->
            case recover_apply_projection_value_locator_entries(acc.sm_state, index, entries) do
              :ok -> acc
              {:error, reason} -> %{acc | error: reason}
            end

          _bad_index ->
            %{acc | error: {:bad_apply_projection_position, position}}
        end
      end

      defp recover_apply_projection_value_locator_record(_log_index, _entry, acc), do: acc

      defp recover_apply_projection_value_locator_entries(sm_state, index, entries) do
        now = storage_expiry_cutoff_ms()

        entries =
          Enum.flat_map(entries, fn
            {key, value, expire_at_ms}
            when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
              if live_expire_at?(expire_at_ms, now) and generated_flow_value_ref?(key) do
                [{key, expire_at_ms, {:waraft_apply_projection, index}, 0, byte_size(value)}]
              else
                []
              end

            _invalid ->
              []
          end)

        case entries do
          [] ->
            :ok

          [_ | _] ->
            sm_state.shard_data_path
            |> FlowLMDB.path()
            |> FlowLMDB.write_batch(FlowLMDB.segment_value_pin_batch_put_ops(entries))
        end
      end

      defp generated_flow_value_ref?(key) do
        Ferricstore.Flow.HistoryProjector.ValueProjection.generated_flow_value_ref?(key)
      end

      defp consume_apply_projection_replay_dependencies do
        dependencies =
          Process.get(@apply_projection_replay_dependencies_key, %{})
          |> normalize_replay_dependency_map()

        clear_apply_projection_replay_dependencies()
        dependencies
      end

      defp clear_apply_projection_replay_dependencies do
        Process.delete(@apply_projection_replay_dependencies_key)
        :ok
      end

      defp merge_apply_projection_replay_dependencies(dependencies, apply_projection)
           when is_map(dependencies) do
        apply_projection = normalize_replay_dependency_map(apply_projection)

        if map_size(apply_projection) == 0 do
          dependencies
        else
          Map.update(dependencies, :apply_projection, apply_projection, fn existing ->
            merge_replay_dependency_maps(existing, apply_projection)
          end)
        end
      end

      defp merge_apply_projection_replay_dependencies(dependencies, _apply_projection),
        do: dependencies

      defp spill_apply_projection_replay_dependencies(%{ctx: %{data_dir: data_dir}} = handle) do
        handle
        |> Map.get(:replay_dependencies, replay_dependency_defaults())
        |> Map.get(:apply_projection, %{})
        |> normalize_replay_dependency_map()
        |> Enum.each(fn {shard_index, index} ->
          unless WARaftSegmentReader.apply_projection_dependency_ready?(
                   data_dir,
                   shard_index,
                   index
                 ) do
            _ = WARaftSegmentReader.spill_apply_projection_cache(data_dir, shard_index)
          end
        end)

        :ok
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      defp spill_apply_projection_replay_dependencies(_handle), do: :ok

      defp apply_projection_replay_dependencies_ready?(%{ctx: %{data_dir: data_dir}} = handle) do
        handle
        |> Map.get(:replay_dependencies, replay_dependency_defaults())
        |> Map.get(:apply_projection, %{})
        |> normalize_replay_dependency_map()
        |> Enum.all?(fn {shard_index, index} ->
          WARaftSegmentReader.apply_projection_dependency_ready?(data_dir, shard_index, index)
        end)
      end

      defp apply_projection_replay_dependencies_ready?(_handle), do: true

      defp history_replay_dependencies_ready?(handle) do
        handle
        |> Map.get(:replay_dependencies, replay_dependency_defaults())
        |> Map.get(:history, %{})
        |> normalize_replay_dependency_map()
        |> Enum.all?(fn {shard_index, index} ->
          HistoryProjector.durable?(
            Map.get(handle, :ctx),
            shard_index,
            replay_dependency_shard_data_path(handle, shard_index),
            index
          )
        end)
      end

      defp request_history_replay_dependencies(handle) do
        handle
        |> Map.get(:replay_dependencies, replay_dependency_defaults())
        |> Map.get(:history, %{})
        |> normalize_replay_dependency_map()
        |> Enum.each(fn {shard_index, index} ->
          HistoryProjector.request(
            Map.get(handle, :ctx),
            shard_index,
            replay_dependency_shard_data_path(handle, shard_index),
            index
          )
        end)

        :ok
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      defp flush_history_replay_dependencies(handle, timeout_ms) do
        handle
        |> Map.get(:replay_dependencies, replay_dependency_defaults())
        |> Map.get(:history, %{})
        |> normalize_replay_dependency_map()
        |> Enum.each(fn {shard_index, _index} ->
          _ =
            HistoryProjector.flush(
              Map.get(handle, :ctx),
              shard_index,
              timeout_ms
            )
        end)

        :ok
      end

      defp replay_dependency_defaults, do: %{history: %{}, apply_projection: %{}}

      defp replay_dependency_defaults(kind, dependencies)
           when kind in [:history, :apply_projection] do
        replay_dependency_defaults()
        |> Map.put(kind, dependencies)
      end

      defp replay_dependency_defaults(_kind, _dependencies), do: replay_dependency_defaults()

      defp apply_projection_locations(batch, offset) do
        Enum.map(batch, fn
          {:put, _key, value, _expire_at_ms} -> {:put, offset, byte_size(value)}
          {:put_cold, _key, value, _expire_at_ms, _lfu} -> {:put, offset, byte_size(value)}
          {:delete, key, _prob_path} -> {:delete, offset, byte_size(key)}
        end)
      end

      defp apply_projection_entries(batch) do
        Enum.flat_map(batch, fn
          {:put, key, value, expire_at_ms}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
            [{key, value, expire_at_ms}]

          {:put_cold, key, value, expire_at_ms, _lfu}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
            [{key, value, expire_at_ms}]

          _delete_or_invalid ->
            []
        end)
      end

      defp maybe_mark_bitcask_dirty(handle), do: handle

      defp maybe_update_label(handle, :keep_label), do: handle
      defp maybe_update_label(handle, {:replace_label, label}), do: %{handle | label: label}

      defp maybe_put_status(status, _key, nil), do: status
      defp maybe_put_status(status, key, value), do: [{key, value} | status]

      defp durable_position(%{bitcask_dirty?: true} = handle) do
        last_clean_position(handle)
      end

      defp durable_position(handle) do
        if replay_dependencies_ready?(handle) do
          Map.get(handle, :position)
        else
          last_clean_position(handle)
        end
      end

      defp last_clean_position(handle) do
        Map.get(
          handle,
          :last_clean_position,
          Map.get(handle, :persisted_position, Map.get(handle, :position))
        )
      end

      defp merge_replay_dependencies(handle, dependencies) when is_map(dependencies) do
        Enum.reduce([:history, :apply_projection], handle, fn kind, acc ->
          dependency_map =
            dependencies
            |> Map.get(kind, %{})
            |> normalize_replay_dependency_map()

          if map_size(dependency_map) == 0 do
            acc
          else
            Map.update(
              acc,
              :replay_dependencies,
              replay_dependency_defaults(kind, dependency_map),
              fn
                existing ->
                  existing = existing || replay_dependency_defaults()

                  Map.update(existing, kind, dependency_map, fn existing_map ->
                    merge_replay_dependency_maps(existing_map, dependency_map)
                  end)
              end
            )
          end
        end)
      end

      defp merge_replay_dependencies(handle, _dependencies), do: handle

      defp clear_replay_dependencies(handle),
        do: Map.put(handle, :replay_dependencies, replay_dependency_defaults())

      defp replay_dependencies_ready?(handle) do
        history_replay_dependencies_ready?(handle) and
          apply_projection_replay_dependencies_ready?(handle)
      end

      defp request_replay_dependencies(handle) do
        request_history_replay_dependencies(handle)
        spill_apply_projection_replay_dependencies(handle)
        :ok
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end

      defp request_replay_dependencies_async(handle) do
        request_history_replay_dependencies(handle)
        maybe_start_apply_projection_replay_spill(handle)
      rescue
        _ -> handle
      catch
        _, _ -> handle
      end

      defp maybe_start_apply_projection_replay_spill(
             %{apply_projection_cache_compaction: _} = handle
           ),
           do: handle

      defp maybe_start_apply_projection_replay_spill(%{ctx: %{data_dir: data_dir}} = handle) do
        dependencies =
          handle
          |> Map.get(:replay_dependencies, replay_dependency_defaults())
          |> Map.get(:apply_projection, %{})
          |> normalize_replay_dependency_map()

        cond do
          map_size(dependencies) == 0 ->
            handle

          apply_projection_replay_dependencies_ready?(handle) ->
            handle

          true ->
            count =
              WARaftSegmentReader.apply_projection_cache_count(
                data_dir,
                handle.shard_index
              )

            {:ok, requested_handle} = start_apply_projection_cache_compaction(handle, count, 0)
            requested_handle
        end
      end

      defp maybe_start_apply_projection_replay_spill(handle), do: handle

      defp flush_replay_dependencies_before_close(handle) do
        if replay_dependencies_ready?(handle) do
          handle
        else
          timeout_ms = replay_dependency_close_flush_timeout_ms()

          flush_history_replay_dependencies(handle, timeout_ms)
          request_replay_dependencies(handle)
          wait_replay_dependencies_ready(handle, timeout_ms)
        end
      rescue
        _ -> handle
      catch
        _, _ -> handle
      end

      defp wait_replay_dependencies_ready(handle, timeout_ms) do
        deadline = System.monotonic_time(:millisecond) + max(timeout_ms, 0)
        do_wait_replay_dependencies_ready(handle, deadline)
      end

      defp do_wait_replay_dependencies_ready(handle, deadline) do
        cond do
          replay_dependencies_ready?(handle) ->
            handle

          System.monotonic_time(:millisecond) >= deadline ->
            handle

          true ->
            Process.sleep(10)
            do_wait_replay_dependencies_ready(handle, deadline)
        end
      end

      defp replay_dependency_close_flush_timeout_ms do
        Application.get_env(
          :ferricstore,
          :waraft_replay_dependency_close_flush_timeout_ms,
          10_000
        )
      end

      defp replay_dependency_shard_data_path(%{ctx: %{data_dir: data_dir}}, shard_index)
           when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
        Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
      end

      defp replay_dependency_shard_data_path(%{sm_state: %{shard_data_path: path}}, _shard_index)
           when is_binary(path),
           do: path

      defp replay_dependency_shard_data_path(_handle, shard_index),
        do: Path.join(["data", "shard_#{shard_index}"])

      defp merge_replay_dependency_maps(left, right) do
        right
        |> normalize_replay_dependency_map()
        |> Enum.reduce(normalize_replay_dependency_map(left), fn {shard_index, index}, acc ->
          Map.update(acc, shard_index, index, &max(&1, index))
        end)
      end

      defp normalize_replay_dependency_map(dependencies) when is_map(dependencies) do
        dependencies
        |> Enum.reduce(%{}, fn
          {shard_index, index}, acc
          when is_integer(shard_index) and shard_index >= 0 and is_integer(index) and index > 0 ->
            Map.update(acc, shard_index, index, &max(&1, index))

          _other, acc ->
            acc
        end)
      end

      defp normalize_replay_dependency_map(_dependencies), do: %{}
    end
  end
end
