defmodule Ferricstore.Raft.StateMachine.Sections.CrossShardReads do
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
      alias Ferricstore.ExpiryContext
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

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, CompoundRevisionIndex, ZSetIndex}
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp cross_shard_ctx(anchor_state, shard_idx, data_dir, instance_ctx) do
        if shard_idx == anchor_state.shard_index do
          %{
            instance_ctx: instance_ctx,
            keydir: anchor_state.ets,
            index: shard_idx,
            data_dir: data_dir,
            shard_data_path: anchor_state.shard_data_path,
            active_file_path: anchor_state.active_file_path,
            active_file_id: anchor_state.active_file_id,
            zset_score_index_name: anchor_state.zset_score_index_name,
            zset_score_lookup_name: anchor_state.zset_score_lookup_name,
            compound_member_index_name: Map.get(anchor_state, :compound_member_index_name),
            compound_revision_index_name: Map.get(anchor_state, :compound_revision_index_name),
            flow_index_name: Map.get(anchor_state, :flow_index_name),
            flow_lookup_name: Map.get(anchor_state, :flow_lookup_name),
            promoted_instances: Map.get(anchor_state, :promoted_instances, %{})
          }
        else
          {file_id, file_path, shard_data_path} =
            Ferricstore.Store.ActiveFile.get(instance_ctx, shard_idx)

          keydir =
            if instance_ctx do
              elem(instance_ctx.keydir_refs, shard_idx)
            else
              :"keydir_#{shard_idx}"
            end

          instance_name = if instance_ctx, do: instance_ctx.name, else: anchor_state.instance_name

          {zset_score_index_name, zset_score_lookup_name} =
            ZSetIndex.table_names(instance_name, shard_idx)

          {flow_index_name, flow_lookup_name} =
            NativeFlowIndex.table_names(instance_name, shard_idx)

          %{
            instance_ctx: instance_ctx,
            keydir: keydir,
            index: shard_idx,
            data_dir: data_dir,
            shard_data_path: shard_data_path,
            active_file_path: file_path,
            active_file_id: file_id,
            zset_score_index_name: zset_score_index_name,
            zset_score_lookup_name: zset_score_lookup_name,
            compound_member_index_name: CompoundMemberIndex.table_name(instance_name, shard_idx),
            compound_revision_index_name:
              CompoundRevisionIndex.table_name(instance_name, shard_idx),
            flow_index_name: flow_index_name,
            flow_lookup_name: flow_lookup_name
          }
        end
      end

      defp cross_shard_route_key(%{slot_map: _} = instance_ctx, key, _default_idx) do
        Router.shard_for(instance_ctx, key)
      end

      defp cross_shard_route_key(_instance_ctx, _key, default_idx), do: default_idx

      defp cross_shard_state_for_key(anchor_state, key) when is_binary(key) do
        instance_ctx = cross_shard_instance_ctx(anchor_state)

        shard_idx =
          if instance_ctx, do: Router.shard_for(instance_ctx, key), else: anchor_state.shard_index

        cross_shard_state_for_index(anchor_state, shard_idx, instance_ctx)
      end

      defp cross_shard_state_for_index(anchor_state, shard_idx)
           when is_integer(shard_idx) and shard_idx >= 0 do
        cross_shard_state_for_index(
          anchor_state,
          shard_idx,
          cross_shard_instance_ctx(anchor_state)
        )
      end

      defp cross_shard_state_for_index(anchor_state, shard_idx, instance_ctx) do
        ctx = cross_shard_ctx(anchor_state, shard_idx, anchor_state.data_dir, instance_ctx)

        Map.merge(anchor_state, %{
          shard_index: ctx.index,
          ets: ctx.keydir,
          shard_data_path: ctx.shard_data_path,
          shard_data_path_expanded: Path.expand(ctx.shard_data_path),
          active_file_path: ctx.active_file_path,
          active_file_id: ctx.active_file_id,
          zset_score_index_name: ctx.zset_score_index_name,
          zset_score_lookup_name: ctx.zset_score_lookup_name,
          flow_index_name: ctx.flow_index_name,
          flow_lookup_name: ctx.flow_lookup_name,
          flow_lmdb_path: Ferricstore.Flow.LMDB.path(ctx.shard_data_path),
          flow_lmdb_mirror?: false
        })
      end

      defp cross_shard_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx} = state) do
        if instance_data_path?(ctx, state), do: ctx, else: nil
      end

      defp cross_shard_instance_ctx(%{instance_ctx: ctx}) when is_map(ctx) do
        if Map.has_key?(ctx, :shard_count) and Map.has_key?(ctx, :keydir_refs) do
          ctx
        else
          nil
        end
      end

      defp cross_shard_instance_ctx(state) do
        case instance_ctx_for_state(state) do
          %FerricStore.Instance{} = ctx ->
            if instance_data_path?(ctx, state), do: ctx, else: nil

          ctx when is_map(ctx) ->
            if Map.has_key?(ctx, :shard_count) and Map.has_key?(ctx, :keydir_refs) do
              ctx
            else
              nil
            end

          _ ->
            nil
        end
      end

      defp sm_tx_pending_meta(key) do
        pending = Process.get(:tx_pending_values, %{})
        expiry_context = ExpiryContext.capture()

        case Map.get(pending, key) do
          {value, exp} when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live ->
                {value, exp}

              :expired ->
                sm_tx_drop_pending(key)
                nil

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                nil
            end

          nil ->
            nil
        end
      end

      defp normalize_get_value(nil), do: nil
      defp normalize_get_value(value) when is_integer(value), do: Integer.to_string(value)

      defp normalize_get_value(value) when is_float(value),
        do: Ferricstore.Store.ValueCodec.format_float(value)

      defp normalize_get_value(value), do: value

      defp sm_tx_put_pending(key, value, expire_at_ms) do
        pending = Process.get(:tx_pending_values, %{})
        Process.put(:tx_pending_values, Map.put(pending, key, {value, expire_at_ms}))

        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, key) do
          Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
        end
      end

      defp sm_tx_drop_pending(key) do
        pending = Process.get(:tx_pending_values, %{})
        Process.put(:tx_pending_values, Map.delete(pending, key))
      end

      defp sm_merge_tx_pending_prefix(results, prefix) do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())
        expiry_context = ExpiryContext.capture()
        prefix_len = byte_size(prefix)

        base =
          results
          |> Enum.reject(fn {field, _value} -> MapSet.member?(deleted, prefix <> field) end)
          |> Map.new()

        Process.get(:tx_pending_values, %{})
        |> Enum.reduce(base, fn
          {key, {value, exp}}, acc when is_binary(key) and byte_size(key) >= prefix_len ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live ->
                if String.starts_with?(key, prefix) and not MapSet.member?(deleted, key) do
                  field =
                    case :binary.split(key, <<0>>) do
                      [_pre, sub] -> sub
                      _ -> key
                    end

                  Map.put(acc, field, value)
                else
                  acc
                end

              :expired ->
                acc

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                acc
            end

          _other, acc ->
            acc
        end)
        |> Map.to_list()
        |> Enum.sort_by(fn {field, _value} -> field end)
      end

      # Reads a value from a shard's keydir ETS table with cold-read fallback.
      defp cross_shard_ets_read(ctx, key) do
        cross_shard_ets_read_from_path(ctx, key, ctx.shard_data_path)
      end

      defp cross_shard_ets_exists?(ctx, key) do
        expiry_context = ExpiryContext.capture()

        try do
          case :ets.lookup(ctx.keydir, key) do
            [{^key, value, exp, _lfu, _fid, _off, _vsize} = entry]
            when value != nil and is_integer(exp) and exp >= 0 ->
              case ExpiryContext.classify(expiry_context, exp) do
                :live ->
                  true

                :expired ->
                  cross_shard_delete_keydir_entry(ctx, entry)
                  false

                {:unsafe, reason} ->
                  record_state_read_failure(reason)
                  false
              end

            [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
            when is_integer(exp) and exp >= 0 ->
              case ExpiryContext.classify(expiry_context, exp) do
                :live
                when valid_cold_location(fid, off, vsize) or
                       valid_waraft_segment_location(fid, off, vsize) ->
                  true

                :live ->
                  record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
                  false

                :expired ->
                  cross_shard_delete_keydir_entry(ctx, entry)
                  false

                {:unsafe, reason} ->
                  record_state_read_failure(reason)
                  false
              end

            [entry] ->
              record_state_read_failure({:invalid_keydir_entry, key, entry})
              false

            [] ->
              false
          end
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_exists)
            record_state_read_failure(:keydir_unavailable)
            false
        end
      end

      defp cross_shard_ets_read_from_path(ctx, key, data_path) do
        expiry_context = ExpiryContext.capture()

        try do
          case :ets.lookup(ctx.keydir, key) do
            [{^key, value, exp, _lfu, fid, off, vsize} = entry]
            when is_integer(exp) and exp >= 0 ->
              case ExpiryContext.classify(expiry_context, exp) do
                :live ->
                  cross_shard_live_value(ctx, data_path, key, value, fid, off, vsize)

                :expired ->
                  cross_shard_delete_keydir_entry(ctx, entry)
                  nil

                {:unsafe, reason} ->
                  record_state_read_failure(reason)
                  nil
              end

            [entry] ->
              record_state_read_failure({:invalid_keydir_entry, key, entry})
              nil

            [] ->
              nil
          end
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_get)
            record_state_read_failure(:keydir_unavailable)
            nil
        end
      end

      defp cross_shard_live_value(_ctx, _data_path, _key, value, _fid, _off, _vsize)
           when value != nil,
           do: value

      defp cross_shard_live_value(ctx, data_path, key, nil, fid, off, vsize)
           when valid_cold_location(fid, off, vsize) or
                  valid_waraft_segment_location(fid, off, vsize) do
        case cross_shard_read_cold_value(ctx, data_path, key, fid, off, vsize) do
          {:ok, value} when is_binary(value) ->
            materialize_cross_shard_cold_value(ctx, value)

          {:ok, invalid} ->
            record_cross_shard_read_failure({:invalid_value, invalid})

          {:error, reason} ->
            record_cross_shard_read_failure(reason)

          :not_found ->
            record_cross_shard_read_failure(:not_found)

          invalid ->
            record_cross_shard_read_failure({:invalid_cold_read_result, invalid})
        end
      end

      defp cross_shard_live_value(_ctx, _data_path, _key, nil, fid, off, vsize) do
        record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
        nil
      end

      # Reads value + expire_at_ms from a shard's keydir ETS table.
      defp cross_shard_ets_read_meta(ctx, key) do
        cross_shard_ets_read_meta_from_path(ctx, key, ctx.shard_data_path)
      end

      defp materialize_cross_shard_cold_value(ctx, value) do
        threshold = BlobValue.threshold(Map.get(ctx, :instance_ctx))

        case BlobValue.maybe_materialize(ctx.data_dir, ctx.index, threshold, value) do
          {:ok, materialized} -> materialized
          {:error, reason} -> record_cross_shard_read_failure({:blob_ref_unavailable, reason})
        end
      end

      defp record_cross_shard_read_failure(reason) do
        record_state_read_failure({:cold_value_unavailable, reason})
        nil
      end

      defp cross_shard_read_cold_value(_ctx, data_path, key, fid, off, value_size)
           when valid_cold_location(fid, off, value_size) do
        path = sm_file_path_from_path(data_path, fid)
        read_cold_async(path, off, key)
      end

      defp cross_shard_read_cold_value(ctx, _data_path, key, fid, _off, value_size)
           when valid_waraft_segment_location(fid, 0, value_size) do
        Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
          ctx,
          ctx.index,
          fid,
          key
        )
      end

      defp cross_shard_read_cold_value(_ctx, _data_path, _key, _fid, _off, _value_size), do: :miss

      defp cross_shard_ets_read_meta_from_path(ctx, key, data_path) do
        expiry_context = ExpiryContext.capture()

        try do
          case :ets.lookup(ctx.keydir, key) do
            [{^key, value, exp, _lfu, fid, off, vsize} = entry]
            when is_integer(exp) and exp >= 0 ->
              case ExpiryContext.classify(expiry_context, exp) do
                :live ->
                  case cross_shard_live_value(ctx, data_path, key, value, fid, off, vsize) do
                    nil -> nil
                    materialized -> {materialized, exp}
                  end

                :expired ->
                  cross_shard_delete_keydir_entry(ctx, entry)
                  nil

                {:unsafe, reason} ->
                  record_state_read_failure(reason)
                  nil
              end

            [entry] ->
              record_state_read_failure({:invalid_keydir_entry, key, entry})
              nil

            [] ->
              nil
          end
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_get_meta)
            record_state_read_failure(:keydir_unavailable)
            nil
        end
      end

      defp cross_shard_prefix_scan(ctx, prefix) do
        cross_shard_prefix_scan_from_path(ctx, prefix, ctx.shard_data_path)
      end

      defp cross_shard_prefix_scan_from_path(ctx, prefix, data_path) do
        expiry_context = ExpiryContext.capture()
        prefix_len = byte_size(prefix)

        ms = [
          {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
           ], [:"$_"]}
        ]

        try do
          {tokens, cold_entries, _cold_count} =
            :ets.select(ctx.keydir, ms)
            |> Enum.reduce({[], [], 0}, fn
              {key, value, exp, _lfu, fid, off, vsize} = keydir_entry,
              {tokens, cold_entries, cold_count} ->
                if is_integer(exp) and exp >= 0 do
                  case ExpiryContext.classify(expiry_context, exp) do
                    :live ->
                      cond do
                        value == nil and
                            not valid_cross_shard_cold_location_value?(fid, off, vsize) ->
                          record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
                          {tokens, cold_entries, cold_count}

                        value == nil and valid_cold_location(fid, off, vsize) ->
                          field = sm_prefix_field(key)
                          path = sm_file_path_from_path(data_path, fid)
                          entry = {field, key, {:bitcask, path, off}}
                          {[{:cold, cold_count} | tokens], [entry | cold_entries], cold_count + 1}

                        value == nil and valid_waraft_segment_location(fid, off, vsize) ->
                          field = sm_prefix_field(key)
                          entry = {field, key, {:waraft, fid}}
                          {[{:cold, cold_count} | tokens], [entry | cold_entries], cold_count + 1}

                        true ->
                          {[{:value, {sm_prefix_field(key), value}} | tokens], cold_entries,
                           cold_count}
                      end

                    :expired ->
                      {tokens, cold_entries, cold_count}

                    {:unsafe, reason} ->
                      record_state_read_failure(reason)
                      {tokens, cold_entries, cold_count}
                  end
                else
                  record_state_read_failure({:invalid_keydir_entry, key, keydir_entry})
                  {tokens, cold_entries, cold_count}
                end
            end)

          cold_values =
            cold_entries
            |> Enum.reverse()
            |> cross_shard_read_cold_batch(ctx)
            |> List.to_tuple()

          tokens
          |> Enum.flat_map(fn
            {:value, result} ->
              [result]

            {:cold, index} ->
              case elem(cold_values, index) do
                nil -> []
                result -> [result]
              end
          end)
          |> Enum.sort_by(fn {field, _} -> field end)
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_prefix_scan)
            record_state_read_failure(:keydir_unavailable)
            []
        end
      end

      defp cross_shard_read_cold_batch([], _ctx), do: []

      defp cross_shard_read_cold_batch(entries, ctx) do
        {bitcask_entries, waraft_entries} =
          Enum.split_with(entries, fn
            {_field, _key, {:bitcask, _path, _off}} -> true
            _entry -> false
          end)

        values_by_entry =
          %{}
          |> cross_shard_read_cold_bitcask_values(ctx, bitcask_entries)
          |> cross_shard_read_cold_waraft_values(ctx, waraft_entries)

        Enum.map(entries, fn entry -> Map.get(values_by_entry, entry) end)
      end

      defp cross_shard_read_cold_bitcask_values(acc, _ctx, []), do: acc

      defp cross_shard_read_cold_bitcask_values(acc, ctx, entries) do
        locations =
          Enum.map(entries, fn {_field, key, {:bitcask, path, off}} -> {path, off, key} end)

        values =
          locations
          |> Ferricstore.Store.ColdRead.pread_batch_keyed(@cold_read_timeout_ms)
          |> normalize_state_machine_batch_values(length(entries))

        emit_state_machine_batch_cold_errors(entries, values, fn {_field, _key,
                                                                  {:bitcask, path, _off}} ->
          path
        end)

        Enum.zip(entries, values)
        |> Enum.reduce(acc, fn
          {entry = {_field, _key, _location}, value}, acc when is_binary(value) ->
            case materialize_cross_shard_cold_value(ctx, value) do
              materialized when is_binary(materialized) ->
                Map.put(acc, entry, cross_shard_cold_result(entry, materialized))

              _failed ->
                acc
            end

          {_entry, {:error, reason}}, acc ->
            record_state_read_failure({:cold_value_unavailable, reason})
            acc

          {_entry, invalid}, acc ->
            record_state_read_failure({:invalid_cold_read_result, invalid})
            acc
        end)
      end

      defp cross_shard_read_cold_waraft_values(acc, _ctx, []), do: acc

      defp cross_shard_read_cold_waraft_values(acc, ctx, entries) do
        entries
        |> Enum.group_by(fn {_field, _key, {:waraft, fid}} -> fid end)
        |> Enum.reduce(acc, fn {fid, grouped}, acc ->
          keys = Enum.map(grouped, fn {_field, key, _location} -> key end)

          case Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                 ctx,
                 ctx.index,
                 fid,
                 keys
               ) do
            {:ok, values_by_key} when is_map(values_by_key) ->
              Enum.reduce(grouped, acc, fn entry = {_field, key, _location}, acc ->
                case Map.fetch(values_by_key, key) do
                  {:ok, value} when is_binary(value) ->
                    case materialize_cross_shard_cold_value(ctx, value) do
                      materialized when is_binary(materialized) ->
                        Map.put(acc, entry, cross_shard_cold_result(entry, materialized))

                      _failed ->
                        acc
                    end

                  _missing ->
                    record_state_read_failure({:waraft_value_unavailable, :not_found})
                    acc
                end
              end)

            {:error, reason} ->
              record_state_read_failure({:waraft_value_unavailable, reason})
              acc

            invalid ->
              record_state_read_failure({:invalid_waraft_read_result, invalid})
              acc
          end
        end)
      end

      defp cross_shard_cold_result({field, _key, _location}, value), do: {field, value}

      defp normalize_state_machine_batch_values({:ok, values}, count)
           when is_list(values) and length(values) == count,
           do: values

      defp normalize_state_machine_batch_values({:ok, _bad_values}, count),
        do: List.duplicate({:error, :batch_result_length_mismatch}, count)

      defp normalize_state_machine_batch_values({:error, reason}, count),
        do: List.duplicate({:error, reason}, count)

      defp emit_state_machine_batch_cold_errors(entries, values, path_fun) do
        entries
        |> Enum.zip(values)
        |> Enum.reduce(%{}, fn
          {entry, {:error, raw_reason}}, acc ->
            path = path_fun.(entry)
            Map.update(acc, {path, raw_reason}, 1, &(&1 + 1))

          {_entry, _value}, acc ->
            acc
        end)
        |> Enum.each(fn {{path, raw_reason}, count} ->
          ColdRead.emit_pread_error(path, raw_reason, count)
        end)
      end

      defp sm_prefix_field(key) do
        case :binary.split(key, <<0>>) do
          [_pre, sub] -> sub
          _ -> key
        end
      end

      defp cross_shard_compound_read(ctx, redis_key, compound_key) do
        case sm_tx_pending_meta(compound_key) do
          {value, _exp} ->
            value

          nil ->
            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil -> cross_shard_ets_read(ctx, compound_key)
              dedicated_path -> cross_shard_ets_read_from_path(ctx, compound_key, dedicated_path)
            end
        end
      end

      defp cross_shard_compound_read_meta(ctx, redis_key, compound_key) do
        case sm_tx_pending_meta(compound_key) do
          nil ->
            case promoted_compound_path(ctx, redis_key, compound_key) do
              nil ->
                cross_shard_ets_read_meta(ctx, compound_key)

              dedicated_path ->
                cross_shard_ets_read_meta_from_path(ctx, compound_key, dedicated_path)
            end

          meta ->
            meta
        end
      end

      defp cross_shard_compound_batch_read(ctx, redis_key, compound_keys) do
        ctx
        |> cross_shard_compound_batch_read_meta(redis_key, compound_keys)
        |> Enum.map(fn
          {value, _exp} -> value
          nil -> nil
        end)
      end

      defp cross_shard_batch_read(ctx, keys) do
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        entries =
          keys
          |> Enum.with_index()
          |> Enum.reject(fn {key, _index} -> MapSet.member?(deleted, key) end)

        entries
        |> Enum.map(fn {key, _index} -> key end)
        |> then(&cross_shard_ets_batch_read_meta_from_path(ctx, &1, ctx.shard_data_path))
        |> Enum.map(fn
          {value, _exp} -> value
          nil -> nil
        end)
        |> then(&merge_indexed_values(%{}, entries, &1))
        |> values_for_indexes(keys)
      end

      defp cross_shard_routed_batch_read(keys, ctx_for_key) do
        grouped =
          keys
          |> Enum.with_index()
          |> Enum.reduce(%{}, fn {key, index}, acc ->
            ctx = ctx_for_key.(key)

            Map.update(acc, ctx.index, {ctx, [{key, index}]}, fn {existing_ctx, entries} ->
              {existing_ctx, [{key, index} | entries]}
            end)
          end)

        results =
          Enum.reduce(grouped, %{}, fn {_idx, {ctx, entries}}, acc ->
            ordered = Enum.reverse(entries)
            shard_keys = Enum.map(ordered, fn {key, _index} -> key end)
            shard_values = cross_shard_batch_read(ctx, shard_keys)

            ordered
            |> Enum.zip(shard_values)
            |> Enum.reduce(acc, fn {{_key, index}, value}, inner ->
              Map.put(inner, index, value)
            end)
          end)

        keys
        |> Enum.with_index()
        |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
      end

      defp cross_shard_compound_batch_read_meta(_ctx, _redis_key, []), do: []

      defp cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys) do
        data_path =
          case promoted_compound_path(ctx, redis_key, hd(compound_keys)) do
            nil -> ctx.shard_data_path
            dedicated_path -> dedicated_path
          end

        cross_shard_ets_batch_read_meta_from_path(ctx, compound_keys, data_path)
      end

      defp cross_shard_ets_batch_read_meta_from_path(ctx, keys, data_path) do
        expiry_context = ExpiryContext.capture()

        {warm_results, cold_reads} =
          keys
          |> Enum.with_index()
          |> Enum.reduce({%{}, []}, fn {key, index}, {results, cold} ->
            case sm_tx_pending_meta(key) do
              {value, exp} ->
                {Map.put(results, index, {value, exp}), cold}

              nil ->
                cross_shard_collect_batch_ets(
                  ctx,
                  key,
                  index,
                  data_path,
                  expiry_context,
                  results,
                  cold
                )
            end
          end)

        results = cross_shard_read_cold_meta_batch(ctx, warm_results, Enum.reverse(cold_reads))

        keys
        |> Enum.with_index()
        |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
      end

      defp cross_shard_collect_batch_ets(
             ctx,
             key,
             index,
             data_path,
             expiry_context,
             results,
             cold
           ) do
        case :ets.lookup(ctx.keydir, key) do
          [{^key, value, exp, _lfu, _fid, _off, _vsize} = entry]
          when value != nil and is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live ->
                {Map.put(results, index, {value, exp}), cold}

              :expired ->
                cross_shard_delete_keydir_entry(ctx, entry)
                {results, cold}

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                {results, cold}
            end

          [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
          when is_integer(exp) and exp >= 0 ->
            case ExpiryContext.classify(expiry_context, exp) do
              :live when valid_cold_location(fid, off, vsize) ->
                path = sm_file_path_from_path(data_path, fid)
                {results, [{index, key, {:bitcask, path, off}, exp} | cold]}

              :live when valid_waraft_segment_location(fid, off, vsize) ->
                {results, [{index, key, {:waraft, fid}, exp} | cold]}

              :live ->
                record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
                {results, cold}

              :expired ->
                cross_shard_delete_keydir_entry(ctx, entry)
                {results, cold}

              {:unsafe, reason} ->
                record_state_read_failure(reason)
                {results, cold}
            end

          _ ->
            {results, cold}
        end
      rescue
        ArgumentError ->
          record_state_read_failure(:keydir_unavailable)
          {results, cold}
      end

      defp cross_shard_read_cold_meta_batch(_ctx, results, []), do: results

      defp cross_shard_read_cold_meta_batch(ctx, results, cold_reads) do
        {bitcask_reads, waraft_reads} =
          Enum.split_with(cold_reads, fn
            {_index, _key, {:bitcask, _path, _off}, _exp} -> true
            _read -> false
          end)

        results
        |> cross_shard_read_cold_meta_bitcask_batch(ctx, bitcask_reads)
        |> cross_shard_read_cold_meta_waraft_batch(ctx, waraft_reads)
      end

      defp cross_shard_read_cold_meta_bitcask_batch(results, _ctx, []), do: results

      defp cross_shard_read_cold_meta_bitcask_batch(results, ctx, cold_reads) do
        locations =
          Enum.map(cold_reads, fn {_index, key, {:bitcask, path, off}, _exp} ->
            {path, off, key}
          end)

        values =
          locations
          |> Ferricstore.Store.ColdRead.pread_batch_keyed(@cold_read_timeout_ms)
          |> normalize_state_machine_batch_values(length(cold_reads))

        emit_state_machine_batch_cold_errors(cold_reads, values, fn
          {_index, _key, {:bitcask, path, _off}, _exp} -> path
        end)

        cold_reads
        |> Enum.zip(values)
        |> Enum.reduce(results, fn
          {{index, _key, _location, exp}, value}, acc when is_binary(value) ->
            case materialize_cross_shard_cold_value(ctx, value) do
              materialized when is_binary(materialized) ->
                Map.put(acc, index, {materialized, exp})

              _failed ->
                acc
            end

          {_read, {:error, reason}}, acc ->
            record_state_read_failure({:cold_value_unavailable, reason})
            acc

          {_read, invalid}, acc ->
            record_state_read_failure({:invalid_cold_read_result, invalid})
            acc
        end)
      end

      defp cross_shard_read_cold_meta_waraft_batch(results, _ctx, []), do: results

      defp cross_shard_read_cold_meta_waraft_batch(results, ctx, cold_reads) do
        cold_reads
        |> Enum.group_by(fn {_index, _key, {:waraft, fid}, _exp} -> fid end)
        |> Enum.reduce(results, fn {fid, grouped}, acc ->
          keys = Enum.map(grouped, fn {_index, key, _location, _exp} -> key end)

          case Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                 ctx,
                 ctx.index,
                 fid,
                 keys
               ) do
            {:ok, values_by_key} when is_map(values_by_key) ->
              Enum.reduce(grouped, acc, fn {index, key, _location, exp}, acc ->
                case Map.fetch(values_by_key, key) do
                  {:ok, value} when is_binary(value) ->
                    case materialize_cross_shard_cold_value(ctx, value) do
                      materialized when is_binary(materialized) ->
                        Map.put(acc, index, {materialized, exp})

                      _failed ->
                        acc
                    end

                  _missing ->
                    record_state_read_failure({:waraft_value_unavailable, :not_found})
                    acc
                end
              end)

            {:error, reason} ->
              record_state_read_failure({:waraft_value_unavailable, reason})
              acc

            invalid ->
              record_state_read_failure({:invalid_waraft_read_result, invalid})
              acc
          end
        end)
      end

      defp cross_shard_compound_scan(ctx, redis_key, prefix) do
        results =
          case promoted_compound_path(ctx, redis_key, prefix) do
            nil -> cross_shard_prefix_scan(ctx, prefix)
            dedicated_path -> cross_shard_prefix_scan_from_path(ctx, prefix, dedicated_path)
          end

        sm_merge_tx_pending_prefix(results, prefix)
      end

      defp cross_shard_zset_index_read(ctx, redis_key, fun) do
        cond do
          not sm_zset_index_clean?() ->
            :unavailable

          not zset_index_tables?(ctx) ->
            :unavailable

          true ->
            case cross_shard_zset_index_state(ctx, redis_key) do
              {:ok, state} -> fun.(state)
              {:error, {:storage_read_failed, _reason}} = failure -> failure
            end
        end
      end

      defp cross_shard_zset_index_state(ctx, redis_key) do
        prefix = CompoundKey.zset_prefix(redis_key)
        data_path = promoted_compound_path(ctx, redis_key, prefix) || ctx.shard_data_path

        state = %{
          keydir: ctx.keydir,
          shard_data_path: ctx.shard_data_path,
          zset_score_index: ctx.zset_score_index_name,
          zset_score_lookup: ctx.zset_score_lookup_name,
          zset_index_ready: MapSet.new()
        }

        ZSetIndex.ensure(state, redis_key, prefix, data_path)
      end

      defp zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup})
           when is_atom(index) and is_atom(lookup) do
        :ets.info(index) != :undefined and :ets.info(lookup) != :undefined
      end

      defp zset_index_tables?(_ctx), do: false

      defp sm_zset_index_clean? do
        map_size(Process.get(:tx_pending_values, %{})) == 0 and
          MapSet.size(Process.get(:tx_deleted_keys, MapSet.new())) == 0
      end

      defp promoted_compound_path(ctx, redis_key, compound_key_or_prefix) do
        Promotion.await_compaction_latch(ctx, redis_key)

        case compound_type_from_key(compound_key_or_prefix) do
          nil ->
            nil

          type ->
            promoted_catalog_path(ctx, redis_key) || promoted_marker_path(ctx, redis_key, type)
        end
      end

      defp promoted_catalog_path(ctx, redis_key) do
        if promoted_instance_removal_pending?(redis_key) do
          nil
        else
          case Map.get(Map.get(ctx, :promoted_instances, %{}), redis_key) do
            %{path: path} when is_binary(path) -> path
            path when is_binary(path) -> path
            _missing -> nil
          end
        end
      end

      defp promoted_instance_removal_pending?(redis_key) do
        case Process.get(:sm_pending_compound_promotion_removals) do
          %MapSet{} = removals -> MapSet.member?(removals, redis_key)
          _not_applying -> false
        end
      end

      defp promoted_marker_path(ctx, redis_key, :collection_metadata) do
        marker_key = Promotion.marker_key(redis_key)
        marker_ctx = promoted_marker_ctx(ctx)

        type =
          case cross_shard_ets_read(marker_ctx, marker_key) do
            "hash" -> :hash
            "set" -> :set
            "zset" -> :zset
            _other -> nil
          end

        if type do
          Promotion.dedicated_path(
            promoted_data_dir(ctx),
            ctx_index(ctx),
            type,
            redis_key
          )
        end
      end

      defp promoted_marker_path(ctx, redis_key, type) do
        marker_key = Promotion.marker_key(redis_key)
        marker_ctx = promoted_marker_ctx(ctx)
        expected_type = Atom.to_string(type)

        case cross_shard_ets_read(marker_ctx, marker_key) do
          ^expected_type ->
            Promotion.dedicated_path(
              promoted_data_dir(ctx),
              ctx_index(ctx),
              type,
              redis_key
            )

          nil ->
            if live_promotion_marker_entry?(marker_ctx.keydir, marker_key) do
              Promotion.dedicated_path(
                promoted_data_dir(ctx),
                ctx_index(ctx),
                type,
                redis_key
              )
            end

          _other_type ->
            nil
        end
      end

      defp promoted_marker_ctx(%{keydir: _keydir} = ctx), do: ctx
      defp promoted_marker_ctx(%{ets: keydir} = ctx), do: Map.put(ctx, :keydir, keydir)

      defp live_promotion_marker_entry?(keydir, marker_key) do
        now = apply_now_ms()

        case :ets.lookup(keydir, marker_key) do
          [{^marker_key, _value, expire_at_ms, _lfu, _fid, _offset, _value_size}] ->
            expire_at_ms == 0 or expire_at_ms > now

          _missing ->
            false
        end
      rescue
        ArgumentError -> false
      end

      defp promoted_compound_batch_path(ctx, redis_key, compound_entries) do
        {targets, _cached_types} =
          Enum.reduce(compound_entries, {MapSet.new(), %{}}, fn entry, {targets, cached_types} ->
            compound_key = compound_batch_key(entry)
            type = compound_type_from_key(compound_key)

            case Map.fetch(cached_types, type) do
              {:ok, target} ->
                {MapSet.put(targets, target), cached_types}

              :error ->
                target = promoted_compound_path(ctx, redis_key, compound_key)
                {MapSet.put(targets, target), Map.put(cached_types, type, target)}
            end
          end)

        case MapSet.to_list(targets) do
          [] -> nil
          [target] -> target
          _mixed -> :mixed
        end
      end

      defp compound_batch_key({compound_key, _value, _expire_at_ms}), do: compound_key
      defp compound_batch_key(compound_key) when is_binary(compound_key), do: compound_key

      defp ctx_index(%{index: index}), do: index
      defp ctx_index(%{shard_index: index}), do: index

      defp promoted_data_dir(%{data_dir: data_dir}) do
        cond do
          Ferricstore.FS.dir?(Path.join(data_dir, "dedicated")) ->
            data_dir

          Ferricstore.FS.dir?(Path.join(Path.dirname(data_dir), "dedicated")) ->
            Path.dirname(data_dir)

          true ->
            data_dir
        end
      end

      defp compound_type_from_key("H:" <> _), do: :hash
      defp compound_type_from_key("S:" <> _), do: :set
      defp compound_type_from_key("Z:" <> _), do: :zset
      defp compound_type_from_key("T:" <> _), do: :collection_metadata
      defp compound_type_from_key(_), do: nil

      defp cross_shard_prefix_count(ctx, prefix) do
        prefix_len = byte_size(prefix)
        expiry_context = ExpiryContext.capture()

        ms = [
          {{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
           ], [:"$_"]}
        ]

        try do
          :ets.select(ctx.keydir, ms)
          |> Enum.reduce(
            0,
            fn {key, value, exp, _lfu, fid, off, vsize} = keydir_entry, count ->
              if is_integer(exp) and exp >= 0 do
                case ExpiryContext.classify(expiry_context, exp) do
                  :live ->
                    cond do
                      value != nil ->
                        count + 1

                      valid_cross_shard_cold_location_value?(fid, off, vsize) ->
                        count + 1

                      true ->
                        record_state_read_failure({:invalid_cold_location, {fid, off, vsize}})
                        count
                    end

                  :expired ->
                    count

                  {:unsafe, reason} ->
                    record_state_read_failure(reason)
                    count
                end
              else
                record_state_read_failure({:invalid_keydir_entry, key, keydir_entry})
                count
              end
            end
          )
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_prefix_count)
            record_state_read_failure(:keydir_unavailable)
            0
        end
      end

      defp emit_cross_shard_keydir_unavailable(ctx, request) do
        :telemetry.execute(
          [:ferricstore, :store, :shard_unavailable],
          %{count: 1},
          %{
            request: request,
            reason: :keydir_unavailable,
            shard_index: Map.get(ctx, :index),
            source: :raft_apply
          }
        )
      end

      defp cross_shard_delete_prefix(ctx, prefix, delete_fn) do
        prefix_len = byte_size(prefix)

        ms = [
          {{:"$1", :_, :_, :_, :_, :_, :_},
           [
             {:andalso, {:is_binary, :"$1"},
              {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
               {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
           ], [:"$1"]}
        ]

        try do
          keys = :ets.select(ctx.keydir, ms)
          Enum.each(keys, fn key -> delete_fn.(key) end)
        rescue
          ArgumentError ->
            emit_cross_shard_keydir_unavailable(ctx, :cross_shard_delete_prefix)
            record_state_read_failure(:keydir_unavailable)
            :ok
        end

        :ok
      end

      defp sm_file_path_from_path(data_path, file_id) do
        Path.join(
          data_path,
          "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
        )
      end

      defp valid_cold_location_value?(file_id, offset, value_size) do
        is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
          is_integer(value_size) and value_size >= 0
      end

      defp valid_cross_shard_cold_location_value?(file_id, offset, value_size) do
        valid_cold_location_value?(file_id, offset, value_size) or
          valid_waraft_segment_location_value?(file_id, offset, value_size)
      end
    end
  end
end
