defmodule Ferricstore.Store.Router.Part10 do
  @moduledoc false

  # Extracted from Router: compound_batch_get .. flow_index_count_all
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.HLC
      alias Ferricstore.HyperLogLog, as: HLL
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Raft.ReplyAwaiter
      alias Ferricstore.Stats
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.CompoundCommand
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      @spec compound_type_claim(
              FerricStore.Instance.t(),
              binary(),
              CompoundKey.data_type()
            ) :: :ok | {:ok, :created} | {:error, term()}
      def compound_type_claim(ctx, redis_key, type) do
        idx = shard_for(ctx, redis_key)
        command = CompoundCommand.type_claim(redis_key, type)

        if durable_raft_ctx?(ctx) do
          quorum_write(ctx, idx, command)
        else
          safe_write_call(ctx, idx, command)
        end
      end

      @spec compound_batch_get(FerricStore.Instance.t(), binary(), [binary()]) ::
              [binary() | nil | ReadResult.failure()]
      def compound_batch_get(ctx, redis_key, compound_keys) do
        idx = shard_for(ctx, redis_key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()
        waraft? = selected_waraft_ctx?(ctx)

        if promoted_compound_collection?(keydir, redis_key, now) and
             Enum.any?(compound_keys, &(not shared_log_compound_key?(&1))) do
          case safe_read_call(ctx, idx, {:compound_batch_get, redis_key, compound_keys}) do
            {:ok, values} ->
              normalize_compound_batch_reply(values, length(compound_keys))

            :unavailable ->
              List.duplicate(ReadResult.failure(:shard_unavailable), length(compound_keys))
          end
        else
          {results, {fallback_keys, hot_hits}} =
            Enum.map_reduce(compound_keys, {[], []}, fn compound_key, {fallback_keys, hot_hits} ->
              case ets_get_full(ctx, idx, keydir, compound_key, now) do
                {:hit, value, lfu} ->
                  {{:value, value}, {fallback_keys, [{keydir, compound_key, lfu} | hot_hits]}}

                {:cold, file_id, offset, value_size}
                when valid_cold_location(file_id, offset, value_size) or
                       valid_waraft_segment_location(file_id, offset, value_size) ->
                  case direct_waraft_compound_cold_get(
                         ctx,
                         idx,
                         keydir,
                         compound_key,
                         file_id,
                         offset,
                         value_size,
                         now
                       ) do
                    {:ok, value} -> {{:value, value}, {fallback_keys, hot_hits}}
                    :fallback -> {:fallback, {[compound_key | fallback_keys], hot_hits}}
                  end

                terminal when waraft? and terminal in [:miss, :expired] ->
                  {{:value, nil}, {fallback_keys, hot_hits}}

                :no_table when waraft? ->
                  {{:value, ReadResult.failure(:keydir_unavailable)}, {fallback_keys, hot_hits}}

                {:invalid, entry} ->
                  {{:value, ReadResult.failure({:invalid_keydir_entry, entry})},
                   {fallback_keys, hot_hits}}

                _other ->
                  {:fallback, {[compound_key | fallback_keys], hot_hits}}
              end
            end)

          sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

          fallback_values =
            case fallback_keys do
              [] ->
                []

              keys ->
                pending_keys = Enum.reverse(keys)

                case safe_read_call(ctx, idx, {:compound_batch_get, redis_key, pending_keys}) do
                  {:ok, values} ->
                    normalize_compound_batch_reply(values, length(pending_keys))

                  :unavailable ->
                    List.duplicate(ReadResult.failure(:shard_unavailable), length(pending_keys))
                end
            end

          {values, []} =
            Enum.map_reduce(results, fallback_values, fn
              {:value, value}, remaining -> {value, remaining}
              :fallback, [value | remaining] -> {value, remaining}
            end)

          values
        end
      end

      @doc false
      @spec compound_batch_get_on_route_keys(
              FerricStore.Instance.t(),
              [{binary(), binary()}]
            ) :: [binary() | nil | ReadResult.failure()]
      def compound_batch_get_on_route_keys(_ctx, []), do: []

      def compound_batch_get_on_route_keys(ctx, route_lookup_pairs) do
        indexed_groups =
          route_lookup_pairs
          |> Enum.with_index()
          |> Enum.group_by(
            fn {{route_key, _lookup_key}, _index} -> route_key end,
            fn {{_route_key, lookup_key}, index} -> {lookup_key, index} end
          )

        indexed_values =
          Enum.flat_map(indexed_groups, fn {route_key, group} ->
            lookup_keys = Enum.map(group, &elem(&1, 0))
            values = compound_batch_get(ctx, route_key, lookup_keys)

            values =
              if is_list(values) and length(values) == length(group) do
                values
              else
                List.duplicate(ReadResult.failure(:invalid_shard_batch_reply), length(group))
              end

            Enum.zip_with(group, values, fn {_lookup_key, index}, value -> {index, value} end)
          end)

        indexed_values
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.map(&elem(&1, 1))
      end

      defp retry_or_fallback_compound_get(
             ctx,
             idx,
             keydir,
             redis_key,
             compound_key,
             original_location,
             now
           ) do
        case retry_changed_cold_value(ctx, idx, keydir, compound_key, original_location, now) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, compound_key)

            warm_ets_after_cold_read(
              ctx,
              idx,
              keydir,
              compound_key,
              value,
              retry_file_id,
              retry_offset
            )

            value

          {:hot, value} ->
            value

          :miss ->
            fallback_compound_get(ctx, idx, redis_key, compound_key)
        end
      end

      defp fallback_compound_get(ctx, idx, redis_key, compound_key) do
        case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
          {:ok, value} -> value
          :unavailable -> ReadResult.failure(:shard_unavailable)
        end
      end

      defp direct_waraft_compound_cold_get(
             ctx,
             idx,
             keydir,
             compound_key,
             file_id,
             offset,
             value_size,
             now
           ) do
        case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            {:ok, value}

          _ ->
            case retry_changed_cold_value(
                   ctx,
                   idx,
                   keydir,
                   compound_key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, compound_key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  compound_key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {:ok, value}

              {:hot, value} ->
                {:ok, value}

              :miss ->
                :fallback
            end
        end
      end

      defp read_compound_cold_materialized(
             ctx,
             idx,
             file_id,
             _offset,
             key
           )
           when is_tuple(file_id) and tuple_size(file_id) == 2 and
                  (elem(file_id, 0) == :waraft_segment or
                     elem(file_id, 0) == :waraft_projection or
                     elem(file_id, 0) == :waraft_apply_projection) and
                  is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0,
           do: read_waraft_segment_materialized(ctx, idx, file_id, key)

      defp read_compound_cold_materialized(ctx, idx, file_id, offset, key) do
        path = cold_file_path(ctx, idx, file_id)
        read_cold_materialized(ctx, idx, path, offset, key)
      end

      @spec compound_get_meta(FerricStore.Instance.t(), binary(), binary()) ::
              {binary(), non_neg_integer()} | nil | ReadResult.failure()
      def compound_get_meta(ctx, redis_key, compound_key) do
        idx = shard_for(ctx, redis_key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()

        if promoted_data_compound_key?(keydir, redis_key, compound_key, now) do
          fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
        else
          compound_get_meta_from_keydir(ctx, idx, keydir, redis_key, compound_key, now)
        end
      end

      defp compound_get_meta_from_keydir(ctx, idx, keydir, redis_key, compound_key, now) do
        case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, expire_at_ms, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
            {value, expire_at_ms}

          {:cold, file_id, offset, value_size, expire_at_ms}
          when valid_cold_location(file_id, offset, value_size) or
                 valid_waraft_segment_location(file_id, offset, value_size) ->
            case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
              {:ok, value} when is_binary(value) ->
                Stats.record_cold_read(ctx, compound_key)
                warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
                {value, expire_at_ms}

              _ ->
                retry_or_fallback_compound_get_meta(
                  ctx,
                  idx,
                  keydir,
                  redis_key,
                  compound_key,
                  {file_id, offset, value_size},
                  now
                )
            end

          terminal when terminal in [:miss, :expired] ->
            if selected_waraft_ctx?(ctx) do
              nil
            else
              fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
            end

          :no_table ->
            if selected_waraft_ctx?(ctx) do
              ReadResult.failure(:keydir_unavailable)
            else
              fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
            end

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          _other ->
            fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
        end
      end

      @spec compound_batch_get_meta(FerricStore.Instance.t(), binary(), [binary()]) ::
              [{binary(), non_neg_integer()} | nil | ReadResult.failure()]
      def compound_batch_get_meta(ctx, redis_key, compound_keys) do
        idx = shard_for(ctx, redis_key)
        keydir = resolve_keydir(ctx, idx)
        now = HLC.now_ms()
        waraft? = selected_waraft_ctx?(ctx)

        if promoted_compound_collection?(keydir, redis_key, now) and
             Enum.any?(compound_keys, &(not shared_log_compound_key?(&1))) do
          case safe_read_call(ctx, idx, {:compound_batch_get_meta, redis_key, compound_keys}) do
            {:ok, metas} ->
              normalize_compound_batch_reply(metas, length(compound_keys))

            :unavailable ->
              List.duplicate(ReadResult.failure(:shard_unavailable), length(compound_keys))
          end
        else
          {results, {fallback_keys, hot_hits}} =
            Enum.map_reduce(compound_keys, {[], []}, fn compound_key, {fallback_keys, hot_hits} ->
              case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
                {:hit, value, expire_at_ms, lfu} ->
                  {{:value, {value, expire_at_ms}},
                   {fallback_keys, [{keydir, compound_key, lfu} | hot_hits]}}

                {:cold, file_id, offset, value_size, expire_at_ms}
                when readable_cold_ref?(file_id, offset, value_size) ->
                  case direct_waraft_compound_cold_get_meta(
                         ctx,
                         idx,
                         keydir,
                         compound_key,
                         file_id,
                         offset,
                         value_size,
                         now,
                         expire_at_ms
                       ) do
                    {:ok, meta} -> {{:value, meta}, {fallback_keys, hot_hits}}
                    :fallback -> {:fallback, {[compound_key | fallback_keys], hot_hits}}
                  end

                terminal when waraft? and terminal in [:miss, :expired] ->
                  {{:value, nil}, {fallback_keys, hot_hits}}

                :no_table when waraft? ->
                  {{:value, ReadResult.failure(:keydir_unavailable)}, {fallback_keys, hot_hits}}

                {:invalid, entry} ->
                  {{:value, ReadResult.failure({:invalid_keydir_entry, entry})},
                   {fallback_keys, hot_hits}}

                _other ->
                  {:fallback, {[compound_key | fallback_keys], hot_hits}}
              end
            end)

          sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

          fallback_values =
            case fallback_keys do
              [] ->
                []

              keys ->
                pending_keys = Enum.reverse(keys)

                case safe_read_call(ctx, idx, {:compound_batch_get_meta, redis_key, pending_keys}) do
                  {:ok, metas} ->
                    normalize_compound_batch_reply(metas, length(pending_keys))

                  :unavailable ->
                    List.duplicate(ReadResult.failure(:shard_unavailable), length(pending_keys))
                end
            end

          {values, []} =
            Enum.map_reduce(results, fallback_values, fn
              {:value, value}, remaining -> {value, remaining}
              :fallback, [value | remaining] -> {value, remaining}
            end)

          values
        end
      end

      defp promoted_data_compound_key?(keydir, redis_key, compound_key, now) do
        not shared_log_compound_key?(compound_key) and
          promoted_compound_collection?(keydir, redis_key, now)
      end

      defp promoted_compound_collection?(keydir, redis_key, now) do
        marker_key = Ferricstore.Store.CompoundKey.promotion_marker_key(redis_key)

        case :ets.lookup(keydir, marker_key) do
          [{_, _value, 0, _lfu, _fid, _off, _vsize}] -> true
          [{_, _value, exp, _lfu, _fid, _off, _vsize}] when exp > now -> true
          _ -> false
        end
      rescue
        ArgumentError -> false
      end

      defp normalize_compound_batch_reply(values, expected_count)
           when is_list(values) and is_integer(expected_count) and expected_count >= 0 do
        if compound_batch_reply_exact?(values, expected_count) do
          values
        else
          invalid_compound_batch_reply(expected_count)
        end
      end

      defp normalize_compound_batch_reply(_invalid, expected_count),
        do: invalid_compound_batch_reply(expected_count)

      defp compound_batch_reply_exact?([], 0), do: true

      defp compound_batch_reply_exact?([_value | rest], remaining) when remaining > 0,
        do: compound_batch_reply_exact?(rest, remaining - 1)

      defp compound_batch_reply_exact?(_values, _remaining), do: false

      defp invalid_compound_batch_reply(expected_count) do
        List.duplicate(ReadResult.failure(:invalid_shard_batch_reply), expected_count)
      end

      defp shared_log_compound_key?(<<"PM:", _rest::binary>>), do: true
      defp shared_log_compound_key?(_key), do: false

      defp direct_waraft_compound_cold_get_meta(
             ctx,
             idx,
             keydir,
             compound_key,
             file_id,
             offset,
             value_size,
             now,
             expire_at_ms
           ) do
        case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            {:ok, {value, expire_at_ms}}

          _ ->
            case retry_changed_cold_meta(
                   ctx,
                   idx,
                   keydir,
                   compound_key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, compound_key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  compound_key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {:ok, {value, retry_expire_at_ms}}

              {:hot, value, retry_expire_at_ms} ->
                {:ok, {value, retry_expire_at_ms}}

              :miss ->
                :fallback
            end
        end
      end

      defp retry_or_fallback_compound_get_meta(
             ctx,
             idx,
             keydir,
             redis_key,
             compound_key,
             original_location,
             now
           ) do
        case retry_changed_cold_meta(ctx, idx, keydir, compound_key, original_location, now) do
          {:cold, value, expire_at_ms, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, compound_key)

            warm_ets_after_cold_read(
              ctx,
              idx,
              keydir,
              compound_key,
              value,
              retry_file_id,
              retry_offset
            )

            {value, expire_at_ms}

          {:hot, value, expire_at_ms} ->
            {value, expire_at_ms}

          :miss ->
            fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
        end
      end

      defp fallback_compound_get_meta(ctx, idx, redis_key, compound_key) do
        case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
          {:ok, meta} -> meta
          :unavailable -> ReadResult.failure(:shard_unavailable)
        end
      end

      @spec compound_put(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              binary(),
              non_neg_integer()
            ) ::
              :ok | {:error, term()}
      def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
        idx = shard_for(ctx, redis_key)

        if durable_raft_ctx?(ctx) do
          quorum_write(ctx, idx, CompoundCommand.put(compound_key, value, expire_at_ms))
        else
          safe_write_call(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
        end
      end

      @spec compound_batch_put(
              FerricStore.Instance.t(),
              binary(),
              [{binary(), binary(), non_neg_integer()}]
            ) :: :ok | {:error, term()}
      def compound_batch_put(_ctx, _redis_key, []), do: :ok

      def compound_batch_put(ctx, redis_key, entries) do
        idx = shard_for(ctx, redis_key)

        if durable_raft_ctx?(ctx) do
          ctx
          |> quorum_write(idx, CompoundCommand.batch_put(redis_key, entries))
          |> normalize_compound_batch_write_result(length(entries))
        else
          safe_write_call(ctx, idx, {:compound_batch_put, redis_key, entries})
        end
      end

      @spec compound_delete(FerricStore.Instance.t(), binary(), binary()) ::
              :ok | {:error, term()}
      def compound_delete(ctx, redis_key, compound_key) do
        idx = shard_for(ctx, redis_key)

        if durable_raft_ctx?(ctx) do
          quorum_write(ctx, idx, CompoundCommand.delete(compound_key))
        else
          safe_write_call(ctx, idx, {:compound_delete, redis_key, compound_key})
        end
      end

      @spec compound_batch_delete(FerricStore.Instance.t(), binary(), [binary()]) ::
              :ok | {:error, term()}
      def compound_batch_delete(_ctx, _redis_key, []), do: :ok

      def compound_batch_delete(ctx, redis_key, compound_keys) do
        idx = shard_for(ctx, redis_key)

        if durable_raft_ctx?(ctx) do
          ctx
          |> quorum_write(idx, CompoundCommand.batch_delete(redis_key, compound_keys))
          |> normalize_compound_batch_write_result(length(compound_keys))
        else
          safe_write_call(ctx, idx, {:compound_batch_delete, redis_key, compound_keys})
        end
      end

      @doc false
      def __normalize_compound_batch_write_result_for_test__(result, expected_count),
        do: normalize_compound_batch_write_result(result, expected_count)

      defp normalize_compound_batch_write_result(results, expected_count)
           when is_list(results),
           do: CompoundCommand.normalize_batch_reply({:ok, results}, expected_count)

      defp normalize_compound_batch_write_result(result, expected_count),
        do: CompoundCommand.normalize_batch_reply(result, expected_count)

      defp origin_compound_get(ctx, idx, keydir, compound_key) do
        now = HLC.now_ms()

        case ets_get_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, _lfu} ->
            value

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            case read_cold_materialized(ctx, idx, path, offset, compound_key) do
              {:ok, value} when is_binary(value) ->
                warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
                value

              _ ->
                case retry_changed_cold_value(
                       ctx,
                       idx,
                       keydir,
                       compound_key,
                       {file_id, offset, value_size},
                       now
                     ) do
                  {:cold, value, retry_file_id, retry_offset} ->
                    warm_ets_after_cold_read(
                      ctx,
                      idx,
                      keydir,
                      compound_key,
                      value,
                      retry_file_id,
                      retry_offset
                    )

                    value

                  {:hot, value} ->
                    value

                  :miss ->
                    nil
                end
            end

          _ ->
            nil
        end
      end

      @spec compound_scan(FerricStore.Instance.t(), binary(), binary()) ::
              [{binary(), binary()}] | ReadResult.failure()
      def compound_scan(ctx, redis_key, prefix) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          idx
          |> direct_compound_scan(ctx, prefix)
          |> ReadResult.map_success(&Enum.sort_by(&1, fn {field, _value} -> field end))
        else
          case safe_read_call(ctx, idx, {:compound_scan, redis_key, prefix}) do
            {:ok, results} -> results
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      @spec compound_scan_raw(FerricStore.Instance.t(), binary(), binary()) ::
              [{binary(), binary()}] | ReadResult.failure()
      def compound_scan_raw(ctx, redis_key, prefix) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          direct_compound_scan(idx, ctx, prefix)
        else
          case safe_read_call(ctx, idx, {:compound_scan, redis_key, prefix}) do
            {:ok, results} -> results
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      @doc false
      @spec compound_scan_raw_bounded(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              map()
            ) :: term()
      def compound_scan_raw_bounded(ctx, redis_key, prefix, limits) when is_map(limits) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          state = direct_compound_read_state(ctx, idx)

          data_path =
            Ferricstore.Store.Shard.Compound.Promoted.promoted_store(state, redis_key) ||
              state.shard_data_path

          Ferricstore.Store.Shard.ETS.prefix_scan_entries_bounded(
            state,
            prefix,
            data_path,
            limits
          )
        else
          case safe_read_call(
                 ctx,
                 idx,
                 {:compound_scan_bounded, redis_key, prefix, limits}
               ) do
            {:ok, results} -> results
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      @doc false
      @spec compound_scan_page(
              FerricStore.Instance.t(),
              binary(),
              binary(),
              0 | {:after, binary()},
              pos_integer(),
              binary() | nil,
              boolean()
            ) ::
              {:ok, {0 | {:after, binary()}, [{binary(), binary() | nil}]}}
              | ReadResult.failure()
      def compound_scan_page(
            ctx,
            redis_key,
            prefix,
            cursor,
            count,
            match_pattern,
            fields_only
          ) do
        idx = shard_for(ctx, redis_key)

        request =
          {:compound_scan_page, redis_key, prefix, cursor, count, match_pattern, fields_only}

        if selected_waraft_ctx?(ctx) do
          direct_compound_scan_page(
            ctx,
            idx,
            redis_key,
            prefix,
            cursor,
            count,
            match_pattern,
            fields_only
          )
        else
          case safe_read_call(ctx, idx, request) do
            {:ok, result} -> result
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      @spec compound_fields(FerricStore.Instance.t(), binary(), binary()) ::
              [binary()] | ReadResult.failure()
      def compound_fields(ctx, redis_key, prefix) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          ctx
          |> direct_compound_fields(idx, prefix)
          |> Enum.sort()
        else
          case safe_read_call(ctx, idx, {:compound_fields, redis_key, prefix}) do
            {:ok, fields} -> fields
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      @spec compound_count(FerricStore.Instance.t(), binary(), binary()) ::
              non_neg_integer() | ReadResult.failure()
      def compound_count(ctx, redis_key, prefix) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          ctx
          |> resolve_keydir(idx)
          |> Ferricstore.Store.Shard.ETS.prefix_count_entries(prefix)
        else
          case safe_read_call(ctx, idx, {:compound_count, redis_key, prefix}) do
            {:ok, count} -> count
            :unavailable -> ReadResult.failure(:shard_unavailable)
          end
        end
      end

      defp direct_compound_scan(idx, ctx, prefix) do
        shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

        ctx
        |> direct_compound_read_state(idx)
        |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, shard_data_path)
      end

      defp direct_compound_fields(ctx, idx, prefix) do
        ctx
        |> direct_compound_read_state(idx)
        |> Ferricstore.Store.Shard.ETS.prefix_scan_fields(prefix)
      end

      defp direct_compound_scan_page(
             ctx,
             idx,
             redis_key,
             prefix,
             cursor,
             count,
             match_pattern,
             fields_only
           ) do
        state = direct_compound_read_state(ctx, idx)
        index = state.compound_member_index

        case Ferricstore.Store.Shard.CompoundMemberIndex.scan_page(
               index,
               state,
               prefix,
               cursor,
               count,
               match_pattern
             ) do
          {:ok, {next_cursor, members}} when fields_only ->
            {:ok, {next_cursor, Enum.map(members, &{&1, nil})}}

          {:ok, {next_cursor, members}} ->
            compound_keys = Enum.map(members, &(prefix <> &1))
            values = compound_batch_get(ctx, redis_key, compound_keys)

            cond do
              not is_list(values) or length(values) != length(members) ->
                ReadResult.failure(:invalid_compound_scan_page_reply)

              failure = ReadResult.first_failure(values) ->
                failure

              true ->
                pairs =
                  members
                  |> Enum.zip(values)
                  |> Enum.reject(fn {_member, value} -> is_nil(value) end)

                {:ok, {next_cursor, pairs}}
            end

          {:error, reason} ->
            ReadResult.failure({:compound_scan_page_failed, reason})

          :unavailable ->
            ReadResult.failure(:compound_member_index_unavailable)
        end
      end

      defp direct_compound_read_state(ctx, idx) do
        keydir = resolve_keydir(ctx, idx)

        %{
          keydir: keydir,
          ets: keydir,
          compound_member_index:
            Ferricstore.Store.Shard.CompoundMemberIndex.table_name(ctx.name, idx),
          data_dir: ctx.data_dir,
          shard_data_path: Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx),
          index: idx,
          instance_ctx: ctx
        }
      end

      @spec zset_score_range(FerricStore.Instance.t(), binary(), term(), term(), boolean()) ::
              {:ok, [{binary(), float()}]} | :unavailable
      def zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          case direct_zset_score_range(ctx, idx, redis_key, min_bound, max_bound, reverse?) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            result -> {:ok, result}
          end
        else
          ctx
          |> safe_read_call(idx, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
          |> unwrap_zset_index_reply()
        end
      end

      @spec zset_score_range_slice(
              FerricStore.Instance.t(),
              binary(),
              term(),
              term(),
              boolean(),
              non_neg_integer(),
              non_neg_integer() | :all
            ) ::
              {:ok, [{binary(), float()}]} | :unavailable
      def zset_score_range_slice(ctx, redis_key, min_bound, max_bound, reverse?, offset, count) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          case direct_zset_score_range(ctx, idx, redis_key, min_bound, max_bound, reverse?) do
            {:error, {:storage_read_failed, _reason}} = failure ->
              failure

            members ->
              {:ok, apply_zset_slice(members, offset, count)}
          end
        else
          ctx
          |> safe_read_call(
            idx,
            {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count}
          )
          |> unwrap_zset_index_reply()
        end
      end

      @spec zset_score_count(FerricStore.Instance.t(), binary(), term(), term()) ::
              {:ok, non_neg_integer()} | :unavailable
      def zset_score_count(ctx, redis_key, min_bound, max_bound) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          case direct_zset_score_count(ctx, idx, redis_key, min_bound, max_bound) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            result -> {:ok, result}
          end
        else
          ctx
          |> safe_read_call(idx, {:zset_score_count, redis_key, min_bound, max_bound})
          |> unwrap_zset_index_reply()
        end
      end

      @spec zset_score_count_many(FerricStore.Instance.t(), [{binary(), term(), term()}]) ::
              {:ok, [non_neg_integer()]} | :unavailable
      def zset_score_count_many(_ctx, []), do: {:ok, []}

      def zset_score_count_many(ctx, [{first_key, _min, _max} | _] = queries) do
        if selected_waraft_ctx?(ctx) do
          direct_zset_counts(ctx, queries)
        else
          idx = shard_for(ctx, first_key)

          if Enum.all?(queries, fn {key, _min_bound, _max_bound} -> shard_for(ctx, key) == idx end) do
            ctx
            |> safe_read_call(idx, {:zset_score_count_many, queries})
            |> unwrap_zset_index_reply()
          else
            zset_score_count_many_cross_shard(ctx, queries)
          end
        end
      end

      defp zset_score_count_many_cross_shard(ctx, queries) do
        Enum.reduce_while(queries, {:ok, []}, fn {key, min_bound, max_bound}, {:ok, acc} ->
          case zset_score_count(ctx, key, min_bound, max_bound) do
            {:ok, count} -> {:cont, {:ok, [count | acc]}}
            {:error, {:storage_read_failed, _reason}} = failure -> {:halt, failure}
            :unavailable -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          :unavailable -> :unavailable
        end
      end

      @spec zset_score_count_all_many_no_build(FerricStore.Instance.t(), [binary()]) ::
              {:ok, [non_neg_integer()]} | :unavailable
      def zset_score_count_all_many_no_build(_ctx, []), do: {:ok, []}

      def zset_score_count_all_many_no_build(ctx, [first_key | _] = keys) do
        if selected_waraft_ctx?(ctx) do
          queries = Enum.map(keys, &{&1, :neg_inf, :inf})
          direct_zset_counts(ctx, queries)
        else
          idx = shard_for(ctx, first_key)

          if Enum.all?(keys, fn key -> shard_for(ctx, key) == idx end) do
            ctx
            |> safe_read_call(idx, {:zset_score_count_all_many_no_build, keys})
            |> unwrap_zset_index_reply()
          else
            zset_score_count_all_many_no_build_cross_shard(ctx, keys)
          end
        end
      end

      defp zset_score_count_all_many_no_build_cross_shard(ctx, keys) do
        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          idx = shard_for(ctx, key)

          case safe_read_call(ctx, idx, {:zset_score_count_all_many_no_build, [key]}) do
            {:ok, [count]} -> {:cont, {:ok, [count | acc]}}
            :unavailable -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          :unavailable -> :unavailable
        end
      end

      defp direct_zset_counts(ctx, queries) do
        Enum.reduce_while(queries, {:ok, []}, fn {key, min_bound, max_bound}, {:ok, counts} ->
          case direct_zset_score_count(ctx, shard_for(ctx, key), key, min_bound, max_bound) do
            {:error, {:storage_read_failed, _reason}} = failure -> {:halt, failure}
            count -> {:cont, {:ok, [count | counts]}}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end
      end

      @spec flow_index_score_range_slice(
              FerricStore.Instance.t(),
              binary(),
              term(),
              term(),
              boolean(),
              non_neg_integer(),
              non_neg_integer() | :all
            ) :: {:ok, [{binary(), float()}]} | :unavailable
      def flow_index_score_range_slice(ctx, key, min_bound, max_bound, reverse?, offset, count) do
        idx = shard_for(ctx, key)

        if selected_waraft_ctx?(ctx) do
          direct_flow_index_score_range_slice(
            ctx,
            idx,
            key,
            min_bound,
            max_bound,
            reverse?,
            offset,
            count
          )
        else
          ctx
          |> safe_read_call(
            idx,
            {:flow_index_score_range_slice, key, min_bound, max_bound, reverse?, offset, count}
          )
          |> unwrap_zset_index_reply()
        end
      end

      @spec flow_index_rank_range(
              FerricStore.Instance.t(),
              binary(),
              non_neg_integer(),
              non_neg_integer(),
              boolean()
            ) :: {:ok, [{binary(), float()}]} | :unavailable
      def flow_index_rank_range(ctx, key, start_idx, stop_idx, reverse?) do
        idx = shard_for(ctx, key)

        if selected_waraft_ctx?(ctx) do
          direct_flow_index_rank_range(ctx, idx, key, start_idx, stop_idx, reverse?)
        else
          ctx
          |> safe_read_call(idx, {:flow_index_rank_range, key, start_idx, stop_idx, reverse?})
          |> unwrap_zset_index_reply()
        end
      end

      @spec flow_index_rank_range_many(
              FerricStore.Instance.t(),
              [{binary(), non_neg_integer(), non_neg_integer(), boolean()}]
            ) :: {:ok, [[{binary(), float()}]]} | :unavailable
      def flow_index_rank_range_many(_ctx, []), do: {:ok, []}

      def flow_index_rank_range_many(ctx, requests) when is_list(requests) do
        if selected_waraft_ctx?(ctx) do
          direct_flow_index_rank_range_many(ctx, requests)
        else
          requests
          |> Enum.with_index()
          |> Enum.group_by(fn {{key, _start_idx, _stop_idx, _reverse?}, _index} ->
            shard_for(ctx, key)
          end)
          |> Enum.reduce_while({:ok, %{}}, fn {idx, indexed_requests}, {:ok, acc} ->
            shard_requests = Enum.map(indexed_requests, fn {request, _index} -> request end)

            case safe_read_call(ctx, idx, {:flow_index_rank_range_many, shard_requests})
                 |> unwrap_zset_index_reply() do
              {:ok, results} when is_list(results) ->
                if valid_flow_index_rank_batch?(results, length(indexed_requests)) do
                  indexed =
                    indexed_requests
                    |> Enum.zip(results)
                    |> Enum.reduce(acc, fn {{_request, original_index}, result}, next_acc ->
                      Map.put(next_acc, original_index, result)
                    end)

                  {:cont, {:ok, indexed}}
                else
                  {:halt, :unavailable}
                end

              :unavailable ->
                {:halt, :unavailable}

              _other ->
                {:halt, :unavailable}
            end
          end)
          |> case do
            {:ok, indexed} ->
              {:ok, Enum.map(0..(length(requests) - 1)//1, &Map.fetch!(indexed, &1))}

            :unavailable ->
              :unavailable
          end
        end
      end

      @spec flow_index_count_all(FerricStore.Instance.t(), binary()) ::
              {:ok, non_neg_integer()} | :unavailable
      def flow_index_count_all(ctx, key) do
        idx = shard_for(ctx, key)

        if selected_waraft_ctx?(ctx) do
          direct_flow_index_count_all(ctx, idx, key)
        else
          ctx
          |> safe_read_call(idx, {:flow_index_count_all, key})
          |> unwrap_zset_index_reply()
        end
      end
    end
  end
end
