defmodule Ferricstore.Raft.StateMachine.Sections.CompoundApply do
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
        PublicationEpoch,
        Router,
        ValueCodec
      }

      alias Ferricstore.Store.Shard.{CompoundMemberIndex, ZSetIndex}
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

      defp maybe_prepare_delete_batch_fast(state, keys) do
        now_ms = apply_now_ms()

        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          case safe_ets_lookup(state.ets, key) do
            [{^key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}] ->
              {:halt, :fallback}

            [{^key, nil, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
              {:halt, :fallback}

            [{^key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
            when expire_at_ms != 0 and expire_at_ms <= now_ms ->
              {:halt, :fallback}

            [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
            when is_binary(value) ->
              {:cont, {:ok, [{key, prob_file_path_from_delete_value(state, key, value)} | acc]}}

            [] ->
              {:cont, {:ok, [{key, nil} | acc]}}

            _other ->
              {:halt, :fallback}
          end
        end)
      end

      defp prob_file_path_from_delete_value(state, key, value) when is_binary(value) do
        case safe_binary_to_term(value) do
          {:bloom_meta, meta} when is_map(meta) -> prob_path(state, key, "bloom")
          {:cms_meta, meta} when is_map(meta) -> prob_path(state, key, "cms")
          {:cuckoo_meta, meta} when is_map(meta) -> prob_path(state, key, "cuckoo")
          {:topk_meta, meta} when is_map(meta) -> prob_path(state, key, "topk")
          _ -> nil
        end
      end

      defp safe_binary_to_term(value) do
        case Ferricstore.TermCodec.decode(value) do
          {:ok, term} -> term
          {:error, :invalid_external_term} -> :not_term
        end
      end

      defp apply_delete_batch_keys(state, keys) do
        case apply_delete_batch_keys_fast(state, keys) do
          :fallback ->
            Enum.map(keys, fn key ->
              redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

              case check_fetch_or_compute_lock(state, redis_key, nil) do
                :ok -> do_delete(state, key)
                {:error, _reason} = error -> error
              end
            end)

          results ->
            results
        end
      end

      defp apply_compound_batch_put_entries(state, redis_key, entries) do
        cond do
          not valid_compound_put_entries?(entries) ->
            {:error, :invalid_compound_batch_entry}

          not compound_put_entries_for_key?(redis_key, entries) ->
            {:error, :compound_batch_cross_key}

          true ->
            with :ok <- validate_compound_batch_values(state, entries) do
              compound_batch_checked_results(state, redis_key, entries, fn ->
                do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries)
              end)
            end
        end
      end

      defp do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries) do
        with :ok <- validate_compound_batch_target(state, redis_key, entries) do
          case prepare_apply_blob_command(state, {:compound_batch_put, redis_key, entries}) do
            {:ok, {:compound_blob_batch_put, ^redis_key, blob_entries}} ->
              with :ok <- validate_raw_compound_blob_batch_target(state, redis_key, blob_entries),
                   {:ok, prepared_entries} <-
                     prepare_compound_blob_batch_entries(state, blob_entries) do
                do_compound_blob_batch_put(state, redis_key, prepared_entries)
              end

            {:ok, {:compound_batch_put, ^redis_key, ^entries}} ->
              do_compound_batch_put_value_validated(state, redis_key, entries)

            {:ok, _other} ->
              {:error, :invalid_compound_batch_entry}

            {:error, _reason} = error ->
              error
          end
        end
      end

      defp validate_compound_batch_target(_state, _redis_key, []), do: :ok

      defp validate_compound_batch_target(state, redis_key, entries) do
        case compound_batch_put_target(state, redis_key, entries) do
          :mixed -> {:error, :mixed_compound_batch_targets}
          _homogeneous -> :ok
        end
      end

      defp apply_compound_blob_batch_put_entries(state, redis_key, entries) do
        cond do
          not valid_compound_blob_put_entries?(entries) ->
            {:error, :invalid_compound_blob_batch_entry}

          not compound_blob_put_entries_for_key?(redis_key, entries) ->
            {:error, :compound_batch_cross_key}

          true ->
            with :ok <- validate_compound_blob_batch_values(state, entries),
                 :ok <- validate_raw_compound_blob_batch_target(state, redis_key, entries),
                 {:ok, prepared_entries} <- prepare_compound_blob_batch_entries(state, entries) do
              compound_batch_checked_results(state, redis_key, entries, fn ->
                do_compound_blob_batch_put(state, redis_key, prepared_entries)
              end)
            end
        end
      end

      defp validate_compound_batch_values(state, entries) do
        Enum.reduce_while(entries, :ok, fn
          {_compound_key, value, _expire_at_ms}, :ok ->
            case Ferricstore.Raft.ApplyLimits.validate_value(state, value) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
        end)
      end

      defp validate_compound_blob_batch_values(state, entries) do
        Enum.reduce_while(entries, :ok, fn
          {_compound_key, value, _expire_at_ms, :value}, :ok ->
            case Ferricstore.Raft.ApplyLimits.validate_value(state, value) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end

          {_compound_key, encoded_ref, _expire_at_ms, :blob_ref}, :ok ->
            case BlobRef.decode(encoded_ref) do
              {:ok, ref} ->
                case Ferricstore.Raft.ApplyLimits.validate_value_size(state, ref.size) do
                  :ok -> {:cont, :ok}
                  {:error, _reason} = error -> {:halt, error}
                end

              :error ->
                {:halt, {:error, {:blob_ref_unavailable, :invalid_blob_ref}}}
            end
        end)
      end

      defp validate_raw_compound_blob_batch_target(_state, _redis_key, []), do: :ok

      defp validate_raw_compound_blob_batch_target(state, redis_key, [first | rest]) do
        with {:ok, first_key} <- raw_compound_blob_key(first) do
          first_path = promoted_compound_path(state, redis_key, first_key)

          Enum.reduce_while(rest, :ok, fn entry, :ok ->
            case raw_compound_blob_key(entry) do
              {:ok, compound_key} ->
                if promoted_compound_path(state, redis_key, compound_key) == first_path,
                  do: {:cont, :ok},
                  else: {:halt, {:error, :mixed_compound_batch_targets}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end)
        end
      end

      defp raw_compound_blob_key({compound_key, value, expire_at_ms, kind})
           when is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) and
                  expire_at_ms >= 0 and kind in [:value, :blob_ref],
           do: {:ok, compound_key}

      defp raw_compound_blob_key(_invalid),
        do: {:error, :invalid_compound_blob_batch_entry}

      defp prepare_compound_blob_batch_entries(state, entries) do
        entries
        |> Enum.reduce_while({:ok, []}, fn
          {compound_key, value, expire_at_ms, :value}, {:ok, acc}
          when is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 ->
            {:cont, {:ok, [{:value, compound_key, value, expire_at_ms} | acc]}}

          {compound_key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, acc}
          when is_binary(compound_key) and is_binary(encoded_ref) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 ->
            case materialize_blob_ref(state, encoded_ref) do
              {:ok, materialized} ->
                {:cont,
                 {:ok, [{:blob_ref, compound_key, encoded_ref, expire_at_ms, materialized} | acc]}}

              {:error, _reason} = error ->
                {:halt, error}
            end

          _entry, {:ok, _acc} ->
            {:halt, {:error, :invalid_compound_blob_batch_entry}}
        end)
        |> case do
          {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
          {:error, _reason} = error -> error
        end
      end

      defp do_compound_blob_batch_put(_state, _redis_key, []), do: :ok

      defp do_compound_blob_batch_put(state, redis_key, prepared_entries) do
        case compound_blob_batch_target(state, redis_key, prepared_entries) do
          :shared ->
            do_shared_compound_blob_batch_put(state, redis_key, prepared_entries)

          {:promoted, dedicated_path} ->
            do_promoted_compound_blob_batch_put(
              state,
              redis_key,
              prepared_entries,
              dedicated_path
            )

          :mixed ->
            {:error, :mixed_compound_batch_targets}
        end
      end

      defp do_shared_compound_blob_batch_put(state, redis_key, prepared_entries) do
        prepared_entries
        |> Enum.reduce_while(:ok, fn
          {:value, compound_key, value, expire_at_ms}, :ok ->
            case do_compound_put_value_validated(
                   state,
                   redis_key,
                   compound_key,
                   value,
                   expire_at_ms
                 ) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end

          {:blob_ref, compound_key, encoded_ref, expire_at_ms, materialized}, :ok ->
            case do_compound_put_blob_ref_validated(
                   state,
                   redis_key,
                   compound_key,
                   encoded_ref,
                   expire_at_ms,
                   materialized
                 ) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
        end)
      end

      defp do_promoted_compound_blob_batch_put(
             state,
             redis_key,
             prepared_entries,
             dedicated_path
           ) do
        Promotion.await_compaction_latch(state, redis_key)

        active = Promotion.find_active(dedicated_path)
        fid = parse_fid_from_path(active)
        disk_entries = Enum.map(prepared_entries, &compound_blob_disk_entry/1)
        maintenance = promoted_batch_put_maintenance(state, disk_entries)

        case validate_promoted_append_locations(
               append_promoted_batch(active, disk_entries),
               length(prepared_entries)
             ) do
          {:ok, locations} ->
            prepared_entries
            |> Enum.zip(locations)
            |> Enum.each(fn
              {{:value, compound_key, value, expire_at_ms}, {offset, value_size}} ->
                publish_promoted_compound_blob_entry(
                  state,
                  redis_key,
                  compound_key,
                  value,
                  expire_at_ms,
                  fid,
                  offset,
                  value_size
                )

              {{:blob_ref, compound_key, _encoded_ref, expire_at_ms, materialized},
               {offset, value_size}} ->
                publish_promoted_compound_blob_entry(
                  state,
                  redis_key,
                  compound_key,
                  materialized,
                  expire_at_ms,
                  fid,
                  offset,
                  value_size
                )
            end)

            queue_promoted_maintenance_after_flush(redis_key, maintenance)

            queue_promoted_revision_puts_after_flush(
              Map.get(state, :compound_revision_index_name),
              Enum.map(prepared_entries, &compound_blob_key/1)
            )

            :ok

          {:error, _reason} = error ->
            error
        end
      end

      defp publish_promoted_compound_blob_entry(
             state,
             redis_key,
             compound_key,
             logical_value,
             expire_at_ms,
             fid,
             offset,
             value_size
           ) do
        ets_value = value_for_ets(logical_value, hot_cache_threshold(state))
        track_keydir_binary_delta(state, compound_key, ets_value, expire_at_ms)

        :ets.insert(
          state.ets,
          {compound_key, ets_value, expire_at_ms, LFU.initial(), fid, offset, value_size}
        )

        CompoundMemberIndex.put(
          Map.get(state, :compound_member_index_name),
          compound_key,
          expire_at_ms
        )

        sm_tx_put_pending(compound_key, logical_value, expire_at_ms)
        remove_tx_deleted_key(compound_key)
        zset_index_put(state, redis_key, compound_key, logical_value)
      end

      defp compound_blob_batch_target(state, redis_key, [first | rest]) do
        first_path = promoted_compound_path(state, redis_key, compound_blob_key(first))

        if Enum.all?(rest, fn entry ->
             promoted_compound_path(state, redis_key, compound_blob_key(entry)) == first_path
           end) do
          case first_path do
            nil -> :shared
            dedicated_path -> {:promoted, dedicated_path}
          end
        else
          :mixed
        end
      end

      defp compound_blob_key({:value, compound_key, _value, _expire_at_ms}), do: compound_key

      defp compound_blob_key(
             {:blob_ref, compound_key, _encoded_ref, _expire_at_ms, _materialized}
           ),
           do: compound_key

      defp compound_blob_disk_entry({:value, compound_key, value, expire_at_ms}),
        do: {compound_key, to_disk_binary(value), expire_at_ms}

      defp compound_blob_disk_entry(
             {:blob_ref, compound_key, encoded_ref, expire_at_ms, _materialized}
           ),
           do: {compound_key, to_disk_binary(encoded_ref), expire_at_ms}

      defp remove_tx_deleted_key(compound_key) do
        sm_tx_unmark_deleted(compound_key)
      end

      defp do_compound_put_blob_ref_validated(
             state,
             redis_key,
             compound_key,
             encoded_ref,
             expire_at_ms,
             materialized
           ) do
        result =
          case promoted_compound_path(state, redis_key, compound_key) do
            nil ->
              raw_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms, materialized)

            dedicated_path ->
              do_promoted_compound_put(
                state,
                redis_key,
                compound_key,
                encoded_ref,
                expire_at_ms,
                dedicated_path
              )
          end

        if result == :ok do
          zset_index_put(state, redis_key, compound_key, materialized)
        end

        result
      end

      defp apply_compound_batch_delete_keys(state, redis_key, compound_keys) do
        cond do
          not compound_delete_keys_for_key?(redis_key, compound_keys) ->
            {:error, :compound_batch_cross_key}

          true ->
            compound_batch_checked_results(state, redis_key, compound_keys, fn ->
              do_compound_batch_delete(state, redis_key, compound_keys)
            end)
        end
      end

      defp compound_batch_checked_results(state, redis_key, items, fun) do
        case check_fetch_or_compute_lock(state, redis_key, nil) do
          :ok ->
            case fun.() do
              :ok -> List.duplicate(:ok, length(items))
              {:error, _reason} = error -> error
            end

          {:error, :key_locked} = error ->
            List.duplicate(error, length(items))
        end
      end

      defp compound_put_entries_for_key?(redis_key, entries) do
        Enum.all?(entries, fn
          {compound_key, _value, _expire_at_ms} when is_binary(compound_key) ->
            CompoundKey.extract_redis_key(compound_key) == redis_key

          _entry ->
            false
        end)
      end

      defp valid_compound_put_entries?(entries) do
        Enum.all?(entries, fn
          {compound_key, value, expire_at_ms} ->
            is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) and
              expire_at_ms >= 0

          _entry ->
            false
        end)
      end

      defp compound_blob_put_entries_for_key?(redis_key, entries) do
        Enum.all?(entries, fn
          {compound_key, _value_or_ref, _expire_at_ms, kind}
          when is_binary(compound_key) and kind in [:value, :blob_ref] ->
            CompoundKey.extract_redis_key(compound_key) == redis_key

          _entry ->
            false
        end)
      end

      defp valid_compound_blob_put_entries?(entries) do
        Enum.all?(entries, fn
          {compound_key, value_or_ref, expire_at_ms, kind} ->
            is_binary(compound_key) and is_binary(value_or_ref) and is_integer(expire_at_ms) and
              expire_at_ms >= 0 and kind in [:value, :blob_ref]

          _entry ->
            false
        end)
      end

      defp compound_delete_keys_for_key?(redis_key, compound_keys) do
        Enum.all?(compound_keys, fn
          compound_key when is_binary(compound_key) ->
            CompoundKey.extract_redis_key(compound_key) == redis_key

          _key ->
            false
        end)
      end

      defp maybe_queue_origin_pending_put(state, key, value, expire_at_ms) do
        expected_value = value_for_ets(value, hot_cache_threshold(state))

        case safe_ets_lookup(state.ets, key) do
          [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, 0, _vs}]
          when expected_value != nil ->
            queue_pending_put(key, to_disk_binary(value), expire_at_ms)

          _ ->
            :ok
        end
      end

      defp maybe_queue_already_applied_origin_put(
             state,
             key,
             {:put, _key, value, expire_at_ms},
             expected_value,
             expire_at_ms
           ) do
        case :ets.lookup(state.ets, key) do
          [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, _off, _value_size}] ->
            queue_pending_put(key, to_disk_binary(value), expire_at_ms)

          _ ->
            :ok
        end
      end

      defp maybe_queue_already_applied_origin_put(
             _state,
             _key,
             _inner_cmd,
             _expected_value,
             _expire_at_ms
           ) do
        :ok
      end

      defp apply_origin_async_put(state, key, value, expire_at_ms) do
        expected_value = value_for_ets(value, hot_cache_threshold(state))
        disk_value = to_disk_binary(value)

        case :ets.lookup(state.ets, key) do
          [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, 0, _vs}]
          when expected_value != nil ->
            queue_origin_async_put(state, key, value, expire_at_ms)

          [{^key, ^expected_value, ^expire_at_ms, _lfu, fid, off, vs}]
          when fid != :pending and valid_cold_location(fid, off, vs) ->
            if origin_cold_put_already_applied?(state, key, fid, off, vs, disk_value) do
              :ok
            else
              apply_single(state, {:put, key, value, expire_at_ms})
            end

          [{^key, _other_value, _other_exp, _lfu, :pending, _off, _vs}] ->
            queue_origin_async_put(state, key, value, expire_at_ms)

          _ ->
            apply_single(state, {:put, key, value, expire_at_ms})
        end
      end

      defp queue_origin_async_put(state, key, value, expire_at_ms) do
        case maybe_externalize_apply_value(state, value) do
          {:ok, :value, value} ->
            queue_pending_put(key, to_disk_binary(value), expire_at_ms)
            :ok

          {:ok, :blob_ref, encoded_ref, materialized_value} ->
            disk_value = to_disk_binary(encoded_ref)
            record_pending_original(state, key)

            unless standalone_staged_apply?() do
              track_keydir_binary_delta(state, key, nil, expire_at_ms)

              :ets.insert(
                state.ets,
                {key, nil, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_value)}
              )
            end

            queue_pending_put_cold(key, disk_value, expire_at_ms, LFU.initial())
            put_pending_value(key, materialized_value, expire_at_ms)
            Process.put(:sm_pending_fast_staged_put_batch, true)
            :ok

          {:error, _reason} = error ->
            error
        end
      end

      defp origin_cold_put_already_applied?(_state, _key, _fid, _off, value_size, disk_value)
           when value_size != byte_size(disk_value) do
        false
      end

      defp origin_cold_put_already_applied?(state, key, fid, off, _value_size, disk_value) do
        path = sm_file_path(state, fid)

        case read_cold_async(path, off, key) do
          {:ok, ^disk_value} -> true
          _ -> false
        end
      end

      defp origin_command_already_applied?(state, key, inner_cmd, expected_value, expire_at_ms) do
        case :ets.lookup(state.ets, key) do
          [{^key, ^expected_value, ^expire_at_ms, _lfu, _fid, _off, _vs}]
          when expected_value != nil ->
            true

          [{^key, nil, ^expire_at_ms, _lfu, fid, off, vs}]
          when expected_value == nil and valid_cold_location(fid, off, vs) ->
            true

          [{^key, current_value, _current_exp, _lfu, _fid, _off, _vs}] ->
            origin_command_already_in_current_value?(inner_cmd, current_value, expected_value)

          _ ->
            false
        end
      end

      defp origin_replay_decision(
             state,
             key,
             inner_cmd,
             before_value,
             before_expire_at_ms,
             expected_value,
             expire_at_ms
           ) do
        case :ets.lookup(state.ets, key) do
          [{^key, current_value, current_expire_at_ms, _lfu, :pending, _off, _value_size}] ->
            pending_origin_replay_decision(
              inner_cmd,
              current_value,
              current_expire_at_ms,
              before_value,
              before_expire_at_ms,
              expected_value,
              expire_at_ms
            )

          _ ->
            committed_origin_replay_decision(
              state,
              key,
              inner_cmd,
              before_value,
              before_expire_at_ms,
              expected_value,
              expire_at_ms
            )
        end
      end

      defp committed_origin_replay_decision(
             state,
             key,
             inner_cmd,
             before_value,
             before_expire_at_ms,
             expected_value,
             expire_at_ms
           ) do
        case do_get_meta(state, key) do
          {^expected_value, ^expire_at_ms} when expected_value != nil ->
            :already_applied

          {^before_value, ^before_expire_at_ms} when before_value != nil ->
            :apply

          nil when before_value == nil ->
            :apply

          nil when expected_value == nil ->
            :apply_expected

          _other ->
            pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value)
        end
      end

      defp pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value) do
        case :ets.lookup(state.ets, key) do
          [{^key, current_value, current_expire_at_ms, _lfu, :pending, _off, _value_size}] ->
            pending_origin_replay_decision(
              inner_cmd,
              current_value,
              current_expire_at_ms,
              current_value,
              current_expire_at_ms,
              expected_value,
              current_expire_at_ms
            )

          _ ->
            :newer_local_value
        end
      end

      defp pending_origin_replay_decision(
             {:delete, _key},
             current_value,
             current_expire_at_ms,
             before_value,
             before_expire_at_ms,
             nil,
             _expected_expire_at_ms
           )
           when current_value != before_value or current_expire_at_ms != before_expire_at_ms do
        :newer_local_value
      end

      defp pending_origin_replay_decision(
             {:getdel, _key},
             current_value,
             current_expire_at_ms,
             before_value,
             before_expire_at_ms,
             nil,
             _expected_expire_at_ms
           )
           when current_value != before_value or current_expire_at_ms != before_expire_at_ms do
        :newer_local_value
      end

      defp pending_origin_replay_decision(
             {:getset, _key, _new_value},
             current_value,
             current_expire_at_ms,
             _before_value,
             _before_expire_at_ms,
             expected_value,
             expected_expire_at_ms
           )
           when current_value == expected_value and current_expire_at_ms == expected_expire_at_ms do
        :apply_expected
      end

      defp pending_origin_replay_decision(
             _inner_cmd,
             current_value,
             current_expire_at_ms,
             _before_value,
             _before_expire_at_ms,
             expected_value,
             expected_expire_at_ms
           )
           when current_value == expected_value and current_expire_at_ms == expected_expire_at_ms do
        :already_applied
      end

      defp pending_origin_replay_decision(
             inner_cmd,
             current_value,
             _current_expire_at_ms,
             _before_value,
             _before_expire_at_ms,
             expected_value,
             _expected_expire_at_ms
           ) do
        if origin_command_provably_in_current_value?(inner_cmd, current_value, expected_value) do
          :newer_local_value
        else
          # A pending local value has no Raft index attached. If this command type
          # cannot prove that the pending value includes the accepted origin result,
          # materialize the accepted value and let later Ra entries replay in order.
          :apply_expected
        end
      end

      defp apply_origin_checked_expected(
             state,
             key,
             {:getdel, _key},
             before_value,
             nil,
             _expire_at_ms
           ) do
        _ = do_delete(state, key)
        origin_checked_expected_result({:getdel, key}, before_value, nil)
      end

      defp apply_origin_checked_expected(
             state,
             key,
             inner_cmd,
             before_value,
             expected_value,
             expire_at_ms
           ) do
        case expected_value do
          nil ->
            apply_single(state, inner_cmd)

          value ->
            do_put(state, key, value, expire_at_ms)
            origin_checked_expected_result(inner_cmd, before_value, value)
        end
      end

      defp origin_checked_expected_result({:incr, _key, _delta}, _before_value, expected_value) do
        case coerce_integer(expected_value) do
          {:ok, value} -> {:ok, value}
          :error -> :ok
        end
      end

      defp origin_checked_expected_result(
             {:incr_float, _key, _delta},
             _before_value,
             expected_value
           ) do
        case coerce_float(expected_value) do
          {:ok, value} -> {:ok, value}
          :error -> :ok
        end
      end

      defp origin_checked_expected_result({:append, _key, _suffix}, _before_value, expected_value)
           when is_binary(expected_value) do
        {:ok, byte_size(expected_value)}
      end

      defp origin_checked_expected_result(
             {:getset, _key, _new_value},
             before_value,
             _expected_value
           ) do
        before_value
      end

      defp origin_checked_expected_result(
             {:getex, _key, _expire_at_ms},
             _before_value,
             expected_value
           ) do
        expected_value
      end

      defp origin_checked_expected_result(
             {:setrange, _key, _offset, _value},
             _before_value,
             expected_value
           )
           when is_binary(expected_value) do
        {:ok, byte_size(expected_value)}
      end

      defp origin_checked_expected_result(_inner_cmd, _before_value, _expected_value) do
        :ok
      end

      defp origin_command_already_in_current_value?(
             {:incr, _key, delta},
             current_value,
             expected_value
           ) do
        with {:ok, current} <- coerce_integer(current_value),
             {:ok, expected} <- coerce_integer(expected_value) do
          if delta >= 0, do: current >= expected, else: current <= expected
        else
          _ -> true
        end
      end

      defp origin_command_already_in_current_value?(
             {:incr_float, _key, delta},
             current_value,
             expected_value
           ) do
        with {:ok, current} <- coerce_float(current_value),
             {:ok, expected} <- coerce_float(expected_value) do
          if delta >= 0.0, do: current >= expected, else: current <= expected
        else
          _ -> true
        end
      end

      defp origin_command_already_in_current_value?(
             {:append, _key, _suffix},
             current_value,
             expected
           )
           when is_binary(current_value) and is_binary(expected) do
        String.starts_with?(current_value, expected)
      end

      defp origin_command_already_in_current_value?(_inner_cmd, _current_value, _expected_value) do
        true
      end

      defp origin_command_provably_in_current_value?(
             {:incr, _key, delta},
             current_value,
             expected_value
           ) do
        with {:ok, current} <- coerce_integer(current_value),
             {:ok, expected} <- coerce_integer(expected_value) do
          if delta >= 0, do: current >= expected, else: current <= expected
        else
          _ -> false
        end
      end

      defp origin_command_provably_in_current_value?(
             {:incr_float, _key, delta},
             current_value,
             expected_value
           ) do
        with {:ok, current} <- coerce_float(current_value),
             {:ok, expected} <- coerce_float(expected_value) do
          if delta >= 0.0, do: current >= expected, else: current <= expected
        else
          _ -> false
        end
      end

      defp origin_command_provably_in_current_value?(
             {:append, _key, _suffix},
             current_value,
             expected
           )
           when is_binary(current_value) and is_binary(expected) do
        String.starts_with?(current_value, expected)
      end

      defp origin_command_provably_in_current_value?(
             {:put, _key, _value, _expire_at_ms},
             _current_value,
             _expected_value
           ) do
        true
      end

      # Deletes are materialized as tombstones, not as a value shape. A pending
      # value can never prove that a later DELETE/GETDEL has already reached disk.
      defp origin_command_provably_in_current_value?({:delete, _key}, _current_value, nil),
        do: false

      defp origin_command_provably_in_current_value?({:getdel, _key}, _current_value, nil),
        do: false

      defp origin_command_provably_in_current_value?(_inner_cmd, _current_value, _expected_value) do
        false
      end

      defp with_cross_shard_pending_writes(state, fun) do
        init_pending_write_process_state(state)
        Process.put(:sm_cross_shard_pending_writes, [])
        Process.put(:sm_cross_shard_pending_originals, %{})
        Process.put(:sm_tx_promoted_latches, %{})

        try do
          result =
            fun
            |> run_with_invisible_transaction_staging()
            |> state_storage_failure_result()

          case cross_shard_pending_error_result(result) do
            {:error, _reason} = error ->
              rollback_cross_shard_pending_writes(state)
              rollback_pending_writes(state)
              error

            nil ->
              case prepare_pending_flow_native_batches(state) do
                :ok ->
                  flush_prepared_cross_shard_pending_writes(state, result)

                {:error, _reason} = error ->
                  rollback_cross_shard_pending_writes(state)
                  rollback_pending_writes(state)
                  error
              end
          end
        rescue
          error ->
            rollback_cross_shard_pending_writes_unless_published(state)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            rollback_cross_shard_pending_writes_unless_published(state)
            :erlang.raise(kind, reason, __STACKTRACE__)
        after
          release_transaction_promotion_latches()
          Process.delete(:sm_cross_shard_pending_writes)
          Process.delete(:sm_cross_shard_pending_originals)
          Process.delete(:sm_tx_promoted_latches)
          clear_pending_write_process_state()
        end
      end

      defp rollback_cross_shard_pending_writes_unless_published(state) do
        unless Process.get(:sm_pending_storage_published?, false) do
          rollback_cross_shard_pending_writes(state)
          rollback_pending_writes(state)
        end

        :ok
      end

      defp flush_prepared_cross_shard_pending_writes(state, result) do
        case flush_cross_shard_pending_writes(state) do
          {:ok, flushed_state, successful_groups} ->
            if successful_groups != [] do
              Process.put(:sm_pending_storage_published?, true)
            end

            :ok = publish_cross_shard_transaction(flushed_state, successful_groups)
            record_standalone_published_mutations(successful_groups)
            :ok = dispatch_pending_compound_promotions(flushed_state)

            case publish_pending_flow_history_projections(flushed_state) do
              :ok ->
                observe_pending_lmdb_mirror_enqueue(
                  state,
                  enqueue_pending_lmdb_mirror(state)
                )

                {result, flushed_state}

              {:error, reason} ->
                handle_flow_history_projection_publish_failure(flushed_state, reason)

                observe_pending_lmdb_mirror_enqueue(
                  state,
                  enqueue_pending_lmdb_mirror(state)
                )

                {result, flushed_state}
            end

          {:error, reason, partial_state, successful_groups, journal_txid} ->
            case compensate_cross_shard_partial_writes(
                   partial_state,
                   successful_groups,
                   Process.get(:sm_cross_shard_pending_originals, %{})
                 ) do
              {:ok, compensated_state} ->
                case abort_standalone_cross_shard_journal(state, journal_txid) do
                  :ok ->
                    rollback_cross_shard_pending_writes(state)
                    rollback_pending_writes(state)
                    {:error, reason, compensated_state}

                  {:error, abort_reason} ->
                    rollback_cross_shard_pending_writes(state)
                    rollback_pending_writes(state)
                    block_release_cursor_for_apply()

                    {:error,
                     {:cross_shard_compensation_failed,
                      {:standalone_tx_abort_failed, abort_reason}}, compensated_state}
                end

              {:error, compensation_reason, compensated_state} ->
                rollback_cross_shard_pending_writes(state)
                rollback_pending_writes(state)
                block_release_cursor_for_apply()

                {:error, {:cross_shard_compensation_failed, compensation_reason},
                 compensated_state}
            end
        end
      end

      defp record_standalone_published_mutations(successful_groups) do
        case Process.get(:sm_standalone_apply_stats) do
          %{published_mutations: published} = stats when is_integer(published) ->
            count =
              Enum.reduce(successful_groups, 0, fn
                {_idx, _file_path, _file_id, _keydir, entries, _locations}, acc ->
                  acc + length(entries)
              end)

            Process.put(
              :sm_standalone_apply_stats,
              %{stats | published_mutations: published + count}
            )

          _not_tracking ->
            :ok
        end

        :ok
      end

      defp publish_cross_shard_transaction(state, successful_groups) do
        with_cross_shard_publication_epochs(state, successful_groups, fn ->
          publish_cross_shard_pending_groups(state, successful_groups)
          :ok = flush_pending_stream_cache_cleanups()
          :ok = flush_pending_compound_member_indexes()
          :ok = flush_pending_zset_indexes(state)
          :ok = flush_pending_flow_native_indexes(state)
          :ok = publish_pending_compound_revisions(state)
        end)
      end

      defp with_cross_shard_publication_epochs(state, successful_groups, fun)
           when is_function(fun, 0) do
        ctx = Map.get(state, :instance_ctx, %{})

        tokens =
          successful_groups
          |> Enum.map(fn {idx, _file_path, _file_id, _keydir, _entries, _locations} -> idx end)
          |> Enum.uniq()
          |> Enum.sort()
          |> acquire_cross_shard_publication_epochs(ctx, [])

        try do
          fun.()
        after
          Enum.each(tokens, &PublicationEpoch.end_write/1)
        end
      end

      defp acquire_cross_shard_publication_epochs([], _ctx, tokens), do: tokens

      defp acquire_cross_shard_publication_epochs([idx | indexes], ctx, tokens) do
        token = PublicationEpoch.begin_write(ctx, idx)

        try do
          acquire_cross_shard_publication_epochs(indexes, ctx, [token | tokens])
        rescue
          error ->
            PublicationEpoch.end_write(token)
            reraise error, __STACKTRACE__
        catch
          kind, reason ->
            PublicationEpoch.end_write(token)
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      defp run_with_invisible_transaction_staging(fun) when is_function(fun, 0) do
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

      defp release_transaction_promotion_latches do
        :sm_tx_promoted_latches
        |> Process.get(%{})
        |> Map.values()
        |> Enum.each(&Promotion.release_compaction_latch/1)

        :ok
      end

      defp cross_shard_pending_error_result({:error, _reason} = error), do: error
      defp cross_shard_pending_error_result({:error, reason, _state}), do: {:error, reason}
      defp cross_shard_pending_error_result(_result), do: nil

      defp flush_cross_shard_pending_writes(state) do
        pending =
          :sm_cross_shard_pending_writes
          |> Process.put([])
          |> Enum.reverse()

        flush_cross_shard_pending_writes(state, pending)
      end

      defp flush_cross_shard_pending_writes(state, pending) do
        journal? = standalone_staged_apply?()

        groups =
          pending
          |> Enum.group_by(&cross_shard_pending_target/1)
          |> Enum.sort_by(fn {target, _entries} -> target end)

        case prepare_standalone_cross_shard_journal(state, journal?, groups) do
          {:ok, journal_txid} ->
            flush_cross_shard_pending_groups(state, groups, journal_txid)

          {:error, reason} ->
            {:error, {:bitcask_append_failed, {:standalone_tx_prepare_failed, reason}}, state, [],
             nil}
        end
      end

      defp flush_cross_shard_pending_groups(
             state,
             groups,
             journal_txid
           ) do
        groups
        |> Enum.reduce_while({:ok, state, []}, fn
          {{idx, file_path, file_id, keydir}, entries}, {:ok, acc_state, successful_groups} ->
            batch = Enum.map(entries, &cross_shard_pending_to_batch_entry/1)
            append_result = append_pending_batch(file_path, batch)
            validated_append_result = validate_append_result(batch, append_result)

            case validated_append_result do
              {:ok, locations} ->
                acc_state =
                  acc_state
                  |> track_cross_shard_append_bytes(
                    idx,
                    file_path,
                    file_id,
                    bitcask_record_bytes(batch)
                  )
                  |> mark_cross_shard_checkpoint_dirty(idx)

                group = {idx, file_path, file_id, keydir, entries, locations}
                {:cont, {:ok, acc_state, [group | successful_groups]}}

              {:error, reason} ->
                {:halt,
                 {:error, {:bitcask_append_failed, reason}, acc_state, successful_groups,
                  journal_txid}}
            end
        end)
        |> case do
          {:ok, flushed_state, successful_groups} ->
            case commit_standalone_cross_shard_journal(state, journal_txid) do
              :ok ->
                {:ok, flushed_state, successful_groups}

              {:error, reason} ->
                {:error, {:bitcask_append_failed, {:standalone_tx_commit_failed, reason}},
                 flushed_state, successful_groups, journal_txid}
            end

          {:error, _reason, _partial_state, _successful_groups, _journal_txid} = error ->
            error
        end
      end

      defp prepare_standalone_cross_shard_journal(_state, false, _groups), do: {:ok, nil}
      defp prepare_standalone_cross_shard_journal(_state, true, []), do: {:ok, nil}

      defp prepare_standalone_cross_shard_journal(state, true, groups) do
        originals = Process.get(:sm_cross_shard_pending_originals, %{})

        with {:ok, undo_groups} <- cross_shard_undo_groups(state, groups, originals),
             {:ok, txid} <-
               Ferricstore.Store.StandaloneTxLog.prepare(state.data_dir, undo_groups) do
          {:ok, txid}
        end
      end

      defp cross_shard_undo_groups(state, groups, originals) do
        Enum.reduce_while(groups, {:ok, []}, fn
          {{idx, file_path, _file_id, keydir}, entries}, {:ok, undo_groups} ->
            case cross_shard_compensation_batch(
                   state,
                   idx,
                   keydir,
                   file_path,
                   entries,
                   originals
                 ) do
              {:ok, undo_batch} when undo_batch != [] ->
                {:cont, {:ok, [{file_path, undo_batch} | undo_groups]}}

              {:ok, []} ->
                {:cont, {:ok, undo_groups}}

              {:error, reason} ->
                {:halt, {:error, reason}}
            end
        end)
        |> case do
          {:ok, undo_groups} -> {:ok, Enum.reverse(undo_groups)}
          {:error, _reason} = error -> error
        end
      end

      defp commit_standalone_cross_shard_journal(_state, nil), do: :ok

      defp commit_standalone_cross_shard_journal(state, txid) do
        Ferricstore.Store.StandaloneTxLog.commit(state.data_dir, txid)
      end

      defp abort_standalone_cross_shard_journal(_state, nil), do: :ok

      defp abort_standalone_cross_shard_journal(state, txid) do
        Ferricstore.Store.StandaloneTxLog.abort(state.data_dir, txid)
      end

      defp cross_shard_pending_target(
             {:put, idx, keydir, file_path, file_id, _key, _ets, _disk, _exp}
           ),
           do: {idx, file_path, file_id, keydir}

      defp cross_shard_pending_target({:delete, idx, keydir, file_path, file_id, _key}),
        do: {idx, file_path, file_id, keydir}

      defp cross_shard_pending_to_batch_entry(
             {:put, _idx, _keydir, _file_path, _file_id, key, _ets_value, disk_value,
              expire_at_ms}
           ),
           do: {:put, key, disk_value, expire_at_ms}

      defp cross_shard_pending_to_batch_entry(
             {:delete, _idx, _keydir, _file_path, _file_id, key}
           ),
           do: {:delete, key, nil}
    end
  end
end
