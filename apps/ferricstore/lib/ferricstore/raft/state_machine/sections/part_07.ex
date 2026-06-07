defmodule Ferricstore.Raft.StateMachine.Sections.Part07 do
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
      alias Ferricstore.Commands.Json
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
      {:bloom_meta, %{path: path}} -> path
      {:cms_meta, _} -> prob_path(state, key, "cms")
      {:cuckoo_meta, _} -> prob_path(state, key, "cuckoo")
      {:topk_meta, %{path: path}} -> path
      _ -> nil
    end
  end

  defp safe_binary_to_term(value) do
    :erlang.binary_to_term(value, [:safe])
  rescue
    _ -> :not_term
  end

  defp apply_delete_batch_keys(state, keys) do
    case Map.get(state, :cross_shard_locks, %{}) do
      locks when map_size(locks) == 0 ->
        case apply_delete_batch_keys_fast(state, keys) do
          :fallback ->
            Enum.map(keys, fn key -> do_delete(state, key) end)

          results ->
            results
        end

      _locks ->
        Enum.map(keys, fn key ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          case check_key_lock(state, redis_key, nil) do
            :ok -> do_delete(state, key)
            {:error, :key_locked} -> {:error, :key_locked}
          end
        end)
    end
  end

  defp apply_compound_batch_put_entries(state, redis_key, entries) do
    cond do
      not compound_put_entries_for_key?(redis_key, entries) ->
        {:error, :compound_batch_cross_key}

      Map.get(state, :cross_shard_locks, %{}) != %{} ->
        compound_batch_lock_checked_results(state, redis_key, entries, fn ->
          do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries)
        end)

      true ->
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            case do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries) do
              :ok -> List.duplicate(:ok, length(entries))
              {:error, _reason} = error -> error
            end

          {:error, :key_locked} = error ->
            List.duplicate(error, length(entries))
        end
    end
  end

  defp do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries) do
    case prepare_apply_blob_command(state, {:compound_batch_put, redis_key, entries}) do
      {:ok, {:compound_blob_batch_put, ^redis_key, blob_entries}} ->
        with {:ok, prepared_entries} <- prepare_compound_blob_batch_entries(state, blob_entries) do
          do_compound_blob_batch_put(state, redis_key, prepared_entries)
        end

      {:ok, {:compound_batch_put, ^redis_key, ^entries}} ->
        do_compound_batch_put(state, redis_key, entries)

      {:ok, _other} ->
        {:error, :invalid_compound_batch_entry}

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_compound_blob_batch_put_entries(state, redis_key, entries) do
    with true <- compound_blob_put_entries_for_key?(redis_key, entries),
         {:ok, prepared_entries} <- prepare_compound_blob_batch_entries(state, entries) do
      cond do
        Map.get(state, :cross_shard_locks, %{}) != %{} ->
          compound_batch_lock_checked_results(state, redis_key, entries, fn ->
            do_compound_blob_batch_put(state, redis_key, prepared_entries)
          end)

        true ->
          case check_key_lock(state, redis_key, nil) do
            :ok ->
              case do_compound_blob_batch_put(state, redis_key, prepared_entries) do
                :ok -> List.duplicate(:ok, length(entries))
                {:error, _reason} = error -> error
              end

            {:error, :key_locked} = error ->
              List.duplicate(error, length(entries))
          end
      end
    else
      false -> {:error, :compound_batch_cross_key}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_compound_blob_batch_entries(state, entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {compound_key, value, expire_at_ms, :value}, {:ok, acc}
      when is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) ->
        {:cont, {:ok, [{:value, compound_key, value, expire_at_ms} | acc]}}

      {compound_key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, acc}
      when is_binary(compound_key) and is_binary(encoded_ref) and is_integer(expire_at_ms) ->
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

  defp do_compound_blob_batch_put(state, redis_key, prepared_entries) do
    prepared_entries
    |> Enum.reduce_while(:ok, fn
      {:value, compound_key, value, expire_at_ms}, :ok ->
        case do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
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

      Map.get(state, :cross_shard_locks, %{}) != %{} ->
        compound_batch_lock_checked_results(state, redis_key, compound_keys, fn ->
          do_compound_batch_delete(state, redis_key, compound_keys)
        end)

      true ->
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            case do_compound_batch_delete(state, redis_key, compound_keys) do
              :ok -> List.duplicate(:ok, length(compound_keys))
              {:error, _reason} = error -> error
            end

          {:error, :key_locked} = error ->
            List.duplicate(error, length(compound_keys))
        end
    end
  end

  defp compound_batch_lock_checked_results(state, redis_key, items, fun) do
    case check_key_lock(state, redis_key, nil) do
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

  defp compound_blob_put_entries_for_key?(redis_key, entries) do
    Enum.all?(entries, fn
      {compound_key, _value_or_ref, _expire_at_ms, kind}
      when is_binary(compound_key) and kind in [:value, :blob_ref] ->
        CompoundKey.extract_redis_key(compound_key) == redis_key

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

  defp origin_checked_expected_result({:getset, _key, _new_value}, before_value, _expected_value) do
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

  defp origin_command_already_in_current_value?({:append, _key, _suffix}, current_value, expected)
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
  defp origin_command_provably_in_current_value?({:delete, _key}, _current_value, nil), do: false

  defp origin_command_provably_in_current_value?({:getdel, _key}, _current_value, nil), do: false

  defp origin_command_provably_in_current_value?(_inner_cmd, _current_value, _expected_value) do
    false
  end

  defp normalize_stamped_command({:ratelimit_add, key, window_ms, max, count, _legacy_now_ms}) do
    {:ratelimit_add, key, window_ms, max, count}
  end

  defp normalize_stamped_command({:batch, commands}) when is_list(commands) do
    {:batch, Enum.map(commands, &normalize_stamped_command/1)}
  end

  defp normalize_stamped_command({:async, command}) do
    {:async, normalize_stamped_command(command)}
  end

  defp normalize_stamped_command(command), do: command

  defp with_cross_shard_pending_writes(state, fun) do
    init_pending_write_process_state(state)
    Process.put(:sm_cross_shard_pending_writes, [])
    Process.put(:sm_cross_shard_pending_originals, %{})

    try do
      result = fun.()

      case cross_shard_pending_error_result(result) do
        {:error, _reason} = error ->
          rollback_cross_shard_pending_writes(state)
          rollback_pending_writes(state)
          error

        nil ->
          case flush_cross_shard_pending_writes(state) do
            {:ok, flushed_state} ->
              :ok = flush_pending_flow_native_indexes(flushed_state)

              case publish_pending_flow_history_projections(flushed_state) do
                :ok ->
                  observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                  {result, flushed_state}

                {:error, reason} ->
                  handle_flow_history_projection_publish_failure(flushed_state, reason)
                  observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                  {result, flushed_state}
              end

            {:error, reason, partial_state, successful_groups} ->
              case compensate_cross_shard_partial_writes(
                     partial_state,
                     successful_groups,
                     Process.get(:sm_cross_shard_pending_originals, %{})
                   ) do
                {:ok, compensated_state} ->
                  rollback_cross_shard_pending_writes(state)
                  rollback_pending_writes(state)
                  {:error, reason, compensated_state}

                {:error, compensation_reason, compensated_state} ->
                  rollback_cross_shard_pending_writes(state)
                  rollback_pending_writes(state)
                  block_release_cursor_for_apply()

                  {:error, {:cross_shard_compensation_failed, compensation_reason},
                   compensated_state}
              end
          end
      end
    rescue
      error ->
        rollback_cross_shard_pending_writes(state)
        rollback_pending_writes(state)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        rollback_cross_shard_pending_writes(state)
        rollback_pending_writes(state)
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      Process.delete(:sm_cross_shard_pending_writes)
      Process.delete(:sm_cross_shard_pending_originals)
      clear_pending_write_process_state()
    end
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
    staged_publish? = standalone_staged_apply?()

    pending
    |> Enum.group_by(&cross_shard_pending_target/1)
    |> Enum.reduce_while({:ok, state, []}, fn {{idx, file_path, file_id, keydir}, entries},
                                              {:ok, acc_state, successful_groups} ->
      batch = Enum.map(entries, &cross_shard_pending_to_batch_entry/1)
      append_result = append_pending_batch(file_path, batch)
      validated_append_result = validate_append_result(batch, append_result)

      case validated_append_result do
        {:ok, locations} ->
          unless staged_publish? do
            apply_cross_shard_pending_locations(keydir, file_id, entries, locations)
          end

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
          {:halt, {:error, {:bitcask_append_failed, reason}, acc_state, successful_groups}}
      end
    end)
    |> case do
      {:ok, flushed_state, successful_groups} ->
        if staged_publish? do
          publish_cross_shard_pending_groups(flushed_state, successful_groups)
        end

        {:ok, flushed_state}

      {:error, _reason, _partial_state, _successful_groups} = error ->
        error
    end
  end

  defp cross_shard_pending_target(
         {:put, idx, keydir, file_path, file_id, _key, _ets, _disk, _exp}
       ),
       do: {idx, file_path, file_id, keydir}

  defp cross_shard_pending_target({:delete, idx, keydir, file_path, file_id, _key}),
    do: {idx, file_path, file_id, keydir}

  defp cross_shard_pending_to_batch_entry(
         {:put, _idx, _keydir, _file_path, _file_id, key, _ets_value, disk_value, expire_at_ms}
       ),
       do: {:put, key, disk_value, expire_at_ms}

  defp cross_shard_pending_to_batch_entry({:delete, _idx, _keydir, _file_path, _file_id, key}),
    do: {:delete, key, nil}

    end
  end
end
