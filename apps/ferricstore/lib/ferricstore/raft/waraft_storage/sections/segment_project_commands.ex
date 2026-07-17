defmodule Ferricstore.Raft.WARaftStorage.Sections.SegmentProjectCommands do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.HLC
      alias Ferricstore.Flow.HistoryProjector
      alias Ferricstore.Flow.Keys, as: FlowKeys
      alias Ferricstore.Flow.LMDB, as: FlowLMDB
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Raft.CommandStamp
      alias Ferricstore.Raft.WARaftSegmentReader
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.ColdRead
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Promotion
      alias Ferricstore.Store.Shard.CompoundMemberIndex
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
      alias Ferricstore.Store.Shard.ZSetIndex

      defp segment_project_command({:put, key, value, expire_at_ms}, position, sm_state) do
        redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

        with :ok <- segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil),
             true <- segment_projectable_put?(sm_state, key, value, expire_at_ms) do
          {:ok, segment_project_put(sm_state, key, value, expire_at_ms, position), :ok, 1}
        else
          {:error, _reason} = error -> {:ok, sm_state, error, 0}
          false -> :unsupported
        end
      end

      defp segment_project_command(
             {:put_blob_ref, key, encoded_ref, expire_at_ms},
             position,
             sm_state
           ) do
        redis_key = if is_binary(key), do: CompoundKey.extract_redis_key(key)

        with :ok <- segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil),
             true <- segment_projectable_blob_ref_put?(sm_state, key, encoded_ref, expire_at_ms) do
          case verify_segment_blob_refs(sm_state, [encoded_ref]) do
            :ok ->
              {:ok,
               segment_project_put_blob_ref(sm_state, key, encoded_ref, expire_at_ms, position),
               :ok, 1}

            {:error, _reason} = error ->
              {:ok, sm_state, error, 0}
          end
        else
          {:error, _reason} = error -> {:ok, sm_state, error, 0}
          false -> :unsupported
        end
      end

      defp segment_project_command({:delete, key}, _position, sm_state) when is_binary(key) do
        redis_key = CompoundKey.extract_redis_key(key)

        case segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) do
          :ok -> {:ok, segment_project_delete(sm_state, key), :ok, 1}
          {:error, _reason} = error -> {:ok, sm_state, error, 0}
        end
      end

      defp segment_project_command(
             {:compound_put, compound_key, value, expire_at_ms},
             position,
             sm_state
           ) do
        redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

        with :ok <- segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil),
             true <-
               segment_projectable_compound_put?(
                 sm_state,
                 redis_key,
                 compound_key,
                 value,
                 expire_at_ms
               ) do
          new_sm_state =
            sm_state
            |> segment_project_put(compound_key, value, expire_at_ms, position)
            |> segment_project_zset_put(redis_key, compound_key, value)

          {:ok, new_sm_state, :ok, 1}
        else
          {:error, _reason} = error -> {:ok, sm_state, error, 0}
          false -> :unsupported
        end
      end

      defp segment_project_command(
             {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms},
             position,
             sm_state
           ) do
        redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

        if segment_projectable_compound_blob_ref_put?(
             sm_state,
             redis_key,
             compound_key,
             encoded_ref,
             expire_at_ms
           ) and segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) == :ok do
          case verify_segment_blob_refs(sm_state, [encoded_ref]) do
            :ok ->
              new_sm_state =
                sm_state
                |> segment_project_put_blob_ref(compound_key, encoded_ref, expire_at_ms, position)
                |> segment_project_zset_put(redis_key, compound_key, encoded_ref)

              {:ok, new_sm_state, :ok, 1}

            {:error, _reason} = error ->
              {:ok, sm_state, error, 0}
          end
        else
          case segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) do
            {:error, _reason} = error -> {:ok, sm_state, error, 0}
            _other -> :unsupported
          end
        end
      end

      defp segment_project_command({:compound_delete, compound_key}, _position, sm_state)
           when is_binary(compound_key) do
        redis_key = CompoundKey.extract_redis_key(compound_key)

        with :ok <- segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil),
             true <- segment_shared_compound_projection_safe?(sm_state, redis_key) do
          new_sm_state =
            sm_state
            |> segment_project_delete(compound_key)
            |> segment_project_zset_delete(redis_key, compound_key)

          {:ok, new_sm_state, :ok, 1}
        else
          {:error, _reason} = error ->
            {:ok, sm_state, error, 0}

          false ->
            :unsupported
        end
      end

      defp segment_project_command({:put_batch, entries}, position, sm_state)
           when is_list(entries) do
        cond do
          segment_projection_put_entries_locked?(sm_state, entries) ->
            case put_batch_entry_commands(entries) do
              {:ok, commands} -> segment_project_generic_batch(commands, position, sm_state)
              :error -> :unsupported
            end

          not segment_project_batch_string_overwrites_safe?(sm_state, entries) ->
            :unsupported

          segment_project_batch_has_blob_candidate?(sm_state, entries) ->
            :unsupported

          true ->
            file_id = {:waraft_segment, position_index(position)}
            offset = segment_record_offset(sm_state, position)
            shard_state = shard_ets_state_from_sm(sm_state)
            threshold = ShardETS.hot_cache_threshold(shard_state)

            case segment_project_batch_hot_cache_threshold(entries, threshold) do
              {:ok, batch_threshold} ->
                case ShardETS.ets_insert_fresh_no_expiry_many_with_location(
                       shard_state,
                       entries,
                       file_id,
                       offset,
                       batch_threshold
                     ) do
                  {:ok, count} ->
                    {:ok, sm_state, {:ok, List.duplicate(:ok, count)}, count}

                  :fallback ->
                    apply_segment_put_batch_entries(
                      entries,
                      sm_state,
                      shard_state,
                      threshold,
                      file_id,
                      offset,
                      0
                    )
                end

              :per_key ->
                apply_segment_put_batch_entries(
                  entries,
                  sm_state,
                  shard_state,
                  threshold,
                  file_id,
                  offset,
                  0
                )
            end
        end
      end

      defp segment_project_command({:put_blob_batch, entries}, position, sm_state)
           when is_list(entries) do
        with {:ok, prepared, encoded_refs} <- prepare_segment_blob_batch_entries(entries),
             true <- segment_project_prepared_string_overwrites_safe?(sm_state, prepared),
             :ok <- verify_segment_blob_refs(sm_state, encoded_refs) do
          {new_sm_state, results, applied_count} =
            segment_project_put_blob_batch(prepared, position, sm_state)

          {:ok, new_sm_state, {:ok, results}, applied_count}
        else
          {:unsupported, _reason} ->
            :unsupported

          {:error, _reason} = error ->
            {:ok, sm_state, error, 0}

          false ->
            :unsupported
        end
      end

      defp segment_project_put_blob_batch(prepared, position, sm_state) do
        locks = Map.get(sm_state, :fetch_or_compute_locks, %{})

        if map_size(locks) == 0 do
          count = length(prepared)

          new_sm_state =
            Enum.reduce(prepared, sm_state, fn
              {:value, key, value, expire_at_ms}, acc ->
                segment_project_put(acc, key, value, expire_at_ms, position)

              {:blob_ref, key, encoded_ref, expire_at_ms}, acc ->
                segment_project_put_blob_ref(acc, key, encoded_ref, expire_at_ms, position)
            end)

          {new_sm_state, List.duplicate(:ok, count), count}
        else
          prepared
          |> Enum.reduce({sm_state, [], 0}, fn
            {:value, key, value, expire_at_ms}, {acc, results, count} ->
              redis_key = CompoundKey.extract_redis_key(key)

              case segment_project_check_fetch_or_compute_lock(acc, redis_key, nil) do
                :ok ->
                  next = segment_project_put(acc, key, value, expire_at_ms, position)
                  {next, [:ok | results], count + 1}

                {:error, _reason} = error ->
                  {acc, [error | results], count}
              end

            {:blob_ref, key, encoded_ref, expire_at_ms}, {acc, results, count} ->
              redis_key = CompoundKey.extract_redis_key(key)

              case segment_project_check_fetch_or_compute_lock(acc, redis_key, nil) do
                :ok ->
                  next =
                    segment_project_put_blob_ref(
                      acc,
                      key,
                      encoded_ref,
                      expire_at_ms,
                      position
                    )

                  {next, [:ok | results], count + 1}

                {:error, _reason} = error ->
                  {acc, [error | results], count}
              end
          end)
          |> then(fn {new_sm_state, results, count} ->
            {new_sm_state, Enum.reverse(results), count}
          end)
        end
      end

      defp segment_project_command(
             {:compound_batch_put, redis_key, entries},
             position,
             sm_state
           )
           when is_binary(redis_key) and is_list(entries) do
        case segment_compound_batch_put_shape(redis_key, entries) do
          {:ok, last_compound_key, count} ->
            case segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) do
              :ok ->
                if segment_compound_batch_target_safe?(
                     sm_state,
                     redis_key,
                     last_compound_key,
                     count
                   ) do
                  new_sm_state =
                    Enum.reduce(entries, sm_state, fn
                      {compound_key, value, expire_at_ms}, acc ->
                        acc
                        |> segment_project_put(compound_key, value, expire_at_ms, position)
                        |> segment_project_zset_put(redis_key, compound_key, value)
                    end)

                  {:ok, new_sm_state, {:ok, List.duplicate(:ok, count)}, count}
                else
                  :unsupported
                end

              {:error, _reason} = error ->
                {:ok, sm_state, {:ok, List.duplicate(error, count)}, 0}
            end

          :error ->
            :unsupported
        end
      end

      defp segment_project_command(
             {:compound_blob_batch_put, redis_key, entries},
             position,
             sm_state
           )
           when is_binary(redis_key) and is_list(entries) do
        with {:ok, prepared, encoded_refs} <-
               prepare_segment_compound_blob_batch_entries(redis_key, entries),
             true <-
               segment_projectable_prepared_compound_blob_batch?(sm_state, redis_key, prepared),
             :ok <- verify_segment_blob_refs(sm_state, encoded_refs) do
          count = length(entries)

          case segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) do
            :ok ->
              new_sm_state =
                Enum.reduce(prepared, sm_state, fn
                  {:value, compound_key, value, expire_at_ms}, acc ->
                    acc
                    |> segment_project_put(compound_key, value, expire_at_ms, position)
                    |> segment_project_zset_put(redis_key, compound_key, value)

                  {:blob_ref, compound_key, encoded_ref, expire_at_ms}, acc ->
                    acc
                    |> segment_project_put_blob_ref(
                      compound_key,
                      encoded_ref,
                      expire_at_ms,
                      position
                    )
                    |> segment_project_zset_put(redis_key, compound_key, encoded_ref)
                end)

              {:ok, new_sm_state, {:ok, List.duplicate(:ok, count)}, count}

            {:error, _reason} = error ->
              {:ok, sm_state, {:ok, List.duplicate(error, count)}, 0}
          end
        else
          {:unsupported, _reason} ->
            :unsupported

          false ->
            :unsupported

          {:error, _reason} = error ->
            {:ok, sm_state, error, 0}
        end
      end

      defp segment_project_command({:delete_batch, keys}, position, sm_state)
           when is_list(keys) do
        if segment_projection_keys_locked?(sm_state, keys) do
          case delete_batch_entry_commands(keys) do
            {:ok, commands} -> segment_project_generic_batch(commands, position, sm_state)
            :error -> :unsupported
          end
        else
          if Enum.all?(keys, &is_binary/1) do
            new_sm_state = Enum.reduce(keys, sm_state, &segment_project_delete(&2, &1))
            {:ok, new_sm_state, {:ok, List.duplicate(:ok, length(keys))}, length(keys)}
          else
            :unsupported
          end
        end
      end

      defp segment_project_command({:flush_shard, {physical_ms, logical}}, _position, _sm_state)
           when is_integer(physical_ms) and physical_ms >= 0 and is_integer(logical) and
                  logical >= 0 do
        :unsupported
      end

      defp segment_project_command(
             {:compound_batch_delete, redis_key, compound_keys},
             _position,
             sm_state
           )
           when is_binary(redis_key) and is_list(compound_keys) do
        case segment_compound_batch_delete_shape(redis_key, compound_keys) do
          {:ok, count} ->
            case segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) do
              :ok ->
                if segment_shared_compound_projection_safe?(sm_state, redis_key) do
                  new_sm_state =
                    Enum.reduce(compound_keys, sm_state, fn compound_key, acc ->
                      acc
                      |> segment_project_delete(compound_key)
                      |> segment_project_zset_delete(redis_key, compound_key)
                    end)

                  {:ok, new_sm_state, {:ok, List.duplicate(:ok, count)}, count}
                else
                  :unsupported
                end

              {:error, _reason} = error ->
                {:ok, sm_state, {:ok, List.duplicate(error, count)}, 0}
            end

          :error ->
            :unsupported
        end
      end

      defp segment_project_command({:compound_delete_prefix, prefix}, _position, sm_state)
           when is_binary(prefix) do
        redis_key = CompoundKey.extract_redis_key(prefix)

        with :ok <- segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil),
             true <- segment_shared_compound_projection_safe?(sm_state, redis_key) do
          case segment_project_delete_prefix(sm_state, redis_key, prefix) do
            {:ok, new_sm_state} -> {:ok, new_sm_state, :ok, 1}
            {:error, _reason} = error -> {:ok, sm_state, error, 0}
          end
        else
          {:error, _reason} = error ->
            {:ok, sm_state, error, 0}

          false ->
            :unsupported
        end
      end

      defp segment_project_command({:batch, commands}, position, sm_state)
           when is_list(commands) do
        case segment_project_decode_batch(commands, :unknown, [], []) do
          {:put_batch, entries} ->
            segment_project_command({:put_batch, entries}, position, sm_state)

          {:delete_batch, keys} ->
            segment_project_command({:delete_batch, keys}, position, sm_state)

          {:generic, commands} ->
            segment_project_generic_batch(commands, position, sm_state)
        end
      end

      defp segment_project_command(_command, _position, _sm_state), do: :unsupported

      defp put_batch_entry_commands(entries) do
        Enum.reduce_while(entries, {:ok, []}, fn
          {key, value, expire_at_ms}, {:ok, acc}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 ->
            {:cont, {:ok, [{:put, key, value, expire_at_ms} | acc]}}

          _entry, {:ok, _acc} ->
            {:halt, :error}
        end)
        |> case do
          {:ok, commands} -> {:ok, Enum.reverse(commands)}
          :error -> :error
        end
      end

      defp delete_batch_entry_commands(keys) do
        Enum.reduce_while(keys, {:ok, []}, fn
          key, {:ok, acc} when is_binary(key) ->
            {:cont, {:ok, [{:delete, key} | acc]}}

          _key, {:ok, _acc} ->
            {:halt, :error}
        end)
        |> case do
          {:ok, commands} -> {:ok, Enum.reverse(commands)}
          :error -> :error
        end
      end

      defp segment_project_decode_batch([], :put, _decoded_acc, entries) do
        {:put_batch, Enum.reverse(entries)}
      end

      defp segment_project_decode_batch([], :delete, _decoded_acc, keys) do
        {:delete_batch, Enum.reverse(keys)}
      end

      defp segment_project_decode_batch([], _kind, decoded_acc, _fast_acc) do
        {:generic, Enum.reverse(decoded_acc)}
      end

      defp segment_project_decode_batch([command | rest], kind, decoded_acc, fast_acc) do
        decoded = decoded_replay_command(command)

        case {kind, decoded} do
          {:unknown, {:put, key, value, expire_at_ms}}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 ->
            segment_project_decode_batch(rest, :put, [decoded | decoded_acc], [
              {key, value, expire_at_ms} | fast_acc
            ])

          {:put, {:put, key, value, expire_at_ms}}
          when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                 expire_at_ms >= 0 ->
            segment_project_decode_batch(rest, :put, [decoded | decoded_acc], [
              {key, value, expire_at_ms} | fast_acc
            ])

          {:unknown, {:delete, key}} when is_binary(key) ->
            segment_project_decode_batch(rest, :delete, [decoded | decoded_acc], [key | fast_acc])

          {:delete, {:delete, key}} when is_binary(key) ->
            segment_project_decode_batch(rest, :delete, [decoded | decoded_acc], [key | fast_acc])

          {:generic, _decoded} ->
            segment_project_decode_batch(rest, :generic, [decoded | decoded_acc], [])

          {_homogeneous, _decoded} ->
            segment_project_decode_batch(rest, :generic, [decoded | decoded_acc], [])
        end
      end

      defp segment_project_generic_batch(commands, position, sm_state) do
        if Enum.all?(commands, &segment_projectable_batch_command?(sm_state, &1)) do
          Enum.reduce_while(commands, {sm_state, [], 0}, fn command,
                                                            {acc_state, acc_results, acc_count} ->
            {:ok, next_state, result, count} =
              segment_project_command(command, position, acc_state)

            if storage_apply_failure?(result) do
              {:halt, {:storage_error, next_state, result, acc_count + count}}
            else
              {:cont,
               {next_state, [single_segment_project_result(result) | acc_results],
                acc_count + count}}
            end
          end)
          |> case do
            {:storage_error, new_sm_state, result, applied_increment} ->
              {:ok, new_sm_state, result, applied_increment}

            {new_sm_state, results, applied_increment} ->
              {:ok, new_sm_state, {:ok, Enum.reverse(results)}, applied_increment}
          end
        else
          :unsupported
        end
      end

      defp segment_projectable_batch_command?(sm_state, {:put, key, value, expire_at_ms}),
        do: segment_projectable_put?(sm_state, key, value, expire_at_ms)

      defp segment_projectable_batch_command?(_sm_state, {:delete, key}), do: is_binary(key)

      defp segment_projectable_batch_command?(
             sm_state,
             {:compound_put, compound_key, value, expire_at_ms}
           ) do
        redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

        segment_projectable_compound_put?(
          sm_state,
          redis_key,
          compound_key,
          value,
          expire_at_ms
        )
      end

      defp segment_projectable_batch_command?(sm_state, {:compound_delete, compound_key}) do
        redis_key = if is_binary(compound_key), do: CompoundKey.extract_redis_key(compound_key)

        is_binary(compound_key) and segment_shared_compound_projection_safe?(sm_state, redis_key)
      end

      defp segment_projectable_batch_command?(sm_state, {:compound_batch_put, redis_key, entries})
           when is_binary(redis_key) and is_list(entries),
           do: segment_projectable_compound_batch_put?(sm_state, redis_key, entries)

      defp segment_projectable_batch_command?(sm_state, {:compound_batch_delete, redis_key, keys})
           when is_binary(redis_key) and is_list(keys),
           do:
             segment_shared_compound_projection_safe?(sm_state, redis_key) and
               Enum.all?(keys, &compound_key_for_redis_key?(redis_key, &1))

      defp segment_projectable_batch_command?(sm_state, {:compound_delete_prefix, prefix}) do
        redis_key = if is_binary(prefix), do: CompoundKey.extract_redis_key(prefix)

        is_binary(prefix) and segment_shared_compound_projection_safe?(sm_state, redis_key)
      end

      defp segment_projectable_batch_command?(_sm_state, _command), do: false

      defp segment_projectable_put?(sm_state, key, value, expire_at_ms) do
        is_binary(key) and is_binary(value) and non_neg_integer?(expire_at_ms) and
          segment_projection_fast_key?(key) and
          not segment_blob_candidate?(sm_state, value) and
          segment_project_string_overwrite_safe?(sm_state, key)
      end

      defp segment_project_string_overwrite_safe?(sm_state, key) do
        CompoundKey.internal_key?(key) or
          :ets.lookup(Map.fetch!(sm_state, :ets), CompoundKey.type_key(key)) == []
      rescue
        ArgumentError -> false
      end

      defp segment_project_batch_string_overwrites_safe?(sm_state, entries) do
        Enum.all?(entries, fn
          {key, _value, _expire_at_ms} when is_binary(key) ->
            segment_project_string_overwrite_safe?(sm_state, key)

          _invalid ->
            false
        end)
      end

      defp segment_project_prepared_string_overwrites_safe?(sm_state, prepared) do
        Enum.all?(prepared, fn
          {_kind, key, _value, _expire_at_ms} ->
            segment_project_string_overwrite_safe?(sm_state, key)
        end)
      end

      # Flow policy writes are issued as raw PUTs but carry LMDB projection side
      # effects. Keep them on the full state-machine path; policy writes are cold.
      defp segment_projection_fast_key?(key), do: not FlowKeys.policy_key?(key)

      defp segment_blob_candidate?(sm_state, value) when is_binary(value) do
        threshold = BlobValue.threshold(Map.get(sm_state, :instance_ctx))

        threshold > 0 and
          (byte_size(value) >= threshold or BlobRef.encoded_size?(byte_size(value)))
      end

      defp segment_blob_candidate?(_sm_state, _value), do: false

      defp segment_project_batch_has_blob_candidate?(sm_state, entries) do
        Enum.any?(entries, fn
          {_key, value, _expire_at_ms} -> segment_blob_candidate?(sm_state, value)
          _entry -> false
        end)
      end

      defp apply_segment_put_batch_entries(
             [],
             sm_state,
             _shard_state,
             _threshold,
             _file_id,
             _offset,
             count
           ) do
        {:ok, sm_state, {:ok, List.duplicate(:ok, count)}, count}
      end

      defp apply_segment_put_batch_entries(
             [{key, value, expire_at_ms} | rest],
             sm_state,
             shard_state,
             threshold,
             file_id,
             offset,
             count
           )
           when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) and
                  expire_at_ms >= 0 do
        entry_threshold = segment_project_hot_cache_threshold(shard_state, key, threshold)

        sm_state =
          segment_project_put_at_location(
            sm_state,
            shard_state,
            entry_threshold,
            key,
            value,
            expire_at_ms,
            file_id,
            offset
          )

        apply_segment_put_batch_entries(
          rest,
          sm_state,
          shard_state,
          threshold,
          file_id,
          offset,
          count + 1
        )
      end

      defp apply_segment_put_batch_entries(
             _invalid,
             _sm_state,
             _shard_state,
             _threshold,
             _file_id,
             _offset,
             _count
           ) do
        :unsupported
      end

      defp segment_projectable_compound_put?(
             sm_state,
             redis_key,
             compound_key,
             value,
             expire_at_ms
           ) do
        compound_key_for_redis_key?(redis_key, compound_key) and is_binary(value) and
          non_neg_integer?(expire_at_ms) and
          segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, 1)
      end

      defp segment_projectable_compound_batch_put?(sm_state, redis_key, entries) do
        case segment_compound_batch_put_shape(redis_key, entries) do
          {:ok, last_compound_key, count} ->
            segment_compound_batch_target_safe?(
              sm_state,
              redis_key,
              last_compound_key,
              count
            )

          :error ->
            false
        end
      end

      defp segment_compound_batch_put_shape(redis_key, entries) do
        Enum.reduce_while(entries, {:ok, nil, 0}, fn
          {compound_key, value, expire_at_ms}, {:ok, _last_compound_key, count}
          when is_binary(value) ->
            if compound_key_for_redis_key?(redis_key, compound_key) and
                 non_neg_integer?(expire_at_ms) do
              {:cont, {:ok, compound_key, count + 1}}
            else
              {:halt, :error}
            end

          _entry, _acc ->
            {:halt, :error}
        end)
      end

      defp segment_compound_batch_target_safe?(_sm_state, _redis_key, nil, 0), do: true

      defp segment_compound_batch_target_safe?(
             sm_state,
             redis_key,
             compound_key,
             count
           ) do
        segment_shared_compound_projection_safe?(
          sm_state,
          redis_key,
          compound_key,
          count
        )
      end

      defp segment_compound_batch_delete_shape(redis_key, compound_keys) do
        Enum.reduce_while(compound_keys, {:ok, 0}, fn compound_key, {:ok, count} ->
          if compound_key_for_redis_key?(redis_key, compound_key) do
            {:cont, {:ok, count + 1}}
          else
            {:halt, :error}
          end
        end)
      end

      defp segment_projectable_prepared_compound_blob_batch?(sm_state, redis_key, prepared) do
        case List.last(prepared) do
          {:value, compound_key, _value, _expire_at_ms} ->
            segment_shared_compound_projection_safe?(
              sm_state,
              redis_key,
              compound_key,
              length(prepared)
            )

          {:blob_ref, compound_key, _encoded_ref, _expire_at_ms} ->
            segment_shared_compound_projection_safe?(
              sm_state,
              redis_key,
              compound_key,
              length(prepared)
            )

          nil ->
            true

          _entry ->
            false
        end
      end

      defp segment_projectable_blob_ref_put?(sm_state, key, encoded_ref, expire_at_ms) do
        is_binary(key) and is_binary(encoded_ref) and non_neg_integer?(expire_at_ms) and
          segment_project_string_overwrite_safe?(sm_state, key)
      end

      defp segment_projectable_compound_blob_ref_put?(
             sm_state,
             redis_key,
             compound_key,
             encoded_ref,
             expire_at_ms
           ) do
        compound_key_for_redis_key?(redis_key, compound_key) and is_binary(encoded_ref) and
          non_neg_integer?(expire_at_ms) and
          segment_shared_compound_projection_safe?(sm_state, redis_key, compound_key, 1)
      end

      defp segment_shared_compound_projection_safe?(sm_state, redis_key) do
        is_binary(redis_key) and
          CompoundMemberIndex.ready?(Map.get(sm_state, :compound_member_index_name)) and
          not segment_compound_promoted?(sm_state, redis_key)
      end

      defp segment_shared_compound_projection_safe?(
             sm_state,
             redis_key,
             compound_key,
             write_count
           ) do
        segment_shared_compound_projection_safe?(sm_state, redis_key) and
          not segment_compound_promotion_candidate?(
            sm_state,
            redis_key,
            compound_key,
            write_count
          )
      end

      defp segment_compound_promoted?(sm_state, redis_key) do
        Map.has_key?(Map.get(sm_state, :promoted_instances, %{}), redis_key) or
          segment_live_promotion_marker?(sm_state, redis_key)
      end

      defp segment_live_promotion_marker?(sm_state, redis_key) do
        marker_key = Promotion.marker_key(redis_key)
        now = storage_expiry_cutoff_ms()

        case :ets.lookup(Map.fetch!(sm_state, :ets), marker_key) do
          [{^marker_key, type, expire_at_ms, _lfu, _fid, _offset, _value_size}]
          when type in ["hash", "set", "zset"] and
                 (expire_at_ms == 0 or expire_at_ms > now) ->
            true

          _missing_or_invalid ->
            false
        end
      rescue
        ArgumentError -> false
      end

      defp segment_compound_promotion_candidate?(sm_state, redis_key, compound_key, write_count) do
        threshold = Map.fetch!(sm_state, :apply_context).promotion_threshold

        cond do
          threshold == 0 ->
            false

          not is_integer(write_count) or write_count <= 0 ->
            false

          true ->
            case segment_compound_prefix(redis_key, compound_key) do
              nil ->
                false

              prefix ->
                shard_state = shard_ets_state_from_sm(sm_state)
                ShardETS.prefix_count_entries(shard_state, prefix) + write_count > threshold
            end
        end
      end

      defp segment_compound_prefix(redis_key, <<"H:", _rest::binary>>),
        do: CompoundKey.hash_prefix(redis_key)

      defp segment_compound_prefix(redis_key, <<"S:", _rest::binary>>),
        do: CompoundKey.set_prefix(redis_key)

      defp segment_compound_prefix(redis_key, <<"Z:", _rest::binary>>),
        do: CompoundKey.zset_prefix(redis_key)

      defp segment_compound_prefix(_redis_key, _compound_key), do: nil

      defp compound_key_for_redis_key?(redis_key, compound_key)
           when is_binary(redis_key) and is_binary(compound_key),
           do: CompoundKey.extract_redis_key(compound_key) == redis_key

      defp compound_key_for_redis_key?(_redis_key, _compound_key), do: false

      defp non_neg_integer?(value), do: is_integer(value) and value >= 0

      defp segment_project_batch_hot_cache_threshold(entries, default_threshold) do
        if Enum.any?(entries, fn
             {key, _value, _expire_at_ms} -> segment_project_cold_flow_key?(key)
             _entry -> false
           end) do
          :per_key
        else
          {:ok, default_threshold}
        end
      end

      defp segment_project_hot_cache_threshold(shard_state, key) do
        segment_project_hot_cache_threshold(
          shard_state,
          key,
          ShardETS.hot_cache_threshold(shard_state)
        )
      end

      defp segment_project_hot_cache_threshold(_shard_state, key, default_threshold)
           when is_binary(key) do
        if segment_project_cold_flow_key?(key), do: 0, else: default_threshold
      end

      defp segment_project_hot_cache_threshold(_shard_state, _key, default_threshold),
        do: default_threshold

      defp segment_project_cold_flow_key?(key) when is_binary(key),
        do: FlowKeys.value_key?(key) or FlowKeys.history_key?(key) or FlowKeys.registry_key?(key)

      defp segment_project_cold_flow_key?(_key), do: false

      defp segment_project_check_fetch_or_compute_lock(_sm_state, nil, _owner_ref),
        do: {:error, :key_locked}

      defp segment_project_check_fetch_or_compute_lock(sm_state, key, owner_ref)
           when is_binary(key) do
        locks = Map.get(sm_state, :fetch_or_compute_locks, %{})

        if map_size(locks) == 0 do
          :ok
        else
          now = storage_expiry_cutoff_ms()

          case Map.get(locks, key) do
            nil ->
              :ok

            {^owner_ref, _expires_at_ms} ->
              :ok

            {_other_owner, expires_at_ms}
            when is_integer(expires_at_ms) and expires_at_ms <= now ->
              :ok

            {_other_owner, _expires_at_ms} ->
              {:error, :key_locked}
          end
        end
      end

      defp segment_project_check_fetch_or_compute_lock(_sm_state, _key, _owner_ref),
        do: {:error, :key_locked}

      defp segment_projection_put_entries_locked?(sm_state, entries) do
        Enum.any?(entries, fn
          {key, _value, _expire_at_ms} when is_binary(key) ->
            redis_key = CompoundKey.extract_redis_key(key)
            segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) != :ok

          _invalid ->
            false
        end)
      end

      defp segment_projection_keys_locked?(sm_state, keys) do
        Enum.any?(keys, fn
          key when is_binary(key) ->
            redis_key = CompoundKey.extract_redis_key(key)
            segment_project_check_fetch_or_compute_lock(sm_state, redis_key, nil) != :ok

          _invalid ->
            false
        end)
      end

      defp with_segment_projection_command_time({:ttb, binary}, fun) when is_binary(binary) do
        case CommandStamp.decode_ttb(binary) do
          {:ok, decoded} -> with_segment_projection_command_time(decoded, fun)
          {:error, :invalid_preencoded_command} -> fun.()
        end
      end

      defp with_segment_projection_command_time(
             {_inner_command,
              %{
                hlc_ts: {physical_ms, logical} = remote_ts,
                wall_time_ms: wall_time_ms
              }},
             fun
           )
           when is_integer(physical_ms) and is_integer(logical) and is_integer(wall_time_ms) do
        try do
          _ = HLC.update(remote_ts)
        rescue
          _error -> :ok
        end

        CommandTime.with_expiry_context(physical_ms, wall_time_ms, fun)
      end

      defp with_segment_projection_command_time(_command, fun), do: fun.()

      @doc false
      def __with_segment_projection_command_time_for_test__(command, fun),
        do: with_segment_projection_command_time(command, fun)

      if Mix.env() == :test do
        @doc false
        def __segment_live_promotion_marker_for_test__(sm_state, redis_key),
          do: segment_live_promotion_marker?(sm_state, redis_key)

        @doc false
        def __segment_project_check_fetch_or_compute_lock_for_test__(
              sm_state,
              key,
              owner_ref
            ),
            do: segment_project_check_fetch_or_compute_lock(sm_state, key, owner_ref)
      end

      defp emit_segment_projection_apply_telemetry(
             sm_state,
             command,
             started_at,
             result,
             applied_count
           ) do
        :telemetry.execute(
          [:ferricstore, :waraft, :segment_projection, :apply],
          %{
            duration_us: segment_projection_duration_us(started_at),
            applied_count: max(applied_count, 0)
          },
          %{
            shard_index: Map.get(sm_state, :shard_index, :unknown),
            command_shape: segment_projection_command_shape(command),
            result: segment_projection_result_class(result)
          }
        )
      rescue
        _ -> :ok
      end

      defp segment_projection_duration_us(started_at) do
        System.monotonic_time()
        |> Kernel.-(started_at)
        |> System.convert_time_unit(:native, :microsecond)
        |> max(0)
      end

      defp segment_projection_command_shape(command)
           when is_tuple(command) and tuple_size(command) > 0,
           do: elem(command, 0)

      defp segment_projection_command_shape(_command), do: :unknown

      defp segment_projection_result_class({:error, _reason}), do: :error
      defp segment_projection_result_class(_result), do: :ok
    end
  end
end
