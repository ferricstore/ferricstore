defmodule FerricstoreServer.Connection.Pipeline.FastPaths do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias FerricstoreServer.Resp.Encoder
      alias Ferricstore.Commands.Dispatcher
      alias Ferricstore.Commands.Flow, as: FlowCommand
      alias Ferricstore.Stats
      alias Ferricstore.Store.PipelinePlanner
      alias Ferricstore.Store.Router
      alias FerricstoreServer.Connection.Auth, as: ConnAuth
      alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
      alias FerricstoreServer.Connection.Store, as: ConnStore
      alias FerricstoreServer.Connection.TcpOpts
      alias FerricstoreServer.Connection.Tracking, as: ConnTracking

      require Logger

      defp try_batch_get_fast_path(commands, state, send_response_fn) do
        if requires_sequential_dispatch?(state) do
          :fallback
        else
          case extract_plain_gets(commands) do
            {:ok, keys} ->
              # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
              if not full_acl_fast_path?(state.acl_cache) do
                :fallback
              else
                Stats.incr_commands_by(state.stats_counter, length(keys))

                result =
                  case state.sandbox_namespace do
                    nil ->
                      dispatch_batch_get_results(keys, keys, state, send_response_fn)

                    namespace ->
                      planned_keys =
                        PipelinePlanner.plan_keys(state.instance_ctx, keys, namespace)

                      dispatch_planned_batch_get_results(
                        planned_keys,
                        keys,
                        state,
                        send_response_fn
                      )
                  end

                {:ok, result}
              end

            :fallback ->
              :fallback
          end
        end
      end

      defp extract_plain_gets(commands), do: extract_plain_gets(commands, [])

      defp extract_plain_gets([], acc), do: {:ok, Enum.reverse(acc)}

      defp extract_plain_gets([{:command, "GET", [key], {:get, key}, [key]} | rest], acc)
           when is_binary(key),
           do: extract_plain_gets(rest, [key | acc])

      defp extract_plain_gets(_, _acc), do: :fallback

      defp dispatch_batch_get_results(
             keys,
             lookup_keys,
             %{transport: transport} = state,
             send_response_fn
           )
           when transport in [:ranch_tcp, :ranch_ssl] do
        case safe_dispatch(fn ->
               Router.batch_get_with_deferred_blob_file_refs_and_presence(
                 state.instance_ctx,
                 lookup_keys,
                 ConnSendfile.threshold_bytes()
               )
             end) do
          {:ok, {results, has_file_ref?}} ->
            if has_file_ref? do
              stream_get_results(keys, lookup_keys, results, state, send_response_fn)
            else
              next_state = ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)

              send_or_quit(
                state,
                send_response_fn,
                Encoder.encode_bulk_strings_or_nulls(results),
                next_state
              )
            end

          {:error, err} ->
            send_or_quit(
              state,
              send_response_fn,
              List.duplicate(Encoder.encode(err), length(keys))
            )
        end
      end

      defp dispatch_batch_get_results(keys, lookup_keys, state, send_response_fn) do
        case safe_dispatch(fn -> Router.batch_get(state.instance_ctx, lookup_keys) end) do
          {:ok, values} ->
            next_state = ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)

            send_or_quit(
              state,
              send_response_fn,
              Encoder.encode_bulk_strings_or_nulls(values),
              next_state
            )

          {:error, err} ->
            send_or_quit(
              state,
              send_response_fn,
              List.duplicate(Encoder.encode(err), length(lookup_keys))
            )
        end
      end

      defp dispatch_planned_batch_get_results(
             planned_keys,
             keys,
             %{transport: transport} = state,
             send_response_fn
           )
           when transport in [:ranch_tcp, :ranch_ssl] do
        case safe_dispatch(fn ->
               Router.batch_get_with_deferred_blob_file_refs_planned_and_presence(
                 state.instance_ctx,
                 planned_keys,
                 ConnSendfile.threshold_bytes()
               )
             end) do
          {:ok, {results, has_file_ref?}} ->
            if has_file_ref? do
              stream_get_results(planned_keys, results, state, send_response_fn)
            else
              next_state = ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)

              send_or_quit(
                state,
                send_response_fn,
                Encoder.encode_bulk_strings_or_nulls(results),
                next_state
              )
            end

          {:error, err} ->
            send_or_quit(
              state,
              send_response_fn,
              List.duplicate(Encoder.encode(err), length(planned_keys))
            )
        end
      end

      defp dispatch_planned_batch_get_results(planned_keys, keys, state, send_response_fn) do
        case safe_dispatch(fn -> Router.batch_get_planned(state.instance_ctx, planned_keys) end) do
          {:ok, values} ->
            next_state = ConnTracking.maybe_track_read("MGET", keys, :pipeline_ok, state)

            send_or_quit(
              state,
              send_response_fn,
              Encoder.encode_bulk_strings_or_nulls(values),
              next_state
            )

          {:error, err} ->
            send_or_quit(
              state,
              send_response_fn,
              List.duplicate(Encoder.encode(err), length(planned_keys))
            )
        end
      end

      defp stream_get_results(keys, lookup_keys, results, state, send_response_fn) do
        TcpOpts.set_cork(state.socket, true)

        {result, file_cache} =
          keys
          |> Enum.zip(lookup_keys)
          |> Enum.zip(results)
          |> Enum.reduce_while({{:continue, state}, ConnSendfile.new_file_cache()}, fn
            {{key, lookup_key}, {:file_ref, path, offset, size}},
            {{:continue, acc_state}, file_cache} ->
              case ConnSendfile.send_file_ref_response_cached(
                     key,
                     lookup_key,
                     path,
                     offset,
                     size,
                     acc_state,
                     file_cache
                   ) do
                {:sent, new_state, new_cache} ->
                  {:cont, {{:continue, new_state}, new_cache}}

                {:fallback, new_cache} ->
                  value =
                    Router.get(
                      acc_state.instance_ctx,
                      namespace_key(acc_state.sandbox_namespace, key)
                    )

                  case send_response_result(acc_state, send_response_fn, Encoder.encode(value)) do
                    :ok ->
                      tracked_state =
                        ConnTracking.maybe_track_read("GET", [key], value, acc_state)

                      {:cont, {{:continue, tracked_state}, new_cache}}

                    {:error, _reason} ->
                      {:halt, {{:quit, acc_state}, new_cache}}
                  end

                {:error_after_header, _reason, new_cache} ->
                  {:halt, {{:quit, acc_state}, new_cache}}
              end

            {{key, _lookup_key}, value}, {{:continue, acc_state}, file_cache} ->
              case send_response_result(acc_state, send_response_fn, Encoder.encode(value)) do
                :ok ->
                  tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
                  {:cont, {{:continue, tracked_state}, file_cache}}

                {:error, _reason} ->
                  {:halt, {{:quit, acc_state}, file_cache}}
              end
          end)

        ConnSendfile.close_file_cache(file_cache)

        TcpOpts.set_cork(state.socket, false)
        result
      end

      defp stream_get_results(planned_keys, results, state, send_response_fn) do
        TcpOpts.set_cork(state.socket, true)

        {result, file_cache} =
          planned_keys
          |> Enum.zip(results)
          |> Enum.reduce_while({{:continue, state}, ConnSendfile.new_file_cache()}, fn
            {{key, lookup_key, _shard_index, _keydir}, {:file_ref, path, offset, size}},
            {{:continue, acc_state}, file_cache} ->
              case ConnSendfile.send_file_ref_response_cached(
                     key,
                     lookup_key,
                     path,
                     offset,
                     size,
                     acc_state,
                     file_cache
                   ) do
                {:sent, new_state, new_cache} ->
                  {:cont, {{:continue, new_state}, new_cache}}

                {:fallback, new_cache} ->
                  value = Router.get(acc_state.instance_ctx, lookup_key)

                  case send_response_result(acc_state, send_response_fn, Encoder.encode(value)) do
                    :ok ->
                      tracked_state =
                        ConnTracking.maybe_track_read("GET", [key], value, acc_state)

                      {:cont, {{:continue, tracked_state}, new_cache}}

                    {:error, _reason} ->
                      {:halt, {{:quit, acc_state}, new_cache}}
                  end

                {:error_after_header, _reason, new_cache} ->
                  {:halt, {{:quit, acc_state}, new_cache}}
              end

            {{key, _lookup_key, _shard_index, _keydir}, value},
            {{:continue, acc_state}, file_cache} ->
              case send_response_result(acc_state, send_response_fn, Encoder.encode(value)) do
                :ok ->
                  tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
                  {:cont, {{:continue, tracked_state}, file_cache}}

                {:error, _reason} ->
                  {:halt, {{:quit, acc_state}, file_cache}}
              end
          end)

        ConnSendfile.close_file_cache(file_cache)

        TcpOpts.set_cork(state.socket, false)
        result
      end

      # ---------------------------------------------------------------------------
      # Batch SET fast path
      # ---------------------------------------------------------------------------
      # When ALL commands in a pipeline batch are plain `SET key value` (no options),
      # bypass the normal sliding_window_dispatch which serializes each SET.
      #
      # Submit all SETs to Batcher(s) concurrently, wait for Ra commits, then send
      # all responses. This lets all SETs share the same Batcher batch + WAL
      # fdatasync instead of serializing through it.

      defp try_batch_set_fast_path(commands, state, send_response_fn) do
        if requires_sequential_dispatch?(state) do
          :fallback
        else
          case extract_plain_sets(commands) do
            {:ok, kv_pairs} ->
              ctx = state.instance_ctx
              write_pairs = namespace_kv_pairs(state.sandbox_namespace, kv_pairs)

              # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
              if not full_acl_fast_path?(state.acl_cache) do
                :fallback
              else
                pressure_ok =
                  :atomics.get(ctx.pressure_flags, 1) == 0 and
                    :atomics.get(ctx.pressure_flags, 2) == 0

                # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
                if not pressure_ok do
                  :fallback
                else
                  {:ok, do_batch_set_quorum(write_pairs, state, send_response_fn)}
                end
              end

            :fallback ->
              :fallback
          end
        end
      end

      defp extract_plain_sets(commands) do
        extract_plain_sets(commands, [])
      end

      defp extract_plain_sets([], acc), do: {:ok, Enum.reverse(acc)}

      defp extract_plain_sets(
             [{:command, "SET", [key, value], {:set, key, value}, [key]} | rest],
             acc
           )
           when is_binary(key) and is_binary(value) and
                  byte_size(key) <= @max_key_size and byte_size(value) < @max_value_size,
           do: extract_plain_sets(rest, [{key, value} | acc])

      defp extract_plain_sets(_, _acc), do: :fallback

      defp do_batch_set_quorum(kv_pairs, state, send_response_fn) do
        Stats.incr_commands_by(state.stats_counter, length(kv_pairs))

        response =
          case safe_dispatch(fn -> Router.batch_quorum_put(state.instance_ctx, kv_pairs) end) do
            {:ok, results} ->
              kv_pairs
              |> Enum.zip(results)
              |> Enum.map(fn
                {{key, value}, :ok} ->
                  notify_pipeline_set_success(key, value, state)
                  Encoder.ok_response()

                {_kv_pair, {:error, _} = err} ->
                  Encoder.encode(err)
              end)

            {:error, err} ->
              List.duplicate(Encoder.encode(err), length(kv_pairs))
          end

        send_or_quit(state, send_response_fn, response)
      end

      # ---------------------------------------------------------------------------
      # Mixed GET+SET fast path
      # ---------------------------------------------------------------------------
      # When a pipeline contains only plain GETs and plain SETs (the 80/20 case),
      # split into two groups, batch each, then reassemble responses in order.

      defp try_mixed_fast_path(commands, state, handle_command_fn, send_response_fn) do
        if requires_sequential_dispatch?(state) do
          general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
        else
          # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
          if not full_acl_fast_path?(state.acl_cache) do
            general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
          else
            case classify_mixed_pipeline(commands) do
              {:ok, ops} ->
                do_mixed_fast_path(ops, state, send_response_fn)

              :fallback ->
                general_batch_dispatch(commands, state, handle_command_fn, send_response_fn)
            end
          end
        end
      end

      defp classify_mixed_pipeline(commands),
        do: classify_mixed_pipeline(commands, [], MapSet.new(), MapSet.new())

      defp classify_mixed_pipeline([], acc, _read_keys, _written_keys),
        do: {:ok, Enum.reverse(acc)}

      defp classify_mixed_pipeline(
             [{:command, "GET", [key], {:get, key}, [key]} | rest],
             acc,
             read_keys,
             written_keys
           )
           when is_binary(key) do
        if MapSet.member?(written_keys, key) do
          :fallback
        else
          classify_mixed_pipeline(
            rest,
            [{:get, key} | acc],
            MapSet.put(read_keys, key),
            written_keys
          )
        end
      end

      defp classify_mixed_pipeline(
             [{:command, "SET", [key, value], {:set, key, value}, [key]} | rest],
             acc,
             read_keys,
             written_keys
           )
           when is_binary(key) and is_binary(value) and byte_size(key) <= @max_key_size and
                  byte_size(value) < @max_value_size do
        if MapSet.member?(read_keys, key) do
          :fallback
        else
          classify_mixed_pipeline(
            rest,
            [{:set, key, value} | acc],
            read_keys,
            MapSet.put(written_keys, key)
          )
        end
      end

      defp classify_mixed_pipeline(_, _, _, _), do: :fallback

      defp do_mixed_fast_path(ops, state, send_response_fn) do
        ctx = state.instance_ctx
        count = length(ops)
        Stats.incr_commands_by(state.stats_counter, count)

        get_keys = for {:get, key} <- ops, do: key
        lookup_keys = namespace_keys(state.sandbox_namespace, get_keys)
        set_pairs = for {:set, key, value} <- ops, do: {key, value}
        write_pairs = namespace_kv_pairs(state.sandbox_namespace, set_pairs)

        # Reads and writes are disjoint here, so SETs can be grouped without
        # changing any GET result in the same pipeline.
        set_slots =
          case write_pairs do
            [] ->
              []

            _ ->
              pressure_ok =
                :atomics.get(ctx.pressure_flags, 1) == 0 and
                  :atomics.get(ctx.pressure_flags, 2) == 0

              # credo:disable-for-next-line Credo.Check.Refactor.NegatedConditionsWithElse
              results =
                if not pressure_ok do
                  List.duplicate({:error, "ERR server under pressure"}, length(write_pairs))
                else
                  case safe_dispatch(fn -> mixed_set_results(ctx, write_pairs) end) do
                    {:ok, results} -> results
                    {:error, err} -> List.duplicate(err, length(write_pairs))
                  end
                end

              encode_mixed_set_slots(set_pairs, results, state)
          end

        {get_slots, has_file_ref?} =
          case get_keys do
            [] ->
              {[], false}

            _ ->
              get_dispatch =
                if state.transport in [:ranch_tcp, :ranch_ssl] do
                  fn ->
                    Router.batch_get_with_deferred_blob_file_refs_and_presence(
                      ctx,
                      lookup_keys,
                      ConnSendfile.threshold_bytes()
                    )
                  end
                else
                  fn -> Router.batch_get(ctx, lookup_keys) end
                end

              case safe_dispatch(get_dispatch) do
                {:ok, {values, has_file_ref?}} ->
                  slots = encode_mixed_get_slots(get_keys, lookup_keys, values)
                  {slots, has_file_ref?}

                {:ok, values} ->
                  slots = encode_mixed_get_slots(get_keys, lookup_keys, values)
                  {slots, has_file_ref_slot?(slots)}

                {:error, err} ->
                  {List.duplicate(Encoder.encode(err), length(get_keys)), false}
              end
          end

        response_slots = assemble_mixed_slots(ops, get_slots, set_slots)

        if has_file_ref? do
          stream_mixed_results(response_slots, state, send_response_fn)
        else
          response = Enum.map(response_slots, &response_iodata/1)

          send_or_quit(
            state,
            send_response_fn,
            response,
            track_mixed_get_results(response_slots, state)
          )
        end
      end

      defp encode_mixed_get_slots(get_keys, lookup_keys, values) do
        get_keys
        |> Enum.zip(lookup_keys)
        |> Enum.zip(values)
        |> Enum.map(fn
          {{key, lookup_key}, {:file_ref, path, offset, size}} ->
            {:file_ref, key, lookup_key, path, offset, size}

          {{key, _lookup_key}, value} ->
            {:get_encoded, key, Encoder.encode(value), value}
        end)
      end

      defp encode_mixed_set_slots(set_pairs, results, state) do
        set_pairs
        |> Enum.zip(results)
        |> Enum.map(fn
          {{key, value}, :ok} ->
            notify_pipeline_set_success(key, value, state)
            Encoder.ok_response()

          {_set_pair, {:error, _} = err} ->
            Encoder.encode(err)
        end)
      end

      defp has_file_ref_slot?(slots), do: Enum.any?(slots, &file_ref_slot?/1)

      defp file_ref_slot?({:file_ref, _key, _lookup_key, _path, _offset, _size}), do: true
      defp file_ref_slot?(_slot), do: false

      defp assemble_mixed_slots(ops, get_slots, set_slots),
        do: assemble_mixed_slots(ops, get_slots, set_slots, [])

      defp assemble_mixed_slots([], [], [], acc), do: Enum.reverse(acc)

      defp assemble_mixed_slots([{:get, _key} | rest], [slot | get_slots], set_slots, acc),
        do: assemble_mixed_slots(rest, get_slots, set_slots, [slot | acc])

      defp assemble_mixed_slots(
             [{:set, _key, _value} | rest],
             get_slots,
             [slot | set_slots],
             acc
           ),
           do: assemble_mixed_slots(rest, get_slots, set_slots, [slot | acc])

      defp response_iodata({:get_encoded, _key, encoded, _value}), do: encoded
      defp response_iodata(encoded), do: encoded

      defp track_mixed_get_results(results, state) do
        Enum.reduce(results, state, fn
          {:get_encoded, key, _encoded, value}, acc_state ->
            ConnTracking.maybe_track_read("GET", [key], value, acc_state)

          _other, acc_state ->
            acc_state
        end)
      end

      defp stream_mixed_results(response_slots, state, send_response_fn) do
        TcpOpts.set_cork(state.socket, true)

        {result, file_cache} =
          Enum.reduce_while(
            response_slots,
            {{:continue, state}, ConnSendfile.new_file_cache()},
            fn
              {:file_ref, key, lookup_key, path, offset, size},
              {{:continue, acc_state}, file_cache} ->
                case ConnSendfile.send_file_ref_response_cached(
                       key,
                       lookup_key,
                       path,
                       offset,
                       size,
                       acc_state,
                       file_cache
                     ) do
                  {:sent, new_state, new_cache} ->
                    {:cont, {{:continue, new_state}, new_cache}}

                  {:fallback, new_cache} ->
                    case send_response_result(
                           acc_state,
                           send_response_fn,
                           Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                         ) do
                      :ok ->
                        {:cont,
                         {{:continue,
                           ConnTracking.maybe_track_read("GET", [key], :fallback_ok, acc_state)},
                          new_cache}}

                      {:error, _reason} ->
                        {:halt, {{:quit, acc_state}, new_cache}}
                    end

                  {:error_after_header, _reason, new_cache} ->
                    {:halt, {{:quit, acc_state}, new_cache}}
                end

              {:get_encoded, key, encoded, value}, {{:continue, acc_state}, file_cache} ->
                case send_response_result(acc_state, send_response_fn, encoded) do
                  :ok ->
                    tracked_state = ConnTracking.maybe_track_read("GET", [key], value, acc_state)
                    {:cont, {{:continue, tracked_state}, file_cache}}

                  {:error, _reason} ->
                    {:halt, {{:quit, acc_state}, file_cache}}
                end

              encoded, {{:continue, acc_state}, file_cache} ->
                case send_response_result(acc_state, send_response_fn, encoded) do
                  :ok -> {:cont, {{:continue, acc_state}, file_cache}}
                  {:error, _reason} -> {:halt, {{:quit, acc_state}, file_cache}}
                end
            end
          )

        ConnSendfile.close_file_cache(file_cache)

        TcpOpts.set_cork(state.socket, false)
        result
      end

      defp mixed_set_results(ctx, kv_pairs) do
        Router.batch_quorum_put(ctx, kv_pairs)
      end

      defp notify_pipeline_set_success(key, value, state) do
        ConnTracking.maybe_notify_keyspace("SET", [key, value], :ok)
        ConnTracking.maybe_notify_tracking("SET", [key, value], :ok, state)
      end

      # ---------------------------------------------------------------------------
      # Batch FLOW write fast path
      # ---------------------------------------------------------------------------
      # Pipelined one-by-one Flow writes keep per-command semantics, but can still
      # share one Raft batch per shard. This is intentionally not FLOW.*_MANY: one
      # bad command fails only that command, while surrounding writes still commit.
      #
      # Terminal-capable commands stay on the normal router path: they may need
      # cross-shard parent/child coordination that the independent batch path does
      # not own. FLOW.RETRY is included because exhausted retries can enter a
      # terminal state.
    end
  end
end
