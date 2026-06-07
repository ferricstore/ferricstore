defmodule FerricstoreServer.Connection.Pipeline.Streaming do
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

      defp dispatch_pure_single(idx, cmd, name, args, store, state, prefetched_reads) do
        {_name, _args, ast, keys} = command_parts(cmd)
        acl_name = ConnAuth.acl_command_name(name, args, ast)
        Stats.incr_commands(state.stats_counter)

        result =
          case ConnAuth.check_command_cached(state.acl_cache, acl_name) do
            :ok ->
              case ConnAuth.check_keys_cached(state.acl_cache, acl_name, keys) do
                :ok ->
                  try do
                    dispatch_pure_command(idx, name, args, ast, store, state, prefetched_reads)
                  catch
                    :exit, {:noproc, _} ->
                      {:error, "ERR server not ready, shard process unavailable"}

                    :exit, {reason, _} ->
                      internal_error(:exit, reason)

                    kind, reason ->
                      internal_error(kind, reason)
                  end

                {:error, _} = err ->
                  log_acl_denied(state, acl_name)
                  err
              end

            {:error, _} = err ->
              log_acl_denied(state, acl_name)
              err
          end

        {response_entry(result), track_pure_result(name, args, result, state)}
      end

      defp prefetch_pure_tcp_reads(commands, %{transport: transport} = state)
           when transport in [:ranch_tcp, :ranch_ssl] do
        {gets, mgets, getranges, _written_keys, _prefetch_blocked?} =
          commands
          |> Enum.with_index()
          |> Enum.reduce({[], [], [], MapSet.new(), false}, fn {cmd, idx},
                                                               {get_acc, mget_acc, getrange_acc,
                                                                written_keys, prefetch_blocked?} ->
            case command_parts(cmd) do
              {"GET", [key], {:get, key}, [key]} when is_binary(key) ->
                if prefetch_blocked? or MapSet.member?(written_keys, key) do
                  {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
                else
                  {
                    [{idx, key, namespace_key(state.sandbox_namespace, key)} | get_acc],
                    mget_acc,
                    getrange_acc,
                    written_keys,
                    prefetch_blocked?
                  }
                end

              {"MGET", keys, {:mget, keys}, keys} when is_list(keys) and keys != [] ->
                if prefetch_blocked? or any_written_key?(keys, written_keys) do
                  {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
                else
                  lookup_keys = namespace_keys(state.sandbox_namespace, keys)

                  {get_acc, [{idx, keys, lookup_keys} | mget_acc], getrange_acc, written_keys,
                   prefetch_blocked?}
                end

              {"GETRANGE", [key, _start_arg, _end_arg] = args,
               {:getrange, key, start_idx, end_idx}, [key]}
              when is_binary(key) and is_integer(start_idx) and is_integer(end_idx) ->
                if prefetch_blocked? or MapSet.member?(written_keys, key) do
                  {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
                else
                  {get_acc, mget_acc, [{idx, args, key, start_idx, end_idx} | getrange_acc],
                   written_keys, prefetch_blocked?}
                end

              {name, _args, _ast, keys} when is_list(keys) and keys != [] ->
                if read_only_keyed_command?(name) do
                  {get_acc, mget_acc, getrange_acc, written_keys, prefetch_blocked?}
                else
                  {get_acc, mget_acc, getrange_acc, mark_written_keys(written_keys, keys),
                   prefetch_blocked?}
                end

              {name, _args, _ast, keys} when is_list(keys) ->
                {get_acc, mget_acc, getrange_acc, written_keys,
                 prefetch_blocked? or prefetch_keyless_barrier_command?(name)}
            end
          end)

        gets
        |> Enum.reverse()
        |> prefetch_tcp_get_ops(state)
        |> Map.merge(prefetch_tcp_mget_ops(Enum.reverse(mgets), state))
        |> Map.merge(prefetch_tcp_getrange_ops(Enum.reverse(getranges), state))
      end

      defp prefetch_pure_tcp_reads(_commands, _state), do: %{}

      defp any_written_key?(keys, written_keys) do
        Enum.any?(keys, &MapSet.member?(written_keys, &1))
      end

      defp mark_written_keys(written_keys, keys) do
        Enum.reduce(keys, written_keys, fn key, acc -> MapSet.put(acc, key) end)
      end

      defp read_only_keyed_command?(name),
        do: MapSet.member?(@prefetch_read_only_keyed_cmds, name)

      defp prefetch_keyless_barrier_command?(name) do
        not MapSet.member?(@prefetch_read_only_keyless_cmds, name)
      end

      defp prefetch_tcp_get_ops(gets, state) do
        case gets do
          [] ->
            %{}

          _ ->
            lookup_keys = Enum.map(gets, fn {_idx, _key, lookup_key} -> lookup_key end)

            case safe_dispatch(fn ->
                   Router.batch_get_with_deferred_blob_file_refs(
                     state.instance_ctx,
                     lookup_keys,
                     ConnSendfile.threshold_bytes()
                   )
                 end) do
              {:ok, results} ->
                gets
                |> Enum.zip(results)
                |> Map.new(fn
                  {{idx, key, lookup_key}, {:file_ref, path, offset, size}} ->
                    {idx, {:file_ref, key, lookup_key, path, offset, size}}

                  {{idx, _key, _lookup_key}, value} ->
                    {idx, value}
                end)

              {:error, err} ->
                Map.new(gets, fn {idx, _key, _lookup_key} -> {idx, err} end)
            end
        end
      end

      defp prefetch_tcp_mget_ops(mgets, state) do
        Map.new(mgets, fn {idx, keys, lookup_keys} ->
          {idx, prefetch_tcp_mget_result(keys, lookup_keys, state)}
        end)
      end

      defp prefetch_tcp_getrange_ops(getranges, state) do
        Enum.reduce(getranges, %{}, fn {idx, args, key, start_idx, end_idx}, acc ->
          case ConnSendfile.getrange_cold_response(args, key, start_idx, end_idx, state) do
            :fallback ->
              acc

            result ->
              Map.put(acc, idx, result)
          end
        end)
      end

      defp prefetch_tcp_mget_result(keys, lookup_keys, state) do
        case safe_dispatch(fn ->
               Router.batch_get_with_deferred_blob_file_refs(
                 state.instance_ctx,
                 lookup_keys,
                 ConnSendfile.threshold_bytes()
               )
             end) do
          {:ok, results} ->
            if Enum.any?(results, &match?({:file_ref, _, _, _}, &1)) do
              {:array, keys, mget_response_elements(keys, lookup_keys, results)}
            else
              results
            end

          {:error, err} ->
            err
        end
      end

      defp mget_response_elements(keys, lookup_keys, results) do
        keys
        |> Enum.zip(lookup_keys)
        |> Enum.zip(results)
        |> Enum.map(fn
          {{key, lookup_key}, {:file_ref, path, offset, size}} ->
            {:file_ref, key, lookup_key, path, offset, size}

          {{_key, _lookup_key}, value} ->
            {:encoded, Encoder.encode(value)}
        end)
      end

      defp dispatch_pure_command(idx, name, args, ast, store, state, prefetched_reads) do
        case Map.fetch(prefetched_reads, idx) do
          {:ok, result} ->
            result

          :error ->
            dispatch_store_command(
              name,
              args,
              ast,
              store,
              state.instance_ctx,
              state.sandbox_namespace
            )
        end
      end

      defp response_entry({:file_ref, _key, _lookup_key, _path, _offset, _size} = file_ref),
        do: file_ref

      defp response_entry(
             {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size, _validator} =
               file_range
           ),
           do: file_range

      defp response_entry({:array, _keys, _elements} = array), do: array
      defp response_entry({:value, value}), do: {:encoded, Encoder.encode(value)}

      defp response_entry(result), do: {:encoded, Encoder.encode(result)}

      defp streaming_response?({:file_ref, _key, _lookup_key, _path, _offset, _size}), do: true

      defp streaming_response?(
             {:file_range, _args, _key, _start_idx, _end_idx, _path, _offset, _size, _validator}
           ),
           do: true

      defp streaming_response?({:array, _keys, _elements}), do: true
      defp streaming_response?(_result), do: false

      defp track_pure_result(name, args, result, state) when is_tuple(result) do
        if streaming_response?(result) do
          state
        else
          track_non_streaming_pure_result(name, args, result, state)
        end
      end

      defp track_pure_result(name, args, result, state),
        do: track_non_streaming_pure_result(name, args, result, state)

      defp track_non_streaming_pure_result(name, args, result, state) do
        ConnTracking.maybe_notify_keyspace(name, args, result)
        ConnTracking.maybe_notify_tracking(name, args, result, state)
        ConnTracking.maybe_track_read(name, args, result, state)
      end

      defp stream_response_entries(entries, state, send_response_fn) do
        {result, file_cache} =
          Enum.reduce_while(
            entries,
            {{:continue, state}, ConnSendfile.new_file_cache()},
            fn
              {:array, keys, elements}, {{:continue, acc_state}, file_cache} ->
                case stream_array_response(
                       keys,
                       elements,
                       acc_state,
                       send_response_fn,
                       file_cache
                     ) do
                  {{:sent, new_state}, new_cache} ->
                    {:cont, {{:continue, new_state}, new_cache}}

                  {{:error_after_header, _reason}, new_cache} ->
                    {:halt, {{:quit, acc_state}, new_cache}}
                end

              {:file_range, args, key, start_idx, end_idx, path, offset, size, validator},
              {{:continue, acc_state}, file_cache} ->
                case ConnSendfile.send_file_range_response_cached(
                       args,
                       path,
                       offset,
                       size,
                       validator,
                       acc_state,
                       file_cache
                     ) do
                  {:sent, new_state, new_cache} ->
                    {:cont, {{:continue, new_state}, new_cache}}

                  {:fallback, new_cache} ->
                    case send_response_result(
                           acc_state,
                           send_response_fn,
                           Encoder.encode(
                             ConnSendfile.materialize_getrange(key, start_idx, end_idx, acc_state)
                           )
                         ) do
                      :ok ->
                        tracked_state =
                          ConnTracking.maybe_track_read("GETRANGE", args, :fallback_ok, acc_state)

                        {:cont, {{:continue, tracked_state}, new_cache}}

                      {:error, _reason} ->
                        {:halt, {{:quit, acc_state}, new_cache}}
                    end

                  {:error_after_header, _reason, new_cache} ->
                    {:halt, {{:quit, acc_state}, new_cache}}
                end

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
                        tracked_state =
                          ConnTracking.maybe_track_read("GET", [key], :fallback_ok, acc_state)

                        {:cont, {{:continue, tracked_state}, new_cache}}

                      {:error, _reason} ->
                        {:halt, {{:quit, acc_state}, new_cache}}
                    end

                  {:error_after_header, _reason, new_cache} ->
                    {:halt, {{:quit, acc_state}, new_cache}}
                end

              {:encoded, encoded}, {{:continue, acc_state}, file_cache} ->
                case send_response_result(acc_state, send_response_fn, encoded) do
                  :ok -> {:cont, {{:continue, acc_state}, file_cache}}
                  {:error, _reason} -> {:halt, {{:quit, acc_state}, file_cache}}
                end
            end
          )

        ConnSendfile.close_file_cache(file_cache)
        result
      end

      defp stream_array_response(keys, elements, state, send_response_fn, file_cache) do
        {result, file_cache} =
          case send_response_result(state, send_response_fn, [
                 "*",
                 Integer.to_string(length(elements)),
                 "\r\n"
               ]) do
            :ok ->
              Enum.reduce_while(elements, {{:sent, state}, file_cache}, fn
                {:file_ref, key, lookup_key, path, offset, size},
                {{:sent, acc_state}, file_cache} ->
                  case ConnSendfile.send_file_ref_element_response_cached(
                         key,
                         lookup_key,
                         path,
                         offset,
                         size,
                         acc_state,
                         file_cache
                       ) do
                    {:sent, new_state, new_cache} ->
                      {:cont, {{:sent, new_state}, new_cache}}

                    {:fallback, new_cache} ->
                      case send_response_result(
                             acc_state,
                             send_response_fn,
                             Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                           ) do
                        :ok -> {:cont, {{:sent, acc_state}, new_cache}}
                        {:error, reason} -> {:halt, {{:error_after_header, reason}, new_cache}}
                      end

                    {:error_after_header, reason, new_cache} ->
                      {:halt, {{:error_after_header, reason}, new_cache}}
                  end

                {:encoded, encoded}, {{:sent, acc_state}, file_cache} ->
                  case send_response_result(acc_state, send_response_fn, encoded) do
                    :ok -> {:cont, {{:sent, acc_state}, file_cache}}
                    {:error, reason} -> {:halt, {{:error_after_header, reason}, file_cache}}
                  end
              end)

            {:error, reason} ->
              {{:error_after_header, reason}, file_cache}
          end

        result =
          case result do
            {:sent, new_state} ->
              {:sent, ConnTracking.maybe_track_read("MGET", keys, :sendfile_ok, new_state)}

            other ->
              other
          end

        {result, file_cache}
      end
    end
  end
end
