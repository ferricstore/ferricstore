defmodule FerricstoreServer.Connection.Pipeline.PureBatch do
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

      defp general_batch_dispatch(commands, state, handle_command_fn, send_response_fn) do
        if requires_sequential_dispatch?(state) or not full_acl_fast_path?(state.acl_cache) do
          sequential_dispatch(commands, state, handle_command_fn, send_response_fn)
        else
          case split_at_stateful(commands, state) do
            {:all_pure, pure_cmds} ->
              do_batch_pure(pure_cmds, state, send_response_fn)

            {:split, pure_prefix, stateful_cmd, rest} ->
              case do_batch_pure(pure_prefix, state, send_response_fn) do
                {:quit, _} = quit ->
                  quit

                {:continue, new_state} ->
                  case handle_command_fn.(stateful_cmd, new_state) do
                    {:quit, response, quit_state} ->
                      _ = send_response_result(quit_state, send_response_fn, response)
                      {:quit, quit_state}

                    {:continue, response, new_state2} ->
                      case send_response_result(new_state2, send_response_fn, response) do
                        :ok ->
                          if rest == [] do
                            {:continue, new_state2}
                          else
                            general_batch_dispatch(
                              rest,
                              new_state2,
                              handle_command_fn,
                              send_response_fn
                            )
                          end

                        {:error, _reason} ->
                          {:quit, new_state2}
                      end
                  end
              end
          end
        end
      end

      defp split_at_stateful(commands, state) do
        split_at_stateful(commands, state, [])
      end

      defp split_at_stateful([], _state, acc), do: {:all_pure, Enum.reverse(acc)}

      defp split_at_stateful([cmd | rest], state, acc) do
        name = extract_command_name(cmd)

        if stateful_pipeline_command?(cmd) or MapSet.member?(@stateful_cmds, name) or
             (is_binary(name) and String.starts_with?(name, "CLIENT")) do
          {:split, Enum.reverse(acc), cmd, rest}
        else
          split_at_stateful(rest, state, [cmd | acc])
        end
      end

      defp stateful_pipeline_command?(
             {:command, "FLOW.CLAIM_DUE", _args, {:flow_claim_due, _type, opts}, _keys}
           )
           when is_list(opts),
           do: Keyword.has_key?(opts, :block_ms)

      defp stateful_pipeline_command?(
             {:command, "XREAD", _args, {:xread, _count, {:block, _timeout_ms}, _stream_ids},
              _keys}
           ),
           do: true

      defp stateful_pipeline_command?(
             {:command, "XREADGROUP", _args,
              {:xreadgroup, _group, _consumer, {_count, {:block, _timeout_ms}, _stream_ids}},
              _keys}
           ),
           do: true

      defp stateful_pipeline_command?({:command, "FETCH_OR_COMPUTE", _args, _ast, _keys}),
        do: true

      defp stateful_pipeline_command?(_command), do: false

      defp do_batch_pure([], state, _send_response_fn), do: {:continue, state}

      defp do_batch_pure(commands, state, send_response_fn) do
        store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

        prefetched_reads =
          if has_flow_write?(commands) do
            %{}
          else
            prefetch_pure_tcp_reads(commands, state)
          end

        {action, responses, final_state} =
          commands
          |> Enum.with_index()
          |> dispatch_pure_segments(state, store, prefetched_reads, [])

        responses = Enum.reverse(responses)

        if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, true)

        result =
          if Enum.any?(responses, &streaming_response?/1) do
            stream_response_entries(responses, final_state, send_response_fn)
          else
            response = Enum.map(responses, fn {:encoded, encoded} -> encoded end)

            case send_response_result(final_state, send_response_fn, response) do
              :ok -> {action, final_state}
              {:error, _reason} -> {:quit, final_state}
            end
          end

        if state.transport == :ranch_tcp, do: TcpOpts.set_cork(state.socket, false)
        result
      end

      defp dispatch_pure_segments([], state, _store, _prefetched_reads, acc) do
        {:continue, acc, state}
      end

      defp dispatch_pure_segments([{cmd, idx} | rest], state, store, prefetched_reads, acc) do
        case flow_write_op(cmd) do
          {:ok, op} ->
            {ops, rest} = take_flow_write_segment(rest, [op])
            entries = dispatch_flow_write_segment(Enum.reverse(ops), state)

            dispatch_pure_segments(
              rest,
              state,
              store,
              prefetched_reads,
              prepend_pipeline_entries(entries, acc)
            )

          :fallback ->
            case flow_claim_due_op(cmd) do
              {:ok, op} ->
                {ops, rest} = take_flow_claim_due_segment(rest, [op])
                entries = dispatch_flow_claim_due_segment(Enum.reverse(ops), state)

                dispatch_pure_segments(
                  rest,
                  state,
                  store,
                  prefetched_reads,
                  prepend_pipeline_entries(entries, acc)
                )

              :fallback ->
                case flow_read_op(cmd) do
                  {:ok, op} ->
                    {ops, rest} = take_flow_read_segment(rest, [op])
                    entries = dispatch_flow_read_segment(Enum.reverse(ops), state)

                    dispatch_pure_segments(
                      rest,
                      state,
                      store,
                      prefetched_reads,
                      prepend_pipeline_entries(entries, acc)
                    )

                  :fallback ->
                    case phase1_write_op(cmd, idx, state) do
                      {:ok, op} ->
                        {ops, rest} = take_phase1_write_segment(rest, [op], state)

                        {entries, new_state} =
                          dispatch_phase1_write_segment(
                            Enum.reverse(ops),
                            state,
                            store,
                            prefetched_reads
                          )

                        dispatch_pure_segments(
                          rest,
                          new_state,
                          store,
                          prefetched_reads,
                          prepend_pipeline_entries(entries, acc)
                        )

                      :fallback ->
                        {name, args, _ast, _keys} = command_parts(cmd)

                        {entry, new_state} =
                          dispatch_pure_single(
                            idx,
                            cmd,
                            name,
                            args,
                            store,
                            state,
                            prefetched_reads
                          )

                        dispatch_pure_segments(rest, new_state, store, prefetched_reads, [
                          entry | acc
                        ])
                    end
                end
            end
        end
      end

      defp prepend_pipeline_entries(entries, acc) do
        Enum.reduce(entries, acc, fn entry, next_acc -> [entry | next_acc] end)
      end

      defp take_flow_write_segment([{cmd, idx} | rest], acc) do
        case flow_write_op(cmd) do
          {:ok, op} -> take_flow_write_segment(rest, [op | acc])
          :fallback -> {acc, [{cmd, idx} | rest]}
        end
      end

      defp take_flow_write_segment([], acc), do: {acc, []}

      defp take_flow_claim_due_segment([{cmd, idx} | rest], acc) do
        case flow_claim_due_op(cmd) do
          {:ok, op} -> take_flow_claim_due_segment(rest, [op | acc])
          :fallback -> {acc, [{cmd, idx} | rest]}
        end
      end

      defp take_flow_claim_due_segment([], acc), do: {acc, []}

      defp take_flow_read_segment([{cmd, idx} | rest], acc) do
        case flow_read_op(cmd) do
          {:ok, op} -> take_flow_read_segment(rest, [op | acc])
          :fallback -> {acc, [{cmd, idx} | rest]}
        end
      end

      defp take_flow_read_segment([], acc), do: {acc, []}

      defp take_phase1_write_segment([{cmd, idx} | rest], acc, state) do
        case phase1_write_op(cmd, idx, state) do
          {:ok, op} -> take_phase1_write_segment(rest, [op | acc], state)
          :fallback -> {acc, [{cmd, idx} | rest]}
        end
      end

      defp take_phase1_write_segment([], acc, _state), do: {acc, []}

      defp dispatch_flow_write_segment(ops, state) do
        Stats.incr_commands_by(state.stats_counter, length(ops))

        case safe_dispatch(fn ->
               Ferricstore.Flow.pipeline_write_batch_independent(state.instance_ctx, ops)
             end) do
          {:ok, results} ->
            Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

          {:error, err} ->
            List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
        end
      end

      defp dispatch_flow_claim_due_segment(ops, state) do
        Stats.incr_commands_by(state.stats_counter, length(ops))

        case safe_dispatch(fn ->
               Ferricstore.Flow.pipeline_claim_due_batch(state.instance_ctx, ops)
             end) do
          {:ok, results} ->
            Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

          {:error, err} ->
            List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
        end
      end

      defp dispatch_flow_read_segment(ops, state) do
        Stats.incr_commands_by(state.stats_counter, length(ops))

        case safe_dispatch(fn ->
               Ferricstore.Flow.pipeline_read_batch(state.instance_ctx, ops)
             end) do
          {:ok, results} ->
            Enum.map(results, fn result -> {:encoded, encode_flow_result(result)} end)

          {:error, err} ->
            List.duplicate({:encoded, Encoder.encode(err)}, length(ops))
        end
      end

      defp dispatch_phase1_write_segment(ops, state, store, prefetched_reads) do
        if phase1_write_pressure_ok?(state.instance_ctx) do
          Stats.incr_commands_by(state.stats_counter, length(ops))

          keyed_commands = Enum.map(ops, fn op -> {op.key, op.command} end)

          case safe_dispatch(fn ->
                 Router.pipeline_write_batch(state.instance_ctx, keyed_commands)
               end) do
            {:ok, results} ->
              Enum.zip(ops, results)
              |> Enum.reduce({[], state}, fn {op, result}, {entries, acc_state} ->
                {
                  [response_entry(result) | entries],
                  track_non_streaming_pure_result(op.name, op.args, result, acc_state)
                }
              end)
              |> then(fn {entries, final_state} -> {Enum.reverse(entries), final_state} end)

            {:error, err} ->
              {List.duplicate({:encoded, Encoder.encode(err)}, length(ops)), state}
          end
        else
          dispatch_phase1_write_segment_sequential(ops, state, store, prefetched_reads)
        end
      end

      defp dispatch_phase1_write_segment_sequential(ops, state, store, prefetched_reads) do
        Enum.reduce(ops, {[], state}, fn op, {entries, acc_state} ->
          {entry, new_state} =
            dispatch_pure_single(
              op.idx,
              op.cmd,
              op.name,
              op.args,
              store,
              acc_state,
              prefetched_reads
            )

          {[entry | entries], new_state}
        end)
        |> then(fn {entries, final_state} -> {Enum.reverse(entries), final_state} end)
      end

      defp phase1_write_pressure_ok?(ctx),
        do: :atomics.get(ctx.pressure_flags, 1) == 0 and :atomics.get(ctx.pressure_flags, 2) == 0

      defp has_flow_write?(commands) do
        Enum.any?(commands, fn command ->
          match?({:ok, _op}, flow_write_op(command))
        end)
      end

      defp phase1_write_op(
             {:command, name, args, ast, _keys} = cmd,
             idx,
             %{sandbox_namespace: namespace} = state
           )
           when is_binary(name) and is_list(args) do
        if not phase1_write_batch_enabled?(state) do
          :fallback
        else
          with {:ok, key, command} <- phase1_write_command(ast, namespace) do
            {:ok, %{idx: idx, cmd: cmd, name: name, args: args, key: key, command: command}}
          end
        end
      end

      defp phase1_write_op(_command, _idx, _state), do: :fallback

      defp phase1_write_batch_enabled?(%{transport: transport, instance_ctx: %{name: :default}})
           when transport in [:ranch_tcp, :ranch_ssl],
           do: true

      defp phase1_write_batch_enabled?(_state), do: false

      defp phase1_write_command({:set, key, value}, namespace)
           when is_binary(key) and is_binary(value) and
                  byte_size(key) <= @max_key_size and byte_size(value) < @max_value_size,
           do: phase1_key_command(key, {:put, namespace_key(namespace, key), value, 0}, namespace)

      defp phase1_write_command({:set, _key, _value}, _namespace), do: :fallback

      defp phase1_write_command({:incr, key}, namespace) when is_binary(key),
        do: phase1_key_command(key, {:incr, namespace_key(namespace, key), 1}, namespace)

      defp phase1_write_command({:decr, key}, namespace) when is_binary(key),
        do: phase1_key_command(key, {:incr, namespace_key(namespace, key), -1}, namespace)

      defp phase1_write_command({:incrby, key, delta}, namespace)
           when is_binary(key) and is_integer(delta),
           do: phase1_key_command(key, {:incr, namespace_key(namespace, key), delta}, namespace)

      defp phase1_write_command({:decrby, key, delta}, namespace)
           when is_binary(key) and is_integer(delta),
           do: phase1_key_command(key, {:incr, namespace_key(namespace, key), -delta}, namespace)

      defp phase1_write_command({:incrbyfloat, key, delta}, namespace)
           when is_binary(key) and is_float(delta),
           do:
             phase1_key_command(
               key,
               {:incr_float, namespace_key(namespace, key), delta},
               namespace
             )

      defp phase1_write_command({:append, key, suffix}, namespace) when is_binary(key),
        do: phase1_key_command(key, {:append, namespace_key(namespace, key), suffix}, namespace)

      defp phase1_write_command({:setrange, key, offset, value}, namespace)
           when is_binary(key) and is_integer(offset) and offset >= 0 and
                  offset <= @max_setrange_offset,
           do:
             phase1_key_command(
               key,
               {:setrange, namespace_key(namespace, key), offset, value},
               namespace
             )

      defp phase1_write_command({:setrange, _key, _offset, _value}, _namespace), do: :fallback

      defp phase1_write_command({:setbit, key, offset, bit}, namespace)
           when is_binary(key) and is_integer(offset) and offset >= 0 and
                  offset <= @max_bit_offset and
                  bit in [0, 1],
           do:
             phase1_key_command(
               key,
               {:setbit, namespace_key(namespace, key), offset, bit},
               namespace
             )

      defp phase1_write_command({:setbit, _key, _offset, _bit}, _namespace), do: :fallback

      defp phase1_write_command({:hincrby, key, field, delta}, namespace)
           when is_binary(key) and is_integer(delta),
           do:
             phase1_key_command(
               key,
               {:hincrby, namespace_key(namespace, key), field, delta},
               namespace
             )

      defp phase1_write_command({:hincrbyfloat, key, field, delta}, namespace)
           when is_binary(key) and is_float(delta),
           do:
             phase1_key_command(
               key,
               {:hincrbyfloat, namespace_key(namespace, key), field, delta},
               namespace
             )

      defp phase1_write_command({:zincrby, key, increment, member}, namespace)
           when is_binary(key) and is_float(increment),
           do:
             phase1_key_command(
               key,
               {:zincrby, namespace_key(namespace, key), increment, member},
               namespace
             )

      defp phase1_write_command({:pfadd, [key | elements]}, namespace) when is_binary(key),
        do: phase1_key_command(key, {:pfadd, namespace_key(namespace, key), elements}, namespace)

      defp phase1_write_command({:json_set, key, path, value, flags}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)) and is_binary(value) and
                  is_list(flags),
           do:
             phase1_key_command(
               key,
               {:json_set, namespace_key(namespace, key), path, value, flags},
               namespace
             )

      defp phase1_write_command({:json_set, _key, _path, _value, _flags}, _namespace),
        do: :fallback

      defp phase1_write_command({:json_del, key, path}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)),
           do:
             phase1_key_command(key, {:json_del, namespace_key(namespace, key), path}, namespace)

      defp phase1_write_command({:json_del, _key, _path}, _namespace), do: :fallback

      defp phase1_write_command({:json_numincrby, key, path, increment}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)) and is_number(increment),
           do:
             phase1_key_command(
               key,
               {:json_numincrby, namespace_key(namespace, key), path, increment},
               namespace
             )

      defp phase1_write_command({:json_numincrby, _key, _path, _increment}, _namespace),
        do: :fallback

      defp phase1_write_command({:json_arrappend, key, path, values}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)) and is_list(values),
           do:
             phase1_key_command(
               key,
               {:json_arrappend, namespace_key(namespace, key), path, values},
               namespace
             )

      defp phase1_write_command({:json_arrappend, _key, _path, _values}, _namespace),
        do: :fallback

      defp phase1_write_command({:json_toggle, key, path}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)),
           do:
             phase1_key_command(
               key,
               {:json_toggle, namespace_key(namespace, key), path},
               namespace
             )

      defp phase1_write_command({:json_toggle, _key, _path}, _namespace), do: :fallback

      defp phase1_write_command({:json_clear, key, path}, namespace)
           when is_binary(key) and (is_binary(path) or is_list(path)),
           do:
             phase1_key_command(
               key,
               {:json_clear, namespace_key(namespace, key), path},
               namespace
             )

      defp phase1_write_command({:json_clear, _key, _path}, _namespace), do: :fallback

      defp phase1_write_command(_ast, _namespace), do: :fallback

      defp phase1_key_command(key, command, namespace) do
        {:ok, namespace_key(namespace, key), command}
      end
    end
  end
end
