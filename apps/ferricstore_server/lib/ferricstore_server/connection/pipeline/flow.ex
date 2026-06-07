defmodule FerricstoreServer.Connection.Pipeline.Flow do
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

      defp try_batch_flow_write_fast_path(commands, state, send_response_fn) do
        if requires_sequential_dispatch?(state) do
          :fallback
        else
          if full_acl_fast_path?(state.acl_cache) do
            case extract_flow_writes(commands) do
              {:ok, writes} ->
                Stats.incr_commands_by(state.stats_counter, length(writes))

                response =
                  case safe_dispatch(fn ->
                         Ferricstore.Flow.pipeline_write_batch_independent(
                           state.instance_ctx,
                           writes
                         )
                       end) do
                    {:ok, results} ->
                      Enum.map(results, &encode_flow_result/1)

                    {:error, err} ->
                      List.duplicate(Encoder.encode(err), length(writes))
                  end

                {:ok, send_or_quit(state, send_response_fn, response)}

              :fallback ->
                :fallback
            end
          else
            :fallback
          end
        end
      end

      defp extract_flow_writes(commands), do: extract_flow_writes(commands, [])

      defp extract_flow_writes([], acc), do: {:ok, Enum.reverse(acc)}

      defp extract_flow_writes([command | rest], acc) do
        case flow_write_op(command) do
          {:ok, op} -> extract_flow_writes(rest, [op | acc])
          :fallback -> :fallback
        end
      end

      defp flow_write_op({:command, "FLOW.CREATE", _args, {:flow_create, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op(
             {:command, "FLOW.TRANSITION", _args,
              {:flow_transition, id, from_state, to_state, opts} = ast, _keys}
           )
           when is_binary(id) and is_binary(from_state) and is_binary(to_state) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op(
             {:command, "FLOW.COMPLETE", _args, {:flow_complete, id, lease_token, opts} = ast,
              _keys}
           )
           when is_binary(id) and is_binary(lease_token) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op(
             {:command, "FLOW.RETRY", _args, {:flow_retry, id, lease_token, opts} = ast, _keys}
           )
           when is_binary(id) and is_binary(lease_token) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op(
             {:command, "FLOW.FAIL", _args, {:flow_fail, id, lease_token, opts} = ast, _keys}
           )
           when is_binary(id) and is_binary(lease_token) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op({:command, "FLOW.CANCEL", _args, {:flow_cancel, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op({:command, "FLOW.REWIND", _args, {:flow_rewind, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_write_op(_command), do: :fallback

      defp flow_claim_due_op(
             {:command, "FLOW.CLAIM_DUE", _args, {:flow_claim_due, type, opts}, _keys}
           )
           when is_binary(type) and is_list(opts) do
        if Keyword.has_key?(opts, :block_ms) do
          :fallback
        else
          {:ok, {:claim_due, type, opts}}
        end
      end

      defp flow_claim_due_op(_command), do: :fallback

      defp flow_read_op({:command, "FLOW.GET", _args, {:flow_get, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op({:command, "FLOW.HISTORY", _args, {:flow_history, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op({:command, "FLOW.LIST", _args, {:flow_list, type, opts} = ast, _keys})
           when is_binary(type) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op(
             {:command, "FLOW.TERMINALS", _args, {:flow_terminals, type, opts} = ast, _keys}
           )
           when is_binary(type) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op(
             {:command, "FLOW.FAILURES", _args, {:flow_failures, type, opts} = ast, _keys}
           )
           when is_binary(type) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op(
             {:command, "FLOW.BY_PARENT", _args, {:flow_by_parent, id, opts} = ast, _keys}
           )
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op({:command, "FLOW.BY_ROOT", _args, {:flow_by_root, id, opts} = ast, _keys})
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op(
             {:command, "FLOW.BY_CORRELATION", _args, {:flow_by_correlation, id, opts} = ast,
              _keys}
           )
           when is_binary(id) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op({:command, "FLOW.INFO", _args, {:flow_info, type, opts} = ast, _keys})
           when is_binary(type) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op({:command, "FLOW.STUCK", _args, {:flow_stuck, type, opts} = ast, _keys})
           when is_binary(type) and is_list(opts),
           do: {:ok, ast}

      defp flow_read_op(_command), do: :fallback

      defp encode_flow_result(result) do
        result
        |> FlowCommand.normalize_result()
        |> Encoder.encode()
      end

      # ---------------------------------------------------------------------------
      # General batch dispatch (fallback for non-GET/SET pipelines)
      # ---------------------------------------------------------------------------
      # Dispatches each command through the Dispatcher with per-command ACL checks,
      # batches all responses, and sends them in one write. Stateful commands
      # (MULTI, AUTH, SUBSCRIBE, etc.) force a flush-and-sequential boundary.
    end
  end
end
