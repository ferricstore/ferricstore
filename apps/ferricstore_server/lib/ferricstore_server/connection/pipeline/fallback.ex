defmodule FerricstoreServer.Connection.Pipeline.Fallback do
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

      defp log_acl_denied(state, name) do
        FerricstoreServer.Acl.log_command_denied(
          state.username,
          name,
          format_peer(state.peer),
          state.client_id
        )
      end

      # Pure sequential fallback for auth-required or queuing states
      defp sequential_dispatch(commands, state, handle_command_fn, send_response_fn) do
        if state.transport == :ranch_tcp and sequential_cork_safe?(commands) do
          TcpOpts.set_cork(state.socket, true)
          result = do_sequential(commands, state, handle_command_fn, send_response_fn)
          TcpOpts.set_cork(state.socket, false)
          result
        else
          do_sequential(commands, state, handle_command_fn, send_response_fn)
        end
      end

      defp do_sequential([], state, _handle_command_fn, _send_response_fn), do: {:continue, state}

      defp do_sequential([cmd | rest], state, handle_command_fn, send_response_fn) do
        case handle_command_fn.(cmd, state) do
          {:quit, response, quit_state} ->
            _ = send_response_result(quit_state, send_response_fn, response)
            {:quit, quit_state}

          {:continue, response, new_state} ->
            case send_response_result(new_state, send_response_fn, response) do
              :ok -> do_sequential(rest, new_state, handle_command_fn, send_response_fn)
              {:error, _reason} -> {:quit, new_state}
            end
        end
      end

      defp sequential_cork_safe?(commands), do: not pipeline_contains_blocking_command?(commands)

      defp pipeline_contains_blocking_command?(commands) do
        Enum.any?(commands, fn command ->
          name = extract_command_name(command)

          stateful_pipeline_command?(command) or
            name in ~w(BLPOP BRPOP BLMOVE BLMPOP)
        end)
      end

      # ---------------------------------------------------------------------------
      # Private helpers
      # ---------------------------------------------------------------------------

      defp send_response_result(state, send_response_fn, iodata) do
        send_response_fn.(state.socket, state.transport, iodata)
      end

      defp send_or_quit(state, send_response_fn, iodata) do
        send_or_quit(state, send_response_fn, iodata, state)
      end

      defp send_or_quit(send_state, send_response_fn, iodata, continue_state) do
        case send_response_result(send_state, send_response_fn, iodata) do
          :ok -> {:continue, continue_state}
          {:error, _reason} -> {:quit, continue_state}
        end
      end

      defp command_parts({:command, name, args, ast, keys})
           when is_binary(name) and is_list(args) and is_list(keys),
           do: {name, args, ast, keys}

      defp command_parts(_other), do: {"UNKNOWN", [], {:unknown, "UNKNOWN", []}, []}

      defp dispatch_store_command(
             _name,
             _args,
             {:pfadd, [key | elements]},
             _store,
             ctx,
             namespace
           ) do
        Router.pfadd(ctx, namespace_key(namespace, key), elements)
      end

      defp dispatch_store_command(
             _name,
             _args,
             {:pfmerge, [dest_key | source_keys]},
             _store,
             ctx,
             namespace
           ) do
        Router.pfmerge(
          ctx,
          namespace_key(namespace, dest_key),
          namespace_keys(namespace, source_keys)
        )
      end

      defp dispatch_store_command(
             _name,
             _args,
             {:json_set, key, path, value, flags},
             _store,
             ctx,
             namespace
           ) do
        Router.json_set(ctx, namespace_key(namespace, key), path, value, flags)
      end

      defp dispatch_store_command(_name, _args, {:json_del, key, path}, _store, ctx, namespace) do
        Router.json_del(ctx, namespace_key(namespace, key), path)
      end

      defp dispatch_store_command(
             _name,
             _args,
             {:json_numincrby, key, path, increment},
             _store,
             ctx,
             namespace
           ) do
        Router.json_numincrby(ctx, namespace_key(namespace, key), path, increment)
      end

      defp dispatch_store_command(
             _name,
             _args,
             {:json_arrappend, key, path, values},
             _store,
             ctx,
             namespace
           ) do
        Router.json_arrappend(ctx, namespace_key(namespace, key), path, values)
      end

      defp dispatch_store_command(_name, _args, {:json_toggle, key, path}, _store, ctx, namespace) do
        Router.json_toggle(ctx, namespace_key(namespace, key), path)
      end

      defp dispatch_store_command(_name, _args, {:json_clear, key, path}, _store, ctx, namespace) do
        Router.json_clear(ctx, namespace_key(namespace, key), path)
      end

      defp dispatch_store_command(name, args, ast, store, _ctx, _namespace)
           when is_tuple(ast) and tuple_size(ast) in 2..5 do
        case Dispatcher.dispatch_ast(ast, store) do
          {:error, "ERR unsupported command AST"} ->
            {:error,
             "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}

          result ->
            result
        end
      end

      defp dispatch_store_command(_name, _args, ast, store, _ctx, _namespace)
           when ast in ~w(ping)a,
           do: Dispatcher.dispatch_ast(ast, store)

      defp dispatch_store_command(name, args, _ast, _store, _ctx, _namespace) do
        {:error,
         "ERR unsupported command AST for '#{String.downcase(name)}' command with #{length(args)} args"}
      end

      defp namespace_key(nil, key), do: key
      defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key

      defp namespace_keys(nil, keys), do: keys

      defp namespace_keys(namespace, keys) when is_binary(namespace),
        do: Enum.map(keys, &(namespace <> &1))

      defp namespace_kv_pairs(nil, kv_pairs), do: kv_pairs

      defp namespace_kv_pairs(namespace, kv_pairs) when is_binary(namespace),
        do: Enum.map(kv_pairs, fn {key, value} -> {namespace <> key, value} end)

      defp safe_dispatch(fun) do
        {:ok, fun.()}
      catch
        :exit, {:noproc, _} ->
          {:error, {:error, "ERR server not ready, shard process unavailable"}}

        :exit, {reason, _} ->
          {:error, internal_error(:exit, reason)}

        kind, reason ->
          {:error, internal_error(kind, reason)}
      end

      defp internal_error(kind, reason) do
        Logger.error(fn ->
          "FerricStore pipeline internal error: #{inspect({kind, reason}, limit: 20)}"
        end)

        {:error, "ERR internal error"}
      end

      defp extract_command_name({:command, name, _args, _ast, _keys}) when is_binary(name),
        do: name

      defp extract_command_name(_), do: "UNKNOWN"

      defp full_acl_fast_path?(:full_access), do: true

      defp full_acl_fast_path?(%{
             commands: :all,
             keys: :all,
             channels: :all,
             enabled: true,
             denied_commands: %MapSet{map: denied}
           })
           when map_size(denied) == 0,
           do: true

      defp full_acl_fast_path?(_cache), do: false

      defp requires_sequential_dispatch?(state) do
        requires_auth?(state) or state.multi_state == :queuing or pubsub_mode?(state)
      end

      defp requires_auth?(state) do
        not state.authenticated and
          (Map.get(state, :require_auth, false) or live_requirepass_enabled?())
      end

      defp pubsub_mode?(%{pubsub_channels: nil, pubsub_patterns: nil}), do: false

      defp pubsub_mode?(state) do
        pubsub_count(Map.get(state, :pubsub_channels)) +
          pubsub_count(Map.get(state, :pubsub_patterns)) >
          0
      end

      defp pubsub_count(nil), do: 0
      defp pubsub_count(%MapSet{} = set), do: MapSet.size(set)

      defp live_requirepass_enabled? do
        Ferricstore.Config.get_value("requirepass") not in [nil, ""]
      end

      defp format_peer(nil), do: "unknown"
      defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
    end
  end
end
