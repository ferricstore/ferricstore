defmodule FerricstoreServer.Native.Session do
  @moduledoc false

  alias Ferricstore.PubSub, as: PS
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator, as: TxCoordinator
  alias FerricstoreServer.Acl.Protection
  alias FerricstoreServer.Connection.Auth, as: ConnAuth

  @max_subscriptions 100_000
  @max_multi_queue_size 100_000

  @session_commands MapSet.new(~w(
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
    MULTI EXEC DISCARD WATCH UNWATCH
  ))

  @transaction_passthrough MapSet.new(~w(MULTI EXEC DISCARD WATCH UNWATCH))
  @blocked_in_transaction MapSet.new(~w(
    BLPOP BRPOP BLMOVE BLMPOP XREAD XREADGROUP
    SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE
  ))

  @spec session_command?(binary()) :: boolean()
  def session_command?(cmd) when is_binary(cmd), do: MapSet.member?(@session_commands, cmd)
  def session_command?(_cmd), do: false

  @spec parse_command(map()) ::
          {:ok, binary(), [binary()], term(), [binary()]} | {:error, binary()}
  def parse_command(payload) when is_map(payload) do
    with {:ok, command} <- require_binary(payload, "command"),
         {:ok, args} <- raw_command_args(payload) do
      Ferricstore.Commands.Dispatcher.parse_raw(command, args)
    end
  end

  @spec execute(map(), map()) :: {:ok | :error | :bad_request | :auth | :noperm, term(), map()}
  def execute(payload, state) do
    with {:ok, cmd, args, ast, keys} <- parse_command(payload),
         :ok <- authorize_command(cmd, args, ast, keys, state) do
      execute_parsed(cmd, args, ast, keys, state)
    else
      {:error, status, reason} -> {status, reason, state}
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  @spec authorize_command(binary(), [binary()], term(), [binary()], map()) ::
          :ok | {:error, atom(), binary()}
  def authorize_command(cmd, args, ast, keys, state) do
    cond do
      Map.get(state, :require_auth) and not Map.get(state, :authenticated) ->
        {:error, :auth, "NOAUTH Authentication required."}

      true ->
        acl_cmd = ConnAuth.acl_command_name(cmd, args, ast)

        case InternalKey.authorize_command(cmd, keys) do
          :ok ->
            with :ok <- ConnAuth.check_command_cached(state.acl_cache, acl_cmd),
                 :ok <- authorize_channels(cmd, args, state),
                 :ok <- ConnAuth.check_keys_cached(state.acl_cache, acl_cmd, keys) do
              :ok
            else
              {:error, reason} ->
                log_acl_denial(state, acl_cmd)
                {:error, :noperm, reason}
            end

          {:error, reason} ->
            {:error, :error, reason}
        end
    end
  end

  @spec clear(map()) :: map()
  def clear(state) do
    cleanup_pubsub(state)

    state
    |> clear_transaction()
    |> Map.merge(%{pubsub_channels: nil, pubsub_patterns: nil})
  end

  @spec cleanup_pubsub(map()) :: :ok
  def cleanup_pubsub(state) do
    if Map.get(state, :pubsub_channels) != nil or Map.get(state, :pubsub_patterns) != nil do
      PS.cleanup(self())
    end

    :ok
  end

  @spec pubsub_payload(:message | :pmessage, binary(), binary() | nil, binary()) :: map()
  def pubsub_payload(:message, channel, _pattern, message) do
    %{"kind" => "message", "channel" => channel, "message" => message}
  end

  def pubsub_payload(:pmessage, channel, pattern, message) do
    %{"kind" => "pmessage", "pattern" => pattern, "channel" => channel, "message" => message}
  end

  defp execute_parsed(cmd, args, ast, keys, %{multi_state: :queuing} = state)
       when not is_nil(cmd) do
    if MapSet.member?(@transaction_passthrough, cmd) do
      execute_session_command(cmd, args, ast, keys, state)
    else
      queue_transaction_command(cmd, args, ast, keys, state)
    end
  end

  defp execute_parsed(cmd, args, ast, keys, state),
    do: execute_session_command(cmd, args, ast, keys, state)

  defp execute_session_command("SUBSCRIBE", [], _ast, _keys, state),
    do: {:bad_request, "ERR wrong number of arguments for 'subscribe' command", state}

  defp execute_session_command("SUBSCRIBE", channels, _ast, _keys, state),
    do: subscribe_channels(channels, state)

  defp execute_session_command("UNSUBSCRIBE", channels, _ast, _keys, state),
    do: unsubscribe_channels(channels, state)

  defp execute_session_command("PSUBSCRIBE", [], _ast, _keys, state),
    do: {:bad_request, "ERR wrong number of arguments for 'psubscribe' command", state}

  defp execute_session_command("PSUBSCRIBE", patterns, _ast, _keys, state),
    do: subscribe_patterns(patterns, state)

  defp execute_session_command("PUNSUBSCRIBE", patterns, _ast, _keys, state),
    do: unsubscribe_patterns(patterns, state)

  defp execute_session_command("MULTI", _args, _ast, _keys, %{multi_state: :queuing} = state),
    do: {:error, "ERR MULTI calls can not be nested", state}

  defp execute_session_command("MULTI", _args, _ast, _keys, state) do
    {:ok, "OK",
     %{state | multi_state: :queuing, multi_queue: [], multi_queue_count: 0, multi_error: false}}
  end

  defp execute_session_command("EXEC", _args, _ast, _keys, %{multi_state: :none} = state),
    do: {:error, "ERR EXEC without MULTI", state}

  defp execute_session_command("EXEC", _args, _ast, _keys, %{multi_error: true} = state),
    do:
      {:error, "EXECABORT Transaction discarded because of previous errors.",
       clear_transaction(state)}

  defp execute_session_command("EXEC", _args, _ast, _keys, state) do
    state = ConnAuth.refresh_acl_session(state)
    ordered = Enum.reverse(state.multi_queue)

    with :ok <- reauthorize_transaction(ordered, state) do
      entries = Enum.map(ordered, fn {cmd, args, ast, _keys} -> {cmd, args, ast} end)

      result =
        TxCoordinator.execute(entries, state.watched_keys, Map.get(state, :sandbox_namespace))

      {:ok, native_tx_result(result), clear_transaction(state)}
    else
      {:error, reason} -> {:noperm, reason, clear_transaction(state)}
    end
  end

  defp execute_session_command("DISCARD", _args, _ast, _keys, %{multi_state: :none} = state),
    do: {:error, "ERR DISCARD without MULTI", state}

  defp execute_session_command("DISCARD", _args, _ast, _keys, state),
    do: {:ok, "OK", clear_transaction(state)}

  defp execute_session_command("WATCH", _args, _ast, _keys, %{multi_state: :queuing} = state),
    do: {:error, "ERR WATCH inside MULTI is not allowed", state}

  defp execute_session_command("WATCH", [], _ast, _keys, state),
    do: {:bad_request, "ERR wrong number of arguments for 'watch' command", state}

  defp execute_session_command("WATCH", keys, _ast, _acl_keys, state) do
    watched =
      Enum.reduce(keys, state.watched_keys, fn key, acc ->
        watched_key = namespace_key(Map.get(state, :sandbox_namespace), key)
        Map.put(acc, watched_key, Router.watch_token(state.instance_ctx, watched_key))
      end)

    {:ok, "OK", %{state | watched_keys: watched}}
  catch
    :exit, {reason, _} ->
      {:error, "ERR server not ready: #{inspect(reason)}", state}
  end

  defp execute_session_command("UNWATCH", _args, _ast, _keys, state),
    do: {:ok, "OK", %{state | watched_keys: %{}}}

  defp execute_session_command(_cmd, _args, _ast, _keys, state),
    do: {:bad_request, "ERR native command is not a session command", state}

  defp queue_transaction_command(cmd, args, ast, keys, state) do
    cond do
      state.multi_queue_count >= @max_multi_queue_size ->
        {:error,
         "ERR MULTI queue overflow (max #{@max_multi_queue_size} commands), transaction discarded",
         clear_transaction(state)}

      MapSet.member?(@blocked_in_transaction, cmd) ->
        {:error, "ERR Command not allowed inside a transaction", %{state | multi_error: true}}

      transaction_ast_error(ast) != nil ->
        {:error, transaction_ast_error(ast), %{state | multi_error: true}}

      true ->
        {:ok, "QUEUED",
         %{
           state
           | multi_queue: [{cmd, args, ast, keys} | state.multi_queue],
             multi_queue_count: state.multi_queue_count + 1
         }}
    end
  end

  defp clear_transaction(state) do
    %{
      state
      | multi_state: :none,
        multi_queue: [],
        multi_queue_count: 0,
        multi_error: false,
        watched_keys: %{}
    }
  end

  defp reauthorize_transaction(queue, state) do
    Enum.reduce_while(queue, :ok, fn {cmd, args, ast, keys}, :ok ->
      acl_cmd = ConnAuth.acl_command_name(cmd, args, ast)

      case {ConnAuth.check_command_cached(state.acl_cache, acl_cmd),
            ConnAuth.check_keys_cached(state.acl_cache, acl_cmd, keys)} do
        {:ok, :ok} -> {:cont, :ok}
        {{:error, reason}, _} -> {:halt, {:error, reason}}
        {_, {:error, reason}} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp subscribe_channels(channels, state) do
    state = ensure_pubsub_sets(state)
    unique = MapSet.new(channels)
    new_channels = MapSet.difference(unique, state.pubsub_channels)

    if subscription_count(state) + MapSet.size(new_channels) > @max_subscriptions do
      {:error, "ERR max subscriptions per connection (#{@max_subscriptions}) reached", state}
    else
      new_channels |> MapSet.to_list() |> PS.subscribe_many(self())

      {acks, state} =
        Enum.map_reduce(channels, state, fn channel, acc ->
          acc = %{acc | pubsub_channels: MapSet.put(acc.pubsub_channels, channel)}
          {["subscribe", channel, subscription_count(acc)], acc}
        end)

      {:ok, acks, state}
    end
  end

  defp unsubscribe_channels([], state) do
    state = ensure_pubsub_sets(state)

    if MapSet.size(state.pubsub_channels) == 0 do
      {:ok, [["unsubscribe", nil, subscription_count(state)]], state}
    else
      unsubscribe_channels(MapSet.to_list(state.pubsub_channels), state)
    end
  end

  defp unsubscribe_channels(channels, state) do
    state = ensure_pubsub_sets(state)
    PS.unsubscribe_many(channels, self())

    {acks, state} =
      Enum.map_reduce(channels, state, fn channel, acc ->
        acc = %{acc | pubsub_channels: MapSet.delete(acc.pubsub_channels, channel)}
        {["unsubscribe", channel, subscription_count(acc)], acc}
      end)

    {:ok, acks, state}
  end

  defp subscribe_patterns(patterns, state) do
    state = ensure_pubsub_sets(state)
    unique = MapSet.new(patterns)
    new_patterns = MapSet.difference(unique, state.pubsub_patterns)

    if subscription_count(state) + MapSet.size(new_patterns) > @max_subscriptions do
      {:error, "ERR max subscriptions per connection (#{@max_subscriptions}) reached", state}
    else
      new_patterns |> MapSet.to_list() |> PS.psubscribe_many(self())

      {acks, state} =
        Enum.map_reduce(patterns, state, fn pattern, acc ->
          acc = %{acc | pubsub_patterns: MapSet.put(acc.pubsub_patterns, pattern)}
          {["psubscribe", pattern, subscription_count(acc)], acc}
        end)

      {:ok, acks, state}
    end
  end

  defp unsubscribe_patterns([], state) do
    state = ensure_pubsub_sets(state)

    if MapSet.size(state.pubsub_patterns) == 0 do
      {:ok, [["punsubscribe", nil, subscription_count(state)]], state}
    else
      unsubscribe_patterns(MapSet.to_list(state.pubsub_patterns), state)
    end
  end

  defp unsubscribe_patterns(patterns, state) do
    state = ensure_pubsub_sets(state)
    PS.punsubscribe_many(patterns, self())

    {acks, state} =
      Enum.map_reduce(patterns, state, fn pattern, acc ->
        acc = %{acc | pubsub_patterns: MapSet.delete(acc.pubsub_patterns, pattern)}
        {["punsubscribe", pattern, subscription_count(acc)], acc}
      end)

    {:ok, acks, state}
  end

  defp authorize_channels(cmd, args, state)
       when cmd in ["SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE"],
       do: ConnAuth.check_channels_cached(state.acl_cache, args)

  defp authorize_channels(_cmd, _args, _state), do: :ok

  defp log_acl_denial(state, command) do
    Protection.log_command_denied(
      Map.get(state, :username),
      command,
      format_peer(Map.get(state, :peer)),
      Map.get(state, :client_id)
    )
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
  defp format_peer(peer), do: inspect(peer)

  defp ensure_pubsub_sets(%{pubsub_channels: nil} = state),
    do: %{state | pubsub_channels: MapSet.new(), pubsub_patterns: MapSet.new()}

  defp ensure_pubsub_sets(state), do: state

  defp subscription_count(state),
    do: MapSet.size(state.pubsub_channels) + MapSet.size(state.pubsub_patterns)

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key

  defp native_tx_result({:error, _reason} = error), do: native_value(error)
  defp native_tx_result(nil), do: nil
  defp native_tx_result(results) when is_list(results), do: Enum.map(results, &native_value/1)
  defp native_tx_result(other), do: native_value(other)

  defp native_value({:simple, value}), do: value
  defp native_value({:bulk, value}), do: value
  defp native_value({:integer, value}), do: value
  defp native_value({:array, value}), do: value
  defp native_value({:push, value}), do: value
  defp native_value({:error, reason}), do: %{"error" => reason}
  defp native_value(:ok), do: "OK"
  defp native_value(value), do: value

  defp transaction_ast_error({:unknown, cmd, _args}) when is_binary(cmd),
    do: "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "

  defp transaction_ast_error({:error, reason}), do: reason

  defp transaction_ast_error(tuple) when is_tuple(tuple) do
    tuple |> Tuple.to_list() |> Enum.find_value(&transaction_ast_error/1)
  end

  defp transaction_ast_error(list) when is_list(list),
    do: Enum.find_value(list, &transaction_ast_error/1)

  defp transaction_ast_error(_other), do: nil

  defp require_binary(payload, field) do
    case Map.get(payload, field) do
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR native field #{field} must be a binary"}
    end
  end

  defp raw_command_args(%{"args" => args}) when is_list(args),
    do: {:ok, Enum.map(args, &native_arg/1)}

  defp raw_command_args(%{"args" => _args}),
    do: {:error, "ERR native COMMAND_EXEC args must be a list"}

  defp raw_command_args(_payload), do: {:ok, []}

  defp native_arg(value) when is_binary(value), do: value
  defp native_arg(value) when is_integer(value), do: Integer.to_string(value)
  defp native_arg(value) when is_float(value), do: :erlang.float_to_binary(value, [:compact])
  defp native_arg(value) when is_atom(value), do: value |> Atom.to_string() |> String.upcase()
  defp native_arg(value), do: to_string(value)
end
