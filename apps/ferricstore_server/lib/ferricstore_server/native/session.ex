defmodule FerricstoreServer.Native.Session do
  @moduledoc false

  alias Ferricstore.PubSub, as: PS
  alias Ferricstore.Commands.{PreparedCommand, TransactionPolicy}
  alias FerricstoreServer.Native.FQLParser
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Store.Router
  alias Ferricstore.Transaction.Coordinator, as: TxCoordinator
  alias FerricstoreServer.Acl.Protection
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Native.{OutboundBudget, ResourceBudget}

  @max_subscriptions 100_000
  @max_multi_queue_size 100_000
  @default_multi_queue_byte_limit 32 * 1024 * 1024
  @default_watch_key_limit 10_000
  @default_watch_key_byte_limit 16 * 1024 * 1024
  @watched_key_overhead_bytes 64
  @subscription_entry_overhead_bytes 64
  @default_subscription_byte_limit 16 * 1024 * 1024

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

  @spec prepare_command(map()) :: {:ok, PreparedCommand.t()} | {:error, binary()}
  def prepare_command(payload) when is_map(payload) do
    with {:ok, command} <- require_binary(payload, "command"),
         {:ok, args} <- raw_command_args(payload) do
      Ferricstore.Commands.Dispatcher.prepare_raw(command, args, flow_query_parser: FQLParser)
    end
  end

  @spec prepare_authorized(map(), map()) ::
          {:ok, PreparedCommand.t()} | {:error, atom(), binary()} | {:error, binary()}
  def prepare_authorized(payload, state) when is_map(payload) and is_map(state) do
    with :ok <- authorize_before_prepare(payload, state) do
      prepare_command(payload)
    end
  end

  @spec execute(map(), map()) :: {:ok | :error | :bad_request | :auth | :noperm, term(), map()}
  def execute(payload, state) do
    with {:ok, prepared} <- prepare_authorized(payload, state) do
      execute_prepared(prepared, state)
    else
      {:error, status, reason} -> {status, reason, state}
      {:error, reason} -> {:bad_request, reason, state}
    end
  end

  defp authorize_before_prepare(%{"command" => command}, state) when is_binary(command) do
    case ConnAuth.acl_command_preflight_alternatives(command) do
      [] -> :ok
      commands -> authorize_any_command(commands, state)
    end
  end

  defp authorize_before_prepare(_payload, _state), do: :ok

  defp authorize_any_command(commands, state) do
    cond do
      Map.get(state, :require_auth) and not Map.get(state, :authenticated) ->
        {:error, :auth, "NOAUTH Authentication required."}

      true ->
        case ConnAuth.check_any_command_cached(state.acl_cache, commands) do
          :ok ->
            :ok

          {:error, command, reason} ->
            log_acl_denial(state, command)
            {:error, :noperm, reason}
        end
    end
  end

  @spec execute_prepared(PreparedCommand.t(), map()) ::
          {:ok | :error | :bad_request | :auth | :noperm, term(), map()}
  def execute_prepared(%PreparedCommand{} = prepared, state) do
    case authorize_command(prepared, state) do
      :ok -> execute_prepared_command(prepared, state)
      {:error, status, reason} -> {status, reason, state}
    end
  end

  @spec authorize_command(PreparedCommand.t(), map()) :: :ok | {:error, atom(), binary()}
  def authorize_command(%PreparedCommand{} = prepared, state) do
    cond do
      Map.get(state, :require_auth) and not Map.get(state, :authenticated) ->
        {:error, :auth, "NOAUTH Authentication required."}

      true ->
        acl_commands =
          ConnAuth.acl_command_names(prepared.command, prepared.args, prepared.ast)

        case InternalKey.authorize_command(prepared.command, prepared.acl_keys) do
          :ok ->
            with :ok <- ConnAuth.check_commands_cached(state.acl_cache, acl_commands),
                 :ok <-
                   ConnAuth.check_prepared_resources_cached(state.acl_cache, prepared) do
              :ok
            else
              {:error, acl_command, reason} ->
                log_acl_denial(state, acl_command)
                {:error, :noperm, reason}

              {:error, reason} ->
                audit_command =
                  ConnAuth.acl_command_name(prepared.command, prepared.args, prepared.ast)

                log_acl_denial(state, audit_command)
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
    |> Map.merge(%{
      pubsub_channels: nil,
      pubsub_patterns: nil,
      pubsub_subscription_bytes: 0,
      pubsub_subscription_token: nil
    })
  end

  @spec cleanup_pubsub(map()) :: :ok
  def cleanup_pubsub(state) do
    if Map.get(state, :pubsub_channels) != nil or Map.get(state, :pubsub_patterns) != nil do
      PS.cleanup(self())
    end

    case Map.get(state, :pubsub_subscription_token) do
      token when is_reference(token) ->
        ResourceBudget.release(Map.get(state, :resource_budget, ResourceBudget), token)

      _none ->
        :ok
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

  defp execute_prepared_command(%PreparedCommand{} = prepared, %{multi_state: :queuing} = state) do
    if MapSet.member?(@transaction_passthrough, prepared.command) do
      execute_session_command(
        prepared.command,
        prepared.args,
        prepared.ast,
        prepared.acl_keys,
        state
      )
    else
      queue_transaction_command(prepared, state)
    end
  end

  defp execute_prepared_command(%PreparedCommand{} = prepared, state),
    do:
      execute_session_command(
        prepared.command,
        prepared.args,
        prepared.ast,
        prepared.acl_keys,
        state
      )

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
     %{
       state
       | multi_state: :queuing,
         multi_queue: [],
         multi_queue_count: 0,
         multi_queue_bytes: 0,
         multi_error: false
     }}
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
      entries = Enum.map(ordered, &transaction_entry/1)

      case TxCoordinator.execute(
             entries,
             state.watched_keys,
             Map.get(state, :sandbox_namespace)
           ) do
        {:error, reason} -> {:error, reason, clear_transaction(state)}
        result -> {:ok, native_tx_result(result), clear_transaction(state)}
      end
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
    watched_keys =
      keys
      |> Enum.map(&namespace_key(Map.get(state, :sandbox_namespace), &1))
      |> Enum.uniq()
      |> Enum.map(&detach_retained_binary/1)

    new_keys = Enum.reject(watched_keys, &Map.has_key?(state.watched_keys, &1))
    resulting_key_count = map_size(state.watched_keys) + length(new_keys)

    added_bytes =
      Enum.reduce(new_keys, 0, fn key, total ->
        total + byte_size(key) + @watched_key_overhead_bytes
      end)

    resulting_key_bytes = Map.get(state, :watched_key_bytes, 0) + added_bytes
    key_limit = Map.get(state, :watch_key_limit, @default_watch_key_limit)
    byte_limit = Map.get(state, :watch_key_byte_limit, @default_watch_key_byte_limit)

    cond do
      resulting_key_count > key_limit ->
        {:error, "ERR WATCH key limit exceeded (max #{key_limit})", state}

      resulting_key_bytes > byte_limit ->
        {:error, "ERR WATCH byte limit exceeded (max #{byte_limit} bytes)", state}

      true ->
        previous_bytes = retained_session_bytes(state)

        case reserve_retained_session_bytes(
               state,
               Map.get(state, :multi_queue_bytes, 0) + resulting_key_bytes
             ) do
          {:ok, reserved_state} ->
            case safe_watch_tokens(state.instance_ctx, watched_keys) do
              %{} = tokens ->
                {:ok, "OK",
                 %{
                   reserved_state
                   | watched_keys: Map.merge(state.watched_keys, tokens),
                     watched_key_bytes: resulting_key_bytes
                 }}

              {:error, reason} ->
                restore_retained_session_bytes(reserved_state, previous_bytes)
                {:error, "ERR WATCH unavailable: #{inspect(reason)}", state}

              {:server_not_ready, reason} ->
                restore_retained_session_bytes(reserved_state, previous_bytes)
                {:error, "ERR server not ready: #{inspect(reason)}", state}
            end

          {:error, {:limit, :session_bytes}} ->
            {:error, "ERR native global retained session byte limit reached", state}

          {:error, _reason} ->
            {:error, "ERR native retained session budget unavailable", state}
        end
    end
  end

  defp execute_session_command("UNWATCH", _args, _ast, _keys, state) do
    state = %{state | watched_keys: %{}, watched_key_bytes: 0}
    {:ok, "OK", shrink_retained_session_bytes(state, Map.get(state, :multi_queue_bytes, 0))}
  end

  defp execute_session_command(_cmd, _args, _ast, _keys, state),
    do: {:bad_request, "ERR native command is not a session command", state}

  defp queue_transaction_command(%PreparedCommand{} = prepared, state) do
    cond do
      state.multi_queue_count >= @max_multi_queue_size ->
        {:error,
         "ERR MULTI queue overflow (max #{@max_multi_queue_size} commands), transaction discarded",
         clear_transaction(state)}

      MapSet.member?(@blocked_in_transaction, prepared.command) ->
        {:error, "ERR Command not allowed inside a transaction", %{state | multi_error: true}}

      prepared.routing_scope == :coordinated ->
        {:error, "ERR coordinated command is not supported inside a transaction",
         %{state | multi_error: true}}

      not PreparedCommand.transaction_safe?(prepared) ->
        {:error, reason} = TransactionPolicy.error(prepared.command)
        {:error, reason, %{state | multi_error: true}}

      transaction_ast_error(prepared.ast) != nil ->
        {:error, transaction_ast_error(prepared.ast), %{state | multi_error: true}}

      true ->
        queue_transaction_command_within_byte_limit(prepared, state)
    end
  end

  defp queue_transaction_command_within_byte_limit(prepared, state) do
    prepared = PreparedCommand.detach_retained_binaries(prepared)
    command_bytes = :erlang.external_size(prepared)
    queued_bytes = Map.get(state, :multi_queue_bytes, 0)
    byte_limit = Map.get(state, :multi_queue_byte_limit, @default_multi_queue_byte_limit)

    if queued_bytes + command_bytes > byte_limit do
      {:error,
       "ERR MULTI queue byte limit exceeded (max #{byte_limit} bytes), transaction discarded",
       clear_transaction(state)}
    else
      resulting_queue_bytes = queued_bytes + command_bytes
      resulting_retained_bytes = Map.get(state, :watched_key_bytes, 0) + resulting_queue_bytes

      case reserve_retained_session_bytes(state, resulting_retained_bytes) do
        {:ok, state} ->
          {:ok, "QUEUED",
           %{
             state
             | multi_queue: [prepared | state.multi_queue],
               multi_queue_count: state.multi_queue_count + 1,
               multi_queue_bytes: resulting_queue_bytes
           }}

        {:error, {:limit, :session_bytes}} ->
          {:error, "ERR native global retained session byte limit reached",
           %{state | multi_error: true}}

        {:error, _reason} ->
          {:error, "ERR native retained session budget unavailable", %{state | multi_error: true}}
      end
    end
  end

  defp clear_transaction(state) do
    state = release_retained_session_bytes(state)

    %{
      state
      | multi_state: :none,
        multi_queue: [],
        multi_queue_count: 0,
        multi_queue_bytes: 0,
        multi_error: false,
        watched_keys: %{},
        watched_key_bytes: 0
    }
  end

  defp safe_watch_tokens(instance_ctx, watched_keys) do
    Router.watch_tokens(instance_ctx, watched_keys)
  catch
    :exit, {reason, _} -> {:server_not_ready, reason}
  end

  defp retained_session_bytes(state),
    do: Map.get(state, :multi_queue_bytes, 0) + Map.get(state, :watched_key_bytes, 0)

  defp reserve_retained_session_bytes(state, target_bytes) do
    budget = Map.get(state, :resource_budget, ResourceBudget)

    case {Map.get(state, :session_byte_token), target_bytes} do
      {token, 0} when is_reference(token) ->
        ResourceBudget.release(budget, token)
        {:ok, Map.put(state, :session_byte_token, nil)}

      {token, _positive} when is_reference(token) ->
        case ResourceBudget.resize(budget, token, target_bytes) do
          :ok -> {:ok, state}
          {:error, _reason} = error -> error
        end

      {_none, 0} ->
        {:ok, Map.put(state, :session_byte_token, nil)}

      {_none, _positive} ->
        case ResourceBudget.acquire(budget, :session_bytes, self(), target_bytes) do
          {:ok, token} -> {:ok, Map.put(state, :session_byte_token, token)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp shrink_retained_session_bytes(state, target_bytes) do
    case reserve_retained_session_bytes(state, target_bytes) do
      {:ok, state} -> state
      {:error, _reason} -> Map.put(state, :session_byte_token, nil)
    end
  end

  defp restore_retained_session_bytes(state, previous_bytes) do
    _state = shrink_retained_session_bytes(state, previous_bytes)
    :ok
  end

  defp release_retained_session_bytes(state) do
    case Map.get(state, :session_byte_token) do
      token when is_reference(token) ->
        ResourceBudget.release(Map.get(state, :resource_budget, ResourceBudget), token)
        Map.put(state, :session_byte_token, nil)

      _none ->
        state
    end
  end

  defp reauthorize_transaction(queue, state) do
    Enum.reduce_while(queue, :ok, fn entry, :ok ->
      prepared = transaction_prepared(entry)

      acl_commands =
        ConnAuth.acl_command_names(prepared.command, prepared.args, prepared.ast)

      case {ConnAuth.check_commands_cached(state.acl_cache, acl_commands),
            ConnAuth.check_keys_cached(state.acl_cache, prepared)} do
        {:ok, :ok} -> {:cont, :ok}
        {{:error, _command, reason}, _} -> {:halt, {:error, reason}}
        {_, {:error, reason}} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp transaction_entry(%PreparedCommand{} = prepared), do: prepared

  defp transaction_prepared(%PreparedCommand{} = prepared), do: prepared

  defp subscribe_channels(channels, state) do
    subscribe_values(
      channels,
      state,
      :pubsub_channels,
      &PS.subscribe_many/2,
      "subscribe"
    )
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
    unsubscribe_values(
      channels,
      state,
      :pubsub_channels,
      &PS.unsubscribe_many/2,
      "unsubscribe"
    )
  end

  defp subscribe_patterns(patterns, state) do
    subscribe_values(
      patterns,
      state,
      :pubsub_patterns,
      &PS.psubscribe_many/2,
      "psubscribe"
    )
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
    unsubscribe_values(
      patterns,
      state,
      :pubsub_patterns,
      &PS.punsubscribe_many/2,
      "punsubscribe"
    )
  end

  defp subscribe_values(values, state, set_key, subscribe_fun, ack_kind) do
    state = ensure_pubsub_sets(state)
    current = Map.fetch!(state, set_key)
    new_values = values |> MapSet.new() |> MapSet.difference(current)

    cond do
      subscription_count(state) + MapSet.size(new_values) > @max_subscriptions ->
        {:error, "ERR max subscriptions per connection (#{@max_subscriptions}) reached", state}

      true ->
        added_bytes = subscription_set_bytes(new_values)

        case reserve_subscription_bytes(state, added_bytes) do
          {:ok, reserved_state} ->
            detached = Map.new(new_values, &{&1, :binary.copy(&1)})
            detached_values = Map.values(detached)

            guarded_subscribe_fun = fn values, pid ->
              case install_pubsub_delivery_guard(reserved_state, pid) do
                :ok -> subscribe_fun.(values, pid)
                {:error, _reason} = error -> error
              end
            end

            case safe_pubsub_update(guarded_subscribe_fun, detached_values) do
              :ok ->
                {acks, updated_state} =
                  Enum.map_reduce(values, reserved_state, fn value, acc ->
                    retained = Map.fetch!(acc, set_key)

                    acc =
                      case {MapSet.member?(retained, value), Map.fetch(detached, value)} do
                        {false, {:ok, detached_value}} ->
                          Map.put(acc, set_key, MapSet.put(retained, detached_value))

                        _existing_or_duplicate ->
                          acc
                      end

                    {[ack_kind, value, subscription_count(acc)], acc}
                  end)

                {:ok, acks, updated_state}

              {:error, reason} ->
                rollback_subscription_bytes(reserved_state, state)
                {:error, "ERR pubsub subscription failed: #{inspect(reason)}", state}
            end

          {:error, reason} ->
            {:error, reason, state}
        end
    end
  end

  defp unsubscribe_values(values, state, set_key, unsubscribe_fun, ack_kind) do
    state = ensure_pubsub_sets(state)
    unsubscribe_fun.(values, self())

    {acks, state} =
      Enum.map_reduce(values, state, fn value, acc ->
        retained = Map.fetch!(acc, set_key)

        acc =
          if MapSet.member?(retained, value) do
            acc
            |> Map.put(set_key, MapSet.delete(retained, value))
            |> Map.update!(:pubsub_subscription_bytes, fn bytes ->
              max(bytes - subscription_entry_bytes(value), 0)
            end)
          else
            acc
          end

        {[ack_kind, value, subscription_count(acc)], acc}
      end)

    {:ok, acks, sync_subscription_lease(state)}
  end

  defp reserve_subscription_bytes(state, 0), do: {:ok, state}

  defp reserve_subscription_bytes(state, added_bytes) do
    current_bytes = Map.get(state, :pubsub_subscription_bytes, 0)
    total_bytes = current_bytes + added_bytes

    max_bytes =
      Map.get(state, :max_pubsub_subscription_bytes, @default_subscription_byte_limit)

    if total_bytes > max_bytes do
      {:error, "ERR native subscription byte limit (#{max_bytes}) reached"}
    else
      budget = Map.get(state, :resource_budget, ResourceBudget)

      case Map.get(state, :pubsub_subscription_token) do
        token when is_reference(token) ->
          case ResourceBudget.resize(budget, token, total_bytes) do
            :ok -> {:ok, %{state | pubsub_subscription_bytes: total_bytes}}
            {:error, _reason} -> {:error, "ERR native global subscription byte limit reached"}
          end

        _none ->
          case ResourceBudget.acquire(budget, :subscription_bytes, self(), total_bytes) do
            {:ok, token} ->
              {:ok,
               state
               |> Map.put(:pubsub_subscription_bytes, total_bytes)
               |> Map.put(:pubsub_subscription_token, token)}

            {:error, _reason} ->
              {:error, "ERR native global subscription byte limit reached"}
          end
      end
    end
  end

  defp sync_subscription_lease(state) do
    bytes = Map.get(state, :pubsub_subscription_bytes, 0)
    token = Map.get(state, :pubsub_subscription_token)
    budget = Map.get(state, :resource_budget, ResourceBudget)

    cond do
      not is_reference(token) ->
        state

      bytes == 0 ->
        ResourceBudget.release(budget, token)
        Map.put(state, :pubsub_subscription_token, nil)

      true ->
        _result = ResourceBudget.resize(budget, token, bytes)
        state
    end
  end

  defp rollback_subscription_bytes(reserved_state, previous_state) do
    budget = Map.get(reserved_state, :resource_budget, ResourceBudget)
    previous_bytes = Map.get(previous_state, :pubsub_subscription_bytes, 0)

    case {Map.get(reserved_state, :pubsub_subscription_token),
          Map.get(previous_state, :pubsub_subscription_token)} do
      {token, previous_token} when is_reference(token) and token == previous_token ->
        _result = ResourceBudget.resize(budget, token, previous_bytes)

      {token, _none} when is_reference(token) ->
        ResourceBudget.release(budget, token)

      _other ->
        :ok
    end
  end

  defp safe_pubsub_update(fun, values) do
    case fun.(values, self()) do
      :ok -> :ok
      other -> {:error, other}
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp install_pubsub_delivery_guard(
         %{
           outbound_counter: counter,
           max_outbound_bytes: max_bytes,
           resource_budget: resource_budget
         },
         pid
       ) do
    budget_state = %{
      outbound_counter: counter,
      max_outbound_bytes: max_bytes,
      resource_budget: resource_budget
    }

    PS.set_delivery_guard(pid, fn bytes ->
      OutboundBudget.reserve_bytes(budget_state, pid, bytes)
    end)
  end

  defp install_pubsub_delivery_guard(_state, _pid), do: :ok

  defp subscription_set_bytes(values),
    do: Enum.reduce(values, 0, &(subscription_entry_bytes(&1) + &2))

  defp subscription_entry_bytes(value),
    do: byte_size(value) + @subscription_entry_overhead_bytes

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

  defp ensure_pubsub_sets(state) do
    state =
      if Map.get(state, :pubsub_channels) == nil do
        state
        |> Map.put(:pubsub_channels, MapSet.new())
        |> Map.put(:pubsub_patterns, MapSet.new())
      else
        state
      end

    state
    |> Map.put_new(:pubsub_subscription_bytes, 0)
    |> Map.put_new(:pubsub_subscription_token, nil)
  end

  defp subscription_count(state),
    do: MapSet.size(state.pubsub_channels) + MapSet.size(state.pubsub_patterns)

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key

  defp detach_retained_binary(binary) do
    if :binary.referenced_byte_size(binary) > byte_size(binary),
      do: :binary.copy(binary),
      else: binary
  end

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

  defp raw_command_args(%{"args" => args}) when is_list(args), do: native_args(args, [])

  defp raw_command_args(%{"args" => _args}),
    do: {:error, "ERR native COMMAND_EXEC args must be a list"}

  defp raw_command_args(_payload), do: {:ok, []}

  defp native_args([], acc), do: {:ok, Enum.reverse(acc)}

  defp native_args([value | rest], acc) do
    case native_arg(value) do
      {:ok, value} -> native_args(rest, [value | acc])
      :error -> {:error, "ERR native field args contains an unsupported value"}
    end
  end

  defp native_arg(value) when is_binary(value), do: {:ok, value}
  defp native_arg(value) when is_integer(value), do: {:ok, Integer.to_string(value)}

  defp native_arg(value) when is_float(value),
    do: {:ok, :erlang.float_to_binary(value, [:compact])}

  defp native_arg(value) when is_atom(value),
    do: {:ok, value |> Atom.to_string() |> String.upcase()}

  defp native_arg(_value), do: :error
end
