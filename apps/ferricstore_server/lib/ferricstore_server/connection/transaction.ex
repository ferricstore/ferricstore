defmodule FerricstoreServer.Connection.Transaction do
  @moduledoc "MULTI/EXEC/DISCARD/WATCH transaction lifecycle for a client connection."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  # Maximum commands queued inside a MULTI transaction (100K).
  @max_multi_queue_size 100_000
  @single_value_ast_tags ~w(get incr decr strlen getdel getex ttl pttl persist expiretime pexpiretime llen lpop rpop hgetall hkeys hvals hlen hrandfield hscan httl hpersist hpttl hexpiretime hgetdel smembers scard srandmember spop zscore zrank zrevrank zcard zpopmin zpopmax zrandmember bitcount bitpos type xlen)a
  @blocked_in_transaction_ast_tags ~w(blpop brpop blmove blmpop subscribe unsubscribe psubscribe punsubscribe)a
  @ast_command_names Map.new(@single_value_ast_tags, fn tag ->
                       {tag, tag |> Atom.to_string() |> String.replace("_", ".")}
                     end)

  @type conn_result :: {:continue, iodata(), map()} | {:block, map()} | {:close, iodata(), map()}

  @spec dispatch_multi(list(), map()) :: conn_result()
  @doc false
  def dispatch_multi(_args, %{multi_state: :queuing} = state) do
    {:continue, Encoder.encode({:error, "ERR MULTI calls can not be nested"}), state}
  end

  def dispatch_multi(_args, state) do
    new_state =
      state
      |> clear_multi_error()
      |> Map.merge(%{multi_state: :queuing, multi_queue: [], multi_queue_count: 0})

    {:continue, Encoder.encode(:ok), new_state}
  end

  @spec dispatch_exec(list(), map()) :: conn_result()
  @doc false
  def dispatch_exec(_args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR EXEC without MULTI"}), state}
  end

  def dispatch_exec(_args, %{multi_error: true} = state) do
    new_state = clear_transaction_state(state)

    {:continue,
     Encoder.encode({:error, "EXECABORT Transaction discarded because of previous errors."}),
     new_state}
  end

  def dispatch_exec(_args, state) do
    # EXEC is the transaction authorization boundary. Refresh synchronously so a
    # queued write cannot race ahead of an ACL change whose invalidation message
    # has not been processed by this connection yet.
    state = ConnAuth.refresh_acl_session(state)
    result = execute_transaction(state)
    state = apply_exec_tracking_effects(result, state)

    new_state = clear_transaction_state(state)

    {:continue, Encoder.encode(result), new_state}
  end

  @spec dispatch_discard(list(), map()) :: conn_result()
  @doc false
  def dispatch_discard(_args, %{multi_state: :none} = state) do
    {:continue, Encoder.encode({:error, "ERR DISCARD without MULTI"}), state}
  end

  def dispatch_discard(_args, state) do
    new_state = clear_transaction_state(state)

    {:continue, Encoder.encode(:ok), new_state}
  end

  @spec dispatch_watch(list(), map()) :: conn_result()
  @doc false
  def dispatch_watch(_args, %{multi_state: :queuing} = state) do
    {:continue, Encoder.encode({:error, "ERR WATCH inside MULTI is not allowed"}), state}
  end

  def dispatch_watch([], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'watch' command"}),
     state}
  end

  def dispatch_watch(keys, state) do
    try do
      new_watched =
        Enum.reduce(keys, state.watched_keys, fn key, acc ->
          watched_key = namespace_key(state.sandbox_namespace, key)
          Map.put(acc, watched_key, Router.watch_token(state.instance_ctx, watched_key))
        end)

      {:continue, Encoder.encode(:ok), %{state | watched_keys: new_watched}}
    catch
      :exit, {reason, _} ->
        {:continue, Encoder.encode({:error, "ERR server not ready: #{inspect(reason)}"}), state}
    end
  end

  @spec dispatch_unwatch(list(), map()) :: conn_result()
  @doc false
  def dispatch_unwatch(_args, state) do
    {:continue, Encoder.encode(:ok), %{state | watched_keys: %{}}}
  end

  @spec dispatch_queue(binary(), list(), term(), [binary()], map()) :: conn_result()
  @doc """
  Handles queuing of commands during MULTI mode. Called when `multi_state: :queuing`
  for commands that are not in the passthrough set (EXEC, DISCARD, MULTI, WATCH, UNWATCH).
  """
  def dispatch_queue(cmd, args, ast, keys, state) do
    if state.multi_queue_count >= @max_multi_queue_size do
      new_state =
        %{
          state
          | multi_state: :none,
            multi_queue: [],
            multi_queue_count: 0,
            watched_keys: %{}
        }
        |> clear_multi_error()

      {:continue,
       Encoder.encode(
         {:error,
          "ERR MULTI queue overflow (max #{@max_multi_queue_size} commands), transaction discarded"}
       ), new_state}
    else
      case validate_command(cmd, ast) do
        :ok ->
          new_queue = [{cmd, args, ast, keys} | state.multi_queue]
          new_count = state.multi_queue_count + 1

          {:continue, Encoder.encode({:simple, "QUEUED"}),
           %{state | multi_queue: new_queue, multi_queue_count: new_count}}

        {:error, _msg} = err ->
          {:continue, Encoder.encode(err), mark_multi_error(state)}
      end
    end
  end

  defp clear_transaction_state(state) do
    state
    |> clear_multi_error()
    |> Map.merge(%{multi_state: :none, multi_queue: [], multi_queue_count: 0, watched_keys: %{}})
  end

  defp mark_multi_error(state), do: Map.put(state, :multi_error, true)
  defp clear_multi_error(state), do: Map.put(state, :multi_error, false)

  # ---------------------------------------------------------------------------
  # Transaction execution
  # ---------------------------------------------------------------------------

  defp execute_transaction(
         %{watched_keys: watched, multi_queue: queue, sandbox_namespace: ns} = state
       ) do
    # Queue is stored in reverse order (prepend during MULTI) for O(1)
    # queuing. Reverse here at EXEC time to restore command ordering.
    ordered_queue = Enum.reverse(queue)

    with :ok <- reauthorize_watched_keys(watched, state),
         :ok <- reauthorize_transaction(ordered_queue, state) do
      ordered_queue
      |> Enum.map(&transaction_entry/1)
      |> Ferricstore.Transaction.Coordinator.execute(watched, ns)
    end
  end

  defp apply_exec_tracking_effects(results, state) when is_list(results) do
    state.multi_queue
    |> Enum.reverse()
    |> Enum.zip(results)
    |> Enum.reduce(state, fn {entry, result}, acc_state ->
      {cmd, args, _ast} = transaction_entry(entry)
      ConnTracking.maybe_notify_keyspace(cmd, args, result)
      new_state = ConnTracking.maybe_track_read(cmd, args, result, acc_state)
      ConnTracking.maybe_notify_tracking(cmd, args, result, new_state)
      new_state
    end)
  end

  defp apply_exec_tracking_effects(_aborted_or_error, state), do: state

  defp reauthorize_watched_keys(watched, _state) when map_size(watched) == 0, do: :ok

  defp reauthorize_watched_keys(watched, state) do
    watched_keys = Map.keys(watched)

    with :ok <- ConnAuth.check_command_cached(state.acl_cache, "WATCH"),
         :ok <- ConnAuth.check_keys_cached(state.acl_cache, "WATCH", watched_keys) do
      :ok
    end
  end

  defp reauthorize_transaction(queue, state) do
    Enum.reduce_while(queue, :ok, fn entry, :ok ->
      {cmd, args, ast} = transaction_entry(entry)
      keys = transaction_acl_keys(entry)
      acl_cmd = ConnAuth.acl_command_name(cmd, args, ast)

      case {ConnAuth.check_command_cached(state.acl_cache, acl_cmd),
            ConnAuth.check_keys_cached(state.acl_cache, acl_cmd, keys)} do
        {:ok, :ok} -> {:cont, :ok}
        {{:error, _} = err, _} -> {:halt, err}
        {_, {:error, _} = err} -> {:halt, err}
      end
    end)
  end

  defp transaction_entry({cmd, args, ast, _keys}), do: {cmd, args, ast}
  defp transaction_entry({cmd, args, ast}), do: {cmd, args, ast}
  defp transaction_entry({cmd, args}), do: {cmd, args, nil}

  defp transaction_acl_keys({_cmd, _args, _ast, keys}) when is_list(keys), do: keys
  defp transaction_acl_keys(_entry), do: []

  # ---------------------------------------------------------------------------
  # Command validation (for queue-time syntax checking)
  # ---------------------------------------------------------------------------

  defp validate_command(cmd, ast) do
    case ast_queue_error(ast) do
      nil -> :ok
      {:error, "ERR unsupported command AST"} -> unknown_command_error(cmd)
      {:error, _} = err -> err
    end
  end

  defp ast_queue_error({:unknown, cmd, _args}) when is_binary(cmd),
    do: unknown_command_error(cmd)

  defp ast_queue_error({tag, {:error, _reason} = err})
       when tag in @blocked_in_transaction_ast_tags,
       do: err

  defp ast_queue_error({tag, _args}) when tag in @blocked_in_transaction_ast_tags,
    do: command_not_allowed_in_transaction()

  defp ast_queue_error({tag, _keys, _timeout_ms}) when tag in ~w(blpop brpop)a,
    do: command_not_allowed_in_transaction()

  defp ast_queue_error({:blmove, _source, _destination, _from_dir, _to_dir, _timeout_ms}),
    do: command_not_allowed_in_transaction()

  defp ast_queue_error({:blmpop, _keys, _direction, _count, _timeout_ms}),
    do: command_not_allowed_in_transaction()

  defp ast_queue_error({:xread, _count, {:block, _timeout_ms}, _stream_ids}),
    do: command_not_allowed_in_transaction()

  defp ast_queue_error(
         {:xreadgroup, _group, _consumer, {_count, {:block, _timeout_ms}, _stream_ids}}
       ),
       do: command_not_allowed_in_transaction()

  defp ast_queue_error({:flow_claim_due, _type, opts}) when is_list(opts) do
    if Keyword.has_key?(opts, :block_ms) do
      command_not_allowed_in_transaction()
    end
  end

  # Rust emits `{known_tag, raw_args}` for known commands whose arity does not
  # match a typed AST. Queue-time validation must stay syntax-only; calling the
  # real command handlers here can mutate state or require store callbacks.
  defp ast_queue_error({tag, args}) when tag in @single_value_ast_tags and is_list(args),
    do: wrong_arity_ast(tag)

  defp ast_queue_error({:error, _} = err), do: err

  defp ast_queue_error(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.find_value(&ast_queue_error/1)
  end

  defp ast_queue_error(list) when is_list(list), do: Enum.find_value(list, &ast_queue_error/1)
  defp ast_queue_error(_), do: nil

  defp wrong_arity_ast(tag) do
    name = Map.get(@ast_command_names, tag, tag |> Atom.to_string() |> String.replace("_", "."))
    {:error, "ERR wrong number of arguments for '#{name}' command"}
  end

  defp unknown_command_error(cmd),
    do: {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}

  defp command_not_allowed_in_transaction,
    do: {:error, "ERR Command not allowed inside a transaction"}

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key
end
