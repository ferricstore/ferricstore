defmodule FerricstoreServer.Connection.Blocking do
  @moduledoc "Blocking command handlers (BLPOP, BRPOP, BLMOVE, BLMPOP, XREAD BLOCK) with waiter registration and client-disconnect detection."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.List
  alias Ferricstore.Commands.Stream, as: StreamCmd
  alias Ferricstore.Waiters
  alias FerricstoreServer.Connection.Store, as: ConnStore

  @type conn_result :: {:continue, iodata(), map()} | {:block, map()} | {:close, iodata(), map()}

  @spec dispatch_blpop_ast([binary()], non_neg_integer(), map()) :: conn_result()
  @doc false
  def dispatch_blpop_ast(keys, timeout_ms, state) do
    dispatch_blocking_parsed(:blpop, keys, timeout_ms, state)
  end

  @spec dispatch_brpop_ast([binary()], non_neg_integer(), map()) :: conn_result()
  @doc false
  def dispatch_brpop_ast(keys, timeout_ms, state) do
    dispatch_blocking_parsed(:brpop, keys, timeout_ms, state)
  end

  @spec dispatch_blmove_ast(
          binary(),
          binary(),
          :left | :right,
          :left | :right,
          non_neg_integer(),
          map()
        ) :: conn_result()
  @doc false
  def dispatch_blmove_ast(source, destination, from_dir, to_dir, timeout_ms, state) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

    # Try immediate LMOVE
    case List.handle(
           "LMOVE",
           [source, destination, to_string(from_dir), to_string(to_dir)],
           store
         ) do
      nil ->
        # Source is empty -- block if timeout allows
        if timeout_ms == 0 do
          do_blmove_wait([source], 300_000, source, destination, from_dir, to_dir, store, state)
        else
          do_blmove_wait(
            [source],
            timeout_ms,
            source,
            destination,
            from_dir,
            to_dir,
            store,
            state
          )
        end

      {:error, _} = err ->
        {:continue, Encoder.encode(err), state}

      value ->
        {:continue, Encoder.encode(value), state}
    end
  end

  @spec dispatch_blmpop_ast([binary()], :left | :right, pos_integer(), non_neg_integer(), map()) ::
          conn_result()
  @doc false
  def dispatch_blmpop_ast(keys, direction, count, timeout_ms, state) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
    pop_cmd = if direction == :left, do: "LPOP", else: "RPOP"

    # Build the count arg list: omit count arg when count == 1
    # to get a single-element return (not wrapped in a list)
    pop_args_fn = fn key ->
      if count == 1, do: [key], else: [key, to_string(count)]
    end

    # Try immediate pop on each key (first non-empty wins)
    immediate =
      Enum.find_value(keys, fn key ->
        case List.handle(pop_cmd, pop_args_fn.(key), store) do
          nil -> nil
          {:error, _} -> nil
          value -> {key, value}
        end
      end)

    case immediate do
      {key, value} ->
        # Wrap single value into a list for consistent BLMPOP format
        elements = if is_list(value), do: value, else: [value]
        {:continue, Encoder.encode([key, elements]), state}

      nil ->
        if timeout_ms == 0 do
          do_blmpop_wait(keys, 300_000, pop_cmd, pop_args_fn, store, state)
        else
          do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state)
        end
    end
  end

  @spec dispatch_xread_ast(term(), list(), map()) :: conn_result()
  @doc false
  def dispatch_xread_ast(ast, args, state) do
    alias Ferricstore.Commands.Dispatcher
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

    result =
      try do
        Dispatcher.dispatch_ast(ast, store)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          {:error, "ERR internal error: #{inspect(reason)}"}
      end

    handle_xread_result(result, args, store, state)
  end

  defp handle_xread_result(result, args, store, state) do
    case result do
      {:block, timeout_ms, stream_ids, count} ->
        dispatch_xread_block(timeout_ms, stream_ids, count, store, state)

      other ->
        alias FerricstoreServer.Connection.Tracking, as: ConnTracking
        ConnTracking.maybe_notify_keyspace("XREAD", args, other)
        new_state = ConnTracking.maybe_track_read("XREAD", args, other, state)
        ConnTracking.maybe_notify_tracking("XREAD", args, other, state)
        {:continue, Encoder.encode(other), new_state}
    end
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  defp dispatch_blocking_parsed(pop_dir, keys, timeout_ms, state) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
    pop_cmd = if pop_dir == :blpop, do: "LPOP", else: "RPOP"

    # Try immediate pop on each key (first non-empty wins)
    immediate =
      Enum.find_value(keys, fn key ->
        case List.handle(pop_cmd, [key], store) do
          nil -> nil
          {:error, _} -> nil
          value -> [key, value]
        end
      end)

    if immediate do
      {:continue, Encoder.encode(immediate), state}
    else
      if timeout_ms == 0 do
        # timeout=0 means block forever (Redis semantics), but we cap at 5 min
        do_block_wait(keys, 300_000, pop_cmd, store, state)
      else
        do_block_wait(keys, timeout_ms, pop_cmd, store, state)
      end
    end
  end

  defp do_block_wait(keys, timeout_ms, pop_cmd, store, state) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    result = block_wait_loop(state, deadline, timeout_ms, pop_cmd, store)

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp block_wait_loop(state, deadline, timeout_ms, pop_cmd, store) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))

    receive do
      {:waiter_notify, notified_key} ->
        case List.handle(pop_cmd, [notified_key], store) do
          nil -> nil
          {:error, _} -> nil
          value -> {:ok, [notified_key, value]}
        end

      # TCP data arriving during block -- buffer it and keep waiting.
      {:tcp, _socket, _data} ->
        block_wait_loop(state, deadline, timeout_ms, pop_cmd, store)

      {:ssl, _socket, _data} ->
        block_wait_loop(state, deadline, timeout_ms, pop_cmd, store)

      {:tcp_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)
        block_wait_loop(state, deadline, timeout_ms, pop_cmd, store)

      {:ssl_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)
        block_wait_loop(state, deadline, timeout_ms, pop_cmd, store)

      {:tcp_closed, _socket} ->
        :client_closed

      {:tcp_error, _socket, _reason} ->
        :client_closed

      {:ssl_closed, _socket} ->
        :client_closed

      {:ssl_error, _socket, _reason} ->
        :client_closed
    after
      remaining ->
        nil
    end
  end

  defp do_blmove_wait(keys, timeout_ms, source, destination, from_dir, to_dir, store, state) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    notify_fn = fn _notified_key ->
      case List.handle(
             "LMOVE",
             [source, destination, to_string(from_dir), to_string(to_dir)],
             store
           ) do
        nil -> nil
        {:error, _} -> nil
        value -> {:ok, value}
      end
    end

    result = generic_wait_loop(state, deadline, timeout_ms, notify_fn)

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    Enum.each(keys, fn key -> Waiters.register(key, self(), deadline) end)

    notify_fn = fn notified_key ->
      case List.handle(pop_cmd, pop_args_fn.(notified_key), store) do
        nil ->
          nil

        {:error, _} ->
          nil

        value ->
          elements = if is_list(value), do: value, else: [value]
          {:ok, [notified_key, elements]}
      end
    end

    result = generic_wait_loop(state, deadline, timeout_ms, notify_fn)

    Enum.each(keys, fn key -> Waiters.unregister(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp dispatch_xread_block(timeout_ms, stream_ids, count, store, state) do
    keys = Enum.map(stream_ids, fn {key, _id} -> key end)
    effective_timeout = if timeout_ms == 0, do: 300_000, else: timeout_ms
    deadline = System.monotonic_time(:millisecond) + effective_timeout

    # Register as waiter for all watched stream keys.
    Enum.each(stream_ids, fn {key, id_str} ->
      StreamCmd.register_stream_waiter(key, self(), id_str)
    end)

    # Cap timeout=0 (block forever) at 5 minutes.
    effective_timeout = if timeout_ms == 0, do: 300_000, else: timeout_ms

    notify_fn = fn _notified_key ->
      read_result =
        try do
          StreamCmd.handle("XREAD", build_xread_args(stream_ids, count), store)
        catch
          _, _ -> []
        end

      case read_result do
        {:block, _, _, _} -> nil
        other when is_list(other) and other != [] -> {:ok, other}
        _ -> nil
      end
    end

    result =
      generic_wait_loop(state, deadline, effective_timeout, notify_fn,
        waiter_msg: :stream_waiter_notify
      )

    # Cleanup: unregister from all stream keys.
    Enum.each(keys, fn key -> StreamCmd.unregister_stream_waiter(key, self()) end)

    case result do
      :client_closed ->
        cleanup_connection(state)
        state.transport.close(state.socket)
        {:quit, Encoder.encode(nil), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  # Generalized wait loop — keeps the socket in its configured active_mode,
  # handles TCP events inline, and calls notify_fn when the waiter is notified.
  # This avoids the active: :once deadlock bug.
  defp generic_wait_loop(state, deadline, _timeout_ms, notify_fn, opts \\ []) do
    remaining = max(0, deadline - System.monotonic_time(:millisecond))
    waiter_msg = Keyword.get(opts, :waiter_msg, :waiter_notify)

    receive do
      {^waiter_msg, notified_key} ->
        notify_fn.(notified_key)

      {:tcp, _socket, _data} ->
        generic_wait_loop(state, deadline, 0, notify_fn, opts)

      {:ssl, _socket, _data} ->
        generic_wait_loop(state, deadline, 0, notify_fn, opts)

      {:tcp_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)
        generic_wait_loop(state, deadline, 0, notify_fn, opts)

      {:ssl_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)
        generic_wait_loop(state, deadline, 0, notify_fn, opts)

      {:tcp_closed, _socket} ->
        :client_closed

      {:tcp_error, _socket, _reason} ->
        :client_closed

      {:ssl_closed, _socket} ->
        :client_closed

      {:ssl_error, _socket, _reason} ->
        :client_closed
    after
      remaining ->
        nil
    end
  end

  defp build_xread_args(stream_ids, count) do
    keys = Enum.map(stream_ids, fn {key, _id} -> key end)
    ids = Enum.map(stream_ids, fn {_key, id} -> id end)

    count_args = if count == :infinity, do: [], else: ["COUNT", Integer.to_string(count)]
    count_args ++ ["STREAMS"] ++ keys ++ ids
  end

  # Cleanup helper -- delegates to the same logic as the main connection module.
  defp cleanup_connection(state) do
    duration_ms = System.monotonic_time(:millisecond) - state.created_at

    Ferricstore.AuditLog.log(:connection_close, %{
      client_id: state.client_id,
      client_ip: format_peer(state.peer),
      duration_ms: duration_ms
    })

    if state.pubsub_channels do
      Enum.each(state.pubsub_channels, &Ferricstore.PubSub.unsubscribe(&1, self()))
    end

    if state.pubsub_patterns do
      Enum.each(state.pubsub_patterns, &Ferricstore.PubSub.punsubscribe(&1, self()))
    end

    FerricstoreServer.ClientTracking.cleanup(self())
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    Ferricstore.Stats.decr_connections()
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
