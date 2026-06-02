defmodule FerricstoreServer.Connection.Blocking do
  @moduledoc "Blocking command handlers (BLPOP, BRPOP, BLMOVE, BLMPOP, XREAD BLOCK) with waiter registration and client-disconnect detection."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Commands.List
  alias Ferricstore.Commands.Stream, as: StreamCmd
  alias Ferricstore.Store.Ops
  alias Ferricstore.Flow.ClaimWaiters
  alias Ferricstore.Waiters
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  require Logger

  @type conn_result :: {:continue, iodata(), map()} | {:quit, iodata(), map()}
  @default_blocked_buffer_max_bytes 134_217_728
  @block_forever :infinity

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
    case safe_list_handle(
           "LMOVE",
           [source, destination, to_string(from_dir), to_string(to_dir)],
           store
         ) do
      {:ok, nil} ->
        # Source is empty -- block if timeout allows
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

      {:ok, {:error, _} = err} ->
        {:continue, Encoder.encode(err), state}

      {:ok, value} ->
        notify_blmove_success(source, destination, value, state)
        {:continue, Encoder.encode(value), state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}
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
    immediate = immediate_blmpop(keys, pop_cmd, pop_args_fn, store)

    case immediate do
      {:ok, {key, value}} ->
        # Wrap single value into a list for consistent BLMPOP format
        elements = if is_list(value), do: value, else: [value]
        notify_blocking_pop_success(pop_cmd, [key, elements], state)
        {:continue, Encoder.encode([key, elements]), state}

      nil ->
        do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state)

      {:error, err} ->
        {:continue, Encoder.encode(err), state}
    end
  end

  @spec dispatch_xread_ast(term(), list(), map()) :: conn_result()
  @doc false
  def dispatch_xread_ast(ast, args, state) do
    dispatch_stream_read_ast("XREAD", ast, args, state)
  end

  @spec dispatch_stream_read_ast(binary(), term(), list(), map()) :: conn_result()
  @doc false
  def dispatch_stream_read_ast(cmd, ast, args, state) when cmd in ["XREAD", "XREADGROUP"] do
    alias Ferricstore.Commands.Dispatcher
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)

    result =
      try do
        Dispatcher.dispatch_ast(ast, store)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          internal_error(:exit, reason)
      end

    handle_stream_read_result(cmd, result, args, ast, store, state)
  end

  @spec dispatch_flow_claim_due_ast(binary(), keyword(), map()) :: conn_result()
  @doc false
  def dispatch_flow_claim_due_ast(type, opts, state) when is_binary(type) and is_list(opts) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
    timeout_ms = Keyword.get(opts, :block_ms, 0)
    claim_opts = Keyword.delete(opts, :block_ms)

    if Keyword.has_key?(opts, :block_ms) and is_integer(timeout_ms) and timeout_ms >= 0 do
      do_flow_claim_due_wait(type, claim_opts, timeout_ms, store, state)
    else
      case safe_dispatch({:flow_claim_due, type, claim_opts}, store) do
        result when is_list(result) and result != [] ->
          {:continue, Encoder.encode(result), state}

        [] ->
          {:continue, Encoder.encode([]), state}

        {:error, _reason} = error ->
          {:continue, Encoder.encode(error), state}

        other ->
          {:continue, Encoder.encode(other), state}
      end
    end
  end

  defp handle_stream_read_result(cmd, result, args, ast, store, state) do
    case result do
      {:block, timeout_ms, stream_ids, count} ->
        dispatch_stream_read_block(cmd, ast, timeout_ms, stream_ids, count, store, state, args)

      other ->
        alias FerricstoreServer.Connection.Tracking, as: ConnTracking
        ConnTracking.maybe_notify_keyspace(cmd, args, other)
        new_state = ConnTracking.maybe_track_read(cmd, args, other, state)
        ConnTracking.maybe_notify_tracking(cmd, args, other, state)
        {:continue, Encoder.encode(other), new_state}
    end
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  defp dispatch_blocking_parsed(pop_dir, keys, timeout_ms, state) do
    store = ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
    pop_cmd = if pop_dir == :blpop, do: "LPOP", else: "RPOP"
    block_cmd = if pop_dir == :blpop, do: "BLPOP", else: "BRPOP"

    # Try immediate pop on each key (first non-empty wins)
    immediate = immediate_blocking_pop(keys, pop_cmd, store)

    case immediate do
      {:ok, value} ->
        notify_blocking_pop_success(pop_cmd, value, state)
        {:continue, Encoder.encode(value), state}

      nil ->
        do_block_wait(keys, timeout_ms, pop_cmd, block_cmd, store, state)

      {:error, err} ->
        {:continue, Encoder.encode(err), state}
    end
  end

  defp do_block_wait(keys, timeout_ms, pop_cmd, block_cmd, store, state) do
    deadline = block_deadline(timeout_ms)
    waiter_pairs = list_waiter_pairs(state, keys)
    waiter_key_map = list_waiter_key_map(waiter_pairs)
    maybe_run_list_block_before_register_hook(block_cmd, keys, state)
    register_list_waiters(waiter_pairs, deadline)

    {result, state} =
      case blocked_acl_refresh(state, block_cmd, keys) do
        {:ok, state} ->
          case immediate_blocking_pop(keys, pop_cmd, store) do
            {:ok, value} ->
              {{:ok, value}, state}

            {:error, err} ->
              {{:error, err}, state}

            nil ->
              block_wait_loop(
                state,
                deadline,
                timeout_ms,
                pop_cmd,
                block_cmd,
                keys,
                waiter_pairs,
                waiter_key_map,
                store,
                [],
                0,
                blocked_buffer_max_bytes()
              )
          end

        {{:error, _reason} = error, state} ->
          {error, state}
      end

    unregister_list_waiters(waiter_pairs)

    case result do
      :client_closed ->
        {:quit, "", state}

      {:blocked_buffer_overflow, err} ->
        {:quit, Encoder.encode(err), state}

      {:ok, value} ->
        notify_blocking_pop_success(pop_cmd, value, state)
        {:continue, Encoder.encode(value), state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp block_wait_loop(
         state,
         deadline,
         timeout_ms,
         pop_cmd,
         acl_command,
         acl_keys,
         waiter_pairs,
         waiter_key_map,
         store,
         buffered_chunks,
         buffered_bytes,
         buffer_max
       ) do
    rearm_blocked_socket_if_once(state)
    remaining = block_remaining(deadline)

    receive do
      {:waiter_notify, notified_waiter_key} ->
        case Map.fetch(waiter_key_map, notified_waiter_key) do
          {:ok, notified_key} ->
            case blocked_acl_refresh(state, acl_command, acl_keys) do
              {:ok, state} ->
                result =
                  case safe_list_handle(pop_cmd, [notified_key], store) do
                    {:ok, nil} -> nil
                    {:ok, {:error, _} = err} -> {:error, err}
                    {:ok, value} -> {:ok, [notified_key, value]}
                    {:error, err} -> {:error, err}
                  end

                case result do
                  nil ->
                    reregister_list_waiters(waiter_pairs, deadline)

                    block_wait_loop(
                      state,
                      deadline,
                      timeout_ms,
                      pop_cmd,
                      acl_command,
                      acl_keys,
                      waiter_pairs,
                      waiter_key_map,
                      store,
                      buffered_chunks,
                      buffered_bytes,
                      buffer_max
                    )

                  _ ->
                    {result, append_buffered_data(state, buffered_chunks)}
                end

              {{:error, _reason} = error, state} ->
                {error, append_buffered_data(state, buffered_chunks)}
            end

          _ ->
            block_wait_loop(
              state,
              deadline,
              timeout_ms,
              pop_cmd,
              acl_command,
              acl_keys,
              waiter_pairs,
              waiter_key_map,
              store,
              buffered_chunks,
              buffered_bytes,
              buffer_max
            )
        end

      # TCP data arriving during block -- buffer it and keep waiting.
      {:tcp, _socket, data} ->
        case buffer_blocked_chunk(buffered_chunks, buffered_bytes, buffer_max, data) do
          {:ok, chunks, bytes} ->
            block_wait_loop(
              state,
              deadline,
              timeout_ms,
              pop_cmd,
              acl_command,
              acl_keys,
              waiter_pairs,
              waiter_key_map,
              store,
              chunks,
              bytes,
              buffer_max
            )

          {:error, _reason} ->
            {blocked_buffer_overflow_result(buffer_max), state}
        end

      {:ssl, _socket, data} ->
        case buffer_blocked_chunk(buffered_chunks, buffered_bytes, buffer_max, data) do
          {:ok, chunks, bytes} ->
            block_wait_loop(
              state,
              deadline,
              timeout_ms,
              pop_cmd,
              acl_command,
              acl_keys,
              waiter_pairs,
              waiter_key_map,
              store,
              chunks,
              bytes,
              buffer_max
            )

          {:error, _reason} ->
            {blocked_buffer_overflow_result(buffer_max), state}
        end

      {:tcp_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)

        block_wait_loop(
          state,
          deadline,
          timeout_ms,
          pop_cmd,
          acl_command,
          acl_keys,
          waiter_pairs,
          waiter_key_map,
          store,
          buffered_chunks,
          buffered_bytes,
          buffer_max
        )

      {:ssl_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)

        block_wait_loop(
          state,
          deadline,
          timeout_ms,
          pop_cmd,
          acl_command,
          acl_keys,
          waiter_pairs,
          waiter_key_map,
          store,
          buffered_chunks,
          buffered_bytes,
          buffer_max
        )

      {:acl_invalidate, username} ->
        case blocked_acl_invalidation(state, username, acl_command, acl_keys) do
          {:ok, new_state} ->
            block_wait_loop(
              new_state,
              deadline,
              timeout_ms,
              pop_cmd,
              acl_command,
              acl_keys,
              waiter_pairs,
              waiter_key_map,
              store,
              buffered_chunks,
              buffered_bytes,
              buffer_max
            )

          {{:error, _reason} = error, new_state} ->
            {error, append_buffered_data(new_state, buffered_chunks)}
        end

      {:tcp_closed, _socket} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:tcp_error, _socket, _reason} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:ssl_closed, _socket} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:ssl_error, _socket, _reason} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      :client_kill ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}
    after
      remaining ->
        {nil, append_buffered_data(state, buffered_chunks)}
    end
  end

  defp do_blmove_wait(keys, timeout_ms, source, destination, from_dir, to_dir, store, state) do
    deadline = block_deadline(timeout_ms)
    waiter_pairs = list_waiter_pairs(state, keys)
    waiter_keys = list_waiter_keys(waiter_pairs)
    maybe_run_list_block_before_register_hook("BLMOVE", keys, state)
    register_list_waiters(waiter_pairs, deadline)

    notify_fn = fn _notified_key ->
      case safe_list_handle(
             "LMOVE",
             [source, destination, to_string(from_dir), to_string(to_dir)],
             store
           ) do
        {:ok, nil} -> nil
        {:ok, {:error, _} = err} -> {:error, err}
        {:ok, value} -> {:ok, value}
        {:error, err} -> {:error, err}
      end
    end

    {result, state} =
      case blocked_acl_refresh(state, "BLMOVE", [source, destination]) do
        {:ok, state} ->
          case recheck_registered_waiters(keys, notify_fn) do
            nil ->
              generic_wait_loop(state, deadline, timeout_ms, notify_fn,
                acl_command: "BLMOVE",
                acl_keys: [source, destination],
                on_nil: fn -> reregister_list_waiters(waiter_pairs, deadline) end,
                waiter_keys: waiter_keys
              )

            result ->
              {result, state}
          end

        {{:error, _reason} = error, state} ->
          {error, state}
      end

    unregister_list_waiters(waiter_pairs)

    case result do
      :client_closed ->
        {:quit, "", state}

      {:blocked_buffer_overflow, err} ->
        {:quit, Encoder.encode(err), state}

      {:ok, value} ->
        notify_blmove_success(source, destination, value, state)
        {:continue, Encoder.encode(value), state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp do_blmpop_wait(keys, timeout_ms, pop_cmd, pop_args_fn, store, state) do
    deadline = block_deadline(timeout_ms)
    waiter_pairs = list_waiter_pairs(state, keys)
    waiter_key_map = list_waiter_key_map(waiter_pairs)
    waiter_keys = list_waiter_keys(waiter_pairs)
    maybe_run_list_block_before_register_hook("BLMPOP", keys, state)
    register_list_waiters(waiter_pairs, deadline)

    notify_fn = fn notified_key ->
      logical_key = Map.get(waiter_key_map, notified_key, notified_key)

      case safe_list_handle(pop_cmd, pop_args_fn.(logical_key), store) do
        {:ok, nil} ->
          nil

        {:ok, {:error, _} = err} ->
          {:error, err}

        {:ok, value} ->
          elements = if is_list(value), do: value, else: [value]
          {:ok, [logical_key, elements]}

        {:error, err} ->
          {:error, err}
      end
    end

    {result, state} =
      case blocked_acl_refresh(state, "BLMPOP", keys) do
        {:ok, state} ->
          case recheck_registered_waiters(keys, notify_fn) do
            nil ->
              generic_wait_loop(state, deadline, timeout_ms, notify_fn,
                acl_command: "BLMPOP",
                acl_keys: keys,
                on_nil: fn -> reregister_list_waiters(waiter_pairs, deadline) end,
                waiter_keys: waiter_keys
              )

            result ->
              {result, state}
          end

        {{:error, _reason} = error, state} ->
          {error, state}
      end

    unregister_list_waiters(waiter_pairs)

    case result do
      :client_closed ->
        {:quit, "", state}

      {:blocked_buffer_overflow, err} ->
        {:quit, Encoder.encode(err), state}

      {:ok, value} ->
        notify_blocking_pop_success(pop_cmd, value, state)
        {:continue, Encoder.encode(value), state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}

      nil ->
        {:continue, Encoder.encode(nil), state}
    end
  end

  defp dispatch_stream_read_block(cmd, ast, timeout_ms, stream_ids, _count, store, state, args) do
    alias Ferricstore.Commands.Dispatcher

    keys = Enum.map(stream_ids, fn {key, _id} -> key end)
    deadline = block_deadline(timeout_ms)

    notify_fn = fn _notified_key ->
      read_result =
        try do
          Dispatcher.dispatch_ast(ast, store)
        catch
          _, _ -> []
        end

      case read_result do
        {:block, _, _, _} -> nil
        other when is_list(other) and other != [] -> {:ok, other}
        _ -> nil
      end
    end

    maybe_run_stream_block_before_register_hook(cmd, ast, state)

    register_stream_waiters(stream_ids)

    {result, state} =
      case blocked_acl_refresh(state, cmd, keys) do
        {:ok, state} ->
          case notify_fn.(nil) do
            nil ->
              generic_wait_loop(state, deadline, timeout_ms, notify_fn,
                waiter_msg: :stream_waiter_notify,
                waiter_keys: keys,
                on_nil: fn -> reregister_stream_waiters(stream_ids) end,
                acl_command: cmd,
                acl_keys: keys
              )

            immediate_result ->
              {immediate_result, state}
          end

        {{:error, _reason} = error, state} ->
          {error, state}
      end

    # Cleanup: unregister from all stream keys.
    Enum.each(keys, fn key -> StreamCmd.unregister_stream_waiter(key, self()) end)

    case result do
      :client_closed ->
        {:quit, "", state}

      {:blocked_buffer_overflow, err} ->
        {:quit, Encoder.encode(err), state}

      {:ok, value} ->
        new_state = ConnTracking.maybe_track_read(cmd, args, value, state)
        {:continue, Encoder.encode(value), new_state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}

      nil ->
        new_state = ConnTracking.maybe_track_read(cmd, args, nil, state)
        {:continue, Encoder.encode(nil), new_state}
    end
  end

  defp register_stream_waiters(stream_ids) do
    Enum.each(stream_ids, fn {key, id_str} ->
      StreamCmd.register_stream_waiter(key, self(), id_str)
    end)
  end

  defp reregister_stream_waiters(stream_ids) do
    Enum.each(stream_ids, fn {key, id_str} ->
      StreamCmd.unregister_stream_waiter(key, self())
      StreamCmd.register_stream_waiter(key, self(), id_str)
    end)
  end

  defp list_waiter_pairs(%{sandbox_namespace: namespace}, keys) when is_binary(namespace) do
    Enum.map(keys, fn key -> {key, namespace <> key} end)
  end

  defp list_waiter_pairs(_state, keys), do: Enum.map(keys, fn key -> {key, key} end)

  defp list_waiter_key_map(waiter_pairs) do
    Map.new(waiter_pairs, fn {logical_key, waiter_key} -> {waiter_key, logical_key} end)
  end

  defp list_waiter_keys(waiter_pairs),
    do: Enum.map(waiter_pairs, fn {_logical, waiter} -> waiter end)

  defp register_list_waiters(waiter_pairs, deadline) do
    Enum.each(waiter_pairs, fn {_logical_key, waiter_key} ->
      Waiters.register(waiter_key, self(), waiter_deadline(deadline))
    end)
  end

  defp unregister_list_waiters(waiter_pairs) do
    Enum.each(waiter_pairs, fn {_logical_key, waiter_key} ->
      Waiters.unregister(waiter_key, self())
    end)
  end

  defp reregister_list_waiters(waiter_pairs, deadline) do
    if not block_expired?(deadline) do
      Enum.each(waiter_pairs, fn {_logical_key, waiter_key} ->
        Waiters.unregister(waiter_key, self())
        Waiters.register(waiter_key, self(), waiter_deadline(deadline))
      end)
    end
  end

  defp reregister_flow_claim_waiters(keys, deadline, limit) do
    if not block_expired?(deadline) do
      ClaimWaiters.unregister(keys, self())
      ClaimWaiters.register(keys, self(), waiter_deadline(deadline), limit: limit)
    else
      :ok
    end
  end

  defp recheck_registered_waiters(keys, notify_fn) do
    Enum.reduce_while(keys, nil, fn key, nil ->
      case notify_fn.(key) do
        nil -> {:cont, nil}
        result -> {:halt, result}
      end
    end)
  end

  defp do_flow_claim_due_wait(type, claim_opts, timeout_ms, store, state) do
    try do
      with {:ok, keys, limit} <- Ferricstore.Flow.claim_due_wait_registration(type, claim_opts) do
        deadline = block_deadline(timeout_ms)
        acl_keys = flow_claim_due_acl_keys(type, claim_opts)

        case ClaimWaiters.register(keys, self(), waiter_deadline(deadline), limit: limit) do
          :ok ->
            do_registered_flow_claim_due_wait(
              type,
              claim_opts,
              timeout_ms,
              store,
              state,
              keys,
              limit,
              deadline,
              acl_keys
            )

          {:error, _reason} = error ->
            {:continue, Encoder.encode(error), state}
        end
      else
        {:error, _reason} = error ->
          {:continue, Encoder.encode(error), state}
      end
    after
      ClaimWaiters.cleanup(self())
    end
  end

  defp do_registered_flow_claim_due_wait(
         type,
         claim_opts,
         timeout_ms,
         store,
         state,
         keys,
         limit,
         deadline,
         acl_keys
       ) do
    notify_fn = fn _notified_key ->
      case safe_dispatch({:flow_claim_due, type, claim_opts}, store) do
        result when is_list(result) and result != [] -> {:ok, result}
        [] -> nil
        {:error, _reason} = error -> {:error, error}
        _other -> nil
      end
    end

    {result, state} =
      case blocked_acl_refresh(state, "FLOW.CLAIM_DUE", acl_keys) do
        {:ok, state} ->
          case safe_dispatch({:flow_claim_due, type, claim_opts}, store) do
            result when is_list(result) and result != [] ->
              {{:ok, result}, state}

            [] ->
              Ferricstore.Flow.schedule_claim_due_waiter_next_due(
                state.instance_ctx,
                type,
                claim_opts
              )

              generic_wait_loop(state, deadline, timeout_ms, notify_fn,
                waiter_msg: ClaimWaiters.message(),
                on_nil: fn ->
                  with :ok <- reregister_flow_claim_waiters(keys, deadline, limit) do
                    Ferricstore.Flow.schedule_claim_due_waiter_next_due(
                      state.instance_ctx,
                      type,
                      claim_opts
                    )
                  end
                end,
                acl_command: "FLOW.CLAIM_DUE",
                acl_keys: acl_keys
              )

            {:error, _reason} = error ->
              {{:error, error}, state}

            _other ->
              {nil, state}
          end

        {{:error, _reason} = error, state} ->
          {error, state}
      end

    ClaimWaiters.unregister(keys, self())

    case result do
      :client_closed ->
        {:quit, "", state}

      {:blocked_buffer_overflow, err} ->
        {:quit, Encoder.encode(err), state}

      {:ok, value} ->
        {:continue, Encoder.encode(value), state}

      {:error, err} ->
        {:continue, Encoder.encode(err), state}

      nil ->
        {:continue, Encoder.encode([]), state}
    end
  end

  # Generalized wait loop — keeps the socket in its configured active_mode,
  # handles TCP events inline, and calls notify_fn when the waiter is notified.
  # This avoids the active: :once deadlock bug.
  defp generic_wait_loop(state, deadline, _timeout_ms, notify_fn, opts) do
    generic_wait_loop_buffered(
      state,
      deadline,
      notify_fn,
      opts,
      [],
      0,
      blocked_buffer_max_bytes()
    )
  end

  defp generic_wait_loop_buffered(
         state,
         deadline,
         notify_fn,
         opts,
         buffered_chunks,
         buffered_bytes,
         buffer_max
       ) do
    rearm_blocked_socket_if_once(state)
    remaining = block_remaining(deadline)
    waiter_msg = Keyword.get(opts, :waiter_msg, :waiter_notify)
    idle_fn = Keyword.get(opts, :idle_fn)
    on_nil = Keyword.get(opts, :on_nil, fn -> :ok end)
    wait_ms = generic_wait_ms(remaining, opts, idle_fn)

    receive do
      {^waiter_msg, notified_key} ->
        if waiter_key_allowed?(notified_key, Keyword.get(opts, :waiter_keys, :any)) do
          case blocked_acl_refresh(
                 state,
                 Keyword.get(opts, :acl_command),
                 Keyword.get(opts, :acl_keys, [])
               ) do
            {:ok, state} ->
              case notify_fn.(notified_key) do
                nil ->
                  case on_nil.() do
                    {:error, _reason} = error ->
                      {error, append_buffered_data(state, buffered_chunks)}

                    _other ->
                      generic_wait_loop_buffered(
                        state,
                        deadline,
                        notify_fn,
                        opts,
                        buffered_chunks,
                        buffered_bytes,
                        buffer_max
                      )
                  end

                result ->
                  {result, append_buffered_data(state, buffered_chunks)}
              end

            {{:error, _reason} = error, state} ->
              {error, append_buffered_data(state, buffered_chunks)}
          end
        else
          generic_wait_loop_buffered(
            state,
            deadline,
            notify_fn,
            opts,
            buffered_chunks,
            buffered_bytes,
            buffer_max
          )
        end

      {:tcp, _socket, data} ->
        case buffer_blocked_chunk(buffered_chunks, buffered_bytes, buffer_max, data) do
          {:ok, chunks, bytes} ->
            generic_wait_loop_buffered(
              state,
              deadline,
              notify_fn,
              opts,
              chunks,
              bytes,
              buffer_max
            )

          {:error, _reason} ->
            {blocked_buffer_overflow_result(buffer_max), state}
        end

      {:ssl, _socket, data} ->
        case buffer_blocked_chunk(buffered_chunks, buffered_bytes, buffer_max, data) do
          {:ok, chunks, bytes} ->
            generic_wait_loop_buffered(
              state,
              deadline,
              notify_fn,
              opts,
              chunks,
              bytes,
              buffer_max
            )

          {:error, _reason} ->
            {blocked_buffer_overflow_result(buffer_max), state}
        end

      {:tcp_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)

        generic_wait_loop_buffered(
          state,
          deadline,
          notify_fn,
          opts,
          buffered_chunks,
          buffered_bytes,
          buffer_max
        )

      {:ssl_passive, _socket} ->
        state.transport.setopts(state.socket, active: state.active_mode)

        generic_wait_loop_buffered(
          state,
          deadline,
          notify_fn,
          opts,
          buffered_chunks,
          buffered_bytes,
          buffer_max
        )

      {:acl_invalidate, username} ->
        case blocked_acl_invalidation(
               state,
               username,
               Keyword.get(opts, :acl_command),
               Keyword.get(opts, :acl_keys, [])
             ) do
          {:ok, new_state} ->
            generic_wait_loop_buffered(
              new_state,
              deadline,
              notify_fn,
              opts,
              buffered_chunks,
              buffered_bytes,
              buffer_max
            )

          {{:error, _reason} = error, new_state} ->
            {error, append_buffered_data(new_state, buffered_chunks)}
        end

      {:tcp_closed, _socket} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:tcp_error, _socket, _reason} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:ssl_closed, _socket} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      {:ssl_error, _socket, _reason} ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}

      :client_kill ->
        {:client_closed, append_buffered_data(state, buffered_chunks)}
    after
      wait_ms ->
        handle_generic_idle_wait(
          state,
          deadline,
          notify_fn,
          opts,
          buffered_chunks,
          buffered_bytes,
          buffer_max,
          idle_fn
        )
    end
  end

  defp generic_wait_ms(remaining, opts, idle_fn) do
    cond do
      is_function(idle_fn, 0) and remaining == @block_forever ->
        idle_interval_ms(opts)

      is_function(idle_fn, 0) ->
        min(remaining, idle_interval_ms(opts))

      true ->
        remaining
    end
  end

  defp handle_generic_idle_wait(
         state,
         deadline,
         notify_fn,
         opts,
         buffered_chunks,
         buffered_bytes,
         buffer_max,
         idle_fn
       ) do
    cond do
      not is_function(idle_fn, 0) or block_expired?(deadline) ->
        {nil, append_buffered_data(state, buffered_chunks)}

      true ->
        case idle_fn.() do
          nil ->
            generic_wait_loop_buffered(
              state,
              deadline,
              notify_fn,
              opts,
              buffered_chunks,
              buffered_bytes,
              buffer_max
            )

          result ->
            {result, append_buffered_data(state, buffered_chunks)}
        end
    end
  end

  defp idle_interval_ms(opts) do
    case Keyword.get(opts, :idle_interval_ms, 100) do
      value when is_integer(value) and value > 0 -> value
      _other -> 100
    end
  end

  defp waiter_key_allowed?(_notified_key, :any), do: true
  defp waiter_key_allowed?(notified_key, keys) when is_list(keys), do: notified_key in keys
  defp waiter_key_allowed?(_notified_key, _keys), do: true

  @doc false
  def __block_deadline_for_test__(timeout_ms), do: block_deadline(timeout_ms)

  defp block_deadline(0), do: @block_forever

  defp block_deadline(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    System.monotonic_time(:millisecond) + timeout_ms
  end

  defp block_remaining(@block_forever), do: @block_forever

  defp block_remaining(deadline) when is_integer(deadline) do
    max(0, deadline - System.monotonic_time(:millisecond))
  end

  defp block_expired?(@block_forever), do: false
  defp block_expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  defp rearm_blocked_socket_if_once(%{active_mode: :once, transport: transport, socket: socket}) do
    # active: :once becomes passive after every TCP message and does not send a
    # tcp_passive marker. Blocking commands own the receive loop while waiting,
    # so they must re-arm here to see queued input and disconnects promptly.
    _ = transport.setopts(socket, active: :once)
    :ok
  end

  defp rearm_blocked_socket_if_once(_state), do: :ok

  defp waiter_deadline(@block_forever), do: 0
  defp waiter_deadline(deadline), do: deadline

  defp buffer_blocked_chunk(chunks, buffered_bytes, buffer_max, data) do
    bytes = buffered_bytes + byte_size(data)

    if bytes > buffer_max do
      {:error, blocked_buffer_overflow_error(buffer_max)}
    else
      {:ok, [data | chunks], bytes}
    end
  end

  defp blocked_buffer_overflow_error(max_bytes),
    do: {:error, "ERR blocked command buffer overflow (max #{max_bytes} bytes)"}

  defp blocked_buffer_overflow_result(max_bytes),
    do: {:blocked_buffer_overflow, blocked_buffer_overflow_error(max_bytes)}

  defp blocked_buffer_max_bytes do
    case Application.get_env(
           :ferricstore_server,
           :blocked_command_buffer_max_bytes,
           @default_blocked_buffer_max_bytes
         ) do
      value when is_integer(value) and value > 0 -> value
      _other -> @default_blocked_buffer_max_bytes
    end
  end

  defp append_buffered_data(state, []), do: state

  defp append_buffered_data(state, buffered_chunks) do
    buffered = IO.iodata_to_binary(Enum.reverse(buffered_chunks))

    if state.buffer == "" do
      %{state | buffer: buffered}
    else
      %{state | buffer: state.buffer <> buffered}
    end
  end

  defp immediate_blocking_pop(keys, pop_cmd, store) do
    Enum.reduce_while(keys, nil, fn key, nil ->
      case safe_list_handle(pop_cmd, [key], store) do
        {:ok, nil} -> {:cont, nil}
        {:ok, {:error, _} = err} -> {:halt, {:error, err}}
        {:ok, value} -> {:halt, {:ok, [key, value]}}
        {:error, err} -> {:halt, {:error, err}}
      end
    end)
  end

  defp notify_blocking_pop_success(pop_cmd, [key, _value], state) when pop_cmd in ~w(LPOP RPOP) do
    ConnTracking.maybe_notify_keyspace(pop_cmd, [key], :ok)
    ConnTracking.maybe_notify_tracking(pop_cmd, [key], :ok, state)
    Waiters.notify_push(key)
  end

  defp notify_blocking_pop_success(_pop_cmd, _result, _state), do: :ok

  defp notify_blmove_success(_source, _destination, nil, _state), do: :ok
  defp notify_blmove_success(_source, _destination, {:error, _}, _state), do: :ok

  defp notify_blmove_success(source, destination, _value, state) do
    ConnTracking.maybe_notify_keyspace("LMOVE", [source, destination], :ok)
    ConnTracking.maybe_notify_tracking("LMOVE", [source, destination], :ok, state)
  end

  defp immediate_blmpop(keys, pop_cmd, pop_args_fn, store) do
    Enum.reduce_while(keys, nil, fn key, nil ->
      case safe_list_handle(pop_cmd, pop_args_fn.(key), store) do
        {:ok, nil} -> {:cont, nil}
        {:ok, {:error, _} = err} -> {:halt, {:error, err}}
        {:ok, value} -> {:halt, {:ok, {key, value}}}
        {:error, err} -> {:halt, {:error, err}}
      end
    end)
  end

  defp blocked_acl_invalidation(state, username, command, keys) do
    new_state = ConnAuth.maybe_refresh_acl_cache(state, username)

    if username in [:all, state.username] do
      blocked_acl_check(new_state, command, keys)
    else
      {:ok, new_state}
    end
  end

  defp blocked_acl_refresh(state, nil, _keys), do: {:ok, state}
  defp blocked_acl_refresh(%{acl_cache: nil} = state, _command, _keys), do: {:ok, state}

  defp blocked_acl_refresh(state, command, keys) do
    state
    |> Map.put(:acl_cache, ConnAuth.build_acl_cache(state.username))
    |> blocked_acl_check(command, keys)
  end

  defp blocked_acl_check(state, nil, _keys), do: {:ok, state}

  defp blocked_acl_check(state, command, keys) do
    keys = if is_list(keys), do: keys, else: []

    with :ok <- ConnAuth.check_command_cached(state.acl_cache, command),
         :ok <- ConnAuth.check_keys_cached(state.acl_cache, command, keys) do
      {:ok, state}
    else
      {:error, reason} -> {{:error, {:error, reason}}, state}
    end
  end

  defp flow_claim_due_acl_keys(type, opts) do
    cond do
      is_list(Keyword.get(opts, :partition_keys)) ->
        Keyword.fetch!(opts, :partition_keys)

      is_binary(Keyword.get(opts, :partition_key)) ->
        [Keyword.fetch!(opts, :partition_key)]

      true ->
        [type]
    end
  end

  defp safe_list_handle(cmd, args, store) do
    {:ok, dispatch_list_handle(cmd, args, store)}
  catch
    :exit, {:noproc, _} ->
      {:error, {:error, "ERR server not ready, shard process unavailable"}}

    :exit, {reason, _} ->
      {:error, internal_error(:exit, reason)}

    kind, reason ->
      {:error, internal_error(kind, reason)}
  end

  defp dispatch_list_handle(cmd, args, store) when cmd in ~w(LPOP RPOP) do
    case parse_pop_args(cmd, args) do
      {:ok, key, count} ->
        if atomic_list_store?(store) do
          Ops.list_op(store, key, {pop_direction(cmd), count})
        else
          List.handle(cmd, args, store)
        end

      {:error, _} = error ->
        error
    end
  end

  defp dispatch_list_handle(cmd, args, store), do: List.handle(cmd, args, store)

  defp parse_pop_args(_cmd, [key]), do: {:ok, key, 1}

  defp parse_pop_args(_cmd, [key, count_str]) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 -> {:ok, key, count}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_pop_args("LPOP", _args),
    do: {:error, "ERR wrong number of arguments for 'lpop' command"}

  defp parse_pop_args("RPOP", _args),
    do: {:error, "ERR wrong number of arguments for 'rpop' command"}

  defp pop_direction("LPOP"), do: :lpop
  defp pop_direction("RPOP"), do: :rpop

  defp atomic_list_store?(store) when is_map(store),
    do: is_function(Map.get(store, :list_op), 2)

  defp atomic_list_store?(_store), do: false

  defp safe_dispatch(ast, store) do
    Ferricstore.Commands.Dispatcher.dispatch_ast(ast, store)
  catch
    :exit, {:noproc, _} ->
      {:error, "ERR server not ready, shard process unavailable"}

    :exit, {reason, _} ->
      internal_error(:exit, reason)

    kind, reason ->
      internal_error(kind, reason)
  end

  defp internal_error(kind, reason) do
    Logger.error(fn ->
      "FerricStore blocking connection internal error: #{inspect({kind, reason}, limit: 20)}"
    end)

    {:error, "ERR internal error"}
  end

  defp maybe_run_stream_block_before_register_hook(cmd, ast, state) do
    case Process.get(:ferricstore_stream_block_before_register_hook) do
      fun when is_function(fun, 3) -> fun.(cmd, ast, state)
      fun when is_function(fun, 0) -> fun.()
      _other -> :ok
    end
  end

  defp maybe_run_list_block_before_register_hook(cmd, keys, state) do
    case Process.get(:ferricstore_list_block_before_register_hook) do
      fun when is_function(fun, 3) -> fun.(cmd, keys, state)
      fun when is_function(fun, 0) -> fun.()
      _other -> :ok
    end
  end
end
