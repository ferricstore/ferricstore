defmodule FerricstoreServer.Connection.Blocking.WaitLoop do
  @moduledoc false

  @default_blocked_buffer_max_bytes 134_217_728
  @block_forever :infinity

  def generic_wait_loop(state, deadline, _timeout_ms, notify_fn, opts) do
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

  def generic_wait_loop_buffered(
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
          case acl_refresh(opts, state) do
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
        case acl_invalidation(opts, state, username) do
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

  def generic_wait_ms(remaining, opts, idle_fn) do
    cond do
      is_function(idle_fn, 0) and remaining == @block_forever ->
        idle_interval_ms(opts)

      is_function(idle_fn, 0) ->
        min(remaining, idle_interval_ms(opts))

      true ->
        remaining
    end
  end

  def handle_generic_idle_wait(
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

  def idle_interval_ms(opts) do
    case Keyword.get(opts, :idle_interval_ms, 100) do
      value when is_integer(value) and value > 0 -> value
      _other -> 100
    end
  end

  def waiter_key_allowed?(_notified_key, :any), do: true
  def waiter_key_allowed?(notified_key, keys) when is_list(keys), do: notified_key in keys
  def waiter_key_allowed?(_notified_key, _keys), do: true

  @doc false
  def block_deadline(0), do: @block_forever

  def block_deadline(timeout_ms) when is_integer(timeout_ms) and timeout_ms > 0 do
    System.monotonic_time(:millisecond) + timeout_ms
  end

  def block_remaining(@block_forever), do: @block_forever

  def block_remaining(deadline) when is_integer(deadline) do
    max(0, deadline - System.monotonic_time(:millisecond))
  end

  def block_expired?(@block_forever), do: false
  def block_expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  def rearm_blocked_socket_if_once(%{active_mode: :once, transport: transport, socket: socket}) do
    # active: :once becomes passive after every TCP message and does not send a
    # tcp_passive marker. Blocking commands own the receive loop while waiting,
    # so they must re-arm here to see queued input and disconnects promptly.
    _ = transport.setopts(socket, active: :once)
    :ok
  end

  def rearm_blocked_socket_if_once(_state), do: :ok

  def waiter_deadline(@block_forever), do: 0
  def waiter_deadline(deadline), do: deadline

  def buffer_blocked_chunk(chunks, buffered_bytes, buffer_max, data) do
    bytes = buffered_bytes + byte_size(data)

    if bytes > buffer_max do
      {:error, blocked_buffer_overflow_error(buffer_max)}
    else
      {:ok, [data | chunks], bytes}
    end
  end

  def blocked_buffer_overflow_error(max_bytes),
    do: {:error, "ERR blocked command buffer overflow (max #{max_bytes} bytes)"}

  def blocked_buffer_overflow_result(max_bytes),
    do: {:blocked_buffer_overflow, blocked_buffer_overflow_error(max_bytes)}

  def blocked_buffer_max_bytes do
    case Application.get_env(
           :ferricstore_server,
           :blocked_command_buffer_max_bytes,
           @default_blocked_buffer_max_bytes
         ) do
      value when is_integer(value) and value > 0 -> value
      _other -> @default_blocked_buffer_max_bytes
    end
  end

  def append_buffered_data(state, []), do: state

  def append_buffered_data(state, buffered_chunks) do
    buffered = IO.iodata_to_binary(Enum.reverse(buffered_chunks))

    if state.buffer == "" do
      %{state | buffer: buffered}
    else
      %{state | buffer: state.buffer <> buffered}
    end
  end


  defp acl_refresh(opts, state) do
    Keyword.fetch!(opts, :acl_refresh_fn).(
      state,
      Keyword.get(opts, :acl_command),
      Keyword.get(opts, :acl_keys, [])
    )
  end

  defp acl_invalidation(opts, state, username) do
    Keyword.fetch!(opts, :acl_invalidation_fn).(
      state,
      username,
      Keyword.get(opts, :acl_command),
      Keyword.get(opts, :acl_keys, [])
    )
  end
end
