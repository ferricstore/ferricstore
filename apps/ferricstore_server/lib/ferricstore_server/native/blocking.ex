defmodule FerricstoreServer.Native.Blocking do
  @moduledoc false

  alias Ferricstore.Commands.Blocking, as: BlockingCmd
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.List, as: ListCmd
  alias Ferricstore.Commands.Stream, as: StreamCmd
  alias Ferricstore.Waiters
  alias FerricstoreServer.Native.Session

  @blocking_commands MapSet.new(~w(BLPOP BRPOP BLMOVE BLMPOP XREAD XREADGROUP))

  @spec blocking_command?(binary()) :: boolean()
  def blocking_command?(cmd) when is_binary(cmd), do: MapSet.member?(@blocking_commands, cmd)
  def blocking_command?(_cmd), do: false

  @spec start_request(map(), map(), map()) ::
          {:ok, pid()} | {:error, atom(), binary()}
  def start_request(payload, state, meta) do
    with {:ok, cmd, args, ast, keys} <- Session.parse_command(payload),
         true <-
           blocking_command?(cmd) || {:error, :bad_request, "ERR native command is not blocking"},
         :ok <- Session.authorize_command(cmd, args, ast, keys, state) do
      parent = self()
      ctx = state.instance_ctx

      pid =
        spawn(fn ->
          {status, value} = run_blocking(cmd, args, ast, ctx)
          send(parent, {:native_blocking_response, meta, self(), status, value})
        end)

      {:ok, pid}
    else
      {:error, status, reason} -> {:error, status, reason}
      {:error, reason} -> {:error, :bad_request, reason}
      false -> {:error, :bad_request, "ERR native command is not blocking"}
    end
  end

  defp run_blocking("BLPOP", args, _ast, store), do: run_pop(args, store, :left)
  defp run_blocking("BRPOP", args, _ast, store), do: run_pop(args, store, :right)
  defp run_blocking("BLMOVE", args, _ast, store), do: run_blmove(args, store)
  defp run_blocking("BLMPOP", args, _ast, store), do: run_blmpop(args, store)

  defp run_blocking(cmd, _args, ast, store) when cmd in ["XREAD", "XREADGROUP"],
    do: run_stream_read(ast, store)

  defp run_blocking(_cmd, _args, _ast, _store),
    do: {:bad_request, "ERR unsupported blocking command"}

  defp run_pop(args, store, direction) do
    with {:ok, keys, timeout_ms} <- BlockingCmd.parse_blpop_args(args) do
      wait_keys = keys

      try do
        case first_pop(keys, store, direction) do
          nil -> wait_for_list(wait_keys, timeout_ms, fn -> first_pop(keys, store, direction) end)
          other -> native_result(other)
        end
      after
        Waiters.cleanup(self())
      end
    else
      {:error, reason} -> {:bad_request, reason}
    end
  end

  defp run_blmove(args, store) do
    with {:ok, source, destination, from_dir, to_dir, timeout_ms} <-
           BlockingCmd.parse_blmove_args(args) do
      try do
        case ListCmd.handle_ast({:lmove, source, destination, from_dir, to_dir}, store) do
          nil ->
            wait_for_list([source], timeout_ms, fn ->
              ListCmd.handle_ast({:lmove, source, destination, from_dir, to_dir}, store)
            end)

          other ->
            native_result(other)
        end
      after
        Waiters.cleanup(self())
      end
    else
      {:error, reason} -> {:bad_request, reason}
    end
  end

  defp run_blmpop(args, store) do
    with {:ok, keys, direction, count, timeout_ms} <- BlockingCmd.parse_blmpop_args(args) do
      try do
        pop = fn -> first_mpop(keys, store, direction, count) end

        case pop.() do
          nil -> wait_for_list(keys, timeout_ms, pop)
          other -> native_result(other)
        end
      after
        Waiters.cleanup(self())
      end
    else
      {:error, reason} -> {:bad_request, reason}
    end
  end

  defp run_stream_read(ast, store) do
    case Dispatcher.dispatch_ast(ast, store) do
      {:block, timeout_ms, stream_ids, _count} ->
        try do
          wait_for_stream(stream_ids, timeout_ms, fn ->
            case Dispatcher.dispatch_ast(ast, store) do
              {:block, _timeout_ms, _stream_ids, _count} -> nil
              [] -> nil
              result -> result
            end
          end)
        after
          StreamCmd.cleanup_stream_waiters(self())
        end

      other ->
        native_result(other)
    end
  catch
    :exit, {:noproc, _} -> {:error, "ERR server not ready, shard process unavailable"}
  end

  defp first_pop(keys, store, direction) do
    ast_tag = if direction == :left, do: :lpop, else: :rpop

    Enum.reduce_while(keys, nil, fn key, nil ->
      case ListCmd.handle_ast({ast_tag, key}, store) do
        nil -> {:cont, nil}
        {:error, _reason} = error -> {:halt, error}
        value -> {:halt, [key, value]}
      end
    end)
  end

  defp first_mpop(keys, store, direction, count) do
    ast_tag = if direction == :left, do: :lpop, else: :rpop

    Enum.reduce_while(keys, nil, fn key, nil ->
      case ListCmd.handle_ast({ast_tag, key, count}, store) do
        nil -> {:cont, nil}
        [] -> {:cont, nil}
        {:error, _reason} = error -> {:halt, error}
        value when is_list(value) -> {:halt, [key, value]}
        value -> {:halt, [key, [value]]}
      end
    end)
  end

  defp wait_for_list(keys, timeout_ms, pop_fun) do
    deadline = deadline(timeout_ms)
    Enum.each(keys, &Waiters.register(&1, self(), waiter_deadline(deadline)))

    case pop_fun.() do
      nil -> wait_list_loop(deadline, pop_fun)
      other -> native_result(other)
    end
  end

  defp wait_list_loop(deadline, pop_fun) do
    receive do
      {:waiter_notify, _key} ->
        case pop_fun.() do
          nil -> wait_list_loop(deadline, pop_fun)
          other -> native_result(other)
        end
    after
      wait_ms(deadline) ->
        case pop_fun.() do
          nil -> {:ok, nil}
          other -> native_result(other)
        end
    end
  end

  defp wait_for_stream(stream_ids, timeout_ms, read_fun) do
    deadline = deadline(timeout_ms)
    Enum.each(stream_ids, fn {key, id} -> StreamCmd.register_stream_waiter(key, self(), id) end)

    case read_fun.() do
      nil -> wait_stream_loop(deadline, stream_ids, read_fun)
      other -> native_result(other)
    end
  end

  defp wait_stream_loop(deadline, stream_ids, read_fun) do
    receive do
      {:stream_waiter_notify, _key} ->
        case read_fun.() do
          nil ->
            Enum.each(stream_ids, fn {key, id} ->
              StreamCmd.register_stream_waiter(key, self(), id)
            end)

            wait_stream_loop(deadline, stream_ids, read_fun)

          other ->
            native_result(other)
        end
    after
      wait_ms(deadline) ->
        case read_fun.() do
          nil -> {:ok, nil}
          other -> native_result(other)
        end
    end
  end

  defp native_result({:error, reason}), do: {:error, reason}
  defp native_result(:ok), do: {:ok, "OK"}
  defp native_result(value), do: {:ok, value}

  defp deadline(0), do: 0
  defp deadline(timeout_ms), do: System.monotonic_time(:millisecond) + timeout_ms

  defp waiter_deadline(0), do: 0
  defp waiter_deadline(deadline), do: deadline

  defp wait_ms(0), do: :infinity

  defp wait_ms(deadline) do
    max(deadline - System.monotonic_time(:millisecond), 0)
  end
end
