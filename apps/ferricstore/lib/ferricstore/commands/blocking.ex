defmodule Ferricstore.Commands.Blocking do
  @moduledoc """
  Parses and validates blocking list commands: BLPOP, BRPOP, BLMOVE, BLMPOP.

  These commands are *not* dispatched through the normal `Dispatcher` because
  they require connection-level blocking (the connection process enters a
  `receive` block). Instead, `Connection` calls this module to parse args and
  then handles the blocking logic itself.

  ## Blocking semantics

  When the target list is non-empty, the command executes immediately (pop and
  return). When the list is empty, the connection process registers as a waiter
  in the `Ferricstore.Waiters` ETS table and enters a `receive` block with an
  `after` clause for the timeout. When another client pushes to the watched key,
  the waiter is notified via `{:waiter_notify, key}`. The BEAM runtime handles
  the timeout with zero polling.

  ## Supported commands

    * `BLPOP key [key ...] timeout` -- block until left pop
    * `BRPOP key [key ...] timeout` -- block until right pop
    * `BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout` -- blocking LMOVE
    * `BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]` -- blocking LMPOP
  """

  @max_timeout_ms 0xFFFFFFFF

  @doc """
  Parses BLPOP/BRPOP arguments into `{:ok, keys, timeout_ms}` or `{:error, msg}`.

  The last argument is the timeout in seconds (float or integer).
  All preceding arguments are keys.

  ## Parameters

    - `args` -- list of string arguments

  ## Returns

    - `{:ok, keys, timeout_ms}` -- parsed keys and timeout in milliseconds
    - `{:error, reason}` -- parse error
  """
  @spec parse_blpop_args([binary()]) :: {:ok, [binary()], non_neg_integer()} | {:error, binary()}
  def parse_blpop_args(args) when length(args) >= 2 do
    {keys, [timeout_str]} = Enum.split(args, -1)

    case parse_timeout(timeout_str) do
      {:ok, timeout_ms} ->
        if keys == [] do
          {:error, "ERR wrong number of arguments for 'blpop' command"}
        else
          {:ok, keys, timeout_ms}
        end

      {:error, _} = err ->
        err
    end
  end

  def parse_blpop_args(_args) do
    {:error, "ERR wrong number of arguments for 'blpop' command"}
  end

  @doc """
  Parses BLMOVE arguments into `{:ok, source, destination, from_dir, to_dir, timeout_ms}`.

  ## Parameters

    - `args` -- `[source, destination, wherefrom, whereto, timeout]`

  ## Returns

    - `{:ok, source, destination, from_dir, to_dir, timeout_ms}`
    - `{:error, reason}`
  """
  @spec parse_blmove_args([binary()]) ::
          {:ok, binary(), binary(), :left | :right, :left | :right, non_neg_integer()}
          | {:error, binary()}
  def parse_blmove_args([source, destination, wherefrom, whereto, timeout_str]) do
    with {:ok, from_dir} <- parse_direction(wherefrom),
         {:ok, to_dir} <- parse_direction(whereto),
         {:ok, timeout_ms} <- parse_timeout(timeout_str) do
      {:ok, source, destination, from_dir, to_dir, timeout_ms}
    end
  end

  def parse_blmove_args(_args) do
    {:error, "ERR wrong number of arguments for 'blmove' command"}
  end

  @doc """
  Parses BLMPOP arguments.

  Format: `timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]`

  ## Returns

    - `{:ok, keys, direction, count, timeout_ms}`
    - `{:error, reason}`
  """
  @spec parse_blmpop_args([binary()]) ::
          {:ok, [binary()], :left | :right, pos_integer(), non_neg_integer()}
          | {:error, binary()}
  def parse_blmpop_args([timeout_str, numkeys_str | rest]) when rest != [] do
    with {:ok, timeout_ms} <- parse_timeout(timeout_str),
         {numkeys, ""} <- Integer.parse(numkeys_str),
         true <- numkeys > 0 and length(rest) >= numkeys + 1 do
      {keys, dir_and_opts} = Enum.split(rest, numkeys)

      case dir_and_opts do
        [dir_str | count_opts] ->
          with {:ok, direction} <- parse_direction(dir_str),
               {:ok, count} <- parse_blmpop_count(count_opts) do
            {:ok, keys, direction, count, timeout_ms}
          end

        _ ->
          {:error, "ERR syntax error"}
      end
    else
      :error -> {:error, "ERR value is not an integer or out of range"}
      false -> {:error, "ERR syntax error"}
      {:error, _} = err -> err
    end
  end

  def parse_blmpop_args(_args) do
    {:error, "ERR wrong number of arguments for 'blmpop' command"}
  end

  # ===========================================================================
  # Private -- parsing helpers
  # ===========================================================================

  defp parse_timeout(str) do
    case Float.parse(str) do
      {val, ""} when val >= 0 and val <= @max_timeout_ms / 1_000 ->
        {:ok, trunc(val * 1000)}

      {_val, ""} ->
        timeout_range_error(str)

      _ ->
        case Integer.parse(str) do
          {val, ""} when val >= 0 and val <= div(@max_timeout_ms, 1_000) ->
            {:ok, val * 1000}

          {_val, ""} -> {:error, "ERR timeout is negative"}
          _ -> {:error, "ERR timeout is not a float or out of range"}
        end
    end
  rescue
    ArgumentError -> {:error, "ERR timeout is not a float or out of range"}
    ArithmeticError -> {:error, "ERR timeout is not a float or out of range"}
  end

  defp timeout_range_error("-" <> _rest), do: {:error, "ERR timeout is negative"}
  defp timeout_range_error(_str), do: {:error, "ERR timeout is not a float or out of range"}

  defp parse_direction(dir) do
    case String.upcase(dir) do
      "LEFT" -> {:ok, :left}
      "RIGHT" -> {:ok, :right}
      _ -> {:error, "ERR syntax error"}
    end
  end

  defp parse_blmpop_count([]), do: {:ok, 1}

  defp parse_blmpop_count([keyword, count_str]) when is_binary(keyword) do
    if String.upcase(keyword) != "COUNT" do
      {:error, "ERR syntax error"}
    else
      case Integer.parse(count_str) do
        {count, ""} when count > 0 -> {:ok, count}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  defp parse_blmpop_count(_), do: {:error, "ERR syntax error"}

  alias Ferricstore.Commands.List

  # Simple non-blocking handle/3 for commands dispatched from connection.ex.
  # Returns the result immediately without blocking (timeout=0 behavior).
  @spec handle(binary(), [binary()], map()) :: term()
  def handle("BLPOP", args, store), do: do_blocking_pop(args, store, "LPOP")
  def handle("BRPOP", args, store), do: do_blocking_pop(args, store, "RPOP")

  def handle("BLMOVE", args, store) do
    case parse_blmove_args(args) do
      {:ok, source, destination, from_dir, to_dir, _timeout_ms} ->
        List.handle_ast({:lmove, source, destination, from_dir, to_dir}, store)

      {:error, _} = err ->
        err
    end
  end

  def handle("BLMPOP", args, store) do
    case parse_blmpop_args(args) do
      {:ok, keys, direction, count, _timeout_ms} ->
        pop_cmd = if direction == :left, do: "LPOP", else: "RPOP"

        pop_args_fn = fn key ->
          if count == 1, do: [key], else: [key, to_string(count)]
        end

        Enum.reduce_while(keys, nil, fn key, _acc ->
          case List.handle(pop_cmd, pop_args_fn.(key), store) do
            nil ->
              {:cont, nil}

            {:error, _} = err ->
              {:halt, err}

            value ->
              elements = if is_list(value), do: value, else: [value]
              {:halt, [key, elements]}
          end
        end)

      {:error, _} = err ->
        err
    end
  end

  defp do_blocking_pop(args, store, pop_cmd) do
    case parse_blpop_args(args) do
      {:ok, keys, _timeout_ms} ->
        Enum.reduce_while(keys, nil, fn key, _acc ->
          case List.handle(pop_cmd, [key], store) do
            nil -> {:cont, nil}
            {:error, _} = err -> {:halt, err}
            value -> {:halt, [key, value]}
          end
        end)

      {:error, _} = err ->
        err
    end
  end
end
