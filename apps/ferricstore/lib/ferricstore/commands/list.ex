defmodule Ferricstore.Commands.List do
  @moduledoc """
  Handles Redis list commands using compound key storage.
  """

  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.{ListOps, Ops, TypeRegistry}

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("LPUSH", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_or_set(key, :list, store) do
      result = ListOps.execute(key, store, {:lpush, elements})
      if is_integer(result) and result > 0, do: Ops.on_push(store, key)
      result
    end
  end
  def handle("LPUSH", _, _), do: {:error, "ERR wrong number of arguments for 'lpush' command"}

  def handle("RPUSH", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_or_set(key, :list, store) do
      result = ListOps.execute(key, store, {:rpush, elements})
      if is_integer(result) and result > 0, do: Ops.on_push(store, key)
      result
    end
  end
  def handle("RPUSH", _, _), do: {:error, "ERR wrong number of arguments for 'rpush' command"}

  def handle("LPOP", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store), do: ListOps.execute(key, store, {:lpop, 1})
  end
  def handle("LPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          do_pop(key, store, :lpop, count)
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LPOP", _, _), do: {:error, "ERR wrong number of arguments for 'lpop' command"}

  def handle("RPOP", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store), do: ListOps.execute(key, store, {:rpop, 1})
  end
  def handle("RPOP", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(count_str) do
        {count, ""} when count >= 0 ->
          do_pop(key, store, :rpop, count)
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("RPOP", _, _), do: {:error, "ERR wrong number of arguments for 'rpop' command"}

  def handle("LRANGE", [key, start_str, stop_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} -> ListOps.execute(key, store, {:lrange, start, stop})
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LRANGE", _, _), do: {:error, "ERR wrong number of arguments for 'lrange' command"}

  def handle("LLEN", [key], store), do: with(:ok <- TypeRegistry.check_type(key, :list, store), do: ListOps.execute(key, store, :llen))
  def handle("LLEN", _, _), do: {:error, "ERR wrong number of arguments for 'llen' command"}

  def handle("LINDEX", [key, index_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(index_str) do
        {index, ""} -> ListOps.execute(key, store, {:lindex, index})
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LINDEX", _, _), do: {:error, "ERR wrong number of arguments for 'lindex' command"}

  def handle("LSET", [key, index_str, element], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(index_str) do
        {index, ""} -> ListOps.execute(key, store, {:lset, index, element})
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LSET", _, _), do: {:error, "ERR wrong number of arguments for 'lset' command"}

  def handle("LREM", [key, count_str, element], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case Integer.parse(count_str) do
        {count, ""} -> ListOps.execute(key, store, {:lrem, count, element})
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LREM", _, _), do: {:error, "ERR wrong number of arguments for 'lrem' command"}

  def handle("LTRIM", [key, start_str, stop_str], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case {Integer.parse(start_str), Integer.parse(stop_str)} do
        {{start, ""}, {stop, ""}} -> ListOps.execute(key, store, {:ltrim, start, stop})
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end
  def handle("LTRIM", _, _), do: {:error, "ERR wrong number of arguments for 'ltrim' command"}

  def handle("LPOS", [key, element | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case parse_lpos_opts(opts) do
        {:ok, rank, count, maxlen} -> ListOps.execute(key, store, {:lpos, element, rank, count, maxlen})
        {:error, _} = error -> error
      end
    end
  end
  def handle("LPOS", _, _), do: {:error, "ERR wrong number of arguments for 'lpos' command"}

  def handle("LINSERT", [key, direction_str, pivot, element], store) do
    with :ok <- TypeRegistry.check_type(key, :list, store) do
      case parse_direction(direction_str) do
        {:ok, direction} -> ListOps.execute(key, store, {:linsert, direction, pivot, element})
        :error -> {:error, "ERR syntax error"}
      end
    end
  end
  def handle("LINSERT", _, _), do: {:error, "ERR wrong number of arguments for 'linsert' command"}

  def handle("LMOVE", [source, destination, from_str, to_str], store) do
    with {:ok, from_dir} <- parse_lr_direction(from_str), {:ok, to_dir} <- parse_lr_direction(to_str) do
      CrossShardOp.execute(
        [{source, :read_write}, {destination, :write}],
        fn unified_store ->
          checked_lmove(source, destination, unified_store, from_dir, to_dir)
        end,
        store: store,
        intent: %{command: :lmove, keys: %{source: source, dest: destination}}
      )
    else :error -> {:error, "ERR syntax error"} end
  end
  def handle("LMOVE", _, _), do: {:error, "ERR wrong number of arguments for 'lmove' command"}

  def handle("RPOPLPUSH", [source, destination], store), do: handle("LMOVE", [source, destination, "RIGHT", "LEFT"], store)
  def handle("RPOPLPUSH", _, _), do: {:error, "ERR wrong number of arguments for 'rpoplpush' command"}

  def handle("LPUSHX", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_type(key, :list, store), do: ListOps.execute(key, store, {:lpushx, elements})
  end
  def handle("LPUSHX", _, _), do: {:error, "ERR wrong number of arguments for 'lpushx' command"}

  def handle("RPUSHX", [key | elements], store) when elements != [] do
    with :ok <- TypeRegistry.check_type(key, :list, store), do: ListOps.execute(key, store, {:rpushx, elements})
  end
  def handle("RPUSHX", _, _), do: {:error, "ERR wrong number of arguments for 'rpushx' command"}

  defp do_pop(key, store, _direction, 0) do
    if ListOps.read_meta(key, store) == nil, do: nil, else: []
  end

  defp do_pop(key, store, direction, count) do
    ListOps.execute(key, store, {direction, count})
  end

  defp parse_lpos_opts(opts), do: parse_lpos_opts(opts, 1, nil, 0)
  defp parse_lpos_opts([], rank, count, maxlen), do: {:ok, rank, count, maxlen}
  defp parse_lpos_opts(["RANK", val | rest], _, count, maxlen) do
    case Integer.parse(val) do
      {0, ""} -> {:error, "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use NEGATIVE to start from the end of the list"}
      {r, ""} -> parse_lpos_opts(rest, r, count, maxlen)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  defp parse_lpos_opts(["COUNT", val | rest], rank, _, maxlen) do
    case Integer.parse(val) do
      {c, ""} when c >= 0 -> parse_lpos_opts(rest, rank, c, maxlen)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  defp parse_lpos_opts(["MAXLEN", val | rest], rank, count, _) do
    case Integer.parse(val) do
      {m, ""} when m >= 0 -> parse_lpos_opts(rest, rank, count, m)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end
  defp parse_lpos_opts([unknown | _], _, _, _), do: {:error, "ERR syntax error, option '#{unknown}' not recognized"}

  defp parse_direction(str) do
    case String.upcase(str) do
      "BEFORE" -> {:ok, :before}
      "AFTER" -> {:ok, :after}
      _ -> :error
    end
  end
  defp parse_lr_direction(str) do
    case String.upcase(str) do
      "LEFT" -> {:ok, :left}
      "RIGHT" -> {:ok, :right}
      _ -> :error
    end
  end

  defp checked_lmove(source, destination, store, from_dir, to_dir) do
    with :ok <- TypeRegistry.check_type(source, :list, store) do
      case ListOps.read_meta(source, store) do
        nil ->
          nil

        {0, _, _} ->
          nil

        _meta ->
          with :ok <- TypeRegistry.check_or_set(destination, :list, store) do
            ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
          end
      end
    end
  end
end
