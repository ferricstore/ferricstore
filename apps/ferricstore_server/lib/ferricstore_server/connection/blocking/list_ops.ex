defmodule FerricstoreServer.Connection.Blocking.ListOps do
  @moduledoc false

  alias Ferricstore.Commands.List
  alias Ferricstore.Store.Ops

  require Logger

  def safe_list_handle(cmd, args, store) do
    {:ok, dispatch_list_handle(cmd, args, store)}
  catch
    :exit, {:noproc, _} ->
      {:error, {:error, "ERR server not ready, shard process unavailable"}}

    :exit, {reason, _} ->
      {:error, internal_error(:exit, reason)}

    kind, reason ->
      {:error, internal_error(kind, reason)}
  end

  def dispatch_list_handle(cmd, args, store) when cmd in ~w(LPOP RPOP) do
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

  def dispatch_list_handle(cmd, args, store), do: List.handle(cmd, args, store)

  def parse_pop_args(_cmd, [key]), do: {:ok, key, 1}

  def parse_pop_args(_cmd, [key, count_str]) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 -> {:ok, key, count}
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def parse_pop_args("LPOP", _args),
    do: {:error, "ERR wrong number of arguments for 'lpop' command"}

  def parse_pop_args("RPOP", _args),
    do: {:error, "ERR wrong number of arguments for 'rpop' command"}

  def pop_direction("LPOP"), do: :lpop
  def pop_direction("RPOP"), do: :rpop

  def atomic_list_store?(store) when is_map(store),
    do: is_function(Map.get(store, :list_op), 2)

  def atomic_list_store?(_store), do: false

  defp internal_error(kind, reason) do
    Logger.error(fn ->
      "FerricStore blocking connection internal error: #{inspect({kind, reason}, limit: 20)}"
    end)

    {:error, "ERR internal error"}
  end
end
