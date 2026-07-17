defmodule Ferricstore.Stream.LocalState do
  @moduledoc false

  alias Ferricstore.Commands.Stream.CacheKey
  alias Ferricstore.Commands.Stream.Waiters

  @tables [
    Ferricstore.Stream.Meta,
    Ferricstore.Stream.Groups,
    Ferricstore.Stream.Index
  ]

  @spec clear() :: :ok
  def clear, do: clear(nil)

  @spec clear(term()) :: :ok
  def clear(store) do
    scope = CacheKey.scope(store)

    Enum.each(@tables, fn table ->
      if :ets.whereis(table) != :undefined do
        clear_table(table, scope)
      end
    end)

    Waiters.notify_scope(store)
    :ok
  end

  defp clear_table(table, :unscoped), do: :ets.delete_all_objects(table)

  defp clear_table(Ferricstore.Stream.Meta = table, {:ok, scope}) do
    delete_scoped(table, {{{:"$1", :_}, :_, :_, :_, :_, :_}, scope})
  end

  defp clear_table(Ferricstore.Stream.Groups = table, {:ok, scope}) do
    delete_scoped(table, {{{{:"$1", :_}, :_}, :_, :_, :_}, scope})
  end

  defp clear_table(Ferricstore.Stream.Index = table, {:ok, scope}) do
    delete_scoped(table, {{{:ready, {:"$1", :_}}, :_}, scope})
    delete_scoped(table, {{{{:"$1", :_}, :_, :_}, :_, :_}, scope})
  end

  defp delete_scoped(table, {head, scope}) do
    :ets.select_delete(table, [
      {head, [{:"=:=", :"$1", {:const, scope}}], [true]}
    ])
  end
end
