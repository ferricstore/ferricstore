defmodule Ferricstore.Test.KvMachine do
  @moduledoc """
  Lightweight in-memory KV state machine for replication cluster tests.

  State is a plain `%{key => value}` map. Supports `:put`, `:delete`,
  `:get`, and `:batch` commands.

  Used by legacy cluster benchmark fixtures.
  """

  def init(_config), do: %{}

  def apply(_meta, {:put, key, value}, state) do
    {Map.put(state, key, value), :ok}
  end

  def apply(_meta, {:get, key}, state) do
    {state, Map.get(state, key)}
  end

  def apply(_meta, {:delete, key}, state) do
    {Map.delete(state, key), :ok}
  end

  def apply(_meta, {:batch, commands}, state) do
    new_state =
      Enum.reduce(commands, state, fn
        {:put, key, value}, acc -> Map.put(acc, key, value)
        {:delete, key}, acc -> Map.delete(acc, key)
        _, acc -> acc
      end)

    {new_state, :ok}
  end

  def apply(_meta, _cmd, state), do: {state, :ok}

  def state_enter(_role, _state), do: []

  @doc "Identity function for query fixtures."
  def identity(state), do: state
end
