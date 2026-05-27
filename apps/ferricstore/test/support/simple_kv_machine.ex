defmodule Ferricstore.Test.SimpleKvMachine do
  def init(_config), do: %{}

  def apply(_meta, {:put, key, value}, state) do
    {Map.put(state, key, value), :ok}
  end

  def apply(_meta, {:get, key}, state) do
    {state, Map.get(state, key)}
  end

  def apply(_meta, _cmd, state), do: {state, :ok}

  def state_enter(_role, _state), do: []

  def count(state), do: map_size(state)
end
