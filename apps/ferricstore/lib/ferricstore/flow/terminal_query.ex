defmodule Ferricstore.Flow.TerminalQuery do
  @moduledoc false

  def state(opts, terminal_states) do
    case Keyword.get(opts, :state, "any") do
      "any" ->
        {:ok, "any"}

      state ->
        if state in terminal_states do
          {:ok, state}
        else
          {:error, "ERR flow terminal state must be failed, completed, cancelled, or any"}
        end
    end
  end

  def ids_from_chunks(chunks, count, terminal_states) do
    chunks
    |> Enum.flat_map(& &1)
    |> Enum.uniq()
    |> Enum.take(count * length(terminal_states))
  end

  def filter_any(records, terminal_states) do
    Enum.filter(records, &(Map.get(&1, :state) in terminal_states))
  end

  def filter_state(records, state) do
    Enum.filter(records, &(Map.get(&1, :state) == state))
  end
end
