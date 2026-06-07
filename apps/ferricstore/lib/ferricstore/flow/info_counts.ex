defmodule Ferricstore.Flow.InfoCounts do
  @moduledoc false

  alias Ferricstore.Flow.Keys

  def zero_counts(default_state, terminal_states) do
    [default_state, "running" | terminal_states]
    |> Map.new(&{String.to_atom(&1), 0})
  end

  def state_keys(type, partition_key, default_state, terminal_states) do
    Enum.map([default_state, "running" | terminal_states], fn state ->
      {state, Keys.state_index_key(type, state, partition_key)}
    end)
  end

  def inflight_key(type, partition_key) do
    {"inflight", Keys.inflight_index_key(type, partition_key)}
  end

  def merge_auto({counts_acc, inflight_acc}, counts, inflight) do
    merged =
      Map.merge(counts_acc, counts, fn _state, left, right ->
        left + right
      end)

    {merged, inflight_acc + inflight}
  end

  def terminal_keys(state_keys, terminal_states) do
    state_keys
    |> Enum.filter(fn {state, _key} -> state in terminal_states end)
    |> Enum.map(fn {_state, key} -> key end)
  end

  def merge_terminal_counts(acc, terminal_keys, counts) do
    terminal_keys
    |> Enum.zip(counts)
    |> Enum.reduce(acc, fn {key, count}, count_acc ->
      Map.update!(count_acc, key, &(&1 + count))
    end)
  end
end
