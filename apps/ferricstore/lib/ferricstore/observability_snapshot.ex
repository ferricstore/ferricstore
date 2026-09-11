defmodule Ferricstore.ObservabilitySnapshot do
  @moduledoc false

  # Retaining a small top-N list alone does not bound a large ETS traversal.
  def fold(reducer, initial, table, budget \\ 10_000)
  def fold(_reducer, initial, _table, 0), do: initial

  def fold(reducer, initial, table, budget)
      when is_function(reducer, 2) and is_integer(budget) and budget > 0 do
    halt = make_ref()

    try do
      {_remaining, result} =
        :ets.foldl(
          fn row, {remaining, acc} ->
            next = reducer.(row, acc)
            if remaining == 1, do: throw({halt, next}), else: {remaining - 1, next}
          end,
          {budget, initial},
          table
        )

      result
    catch
      {^halt, result} -> result
    end
  end
end
