defmodule Ferricstore.Commands.Set.Random do
  @moduledoc false

  @max_replacement_count 10_000

  def select_random_members(_members, count) when count < -@max_replacement_count,
    do: {:error, "ERR count exceeds maximum allowed response size"}

  def select_random_members(members, count) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        Enum.take_random(members, count)

      count < 0 ->
        abs_count = abs(count)

        if members == [] do
          []
        else
          tuple = List.to_tuple(members)
          size = tuple_size(tuple)
          for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)
        end
    end
  end
end
