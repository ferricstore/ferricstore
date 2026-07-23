defmodule Ferricstore.Flow.IndexZSet do
  @moduledoc false

  alias Ferricstore.Flow.ScoreBound
  alias Ferricstore.Store.Router

  def card(ctx, key) do
    case Router.flow_index_count_all(ctx, key) do
      {:ok, count} -> {:ok, count}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end

  def range(ctx, key, start, stop) do
    case Router.flow_index_rank_range(ctx, key, start, stop, false) do
      {:ok, members} -> {:ok, Enum.map(members, fn {member, _score} -> member end)}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end

  def range_by_score(ctx, key, min, max, count, reverse? \\ false, boundary \\ nil) do
    with {:ok, min_bound, max_bound} <-
           cursor_bounds(ScoreBound.parse(min), ScoreBound.parse(max), reverse?, boundary) do
      case Router.flow_index_score_range_slice(
             ctx,
             key,
             min_bound,
             max_bound,
             reverse?,
             0,
             count
           ) do
        {:ok, members} -> {:ok, Enum.map(members, fn {member, _score} -> member end)}
        :unavailable -> {:error, "ERR flow index unavailable"}
      end
    end
  end

  defp cursor_bounds(min_bound, max_bound, _reverse?, nil),
    do: {:ok, min_bound, max_bound}

  defp cursor_bounds(_min_bound, max_bound, false, {score, member})
       when is_integer(score) and score >= 0 and is_binary(member) and member != "",
       do: {:ok, {:cursor_after, score, member}, max_bound}

  defp cursor_bounds(min_bound, _max_bound, true, {score, member})
       when is_integer(score) and score >= 0 and is_binary(member) and member != "",
       do: {:ok, min_bound, {:cursor_before, score, member}}

  defp cursor_bounds(_min_bound, _max_bound, _reverse?, _boundary),
    do: {:error, "ERR invalid flow index cursor"}
end
