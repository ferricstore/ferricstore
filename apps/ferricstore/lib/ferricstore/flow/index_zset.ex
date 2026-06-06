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

  def range_by_score(ctx, key, min, max) do
    case Router.flow_index_score_range_slice(
           ctx,
           key,
           ScoreBound.parse(min),
           ScoreBound.parse(max),
           false,
           0,
           :all
         ) do
      {:ok, members} -> {:ok, Enum.map(members, fn {member, _score} -> member end)}
      :unavailable -> {:error, "ERR flow index unavailable"}
    end
  end
end
