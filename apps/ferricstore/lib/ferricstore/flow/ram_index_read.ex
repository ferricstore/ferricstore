defmodule Ferricstore.Flow.RAMIndexRead do
  @moduledoc false

  alias Ferricstore.Store.Router

  def rank_entries(_ctx, _index_key, count) when count <= 0, do: {:ok, []}

  def rank_entries(ctx, index_key, count) do
    case Router.flow_index_rank_range(ctx, index_key, 0, count - 1, false) do
      {:ok, entries} -> {:ok, entries}
      :unavailable -> {:ok, []}
    end
  end

  def terminal_entries(ctx, index_key, count, nil) do
    rank_entries(ctx, index_key, count)
  end

  def terminal_entries(ctx, index_key, count, query) do
    score_entries(ctx, index_key, query, count)
  end

  def score_entries(_ctx, _index_key, _query, count) when count <= 0, do: {:ok, []}

  def score_entries(ctx, index_key, query, count) do
    case Router.flow_index_score_range_slice(
           ctx,
           index_key,
           min_bound(query.from_ms),
           max_bound(query.to_ms),
           query.rev?,
           0,
           count
         ) do
      {:ok, entries} -> {:ok, entries}
      :unavailable -> {:ok, []}
    end
  end

  def reverse?(%{rev?: true}), do: true
  def reverse?(_query), do: false

  def maybe_reverse(entries, true), do: Enum.reverse(entries)
  def maybe_reverse(entries, false), do: entries

  def min_bound(nil), do: :neg_inf
  def min_bound(ms), do: {:inclusive, ms}

  def max_bound(nil), do: :pos_inf
  def max_bound(ms), do: {:inclusive, ms}
end
