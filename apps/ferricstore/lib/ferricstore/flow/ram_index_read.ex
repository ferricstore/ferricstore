defmodule Ferricstore.Flow.RAMIndexRead do
  @moduledoc false

  alias Ferricstore.Store.Router

  def rank_entries(_ctx, _index_key, count) when count <= 0, do: {:ok, []}

  def rank_entries(ctx, index_key, count) do
    case Router.flow_index_rank_range(ctx, index_key, 0, count - 1, false) do
      {:ok, entries} when is_list(entries) -> {:ok, entries}
      :unavailable -> {:error, :flow_index_unavailable}
      {:error, _reason} = error -> error
      _invalid -> {:error, :flow_index_unavailable}
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
           max_bound(query),
           query.rev?,
           0,
           count
         ) do
      {:ok, entries} when is_list(entries) -> {:ok, entries}
      :unavailable -> {:error, :flow_index_unavailable}
      {:error, _reason} = error -> error
      _invalid -> {:error, :flow_index_unavailable}
    end
  end

  def reverse?(%{rev?: true}), do: true
  def reverse?(_query), do: false

  def maybe_reverse(entries, true), do: Enum.reverse(entries)
  def maybe_reverse(entries, false), do: entries

  def min_bound(nil), do: :neg_inf
  def min_bound(ms), do: {:inclusive, ms}

  def max_bound(%{rev?: true, to_ms: to_ms, before_id: before_id})
      when is_integer(to_ms) and is_binary(before_id) and before_id != "",
      do: {:cursor_before, to_ms, before_id}

  def max_bound(%{to_ms: to_ms}), do: max_bound(to_ms)
  def max_bound(nil), do: :pos_inf
  def max_bound(ms), do: {:inclusive, ms}
end
