defmodule Ferricstore.Flow.PipelineHistoryRead do
  @moduledoc false

  def results([], _ctx, _callbacks), do: %{}

  def results(history_ops, ctx, callbacks) when is_map(callbacks) do
    hot_ops =
      Enum.filter(history_ops, fn
        {_idx, _id, _partition_key, _history_key, _query, false, false, %{enabled?: false}} ->
          true

        _cold_or_consistent ->
          false
      end)

    cold_ops = history_ops -- hot_ops

    cold_results =
      Map.new(cold_ops, fn {idx, id, partition_key, history_key, query, include_cold?,
                            consistent?, value_return} ->
        {idx,
         callbacks.read.(
           ctx,
           id,
           partition_key,
           history_key,
           query,
           include_cold?,
           consistent?,
           value_return
         )}
      end)

    hot_results =
      if hot_ops == [] do
        %{}
      else
        hot_results(hot_ops, ctx, callbacks)
      end

    Map.merge(hot_results, cold_results)
  end

  defp hot_results(history_ops, ctx, callbacks) do
    requests =
      Enum.map(history_ops, fn {idx, id, partition_key, history_key, query, false, false,
                                value_return} ->
        fetch_count = callbacks.fetch_count.(query)

        {start_idx, stop_idx} =
          callbacks.hot_range.(ctx, id, partition_key, history_key, fetch_count)

        {idx, id, partition_key, history_key, query, start_idx, stop_idx, false, value_return}
      end)

    router_requests =
      Enum.map(requests, fn {_idx, _id, _partition_key, history_key, _query, start_idx, stop_idx,
                             reverse?, _value_return} ->
        {history_key, start_idx, stop_idx, reverse?}
      end)

    case callbacks.rank_range_many.(ctx, router_requests) do
      {:ok, rank_results} ->
        history_ops
        |> Enum.zip(rank_results)
        |> Map.new(fn {{idx, id, partition_key, history_key, query, _include_cold?, _consistent?,
                        value_return}, rank_result} ->
          {idx,
           result_from_rank(
             ctx,
             id,
             partition_key,
             history_key,
             query,
             rank_result,
             value_return,
             callbacks
           )}
        end)

      :unavailable ->
        Map.new(history_ops, fn {idx, _id, _partition_key, history_key, query, _include_cold?,
                                 _consistent?, value_return} ->
          {idx, callbacks.fallback.(ctx, history_key, query, value_return)}
        end)
    end
  end

  defp result_from_rank(
         ctx,
         _id,
         _partition_key,
         history_key,
         query,
         [],
         value_return,
         callbacks
       ),
       do: callbacks.fallback.(ctx, history_key, query, value_return)

  defp result_from_rank(
         ctx,
         id,
         partition_key,
         history_key,
         query,
         event_refs,
         value_return,
         callbacks
       ) do
    event_ids = Enum.map(event_refs, fn {event_id, _score} -> event_id end)

    with {:ok, events} <-
           callbacks.from_event_ids.(ctx, id, partition_key, history_key, event_ids, value_return) do
      {:ok, callbacks.apply_query.(events, query)}
    end
  end
end
