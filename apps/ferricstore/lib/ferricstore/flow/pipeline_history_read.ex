defmodule Ferricstore.Flow.PipelineHistoryRead do
  @moduledoc false

  alias Ferricstore.BatchResult

  @batch_read_error {:error, "ERR flow history batch read result mismatch"}

  def results([], _ctx, _callbacks), do: %{}

  def results(history_ops, ctx, callbacks) when is_map(callbacks) do
    {hot_ops, cold_ops} = split_hot_cold_ops(history_ops)

    cold_results = cold_results(cold_ops, ctx, callbacks)

    hot_results =
      if hot_ops == [] do
        %{}
      else
        hot_results(hot_ops, ctx, callbacks)
      end

    Map.merge(hot_results, cold_results)
  end

  defp split_hot_cold_ops(history_ops) do
    {hot_ops, cold_ops} =
      Enum.reduce(history_ops, {[], []}, fn
        {_idx, _id, _partition_key, _history_key, _query, false, false, %{enabled?: false}} = op,
        {hot_acc, cold_acc} ->
          {[op | hot_acc], cold_acc}

        op, {hot_acc, cold_acc} ->
          {hot_acc, [op | cold_acc]}
      end)

    {Enum.reverse(hot_ops), Enum.reverse(cold_ops)}
  end

  defp cold_results([], _ctx, _callbacks), do: %{}

  defp cold_results(cold_ops, ctx, callbacks) do
    {consistent_ops, direct_ops} =
      Enum.split_with(cold_ops, fn
        {_idx, _id, _partition_key, _history_key, _query, _include_cold?, true, _value_return} ->
          true

        _op ->
          false
      end)

    direct_results = read_cold_ops(direct_ops, ctx, callbacks, false)

    consistent_results =
      case prepare_consistent_history(consistent_ops, ctx, callbacks) do
        :ok ->
          read_cold_ops(consistent_ops, ctx, callbacks, true)

        {:error, _reason} = error ->
          Map.new(consistent_ops, fn
            {idx, _id, _partition_key, _history_key, _query, _include_cold?, _consistent?,
             _value_return} ->
              {idx, error}
          end)
      end

    Map.merge(direct_results, consistent_results)
  end

  defp read_cold_ops(cold_ops, ctx, callbacks, consistent_already_prepared?) do
    Map.new(cold_ops, fn {idx, id, partition_key, history_key, query, include_cold?, consistent?,
                          value_return} ->
      read_consistent? = if consistent_already_prepared?, do: false, else: consistent?

      {idx,
       callbacks.read.(
         ctx,
         id,
         partition_key,
         history_key,
         query,
         include_cold?,
         read_consistent?,
         value_return
       )}
    end)
  end

  defp prepare_consistent_history([], _ctx, _callbacks), do: :ok

  defp prepare_consistent_history(consistent_ops, ctx, %{prepare_consistent: prepare})
       when is_function(prepare, 2) do
    prepare.(ctx, consistent_ops)
  end

  defp prepare_consistent_history(_consistent_ops, _ctx, _callbacks), do: :ok

  defp hot_results(history_ops, ctx, callbacks) do
    context_by_flow = decode_contexts_for_ops(history_ops, ctx, callbacks)

    {ready_results, requests} =
      Enum.reduce(history_ops, {%{}, []}, fn {idx, id, partition_key, history_key, query, false,
                                              false, value_return} = op,
                                             {ready_acc, request_acc} ->
        case Map.fetch(context_by_flow, {id, partition_key}) do
          {:ok, {:error, _reason} = error} ->
            {Map.put(ready_acc, idx, error), request_acc}

          {:ok, decode_context} ->
            fetch_count = callbacks.fetch_count.(query)

            if no_hot_history?(decode_context) do
              {Map.put(ready_acc, idx, {:ok, []}), request_acc}
            else
              {start_idx, stop_idx} =
                hot_range(
                  ctx,
                  id,
                  partition_key,
                  history_key,
                  fetch_count,
                  decode_context,
                  callbacks
                )

              {ready_acc,
               [
                 {op, idx, id, partition_key, history_key, query, start_idx, stop_idx, false,
                  value_return, decode_context}
                 | request_acc
               ]}
            end

          :error ->
            {Map.put(ready_acc, idx, @batch_read_error), request_acc}
        end
      end)

    requests = Enum.reverse(requests)

    router_requests =
      Enum.map(requests, fn {_op, _idx, _id, _partition_key, history_key, _query, start_idx,
                             stop_idx, reverse?, _value_return, _decode_context} ->
        {history_key, start_idx, stop_idx, reverse?}
      end)

    if requests == [] do
      ready_results
    else
      case callbacks.rank_range_many.(ctx, router_requests) do
        {:ok, rank_results} ->
          ranked_results = hot_rank_results(requests, rank_results, ctx, callbacks)

          Map.merge(ready_results, ranked_results)

        :unavailable ->
          fallback_results =
            Map.new(requests, fn {_op, idx, _id, _partition_key, history_key, query, _start_idx,
                                  _stop_idx, _reverse?, value_return, _decode_context} ->
              {idx, callbacks.fallback.(ctx, history_key, query, value_return)}
            end)

          Map.merge(ready_results, fallback_results)

        _invalid ->
          Map.merge(ready_results, failed_hot_results(requests))
      end
    end
  end

  defp hot_rank_results(requests, rank_results, ctx, callbacks) do
    case BatchResult.map_exact(requests, rank_results, fn
           {_op, idx, id, partition_key, history_key, query, _start_idx, _stop_idx, _reverse?,
            value_return, decode_context},
           rank_result ->
             {idx,
              result_from_rank(
                ctx,
                id,
                partition_key,
                history_key,
                query,
                rank_result,
                value_return,
                decode_context,
                callbacks
              )}
         end) do
      {:ok, results} -> Map.new(results)
      {:error, _reason} -> failed_hot_results(requests)
    end
  end

  defp failed_hot_results(requests) do
    Map.new(requests, fn {_op, idx, _id, _partition_key, _history_key, _query, _start_idx,
                          _stop_idx, _reverse?, _value_return, _decode_context} ->
      {idx, @batch_read_error}
    end)
  end

  defp result_from_rank(
         ctx,
         _id,
         _partition_key,
         history_key,
         query,
         [],
         value_return,
         _decode_context,
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
         decode_context,
         callbacks
       ) do
    with {:ok, event_ids} <- event_ids(event_refs),
         {:ok, events} <-
           from_event_ids(
             ctx,
             id,
             partition_key,
             history_key,
             event_ids,
             value_return,
             decode_context,
             callbacks
           ) do
      {:ok, callbacks.apply_query.(events, query)}
    end
  end

  defp event_ids(event_refs) when is_list(event_refs) do
    Enum.reduce_while(event_refs, {:ok, []}, fn
      {event_id, score}, {:ok, acc} when is_binary(event_id) and is_number(score) ->
        {:cont, {:ok, [event_id | acc]}}

      _invalid, _acc ->
        {:halt, @batch_read_error}
    end)
    |> case do
      {:ok, ids} -> {:ok, Enum.reverse(ids)}
      @batch_read_error -> @batch_read_error
    end
  end

  defp event_ids(_invalid), do: @batch_read_error

  defp decode_contexts_for_ops(history_ops, ctx, callbacks) do
    flows =
      Enum.map(history_ops, fn
        {_idx, id, partition_key, _history_key, _query, _include_cold?, _consistent?,
         _value_return} ->
          {id, partition_key}
      end)

    case callbacks do
      %{decode_contexts: decode_contexts} when is_function(decode_contexts, 2) ->
        case decode_contexts.(ctx, flows) do
          contexts when is_map(contexts) -> contexts
          _invalid -> Map.new(flows, &{&1, @batch_read_error})
        end

      _without_context_decoder ->
        Map.new(flows, &{&1, nil})
    end
  end

  defp hot_range(
         ctx,
         _id,
         _partition_key,
         history_key,
         fetch_count,
         %{history_hot_max_events: max},
         %{hot_range_for_max: hot_range_for_max}
       )
       when is_function(hot_range_for_max, 4) and is_integer(max) and max > 0 do
    hot_range_for_max.(ctx, history_key, fetch_count, max)
  end

  defp hot_range(ctx, id, partition_key, history_key, fetch_count, _decode_context, callbacks) do
    callbacks.hot_range.(ctx, id, partition_key, history_key, fetch_count)
  end

  defp no_hot_history?(%{history_hot_max_events: max}) when is_integer(max) and max <= 0,
    do: true

  defp no_hot_history?(_decode_context), do: false

  defp from_event_ids(
         ctx,
         id,
         partition_key,
         history_key,
         event_ids,
         value_return,
         decode_context,
         %{from_event_ids_with_context: from_event_ids_with_context}
       )
       when is_function(from_event_ids_with_context, 7) and not is_nil(decode_context) do
    from_event_ids_with_context.(
      ctx,
      id,
      partition_key,
      history_key,
      event_ids,
      value_return,
      decode_context
    )
  end

  defp from_event_ids(
         ctx,
         id,
         partition_key,
         history_key,
         event_ids,
         value_return,
         _decode_context,
         callbacks
       ) do
    callbacks.from_event_ids.(ctx, id, partition_key, history_key, event_ids, value_return)
  end
end
