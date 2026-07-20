defmodule Ferricstore.Flow.PipelineClaimDue do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.Flow.{ClaimScope, Keys}
  alias Ferricstore.Store.Router

  def results([], _ctx, acc, stats, _callbacks), do: {Enum.reverse(acc), stats}

  def results(commands, ctx, acc, stats, callbacks) do
    if global_grouping_safe?(commands) do
      grouped_results(commands, ctx, stats, callbacks)
    else
      adjacent_results(commands, ctx, acc, stats, callbacks)
    end
  end

  defp adjacent_results([], _ctx, acc, stats, _callbacks), do: {Enum.reverse(acc), stats}

  defp adjacent_results([{:error, _reason} = error | rest], ctx, acc, stats, callbacks),
    do: adjacent_results(rest, ctx, [error | acc], stats, callbacks)

  defp adjacent_results([{:ok, claim} | rest], ctx, acc, stats, callbacks) do
    {run, rest} = take_compatible_claims(rest, claim.key, [claim])
    claims = Enum.reverse(run)
    {results, stats} = execute_run(ctx, claims, stats, callbacks)
    adjacent_results(rest, ctx, prepend_results(results, acc), stats, callbacks)
  end

  defp prepend_results(results, acc) do
    Enum.reduce(results, acc, fn result, acc -> [result | acc] end)
  end

  defp take_compatible_claims(
         [{:ok, %{key: key, groupable?: true} = claim} | rest],
         key,
         [%{groupable?: true} | _] = acc
       ),
       do: take_compatible_claims(rest, key, [claim | acc])

  defp take_compatible_claims(rest, _key, acc), do: {acc, rest}

  defp global_grouping_safe?(commands) do
    commands
    |> Enum.reduce_while(%{}, fn
      {:ok, %{groupable?: false}}, _seen ->
        {:halt, false}

      {:ok, %{queue_key: queue_key, key: key, groupable?: true}}, seen ->
        case Map.get(seen, queue_key) do
          nil -> {:cont, Map.put(seen, queue_key, key)}
          ^key -> {:cont, seen}
          _conflicting_key -> {:halt, false}
        end

      {:error, _reason}, seen ->
        {:cont, seen}
    end)
    |> is_map()
  end

  defp grouped_results(commands, ctx, stats, callbacks) do
    {groups, indexed_results} =
      commands
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}}, fn
        {{:ok, claim}, idx}, {group_acc, result_acc} ->
          {Map.update(group_acc, claim.key, [{idx, claim}], fn acc -> [{idx, claim} | acc] end),
           result_acc}

        {{:error, _reason} = error, idx}, {group_acc, result_acc} ->
          {group_acc, Map.put(result_acc, idx, error)}
      end)

    {singletons, indexed_results, stats} =
      Enum.reduce(groups, {[], indexed_results, stats}, fn {_key, indexed_claims},
                                                           {singleton_acc, result_acc, stats_acc} ->
        indexed_claims = Enum.reverse(indexed_claims)

        case indexed_claims do
          [{_idx, _claim} = singleton] ->
            {[singleton | singleton_acc], result_acc, stats_acc}

          _ ->
            claims = Enum.map(indexed_claims, fn {_idx, claim} -> claim end)
            {results, stats_acc} = execute_run(ctx, claims, stats_acc, callbacks)

            {result_acc, stats_acc} =
              indexed_claims
              |> Enum.map(fn {idx, _claim} -> idx end)
              |> Enum.zip(results)
              |> Enum.reduce({result_acc, stats_acc}, fn {idx, result}, {acc, stats} ->
                {Map.put(acc, idx, result), stats}
              end)

            {singleton_acc, result_acc, stats_acc}
        end
      end)

    {indexed_results, stats} =
      execute_singleton_batch(ctx, Enum.reverse(singletons), indexed_results, stats, callbacks)

    results = for idx <- 0..(length(commands) - 1), do: Map.fetch!(indexed_results, idx)
    {results, stats}
  end

  defp execute_singleton_batch(_ctx, [], indexed_results, stats, _callbacks),
    do: {indexed_results, stats}

  defp execute_singleton_batch(ctx, indexed_claims, indexed_results, stats, callbacks) do
    {router_claims, routed_claims} =
      Enum.split_with(indexed_claims, fn {_idx, claim} ->
        router_required?(claim)
      end)

    {indexed_results, stats} =
      execute_router_singletons(ctx, router_claims, indexed_results, stats, callbacks)

    execute_routed_singleton_batch(ctx, routed_claims, indexed_results, stats, callbacks)
  end

  defp execute_router_singletons(_ctx, [], indexed_results, stats, _callbacks),
    do: {indexed_results, stats}

  defp execute_router_singletons(ctx, indexed_claims, indexed_results, stats, callbacks) do
    indexed_results =
      Enum.reduce(indexed_claims, indexed_results, fn {idx, claim}, acc ->
        result = hydrated_result(ctx, claim, router(ctx, claim.attrs), callbacks)
        Map.put(acc, idx, result)
      end)

    {indexed_results, %{stats | groups: stats.groups + length(indexed_claims)}}
  end

  defp execute_routed_singleton_batch(_ctx, [], indexed_results, stats, _callbacks),
    do: {indexed_results, stats}

  defp execute_routed_singleton_batch(ctx, indexed_claims, indexed_results, stats, callbacks) do
    keyed_commands =
      Enum.map(indexed_claims, fn {_idx, claim} ->
        key = route_key(claim.attrs)
        {key, {:flow_claim_due, key, claim.attrs}}
      end)

    results = pipeline_write_batch(ctx, keyed_commands, callbacks)

    indexed_results =
      case BatchResult.map_exact(indexed_claims, results, fn {idx, claim}, result ->
             {idx, hydrated_result(ctx, claim, result, callbacks)}
           end) do
        {:ok, pairs} ->
          Enum.reduce(pairs, indexed_results, fn {idx, result}, acc ->
            Map.put(acc, idx, result)
          end)

        {:error, _reason} ->
          Enum.reduce(indexed_claims, indexed_results, fn {idx, _claim}, acc ->
            Map.put(acc, idx, {:error, "ERR pipeline claim batch result mismatch"})
          end)
      end

    {indexed_results, %{stats | groups: stats.groups + 1, batched_calls: stats.batched_calls + 1}}
  end

  defp pipeline_write_batch(ctx, keyed_commands, callbacks) do
    case callbacks do
      %{pipeline_write_batch: fun} when is_function(fun, 2) -> fun.(ctx, keyed_commands)
      _other -> Router.pipeline_write_batch(ctx, keyed_commands)
    end
  end

  defp router_required?(%{attrs: %{partition_keys: [_ | _]}}), do: true
  defp router_required?(%{attrs: %{partition_key: :auto}}), do: true
  defp router_required?(%{attrs: %{partition_key: :any}}), do: true
  defp router_required?(_claim), do: false

  defp route_key(%{
         type: type,
         state: state,
         priority: priority,
         partition_keys: [partition_key | _]
       }) do
    Keys.due_key(type, route_state(state), priority || 0, partition_key)
  end

  defp route_key(%{
         type: type,
         state: state,
         priority: priority,
         partition_key: partition_key
       }) do
    Keys.due_key(type, route_state(state), priority || 0, partition_key)
  end

  defp route_state(:any), do: "queued"
  defp route_state([state | _]) when is_binary(state), do: state
  defp route_state(state) when is_binary(state), do: state
  defp route_state(_state), do: "queued"

  defp hydrated_result(ctx, claim, {:ok, records}, callbacks) when is_list(records) do
    with :ok <- ClaimScope.verify_records(records, Map.get(claim, :expected_metadata, %{})) do
      {:ok,
       callbacks.return_records.(
         ctx,
         records,
         claim.payload_return,
         claim.return_mode,
         claim.named_values
       )}
    end
  end

  defp hydrated_result(_ctx, _claim, other, _callbacks), do: other

  defp execute_run(ctx, [%{type: type, opts: opts} = claim], stats, callbacks) do
    metadata = Map.get(claim, :expected_metadata, %{})

    {[callbacks.claim_due_result.(ctx, type, opts, metadata)],
     %{stats | groups: stats.groups + 1}}
  end

  defp execute_run(
         ctx,
         [
           %{
             attrs: %{state: state},
             reclaim_expired?: true,
             reclaim_ratio: reclaim_ratio
           }
           | _
         ] = claims,
         stats,
         callbacks
       )
       when state != "running" and reclaim_ratio > 0 do
    results = execute_reclaim_run(ctx, claims, reclaim_ratio, callbacks)
    {results, %{stats | groups: stats.groups + 1, coalesced_calls: stats.coalesced_calls + 1}}
  end

  defp execute_run(ctx, [%{type: type, opts: opts} | _] = claims, stats, callbacks) do
    total_limit = Enum.reduce(claims, 0, fn %{limit: limit}, acc -> acc + limit end)
    combined_opts = Keyword.put(opts, :limit, total_limit)

    results =
      case callbacks.claim_due_result.(
             ctx,
             type,
             combined_opts,
             Map.get(hd(claims), :expected_metadata, %{})
           ) do
        {:ok, records} ->
          split_records(records, claims, [])

        {:error, _reason} = error ->
          List.duplicate(error, length(claims))
      end

    {results, %{stats | groups: stats.groups + 1, coalesced_calls: stats.coalesced_calls + 1}}
  end

  defp execute_reclaim_run(ctx, [%{attrs: base_attrs} | _] = claims, reclaim_ratio, callbacks) do
    initial_caps =
      Enum.map(claims, fn %{limit: limit} ->
        max(1, div(limit * reclaim_ratio + 99, 100))
      end)

    with {:ok, reclaimed_first} <-
           router(ctx, %{
             base_attrs
             | state: "running",
               limit: Enum.sum(initial_caps)
           }),
         {first_allocations, _unused} <- allocate_records(reclaimed_first, initial_caps),
         normal_caps = remaining_caps(claims, first_allocations),
         normal_attrs =
           callbacks.normal_attrs.(
             base_attrs,
             callbacks.normal_state_filter.(base_attrs.state),
             Enum.sum(normal_caps)
           ),
         {:ok, normal} <- router_maybe(ctx, normal_attrs),
         {normal_allocations, _unused} <- allocate_records(normal, normal_caps),
         final_caps = remaining_caps(claims, first_allocations, normal_allocations),
         {:ok, reclaimed_more} <-
           router(ctx, %{
             base_attrs
             | state: "running",
               limit: Enum.sum(final_caps)
           }),
         {final_allocations, _unused} <- allocate_records(reclaimed_more, final_caps) do
      [first_allocations, normal_allocations, final_allocations]
      |> combine_allocations()
      |> Enum.map(fn allocations ->
        {:ok, Enum.flat_map(allocations, & &1)}
      end)
      |> hydrate_results(
        ctx,
        hd(claims).payload_return,
        hd(claims).return_mode,
        hd(claims).named_values,
        Map.get(hd(claims), :expected_metadata, %{}),
        callbacks
      )
    else
      {:error, _reason} = error -> List.duplicate(error, length(claims))
    end
  end

  defp router_maybe(_ctx, nil), do: {:ok, []}
  defp router_maybe(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp router_maybe(ctx, attrs), do: router(ctx, attrs)

  defp router(_ctx, %{limit: limit}) when limit <= 0, do: {:ok, []}
  defp router(ctx, attrs), do: Router.flow_claim_due(ctx, attrs)

  defp allocate_records(records, caps) do
    Enum.map_reduce(caps, records, fn cap, remaining_records ->
      Enum.split(remaining_records, cap)
    end)
  end

  defp combine_allocations([first_allocations, normal_allocations, final_allocations]) do
    first_allocations
    |> Enum.zip(normal_allocations)
    |> Enum.zip(final_allocations)
    |> Enum.map(fn {{first, normal}, final} -> [first, normal, final] end)
  end

  defp remaining_caps(claims, allocations) do
    claims
    |> Enum.zip(allocations)
    |> Enum.map(fn {%{limit: limit}, records} -> limit - length(records) end)
  end

  defp remaining_caps(claims, first_allocations, normal_allocations) do
    claims
    |> Enum.zip(first_allocations)
    |> Enum.zip(normal_allocations)
    |> Enum.map(fn {{%{limit: limit}, first}, normal} ->
      limit - length(first) - length(normal)
    end)
  end

  defp hydrate_results(
         results,
         ctx,
         payload_return,
         return_mode,
         named_values,
         expected_metadata,
         callbacks
       ) do
    Enum.map(results, fn
      {:ok, records} ->
        with :ok <- ClaimScope.verify_records(records, expected_metadata) do
          {:ok,
           callbacks.return_records.(ctx, records, payload_return, return_mode, named_values)}
        end

      other ->
        other
    end)
  end

  defp split_records(_records, [], acc), do: Enum.reverse(acc)

  defp split_records(records, [%{limit: limit} | rest], acc) do
    {claimed, records} = Enum.split(records, limit)
    split_records(records, rest, [{:ok, claimed} | acc])
  end
end
