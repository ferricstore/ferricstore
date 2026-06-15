defmodule Ferricstore.Flow.PipelineRead do
  @moduledoc false

  alias Ferricstore.Flow.ValueHydration
  alias Ferricstore.Store.Router

  def batch(ctx, ops, callbacks) do
    started = callbacks.start.()

    case fast_get_batch(ctx, ops, callbacks, started) do
      {:ok, results} -> results
      :fallback -> generic_batch(ctx, ops, callbacks, started)
    end
  end

  defp generic_batch(ctx, ops, callbacks, started) do
    {get_ops, history_ops, other_ops, keyed_other_ops, indexed_results} =
      ops
      |> Enum.with_index()
      |> Enum.reduce({[], [], [], %{}, %{}}, fn {op, idx},
                                                {get_acc, history_acc, other_acc, keyed_acc,
                                                 result_acc} ->
        case callbacks.command.(ctx, op) do
          {:get, id, partition_key, payload_return} ->
            {[{idx, id, partition_key, payload_return} | get_acc], history_acc, other_acc,
             keyed_acc, result_acc}

          {:history, id, partition_key, history_key, query, include_cold?, consistent?,
           value_return} ->
            {get_acc,
             [
               {idx, id, partition_key, history_key, query, include_cold?, consistent?,
                value_return}
               | history_acc
             ], other_acc, keyed_acc, result_acc}

          {:other, fun} ->
            {get_acc, history_acc, [{idx, fun} | other_acc], keyed_acc, result_acc}

          {:other, key, fun} ->
            keyed_acc = Map.update(keyed_acc, key, [{idx, fun}], &[{idx, fun} | &1])
            {get_acc, history_acc, other_acc, keyed_acc, result_acc}

          {:error, _reason} = error ->
            {get_acc, history_acc, other_acc, keyed_acc, Map.put(result_acc, idx, error)}
        end
      end)

    indexed_results =
      get_ops
      |> Enum.reverse()
      |> get_results(ctx, callbacks)
      |> Map.merge(indexed_results)

    indexed_results =
      history_ops
      |> Enum.reverse()
      |> callbacks.history_results.(ctx)
      |> Map.merge(indexed_results)

    indexed_results =
      other_ops
      |> Enum.reverse()
      |> Enum.reduce(indexed_results, fn {idx, fun}, acc ->
        Map.put(acc, idx, fun.())
      end)

    indexed_results =
      Enum.reduce(keyed_other_ops, indexed_results, fn {_key, entries}, acc ->
        [{_idx, fun} | _rest] = entries = Enum.reverse(entries)
        result = fun.()
        Enum.reduce(entries, acc, fn {idx, _fun}, acc -> Map.put(acc, idx, result) end)
      end)

    results = for idx <- 0..(length(ops) - 1), do: Map.fetch!(indexed_results, idx)

    callbacks.observe.(started, ops)
    results
  end

  defp fast_get_batch(_ctx, [], _callbacks, _started), do: {:ok, []}

  defp fast_get_batch(ctx, ops, callbacks, started) do
    with {:ok, get_ops} <- fast_get_ops(ctx, ops, callbacks),
         true <- fast_get_payload_disabled?(get_ops) do
      results =
        get_ops
        |> fast_get_results(ctx, callbacks)
        |> case do
          {:ordered, results} -> results
          {:indexed, indexed_pairs} -> ordered_results(indexed_pairs, length(ops))
        end

      callbacks.observe.(started, ops)
      {:ok, results}
    else
      _ -> :fallback
    end
  end

  defp fast_get_ops(ctx, ops, callbacks) do
    ops
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {op, idx}, {:ok, acc} ->
      case callbacks.command.(ctx, op) do
        {:get, id, partition_key, payload_return} ->
          {:cont, {:ok, [{idx, id, partition_key, payload_return} | acc]}}

        _other ->
          {:halt, :fallback}
      end
    end)
    |> case do
      {:ok, get_ops} -> {:ok, Enum.reverse(get_ops)}
      :fallback -> :fallback
    end
  end

  defp fast_get_payload_disabled?(get_ops) do
    Enum.all?(get_ops, fn {_idx, _id, _partition_key, payload_return} ->
      not Map.fetch!(payload_return, :enabled?)
    end)
  end

  defp fast_get_results(
         [{_idx, _id, partition_key, _payload_return} | rest] = get_ops,
         ctx,
         callbacks
       ) do
    batch_get = Map.fetch!(callbacks, :batch_get)

    if same_partition?(rest, partition_key) do
      ids = Enum.map(get_ops, fn {_idx, id, _partition_key, _payload_return} -> id end)
      values = batch_get.(ctx, ids, partition_key)

      results =
        values
        |> Enum.map(fn value -> callbacks.decode_get.(value) end)

      {:ordered, results}
    else
      {:indexed, fast_get_partitioned_results(get_ops, ctx, callbacks, batch_get)}
    end
  end

  defp fast_get_results([], _ctx, _callbacks), do: {:ordered, []}

  defp same_partition?([], _partition_key), do: true

  defp same_partition?([{_idx, _id, partition_key, _payload_return} | rest], partition_key),
    do: same_partition?(rest, partition_key)

  defp same_partition?(_get_ops, _partition_key), do: false

  defp fast_get_partitioned_results(get_ops, ctx, callbacks, batch_get) do
    get_ops
    |> Enum.group_by(fn {_idx, _id, partition_key, _payload_return} -> partition_key end)
    |> Enum.flat_map(fn {partition_key, group} ->
      ids = Enum.map(group, fn {_idx, id, _partition_key, _payload_return} -> id end)
      values = batch_get.(ctx, ids, partition_key)

      group
      |> Enum.zip(values)
      |> Enum.map(fn {{idx, _id, _partition_key, _payload_return}, value} ->
        {idx, callbacks.decode_get.(value)}
      end)
    end)
  end

  defp ordered_results(indexed_pairs, count) do
    indexed_results = Map.new(indexed_pairs)
    for idx <- 0..(count - 1), do: Map.fetch!(indexed_results, idx)
  end

  def get_results([], _ctx, _callbacks), do: %{}

  def get_results(get_ops, ctx, callbacks) do
    decoded =
      get_ops
      |> Enum.group_by(fn {_idx, _id, partition_key, _payload_return} -> partition_key end)
      |> Enum.flat_map(fn {partition_key, group} ->
        ids = Enum.map(group, fn {_idx, id, _partition_key, _payload_return} -> id end)
        values = Router.flow_batch_get(ctx, ids, partition_key)

        group
        |> Enum.zip(values)
        |> Enum.map(fn {{idx, _id, _partition_key, payload_return}, value} ->
          {idx, callbacks.decode_get.(value), payload_return}
        end)
      end)

    decoded
    |> hydrate_get_results(ctx)
    |> Map.new()
  end

  def hydrate_get_results(decoded, ctx) do
    {records, pass_through} =
      Enum.reduce(decoded, {[], []}, fn
        {idx, {:ok, record}, payload_return}, {records_acc, pass_acc} when is_map(record) ->
          {[{idx, record, payload_return} | records_acc], pass_acc}

        {idx, result, _payload_return}, {records_acc, pass_acc} ->
          {records_acc, [{idx, result} | pass_acc]}
      end)

    hydrated =
      records
      |> Enum.reverse()
      |> Enum.group_by(fn {_idx, _record, payload_return} ->
        {Map.fetch!(payload_return, :enabled?), Map.fetch!(payload_return, :max_bytes)}
      end)
      |> Enum.flat_map(fn
        {{false, _max_bytes}, entries} ->
          Enum.map(entries, fn {idx, record, _payload_return} -> {idx, {:ok, record}} end)

        {{true, max_bytes}, entries} ->
          hydrated_records =
            ValueHydration.payload_records(
              ctx,
              Enum.map(entries, fn {_idx, record, _payload_return} -> record end),
              %{enabled?: true, max_bytes: max_bytes}
            )

          entries
          |> Enum.map(fn {idx, _record, _payload_return} -> idx end)
          |> Enum.zip(hydrated_records)
          |> Enum.map(fn {idx, record} -> {idx, {:ok, record}} end)
      end)

    pass_through ++ hydrated
  end
end
