alias Ferricstore.Store.Router
alias Ferricstore.Store.SlotMap

defmodule StreamMixedPrepareBench do
  alias Ferricstore.Store.Router

  def legacy(ctx, items) do
    commands =
      Enum.map(items, fn {key, fields} ->
        {key, {:stream_append, key, :auto, fields, nil, false}}
      end)

    {buckets, count} =
      Enum.reduce(commands, {:erlang.make_tuple(ctx.shard_count, {[], []}), 0}, fn
        {key, command}, {buckets, index} ->
          shard_idx = Router.shard_for(ctx, key)
          {shard_commands, indices} = elem(buckets, shard_idx)

          {put_elem(buckets, shard_idx, {[command | shard_commands], [index | indices]}),
           index + 1}
      end)

    prepared =
      0..(ctx.shard_count - 1)
      |> Enum.reduce([], fn shard_idx, prepared ->
        case elem(buckets, shard_idx) do
          {[], []} ->
            prepared

          {reversed_commands, reversed_indices} ->
            command =
              reversed_commands
              |> Enum.reverse()
              |> Router.__stream_append_auto_batch_shape_for_test__()
              |> case do
                {:compact, command} -> command
                {:direct_batch, command} -> command
              end

            [{shard_idx, Enum.reverse(reversed_indices), command} | prepared]
        end
      end)
      |> Enum.reverse()

    {prepared, count}
  end
end

shard_count = 12
ctx = %{shard_count: shard_count, slot_map: SlotMap.build_uniform(shard_count)}

keys_by_shard =
  Stream.iterate(0, &(&1 + 1))
  |> Enum.reduce_while(%{}, fn candidate, keys ->
    key = "bench:mixed-prepare:#{candidate}"
    keys = Map.put_new(keys, Router.shard_for(ctx, key), key)

    if map_size(keys) == shard_count, do: {:halt, keys}, else: {:cont, keys}
  end)
  |> Enum.sort()
  |> Enum.map(&elem(&1, 1))

same_shard = Router.shard_for(ctx, hd(keys_by_shard))

same_shard_keys =
  Stream.iterate(0, &(&1 + 1))
  |> Stream.map(&"bench:mixed-prepare:same:#{&1}")
  |> Stream.filter(&(Router.shard_for(ctx, &1) == same_shard))
  |> Enum.take(8)

items_for = fn keys ->
  Enum.map(0..63, fn index ->
    {Enum.at(keys, rem(index, length(keys))), ["field", Integer.to_string(index)]}
  end)
end

distinct_shard_items = items_for.(keys_by_shard)
same_shard_items = items_for.(same_shard_keys)

prepare = &Router.__prepare_stream_append_many_auto_mixed_for_test__(ctx, &1)

true = StreamMixedPrepareBench.legacy(ctx, distinct_shard_items) == prepare.(distinct_shard_items)
true = StreamMixedPrepareBench.legacy(ctx, same_shard_items) == prepare.(same_shard_items)

Benchee.run(
  %{
    "legacy prepare 64 / 12 shards" => fn ->
      StreamMixedPrepareBench.legacy(ctx, distinct_shard_items)
    end,
    "direct prepare 64 / 12 shards" => fn -> prepare.(distinct_shard_items) end,
    "legacy prepare 64 / 8 topics one shard" => fn ->
      StreamMixedPrepareBench.legacy(ctx, same_shard_items)
    end,
    "direct prepare 64 / 8 topics one shard" => fn -> prepare.(same_shard_items) end
  },
  warmup: 2,
  time: 5,
  memory_time: 2,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
