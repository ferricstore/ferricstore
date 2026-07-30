alias Ferricstore.Store.Shard.CompoundMemberIndex

table = :ferricstore_stream_member_index_bench
prefix = "X:bench:stream:" <> <<0>>

objects =
  Enum.map(1..64, fn seq ->
    key = prefix <> "1-#{seq}"
    {{prefix, {1, seq}}, key}
  end)

CompoundMemberIndex.ensure_table!(table)
count_row = {{:"$ferricstore_compound_member_count", prefix}, 64}
combined_rows = [count_row | objects]

Benchee.run(
  %{
    "one ETS insert" => fn ->
      :ets.insert(table, combined_rows)
    end,
    "publish 64 Stream members" => fn ->
      CompoundMemberIndex.publish_stream_append(table, prefix, objects, 64)
    end,
    "two ETS inserts" => fn ->
      :ets.insert(table, objects)
      :ets.insert(table, count_row)
    end
  },
  warmup: 2,
  time: 5,
  memory_time: 0,
  reduction_time: 0,
  print: [benchmarking: false, configuration: false]
)
