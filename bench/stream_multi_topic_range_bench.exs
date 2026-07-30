alias Ferricstore.Store.Shard.CompoundMemberIndex

run_id = System.unique_integer([:positive, :monotonic])
index = :ets.new(:stream_multi_topic_range_index, [:ordered_set, :public])
keydir = :ets.new(:stream_multi_topic_range_keydir, [:set, :public])
topic_count = System.get_env("TOPIC_COUNT", "256") |> String.to_integer()
rows_per_topic = 128
:ets.insert(index, {:"$ferricstore_compound_member_index_ready", true})

for topic <- 1..topic_count do
  prefix =
    "X:bench:topic:#{String.pad_leading(Integer.to_string(topic), 6, "0")}:#{run_id}" <> <<0>>

  for id <- 1..rows_per_topic do
    compound_key = prefix <> "#{id}-0"
    value = :erlang.term_to_binary(["field", "value"])
    :ets.insert(index, {{prefix, {id, 0}}, compound_key})
    :ets.insert(keydir, {compound_key, value, 0, 0, 0, 0, byte_size(value)})
  end
end

target_prefix =
  "X:bench:topic:#{String.pad_leading(Integer.to_string(topic_count), 6, "0")}:#{run_id}" <>
    <<0>>

walk = fn walk, lookup, remaining, acc ->
  case {lookup, remaining} do
    {_lookup, 0} ->
      Enum.reverse(acc)

    {{{^target_prefix, _id} = index_key, [{index_key, compound_key}]}, remaining} ->
      value = :ets.lookup_element(keydir, compound_key, 2)

      id =
        binary_part(
          compound_key,
          byte_size(target_prefix),
          byte_size(compound_key) - byte_size(target_prefix)
        )

      walk.(walk, :ets.next_lookup(index, index_key), remaining - 1, [{id, value} | acc])

    _other ->
      Enum.reverse(acc)
  end
end

ordered_walk = fn ->
  walk.(walk, :ets.next_lookup(index, {target_prefix, {-1, -1}}), rows_per_topic, [])
end

true =
  CompoundMemberIndex.hot_stream_page_once(index, keydir, target_prefix, 0, rows_per_topic) ==
    {:ok, ordered_walk.()}

Benchee.run(
  %{
    "bounded ETS select at last topic" => fn ->
      CompoundMemberIndex.hot_stream_page_once(index, keydir, target_prefix, 0, rows_per_topic)
    end,
    "prefix-seeking ordered walk at last topic" => ordered_walk
  },
  warmup: 2,
  time: 4,
  memory_time: 2,
  reduction_time: 2,
  print: [benchmarking: false, configuration: false]
)
