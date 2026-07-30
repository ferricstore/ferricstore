# FerricStore partitioned Stream producer benchmark.
#
# Run:
#   BENCH_STREAM_PARTITIONS=12 BENCH_STREAM_BATCH=64 \
#     MIX_ENV=bench mix run --no-start bench/stream_partition_bench.exs
#
# This compares concurrent producer batches targeting one ordered Stream with:
#
#   * one whole batch per partitioned Stream, as independent topic producers do;
#   * one batch spread across distinct-shard Streams, as a multi-topic producer does;
#   * the same mixed-topic shape with every Stream pinned to one shard.
#
# Every mode uses xadd_many/1. The mixed mode groups one submitted batch into one
# ordered replicated entry per owning shard and then restores global result order.

Logger.configure(level: :warning)

env_integer = fn name, default ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value > 0 -> value
    _invalid -> raise ArgumentError, "#{name} must be a positive integer"
  end
end

env_non_negative_integer = fn name, default ->
  case Integer.parse(System.get_env(name, default)) do
    {value, ""} when value >= 0 -> value
    _invalid -> raise ArgumentError, "#{name} must be a non-negative integer"
  end
end

env_boolean = fn name, default ->
  case String.downcase(System.get_env(name, default)) do
    value when value in ["1", "true", "yes", "on"] -> true
    value when value in ["0", "false", "no", "off"] -> false
    _invalid -> raise ArgumentError, "#{name} must be a boolean"
  end
end

batch_size = env_integer.("BENCH_STREAM_BATCH", "64")
batch_count = env_integer.("BENCH_STREAM_BATCHES", "64")
concurrency = env_integer.("BENCH_CONCURRENCY", "32")
sample_count = env_integer.("BENCH_SAMPLES", "7")
trace_stream? = env_boolean.("BENCH_STREAM_TRACE", "false")

activity_log_max_len =
  env_non_negative_integer.("BENCH_STREAM_ACTIVITY_LOG_MAX_LEN", "512")

run_id = "#{System.os_time(:microsecond)}_#{System.unique_integer([:positive, :monotonic])}"

data_dir =
  Path.join(
    System.tmp_dir!(),
    "ferricstore_stream_partition_bench_#{run_id}"
  )

File.mkdir_p!(data_dir)
Application.put_env(:ferricstore, :data_dir, data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :stream_activity_log_max_len, activity_log_max_len)

{:ok, _started} = Application.ensure_all_started(:ferricstore)

ctx = FerricStore.Instance.get(:default)
partition_count = min(env_integer.("BENCH_STREAM_PARTITIONS", "12"), ctx.shard_count)

same_shard_topic_count =
  env_integer.("BENCH_STREAM_TOPICS", Integer.to_string(partition_count))

suffix = System.unique_integer([:positive, :monotonic])

partition_keys =
  Stream.iterate(0, &(&1 + 1))
  |> Enum.reduce_while(%{}, fn candidate, keys ->
    key = "bench:superstream:#{suffix}:partition-candidate:#{candidate}"
    shard = Ferricstore.Store.Router.shard_for(ctx, key)
    keys = Map.put_new(keys, shard, key)

    if map_size(keys) == partition_count do
      {:halt, keys}
    else
      {:cont, keys}
    end
  end)
  |> Enum.sort_by(&elem(&1, 0))
  |> Enum.map(&elem(&1, 1))

true = length(partition_keys) == partition_count
hot_key = "bench:superstream:#{suffix}:hot"
hot_shard = Ferricstore.Store.Router.shard_for(ctx, hot_key)

same_shard_keys =
  Stream.iterate(0, &(&1 + 1))
  |> Stream.map(&"bench:superstream:#{suffix}:same-shard-candidate:#{&1}")
  |> Stream.filter(&(Ferricstore.Store.Router.shard_for(ctx, &1) == hot_shard))
  |> Enum.take(same_shard_topic_count)

true = length(same_shard_keys) == same_shard_topic_count
total_entries = batch_size * batch_count

median = fn values ->
  values
  |> Enum.sort()
  |> Enum.at(div(length(values), 2))
end

run_workload = fn items_for_batch ->
  started_at = System.monotonic_time()

  results =
    0..(batch_count - 1)
    |> Task.async_stream(
      fn batch_index ->
        items = items_for_batch.(batch_index)

        FerricStore.xadd_many(items)
      end,
      max_concurrency: concurrency,
      ordered: false,
      timeout: 60_000
    )
    |> Enum.to_list()

  if length(results) != batch_count do
    raise "Stream partition benchmark lost task results"
  end

  case Enum.find(results, fn
         {:ok, batch_results} when length(batch_results) == batch_size ->
           not Enum.all?(batch_results, &match?({:ok, id} when is_binary(id), &1))

         _failure ->
           true
       end) do
    nil -> :ok
    failure -> raise "Stream partition benchmark append failed: #{inspect(failure, limit: 10)}"
  end

  System.monotonic_time() - started_at
end

batch_items = fn batch_index, key_for_item ->
  Enum.map(1..batch_size, fn item_index ->
    {key_for_item.(item_index),
     [
       "batch",
       Integer.to_string(batch_index),
       "item",
       Integer.to_string(item_index)
     ]}
  end)
end

hot_samples = []
partitioned_samples = []
mixed_samples = []
same_shard_mixed_samples = []

{hot_samples, partitioned_samples, mixed_samples, same_shard_mixed_samples} =
  Enum.reduce(
    1..sample_count,
    {hot_samples, partitioned_samples, mixed_samples, same_shard_mixed_samples},
    fn _sample, {hot, partitioned, mixed, same_shard_mixed} ->
      hot_elapsed =
        run_workload.(fn batch_index ->
          batch_items.(batch_index, fn _item_index -> hot_key end)
        end)

      partitioned_elapsed =
        run_workload.(fn batch_index ->
          key = Enum.at(partition_keys, rem(batch_index, partition_count))
          batch_items.(batch_index, fn _item_index -> key end)
        end)

      mixed_elapsed =
        run_workload.(fn batch_index ->
          batch_items.(batch_index, fn item_index ->
            Enum.at(
              partition_keys,
              rem(batch_index * batch_size + item_index - 1, partition_count)
            )
          end)
        end)

      same_shard_mixed_elapsed =
        run_workload.(fn batch_index ->
          batch_items.(batch_index, fn item_index ->
            Enum.at(
              same_shard_keys,
              rem(batch_index * batch_size + item_index - 1, same_shard_topic_count)
            )
          end)
        end)

      {
        [hot_elapsed | hot],
        [partitioned_elapsed | partitioned],
        [mixed_elapsed | mixed],
        [same_shard_mixed_elapsed | same_shard_mixed]
      }
    end
  )

hot_median = median.(hot_samples)
partitioned_median = median.(partitioned_samples)
mixed_median = median.(mixed_samples)
same_shard_mixed_median = median.(same_shard_mixed_samples)
hot_seconds = System.convert_time_unit(hot_median, :native, :microsecond) / 1_000_000

partitioned_seconds =
  System.convert_time_unit(partitioned_median, :native, :microsecond) / 1_000_000

mixed_seconds = System.convert_time_unit(mixed_median, :native, :microsecond) / 1_000_000

same_shard_mixed_seconds =
  System.convert_time_unit(same_shard_mixed_median, :native, :microsecond) / 1_000_000

expected_hot = total_entries * sample_count
{:ok, ^expected_hot} = FerricStore.xlen(hot_key)

partitioned_lengths =
  Enum.map(partition_keys, fn key ->
    {:ok, len} = FerricStore.xlen(key)
    len
  end)

expected_partitioned = total_entries * sample_count * 2
true = Enum.sum(partitioned_lengths) == expected_partitioned

same_shard_lengths =
  Enum.map(same_shard_keys, fn key ->
    {:ok, len} = FerricStore.xlen(key)
    len
  end)

expected_same_shard = total_entries * sample_count
true = Enum.sum(same_shard_lengths) == expected_same_shard

IO.puts("=== FerricStore Partitioned Stream Producer Benchmark ===")

IO.puts(
  "partitions=#{partition_count} batch_size=#{batch_size} batches=#{batch_count} " <>
    "same_shard_topics=#{same_shard_topic_count} concurrency=#{concurrency} " <>
    "samples=#{sample_count} entries_per_sample=#{total_entries} " <>
    "activity_log_max_len=#{activity_log_max_len}"
)

IO.puts(
  "single_stream_median_seconds=#{Float.round(hot_seconds, 4)} " <>
    "entries_per_second=#{Float.round(total_entries / hot_seconds, 1)}"
)

IO.puts(
  "partitioned_stream_median_seconds=#{Float.round(partitioned_seconds, 4)} " <>
    "entries_per_second=#{Float.round(total_entries / partitioned_seconds, 1)} " <>
    "speedup=#{Float.round(hot_seconds / partitioned_seconds, 2)}x"
)

IO.puts(
  "mixed_stream_median_seconds=#{Float.round(mixed_seconds, 4)} " <>
    "entries_per_second=#{Float.round(total_entries / mixed_seconds, 1)} " <>
    "speedup=#{Float.round(hot_seconds / mixed_seconds, 2)}x"
)

IO.puts(
  "same_shard_mixed_stream_median_seconds=#{Float.round(same_shard_mixed_seconds, 4)} " <>
    "entries_per_second=#{Float.round(total_entries / same_shard_mixed_seconds, 1)} " <>
    "speedup=#{Float.round(hot_seconds / same_shard_mixed_seconds, 2)}x"
)

IO.puts(
  "correctness=ok single_stream_entries=#{expected_hot} " <>
    "partitioned_entries=#{Enum.sum(partitioned_lengths)} " <>
    "same_shard_entries=#{Enum.sum(same_shard_lengths)}"
)

if trace_stream? do
  trace_batch = fn label, keys ->
    items =
      batch_items.(sample_count + 1, fn item_index ->
        Enum.at(keys, rem(item_index - 1, length(keys)))
      end)

    previous_trace = Ferricstore.LatencyTrace.start(%{})
    started_at = System.monotonic_time(:microsecond)

    try do
      results = FerricStore.xadd_many(items)
      true = Enum.all?(results, &match?({:ok, id} when is_binary(id), &1))

      IO.puts(
        "trace topology=#{label} total_request_us=#{System.monotonic_time(:microsecond) - started_at}"
      )

      previous_trace
      |> Ferricstore.LatencyTrace.finish()
      |> Enum.sort()
      |> Enum.each(fn {name, duration_us} ->
        IO.puts("trace topology=#{label} #{name}=#{duration_us}")
      end)
    after
      if Ferricstore.LatencyTrace.enabled?() do
        _ = Ferricstore.LatencyTrace.finish(previous_trace)
      end
    end
  end

  trace_batch.("distinct_shards", partition_keys)
  trace_batch.("same_shard", same_shard_keys)
end

_ = Application.stop(:ferricstore)
File.rm_rf!(data_dir)
