# Targeted Flow tail-latency probe for Flow hot commands.
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 FLOW_LMDB_MODE=mirror \
#     FLOW_TAIL_BACKLOG=100000 FLOW_TAIL_ITER=400 mix run --no-start bench/flow_tail_probe.exs

backlog = System.get_env("FLOW_TAIL_BACKLOG", "100000") |> String.to_integer()
iterations = System.get_env("FLOW_TAIL_ITER", "400") |> String.to_integer()
shard_count = System.get_env("FLOW_TAIL_SHARDS", "4") |> String.to_integer()
partition_count = System.get_env("FLOW_TAIL_PARTITIONS", "4") |> String.to_integer()
seed_concurrency = System.get_env("FLOW_TAIL_SEED_CONCURRENCY", "64") |> String.to_integer()
batch_size = System.get_env("FLOW_TAIL_BATCH", "100") |> String.to_integer()
top_count = System.get_env("FLOW_TAIL_TOP", "8") |> String.to_integer()
flow_lmdb_mode = System.get_env("FLOW_LMDB_MODE", "mirror")

release_cursor_interval = System.get_env("FLOW_TAIL_RELEASE_CURSOR_INTERVAL")

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_tail_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)
Application.put_env(:ferricstore, :flow_lmdb_mode, flow_lmdb_mode)

if release_cursor_interval not in [nil, ""] do
  Application.put_env(
    :ferricstore,
    :release_cursor_interval,
    String.to_integer(release_cursor_interval)
  )
end

{:ok, _} = Application.ensure_all_started(:ferricstore)

defmodule FlowTailProbe do
  @events [
    [:ferricstore, :raft, :apply],
    [:ferricstore, :raft, :replay_safe_index, :persist],
    [:ferricstore, :bitcask, :append],
    [:ferricstore, :batcher, :slot_flush],
    [:ferricstore, :batcher, :quorum_submit]
  ]

  def run(backlog, iterations, partition_count, seed_concurrency, batch_size, top_count, data_dir) do
    :ets.new(:flow_tail_events, [:named_table, :public, :ordered_set])
    :ets.new(:flow_tail_samples, [:named_table, :public, :ordered_set])
    attach_telemetry()

    prefix = "flowtail:" <> Integer.to_string(System.unique_integer([:positive]))
    active_type = type(prefix, "active")

    IO.puts("=== Flow Tail Probe ===")
    IO.puts("data_dir=#{data_dir}")

    IO.puts(
      "backlog=#{backlog} iterations=#{iterations} partitions=#{partition_count} batch=#{batch_size}"
    )

    timed("seed active #{backlog}", fn ->
      seed_active(prefix, active_type, backlog, partition_count, seed_concurrency)
    end)

    warm_info(prefix, active_type, partition_count)
    clear_events()

    claim_type = type(prefix, "claim")

    seed_claim_flows(
      prefix,
      claim_type,
      iterations * batch_size,
      partition_count,
      seed_concurrency
    )

    probe("claim_due", iterations, top_count, fn i ->
      {:ok, claimed} =
        FerricStore.flow_claim_due(claim_type,
          worker: "worker-claim",
          lease_ms: 30_000,
          limit: batch_size,
          now_ms: 1_000,
          partition_key: partition(prefix, i, partition_count)
        )

      claimed
    end)

    create_type = type(prefix, "create")

    probe("create", iterations, top_count, fn i ->
      flow_id = id(prefix, "create", i)

      {:ok, flow} =
        FerricStore.flow_create(flow_id,
          type: create_type,
          state: "queued",
          payload_ref: "payload:" <> flow_id,
          run_at_ms: 1_000,
          now_ms: 1_000,
          partition_key: partition(prefix, i, partition_count)
        )

      flow
    end)

    create_many_type = type(prefix, "create_many")

    probe("create_many", iterations, top_count, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        for j <- 1..batch_size do
          %{id: id(prefix, "create_many", i, j)}
        end

      {:ok, flows} =
        FerricStore.flow_create_many(partition_key, items,
          type: create_many_type,
          state: "queued",
          run_at_ms: 1_000,
          now_ms: 1_000
        )

      flows
    end)

    transition_ids = seed_transition_flows(prefix, iterations, partition_count)

    probe("transition", iterations, top_count, fn i ->
      {flow_id, partition_key} = Map.fetch!(transition_ids, i)

      {:ok, flow} =
        FerricStore.flow_transition(flow_id, "queued", "waiting",
          fencing_token: 0,
          run_at_ms: 2_000,
          now_ms: 2_000,
          partition_key: partition_key
        )

      flow
    end)

    transition_many_ids =
      seed_transition_many_flows(prefix, iterations, batch_size, partition_count)

    probe("transition_many", iterations, top_count, fn i ->
      partition_key = partition(prefix, i, partition_count)
      ids = Map.fetch!(transition_many_ids, i)

      items =
        Enum.map(ids, fn flow_id ->
          %{id: flow_id, partition_key: partition_key, fencing_token: 0}
        end)

      {:ok, flows} =
        FerricStore.flow_transition_many(partition_key, "queued", "waiting", items,
          run_at_ms: 2_000,
          now_ms: 2_000
        )

      flows
    end)

    complete_ids = seed_claimed_flows(prefix, "complete", iterations, partition_count)

    probe("complete", iterations, top_count, fn i ->
      {flow_id, partition_key, lease_token, fencing_token} = Map.fetch!(complete_ids, i)

      {:ok, flow} =
        FerricStore.flow_complete(flow_id, lease_token,
          fencing_token: fencing_token,
          result_ref: "result:" <> flow_id,
          now_ms: 2_000,
          partition_key: partition_key
        )

      flow
    end)

    fail_ids = seed_claimed_flows(prefix, "fail", iterations, partition_count)

    probe("fail", iterations, top_count, fn i ->
      {flow_id, partition_key, lease_token, fencing_token} = Map.fetch!(fail_ids, i)

      {:ok, flow} =
        FerricStore.flow_fail(flow_id, lease_token,
          fencing_token: fencing_token,
          error_ref: "error:" <> flow_id,
          now_ms: 2_000,
          partition_key: partition_key
        )

      flow
    end)

    cancel_ids = seed_transition_flows(prefix, iterations, partition_count, "cancel")

    probe("cancel", iterations, top_count, fn i ->
      {flow_id, partition_key} = Map.fetch!(cancel_ids, i)

      {:ok, flow} =
        FerricStore.flow_cancel(flow_id,
          fencing_token: 0,
          now_ms: 2_000,
          partition_key: partition_key
        )

      flow
    end)

    retry_ids = seed_retry_flows(prefix, iterations, partition_count)

    probe("retry", iterations, top_count, fn i ->
      {flow_id, partition_key, lease_token, fencing_token} = Map.fetch!(retry_ids, i)

      {:ok, flow} =
        FerricStore.flow_retry(flow_id, lease_token,
          fencing_token: fencing_token,
          run_at_ms: 2_000,
          now_ms: 2_000,
          partition_key: partition_key
        )

      flow
    end)

    probe("info", iterations, top_count, fn i ->
      {:ok, info} =
        FerricStore.flow_info(active_type,
          partition_key: partition(prefix, i, partition_count)
        )

      info
    end)

    :telemetry.detach(__MODULE__)
  after
    File.rm_rf(data_dir)
  end

  defp attach_telemetry do
    :telemetry.attach_many(
      __MODULE__,
      @events,
      fn event, measurements, metadata, _config ->
        id = System.unique_integer([:monotonic, :positive])
        now_us = System.monotonic_time(:microsecond)
        :ets.insert(:flow_tail_events, {id, now_us, event, measurements, metadata})
      end,
      nil
    )
  end

  defp probe(name, iterations, top_count, fun) do
    clear_events()

    samples =
      for i <- 1..iterations do
        gc_before = :erlang.statistics(:garbage_collection)
        start_us = System.monotonic_time(:microsecond)
        {duration_us, _result} = :timer.tc(fn -> fun.(i) end)
        stop_us = System.monotonic_time(:microsecond)
        gc_after = :erlang.statistics(:garbage_collection)
        sample = {name, i, duration_us, start_us, stop_us, gc_delta(gc_before, gc_after)}
        :ets.insert(:flow_tail_samples, {{name, i}, sample})
        sample
      end

    print_stats(name, samples)
    if top_count > 0, do: print_top_samples(name, samples, top_count)
    print_events_summary(name)
  end

  defp print_stats(name, samples) do
    latencies =
      Enum.map(samples, fn {_name, _i, duration_us, _start, _stop, _gc} -> duration_us end)

    sorted = Enum.sort(latencies)
    avg = Enum.sum(sorted) / length(sorted)

    IO.puts(
      "#{name}: avg=#{round(avg)}us p50=#{percentile(sorted, 0.50)}us p95=#{percentile(sorted, 0.95)}us p99=#{percentile(sorted, 0.99)}us max=#{List.last(sorted)}us"
    )
  end

  defp print_top_samples(name, samples, count) do
    IO.puts("\nTop #{count} #{name} samples:")

    samples
    |> Enum.sort_by(fn {_name, _i, duration_us, _start, _stop, _gc} -> -duration_us end)
    |> Enum.take(count)
    |> Enum.each(fn {_name, i, duration_us, start_us, stop_us, gc_delta} ->
      IO.puts("- i=#{i} duration=#{duration_us}us gc_delta=#{inspect(gc_delta)}")
      print_nearby_events(start_us, stop_us)
    end)

    IO.puts("")
  end

  defp print_nearby_events(start_us, stop_us) do
    margin_us = 5_000

    :ets.tab2list(:flow_tail_events)
    |> Enum.filter(fn {_id, ts, _event, _measurements, _metadata} ->
      ts >= start_us - margin_us and ts <= stop_us + margin_us
    end)
    |> Enum.sort_by(fn {_id, ts, _event, _measurements, _metadata} -> ts end)
    |> Enum.each(fn {_id, ts, event, measurements, metadata} ->
      offset =
        cond do
          ts < start_us -> ts - start_us
          ts > stop_us -> ts - stop_us
          true -> ts - start_us
        end

      IO.puts(
        "  event +#{offset}us #{inspect(event)} #{inspect(trim(measurements))} #{inspect(trim(metadata))}"
      )
    end)
  end

  defp print_events_summary(name) do
    IO.puts("Event max durations for #{name}:")

    :ets.tab2list(:flow_tail_events)
    |> Enum.group_by(fn {_id, _ts, event, _measurements, _metadata} -> event end)
    |> Enum.each(fn {event, rows} ->
      durations =
        rows
        |> Enum.map(fn {_id, _ts, _event, measurements, _metadata} ->
          Map.get(measurements, :duration_us, 0)
        end)
        |> Enum.sort()

      if durations != [] do
        IO.puts(
          "- #{inspect(event)} count=#{length(durations)} p99=#{percentile(durations, 0.99)}us max=#{List.last(durations)}us"
        )
      end
    end)
  end

  defp clear_events do
    :ets.delete_all_objects(:flow_tail_events)
  end

  defp warm_info(prefix, active_type, partition_count) do
    for i <- 1..partition_count do
      FerricStore.flow_info(active_type, partition_key: partition(prefix, i, partition_count))
    end
  end

  defp seed_active(prefix, flow_type, backlog, partition_count, seed_concurrency) do
    progress = :atomics.new(1, signed: false)

    1..backlog
    |> Task.async_stream(
      fn i ->
        {:ok, _} =
          FerricStore.flow_create(id(prefix, "active", i),
            type: flow_type,
            payload_ref: "payload:" <> id(prefix, "active", i),
            run_at_ms: 1_000,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        count = :atomics.add_get(progress, 1, 1)
        if rem(count, 10_000) == 0, do: IO.puts("seeded #{count}/#{backlog}")
      end,
      max_concurrency: seed_concurrency,
      ordered: false,
      timeout: :infinity
    )
    |> Enum.each(fn
      {:ok, _} -> :ok
      {:exit, reason} -> raise "seed task failed: #{inspect(reason)}"
    end)
  end

  defp seed_claim_flows(prefix, flow_type, total, partition_count, seed_concurrency) do
    timed("seed claim_due #{total}", fn ->
      progress = :atomics.new(1, signed: false)

      1..total
      |> Task.async_stream(
        fn i ->
          {:ok, _} =
            FerricStore.flow_create(id(prefix, "claim", i),
              type: flow_type,
              payload_ref: "payload:" <> id(prefix, "claim", i),
              run_at_ms: 1_000,
              now_ms: 1_000,
              partition_key: partition(prefix, i, partition_count)
            )

          count = :atomics.add_get(progress, 1, 1)
          if rem(count, 10_000) == 0, do: IO.puts("seeded claim #{count}/#{total}")
        end,
        max_concurrency: seed_concurrency,
        ordered: false,
        timeout: :infinity
      )
      |> Enum.each(fn
        {:ok, _} -> :ok
        {:exit, reason} -> raise "seed claim task failed: #{inspect(reason)}"
      end)
    end)
  end

  defp seed_retry_flows(prefix, iterations, partition_count) do
    seed_claimed_flows(prefix, "retry", iterations, partition_count)
  end

  defp seed_claimed_flows(prefix, group, iterations, partition_count) do
    flow_type = type(prefix, group)

    for i <- 1..iterations, into: %{} do
      flow_id = id(prefix, group, i)
      partition_key = partition(prefix, i, partition_count)

      {:ok, _} =
        FerricStore.flow_create(flow_id,
          type: flow_type,
          payload_ref: "payload:" <> flow_id,
          run_at_ms: 1_000,
          now_ms: 1_000,
          partition_key: partition_key
        )

      {:ok, [claimed]} =
        FerricStore.flow_claim_due(flow_type,
          worker: "worker-" <> group,
          lease_ms: 30_000,
          limit: 1,
          now_ms: 1_000,
          partition_key: partition_key
        )

      {i, {flow_id, partition_key, claimed.lease_token, claimed.fencing_token}}
    end
  end

  defp seed_transition_flows(prefix, iterations, partition_count, group \\ "transition") do
    flow_type = type(prefix, group)

    for i <- 1..iterations, into: %{} do
      flow_id = id(prefix, group, i)
      partition_key = partition(prefix, i, partition_count)

      {:ok, _} =
        FerricStore.flow_create(flow_id,
          type: flow_type,
          payload_ref: "payload:" <> flow_id,
          run_at_ms: 1_000,
          now_ms: 1_000,
          partition_key: partition_key
        )

      {i, {flow_id, partition_key}}
    end
  end

  defp seed_transition_many_flows(prefix, iterations, batch_size, partition_count) do
    flow_type = type(prefix, "transition_many")

    for i <- 1..iterations, into: %{} do
      partition_key = partition(prefix, i, partition_count)

      ids =
        for j <- 1..batch_size do
          flow_id = id(prefix, "transition_many", i, j)

          {:ok, _} =
            FerricStore.flow_create(flow_id,
              type: flow_type,
              payload_ref: "payload:" <> flow_id,
              run_at_ms: 1_000,
              now_ms: 1_000,
              partition_key: partition_key
            )

          flow_id
        end

      {i, ids}
    end
  end

  defp gc_delta({count_a, words_a, _}, {count_b, words_b, _}),
    do: %{count: count_b - count_a, words: words_b - words_a}

  defp percentile(sorted, q) do
    index =
      sorted
      |> length()
      |> Kernel.*(q)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> min(length(sorted))

    Enum.at(sorted, index - 1)
  end

  defp timed(label, fun) do
    {ms, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(ms / 1000, 2)} ms")
    result
  end

  defp id(prefix, group, i), do: prefix <> ":" <> group <> ":" <> Integer.to_string(i)
  defp id(prefix, group, i, j), do: id(prefix, group, i) <> ":" <> Integer.to_string(j)
  defp type(prefix, suffix), do: prefix <> ":" <> suffix

  defp partition(prefix, i, partition_count) do
    prefix <> ":tenant:" <> Integer.to_string(rem(i - 1, partition_count))
  end

  defp trim(map) when is_map(map) do
    map
    |> Enum.take(6)
    |> Map.new()
  end

  defp trim(other), do: other
end

FlowTailProbe.run(
  backlog,
  iterations,
  partition_count,
  seed_concurrency,
  batch_size,
  top_count,
  bench_data_dir
)
