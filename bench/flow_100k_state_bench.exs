# bench/flow_100k_state_bench.exs
#
# Baseline public Flow command latency with a large active state set.
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 mix run --no-start bench/flow_100k_state_bench.exs
#
# Options:
#   FLOW_100K_BACKLOG=100000
#   FLOW_100K_ITER=200
#   FLOW_100K_SHARDS=4
#   FLOW_100K_PARTITIONS=4
#   FLOW_100K_SEED_CONCURRENCY=32
#   FLOW_100K_CLAIM_LIMITS=10,100
#   FLOW_100K_CREATE_MANY_BATCH=100
#   FLOW_100K_TERMINAL_MANY_BATCH=100

backlog = System.get_env("FLOW_100K_BACKLOG", "100000") |> String.to_integer()
iterations = System.get_env("FLOW_100K_ITER", "200") |> String.to_integer()
shard_count = System.get_env("FLOW_100K_SHARDS", "4") |> String.to_integer()
partition_count = System.get_env("FLOW_100K_PARTITIONS", "4") |> String.to_integer()
seed_concurrency = System.get_env("FLOW_100K_SEED_CONCURRENCY", "32") |> String.to_integer()
create_many_batch = System.get_env("FLOW_100K_CREATE_MANY_BATCH", "100") |> String.to_integer()

transition_many_batch =
  System.get_env("FLOW_100K_TRANSITION_MANY_BATCH", "100") |> String.to_integer()

terminal_many_batch =
  System.get_env("FLOW_100K_TERMINAL_MANY_BATCH", "100") |> String.to_integer()

claim_limits =
  System.get_env("FLOW_100K_CLAIM_LIMITS", "10,100")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_100k_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

defmodule Flow100kStateBench do
  def run(
        backlog,
        iterations,
        shard_count,
        partition_count,
        seed_concurrency,
        create_many_batch,
        transition_many_batch,
        terminal_many_batch,
        claim_limits,
        bench_data_dir
      ) do
    started_at = DateTime.utc_now() |> DateTime.to_iso8601()
    prefix = "flow100k:" <> Integer.to_string(System.unique_integer([:positive]))
    flow_type = type(prefix, "active")

    IO.puts("=== FerricStore Flow 100k State Baseline ===")
    IO.puts("data_dir=#{bench_data_dir}")

    IO.puts(
      "backlog=#{backlog} iterations=#{iterations} shards=#{shard_count} partitions=#{partition_count} claim_limits=#{Enum.join(claim_limits, ",")}"
    )

    IO.puts(
      "seed_concurrency=#{seed_concurrency} create_many_batch=#{create_many_batch} transition_many_batch=#{transition_many_batch} terminal_many_batch=#{terminal_many_batch} flow_lmdb_projection=lagged"
    )

    memory_before = :erlang.memory(:total)

    timed("seed active queued backlog #{backlog}", fn ->
      seed_active(prefix, flow_type, backlog, partition_count, seed_concurrency)
    end)

    memory_after_seed = :erlang.memory(:total)

    IO.puts(
      "beam_memory_before=#{memory_before} beam_memory_after_seed=#{memory_after_seed} delta=#{memory_after_seed - memory_before}\n"
    )

    results =
      []
      |> add(
        result("flow.create under #{backlog}", iterations, fn i ->
          {:ok, flow} =
            FerricStore.flow_create(id(prefix, "create", i),
              type: type(prefix, "create"),
              payload: "payload:" <> id(prefix, "create", i),
              run_at_ms: 1_000,
              now_ms: 1_000,
              partition_key: partition(prefix, i, partition_count)
            )

          flow
        end)
      )
      |> add(bench_create_many(prefix, backlog, iterations, partition_count, create_many_batch))
      |> add(
        result("flow.get from #{backlog}", iterations, fn i ->
          index = sample_index(i, backlog)

          {:ok, flow} =
            FerricStore.flow_get(id(prefix, "active", index),
              partition_key: partition(prefix, index, partition_count)
            )

          flow
        end)
      )
      |> add(
        result("flow.list count=100 from #{backlog}", iterations, fn i ->
          {:ok, records} =
            FerricStore.flow_list(flow_type,
              state: "queued",
              count: 100,
              partition_key: partition(prefix, i, partition_count)
            )

          records
        end)
      )
      |> add(
        result("flow.info over #{backlog}", iterations, fn i ->
          {:ok, info} =
            FerricStore.flow_info(flow_type,
              partition_key: partition(prefix, i, partition_count)
            )

          info
        end)
      )
      |> add_many(bench_history(prefix, backlog, iterations, partition_count, shard_count))
      |> add(bench_stuck(prefix, backlog, iterations, partition_count))
      |> add_many(bench_claim_due(prefix, backlog, iterations, partition_count, claim_limits))
      |> add(bench_transition(prefix, backlog, iterations, partition_count))
      |> add(
        bench_transition_many(prefix, backlog, iterations, partition_count, transition_many_batch)
      )
      |> add(bench_complete(prefix, backlog, iterations, partition_count))
      |> add(
        bench_complete_many(prefix, backlog, iterations, partition_count, terminal_many_batch)
      )
      |> add(bench_retry(prefix, backlog, iterations, partition_count))
      |> add(bench_retry_many(prefix, backlog, iterations, partition_count, terminal_many_batch))
      |> add(bench_fail(prefix, backlog, iterations, partition_count))
      |> add(bench_fail_many(prefix, backlog, iterations, partition_count, terminal_many_batch))
      |> add(bench_cancel(prefix, backlog, iterations, partition_count))
      |> add(bench_cancel_many(prefix, backlog, iterations, partition_count, terminal_many_batch))
      |> add(bench_rewind(prefix, backlog, iterations, partition_count))
      |> Enum.reverse()

    print_table(results)

    write_results(started_at, results, %{
      backlog: backlog,
      iterations: iterations,
      shards: shard_count,
      partitions: partition_count,
      seed_concurrency: seed_concurrency,
      create_many_batch: create_many_batch,
      transition_many_batch: transition_many_batch,
      terminal_many_batch: terminal_many_batch,
      claim_limits: claim_limits,
      memory_before: memory_before,
      memory_after_seed: memory_after_seed,
      memory_delta: memory_after_seed - memory_before
    })
  end

  defp seed_active(prefix, flow_type, backlog, partition_count, seed_concurrency) do
    progress = :atomics.new(1, signed: false)

    1..backlog
    |> Task.async_stream(
      fn i ->
        {:ok, _} =
          FerricStore.flow_create(id(prefix, "active", i),
            type: flow_type,
            payload: "payload:" <> id(prefix, "active", i),
            run_at_ms: 1_000,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        count = :atomics.add_get(progress, 1, 1)

        if rem(count, 10_000) == 0 do
          IO.puts("seeded #{count}/#{backlog}")
        end
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

  defp bench_create_many(prefix, backlog, iterations, partition_count, batch_size) do
    flow_type = type(prefix, "create_many")

    result("flow.create_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        for j <- 1..batch_size do
          %{id: id(prefix, "create_many", i, j)}
        end

      {:ok, flows} =
        FerricStore.flow_create_many(partition_key, items,
          type: flow_type,
          state: "queued",
          run_at_ms: 1_000,
          now_ms: 1_000
        )

      flows
    end)
  end

  defp bench_history(prefix, backlog, iterations, partition_count, shard_count) do
    flow_type = type(prefix, "history")

    timed("seed history #{iterations}", fn ->
      for i <- 1..iterations do
        flow = create_claim_complete(prefix, "history", flow_type, i, partition_count)
        true = flow.state == "completed"
      end
    end)

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(shard_count)

    [
      result("flow.history count=10 under #{backlog}", iterations, fn i ->
        {:ok, events} =
          FerricStore.flow_history(id(prefix, "history", i),
            count: 10,
            partition_key: partition(prefix, i, partition_count)
          )

        events
      end),
      result("flow.history include_cold count=10 under #{backlog}", iterations, fn i ->
        {:ok, events} =
          FerricStore.flow_history(id(prefix, "history", i),
            count: 10,
            include_cold: true,
            partition_key: partition(prefix, i, partition_count)
          )

        events
      end),
      result("flow.history cold_consistent count=10 under #{backlog}", iterations, fn i ->
        {:ok, events} =
          FerricStore.flow_history(id(prefix, "history", i),
            count: 10,
            include_cold: true,
            consistent_projection: true,
            partition_key: partition(prefix, i, partition_count)
          )

        events
      end)
    ]
  end

  defp bench_stuck(prefix, backlog, iterations, partition_count) do
    flow_type = type(prefix, "stuck")

    timed("seed stuck #{iterations}", fn ->
      for i <- 1..iterations do
        {:ok, _} =
          FerricStore.flow_create(id(prefix, "stuck", i),
            type: flow_type,
            payload: "payload:" <> id(prefix, "stuck", i),
            run_at_ms: 1_000,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        {:ok, [_claimed]} =
          FerricStore.flow_claim_due(flow_type,
            worker: "worker-stuck",
            lease_ms: 1,
            limit: 1,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )
      end
    end)

    result("flow.stuck count=100 under #{backlog}", iterations, fn i ->
      {:ok, records} =
        FerricStore.flow_stuck(flow_type,
          count: 100,
          older_than_ms: 0,
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      records
    end)
  end

  defp bench_claim_due(prefix, backlog, iterations, partition_count, claim_limits) do
    Enum.map(claim_limits, fn limit ->
      flow_type = type(prefix, "claim#{limit}")
      total = iterations * limit

      timed("seed claim_due limit=#{limit} total=#{total}", fn ->
        for i <- 1..total do
          {:ok, _} =
            FerricStore.flow_create(id(prefix, "claim#{limit}", i),
              type: flow_type,
              payload: "payload:" <> id(prefix, "claim#{limit}", i),
              run_at_ms: 1_000,
              now_ms: 1_000,
              partition_key: partition(prefix, i, partition_count)
            )
        end
      end)

      result("flow.claim_due limit=#{limit} from #{backlog}", iterations, fn i ->
        {:ok, claimed} =
          FerricStore.flow_claim_due(flow_type,
            worker: "worker-claim",
            lease_ms: 30_000,
            limit: limit,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        claimed
      end)
    end)
  end

  defp bench_transition(prefix, backlog, iterations, partition_count) do
    flow_type = type(prefix, "transition")

    timed("seed transition #{iterations}", fn ->
      for i <- 1..iterations do
        create_flow(prefix, "transition", flow_type, i, partition_count)
      end
    end)

    result("flow.transition under #{backlog}", iterations, fn i ->
      {:ok, transitioned} =
        FerricStore.flow_transition(id(prefix, "transition", i), "queued", "waiting",
          fencing_token: 0,
          run_at_ms: 2_000,
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      transitioned
    end)
  end

  defp bench_transition_many(prefix, backlog, iterations, partition_count, batch_size) do
    flow_type = type(prefix, "transition_many")

    timed("seed transition_many #{iterations}x#{batch_size}", fn ->
      for i <- 1..iterations do
        partition_key = partition(prefix, i, partition_count)

        items =
          for j <- 1..batch_size do
            %{id: id(prefix, "transition_many", i, j)}
          end

        {:ok, _flows} =
          FerricStore.flow_create_many(partition_key, items,
            type: flow_type,
            state: "queued",
            run_at_ms: 1_000,
            now_ms: 1_000
          )
      end
    end)

    result("flow.transition_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        for j <- 1..batch_size do
          %{id: id(prefix, "transition_many", i, j), fencing_token: 0}
        end

      {:ok, transitioned} =
        FerricStore.flow_transition_many(partition_key, "queued", "waiting", items,
          run_at_ms: 2_000,
          now_ms: 2_000
        )

      transitioned
    end)
  end

  defp bench_complete(prefix, backlog, iterations, partition_count) do
    claimed = preclaim(prefix, "complete", iterations, partition_count)

    result("flow.complete under #{backlog}", iterations, fn i ->
      flow = Enum.fetch!(claimed, i - 1)

      {:ok, completed} =
        FerricStore.flow_complete(flow.id, flow.lease_token,
          fencing_token: flow.fencing_token,
          result: "result:" <> flow.id,
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      completed
    end)
  end

  defp bench_complete_many(prefix, backlog, iterations, partition_count, batch_size) do
    claimed = preclaim_many(prefix, "complete_many", iterations, partition_count, batch_size)

    result("flow.complete_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        claimed
        |> Enum.fetch!(i - 1)
        |> Enum.map(fn flow ->
          %{id: flow.id, lease_token: flow.lease_token, fencing_token: flow.fencing_token}
        end)

      {:ok, completed} =
        FerricStore.flow_complete_many(partition_key, items,
          result: "result:complete_many:#{i}",
          now_ms: 2_000
        )

      completed
    end)
  end

  defp bench_retry(prefix, backlog, iterations, partition_count) do
    claimed = preclaim(prefix, "retry", iterations, partition_count)

    result("flow.retry under #{backlog}", iterations, fn i ->
      flow = Enum.fetch!(claimed, i - 1)

      {:ok, retried} =
        FerricStore.flow_retry(flow.id, flow.lease_token,
          fencing_token: flow.fencing_token,
          error: "error:" <> flow.id,
          run_at_ms: 3_000,
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      retried
    end)
  end

  defp bench_retry_many(prefix, backlog, iterations, partition_count, batch_size) do
    claimed = preclaim_many(prefix, "retry_many", iterations, partition_count, batch_size)

    result("flow.retry_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        claimed
        |> Enum.fetch!(i - 1)
        |> Enum.map(fn flow ->
          %{id: flow.id, lease_token: flow.lease_token, fencing_token: flow.fencing_token}
        end)

      {:ok, retried} =
        FerricStore.flow_retry_many(partition_key, items,
          error: "error:retry_many:#{i}",
          run_at_ms: 3_000,
          now_ms: 2_000
        )

      retried
    end)
  end

  defp bench_fail(prefix, backlog, iterations, partition_count) do
    claimed = preclaim(prefix, "fail", iterations, partition_count)

    result("flow.fail under #{backlog}", iterations, fn i ->
      flow = Enum.fetch!(claimed, i - 1)

      {:ok, failed} =
        FerricStore.flow_fail(flow.id, flow.lease_token,
          fencing_token: flow.fencing_token,
          error: "error:" <> flow.id,
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      failed
    end)
  end

  defp bench_fail_many(prefix, backlog, iterations, partition_count, batch_size) do
    claimed = preclaim_many(prefix, "fail_many", iterations, partition_count, batch_size)

    result("flow.fail_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        claimed
        |> Enum.fetch!(i - 1)
        |> Enum.map(fn flow ->
          %{id: flow.id, lease_token: flow.lease_token, fencing_token: flow.fencing_token}
        end)

      {:ok, failed} =
        FerricStore.flow_fail_many(partition_key, items,
          error: "error:fail_many:#{i}",
          now_ms: 2_000
        )

      failed
    end)
  end

  defp bench_cancel(prefix, backlog, iterations, partition_count) do
    flow_type = type(prefix, "cancel")

    timed("seed cancel #{iterations}", fn ->
      for i <- 1..iterations do
        create_flow(prefix, "cancel", flow_type, i, partition_count)
      end
    end)

    result("flow.cancel under #{backlog}", iterations, fn i ->
      {:ok, cancelled} =
        FerricStore.flow_cancel(id(prefix, "cancel", i),
          fencing_token: 0,
          reason_ref: "cancel:" <> id(prefix, "cancel", i),
          now_ms: 2_000,
          partition_key: partition(prefix, i, partition_count)
        )

      cancelled
    end)
  end

  defp bench_cancel_many(prefix, backlog, iterations, partition_count, batch_size) do
    flow_type = type(prefix, "cancel_many")

    timed("seed cancel_many #{iterations}x#{batch_size}", fn ->
      for i <- 1..iterations do
        partition_key = partition(prefix, i, partition_count)

        items =
          for j <- 1..batch_size do
            %{id: id(prefix, "cancel_many", i, j)}
          end

        {:ok, _flows} =
          FerricStore.flow_create_many(partition_key, items,
            type: flow_type,
            state: "queued",
            run_at_ms: 1_000,
            now_ms: 1_000
          )
      end
    end)

    result("flow.cancel_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
      partition_key = partition(prefix, i, partition_count)

      items =
        for j <- 1..batch_size do
          %{id: id(prefix, "cancel_many", i, j), fencing_token: 0}
        end

      {:ok, cancelled} =
        FerricStore.flow_cancel_many(partition_key, items,
          reason_ref: "cancel:cancel_many:#{i}",
          now_ms: 2_000
        )

      cancelled
    end)
  end

  defp bench_rewind(prefix, backlog, iterations, partition_count) do
    events =
      timed("seed rewind #{iterations}", fn ->
        for i <- 1..iterations do
          flow_type = type(prefix, "rewind")
          flow = create_claim_complete(prefix, "rewind", flow_type, i, partition_count)

          {:ok, [{event_id, _fields} | _]} =
            FerricStore.flow_history(flow.id,
              count: 10,
              partition_key: partition(prefix, i, partition_count)
            )

          event_id
        end
      end)

    result("flow.rewind under #{backlog}", iterations, fn i ->
      {:ok, rewound} =
        FerricStore.flow_rewind(id(prefix, "rewind", i),
          to_event: Enum.fetch!(events, i - 1),
          expect_state: "completed",
          run_at_ms: 3_000,
          now_ms: 3_000,
          partition_key: partition(prefix, i, partition_count)
        )

      rewound
    end)
  end

  defp create_flow(prefix, group, flow_type, i, partition_count) do
    {:ok, flow} =
      FerricStore.flow_create(id(prefix, group, i),
        type: flow_type,
        payload: "payload:" <> id(prefix, group, i),
        run_at_ms: 1_000,
        now_ms: 1_000,
        partition_key: partition(prefix, i, partition_count)
      )

    flow
  end

  defp create_claim(prefix, group, flow_type, i, partition_count) do
    flow = create_flow(prefix, group, flow_type, i, partition_count)

    {:ok, [claimed]} =
      FerricStore.flow_claim_due(flow_type,
        worker: "worker-" <> group,
        lease_ms: 30_000,
        limit: 1,
        now_ms: 1_000,
        partition_key: partition(prefix, i, partition_count)
      )

    true = claimed.id == flow.id
    claimed
  end

  defp create_claim_complete(prefix, group, flow_type, i, partition_count) do
    claimed = create_claim(prefix, group, flow_type, i, partition_count)

    {:ok, completed} =
      FerricStore.flow_complete(claimed.id, claimed.lease_token,
        fencing_token: claimed.fencing_token,
        result: "result:" <> claimed.id,
        now_ms: 2_000,
        partition_key: partition(prefix, i, partition_count)
      )

    completed
  end

  defp preclaim(prefix, group, iterations, partition_count) do
    flow_type = type(prefix, group)

    timed("seed #{group} claimed #{iterations}", fn ->
      for i <- 1..iterations do
        {:ok, _} =
          FerricStore.flow_create(id(prefix, group, i),
            type: flow_type,
            payload: "payload:" <> id(prefix, group, i),
            run_at_ms: 1_000,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )
      end

      for i <- 1..iterations do
        {:ok, [claimed]} =
          FerricStore.flow_claim_due(flow_type,
            worker: "worker-" <> group,
            lease_ms: 30_000,
            limit: 1,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        claimed
      end
    end)
  end

  defp preclaim_many(prefix, group, iterations, partition_count, batch_size) do
    flow_type = type(prefix, group)

    timed("seed #{group} claimed #{iterations}x#{batch_size}", fn ->
      for i <- 1..iterations do
        partition_key = partition(prefix, i, partition_count)

        items =
          for j <- 1..batch_size do
            %{id: id(prefix, group, i, j), payload: "payload:" <> id(prefix, group, i, j)}
          end

        {:ok, _flows} =
          FerricStore.flow_create_many(partition_key, items,
            type: flow_type,
            state: "queued",
            run_at_ms: 1_000,
            now_ms: 1_000
          )
      end

      for i <- 1..iterations do
        {:ok, claimed} =
          FerricStore.flow_claim_due(flow_type,
            worker: "worker-" <> group,
            lease_ms: 30_000,
            limit: batch_size,
            now_ms: 1_000,
            partition_key: partition(prefix, i, partition_count)
          )

        true = length(claimed) == batch_size
        claimed
      end
    end)
  end

  defp result(name, iterations, fun) do
    latencies =
      for i <- 1..iterations do
        {us, _result} = :timer.tc(fn -> fun.(i) end)
        us
      end

    stats(name, latencies)
  end

  defp stats(name, latencies) do
    sorted = Enum.sort(latencies)
    count = length(sorted)
    avg = Enum.sum(sorted) / count

    %{
      name: name,
      count: count,
      avg_us: avg,
      p50_us: percentile(sorted, 0.50),
      p95_us: percentile(sorted, 0.95),
      p99_us: percentile(sorted, 0.99),
      max_us: List.last(sorted),
      ops_sec: 1_000_000 / avg
    }
  end

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

  defp print_table(results) do
    IO.puts("| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |")
    IO.puts("|---|---:|---:|---:|---:|---:|---:|---:|")

    Enum.each(results, fn row ->
      IO.puts(
        "| #{row.name} | #{round(row.ops_sec)} | #{round(row.avg_us)} | #{row.p50_us} | #{row.p95_us} | #{row.p99_us} | #{row.max_us} | #{row.count} |"
      )
    end)
  end

  defp write_results(started_at, results, config) do
    File.mkdir_p!("bench/results")

    stamp =
      started_at
      |> String.replace(":", "")
      |> String.replace("-", "")
      |> String.replace(~r/\..*/, "")

    path = "bench/results/flow_100k_state_baseline_#{stamp}.md"

    lines = [
      "# Flow 100k State Baseline",
      "",
      "- started_at: #{started_at}",
      "- backlog: #{config.backlog}",
      "- iterations: #{config.iterations}",
      "- shards: #{config.shards}",
      "- partitions: #{config.partitions}",
      "- seed_concurrency: #{config.seed_concurrency}",
      "- create_many_batch: #{config.create_many_batch}",
      "- transition_many_batch: #{config.transition_many_batch}",
      "- terminal_many_batch: #{config.terminal_many_batch}",
      "- flow_lmdb_projection: lagged",
      "- claim_limits: #{Enum.join(config.claim_limits, ",")}",
      "- beam_memory_before: #{config.memory_before}",
      "- beam_memory_after_seed: #{config.memory_after_seed}",
      "- beam_memory_delta: #{config.memory_delta}",
      "",
      "| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |",
      "|---|---:|---:|---:|---:|---:|---:|---:|"
    ]

    rows =
      Enum.map(results, fn row ->
        "| #{row.name} | #{round(row.ops_sec)} | #{round(row.avg_us)} | #{row.p50_us} | #{row.p95_us} | #{row.p99_us} | #{row.max_us} | #{row.count} |"
      end)

    File.write!(path, Enum.join(lines ++ rows, "\n") <> "\n")
    IO.puts("\nWrote #{path}")
  end

  defp timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms")
    result
  end

  defp add(results, result), do: [result | results]
  defp add_many(results, many), do: Enum.reduce(many, results, &[&1 | &2])

  defp sample_index(i, backlog), do: rem(i * 7919, backlog) + 1
  defp id(prefix, group, i), do: prefix <> ":" <> group <> ":" <> Integer.to_string(i)

  defp id(prefix, group, i, j),
    do: prefix <> ":" <> group <> ":" <> Integer.to_string(i) <> ":" <> Integer.to_string(j)

  defp type(prefix, group), do: prefix <> ":" <> group

  defp partition(_prefix, _i, partition_count) when partition_count <= 1, do: nil

  defp partition(prefix, i, partition_count) do
    prefix <> ":partition:" <> Integer.to_string(rem(i - 1, partition_count))
  end
end

try do
  Flow100kStateBench.run(
    backlog,
    iterations,
    shard_count,
    partition_count,
    seed_concurrency,
    create_many_batch,
    transition_many_batch,
    terminal_many_batch,
    claim_limits,
    bench_data_dir
  )
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  File.rm_rf!(bench_data_dir)
end
