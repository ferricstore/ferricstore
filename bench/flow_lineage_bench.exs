# bench/flow_lineage_bench.exs
#
# Lineage-heavy Flow benchmark:
#   - root + child workflows with parent/root/correlation metadata
#   - partitioned queries by parent/root/correlation
#   - terminal LMDB projection queries after complete
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/flow_lineage_bench.exs

backlog = System.get_env("FLOW_LINEAGE_BACKLOG", "100000") |> String.to_integer()
iterations = System.get_env("FLOW_LINEAGE_ITER", "200") |> String.to_integer()
shard_count = System.get_env("FLOW_LINEAGE_SHARDS", "4") |> String.to_integer()
partition_count = System.get_env("FLOW_LINEAGE_PARTITIONS", "4") |> String.to_integer()
root_count = System.get_env("FLOW_LINEAGE_ROOTS", "1000") |> String.to_integer()
terminal_root_count = System.get_env("FLOW_LINEAGE_TERMINAL_ROOTS", "100") |> String.to_integer()
seed_concurrency = System.get_env("FLOW_LINEAGE_SEED_CONCURRENCY", "32") |> String.to_integer()
query_count = System.get_env("FLOW_LINEAGE_QUERY_COUNT", "100") |> String.to_integer()
terminal_count = System.get_env("FLOW_LINEAGE_TERMINAL", "10000") |> String.to_integer()

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_lineage_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

defmodule FlowLineageBench do
  def run(config) do
    started_at = DateTime.utc_now() |> DateTime.to_iso8601()
    prefix = "flowlineage:" <> Integer.to_string(System.unique_integer([:positive]))
    type = key(prefix, "type", "main")
    terminal_type = key(prefix, "type", "terminal")

    IO.puts("=== FerricStore Flow Lineage Bench ===")
    IO.puts("data_dir=#{config.data_dir}")

    IO.puts(
      "backlog=#{config.backlog} terminal=#{config.terminal_count} iterations=#{config.iterations} roots=#{config.root_count} terminal_roots=#{config.terminal_root_count} shards=#{config.shard_count} partitions=#{config.partition_count} query_count=#{config.query_count}"
    )

    IO.puts("seed_concurrency=#{config.seed_concurrency} flow_lmdb_projection=lagged")

    memory_before = :erlang.memory(:total)

    timed("seed roots #{config.root_count}", fn ->
      seed_roots(prefix, type, :active, config.root_count, config.partition_count)
    end)

    timed("seed lineage active backlog #{config.backlog}", fn ->
      seed_children(
        prefix,
        type,
        :active,
        config.backlog,
        config.root_count,
        config.partition_count,
        config.seed_concurrency
      )
    end)

    memory_after_seed = :erlang.memory(:total)

    IO.puts(
      "beam_memory_before=#{memory_before} beam_memory_after_seed=#{memory_after_seed} delta=#{memory_after_seed - memory_before}\n"
    )

    results =
      []
      |> add(
        result("flow.create lineage under #{config.backlog}", config.iterations, fn i ->
          create_child(
            prefix,
            "create",
            type,
            :active,
            i,
            config.root_count,
            config.partition_count
          )
        end)
      )
      |> add(
        result("flow.by_parent hot count=#{config.query_count}", config.iterations, fn i ->
          query_parent(prefix, i, config.root_count, config.partition_count, config.query_count)
        end)
      )
      |> add(
        result("flow.by_root hot count=#{config.query_count}", config.iterations, fn i ->
          query_root(
            prefix,
            :active,
            i,
            config.root_count,
            config.partition_count,
            config.query_count
          )
        end)
      )
      |> add(
        result("flow.by_correlation hot count=#{config.query_count}", config.iterations, fn i ->
          query_correlation(
            prefix,
            :active,
            i,
            config.root_count,
            config.partition_count,
            config.query_count
          )
        end)
      )

    timed("seed terminal roots #{config.terminal_root_count}", fn ->
      seed_roots(
        prefix,
        terminal_type,
        :terminal,
        config.terminal_root_count,
        config.partition_count
      )
    end)

    timed("seed terminal lineage #{config.terminal_count}", fn ->
      seed_terminal_lineage(
        prefix,
        terminal_type,
        :terminal,
        config.terminal_count,
        config.terminal_root_count,
        config.partition_count
      )
    end)

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(config.shard_count)

    results =
      results
      |> add(
        result(
          "flow.by_root terminal lmdb count=#{config.query_count}",
          config.iterations,
          fn i ->
            query_root(
              prefix,
              :terminal,
              i,
              config.terminal_root_count,
              config.partition_count,
              config.query_count
            )
          end
        )
      )
      |> add(
        result(
          "flow.by_correlation terminal lmdb count=#{config.query_count}",
          config.iterations,
          fn i ->
            query_correlation(
              prefix,
              :terminal,
              i,
              config.terminal_root_count,
              config.partition_count,
              config.query_count
            )
          end
        )
      )
      |> Enum.reverse()

    print_table(results)

    write_results(
      started_at,
      results,
      Map.merge(config, %{
        memory_before: memory_before,
        memory_after_seed: memory_after_seed,
        memory_delta: memory_after_seed - memory_before
      })
    )
  end

  defp seed_roots(prefix, type, namespace, root_count, partition_count) do
    for i <- 1..root_count do
      root = root_id(prefix, namespace, i)

      {:ok, _} =
        FerricStore.flow_create(root,
          type: type,
          run_at_ms: 1_000,
          now_ms: 1_000,
          correlation_id: correlation_id(prefix, namespace, i),
          partition_key: partition(prefix, i, partition_count)
        )
    end
  end

  defp seed_children(
         prefix,
         type,
         namespace,
         backlog,
         root_count,
         partition_count,
         seed_concurrency
       ) do
    progress = :atomics.new(1, signed: false)

    1..backlog
    |> Task.async_stream(
      fn i ->
        create_child(prefix, "active", type, namespace, i, root_count, partition_count)
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

  defp seed_terminal_lineage(prefix, type, namespace, terminal_count, root_count, partition_count) do
    for i <- 1..terminal_count do
      flow = create_child(prefix, "terminal", type, namespace, i, root_count, partition_count)

      {:ok, [claimed]} =
        FerricStore.flow_claim_due(type,
          worker: "worker-terminal",
          lease_ms: 30_000,
          limit: 1,
          now_ms: 2_000,
          partition_key: flow.partition_key
        )

      {:ok, completed} =
        FerricStore.flow_complete(claimed.id, claimed.lease_token,
          fencing_token: claimed.fencing_token,
          result: "result:" <> claimed.id,
          now_ms: 3_000,
          partition_key: flow.partition_key
        )

      true = completed.state == "completed"

      if rem(i, 1_000) == 0 do
        IO.puts("seeded terminal #{i}/#{terminal_count}")
      end
    end
  end

  defp create_child(prefix, group, type, namespace, i, root_count, partition_count) do
    root_index = root_index(i, root_count)
    root = root_id(prefix, namespace, root_index)

    id = key(prefix, "#{namespace}:#{group}", i)

    {:ok, flow} =
      FerricStore.flow_create(id,
        type: type,
        payload: "payload:" <> id,
        parent_flow_id: root,
        root_flow_id: root,
        correlation_id: correlation_id(prefix, namespace, root_index),
        run_at_ms: 1_000,
        now_ms: 1_000,
        partition_key: partition(prefix, root_index, partition_count)
      )

    flow
  end

  defp query_parent(prefix, i, root_count, partition_count, count) do
    root_index = root_index(i, root_count)

    {:ok, records} =
      FerricStore.flow_by_parent(root_id(prefix, :active, root_index),
        partition_key: partition(prefix, root_index, partition_count),
        count: count
      )

    records
  end

  defp query_root(prefix, namespace, i, root_count, partition_count, count) do
    root_index = root_index(i, root_count)

    {:ok, records} =
      FerricStore.flow_by_root(root_id(prefix, namespace, root_index),
        partition_key: partition(prefix, root_index, partition_count),
        count: count
      )

    records
  end

  defp query_correlation(prefix, namespace, i, root_count, partition_count, count) do
    root_index = root_index(i, root_count)

    {:ok, records} =
      FerricStore.flow_by_correlation(correlation_id(prefix, namespace, root_index),
        partition_key: partition(prefix, root_index, partition_count),
        count: count
      )

    records
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

    path = "bench/results/flow_lineage_bench_#{stamp}.md"

    lines = [
      "# Flow Lineage Bench",
      "",
      "- started_at: #{started_at}",
      "- backlog: #{config.backlog}",
      "- terminal_count: #{config.terminal_count}",
      "- iterations: #{config.iterations}",
      "- roots: #{config.root_count}",
      "- terminal_roots: #{config.terminal_root_count}",
      "- shards: #{config.shard_count}",
      "- partitions: #{config.partition_count}",
      "- query_count: #{config.query_count}",
      "- seed_concurrency: #{config.seed_concurrency}",
      "- flow_lmdb_projection: lagged",
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
  defp key(prefix, group, suffix), do: prefix <> ":" <> group <> ":" <> to_string(suffix)
  defp root_id(prefix, namespace, i), do: key(prefix, "#{namespace}:root", i)
  defp correlation_id(prefix, namespace, i), do: key(prefix, "#{namespace}:corr", i)
  defp root_index(i, root_count), do: rem(i - 1, root_count) + 1
  defp partition(_prefix, _i, partition_count) when partition_count <= 1, do: nil

  defp partition(prefix, i, partition_count) do
    prefix <> ":partition:" <> Integer.to_string(rem(i - 1, partition_count))
  end
end

try do
  FlowLineageBench.run(%{
    backlog: backlog,
    terminal_count: terminal_count,
    terminal_root_count: terminal_root_count,
    iterations: iterations,
    shard_count: shard_count,
    partition_count: partition_count,
    root_count: root_count,
    seed_concurrency: seed_concurrency,
    query_count: query_count,
    data_dir: bench_data_dir
  })
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  File.rm_rf(bench_data_dir)
end
