# bench/flow_pipeline_write_bench.exs
#
# Targeted Flow write batch benchmark:
#   * pipeline create batch
#   * flow_create_many batch
#   * pipeline transition batch
#   * flow_transition_many batch
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 mix run --no-start \
#     -e 'Code.require_file("bench/flow_pipeline_write_bench.exs")'
#
# Options:
#   FLOW_PIPE_BACKLOG=100000
#   FLOW_PIPE_ITER=100
#   FLOW_PIPE_BATCH=100
#   FLOW_PIPE_SHARDS=4
#   FLOW_PIPE_PARTITIONS=4

backlog = System.get_env("FLOW_PIPE_BACKLOG", "100000") |> String.to_integer()
iterations = System.get_env("FLOW_PIPE_ITER", "100") |> String.to_integer()
batch_size = System.get_env("FLOW_PIPE_BATCH", "100") |> String.to_integer()
shard_count = System.get_env("FLOW_PIPE_SHARDS", "4") |> String.to_integer()
partition_count = System.get_env("FLOW_PIPE_PARTITIONS", "4") |> String.to_integer()

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_pipeline_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

defmodule FlowPipelineWriteBench do
  def run(backlog, iterations, batch_size, partition_count, data_dir) do
    prefix = "flowpipe:" <> Integer.to_string(System.unique_integer([:positive]))
    ctx = FerricStore.Instance.get(:default)

    IO.puts("=== FerricStore Flow Pipeline Write Bench ===")
    IO.puts("data_dir=#{data_dir}")

    IO.puts(
      "mode=#{Ferricstore.ReplicationMode.current()} backlog=#{backlog} iterations=#{iterations} batch=#{batch_size} partitions=#{partition_count}"
    )

    timed("seed active backlog #{backlog}", fn ->
      seed_create_many(
        prefix,
        "active",
        type(prefix, "active"),
        backlog,
        batch_size,
        partition_count
      )
    end)

    transition_pipeline_ids =
      timed("seed transition pipeline #{iterations}x#{batch_size}", fn ->
        seed_groups(prefix, "transition_pipeline", iterations, batch_size, partition_count)
      end)

    transition_many_ids =
      timed("seed transition_many #{iterations}x#{batch_size}", fn ->
        seed_groups(prefix, "transition_many", iterations, batch_size, partition_count)
      end)

    results = [
      result("flow.pipeline create batch=#{batch_size} under #{backlog}", iterations, fn i ->
        partition_key = partition(prefix, i, partition_count)

        ops =
          for j <- 1..batch_size do
            id = id(prefix, "pipeline_create", i, j)

            {:create, id,
             [
               type: type(prefix, "pipeline_create"),
               state: "queued",
               payload: "payload:" <> id,
               run_at_ms: 1_000,
               now_ms: 1_000,
               partition_key: partition_key
             ]}
          end

        results = Ferricstore.Flow.pipeline_write_batch_independent(ctx, ops)
        assert_no_errors!(results)
      end),
      result("flow.create_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
        partition_key = partition(prefix, i, partition_count)

        items =
          for j <- 1..batch_size do
            %{id: id(prefix, "create_many", i, j), payload: "payload"}
          end

        {:ok, flows} =
          FerricStore.flow_create_many(partition_key, items,
            type: type(prefix, "create_many"),
            state: "queued",
            run_at_ms: 1_000,
            now_ms: 1_000
          )

        flows
      end),
      result("flow.pipeline transition batch=#{batch_size} under #{backlog}", iterations, fn i ->
        partition_key = partition(prefix, i, partition_count)
        ids = Map.fetch!(transition_pipeline_ids, i)

        ops =
          Enum.map(ids, fn flow_id ->
            {:transition, flow_id, "queued", "waiting",
             [
               fencing_token: 0,
               run_at_ms: 2_000,
               now_ms: 2_000,
               partition_key: partition_key
             ]}
          end)

        results = Ferricstore.Flow.pipeline_write_batch_independent(ctx, ops)
        assert_no_errors!(results)
      end),
      result("flow.transition_many batch=#{batch_size} under #{backlog}", iterations, fn i ->
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
    ]

    print_table(results)
  after
    IO.puts("\nCleaning up bench data directory: #{data_dir}")
    File.rm_rf(data_dir)
  end

  defp seed_groups(prefix, group, iterations, batch_size, partition_count) do
    flow_type = type(prefix, group)

    for i <- 1..iterations, into: %{} do
      partition_key = partition(prefix, i, partition_count)

      items =
        for j <- 1..batch_size do
          %{id: id(prefix, group, i, j), payload: "payload"}
        end

      {:ok, flows} =
        FerricStore.flow_create_many(partition_key, items,
          type: flow_type,
          state: "queued",
          run_at_ms: 1_000,
          now_ms: 1_000
        )

      {i, Enum.map(flows, & &1.id)}
    end
  end

  defp seed_create_many(prefix, group, flow_type, total, batch_size, partition_count) do
    total_batches = div(total + batch_size - 1, batch_size)

    for i <- 1..total_batches do
      partition_key = partition(prefix, i, partition_count)
      first = (i - 1) * batch_size + 1
      last = min(i * batch_size, total)

      items =
        for j <- first..last do
          %{id: id(prefix, group, j), payload: "payload:" <> id(prefix, group, j)}
        end

      {:ok, _flows} =
        FerricStore.flow_create_many(partition_key, items,
          type: flow_type,
          state: "queued",
          run_at_ms: 1_000,
          now_ms: 1_000
        )
    end
  end

  defp result(label, iterations, fun) do
    samples =
      for i <- 1..iterations do
        {us, _result} = :timer.tc(fn -> fun.(i) end)
        us
      end

    sorted = Enum.sort(samples)
    n = length(sorted)
    sum = Enum.sum(sorted)

    %{
      label: label,
      n: n,
      ops_s: round(n / (sum / 1_000_000)),
      avg_us: round(sum / n),
      p50_us: percentile(sorted, 0.50),
      p95_us: percentile(sorted, 0.95),
      p99_us: percentile(sorted, 0.99),
      max_us: List.last(sorted)
    }
  end

  defp print_table(results) do
    IO.puts("| command | ops/s | avg us | p50 us | p95 us | p99 us | max us | n |")
    IO.puts("|---|---:|---:|---:|---:|---:|---:|---:|")

    Enum.each(results, fn row ->
      IO.puts(
        "| #{row.label} | #{row.ops_s} | #{row.avg_us} | #{row.p50_us} | #{row.p95_us} | #{row.p99_us} | #{row.max_us} | #{row.n} |"
      )
    end)
  end

  defp percentile(sorted, pct) do
    idx =
      sorted
      |> length()
      |> Kernel.*(pct)
      |> Float.ceil()
      |> trunc()
      |> max(1)
      |> min(length(sorted))

    Enum.at(sorted, idx - 1)
  end

  defp assert_no_errors!(results) do
    case Enum.find(results, &match?({:error, _}, &1)) do
      nil -> results
      error -> raise "flow pipeline batch returned #{inspect(error)}"
    end
  end

  defp timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms")
    result
  end

  defp id(prefix, group, i), do: "#{prefix}:#{group}:#{i}"
  defp id(prefix, group, i, j), do: "#{prefix}:#{group}:#{i}:#{j}"
  defp type(prefix, group), do: "#{prefix}:#{group}"
  defp partition(_prefix, _i, partition_count) when partition_count <= 1, do: nil
  defp partition(prefix, i, partition_count), do: "#{prefix}:p:#{rem(i - 1, partition_count)}"
end

FlowPipelineWriteBench.run(backlog, iterations, batch_size, partition_count, bench_data_dir)
