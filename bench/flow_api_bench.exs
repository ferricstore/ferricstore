# bench/flow_api_bench.exs
#
# Native Flow API benchmark.
#
# Measures the real public `FerricStore.flow_*` path added by native Flow Raft
# commands. This is different from `flow_native_batch_bench.exs`, which measured
# a synthetic command floor before the public Flow API existed.
#
# Run:
#   MIX_ENV=bench FERRICSTORE_BUILD=1 mix run --no-start bench/flow_api_bench.exs
#
# Options:
#   FLOW_API_BACKLOGS=1000,10000
#   FLOW_API_CLAIM=10
#   FLOW_API_CLAIM_SEED=5000
#   FLOW_API_SHARDS=1
#   FLOW_API_CASES=create,claim,lifecycle,retry,history
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
claim_count = System.get_env("FLOW_API_CLAIM", "10") |> String.to_integer()
claim_seed = System.get_env("FLOW_API_CLAIM_SEED", "5000") |> String.to_integer()
shard_count = System.get_env("FLOW_API_SHARDS", "1") |> String.to_integer()

cases =
  System.get_env("FLOW_API_CASES", "create,claim,lifecycle,retry,history")
  |> String.split(",", trim: true)
  |> MapSet.new()

backlogs =
  System.get_env("FLOW_API_BACKLOGS", "1000,10000")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_api_bench_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Native Flow API Bench ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "backlogs=#{Enum.join(backlogs, ",")} claim=#{claim_count} claim_seed=#{claim_seed} shards=#{shard_count} cases=#{Enum.join(cases, ",")} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel}\n"
)

defmodule FlowApiBench do
  def flow_id(prefix, i), do: prefix <> ":" <> Integer.to_string(i)

  def seed_due(prefix, type, count) do
    Enum.each(1..count, fn i ->
      {:ok, _} =
        FerricStore.flow_create(flow_id(prefix, i),
          type: type,
          state: "queued",
          payload_ref: "payload:" <> flow_id(prefix, i),
          run_at_ms: 1_000,
          now_ms: 1_000
        )
    end)
  end

  def create(prefix, type, counter) do
    i = next_i(counter)
    id = flow_id(prefix, i)

    {:ok, flow} =
      FerricStore.flow_create(id,
        type: type,
        state: "queued",
        payload_ref: "payload:" <> id,
        run_at_ms: 1_000,
        now_ms: 1_000
      )

    flow
  end

  def claim_due(type, count, counter) do
    i = next_i(counter)

    {:ok, claimed} =
      FerricStore.flow_claim_due(type,
        state: "queued",
        worker: "worker-a",
        lease_ms: 30_000,
        limit: count,
        now_ms: 1_000 + i
      )

    claimed
  end

  def lifecycle(prefix, type, counter) do
    flow = create(prefix, type, counter)

    {:ok, [claimed]} =
      FerricStore.flow_claim_due(type,
        state: "queued",
        worker: "worker-a",
        lease_ms: 30_000,
        limit: 1,
        now_ms: 1_000
      )

    true = claimed.id == flow.id
    {:ok, completed} = FerricStore.flow_complete(claimed.id, claimed.lease_token)
    completed
  end

  def retry_cycle(prefix, type, counter) do
    flow = create(prefix, type, counter)

    {:ok, [claimed]} =
      FerricStore.flow_claim_due(type,
        state: "queued",
        worker: "worker-a",
        lease_ms: 30_000,
        limit: 1,
        now_ms: 1_000
      )

    true = claimed.id == flow.id

    {:ok, retried} =
      FerricStore.flow_retry(claimed.id, claimed.lease_token,
        error_ref: "error:" <> claimed.id,
        run_at_ms: 2_000,
        now_ms: 1_500
      )

    retried
  end

  def history_read(prefix, type, counter) do
    flow = lifecycle(prefix, type, counter)
    {:ok, events} = FerricStore.flow_history(flow.id, count: 10)
    events
  end

  def next_i(counter) do
    :counters.add(counter, 1, 1)
    :counters.get(counter, 1)
  end

  def timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms result=#{inspect(short(result))}")
    result
  end

  def short(list) when is_list(list), do: length(list)
  def short(%{id: id, state: state}), do: %{id: id, state: state}
  def short(other), do: other

  def maybe_put_bench(benches, cases, case_name, label, fun) do
    if MapSet.member?(cases, case_name), do: Map.put(benches, label, fun), else: benches
  end
end

try do
  Enum.each(backlogs, fn backlog ->
    prefix = "flowapi#{backlog}:#{System.unique_integer([:positive])}"
    due_type = prefix <> ":due"
    claim_type = prefix <> ":claim"
    create_type = prefix <> ":create"
    lifecycle_type = prefix <> ":life"
    retry_type = prefix <> ":retry"
    history_type = prefix <> ":history"

    create_counter = :counters.new(1, [:atomics])
    claim_counter = :counters.new(1, [:atomics])
    lifecycle_counter = :counters.new(1, [:atomics])
    retry_counter = :counters.new(1, [:atomics])
    history_counter = :counters.new(1, [:atomics])

    IO.puts("\n--- backlog=#{backlog} ---")

    FlowApiBench.timed("seed due backlog #{backlog}", fn ->
      FlowApiBench.seed_due(prefix <> ":due", due_type, backlog)
    end)

    FlowApiBench.timed("seed claim backlog #{claim_seed}", fn ->
      FlowApiBench.seed_due(prefix <> ":claim", claim_type, claim_seed)
    end)

    benches =
      %{}
      |> FlowApiBench.maybe_put_bench(cases, "create", "flow_api create backlog=#{backlog}", fn ->
        flow = FlowApiBench.create(prefix <> ":create", create_type, create_counter)
        true = flow.state == "queued"
      end)
      |> FlowApiBench.maybe_put_bench(
        cases,
        "claim",
        "flow_api claim_due#{claim_count} backlog=#{backlog}",
        fn ->
          claimed = FlowApiBench.claim_due(claim_type, claim_count, claim_counter)
          true = length(claimed) == claim_count
        end
      )
      |> FlowApiBench.maybe_put_bench(
        cases,
        "lifecycle",
        "flow_api lifecycle create-claim-complete backlog=#{backlog}",
        fn ->
          flow = FlowApiBench.lifecycle(prefix <> ":life", lifecycle_type, lifecycle_counter)
          true = flow.state == "completed"
        end
      )
      |> FlowApiBench.maybe_put_bench(
        cases,
        "retry",
        "flow_api retry cycle backlog=#{backlog}",
        fn ->
          flow = FlowApiBench.retry_cycle(prefix <> ":retry", retry_type, retry_counter)
          true = flow.state == "queued"
        end
      )
      |> FlowApiBench.maybe_put_bench(
        cases,
        "history",
        "flow_api lifecycle+history backlog=#{backlog}",
        fn ->
          events = FlowApiBench.history_read(prefix <> ":history", history_type, history_counter)
          true = length(events) == 3
        end
      )

    Benchee.run(
      benches,
      warmup: bench_warmup,
      time: bench_time,
      parallel: bench_parallel,
      memory_time: 0,
      formatters: [Benchee.Formatters.Console]
    )
  end)
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  File.rm_rf!(bench_data_dir)
end
