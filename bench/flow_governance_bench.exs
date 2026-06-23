# bench/flow_governance_bench.exs
#
# Governance hot-path benchmark.
#
# Measures real public FerricFlow governance APIs through the normal FerricStore
# application path. This is intended to answer: what is the cost of opt-in
# budget/limit governance when the rest of Flow is unchanged?
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/flow_governance_bench.exs
#
# Options:
#   GOV_BENCH_CASES=budget_reserve,budget_cycle,budget_shared_cycle,limit_lease,limit_spend_release,claim_empty_governed,claim_complete,claim_complete_governed
#   GOV_BENCH_BACKLOG=50000
#   GOV_BENCH_BATCH=100
#   GOV_BENCH_SHARDS=16
#   GOV_BENCH_PARTITIONS=16
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
backlog = System.get_env("GOV_BENCH_BACKLOG", "50000") |> String.to_integer()
batch = System.get_env("GOV_BENCH_BATCH", "100") |> String.to_integer()
shard_count = System.get_env("GOV_BENCH_SHARDS", "16") |> String.to_integer()
partition_count = System.get_env("GOV_BENCH_PARTITIONS", "16") |> String.to_integer()

cases =
  System.get_env(
    "GOV_BENCH_CASES",
    "budget_reserve,budget_cycle,budget_shared_cycle,limit_lease,limit_spend_release,claim_empty_governed,claim_complete,claim_complete_governed"
  )
  |> String.split(",", trim: true)
  |> MapSet.new()

bench_data_dir =
  Path.join(
    System.tmp_dir!(),
    "ferricstore_flow_governance_bench_#{System.unique_integer([:positive])}"
  )

File.mkdir_p!(bench_data_dir)

Logger.configure(level: :warning)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, shard_count)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Flow Governance Bench ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "backlog=#{backlog} batch=#{batch} shards=#{shard_count} partitions=#{partition_count} cases=#{Enum.join(cases, ",")} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel}\n"
)

defmodule FlowGovernanceBench do
  def flow_id(prefix, i), do: prefix <> ":flow:" <> Integer.to_string(i)
  def scope(prefix, i), do: prefix <> ":scope:" <> Integer.to_string(i)
  def reservation_id(prefix, i), do: prefix <> ":reservation:" <> Integer.to_string(i)

  def partition_key(_prefix, _i, partition_count) when partition_count <= 1, do: nil

  def partition_key(prefix, i, partition_count) do
    prefix <> ":partition:" <> Integer.to_string(rem(i - 1, partition_count))
  end

  def maybe_partition(opts, nil), do: opts
  def maybe_partition(opts, partition_key), do: Keyword.put(opts, :partition_key, partition_key)

  def next_i(counter), do: :atomics.add_get(counter, 1, 1)

  def seed_due(prefix, type, count, partition_count) do
    Enum.each(1..count, fn i ->
      :ok =
        FerricStore.flow_create(
          flow_id(prefix, i),
          maybe_partition(
            [
              type: type,
              state: "queued",
              run_at_ms: 1_000,
              now_ms: 1_000
            ],
            partition_key(prefix, i, partition_count)
          )
        )
    end)
  end

  def budget_reserve(prefix, counter) do
    i = next_i(counter)

    {:ok, result} =
      FerricStore.flow_budget_reserve(scope(prefix, i), 1,
        limit: 1_000_000,
        window_ms: 60_000,
        reservation_id: reservation_id(prefix, i),
        now_ms: 1_000
      )

    true = result.status == :reserved
  end

  def budget_cycle(prefix, counter) do
    i = next_i(counter)
    scope = scope(prefix, i)
    reservation_id = reservation_id(prefix, i)

    {:ok, reserved} =
      FerricStore.flow_budget_reserve(scope, 1,
        limit: 1_000_000,
        window_ms: 60_000,
        reservation_id: reservation_id,
        now_ms: 1_000
      )

    {:ok, committed} =
      FerricStore.flow_budget_commit(scope, reservation_id, 1,
        usage: %{amount: 1},
        now_ms: 1_001
      )

    true = reserved.status == :reserved and committed.status == :committed
  end

  def budget_shared_cycle(scope, prefix, counter) do
    i = next_i(counter)
    reservation_id = reservation_id(prefix, i)

    {:ok, reserved} =
      FerricStore.flow_budget_reserve(scope, 1,
        limit: 10_000_000,
        window_ms: 3_600_000,
        reservation_id: reservation_id,
        now_ms: 1_000 + i
      )

    {:ok, committed} =
      FerricStore.flow_budget_commit(scope, reservation_id, 1,
        usage: %{amount: 1},
        now_ms: 1_001 + i
      )

    true = reserved.status == :reserved and committed.status == :committed
  end

  def limit_lease(prefix, counter) do
    i = next_i(counter)

    {:ok, leased} =
      FerricStore.flow_limit_lease(scope(prefix, i),
        shard_id: rem(i, 16),
        amount: 1,
        limit: 1_000_000,
        ttl_ms: 60_000,
        now_ms: 1_000
      )

    true = get_in(leased, [:lease, :available]) >= 1
  end

  def limit_spend_release(scope, counter) do
    _i = next_i(counter)
    shard_id = 0

    {:ok, spent} =
      FerricStore.flow_limit_spend(scope,
        shard_id: shard_id,
        amount: 1,
        now_ms: 1_001
      )

    {:ok, released} = FerricStore.flow_limit_release(scope, shard_id: shard_id, amount: 1)

    true = get_in(spent, [:lease, :in_use]) >= 1 and is_map(released)
  end

  def claim_complete(prefix, type, batch, counter, partition_count, opts \\ []) do
    i = next_i(counter)
    partition_key = partition_key(prefix, i, partition_count)

    {:ok, claimed} =
      FerricStore.flow_claim_due(
        type,
        maybe_partition(
          [
            state: "queued",
            worker: "worker-a",
            lease_ms: 30_000,
            limit: batch,
            now_ms: 1_000 + i
          ] ++ opts,
          partition_key
        )
      )

    case claimed do
      [] ->
        0

      jobs ->
        complete_items =
          Enum.map(jobs, fn job ->
            %{
              id: job.id,
              lease_token: job.lease_token,
              fencing_token: job.fencing_token
            }
          end)

        :ok =
          FerricStore.flow_complete_many(
            partition_key,
            complete_items,
            [now_ms: 2_000 + i] ++ release_opts(opts)
          )

        length(jobs)
    end
  end

  def governed_claim_complete(prefix, type, limit_scope, batch, counter, partition_count) do
    claim_complete(prefix, type, batch, counter, partition_count,
      governance_limit_scope: limit_scope,
      governance_shard_id: 0
    )
  end

  def empty_governed_claim(prefix, type, limit_scope, counter, partition_count) do
    i = next_i(counter)
    partition_key = partition_key(prefix, i, partition_count)

    {:ok, []} =
      FerricStore.flow_claim_due(
        type,
        maybe_partition(
          [
            state: "queued",
            worker: "worker-empty",
            lease_ms: 30_000,
            limit: 1,
            now_ms: 1_000 + i,
            governance_limit_scope: limit_scope,
            governance_shard_id: 0
          ],
          partition_key
        )
      )

    true
  end

  def wait_empty_claim_presence(prefix, type, partition_count) do
    partition_key = partition_key(prefix, 1, partition_count)

    attrs =
      [
        type: type,
        state: "queued",
        priority: nil,
        limit: 1,
        now_ms: 1_000
      ]
      |> maybe_partition(partition_key)
      |> Map.new()

    ctx = FerricStore.Instance.get(:default)

    Enum.reduce_while(1..100, :unknown, fn _attempt, _acc ->
      case Ferricstore.Store.Router.flow_claim_due_presence(ctx, attrs) do
        :empty ->
          {:halt, :empty}

        _other ->
          Process.sleep(25)
          {:cont, :unknown}
      end
    end)
  end

  defp release_opts(opts) do
    opts
    |> Keyword.take([:governance_limit_scope, :governance_shard_id])
  end

  def timed(label, fun) do
    {us, result} = :timer.tc(fun)
    IO.puts("#{label}: #{Float.round(us / 1000, 2)} ms result=#{inspect(short(result))}")
    result
  end

  def drain(label, target, fun) when is_integer(target) and target > 0 and is_function(fun, 0) do
    {us, total} =
      :timer.tc(fn ->
        drain_loop(fun, target, 0, 0)
      end)

    seconds = us / 1_000_000
    rate = if seconds > 0, do: total / seconds, else: 0.0

    IO.puts(
      "#{label}: total=#{total} seconds=#{Float.round(seconds, 3)} rate=#{Float.round(rate, 1)}/s"
    )

    %{total: total, seconds: seconds, rate: rate}
  end

  def loop(label, target, fun) when is_integer(target) and target > 0 and is_function(fun, 0) do
    {us, total} =
      :timer.tc(fn ->
        Enum.reduce(1..target, 0, fn _, acc ->
          if fun.(), do: acc + 1, else: acc
        end)
      end)

    seconds = us / 1_000_000
    rate = if seconds > 0, do: total / seconds, else: 0.0

    IO.puts(
      "#{label}: total=#{total} seconds=#{Float.round(seconds, 3)} rate=#{Float.round(rate, 1)}/s"
    )

    %{total: total, seconds: seconds, rate: rate}
  end

  defp drain_loop(_fun, target, total, _empty_rounds) when total >= target, do: total

  defp drain_loop(fun, target, total, empty_rounds) do
    case fun.() do
      count when is_integer(count) and count > 0 ->
        drain_loop(fun, target, total + count, 0)

      _empty ->
        if empty_rounds >= 32 do
          total
        else
          drain_loop(fun, target, total, empty_rounds + 1)
        end
    end
  end

  def short(list) when is_list(list), do: length(list)
  def short(%{rate: rate, total: total}), do: %{rate: Float.round(rate, 1), total: total}
  def short(other), do: other

  def maybe_put_bench(benches, cases, case_name, label, fun) do
    if MapSet.member?(cases, case_name), do: Map.put(benches, label, fun), else: benches
  end
end

try do
  prefix = "govbench:#{System.unique_integer([:positive])}"
  normal_type = prefix <> ":normal"
  governed_type = prefix <> ":governed"
  limit_scope = prefix <> ":running-limit"
  limit_hot_scope = prefix <> ":hot-limit"
  limit_empty_scope = prefix <> ":empty-limit"
  budget_shared_scope = prefix <> ":budget-shared"

  budget_reserve_counter = :atomics.new(1, signed: false)
  budget_cycle_counter = :atomics.new(1, signed: false)
  budget_shared_cycle_counter = :atomics.new(1, signed: false)
  limit_lease_counter = :atomics.new(1, signed: false)
  limit_spend_release_counter = :atomics.new(1, signed: false)
  empty_governed_claim_counter = :atomics.new(1, signed: false)
  normal_claim_counter = :atomics.new(1, signed: false)
  governed_claim_counter = :atomics.new(1, signed: false)

  if MapSet.member?(cases, "claim_complete") do
    FlowGovernanceBench.timed("seed normal claim backlog #{backlog}", fn ->
      FlowGovernanceBench.seed_due(prefix <> ":normal", normal_type, backlog, partition_count)
    end)
  end

  if MapSet.member?(cases, "claim_complete_governed") do
    FlowGovernanceBench.timed("seed governed claim backlog #{backlog}", fn ->
      FlowGovernanceBench.seed_due(prefix <> ":governed", governed_type, backlog, partition_count)
    end)

    FlowGovernanceBench.timed("seed governance limit #{backlog}", fn ->
      FerricStore.flow_limit_lease(limit_scope,
        shard_id: 0,
        amount: backlog,
        limit: backlog,
        ttl_ms: 300_000,
        now_ms: 1_000
      )
    end)
  end

  if MapSet.member?(cases, "limit_spend_release") do
    FlowGovernanceBench.timed("seed hot governance limit #{backlog}", fn ->
      FerricStore.flow_limit_lease(limit_hot_scope,
        shard_id: 0,
        amount: backlog,
        limit: backlog,
        ttl_ms: 300_000,
        now_ms: 1_000
      )
    end)
  end

  empty_claim_presence =
    if MapSet.member?(cases, "claim_empty_governed") do
      FlowGovernanceBench.timed("seed future empty-claim index", fn ->
        FerricStore.flow_create(
          prefix <> ":empty:init",
          FlowGovernanceBench.maybe_partition(
            [
              type: prefix <> ":empty-type",
              state: "queued",
              run_at_ms: 60_000,
              now_ms: 1_000
            ],
            FlowGovernanceBench.partition_key(prefix <> ":empty", 1, partition_count)
          )
        )
      end)

      FlowGovernanceBench.timed("seed exhausted empty governance limit", fn ->
        with {:ok, _lease} <-
               FerricStore.flow_limit_lease(limit_empty_scope,
                 shard_id: 0,
                 amount: 1,
                 limit: 1,
                 ttl_ms: 300_000,
                 now_ms: 1_000
               ) do
          FerricStore.flow_limit_spend(limit_empty_scope,
            shard_id: 0,
            amount: 1,
            now_ms: 1_001
          )
        end
      end)

      FlowGovernanceBench.timed("wait empty claim precheck", fn ->
        FlowGovernanceBench.wait_empty_claim_presence(
          prefix <> ":empty",
          prefix <> ":empty-type",
          partition_count
        )
      end)
    else
      :not_requested
    end

  if MapSet.member?(cases, "claim_complete") do
    FlowGovernanceBench.drain("flow drain claim#{batch}+complete baseline", backlog, fn ->
      FlowGovernanceBench.claim_complete(
        prefix <> ":normal",
        normal_type,
        batch,
        normal_claim_counter,
        partition_count
      )
    end)
  end

  if MapSet.member?(cases, "claim_complete_governed") do
    FlowGovernanceBench.drain("flow drain claim#{batch}+complete governed-limit", backlog, fn ->
      FlowGovernanceBench.governed_claim_complete(
        prefix <> ":governed",
        governed_type,
        limit_scope,
        batch,
        governed_claim_counter,
        partition_count
      )
    end)
  end

  if MapSet.member?(cases, "budget_shared_cycle") do
    FlowGovernanceBench.loop("governance budget same-scope reserve+commit", backlog, fn ->
      FlowGovernanceBench.budget_shared_cycle(
        budget_shared_scope,
        prefix <> ":budget-shared",
        budget_shared_cycle_counter
      )
    end)
  end

  if MapSet.member?(cases, "claim_empty_governed") and empty_claim_presence == :empty do
    FlowGovernanceBench.loop("flow empty claim governed-limit", backlog, fn ->
      FlowGovernanceBench.empty_governed_claim(
        prefix <> ":empty",
        prefix <> ":empty-type",
        limit_empty_scope,
        empty_governed_claim_counter,
        partition_count
      )
    end)
  end

  if MapSet.member?(cases, "claim_empty_governed") and empty_claim_presence != :empty do
    IO.puts("flow empty claim governed-limit: skipped precheck=#{inspect(empty_claim_presence)}")
  end

  benches =
    %{}
    |> FlowGovernanceBench.maybe_put_bench(
      cases,
      "budget_reserve",
      "governance budget reserve",
      fn ->
        FlowGovernanceBench.budget_reserve(prefix <> ":budget-reserve", budget_reserve_counter)
      end
    )
    |> FlowGovernanceBench.maybe_put_bench(
      cases,
      "budget_cycle",
      "governance budget reserve+commit",
      fn ->
        FlowGovernanceBench.budget_cycle(prefix <> ":budget-cycle", budget_cycle_counter)
      end
    )
    |> FlowGovernanceBench.maybe_put_bench(
      cases,
      "limit_lease",
      "governance limit lease",
      fn ->
        FlowGovernanceBench.limit_lease(prefix <> ":limit-lease", limit_lease_counter)
      end
    )
    |> FlowGovernanceBench.maybe_put_bench(
      cases,
      "limit_spend_release",
      "governance limit spend+release",
      fn ->
        FlowGovernanceBench.limit_spend_release(limit_hot_scope, limit_spend_release_counter)
      end
    )

  if map_size(benches) > 0 do
    Benchee.run(
      benches,
      warmup: bench_warmup,
      time: bench_time,
      parallel: bench_parallel,
      memory_time: 0,
      formatters: [Benchee.Formatters.Console]
    )
  end
after
  IO.puts("\nCleaning up bench data directory: #{bench_data_dir}")
  Application.stop(:ferricstore_server)
  Application.stop(:ferricstore)
  Process.sleep(500)
  File.rm_rf(bench_data_dir)
end
