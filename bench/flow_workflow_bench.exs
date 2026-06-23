# bench/flow_workflow_bench.exs
#
# Flow-like mixed primitive workload.
#
# This benchmarks the shape described in docs/ferricstore-flow-design.md:
# latest flow record in KV, due/retry index in sorted set, and audit/history in
# streams. It is intentionally built from current public primitives, so numbers
# include today's multi-command cost rather than a future native FLOW Raft command.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/flow_workflow_bench.exs
#
# Options:
#   FLOW_BENCH_BACKLOGS=1000,10000
#   FLOW_BENCH_CLAIM=10
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
claim_count = System.get_env("FLOW_BENCH_CLAIM", "10") |> String.to_integer()

backlogs =
  System.get_env("FLOW_BENCH_BACKLOGS", "1000,10000")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(System.tmp_dir!(), "ferricstore_flow_bench_#{System.unique_integer([:positive])}")

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Flow-like Workflow Bench ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "backlogs=#{Enum.join(backlogs, ",")} claim=#{claim_count} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel}\n"
)

defmodule FlowWorkflowBench do
  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Commands.Stream

  def ctx, do: FerricStore.Instance.get(:default)

  def flow_id(prefix, i), do: "#{prefix}:#{Integer.to_string(i)}"
  def flow_key(id), do: "flow:{" <> id <> "}:state"
  def history_key(id), do: "flow:{" <> id <> "}:history"
  def due_key(prefix), do: "flow_due:{" <> prefix <> "}:ready:p0"

  def record(id, state, attempts \\ 0) do
    :erlang.term_to_binary(%{
      id: id,
      state: state,
      attempts: attempts,
      next_run_at_ms: 1_000,
      payload: "payload:" <> id
    })
  end

  def event_fields(event, worker \\ "w1") do
    [
      "event",
      event,
      "worker",
      worker,
      "at",
      Integer.to_string(System.monotonic_time(:millisecond))
    ]
  end

  def schedule(prefix, i) do
    id = flow_id(prefix, i)
    :ok = FerricStore.set(flow_key(id), record(id, "scheduled"))
    {:ok, added} = FerricStore.zadd(due_key(prefix), [{1_000.0, id}])
    true = added in [0, 1]
    {:ok, _stream_id} = FerricStore.xadd(history_key(id), event_fields("scheduled"))
    id
  end

  def claim_one(prefix, id) do
    {:ok, _removed} = FerricStore.zrem(due_key(prefix), [id])
    :ok = FerricStore.set(flow_key(id), record(id, "running"))
    {:ok, _stream_id} = FerricStore.xadd(history_key(id), event_fields("claimed"))
    id
  end

  def complete_one(id) do
    :ok = FerricStore.set(flow_key(id), record(id, "completed"))
    {:ok, _stream_id} = FerricStore.xadd(history_key(id), event_fields("completed"))
    _trimmed = Stream.handle_ast({:xtrim, history_key(id), {:maxlen, false, 100}}, ctx())
    :ok
  end

  def retry_one(prefix, id, attempt) do
    :ok = FerricStore.set(flow_key(id), record(id, "retry_scheduled", attempt))
    {:ok, _added} = FerricStore.zadd(due_key(prefix), [{2_000.0 + attempt, id}])
    {:ok, _stream_id} = FerricStore.xadd(history_key(id), event_fields("retry"))
    id
  end

  def history_read(id, count) do
    {:ok, entries} = FerricStore.xrange(history_key(id), "-", "+", count: count)
    entries
  end

  def due_select(prefix, now_ms, count) do
    SortedSet.handle(
      "ZRANGEBYSCORE",
      [
        due_key(prefix),
        "-inf",
        Integer.to_string(now_ms),
        "LIMIT",
        "0",
        Integer.to_string(count)
      ],
      ctx()
    )
  end

  def claim_due(prefix, now_ms, count) do
    ids = due_select(prefix, now_ms, count)
    Enum.each(ids, &claim_one(prefix, &1))
    ids
  end

  def lifecycle(prefix, i) do
    id = schedule(prefix, i)
    [_id] = claim_due(prefix, 1_000, 1)
    complete_one(id)
    history_read(id, 10)
  end

  def retry_cycle(prefix, i) do
    id = schedule(prefix, i)
    [^id] = claim_due(prefix, 1_000, 1)
    retry_one(prefix, id, rem(i, 5) + 1)
    history_read(id, 10)
  end

  def seed_due(prefix, count) do
    Enum.each(1..count, fn i -> schedule(prefix, i) end)
  end

  def seed_history(prefix, count) do
    Enum.each(1..count, fn i ->
      id = flow_id(prefix, i)
      :ok = FerricStore.set(flow_key(id), record(id, "completed"))

      Enum.each(["scheduled", "claimed", "completed"], fn event ->
        {:ok, _} = FerricStore.xadd(history_key(id), event_fields(event))
      end)
    end)
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
  def short({:ok, list}) when is_list(list), do: {:ok, length(list)}
  def short(other), do: other
end

try do
  Enum.each(backlogs, fn backlog ->
    prefix = "flowbench#{backlog}:#{System.unique_integer([:positive])}"
    claim_prefix = prefix <> ":claim"
    lifecycle_counter = :counters.new(1, [])
    retry_counter = :counters.new(1, [])

    IO.puts("\n--- backlog=#{backlog} ---")

    FlowWorkflowBench.timed("seed due backlog #{backlog}", fn ->
      FlowWorkflowBench.seed_due(prefix, backlog)
    end)

    FlowWorkflowBench.timed("seed claim backlog #{backlog}", fn ->
      FlowWorkflowBench.seed_due(claim_prefix, backlog)
    end)

    FlowWorkflowBench.timed("seed history #{backlog}", fn ->
      FlowWorkflowBench.seed_history(prefix <> ":history", backlog)
    end)

    FlowWorkflowBench.due_select(prefix, 1_000, claim_count)

    FlowWorkflowBench.history_read(
      FlowWorkflowBench.flow_id(prefix <> ":history", div(backlog, 2)),
      10
    )

    Benchee.run(
      %{
        "flow due_select#{claim_count} backlog=#{backlog}" => fn ->
          result = FlowWorkflowBench.due_select(prefix, 1_000, claim_count)
          true = length(result) <= claim_count
        end,
        "flow claim_due#{claim_count} backlog=#{backlog}" => fn ->
          result = FlowWorkflowBench.claim_due(claim_prefix, 1_000, claim_count)
          true = length(result) == claim_count
        end,
        "flow lifecycle create-claim-complete backlog=#{backlog}" => fn ->
          i = FlowWorkflowBench.next_i(lifecycle_counter)
          entries = FlowWorkflowBench.lifecycle(prefix <> ":life", i)
          true = length(entries) >= 3
        end,
        "flow retry cycle backlog=#{backlog}" => fn ->
          i = FlowWorkflowBench.next_i(retry_counter)
          entries = FlowWorkflowBench.retry_cycle(prefix <> ":retry", i)
          true = length(entries) >= 3
        end,
        "flow history read10 backlog=#{backlog}" => fn ->
          id =
            FlowWorkflowBench.flow_id(
              prefix <> ":history",
              rem(System.unique_integer([:positive]), backlog) + 1
            )

          entries = FlowWorkflowBench.history_read(id, 10)
          true = length(entries) <= 10
        end
      },
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
