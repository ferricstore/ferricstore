# bench/flow_native_batch_bench.exs
#
# Flow native-command floor benchmark.
#
# This does not expose a production FLOW API. It measures the cost floor for a
# future native Flow Raft command by collapsing Flow mutations into one Raft
# batch/apply using existing storage primitives:
#   state KV record + due zset entry + history stream entry blob.
#
# Run:
#   MIX_ENV=bench mix run --no-start bench/flow_native_batch_bench.exs
#
# Options:
#   FLOW_PRIMITIVE_BACKLOGS=1000,10000
#   FLOW_PRIMITIVE_CLAIM=10
#   BENCH_WARMUP=1
#   BENCH_TIME=3
#   BENCH_PARALLEL=1

bench_warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
bench_parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()
claim_count = System.get_env("FLOW_PRIMITIVE_CLAIM", "10") |> String.to_integer()

backlogs =
  System.get_env("FLOW_PRIMITIVE_BACKLOGS", "1000,10000")
  |> String.split(",", trim: true)
  |> Enum.map(&String.to_integer/1)

bench_data_dir =
  Path.join(
    System.tmp_dir!(),
    "ferricstore_flow_primitive_bench_#{System.unique_integer([:positive])}"
  )

File.mkdir_p!(bench_data_dir)

Application.put_env(:ferricstore, :data_dir, bench_data_dir)
Application.put_env(:ferricstore, :port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :shard_count, 1)
Application.put_env(:ferricstore, :hot_cache_max_value_size, 512)

{:ok, _} = Application.ensure_all_started(:ferricstore)

IO.puts("=== FerricStore Flow Native Batch Bench ===")
IO.puts("Data dir: #{bench_data_dir}")

IO.puts(
  "backlogs=#{Enum.join(backlogs, ",")} claim=#{claim_count} warmup=#{bench_warmup}s time=#{bench_time}s parallel=#{bench_parallel}\n"
)

defmodule FlowNativeBatchBench do
  alias Ferricstore.Commands.SortedSet
  alias Ferricstore.Raft.{Batcher, ReplyAwaiter}
  alias Ferricstore.Store.CompoundKey

  @sep <<0>>

  def ctx, do: FerricStore.Instance.get(:default)

  def flow_id(prefix, i), do: "#{prefix}:#{Integer.to_string(i)}"
  def state_key(id), do: "flow:{" <> id <> "}:state"
  def history_key(id), do: "flow:{" <> id <> "}:history"
  def due_key(prefix), do: "flow_due:{" <> prefix <> "}:ready:p0"

  def record(id, state, version, opts \\ []) do
    :erlang.term_to_binary(%{
      id: id,
      state: state,
      version: version,
      attempts: Keyword.get(opts, :attempts, 0),
      next_run_at_ms: Keyword.get(opts, :next_run_at_ms, 1_000),
      lease_owner: Keyword.get(opts, :lease_owner),
      lease_token: Keyword.get(opts, :lease_token),
      lease_deadline_ms: Keyword.get(opts, :lease_deadline_ms, 0),
      payload_ref: "payload:" <> id,
      result_ref: Keyword.get(opts, :result_ref)
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

  def stream_entry_key(stream_key, id_str), do: "X:#{stream_key}" <> @sep <> id_str

  def stream_id(counter) do
    :counters.add(counter, 1, 1)

    Integer.to_string(System.system_time(:millisecond)) <>
      "-" <> Integer.to_string(:counters.get(counter, 1))
  end

  def due_put_cmd(due_key, id, score) do
    {:compound_put, CompoundKey.zset_member(due_key, id), Float.to_string(score), 0}
  end

  def due_delete_cmd(due_key, id), do: {:compound_delete, CompoundKey.zset_member(due_key, id)}

  def history_put_cmd(history_key, id_str, fields) do
    {:compound_put, stream_entry_key(history_key, id_str), :erlang.term_to_binary(fields), 0}
  end

  def create_cmds(prefix, id, event_counter, run_at_ms \\ 1_000) do
    due = due_key(prefix)

    [
      {:put, state_key(id), record(id, "scheduled", 1, next_run_at_ms: run_at_ms), 0},
      {:compound_put, CompoundKey.type_key(due), CompoundKey.encode_type(:zset), 0},
      due_put_cmd(due, id, run_at_ms * 1.0),
      history_put_cmd(history_key(id), stream_id(event_counter), event_fields("scheduled"))
    ]
  end

  def claim_cmds(prefix, id, event_counter, worker, lease_ms, version) do
    now_ms = System.system_time(:millisecond)
    token = worker <> ":" <> Integer.to_string(version)

    [
      due_delete_cmd(due_key(prefix), id),
      {:put, state_key(id),
       record(id, "running", version,
         lease_owner: worker,
         lease_token: token,
         lease_deadline_ms: now_ms + lease_ms
       ), 0},
      history_put_cmd(history_key(id), stream_id(event_counter), event_fields("claimed", worker))
    ]
  end

  def complete_cmds(id, event_counter, worker, version) do
    [
      {:put, state_key(id),
       record(id, "completed", version,
         lease_owner: nil,
         lease_token: nil,
         lease_deadline_ms: 0,
         result_ref: "result:" <> id
       ), 0},
      history_put_cmd(
        history_key(id),
        stream_id(event_counter),
        event_fields("completed", worker)
      )
    ]
  end

  def retry_cmds(prefix, id, event_counter, worker, attempt, version) do
    run_at_ms = 2_000 + attempt

    [
      {:put, state_key(id),
       record(id, "retry_scheduled", version,
         attempts: attempt,
         next_run_at_ms: run_at_ms,
         lease_owner: nil,
         lease_token: nil,
         lease_deadline_ms: 0
       ), 0},
      due_put_cmd(due_key(prefix), id, run_at_ms * 1.0),
      history_put_cmd(history_key(id), stream_id(event_counter), event_fields("retry", worker))
    ]
  end

  def raft_batch(commands) do
    {from, token} = ReplyAwaiter.new()
    Batcher.write_batch(0, commands, from)

    case ReplyAwaiter.await(token, 10_000, {:error, :timeout}) do
      {:ok, results} when is_list(results) -> results
      other -> raise "raft batch failed: #{inspect(other)}"
    end
  end

  def seed_due(prefix, count, event_counter) do
    Enum.each(1..count, fn i ->
      id = flow_id(prefix, i)
      results = raft_batch(create_cmds(prefix, id, event_counter))
      true = Enum.all?(results, &ok_result?/1)
    end)
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

  def claim_due(prefix, now_ms, count, event_counter) do
    ids = due_select(prefix, now_ms, count)

    commands =
      ids
      |> Enum.with_index(1)
      |> Enum.flat_map(fn {id, offset} ->
        claim_cmds(prefix, id, event_counter, "worker-a", 30_000, offset)
      end)

    results = raft_batch(commands)
    true = Enum.all?(results, &ok_result?/1)
    ids
  end

  def lifecycle(prefix, i, event_counter) do
    id = flow_id(prefix, i)
    true = Enum.all?(raft_batch(create_cmds(prefix, id, event_counter)), &ok_result?/1)
    [^id] = claim_due(prefix, 1_000, 1, event_counter)
    true = Enum.all?(raft_batch(complete_cmds(id, event_counter, "worker-a", 3)), &ok_result?/1)
    id
  end

  def retry_cycle(prefix, i, event_counter) do
    id = flow_id(prefix, i)
    true = Enum.all?(raft_batch(create_cmds(prefix, id, event_counter)), &ok_result?/1)
    [^id] = claim_due(prefix, 1_000, 1, event_counter)

    true =
      Enum.all?(
        raft_batch(retry_cmds(prefix, id, event_counter, "worker-a", rem(i, 5) + 1, 3)),
        &ok_result?/1
      )

    id
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
  def short(other), do: other

  defp ok_result?(:ok), do: true
  defp ok_result?({:ok, _}), do: true
  defp ok_result?(value) when is_binary(value), do: true
  defp ok_result?(_), do: false
end

try do
  Enum.each(backlogs, fn backlog ->
    prefix = "flowprimitive#{backlog}:#{System.unique_integer([:positive])}"
    claim_prefix = prefix <> ":claim"

    claim_seed_count =
      max(backlog * 5, claim_count * max(bench_warmup + bench_time, 1) * 250)

    event_counter = :counters.new(1, [:atomics])
    lifecycle_counter = :counters.new(1, [:atomics])
    retry_counter = :counters.new(1, [:atomics])

    IO.puts("\n--- backlog=#{backlog} ---")

    FlowNativeBatchBench.timed("seed native due backlog #{backlog}", fn ->
      FlowNativeBatchBench.seed_due(prefix, backlog, event_counter)
    end)

    FlowNativeBatchBench.timed("seed native claim backlog #{claim_seed_count}", fn ->
      FlowNativeBatchBench.seed_due(claim_prefix, claim_seed_count, event_counter)
    end)

    FlowNativeBatchBench.due_select(prefix, 1_000, claim_count)

    Benchee.run(
      %{
        "flow primitive due_select#{claim_count} backlog=#{backlog}" => fn ->
          result = FlowNativeBatchBench.due_select(prefix, 1_000, claim_count)
          true = length(result) <= claim_count
        end,
        "flow primitive claim_due#{claim_count} backlog=#{backlog}" => fn ->
          result = FlowNativeBatchBench.claim_due(claim_prefix, 1_000, claim_count, event_counter)
          true = length(result) == claim_count
        end,
        "flow primitive lifecycle create-claim-complete backlog=#{backlog}" => fn ->
          i = FlowNativeBatchBench.next_i(lifecycle_counter)
          id = FlowNativeBatchBench.lifecycle(prefix <> ":life", i, event_counter)
          true = is_binary(id)
        end,
        "flow primitive retry cycle backlog=#{backlog}" => fn ->
          i = FlowNativeBatchBench.next_i(retry_counter)
          id = FlowNativeBatchBench.retry_cycle(prefix <> ":retry", i, event_counter)
          true = is_binary(id)
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
