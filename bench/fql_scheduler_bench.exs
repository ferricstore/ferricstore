# Saturates the normal-scheduler FQL NIF while a heartbeat process measures
# scheduler delay. Run with MIX_ENV=bench mix run --no-start bench/fql_scheduler_bench.exs.

Code.require_file("support/query_performance.exs", __DIR__)

defmodule Ferricstore.Bench.FQLScheduler do
  alias Ferricstore.Bench.QueryPerformance
  alias FerricstoreServer.Native.NIF

  def run do
    schedulers = System.schedulers_online()

    concurrencies =
      QueryPerformance.integer_list_env(
        "BENCH_CONCURRENCY",
        Enum.uniq([1, schedulers, schedulers * 4]),
        min: 1
      )

    duration_ms = QueryPerformance.int_env("BENCH_DURATION_MS", 3_000, min: 100)
    heartbeat_ms = QueryPerformance.int_env("BENCH_HEARTBEAT_MS", 2, min: 1)
    sample_every = QueryPerformance.int_env("BENCH_SAMPLE_EVERY", 128, min: 1)
    max_bytes = 16 * 1024

    workloads = %{
      "point" => "FROM runs WHERE partition_key = @partition AND run_id = @run_id RETURN RECORD",
      "max_malformed" => "'" <> String.duplicate("x", max_bytes - 1)
    }

    metrics =
      for {workload, query} <- workloads,
          concurrency <- concurrencies,
          into: %{} do
        key = "#{workload}/concurrency-#{concurrency}"
        {key, run_case(key, query, concurrency, duration_ms, heartbeat_ms, sample_every)}
      end

    QueryPerformance.write_manual_metrics("fql-scheduler", metrics)
  end

  defp run_case(name, query, concurrency, duration_ms, heartbeat_ms, sample_every) do
    case NIF.parse_fql(query) do
      {:ok, _, _, _, _, _, _, _} -> :ok
      {:error, _, _} -> :ok
      invalid -> raise "unexpected FQL NIF result: #{inspect(invalid)}"
    end

    parent = self()
    heartbeat = spawn_link(fn -> heartbeat(parent, heartbeat_ms) end)
    started_ns = System.monotonic_time(:nanosecond)
    deadline_ns = started_ns + System.convert_time_unit(duration_ms, :millisecond, :nanosecond)

    tasks =
      for _ <- 1..concurrency do
        Task.async(fn -> worker(query, deadline_ns, sample_every, 0, []) end)
      end

    worker_results = Enum.map(tasks, &Task.await(&1, duration_ms + 30_000))
    elapsed_ns = System.monotonic_time(:nanosecond) - started_ns
    heartbeat_ref = make_ref()
    send(heartbeat, {:stop, heartbeat_ref})

    {heartbeat_samples, missed_heartbeats} =
      receive do
        {:heartbeat_samples, ^heartbeat_ref, samples, missed} -> {samples, missed}
      after
        5_000 -> raise "heartbeat process did not stop"
      end

    operations = Enum.reduce(worker_results, 0, fn {count, _samples}, total -> total + count end)
    operation_samples = Enum.flat_map(worker_results, &elem(&1, 1))
    heartbeat_summary = QueryPerformance.latency_summary(heartbeat_samples)
    operation_summary = QueryPerformance.latency_summary(operation_samples)
    ops_per_second = operations * 1_000_000_000 / max(elapsed_ns, 1)

    QueryPerformance.print_summary("#{name} heartbeat", heartbeat_summary)
    QueryPerformance.print_summary("#{name} operation", operation_summary)

    IO.puts(
      "#{name} operations=#{operations} ops_per_second=#{Float.round(ops_per_second, 2)} " <>
        "schedulers=#{System.schedulers_online()} p50=#{heartbeat_summary["p50_ns"]} " <>
        "p95=#{heartbeat_summary["p95_ns"]} p99=#{heartbeat_summary["p99_ns"]} " <>
        "missed_heartbeats=#{missed_heartbeats}"
    )

    Map.merge(heartbeat_summary, %{
      "operations" => operations,
      "ops_per_second" => ops_per_second,
      "operation_median_ns" => operation_summary["median_ns"],
      "operation_p95_ns" => operation_summary["p95_ns"],
      "operation_p99_ns" => operation_summary["p99_ns"],
      "missed_heartbeats" => missed_heartbeats,
      "concurrency" => concurrency,
      "query_bytes" => byte_size(query)
    })
  end

  defp worker(query, deadline_ns, sample_every, count, samples) do
    if System.monotonic_time(:nanosecond) >= deadline_ns do
      {count, Enum.reverse(samples)}
    else
      {elapsed_ns, _result} = QueryPerformance.timed_ns(fn -> NIF.parse_fql(query) end)
      next_count = count + 1

      next_samples =
        if rem(next_count, sample_every) == 0, do: [elapsed_ns | samples], else: samples

      worker(query, deadline_ns, sample_every, next_count, next_samples)
    end
  end

  defp heartbeat(parent, interval_ms) do
    interval_ns = System.convert_time_unit(interval_ms, :millisecond, :nanosecond)
    expected_ns = System.monotonic_time(:nanosecond) + interval_ns
    schedule_tick(expected_ns)
    heartbeat_loop(parent, interval_ns, expected_ns, [], 0)
  end

  defp heartbeat_loop(parent, interval_ns, expected_ns, samples, missed_heartbeats) do
    receive do
      :heartbeat ->
        now_ns = System.monotonic_time(:nanosecond)
        delay_ns = max(now_ns - expected_ns, 0)
        missed = div(delay_ns, interval_ns)
        next_expected_ns = expected_ns + (missed + 1) * interval_ns
        schedule_tick(next_expected_ns)

        heartbeat_loop(
          parent,
          interval_ns,
          next_expected_ns,
          [delay_ns | samples],
          missed_heartbeats + missed
        )

      {:stop, ref} ->
        send(parent, {
          :heartbeat_samples,
          ref,
          Enum.reverse(samples),
          missed_heartbeats
        })
    end
  end

  defp schedule_tick(expected_ns) do
    delay_ns = max(expected_ns - System.monotonic_time(:nanosecond), 0)
    delay_ms = ceil_delay_ms(delay_ns)
    Process.send_after(self(), :heartbeat, delay_ms)
  end

  defp ceil_delay_ms(delay_ns) do
    nanoseconds_per_millisecond = System.convert_time_unit(1, :millisecond, :nanosecond)
    div(delay_ns + nanoseconds_per_millisecond - 1, nanoseconds_per_millisecond)
  end
end

Ferricstore.Bench.FQLScheduler.run()
