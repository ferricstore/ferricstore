# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/auth_cache_maintenance_perf.exs
# Benchmarks the actual cache actor at its configured capacity, including misses.

defmodule FerricstoreBench.AuthCacheMaintenance do
  @source "apps/ferricstore_http/lib/ferricstore_http/auth/cache.ex"
  @output "bench/results/auth-cache-maintenance-perf.json"

  def run do
    source =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    expiration =
      """
          Enum.each(:ets.tab2list(state.table), fn
            {cache_key, _session, expires_at_ms, _last_used_ms} when expires_at_ms <= now_ms ->
              :ets.delete(state.table, cache_key)

            _active ->
              :ok
          end)
      """
      |> String.trim_trailing()

    candidate =
      source
      |> String.replace(
        expiration,
        "    :ets.select_delete(state.table, [{{:_, :_, :\"$1\", :_}, [{:\"=<\", :\"$1\", now_ms}], [true]}])"
      )
      |> String.replace(
        ":ets.foldl(&least_recent/2, nil, table)",
        "Enum.reduce(lru_metadata(table), nil, &least_recent/2)"
      )
      |> String.replace("|> :ets.tab2list()", "|> lru_metadata()")
      |> String.replace(
        "  defp least_recent(entry, nil), do: entry",
        """
          defp lru_metadata(table) do
            :ets.select(table, [{{:"$1", :_, :"$2", :"$3"}, [], [{{:"$1", nil, :"$2", :"$3"}}]}])
          end

          defp least_recent(entry, nil), do: entry
        """
        |> String.trim_trailing()
      )

    if candidate == source or String.contains?(candidate, expiration),
      do: raise("candidate transform failed")

    variants =
      for {name, code} <- [baseline: source, metadata_only: candidate] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          code
          |> String.replace(
            "defmodule FerricstoreHttp.Auth.Cache do",
            "defmodule #{inspect(module)} do"
          )
          |> Code.compile_string()

        {name, module}
      end

    {:ok, supervisor} = Task.Supervisor.start_link()

    try do
      results =
        for entries <- [1_000, 10_000],
            groups <- [0, 32],
            trial <- 1..3,
            {variant, module} <-
              if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
          session = %{
            subject: "bench",
            groups:
              if(groups == 0,
                do: [],
                else: Enum.map(1..groups, &%{name: "group-#{&1}", commands: ["GET", "HGET"]})
              )
          }

          {:ok, pid} =
            module.start_link(
              max_entries: entries,
              ttl_ms: 300_000,
              sweep_interval_ms: 3_600_000,
              task_supervisor: supervisor,
              clock: fn -> 1_000_000 end
            )

          try do
            for i <- 1..entries,
                do: :ets.insert(module, {<<i::64>>, session, 1_300_000, 1_000_000 - i})

            :erlang.garbage_collect(pid)

            sweep =
              measure(
                pid,
                fn ->
                  send(pid, :sweep)
                  module.stats()
                end,
                10
              )

            misses =
              measure(
                pid,
                fn ->
                  key = <<System.unique_integer([:positive, :monotonic]) + 1_000_000::64>>

                  {:ok, ^session, :miss} =
                    module.fetch(key, :scope, fn -> {:ok, session} end, 10_000)
                end,
                100
              )

            true = module.stats().entries == entries

            {:ok, ^session, :miss} =
              module.fetch("hit-control", :scope, fn -> {:ok, session} end, 10_000)

            hits =
              measure(
                pid,
                fn ->
                  {:ok, ^session, :hit} =
                    module.fetch("hit-control", :scope, fn -> raise("hit missed") end, 10_000)
                end,
                10_000
              )

            row = %{
              variant: variant,
              entries: entries,
              groups: groups,
              trial: trial,
              sweep: sweep,
              misses: misses,
              hits: hits
            }

            IO.puts(Jason.encode!(row))
            row
          after
            GenServer.stop(pid)
          end
        end

      File.mkdir_p!(Path.dirname(@output))

      File.write!(
        @output,
        Jason.encode!(%{baseline_source: source, candidate_source: candidate, results: results},
          pretty: true
        )
      )
    after
      Supervisor.stop(supervisor)
    end
  end

  defp measure(pid, operation, count) do
    :erlang.garbage_collect(pid)
    {:reductions, before} = Process.info(pid, :reductions)
    started = System.monotonic_time(:nanosecond)

    times =
      for _ <- 1..count do
        t0 = System.monotonic_time(:nanosecond)
        operation.()
        (System.monotonic_time(:nanosecond) - t0) / 1_000
      end

    elapsed = System.monotonic_time(:nanosecond) - started
    {:reductions, after_count} = Process.info(pid, :reductions)
    {:memory, memory} = Process.info(pid, :memory)
    sorted = Enum.sort(times)

    %{
      ops_per_second: count / (elapsed / 1.0e9),
      actor_reductions_per_op: (after_count - before) / count,
      actor_memory_after_bytes: memory,
      p50_us: Enum.at(sorted, ceil(count * 0.5) - 1),
      p95_us: Enum.at(sorted, ceil(count * 0.95) - 1),
      p99_us: Enum.at(sorted, ceil(count * 0.99) - 1)
    }
  end
end

FerricstoreBench.AuthCacheMaintenance.run()
