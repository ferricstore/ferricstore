# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/native_cleanup_perf.exs
# Component benchmark of the real DOWN callback with unrelated live scopes.

defmodule FerricstoreBench.NativeCleanup do
  @source "apps/ferricstore_server/lib/ferricstore_server/native/resource_budget.ex"
  @output "bench/results/native-cleanup-perf.json"

  def run do
    source =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    replacement = """
      defp release_scoped_owner_leases(budget, owner) do
        case :ets.info(budget.scoped_owner_leases, :size) do
          size when size in [0, :undefined] -> MapSet.new()
          _nonempty ->
            Enum.reduce(@resources, MapSet.new(), fn resource, resources ->
              key = {owner, resource}
              case safe_take_lease(budget.scoped_owner_leases, key) do
                [{^key, amount}] ->
                  release_amount(budget, resource, amount)
                  MapSet.put(resources, resource)
                _already_released -> resources
              end
            end)
        end
      end

    """

    candidate =
      Regex.replace(
        ~r/  defp release_scoped_owner_leases\(budget, owner\) do.*?(?=  defp reclaim_dead_scoped_owners)/s,
        source,
        replacement
      )

    if candidate == source, do: raise("candidate transform failed")

    variants =
      for {name, code} <- [baseline: source, indexed: candidate] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        Code.compile_string(
          String.replace(
            code,
            "defmodule FerricstoreServer.Native.ResourceBudget do",
            "defmodule #{inspect(module)} do"
          )
        )

        {name, module}
      end

    {owner, monitor} = spawn_monitor(fn -> :ok end)

    receive do
      {:DOWN, ^monitor, :process, ^owner, :normal} -> :ok
    end

    results =
      for unrelated <- [0, 16, 128, 4_096],
          trial <- 1..3,
          {variant, module} <- if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
        {:ok, server} = module.start_link(name: module, scoped_sweep_interval_ms: 3_600_000)

        others =
          for _ <- List.duplicate(:owner, unrelated),
              do: spawn(fn -> Process.sleep(:infinity) end)

        try do
          state = :sys.get_state(server)
          table = state.budget.scoped_owner_leases
          for pid <- others, do: :ets.insert(table, {{pid, :executions}, 1})
          :atomics.put(state.budget.counters, 1, unrelated)
          ref = make_ref()
          state = %{state | monitors: %{ref => owner}, owner_monitors: %{owner => ref}}

          Task.async(fn ->
            for _ <- 1..100,
                do:
                  {:noreply, _} =
                    module.handle_info({:DOWN, ref, :process, owner, :normal}, state)

            timings =
              for _ <- 1..1_000 do
                started = System.monotonic_time(:nanosecond)

                {:noreply, next} =
                  module.handle_info({:DOWN, ref, :process, owner, :normal}, state)

                true = next.owner_monitors == %{}
                (System.monotonic_time(:nanosecond) - started) / 1_000
              end
              |> Enum.sort()

            true = :ets.info(table, :size) == unrelated
            true = :atomics.get(state.budget.counters, 1) == unrelated

            result = %{
              variant: variant,
              trial: trial,
              unrelated_scopes: unrelated,
              p50_us: Enum.at(timings, 499),
              p95_us: Enum.at(timings, 949),
              p99_us: Enum.at(timings, 989)
            }

            IO.puts(Jason.encode!(result))
            result
          end)
          |> Task.await(30_000)
        after
          GenServer.stop(server)
          for pid <- others, do: Process.exit(pid, :kill)
        end
      end

    File.mkdir_p!(Path.dirname(@output))

    File.write!(
      @output,
      Jason.encode!(%{baseline_source: source, candidate_source: candidate, results: results},
        pretty: true
      )
    )
  end
end

FerricstoreBench.NativeCleanup.run()
