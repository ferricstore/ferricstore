# Run with MIX_ENV=test mix run --no-start tools/native-resource-budget-bench.exs.
alias FerricstoreServer.Native.ResourceBudget

iterations = String.to_integer(System.get_env("BUDGET_BENCH_ITERATIONS", "10000"))

budget_module =
  case System.get_env("BUDGET_BENCH_BASELINE_REF") do
    nil ->
      ResourceBudget

    ref ->
      path = "apps/ferricstore_server/lib/ferricstore_server/native/resource_budget.ex"
      {source, 0} = System.cmd("git", ["show", ref <> ":" <> path])
      {:defmodule, metadata, [_name, body]} = Code.string_to_quoted!(source)

      Code.compile_quoted(
        {:defmodule, metadata, [FerricstoreServer.Native.ResourceBudgetBaseline, body]}
      )

      FerricstoreServer.Native.ResourceBudgetBaseline
  end

{:ok, budget} =
  budget_module.start_link(
    name: :resource_budget_benchmark,
    limits: %{executions: 128, inbound_bytes: 1_048_576}
  )

try do
  for workers <- [1, 8], mode <- [:scoped, :transferable, :resize], round <- 1..3 do
    operation =
      case mode do
        :scoped ->
          fn ->
            {:ok, lease} = budget_module.acquire_scoped(budget, :executions, 1)
            :ok = budget_module.release_scoped(lease)
          end

        :transferable ->
          fn ->
            {:ok, lease} = budget_module.acquire(budget, :executions, self(), 1)
            :ok = budget_module.release(budget, lease)
          end

        :resize ->
          fn ->
            {:ok, lease} = budget_module.acquire(budget, :inbound_bytes, self(), 64)
            :ok = budget_module.resize(budget, lease, 128)
            :ok = budget_module.resize(budget, lease, 64)
            :ok = budget_module.release(budget, lease)
          end
      end

    :erlang.garbage_collect()
    {reductions, _} = :erlang.statistics(:reductions)
    {cpu_ms, _} = :erlang.statistics(:runtime)

    {wall_us, _} =
      :timer.tc(fn ->
        1..workers
        |> Task.async_stream(
          fn _ ->
            Enum.each(1..iterations, fn _ -> operation.() end)
          end,
          max_concurrency: workers,
          timeout: 120_000
        )
        |> Enum.each(fn {:ok, :ok} -> :ok end)
      end)

    {end_cpu_ms, _} = :erlang.statistics(:runtime)
    {end_reductions, _} = :erlang.statistics(:reductions)

    unless Enum.all?(budget_module.usage(budget), fn {_, used} -> used == 0 end),
      do: raise("resource lease leaked during benchmark")

    IO.inspect(
      %{
        implementation: budget_module,
        mode: mode,
        workers: workers,
        round: round,
        iterations: iterations * workers,
        wall_us: wall_us,
        cpu_ms: end_cpu_ms - cpu_ms,
        reductions: end_reductions - reductions
      },
      label: "resource_budget_benchmark",
      limit: :infinity
    )
  end

  idle_clients = String.to_integer(System.get_env("BUDGET_BENCH_IDLE_CLIENTS", "0"))

  if idle_clients > 0 do
    parent = self()

    clients =
      for _ <- 1..idle_clients do
        spawn_monitor(fn ->
          {:ok, token} = budget_module.acquire(budget, :executions, self(), 0)
          :ok = budget_module.release(budget, token)
          send(parent, :idle_client_ready)
          receive do: (:stop -> :ok)
        end)
      end

    Enum.each(clients, fn _ ->
      receive do: (:idle_client_ready -> :ok)
    end)

    Process.sleep(200)
    {:reductions, before_reductions} = Process.info(budget, :reductions)
    {before_cpu, _} = :erlang.statistics(:runtime)
    Process.sleep(1_000)
    {after_cpu, _} = :erlang.statistics(:runtime)
    {:reductions, after_reductions} = Process.info(budget, :reductions)

    IO.inspect(
      %{
        implementation: budget_module,
        idle_clients: idle_clients,
        observation_ms: 1_000,
        coordinator_reductions: after_reductions - before_reductions,
        cpu_ms: after_cpu - before_cpu
      },
      label: "resource_budget_idle_benchmark"
    )

    Enum.each(clients, fn {pid, ref} ->
      send(pid, :stop)
      receive do: ({:DOWN, ^ref, :process, ^pid, :normal} -> :ok)
    end)
  end
after
  GenServer.stop(budget)
end
