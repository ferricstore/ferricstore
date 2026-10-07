# ERL_FLAGS='+S 8:8' mise exec -- mix run --no-start bench/waiter_churn_perf.exs
# Real blocking workers with a large unrelated waiter population. Outbound
# leases hold each completed worker alive so its reductions can be measured.

defmodule FerricstoreBench.WaiterChurn do
  alias FerricstoreServer.Native.{OutboundBudget, Session}
  alias Ferricstore.Waiters
  @source "apps/ferricstore_server/lib/ferricstore_server/native/blocking.ex"
  @output "bench/results/waiter-churn-perf.json"

  def run do
    source =
      if File.exists?(@output),
        do: Jason.decode!(File.read!(@output))["baseline_source"],
        else: File.read!(@source)

    candidate =
      source
      |> String.replace("Waiters.cleanup(self())", "cleanup_list_waiters(keys)")
      |> String.replace(
        "BlockingCmd.parse_blmove_args(args) do",
        "BlockingCmd.parse_blmove_args(args) do\n      keys = [source]"
      )
      |> String.replace("finish_list_pop(", "finish_list_pop(keys, ")
      |> String.replace(
        "  defp finish_list_pop(keys, key, store, result) do",
        """
          defp cleanup_list_waiters(keys) do
            Enum.each(keys, &Waiters.unregister(&1, self()))
          end

          defp finish_list_pop(keys, key, store, result) do
        """
        |> String.trim_trailing()
      )

    variants =
      for {name, code} <- [baseline: source, scoped: candidate] do
        module = Module.concat(__MODULE__, name |> to_string() |> Macro.camelize())

        [{^module, _}] =
          code
          |> String.replace(
            "defmodule FerricstoreServer.Native.Blocking do",
            "defmodule #{inspect(module)} do"
          )
          |> Code.compile_string()

        {name, module}
      end

    root = Path.join(System.tmp_dir!(), "ferricstore-waiter-churn-#{System.pid()}")
    if File.exists?(root), do: raise("fixture exists")

    for {key, value} <- [
          data_dir: root,
          node_name: nil,
          shard_count: 4,
          native_port: 0,
          health_port: 0,
          health_probe_port: 0
        ],
        do: Application.put_env(:ferricstore, key, value)

    Logger.configure(level: :error)

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      ctx = FerricStore.Instance.get(:default)

      results =
        for population <- [0, 512, 4_096] do
          parent = self()

          owners =
            for {_entry, i} <- List.duplicate(:entry, population) |> Enum.with_index() do
              spawn(fn ->
                Waiters.register("parked:#{i}", self(), 0)
                send(parent, {:parked, self()})
                Process.sleep(:infinity)
              end)
            end

          for pid <- owners do
            receive do
              {:parked, ^pid} -> :ok
            after
              10_000 -> raise("parked waiter failed to start")
            end
          end

          try do
            for trial <- 1..3,
                {variant, module} <-
                  if(rem(trial, 2) == 0, do: Enum.reverse(variants), else: variants) do
              counter = OutboundBudget.new_counter()

              state = %{
                instance_ctx: ctx,
                acl_cache: :full_access,
                require_auth: false,
                authenticated: true,
                resource_budget: FerricstoreServer.Native.ResourceBudget,
                outbound_counter: counter,
                max_outbound_bytes: 1_000_000
              }

              key = "churn:source"

              {:ok, prepared} =
                Session.prepare_command(%{"command" => "BLPOP", "args" => [key, "0"]})

              samples =
                for _ <- 1..30 do
                  meta = %{request_id: make_ref()}
                  {:ok, worker, monitor} = module.start_prepared(prepared, state, meta)
                  await_registration(key, worker, System.monotonic_time(:millisecond) + 5_000)
                  started = System.monotonic_time(:microsecond)
                  1 = Ferricstore.Commands.List.handle_ast({:rpush, [key, "value"]}, ctx)

                  lease =
                    receive do
                      {:native_blocking_response_budgeted, ^meta, ^worker, :ok, [^key, "value"],
                       lease} ->
                        lease
                    after
                      5_000 -> raise("worker response timed out")
                    end

                  elapsed = System.monotonic_time(:microsecond) - started
                  {:reductions, reductions} = Process.info(worker, :reductions)
                  :ok = OutboundBudget.release(lease)
                  send(worker, {:native_blocking_outbound_released, lease.resource_token})

                  receive do
                    {:DOWN, ^monitor, :process, ^worker, :normal} -> :ok
                  after
                    5_000 -> raise("worker did not exit")
                  end

                  %{us: elapsed, reductions: reductions}
                end

              true = Waiters.total_count() == population

              result = %{
                population: population,
                variant: variant,
                trial: trial,
                p50_us: percentile(samples, :us, 0.5),
                p99_us: percentile(samples, :us, 0.99),
                p50_reductions: percentile(samples, :reductions, 0.5)
              }

              IO.puts(Jason.encode!(result))
              result
            end
          after
            started = System.monotonic_time(:microsecond)
            for owner <- owners, do: Process.exit(owner, :kill)
            await_empty(System.monotonic_time(:millisecond) + 30_000)

            drain = %{
              population: population,
              elapsed_us: System.monotonic_time(:microsecond) - started,
              remaining: Waiters.total_count()
            }

            Process.put({__MODULE__, :drains}, [drain | Process.get({__MODULE__, :drains}, [])])
            IO.puts("WAITER_DRAIN " <> Jason.encode!(drain))
          end
        end
        |> List.flatten()

      File.mkdir_p!(Path.dirname(@output))

      File.write!(
        @output,
        Jason.encode!(
          %{
            baseline_source: source,
            candidate_source: candidate,
            results: results,
            drains: Process.get({__MODULE__, :drains}, [])
          },
          pretty: true
        )
      )
    after
      Application.stop(:ferricstore_server)
      Application.stop(:ferricstore)
      File.rm_rf!(root)
    end
  end

  defp await_registration(key, pid, deadline) do
    if Enum.any?(:ets.lookup(:ferricstore_waiters, key), &(elem(&1, 1) == pid)) do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline, do: raise("waiter registration timeout")
      :erlang.yield()
      await_registration(key, pid, deadline)
    end
  end

  defp await_empty(deadline) do
    if Waiters.total_count() == 0 do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline, do: raise("waiter cleanup timed out")
      Process.sleep(5)
      await_empty(deadline)
    end
  end

  defp percentile(samples, key, q),
    do:
      samples
      |> Enum.map(&Map.fetch!(&1, key))
      |> Enum.sort()
      |> Enum.at(ceil(length(samples) * q) - 1)
end

FerricstoreBench.WaiterChurn.run()
