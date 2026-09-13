# After a test build:
# elixir --erl '+S 16:16' -pa '_build/test/lib/*/ebin' tools/idle-cpu-bench.exs [provider.beam]
# Each run uses fresh storage, ephemeral ports, and the production scheduler.
[beam, router_beam | _] = System.argv() ++ [nil, nil]

if beam do
  module = :ferricstore_waraft_spike_segment_log
  {:module, ^module} = :code.load_binary(module, to_charlist(beam), File.read!(beam))
end

if router_beam do
  module = Ferricstore.Store.Router

  {:module, ^module} =
    :code.load_binary(module, to_charlist(router_beam), File.read!(router_beam))
end

if limits_beam = System.get_env("FERRICSTORE_IDLE_LIMITS_BEAM") do
  module = Ferricstore.OperationalLimits

  {:module, ^module} =
    :code.load_binary(module, to_charlist(limits_beam), File.read!(limits_beam))
end

root = Path.join(System.tmp_dir!(), "ferricstore-idle-bench-#{System.pid()}")
config = Config.Reader.read!("config/config.exs", env: :prod, target: :host)
Application.put_all_env(config)
Application.put_env(:logger, :level, :warning)
Application.put_env(:libcluster, :topologies, :disabled)
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :shard_count, 16)
Application.put_env(:ferricstore, :native_port, 0)
Application.put_env(:ferricstore, :health_port, 0)
Application.put_env(:ferricstore, :health_probe_port, 0)
Application.put_env(:ferricstore, :flow_scheduler_enabled, true)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore_http, :enabled, false)
Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:os_mon)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore_server)
  Process.sleep(5_000)
  ctx = FerricStore.Instance.get(:default)
  writes = fn -> for i <- 1..ctx.shard_count, do: :counters.get(ctx.write_version, i) end

  reductions = fn ->
    Enum.reduce(Process.list(), 0, fn pid, total ->
      case Process.info(pid, [:initial_call, :reductions]) do
        [initial_call: {:erts_literal_area_collector, _, _}, reductions: n] -> total + n
        _ -> total
      end
    end)
  end

  process_snapshot = fn ->
    if System.get_env("FERRICSTORE_IDLE_PROFILE") == "1" do
      Map.new(Process.list(), fn pid ->
        {pid,
         Process.info(pid, :reductions)
         |> then(fn
           {:reductions, n} -> n
           _ -> 0
         end)}
      end)
    else
      %{}
    end
  end

  variants =
    if baseline_path = System.get_env("FERRICSTORE_IDLE_COMPARE_LIMITS_BEAM") do
      module = Ferricstore.OperationalLimits
      {^module, current_code, current_path} = :code.get_object_code(module)
      before = {"before", File.read!(baseline_path), to_charlist(baseline_path)}
      after_variant = {"after", current_code, current_path}

      Enum.flat_map(1..3, fn round ->
        if rem(round, 2) == 0, do: [after_variant, before], else: [before, after_variant]
      end)
    else
      List.duplicate({"current", nil, nil}, 3)
    end

  for {{variant, code, path}, sample} <- Enum.with_index(variants, 1) do
    if code do
      module = Ferricstore.OperationalLimits
      :code.purge(module)
      {:module, ^module} = :code.load_binary(module, path, code)
      Process.sleep(1_000)
    end

    before_writes = writes.()
    before_reductions = reductions.()
    before_processes = process_snapshot.()
    {before_cpu, _} = :erlang.statistics(:runtime)
    before_wall = System.monotonic_time(:millisecond)
    Process.sleep(10_000)
    wall_ms = System.monotonic_time(:millisecond) - before_wall
    {after_cpu, _} = :erlang.statistics(:runtime)

    IO.inspect(%{
      sample: sample,
      variant: variant,
      beam: beam || "current",
      wall_ms: wall_ms,
      vm_cpu_percent_of_one_core: Float.round(100 * (after_cpu - before_cpu) / wall_ms, 2),
      literal_collector_reductions: reductions.() - before_reductions,
      shard_write_delta: Enum.sum(writes.()) - Enum.sum(before_writes)
    })

    if map_size(before_processes) > 0 do
      process_snapshot.()
      |> Enum.map(fn {pid, n} -> {pid, n - Map.get(before_processes, pid, n)} end)
      |> Enum.sort_by(&elem(&1, 1), :desc)
      |> Enum.take(10)
      |> Enum.map(fn {pid, n} ->
        {pid, n, Process.info(pid, [:registered_name, :initial_call, :current_function])}
      end)
      |> IO.inspect(label: "hot processes", limit: :infinity)
    end
  end
after
  Application.stop(:ferricstore_server)
  Application.stop(:ferricstore)
  File.rm_rf!(root)
end
