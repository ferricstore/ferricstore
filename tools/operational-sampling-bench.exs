# elixir --erl '+S 2:2' -pa '_build/test/lib/*/ebin' tools/operational-sampling-bench.exs before.beam
[baseline_path] = System.argv()
module = Ferricstore.OperationalLimits
{:module, ^module} = Code.ensure_loaded(module)
{^module, current_code, current_path} = :code.get_object_code(module)
baseline_code = File.read!(baseline_path)
{:ok, _} = Application.ensure_all_started(:os_mon)

variants = [
  {"before", baseline_code, to_charlist(baseline_path)},
  {"after", current_code, current_path}
]

opts = [data_dir: System.tmp_dir!(), memory_bytes: 1_073_741_824, rss_bytes: 1]

for round <- 1..5,
    {label, code, path} <- if(rem(round, 2) == 0, do: Enum.reverse(variants), else: variants) do
  :code.purge(module)
  {:module, ^module} = :code.load_binary(module, path, code)
  for _ <- 1..10, do: module.snapshot(opts)
  {:reductions, before_reductions} = Process.info(self(), :reductions)
  {before_cpu, _} = :erlang.statistics(:runtime)

  {elapsed_us, _} =
    :timer.tc(fn ->
      for _ <- 1..200 do
        %{disk: %{total_bytes: total}, memory: %{limit_bytes: 1_073_741_824}} =
          module.snapshot(opts)

        true = total > 0
      end
    end)

  {after_cpu, _} = :erlang.statistics(:runtime)
  {:reductions, after_reductions} = Process.info(self(), :reductions)

  IO.inspect(%{
    round: round,
    variant: label,
    samples: 200,
    wall_us_per_sample: elapsed_us / 200,
    vm_cpu_us_per_sample: (after_cpu - before_cpu) * 1_000 / 200,
    reductions_per_sample: (after_reductions - before_reductions) / 200
  })
end
