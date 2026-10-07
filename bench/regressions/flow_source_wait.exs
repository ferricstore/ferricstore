# MIX_ENV=test BENCH_FLOW_SOURCE_WAIT=sequential|batch ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/regressions/flow_source_wait.exs
Code.require_file("../support/flow_source_wait_variant.exs", __DIR__)
{mode, _, code_root} = FerricstoreBench.FlowSourceWaitVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "flow-source-regression-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore, :waraft_single_hset_coalescing, false)
Application.put_env(:ferricstore, :test_data_dir_auto_cleanup, false)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ExUnit.start(autorun: false, seed: 873_483, max_cases: 1, formatters: [ExUnit.CLIFormatter])

  Code.require_file(
    "../../apps/ferricstore/test/ferricstore/flow/lmdb_writer/source_wait_test.exs",
    __DIR__
  )

  %{failures: failures} = results = ExUnit.run()
  IO.inspect(%{mode: mode, results: results}, label: "SOURCE_WAIT_REGRESSION")

  if (mode == "batch" and failures != 0) or (mode == "sequential" and failures == 0),
    do: raise("source-wait regression did not distinguish the writer variants")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
