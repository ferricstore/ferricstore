# Explicit archived-candidate regression; normal application runs retain verified sources.
Code.require_file("../support/flow_cache_variant.exs", __DIR__)
System.put_env("BENCH_FLOW_CACHE", "batched")
{_, _, code_root} = FerricstoreBench.FlowCacheVariant.prepare()
root = Path.join([System.tmp_dir!(), "opencode", "flow-cache-regression-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore, :test_data_dir_auto_cleanup, false)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ExUnit.start(autorun: false, seed: 873_483, formatters: [ExUnit.CLIFormatter])
  Code.require_file("lmdb_rebuilder_batch_durability_test.exs", __DIR__)
  %{failures: failures} = ExUnit.run()
  if failures > 0, do: raise("archived cache batching regression failed")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
