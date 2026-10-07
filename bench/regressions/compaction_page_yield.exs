# MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/regressions/compaction_page_yield.exs
Code.require_file("../support/compaction_latch_variant.exs", __DIR__)
System.put_env("BENCH_COMPACTION_LATCH", System.get_env("BENCH_COMPACTION_LATCH", "pages"))
{mode, _, code_root} = FerricstoreBench.CompactionLatchVariant.prepare()
if mode == "whole", do: raise("this regression harness requires an explicit rejected candidate")
root = Path.join([System.tmp_dir!(), "opencode", "compaction-page-regression-#{System.pid()}"])
if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore, :waraft_single_hset_coalescing, false)
Application.put_env(:ferricstore, :test_data_dir_auto_cleanup, false)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ExUnit.start(autorun: false, formatters: [ExUnit.CLIFormatter])
  Code.require_file("compaction_page_yield_test.exs", __DIR__)
  %{failures: failures} = ExUnit.run()
  if failures > 0, do: raise("rejected compaction candidate regression failed")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  if code_root, do: File.rm_rf!(code_root)
end
