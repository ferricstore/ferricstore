# MIX_ENV=test ERL_FLAGS='+S 4:4' mise exec -- mix run --no-start bench/regressions/separated_compaction.exs
Code.require_file("../support/separated_compaction_variant.exs", __DIR__)
{_, code_root} = FerricstoreBench.SeparatedCompactionVariant.prepare()

root =
  Path.join([System.tmp_dir!(), "opencode", "separate-compaction-regression-#{System.pid()}"])

if File.exists?(root), do: raise("fixture exists")
Application.put_env(:ferricstore, :data_dir, root)
Application.put_env(:ferricstore, :node_name, nil)
Application.put_env(:ferricstore, :test_data_dir_auto_cleanup, false)
Application.put_env(:ferricstore, :waraft_single_hset_coalescing, false)
Logger.configure(level: :error)

try do
  {:ok, _} = Application.ensure_all_started(:ferricstore)
  ExUnit.start(autorun: false, formatters: [ExUnit.CLIFormatter])
  Code.require_file("separated_compaction_test.exs", __DIR__)
  %{failures: failures} = ExUnit.run()
  if failures > 0, do: raise("separate compaction regression failed")
after
  Application.stop(:ferricstore)
  File.rm_rf!(root)
  File.rm_rf!(code_root)
end
