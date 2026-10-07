defmodule FerricstoreBench.CompactionSyncVariant do
  @moduledoc false

  def prepare do
    mode = System.get_env("BENCH_COMPACTION_SYNC", "page")
    path = "apps/ferricstore/lib/ferricstore/store/shard/compound/promoted.ex"
    source = File.read!(path)

    case mode do
      "page" ->
        {mode, source, nil}

      "grouped" ->
        # Rejected experiment; only load its exact archived source in a fresh VM.
        source =
          "bench/results/compaction-copy-grouped-1.json"
          |> File.read!()
          |> Jason.decode!()
          |> Map.fetch!("source")

        root = Path.join([System.tmp_dir!(), "opencode", "compaction-sync-#{System.pid()}"])
        if File.exists?(root), do: raise("compaction sync fixture exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)
        [{module, beam}] = Code.compile_string(source, path)
        beam_path = Path.join(root, "#{module}.beam")
        File.write!(beam_path, beam)
        {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        {mode, source, root}

      "legacy_cleanup" ->
        source =
          "bench/results/separate-output-frozen-saturated-whole-1.json"
          |> File.read!()
          |> Jason.decode!()
          |> Map.fetch!("compaction_sync_source")

        root =
          Path.join([System.tmp_dir!(), "opencode", "legacy-cleanup-control-#{System.pid()}"])

        if File.exists?(root), do: raise("legacy control exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)
        [{module, beam}] = Code.compile_string(source, path)
        beam_path = Path.join(root, "#{module}.beam")
        File.write!(beam_path, beam)
        {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        {mode, source, root}

      other ->
        raise("invalid compaction sync mode: #{other}")
    end
  end
end
