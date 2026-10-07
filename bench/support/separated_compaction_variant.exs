defmodule FerricstoreBench.SeparatedCompactionVariant do
  @moduledoc false

  def prepare do
    bundle =
      "bench/results/separate-output-candidate-source.json" |> File.read!() |> Jason.decode!()

    sources = Map.fetch!(bundle, "sources")
    root = Path.join([System.tmp_dir!(), "opencode", "separate-compaction-#{System.pid()}"])
    if File.exists?(root), do: raise("candidate directory exists")
    File.mkdir_p!(root)

    Code.compiler_options(
      ignore_module_conflict: true,
      no_warn_undefined: [{Ferricstore.Store.Promotion, :with_compaction_turn, 3}]
    )

    paths = [
      "apps/ferricstore/lib/ferricstore/store/compaction_plan.ex",
      "apps/ferricstore/lib/ferricstore/store/promotion.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/compound/separated_compaction.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/startup.ex",
      "apps/ferricstore/lib/ferricstore/store/shard/info.ex",
      "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/snapshot_metadata.ex"
    ]

    for path <- paths do
      source = Map.fetch!(sources, path)
      true = Base.encode16(:crypto.hash(:sha256, source), case: :lower) == bundle["sha256"][path]
      compile_and_load(source, path, root)
    end

    for path <- [
          "apps/ferricstore/lib/ferricstore/store/shard.ex",
          "apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex"
        ] do
      compile_and_load(File.read!(path), path, root)
    end

    Application.put_env(:ferricstore, :promoted_compaction_layout, :separate)
    true = function_exported?(Ferricstore.Store.Promotion, :with_compaction_turn, 3)
    {sources, root}
  end

  defp compile_and_load(source, path, root) do
    for {module, beam} <- Code.compile_string(source, path) do
      filename = Path.join(root, "#{module}.beam")
      File.write!(filename, beam)
      {:module, ^module} = :code.load_binary(module, String.to_charlist(filename), beam)
    end
  end
end
