defmodule FerricstoreBench.FlowCacheVariant do
  @moduledoc false
  @paths [
    "apps/ferricstore/lib/ferricstore/raft/waraft_segment_reader.ex",
    "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder/cold_state.ex"
  ]
  @archive "bench/results/flow-cache-current-stall-control-1.json"

  def prepare do
    mode = System.get_env("BENCH_FLOW_CACHE", "retained")
    true = mode in ["retained", "legacy", "batched", "select_only", "batch_only"]

    replaced =
      case mode do
        "batched" -> @paths
        "select_only" -> Enum.take(@paths, 1)
        "batch_only" -> Enum.drop(@paths, 1)
        _retained -> []
      end

    archive =
      if replaced == [] do
        nil
      else
        report = @archive |> File.read!() |> Jason.decode!()
        sources = Map.fetch!(report, "cache_sources")
        hashes = get_in(report, ["publication_identity", "sha256"])

        for path <- @paths do
          expected = Map.fetch!(hashes, path)

          ^expected =
            Base.encode16(:crypto.hash(:sha256, Map.fetch!(sources, path)), case: :lower)
        end

        sources
      end

    sources =
      Map.new(@paths, fn path ->
        {path, if(path in replaced, do: Map.fetch!(archive, path), else: File.read!(path))}
      end)

    root =
      if replaced == [] do
        nil
      else
        root = Path.join([System.tmp_dir!(), "opencode", "flow-cache-#{System.pid()}"])
        if File.exists?(root), do: raise("cache control exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)

        for path <- @paths, path in replaced do
          [{module, beam}] = Code.compile_string(Map.fetch!(sources, path), path)
          beam_path = Path.join(root, "#{module}.beam")
          File.write!(beam_path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        end

        root
      end

    {mode, sources, root}
  end
end
