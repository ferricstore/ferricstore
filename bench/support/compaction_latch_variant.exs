defmodule FerricstoreBench.CompactionLatchVariant do
  @moduledoc false

  Code.require_file("separated_compaction_variant.exs", __DIR__)

  def prepare do
    mode = System.get_env("BENCH_COMPACTION_LATCH", "whole")
    path = "apps/ferricstore/lib/ferricstore/store/shard/info.ex"
    source = File.read!(path)

    case mode do
      "whole" ->
        Application.put_env(:ferricstore, :promoted_compaction_layout, :whole)
        {mode, source, nil}

      "separate" ->
        {sources, root} = FerricstoreBench.SeparatedCompactionVariant.prepare()

        {mode,
         %{
           info: sources[path],
           separate:
             sources[
               "apps/ferricstore/lib/ferricstore/store/shard/compound/separated_compaction.ex"
             ]
         }, root}

      candidate when candidate in ["pages", "single_page"] ->
        archive =
          if candidate == "pages",
            do: "compaction-latch-four-page-saturated-pages-1.json",
            else: "compaction-latch-pilot-saturated-pages-1.json"

        report = Path.join("bench/results", archive) |> File.read!() |> Jason.decode!()
        source = Map.fetch!(report, "compaction_latch_source")
        promoted = Map.fetch!(report, "compaction_sync_source")
        compound_path = "apps/ferricstore/lib/ferricstore/store/shard/compound.ex"
        compound = File.read!(compound_path)

        needle = """
          def compact_dedicated_result_latched(state, redis_key, dedicated_path),
            do: Promoted.compact_dedicated_result_latched(state, redis_key, dedicated_path)
        """

        true = String.contains?(compound, needle)

        compound =
          String.replace(
            compound,
            needle,
            needle <>
              """

                @doc false
                def compact_dedicated_result_latched(state, redis_key, dedicated_path, latch_token),
                  do: Promoted.compact_dedicated_result_latched(state, redis_key, dedicated_path, latch_token)
              """
          )

        root =
          Path.join([System.tmp_dir!(), "opencode", "compaction-latch-variant-#{System.pid()}"])

        if File.exists?(root), do: raise("variant fixture exists")
        File.mkdir_p!(root)

        Code.compiler_options(
          ignore_module_conflict: true,
          no_warn_undefined: [
            {Ferricstore.Store.Shard.Compound.Promoted, :compact_dedicated_result_latched, 4},
            {Ferricstore.Store.Shard.Compound, :compact_dedicated_result_latched, 4}
          ]
        )

        modules =
          Code.compile_string(
            promoted,
            "apps/ferricstore/lib/ferricstore/store/shard/compound/promoted.ex"
          ) ++
            Code.compile_string(compound, compound_path) ++
            Code.compile_string(source, path) ++
            Code.compile_file("apps/ferricstore/lib/ferricstore/store/shard.ex")

        for {module, beam} <- modules do
          beam_path = Path.join(root, "#{module}.beam")
          File.write!(beam_path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        end

        true =
          function_exported?(
            Ferricstore.Store.Shard.Compound.Promoted,
            :compact_dedicated_result_latched,
            4
          )

        true =
          function_exported?(
            Ferricstore.Store.Shard.Compound,
            :compact_dedicated_result_latched,
            4
          )

        {mode, %{info: source, promoted: promoted, compound: compound}, root}

      other ->
        raise("invalid compaction latch variant: #{other}")
    end
  end
end
