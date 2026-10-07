defmodule FerricstoreBench.FlowSnapshotVariant do
  @moduledoc false
  @baseline "e5f59ba7959710773729812343ae9bcd62d8d15f"
  @paths [
    "apps/ferricstore/lib/ferricstore/flow/history_projector.ex",
    "apps/ferricstore/lib/ferricstore/flow/lmdb_rebuilder.ex"
  ]

  def prepare do
    mode = System.get_env("BENCH_SNAPSHOT_PROJECTION", "bounded")
    true = mode in ["bounded", "legacy", "pages_only"]
    paths = if mode == "legacy", do: @paths, else: Enum.take(@paths, 1)
    root = Path.join([System.tmp_dir!(), "opencode", "flow-snapshot-#{System.pid()}"])

    root =
      if mode == "bounded" do
        nil
      else
        if File.exists?(root), do: raise("snapshot control exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)

        for path <- paths do
          {source, 0} = System.cmd("git", ["show", @baseline <> ":" <> path])
          [{module, beam}] = Code.compile_string(source, path)
          beam_path = Path.join(root, "#{module}.beam")
          File.write!(beam_path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        end

        root
      end

    sources =
      Map.new(@paths, fn path ->
        source =
          if mode != "bounded" and path in paths do
            {source, 0} = System.cmd("git", ["show", @baseline <> ":" <> path])
            source
          else
            File.read!(path)
          end

        {path, source}
      end)

    {mode, sources, root}
  end
end
