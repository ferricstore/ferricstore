defmodule FerricstoreBench.SnapshotCopyVariant do
  @moduledoc false
  @path "apps/ferricstore/lib/ferricstore/raft/waraft_storage/sections/snapshot_install.ex"

  def prepare do
    mode = System.get_env("BENCH_SNAPSHOT_COPY", "baseline")

    case mode do
      "baseline" ->
        {mode, File.read!(@path), nil}

      "single_pass" ->
        # Rejected optimization: normal runs keep production synchronization.
        # Load the exact archived candidate only in an explicit fresh-VM control.
        source =
          "bench/results/snapshot-copy-lockfix-retry-single_pass-1.json"
          |> File.read!()
          |> Jason.decode!()
          |> Map.fetch!("source")

        root = Path.join([System.tmp_dir!(), "opencode", "snapshot-copy-control-#{System.pid()}"])
        if File.exists?(root), do: raise("control fixture exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)

        modules =
          Code.compile_string(source, @path) ++
            Code.compile_file("apps/ferricstore/lib/ferricstore/raft/waraft_storage.ex")

        for {module, beam} <- modules do
          path = Path.join(root, "#{module}.beam")
          File.write!(path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(path), beam)
        end

        {mode, source, root}

      other ->
        raise("invalid snapshot copy variant: #{other}")
    end
  end
end
