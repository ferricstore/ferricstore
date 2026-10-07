defmodule FerricstoreBench.HsetGroupVariant do
  @moduledoc false
  def prepare do
    mode = System.get_env("BENCH_HSET_GROUP", "coalesced")
    path = "apps/ferricstore/lib/ferricstore/raft/waraft_backend/sections/public_api.ex"
    source = File.read!(path)

    case mode do
      "coalesced" ->
        Application.put_env(:ferricstore, :waraft_single_hset_coalescing, true)
        {mode, source, nil}

      "direct" ->
        Application.put_env(:ferricstore, :waraft_single_hset_coalescing, false)
        {mode, source, nil}

      "apply_backlog" ->
        Application.put_env(:ferricstore, :waraft_single_hset_coalescing, true)
        needle = "Ferricstore.Raft.WARaftBackend.HsetCadence.busy?(shard_index)"
        true = String.contains?(source, needle)

        source =
          String.replace(
            source,
            needle,
            ":wa_raft_queue.apply_queue_size(@table, partition(shard_index)) >= 2"
          )

        root = Path.join([System.tmp_dir!(), "opencode", "hset-apply-backlog-#{System.pid()}"])
        if File.exists?(root), do: raise("control fixture exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)

        modules =
          Code.compile_string(source, path) ++
            Code.compile_file("apps/ferricstore/lib/ferricstore/raft/waraft_backend.ex")

        for {module, beam} <- modules do
          beam_path = Path.join(root, "#{module}.beam")
          File.write!(beam_path, beam)
          {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        end

        {mode, source, root}

      other ->
        raise("invalid HSET group variant: #{other}")
    end
  end
end
