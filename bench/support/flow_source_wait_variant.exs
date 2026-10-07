defmodule FerricstoreBench.FlowSourceWaitVariant do
  @moduledoc false
  @baseline "e5f59ba7959710773729812343ae9bcd62d8d15f"
  def prepare do
    mode = System.get_env("BENCH_FLOW_SOURCE_WAIT", "batch")
    path = "apps/ferricstore/lib/ferricstore/flow/lmdb_writer.ex"
    source = File.read!(path)

    case mode do
      "batch" ->
        {mode, source, nil}

      "sequential" ->
        {source, 0} = System.cmd("git", ["show", @baseline <> ":" <> path])
        root = Path.join([System.tmp_dir!(), "opencode", "flow-source-wait-#{System.pid()}"])
        if File.exists?(root), do: raise("control exists")
        File.mkdir_p!(root)
        Code.compiler_options(ignore_module_conflict: true)
        [{module, beam}] = Code.compile_string(source, path)
        beam_path = Path.join(root, "#{module}.beam")
        File.write!(beam_path, beam)
        {:module, ^module} = :code.load_binary(module, String.to_charlist(beam_path), beam)
        {mode, source, root}

      other ->
        raise("invalid Flow source-wait control: #{other}")
    end
  end
end
