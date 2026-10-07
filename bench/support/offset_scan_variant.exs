defmodule FerricstoreBench.OffsetScanVariant do
  @moduledoc false
  @module :ferricstore_waraft_spike_segment_log
  @main "apps/ferricstore/src/ferricstore_waraft_spike_segment_log.erl"
  @sections "apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections"

  def prepare do
    mode = System.get_env("BENCH_OFFSET_SCAN", "buffered")
    true = mode in ["buffered", "raw"]
    source = File.read!(Path.join(@sections, "part_05.hrl"))

    {source, root} =
      if mode == "raw" do
        # Change only the fallback descriptor's buffer. Loading an entire old
        # section would also remove later cleanup and recovery corrections.
        buffered_open =
          ~r/open_verified_segment_file\(Path, \[read, raw, binary,\s+\{read_ahead, \?OFFSET_SCAN_READ_AHEAD_BYTES\}\]\)/

        [_match] = Regex.scan(buffered_open, source)

        source =
          Regex.replace(
            buffered_open,
            source,
            "open_verified_segment_file(Path, [read, raw, binary])"
          )

        root = Path.join([System.tmp_dir!(), "opencode", "offset-scan-control-#{System.pid()}"])
        if File.exists?(root), do: raise("offset control exists")
        directory = Path.join(root, "ferricstore_waraft_spike_segment_log/sections")
        File.mkdir_p!(directory)
        File.write!(Path.join(root, Path.basename(@main)), File.read!(@main))

        for part <- Path.wildcard(Path.join(@sections, "*.hrl")) do
          contents = if Path.basename(part) == "part_05.hrl", do: source, else: File.read!(part)
          File.write!(Path.join(directory, Path.basename(part)), contents)
        end

        {:ok, @module, beam, []} =
          :compile.file(
            String.to_charlist(Path.join(root, Path.basename(@main))),
            [
              :binary,
              :return_errors,
              :return_warnings,
              {:i, String.to_charlist(root)},
              {:i, String.to_charlist("deps/wa_raft/include")}
            ]
          )

        path = Path.join(root, "#{@module}.beam")
        File.write!(path, beam)
        {:module, @module} = :code.load_binary(@module, String.to_charlist(path), beam)
        {source, root}
      else
        {source, nil}
      end

    {mode, source, root}
  end
end
