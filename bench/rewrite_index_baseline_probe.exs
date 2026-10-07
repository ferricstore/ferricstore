# Reproduce the deleted-directory leak using the committed provider in a private VM.
defmodule FerricstoreBench.RewriteIndexBaselineProbe do
  @module :ferricstore_waraft_spike_segment_log
  @baseline "e5f59ba7959710773729812343ae9bcd62d8d15f"
  @source_dir "apps/ferricstore/src"
  @parts "ferricstore_waraft_spike_segment_log/sections"
  @table :ferricstore_waraft_segment_offset_registry

  def run do
    {:ok, _} = Application.ensure_all_started(:telemetry)
    root = Path.join([System.tmp_dir!(), "opencode", "rewrite-baseline-#{System.pid()}"])
    if File.exists?(root), do: raise("fixture exists")
    code_root = Path.join(root, "code")
    File.mkdir_p!(Path.join(code_root, @parts))

    for path <- [
          "ferricstore_waraft_spike_segment_log.erl"
          | Enum.map(1..7, &(@parts <> "/part_0#{&1}.hrl"))
        ] do
      {source, 0} = System.cmd("git", ["show", @baseline <> ":" <> @source_dir <> "/" <> path])
      File.write!(Path.join(code_root, path), source)
    end

    {:ok, @module, beam, []} =
      :compile.file(
        String.to_charlist(Path.join(code_root, "#{@module}.erl")),
        [
          :binary,
          :return_errors,
          :return_warnings,
          {:i, String.to_charlist(code_root)},
          {:i, String.to_charlist("deps/wa_raft/include")}
        ]
      )

    path = Path.join(code_root, "#{@module}.beam")
    File.write!(path, beam)
    {:module, @module} = :code.load_binary(@module, String.to_charlist(path), beam)
    data = Path.join(root, "data")
    dir = Path.join(data, "segment_log")
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 8_192)

    :ok =
      @module.write_projection_batches_sync(
        to_charlist(data),
        for(i <- 1..9_000, do: {{:raft_log_pos, i, 0}, [{"k", "v", 0}]})
      )

    try do
      results =
        for round <- 1..3 do
          Application.put_env(
            :ferricstore,
            :waraft_segment_log_sync_dir_hook,
            {:fail_on_count, 3, self()}
          )

          {:error, _} =
            @module.write_projection(
              to_charlist(data),
              {:raft_log_pos, 10_000 + round, 0},
              for(i <- 1..9_000, do: {"f-#{i}", "v", 0})
            )

          temporary =
            :ets.tab2list(@table)
            |> Enum.filter(fn {{key, _}, _, _, _} ->
              String.starts_with?(key, dir <> ".rewrite.staging.")
            end)

          directories = temporary |> Enum.map(fn {{key, _}, _, _, _} -> key end) |> Enum.uniq()
          true = length(temporary) == round * 8_194
          true = Enum.all?(directories, &(not File.exists?(&1)))

          %{
            round: round,
            deleted_directories: directories,
            stale_entries: length(temporary),
            bytes: :ets.info(@table, :memory) * :erlang.system_info(:wordsize)
          }
        end

      report = %{diagnostic_only: true, source: @baseline, repeated_failures: results}
      File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
      IO.inspect(report, label: "REWRITE_BASELINE", limit: :infinity)
    after
      File.rm_rf!(root)
    end
  end
end

FerricstoreBench.RewriteIndexBaselineProbe.run()
