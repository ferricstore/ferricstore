defmodule FerricstoreBench.RewriteIndexReclaimProbe do
  alias :ferricstore_waraft_spike_segment_log, as: Provider
  @table :ferricstore_waraft_segment_offset_registry

  def run do
    {:ok, _} = Application.ensure_all_started(:telemetry)
    root = Path.join([System.tmp_dir!(), "opencode", "rewrite-reclaim-#{System.pid()}"])
    if File.exists?(root), do: raise("fixture exists")
    dir = Path.join(root, "segment_log")
    Application.put_env(:ferricstore, :waraft_segment_log_offset_registry_max_entries, 8_192)

    assert_ok(
      Provider.write_projection_batches_sync(
        to_charlist(root),
        for(index <- 1..9_000, do: {{:raft_log_pos, index, 0}, [{"k", "v", 0}]})
      )
    )

    rows = :ets.match_object(@table, {{dir, :_}, :_, :_, :_})
    true = length(rows) == 8_194
    stages = :atomics.new(1, [])

    Application.put_env(:ferricstore, :waraft_segment_log_sync_dir_hook, fn path ->
      count = :ets.select_count(@table, [{{{path, :_}, :_, :_, :_}, [], [true]}])

      if String.contains?(path, ".rewrite.staging.") and count == 8_194 do
        :atomics.add(stages, 1, 1)
        {:error, :injected_stage_sync}
      else
        :ok
      end
    end)

    try do
      repeated =
        for round <- 1..10 do
          {:error, _} =
            Provider.write_projection(
              to_charlist(root),
              {:raft_log_pos, 10_000 + round, 0},
              for(i <- 1..9_000, do: {"f-#{i}", "v", 0})
            )

          snapshot = footprint()
          true = snapshot.entries == 8_194
          snapshot
        end

      for suffix <- 1..23 do
        key = dir <> ".rewrite.staging.#{suffix}"

        :ets.insert(
          @table,
          Enum.map(rows, fn {{_, index}, ordinal, offset, size} ->
            {{key, index}, ordinal, offset, size}
          end)
        )
      end

      before = footprint()
      {:ok, reclaimed} = Provider.reclaim_abandoned_rewrite_indexes()
      after_reclaim = footprint()
      true = reclaimed.offset_entries == 188_462
      true = after_reclaim.entries == 8_194

      source_paths = [
        "apps/ferricstore/src/ferricstore_waraft_spike_segment_log.erl"
        | Enum.map(
            1..7,
            &"apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections/part_0#{&1}.hrl"
          )
      ]

      report = %{
        diagnostic_only: true,
        workspace_source_sha256:
          Map.new(source_paths, fn path ->
            {path, Base.encode16(:crypto.hash(:sha256, File.read!(path)), case: :lower)}
          end),
        loaded_provider_md5: Base.encode16(Provider.module_info(:md5), case: :lower),
        offset_scan_mode: System.get_env("BENCH_OFFSET_SCAN", "buffered"),
        created_full_stages: :atomics.get(stages, 1),
        repeated_failures: repeated,
        before_reclaim: before,
        reclaimed: reclaimed,
        after_reclaim: after_reclaim
      }

      File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
      IO.inspect(report, label: "REWRITE_RECLAIM")
    after
      File.rm_rf!(root)
    end
  end

  defp footprint do
    %{
      entries: :ets.info(@table, :size),
      bytes: :ets.info(@table, :memory) * :erlang.system_info(:wordsize)
    }
  end

  defp assert_ok(:ok), do: :ok
end

FerricstoreBench.RewriteIndexReclaimProbe.run()
