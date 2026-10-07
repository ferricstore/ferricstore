defmodule FerricstoreBench.OffsetScanPerf do
  @moduledoc false
  alias :ferricstore_waraft_spike_segment_log, as: SegmentLog

  def run do
    root =
      Path.join([
        System.tmp_dir!(),
        "opencode",
        "offset-scan-#{System.pid()}",
        "apply_projection_log"
      ])

    if File.exists?(root), do: raise("fixture exists")
    {:ok, _} = Application.ensure_all_started(:telemetry)
    payload = String.duplicate("x", 128)

    batches =
      for index <- 1..2_000,
          do: {{:raft_log_pos, index * 2, 0}, [{"key", payload, 0}]}

    try do
      :ok = SegmentLog.write_projection_batches_sync(String.to_charlist(root), batches)

      {us, results} =
        :timer.tc(fn ->
          for index <- 1..128,
              do: SegmentLog.location_for_index(String.to_charlist(root), index * 2 + 1)
        end)

      true = Enum.all?(results, &(&1 == :not_found))

      report = %{
        component_only: true,
        records: 2_000,
        missing_queries: 128,
        elapsed_us: us,
        source:
          File.read!(
            "apps/ferricstore/src/ferricstore_waraft_spike_segment_log/sections/part_05.hrl"
          )
      }

      File.write!(System.fetch_env!("BENCH_OUTPUT"), Jason.encode!(report, pretty: true))
      IO.inspect(Map.drop(report, [:source]), label: "OFFSET_SCAN")
    after
      File.rm_rf!(Path.dirname(root))
    end
  end
end

FerricstoreBench.OffsetScanPerf.run()
