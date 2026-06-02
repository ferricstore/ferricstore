Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule WaraftSpikeRecoveryBench do
  @default_total 100_000
  @default_pipeline 500
  @default_data_size 256

  def run do
    total = env_int("TOTAL", @default_total)
    pipeline = env_int("PIPELINE", @default_pipeline)
    data_size = env_int("DATA_SIZE", @default_data_size)
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-recovery-#{System.system_time(:second)}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      :ok = :ferricstore_waraft_spike.start_segment_log(String.to_charlist(root))
      {:ok, write_result} = :ferricstore_waraft_spike_load.run(total, 1, pipeline, data_size, 0)
      :ok = :ferricstore_waraft_spike.stop()

      started_at = System.monotonic_time(:microsecond)
      :ok = :ferricstore_waraft_spike.start_segment_log(String.to_charlist(root))
      recovery_us = max(System.monotonic_time(:microsecond) - started_at, 1)

      expected = :binary.copy("x", data_size)
      {:ok, ^expected} = :ferricstore_waraft_spike.get(<<"bench:k", Integer.to_string(total)::binary>>)

      IO.puts("""
      WARaft spike recovery replay
      total=#{total} pipeline=#{pipeline} data_size=#{data_size}
      write_ops_per_sec=#{Float.round(write_result.ops_per_sec, 2)}
      recovery_ms=#{Float.round(recovery_us / 1000, 2)}
      recovery_ops_per_sec=#{Float.round(total * 1_000_000 / recovery_us, 2)}
      """)
    after
      :ferricstore_waraft_spike.stop()
      File.rm_rf(root)
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

WaraftSpikeRecoveryBench.run()
