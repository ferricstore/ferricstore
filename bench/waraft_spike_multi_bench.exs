Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule WaraftSpikeMultiBench do
  @default_total 200_000
  @default_concurrency 200
  @default_pipeline 50
  @default_data_size 256
  @default_warmup 10_000
  @default_partitions 4

  def run do
    total = env_int("TOTAL", @default_total)
    concurrency = env_int("CONCURRENCY", @default_concurrency)
    pipeline = env_int("PIPELINE", @default_pipeline)
    data_size = env_int("DATA_SIZE", @default_data_size)
    warmup = env_int("WARMUP", @default_warmup)
    partitions = env_int("PARTITIONS", @default_partitions)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-multi-bench-#{System.system_time(:second)}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      configure_waraft_wal_env()
      start!(root, partitions)

      {:ok, result} =
        :ferricstore_waraft_spike_load.run_multi(
          total,
          concurrency,
          pipeline,
          data_size,
          warmup,
          partitions
        )

      IO.puts("""
      WARaft spike #{partitions}-partition set
      total=#{total} concurrency=#{concurrency} pipeline=#{pipeline} data_size=#{data_size} warmup=#{warmup}
      elapsed_ms=#{Float.round(result.elapsed_us / 1000, 2)}
      ops_per_sec=#{Float.round(result.ops_per_sec, 2)}
      mb_per_sec=#{Float.round(result.mb_per_sec, 2)}
      """)
    after
      :ferricstore_waraft_spike.stop()
      File.rm_rf(root)
    end
  end

  defp start!(root, partitions) do
    :ok =
      :ferricstore_waraft_spike.start_multi_volatile_segment_log(
        String.to_charlist(root),
        partitions
      )
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp configure_waraft_wal_env do
    put_optional_int_env("WAL_COMMIT_DELAY_US", :wal_commit_delay_us)
    put_optional_int_env("WAL_MAX_BUFFER_BYTES", :wal_max_buffer_bytes)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_preallocate_bytes,
      env_int("WARAFT_SEGMENT_PREALLOCATE_BYTES", 256 * 1024 * 1024)
    )
  end

  defp put_optional_int_env(env, app_key) do
    case System.get_env(env) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end
end

WaraftSpikeMultiBench.run()
