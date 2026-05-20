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
    log = System.get_env("LOG", "ets")

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-multi-bench-#{System.system_time(:second)}"
      )

    File.rm_rf!(root)
    File.mkdir_p!(root)

    try do
      start!(log, root, partitions)

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
      WARaft spike #{partitions}-partition set log=#{log}
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

  defp start!("ets", root, partitions) do
    :ok = :ferricstore_waraft_spike.start_multi_volatile(String.to_charlist(root), partitions)
  end

  defp start!("segment", root, partitions) do
    :ok =
      :ferricstore_waraft_spike.start_multi_volatile_segment_log(
        String.to_charlist(root),
        partitions
      )
  end

  defp start!(other, _root, _partitions) do
    raise("unsupported LOG=#{inspect(other)}; expected ets or segment")
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

WaraftSpikeMultiBench.run()
