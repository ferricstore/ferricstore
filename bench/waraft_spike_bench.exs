Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule WaraftSpikeBench do
  @default_total 200_000
  @default_concurrency 200
  @default_pipeline 50
  @default_data_size 256
  @default_warmup 10_000

  def run do
    total = env_int("TOTAL", @default_total)
    concurrency = env_int("CONCURRENCY", @default_concurrency)
    pipeline = env_int("PIPELINE", @default_pipeline)
    data_size = env_int("DATA_SIZE", @default_data_size)
    warmup = env_int("WARMUP", @default_warmup)
    log = System.get_env("LOG", "ets")
    mode = System.get_env("BENCH_MODE", "set")
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-bench-#{System.system_time(:second)}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    :ok = start_spike(log, root)
    {:ok, result} = run_load(mode, total, concurrency, pipeline, data_size, warmup)

    IO.puts("""
    WARaft spike one-shard #{mode}
    log=#{log} mode=#{mode} total=#{result.ops} concurrency=#{concurrency} pipeline=#{pipeline} data_size=#{data_size} warmup=#{warmup}
    reads=#{Map.get(result, :reads, 0)} writes=#{Map.get(result, :writes, result.ops)}
    elapsed_ms=#{Float.round(result.elapsed_us / 1000, 2)}
    ops_per_sec=#{Float.round(result.ops_per_sec, 2)}
    mb_per_sec=#{Float.round(result.mb_per_sec, 2)}
    """)
  after
    :ferricstore_waraft_spike.stop()
  end

  defp start_spike("ets", root) do
    :ferricstore_waraft_spike.start_volatile(String.to_charlist(root))
  end

  defp start_spike("segment", root) do
    :ferricstore_waraft_spike.start_volatile_segment_log(String.to_charlist(root))
  end

  defp start_spike(other, _root) do
    raise("unsupported LOG=#{inspect(other)}; expected ets or segment")
  end

  defp run_load("set", total, concurrency, pipeline, data_size, warmup) do
    :ferricstore_waraft_spike_load.run(total, concurrency, pipeline, data_size, warmup)
  end

  defp run_load("mixed", total, concurrency, pipeline, data_size, warmup) do
    :ferricstore_waraft_spike_load.run_mixed(total, concurrency, pipeline, data_size, warmup)
  end

  defp run_load(other, _total, _concurrency, _pipeline, _data_size, _warmup) do
    raise("unsupported BENCH_MODE=#{inspect(other)}; expected set or mixed")
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

WaraftSpikeBench.run()
