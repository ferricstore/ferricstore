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
    log = System.get_env("LOG", "segment")
    mode = System.get_env("BENCH_MODE", "set")
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-bench-#{System.system_time(:second)}")
    File.rm_rf!(root)
    File.mkdir_p!(root)

    configure_waraft_wal_env()
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

  defp start_spike("segment", root) do
    :ferricstore_waraft_spike.start_volatile_segment_log(String.to_charlist(root))
  end

  defp start_spike(other, _root) do
    raise("unsupported LOG=#{inspect(other)}; expected segment")
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

  defp configure_waraft_wal_env do
    put_optional_int_env("WAL_COMMIT_DELAY_US", :wal_commit_delay_us)
    put_optional_int_env("WAL_MAX_BUFFER_BYTES", :wal_max_buffer_bytes)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)
    put_optional_int_env("WARAFT_SEGMENT_SYNC_DELAY_US", :waraft_segment_log_sync_delay_us)

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_io_mode,
      env_atom("WARAFT_SEGMENT_IO_MODE", :file, [:file, :wal_nif])
    )

    Application.put_env(
      :ferricstore,
      :waraft_segment_log_preallocate_bytes,
      env_int("WARAFT_SEGMENT_PREALLOCATE_BYTES", 256 * 1024 * 1024)
    )

    put_optional_atom_env("WARAFT_SEGMENT_SYNC_METHOD", :waraft_segment_log_sync_method, [
      :datasync,
      :sync,
      :auto
    ])
  end

  defp put_optional_int_env(env, app_key) do
    case System.get_env(env) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_atom_env(env, app_key, allowed) do
    case System.get_env(env) do
      nil ->
        :ok

      value ->
        atom = String.to_existing_atom(value)

        unless atom in allowed do
          raise "unsupported #{env}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end

        Application.put_env(:ferricstore, app_key, atom)
    end
  end

  defp env_atom(name, default, allowed) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        atom = String.to_existing_atom(value)

        unless atom in allowed do
          raise "unsupported #{name}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end

        atom
    end
  end
end

WaraftSpikeBench.run()
