Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

Code.require_file("bench/support/resp_router_load.exs")

defmodule WaraftRespRouterBench do
  @moduledoc false

  alias Ferricstore.Bench.RespRouterLoad

  def run do
    backend = env_backend("BACKEND", :ra)
    mode = env_atom("BENCH_MODE", :set, [:set, :get, :mixed])
    total = env_int("TOTAL", 200_000)
    concurrency = env_int("CONCURRENCY", 200)
    pipeline = env_int("PIPELINE", 50)
    data_size = env_int("DATA_SIZE", 256)
    key_count = env_int("KEY_COUNT", min(total, 100_000))
    warmup = env_int("WARMUP", 10_000)
    shards = env_int("SHARDS", 4)
    payload = RespRouterLoad.payload(data_size)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-resp-#{backend}-#{System.unique_integer([:positive])}"
      )

    configure_app(backend, data_dir, shards)

    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    port = FerricstoreServer.Listener.port()

    if mode in [:get, :mixed] do
      IO.puts("preloading #{key_count} keys")

      _ =
        RespRouterLoad.preload(port, key_count, payload,
          concurrency: min(concurrency, 32),
          pipeline: pipeline
        )
    end

    if warmup > 0 do
      _ =
        RespRouterLoad.run(port,
          mode: mode,
          total: warmup,
          concurrency: min(concurrency, 32),
          pipeline: pipeline,
          payload: payload,
          key_count: key_count
        )
    end

    result =
      RespRouterLoad.run(port,
        mode: mode,
        total: total,
        concurrency: concurrency,
        pipeline: pipeline,
        payload: payload,
        key_count: key_count
      )

    print_result(result, %{
      backend: backend,
      mode: mode,
      total: total,
      concurrency: concurrency,
      pipeline: pipeline,
      data_size: data_size,
      key_count: key_count,
      shards: shards,
      data_dir: data_dir,
      port: port
    })
  after
    _ = Application.stop(:ferricstore_server)
    _ = Application.stop(:ferricstore)
  end

  defp configure_app(backend, data_dir, shards) do
    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :raft_backend, backend)

    case {backend, System.get_env("WARAFT_LOG", "segment")} do
      {:waraft, "ets"} ->
        Application.put_env(:ferricstore, :waraft_log_module, :wa_raft_log_ets)

      {:waraft, _segment} ->
        Application.delete_env(:ferricstore, :waraft_log_module)

      _ ->
        :ok
    end
  end

  defp print_result(result, config) do
    IO.puts("""
    FerricStore real RESP/router benchmark
    backend=#{config.backend} mode=#{config.mode} total=#{config.total} concurrency=#{config.concurrency} pipeline=#{config.pipeline} data_size=#{config.data_size} key_count=#{config.key_count} shards=#{config.shards}
    port=#{config.port}
    data_dir=#{config.data_dir}
    ops=#{result.ops} batches=#{result.batches} short_reads=#{result.short_reads}
    elapsed_ms=#{Float.round(result.elapsed_us / 1000, 2)}
    ops_per_sec=#{Float.round(result.ops_per_sec, 2)}
    mb_per_sec=#{Float.round(result.mb_per_sec, 2)}
    batch_p50_ms=#{Float.round(result.batch_p50_us / 1000, 3)}
    batch_p95_ms=#{Float.round(result.batch_p95_us / 1000, 3)}
    batch_p99_ms=#{Float.round(result.batch_p99_us / 1000, 3)}
    batch_p999_ms=#{Float.round(result.batch_p999_us / 1000, 3)}
    approx_op_p99_ms=#{Float.round(result.batch_p99_us / max(config.pipeline, 1) / 1000, 3)}
    """)
  end

  defp env_backend(name, default) do
    case System.get_env(name) do
      nil -> default
      "ra" -> :ra
      "waraft" -> :waraft
      other -> raise "unsupported #{name}=#{inspect(other)}; expected ra or waraft"
    end
  end

  defp env_atom(name, default, allowed) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        atom = String.to_existing_atom(value)

        if atom in allowed do
          atom
        else
          raise "unsupported #{name}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end
    end
  rescue
    ArgumentError ->
      raise "unsupported #{name}=#{inspect(System.get_env(name))}; expected one of #{inspect(allowed)}"
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end
end

WaraftRespRouterBench.run()
