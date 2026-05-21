Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

Code.require_file("bench/support/resp_router_load.exs")

defmodule WaraftRespRouterBench do
  @moduledoc false

  alias Ferricstore.Bench.RespRouterLoad

  def run do
    stop_started_apps()

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

  defp stop_started_apps do
    # `mix run` starts OTP applications before evaluating this script. Stop any
    # default boot first so the benchmark owns data_dir, shard_count, and backend.
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
    end
  end

  defp configure_app(backend, data_dir, shards) do
    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    maybe_attach_segment_append_trace()

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :raft_backend, backend)
    Application.put_env(:ferricstore, :waraft_storage_apply_mode, :segment_keydir)
    Application.delete_env(:ferricstore, :waraft_log_module)
    put_optional_int_env("WAL_COMMIT_DELAY_US", :wal_commit_delay_us)
    put_optional_int_env("WAL_MAX_BUFFER_BYTES", :wal_max_buffer_bytes)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)
    put_optional_int_env("WARAFT_SEGMENT_SYNC_DELAY_US", :waraft_segment_log_sync_delay_us)

    put_optional_interval_env(
      "WARAFT_STORAGE_METADATA_PERSIST_EVERY",
      :waraft_storage_metadata_persist_every
    )

    put_optional_int_env(
      "WARAFT_SEGMENT_RECORDS_PER_SEGMENT",
      :waraft_segment_log_records_per_segment
    )

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

    if backend == :waraft and System.get_env("WARAFT_LOG") do
      raise "WARAFT_LOG is no longer supported; WARaft benchmarks use durable segment/keydir storage"
    end
  end

  defp put_optional_int_env(env, app_key) do
    case System.get_env(env) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_interval_env(env, app_key) do
    case System.get_env(env) do
      nil ->
        :ok

      value when value in ["never", ":never"] ->
        Application.put_env(:ferricstore, app_key, :never)

      value ->
        Application.put_env(:ferricstore, app_key, String.to_integer(value))
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

  defp maybe_attach_segment_append_trace do
    if System.get_env("TRACE_WARAFT_SEGMENT_APPEND") in ["1", "true", "TRUE"] do
      {:ok, _} = Application.ensure_all_started(:telemetry)

      table = :waraft_resp_router_segment_append_trace

      case :ets.whereis(table) do
        :undefined -> :ets.new(table, [:named_table, :public, :set])
        _tid -> :ets.delete_all_objects(table)
      end

      :ets.insert(table, {:events, 0})
      :ets.insert(table, {:records, 0})
      :ets.insert(table, {:bytes, 0})
      :ets.insert(table, {:duration_native, 0})

      _ = :telemetry.detach("waraft-resp-router-segment-append-trace")

      :ok =
        :telemetry.attach(
          "waraft-resp-router-segment-append-trace",
          [:ferricstore, :waraft, :segment_log, :append],
          fn _event, measurements, metadata, _config ->
            :ets.update_counter(table, :events, {2, 1}, {:events, 0})

            :ets.update_counter(
              table,
              :records,
              {2, Map.get(measurements, :count, 0)},
              {:records, 0}
            )

            :ets.update_counter(table, :bytes, {2, Map.get(measurements, :bytes, 0)}, {:bytes, 0})

            :ets.update_counter(
              table,
              :duration_native,
              {2, Map.get(measurements, :duration, 0)},
              {:duration_native, 0}
            )

            if Map.get(metadata, :new_segment) do
              :ets.update_counter(table, :new_segments, {2, 1}, {:new_segments, 0})
            end
          end,
          nil
        )
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

    print_segment_append_trace()
  end

  defp print_segment_append_trace do
    table = :waraft_resp_router_segment_append_trace

    case :ets.whereis(table) do
      :undefined ->
        :ok

      _tid ->
        events = lookup_trace(table, :events)
        records = lookup_trace(table, :records)
        bytes = lookup_trace(table, :bytes)
        duration_native = lookup_trace(table, :duration_native)
        new_segments = lookup_trace(table, :new_segments)

        avg_records =
          if events > 0 do
            Float.round(records / events, 2)
          else
            0.0
          end

        IO.puts("""
        waraft_segment_append_events=#{events}
        waraft_segment_append_records=#{records}
        waraft_segment_append_avg_records=#{avg_records}
        waraft_segment_append_bytes=#{bytes}
        waraft_segment_append_new_segments=#{new_segments}
        waraft_segment_append_total_ms=#{Float.round(System.convert_time_unit(duration_native, :native, :microsecond) / 1000, 3)}
        """)
    end
  end

  defp lookup_trace(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] when is_integer(value) -> value
      _ -> 0
    end
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
