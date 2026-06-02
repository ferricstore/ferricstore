Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

Code.require_file("bench/support/resp_router_load.exs")

defmodule WaraftRespRouterBench do
  @moduledoc false

  alias Ferricstore.Bench.RespRouterLoad

  def run do
    stop_started_apps()

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
        "ferricstore-waraft-resp-#{System.unique_integer([:positive])}"
      )

    configure_app(data_dir, shards)

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

    profile_before = maybe_profile_snapshot()
    maybe_start_eprof()

    result =
      RespRouterLoad.run(port,
        mode: mode,
        total: total,
        concurrency: concurrency,
        pipeline: pipeline,
        payload: payload,
        key_count: key_count
      )

    maybe_stop_eprof()
    profile_after = maybe_profile_snapshot()

    print_result(result, %{
      backend: :waraft,
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

    maybe_print_profile(profile_before, profile_after)
    maybe_print_eprof()
  after
    _ = maybe_stop_eprof()
    _ = Application.stop(:ferricstore_server)
    _ = Application.stop(:ferricstore)
  end

  defp stop_started_apps do
    # `mix run` starts OTP applications before evaluating this script. Stop any
    # default boot first so the benchmark owns data_dir and shard_count.
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
    end
  end

  defp configure_app(data_dir, shards) do
    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    maybe_attach_segment_append_trace()

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.delete_env(:ferricstore, :waraft_log_module)
    put_optional_int_env("WAL_COMMIT_DELAY_US", :wal_commit_delay_us)
    put_optional_int_env("WAL_MAX_BUFFER_BYTES", :wal_max_buffer_bytes)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)

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
      :waraft_segment_log_preallocate_bytes,
      env_int("WARAFT_SEGMENT_PREALLOCATE_BYTES", 256 * 1024 * 1024)
    )

    if System.get_env("WARAFT_LOG") do
      raise "WARAFT_LOG is no longer supported; WARaft benchmarks use durable segment/keydir storage"
    end
  end

  defp put_optional_int_env(env, app_key) do
    case System.get_env(env) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_bool_env(env, app_key) do
    case System.get_env(env) do
      nil ->
        :ok

      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] ->
        Application.put_env(:ferricstore, app_key, true)

      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] ->
        Application.put_env(:ferricstore, app_key, false)

      value ->
        raise "unsupported #{env}=#{inspect(value)}; expected boolean"
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

  defp maybe_profile_snapshot do
    if env_bool("PROFILE_PROCESSES", false) do
      Process.list()
      |> Enum.map(fn pid ->
        info =
          Process.info(pid, [
            :registered_name,
            :current_function,
            :initial_call,
            :reductions,
            :message_queue_len,
            :memory
          ]) || []

        {pid, Map.new(info)}
      end)
      |> Map.new()
    else
      nil
    end
  end

  defp maybe_print_profile(nil, _after_snapshot), do: :ok
  defp maybe_print_profile(_before_snapshot, nil), do: :ok

  defp maybe_print_profile(before_snapshot, after_snapshot) do
    rows =
      after_snapshot
      |> Enum.map(fn {pid, after_info} ->
        before_info = Map.get(before_snapshot, pid, %{})
        reductions = Map.get(after_info, :reductions, 0) - Map.get(before_info, :reductions, 0)

        %{
          pid: pid,
          reductions: reductions,
          registered_name: Map.get(after_info, :registered_name, []),
          current_function: Map.get(after_info, :current_function),
          initial_call: Map.get(after_info, :initial_call),
          message_queue_len: Map.get(after_info, :message_queue_len, 0),
          memory: Map.get(after_info, :memory, 0)
        }
      end)
      |> Enum.filter(&(&1.reductions > 0))
      |> Enum.sort_by(& &1.reductions, :desc)
      |> Enum.take(30)

    IO.puts("process_profile_top_reductions")

    Enum.each(rows, fn row ->
      IO.puts(
        "reductions=#{row.reductions} mq=#{row.message_queue_len} memory=#{row.memory} " <>
          "pid=#{inspect(row.pid)} name=#{inspect(row.registered_name)} " <>
          "current=#{inspect(row.current_function)} initial=#{inspect(row.initial_call)}"
      )
    end)
  end

  defp maybe_start_eprof do
    case System.get_env("PROFILE_EPROF") do
      "storage" ->
        case :code.which(:eprof) do
          :non_existing ->
            IO.puts("eprof_unavailable=true")

          _path ->
            storage_pids =
              Process.list()
              |> Enum.filter(fn pid ->
                case Process.info(pid, :registered_name) do
                  {:registered_name, name} when is_atom(name) ->
                    name
                    |> Atom.to_string()
                    |> String.starts_with?("raft_storage_ferricstore_waraft_backend_")

                  _other ->
                    false
                end
              end)

            Process.put(:waraft_resp_router_eprof_started?, true)
            {:ok, _pid} = apply(:eprof, :start, [])
            :ok = apply(:eprof, :start_profiling, [storage_pids])
        end

      nil ->
        :ok

      other ->
        raise "unsupported PROFILE_EPROF=#{inspect(other)}; expected storage"
    end
  end

  defp maybe_stop_eprof do
    if Process.get(:waraft_resp_router_eprof_started?) do
      _ = apply(:eprof, :stop_profiling, [])
      :ok
    else
      :ok
    end
  end

  defp maybe_print_eprof do
    if Process.get(:waraft_resp_router_eprof_started?) do
      IO.puts("eprof_storage_total")
      apply(:eprof, :analyze, [:total])
      apply(:eprof, :stop, [])
      Process.delete(:waraft_resp_router_eprof_started?)
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

  defp env_bool(name, default) do
    case System.get_env(name) do
      nil -> default
      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] -> true
      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] -> false
      value -> raise "unsupported #{name}=#{inspect(value)}; expected boolean"
    end
  end
end

WaraftRespRouterBench.run()
