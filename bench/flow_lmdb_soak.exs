Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule FlowLMDBSoak do
  @moduledoc false

  def run do
    backend = atom_env("BACKEND", :waraft, [:ra, :waraft])
    duration_s = int_env("DURATION_SECONDS", 1_800)
    flows_per_run = int_env("FLOWS_PER_RUN", 5_000)
    target_fps = int_env("TARGET_FLOWS_PER_SEC", 5_000)
    sample_interval_s = int_env("SAMPLE_INTERVAL_SECONDS", 10)
    final_drain_s = int_env("FINAL_DRAIN_SECONDS", 15)
    shards = int_env("SHARDS", 16)

    data_dir =
      System.get_env("DATA_DIR") ||
        Path.join(System.tmp_dir!(), "ferricstore-flow-lmdb-soak-#{unique()}")

    stop_started_apps()
    configure_app(backend, data_dir, shards)

    started_native = System.monotonic_time()
    deadline_native = started_native + System.convert_time_unit(duration_s, :second, :native)
    next_sample_native = started_native

    stats = %{
      completed: 0,
      created: 0,
      iterations: 0,
      failures: 0,
      max_pending_ops: 0,
      max_oldest_lag_ms: 0.0,
      max_replay_safe_lag: 0,
      max_flush_failures: 0,
      max_binary_mem_mb: 0.0,
      max_total_mem_mb: 0.0,
      max_disk_mb: 0.0
    }

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      port = FerricstoreServer.Listener.port()

      IO.puts(
        "flow_lmdb_soak backend=#{backend} port=#{port} data_dir=#{data_dir} " <>
          "duration_s=#{duration_s} flows_per_run=#{flows_per_run} target_fps=#{target_fps} " <>
          "shards=#{shards} lmdb_mode=#{Application.get_env(:ferricstore, :flow_lmdb_mode)} " <>
          "flush_interval_ms=#{Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms)} " <>
          "max_batch_ops=#{Application.get_env(:ferricstore, :flow_lmdb_max_batch_ops)}"
      )

      IO.puts(
        "sample elapsed_s iter created completed avg_completed_s pending_ops max_oldest_lag_ms " <>
          "max_replay_lag flush_failures total_mem_mb binary_mem_mb disk_mb"
      )

      stats =
        loop(%{
          port: port,
          deadline_native: deadline_native,
          started_native: started_native,
          next_sample_native: next_sample_native,
          sample_interval_native: System.convert_time_unit(sample_interval_s, :second, :native),
          flows_per_run: flows_per_run,
          target_fps: target_fps,
          stats: stats
        })

      stats = wait_for_drain(stats, started_native, final_drain_s)
      print_summary(stats, started_native, data_dir)
    after
      stop_started_apps()
      remove_data_dir(data_dir)
    end
  end

  defp loop(state) do
    if System.monotonic_time() >= state.deadline_native do
      maybe_sample(state.stats, state.started_native, state.stats.iterations, true).stats
    else
      iteration_started = System.monotonic_time()
      {result, status, output} = run_python(state.port, state.flows_per_run)
      iteration_elapsed_ms = elapsed_ms(iteration_started)

      stats =
        state.stats
        |> Map.update!(:iterations, &(&1 + 1))
        |> Map.update!(:failures, &(&1 + if(status == 0, do: 0, else: 1)))
        |> Map.update!(:created, &(&1 + int_result(result, "created")))
        |> Map.update!(:completed, &(&1 + int_result(result, "completed")))

      if status != 0 do
        IO.puts(
          "python_iteration_failed status=#{status} output=#{inspect(String.slice(output, 0, 2000))}"
        )
      end

      maybe_print_iteration(state.stats.iterations + 1, result, iteration_elapsed_ms)

      sample =
        maybe_sample(
          stats,
          state.started_native,
          stats.iterations,
          System.monotonic_time() >= state.next_sample_native
        )

      stats = sample.stats

      sleep_for_rate(state.flows_per_run, state.target_fps, iteration_started)

      next_sample_native =
        if sample.sampled?,
          do: System.monotonic_time() + state.sample_interval_native,
          else: state.next_sample_native

      loop(%{state | stats: stats, next_sample_native: next_sample_native})
    end
  end

  defp maybe_print_iteration(iteration, result, elapsed_ms) do
    if rem(iteration, int_env("ITERATION_LOG_EVERY", 20)) == 0 do
      IO.puts(
        "iteration=#{iteration} elapsed_ms=#{elapsed_ms} created=#{int_result(result, "created")} " <>
          "completed=#{int_result(result, "completed")} python_fps=#{float_result(result, "end_to_end_flows_per_sec")}"
      )
    end
  end

  defp maybe_sample(stats, started_native, iteration, due?) do
    if due? do
      lmdb = lmdb_status()
      total_mem_mb = bytes_to_mb(:erlang.memory(:total))
      binary_mem_mb = bytes_to_mb(:erlang.memory(:binary))
      disk_mb = bytes_to_mb(dir_bytes(Application.get_env(:ferricstore, :data_dir)))
      elapsed_s = elapsed_s(started_native)
      avg_completed_s = if elapsed_s > 0, do: stats.completed / elapsed_s, else: 0.0

      stats =
        stats
        |> Map.update!(:max_pending_ops, &max(&1, lmdb.pending_ops))
        |> Map.update!(:max_oldest_lag_ms, &max(&1, lmdb.max_oldest_lag_ms))
        |> Map.update!(:max_replay_safe_lag, &max(&1, lmdb.max_replay_safe_lag))
        |> Map.update!(:max_flush_failures, &max(&1, lmdb.flush_failures))
        |> Map.update!(:max_total_mem_mb, &max(&1, total_mem_mb))
        |> Map.update!(:max_binary_mem_mb, &max(&1, binary_mem_mb))
        |> Map.update!(:max_disk_mb, &max(&1, disk_mb))

      IO.puts(
        "sample elapsed_s=#{Float.round(elapsed_s, 1)} iter=#{iteration} " <>
          "created=#{stats.created} completed=#{stats.completed} " <>
          "avg_completed_s=#{Float.round(avg_completed_s, 1)} " <>
          "pending_ops=#{lmdb.pending_ops} max_oldest_lag_ms=#{Float.round(lmdb.max_oldest_lag_ms, 2)} " <>
          "max_replay_lag=#{lmdb.max_replay_safe_lag} flush_failures=#{lmdb.flush_failures} " <>
          "total_mem_mb=#{Float.round(total_mem_mb, 1)} binary_mem_mb=#{Float.round(binary_mem_mb, 1)} " <>
          "disk_mb=#{Float.round(disk_mb, 1)}"
      )

      %{stats: stats, sampled?: true}
    else
      %{stats: stats, sampled?: false}
    end
  end

  defp wait_for_drain(stats, started_native, final_drain_s) do
    deadline = System.monotonic_time() + System.convert_time_unit(final_drain_s, :second, :native)

    Enum.reduce_while(Stream.cycle([:tick]), stats, fn _, acc ->
      lmdb = lmdb_status()

      if System.monotonic_time() >= deadline or
           (lmdb.pending_ops == 0 and lmdb.max_replay_safe_lag == 0) do
        {:halt, maybe_sample(acc, started_native, acc.iterations, true).stats}
      else
        Process.sleep(1_000)
        {:cont, maybe_sample(acc, started_native, acc.iterations, true).stats}
      end
    end)
  end

  defp print_summary(stats, started_native, data_dir) do
    elapsed_s = elapsed_s(started_native)

    IO.puts(
      "summary elapsed_s=#{Float.round(elapsed_s, 1)} iterations=#{stats.iterations} " <>
        "created=#{stats.created} completed=#{stats.completed} failures=#{stats.failures} " <>
        "avg_completed_s=#{Float.round(stats.completed / max(elapsed_s, 0.001), 1)} " <>
        "max_pending_ops=#{stats.max_pending_ops} " <>
        "max_oldest_lag_ms=#{Float.round(stats.max_oldest_lag_ms, 2)} " <>
        "max_replay_lag=#{stats.max_replay_safe_lag} max_flush_failures=#{stats.max_flush_failures} " <>
        "max_total_mem_mb=#{Float.round(stats.max_total_mem_mb, 1)} " <>
        "max_binary_mem_mb=#{Float.round(stats.max_binary_mem_mb, 1)} " <>
        "max_disk_mb=#{Float.round(stats.max_disk_mb, 1)} data_dir=#{data_dir}"
    )
  end

  defp run_python(port, flows) do
    args = [
      "examples/dbos_style_benchmark.py",
      "--url",
      "redis://127.0.0.1:#{port}/0",
      "--mode",
      "queued",
      "--queued-shape",
      env("QUEUED_SHAPE", "live"),
      "--transport",
      env("TRANSPORT", "many"),
      "--worker-api",
      "lowlevel",
      "--worker-mode",
      "owner-wakeup",
      "--partition-mode",
      "auto",
      "--flows",
      Integer.to_string(flows),
      "--workers",
      env("WORKERS", "16"),
      "--producers",
      env("PRODUCERS", "8"),
      "--partitions",
      env("PARTITIONS", "1024"),
      "--claim-batch-size",
      env("CLAIM_BATCH_SIZE", "1000"),
      "--claim-partition-batch-size",
      env("CLAIM_PARTITION_BATCH_SIZE", "16"),
      "--create-batch-size",
      env("CREATE_BATCH_SIZE", "1000"),
      "--complete-async-depth",
      env("COMPLETE_ASYNC_DEPTH", "4"),
      "--server-shards",
      Integer.to_string(int_env("SHARDS", 16)),
      "--wake-coalesce-ms",
      env("WAKE_COALESCE_MS", "0"),
      "--claim-job-only",
      "--payload-bytes",
      env("PAYLOAD_BYTES", "0"),
      "--result-bytes",
      env("RESULT_BYTES", "0")
    ]

    {output, status} = System.cmd(python(), args, cd: sdk_dir(), stderr_to_stdout: true)
    {parse_python_dict(output), status, output}
  end

  defp parse_python_dict(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find_value(%{}, fn line ->
      if String.starts_with?(String.trim(line), "{"), do: parse_dict_line(line), else: nil
    end)
  end

  defp parse_dict_line(line) do
    Regex.scan(~r/'([^']+)': ([^,}]+)/, line)
    |> Map.new(fn [_match, key, value] -> {key, String.trim(value)} end)
  end

  defp int_result(result, key) do
    case Map.get(result, key) do
      nil -> 0
      value -> value |> parse_number() |> round()
    end
  rescue
    _ -> 0
  end

  defp float_result(result, key) do
    case Map.get(result, key) do
      nil -> 0.0
      value -> parse_number(value)
    end
  rescue
    _ -> 0.0
  end

  defp parse_number(value) do
    value = value |> String.trim() |> String.trim("'")

    case Float.parse(value) do
      {number, _rest} -> number
      :error -> 0.0
    end
  end

  defp lmdb_status do
    case safe_instance() do
      nil ->
        %{pending_ops: 0, max_oldest_lag_ms: 0.0, max_replay_safe_lag: 0, flush_failures: 0}

      ctx ->
        shards = max(Map.get(ctx, :shard_count, 0), 0)

        Enum.reduce(
          0..max(shards - 1, 0),
          %{pending_ops: 0, max_oldest_lag_ms: 0.0, max_replay_safe_lag: 0, flush_failures: 0},
          fn shard, acc ->
            pending_ops = atomic(ctx, :flow_lmdb_writer_pending_ops, shard)
            age_us = atomic(ctx, :flow_lmdb_writer_oldest_pending_age_us, shard)
            requested = atomic(ctx, :flow_lmdb_replay_safe_requested_index, shard)
            durable = atomic(ctx, :flow_lmdb_replay_safe_index, shard)
            failures = atomic(ctx, :flow_lmdb_writer_flush_failures, shard)
            replay_lag = max(requested - durable, 0)

            %{
              pending_ops: acc.pending_ops + pending_ops,
              max_oldest_lag_ms: max(acc.max_oldest_lag_ms, age_us / 1000),
              max_replay_safe_lag: max(acc.max_replay_safe_lag, replay_lag),
              flush_failures: acc.flush_failures + failures
            }
          end
        )
    end
  end

  defp atomic(ctx, field, shard) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        if shard < :atomics.info(ref).size, do: :atomics.get(ref, shard + 1), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp safe_instance do
    FerricStore.Instance.get(:default)
  rescue
    _ -> nil
  end

  defp configure_app(backend, data_dir, shards) do
    remove_data_dir(data_dir)
    File.mkdir_p!(data_dir)

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, shards)
    Application.put_env(:ferricstore, :raft_backend, backend)
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :max_memory_bytes, int_env("FERRICSTORE_MAX_MEMORY", 0))
    Application.put_env(:ferricstore, :memory_guard_interval_ms, 60 * 60 * 1000)
    Application.put_env(:ferricstore, :flow_lmdb_enabled, true)
    Application.put_env(:ferricstore, :flow_lmdb_mode, env("FLOW_LMDB_MODE", "lagged"))

    Application.put_env(
      :ferricstore,
      :flow_lmdb_flush_interval_ms,
      int_env("FLOW_LMDB_FLUSH_INTERVAL_MS", 1_000)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_max_batch_ops,
      int_env("FLOW_LMDB_MAX_BATCH_OPS", 25_000)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_flush_chunk_ops,
      int_env("FLOW_LMDB_FLUSH_CHUNK_OPS", 10_000)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_flush_chunk_pause_ms,
      int_env("FLOW_LMDB_FLUSH_CHUNK_PAUSE_MS", 1)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_flush_jitter_ms,
      int_env("FLOW_LMDB_FLUSH_JITTER_MS", 250)
    )

    Application.put_env(
      :ferricstore,
      :flow_lmdb_max_concurrent_flushes,
      int_env("FLOW_LMDB_MAX_CONCURRENT_FLUSHES", 1)
    )

    Application.delete_env(:ferricstore, :waraft_log_module)
    put_optional_bool_env("WARAFT_ASYNC_LOG_APPEND", :waraft_async_log_append)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)
    put_optional_int_env("WARAFT_APPLY_LOG_BATCH_SIZE", :waraft_apply_log_batch_size)
    put_optional_int_env("WARAFT_APPLY_BATCH_MAX_BYTES", :waraft_apply_batch_max_bytes)
    put_optional_int_env("WARAFT_SEGMENT_SYNC_DELAY_US", :waraft_segment_log_sync_delay_us)

    put_optional_int_env(
      "WARAFT_SEGMENT_PREALLOCATE_BYTES",
      :waraft_segment_log_preallocate_bytes
    )

    put_optional_int_env(
      "WARAFT_SEGMENT_RECORDS_PER_SEGMENT",
      :waraft_segment_log_records_per_segment
    )

    put_optional_atom_env("WARAFT_SEGMENT_IO_MODE", :waraft_segment_log_io_mode, [:file, :wal_nif])

    put_optional_atom_env("WARAFT_SEGMENT_SYNC_METHOD", :waraft_segment_log_sync_method, [
      :datasync,
      :sync,
      :auto
    ])

    put_optional_atom_env("WARAFT_FILE_WRITER_MODE", :waraft_segment_log_file_writer_mode, [
      :direct,
      :persistent,
      :process
    ])

    put_optional_int_env(
      "WARAFT_FILE_WRITER_GROUP_DELAY_MS",
      :waraft_segment_log_file_writer_group_delay_ms
    )
  end

  defp sleep_for_rate(_flows, target_fps, _started) when target_fps <= 0, do: :ok

  defp sleep_for_rate(flows, target_fps, started) do
    target_ms = div(flows * 1000, max(target_fps, 1))
    actual_ms = elapsed_ms(started)
    sleep_ms = target_ms - actual_ms
    if sleep_ms > 0, do: Process.sleep(sleep_ms), else: :ok
  end

  defp elapsed_ms(started),
    do: System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

  defp elapsed_s(started),
    do: System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond) / 1000

  defp bytes_to_mb(bytes), do: bytes / 1_048_576

  defp dir_bytes(path) when is_binary(path) do
    case System.cmd("du", ["-sk", path], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.split()
        |> List.first()
        |> case do
          nil -> 0
          value -> String.to_integer(value) * 1024
        end

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp dir_bytes(_), do: 0

  defp stop_started_apps do
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
    end
  end

  defp remove_data_dir(data_dir) do
    if System.get_env("KEEP_DATA_DIR") in ["1", "true", "TRUE", "yes", "YES"] do
      :ok
    else
      case System.cmd("rm", ["-rf", data_dir], stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {output, status} -> IO.puts("cleanup_failed status=#{status} output=#{inspect(output)}")
      end
    end
  end

  defp python, do: env("PYTHON", Path.join(sdk_dir(), ".venv/bin/python"))
  defp sdk_dir, do: env("SDK_DIR", "/Users/yoavgea/repos/ferricstore-python")
  defp env(name, default), do: System.get_env(name) || default
  defp unique, do: System.unique_integer([:positive])

  defp int_env(name, default) do
    name
    |> env(Integer.to_string(default))
    |> String.to_integer()
  end

  defp atom_env(name, default, allowed) do
    value = env(name, Atom.to_string(default))
    atom = String.to_existing_atom(value)

    if atom in allowed do
      atom
    else
      raise "unsupported #{name}=#{inspect(value)}; expected one of #{inspect(allowed)}"
    end
  end

  defp put_optional_int_env(env_name, app_key) do
    case System.get_env(env_name) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_bool_env(env_name, app_key) do
    case System.get_env(env_name) do
      nil ->
        :ok

      value when value in ["1", "true", "TRUE", "yes", "YES", "on", "ON"] ->
        Application.put_env(:ferricstore, app_key, true)

      value when value in ["0", "false", "FALSE", "no", "NO", "off", "OFF"] ->
        Application.put_env(:ferricstore, app_key, false)

      value ->
        raise "unsupported #{env_name}=#{inspect(value)}; expected boolean"
    end
  end

  defp put_optional_atom_env(env_name, app_key, allowed) do
    case System.get_env(env_name) do
      nil ->
        :ok

      value ->
        atom = String.to_existing_atom(value)

        if atom in allowed do
          Application.put_env(:ferricstore, app_key, atom)
        else
          raise "unsupported #{env_name}=#{inspect(value)}; expected one of #{inspect(allowed)}"
        end
    end
  end
end

FlowLMDBSoak.run()
