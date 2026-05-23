Logger.configure(level: :warning)
:logger.set_primary_config(:level, :warning)

defmodule FlowPythonBackendProfile do
  @moduledoc false

  @events [
    [:ferricstore, :flow, :create, :stop],
    [:ferricstore, :flow, :claim_due, :stop],
    [:ferricstore, :flow, :complete, :stop],
    [:ferricstore, :flow, :pipeline_claim_due_batch],
    [:ferricstore, :batcher, :slot_flush],
    [:ferricstore, :batcher, :quorum_submit],
    [:ferricstore, :bitcask, :append],
    [:ferricstore, :waraft, :batcher, :slot_flush],
    [:ferricstore, :waraft, :batcher, :hot_flush],
    [:ferricstore, :waraft, :segment_log, :append],
    [:ferricstore, :waraft, :storage, :payload_fsync],
    [:ferricstore, :waraft, :storage_blocked],
    [:ferricstore, :waraft, :commit_bytes, :rejected]
  ]

  def run do
    run_backend(:waraft)
  end

  defp run_backend(backend) do
    stop_started_apps()

    table = telemetry_table(backend)
    init_table(table)
    handler_id = "flow-python-backend-profile-#{backend}-#{System.unique_integer([:positive])}"
    {:ok, _} = Application.ensure_all_started(:telemetry)
    attach!(handler_id, table)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-flow-python-profile-#{backend}-#{System.unique_integer([:positive])}"
      )

    configure_app(data_dir)

    started = System.monotonic_time()

    try do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      port = FerricstoreServer.Listener.port()

      {output, status} =
        System.cmd(python(), benchmark_args(port), cd: sdk_dir(), stderr_to_stdout: true)

      elapsed_ms =
        System.convert_time_unit(System.monotonic_time() - started, :native, :millisecond)

      IO.puts("\n=== backend=#{backend} status=#{status} elapsed_ms=#{elapsed_ms} ===")
      IO.write(output)
      print_profile(table)
      maybe_flush_flow_projection()
      maybe_print_pending_keydir()
      maybe_print_storage_breakdown()
    after
      :telemetry.detach(handler_id)
      stop_started_apps()
      remove_data_dir(data_dir)
    end
  end

  defp configure_app(data_dir) do
    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)

    Application.put_env(:libcluster, :topologies, [])
    Application.put_env(:ferricstore, :data_dir, data_dir)
    Application.put_env(:ferricstore, :port, 0)
    Application.put_env(:ferricstore, :health_port, 0)
    Application.put_env(:ferricstore, :shard_count, int_env("SHARDS", 16))
    Application.put_env(:ferricstore, :protected_mode, false)
    Application.put_env(:ferricstore, :max_memory_bytes, 100_000_000_000)
    Application.put_env(:ferricstore, :memory_guard_interval_ms, 60 * 60 * 1000)
    IO.puts("flow_lmdb_projection=lagged")

    put_optional_limit_env(
      [
        "FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES",
        "FERRICSTORE_FLOW_HISTORY_PROJECTOR_MAX_PENDING_ENTRIES"
      ],
      :flow_history_projector_max_pending_entries
    )

    put_optional_limit_env(
      [
        "FLOW_LMDB_WRITER_MAX_MAILBOX_MESSAGES",
        "FERRICSTORE_FLOW_LMDB_WRITER_MAX_MAILBOX_MESSAGES"
      ],
      :flow_lmdb_writer_max_mailbox_messages
    )

    put_optional_limit_env(
      ["FLOW_LMDB_WRITER_MAX_ENQUEUE_OPS", "FERRICSTORE_FLOW_LMDB_WRITER_MAX_ENQUEUE_OPS"],
      :flow_lmdb_writer_max_enqueue_ops
    )

    Application.delete_env(:ferricstore, :waraft_log_module)
    put_optional_int_env("WARAFT_COMMIT_BATCH_INTERVAL_MS", :waraft_commit_batch_interval_ms)
    put_optional_int_env("WARAFT_COMMIT_BATCH_MAX", :waraft_commit_batch_max)

    put_optional_int_env(
      "WARAFT_MAX_LOG_ENTRIES_PER_HEARTBEAT",
      :waraft_max_log_entries_per_heartbeat
    )

    put_optional_int_env("WARAFT_MAX_HEARTBEAT_SIZE", :waraft_max_heartbeat_size)
    put_optional_int_env("WARAFT_APPLY_LOG_BATCH_SIZE", :waraft_apply_log_batch_size)
    put_optional_int_env("WARAFT_APPLY_BATCH_MAX_BYTES", :waraft_apply_batch_max_bytes)

    put_optional_limit_env(
      ["WARAFT_SEGMENT_LOG_MAX_ETS_BYTES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_BYTES"],
      :waraft_segment_log_max_ets_bytes
    )

    put_optional_limit_env(
      ["WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MAX_ETS_ENTRIES"],
      :waraft_segment_log_max_ets_entries
    )

    put_optional_limit_env(
      ["WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES", "FERRICSTORE_WARAFT_SEGMENT_LOG_MIN_ETS_ENTRIES"],
      :waraft_segment_log_min_ets_entries
    )

    put_optional_int_env(
      "WARAFT_SEGMENT_PREALLOCATE_BYTES",
      :waraft_segment_log_preallocate_bytes
    )

    put_optional_int_env(
      "WARAFT_SEGMENT_RECORDS_PER_SEGMENT",
      :waraft_segment_log_records_per_segment
    )
  end

  defp benchmark_args(port) do
    [
      "examples/dbos_style_benchmark.py",
      "--url",
      "redis://127.0.0.1:#{port}/0",
      "--mode",
      "queued",
      "--queued-shape",
      "live",
      "--transport",
      env("TRANSPORT", "many"),
      "--worker-api",
      "lowlevel",
      "--worker-mode",
      "owner-wakeup",
      "--partition-mode",
      "auto",
      "--flows",
      env("FLOWS", "1000000"),
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
      env("SHARDS", "16"),
      "--wake-coalesce-ms",
      "0",
      "--claim-job-only"
    ]
  end

  defp attach!(handler_id, table) do
    :ok =
      :telemetry.attach_many(
        handler_id,
        @events,
        fn event, measurements, metadata, _config ->
          record_event(table, event, measurements, metadata)
        end,
        nil
      )
  end

  defp record_event(table, event, measurements, metadata) do
    key = {event, event_group(event, metadata)}
    duration_us = duration_us(measurements)
    batch_size = int_measurement(measurements, :batch_size)
    count = int_measurement(measurements, :count)
    bytes = int_measurement(measurements, :batch_bytes) + int_measurement(measurements, :bytes)

    :ets.update_counter(
      table,
      key,
      [
        {2, 1},
        {3, duration_us},
        {4, batch_size},
        {5, count},
        {6, bytes}
      ],
      {key, 0, 0, 0, 0, 0}
    )

    update_max(table, {:max_batch, key}, batch_size)
  rescue
    _ -> :ok
  end

  defp event_group([:ferricstore, :batcher, :quorum_submit], metadata),
    do: Map.get(metadata, :command_shape, :unknown)

  defp event_group([:ferricstore, :batcher, :slot_flush], metadata),
    do: {Map.get(metadata, :write_path, :unknown), Map.get(metadata, :prefix, :unknown)}

  defp event_group([:ferricstore, :bitcask, :append], metadata),
    do: Map.get(metadata, :status, :unknown)

  defp event_group([:ferricstore, :waraft, :batcher, :hot_flush], metadata),
    do: {Map.get(metadata, :kind, :unknown), Map.get(metadata, :result, :unknown)}

  defp event_group([:ferricstore, :waraft, :batcher, :slot_flush], metadata),
    do: {Map.get(metadata, :prefix, :unknown), Map.get(metadata, :result, :unknown)}

  defp event_group([:ferricstore, :waraft, :segment_log, :append], metadata),
    do:
      {Map.get(metadata, :kind, :unknown),
       if(Map.get(metadata, :new_segment), do: :new_segment, else: :same_segment)}

  defp event_group(_event, _metadata), do: :all

  defp duration_us(%{duration_us: value}) when is_integer(value), do: value
  defp duration_us(%{duration_ms: value}) when is_integer(value), do: value * 1000

  defp duration_us(%{duration: value}) when is_integer(value),
    do: System.convert_time_unit(value, :native, :microsecond)

  defp duration_us(_measurements), do: 0

  defp int_measurement(measurements, key) do
    case Map.get(measurements, key, 0) do
      value when is_integer(value) and value >= 0 -> value
      _ -> 0
    end
  end

  defp update_max(table, key, value) when is_integer(value) and value > 0 do
    current =
      case :ets.lookup(table, key) do
        [{^key, existing}] -> existing
        _ -> 0
      end

    if value > current, do: :ets.insert(table, {key, value})
  end

  defp update_max(_table, _key, _value), do: :ok

  defp print_profile(table) do
    rows =
      table
      |> :ets.tab2list()
      |> Enum.filter(&match?({{event, _group}, _, _, _, _, _} when is_list(event), &1))
      |> Enum.sort_by(fn {{event, group}, _events, _duration, _batch, _count, _bytes} ->
        {Enum.join(Enum.map(event, &to_string/1), "."), inspect(group)}
      end)

    IO.puts("telemetry_profile:")

    Enum.each(rows, fn {key = {event, group}, events, duration_us, batch_size, count, bytes} ->
      max_batch = lookup(table, {:max_batch, key})
      avg_duration_us = if events > 0, do: div(duration_us, events), else: 0
      avg_batch = if events > 0, do: Float.round(batch_size / events, 2), else: 0.0

      IO.puts(
        "  event=#{Enum.join(Enum.map(event, &to_string/1), ".")} group=#{inspect(group)} " <>
          "events=#{events} count=#{count} batch_sum=#{batch_size} avg_batch=#{avg_batch} " <>
          "max_batch=#{max_batch} bytes=#{bytes} total_ms=#{Float.round(duration_us / 1000, 3)} " <>
          "avg_us=#{avg_duration_us}"
      )
    end)
  end

  defp maybe_print_pending_keydir do
    if System.get_env("INSPECT_PENDING") in ["1", "true", "TRUE", "yes", "YES"] do
      print_pending_keydir("pending_keydir")
      Process.sleep(int_env("INSPECT_PENDING_AFTER_MS", 500))
      print_pending_keydir("pending_keydir_after_wait")
    end
  rescue
    error -> IO.puts("pending_keydir_inspect_failed=#{inspect(error)}")
  end

  defp print_pending_keydir(label) do
    ctx = FerricStore.Instance.get(:default)
    IO.puts("#{label}:")

    for shard <- 0..(ctx.shard_count - 1) do
      table = elem(ctx.keydir_refs, shard)

      count =
        :ets.select_count(table, [
          {{:_, :_, :_, :_, :pending, :_, :_}, [], [true]}
        ])

      sample =
        table
        |> :ets.match_object({:_, :_, :_, :_, :pending, :_, :_})
        |> Enum.take(5)

      IO.puts("  shard=#{shard} pending=#{count} sample=#{inspect(sample)}")
    end
  end

  defp maybe_print_storage_breakdown do
    if System.get_env("INSPECT_STORAGE") in ["1", "true", "TRUE", "yes", "YES"] do
      print_apply_projection_cache()
      print_keydir_breakdown()
      print_flow_index_breakdown()
    end
  rescue
    error -> IO.puts("storage_breakdown_failed=#{inspect(error)}")
  end

  defp print_apply_projection_cache do
    table = :ferricstore_waraft_apply_projection_cache

    case :ets.whereis(table) do
      :undefined ->
        IO.puts("apply_projection_cache: missing")

      tid ->
        words = :ets.info(tid, :memory) || 0
        size = :ets.info(tid, :size) || 0
        bytes = words * :erlang.system_info(:wordsize)

        IO.puts("apply_projection_cache: rows=#{size} ets_bytes=#{bytes}")
    end
  end

  defp maybe_flush_flow_projection do
    if System.get_env("INSPECT_FLUSH") in ["1", "true", "TRUE", "yes", "YES"] do
      ctx = FerricStore.Instance.get(:default)
      timeout = int_env("INSPECT_FLUSH_TIMEOUT_MS", 30_000)

      IO.puts(
        "inspect_flush_lmdb=#{inspect(Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count, timeout))}"
      )

      history_result =
        for shard <- 0..(ctx.shard_count - 1), reduce: :ok do
          acc ->
            case Ferricstore.Flow.HistoryProjector.flush(ctx, shard, timeout) do
              :ok -> acc
              {:error, _reason} = error -> error
            end
        end

      IO.puts("inspect_flush_history=#{inspect(history_result)}")
    end
  rescue
    error -> IO.puts("inspect_flush_failed=#{inspect(error)}")
  end

  defp print_keydir_breakdown do
    ctx = FerricStore.Instance.get(:default)
    IO.puts("keydir_breakdown:")

    totals =
      for shard <- 0..(ctx.shard_count - 1),
          reduce: %{rows: 0, hot: 0, hot_bytes: 0, nils: 0, kinds: %{}} do
        acc ->
          table = elem(ctx.keydir_refs, shard)

          {rows, hot, hot_bytes, nils, kinds} =
            :ets.foldl(
              fn
                {key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size},
                {rows, hot, bytes, nils, kinds}
                when is_binary(value) ->
                  {rows + 1, hot + 1, bytes + byte_size(value), nils, count_key_kind(kinds, key)}

                {key, nil, _expire_at_ms, _lfu, _file_id, _offset, _value_size},
                {rows, hot, bytes, nils, kinds} ->
                  {rows + 1, hot, bytes, nils + 1, count_key_kind(kinds, key)}

                _row, {rows, hot, bytes, nils, kinds} ->
                  {rows + 1, hot, bytes, nils, kinds}
              end,
              {0, 0, 0, 0, %{}},
              table
            )

          IO.puts(
            "  shard=#{shard} rows=#{rows} hot=#{hot} hot_bytes=#{hot_bytes} nil=#{nils} kinds=#{inspect(kinds)}"
          )

          %{
            rows: acc.rows + rows,
            hot: acc.hot + hot,
            hot_bytes: acc.hot_bytes + hot_bytes,
            nils: acc.nils + nils,
            kinds: merge_counts(acc.kinds, kinds)
          }
      end

    IO.puts(
      "  total rows=#{totals.rows} hot=#{totals.hot} hot_bytes=#{totals.hot_bytes} nil=#{totals.nils} kinds=#{inspect(totals.kinds)}"
    )
  end

  defp count_key_kind(kinds, key), do: Map.update(kinds, key_kind(key), 1, &(&1 + 1))

  defp key_kind("X:f:" <> _rest), do: :flow_history

  defp key_kind("f:" <> rest) when is_binary(rest) do
    cond do
      String.contains?(rest, "}:s:") -> :flow_state
      String.contains?(rest, "}:v:") -> :flow_value
      String.contains?(rest, "}:policy:") -> :flow_policy
      true -> :flow_other
    end
  end

  defp key_kind(_key), do: :other

  defp merge_counts(left, right) do
    Enum.reduce(right, left, fn {key, value}, acc ->
      Map.update(acc, key, value, &(&1 + value))
    end)
  end

  defp print_flow_index_breakdown do
    ctx = FerricStore.Instance.get(:default)
    IO.puts("flow_index_breakdown:")

    for shard <- 0..(ctx.shard_count - 1) do
      {index, lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard)
      index_size = ets_size(index)
      lookup_size = ets_size(lookup)
      IO.puts("  shard=#{shard} index=#{index_size} lookup=#{lookup_size}")
    end
  end

  defp ets_size(table) do
    case :ets.whereis(table) do
      :undefined -> 0
      tid -> :ets.info(tid, :size) || 0
    end
  end

  defp lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> value
      _ -> 0
    end
  end

  defp init_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ets.new(table, [:named_table, :public, :set])
      _ -> :ets.delete_all_objects(table)
    end
  end

  defp telemetry_table(backend), do: :"flow_python_backend_profile_#{backend}"

  defp stop_started_apps do
    for app <- [:ferricstore_server, :ferricstore_ecto, :ferricstore_session, :ferricstore] do
      _ = Application.stop(app)
    end
  end

  defp remove_data_dir(data_dir) do
    # WARaft benchmarks preallocate large segment files. BEAM File.rm_rf/1 can spend
    # minutes cleaning them up on macOS, which makes benchmark process time useless.
    case System.cmd("rm", ["-rf", data_dir], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> IO.puts("cleanup_failed status=#{status} output=#{inspect(output)}")
    end
  end

  defp python, do: env("PYTHON", Path.join(sdk_dir(), ".venv/bin/python"))
  defp sdk_dir, do: env("SDK_DIR", "/Users/yoavgea/repos/ferricstore-python")

  defp int_env(name, default) do
    name
    |> env(Integer.to_string(default))
    |> String.to_integer()
  end

  defp env(name, default), do: System.get_env(name) || default

  defp put_optional_int_env(env_name, app_key) do
    case System.get_env(env_name) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, String.to_integer(value))
    end
  end

  defp put_optional_limit_env(env_names, app_key) when is_list(env_names) do
    case Enum.find_value(env_names, fn env_name ->
           case System.get_env(env_name) do
             nil -> nil
             value -> value
           end
         end) do
      nil -> :ok
      value -> Application.put_env(:ferricstore, app_key, parse_limit_env(value))
    end
  end

  defp parse_limit_env(value) do
    case String.downcase(String.trim(value)) do
      value when value in ["", "false", "off", "infinity", "inf", "unlimited"] ->
        :infinity

      value ->
        String.to_integer(value)
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
end

FlowPythonBackendProfile.run()
