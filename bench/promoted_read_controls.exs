# Native TCP SET/GET and public Flow lifecycle controls for the Router change.
Code.require_file("support/promoted_read_variant.exs", __DIR__)
Code.require_file("support/snapshot_copy_variant.exs", __DIR__)
Code.require_file("support/compaction_sync_variant.exs", __DIR__)
Code.require_file("support/flow_source_wait_variant.exs", __DIR__)
Code.require_file("support/flow_snapshot_variant.exs", __DIR__)
Code.require_file("support/flow_cache_variant.exs", __DIR__)
Code.require_file("support/offset_scan_variant.exs", __DIR__)

Code.require_file("support/bootstrap_stall_probe.exs", __DIR__)
Code.require_file("support/hash_stall_profiler.exs", __DIR__)

defmodule FerricstoreBench.PromotedReadControls do
  alias FerricstoreServer.Native.{Codec, Listener}

  def run do
    {variant, source, code_root} = FerricstoreBench.PromotedReadVariant.prepare()
    {source_wait, writer_source, writer_root} = FerricstoreBench.FlowSourceWaitVariant.prepare()

    {snapshot_projection, projection_sources, projection_root} =
      FerricstoreBench.FlowSnapshotVariant.prepare()

    {flow_cache, cache_sources, cache_root} = FerricstoreBench.FlowCacheVariant.prepare()
    {offset_scan, offset_source, offset_root} = FerricstoreBench.OffsetScanVariant.prepare()

    {compaction_sync, compaction_source, compaction_root} =
      FerricstoreBench.CompactionSyncVariant.prepare()

    {snapshot_copy, snapshot_copy_source, snapshot_code_root} =
      FerricstoreBench.SnapshotCopyVariant.prepare()

    parent_dir = System.get_env("BENCH_DATA_PARENT", Path.join(System.tmp_dir!(), "opencode"))
    root = Path.join(parent_dir, "router-control-data-#{System.pid()}")
    if File.exists?(root), do: raise("fixture exists")
    hset_group = System.get_env("BENCH_HSET_GROUP", "direct")
    seconds = System.get_env("BENCH_CONTROL_SECONDS", "5") |> String.to_integer()
    warmup_seconds = System.get_env("BENCH_CONTROL_WARMUP_SECONDS", "1") |> String.to_integer()
    true = seconds > 0 and warmup_seconds >= 0
    true = hset_group in ["direct", "coalesced"]
    Application.put_env(:ferricstore, :waraft_single_hset_coalescing, hset_group == "coalesced")

    for {key, value} <- [
          data_dir: root,
          node_name: nil,
          shard_count: 4,
          native_port: 0,
          health_port: 0,
          health_probe_port: 0
        ] do
      Application.put_env(:ferricstore, key, value)
    end

    Logger.configure(level: :error)

    try do
      {:ok, _} =
        if System.get_env("BENCH_BOOTSTRAP_OUTPUT") do
          {:ok, _} = Application.ensure_all_started(:telemetry)

          FerricstoreBench.BootstrapStallProbe.observe_startup(root, fn ->
            Application.ensure_all_started(:ferricstore_server)
          end)
        else
          Application.ensure_all_started(:ferricstore_server)
        end

      profile? = System.get_env("BENCH_CONTROL_PROFILE") == "1"

      if profile?,
        do:
          {:ok, _} = FerricstoreBench.HashStallProfiler.start(FerricStore.Instance.get(:default))

      kinds =
        System.get_env("BENCH_CONTROL_CASES", "native_set_get,flow_lifecycle")
        |> String.split(",")
        |> Enum.map(&String.to_existing_atom/1)

      true = Enum.all?(kinds, &(&1 in [:native_set_get, :flow_lifecycle]))

      results =
        for kind <- kinds do
          parent = self()

          tasks =
            for client <- 0..7 do
              Task.async(fn ->
                socket =
                  if kind == :native_set_get do
                    {:ok, socket} =
                      :gen_tcp.connect(
                        {127, 0, 0, 1},
                        Listener.port(),
                        [:binary, active: false, nodelay: true],
                        5_000
                      )

                    socket
                  end

                try do
                  warm =
                    loop(
                      kind,
                      socket,
                      client,
                      System.monotonic_time(:microsecond) + warmup_seconds * 1_000_000,
                      0,
                      []
                    )

                  send(parent, {:ready, self()})

                  receive do
                    {:run, deadline} ->
                      if value = System.get_env("BENCH_CONTROL_CYCLES_PER_CLIENT"),
                        do: Process.put(:control_remaining_cycles, String.to_integer(value))

                      loop(kind, socket, client, deadline, warm.next, [])
                  end
                after
                  if socket, do: :gen_tcp.close(socket)
                end
              end)
            end

          for %{pid: pid} <- tasks do
            receive do
              {:ready, ^pid} -> :ok
            after
              30_000 -> raise("warmup failed")
            end
          end

          started = System.monotonic_time(:microsecond)
          for %{pid: pid} <- tasks, do: send(pid, {:run, started + seconds * 1_000_000})
          reports = Task.await_many(tasks, seconds * 1_000 + 30_000)
          samples = Enum.flat_map(reports, & &1.samples) |> Enum.sort()
          elapsed = (System.monotonic_time(:microsecond) - started) / 1.0e6

          %{
            scenario: kind,
            cycles_per_second: length(samples) / elapsed,
            p50_us: q(samples, 0.5),
            p95_us: q(samples, 0.95),
            p99_us: q(samples, 0.99),
            p999_us: q(samples, 0.999),
            max_us: List.last(samples),
            cycles: length(samples),
            measured_elapsed_seconds: elapsed,
            clients: 8
          }
        end

      report = %{
        variant: variant,
        hset_group: hset_group,
        source_wait: source_wait,
        snapshot_projection: snapshot_projection,
        offset_scan: offset_scan,
        offset_source: offset_source,
        offset_beam_md5:
          Base.encode16(:ferricstore_waraft_spike_segment_log.module_info(:md5), case: :lower),
        flow_cache: flow_cache,
        cache_sources: cache_sources,
        projection_sources: projection_sources,
        writer_source: writer_source,
        writer_beam_md5:
          Base.encode16(Ferricstore.Flow.LMDBWriter.module_info(:md5), case: :lower),
        flow_clock: System.get_env("BENCH_FLOW_CLOCK", "historical"),
        snapshot_copy: snapshot_copy,
        snapshot_copy_source: snapshot_copy_source,
        compaction_sync: compaction_sync,
        compaction_source: compaction_source,
        promoted_beam_md5:
          Base.encode16(Ferricstore.Store.Shard.Compound.Promoted.module_info(:md5), case: :lower),
        seconds: seconds,
        cycles_per_client: System.get_env("BENCH_CONTROL_CYCLES_PER_CLIENT"),
        warmup_seconds: warmup_seconds,
        bootstrap_traced: System.get_env("BENCH_BOOTSTRAP_OUTPUT") != nil,
        quiesce_before_stop: System.get_env("BENCH_QUIESCE_WRITES") == "1",
        results: results,
        source: source,
        data_parent: parent_dir,
        publication_identity: FerricstoreBench.PromotedReadVariant.source_identity(),
        router_beam_md5: Base.encode16(Ferricstore.Store.Router.module_info(:md5), case: :lower),
        errors: 0
      }

      report =
        if profile?,
          do: Map.put(report, :profile, FerricstoreBench.HashStallProfiler.finish()),
          else: report

      trial = System.get_env("BENCH_TRIAL", "1")

      File.write!(
        System.get_env(
          "BENCH_OUTPUT",
          "bench/results/promoted-read-controls-#{variant}-#{trial}.json"
        ),
        Jason.encode!(report, pretty: true)
      )

      IO.puts(
        Jason.encode!(
          Map.drop(report, [
            :source,
            :snapshot_copy_source,
            :compaction_source,
            :writer_source,
            :projection_sources,
            :cache_sources,
            :offset_source,
            :profile
          ])
        )
      )
    after
      cond do
        System.get_env("BENCH_TRACE_SHUTDOWN") == "1" or
            System.get_env("BENCH_VERIFY_SNAPSHOTS") == "1" ->
          System.put_env("BENCH_BOOTSTRAP_OUTPUT", System.fetch_env!("BENCH_SHUTDOWN_OUTPUT"))

          result =
            FerricstoreBench.BootstrapStallProbe.observe_startup(root, fn ->
              task =
                Task.async(fn ->
                  stop_applications()
                  {:ok, :stopped}
                end)

              timeout =
                System.get_env("BENCH_SHUTDOWN_TIMEOUT_MS", "30000") |> String.to_integer()

              case Task.yield(task, timeout) do
                {:ok, result} ->
                  result

                nil ->
                  Task.shutdown(task, :brutal_kill)
                  {:error, :shutdown_timeout}
              end
            end)

          probe =
            System.fetch_env!("BENCH_SHUTDOWN_OUTPUT")
            |> File.read!()
            |> Jason.decode!()

          storage_results =
            Enum.filter(Map.get(probe, "snapshot_results", []), fn row ->
              row["mfa"] == "{Ferricstore.Raft.WARaftStorage, :create_snapshot, 2}"
            end)

          snapshots_succeeded? =
            length(storage_results) == 4 and Enum.all?(storage_results, &(&1["result"] == ":ok"))

          probe =
            probe
            |> Map.put("application_stop_completed", result == {:ok, :stopped})
            |> Map.put("snapshot_results_verified", true)
            |> Map.put("storage_snapshots_succeeded", snapshots_succeeded?)
            |> Map.put("success", result == {:ok, :stopped} and snapshots_succeeded?)

          File.write!(
            System.fetch_env!("BENCH_SHUTDOWN_OUTPUT"),
            Jason.encode!(probe, pretty: true)
          )

          if result != {:ok, :stopped} or not snapshots_succeeded?, do: System.halt(1)

        output = System.get_env("BENCH_SHUTDOWN_OUTPUT") ->
          started = System.monotonic_time(:microsecond)

          task =
            Task.async(fn ->
              stop_applications()
              {:ok, :stopped}
            end)

          timeout = System.get_env("BENCH_SHUTDOWN_TIMEOUT_MS", "30000") |> String.to_integer()

          result =
            case Task.yield(task, timeout) do
              {:ok, result} ->
                result

              nil ->
                Task.shutdown(task, :brutal_kill)
                {:error, :shutdown_timeout}
            end

          File.write!(
            output,
            Jason.encode!(
              %{
                success: result == {:ok, :stopped},
                result: inspect(result),
                elapsed_us: System.monotonic_time(:microsecond) - started,
                diagnostic_only: false,
                snapshot_results_verified: false
              },
              pretty: true
            )
          )

          if result != {:ok, :stopped}, do: System.halt(1)

        true ->
          Application.stop(:ferricstore_server)
          Application.stop(:ferricstore)
      end

      File.rm_rf!(root)
      if code_root, do: File.rm_rf!(code_root)
      if snapshot_code_root, do: File.rm_rf!(snapshot_code_root)
      if compaction_root, do: File.rm_rf!(compaction_root)
      if writer_root, do: File.rm_rf!(writer_root)
      if projection_root, do: File.rm_rf!(projection_root)
      if cache_root, do: File.rm_rf!(cache_root)
      if offset_root, do: File.rm_rf!(offset_root)
    end
  end

  defp loop(kind, socket, client, deadline, i, samples) do
    started = System.monotonic_time(:microsecond)

    if started >= deadline or Process.get(:control_remaining_cycles) == 0 do
      %{next: i, samples: samples}
    else
      operation(kind, socket, client, i)

      if remaining = Process.get(:control_remaining_cycles),
        do: Process.put(:control_remaining_cycles, remaining - 1)

      us = System.monotonic_time(:microsecond) - started
      loop(kind, socket, client, deadline, i + 1, [us | samples])
    end
  end

  defp stop_applications do
    if System.get_env("BENCH_QUIESCE_WRITES") == "1" do
      lease = {self(), make_ref()}
      :ok = Ferricstore.Raft.WARaftBackend.pause_writes_for_sync_all(4, lease, 30_000)
    end

    :ok = Application.stop(:ferricstore_server)
    :ok = Application.stop(:ferricstore)
  end

  defp operation(:native_set_get, socket, client, i) do
    key = "router-control:#{client}"
    value = "#{client}:#{i}"
    _ = request(socket, 0x0102, i * 2 + 1, %{"key" => key, "value" => value})

    <<0x82, 1, _size::unsigned-32, actual::binary>> =
      request(socket, 0x0101, i * 2 + 2, %{"key" => key})

    ^value = actual
  end

  defp operation(:flow_lifecycle, _socket, client, i) do
    partition = "control-partition:#{client}"
    id = "control:#{client}:#{i}"
    type = "control-type:#{client}:#{i}"

    now =
      if System.get_env("BENCH_FLOW_CLOCK", "historical") == "wall",
        do: System.system_time(:millisecond),
        else: 1_000

    :ok =
      FerricStore.flow_create(id,
        partition_key: partition,
        type: type,
        state: "queued",
        payload: "payload",
        run_at_ms: now,
        now_ms: now
      )

    {:ok, [claimed]} =
      FerricStore.flow_claim_due(type,
        partition_key: partition,
        state: "queued",
        worker: "control-worker",
        lease_ms: 30_000,
        limit: 1,
        now_ms: now
      )

    ^id = claimed.id

    :ok =
      FerricStore.flow_complete(id, claimed.lease_token,
        partition_key: partition,
        fencing_token: claimed.fencing_token,
        now_ms: now + 10
      )

    {:ok, completed} = FerricStore.flow_get(id, partition_key: partition)
    "completed" = completed.state
  end

  defp request(socket, opcode, id, payload) do
    :ok = :gen_tcp.send(socket, Codec.encode_frame(opcode, 1, id, Codec.encode_value(payload)))

    {:ok,
     <<"FSNP", 0x81, _flags, 1::unsigned-32, ^opcode::unsigned-16, ^id::unsigned-64,
       size::unsigned-32>>} = :gen_tcp.recv(socket, 24, 10_000)

    {:ok, <<0::unsigned-16, payload::binary>>} = :gen_tcp.recv(socket, size, 10_000)
    payload
  end

  defp q(samples, q), do: Enum.at(samples, ceil(length(samples) * q) - 1)
end

FerricstoreBench.PromotedReadControls.run()
