# Fresh-data diagnostic: traces startup snapshots, native flushes, and actor waits.
defmodule FerricstoreBench.BootstrapStallProbe do
  use GenServer
  alias Ferricstore.Bitcask.NIF
  @retry_mfa {Ferricstore.Flow.LMDBWriter.ProjectionOps, :retry_versioned_source_read, 6}
  @snapshot_mfas [
    {Ferricstore.Raft.WARaftBackend, :create_snapshot, 1},
    {Ferricstore.Raft.WARaftStorage, :create_snapshot, 2}
  ]
  @reconcile_mfas [
    {Ferricstore.Flow.LMDBRebuilder, :read_reconcile_batch, 6},
    {Ferricstore.Flow.LMDBRebuilder, :reconcile_decoded_state, 3},
    {Ferricstore.Flow.LMDBRebuilder, :finish_reconcile_batch, 17},
    {Ferricstore.Flow.LMDBRebuilder, :cleanup_stale_terminal_reverse, 3},
    {Ferricstore.Flow.LMDBRebuilder, :rebuild_composite_projection_ops, 3},
    {Ferricstore.Flow.LMDBRebuilder, :deleted_state_projection_ops, 3},
    {Ferricstore.Flow.LMDBRebuilder.TerminalProjection, :cleanup_stale_terminal_ops, 3},
    {Ferricstore.Flow.Query.QueryRowCodec, :encode, 4},
    {Ferricstore.Flow.LMDB, :active_index_delete_ops_result, 2},
    {Ferricstore.Flow.LMDB.Access, :write_batch, 2}
  ]
  @offset_mfas [
    {:ferricstore_waraft_spike_segment_log, :lookup_offset, 2},
    {:ferricstore_waraft_spike_segment_log, :lookup_offset_index, 2},
    {:ferricstore_waraft_spike_segment_log, :lookup_trusted_offset_index, 2},
    {:ferricstore_waraft_spike_segment_log, :offset_index_frame_matches, 5},
    {:ferricstore_waraft_spike_segment_log, :locate_offset_on_disk, 2}
  ]

  @mfas [
    {NIF, :v2_fsync_dir, 1},
    {NIF, :v2_fsync, 1},
    {NIF, :fs_atomic_replace_nofollow, 3},
    {NIF, :fs_copy_sync_nofollow, 2},
    {:file, :sync, 1},
    {:file, :datasync, 1},
    {:wa_raft_server, :bootstrap, 4},
    {:wa_raft_storage, :make_empty_snapshot, 5},
    {Ferricstore.Raft.WARaftStorage, :make_empty_snapshot, 5},
    {Ferricstore.Raft.WARaftStorage, :open_snapshot, 3},
    {Ferricstore.Raft.WARaftBackend, :create_snapshot, 1},
    {Ferricstore.Raft.WARaftStorage, :create_snapshot, 2},
    {Ferricstore.Flow.HistoryProjectedIndex, :persist, 2},
    {Ferricstore.Flow.HistoryProjector, :publish_projected_index, 4},
    {Ferricstore.Flow.LMDBWriter.Outbox, :reconcile_dirty_projection, 1},
    {Ferricstore.Flow.PolicyMirrorRecovery, :reconcile_shard, 5},
    {Ferricstore.Raft.WARaftStorage, :apply, 3},
    {Ferricstore.Raft.WARaftStorage, :apply, 4},
    @retry_mfa
  ]
  @trace_mfas @mfas ++ @reconcile_mfas ++ @offset_mfas
  @events [
    [:ferricstore, :shard, :startup_phase],
    [:ferricstore, :waraft, :backend, :startup_phase],
    [:ferricstore, :waraft, :storage, :startup_phase],
    [:ferricstore, :waraft, :segment_log, :startup_phase],
    [:ferricstore, :waraft, :commit, :stage]
  ]

  def run do
    parent = System.fetch_env!("BENCH_DATA_PARENT")
    true = File.dir?(parent)
    root = Path.join(parent, "bootstrap-probe-#{System.pid()}")
    if File.exists?(root), do: raise("fixture exists")

    for {key, value} <- [
          data_dir: root,
          node_name: nil,
          shard_count: 4,
          native_port: 0,
          health_port: 0,
          health_probe_port: 0,
          waraft_single_hset_coalescing: false
        ] do
      Application.put_env(:ferricstore, key, value)
    end

    Logger.configure(level: :error)
    {:ok, _} = Application.ensure_all_started(:telemetry)
    result = observe_startup(root, fn -> Application.ensure_all_started(:ferricstore_server) end)
    Application.stop(:ferricstore_server)
    Application.stop(:ferricstore)
    if match?({:ok, _}, result), do: File.rm_rf!(root)
  end

  def observe_startup(root, fun) do
    for {module, _, _} <- @mfas, do: Code.ensure_loaded!(module)
    {:ok, probe} = GenServer.start(__MODULE__, root)
    started = System.monotonic_time(:microsecond)
    result = fun.()
    elapsed = System.monotonic_time(:microsecond) - started
    :erlang.trace(:all, false, [:call, :set_on_spawn])
    for mfa <- @trace_mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    delivered = :erlang.trace_delivered(:all)

    receive do
      {:trace_delivered, :all, ^delivered} -> :ok
    after
      5_000 -> raise("trace delivery barrier timed out")
    end

    :telemetry.detach(__MODULE__)
    report = GenServer.call(probe, :snapshot)
    GenServer.stop(probe)

    report =
      Map.merge(report, %{
        root: root,
        parent: Path.dirname(root),
        elapsed_us: elapsed,
        result: inspect(result, limit: 40),
        success: match?({:ok, _}, result),
        component_only: true,
        otp: System.otp_release(),
        schedulers: System.schedulers_online(),
        dirty_io_schedulers: :erlang.system_info(:dirty_io_schedulers),
        startup_budget_ms:
          Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms, 300_000),
        storage_timeout_after_start:
          inspect(Application.fetch_env(:wa_raft, :raft_storage_call_timeout))
      })

    output = System.get_env("BENCH_BOOTSTRAP_OUTPUT") || System.fetch_env!("BENCH_OUTPUT")
    File.write!(output, Jason.encode!(report, pretty: true))

    IO.inspect(Map.take(report, [:success, :elapsed_us, :result, :timings]),
      label: "BOOTSTRAP_PROBE"
    )

    result
  end

  @impl true
  def init(root) do
    snapshots_only? = System.get_env("BENCH_VERIFY_SNAPSHOTS") == "1"
    mfas = if snapshots_only?, do: @snapshot_mfas, else: @mfas
    for mfa <- mfas, do: :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])

    if not snapshots_only? and System.get_env("BENCH_TRACE_RECONCILE") == "1" do
      for {module, _, _} = mfa <- @reconcile_mfas do
        Code.ensure_loaded!(module)
        :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])
      end
    end

    if not snapshots_only? and System.get_env("BENCH_TRACE_OFFSETS") == "1" do
      for mfa <- @offset_mfas do
        :erlang.trace_pattern(mfa, [{:_, [], [{:return_trace}]}], [:local])
      end
    end

    unless snapshots_only?,
      do: :erlang.trace_pattern(@retry_mfa, [{[:_, :_, :_, 0, :_, :_], [], []}], [:local])

    :erlang.trace(:all, true, [
      :call,
      :monotonic_timestamp,
      :set_on_spawn,
      {:tracer, self()}
    ])

    :telemetry.attach_many(
      __MODULE__,
      @events,
      fn event, measurements, metadata, pid ->
        send(pid, {:phase, Enum.join(event, "."), measurements, metadata})
      end,
      self()
    )

    unless snapshots_only?, do: Process.send_after(self(), :sample, 100)

    {:ok,
     %{
       root: root,
       snapshots_only: snapshots_only?,
       calls: %{},
       call_paths: %{},
       timings: %{},
       phases: [],
       slow_calls: [],
       samples: [],
       work_counts: %{},
       source_examples: [],
       snapshot_results: [],
       reconcile_failures: [],
       reconcile_failure_counts: %{},
       offset_examples: [],
       offset_counts: %{}
     }}
  end

  @impl true
  def handle_info(:sample, state) do
    actors =
      for pid <- Process.list(),
          info =
            Process.info(pid, [:registered_name, :status, :message_queue_len, :current_stacktrace]),
          info != nil,
          name = inspect(info[:registered_name]),
          String.contains?(name, [
            "raft_server_",
            "raft_storage_",
            "LMDBWriter",
            "LMDBFlushCoordinator",
            "history_projector",
            "Shard.",
            "application_controller",
            "Ferricstore.Supervisor",
            "ferricstore_waraft_backend_sup"
          ]) do
        %{
          pid: inspect(pid),
          name: name,
          status: info[:status],
          queue: info[:message_queue_len],
          stack: Enum.take(info[:current_stacktrace], 10) |> Enum.map(&inspect/1)
        }
      end

    at = System.monotonic_time(:microsecond)

    calls =
      Enum.map(state.calls, fn {{pid, mfa}, [started | _]} ->
        Map.merge(
          %{
            pid: inspect(pid),
            mfa: inspect(mfa),
            duration_us: at - System.convert_time_unit(started, :native, :microsecond)
          },
          Map.get(state.call_paths, {pid, mfa}, %{})
        )
      end)

    sample = %{
      at_us: at,
      actors: actors,
      calls: calls,
      run_queues: :erlang.statistics(:run_queue_lengths_all),
      storage_call_timeout_ms: Application.get_env(:wa_raft, :raft_storage_call_timeout, 60_000),
      history_progress: history_progress(),
      flush_coordinator: flush_coordinator()
    }

    Process.send_after(self(), :sample, 100)
    {:noreply, %{state | samples: Enum.take([sample | state.samples], 2_000)}}
  end

  def handle_info({:trace_ts, pid, :call, mfa, at}, state) when mfa in @mfas do
    {:noreply, %{state | calls: Map.update(state.calls, {pid, mfa}, [at], &[at | &1])}}
  end

  def handle_info({:trace_ts, pid, :call, {module, function, args}, at}, state)
      when is_list(args) do
    mfa = {module, function, length(args)}

    if mfa in @trace_mfas do
      paths =
        case {module, function, args} do
          {NIF, :fs_copy_sync_nofollow, [source, dest]} ->
            %{source: source, dest: dest}

          {NIF, _, [path | _]} when is_binary(path) ->
            %{path: path}

          {Ferricstore.Flow.HistoryProjectedIndex, :persist, [path, index]} ->
            %{path: path, index: index}

          {Ferricstore.Flow.HistoryProjector, :publish_projected_index, [_, shard, path, index]} ->
            %{path: path, index: index, shard: shard}

          {Ferricstore.Raft.WARaftStorage, :apply, [command, position | _]} ->
            %{position: inspect(position), command_shape: command_shape(command)}

          {Ferricstore.Raft.WARaftBackend, :create_snapshot, [shard]} ->
            %{shard: shard}

          {Ferricstore.Raft.WARaftStorage, :create_snapshot, [path, handle]} ->
            %{path: to_string(path), shard: handle.shard_index}

          {Ferricstore.Flow.LMDBRebuilder, :finish_reconcile_batch, args} ->
            %{
              path: Enum.at(args, 2),
              shard: Enum.at(args, 5),
              write_result: inspect(hd(args), limit: 30),
              previous_errors: Map.get(List.last(args), :lmdb_errors),
              projection_read_errors: Enum.at(args, 15)
            }

          {Ferricstore.Flow.LMDBRebuilder, :reconcile_decoded_state,
           [{key, _, _, record, locator}, path, previous]} ->
            %{
              path: path,
              key: key,
              version: record.version,
              state: record.state,
              locator: inspect(locator),
              previous_errors: elem(previous, 4)
            }

          {Ferricstore.Flow.Query.QueryRowCodec, :encode, [key, record, locator, _]} ->
            %{key: key, version: Map.get(record, :version), locator: inspect(locator)}

          {Ferricstore.Flow.LMDBRebuilder, :read_reconcile_batch,
           [entries, path, _, shard, _, retries]} ->
            %{
              path: path,
              shard: shard,
              retries: retries,
              keys: Enum.take(entries, 3) |> Enum.map(&inspect(elem(&1, 0)))
            }

          {Ferricstore.Flow.LMDB.Access, :write_batch, [path, ops]} ->
            %{path: path, op_count: length(ops)}

          {Ferricstore.Flow.LMDB, :active_index_delete_ops_result, [path, key]} ->
            %{path: path, key: key}

          {:ferricstore_waraft_spike_segment_log, _function, [dir, index | _]}
          when mfa in @offset_mfas ->
            path = to_string(dir)

            %{
              path: path,
              index: index,
              untrusted:
                :persistent_term.get(
                  {:ferricstore_waraft_spike_segment_log, :offset_index_untrusted, path},
                  false
                )
            }

          {Ferricstore.Flow.LMDBWriter.ProjectionOps, :retry_versioned_source_read,
           [source_state, key, expected, remaining, _, reason]} ->
            %{
              command_shape: "source_retry.#{reason}",
              key: key,
              expected: expected,
              remaining: remaining,
              source: source_state_summary(source_state, key)
            }

          _ ->
            %{}
        end

      {:noreply,
       %{
         state
         | calls:
             if(mfa == @retry_mfa,
               do: state.calls,
               else: Map.update(state.calls, {pid, mfa}, [at], &[at | &1])
             ),
           call_paths:
             if(mfa == @retry_mfa,
               do: state.call_paths,
               else: Map.put(state.call_paths, {pid, mfa}, paths)
             ),
           source_examples:
             if(mfa == @retry_mfa,
               do: Enum.take([paths | state.source_examples], 20),
               else: state.source_examples
             ),
           work_counts:
             if(Map.has_key?(paths, :command_shape),
               do: Map.update(state.work_counts, paths.command_shape, 1, &(&1 + 1)),
               else: state.work_counts
             )
       }}
    else
      {:noreply, state}
    end
  end

  def handle_info({:trace_ts, pid, :return_from, mfa, result, at}, state)
      when mfa in @trace_mfas do
    case Map.get(state.calls, {pid, mfa}, []) do
      [started | rest] ->
        us = System.convert_time_unit(at - started, :native, :microsecond)
        key = inspect(mfa)

        timings =
          Map.update(state.timings, key, %{count: 1, total_us: us, max_us: us}, fn item ->
            %{count: item.count + 1, total_us: item.total_us + us, max_us: max(item.max_us, us)}
          end)

        calls =
          if rest == [],
            do: Map.delete(state.calls, {pid, mfa}),
            else: Map.put(state.calls, {pid, mfa}, rest)

        slow =
          if us >= 100_000 do
            Enum.take(
              [
                %{
                  pid: inspect(pid),
                  mfa: key,
                  duration_us: us,
                  started_us: System.convert_time_unit(started, :native, :microsecond),
                  result: inspect(result, limit: 10)
                }
                | state.slow_calls
              ],
              500
            )
          else
            state.slow_calls
          end

        snapshot_results =
          if mfa in @snapshot_mfas do
            entry =
              Map.merge(Map.get(state.call_paths, {pid, mfa}, %{}), %{
                mfa: key,
                duration_us: us,
                result: inspect(result, limit: 40)
              })

            # Preserve the first real storage results even if repeated stop
            # later probes unregistered backend shard names.
            Enum.take(state.snapshot_results ++ [entry], 128)
          else
            state.snapshot_results
          end

        paths = Map.get(state.call_paths, {pid, mfa}, %{})

        reconcile_failure? =
          mfa in @reconcile_mfas and
            (match?({:error, _}, result) or
               (is_map(result) and
                  Map.get(result, :lmdb_errors, 0) >
                    Map.get(paths, :previous_errors, 0)) or
               (is_tuple(result) and tuple_size(result) == 5 and
                  is_integer(elem(result, 4)) and
                  elem(result, 4) > Map.get(paths, :previous_errors, 0)))

        failures =
          if reconcile_failure? do
            Enum.take(
              [
                Map.merge(paths, %{
                  mfa: key,
                  duration_us: us,
                  result: inspect(result, limit: 40, printable_limit: 500)
                })
                | state.reconcile_failures
              ],
              40
            )
          else
            state.reconcile_failures
          end

        failure_counts =
          if reconcile_failure?,
            do: Map.update(state.reconcile_failure_counts, key, 1, &(&1 + 1)),
            else: state.reconcile_failure_counts

        offset_counts =
          if mfa in @offset_mfas do
            outcome =
              cond do
                result == :not_found -> "missing"
                result == false -> "rejected_frame"
                match?({:ok, _}, result) or result == true -> "hit"
                true -> "error"
              end

            Map.update(state.offset_counts, key <> "." <> outcome, 1, &(&1 + 1))
          else
            state.offset_counts
          end

        offset_examples =
          if mfa in @offset_mfas and
               (result == :not_found or result == false or
                  elem(mfa, 1) == :locate_offset_on_disk) do
            Enum.take(
              [
                Map.merge(paths, %{mfa: key, result: inspect(result), duration_us: us})
                | state.offset_examples
              ],
              40
            )
          else
            state.offset_examples
          end

        {:noreply,
         %{
           state
           | calls: calls,
             timings: timings,
             slow_calls: slow,
             snapshot_results: snapshot_results,
             reconcile_failures: failures,
             reconcile_failure_counts: failure_counts,
             offset_counts: offset_counts,
             offset_examples: offset_examples
         }}

      [] ->
        {:noreply, state}
    end
  end

  def handle_info({:phase, event, measurements, metadata}, state) do
    phase = %{
      event: event,
      measurements: measurements,
      phase: inspect(metadata[:phase]),
      shard: metadata[:shard_index],
      result: inspect(metadata[:result]),
      command_shape: inspect(metadata[:command_shape])
    }

    {:noreply, %{state | phases: [phase | state.phases]}}
  end

  defp history_progress do
    ctx = FerricStore.Instance.get(:default)

    for shard <- 0..(ctx.shard_count - 1) do
      %{
        shard: shard,
        projected: :atomics.get(ctx.flow_history_projected_index, shard + 1),
        requested: :atomics.get(ctx.flow_history_requested_index, shard + 1),
        pending: :atomics.get(ctx.flow_history_projector_pending_entries, shard + 1)
      }
    end
  rescue
    _ -> []
  end

  defp source_state_summary(state, key) do
    table = Ferricstore.Flow.LMDBWriter.ProjectionOps.source_keydir(state)

    case :ets.lookup(table, key) do
      [] ->
        "missing"

      [{_, value, _, _, fid, _, _}] when is_binary(value) ->
        record = Ferricstore.Flow.decode_record(value)
        %{version: record.version, state: record.state, fid: inspect(fid)}

      [row] ->
        %{fid: inspect(elem(row, 4))}
    end
  rescue
    _ -> "unavailable"
  end

  defp flush_coordinator do
    name = Ferricstore.Flow.LMDBFlushCoordinator
    state = :sys.get_state(name, 30)

    %{
      max: state.max,
      available: state.available,
      holders:
        Enum.map(state.holders, fn {_, {pid, scope}} ->
          %{pid: inspect(pid), scope: inspect(scope)}
        end),
      queued: :queue.len(state.queue),
      active_scopes: inspect(state.active_scopes)
    }
  catch
    :exit, _ -> nil
  end

  defp command_shape({:ttb, binary}) do
    case Ferricstore.Raft.CommandStamp.decode_ttb(binary) do
      {:ok, {command, _}} -> command_shape(command)
      _ -> "invalid"
    end
  end

  defp command_shape(binary) when is_binary(binary), do: command_shape({:ttb, binary})

  defp command_shape({command, %{hlc_ts: _}}), do: command_shape(command)

  defp command_shape(command) when is_tuple(command) and tuple_size(command) > 0,
    do: inspect(elem(command, 0))

  defp command_shape(_), do: "other"

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply,
     %{
       timings: state.timings,
       phases: Enum.reverse(state.phases),
       slow_calls: Enum.reverse(state.slow_calls),
       samples: Enum.reverse(state.samples),
       work_counts: state.work_counts,
       source_examples: state.source_examples,
       snapshot_results: state.snapshot_results,
       snapshots_only: state.snapshots_only,
       reconcile_failures: Enum.reverse(state.reconcile_failures),
       reconcile_failure_counts: state.reconcile_failure_counts,
       offset_counts: state.offset_counts,
       offset_examples: Enum.reverse(state.offset_examples)
     }, state}
  end
end
