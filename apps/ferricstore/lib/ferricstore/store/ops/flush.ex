defmodule Ferricstore.Store.Ops.Flush do
  @moduledoc false

  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Raft.MembershipGate
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.WARaftBackend.SyncGate
  alias Ferricstore.Store.Router

  @pause_timeout_ms 30_000
  @resume_timeout_ms 5_000
  @cleanup_timeout_ms 60_000
  @replica_apply_poll_ms 10

  def flush(ctx) do
    operation = fn ->
      LimitCache.with_drained_cache(ctx, fn ->
        with_writes_paused(ctx, fn pause_token -> do_flush(ctx, pause_token) end)
      end)
    end

    if Router.durable_context?(ctx) do
      MembershipGate.with_stable_membership(operation)
    else
      operation.()
    end
  end

  defp do_flush(ctx, pause_token) do
    flush_epoch = Ferricstore.HLC.now()

    with :ok <- SharedRefBackfill.invalidate_verified!(ctx.name, ctx.shard_count),
         :ok <- validate_internal_keydirs(ctx),
         {:ok, flush_positions} <- flush_replicated_shards(ctx, flush_epoch),
         :ok <- run_post_flush_cleanup(ctx, pause_token, flush_positions) do
      :ok
    end
  end

  defp validate_internal_keydirs(ctx) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      keydir = elem(ctx.keydir_refs, shard_index)

      try do
        case :ets.info(keydir, :size) do
          size when is_integer(size) and size >= 0 ->
            {:cont, :ok}

          _unavailable ->
            raise ArgumentError, "keydir table is unavailable"
        end
      rescue
        error in ArgumentError ->
          {:halt, {:error, {:flush_internal_keydir_unavailable, error}}}
      end
    end)
  end

  defp with_writes_paused(ctx, fun) when is_function(fun, 1) do
    pause_lease = SyncGate.lease(self())

    case pause_all_writes(ctx, pause_lease) do
      {:ok, pause_token} ->
        run_while_paused(ctx, pause_token, fun)

      {:error, _reason} = error ->
        error
    end
  end

  defp run_while_paused(ctx, pause_token, fun) do
    try do
      result = fun.(pause_token)
      merge_resume_result(result, resume_all_writes(ctx, pause_token))
    rescue
      error ->
        _ = resume_all_writes(ctx, pause_token)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        _ = resume_all_writes(ctx, pause_token)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp merge_resume_result(result, :ok), do: result

  defp merge_resume_result(:ok, {:error, reason}),
    do: {:error, {:flush_resume_failed, reason}}

  defp merge_resume_result({:error, flush_reason}, {:error, resume_reason}),
    do: {:error, {:flush_failed_and_resume_failed, flush_reason, resume_reason}}

  defp merge_resume_result(result, {:error, resume_reason}),
    do: {:error, {:flush_result_and_resume_failed, result, resume_reason}}

  defp pause_all_writes(ctx, pause_lease) do
    if Router.durable_context?(ctx) do
      pause_durable_writes(ctx, pause_lease)
    else
      pause_standalone_writes(ctx, pause_lease)
    end
  end

  defp pause_durable_writes(ctx, pause_lease) do
    with {:ok, membership_snapshot} <- durable_membership_snapshot(ctx.shard_count) do
      pause_nodes =
        durable_pause_nodes(membership_snapshot.cleanup_targets, node())

      case pause_durable_nodes(
             pause_nodes,
             ctx.name,
             ctx.shard_count,
             pause_lease
           ) do
        {:ok, paused_nodes} ->
          case verify_durable_membership(membership_snapshot) do
            :ok ->
              {:ok, {:durable, paused_nodes, pause_lease, membership_snapshot.cleanup_targets}}

            {:error, _reason} = error ->
              _ =
                resume_durable_nodes(
                  paused_nodes,
                  ctx.name,
                  ctx.shard_count,
                  pause_lease
                )

              error
          end

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp pause_durable_nodes(nodes, instance_name, shard_count, pause_lease) do
    nodes
    |> Enum.reduce_while({:ok, []}, fn target_node, {:ok, paused_nodes} ->
      case call_durable_pause(target_node, instance_name, shard_count, pause_lease) do
        :ok ->
          {:cont, {:ok, [target_node | paused_nodes]}}

        result ->
          paused_nodes = Enum.reverse(paused_nodes)
          _ = resume_durable_nodes(paused_nodes, instance_name, shard_count, pause_lease)
          {:halt, {:error, {:flush_pause_failed, target_node, result}}}
      end
    end)
    |> case do
      {:ok, paused_nodes} -> {:ok, Enum.reverse(paused_nodes)}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  def pause_local_durable(instance_name, shard_count, pause_lease)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    with {:ok, ctx} <- local_instance(instance_name, shard_count),
         :ok <- Batcher.pause_writes_for_sync_all(shard_count, pause_lease, @pause_timeout_ms) do
      case pause_local_shards(ctx, pause_lease) do
        :ok ->
          :ok

        {:error, reason} ->
          _ = Batcher.resume_writes_for_sync_all(shard_count, pause_lease, @resume_timeout_ms)
          {:error, reason}
      end
    end
  end

  def pause_local_durable(_instance_name, shard_count, _pause_lease),
    do: {:error, {:invalid_shard_count, shard_count}}

  defp call_durable_pause(target_node, instance_name, shard_count, pause_lease)
       when target_node == node() do
    pause_local_durable(instance_name, shard_count, pause_lease)
  end

  defp call_durable_pause(target_node, instance_name, shard_count, pause_lease) do
    :erpc.call(
      target_node,
      __MODULE__,
      :pause_local_durable,
      [instance_name, shard_count, pause_lease],
      @pause_timeout_ms + 1_000
    )
  catch
    kind, reason -> {:error, {:remote_pause_failed, kind, reason}}
  end

  defp pause_standalone_writes(ctx, pause_lease) do
    ctx.shard_names
    |> Tuple.to_list()
    |> Enum.reduce_while({:ok, []}, fn shard, {:ok, paused_shards} ->
      case call_standalone_shard(
             shard,
             {:pause_writes, pause_lease},
             @pause_timeout_ms
           ) do
        :ok ->
          {:cont, {:ok, [shard | paused_shards]}}

        {:error, reason} ->
          _ = resume_standalone_shards(Enum.reverse(paused_shards), pause_lease)
          {:halt, {:error, {:flush_pause_failed, shard, reason}}}

        other ->
          _ = resume_standalone_shards(Enum.reverse(paused_shards), pause_lease)
          {:halt, {:error, {:flush_pause_failed, shard, other}}}
      end
    end)
    |> case do
      {:ok, paused_shards} ->
        {:ok, {:standalone, Enum.reverse(paused_shards), pause_lease}}

      {:error, _reason} = error ->
        error
    end
  end

  defp resume_all_writes(
         ctx,
         {:durable, paused_nodes, pause_lease, _cleanup_targets}
       ) do
    resume_durable_nodes(paused_nodes, ctx.name, ctx.shard_count, pause_lease)
  end

  defp resume_all_writes(_ctx, {:standalone, paused_shards, pause_lease}) do
    resume_standalone_shards(paused_shards, pause_lease)
  end

  defp resume_durable_nodes(nodes, instance_name, shard_count, pause_lease) do
    failures =
      Enum.reduce(nodes, [], fn target_node, failures ->
        case call_durable_resume(
               target_node,
               instance_name,
               shard_count,
               pause_lease
             ) do
          :ok -> failures
          other -> [{target_node, other} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:durable_resume_failed, failures}}
    end
  end

  @doc false
  def resume_local_durable(instance_name, shard_count, pause_lease)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 do
    shard_result =
      case local_instance(instance_name, shard_count) do
        {:ok, ctx} ->
          resume_standalone_shards(Tuple.to_list(ctx.shard_names), pause_lease)

        {:error, _reason} = error ->
          error
      end

    gate_result =
      Batcher.resume_writes_for_sync_all(shard_count, pause_lease, @resume_timeout_ms)

    merge_local_resume_results(shard_result, gate_result)
  end

  def resume_local_durable(_instance_name, shard_count, _pause_lease),
    do: {:error, {:invalid_shard_count, shard_count}}

  defp call_durable_resume(target_node, instance_name, shard_count, pause_lease)
       when target_node == node() do
    resume_local_durable(instance_name, shard_count, pause_lease)
  end

  defp call_durable_resume(target_node, instance_name, shard_count, pause_lease) do
    :erpc.call(
      target_node,
      __MODULE__,
      :resume_local_durable,
      [instance_name, shard_count, pause_lease],
      @resume_timeout_ms + 1_000
    )
  catch
    kind, reason -> {:error, {:remote_resume_failed, kind, reason}}
  end

  defp resume_standalone_shards(shards, pause_lease) do
    failures =
      Enum.reduce(shards, [], fn shard, failures ->
        case call_standalone_shard(
               shard,
               {:resume_writes, pause_lease},
               @resume_timeout_ms
             ) do
          :ok -> failures
          other -> [{shard, other} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:standalone_resume_failed, failures}}
    end
  end

  defp call_standalone_shard(shard, command, timeout) do
    GenServer.call(shard, command, timeout)
  catch
    :exit, reason -> {:error, {:shard_call_failed, reason}}
  end

  defp pause_local_shards(ctx, pause_lease) do
    ctx.shard_names
    |> Tuple.to_list()
    |> Enum.reduce_while({:ok, []}, fn shard, {:ok, paused} ->
      case call_standalone_shard(shard, {:pause_writes, pause_lease}, @pause_timeout_ms) do
        :ok ->
          {:cont, {:ok, [shard | paused]}}

        other ->
          _ = resume_standalone_shards(Enum.reverse(paused), pause_lease)
          {:halt, {:error, {:local_shard_pause_failed, shard, other}}}
      end
    end)
    |> case do
      {:ok, _paused} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp flush_replicated_shards(ctx, flush_epoch) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while({:ok, %{}}, fn shard_index, {:ok, positions} ->
      case flush_replicated_shard(ctx, shard_index, flush_epoch) do
        {:ok, deleted} when is_integer(deleted) and deleted >= 0 ->
          maybe_collect_flush_position(ctx, shard_index, positions)

        :ok ->
          maybe_collect_flush_position(ctx, shard_index, positions)

        {:error, reason} ->
          {:halt, {:error, {:flush_shard_failed, shard_index, reason}}}

        other ->
          {:halt, {:error, {:flush_shard_failed, shard_index, other}}}
      end
    end)
  end

  defp maybe_collect_flush_position(ctx, shard_index, positions) do
    if Router.durable_context?(ctx) do
      collect_flush_position(shard_index, positions)
    else
      {:cont, {:ok, positions}}
    end
  end

  defp collect_flush_position(shard_index, positions) do
    case WARaftBackend.storage_position(shard_index) do
      {:ok, {:raft_log_pos, index, term} = position}
      when is_integer(index) and index > 0 and is_integer(term) and term > 0 ->
        {:cont, {:ok, Map.put(positions, shard_index, position)}}

      {:error, reason} ->
        {:halt, {:error, {:flush_position_unavailable, shard_index, reason}}}

      other ->
        {:halt, {:error, {:flush_position_unavailable, shard_index, other}}}
    end
  end

  defp flush_replicated_shard(ctx, shard_index, flush_epoch) do
    if Router.durable_context?(ctx) do
      shard_index
      |> Batcher.write_flush_shard_paused(flush_epoch)
      |> normalize_flush_shard_result(shard_index)
    else
      ctx.shard_names
      |> elem(shard_index)
      |> GenServer.call({:flush_shard_paused, flush_epoch}, :infinity)
    end
  catch
    :exit, reason -> {:error, {:flush_shard_call_failed, reason}}
  end

  defp normalize_flush_shard_result(
         {:error, {:timeout, :unknown_outcome}} = timeout,
         shard_index
       ) do
    case WARaftBackend.storage_status(shard_index) do
      {:ok, status} when is_list(status) ->
        case Keyword.get(status, :blocked_error) do
          nil -> timeout
          reason -> {:error, reason}
        end

      _unavailable ->
        timeout
    end
  end

  defp normalize_flush_shard_result(result, _shard_index), do: result

  defp run_post_flush_cleanup(
         ctx,
         {:durable, _paused_nodes, _pause_lease, cleanup_targets},
         flush_positions
       ) do
    targets =
      Map.new(cleanup_targets, fn {target_node, shard_indexes} ->
        positions = Map.take(flush_positions, shard_indexes)
        {target_node, positions}
      end)

    with :ok <-
           run_cleanup_targets(targets, fn target_node, target_positions ->
             call_post_flush_cleanup(
               target_node,
               ctx.name,
               ctx.shard_count,
               target_positions
             )
           end) do
      post_flush_origin_cleanup(ctx)
    end
  end

  defp run_post_flush_cleanup(
         ctx,
         {:standalone, _paused_shards, _pause_lease},
         _flush_positions
       ) do
    post_flush_origin_cleanup(ctx)
  end

  @doc false
  def post_flush_cleanup_local(instance_name, shard_count, target_positions)
      when is_atom(instance_name) and is_integer(shard_count) and shard_count >= 0 and
             is_map(target_positions) do
    await_local_flush_positions(target_positions)
  end

  def post_flush_cleanup_local(_instance_name, shard_count, _target_positions),
    do: {:error, {:invalid_shard_count, shard_count}}

  defp call_post_flush_cleanup(
         target_node,
         instance_name,
         shard_count,
         target_positions
       )
       when target_node == node() do
    post_flush_cleanup_local(instance_name, shard_count, target_positions)
  end

  defp call_post_flush_cleanup(
         target_node,
         instance_name,
         shard_count,
         target_positions
       ) do
    :erpc.call(
      target_node,
      __MODULE__,
      :post_flush_cleanup_local,
      [instance_name, shard_count, target_positions],
      @cleanup_timeout_ms
    )
  catch
    kind, reason -> {:error, {:remote_cleanup_failed, kind, reason}}
  end

  defp run_cleanup_targets(targets, cleanup_fun) when is_map(targets) do
    max_concurrency = targets |> map_size() |> min(16) |> max(1)

    results =
      targets
      |> Enum.sort_by(&elem(&1, 0))
      |> Task.async_stream(
        fn {target_node, target_positions} ->
          {target_node, cleanup_fun.(target_node, target_positions)}
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: @cleanup_timeout_ms + 1_000,
        on_timeout: :kill_task
      )
      |> Enum.to_list()

    failures =
      Enum.reduce(results, [], fn
        {:ok, {_target_node, :ok}}, failures ->
          failures

        {:ok, {target_node, result}}, failures ->
          [{target_node, result} | failures]

        {:exit, reason}, failures ->
          [{:cleanup_task, reason} | failures]
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:flush_local_cleanup_failed, failures}}
    end
  end

  defp await_local_flush_positions(target_positions) do
    deadline = System.monotonic_time(:millisecond) + @cleanup_timeout_ms

    target_positions
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while(:ok, fn {shard_index, target_position}, :ok ->
      case await_local_flush_position(shard_index, target_position, deadline) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp await_local_flush_position(shard_index, target_position, deadline) do
    case WARaftBackend.storage_position(shard_index) do
      {:ok, current_position} ->
        cond do
          raft_position_reached?(current_position, target_position) ->
            :ok

          System.monotonic_time(:millisecond) >= deadline ->
            {:error, {:flush_replica_apply_timeout, shard_index, target_position}}

          true ->
            Process.sleep(@replica_apply_poll_ms)
            await_local_flush_position(shard_index, target_position, deadline)
        end

      {:error, reason} ->
        {:error, {:flush_replica_position_unavailable, shard_index, reason}}

      other ->
        {:error, {:flush_replica_position_unavailable, shard_index, other}}
    end
  end

  defp raft_position_reached?(
         {:raft_log_pos, current_index, _current_term},
         {:raft_log_pos, target_index, _target_term}
       )
       when is_integer(current_index) and is_integer(target_index),
       do: current_index >= target_index

  defp raft_position_reached?(_current, _target), do: false

  defp durable_membership_snapshot(shard_count) do
    durable_membership_snapshot(shard_count, &WARaftBackend.status/1)
  end

  defp durable_membership_snapshot(shard_count, status_fun) when is_function(status_fun, 1) do
    shard_indexes(shard_count)
    |> Enum.reduce_while({:ok, {%{}, %{}}}, fn
      shard_index, {:ok, {targets, fingerprints}} ->
        case status_fun.(shard_index) do
          status when is_list(status) ->
            case status_membership_snapshot(status) do
              {:ok, %{nodes: nodes, fingerprint: fingerprint}} ->
                next_targets =
                  Enum.reduce(nodes, targets, fn target_node, acc ->
                    Map.update(acc, target_node, [shard_index], &[shard_index | &1])
                  end)

                {:cont, {:ok, {next_targets, Map.put(fingerprints, shard_index, fingerprint)}}}

              {:error, reason} ->
                {:halt, {:error, {:flush_membership_unavailable, shard_index, reason}}}
            end

          {:error, reason} ->
            {:halt, {:error, {:flush_membership_unavailable, shard_index, reason}}}

          other ->
            {:halt, {:error, {:flush_membership_unavailable, shard_index, other}}}
        end
    end)
    |> case do
      {:ok, {targets, fingerprints}} when map_size(targets) > 0 ->
        cleanup_targets =
          Map.new(targets, fn {node_name, shards} ->
            {node_name, Enum.sort(shards)}
          end)

        {:ok,
         %{
           cleanup_targets: cleanup_targets,
           fingerprints: fingerprints,
           shard_count: shard_count
         }}

      {:ok, {_targets, _fingerprints}} ->
        {:error, {:flush_membership_unavailable, :empty_membership}}

      {:error, _reason} = error ->
        error
    end
  end

  if Mix.env() == :test do
    @doc false
    def __durable_cleanup_targets_for_test__(statuses) when is_map(statuses) do
      shard_count = map_size(statuses)

      with {:ok, snapshot} <-
             durable_membership_snapshot(shard_count, &Map.fetch!(statuses, &1)) do
        {:ok, snapshot.cleanup_targets}
      end
    end

    @doc false
    def __durable_pause_plan_for_test__(statuses, origin_node)
        when is_map(statuses) and is_atom(origin_node) do
      shard_count = map_size(statuses)

      with {:ok, snapshot} <-
             durable_membership_snapshot(shard_count, &Map.fetch!(statuses, &1)) do
        {:ok,
         %{
           cleanup_targets: snapshot.cleanup_targets,
           pause_nodes: durable_pause_nodes(snapshot.cleanup_targets, origin_node)
         }}
      end
    end

    @doc false
    def __verify_durable_membership_for_test__(before_statuses, after_statuses)
        when is_map(before_statuses) and is_map(after_statuses) do
      shard_count = map_size(before_statuses)

      with {:ok, snapshot} <-
             durable_membership_snapshot(
               shard_count,
               &Map.fetch!(before_statuses, &1)
             ) do
        verify_durable_membership(snapshot, &Map.fetch!(after_statuses, &1))
      end
    end

    @doc false
    def __run_cleanup_targets_for_test__(targets, cleanup_fun)
        when is_map(targets) and is_function(cleanup_fun, 2) do
      run_cleanup_targets(targets, cleanup_fun)
    end
  end

  defp verify_durable_membership(snapshot) do
    verify_durable_membership(snapshot, &WARaftBackend.status/1)
  end

  defp verify_durable_membership(
         %{shard_count: shard_count, fingerprints: expected},
         status_fun
       )
       when is_function(status_fun, 1) do
    case durable_membership_snapshot(shard_count, status_fun) do
      {:ok, %{fingerprints: actual}} ->
        expected
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce_while(:ok, fn {shard_index, expected_fingerprint}, :ok ->
          actual_fingerprint = Map.get(actual, shard_index)

          if actual_fingerprint == expected_fingerprint do
            {:cont, :ok}
          else
            {:halt,
             {:error,
              {:flush_membership_changed, shard_index, expected_fingerprint, actual_fingerprint}}}
          end
        end)

      {:error, reason} ->
        {:error, {:flush_membership_recheck_failed, reason}}
    end
  end

  defp durable_pause_nodes(cleanup_targets, origin_node) do
    cleanup_targets
    |> Map.keys()
    |> Kernel.++([origin_node])
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp status_membership_snapshot(status) do
    case Keyword.get(status, :config) do
      %{} = config ->
        with {:ok, config_index} <- status_config_index(status),
             {:ok, config_version} <- config_version(config),
             {:ok, participants} <- effective_participants(config),
             {:ok, participant_nodes} <- participant_nodes(participants),
             {:ok, membership_nodes} <-
               optional_participant_nodes(Map.get(config, :membership, [])),
             {:ok, witness_nodes} <-
               optional_participant_nodes(Map.get(config, :witness, [])) do
          if participant_nodes == [] do
            {:error, :empty_membership}
          else
            {:ok,
             %{
               nodes: participant_nodes,
               fingerprint:
                 {config_index, config_version, participant_nodes, membership_nodes,
                  witness_nodes}
             }}
          end
        end

      missing ->
        {:error, {:missing_config, missing}}
    end
  end

  defp status_config_index(status) do
    case Keyword.get(status, :config_index) do
      index when is_integer(index) and index >= 0 -> {:ok, index}
      invalid -> {:error, {:invalid_config_index, invalid}}
    end
  end

  defp config_version(config) do
    case Map.get(config, :version) do
      version when is_integer(version) and version > 0 -> {:ok, version}
      invalid -> {:error, {:invalid_config_version, invalid}}
    end
  end

  defp effective_participants(config) do
    case Map.get(config, :participants, []) do
      [_ | _] = participants ->
        {:ok, participants}

      [] ->
        optional_participants(Map.get(config, :membership, []))

      invalid ->
        {:error, {:invalid_participants, invalid}}
    end
  end

  defp optional_participants(participants) when is_list(participants), do: {:ok, participants}
  defp optional_participants(invalid), do: {:error, {:invalid_participants, invalid}}

  defp optional_participant_nodes(participants) when is_list(participants),
    do: participant_nodes(participants)

  defp optional_participant_nodes(invalid),
    do: {:error, {:invalid_participants, invalid}}

  defp participant_nodes(participants) do
    participants
    |> Enum.reduce_while({:ok, []}, fn participant, {:ok, nodes} ->
      case participant_node(participant) do
        {:ok, node_name} -> {:cont, {:ok, [node_name | nodes]}}
        :error -> {:halt, {:error, {:invalid_participant, participant}}}
      end
    end)
    |> case do
      {:ok, nodes} -> {:ok, nodes |> Enum.uniq() |> Enum.sort()}
      {:error, _reason} = error -> error
    end
  end

  defp participant_node({:raft_identity, _name, node_name}),
    do: validated_participant_node(node_name)

  defp participant_node({_name, node_name}), do: validated_participant_node(node_name)
  defp participant_node(node_name), do: validated_participant_node(node_name)

  defp validated_participant_node(node_name)
       when is_atom(node_name) and node_name not in [nil, true, false, :undefined],
       do: {:ok, node_name}

  defp validated_participant_node(_invalid), do: :error

  defp local_instance(instance_name, shard_count) do
    case FerricStore.Instance.get(instance_name) do
      %FerricStore.Instance{shard_count: ^shard_count} = ctx -> {:ok, ctx}
      %FerricStore.Instance{shard_count: actual} -> {:error, {:shard_count_mismatch, actual}}
    end
  rescue
    error -> {:error, {:instance_unavailable, error}}
  catch
    kind, reason -> {:error, {:instance_unavailable, kind, reason}}
  end

  defp merge_local_resume_results(:ok, :ok), do: :ok

  defp merge_local_resume_results(shard_result, gate_result),
    do: {:error, {:local_resume_failed, shard_result, gate_result}}

  defp post_flush_origin_cleanup(ctx) do
    :ok = clear_stream_tables(ctx)
    :ok = NativeFlowIndex.reset_all(ctx.name, ctx.shard_count)
    :ok
  end

  defp shard_indexes(0), do: []
  defp shard_indexes(shard_count), do: 0..(shard_count - 1)

  defp clear_stream_tables(ctx), do: Ferricstore.Stream.LocalState.clear(ctx)
end
