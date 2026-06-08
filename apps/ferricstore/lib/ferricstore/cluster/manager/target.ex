defmodule Ferricstore.Cluster.Manager.Target do
  @moduledoc false

  require Logger

  alias Ferricstore.Cluster.DataSync
  alias Ferricstore.Cluster.JoinIdentity
  alias Ferricstore.Cluster.TargetMarker
  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @membership_operation_timeout_ms 5_000

  def target_membership_by_shard(target_node, state) do
    case Process.get(:ferricstore_cluster_manager_target_membership_hook) do
      hook when is_function(hook, 2) ->
        normalize_target_membership_result(hook.(target_node, state), target_node)

      _ ->
        target_membership_by_shard_real(target_node, state.shard_count)
    end
  end

  def target_membership_by_shard_real(target_node, shard_count) do
    Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn shard_idx, {:ok, acc} ->
      case target_member?(target_node, shard_idx) do
        {:ok, member?} ->
          {:cont, {:ok, Map.put(acc, shard_idx, member?)}}

        {:error, reason} ->
          {:halt, {:error, {:target_membership_snapshot_failed, target_node, shard_idx, reason}}}
      end
    end)
  end

  def normalize_target_membership_result({:ok, membership}, _target_node)
      when is_map(membership),
      do: {:ok, membership}

  def normalize_target_membership_result(membership, _target_node) when is_map(membership) do
    if Enum.all?(membership, fn {_shard, status} -> is_boolean(status) end) do
      {:ok, membership}
    else
      {:error, {:target_membership_snapshot_failed, :invalid_membership_snapshot}}
    end
  end

  def normalize_target_membership_result({:error, reason}, target_node),
    do: {:error, {:target_membership_snapshot_failed, target_node, reason}}

  def normalize_target_membership_result(other, target_node),
    do: {:error, {:target_membership_snapshot_failed, target_node, {:unexpected_result, other}}}

  def target_member?(target_node, shard_idx) do
    case RaftCluster.members(shard_idx, @membership_operation_timeout_ms) do
      {:ok, members, _leader} ->
        {:ok, Enum.any?(members, &(member_node(&1) == target_node))}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_members_result, other}}
    end
  rescue
    error -> {:error, error}
  end

  def member_node({_name, node}), do: node
  def member_node(%{id: {_name, node}}), do: node
  def member_node(_member), do: nil

  def transfer_target_from_members(members, target_node) when is_list(members) do
    members
    |> Enum.map(&member_node/1)
    |> Enum.find(fn
      node when is_atom(node) and not is_nil(node) -> node != target_node
      _other -> false
    end)
  end

  def transfer_target_from_members(_members, _target_node), do: nil

  def remove_join_added_members(target_node, state, preexisting_membership) do
    rollback_results =
      for shard_idx <- 0..(state.shard_count - 1),
          Map.get(preexisting_membership, shard_idx, true) == false,
          into: %{} do
        {shard_idx, remove_join_added_member(target_node, shard_idx)}
      end

    failed = Enum.filter(rollback_results, fn {_shard_idx, result} -> result != :ok end)

    if failed == [] do
      :ok
    else
      {:error, {:partial_join_rollback, rollback_results}}
    end
  end

  def remove_join_added_members(target_node, state, preexisting_membership, shard_results) do
    rollback_results =
      for shard_idx <- 0..(state.shard_count - 1),
          Map.get(preexisting_membership, shard_idx, true) == false,
          Map.get(shard_results, shard_idx) == :ok,
          into: %{} do
        {shard_idx, remove_join_added_member(target_node, shard_idx)}
      end

    failed = Enum.filter(rollback_results, fn {_shard_idx, result} -> result != :ok end)

    if failed == [] do
      :ok
    else
      {:error, {:partial_join_rollback, rollback_results}}
    end
  end

  def remove_join_added_member(target_node, shard_idx) do
    case Process.get(:ferricstore_cluster_manager_remove_added_member_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, shard_idx)

      _ ->
        RaftCluster.remove_member(shard_idx, target_node)
    end
  end

  # Checks if the target node has pre-existing Bitcask data (disk clone scenario).
  def target_has_data?(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_target_has_data_hook) do
      hook when is_function(hook, 2) ->
        normalize_target_has_data_result(hook.(target_node, shard_count), target_node)

      _ ->
        do_target_has_data?(target_node, shard_count)
    end
  end

  def normalize_target_has_data_result(value, _target_node) when is_boolean(value),
    do: {:ok, value}

  def normalize_target_has_data_result({:ok, value}, _target_node) when is_boolean(value),
    do: {:ok, value}

  def normalize_target_has_data_result({:error, _reason} = error, _target_node), do: error

  def normalize_target_has_data_result(other, target_node),
    do: {:error, {:target_data_probe_failed, target_node, {:unexpected_result, other}}}

  def do_target_has_data?(target_node, shard_count) do
    try do
      target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)

      unless is_map(target_ctx) and is_binary(Map.get(target_ctx, :data_dir)) do
        throw({:target_data_probe_failed, target_node, {:invalid_target_context, target_ctx}})
      end

      Enum.reduce_while(0..(shard_count - 1), {:ok, false}, fn i, {:ok, false} ->
        probe_target_shard_data(target_node, target_ctx.data_dir, i)
      end)
    catch
      {:target_data_probe_failed, ^target_node, reason} ->
        {:error, {:target_data_probe_failed, target_node, reason}}

      kind, reason ->
        {:error, {:target_data_probe_failed, target_node, {kind, reason}}}
    end
  end

  def probe_target_shard_data(target_node, data_dir, shard_idx) do
    case probe_target_shard_data_result(target_node, data_dir, shard_idx) do
      {:ok, true} -> {:halt, {:ok, true}}
      {:ok, false} -> {:cont, {:ok, false}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  def probe_target_shard_data_result(target_node, data_dir, shard_idx) do
    data_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_idx)
    dedicated_path = Path.join([data_dir, "dedicated", "shard_#{shard_idx}"])
    blob_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_idx)

    [
      {:bitcask_logs, data_path},
      {:file_tree, dedicated_path},
      {:file_tree, blob_path}
    ]
    |> Enum.reduce_while({:ok, false}, fn
      {:bitcask_logs, path}, {:ok, false} ->
        probe_target_bitcask_logs(target_node, path)
        |> reduce_target_data_probe()

      {:file_tree, path}, {:ok, false} ->
        probe_target_file_tree(target_node, path)
        |> reduce_target_data_probe()
    end)
  end

  def reduce_target_data_probe({:ok, true}), do: {:halt, {:ok, true}}
  def reduce_target_data_probe({:ok, false}), do: {:cont, {:ok, false}}
  def reduce_target_data_probe({:error, _reason} = error), do: {:halt, error}

  def probe_target_bitcask_logs(target_node, shard_path) do
    case :erpc.call(target_node, File, :ls, [shard_path], 5_000) do
      {:ok, files} ->
        probe_target_log_files(target_node, shard_path, files)

      {:error, :enoent} ->
        {:ok, false}

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, reason}}}

      other ->
        {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, other}}}
    end
  end

  def probe_target_log_files(target_node, shard_path, files) do
    Enum.reduce_while(files, {:ok, false}, fn file, {:ok, false} ->
      if String.ends_with?(file, ".log") do
        case :erpc.call(target_node, File, :stat, [Path.join(shard_path, file)], 5_000) do
          {:ok, %{size: size}} when size > 0 ->
            {:halt, {:ok, true}}

          {:ok, %{size: _size}} ->
            {:cont, {:ok, false}}

          {:error, reason} ->
            {:halt,
             {:error, {:target_data_probe_failed, target_node, {:stat, shard_path, reason}}}}

          other ->
            {:halt,
             {:error, {:target_data_probe_failed, target_node, {:stat, shard_path, other}}}}
        end
      else
        {:cont, {:ok, false}}
      end
    end)
  end

  def probe_target_file_tree(target_node, path) do
    case :erpc.call(target_node, File, :ls, [path], 5_000) do
      {:ok, files} ->
        probe_target_file_tree_entries(target_node, path, files)

      {:error, :enoent} ->
        {:ok, false}

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:ls, path, reason}}}

      other ->
        {:error, {:target_data_probe_failed, target_node, {:ls, path, other}}}
    end
  end

  def probe_target_file_tree_entries(target_node, path, files) do
    Enum.reduce_while(files, {:ok, false}, fn file, {:ok, false} ->
      entry_path = Path.join(path, file)

      case :erpc.call(target_node, File, :stat, [entry_path], 5_000) do
        {:ok, %{type: :directory}} ->
          case probe_target_file_tree(target_node, entry_path) do
            {:ok, true} -> {:halt, {:ok, true}}
            {:ok, false} -> {:cont, {:ok, false}}
            {:error, _reason} = error -> {:halt, error}
          end

        {:ok, %{type: :regular, size: size}} when size > 0 ->
          {:halt, {:ok, true}}

        {:ok, _stat} ->
          {:cont, {:ok, false}}

        {:error, reason} ->
          {:halt, {:error, {:target_data_probe_failed, target_node, {:stat, entry_path, reason}}}}

        other ->
          {:halt, {:error, {:target_data_probe_failed, target_node, {:stat, entry_path, other}}}}
      end
    end)
  end

  def validate_target_data_identity(_target_node, _ctx, false, _replace?), do: :ok

  def validate_target_data_identity(target_node, ctx, true, true) do
    local_state = Ferricstore.ReplicationMode.read(ctx.data_dir)
    target_state = read_target_cluster_state(target_node)

    JoinIdentity.validate(local_state, target_state, target_node)
  end

  def validate_target_data_identity(target_node, ctx, true, _replace?) do
    local_state = Ferricstore.ReplicationMode.read(ctx.data_dir)
    target_state = read_target_cluster_state(target_node)

    case JoinIdentity.validate(local_state, target_state, target_node) do
      :ok ->
        if match?({:error, :enoent}, local_state) do
          Logger.warning(
            "ClusterManager: local cluster_state marker missing; allowing legacy pre-marker join for #{target_node}"
          )
        end

        :ok

      {:error, _reason} = error ->
        error
    end
  end

  def read_target_cluster_state(target_node) do
    case Process.get(:ferricstore_cluster_manager_read_target_cluster_state_hook) do
      hook when is_function(hook, 1) ->
        hook.(target_node)

      _ ->
        do_read_target_cluster_state(target_node)
    end
  end

  def do_read_target_cluster_state(target_node) do
    target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)

    :erpc.call(
      target_node,
      Ferricstore.ReplicationMode,
      :read,
      [target_ctx.data_dir],
      5_000
    )
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  def write_target_cluster_marker(target_node, ctx, barrier_indices) do
    case Process.get(:ferricstore_cluster_manager_write_target_marker_hook) do
      hook when is_function(hook, 3) -> hook.(target_node, ctx, barrier_indices)
      _ -> TargetMarker.write(target_node, ctx, barrier_indices)
    end
  end

  def cleanup_target_data(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_cleanup_target_data_hook) do
      hook when is_function(hook, 2) -> hook.(target_node, shard_count)
      _ -> do_cleanup_target_data(target_node, shard_count)
    end
  end

  def do_cleanup_target_data(target_node, shard_count) do
    try do
      target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)
      cleanup_target_data_dir(target_node, target_ctx.data_dir, shard_count)
    catch
      kind, reason ->
        Logger.warning(
          "ClusterManager: failed while cleaning target data on #{target_node}: #{inspect({kind, reason})}"
        )

        {:error, {:target_cleanup_failed, target_node, {kind, reason}}}
    end
  end

  def cleanup_target_data_dir(target_node, data_dir, shard_count) do
    Enum.each(0..(shard_count - 1), fn i ->
      shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
      :erpc.call(target_node, File, :rm_rf!, [shard_path], 30_000)
    end)

    # REPLACE join must remove every shard-owned side store. Leaving an old
    # blob tree behind could make future large-value refs resolve to unrelated
    # target data after the new cluster baseline is copied.
    Enum.each(["dedicated", "blob", "prob"], fn dir ->
      :erpc.call(target_node, File, :rm_rf!, [Path.join(data_dir, dir)], 30_000)
    end)

    # WARaft replacement/failure cleanup must also remove backend-local state
    # and durable mode markers. Otherwise a target can restart with stale Raft
    # identity or a marker from an unrelated cluster before the new baseline is
    # copied.
    Enum.each(["ra", "waraft"], fn dir ->
      :erpc.call(target_node, File, :rm_rf!, [Path.join(data_dir, dir)], 30_000)
    end)

    marker_path = Ferricstore.ReplicationMode.marker_path(data_dir)
    :erpc.call(target_node, File, :rm, [marker_path], 30_000)
    :erpc.call(target_node, File, :rm, [marker_path <> ".tmp"], 30_000)

    :ok
  end

  @doc false
  def __extract_direct_sync_indices_for_test__(target_node, sync_results) do
    extract_direct_sync_indices(target_node, sync_results)
  end

  @doc false
  def __target_shard_has_data_for_test__(target_node, data_dir, shard_idx) do
    probe_target_shard_data_result(target_node, data_dir, shard_idx)
  end

  @doc false
  def __cleanup_target_data_dir_for_test__(target_node, data_dir, shard_count) do
    cleanup_target_data_dir(target_node, data_dir, shard_count)
  end

  def extract_direct_sync_indices(target_node, sync_results) when is_map(sync_results) do
    with {:ok, target_data_dir} <-
           maybe_target_data_dir_for_wal_bridgeable(target_node, sync_results) do
      Enum.reduce_while(sync_results, {:ok, %{}}, fn
        {shard_idx, {:synced, :wal_bridgeable}}, {:ok, acc} ->
          case read_target_shard_index(target_node, target_data_dir, shard_idx) do
            {:ok, idx} -> {:cont, {:ok, Map.put(acc, shard_idx, idx)}}
            {:error, reason} -> {:halt, {:error, reason}}
          end

        {shard_idx, {:synced, raft_idx}}, {:ok, acc}
        when is_integer(raft_idx) and raft_idx >= 0 ->
          {:cont, {:ok, Map.put(acc, shard_idx, raft_idx)}}

        {shard_idx, {:synced, detail}}, {:ok, _acc} ->
          {:halt,
           {:error,
            {:target_index_read_failed, target_node, shard_idx, {:unknown_sync_detail, detail}}}}

        {shard_idx, other}, {:ok, _acc} ->
          {:halt,
           {:error,
            {:target_index_read_failed, target_node, shard_idx, {:unexpected_sync_result, other}}}}
      end)
    end
  end

  def extract_direct_sync_indices(target_node, sync_results) do
    {:error,
     {:target_index_read_failed, target_node, :sync_results, {:unexpected_result, sync_results}}}
  end

  def maybe_target_data_dir_for_wal_bridgeable(target_node, sync_results) do
    if Enum.any?(sync_results, fn {_shard_idx, result} -> result == {:synced, :wal_bridgeable} end) do
      target_data_dir(target_node)
    else
      {:ok, nil}
    end
  end

  # Auto-join: triggered by :nodeup, runs in a spawned process so
  # handle_info returns immediately. Routes through GenServer.call
  # so the dedup guard in handle_call prevents concurrent joins.
  def do_auto_join(target_node, role) do
    Logger.info("ClusterManager: auto-joining #{target_node} as #{role}")

    case wait_for_remote_app(target_node) do
      :ok ->
        case GenServer.call(Ferricstore.Cluster.Manager, {:add_node, target_node, role}, 120_000) do
          :ok ->
            Logger.info("ClusterManager: auto-join complete for #{target_node}")

          {:error, reason} ->
            Logger.error(
              "ClusterManager: auto-join failed for #{target_node}: #{inspect(reason)}"
            )
        end

      {:error, reason} ->
        Logger.error(
          "ClusterManager: auto-join failed for #{target_node}: remote app not ready #{inspect(reason)}"
        )
    end
  end

  def wait_for_remote_app(target_node, attempts \\ 600) do
    if attempts <= 0 do
      Logger.warning("ClusterManager: timed out waiting for FerricStore on #{target_node}")
      {:error, :remote_app_not_ready}
    else
      with %{} <- :erpc.call(target_node, FerricStore.Instance, :get, [:default], 2_000),
           true <- :erpc.call(target_node, Ferricstore.Health, :ready?, [], 2_000) do
        :ok
      else
        _ ->
          Process.sleep(200)
          wait_for_remote_app(target_node, attempts - 1)
      end
    end
  catch
    _, _ ->
      Process.sleep(200)
      wait_for_remote_app(target_node, attempts - 1)
  end

  # ---------------------------------------------------------------------------
  # Private: add/remove/leave operations
  # ---------------------------------------------------------------------------
  @doc false
  def read_target_indices(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_read_target_indices_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, shard_count)

      _ ->
        do_read_target_indices(target_node, shard_count)
    end
  end

  def do_read_target_indices(target_node, shard_count) do
    with {:ok, target_data_dir} <- target_data_dir(target_node) do
      Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn shard_idx, {:ok, acc} ->
        case read_target_shard_index(target_node, target_data_dir, shard_idx) do
          {:ok, idx} -> {:cont, {:ok, Map.put(acc, shard_idx, idx)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  def target_data_dir(target_node) do
    target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)

    if is_map(target_ctx) and is_binary(Map.get(target_ctx, :data_dir)) do
      {:ok, target_ctx.data_dir}
    else
      {:error,
       {:target_index_read_failed, target_node, :context, {:invalid_target_context, target_ctx}}}
    end
  catch
    kind, reason ->
      {:error, {:target_index_read_failed, target_node, :context, {kind, reason}}}
  end

  def read_target_shard_index(target_node, target_data_dir, shard_idx) do
    case :erpc.call(
           target_node,
           DataSync,
           :read_last_applied_from_disk,
           [target_data_dir, shard_idx],
           5_000
         ) do
      idx when is_integer(idx) and idx >= 0 ->
        {:ok, idx}

      other ->
        {:error, {:target_index_read_failed, target_node, shard_idx, {:unexpected_result, other}}}
    end
  catch
    kind, reason ->
      {:error, {:target_index_read_failed, target_node, shard_idx, {kind, reason}}}
  end
end
