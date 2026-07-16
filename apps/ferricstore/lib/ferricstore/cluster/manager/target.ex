defmodule Ferricstore.Cluster.Manager.Target do
  @moduledoc false

  require Logger

  alias Ferricstore.Cluster.JoinIdentity
  alias Ferricstore.Cluster.TargetMarker
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @membership_operation_timeout_ms 5_000
  @target_log_probe_page_size 1_024

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
    case target_lstat(target_node, shard_path) do
      {:ok, %{type: :directory}} ->
        list_target_bitcask_logs(target_node, shard_path)

      {:ok, %{type: :symlink}} ->
        {:error, {:target_data_probe_failed, target_node, {:symlink, shard_path}}}

      {:ok, %{type: type}} ->
        {:error, {:target_data_probe_failed, target_node, {:not_a_directory, shard_path, type}}}

      {:error, :enoent} ->
        {:ok, false}

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:lstat, shard_path, reason}}}
    end
  end

  defp list_target_bitcask_logs(target_node, shard_path) do
    case :erpc.call(target_node, File, :ls, [shard_path], 5_000) do
      {:ok, files} ->
        probe_target_log_files(target_node, shard_path, files)

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, reason}}}

      other ->
        {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, other}}}
    end
  catch
    kind, reason ->
      {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, {kind, reason}}}}
  end

  def probe_target_log_files(target_node, shard_path, files) do
    Enum.reduce_while(files, {:ok, false}, fn file, {:ok, false} ->
      if String.ends_with?(file, ".log") do
        path = Path.join(shard_path, file)

        case :erpc.call(target_node, __MODULE__, :bitcask_log_has_user_data, [path], 5_000) do
          {:ok, true} ->
            {:halt, {:ok, true}}

          {:ok, false} ->
            {:cont, {:ok, false}}

          {:error, reason} ->
            {:halt, {:error, {:target_data_probe_failed, target_node, {:scan, path, reason}}}}

          other ->
            {:halt, {:error, {:target_data_probe_failed, target_node, {:scan, path, other}}}}
        end
      else
        {:cont, {:ok, false}}
      end
    end)
  end

  @doc false
  def bitcask_log_has_user_data(path) when is_binary(path) do
    bitcask_log_has_user_data(path, 0)
  end

  defp bitcask_log_has_user_data(path, offset) do
    case NIF.v2_scan_file_page(path, offset, @target_log_probe_page_size) do
      {:ok, records, next_offset, done?} ->
        if Enum.any?(records, &target_user_record?/1) do
          {:ok, true}
        else
          continue_target_log_probe(path, offset, next_offset, done?)
        end

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_scan_result, other}}
    end
  end

  defp continue_target_log_probe(_path, _offset, _next_offset, true), do: {:ok, false}

  defp continue_target_log_probe(path, offset, next_offset, false)
       when is_integer(next_offset) and next_offset > offset,
       do: bitcask_log_has_user_data(path, next_offset)

  defp continue_target_log_probe(_path, offset, next_offset, false),
    do: {:error, {:non_advancing_scan, offset, next_offset}}

  defp target_user_record?({key, _offset, _value_size, _expire_at_ms, false})
       when is_binary(key),
       do: not target_bootstrap_control_key?(key)

  defp target_user_record?(_record), do: false

  defp target_bootstrap_control_key?("f:{f}:svb:1:" <> shard_index),
    do: canonical_shard_index?(shard_index)

  defp target_bootstrap_control_key?("f:{f}:svbp:2:" <> shard_index),
    do: canonical_shard_index?(shard_index)

  defp target_bootstrap_control_key?("f:{f}:pcb:1:" <> shard_index),
    do: canonical_shard_index?(shard_index)

  defp target_bootstrap_control_key?(_key), do: false

  defp canonical_shard_index?(value) do
    case Integer.parse(value) do
      {shard_index, ""} when shard_index >= 0 -> Integer.to_string(shard_index) == value
      _invalid -> false
    end
  end

  def probe_target_file_tree(target_node, path) do
    case target_lstat(target_node, path) do
      {:ok, %{type: :directory}} ->
        list_target_file_tree(target_node, path)

      {:ok, %{type: :symlink}} ->
        {:error, {:target_data_probe_failed, target_node, {:symlink, path}}}

      {:ok, %{type: type}} ->
        {:error, {:target_data_probe_failed, target_node, {:not_a_directory, path, type}}}

      {:error, :enoent} ->
        {:ok, false}

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:lstat, path, reason}}}
    end
  end

  defp list_target_file_tree(target_node, path) do
    case :erpc.call(target_node, File, :ls, [path], 5_000) do
      {:ok, files} ->
        probe_target_file_tree_entries(target_node, path, files)

      {:error, reason} ->
        {:error, {:target_data_probe_failed, target_node, {:ls, path, reason}}}

      other ->
        {:error, {:target_data_probe_failed, target_node, {:ls, path, other}}}
    end
  catch
    kind, reason ->
      {:error, {:target_data_probe_failed, target_node, {:ls, path, {kind, reason}}}}
  end

  def probe_target_file_tree_entries(target_node, path, files) do
    Enum.reduce_while(files, {:ok, false}, fn file, {:ok, false} ->
      entry_path = Path.join(path, file)

      case target_lstat(target_node, entry_path) do
        {:ok, %{type: :directory}} ->
          case probe_target_file_tree(target_node, entry_path) do
            {:ok, true} -> {:halt, {:ok, true}}
            {:ok, false} -> {:cont, {:ok, false}}
            {:error, _reason} = error -> {:halt, error}
          end

        {:ok, %{type: :symlink}} ->
          {:halt, {:error, {:target_data_probe_failed, target_node, {:symlink, entry_path}}}}

        {:ok, %{type: :regular, size: size}} when size > 0 ->
          {:halt, {:ok, true}}

        {:ok, _stat} ->
          {:cont, {:ok, false}}

        {:error, reason} ->
          {:halt,
           {:error, {:target_data_probe_failed, target_node, {:lstat, entry_path, reason}}}}
      end
    end)
  end

  defp target_lstat(target_node, path) do
    :erpc.call(target_node, File, :lstat, [path], 5_000)
  catch
    kind, reason -> {:error, {kind, reason}}
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

    JoinIdentity.validate(local_state, target_state, target_node)
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

  def cleanup_target_data_dir(target_node, data_dir, shard_count)
      when is_atom(target_node) and is_binary(data_dir) and is_integer(shard_count) and
             shard_count >= 0 do
    shard_paths =
      shard_count
      |> shard_indexes()
      |> Enum.map(&Ferricstore.DataDir.shard_data_path(data_dir, &1))

    tree_paths =
      shard_paths ++
        Enum.map(["dedicated", "blob", "prob", "ra", "waraft"], &Path.join(data_dir, &1))

    marker_path = Ferricstore.ReplicationMode.marker_path(data_dir)

    with :ok <- remove_target_trees(target_node, tree_paths),
         :ok <- remove_target_files(target_node, [marker_path, marker_path <> ".tmp"]) do
      :ok
    end
  end

  def cleanup_target_data_dir(_target_node, data_dir, shard_count),
    do: {:error, {:target_cleanup_failed, data_dir, {:invalid_shard_count, shard_count}}}

  defp shard_indexes(0), do: []
  defp shard_indexes(shard_count), do: 0..(shard_count - 1)

  defp remove_target_trees(target_node, paths) do
    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case target_fs_call(target_node, :rm_rf, [path]) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:target_cleanup_failed, path, reason}}}
        other -> {:halt, {:error, {:target_cleanup_failed, path, {:unexpected_result, other}}}}
      end
    end)
  end

  defp remove_target_files(target_node, paths) do
    Enum.reduce_while(paths, :ok, fn path, :ok ->
      case target_fs_call(target_node, :rm, [path]) do
        :ok -> {:cont, :ok}
        {:error, {:not_found, _reason}} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:target_cleanup_failed, path, reason}}}
        other -> {:halt, {:error, {:target_cleanup_failed, path, {:unexpected_result, other}}}}
      end
    end)
  end

  defp target_fs_call(target_node, function, args) do
    :erpc.call(target_node, Ferricstore.FS, function, args, 30_000)
  catch
    kind, reason -> {:error, {:rpc_failed, kind, reason}}
  end

  @doc false
  def __target_shard_has_data_for_test__(target_node, data_dir, shard_idx) do
    probe_target_shard_data_result(target_node, data_dir, shard_idx)
  end

  @doc false
  def __cleanup_target_data_dir_for_test__(target_node, data_dir, shard_count) do
    cleanup_target_data_dir(target_node, data_dir, shard_count)
  end

  # Auto-join: triggered by :nodeup, runs in a spawned process so
  # handle_info returns immediately. Routes through GenServer.call
  # so the dedup guard in handle_call prevents concurrent joins.
  def do_auto_join(target_node, role) do
    Logger.info("ClusterManager: auto-joining #{target_node} as #{role}")

    case wait_for_remote_app(target_node) do
      :ok ->
        case GenServer.call(
               Ferricstore.Cluster.Manager,
               {:add_node, target_node, role},
               :infinity
             ) do
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
end
