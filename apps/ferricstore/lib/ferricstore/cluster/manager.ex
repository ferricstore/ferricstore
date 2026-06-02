defmodule Ferricstore.Cluster.Manager do
  @moduledoc """
  Manages cluster membership: monitors node connections, orchestrates join/leave
  flows, and coordinates data sync for new followers.

  Subscribes to :net_kernel nodeup/nodedown events. When a new node connects
  (via libcluster or manual Node.connect), checks if it needs to be added to
  the Raft groups. When a node disconnects, starts a removal timer.

  ## Modes

    * `:standalone` — no remote cluster configured
    * `:cluster` — cluster_nodes configured, actively managing membership
  """

  use GenServer
  require Logger

  alias Ferricstore.Cluster.DataSync
  alias Ferricstore.Cluster.JoinIdentity
  alias Ferricstore.Cluster.TargetMarker
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend

  @default_remove_delay_ms 60_000
  @membership_probe_timeout_ms 1_000
  @membership_operation_timeout_ms 5_000

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current cluster mode (:standalone or :cluster)."
  @spec mode() :: :standalone | :cluster
  def mode do
    GenServer.call(__MODULE__, :mode)
  end

  @doc "Returns sync status for this node (:synced, :syncing, or :not_started)."
  @spec sync_status() :: :synced | :syncing | :not_started
  def sync_status do
    GenServer.call(__MODULE__, :sync_status)
  end

  @doc "Returns a map of all known nodes and their status."
  @spec node_status() :: map()
  def node_status do
    node_status(:default)
  end

  @doc """
  Returns a map of all known nodes and their status using a bounded per-shard
  membership probe timeout.
  """
  @spec node_status(:default | non_neg_integer()) :: map()
  def node_status(membership_timeout) do
    call_timeout =
      case membership_timeout do
        timeout when is_integer(timeout) and timeout >= 0 -> max(5_000, timeout + 1_000)
        _ -> 5_000
      end

    GenServer.call(__MODULE__, {:node_status, membership_timeout}, call_timeout)
  end

  @doc """
  Adds a node to the cluster. Triggers data sync if needed.

  The node must be reachable via Erlang distribution (Node.connect or libcluster).
  """
  @spec add_node(node(), atom(), keyword()) :: :ok | {:error, term()}
  def add_node(node, role \\ :voter, opts \\ []) do
    GenServer.call(__MODULE__, {:add_node, node, role, opts}, 120_000)
  end

  @doc """
  Removes a node from the cluster gracefully.

  If the node is a leader for any shard, leadership is transferred first.
  """
  @spec remove_node(node()) :: :ok | {:error, term()}
  def remove_node(node) do
    GenServer.call(__MODULE__, {:remove_node, node}, 30_000)
  end

  @doc "Gracefully leaves the cluster (called on the departing node)."
  @spec leave() :: :ok | {:error, term()}
  def leave do
    GenServer.call(__MODULE__, :leave, 30_000)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    cluster_nodes = Application.get_env(:ferricstore, :cluster_nodes, [])
    role = Application.get_env(:ferricstore, :cluster_role, :voter)

    remove_delay =
      Keyword.get(
        opts,
        :remove_delay_ms,
        Application.get_env(:ferricstore, :cluster_remove_delay_ms, @default_remove_delay_ms)
      )

    mode = if cluster_nodes == [], do: :standalone, else: :cluster

    # Always subscribe to nodeup/nodedown — even standalone nodes need to
    # detect when a new node wants to join them (auto-discovery).
    if Node.alive?() do
      :net_kernel.monitor_nodes(true, node_type: :visible)
    end

    if mode == :cluster do
      Logger.info(
        "ClusterManager started in cluster mode, role=#{role}, nodes=#{inspect(cluster_nodes)}"
      )
    else
      Logger.info("ClusterManager started in standalone mode")
    end

    state = %{
      mode: mode,
      role: role,
      cluster_nodes: cluster_nodes,
      remove_delay_ms: remove_delay,
      known_nodes: MapSet.new(),
      remove_timers: %{},
      sync_status: if(mode == :cluster, do: :synced, else: :not_started),
      shard_sync_status: %{},
      shard_count: Application.get_env(:ferricstore, :shard_count, 4)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:mode, _from, state) do
    {:reply, state.mode, state}
  end

  def handle_call(:sync_status, _from, state) do
    {:reply, state.sync_status, state}
  end

  def handle_call(:node_status, _from, state) do
    {:reply, build_node_status(state, :default), state}
  end

  def handle_call({:node_status, membership_timeout}, _from, state) do
    {:reply, build_node_status(state, membership_timeout), state}
  end

  def handle_call({:add_node, target_node, role}, from, state) do
    handle_call({:add_node, target_node, role, []}, from, state)
  end

  def handle_call({:add_node, target_node, role, opts}, _from, state) do
    if MapSet.member?(state.known_nodes, target_node) do
      Logger.info("ClusterManager: #{target_node} already known, skipping join")
      {:reply, :ok, state}
    else
      membership = role_to_membership(role)

      result =
        try do
          do_join_node(target_node, membership, state, opts)
        catch
          kind, reason ->
            Logger.error(
              "ClusterManager: join failed for #{target_node}: #{inspect(kind)} #{inspect(reason)}"
            )

            {:error, {kind, reason}}
        end

      state =
        case result do
          :ok -> %{state | known_nodes: MapSet.put(state.known_nodes, target_node)}
          _ -> state
        end

      {:reply, result, state}
    end
  end

  def handle_call({:remove_node, target_node}, _from, state) do
    result = do_remove_node(target_node, state)

    new_state =
      case result do
        :ok -> %{state | known_nodes: MapSet.delete(state.known_nodes, target_node)}
        _ -> state
      end

    {:reply, result, new_state}
  end

  def handle_call(:leave, _from, state) do
    result = do_leave(state)

    new_state =
      case result do
        :ok -> %{state | mode: :standalone}
        _ -> state
      end

    {:reply, result, new_state}
  end

  defp build_node_status(state, membership_timeout) do
    status =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case RaftCluster.members(shard_idx, membership_timeout) do
          {:ok, members, leader} ->
            {shard_idx, %{members: members, leader: leader}}

          {:error, reason} ->
            {shard_idx, %{error: reason}}
        end
      end

    %{
      mode: state.mode,
      role: state.role,
      node: node(),
      connected_nodes: Node.list(),
      known_nodes: MapSet.to_list(state.known_nodes),
      sync_status: state.sync_status,
      shard_sync_status: state.shard_sync_status,
      shards: status
    }
  end

  # ---------------------------------------------------------------------------
  # Node monitoring
  # ---------------------------------------------------------------------------

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Logger.info("ClusterManager: node connected: #{node}")

    state = cancel_remove_timer(state, node)

    cond do
      # Case 1: We know this node — it's in our cluster_nodes config.
      # Only initiate auto-join if WE are an established node (have Raft leaders).
      # Fresh nodes with cluster_nodes config should wait for the existing
      # cluster to add them via Case 2, not try to add existing nodes to themselves.
      node in state.cluster_nodes ->
        new_known = MapSet.put(state.known_nodes, node)

        has_leaders =
          Enum.any?(0..(state.shard_count - 1), fn i ->
            case RaftCluster.members(i, @membership_probe_timeout_ms) do
              {:ok, members, _leader} -> length(members) > 1
              _ -> false
            end
          end)

        if has_leaders do
          spawn(fn ->
            remote_role =
              try do
                :erpc.call(
                  node,
                  Application,
                  :get_env,
                  [:ferricstore, :cluster_role, :voter],
                  5_000
                )
              catch
                _, _ -> :voter
              end

            do_auto_join(node, remote_role)
          end)
        else
          Logger.info(
            "ClusterManager: deferring join for #{node} — we have no multi-member shards yet, waiting for existing cluster to add us"
          )
        end

        {:noreply, %{state | known_nodes: new_known, mode: :cluster}}

      # Case 2: We don't know this node, but it might want to join us.
      # Check if the remote node's cluster_nodes includes us. The joiner's
      # cluster_auto_join flag is the intent signal; the existing node does
      # not need to have booted with cluster_nodes just to accept the request.
      # IMPORTANT: Only the lowest-named existing node handles the join
      # to prevent multiple nodes racing to join the same new node.
      true ->
        spawn(fn ->
          try do
            remote_nodes =
              :erpc.call(node, Application, :get_env, [:ferricstore, :cluster_nodes, []], 5_000)

            remote_auto_join? =
              :erpc.call(
                node,
                Application,
                :get_env,
                [:ferricstore, :cluster_auto_join, false],
                5_000
              ) == true

            if remote_auto_join? and node() in remote_nodes do
              # Deduplicate: only the lowest-named connected node performs the join.
              # All nodes see :nodeup, but only one should act.
              existing_nodes =
                Enum.filter(remote_nodes, fn n -> n != node and n in Node.list() end)

              all_candidates = Enum.sort([node() | existing_nodes])
              coordinator = hd(all_candidates)

              if node() == coordinator do
                remote_role =
                  :erpc.call(
                    node,
                    Application,
                    :get_env,
                    [:ferricstore, :cluster_role, :voter],
                    5_000
                  )

                Logger.info(
                  "ClusterManager: #{node} wants to join us as #{remote_role}, initiating auto-join (coordinator: #{node()})"
                )

                do_auto_join(node, remote_role)
              else
                Logger.debug(
                  "ClusterManager: skipping join for #{node}, coordinator is #{coordinator}"
                )
              end
            else
              Logger.debug(
                "ClusterManager: ignoring #{node}; remote auto-join is disabled or does not target us"
              )
            end
          catch
            kind, reason ->
              Logger.error(
                "ClusterManager: auto-join failed for #{node}: #{inspect(kind)}: #{inspect(reason)}"
              )
          end
        end)

        {:noreply, state}
    end
  end

  def handle_info({:nodedown, _node, _info}, %{mode: :standalone} = state) do
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("ClusterManager: node disconnected: #{node}")

    # Start a delayed removal timer — don't remove immediately (could be transient)
    timer_ref = Process.send_after(self(), {:remove_timeout, node}, state.remove_delay_ms)
    new_timers = Map.put(state.remove_timers, node, timer_ref)

    {:noreply, %{state | remove_timers: new_timers}}
  end

  def handle_info({:remove_timeout, node}, state) do
    if node not in Node.list() do
      Logger.warning(
        "ClusterManager: node #{node} still down after #{state.remove_delay_ms}ms, removing from Raft groups"
      )

      do_remove_node(node, state)
    end

    new_timers = Map.delete(state.remove_timers, node)
    new_known = MapSet.delete(state.known_nodes, node)
    {:noreply, %{state | remove_timers: new_timers, known_nodes: new_known}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Private: join flow (data sync + WARaft membership — used by both auto and manual)
  # ---------------------------------------------------------------------------

  # Full join: sync data FIRST, then add to WARaft groups.
  # Order matters: if we add to membership first, the new node's local
  # consensus runtime can conflict with the cluster's leaders.
  # By syncing data first, the new node receives the cluster's Bitcask files.
  # Then, when added to WARaft, replication starts from the sync point.
  #
  # This is the single code path for both :nodeup auto-join and CLUSTER.JOIN.
  defp do_join_node(target_node, membership, state, opts) do
    if target_node == node() do
      Logger.info("ClusterManager: ignoring self-join for #{target_node}")
      :ok
    else
      do_join_node_remote(target_node, membership, state, opts)
    end
  end

  defp do_join_node_remote(target_node, membership, state, opts) do
    Logger.info("ClusterManager: joining #{target_node} (#{membership})")
    replace? = Keyword.get(opts, :replace, false)

    ctx = FerricStore.Instance.get(:default)

    do_join_node_remote_waraft(target_node, membership, state, ctx, replace?)
  end

  defp do_join_node_remote_waraft(target_node, membership, state, ctx, replace?) do
    with {:ok, target_has_data} <- target_has_data?(target_node, state.shard_count),
         :ok <- validate_target_data_identity(target_node, ctx, target_has_data, replace?),
         {:ok, preexisting_membership} <- target_membership_by_shard(target_node, state),
         :ok <- maybe_cleanup_replace_target(target_node, state, replace?, target_has_data) do
      cleanup_data_on_failure? = replace? or not target_has_data

      add_node_and_persist_waraft_target_marker(
        target_node,
        membership,
        state,
        ctx,
        cleanup_data_on_failure?,
        preexisting_membership
      )
    else
      {:error, reason} = error ->
        Logger.error("ClusterManager: refusing join for #{target_node}: #{inspect(reason)}")
        error
    end
  end

  defp maybe_cleanup_replace_target(_target_node, _state, false, _target_has_data), do: :ok
  defp maybe_cleanup_replace_target(_target_node, _state, true, false), do: :ok

  defp maybe_cleanup_replace_target(target_node, state, true, true) do
    Logger.warning("ClusterManager: replacing pre-existing data on #{target_node}")

    case cleanup_target_data(target_node, state.shard_count) do
      :ok ->
        :ok

      {:error, _reason} = error ->
        Logger.error(
          "ClusterManager: refusing REPLACE join after cleanup failure: #{inspect(error)}"
        )

        error
    end
  end

  defp add_node_and_persist_waraft_target_marker(
         target_node,
         membership,
         state,
         ctx,
         cleanup_data_on_failure?,
         preexisting_membership
       ) do
    {raft_result, shard_results} = do_add_node(target_node, membership, state)

    case raft_result do
      :ok ->
        with {:ok, barrier_indices} <- waraft_barrier_indices(state.shard_count),
             :ok <- write_target_cluster_marker(target_node, ctx, barrier_indices) do
          kickstart_replication(target_node, state.shard_count)
          Logger.info("ClusterManager: #{target_node} joined through WARaft snapshot replication")
          :ok
        else
          {:error, _reason} = err ->
            rollback_waraft_join_after_marker_failure(
              target_node,
              state,
              err,
              preexisting_membership,
              cleanup_data_on_failure?
            )
        end

      {:error, _} = err ->
        Logger.error("ClusterManager: WARaft add failed for #{target_node}: #{inspect(err)}")

        rollback_waraft_join_after_add_failure(
          target_node,
          state,
          err,
          cleanup_data_on_failure?,
          preexisting_membership,
          shard_results
        )
    end
  end

  defp waraft_barrier_indices(shard_count) do
    case Process.get(:ferricstore_cluster_manager_waraft_barrier_indices_hook) do
      hook when is_function(hook, 1) ->
        hook.(shard_count)

      _ ->
        read_waraft_barrier_indices(shard_count)
    end
  end

  defp read_waraft_barrier_indices(shard_count) do
    Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn shard_idx, {:ok, acc} ->
      case WARaftBackend.storage_position(shard_idx) do
        {:ok, {:raft_log_pos, index, _term}} when is_integer(index) and index >= 0 ->
          {:cont, {:ok, Map.put(acc, shard_idx, index)}}

        {:ok, position} ->
          {:halt, {:error, {:waraft_barrier_index_unavailable, shard_idx, position}}}

        {:error, reason} ->
          {:halt, {:error, {:waraft_barrier_index_unavailable, shard_idx, reason}}}

        other ->
          {:halt, {:error, {:waraft_barrier_index_unavailable, shard_idx, other}}}
      end
    end)
  end

  defp rollback_waraft_join_after_marker_failure(
         target_node,
         state,
         marker_error,
         preexisting_membership,
         cleanup_data_on_failure?
       ) do
    Logger.error(
      "ClusterManager: WARaft target marker write failed for #{target_node}: #{inspect(marker_error)}; rolling back membership"
    )

    membership_rollback = remove_join_added_members(target_node, state, preexisting_membership)

    target_rollback =
      cleanup_waraft_target_join_state(target_node, state, cleanup_data_on_failure?)

    case {membership_rollback, target_rollback} do
      {:ok, :ok} ->
        marker_error

      {membership_error, target_error} ->
        Logger.error(
          "ClusterManager: WARaft rollback after target marker failure failed for #{target_node}: #{inspect({membership_error, target_error})}"
        )

        {:error,
         {:waraft_target_marker_failed_rollback_failed, marker_error, membership_error,
          target_error}}
    end
  end

  defp rollback_waraft_join_after_add_failure(
         target_node,
         state,
         add_error,
         cleanup_data_on_failure?,
         preexisting_membership,
         _shard_results
       ) do
    membership_rollback = remove_join_added_members(target_node, state, preexisting_membership)

    target_rollback =
      cleanup_waraft_target_join_state(target_node, state, cleanup_data_on_failure?)

    case {membership_rollback, target_rollback} do
      {:ok, :ok} ->
        add_error

      {membership_error, target_error} ->
        {:error, {:waraft_add_failed_rollback_failed, add_error, membership_error, target_error}}
    end
  end

  defp cleanup_waraft_target_join_state(target_node, state, cleanup_data?) do
    if cleanup_data? do
      cleanup_target_data(target_node, state.shard_count)
    else
      :ok
    end
  end

  defp target_membership_by_shard(target_node, state) do
    case Process.get(:ferricstore_cluster_manager_target_membership_hook) do
      hook when is_function(hook, 2) ->
        normalize_target_membership_result(hook.(target_node, state), target_node)

      _ ->
        target_membership_by_shard_real(target_node, state.shard_count)
    end
  end

  defp target_membership_by_shard_real(target_node, shard_count) do
    Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn shard_idx, {:ok, acc} ->
      case target_member?(target_node, shard_idx) do
        {:ok, member?} ->
          {:cont, {:ok, Map.put(acc, shard_idx, member?)}}

        {:error, reason} ->
          {:halt, {:error, {:target_membership_snapshot_failed, target_node, shard_idx, reason}}}
      end
    end)
  end

  defp normalize_target_membership_result({:ok, membership}, _target_node)
       when is_map(membership),
       do: {:ok, membership}

  defp normalize_target_membership_result(membership, _target_node) when is_map(membership) do
    if Enum.all?(membership, fn {_shard, status} -> is_boolean(status) end) do
      {:ok, membership}
    else
      {:error, {:target_membership_snapshot_failed, :invalid_membership_snapshot}}
    end
  end

  defp normalize_target_membership_result({:error, reason}, target_node),
    do: {:error, {:target_membership_snapshot_failed, target_node, reason}}

  defp normalize_target_membership_result(other, target_node),
    do: {:error, {:target_membership_snapshot_failed, target_node, {:unexpected_result, other}}}

  defp target_member?(target_node, shard_idx) do
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

  defp member_node({_name, node}), do: node
  defp member_node(%{id: {_name, node}}), do: node
  defp member_node(_member), do: nil

  defp transfer_target_from_members(members, target_node) when is_list(members) do
    members
    |> Enum.map(&member_node/1)
    |> Enum.find(fn
      node when is_atom(node) and not is_nil(node) -> node != target_node
      _other -> false
    end)
  end

  defp transfer_target_from_members(_members, _target_node), do: nil

  defp remove_join_added_members(target_node, state, preexisting_membership) do
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

  defp remove_join_added_member(target_node, shard_idx) do
    case Process.get(:ferricstore_cluster_manager_remove_added_member_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, shard_idx)

      _ ->
        RaftCluster.remove_member(shard_idx, target_node)
    end
  end

  # Checks if the target node has pre-existing Bitcask data (disk clone scenario).
  defp target_has_data?(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_target_has_data_hook) do
      hook when is_function(hook, 2) ->
        normalize_target_has_data_result(hook.(target_node, shard_count), target_node)

      _ ->
        do_target_has_data?(target_node, shard_count)
    end
  end

  defp normalize_target_has_data_result(value, _target_node) when is_boolean(value),
    do: {:ok, value}

  defp normalize_target_has_data_result({:ok, value}, _target_node) when is_boolean(value),
    do: {:ok, value}

  defp normalize_target_has_data_result({:error, _reason} = error, _target_node), do: error

  defp normalize_target_has_data_result(other, target_node),
    do: {:error, {:target_data_probe_failed, target_node, {:unexpected_result, other}}}

  defp do_target_has_data?(target_node, shard_count) do
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

  defp probe_target_shard_data(target_node, data_dir, shard_idx) do
    case probe_target_shard_data_result(target_node, data_dir, shard_idx) do
      {:ok, true} -> {:halt, {:ok, true}}
      {:ok, false} -> {:cont, {:ok, false}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp probe_target_shard_data_result(target_node, data_dir, shard_idx) do
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

  defp reduce_target_data_probe({:ok, true}), do: {:halt, {:ok, true}}
  defp reduce_target_data_probe({:ok, false}), do: {:cont, {:ok, false}}
  defp reduce_target_data_probe({:error, _reason} = error), do: {:halt, error}

  defp probe_target_bitcask_logs(target_node, shard_path) do
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

  defp probe_target_log_files(target_node, shard_path, files) do
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

  defp probe_target_file_tree(target_node, path) do
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

  defp probe_target_file_tree_entries(target_node, path, files) do
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

  defp validate_target_data_identity(_target_node, _ctx, false, _replace?), do: :ok

  defp validate_target_data_identity(target_node, ctx, true, true) do
    local_state = Ferricstore.ReplicationMode.read(ctx.data_dir)
    target_state = read_target_cluster_state(target_node)

    JoinIdentity.validate(local_state, target_state, target_node)
  end

  defp validate_target_data_identity(target_node, ctx, true, _replace?) do
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

  defp read_target_cluster_state(target_node) do
    case Process.get(:ferricstore_cluster_manager_read_target_cluster_state_hook) do
      hook when is_function(hook, 1) ->
        hook.(target_node)

      _ ->
        do_read_target_cluster_state(target_node)
    end
  end

  defp do_read_target_cluster_state(target_node) do
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

  defp write_target_cluster_marker(target_node, ctx, barrier_indices) do
    case Process.get(:ferricstore_cluster_manager_write_target_marker_hook) do
      hook when is_function(hook, 3) -> hook.(target_node, ctx, barrier_indices)
      _ -> TargetMarker.write(target_node, ctx, barrier_indices)
    end
  end

  defp cleanup_target_data(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_cleanup_target_data_hook) do
      hook when is_function(hook, 2) -> hook.(target_node, shard_count)
      _ -> do_cleanup_target_data(target_node, shard_count)
    end
  end

  defp do_cleanup_target_data(target_node, shard_count) do
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

  defp cleanup_target_data_dir(target_node, data_dir, shard_count) do
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

  defp extract_direct_sync_indices(target_node, sync_results) when is_map(sync_results) do
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

  defp extract_direct_sync_indices(target_node, sync_results) do
    {:error,
     {:target_index_read_failed, target_node, :sync_results, {:unexpected_result, sync_results}}}
  end

  defp maybe_target_data_dir_for_wal_bridgeable(target_node, sync_results) do
    if Enum.any?(sync_results, fn {_shard_idx, result} -> result == {:synced, :wal_bridgeable} end) do
      target_data_dir(target_node)
    else
      {:ok, nil}
    end
  end

  # Auto-join: triggered by :nodeup, runs in a spawned process so
  # handle_info returns immediately. Routes through GenServer.call
  # so the dedup guard in handle_call prevents concurrent joins.
  defp do_auto_join(target_node, role) do
    Logger.info("ClusterManager: auto-joining #{target_node} as #{role}")

    case wait_for_remote_app(target_node) do
      :ok ->
        case GenServer.call(__MODULE__, {:add_node, target_node, role}, 120_000) do
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

  defp wait_for_remote_app(target_node, attempts \\ 20) do
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

  defp do_add_node(target_node, membership, state) do
    case Process.get(:ferricstore_cluster_manager_do_add_node_hook) do
      hook when is_function(hook, 3) ->
        hook.(target_node, membership, state)

      _ ->
        do_add_node_real(target_node, membership, state)
    end
  end

  defp do_add_node_real(target_node, membership, state) do
    Logger.info(
      "ClusterManager: adding #{target_node} as #{membership} to all #{state.shard_count} shards"
    )

    shard_results =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case RaftCluster.add_member(shard_idx, target_node, membership) do
          :ok ->
            Logger.debug("ClusterManager: added #{target_node} to shard #{shard_idx}")
            {shard_idx, :ok}

          {:error, reason} ->
            Logger.error(
              "ClusterManager: failed to add #{target_node} to shard #{shard_idx}: #{inspect(reason)}"
            )

            {shard_idx, {:error, reason}}
        end
      end

    failed = Enum.filter(shard_results, fn {_, v} -> v != :ok end)

    if failed != [] do
      {{:error, {:partial_add, shard_results}}, shard_results}
    else
      {:ok, shard_results}
    end
  end

  defp do_remove_node(target_node, state) do
    case Process.get(:ferricstore_cluster_manager_do_remove_node_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, state)

      _ ->
        do_remove_node_real(target_node, state)
    end
  end

  defp do_remove_node_real(target_node, state) do
    Logger.info("ClusterManager: removing #{target_node} from all #{state.shard_count} shards")

    shard_results =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        result =
          with :ok <- maybe_transfer_target_leadership(shard_idx, target_node, 100) do
            cluster_remove_member(shard_idx, target_node)
          end

        {shard_idx, normalize_cluster_operation_result(result)}
      end

    failed = Enum.filter(shard_results, fn {_shard_idx, result} -> result != :ok end)

    if failed == [] do
      :ok
    else
      {:error, {:partial_remove, shard_results}}
    end
  end

  defp do_leave(state) do
    Logger.info("ClusterManager: leaving cluster")

    local_node = node()

    shard_results =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        result =
          with :ok <- maybe_transfer_target_leadership(shard_idx, local_node, 200) do
            cluster_remove_member(shard_idx, local_node)
          end

        {shard_idx, normalize_cluster_operation_result(result)}
      end

    failed = Enum.filter(shard_results, fn {_shard_idx, result} -> result != :ok end)

    if failed == [] do
      :ok
    else
      {:error, {:partial_leave, shard_results}}
    end
  end

  defp maybe_transfer_target_leadership(shard_idx, target_node, sleep_ms) do
    case cluster_members(shard_idx) do
      {:ok, members, {_name, ^target_node}} ->
        case transfer_target_from_members(members, target_node) do
          nil -> :ok
          replacement -> transfer_target_leadership(shard_idx, replacement, sleep_ms)
        end

      {:ok, _members, _leader} ->
        :ok

      {:error, reason} ->
        {:error, {:members_failed, reason}}

      other ->
        {:error, {:members_failed, other}}
    end
  end

  defp transfer_target_leadership(shard_idx, target_node, sleep_ms) do
    Logger.info("ClusterManager: transferring shard #{shard_idx} leadership to #{target_node}")

    case cluster_transfer_leadership(shard_idx, target_node) do
      :ok ->
        Process.sleep(sleep_ms)
        :ok

      {:error, reason} ->
        {:error, {:transfer_failed, reason}}

      other ->
        {:error, {:transfer_failed, other}}
    end
  end

  defp cluster_members(shard_idx) do
    case Process.get(:ferricstore_cluster_manager_members_hook) do
      hook when is_function(hook, 1) -> hook.(shard_idx)
      _ -> RaftCluster.members(shard_idx, @membership_operation_timeout_ms)
    end
  end

  defp cluster_transfer_leadership(shard_idx, target_node) do
    case Process.get(:ferricstore_cluster_manager_transfer_leadership_hook) do
      hook when is_function(hook, 2) -> hook.(shard_idx, target_node)
      _ -> RaftCluster.transfer_leadership(shard_idx, target_node)
    end
  end

  defp cluster_remove_member(shard_idx, target_node) do
    case Process.get(:ferricstore_cluster_manager_remove_member_hook) do
      hook when is_function(hook, 2) -> hook.(shard_idx, target_node)
      _ -> RaftCluster.remove_member(shard_idx, target_node)
    end
  end

  defp normalize_cluster_operation_result(:ok), do: :ok
  defp normalize_cluster_operation_result({:error, _reason} = error), do: error
  defp normalize_cluster_operation_result(other), do: {:error, other}

  # ---------------------------------------------------------------------------
  # Private: helpers
  # ---------------------------------------------------------------------------

  defp cancel_remove_timer(state, node) do
    case Map.pop(state.remove_timers, node) do
      {nil, _timers} ->
        state

      {timer_ref, new_timers} ->
        Process.cancel_timer(timer_ref)
        Logger.info("ClusterManager: cancelled removal timer for #{node} (reconnected)")
        %{state | remove_timers: new_timers}
    end
  end

  defp kickstart_replication(_target_node, shard_count) do
    Process.sleep(100)

    needs_election? =
      Enum.any?(0..(shard_count - 1), fn shard_idx ->
        try do
          not match?(
            {:ok, _members, _leader},
            RaftCluster.members(shard_idx, @membership_probe_timeout_ms)
          )
        catch
          _, _ -> true
        end
      end)

    if needs_election? do
      _ = RaftCluster.trigger_shard_elections_parallel(shard_count, timeout: 10_000)
    end
  end

  @doc false
  def read_target_indices(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_read_target_indices_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, shard_count)

      _ ->
        do_read_target_indices(target_node, shard_count)
    end
  end

  defp do_read_target_indices(target_node, shard_count) do
    with {:ok, target_data_dir} <- target_data_dir(target_node) do
      Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn shard_idx, {:ok, acc} ->
        case read_target_shard_index(target_node, target_data_dir, shard_idx) do
          {:ok, idx} -> {:cont, {:ok, Map.put(acc, shard_idx, idx)}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)
    end
  end

  defp target_data_dir(target_node) do
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

  defp read_target_shard_index(target_node, target_data_dir, shard_idx) do
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

  defp role_to_membership(:voter), do: :voter
  defp role_to_membership(:replica), do: :promotable
  defp role_to_membership(:readonly), do: :non_voter
end
