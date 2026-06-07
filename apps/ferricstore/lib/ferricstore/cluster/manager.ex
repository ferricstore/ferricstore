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

  alias Ferricstore.Cluster.Manager.Target
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

            Target.do_auto_join(node, remote_role)
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

                Target.do_auto_join(node, remote_role)
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
    with {:ok, target_has_data} <- Target.target_has_data?(target_node, state.shard_count),
         :ok <- Target.validate_target_data_identity(target_node, ctx, target_has_data, replace?),
         {:ok, preexisting_membership} <- Target.target_membership_by_shard(target_node, state),
         :ok <-
           prepare_waraft_snapshot_target(
             target_node,
             state,
             replace?,
             target_has_data,
             preexisting_membership
           ) do
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

  defp prepare_waraft_snapshot_target(
         target_node,
         state,
         replace?,
         target_has_data,
         preexisting_membership
       ) do
    case Process.get(:ferricstore_cluster_manager_prepare_snapshot_target_hook) do
      hook when is_function(hook, 4) ->
        normalize_cluster_operation_result(hook.(target_node, state, replace?, target_has_data))

      _other ->
        do_prepare_waraft_snapshot_target(
          target_node,
          state,
          replace?,
          target_has_data,
          preexisting_membership
        )
    end
  end

  defp do_prepare_waraft_snapshot_target(
         target_node,
         state,
         replace?,
         target_has_data,
         preexisting_membership
       ) do
    cond do
      not replace? and target_has_data ->
        :ok

      not replace? and target_already_member?(preexisting_membership) ->
        :ok

      Process.get(:ferricstore_cluster_manager_do_add_node_hook) != nil ->
        :ok

      true ->
        if replace? and target_has_data do
          Logger.warning("ClusterManager: replacing pre-existing data on #{target_node}")
        end

        with :ok <- stop_waraft_on_target(target_node, state.shard_count),
             :ok <- Target.cleanup_target_data(target_node, state.shard_count),
             :ok <- start_waraft_on_target_for_snapshot(target_node, state.shard_count) do
          :ok
        else
          {:error, _reason} = error ->
            Logger.error(
              "ClusterManager: refusing join after WARaft snapshot target preparation failure: #{inspect(error)}"
            )

            error

          other ->
            {:error, other}
        end
    end
  end

  defp target_already_member?(membership) when is_map(membership) do
    Enum.any?(membership, fn {_shard_idx, member?} -> member? == true end)
  end

  defp target_already_member?(_membership), do: false

  defp stop_waraft_on_target(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_stop_raft_on_target_hook) do
      hook when is_function(hook, 2) ->
        normalize_cluster_operation_result(hook.(target_node, shard_count))

      _other ->
        try do
          :erpc.call(target_node, Ferricstore.Raft.WARaftBackend, :stop, [], 30_000)
          |> normalize_cluster_operation_result()
        catch
          kind, reason -> {:error, {:target_raft_stop_failed, target_node, {kind, reason}}}
        end
    end
  end

  defp start_waraft_on_target_for_snapshot(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_start_raft_on_target_hook) do
      hook when is_function(hook, 3) ->
        normalize_cluster_operation_result(hook.(target_node, shard_count, :snapshot_join))

      _other ->
        do_start_waraft_on_target_for_snapshot(target_node)
    end
  end

  defp do_start_waraft_on_target_for_snapshot(target_node) do
    try do
      target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)

      :erpc.call(
        target_node,
        Ferricstore.Raft.WARaftBackend,
        :start,
        [
          target_ctx,
          [
            bootstrap: false,
            log_module: Ferricstore.Raft.WARaftBackend.default_log_module(),
            commit_batch_interval_ms:
              Ferricstore.Raft.WARaftBackend.default_commit_batch_interval_ms(),
            commit_batch_max: Ferricstore.Raft.WARaftBackend.default_commit_batch_max()
          ]
        ],
        30_000
      )
      |> normalize_cluster_operation_result()
    catch
      kind, reason -> {:error, {:target_raft_start_failed, target_node, {kind, reason}}}
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
             :ok <- Target.write_target_cluster_marker(target_node, ctx, barrier_indices) do
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

    membership_rollback = Target.remove_join_added_members(target_node, state, preexisting_membership)

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
         shard_results
       ) do
    membership_rollback =
      Target.remove_join_added_members(target_node, state, preexisting_membership, shard_results)

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
      Target.cleanup_target_data(target_node, state.shard_count)
    else
      :ok
    end
  end


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

    timeout_ms = cluster_membership_timeout_ms()

    shard_results =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case add_member_to_shard(shard_idx, target_node, membership, timeout_ms) do
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

    cond do
      failed == [] ->
        {:ok, shard_results}

      transient_partial_add?(failed) and
          target_member_on_all_shards?(target_node, state.shard_count) ->
        Logger.info(
          "ClusterManager: concurrent add for #{target_node} already converged across all shards"
        )

        {:ok, mark_all_shards_ok(shard_results, state.shard_count)}

      true ->
        {{:error, {:partial_add, shard_results}}, shard_results}
    end
  end

  defp add_member_to_shard(shard_idx, target_node, membership, timeout_ms, attempts_left \\ 3) do
    case RaftCluster.add_member(shard_idx, target_node, membership, timeout_ms: timeout_ms) do
      :ok ->
        :ok

      {:error, reason} = error ->
        cond do
          target_member_now?(target_node, shard_idx) ->
            :ok

          attempts_left > 1 and transient_add_member_error?(reason) ->
            Process.sleep(add_member_retry_delay_ms(attempts_left))
            add_member_to_shard(shard_idx, target_node, membership, timeout_ms, attempts_left - 1)

          true ->
            error
        end
    end
  end

  defp add_member_retry_delay_ms(3), do: 100
  defp add_member_retry_delay_ms(2), do: 250
  defp add_member_retry_delay_ms(_attempts_left), do: 500

  defp transient_partial_add?(failed) do
    Enum.any?(failed, fn
      {_shard_idx, {:error, reason}} -> transient_add_member_error?(reason)
      _other -> false
    end)
  end

  defp transient_add_member_error?(:not_ready), do: true
  defp transient_add_member_error?(:peer_ready_timeout), do: true
  defp transient_add_member_error?(:timeout), do: true
  defp transient_add_member_error?({:timeout, _reason}), do: true
  defp transient_add_member_error?({:unknown_outcome, _reason}), do: true
  defp transient_add_member_error?({:membership_unknown_outcome, _reason}), do: true
  defp transient_add_member_error?({:add_member_unknown_outcome, _reason}), do: true
  defp transient_add_member_error?(_reason), do: false

  defp target_member_now?(target_node, shard_idx) do
    case Target.target_member?(target_node, shard_idx) do
      {:ok, true} -> true
      _other -> false
    end
  end

  defp target_member_on_all_shards?(target_node, shard_count) do
    Enum.all?(0..(shard_count - 1), fn shard_idx -> target_member_now?(target_node, shard_idx) end)
  end

  defp mark_all_shards_ok(shard_results, shard_count) do
    Enum.reduce(0..(shard_count - 1), shard_results, fn shard_idx, acc ->
      Map.put(acc, shard_idx, :ok)
    end)
  end

  defp cluster_membership_timeout_ms do
    case Application.get_env(:ferricstore, :cluster_membership_timeout_ms, 30_000) do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms
      _other -> 30_000
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
        case Target.transfer_target_from_members(members, target_node) do
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


   false
  def __extract_direct_sync_indices_for_test__(target_node, sync_results), do: Target.__extract_direct_sync_indices_for_test__(target_node, sync_results)

   false
  def __target_shard_has_data_for_test__(target_node, data_dir, shard_idx), do: Target.__target_shard_has_data_for_test__(target_node, data_dir, shard_idx)

   false
  def __cleanup_target_data_dir_for_test__(target_node, data_dir, shard_count), do: Target.__cleanup_target_data_dir_for_test__(target_node, data_dir, shard_count)

   false
  def read_target_indices(target_node, shard_count), do: Target.read_target_indices(target_node, shard_count)

  defp role_to_membership(:voter), do: :voter
  defp role_to_membership(:replica), do: :promotable
  defp role_to_membership(:readonly), do: :non_voter
end
