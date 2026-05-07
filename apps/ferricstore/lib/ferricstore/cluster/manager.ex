defmodule Ferricstore.Cluster.Manager do
  @moduledoc """
  Manages cluster membership: monitors node connections, orchestrates join/leave
  flows, and coordinates data sync for new followers.

  Subscribes to :net_kernel nodeup/nodedown events. When a new node connects
  (via libcluster or manual Node.connect), checks if it needs to be added to
  the Raft groups. When a node disconnects, starts a removal timer.

  ## Modes

    * `:standalone` — no cluster configured, no-op
    * `:cluster` — cluster_nodes configured, actively managing membership
  """

  use GenServer
  require Logger

  alias Ferricstore.Cluster.DataSync
  alias Ferricstore.Cluster.JoinIdentity
  alias Ferricstore.Cluster.TargetMarker
  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @default_remove_delay_ms 60_000

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
    GenServer.call(__MODULE__, :node_status)
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

  @doc "Promotes a manual standalone node into a one-node Raft cluster."
  @spec enable_cluster(keyword()) :: :ok | {:error, term()}
  def enable_cluster(opts \\ []) do
    GenServer.call(__MODULE__, {:enable_cluster, opts}, 120_000)
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

    if Ferricstore.ReplicationMode.current() == :enabling do
      {:ok, state, {:continue, :recover_enable}}
    else
      {:ok, state}
    end
  end

  @impl true
  def handle_call(:mode, _from, state) do
    {:reply, state.mode, state}
  end

  def handle_call(:sync_status, _from, state) do
    {:reply, state.sync_status, state}
  end

  def handle_call(:node_status, _from, state) do
    status =
      for shard_idx <- 0..(state.shard_count - 1), into: %{} do
        case RaftCluster.members(shard_idx) do
          {:ok, members, leader} ->
            {shard_idx, %{members: members, leader: leader}}

          {:error, reason} ->
            {shard_idx, %{error: reason}}
        end
      end

    {:reply,
     %{
       mode: state.mode,
       role: state.role,
       node: node(),
       connected_nodes: Node.list(),
       known_nodes: MapSet.to_list(state.known_nodes),
       sync_status: state.sync_status,
       shard_sync_status: state.shard_sync_status,
       shards: status
     }, state}
  end

  def handle_call({:enable_cluster, opts}, _from, state) do
    dryrun? = Keyword.get(opts, :dryrun, false)

    case enable_cluster_dryrun(state) do
      :ok when dryrun? ->
        {:reply, :ok, state}

      :ok ->
        case do_enable_cluster(state) do
          :ok ->
            {:reply, :ok, %{state | mode: :cluster, sync_status: :synced}}

          {:error, reason} = error ->
            {:reply, error, Map.put(state, :last_enable_error, reason)}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
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
    new_known = MapSet.delete(state.known_nodes, target_node)
    {:reply, result, %{state | known_nodes: new_known}}
  end

  def handle_call(:leave, _from, state) do
    result = do_leave(state)
    {:reply, result, %{state | mode: :standalone}}
  end

  @impl true
  def handle_continue(:recover_enable, state) do
    Logger.warning("ClusterManager: recovering interrupted CLUSTER.ENABLE promotion")

    case do_recover_enable_cluster(state) do
      :ok ->
        {:noreply, %{state | mode: :cluster, sync_status: :synced}}

      {:error, reason} ->
        {:noreply, Map.put(state, :last_enable_error, reason)}
    end
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
            case RaftCluster.members(i) do
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
      # Check if the remote node's cluster_nodes includes us.
      # IMPORTANT: Only the lowest-named existing node handles the join
      # to prevent multiple nodes racing to join the same new node.
      true ->
        spawn(fn ->
          try do
            remote_nodes =
              :erpc.call(node, Application, :get_env, [:ferricstore, :cluster_nodes, []], 5_000)

            if node() in remote_nodes do
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

                result =
                  GenServer.call(__MODULE__, {:add_node, node, remote_role, []}, 120_000)

                Logger.info("ClusterManager: auto-join result for #{node}: #{inspect(result)}")
              else
                Logger.debug(
                  "ClusterManager: skipping join for #{node}, coordinator is #{coordinator}"
                )
              end
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

  defp enable_cluster_dryrun(state) do
    cond do
      Ferricstore.ReplicationMode.raft?() ->
        :ok

      Ferricstore.ReplicationMode.current() not in [:standalone, :enabling] ->
        {:error, {:invalid_replication_mode, Ferricstore.ReplicationMode.current()}}

      not Node.alive?() ->
        {:error, :node_not_alive}

      state.shard_count < 1 ->
        {:error, :invalid_shard_count}

      true ->
        :ok
    end
  end

  defp do_enable_cluster(_state) do
    case Ferricstore.ReplicationMode.current() do
      :raft -> :ok
      :enabling -> do_recover_enable_cluster()
      :standalone -> do_fresh_enable_cluster()
    end
  end

  defp do_fresh_enable_cluster do
    ctx = FerricStore.Instance.get(:default)
    epoch = System.unique_integer([:positive, :monotonic])

    with :ok <- Ferricstore.ReplicationMode.mark_enabling!(ctx.data_dir, ctx.shard_count, epoch),
         :ok <- complete_enable_cluster(ctx, epoch) do
      :ok
    else
      {:error, reason} = error ->
        Logger.error("ClusterManager: CLUSTER.ENABLE failed: #{inspect(reason)}")
        fail_closed_enable_failure(ctx)
        error

      other ->
        Logger.error("ClusterManager: CLUSTER.ENABLE failed: #{inspect(other)}")
        fail_closed_enable_failure(ctx)
        {:error, other}
    end
  end

  defp do_recover_enable_cluster(_state \\ nil) do
    ctx = FerricStore.Instance.get(:default)
    epoch = existing_promotion_epoch(ctx)

    if Node.alive?() do
      case complete_enable_cluster(ctx, epoch) do
        :ok ->
          Logger.info("ClusterManager: interrupted CLUSTER.ENABLE promotion recovered")
          :ok

        {:error, reason} = error ->
          Logger.error("ClusterManager: CLUSTER.ENABLE recovery failed: #{inspect(reason)}")
          fail_closed_enable_failure(ctx)
          error

        other ->
          Logger.error("ClusterManager: CLUSTER.ENABLE recovery failed: #{inspect(other)}")
          fail_closed_enable_failure(ctx)
          {:error, other}
      end
    else
      Logger.error(
        "ClusterManager: CLUSTER.ENABLE recovery refused because node has no stable distributed name"
      )

      fail_closed_enable_failure(ctx)
      {:error, :node_not_alive}
    end
  end

  defp complete_enable_cluster(ctx, epoch) do
    with :ok <- set_readiness(false),
         :ok <- pause_all_shards(ctx),
         :ok <- flush_all_shards(ctx),
         :ok <- start_local_raft(ctx),
         :ok <- trigger_elections(ctx.shard_count),
         {:ok, barrier_indices} <- commit_barriers(ctx.shard_count),
         :ok <-
           Ferricstore.ReplicationMode.mark_raft!(
             ctx.data_dir,
             ctx.shard_count,
             epoch,
             barrier_indices
           ),
         :ok <- resume_all_shards(ctx),
         :ok <- set_readiness(true) do
      Logger.info("ClusterManager: standalone node promoted to Raft cluster")
      :ok
    end
  end

  defp existing_promotion_epoch(ctx) do
    case Ferricstore.ReplicationMode.read(ctx.data_dir) do
      {:ok, %{promotion_epoch: epoch}} when is_integer(epoch) ->
        epoch

      _ ->
        System.unique_integer([:positive, :monotonic])
    end
  end

  defp fail_closed_enable_failure(ctx) do
    _ = pause_all_shards_best_effort(ctx)
    _ = set_readiness(false)

    Logger.error(
      "ClusterManager: CLUSTER.ENABLE left node fail-closed with writes paused and readiness=false; durable marker remains :enabling"
    )

    :ok
  end

  defp set_readiness(value) do
    Ferricstore.Health.set_ready(value)
    :ok
  end

  defp pause_all_shards(%{shard_count: shard_count} = ctx) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case GenServer.call(elem(ctx.shard_names, i), {:pause_writes}, 30_000) do
        :ok -> {:cont, :ok}
        other -> {:halt, {:error, {:pause_shard_failed, i, other}}}
      end
    end)
  end

  defp resume_all_shards(%{shard_count: shard_count} = ctx) do
    Enum.each(0..(shard_count - 1), fn i ->
      try do
        GenServer.call(elem(ctx.shard_names, i), {:resume_writes}, 5_000)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  defp pause_all_shards_best_effort(%{shard_count: shard_count} = ctx) do
    Enum.each(0..(shard_count - 1), fn i ->
      try do
        GenServer.call(elem(ctx.shard_names, i), {:pause_writes}, 5_000)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  defp flush_all_shards(%{shard_count: shard_count} = ctx) do
    with :ok <- flush_bitcask_writers(ctx),
         :ok <- sync_shards(ctx, shard_count) do
      :ok
    end
  end

  defp flush_bitcask_writers(%{shard_count: shard_count} = ctx) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case Ferricstore.Store.BitcaskWriter.flush(ctx, i, 30_000) do
        :ok -> {:cont, :ok}
        other -> {:halt, {:error, {:bitcask_writer_flush_failed, i, other}}}
      end
    end)
  end

  defp sync_shards(ctx, shard_count) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case GenServer.call(elem(ctx.shard_names, i), :flush, 30_000) do
        :ok -> {:cont, :ok}
        other -> {:halt, {:error, {:shard_sync_failed, i, other}}}
      end
    end)
  end

  defp start_local_raft(%{data_dir: data_dir, shard_count: shard_count} = ctx) do
    with :ok <- Ferricstore.Raft.Cluster.start_system(data_dir),
         :ok <- start_batchers(shard_count),
         :ok <- start_shard_raft(ctx) do
      :ok
    end
  end

  defp start_batchers(shard_count) do
    Enum.reduce_while(Ferricstore.Application.raft_batcher_children(shard_count), :ok, fn spec,
                                                                                          :ok ->
      case Supervisor.start_child(Ferricstore.Supervisor, spec) do
        {:ok, _pid} -> {:cont, :ok}
        {:ok, _pid, _info} -> {:cont, :ok}
        {:error, {:already_started, _pid}} -> {:cont, :ok}
        {:error, :already_present} -> {:cont, :ok}
        other -> {:halt, {:error, {:batcher_start_failed, spec.id, other}}}
      end
    end)
  end

  defp start_shard_raft(%{shard_count: shard_count} = ctx) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
      case GenServer.call(elem(ctx.shard_names, i), :start_raft, 30_000) do
        :ok -> {:cont, :ok}
        other -> {:halt, {:error, {:shard_raft_start_failed, i, other}}}
      end
    end)
  end

  defp trigger_elections(shard_count) do
    case Ferricstore.Raft.Cluster.trigger_shard_elections_parallel(shard_count) do
      :ok -> :ok
      {:error, reason} -> {:error, {:raft_election_failed, reason}}
    end
  end

  defp commit_barriers(shard_count) do
    Enum.reduce_while(0..(shard_count - 1), {:ok, %{}}, fn i, {:ok, acc} ->
      case Ferricstore.Raft.Batcher.write(i, {:batch, []}) do
        {:ok, []} ->
          case raft_last_applied(i) do
            {:ok, index} -> {:cont, {:ok, Map.put(acc, i, index)}}
            {:error, reason} -> {:halt, {:error, {:barrier_index_failed, i, reason}}}
          end

        other ->
          {:halt, {:error, {:barrier_failed, i, other}}}
      end
    end)
  end

  defp raft_last_applied(shard_index) do
    server_id = Ferricstore.Raft.Cluster.shard_server_id(shard_index)

    case :ra.member_overview(server_id) do
      {:ok, overview, _} -> {:ok, Map.get(overview, :last_applied, 0)}
      other -> {:error, other}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: join flow (add to Raft + data sync — used by both auto and manual)
  # ---------------------------------------------------------------------------

  # Full join: sync data FIRST, then add to Raft groups.
  # Order matters: if we add to Raft first, the new node's existing ra servers
  # (started as single-node leaders) conflict with the cluster's leaders.
  # By syncing data first, the new node receives the cluster's Bitcask files.
  # Then when added to Raft, ra can start replicating from the sync point.
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

    # Stop ra on target if it's running (standalone node)
    try do
      stop_raft_on_target(target_node, state.shard_count)
    catch
      _, _ -> :ok
    end

    ctx = FerricStore.Instance.get(:default)

    with {:ok, target_has_data} <- target_has_data?(target_node, state.shard_count),
         :ok <- validate_target_data_identity(target_node, ctx, target_has_data, replace?) do
      if target_has_data and not replace? do
        # Disk clone / rejoin path: target already has Bitcask data.
        Logger.info("ClusterManager: #{target_node} has pre-existing data, skipping data sync")
        stop_raft_on_target(target_node, state.shard_count)

        sync_indices = read_target_indices(target_node, state.shard_count)

        start_target_raft_and_finish_join(
          target_node,
          membership,
          state,
          ctx,
          sync_indices,
          "added to Raft groups (disk clone)",
          false
        )
      else
        join_node_with_data_sync(target_node, membership, state, ctx, replace?, target_has_data)
      end
    else
      {:error, reason} = error ->
        Logger.error("ClusterManager: refusing join for #{target_node}: #{inspect(reason)}")
        error
    end
  end

  defp join_node_with_data_sync(target_node, membership, state, ctx, replace?, target_has_data) do
    with :ok <- maybe_cleanup_replace_target(target_node, state, replace?, target_has_data) do
      # Empty node path: needs data sync.
      # Order: sync data → start ra server → add_member → kickstart.
      # The ra server must be running BEFORE add_member so it can receive
      # the leader's initial append_entries immediately. The server won't
      # elect itself because initial_members includes all cluster nodes
      # and quorum requires votes from nodes that don't know it yet.
      stop_raft_on_target(target_node, state.shard_count)

      case direct_sync(target_node, ctx) do
        {:ok, sync_indices} ->
          start_target_raft_and_finish_join(
            target_node,
            membership,
            state,
            ctx,
            sync_indices,
            "fully joined and synced",
            true
          )

        {:error, reason} ->
          Logger.error("ClusterManager: data sync failed for #{target_node}: #{inspect(reason)}")
          {:error, {:sync_failed, reason}}
      end
    else
      {:error, _reason} = error -> error
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

  defp start_target_raft_and_finish_join(
         target_node,
         membership,
         state,
         ctx,
         sync_indices,
         success_message,
         cleanup_data_on_failure?
       ) do
    case start_raft_on_target(target_node, state.shard_count, sync_indices) do
      :ok ->
        add_node_and_persist_target_marker(
          target_node,
          membership,
          state,
          ctx,
          sync_indices,
          success_message,
          cleanup_data_on_failure?
        )

      {:error, _reason} = error ->
        rollback_target_state_after_start_failure(
          target_node,
          state,
          cleanup_data_on_failure?,
          error
        )
    end
  end

  defp add_node_and_persist_target_marker(
         target_node,
         membership,
         state,
         ctx,
         sync_indices,
         success_message,
         cleanup_data_on_failure?
       ) do
    preexisting_membership = target_membership_by_shard(target_node, state)
    {raft_result, _} = do_add_node(target_node, membership, state)

    case raft_result do
      :ok ->
        case write_target_cluster_marker(target_node, ctx, sync_indices) do
          :ok ->
            kickstart_replication(target_node, state.shard_count)
            Logger.info("ClusterManager: #{target_node} #{success_message}")
            :ok

          {:error, _reason} = err ->
            rollback_join_membership_after_marker_failure(
              target_node,
              state,
              err,
              preexisting_membership,
              cleanup_data_on_failure?
            )
        end

      {:error, _} = err ->
        Logger.error("ClusterManager: Raft add failed for #{target_node}: #{inspect(err)}")
        err
    end
  end

  defp rollback_join_membership_after_marker_failure(
         target_node,
         state,
         marker_error,
         preexisting_membership,
         cleanup_data_on_failure?
       ) do
    Logger.error(
      "ClusterManager: target marker write failed for #{target_node}: #{inspect(marker_error)}; rolling back Raft membership"
    )

    membership_rollback = remove_join_added_members(target_node, state, preexisting_membership)
    target_rollback = cleanup_target_join_state(target_node, state, cleanup_data_on_failure?)

    case {membership_rollback, target_rollback} do
      {:ok, :ok} ->
        marker_error

      {membership_error, target_error} ->
        Logger.error(
          "ClusterManager: rollback after target marker failure failed for #{target_node}: #{inspect({membership_error, target_error})}"
        )

        {:error,
         {:target_marker_failed_rollback_failed, marker_error, membership_error, target_error}}
    end
  end

  defp rollback_target_state_after_start_failure(
         target_node,
         state,
         cleanup_data_on_failure?,
         start_error
       ) do
    Logger.error(
      "ClusterManager: target Raft start failed for #{target_node}: #{inspect(start_error)}; rolling back target state"
    )

    case cleanup_target_join_state(target_node, state, cleanup_data_on_failure?) do
      :ok ->
        start_error

      rollback_error ->
        {:error, {:target_raft_start_failed_rollback_failed, start_error, rollback_error}}
    end
  end

  defp cleanup_target_join_state(target_node, state, cleanup_data?) do
    stop_result = stop_raft_on_target(target_node, state.shard_count)

    cleanup_result =
      if cleanup_data?, do: cleanup_target_data(target_node, state.shard_count), else: :ok

    case {stop_result, cleanup_result} do
      {:ok, :ok} -> :ok
      other -> {:error, {:target_join_state_cleanup_failed, other}}
    end
  end

  defp target_membership_by_shard(target_node, state) do
    case Process.get(:ferricstore_cluster_manager_target_membership_hook) do
      hook when is_function(hook, 2) ->
        hook.(target_node, state)

      _ ->
        for shard_idx <- 0..(state.shard_count - 1), into: %{} do
          status =
            case target_member?(target_node, shard_idx) do
              {:ok, member?} -> member?
              {:error, _reason} -> :unknown
            end

          {shard_idx, status}
        end
    end
  end

  defp target_member?(target_node, shard_idx) do
    case RaftCluster.members(shard_idx) do
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

  defp remove_join_added_members(target_node, state, preexisting_membership) do
    rollback_results =
      for shard_idx <- 0..(state.shard_count - 1),
          preexisting_member_status(preexisting_membership, shard_idx) == false,
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

  defp preexisting_member_status(preexisting_membership, shard_idx)
       when is_map(preexisting_membership) do
    Map.get(preexisting_membership, shard_idx, :unknown)
  end

  defp preexisting_member_status(_preexisting_membership, _shard_idx), do: :unknown

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
    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, shard_idx)

    case :erpc.call(target_node, File, :ls, [shard_path], 5_000) do
      {:ok, files} ->
        case probe_target_log_files(target_node, shard_path, files) do
          {:ok, true} -> {:halt, {:ok, true}}
          {:ok, false} -> {:cont, {:ok, false}}
          {:error, _reason} = error -> {:halt, error}
        end

      {:error, :enoent} ->
        {:cont, {:ok, false}}

      {:error, reason} ->
        {:halt, {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, reason}}}}

      other ->
        {:halt, {:error, {:target_data_probe_failed, target_node, {:ls, shard_path, other}}}}
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

  defp validate_target_data_identity(_target_node, _ctx, false, _replace?), do: :ok
  defp validate_target_data_identity(_target_node, _ctx, true, true), do: :ok

  defp validate_target_data_identity(target_node, ctx, true, false) do
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
    try do
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

      Enum.each(0..(shard_count - 1), fn i ->
        shard_path = Ferricstore.DataDir.shard_data_path(target_ctx.data_dir, i)
        :erpc.call(target_node, File, :rm_rf!, [shard_path], 30_000)
      end)

      :erpc.call(
        target_node,
        File,
        :rm_rf!,
        [Path.join(target_ctx.data_dir, "dedicated")],
        30_000
      )

      :ok
    catch
      kind, reason ->
        Logger.warning(
          "ClusterManager: failed while cleaning target data on #{target_node}: #{inspect({kind, reason})}"
        )

        {:error, {:target_cleanup_failed, target_node, {kind, reason}}}
    end
  end

  defp direct_sync(target_node, ctx) do
    case Process.get(:ferricstore_cluster_manager_direct_sync_hook) do
      hook when is_function(hook, 2) -> hook.(target_node, ctx)
      _ -> do_direct_sync(target_node, ctx)
    end
  end

  defp do_direct_sync(target_node, ctx) do
    case Ferricstore.Cluster.DataSync.sync_all_shards(target_node, ctx) do
      {:ok, sync_results} ->
        Logger.info("ClusterManager: data synced to #{target_node}: #{inspect(sync_results)}")

        # Extract raft indices from sync results.
        # For :wal_bridgeable shards (already had data), read the actual
        # last_applied index from the target's ra DETS file — this tells us
        # what the pre-existing data covers (e.g., disk clone scenario).
        sync_indices =
          for {shard_idx, {:synced, detail}} <- sync_results, into: %{} do
            case detail do
              :wal_bridgeable ->
                # Target had data — read the index from its ra state on disk
                idx =
                  try do
                    target_data_dir =
                      :erpc.call(target_node, FerricStore.Instance, :get, [:default]).data_dir

                    :erpc.call(
                      target_node,
                      Ferricstore.Cluster.DataSync,
                      :read_last_applied_from_disk,
                      [target_data_dir, shard_idx],
                      5_000
                    )
                  catch
                    _, _ -> 0
                  end

                {shard_idx, idx}

              raft_idx when is_integer(raft_idx) ->
                {shard_idx, raft_idx}

              _ ->
                {shard_idx, 0}
            end
          end

        {:ok, sync_indices}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Auto-join: triggered by :nodeup, runs in a spawned process so
  # handle_info returns immediately. Routes through GenServer.call
  # so the dedup guard in handle_call prevents concurrent joins.
  defp do_auto_join(target_node, role) do
    Logger.info("ClusterManager: auto-joining #{target_node} as #{role}")
    wait_for_remote_app(target_node)

    case GenServer.call(__MODULE__, {:add_node, target_node, role}, 120_000) do
      :ok ->
        Logger.info("ClusterManager: auto-join complete for #{target_node}")

      {:error, reason} ->
        Logger.error("ClusterManager: auto-join failed for #{target_node}: #{inspect(reason)}")
    end
  end

  defp wait_for_remote_app(target_node, attempts \\ 20) do
    if attempts <= 0 do
      Logger.warning("ClusterManager: timed out waiting for FerricStore on #{target_node}")
    else
      case :erpc.call(target_node, FerricStore.Instance, :get, [:default], 2_000) do
        %{} -> :ok
        _ -> wait_for_remote_app(target_node, attempts - 1)
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

    for shard_idx <- 0..(state.shard_count - 1) do
      # If the target is leader for this shard, transfer leadership first
      case RaftCluster.members(shard_idx) do
        {:ok, _members, {_name, ^target_node}} ->
          # Target is leader — transfer before removing
          other_voters = Node.list() -- [target_node]

          if other_voters != [] do
            RaftCluster.transfer_leadership(shard_idx, hd(other_voters))
            Process.sleep(100)
          end

        _ ->
          :ok
      end

      RaftCluster.remove_member(shard_idx, target_node)
    end

    :ok
  end

  defp do_leave(state) do
    Logger.info("ClusterManager: leaving cluster")

    for shard_idx <- 0..(state.shard_count - 1) do
      # Transfer leadership away from us if we're the leader
      case RaftCluster.members(shard_idx) do
        {:ok, _members, {_name, node}} when node == node() ->
          other_voters = Node.list()

          if other_voters != [] do
            Logger.info(
              "ClusterManager: transferring shard #{shard_idx} leadership to #{hd(other_voters)}"
            )

            RaftCluster.transfer_leadership(shard_idx, hd(other_voters))
            Process.sleep(200)
          end

        _ ->
          :ok
      end

      # Remove ourselves from the Raft group
      RaftCluster.remove_member(shard_idx, node())
    end

    :ok
  end

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

  # Start Raft servers on the target node. Called after data sync + add_member.
  # Stop and clean up ra servers on the target so they don't conflict
  # with the cluster's Raft groups when we add the target as a member.
  defp stop_raft_on_target(target_node, shard_count) do
    case Process.get(:ferricstore_cluster_manager_stop_raft_on_target_hook) do
      hook when is_function(hook, 2) -> hook.(target_node, shard_count)
      _ -> do_stop_raft_on_target(target_node, shard_count)
    end
  end

  defp do_stop_raft_on_target(target_node, shard_count) do
    Logger.info("ClusterManager: stopping Raft on #{target_node} before join")

    ra_sys = Ferricstore.Raft.Cluster.system_name()

    for shard_idx <- 0..(shard_count - 1) do
      server_id = Ferricstore.Raft.Cluster.shard_server_id_on(shard_idx, target_node)

      try do
        :erpc.call(target_node, :ra, :stop_server, [ra_sys, server_id])
      catch
        _, _ -> :ok
      end

      try do
        :erpc.call(target_node, :ra, :force_delete_server, [ra_sys, server_id])
      catch
        _, _ -> :ok
      end
    end

    Process.sleep(50)
    :ok
  end

  defp start_raft_on_target(target_node, shard_count, sync_indices) do
    case Process.get(:ferricstore_cluster_manager_start_raft_on_target_hook) do
      hook when is_function(hook, 3) -> hook.(target_node, shard_count, sync_indices)
      _ -> do_start_raft_on_target(target_node, shard_count, sync_indices)
    end
  end

  defp do_start_raft_on_target(target_node, shard_count, sync_indices) do
    Logger.info("ClusterManager: starting Raft on #{target_node}")

    with {:ok, cluster_members} <- cluster_member_nodes_for_join(target_node),
         :ok <- start_target_raft_servers(target_node, shard_count, sync_indices, cluster_members),
         :ok <- enable_target_shard_raft(target_node, shard_count) do
      :ok
    else
      {:error, {:cluster_members_unavailable, reason}} ->
        {:error, {:target_raft_start_failed, :cluster_members, reason}}

      {:error, _reason} = error ->
        error
    end
  end

  defp cluster_member_nodes_for_join(target_node) do
    case Process.get(:ferricstore_cluster_manager_cluster_members_hook) do
      hook when is_function(hook, 1) ->
        normalize_cluster_member_nodes(hook.(target_node), target_node)

      _ ->
        do_cluster_member_nodes_for_join(target_node)
    end
  end

  defp do_cluster_member_nodes_for_join(target_node) do
    # Shard 0 Raft membership is the authoritative cluster membership source.
    # Unknown membership must abort join rather than seeding target Raft with
    # arbitrary connected Erlang nodes.
    case RaftCluster.members(0) do
      {:ok, members, _leader} ->
        nodes = Enum.map(members, &member_node/1) |> Enum.reject(&is_nil/1) |> Enum.uniq()
        {:ok, if(target_node in nodes, do: nodes, else: [target_node | nodes])}

      {:error, reason} ->
        {:error, {:cluster_members_unavailable, reason}}

      other ->
        {:error, {:cluster_members_unavailable, {:unexpected_members_result, other}}}
    end
  rescue
    error ->
      {:error, {:cluster_members_unavailable, error}}
  end

  defp normalize_cluster_member_nodes({:ok, nodes}, target_node) when is_list(nodes) do
    nodes = Enum.uniq(nodes)
    {:ok, if(target_node in nodes, do: nodes, else: [target_node | nodes])}
  end

  defp normalize_cluster_member_nodes({:error, reason}, _target_node),
    do: {:error, {:cluster_members_unavailable, reason}}

  defp normalize_cluster_member_nodes(other, _target_node),
    do: {:error, {:cluster_members_unavailable, {:unexpected_result, other}}}

  defp start_target_raft_servers(target_node, shard_count, sync_indices, cluster_members) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn shard_idx, :ok ->
      case start_target_raft_server(target_node, shard_idx, sync_indices, cluster_members) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:target_raft_start_failed, shard_idx, reason}}}
      end
    end)
  end

  defp start_target_raft_server(target_node, shard_idx, sync_indices, cluster_members) do
    target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default])
    shard_data_path = Ferricstore.DataDir.shard_data_path(target_ctx.data_dir, shard_idx)
    keydir = elem(target_ctx.keydir_refs, shard_idx)
    skip_idx = Map.get(sync_indices, shard_idx, 0)

    result =
      :erpc.call(target_node, Ferricstore.Raft.Cluster, :join_shard_server, [
        shard_idx,
        shard_data_path,
        0,
        Path.join(shard_data_path, "00000.log"),
        keydir,
        cluster_members,
        [skip_below_index: skip_idx]
      ])

    case result do
      :ok ->
        Logger.info(
          "ClusterManager: shard #{shard_idx} Raft joined on #{target_node} (skip_below=#{skip_idx})"
        )

        :ok

      {:error, reason} ->
        Logger.warning(
          "ClusterManager: shard #{shard_idx} Raft join failed on #{target_node}: #{inspect(reason)}"
        )

        {:error, reason}

      other ->
        Logger.warning(
          "ClusterManager: shard #{shard_idx} Raft join returned unexpected result on #{target_node}: #{inspect(other)}"
        )

        {:error, {:unexpected_result, other}}
    end
  catch
    kind, reason ->
      Logger.warning(
        "ClusterManager: shard #{shard_idx} Raft join failed on #{target_node}: #{inspect({kind, reason})}"
      )

      {:error, {kind, reason}}
  end

  defp enable_target_shard_raft(target_node, shard_count) do
    Enum.reduce_while(0..(shard_count - 1), :ok, fn shard_idx, :ok ->
      shard_name = :"Ferricstore.Store.Shard.#{shard_idx}"

      try do
        case :erpc.call(target_node, GenServer, :call, [shard_name, :enable_raft, 5_000]) do
          :ok ->
            {:cont, :ok}

          other ->
            {:halt, {:error, {:target_raft_start_failed, shard_idx, {:enable_raft, other}}}}
        end
      catch
        kind, reason ->
          {:halt,
           {:error, {:target_raft_start_failed, shard_idx, {:enable_raft, {kind, reason}}}}}
      end
    end)
  end

  defp kickstart_replication(_target_node, shard_count) do
    Process.sleep(100)

    for shard_idx <- 0..(shard_count - 1) do
      local_id = RaftCluster.shard_server_id(shard_idx)

      has_leader? =
        try do
          case :ra.members(local_id, 2_000) do
            {:ok, _members, _leader} -> true
            _ -> false
          end
        catch
          _, _ -> false
        end

      unless has_leader? do
        try do
          :ra.trigger_election(local_id)
        catch
          _, _ -> :ok
        end
      end
    end
  end

  @doc false
  def read_target_indices(target_node, shard_count) do
    target_data_dir =
      try do
        target_ctx = :erpc.call(target_node, FerricStore.Instance, :get, [:default], 5_000)
        target_ctx.data_dir
      catch
        _, _ -> nil
      end

    if target_data_dir do
      for shard_idx <- 0..(shard_count - 1), into: %{} do
        idx =
          try do
            :erpc.call(
              target_node,
              DataSync,
              :read_last_applied_from_disk,
              [target_data_dir, shard_idx],
              5_000
            )
          catch
            _, _ -> 0
          end

        {shard_idx, idx}
      end
    else
      for shard_idx <- 0..(shard_count - 1), into: %{}, do: {shard_idx, 0}
    end
  end

  defp role_to_membership(:voter), do: :voter
  defp role_to_membership(:replica), do: :promotable
  defp role_to_membership(:readonly), do: :non_voter
end
