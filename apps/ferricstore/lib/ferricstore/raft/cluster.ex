defmodule Ferricstore.Raft.Cluster do
  @moduledoc """
  Manages the ra Raft cluster for FerricStore shards.

  Each shard is an independent Raft group with its own leader. In single-node
  mode (development, testing), each group has exactly one member -- self quorum.

  This module provides functions to:
    * Start the ra system
    * Start individual shard ra servers
    * Build ra server IDs and configurations

  ## Deployment topology (per spec section 2.6)

  Single node: each shard's Raft group has one member. Writes are durable
  after local log append + fsync. No network round trip needed.

  Three-node cluster: each shard's Raft group has three members. Writes
  require quorum (2 of 3) acknowledgement before commit.
  """

  alias Ferricstore.Raft.Backend
  alias Ferricstore.Raft.WARaftBackend

  require Logger

  @ra_system :ferricstore_raft

  @doc """
  Returns the ra system name used by FerricStore.
  """
  @spec system_name() :: atom()
  def system_name, do: @ra_system

  @doc """
  Starts the ra system for FerricStore.

  Must be called before any ra servers are started. The data directory
  for ra's WAL and segment files is placed under `data_dir/ra`.

  ## Parameters

    * `data_dir` -- base data directory for FerricStore
  """
  @spec start_system(binary()) :: :ok | {:error, term()}
  def start_system(data_dir) do
    start_system(data_dir, Backend.selected())
  end

  @doc false
  @spec start_system(binary(), :ra | :waraft) :: :ok | {:error, term()}
  def start_system(data_dir, :ra), do: start_legacy_system(data_dir)
  def start_system(_data_dir, :waraft), do: :ok

  defp start_legacy_system(data_dir) do
    remember_local_raft_node!()

    ra_dir_str = Path.join(data_dir, "ra")
    created? = not Ferricstore.FS.dir?(ra_dir_str)
    Ferricstore.FS.mkdir_p!(ra_dir_str)

    # Fsync the parent so the `ra/` directory entry is durable. ra
    # manages its own files' durability internally, but the dir entry
    # itself needs the parent fsync or a kernel panic between mkdir
    # and ra's first file-create can lose the directory on reboot.
    with :ok <- maybe_fsync_created_ra_dir(created?, data_dir) do
      names = :ra_system.derive_names(@ra_system)

      commit_delay_us =
        Application.get_env(:ferricstore, :wal_commit_delay_us, 6_000)

      config = system_config(ra_dir_str, names, commit_delay_us)

      case :ra_system.start(config) do
        {:ok, _pid} ->
          Logger.info("ra system started: #{inspect(@ra_system)}")
          :ok

        {:error, {:already_started, _pid}} ->
          :ok

        {:error, reason} = err ->
          Logger.error("Failed to start ra system: #{inspect(reason)}")
          err
      end
    end
  end

  defp maybe_fsync_created_ra_dir(false, _data_dir), do: :ok

  defp maybe_fsync_created_ra_dir(true, data_dir) do
    case raft_cluster_fsync_dir(data_dir) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "Failed to fsync ra directory parent #{data_dir} after creating ra/: #{inspect(reason)}"
        )

        {:error, {:fsync_dir_failed, :create_ra_dir, reason}}
    end
  end

  @doc false
  @spec system_config(binary()) :: map()
  def system_config(data_dir) when is_binary(data_dir) do
    names = :ra_system.derive_names(@ra_system)
    commit_delay_us = Application.get_env(:ferricstore, :wal_commit_delay_us, 6_000)

    system_config(data_dir, names, commit_delay_us)
  end

  defp system_config(data_dir, names, commit_delay_us) do
    ra_data_dir = to_charlist(data_dir)

    %{
      name: @ra_system,
      names: names,
      data_dir: ra_data_dir,
      wal_data_dir: ra_data_dir,
      segment_max_entries: ra_segment_max_entries(),
      segment_max_size_bytes: ra_segment_max_size_bytes(),
      wal_max_size_bytes: ra_wal_max_size_bytes(),
      wal_max_batch_size: 32_768,
      wal_compute_checksums: ra_wal_compute_checksums?(),
      wal_pre_allocate: true,
      wal_io_module: :ferricstore_wal_nif,
      wal_commit_delay_us: commit_delay_us
    }
  end

  defp raft_cluster_fsync_dir(path) do
    case Process.get(:ferricstore_raft_cluster_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end
  end

  @doc """
  Stops the FerricStore ra system and clears ra's system registry entry.

  The ra system is started before the FerricStore supervisor tree, so it is not
  a child of `Ferricstore.Supervisor`. The OTP application stop callback must
  call this explicitly after supervised shards/batchers have shut down.
  """
  @spec stop_system() :: :ok | {:error, term()}
  def stop_system do
    stop_system(Backend.selected())
  end

  @doc false
  @spec stop_system(:ra | :waraft) :: :ok | {:error, term()}
  def stop_system(:ra), do: stop_legacy_system()
  def stop_system(:waraft), do: :ok

  defp stop_legacy_system do
    result =
      case :ra_system.stop(@ra_system) do
        :ok ->
          :ok

        {:error, {:not_found, _}} ->
          :ok

        {:error, :not_found} ->
          :ok

        {:error, reason} = error ->
          Logger.warning("Failed to stop ra system #{inspect(@ra_system)}: #{inspect(reason)}")
          error
      end

    Application.delete_env(:ferricstore, :raft_local_node)
    result
  catch
    :exit, {:noproc, _} ->
      Application.delete_env(:ferricstore, :raft_local_node)
      :ok
  end

  @doc """
  Starts a ra server that joins an existing Raft group as a follower.

  Unlike `start_shard_server/6` which may create a new single-node group,
  this function configures `initial_members` with the provided cluster members
  so ra knows to join the existing group. Used when a new node joins an
  already-running cluster after data sync.
  """
  @spec join_shard_server(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          atom(),
          [node()],
          keyword()
        ) ::
          :ok | {:error, term()}
  def join_shard_server(
        shard_index,
        shard_data_path,
        active_file_id,
        active_file_path,
        ets,
        cluster_members,
        opts \\ []
      ) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_shard_start}
    else
      join_legacy_shard_server(
        shard_index,
        shard_data_path,
        active_file_id,
        active_file_path,
        ets,
        cluster_members,
        opts
      )
    end
  end

  defp join_legacy_shard_server(
         shard_index,
         shard_data_path,
         active_file_id,
         active_file_path,
         ets,
         cluster_members,
         opts
       ) do
    ra_sys = Keyword.get(opts, :ra_system, @ra_system)
    membership = Keyword.get(opts, :membership, :voter)

    skip_below_index = replay_skip_below_index(shard_data_path, opts)

    instance_name = Keyword.get(opts, :instance_name, :default)
    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
      ets: ets,
      data_dir: Ferricstore.DataDir.root_from_shard_path(shard_data_path),
      instance_name: instance_name,
      blob_side_channel_threshold_bytes: Keyword.get(opts, :blob_side_channel_threshold_bytes, 0),
      skip_below_index: skip_below_index,
      zset_score_index_name: Keyword.get(opts, :zset_score_index_name),
      zset_score_lookup_name: Keyword.get(opts, :zset_score_lookup_name),
      flow_index_name: Keyword.get(opts, :flow_index_name),
      flow_lookup_name: Keyword.get(opts, :flow_lookup_name)
    }

    initial_members =
      Enum.map(cluster_members, fn member_node ->
        shard_server_id_on(shard_index, member_node)
      end)

    server_config = %{
      id: server_id,
      uid: shard_uid(shard_index),
      cluster_name: shard_cluster_name(shard_index),
      initial_members: initial_members,
      membership: membership,
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: log_init_args_for_shard(shard_index),
      system: ra_sys,
      min_recovery_checkpoint_interval: 1
    }

    case :ra.start_server(ra_sys, server_config) do
      :ok ->
        Logger.info(
          "Shard #{shard_index}: joined cluster with #{length(initial_members)} members"
        )

        :ok

      {:error, reason} ->
        handle_join_start_error_action(ra_sys, server_id, shard_index, reason)
    end
  end

  @doc """
  Returns the ra server ID for a shard on a specific node.
  """
  @spec shard_server_id_on(non_neg_integer(), node()) :: :ra.server_id()
  def shard_server_id_on(shard_index, node) do
    {:"ferricstore_shard_#{shard_index}", node}
  end

  @doc """
  Adds a node to an existing shard's Raft group.

  The membership determines the node's role:
    * `:voter` — full quorum member (default)
    * `:promotable` — receives replication, can be promoted to voter
    * `:non_voter` — permanent read-only, never promoted

  ## Parameters

    * `shard_index` — zero-based shard index
    * `node` — the Erlang node to add
    * `membership` — `:voter`, `:promotable`, or `:non_voter` (default: `:voter`)
  """
  @spec add_member(non_neg_integer(), node(), atom()) :: :ok | {:error, term()}
  def add_member(shard_index, node, membership \\ :voter) do
    if Backend.waraft?() do
      waraft_add_member(shard_index, node, membership)
    else
      add_member_with_retry(shard_index, node, membership, 10)
    end
  end

  defp waraft_add_member(shard_index, node, :voter) do
    case WARaftBackend.add_member(shard_index, node) do
      {:ok, _position} -> :ok
      :already_member -> :ok
      {:error, :already_member} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp waraft_add_member(shard_index, node, :promotable) do
    case WARaftBackend.adjust_membership(shard_index, :remove_membership, node) do
      {:ok, _position} -> :ok
      {:error, :not_a_member} -> waraft_add_participant(shard_index, node)
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp waraft_add_member(shard_index, node, :non_voter) do
    waraft_add_member(shard_index, node, :promotable)
  end

  defp waraft_add_member(_shard_index, _node, membership),
    do: {:error, {:unsupported_waraft_membership, membership}}

  defp waraft_add_participant(shard_index, node) do
    case WARaftBackend.add_participant(shard_index, node) do
      {:ok, _position} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp add_member_with_retry(_shard_index, _node, _membership, 0) do
    {:error, :cluster_change_not_permitted}
  end

  defp add_member_with_retry(shard_index, node, membership, retries) do
    leader = shard_server_id(shard_index)

    new_member =
      case membership do
        :promotable ->
          %{
            id: shard_server_id_on(shard_index, node),
            membership: membership,
            uid: shard_uid(shard_index)
          }

        _ ->
          %{id: shard_server_id_on(shard_index, node), membership: membership}
      end

    case :ra.add_member(leader, new_member) do
      {_, _, _leader} ->
        :ok

      {:error, :already_member} ->
        :ok

      {:error, :cluster_change_not_permitted} ->
        Process.sleep(200)
        add_member_with_retry(shard_index, node, membership, retries - 1)

      {:error, reason} ->
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  end

  @doc """
  Removes a node from a shard's Raft group.
  """
  @spec remove_member(non_neg_integer(), node()) :: :ok | {:error, term()}
  def remove_member(shard_index, node) do
    if Backend.waraft?() do
      waraft_remove_member(shard_index, node)
    else
      leader = shard_server_id(shard_index)
      member = shard_server_id_on(shard_index, node)

      case :ra.remove_member(leader, member) do
        {_, _, _leader} -> :ok
        {:error, :not_member} -> :ok
        {:error, reason} -> {:error, reason}
        {:timeout, _} -> {:error, :timeout}
      end
    end
  end

  defp waraft_remove_member(shard_index, node) do
    case WARaftBackend.adjust_membership(shard_index, :remove_membership, node) do
      {:ok, _position} -> :ok
      {:error, :not_member} -> :ok
      {:error, :not_a_member} -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @doc """
  Returns the current members and leader for a shard's Raft group.
  """
  @spec members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
  def members(shard_index) do
    members(shard_index, :default)
  end

  @spec members(non_neg_integer(), timeout() | :default) ::
          {:ok, list(), term()} | {:error, term()}
  def members(shard_index, timeout) do
    if Backend.waraft?() do
      waraft_members(shard_index, timeout)
    else
      legacy_members(shard_index, timeout)
    end
  end

  defp legacy_members(shard_index, :default), do: :ra.members(shard_server_id(shard_index))

  defp legacy_members(shard_index, timeout),
    do: :ra.members(shard_server_id(shard_index), timeout)

  defp waraft_members(shard_index, :default), do: blocking_waraft_members(shard_index)

  defp waraft_members(shard_index, timeout) when is_integer(timeout) and timeout >= 0 do
    case WARaftBackend.cached_members(shard_index) do
      {:ok, _members, _leader} = result ->
        result

      _miss ->
        timed_waraft_members(shard_index, timeout)
    end
  end

  defp waraft_members(shard_index, _timeout), do: blocking_waraft_members(shard_index)

  defp timed_waraft_members(shard_index, timeout) do
    parent = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        send(parent, {ref, blocking_waraft_members(shard_index)})
      end)

    receive do
      {^ref, result} ->
        result
    after
      timeout ->
        Process.exit(pid, :kill)
        {:error, :timeout}
    end
  end

  defp blocking_waraft_members(shard_index) do
    case WARaftBackend.membership(shard_index) do
      members when is_list(members) ->
        leader =
          case WARaftBackend.status(shard_index) do
            status when is_list(status) ->
              leader_node = Keyword.get(status, :leader_id)
              Enum.find(members, fn {_server, node} -> node == leader_node end)

            _other ->
              nil
          end

        {:ok, members, leader}

      {:error, _reason} = error ->
        error

      other ->
        {:error, other}
    end
  end

  @doc """
  Returns the local Ra member overview for a shard.

  This is intentionally routed through the cluster facade so operational code
  does not accidentally bypass WARaft. WARaft does not expose a legacy ra
  snapshot overview, so callers should treat the returned error as "no legacy
  overview available".
  """
  @spec member_overview(non_neg_integer() | :ra.server_id()) ::
          {:ok, map()} | {:ok, map(), term()} | {:error, term()}
  def member_overview(shard_index) when is_integer(shard_index) and shard_index >= 0 do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_member_overview}
    else
      legacy_member_overview_on(node(), shard_server_id(shard_index))
    end
  end

  def member_overview(server_id), do: member_overview_on(node(), server_id)

  @doc """
  Returns a Ra member overview from `target_node` through the backend facade.

  Remote legacy calls stay here so WARaft replacement guards can prevent
  scattered `:erpc.call(node, :ra, ...)` probes in production code.
  """
  @spec member_overview_on(node(), :ra.server_id()) ::
          {:ok, map()} | {:ok, map(), term()} | {:error, term()}
  def member_overview_on(target_node, server_id) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_member_overview}
    else
      legacy_member_overview_on(target_node, server_id)
    end
  end

  defp legacy_member_overview_on(target_node, server_id) do
    if target_node == node() do
      :ra.member_overview(server_id)
    else
      try do
        :erpc.call(target_node, :ra, :member_overview, [server_id])
      catch
        _, _ -> :error
      end
    end
  end

  @doc """
  Stops a legacy Ra server on `target_node`.

  Kept behind this facade so cluster-management code does not use remote
  `:erpc.call(node, :ra, ...)` directly when WARaft is selected.
  """
  @spec stop_server_on(node(), atom(), :ra.server_id()) :: term()
  def stop_server_on(target_node, ra_sys, server_id) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_shard_stop}
    else
      legacy_stop_server_on(target_node, ra_sys, server_id)
    end
  end

  @doc """
  Force-deletes a legacy Ra server on `target_node` through the Raft facade.
  """
  @spec force_delete_server_on(node(), atom(), :ra.server_id()) :: term()
  def force_delete_server_on(target_node, ra_sys, server_id) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_shard_delete}
    else
      legacy_force_delete_server_on(target_node, ra_sys, server_id)
    end
  end

  defp legacy_stop_server_on(target_node, ra_sys, server_id) do
    if target_node == node() do
      :ra.stop_server(ra_sys, server_id)
    else
      :erpc.call(target_node, :ra, :stop_server, [ra_sys, server_id], 5_000)
    end
  end

  defp legacy_force_delete_server_on(target_node, ra_sys, server_id) do
    if target_node == node() do
      :ra.force_delete_server(ra_sys, server_id)
    else
      :erpc.call(target_node, :ra, :force_delete_server, [ra_sys, server_id], 5_000)
    end
  end

  @doc """
  Transfers leadership of a shard to a specific node.
  """
  @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
  def transfer_leadership(shard_index, target_node) do
    if Backend.waraft?() do
      WARaftBackend.transfer_leadership(shard_index, target_node)
    else
      server_id = shard_server_id(shard_index)
      target_id = shard_server_id_on(shard_index, target_node)
      :ra.transfer_leadership(server_id, target_id)
    end
  end

  @doc """
  Starts a ra server for a single shard.

  In single-node mode, creates a self-quorum Raft group. In cluster mode,
  uses the configured cluster_nodes as initial_members so all nodes form
  a single Raft group per shard.

  The `membership` option controls this node's role in the group:
    * `:voter` — full quorum member (default)
    * `:promotable` — receives replication, can be promoted
    * `:non_voter` — permanent read-only

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `shard_data_path` -- path to shard's Bitcask data directory
    * `active_file_id` -- current active log file ID
    * `active_file_path` -- path to current active log file
    * `ets` -- ETS table name (already created)

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec start_shard_server(
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          binary(),
          atom(),
          keyword()
        ) ::
          :ok | {:error, term()}
  def start_shard_server(
        shard_index,
        shard_data_path,
        active_file_id,
        active_file_path,
        ets,
        opts \\ []
      ) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_shard_start}
    else
      start_legacy_shard_server(
        shard_index,
        shard_data_path,
        active_file_id,
        active_file_path,
        ets,
        opts
      )
    end
  end

  defp start_legacy_shard_server(
         shard_index,
         shard_data_path,
         active_file_id,
         active_file_path,
         ets,
         opts
       ) do
    ra_sys = Keyword.get(opts, :ra_system, @ra_system)
    membership = Keyword.get(opts, :membership, :voter)
    instance_name = Keyword.get(opts, :instance_name, :default)
    wait_for_leader? = wait_for_leader_on_start?(opts)

    skip_below_index = replay_skip_below_index(shard_data_path, opts)

    server_id = shard_server_id(shard_index)

    machine_config = %{
      shard_index: shard_index,
      shard_data_path: shard_data_path,
      active_file_id: active_file_id,
      active_file_path: active_file_path,
      ets: ets,
      data_dir: Ferricstore.DataDir.root_from_shard_path(shard_data_path),
      instance_name: instance_name,
      blob_side_channel_threshold_bytes: Keyword.get(opts, :blob_side_channel_threshold_bytes, 0),
      skip_below_index: skip_below_index,
      zset_score_index_name: Keyword.get(opts, :zset_score_index_name),
      zset_score_lookup_name: Keyword.get(opts, :zset_score_lookup_name),
      flow_index_name: Keyword.get(opts, :flow_index_name),
      flow_lookup_name: Keyword.get(opts, :flow_lookup_name)
    }

    cluster_nodes = Application.get_env(:ferricstore, :cluster_nodes, [])
    initial_members = boot_initial_members(shard_index, server_id, cluster_nodes)

    server_config = %{
      id: server_id,
      uid: shard_uid(shard_index),
      cluster_name: shard_cluster_name(shard_index),
      initial_members: initial_members,
      membership: membership,
      machine: {:module, Ferricstore.Raft.StateMachine, machine_config},
      log_init_args: log_init_args_for_shard(shard_index),
      system: ra_sys,
      min_recovery_checkpoint_interval: 1
    }

    case profile_startup_phase(shard_index, :ra_start_server, fn ->
           :ra.start_server(ra_sys, server_config)
         end) do
      :ok ->
        maybe_trigger_and_wait(server_id, wait_for_leader?)

      {:error, reason} ->
        handle_start_server_error(
          ra_sys,
          server_id,
          server_config,
          shard_index,
          reason,
          wait_for_leader?
        )
    end
  end

  @doc false
  @spec wait_for_leader_on_start?(keyword()) :: boolean()
  def wait_for_leader_on_start?(opts) do
    Keyword.get(opts, :wait_for_leader, true)
  end

  @doc false
  @spec boot_initial_members(non_neg_integer(), :ra.server_id(), [node()]) :: [:ra.server_id()]
  def boot_initial_members(_shard_index, server_id, []), do: [server_id]

  def boot_initial_members(shard_index, server_id, cluster_nodes) when is_list(cluster_nodes) do
    if local_raft_node() in cluster_nodes do
      # Initial cluster bootstrap: every configured node starts with the same
      # full member set, so Ra can elect a quorum immediately.
      Enum.map(cluster_nodes, fn node ->
        shard_server_id_on(shard_index, node)
      end)
    else
      # Auto-join/rejoin bootstrap: the node was pointed at an existing
      # cluster, but it is not a member yet. Starting with remote initial
      # members here creates a dead Ra group that cannot elect and races with
      # the real join flow. Boot locally; Cluster.Manager will stop this local
      # group after the app is ready, sync data, and join the real group.
      [server_id]
    end
  end

  @doc false
  @spec log_init_args_for_shard(non_neg_integer()) :: map()
  def log_init_args_for_shard(shard_index) do
    %{
      uid: shard_uid(shard_index),
      # Release cursor is gated by Bitcask+LMDB durability; snapshots are only
      # a recovery/log compaction aid. Keep snapshots throttled so hot writes do
      # not pay for frequent Ra snapshot I/O.
      min_snapshot_interval: ra_min_snapshot_interval(),
      min_checkpoint_interval: ra_min_checkpoint_interval()
    }
  end

  defp ra_min_snapshot_interval do
    Application.get_env(:ferricstore, :ra_min_snapshot_interval, 10_000_000)
  end

  defp ra_min_checkpoint_interval do
    Application.get_env(:ferricstore, :ra_min_checkpoint_interval, 1_000_000)
  end

  defp ra_segment_max_entries do
    Application.get_env(:ferricstore, :ra_segment_max_entries, 1_048_576)
  end

  defp ra_segment_max_size_bytes do
    Application.get_env(:ferricstore, :ra_segment_max_size_bytes, 256_000_000)
  end

  defp ra_wal_max_size_bytes do
    Application.get_env(:ferricstore, :ra_wal_max_size_bytes, 8_589_934_592)
  end

  defp ra_wal_compute_checksums? do
    Application.get_env(:ferricstore, :ra_wal_compute_checksums, true)
  end

  @doc false
  @spec replay_skip_below_index(binary(), keyword()) :: non_neg_integer()
  def replay_skip_below_index(shard_data_path, opts \\ []) do
    max(
      Keyword.get(opts, :skip_below_index, 0),
      Ferricstore.Raft.ReplaySafeIndex.read(shard_data_path)
    )
  end

  @doc """
  Triggers and waits for all local shard Ra elections concurrently.

  Shard GenServers defer this work during application startup so recovery does
  not serialize replay/election across every shard. Readiness is still gated on
  this function, so clients do not see the node as ready until all shard leaders
  are available.
  """
  @spec trigger_shard_elections_parallel(non_neg_integer(), keyword()) :: :ok | {:error, term()}
  def trigger_shard_elections_parallel(shard_count, opts \\ [])

  def trigger_shard_elections_parallel(0, _opts), do: :ok

  def trigger_shard_elections_parallel(shard_count, opts)
      when is_integer(shard_count) and shard_count > 0 do
    if Backend.waraft?() do
      trigger_waraft_shard_elections_parallel(shard_count, opts)
    else
      trigger_legacy_shard_elections_parallel(shard_count, opts)
    end
  end

  defp trigger_legacy_shard_elections_parallel(shard_count, opts) do
    timeout = Keyword.get(opts, :timeout, 120_000)
    max_concurrency = Keyword.get(opts, :max_concurrency, shard_count)

    0..(shard_count - 1)
    |> Task.async_stream(
      fn shard_index ->
        server_id = shard_server_id(shard_index)

        case trigger_and_wait(server_id) do
          :ok -> :ok
          {:error, reason} -> {:error, {shard_index, reason}}
          other -> {:error, {shard_index, other}}
        end
      end,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok ->
        {:cont, :ok}

      {:ok, {:error, reason}}, :ok ->
        {:halt, {:error, reason}}

      {:exit, reason}, :ok ->
        {:halt, {:error, {:election_task_exit, reason}}}
    end)
  end

  @doc false
  @spec start_error_recovery_action(term()) ::
          :same_uid_restart | :existing_state_restart | :fail_closed
  def start_error_recovery_action({:already_started, _pid}) do
    :same_uid_restart
  end

  # `ra.start_server/2` may wrap an existing child pid in a supervisor
  # shutdown tuple. Treat it exactly like direct `already_started`; deleting
  # Ra state or switching UID here can orphan WAL entries for the real server.
  def start_error_recovery_action(
        {:shutdown, {:failed_to_start_child, _child_id, {:already_started, _pid}}}
      ) do
    :same_uid_restart
  end

  def start_error_recovery_action(:not_new) do
    :existing_state_restart
  end

  def start_error_recovery_action(_reason) do
    :fail_closed
  end

  defp maybe_trigger_and_wait(server_id, true), do: trigger_and_wait(server_id)
  defp maybe_trigger_and_wait(_server_id, false), do: :ok

  defp trigger_and_wait(server_id) do
    shard_index = shard_index_from_server_id(server_id)

    case profile_startup_phase(shard_index, :ra_trigger_election, fn ->
           :ra.trigger_election(server_id)
         end) do
      :ok ->
        profile_startup_phase(shard_index, :ra_wait_leader, fn ->
          wait_for_leader(server_id)
        end)

      {:error, _reason} = err ->
        err

      other ->
        {:error, other}
    end
  end

  defp handle_already_started(ra_sys, server_id, server_config, shard_index, wait_for_leader?) do
    Logger.info(
      "ra server for shard #{shard_index} already running, stopping and restarting with same UID"
    )

    _ = :ra.stop_server(ra_sys, server_id)
    Process.sleep(100)

    case profile_startup_phase(shard_index, :ra_start_server_after_stop, fn ->
           :ra.start_server(ra_sys, server_config)
         end) do
      :ok ->
        maybe_trigger_and_wait(server_id, wait_for_leader?)

      {:error, :not_new} ->
        restart_existing_server(ra_sys, server_id, shard_index, wait_for_leader?)

      {:error, retry_reason} = err ->
        Logger.error(
          "Failed to start ra server (after stop) for shard #{shard_index}: #{inspect(retry_reason)}"
        )

        err
    end
  end

  defp restart_existing_server(ra_sys, server_id, shard_index, wait_for_leader? \\ true) do
    case profile_startup_phase(shard_index, :ra_restart_server, fn ->
           :ra.restart_server(ra_sys, server_id)
         end) do
      :ok ->
        maybe_trigger_and_wait(server_id, wait_for_leader?)

      {:error, restart_reason} = err ->
        Logger.error(
          "Failed to restart ra server for shard #{shard_index}: #{inspect(restart_reason)}"
        )

        err
    end
  end

  defp handle_start_server_error(
         ra_sys,
         server_id,
         server_config,
         shard_index,
         reason,
         wait_for_leader?
       ) do
    case start_error_recovery_action(reason) do
      :same_uid_restart ->
        handle_already_started(ra_sys, server_id, server_config, shard_index, wait_for_leader?)

      :existing_state_restart ->
        restart_existing_server(ra_sys, server_id, shard_index, wait_for_leader?)

      :fail_closed ->
        Logger.error("Failed to start ra server for shard #{shard_index}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp handle_join_start_error_action(ra_sys, server_id, shard_index, reason) do
    handle_join_start_error_action(
      ra_sys,
      server_id,
      shard_index,
      reason,
      start_error_recovery_action(reason)
    )
  end

  defp trigger_waraft_shard_elections_parallel(shard_count, opts) do
    timeout = Keyword.get(opts, :timeout, 120_000)
    max_concurrency = Keyword.get(opts, :max_concurrency, shard_count)

    0..(shard_count - 1)
    |> Task.async_stream(
      fn shard_index ->
        with :ok <- normalize_waraft_election(safe_waraft_trigger_election(shard_index)),
             :ok <- wait_for_waraft_leader(shard_index) do
          :ok
        else
          {:error, reason} -> {:error, {shard_index, reason}}
          other -> {:error, {shard_index, other}}
        end
      end,
      max_concurrency: max_concurrency,
      ordered: false,
      timeout: timeout,
      on_timeout: :kill_task
    )
    |> Enum.reduce_while(:ok, fn
      {:ok, :ok}, :ok ->
        {:cont, :ok}

      {:ok, {:error, reason}}, :ok ->
        {:halt, {:error, reason}}

      {:exit, reason}, :ok ->
        {:halt, {:error, {:election_task_exit, reason}}}
    end)
  end

  defp safe_waraft_trigger_election(shard_index) do
    WARaftBackend.trigger_election(shard_index)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp normalize_waraft_election(:ok), do: :ok
  defp normalize_waraft_election({:error, _reason} = error), do: error
  defp normalize_waraft_election(other), do: {:error, other}

  defp wait_for_waraft_leader(shard_index, attempts \\ 200)
  defp wait_for_waraft_leader(_shard_index, 0), do: {:error, :leader_election_timeout}

  defp wait_for_waraft_leader(shard_index, attempts) do
    case blocking_waraft_members(shard_index) do
      {:ok, _members, {_name, _node}} ->
        :ok

      _other ->
        Process.sleep(50)
        wait_for_waraft_leader(shard_index, attempts - 1)
    end
  end

  defp handle_join_start_error_action(
         _ra_sys,
         _server_id,
         _shard_index,
         _reason,
         :same_uid_restart
       ) do
    :ok
  end

  defp handle_join_start_error_action(
         ra_sys,
         server_id,
         shard_index,
         _reason,
         :existing_state_restart
       ) do
    restart_existing_server(ra_sys, server_id, shard_index)
  end

  defp handle_join_start_error_action(_ra_sys, _server_id, shard_index, reason, :fail_closed) do
    Logger.error("Shard #{shard_index}: failed to join cluster: #{inspect(reason)}")
    {:error, reason}
  end

  @doc """
  Stops and deletes the ra server for a shard.

  Used during shard restarts and in test cleanup.

  ## Parameters

    * `shard_index` -- zero-based shard index
  """
  @spec stop_shard_server(non_neg_integer()) :: :ok | {:error, term()}
  def stop_shard_server(shard_index) do
    if Backend.waraft?() do
      {:error, :unsupported_waraft_shard_stop}
    else
      stop_legacy_shard_server(shard_index)
    end
  end

  defp stop_legacy_shard_server(shard_index) do
    server_id = shard_server_id(shard_index)

    case :ra.stop_server(@ra_system, server_id) do
      :ok -> :ok
      {:error, :noproc} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Returns the ra server ID for a shard.

  The server ID is a `{name, node()}` tuple as required by ra.

  ## Examples

      iex> Ferricstore.Raft.Cluster.shard_server_id(0)
      {:"ferricstore_shard_0", node()}
  """
  @spec shard_server_id(non_neg_integer()) :: :ra.server_id()
  def shard_server_id(shard_index) do
    {:"ferricstore_shard_#{shard_index}", local_raft_node()}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  @doc false
  def local_raft_node do
    Application.get_env(:ferricstore, :raft_local_node, node())
  end

  defp remember_local_raft_node! do
    # Ra server IDs include the Erlang node name. If a test or embedded host
    # starts distribution after FerricStore has booted, node() changes, but the
    # already-started Ra servers keep the original ID. Keep the boot identity
    # stable so membership APIs keep addressing the real local servers.
    Application.put_env(:ferricstore, :raft_local_node, node())
  end

  defp shard_uid(shard_index) do
    "ferricstore_shard_#{shard_index}"
  end

  defp shard_cluster_name(shard_index) do
    :"ferricstore_shard_cluster_#{shard_index}"
  end

  defp profile_startup_phase(shard_index, phase, fun) when is_function(fun, 0) do
    {duration_us, result} = :timer.tc(fun)

    :telemetry.execute(
      [:ferricstore, :shard, :startup_phase],
      %{duration_us: duration_us},
      %{shard_index: shard_index, phase: phase}
    )

    result
  end

  defp shard_index_from_server_id({name, _node}) when is_atom(name) do
    name
    |> Atom.to_string()
    |> String.trim_leading("ferricstore_shard_")
    |> String.to_integer()
  rescue
    _ -> -1
  end

  # Waits for the ra server to elect a leader. In single-node mode this
  # should happen almost immediately after triggering the election.
  defp wait_for_leader(server_id, attempts \\ 200)
  defp wait_for_leader(_server_id, 0), do: {:error, :leader_election_timeout}

  defp wait_for_leader(server_id, attempts) do
    case :ra.members(server_id) do
      {:ok, _members, leader} when leader not in [nil, :undefined] ->
        :ok

      {:ok, _members, _leader_not_ready} ->
        Process.sleep(50)
        wait_for_leader(server_id, attempts - 1)

      {:error, _} ->
        Process.sleep(50)
        wait_for_leader(server_id, attempts - 1)

      {:timeout, _} ->
        Process.sleep(50)
        wait_for_leader(server_id, attempts - 1)
    end
  end
end
