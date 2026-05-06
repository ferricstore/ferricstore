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
    ra_data_dir = Path.join(data_dir, "ra") |> to_charlist()
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
        Application.get_env(:ferricstore, :wal_commit_delay_us, 200)

      config = %{
        name: @ra_system,
        names: names,
        data_dir: ra_data_dir,
        wal_data_dir: ra_data_dir,
        segment_max_entries: 32_768,
        wal_max_batch_size: 32_768,
        wal_compute_checksums: true,
        wal_pre_allocate: true,
        wal_io_module: :ferricstore_wal_nif,
        wal_commit_delay_us: commit_delay_us
      }

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
  catch
    :exit, {:noproc, _} ->
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
    add_member_with_retry(shard_index, node, membership, 10)
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
    leader = shard_server_id(shard_index)
    member = shard_server_id_on(shard_index, node)

    case :ra.remove_member(leader, member) do
      {_, _, _leader} -> :ok
      {:error, :not_member} -> :ok
      {:error, reason} -> {:error, reason}
      {:timeout, _} -> {:error, :timeout}
    end
  end

  @doc """
  Returns the current members and leader for a shard's Raft group.
  """
  @spec members(non_neg_integer()) :: {:ok, list(), term()} | {:error, term()}
  def members(shard_index) do
    :ra.members(shard_server_id(shard_index))
  end

  @doc """
  Transfers leadership of a shard to a specific node.
  """
  @spec transfer_leadership(non_neg_integer(), node()) :: :ok | {:error, term()}
  def transfer_leadership(shard_index, target_node) do
    server_id = shard_server_id(shard_index)
    target_id = shard_server_id_on(shard_index, target_node)
    :ra.transfer_leadership(server_id, target_id)
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
      skip_below_index: skip_below_index,
      zset_score_index_name: Keyword.get(opts, :zset_score_index_name),
      zset_score_lookup_name: Keyword.get(opts, :zset_score_lookup_name),
      flow_index_name: Keyword.get(opts, :flow_index_name),
      flow_lookup_name: Keyword.get(opts, :flow_lookup_name)
    }

    # In cluster mode, initial_members includes all configured nodes.
    # In single-node mode (no cluster_nodes), just self.
    cluster_nodes = Application.get_env(:ferricstore, :cluster_nodes, [])

    initial_members =
      if cluster_nodes == [] do
        [server_id]
      else
        Enum.map(cluster_nodes, fn node ->
          shard_server_id_on(shard_index, node)
        end)
      end

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
    Application.get_env(:ferricstore, :ra_min_snapshot_interval, 200_000)
  end

  defp ra_min_checkpoint_interval do
    Application.get_env(:ferricstore, :ra_min_checkpoint_interval, 16_384)
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
    {:"ferricstore_shard_#{shard_index}", node()}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

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
      {:ok, _members, _leader} ->
        :ok

      {:error, _} ->
        Process.sleep(50)
        wait_for_leader(server_id, attempts - 1)

      {:timeout, _} ->
        Process.sleep(50)
        wait_for_leader(server_id, attempts - 1)
    end
  end
end
