defmodule Ferricstore.Test.ClusterHelper do
  @moduledoc """
  Helpers for spinning up multi-node FerricStore clusters in ExUnit.

  Uses `:peer` (OTP 25+) for in-process BEAM nodes. Each node gets its own
  temporary directory for Bitcask data and runs the full FerricStore
  application including WARaft state machines, ETS tables, and the Bitcask NIF.

  ## Architecture

  Nodes in a cluster form real multi-node WARaft groups. Each shard's group
  includes all N nodes as `initial_members`, so writes go through quorum
  (e.g. 2-of-3) and are replicated to all members. Leader election, failover,
  and log replication are exercised for real.

  `start_cluster/2` orchestrates the startup in the correct order:
  1. Start all peer BEAM nodes
  2. Connect them via Erlang distribution
  3. Set `:cluster_nodes` config on every node (list of all node names)
  4. Start the FerricStore application on every node
  5. Trigger WARaft elections and wait for all shards to have a leader with
     full membership

  ## Single-node addition

  `start_node/1` starts a standalone peer that is NOT part of any initial
  cluster. It can be added later via `Ferricstore.Cluster.Manager.add_node/2`
  for testing dynamic membership changes and WARaft snapshot joins.

  ## Usage

      nodes = ClusterHelper.start_cluster(3)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

  ## OTP Requirement

  Requires OTP 25+ for the `:peer` module. On older OTP versions,
  `peer_available?/0` returns `false` and `start_cluster/2` raises.
  """

  require Logger

  @doc """
  Returns `true` if the `:peer` module is available (OTP 25+).

  Tests should call this at the top of `setup_all` and skip if it returns
  `false`, so that the test suite still compiles on older OTP versions.
  """
  @spec peer_available?() :: boolean()
  def peer_available? do
    Code.ensure_loaded?(:peer) and function_exported?(:peer, :start, 1)
  end

  @doc """
  Starts N FerricStore peer nodes forming a real multi-node WARaft cluster.

  Each node gets:
  - A unique BEAM node name (`ferric_<unique>_<i>@<host>`)
  - Its own temporary directory for Bitcask data
  - The same code path as the test runner
  - An individually started FerricStore application
  - Shared `cluster_nodes` config so all shards form N-member WARaft groups

  ## Parameters

    - `n` -- number of nodes to start (typically 3 or 5)
    - `opts` -- keyword options:
      - `:shards` -- number of shards per node (default: 4)
      - `:timeout` -- leader election timeout in ms (default: 15_000)

  ## Returns

  A list of node maps: `%{name: atom, peer: pid, data_dir: binary, index: integer}`
  """
  @spec start_cluster(pos_integer(), keyword()) :: [map()]
  def start_cluster(n, opts \\ []) do
    unless peer_available?() do
      raise "ClusterHelper requires OTP 25+ for :peer module"
    end

    shards = Keyword.get(opts, :shards, 4)
    timeout = Keyword.get(opts, :timeout, 30_000)
    unique = :erlang.unique_integer([:positive])

    # Ensure the host node is alive for Erlang distribution.
    ensure_distribution!()

    # Phase 1: Start all peer BEAM nodes (without starting the app yet)
    nodes =
      Enum.map(1..n, fn i ->
        node_suffix = "#{unique}_#{i}"
        name = :"ferric_#{node_suffix}"
        data_dir = fresh_generated_data_dir("ferricstore_cluster_#{node_suffix}")

        # Start the peer with the same code paths as the test runner.
        code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

        cookie = Atom.to_charlist(Node.get_cookie())

        {:ok, peer_pid, node_name} =
          :peer.start(%{
            name: name,
            args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
            wait_boot: 120_000
          })

        %{name: node_name, peer: peer_pid, data_dir: data_dir, index: i}
      end)

    node_names = Enum.map(nodes, & &1.name)
    Ferricstore.Test.ClusterHelper.Partition.normalize_cluster_cookies(nodes)

    # Phase 2: Connect all nodes to each other for Erlang distribution.
    # Must happen before app startup so that WARaft servers can communicate
    # during leader election.
    for n1 <- node_names, n2 <- node_names, n1 != n2 do
      :rpc.call(n1, Node, :connect, [n2])
    end

    :ok = ensure_nodes_reachable(node_names, timeout: timeout)

    # Phase 3: Configure all nodes with cluster_nodes and app env,
    # then start the FerricStore application.
    Enum.each(nodes, fn node ->
      configure_remote_node(node.name, node.data_dir, shards)

      # Set cluster_nodes so Raft.Cluster.start_shard_server uses all
      # nodes as initial_members for each shard's Raft group.
      # Static peers are already initial members. Dynamic join is exercised by
      # start_node/1 and must not race destructive target preparation here.
      :ok =
        :rpc.call(node.name, Application, :put_env, [
          :ferricstore,
          :cluster_nodes,
          node_names
        ])

      :ok =
        :rpc.call(node.name, Application, :put_env, [
          :ferricstore,
          :cluster_auto_join,
          false
        ])
    end)

    # Phase 4: Start FerricStore on all nodes CONCURRENTLY. This is critical
    # for multi-node WARaft: each node starts its consensus partitions during
    # application boot. Elections need quorum (e.g. 2 of 3), so if we
    # start sequentially, early nodes wait 500ms per shard for elections
    # that cannot succeed (peers not up yet). Starting concurrently ensures
    # consensus partitions on all nodes come up roughly simultaneously, enabling
    # quorum to be reached promptly.
    tasks =
      Enum.map(nodes, fn node ->
        Task.async(fn -> start_ferricstore_on_node(node.name) end)
      end)

    # Wait for all app starts to complete. Each takes ~2s (4 shards * 500ms
    # election wait). With concurrent start, this is ~2s total instead of 6s.
    Enum.each(tasks, fn task -> Task.await(task, 30_000) end)
    :ok = ensure_peer_mesh_reachable(node_names, timeout: timeout)

    # Re-trigger elections only for shards that don't have a leader yet.
    # Triggering on all nodes causes split-votes on slow CI machines.
    first_node = hd(nodes).name

    for shard <- 0..(shards - 1) do
      has_leader? =
        try do
          case members_on_node(first_node, shard, 2_000) do
            {:ok, _members, _leader} -> true
            _ -> false
          end
        catch
          _, _ -> false
        end

      unless has_leader? do
        :rpc.call(first_node, Ferricstore.Raft.WARaftBackend, :trigger_election, [shard])
      end
    end

    # Phase 5: Wait for all shards to have elected leaders with full
    # membership across the cluster.
    :ok = wait_for_cluster_ready(nodes, shards, timeout)
    :ok = ensure_peer_mesh_reachable(node_names, timeout: timeout)

    nodes
  end

  @doc """
  Starts a single FerricStore peer node, independent of any cluster.

  The node starts with no `cluster_nodes` config, so each shard forms a
  single-member Raft group. The returned node name can be passed to
  `Ferricstore.Cluster.Manager.add_node/2` on an existing cluster node
  to dynamically join it.

  ## Parameters

    - `opts` -- keyword options:
      - `:shards` -- number of shards (default: 4)

  ## Returns

  The Erlang node name atom (e.g. `:"ferric_12345@hostname"`).
  """
  @spec start_node(keyword()) :: atom()
  def start_node(opts \\ []) do
    unless peer_available?() do
      raise "ClusterHelper requires OTP 25+ for :peer module"
    end

    ensure_distribution!()

    shards = Keyword.get(opts, :shards, 4)
    unique = :erlang.unique_integer([:positive])
    name = :"ferric_solo_#{unique}"

    data_dir =
      if Keyword.has_key?(opts, :data_dir) do
        data_dir = Keyword.fetch!(opts, :data_dir)
        File.mkdir_p!(data_dir)
        data_dir
      else
        fresh_generated_data_dir("ferricstore_solo_#{unique}")
      end

    code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    {:ok, peer_pid, node_name} =
      :peer.start(%{
        name: name,
        args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
        wait_boot: 120_000
      })

    # Store peer_pid in process dictionary so stop_node can find it.
    # Also store in a named ETS table for cross-process access.
    ensure_solo_registry!()
    :ets.insert(:ferricstore_solo_peers, {node_name, peer_pid, data_dir})
    :persistent_term.put({__MODULE__, :solo_peer, node_name}, {peer_pid, data_dir})

    configure_remote_node(node_name, data_dir, shards)

    cluster_nodes = Keyword.get(opts, :cluster_nodes, [])

    if cluster_nodes != [] do
      :rpc.call(node_name, Application, :put_env, [:ferricstore, :cluster_nodes, cluster_nodes])
      :rpc.call(node_name, Application, :put_env, [:ferricstore, :cluster_auto_join, true])
    end

    cluster_role = Keyword.get(opts, :cluster_role)

    if cluster_role do
      :rpc.call(node_name, Application, :put_env, [:ferricstore, :cluster_role, cluster_role])
    end

    # Peers start with -connect_all false. Connect them to their configured
    # cluster before starting FerricStore so WARaft/data-sync startup paths do
    # not block waiting for peers they cannot reach yet.
    if cluster_nodes != [] do
      Enum.each(cluster_nodes, fn cn ->
        :rpc.call(node_name, Node, :connect, [cn])
      end)
    end

    start_ferricstore_on_node(node_name)

    # Wait for shards to be alive and accepting calls
    Enum.each(0..(shards - 1), fn i ->
      shard = :"Ferricstore.Store.Shard.#{i}"

      Enum.each(1..50, fn _ ->
        case :rpc.call(node_name, Process, :whereis, [shard]) do
          pid when is_pid(pid) -> :ok
          _ -> Process.sleep(50)
        end
      end)
    end)

    :ok = ensure_node_reachable(node_name, timeout: 10_000)

    node_name
  end

  @doc """
  Ensures the test runner has an Erlang distribution connection to a peer node.

  Cluster tests use `:erpc.call/5` for strict remote calls. `:erpc` does not
  hide transient host-to-peer disconnects, so destructive suites should call
  this before remote operations when previous tests may have torn down peers.
  """
  @spec ensure_node_reachable(atom(), keyword()) :: :ok | {:error, :noconnection}
  def ensure_node_reachable(node_name, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_ensure_node_reachable(node_name, deadline)
  end

  @spec ensure_nodes_reachable([atom()], keyword()) :: :ok | {:error, {atom(), :noconnection}}
  def ensure_nodes_reachable(node_names, opts \\ []) when is_list(node_names) do
    Enum.reduce_while(node_names, :ok, fn node_name, :ok ->
      case ensure_node_reachable(node_name, opts) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {node_name, reason}}}
      end
    end)
  end

  @doc """
  Stops a single node started by `start_node/1` and cleans up its data.
  """
  @spec stop_node(atom()) :: :ok
  def stop_node(node_name) do
    ensure_solo_registry!()

    case lookup_solo_peer(node_name) do
      {:ok, peer_pid, data_dir} ->
        :ets.delete(:ferricstore_solo_peers, node_name)
        :persistent_term.erase({__MODULE__, :solo_peer, node_name})

        try do
          :peer.stop(peer_pid)
        catch
          _, _ -> :ok
        end

        wait_peer_node_down(node_name)
        File.rm_rf(data_dir)
        :ok

      :error ->
        Logger.warning("stop_node: no registered peer for #{node_name}")
        :ok
    end
  end

  @doc """
  Stops all peer nodes and cleans up their data directories.

  Safe to call even if some nodes have already been stopped.
  """
  @spec stop_cluster([map()]) :: :ok
  def stop_cluster(nodes) do
    Enum.each(nodes, fn node ->
      try do
        :peer.stop(node.peer)
      catch
        _, _ -> :ok
      end

      wait_peer_node_down(node.name)
      File.rm_rf(node.data_dir)
    end)

    :ok
  end

  @doc """
  Kills a specific node by stopping its peer process.

  Returns the killed node and the remaining nodes list.

  ## Parameters

    - `nodes` -- list of node maps from `start_cluster/2`
    - `target` -- the node map to kill (or index into nodes list)
  """
  @spec kill_node([map()], map()) :: {map(), [map()]}
  def kill_node(nodes, target) when is_map(target) do
    remaining = Enum.reject(nodes, &(&1.name == target.name))

    try do
      :peer.stop(target.peer)
    catch
      _, _ -> :ok
    end

    for node <- remaining do
      :rpc.call(node.name, :erlang, :disconnect_node, [target.name])
    end

    wait_peer_node_down(target.name)

    remaining
    |> Enum.filter(fn node -> Process.alive?(node.peer) end)
    |> Enum.map(& &1.name)
    |> ensure_nodes_reachable(timeout: 15_000)
    |> case do
      :ok -> :ok
      {:error, _reason} -> :ok
    end

    {target, remaining}
  end

  @doc """
  Restarts a killed cluster node with the same node name and data directory.

  This models beta same-version crash/restart recovery. It intentionally does
  not try to exercise rolling upgrade behavior.

  ## Parameters

    - `nodes` -- desired full cluster list, including the killed node map
    - `target` -- killed node map returned from `kill_node/2`
    - `opts` -- keyword options:
      - `:shards` -- number of shards per node (default: 4)
      - `:timeout` -- leader election timeout in ms (default: 30_000)

  ## Returns

  The restarted node map with a fresh peer pid.
  """
  @spec restart_node([map()], map(), keyword()) :: map()
  def restart_node(nodes, target, opts \\ []) when is_list(nodes) and is_map(target) do
    unless peer_available?() do
      raise "ClusterHelper requires OTP 25+ for :peer module"
    end

    ensure_distribution!()

    shards = Keyword.get(opts, :shards, 4)
    timeout = Keyword.get(opts, :timeout, 30_000)
    node_names = Enum.map(nodes, & &1.name)
    name = node_short_name(target.name)

    code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)
    cookie = Atom.to_charlist(Node.get_cookie())

    {:ok, peer_pid, node_name} =
      :peer.start(%{
        name: name,
        args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
        wait_boot: 120_000
      })

    restarted = %{target | name: node_name, peer: peer_pid}

    live_nodes =
      nodes
      |> Enum.reject(&(&1.name == target.name))
      |> Enum.concat([restarted])

    Ferricstore.Test.ClusterHelper.Partition.normalize_cluster_cookies(live_nodes)

    for n1 <- node_names, n2 <- node_names, n1 != n2 do
      :rpc.call(n1, Node, :connect, [n2])
    end

    :ok = ensure_nodes_reachable(node_names, timeout: timeout)

    configure_remote_node(node_name, target.data_dir, shards)

    :ok =
      :rpc.call(node_name, Application, :put_env, [
        :ferricstore,
        :cluster_nodes,
        node_names
      ])

    :ok =
      :rpc.call(node_name, Application, :put_env, [
        :ferricstore,
        :cluster_auto_join,
        true
      ])

    start_ferricstore_on_node(node_name)

    :ok = ensure_peer_mesh_reachable(node_names, timeout: timeout)
    :ok = wait_for_cluster_ready(live_nodes, shards, timeout)

    restarted
  end

  @doc """
  Kills the leader node for a given shard.

  Finds which node is the leader for the specified shard and stops it.

  ## Parameters

    - `nodes` -- list of node maps from `start_cluster/2`
    - `shard` -- shard index (default: 0)

  ## Returns

  `{killed_node, remaining_nodes}`
  """
  @spec kill_leader([map()], non_neg_integer()) :: {map(), [map()]}
  def kill_leader(nodes, shard \\ 0) do
    leader_name = find_leader(nodes, shard)
    leader_node = Enum.find(nodes, &(&1.name == leader_name))
    kill_node(nodes, leader_node)
  end

  @doc """
  Finds the current Raft leader for a shard.

  Tries each node in order until one returns a successful membership result.
  result.

  ## Returns

  The node name atom of the current leader.
  """
  @spec find_leader([map()], non_neg_integer()) :: atom()
  def find_leader(nodes, shard \\ 0) do
    result =
      Enum.find_value(nodes, fn node ->
        case members_on_node(node.name, shard) do
          {:ok, _members, {_leader_name, leader_node}} ->
            leader_node

          _ ->
            nil
        end
      end)

    result || raise "Could not find leader for shard #{shard}"
  end

  @doc "Simulates a network partition by disconnecting a node from all others."
  @spec partition_node(map(), [map()]) :: :ok
  def partition_node(node, all_nodes),
    do: Ferricstore.Test.ClusterHelper.Partition.partition_node(node, all_nodes)

  @doc "Heals a network partition by reconnecting a node to the cluster."
  @spec heal_partition(map(), [map()]) :: :ok
  def heal_partition(node, all_nodes),
    do: Ferricstore.Test.ClusterHelper.Partition.heal_partition(node, all_nodes)

  @doc """
  Runs a function on a specific FerricStore node via RPC.

  ## Returns

  The result of the remote function call, or `{:badrpc, reason}` on failure.
  """
  @spec run(atom(), module(), atom(), [term()]) :: term()
  def run(node_name, module, function, args) do
    :rpc.call(node_name, module, function, args)
  end

  @doc """
  Stops the WARaft runtime on a peer node while leaving the FerricStore app up.

  Tests use this to model a node whose local consensus runtime is unavailable
  without reaching into removed consensus internals.
  """
  @spec stop_consensus(atom()) :: :ok | term()
  def stop_consensus(node_name) do
    Ferricstore.Test.ClusterHelper.Consensus.stop_consensus(node_name)
  end

  @doc """
  Starts the WARaft runtime on a peer node using that node's default instance.
  """
  @spec start_consensus(atom()) :: :ok | term()
  def start_consensus(node_name) do
    Ferricstore.Test.ClusterHelper.Consensus.start_consensus(node_name)
  end

  @doc """
  Waits until all shards have an elected leader on at least one of the given nodes.

  Polls every 100ms up to the configured timeout.

  ## Parameters

    - `nodes` -- list of node maps
    - `shards` -- number of shards (integer) or range
    - `opts` -- keyword options:
      - `:timeout` -- maximum wait time in ms (default: 5_000)
  """
  @spec wait_for_leaders([map()], pos_integer() | Range.t(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_leaders(nodes, shards, opts \\ [])

  def wait_for_leaders(nodes, shards, opts) when is_integer(shards) do
    Ferricstore.Test.ClusterHelper.Consensus.wait_for_leaders(nodes, shards, opts)
  end

  def wait_for_leaders(nodes, shard_range, opts) do
    Ferricstore.Test.ClusterHelper.Consensus.wait_for_leaders(nodes, shard_range, opts)
  end

  @doc """
  Waits for a node to have all its shards with elected leaders.

  Useful after restarting a single node.
  """
  @spec wait_for_node_leaders(atom(), pos_integer(), keyword()) ::
          :ok | {:error, :timeout_waiting_for_leaders}
  def wait_for_node_leaders(node_name, shards, opts \\ []) do
    Ferricstore.Test.ClusterHelper.Consensus.wait_for_node_leaders(node_name, shards, opts)
  end

  # ---------------------------------------------------------------------------
  # Private: start FerricStore on a remote node
  # ---------------------------------------------------------------------------

  defp start_ferricstore_on_node(node_name) do
    case :rpc.call(node_name, Application, :ensure_all_started, [:ferricstore], 120_000) do
      {:ok, _apps} ->
        :ok

      {:error, reason} ->
        raise "Failed to start FerricStore on #{node_name}: #{inspect(reason)}"

      {:badrpc, reason} ->
        diagnostics = start_failure_diagnostics(node_name)

        raise "RPC to #{node_name} failed while starting FerricStore: #{inspect(reason)}; " <>
                "diagnostics=#{inspect(diagnostics)}"
    end
  end

  defp start_failure_diagnostics(node_name) do
    %{
      connected: Node.ping(node_name),
      applications: safe_rpc(node_name, Application, :started_applications, []),
      ferricstore_env: safe_rpc(node_name, Application, :get_all_env, [:ferricstore])
    }
  end

  defp safe_rpc(node_name, module, function, args) do
    case :rpc.call(node_name, module, function, args, 5_000) do
      {:badrpc, reason} -> {:badrpc, reason}
      other -> other
    end
  end

  # ---------------------------------------------------------------------------
  # Private: wait for cluster readiness (multi-node Raft)
  # ---------------------------------------------------------------------------

  # Waits for all shards to have a leader and the expected number of members.
  # In a multi-node cluster, each shard's Raft group should eventually contain
  # all N nodes as members. We poll until either:
  # - All shards report a leader from any node, OR
  # - The timeout is reached
  defp wait_for_cluster_ready(nodes, shards, timeout) do
    shard_range = 0..(shards - 1)
    expected_members = length(nodes)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_cluster_ready(nodes, shard_range, expected_members, deadline)
  end

  defp do_wait_cluster_ready(nodes, shard_range, expected_members, deadline) do
    all_ready =
      Enum.all?(shard_range, fn shard ->
        Enum.any?(nodes, fn node ->
          case members_on_node(node.name, shard) do
            {:ok, members, _leader} when length(members) == expected_members ->
              true

            {:ok, _members, _leader} ->
              # Leader exists but not all members joined yet
              false

            _ ->
              false
          end
        end)
      end)

    cond do
      all_ready ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        # Fall back to basic leader check — even if not all members are
        # visible yet, having a leader means the cluster is functional.
        # This handles the case where membership reports fewer members during
        # initial convergence. Give 15s extra for CI.
        do_wait_leaders(nodes, shard_range, deadline + 15_000)

      true ->
        Process.sleep(100)
        do_wait_cluster_ready(nodes, shard_range, expected_members, deadline)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: leader waiting loops
  # ---------------------------------------------------------------------------

  defp do_wait_leaders(nodes, shard_range, deadline) do
    alive_names = MapSet.new(nodes, & &1.name)

    all_have_leaders =
      Enum.all?(shard_range, fn shard ->
        Enum.any?(nodes, fn node ->
          case members_on_node(node.name, shard) do
            {:ok, _members, {_shard_name, leader_node}} ->
              MapSet.member?(alive_names, leader_node)

            _ ->
              false
          end
        end)
      end)

    cond do
      all_have_leaders ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        {:error, :timeout_waiting_for_leaders}

      true ->
        Process.sleep(100)
        do_wait_leaders(nodes, shard_range, deadline)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: configure remote node
  # ---------------------------------------------------------------------------

  defp fresh_generated_data_dir(prefix) do
    suffix =
      :crypto.strong_rand_bytes(8)
      |> Base.url_encode64(padding: false)

    data_dir = Path.join(System.tmp_dir!(), "#{prefix}_#{System.os_time(:microsecond)}_#{suffix}")

    File.rm_rf!(data_dir)
    File.mkdir_p!(data_dir)
    data_dir
  end

  defp configure_remote_node(node_name, data_dir, shards) do
    quiet_remote_logger(node_name)

    env_settings = [
      {:data_dir, data_dir},
      {:native_port, 0},
      {:health_port, 0},
      {:health_probe_port, 0},
      {:shard_count, shards},
      {:cluster_nodes, []},
      {:cluster_auto_join, false},
      {:memory_guard_interval_ms, 60_000},
      {:max_memory_bytes, 1_073_741_824},
      {:merge, [check_interval_ms: 600_000, fragmentation_threshold: 0.99]}
    ]

    Enum.each(env_settings, fn {key, value} ->
      :ok = :rpc.call(node_name, Application, :put_env, [:ferricstore, key, value])
    end)
  end

  defp quiet_remote_logger(node_name) do
    _ = :rpc.call(node_name, Application, :put_env, [:logger, :level, :warning])
    _ = :rpc.call(node_name, Logger, :configure, [[level: :warning]])
    :ok
  end

  # ---------------------------------------------------------------------------
  # Private: solo peer registry
  # ---------------------------------------------------------------------------

  # Ensures the ETS table for tracking solo peer nodes exists.
  # Used by start_node/stop_node to map node names back to peer PIDs.
  defp ensure_solo_registry! do
    case :ets.whereis(:ferricstore_solo_peers) do
      :undefined ->
        :ets.new(:ferricstore_solo_peers, [:named_table, :public, :set])

      _ref ->
        :ok
    end
  end

  defp lookup_solo_peer(node_name) do
    ets_result =
      try do
        :ets.lookup(:ferricstore_solo_peers, node_name)
      catch
        _, _ -> []
      end

    case ets_result do
      [{^node_name, peer_pid, data_dir}] ->
        {:ok, peer_pid, data_dir}

      [] ->
        case :persistent_term.get({__MODULE__, :solo_peer, node_name}, :undefined) do
          {peer_pid, data_dir} -> {:ok, peer_pid, data_dir}
          :undefined -> :error
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private: ensure Erlang distribution is started
  # ---------------------------------------------------------------------------

  defp ensure_distribution! do
    Ferricstore.Test.ShardHelpers.ensure_distribution_started!(:ferric_runner)
  end

  defp wait_peer_node_down(node_name, attempts \\ 50)

  defp wait_peer_node_down(_node_name, 0), do: :ok

  defp wait_peer_node_down(node_name, attempts) do
    Node.disconnect(node_name)

    case Node.ping(node_name) do
      :pang ->
        :ok

      :pong ->
        Process.sleep(100)
        wait_peer_node_down(node_name, attempts - 1)
    end
  end

  defp do_ensure_node_reachable(node_name, deadline) do
    case Node.ping(node_name) do
      :pong ->
        :ok

      :pang ->
        if System.monotonic_time(:millisecond) >= deadline do
          {:error, :noconnection}
        else
          Process.sleep(100)
          do_ensure_node_reachable(node_name, deadline)
        end
    end
  end

  defp ensure_peer_mesh_reachable(node_names, opts) do
    timeout = Keyword.get(opts, :timeout, 15_000)
    deadline = System.monotonic_time(:millisecond) + timeout
    do_ensure_peer_mesh_reachable(node_names, deadline)
  end

  defp do_ensure_peer_mesh_reachable(node_names, deadline) do
    all_connected? =
      Enum.all?(node_names, fn n1 ->
        Enum.all?(node_names, fn
          ^n1 ->
            true

          n2 ->
            :rpc.call(n1, Node, :connect, [n2])
            :rpc.call(n1, Node, :ping, [n2]) == :pong
        end)
      end)

    cond do
      all_connected? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:error, :peer_mesh_unreachable}

      true ->
        Process.sleep(100)
        do_ensure_peer_mesh_reachable(node_names, deadline)
    end
  end

  defp node_short_name(node_name) do
    node_name
    |> Atom.to_string()
    |> String.split("@", parts: 2)
    |> hd()
    |> String.to_atom()
  end

  defp members_on_node(node_name, shard, timeout \\ :default)

  defp members_on_node(node_name, shard, :default) do
    :rpc.call(node_name, Ferricstore.Raft.Cluster, :members, [shard])
  end

  defp members_on_node(node_name, shard, timeout) do
    :rpc.call(node_name, Ferricstore.Raft.Cluster, :members, [shard, timeout])
  end
end
