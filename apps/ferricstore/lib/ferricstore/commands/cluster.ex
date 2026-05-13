defmodule Ferricstore.Commands.Cluster do
  @moduledoc """
  Handles FerricStore cluster inspection and management commands.

  Provides per-shard health and statistics information by querying the ETS
  tables owned by each shard GenServer, plus cluster membership operations.

  ## Supported commands

  ### Inspection

    * `CLUSTER.HEALTH` -- returns per-shard status including role, health,
      key count, and memory usage
    * `CLUSTER.STATS` -- returns per-shard key/memory stats plus totals
    * `CLUSTER.KEYSLOT <key>` -- returns the hash slot for a key
    * `CLUSTER.SLOTS` -- returns slot range assignments
    * `CLUSTER.STATUS` -- returns detailed cluster info: nodes, per-shard
      leader/follower info, roles
    * `CLUSTER.ROLE` -- returns this node's configured cluster role

  ### Membership management

    * `CLUSTER.JOIN <node> [REPLACE]` -- adds a node to the cluster
    * `CLUSTER.LEAVE` -- gracefully removes this node from the cluster
    * `CLUSTER.FAILOVER <shard_index> <target_node>` -- transfers shard
      leadership to a specific node
    * `CLUSTER.PROMOTE <node>` -- promotes a replica to voter for all shards
    * `CLUSTER.DEMOTE <node>` -- demotes a voter to replica for all shards
  """

  alias Ferricstore.Cluster.Manager, as: ClusterManager
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Store.{Router, SlotMap}

  @doc """
  Handles a cluster command.

  ## Parameters

    * `cmd` - uppercased command name (e.g. `"CLUSTER.HEALTH"`)
    * `args` - list of string arguments
    * `_store` - injected store map (unused by cluster commands)

  ## Returns

  A list of bulk strings formatted as key-value pairs for RESP3 encoding.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CLUSTER.HEALTH", [], _store) do
    shard_infos = collect_shard_info()

    lines =
      Enum.flat_map(shard_infos, fn {index, info} ->
        role = shard_role(index)

        [
          "shard_#{index}:",
          "  role: #{role}",
          "  status: #{info.status}",
          "  keys: #{info.keys}",
          "  memory_bytes: #{info.memory_bytes}"
        ]
      end)

    Enum.join(lines, "\r\n")
  end

  def handle("CLUSTER.HEALTH", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.health' command"}
  end

  def handle("CLUSTER.STATS", [], _store) do
    shard_infos = collect_shard_info()

    total_keys = Enum.reduce(shard_infos, 0, fn {_idx, info}, acc -> acc + info.keys end)

    total_memory =
      Enum.reduce(shard_infos, 0, fn {_idx, info}, acc -> acc + info.memory_bytes end)

    shard_lines =
      Enum.flat_map(shard_infos, fn {index, info} ->
        [
          "shard_#{index}:",
          "  keys: #{info.keys}",
          "  memory_bytes: #{info.memory_bytes}"
        ]
      end)

    total_lines = [
      "total_keys: #{total_keys}",
      "total_memory_bytes: #{total_memory}"
    ]

    Enum.join(shard_lines ++ total_lines, "\r\n")
  end

  def handle("CLUSTER.STATS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.stats' command"}
  end

  def handle("CLUSTER.KEYSLOT", [key], _store) do
    SlotMap.slot_for_key(key)
  end

  def handle("CLUSTER.KEYSLOT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.keyslot' command"}
  end

  def handle("CLUSTER.SLOTS", [], _store) do
    slot_map = current_slot_map()
    ranges = SlotMap.slot_ranges(slot_map)

    Enum.map(ranges, fn {start_slot, end_slot, shard_index} ->
      [start_slot, end_slot, shard_index]
    end)
  end

  def handle("CLUSTER.SLOTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.slots' command"}
  end

  # -- CLUSTER.STATUS ---------------------------------------------------------

  def handle("CLUSTER.STATUS", [], _store) do
    status = ClusterManager.node_status()

    header = [
      "mode: #{status.mode}",
      "replication_mode: #{Ferricstore.ReplicationMode.current()}",
      "cluster_state: #{cluster_state_summary()}",
      "role: #{status.role}",
      "node: #{status.node}",
      "sync_status: #{status.sync_status}",
      "connected_nodes: #{Enum.map_join(status.connected_nodes, ", ", &Atom.to_string/1)}"
    ]

    shard_lines =
      status.shards
      |> Enum.sort_by(fn {idx, _} -> idx end)
      |> Enum.flat_map(fn
        {idx, %{error: reason}} ->
          ["shard_#{idx}:", "  error: #{inspect(reason)}"]

        {idx, %{members: members, leader: leader}} ->
          leader_node =
            case leader do
              {_name, n} -> Atom.to_string(n)
              _ -> "unknown"
            end

          member_strs =
            Enum.map(members, fn
              {_name, n} -> Atom.to_string(n)
              other -> inspect(other)
            end)

          [
            "shard_#{idx}:",
            "  leader: #{leader_node}",
            "  members: #{Enum.join(member_strs, ", ")}"
          ]
      end)

    Enum.join(header ++ shard_lines, "\r\n")
  end

  def handle("CLUSTER.STATUS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.status' command"}
  end

  # -- CLUSTER.JOIN -----------------------------------------------------------

  def handle("CLUSTER.JOIN", [node_str], _store) do
    with {:ok, node} <- parse_existing_node(node_str) do
      case ClusterManager.add_node(node) do
        :ok -> :ok
        {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
      end
    end
  end

  def handle("CLUSTER.JOIN", [node_str, arg], _store) when is_binary(arg) do
    case String.upcase(arg) do
      "REPLACE" ->
        with {:ok, node} <- parse_existing_node(node_str) do
          case ClusterManager.add_node(node, :voter, replace: true) do
            :ok -> :ok
            {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
          end
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  def handle("CLUSTER.JOIN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.join' command"}
  end

  # -- CLUSTER.LEAVE ----------------------------------------------------------

  def handle("CLUSTER.LEAVE", [], _store) do
    case ClusterManager.leave() do
      :ok -> :ok
      {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
    end
  end

  def handle("CLUSTER.LEAVE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.leave' command"}
  end

  # -- CLUSTER.FAILOVER -------------------------------------------------------

  def handle("CLUSTER.FAILOVER", [shard_str, node_str], _store) do
    with {shard_idx, ""} <- Integer.parse(shard_str),
         {:ok, target} <- parse_existing_node(node_str) do
      case RaftCluster.transfer_leadership(shard_idx, target) do
        :ok -> :ok
        {:error, reason} -> {:error, "ERR #{inspect(reason)}"}
      end
    else
      _ -> {:error, "ERR shard index must be an integer"}
    end
  end

  def handle("CLUSTER.FAILOVER", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.failover' command"}
  end

  # -- CLUSTER.PROMOTE --------------------------------------------------------

  def handle("CLUSTER.PROMOTE", [node_str], _store) do
    with {:ok, target} <- parse_existing_node(node_str) do
      shard_count = FerricStore.Instance.get(:default).shard_count

      results =
        for shard_idx <- 0..(shard_count - 1) do
          RaftCluster.add_member(shard_idx, target, :voter)
        end

      if Enum.all?(results, &(&1 == :ok)) do
        :ok
      else
        {:error, "ERR partial failure: #{inspect(results)}"}
      end
    end
  end

  def handle("CLUSTER.PROMOTE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.promote' command"}
  end

  # -- CLUSTER.DEMOTE ---------------------------------------------------------

  def handle("CLUSTER.DEMOTE", [node_str], _store) do
    with {:ok, target} <- parse_existing_node(node_str) do
      shard_count = FerricStore.Instance.get(:default).shard_count

      results =
        for shard_idx <- 0..(shard_count - 1) do
          RaftCluster.add_member(shard_idx, target, :promotable)
        end

      if Enum.all?(results, &(&1 == :ok)) do
        :ok
      else
        {:error, "ERR partial failure: #{inspect(results)}"}
      end
    end
  end

  def handle("CLUSTER.DEMOTE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.demote' command"}
  end

  # -- CLUSTER.ROLE -----------------------------------------------------------

  def handle("CLUSTER.ROLE", [], _store) do
    role =
      case ClusterManager.mode() do
        :standalone -> "standalone"
        :cluster -> Atom.to_string(Application.get_env(:ferricstore, :cluster_role, :voter))
      end

    role
  end

  def handle("CLUSTER.ROLE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'cluster.role' command"}
  end

  # FERRICSTORE.HOTNESS — returns per-prefix hot/cold read statistics.
  # Accepts optional TOP n and WINDOW seconds arguments.
  # Response is a flat list of key-value string pairs:
  #   ["hot_reads", "1200", "cold_reads", "45", ..., "prefix", "user", ...]
  def handle("FERRICSTORE.HOTNESS", args, _store) do
    alias Ferricstore.Stats

    {top_n, _window} = parse_hotness_args(args)
    entries = Stats.hotness_top(top_n)

    header = [
      "hot_reads",
      Integer.to_string(Stats.total_hot_reads()),
      "cold_reads",
      Integer.to_string(Stats.total_cold_reads()),
      "hot_read_pct",
      format_pct(Stats.hot_read_pct()),
      "cold_reads_per_second",
      format_pct(Stats.cold_reads_per_second()),
      "top_n",
      Integer.to_string(top_n)
    ]

    prefix_entries =
      Enum.flat_map(entries, fn {prefix, hot, cold, cold_pct} ->
        [
          "prefix",
          prefix,
          "hot",
          Integer.to_string(hot),
          "cold",
          Integer.to_string(cold),
          "cold_pct",
          format_pct(cold_pct)
        ]
      end)

    header ++ prefix_entries
  end

  defp parse_hotness_args(args) do
    top_n = parse_top_n(args, 10)
    window = parse_window(args, 0)
    {top_n, window}
  end

  defp parse_top_n([], default), do: default

  defp parse_top_n([opt, n_str | rest], default) do
    if String.upcase(opt) == "TOP" do
      case Integer.parse(n_str) do
        {n, ""} when n > 0 -> n
        _ -> default
      end
    else
      parse_top_n([n_str | rest], default)
    end
  end

  defp parse_top_n([_ | rest], default), do: parse_top_n(rest, default)

  defp parse_window([], default), do: default

  defp parse_window([opt, s_str | rest], default) do
    if String.upcase(opt) == "WINDOW" do
      case Integer.parse(s_str) do
        {s, ""} when s > 0 -> s
        _ -> default
      end
    else
      parse_window([s_str | rest], default)
    end
  end

  defp parse_window([_ | rest], default), do: parse_window(rest, default)

  defp format_pct(val) when is_float(val) do
    :erlang.float_to_binary(val, [{:decimals, 2}])
  end

  # -------------------------------------------------------------------
  # Private
  # -------------------------------------------------------------------

  defp cluster_state_summary do
    data_dir = FerricStore.Instance.get(:default).data_dir

    case Ferricstore.ReplicationMode.read(data_dir) do
      {:ok, state} ->
        mode = Map.get(state, :replication_mode, :unknown)
        cluster_id = Map.get(state, :cluster_id, "unknown")
        epoch = Map.get(state, :promotion_epoch, "none")
        barriers = Map.get(state, :barrier_indices, %{})

        "mode=#{mode} cluster_id=#{cluster_id} promotion_epoch=#{epoch} barrier_indices=#{inspect(barriers)}"

      {:error, :enoent} ->
        "missing promotion_epoch=none barrier_indices=%{}"

      {:error, reason} ->
        "unreadable #{inspect(reason)} promotion_epoch=unknown barrier_indices=unknown"
    end
  rescue
    error ->
      "unavailable #{Exception.message(error)} promotion_epoch=unknown barrier_indices=unknown"
  end

  defp collect_shard_info do
    ctx = default_instance()
    shard_count = if ctx, do: ctx.shard_count, else: configured_shard_count()

    Enum.map(0..(shard_count - 1), fn index ->
      keydir = :"keydir_#{index}"
      name = if ctx, do: Router.shard_name(ctx, index), else: nil

      info =
        try do
          keys = ets_count(keydir)
          keydir_words = ets_memory(keydir)
          word_size = :erlang.system_info(:wordsize)
          memory_bytes = keydir_words * word_size

          status =
            case Process.whereis(name) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
              nil -> "down"
            end

          %{keys: keys, memory_bytes: memory_bytes, status: status}
        rescue
          ArgumentError ->
            %{keys: 0, memory_bytes: 0, status: "down"}
        end

      {index, info}
    end)
  end

  defp default_instance do
    FerricStore.Instance.get(:default)
  rescue
    ArgumentError -> nil
  end

  defp current_slot_map do
    SlotMap.get()
  rescue
    ArgumentError -> SlotMap.build_uniform(configured_shard_count())
  end

  defp configured_shard_count do
    :persistent_term.get(
      :ferricstore_shard_count,
      Application.get_env(:ferricstore, :shard_count, 4)
    )
  end

  defp ets_count(table) do
    case :ets.info(table, :size) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  defp ets_memory(table) do
    case :ets.info(table, :memory) do
      n when is_integer(n) -> n
      _ -> 0
    end
  end

  defp parse_existing_node(node_str) when is_binary(node_str) do
    {:ok, String.to_existing_atom(node_str)}
  rescue
    ArgumentError ->
      {:error, "ERR unknown node; connect the distributed node before using CLUSTER commands"}
  end

  # Returns "leader" or "follower" for the given shard on this node.
  defp shard_role(index) do
    shard_id = Ferricstore.Raft.Cluster.shard_server_id(index)

    case :ra.members(shard_id) do
      {:ok, _members, ^shard_id} -> "leader"
      {:ok, _members, _other} -> "follower"
      _ -> "unknown"
    end
  rescue
    _ -> "unknown"
  catch
    _, _ -> "unknown"
  end
end
