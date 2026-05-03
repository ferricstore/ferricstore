defmodule FerricstoreServer.Health.Dashboard do
  @moduledoc """
  Self-contained HTML dashboard for FerricStore observability (spec 7.3).

  Renders a multi-page HTML dashboard with no external dependencies -- no
  Phoenix, no JavaScript frameworks, no CSS libraries. Pages auto-refresh
  via HTML `<meta http-equiv="refresh">` tags at page-appropriate intervals.

  ## Pages

  1. **Main dashboard** (`/dashboard`) -- top bar, cache perf, shards, memory,
     connections, and navigation links to sub-pages. Refreshes every 2s.
  2. **Slow Log** (`/dashboard/slowlog`) -- full slow log table. Refreshes 5s.
  3. **Merge Status** (`/dashboard/merge`) -- per-shard merge/compaction. Refreshes 10s.
  4. **Namespace Config** (`/dashboard/config`) -- namespace overrides. No refresh.
  5. **Raft Consensus** (`/dashboard/raft`) -- per-shard Raft health, leader,
     term, applied/commit index. The multi-node view. Refreshes 5s.
  6. **Client List** (`/dashboard/clients`) -- active client connections with
     IP, age, idle time. Refreshes 5s.

  ## Architecture

  This module is a pure function module with no process state. Each page has
  a `collect_*` function that gathers data and a `render_*` function that
  produces HTML. The endpoint routes to the appropriate pair.

  The dashboard is served by `FerricstoreServer.Health.Endpoint` at `GET /dashboard*`.
  Since it reuses the existing Ranch health listener (default port 9090), no
  additional ports or dependencies are required.
  """

  alias Ferricstore.{DataDir, Health, MemoryGuard, NamespaceConfig, SlowLog, Stats}
  alias Ferricstore.Merge.Scheduler, as: MergeScheduler
  alias Ferricstore.Raft.Cluster, as: RaftCluster

  @doc false
  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "Dashboard data map containing all sections."
  @type dashboard_data :: %{
          overview: overview_data(),
          shards: [shard_data()],
          hotcold: hotcold_data(),
          memory: memory_data(),
          connections: connections_data(),
          slowlog: [slowlog_entry()],
          merge: [merge_status()],
          namespace_config: [NamespaceConfig.ns_entry()],
          cluster: cluster_data()
        }

  @typedoc "Overview section data."
  @type overview_data :: %{
          status: :ok | :starting,
          uptime_seconds: non_neg_integer(),
          total_keys: non_neg_integer(),
          total_commands: non_neg_integer(),
          total_connections: non_neg_integer(),
          memory_bytes: non_neg_integer(),
          run_id: binary()
        }

  @typedoc "Per-shard status data."
  @type shard_data :: %{
          index: non_neg_integer(),
          status: String.t(),
          keys: non_neg_integer(),
          ets_memory_bytes: non_neg_integer()
        }

  @typedoc "Hot/cold read metrics."
  @type hotcold_data :: %{
          hot_read_pct: float(),
          cold_reads_per_sec: float(),
          total_hot: non_neg_integer(),
          total_cold: non_neg_integer(),
          top_prefixes: [Stats.hotness_entry()]
        }

  @typedoc "Memory pressure data."
  @type memory_data :: %{
          total_bytes: non_neg_integer(),
          max_bytes: non_neg_integer(),
          ratio: float(),
          pressure_level: MemoryGuard.pressure_level(),
          eviction_policy: atom(),
          shards: %{non_neg_integer() => %{bytes: non_neg_integer(), ratio: float()}}
        }

  @typedoc "Connection metrics."
  @type connections_data :: %{
          active: non_neg_integer(),
          blocked: non_neg_integer(),
          tracking: non_neg_integer()
        }

  @typedoc "A single slowlog entry for display."
  @type slowlog_entry :: %{
          id: non_neg_integer(),
          timestamp_us: integer(),
          duration_us: non_neg_integer(),
          command: [binary()]
        }

  @typedoc "Merge scheduler status for one shard."
  @type merge_status :: %{
          shard_index: non_neg_integer(),
          mode: atom(),
          merging: boolean(),
          last_merge_at: integer() | nil,
          merge_count: non_neg_integer(),
          total_bytes_reclaimed: non_neg_integer()
        }

  @typedoc "Cluster topology data."
  @type cluster_data :: %{
          node_name: atom(),
          cluster_mode: :standalone | :cluster,
          cluster_size: non_neg_integer(),
          nodes: [atom()]
        }

  @typedoc "Per-shard Raft consensus data."
  @type raft_shard_data :: %{
          shard: non_neg_integer(),
          status: :ok | :unavailable,
          leader: :ra.server_id() | nil,
          current_term: non_neg_integer(),
          commit_index: non_neg_integer(),
          last_applied: non_neg_integer(),
          log_size: non_neg_integer(),
          members: [:ra.server_id()]
        }

  @typedoc "Active client connection data."
  @type client_data :: %{
          pid: pid(),
          peer: String.t(),
          age_seconds: non_neg_integer(),
          flags: String.t()
        }

  # ---------------------------------------------------------------------------
  # Public API -- Main Dashboard
  # ---------------------------------------------------------------------------

  @doc """
  Collects all dashboard data from running subsystems.
  """
  @spec collect() :: dashboard_data()
  def collect do
    %{
      overview: collect_overview(),
      shards: collect_shards(),
      hotcold: collect_hotcold(),
      memory: collect_memory(),
      connections: collect_connections(),
      slowlog: collect_slowlog(),
      merge: collect_merge(),
      namespace_config: NamespaceConfig.get_all(),
      cluster: collect_cluster(),
      lifecycle: collect_lifecycle(),
      storage_summary: collect_storage_summary()
    }
  end

  @doc """
  Renders the main dashboard page as a complete HTML document.
  """
  @spec render(dashboard_data()) :: binary()
  def render(data) do
    page_head("FerricStore Dashboard", 2) <>
      ~s(<body>\n) <>
      render_top_bar(data) <>
      ~s(<div class="layout">\n) <>
      render_sidebar(data, "overview") <>
      ~s(<div class="main-content">\n<div class="content">) <>
      render_cache_performance(data.hotcold) <>
      render_lifecycle(data.lifecycle) <>
      render_shards(data.shards) <>
      render_memory_alert(data.memory) <>
      render_connections(data.connections) <>
      ~s(</div>\n</div>\n</div>\n) <>
      render_footer(data) <>
      ~s(</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Slow Log Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the slow log sub-page.
  """
  @spec collect_slowlog_page() :: %{slowlog: [slowlog_entry()]}
  def collect_slowlog_page do
    %{slowlog: collect_slowlog()}
  end

  @doc """
  Renders the slow log sub-page.
  """
  @spec render_slowlog_page(%{slowlog: [slowlog_entry()]}) :: binary()
  def render_slowlog_page(data) do
    page_head("Slow Log - FerricStore", 5) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("slowlog") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Slow Log") <>
      ~s(<div class="content">) <>
      render_slowlog_table(data.slowlog) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Merge Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the merge status sub-page.
  """
  @spec collect_merge_page() :: %{merge: [merge_status()]}
  def collect_merge_page do
    %{merge: collect_merge()}
  end

  @doc """
  Renders the merge status sub-page.
  """
  @spec render_merge_page(%{merge: [merge_status()]}) :: binary()
  def render_merge_page(data) do
    page_head("Merge Status - FerricStore", 10) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("merge") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Merge Status") <>
      ~s(<div class="content">) <>
      render_merge_table(data.merge) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Namespace Config Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the namespace config sub-page.
  """
  @spec collect_config_page() :: %{namespace_config: [NamespaceConfig.ns_entry()]}
  def collect_config_page do
    %{namespace_config: NamespaceConfig.get_all()}
  end

  @doc """
  Renders the namespace config sub-page (no auto-refresh).
  """
  @spec render_config_page(%{namespace_config: [NamespaceConfig.ns_entry()]}) :: binary()
  def render_config_page(data) do
    page_head("Namespace Config - FerricStore", nil) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("config") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Namespace Config") <>
      ~s(<div class="content">) <>
      render_config_table(data.namespace_config) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Raft Consensus Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the Raft consensus sub-page.
  """
  @spec collect_raft_page() :: %{raft_shards: [raft_shard_data()], cluster: cluster_data()}
  def collect_raft_page do
    %{
      raft_shards: collect_raft_shards(),
      cluster: collect_cluster()
    }
  end

  @doc """
  Renders the Raft consensus sub-page.
  """
  @spec render_raft_page(%{raft_shards: [raft_shard_data()], cluster: cluster_data()}) :: binary()
  def render_raft_page(data) do
    page_head("Raft Consensus - FerricStore", 5) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("raft") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Raft Consensus") <>
      ~s(<div class="content">) <>
      render_cluster_info(data.cluster) <>
      render_raft_table(data.raft_shards) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Client List Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the client list sub-page.
  """
  @spec collect_clients_page() :: %{clients: [client_data()], connections: connections_data()}
  def collect_clients_page do
    %{
      clients: collect_client_list(),
      connections: collect_connections()
    }
  end

  @doc """
  Renders the client list sub-page.
  """
  @spec render_clients_page(%{clients: [client_data()], connections: connections_data()}) :: binary()
  def render_clients_page(data) do
    page_head("Client List - FerricStore", 5) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("clients") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Client List") <>
      ~s(<div class="content">) <>
      render_clients_summary(data.connections) <>
      render_clients_table(data.clients) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Storage Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the storage sub-page.
  """
  @spec collect_storage_page() :: %{shards: [map()], total_disk_bytes: non_neg_integer(), total_files: non_neg_integer()}
  def collect_storage_page do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")

    shard_storage =
      Enum.map(0..(shard_count() - 1), fn index ->
        shard_dir = DataDir.shard_data_path(data_dir, index)
        {disk_bytes, data_files, hint_files} = scan_shard_dir(shard_dir)

        %{
          index: index,
          disk_bytes: disk_bytes,
          data_file_count: data_files,
          hint_file_count: hint_files
        }
      end)

    total_disk = Enum.reduce(shard_storage, 0, fn s, acc -> acc + s.disk_bytes end)
    total_files = Enum.reduce(shard_storage, 0, fn s, acc -> acc + s.data_file_count + s.hint_file_count end)

    %{shards: shard_storage, total_disk_bytes: total_disk, total_files: total_files}
  end

  @doc """
  Renders the storage sub-page.
  """
  @spec render_storage_page(map()) :: binary()
  def render_storage_page(data) do
    page_head("Storage - FerricStore", 10) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("storage") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Storage") <>
      ~s(<div class="content">) <>
      render_storage_summary(data) <>
      render_storage_table(data.shards) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Public API -- Prefixes Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the key prefixes sub-page.
  """
  @spec collect_prefixes_page() :: %{prefixes: [map()], total_sampled: non_neg_integer()}
  def collect_prefixes_page do
    # Get hotness data for read counts
    hotness = Stats.hotness_top(20)
    hotness_map = Map.new(hotness, fn {prefix, hot, cold, _pct} -> {prefix, {hot, cold}} end)

    # Sample keys from keydir ETS tables to count per-prefix key distribution
    {prefix_counts, total_sampled} = sample_prefix_counts()

    total_keys = Enum.reduce(prefix_counts, 0, fn {_prefix, count}, acc -> acc + count end)

    prefixes =
      prefix_counts
      |> Enum.map(fn {prefix, count} ->
        pct = if total_keys > 0, do: Float.round(count / total_keys * 100, 1), else: 0.0
        {hot, cold} = Map.get(hotness_map, prefix, {0, 0})

        %{
          prefix: prefix,
          keys: count,
          pct: pct,
          hot_reads: hot,
          cold_reads: cold
        }
      end)
      |> Enum.sort_by(fn p -> p.keys end, :desc)
      |> Enum.take(50)

    %{prefixes: prefixes, total_sampled: total_sampled}
  end

  @doc """
  Renders the key prefixes sub-page.
  """
  @spec render_prefixes_page(map()) :: binary()
  def render_prefixes_page(data) do
    page_head("Key Prefixes - FerricStore", 10) <>
      ~s(<body>\n) <>
      ~s(<div class="layout">\n) <>
      render_sidebar_static("prefixes") <>
      ~s(<div class="main-content">\n) <>
      render_subpage_header("Key Prefixes") <>
      ~s(<div class="content">) <>
      render_prefixes_table(data) <>
      ~s(</div>\n</div>\n</div>\n</body>\n</html>\n)
  end

  # ---------------------------------------------------------------------------
  # Data collection (private)
  # ---------------------------------------------------------------------------

  @spec collect_overview() :: overview_data()
  defp collect_overview do
    health = Health.check()

    total_keys =
      health.shards
      |> Enum.map(& &1.keys)
      |> Enum.sum()

    %{
      status: health.status,
      uptime_seconds: health.uptime_seconds,
      total_keys: total_keys,
      total_commands: Stats.total_commands(),
      total_connections: Stats.total_connections(),
      memory_bytes: :erlang.memory(:total),
      run_id: Stats.run_id()
    }
  end

  @spec collect_shards() :: [shard_data()]
  defp collect_shards do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")

    Enum.map(0..(shard_count() - 1), fn index ->
      keydir = :"keydir_#{index}"

      {status, keys, ets_mem} =
        try do
          keys = :ets.info(keydir, :size)
          keydir_words = :ets.info(keydir, :memory)

          mem_bytes =
            case keydir_words do
              n when is_integer(n) ->
                n * :erlang.system_info(:wordsize)

              _ ->
                0
            end

          ctx = FerricStore.Instance.get(:default)
          shard_name = Ferricstore.Store.Router.shard_name(ctx, index)

          shard_status =
            case Process.whereis(shard_name) do
              pid when is_pid(pid) -> if Process.alive?(pid), do: "ok", else: "down"
              nil -> "down"
            end

          {shard_status, keys, mem_bytes}
        rescue
          ArgumentError -> {"down", 0, 0}
        end

      shard_dir = DataDir.shard_data_path(data_dir, index)
      {disk_bytes, _, _} = scan_shard_dir(shard_dir)

      %{index: index, status: status, keys: keys, ets_memory_bytes: ets_mem, disk_bytes: disk_bytes}
    end)
  end

  @spec collect_hotcold() :: hotcold_data()
  defp collect_hotcold do
    rate = :persistent_term.get(:ferricstore_read_sample_rate, 100)
    _hits_sampled = Stats.keyspace_hits()
    misses = Stats.keyspace_misses()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()

    # hot_reads and keyspace_hits are sampled; cold_reads and misses are exact
    hot_est = hot_sampled * rate
    cold_exact = cold_sampled  # NOT sampled -- called on every cold read
    total_hits = hot_est + cold_exact
    total_lookups = total_hits + misses

    uptime = max(Stats.uptime_seconds(), 1)

    %{
      hot_read_pct: Stats.hot_read_pct(),
      cold_reads_per_sec: Stats.cold_reads_per_second(),
      total_hot: hot_est,
      total_cold: cold_exact,
      total_hits: total_hits,
      total_misses: misses,
      total_lookups: total_lookups,
      hit_ratio: if(total_lookups > 0, do: Float.round(total_hits / total_lookups * 100, 1), else: 0.0),
      ram_ratio: if(total_hits > 0, do: Float.round(hot_est / total_hits * 100, 1), else: 0.0),
      disk_ratio: if(total_hits > 0, do: Float.round(cold_exact / total_hits * 100, 1), else: 0.0),
      sample_rate: rate,
      hits_per_sec: Float.round(total_hits / uptime, 1),
      misses_per_sec: Float.round(misses / uptime, 1),
      ops_per_sec: Float.round(Stats.total_commands() / uptime, 1),
      top_prefixes: Stats.hotness_top(10)
    }
  end

  @spec collect_memory() :: memory_data()
  defp collect_memory do
    try do
      stats = MemoryGuard.stats()

      %{
        total_bytes: stats.total_bytes,
        max_bytes: stats.max_bytes,
        ratio: stats.ratio,
        pressure_level: stats.pressure_level,
        eviction_policy: stats.eviction_policy,
        shards: stats.shards
      }
    catch
      :exit, _ ->
        %{
          total_bytes: 0,
          max_bytes: 0,
          ratio: 0.0,
          pressure_level: :ok,
          eviction_policy: :volatile_lru,
          shards: %{}
        }
    end
  end

  @spec collect_connections() :: connections_data()
  defp collect_connections do
    %{
      active: Stats.active_connections(),
      blocked: safe_ets_size(:ferricstore_waiters),
      tracking: safe_ets_size(:ferricstore_tracking_connections)
    }
  end

  @spec collect_slowlog() :: [slowlog_entry()]
  defp collect_slowlog do
    try do
      SlowLog.get(128)
      |> Enum.map(fn {id, timestamp_us, duration_us, command} ->
        %{
          id: id,
          timestamp_us: timestamp_us,
          duration_us: duration_us,
          command: command
        }
      end)
    rescue
      _ -> []
    catch
      :exit, _ -> []
    end
  end

  @spec collect_merge() :: [merge_status()]
  defp collect_merge do
    Enum.map(0..(shard_count() - 1), fn index ->
      try do
        status = MergeScheduler.status(index)

        %{
          shard_index: status.shard_index,
          mode: status.mode,
          merging: status.merging,
          last_merge_at: status.last_merge_at,
          merge_count: status.merge_count,
          total_bytes_reclaimed: status.total_bytes_reclaimed
        }
      catch
        :exit, _ ->
          %{
            shard_index: index,
            mode: :unknown,
            merging: false,
            last_merge_at: nil,
            merge_count: 0,
            total_bytes_reclaimed: 0
          }
      end
    end)
  end

  @spec collect_cluster() :: cluster_data()
  defp collect_cluster do
    this_node = node()
    nodes = [Node.self() | Node.list()]
    size = length(nodes)

    %{
      node_name: this_node,
      cluster_mode: if(size > 1, do: :cluster, else: :standalone),
      cluster_size: size,
      nodes: nodes
    }
  end

  @spec collect_lifecycle() :: map()
  defp collect_lifecycle do
    mg_stats =
      try do
        MemoryGuard.stats()
      catch
        :exit, _ ->
          %{keydir_bytes: 0, keydir_max_ram: 0, keydir_ratio: 0.0}
      end

    keydir_full =
      try do
        MemoryGuard.keydir_full?()
      catch
        :exit, _ -> false
      end

    uptime = max(Stats.uptime_seconds(), 1)
    expired = Stats.expired_keys()
    evicted = Stats.evicted_keys()

    %{
      expired_total: expired,
      evicted_total: evicted,
      expired_per_sec: Float.round(expired / uptime, 1),
      evicted_per_sec: Float.round(evicted / uptime, 1),
      keydir_bytes: mg_stats.keydir_bytes,
      keydir_max_ram: mg_stats.keydir_max_ram,
      keydir_ratio: mg_stats.keydir_ratio,
      keydir_full: keydir_full
    }
  end

  @spec collect_storage_summary() :: %{total_disk_bytes: non_neg_integer()}
  defp collect_storage_summary do
    data_dir = Application.get_env(:ferricstore, :data_dir, "/tmp/ferricstore")

    total =
      Enum.reduce(0..(shard_count() - 1), 0, fn index, acc ->
        shard_dir = DataDir.shard_data_path(data_dir, index)
        {disk_bytes, _, _} = scan_shard_dir(shard_dir)
        acc + disk_bytes
      end)

    %{total_disk_bytes: total}
  end

  # Scans a shard directory for disk usage and file counts.
  # Returns {total_bytes, data_file_count, hint_file_count}.
  @spec scan_shard_dir(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  defp scan_shard_dir(shard_dir) do
    case Ferricstore.FS.ls(shard_dir) do
      {:ok, files} ->
        Enum.reduce(files, {0, 0, 0}, fn file, {bytes, data, hints} ->
          full_path = Path.join(shard_dir, file)

          file_size =
            case File.stat(full_path) do
              {:ok, %{size: size}} -> size
              _ -> 0
            end

          is_data = String.ends_with?(file, ".log")
          is_hint = String.ends_with?(file, ".hint")

          {
            bytes + file_size,
            if(is_data, do: data + 1, else: data),
            if(is_hint, do: hints + 1, else: hints)
          }
        end)

      {:error, _reason} ->
        {0, 0, 0}
    end
  end

  # Samples keys from keydir ETS tables to count per-prefix distribution.
  # Limited to 10,000 keys total to avoid expensive full scans.
  @spec sample_prefix_counts() :: {[{binary(), non_neg_integer()}], non_neg_integer()}
  defp sample_prefix_counts do
    max_sample = 10_000
    sc = shard_count()
    per_shard = div(max_sample, max(sc, 1))

    {counts_map, total} =
      Enum.reduce(0..(sc - 1), {%{}, 0}, fn i, {acc_map, acc_total} ->
        keydir = :"keydir_#{i}"

        try do
          {shard_map, shard_count_val} =
            :ets.foldl(
              fn {key, _val, _exp, _lfu, _fid, _off, _vsize}, {m, c} ->
                if c >= per_shard do
                  {m, c}
                else
                  prefix = Stats.extract_prefix(key)
                  {Map.update(m, prefix, 1, &(&1 + 1)), c + 1}
                end
              end,
              {%{}, 0},
              keydir
            )

          merged =
            Map.merge(acc_map, shard_map, fn _k, v1, v2 -> v1 + v2 end)

          {merged, acc_total + shard_count_val}
        rescue
          _ -> {acc_map, acc_total}
        catch
          :exit, _ -> {acc_map, acc_total}
        end
      end)

    {Enum.to_list(counts_map), total}
  end

  @spec collect_raft_shards() :: [raft_shard_data()]
  defp collect_raft_shards do
    Enum.map(0..(shard_count() - 1), fn i ->
      server_id = RaftCluster.shard_server_id(i)

      try do
        case :ra.member_overview(server_id) do
          {:ok, overview} when is_map(overview) ->
            extract_raft_overview(i, server_id, overview)

          {:ok, overview, _leader} when is_map(overview) ->
            extract_raft_overview(i, server_id, overview)

          {:error, _} ->
            %{shard: i, status: :unavailable, leader: nil, current_term: 0,
              commit_index: 0, last_applied: 0, log_size: 0, members: []}

          {:timeout, _} ->
            %{shard: i, status: :unavailable, leader: nil, current_term: 0,
              commit_index: 0, last_applied: 0, log_size: 0, members: []}
        end
      catch
        :exit, _ ->
          %{shard: i, status: :unavailable, leader: nil, current_term: 0,
            commit_index: 0, last_applied: 0, log_size: 0, members: []}
      end
    end)
  end

  defp extract_raft_overview(i, server_id, overview) do
    %{
      shard: i,
      status: :ok,
      leader: Map.get(overview, :leader, Map.get(overview, :id, server_id)),
      current_term: Map.get(overview, :current_term, 0),
      commit_index: Map.get(overview, :commit_index, 0),
      last_applied: Map.get(overview, :last_applied, 0),
      log_size: Map.get(overview, :log_size, 0),
      members: Map.get(overview, :members, [server_id])
    }
  end

  @spec collect_client_list() :: [client_data()]
  defp collect_client_list do
    try do
      pids = :ranch.procs(FerricstoreServer.Listener, :connections)
      now = System.monotonic_time(:millisecond)

      Enum.map(pids, fn pid ->
        info = Process.info(pid, [:dictionary, :current_function])

        {peer, age, flags} =
          case info do
            nil ->
              {"unknown:0", 0, ""}

            kw ->
              dict = Keyword.get(kw, :dictionary, [])
              state = Keyword.get(dict, :"$conn_state", nil)

              peer_str =
                case state do
                  %{peer: {ip, port}} ->
                    ip_str = :inet.ntoa(ip) |> to_string()
                    "#{ip_str}:#{port}"
                  _ ->
                    "unknown:0"
                end

              created =
                case state do
                  %{created_at: ts} when is_integer(ts) -> ts
                  _ -> now
                end

              age_s = max(0, div(now - created, 1000))

              flag_list =
                []
                |> then(fn f -> if state && Map.get(state, :multi_state) == :queuing, do: ["M" | f], else: f end)
                |> then(fn f -> if state && Map.get(state, :pubsub_channels), do: ["S" | f], else: f end)
                |> then(fn f -> if state && Map.get(state, :tracking) && Map.get(state.tracking, :enabled), do: ["T" | f], else: f end)

              {peer_str, age_s, Enum.join(flag_list)}
          end

        %{pid: pid, peer: peer, age_seconds: age, flags: flags}
      end)
    catch
      _, _ -> []
    end
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Main Dashboard
  # ---------------------------------------------------------------------------

  @spec render_top_bar(dashboard_data()) :: binary()
  defp render_top_bar(data) do
    overview = data.overview
    hotcold = data.hotcold
    memory = data.memory
    conns = data.connections
    cluster = data.cluster

    # Status dot color
    {dot_class, status_text} =
      cond do
        overview.status != :ok -> {"dot-red", "degraded"}
        memory.pressure_level == :reject -> {"dot-red", "rejecting"}
        memory.pressure_level == :pressure -> {"dot-yellow", "pressure"}
        memory.pressure_level == :warning -> {"dot-yellow", "warning"}
        true -> {"dot-green", "healthy"}
      end

    # Hit rate color
    hit_color = hit_rate_color(hotcold.hit_ratio)

    # Memory bar
    mem_pct = if memory.max_bytes > 0, do: Float.round(memory.ratio * 100, 1), else: 0.0
    mem_bar_color = mem_bar_color(mem_pct)
    mem_bar_width = min(mem_pct, 100)

    # Cluster info
    cluster_label =
      case cluster.cluster_mode do
        :standalone -> "standalone"
        :cluster -> "#{cluster.cluster_size}-node cluster"
      end

    node_short = cluster.node_name |> Atom.to_string()

    """
    <div class="top-bar">
      <div class="logo"><span class="status-dot #{dot_class}"></span>FerricStore</div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Node</span>
        <span class="val" style="font-size:0.75rem;">#{escape(node_short)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Cluster</span>
        <span class="val" style="font-size:0.85rem;">#{escape(cluster_label)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Status</span>
        <span class="val" style="font-size:0.85rem;">#{escape(status_text)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Ops/sec</span>
        <span class="val">#{format_rate(hotcold.ops_per_sec)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Hit Rate #{sampled_tag(hotcold.sample_rate)}</span>
        <span class="val" style="color:#{hit_color};">#{hotcold.hit_ratio}%</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Memory</span>
        <span class="val" style="font-size:0.85rem;">#{format_bytes(memory.total_bytes)} / #{format_bytes(memory.max_bytes)}</span>
        <div class="mem-bar-wrap"><div class="mem-bar-fill" style="width:#{mem_bar_width}%;background:#{mem_bar_color};"></div></div>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Connections</span>
        <span class="val">#{format_number(conns.active)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Keys</span>
        <span class="val">#{format_number(overview.total_keys)}</span>
      </div>
    </div>
    """
  end

  @spec render_cache_performance(hotcold_data()) :: binary()
  defp render_cache_performance(data) do
    hit_color = hit_rate_color(data.hit_ratio)

    # RAM bar color -- always green (fast path)
    # Disk bar color -- orange (slow path)
    ram_bar_width = min(data.ram_ratio, 100)
    disk_bar_width = min(data.disk_ratio, 100)

    """
    <div class="section-title">Cache Performance</div>
    <div class="cache-hero">
      <div class="hit-rate-card">
        <div class="hit-rate-num" style="color:#{hit_color};">#{data.hit_ratio}%</div>
        <div class="hit-rate-label">Hit Rate #{sampled_tag(data.sample_rate)}</div>
        <div class="hit-rate-sub">
          <span>#{format_rate(data.hits_per_sec)}</span> hits/sec #{sampled_tag(data.sample_rate)} &middot;
          <span>#{format_rate(data.misses_per_sec)}</span> misses/sec
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Where hits come from</div>
        <div class="source-row">
          <div>
            <div class="source-name">RAM #{sampled_tag(data.sample_rate)} <span class="info-icon" title="Served from ETS in-memory cache. Estimated from 1:#{data.sample_rate} sampling. Latency: ~1-5us.">i</span></div>
            <div class="source-detail">fast path (~1-5us)</div>
          </div>
          <div class="source-pct c-green">#{data.ram_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{ram_bar_width}%;background:#3fb950;"></div></div>
        <div class="source-row">
          <div>
            <div class="source-name">Disk <span class="info-icon" title="Required Bitcask disk read. This is an exact count (not sampled). Latency: ~50-200us. High disk ratio means memory pressure is evicting hot keys.">i</span></div>
            <div class="source-detail">slow path (~50-200us) &middot; exact</div>
          </div>
          <div class="source-pct c-yellow">#{data.disk_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{disk_bar_width}%;background:#d29922;"></div></div>
      </div>
    </div>
    """
  end

  @spec render_lifecycle(map()) :: binary()
  defp render_lifecycle(data) do
    # Evicted card color
    evicted_color =
      cond do
        data.evicted_per_sec > 100 -> "c-red"
        data.evicted_total > 0 -> "c-yellow"
        true -> ""
      end

    # Keydir capacity bar color and percentage
    keydir_pct = if data.keydir_max_ram > 0, do: Float.round(data.keydir_ratio * 100, 1), else: 0.0
    keydir_bar_width = min(keydir_pct, 100)

    keydir_bar_color =
      cond do
        keydir_pct > 90 -> "#f85149"
        keydir_pct > 70 -> "#d29922"
        true -> "#3fb950"
      end

    keydir_pct_class =
      cond do
        keydir_pct > 90 -> "c-red"
        keydir_pct > 70 -> "c-yellow"
        true -> "c-green"
      end

    keydir_full_alert =
      if data.keydir_full do
        """
        <div style="background:#8b1a1a; border:2px solid #f85149; border-radius:8px; padding:12px 16px; margin-bottom:16px; color:#f85149; font-weight:700; font-size:0.85rem;">
          KEYDIR FULL &mdash; new writes are being rejected. Increase max_memory or evict keys.
        </div>
        """
      else
        ""
      end

    """
    <div class="section-title">Key Lifecycle</div>
    #{keydir_full_alert}<div class="cache-hero">
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Expired</div>
        <div class="source-row">
          <div>
            <div class="source-name">Total</div>
          </div>
          <div class="source-pct">#{format_number(data.expired_total)}</div>
        </div>
        <div class="source-row">
          <div>
            <div class="source-name">Rate</div>
          </div>
          <div class="source-pct">#{format_rate(data.expired_per_sec)}/sec</div>
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Evicted</div>
        <div class="source-row">
          <div>
            <div class="source-name">Total</div>
          </div>
          <div class="source-pct #{evicted_color}">#{format_number(data.evicted_total)}</div>
        </div>
        <div class="source-row">
          <div>
            <div class="source-name">Rate</div>
          </div>
          <div class="source-pct #{evicted_color}">#{format_rate(data.evicted_per_sec)}/sec</div>
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Keydir Capacity</div>
        <div class="source-row">
          <div>
            <div class="source-name">#{format_bytes(data.keydir_bytes)} / #{format_bytes(data.keydir_max_ram)}</div>
          </div>
          <div class="source-pct #{keydir_pct_class}">#{keydir_pct}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{keydir_bar_width}%;background:#{keydir_bar_color};"></div></div>
      </div>
    </div>
    """
  end

  @spec render_shards([shard_data()]) :: binary()
  defp render_shards(shards) do
    all_ok = Enum.all?(shards, fn s -> s.status == "ok" end)

    rows =
      Enum.map_join(shards, "\n", fn shard ->
        status_html =
          case shard.status do
            "ok" -> ~s(<span class="c-green">ok</span>)
            _ -> ~s(<span class="c-red">#{escape(shard.status)}</span>)
          end

        disk_bytes = Map.get(shard, :disk_bytes, 0)

        """
        <tr>
          <td>#{shard.index}</td>
          <td>#{status_html}</td>
          <td>#{format_number(shard.keys)}</td>
          <td>#{format_bytes(shard.ets_memory_bytes)}</td>
          <td>#{format_bytes(disk_bytes)}</td>
        </tr>
        """
      end)

    summary_badge =
      if all_ok do
        ~s(<span class="badge badge-ok">all ok</span>)
      else
        down_count = Enum.count(shards, fn s -> s.status != "ok" end)
        ~s(<span class="badge badge-pressure">#{down_count} down</span>)
      end

    """
    <div class="section-title">Shards #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Keys</th><th>Memory</th><th>Disk</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_memory_alert(memory_data()) :: binary()
  defp render_memory_alert(data) do
    # Only show the memory section when there is pressure
    if data.pressure_level == :ok do
      ""
    else
      level_str = Atom.to_string(data.pressure_level)
      pct = if data.max_bytes > 0, do: Float.round(data.ratio * 100, 1), else: 0.0
      bar_color = mem_bar_color(pct)
      bar_width = min(pct, 100)

      badge_class =
        case data.pressure_level do
          :warning -> "badge-warning"
          :pressure -> "badge-pressure"
          :reject -> "badge-reject"
          _ -> "badge-idle"
        end

      level_class =
        case data.pressure_level do
          :warning -> "level-warning"
          :pressure -> "level-pressure"
          :reject -> "level-reject"
          _ -> ""
        end

      action_text =
        case data.pressure_level do
          :warning -> "Consider increasing max_memory or reviewing eviction policy."
          :pressure -> "Eviction active. Keys are being removed under #{escape(Atom.to_string(data.eviction_policy))} policy."
          :reject -> "Writes are being rejected. Increase max_memory immediately."
          _ -> ""
        end

      shard_rows =
        data.shards
        |> Enum.sort_by(fn {index, _} -> index end)
        |> Enum.map_join("\n", fn {index, shard} ->
          shard_pct = Float.round(shard.ratio * 100, 1)

          shard_class =
            cond do
              shard.ratio >= 0.95 -> "c-red"
              shard.ratio >= 0.85 -> "c-red"
              shard.ratio >= 0.70 -> "c-yellow"
              true -> ""
            end

          """
          <tr>
            <td>#{index}</td>
            <td>#{format_bytes(shard.bytes)}</td>
            <td class="#{shard_class}">#{shard_pct}%</td>
          </tr>
          """
        end)

      """
      <div class="section-title">Memory Pressure <span class="badge #{badge_class}">#{escape(level_str)}</span></div>
      <div class="pressure-alert #{level_class}">
        <div class="pressure-details">
          <span>#{format_bytes(data.total_bytes)}</span> / <span>#{format_bytes(data.max_bytes)}</span> (#{pct}%)
          &middot; Policy: <span>#{escape(Atom.to_string(data.eviction_policy))}</span>
        </div>
        <div class="pressure-bar-wrap"><div class="pressure-bar-fill" style="width:#{bar_width}%;background:#{bar_color};"></div></div>
        <div class="pressure-action">#{action_text}</div>
      </div>
      <table>
        <thead>
          <tr><th>Shard</th><th>Bytes</th><th>Usage</th></tr>
        </thead>
        <tbody>
          #{shard_rows}
        </tbody>
      </table>
      """
    end
  end

  @spec render_connections(connections_data()) :: binary()
  defp render_connections(data) do
    blocked_class = if data.blocked > 0, do: "c-yellow", else: ""

    """
    <div class="section-title">Connections</div>
    <div class="conn-row">
      <div class="conn-item">
        <span class="conn-label">Active </span>
        <span class="conn-val">#{format_number(data.active)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Blocked </span>
        <span class="conn-val #{blocked_class}">#{format_number(data.blocked)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Tracking </span>
        <span class="conn-val">#{format_number(data.tracking)}</span>
      </div>
    </div>
    """
  end

  # render_nav_links removed — replaced by sidebar navigation

  @spec render_footer(dashboard_data()) :: binary()
  defp render_footer(data) do
    sample_rate = data.hotcold.sample_rate

    """
    <div class="footer">
      <span>Uptime: #{format_uptime(data.overview.uptime_seconds)} &middot; v0.1.0 &middot; Run #{escape(String.slice(data.overview.run_id, 0, 8))}</span>
      <span>Hit/miss stats estimated from 1:#{sample_rate} sampling &middot; Auto-refresh 2s</span>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Sub-page content sections
  # ---------------------------------------------------------------------------

  @spec render_slowlog_table([slowlog_entry()]) :: binary()
  defp render_slowlog_table(entries) do
    count = length(entries)
    count_label = if count == 0, do: "none", else: "#{count} entries"

    rows =
      case entries do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No slow commands recorded</td></tr>)

        _ ->
          Enum.map_join(entries, "\n", fn entry ->
            cmd_str = Enum.join(entry.command, " ")
            duration_ms = Float.round(entry.duration_us / 1000.0, 2)
            time_str = format_timestamp_us(entry.timestamp_us)

            """
            <tr>
              <td>#{entry.id}</td>
              <td class="mono">#{escape(time_str)}</td>
              <td>#{duration_ms} ms</td>
              <td class="mono">#{escape(cmd_str)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Slow Log <span class="badge badge-idle">#{escape(count_label)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Time</th><th>Duration</th><th>Command</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_merge_table([merge_status()]) :: binary()
  defp render_merge_table(merges) do
    active_count = Enum.count(merges, & &1.merging)
    summary_label = if active_count > 0, do: "#{active_count} active", else: "idle"

    rows =
      Enum.map_join(merges, "\n", fn m ->
        status_badge =
          if m.merging do
            ~s(<span class="badge badge-merging">merging</span>)
          else
            ~s(<span class="badge badge-idle">idle</span>)
          end

        last_merge_str =
          case m.last_merge_at do
            nil -> "never"
            ts -> format_timestamp_ms(ts)
          end

        """
        <tr>
          <td>#{m.shard_index}</td>
          <td>#{escape(Atom.to_string(m.mode))}</td>
          <td>#{status_badge}</td>
          <td>#{last_merge_str}</td>
          <td>#{m.merge_count}</td>
          <td>#{format_bytes(m.total_bytes_reclaimed)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Merge Status <span class="badge badge-idle">#{escape(summary_label)}</span></div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Mode</th><th>Status</th><th>Last Merge</th><th>Merges</th><th>Reclaimed</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_config_table([NamespaceConfig.ns_entry()]) :: binary()
  defp render_config_table(entries) do
    count_label =
      case entries do
        [] -> "defaults"
        list -> "#{length(list)} overrides"
      end

    body =
      case entries do
        [] ->
          ~s[<p style="color:#8b949e; margin: 8px 0; font-size:0.82rem;">All namespaces using built-in defaults (1ms, quorum)</p>]

        _ ->
          rows =
            Enum.map_join(entries, "\n", fn entry ->
              durability_str = Atom.to_string(entry.durability)

              durability_class =
                if entry.durability == :async, do: "c-yellow", else: ""

              changed_at_str =
                if entry.changed_at == 0 do
                  "default"
                else
                  entry.changed_at
                  |> DateTime.from_unix!()
                  |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
                end

              """
              <tr>
                <td class="mono">#{escape(entry.prefix)}</td>
                <td>#{entry.window_ms}</td>
                <td class="#{durability_class}">#{escape(durability_str)}</td>
                <td>#{changed_at_str}</td>
                <td>#{escape(entry.changed_by)}</td>
              </tr>
              """
            end)

          """
          <table>
            <thead>
              <tr><th>Prefix</th><th>Window (ms)</th><th>Durability</th><th>Changed At</th><th>Changed By</th></tr>
            </thead>
            <tbody>
              #{rows}
            </tbody>
          </table>
          """
      end

    """
    <div class="section-title">Namespace Config <span class="badge badge-idle">#{escape(count_label)}</span></div>
    #{body}
    """
  end

  @spec render_cluster_info(cluster_data()) :: binary()
  defp render_cluster_info(cluster) do
    node_str = Atom.to_string(cluster.node_name)

    cluster_badge =
      case cluster.cluster_mode do
        :standalone ->
          ~s(<span class="badge badge-idle">standalone</span>)
        :cluster ->
          ~s(<span class="badge badge-ok">#{cluster.cluster_size}-node cluster</span>)
      end

    nodes_html =
      if cluster.cluster_size > 1 do
        node_items =
          Enum.map_join(cluster.nodes, "", fn n ->
            is_self = n == cluster.node_name
            class = if is_self, do: "c-green", else: ""
            label = if is_self, do: " (this node)", else: ""
            ~s(<span class="#{class}" style="margin-right:16px;">#{escape(Atom.to_string(n))}#{label}</span>)
          end)

        """
        <div style="margin-top:8px; font-size:0.82rem; color:#8b949e;">
          Nodes: #{node_items}
        </div>
        """
      else
        ""
      end

    """
    <div class="section-title">Cluster #{cluster_badge}</div>
    <div class="conn-row" style="flex-direction:column; align-items:flex-start;">
      <div style="font-size:0.85rem;">
        <span class="conn-label">Node: </span>
        <span class="conn-val mono">#{escape(node_str)}</span>
      </div>
      #{nodes_html}
    </div>
    """
  end

  @spec render_raft_table([raft_shard_data()]) :: binary()
  defp render_raft_table(raft_shards) do
    ok_count = Enum.count(raft_shards, &(&1.status == :ok))
    total = length(raft_shards)

    summary_badge =
      if ok_count == total do
        ~s(<span class="badge badge-ok">all ok</span>)
      else
        ~s(<span class="badge badge-pressure">#{total - ok_count} unavailable</span>)
      end

    rows =
      Enum.map_join(raft_shards, "\n", fn rs ->
        status_html =
          case rs.status do
            :ok -> ~s(<span class="c-green">ok</span>)
            _ -> ~s(<span class="c-red">unavailable</span>)
          end

        leader_html =
          case rs.leader do
            nil -> ~s(<span class="c-muted">none</span>)
            {name, leader_node} ->
              is_local = leader_node == node()
              class = if is_local, do: "c-green", else: ""
              leader_str = "#{name}@#{leader_node}"
              ~s(<span class="#{class} mono">#{escape(leader_str)}</span>)
          end

        members_str =
          case rs.members do
            [] -> "-"
            members ->
              Enum.map_join(members, ", ", fn {name, n} -> "#{name}@#{n}" end)
          end

        lag = rs.commit_index - rs.last_applied
        lag_class = cond do
          lag > 1000 -> "c-red"
          lag > 100 -> "c-yellow"
          true -> ""
        end

        """
        <tr>
          <td>#{rs.shard}</td>
          <td>#{status_html}</td>
          <td>#{leader_html}</td>
          <td>#{rs.current_term}</td>
          <td>#{format_number(rs.commit_index)}</td>
          <td class="#{lag_class}">#{format_number(rs.last_applied)}</td>
          <td>#{format_number(rs.log_size)}</td>
          <td class="mono" style="font-size:0.75rem;">#{escape(members_str)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Per-Shard Raft State #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Leader</th><th>Term</th><th>Commit Idx</th><th>Applied Idx</th><th>Log Size</th><th>Members</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_clients_summary(connections_data()) :: binary()
  defp render_clients_summary(conns) do
    blocked_class = if conns.blocked > 0, do: "c-yellow", else: ""

    """
    <div class="conn-row" style="margin-bottom:16px;">
      <div class="conn-item">
        <span class="conn-label">Active </span>
        <span class="conn-val">#{format_number(conns.active)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Blocked </span>
        <span class="conn-val #{blocked_class}">#{format_number(conns.blocked)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Tracking </span>
        <span class="conn-val">#{format_number(conns.tracking)}</span>
      </div>
    </div>
    """
  end

  @spec render_clients_table([client_data()]) :: binary()
  defp render_clients_table(clients) do
    rows =
      case clients do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No active connections</td></tr>)

        _ ->
          Enum.map_join(clients, "\n", fn c ->
            pid_str = inspect(c.pid)

            """
            <tr>
              <td class="mono">#{escape(pid_str)}</td>
              <td class="mono">#{escape(c.peer)}</td>
              <td>#{format_uptime(c.age_seconds)}</td>
              <td>#{escape(c.flags)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Active Connections <span class="badge badge-idle">#{length(clients)}</span></div>
    <table>
      <thead>
        <tr><th>PID</th><th>Client Address</th><th>Age</th><th>Flags</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    <div style="margin-top:8px; font-size:0.72rem; color:#484f58;">
      Flags: M=in MULTI transaction, S=subscribed (pub/sub), T=tracking enabled
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Storage Sub-page
  # ---------------------------------------------------------------------------

  @spec render_storage_summary(map()) :: binary()
  defp render_storage_summary(data) do
    """
    <div class="conn-row" style="margin-bottom:16px;">
      <div class="conn-item">
        <span class="conn-label">Total Disk </span>
        <span class="conn-val">#{format_bytes(data.total_disk_bytes)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Total Files </span>
        <span class="conn-val">#{format_number(data.total_files)}</span>
      </div>
    </div>
    """
  end

  @spec render_storage_table([map()]) :: binary()
  defp render_storage_table(shards) do
    rows =
      Enum.map_join(shards, "\n", fn shard ->
        """
        <tr>
          <td>#{shard.index}</td>
          <td>#{format_bytes(shard.disk_bytes)}</td>
          <td>#{shard.data_file_count}</td>
          <td>#{shard.hint_file_count}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Per-Shard Storage</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Disk Size</th><th>Data Files</th><th>Hint Files</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Prefixes Sub-page
  # ---------------------------------------------------------------------------

  @spec render_prefixes_table(map()) :: binary()
  defp render_prefixes_table(data) do
    prefix_count = length(data.prefixes)
    count_label = if prefix_count == 0, do: "none", else: "#{prefix_count} prefixes"

    rows =
      case data.prefixes do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No keys found</td></tr>)

        _ ->
          Enum.map_join(data.prefixes, "\n", fn p ->
            """
            <tr>
              <td class="mono">#{escape(p.prefix)}</td>
              <td>#{format_number(p.keys)}</td>
              <td>#{p.pct}%</td>
              <td>#{format_number(p.hot_reads)}</td>
              <td>#{format_number(p.cold_reads)}</td>
            </tr>
            """
          end)
      end

    sampled_note =
      if data.total_sampled > 0 do
        ~s(<div style="margin-top:8px; font-size:0.72rem; color:#484f58;">Sampled #{format_number(data.total_sampled)} keys from keydir ETS tables</div>)
      else
        ""
      end

    """
    <div class="section-title">Key Prefixes <span class="badge badge-idle">#{escape(count_label)}</span></div>
    <table>
      <thead>
        <tr><th>Prefix</th><th>Keys</th><th>% of Total</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    #{sampled_note}
    """
  end

  # ---------------------------------------------------------------------------
  # Shared HTML scaffolding
  # ---------------------------------------------------------------------------

  @spec page_head(String.t(), non_neg_integer() | nil) :: binary()
  defp page_head(title, refresh_seconds) do
    refresh_meta =
      case refresh_seconds do
        nil -> ""
        n -> ~s(  <meta http-equiv="refresh" content="#{n}">\n)
      end

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
    #{refresh_meta}  <title>#{escape(title)}</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace; background: #0d1117; color: #c9d1d9; padding: 0; min-height: 100vh; }

        /* Top bar */
        .top-bar { display: flex; align-items: center; gap: 24px; padding: 12px 20px; background: #161b22; border-bottom: 1px solid #30363d; flex-wrap: wrap; }
        .top-bar .logo { font-size: 1.1rem; font-weight: 700; color: #58a6ff; white-space: nowrap; }
        .top-bar .metric { display: flex; flex-direction: column; align-items: center; min-width: 80px; }
        .top-bar .metric .label { font-size: 0.65rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
        .top-bar .metric .val { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }
        .top-bar .sep { width: 1px; height: 32px; background: #30363d; flex-shrink: 0; }

        /* Status badge */
        .status-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; vertical-align: middle; }
        .dot-green { background: #3fb950; box-shadow: 0 0 6px #3fb95066; }
        .dot-yellow { background: #d29922; box-shadow: 0 0 6px #d2992266; }
        .dot-red { background: #f85149; box-shadow: 0 0 6px #f8514966; }

        /* Memory bar in top bar */
        .mem-bar-wrap { width: 80px; height: 6px; background: #21262d; border-radius: 3px; margin-top: 2px; overflow: hidden; }
        .mem-bar-fill { height: 100%; border-radius: 3px; transition: width 0.3s; }

        /* Main content */
        .content { padding: 16px 20px 80px; max-width: 1200px; margin: 0 auto; }

        /* Section headers */
        .section-title { font-size: 0.9rem; font-weight: 600; color: #79c0ff; margin: 24px 0 12px; text-transform: uppercase; letter-spacing: 0.5px; }
        .section-title:first-child { margin-top: 8px; }

        /* Hero hit rate */
        .cache-hero { display: flex; gap: 24px; margin-bottom: 16px; flex-wrap: wrap; }
        .hit-rate-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 28px; text-align: center; min-width: 180px; flex: 0 0 auto; }
        .hit-rate-num { font-size: 3rem; font-weight: 800; line-height: 1.1; }
        .hit-rate-label { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 2px; }
        .hit-rate-sub { font-size: 0.8rem; color: #8b949e; margin-top: 8px; }
        .hit-rate-sub span { color: #c9d1d9; font-weight: 600; }

        /* Source breakdown */
        .source-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px 24px; flex: 1; min-width: 200px; }
        .source-row { display: flex; align-items: center; justify-content: space-between; padding: 8px 0; }
        .source-row + .source-row { border-top: 1px solid #21262d; }
        .source-name { font-size: 0.85rem; color: #c9d1d9; }
        .source-detail { font-size: 0.7rem; color: #484f58; }
        .source-pct { font-size: 1.1rem; font-weight: 700; }
        .source-bar-wrap { width: 100%; height: 4px; background: #21262d; border-radius: 2px; margin-top: 4px; }
        .source-bar-fill { height: 100%; border-radius: 2px; }

        /* Compact table */
        table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; font-size: 0.82rem; }
        th { background: #21262d; color: #8b949e; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 6px 12px; text-align: left; }
        td { padding: 6px 12px; border-top: 1px solid #21262d; }
        tr:hover td { background: #1c2128; }

        /* Status colors */
        .c-green { color: #3fb950; }
        .c-yellow { color: #d29922; }
        .c-red { color: #f85149; }
        .c-muted { color: #484f58; }

        /* Badges */
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
        .badge-ok { background: #238636; color: #fff; }
        .badge-warning { background: #9e6a03; color: #fff; }
        .badge-pressure { background: #da3633; color: #fff; }
        .badge-reject { background: #8b1a1a; color: #fff; }
        .badge-merging { background: #1f6feb; color: #fff; }
        .badge-idle { background: #30363d; color: #8b949e; }

        /* Memory pressure alert */
        .pressure-alert { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px 20px; margin-bottom: 16px; }
        .pressure-alert.level-warning { border-color: #9e6a03; }
        .pressure-alert.level-pressure { border-color: #da3633; }
        .pressure-alert.level-reject { border-color: #f85149; border-width: 2px; }
        .pressure-header { display: flex; align-items: center; gap: 8px; margin-bottom: 8px; }
        .pressure-bar-wrap { width: 100%; height: 8px; background: #21262d; border-radius: 4px; overflow: hidden; margin: 8px 0; }
        .pressure-bar-fill { height: 100%; border-radius: 4px; }
        .pressure-details { font-size: 0.8rem; color: #8b949e; }
        .pressure-details span { color: #c9d1d9; font-weight: 600; }
        .pressure-action { font-size: 0.75rem; color: #d29922; margin-top: 6px; font-style: italic; }

        /* Connections inline */
        .conn-row { display: flex; gap: 24px; align-items: center; background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 10px 16px; font-size: 0.85rem; flex-wrap: wrap; }
        .conn-item .conn-label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.3px; }
        .conn-item .conn-val { font-weight: 700; color: #f0f6fc; }

        /* Sidebar */
        .layout { display: flex; min-height: calc(100vh - 54px); }
        .sidebar { width: 200px; flex-shrink: 0; background: #161b22; border-right: 1px solid #30363d; padding: 12px 0; position: sticky; top: 0; height: calc(100vh - 54px); overflow-y: auto; }
        .sidebar a { display: flex; align-items: center; gap: 8px; padding: 8px 16px; text-decoration: none; color: #c9d1d9; font-size: 0.82rem; transition: background 0.1s; border-left: 3px solid transparent; }
        .sidebar a:hover { background: #1c2128; }
        .sidebar a.active { background: #1c2128; border-left-color: #58a6ff; color: #58a6ff; font-weight: 600; }
        .sidebar .nav-label { flex: 1; }
        .sidebar .nav-badge { font-size: 0.65rem; color: #8b949e; background: #21262d; padding: 1px 6px; border-radius: 8px; white-space: nowrap; }
        .sidebar .nav-section { font-size: 0.65rem; color: #484f58; text-transform: uppercase; letter-spacing: 0.5px; padding: 16px 16px 4px; }
        .main-content { flex: 1; min-width: 0; }

        /* Sub-page header (still used inside main-content on sub-pages) */
        .subpage-header { display: flex; align-items: center; gap: 16px; padding: 12px 20px; background: #0d1117; border-bottom: 1px solid #30363d; }
        .subpage-title { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }

        /* Footer */
        .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #0d1117; border-top: 1px solid #21262d; padding: 6px 20px; font-size: 0.7rem; color: #484f58; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }

        /* Tooltip */
        .info-icon { display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #30363d; color: #8b949e; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; }

        .mono { font-family: 'SFMono-Regular', Consolas, monospace; font-size: 0.82rem; }

        /* Sampling indicator */
        .sampled-tag { display: inline-block; font-size: 0.55rem; color: #8b949e; background: #21262d; padding: 0px 4px; border-radius: 3px; vertical-align: middle; font-weight: 400; letter-spacing: 0; text-transform: none; cursor: help; }

        /* Responsive */
        @media (max-width: 768px) {
          .layout { flex-direction: column; }
          .sidebar { width: 100%; height: auto; position: static; border-right: none; border-bottom: 1px solid #30363d; padding: 8px 0; display: flex; flex-wrap: wrap; overflow-x: auto; }
          .sidebar a { padding: 6px 12px; border-left: none; border-bottom: 2px solid transparent; font-size: 0.75rem; }
          .sidebar a.active { border-left: none; border-bottom-color: #58a6ff; }
          .sidebar .nav-section { display: none; }
          .top-bar { gap: 12px; padding: 10px 12px; }
          .top-bar .metric .val { font-size: 0.9rem; }
          .top-bar .sep { display: none; }
          .content { padding: 12px 12px 70px; }
          .hit-rate-num { font-size: 2.2rem; }
          .cache-hero { flex-direction: column; }
          .hit-rate-card { min-width: unset; }
        }
      </style>
    </head>
    """
  end

  @spec render_subpage_header(String.t()) :: binary()
  defp render_subpage_header(title) do
    """
    <div class="subpage-header">
      <span class="subpage-title">#{escape(title)}</span>
    </div>
    """
  end

  # Sidebar with live badge data (used on main dashboard)
  @spec render_sidebar(dashboard_data(), String.t()) :: binary()
  defp render_sidebar(data, active) do
    slowlog_count = length(data.slowlog)
    slowlog_badge = if slowlog_count == 0, do: "", else: "#{slowlog_count}"

    active_merges = Enum.count(data.merge, & &1.merging)
    merge_badge = if active_merges > 0, do: "#{active_merges}", else: ""

    config_count = length(data.namespace_config)
    config_badge = if config_count == 0, do: "", else: "#{config_count}"

    conns = data.connections
    conns_badge = if conns.active > 0, do: "#{conns.active}", else: ""

    storage_badge = format_bytes(data.storage_summary.total_disk_bytes)

    sidebar_html(active, %{
      "slowlog" => slowlog_badge,
      "merge" => merge_badge,
      "config" => config_badge,
      "clients" => conns_badge,
      "storage" => storage_badge
    })
  end

  # Sidebar without live data (used on sub-pages to avoid expensive data collection)
  @spec render_sidebar_static(String.t()) :: binary()
  defp render_sidebar_static(active) do
    sidebar_html(active, %{"slowlog" => "", "merge" => "", "config" => "", "clients" => "", "storage" => ""})
  end

  defp sidebar_html(active, badges) do
    items = [
      {"overview", "/dashboard", "Overview"},
      {"slowlog", "/dashboard/slowlog", "Slow Log"},
      {"merge", "/dashboard/merge", "Merge Status"},
      {"storage", "/dashboard/storage", "Storage"},
      {"raft", "/dashboard/raft", "Raft Consensus"},
      {"config", "/dashboard/config", "Config"},
      {"clients", "/dashboard/clients", "Clients"},
      {"prefixes", "/dashboard/prefixes", "Key Prefixes"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label} ->
        active_class = if key == active, do: " active", else: ""
        badge_val = Map.get(badges, key, "")

        badge_html =
          if badge_val != "" and badge_val != nil do
            ~s(<span class="nav-badge">#{escape(to_string(badge_val))}</span>)
          else
            ""
          end

        ~s(<a class="#{active_class}" href="#{href}"><span class="nav-label">#{escape(label)}</span>#{badge_html}</a>)
      end)

    """
    <nav class="sidebar">
      #{links}
    </nav>
    """
  end

  # ---------------------------------------------------------------------------
  # Formatting helpers (private)
  # ---------------------------------------------------------------------------

  # Renders a small "~1:N" tag indicating a value is estimated from sampling.
  # Shows the actual configured sample rate so the user knows the precision.
  @spec sampled_tag(pos_integer()) :: binary()
  defp sampled_tag(1), do: ""  # 1:1 = every request, no sampling
  defp sampled_tag(rate) do
    ~s(<span class="sampled-tag" title="Estimated from 1:#{rate} sampling">~1:#{rate}</span>)
  end

  @spec hit_rate_color(float()) :: binary()
  defp hit_rate_color(ratio) do
    cond do
      ratio >= 90.0 -> "#3fb950"
      ratio >= 70.0 -> "#d29922"
      true -> "#f85149"
    end
  end

  @spec mem_bar_color(float()) :: binary()
  defp mem_bar_color(pct) do
    cond do
      pct >= 95.0 -> "#f85149"
      pct >= 85.0 -> "#da3633"
      pct >= 70.0 -> "#d29922"
      true -> "#3fb950"
    end
  end

  @spec format_rate(float()) :: binary()
  defp format_rate(rate) when rate >= 1_000_000.0 do
    "#{Float.round(rate / 1_000_000, 1)}M"
  end

  defp format_rate(rate) when rate >= 1_000.0 do
    "#{Float.round(rate / 1_000, 1)}K"
  end

  defp format_rate(rate) do
    "#{Float.round(rate, 1)}"
  end

  @spec format_uptime(non_neg_integer()) :: binary()
  defp format_uptime(seconds) do
    days = div(seconds, 86_400)
    hours = div(rem(seconds, 86_400), 3_600)
    mins = div(rem(seconds, 3_600), 60)
    secs = rem(seconds, 60)

    cond do
      days > 0 -> "#{days}d #{hours}h #{mins}m"
      hours > 0 -> "#{hours}h #{mins}m #{secs}s"
      mins > 0 -> "#{mins}m #{secs}s"
      true -> "#{secs}s"
    end
  end

  @spec format_bytes(non_neg_integer()) :: binary()
  defp format_bytes(bytes) when bytes >= 1_073_741_824 do
    "#{Float.round(bytes / 1_073_741_824, 2)} GB"
  end

  defp format_bytes(bytes) when bytes >= 1_048_576 do
    "#{Float.round(bytes / 1_048_576, 2)} MB"
  end

  defp format_bytes(bytes) when bytes >= 1_024 do
    "#{Float.round(bytes / 1_024, 2)} KB"
  end

  defp format_bytes(bytes), do: "#{bytes} B"

  @spec format_number(non_neg_integer()) :: binary()
  defp format_number(n) when n >= 1_000_000 do
    "#{Float.round(n / 1_000_000, 2)}M"
  end

  defp format_number(n) when n >= 1_000 do
    "#{Float.round(n / 1_000, 1)}K"
  end

  defp format_number(n), do: Integer.to_string(n)

  @spec format_timestamp_us(integer()) :: binary()
  defp format_timestamp_us(timestamp_us) do
    timestamp_us
    |> div(1_000_000)
    |> DateTime.from_unix!()
    |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
  end

  @spec format_timestamp_ms(integer()) :: binary()
  defp format_timestamp_ms(timestamp_ms) do
    timestamp_ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
  end

  @spec escape(binary()) :: binary()
  defp escape(str) do
    str
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  @spec safe_ets_size(atom()) :: non_neg_integer()
  defp safe_ets_size(table) do
    try do
      case :ets.info(table, :size) do
        :undefined -> 0
        n when is_integer(n) -> n
        _ -> 0
      end
    rescue
      ArgumentError -> 0
    end
  end
end
