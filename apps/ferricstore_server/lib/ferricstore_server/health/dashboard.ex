defmodule FerricstoreServer.Health.Dashboard do
  @moduledoc """
  Self-contained HTML dashboard for FerricStore observability (spec 7.3).

  Renders a multi-page HTML dashboard with no external dependencies -- no
  Phoenix, no JavaScript frameworks, no CSS libraries. Live pages render a
  stable shell and patch named components from small JSON endpoints instead of
  reloading the full document.

  ## Pages

  1. **Main dashboard** (`/dashboard`) -- top bar, cache perf, shards, memory,
     connections, and navigation links to sub-pages. Refreshes every 2s.
  2. **Slow Log** (`/dashboard/slowlog`) -- full slow log table. Refreshes 5s.
  3. **Merge Status** (`/dashboard/merge`) -- per-shard merge/compaction. Refreshes 10s.
  4. **Config** (`/dashboard/config`) -- runtime commands, parameters, and namespace overrides. No refresh.
  5. **Consensus** (`/dashboard/raft`) -- per-shard WARaft health, leader,
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
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{BlobRef, CompoundKey}

  require EEx

  @doc false
  defp shard_count, do: :persistent_term.get(:ferricstore_shard_count, 4)

  @flow_dashboard_sample_limit 400
  @flow_dashboard_recent_limit 40
  @flow_dashboard_max_recent_limit 200
  @flow_dashboard_overview_recent_limit 10
  @flow_dashboard_detail_fetch_timeout_ms 5_000
  @flow_dashboard_list_fetch_timeout_ms 5_000
  @flow_dashboard_history_default_count 50
  @flow_dashboard_history_max_count 250
  @flow_dashboard_timeline_chart_max_events 80
  @flow_dashboard_signal_flow_fetch_limit 80
  @flow_dashboard_signal_history_count 25
  @flow_dashboard_value_ref_limit 40
  @flow_dashboard_value_preview_bytes 8 * 1024
  @flow_dashboard_policy_scan_limit 20_000
  @flow_dashboard_policy_state_preview_limit 6
  @flow_dashboard_policy_key_select_batch 256
  @flow_dashboard_keydir_select_batch 256
  @flow_dashboard_keydir_scan_multiplier 64
  @flow_dashboard_keydir_scan_floor 2_048
  @flow_dashboard_retention_default_limit 100
  @flow_dashboard_retention_max_limit 10_000
  @flow_dashboard_retention_candidate_preview_limit 100
  @flow_terminal_states ~w(completed failed cancelled)
  @flow_dashboard_time_range_options [
    {nil, "All time"},
    {"5m", "Last 5 minutes"},
    {"15m", "Last 15 minutes"},
    {"1h", "Last 1 hour"},
    {"6h", "Last 6 hours"},
    {"24h", "Last 24 hours"}
  ]
  @keyspace_dashboard_default_limit 50
  @keyspace_dashboard_max_limit 500
  @keyspace_dashboard_select_batch 256

  @templates_dir Path.expand("dashboard/templates", __DIR__)
  EEx.function_from_file(
    :defp,
    :template_overview,
    Path.join(@templates_dir, "overview.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :defp,
    :template_slowlog,
    Path.join(@templates_dir, "slowlog.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(:defp, :template_merge, Path.join(@templates_dir, "merge.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:defp, :template_config, Path.join(@templates_dir, "config.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(:defp, :template_raft, Path.join(@templates_dir, "raft.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :defp,
    :template_clients,
    Path.join(@templates_dir, "clients.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :defp,
    :template_storage,
    Path.join(@templates_dir, "storage.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :defp,
    :template_prefixes,
    Path.join(@templates_dir, "prefixes.html.eex"),
    [
      :assigns
    ]
  )

  EEx.function_from_file(
    :defp,
    :template_keyspace,
    Path.join(@templates_dir, "keyspace.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_commands,
    Path.join(@templates_dir, "commands.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_reads,
    Path.join(@templates_dir, "reads.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(:defp, :template_flow, Path.join(@templates_dir, "flow.html.eex"), [
    :assigns
  ])

  EEx.function_from_file(
    :defp,
    :template_flow_states,
    Path.join(@templates_dir, "flow_states.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_workers,
    Path.join(@templates_dir, "flow_workers.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_due,
    Path.join(@templates_dir, "flow_due.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_failures,
    Path.join(@templates_dir, "flow_failures.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_lineage,
    Path.join(@templates_dir, "flow_lineage.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_query,
    Path.join(@templates_dir, "flow_query.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_controls,
    Path.join(@templates_dir, "flow_failures_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_recovery_actions,
    Path.join(@templates_dir, "flow_recovery_actions.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_summary,
    Path.join(@templates_dir, "flow_failures_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_failures_table,
    Path.join(@templates_dir, "flow_failures_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_controls,
    Path.join(@templates_dir, "flow_lineage_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_summary,
    Path.join(@templates_dir, "flow_lineage_summary.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_graph,
    Path.join(@templates_dir, "flow_lineage_graph.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_lineage_table,
    Path.join(@templates_dir, "flow_lineage_table.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_controls,
    Path.join(@templates_dir, "flow_query_controls.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :component_flow_query_result,
    Path.join(@templates_dir, "flow_query_result.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_signals,
    Path.join(@templates_dir, "flow_signals.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_policies,
    Path.join(@templates_dir, "flow_policies.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_retention,
    Path.join(@templates_dir, "flow_retention.html.eex"),
    [:assigns]
  )

  EEx.function_from_file(
    :defp,
    :template_flow_detail,
    Path.join(@templates_dir, "flow_detail.html.eex"),
    [:assigns]
  )

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
          leader: tuple() | nil,
          current_term: non_neg_integer(),
          commit_index: non_neg_integer(),
          last_applied: non_neg_integer(),
          log_size: non_neg_integer(),
          members: [tuple()]
        }

  @typedoc "Active client connection data."
  @type client_data :: %{
          optional(:client_id) => pos_integer(),
          optional(:client_name) => binary() | nil,
          optional(:username) => binary() | nil,
          pid: pid(),
          peer: String.t(),
          age_seconds: non_neg_integer(),
          flags: String.t()
        }

  @typedoc "Configuration command reference row."
  @type config_command_entry :: %{
          command: binary(),
          scope: binary(),
          mutability: binary(),
          notes: binary()
        }

  @typedoc "Configuration parameter reference row."
  @type config_parameter_entry :: %{
          parameter: binary(),
          scope: binary(),
          mutability: binary(),
          notes: binary()
        }

  @typedoc "Configuration dashboard page data."
  @type config_page_data :: %{
          namespace_config: [NamespaceConfig.ns_entry()],
          config_commands: [config_command_entry()],
          config_parameters: [config_parameter_entry()]
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
      flow_summary: collect_flow_summary(),
      storage_summary: collect_storage_summary()
    }
  end

  @doc """
  Renders the main dashboard page as a complete HTML document.
  """
  @spec render(dashboard_data()) :: binary()
  def render(data) do
    render_template(template_overview(%{data: data}))
  end

  @doc """
  Builds the JSON payload used by the live overview dashboard shell.

  Values are HTML component fragments keyed by stable component names. The
  browser patches only components whose HTML changed, preserving scroll,
  selected text, and browser paint state.
  """
  @spec live_overview_payload(dashboard_data()) :: map()
  def live_overview_payload(data) do
    %{
      generated_at_ms: System.system_time(:millisecond),
      components: render_overview_live_components(data)
    }
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
    render_template(template_slowlog(%{data: data}))
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
    render_template(template_merge(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Namespace Config Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the namespace config sub-page.
  """
  @spec collect_config_page() :: config_page_data()
  def collect_config_page do
    %{
      namespace_config: NamespaceConfig.get_all(),
      config_commands: config_command_reference(),
      config_parameters: runtime_config_parameter_reference()
    }
  end

  @doc """
  Renders the namespace config sub-page (no auto-refresh).
  """
  @spec render_config_page(config_page_data() | map()) :: binary()
  def render_config_page(data) do
    render_template(template_config(%{data: data}))
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
    render_template(template_raft(%{data: data}))
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
  @spec render_clients_page(%{clients: [client_data()], connections: connections_data()}) ::
          binary()
  def render_clients_page(data) do
    render_template(template_clients(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Storage Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the storage sub-page.
  """
  @spec collect_storage_page() :: %{
          shards: [map()],
          total_disk_bytes: non_neg_integer(),
          total_files: non_neg_integer()
        }
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

    {total_disk, data_files, hint_files} = scan_storage_tree(data_dir)
    total_files = data_files + hint_files

    %{shards: shard_storage, total_disk_bytes: total_disk, total_files: total_files}
  end

  @doc """
  Renders the storage sub-page.
  """
  @spec render_storage_page(map()) :: binary()
  def render_storage_page(data) do
    render_template(template_storage(%{data: data}))
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
    render_template(template_prefixes(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- KV Keyspace / Commands / Read Path Sub-pages
  # ---------------------------------------------------------------------------

  @doc """
  Collects bounded keydir metadata for the generic KV keyspace inspector.
  """
  @spec collect_keyspace_page(keyword() | map()) :: map()
  def collect_keyspace_page(opts \\ []) do
    filters = keyspace_filters(opts)
    {rows, sampled} = collect_keyspace_rows(filters)

    %{
      filters: filters,
      rows: rows,
      inspected: inspect_keyspace_key(filters.key, rows),
      total_sampled: sampled
    }
  end

  @doc """
  Renders the generic KV keyspace inspector.
  """
  @spec render_keyspace_page(map()) :: binary()
  def render_keyspace_page(data) do
    render_template(template_keyspace(%{data: data}))
  end

  @doc """
  Collects command-level operational data from counters and slowlog samples.
  """
  @spec collect_commands_page() :: map()
  def collect_commands_page do
    slowlog = collect_slowlog()
    uptime = max(Stats.uptime_seconds(), 1)

    %{
      summary: %{
        total_commands: Stats.total_commands(),
        ops_per_sec: Float.round(Stats.total_commands() / uptime, 1),
        slowlog_entries: length(slowlog),
        slowest_us: Enum.reduce(slowlog, 0, fn entry, acc -> max(acc, entry.duration_us) end)
      },
      slow_by_command: group_slowlog_by_command(slowlog),
      command_groups: kv_command_groups()
    }
  end

  @doc """
  Renders the command statistics and command reference page.
  """
  @spec render_commands_page(map()) :: binary()
  def render_commands_page(data) do
    render_template(template_commands(%{data: data}))
  end

  @doc """
  Collects hot/cold read-path data for KV reads.
  """
  @spec collect_reads_page() :: map()
  def collect_reads_page do
    prefixes =
      Stats.hotness_top(50)
      |> Enum.map(fn {prefix, hot, cold, cold_pct} ->
        %{prefix: prefix, hot_reads: hot, cold_reads: cold, cold_pct: cold_pct}
      end)

    %{hotcold: collect_hotcold(), prefixes: prefixes}
  end

  @doc """
  Renders the hot/cold read-path page.
  """
  @spec render_reads_page(map()) :: binary()
  def render_reads_page(data) do
    render_template(template_reads(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- FerricFlow Sub-pages
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the FerricFlow overview page.

  The dashboard intentionally samples durable Flow state records from keydir and
  then asks the existing Flow read APIs for exact per-type lifecycle counts when
  possible. This keeps the UI low-impact while still showing useful operational
  state on large deployments.
  """
  @spec collect_flow_page(keyword()) :: map()
  def collect_flow_page(opts \\ []) when is_list(opts) do
    filters = flow_overview_filters_from_opts(opts)
    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    records = filter_flow_records_by_partition(sampled_records, filters.partition_key)
    types = flow_type_summaries(records)

    %{
      summary: flow_page_summary(types, records),
      projection: collect_flow_projection_health(),
      types: types,
      records: flow_recent_records(records, @flow_dashboard_overview_recent_limit),
      workers: flow_worker_summaries(records),
      filters: filters,
      total_sampled: length(sampled_records),
      filtered_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc false
  @spec flow_opts_from_query(binary()) :: keyword()
  def flow_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> Enum.reverse()
  end

  def flow_opts_from_query(_query), do: []

  @doc false
  @spec flow_page_filters(map()) :: map()
  def flow_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{partition_key: nil})
  end

  @doc """
  Renders the FerricFlow overview page.
  """
  @spec render_flow_page(map()) :: binary()
  def render_flow_page(data) do
    render_template(template_flow(%{data: data}))
  end

  @doc """
  Collects FerricFlow retry/retention policies.

  Policies can exist before any workflow of that type exists, so this page
  combines sampled active Flow types with a bounded scan for policy keys.
  """
  @spec collect_flow_policies_page(keyword()) :: map()
  def collect_flow_policies_page(opts \\ []) when is_list(opts) do
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    active_types = flow_available_types(records)
    policy_scan = collect_flow_policy_type_scan(@flow_dashboard_policy_scan_limit)
    configured_types = Map.get(policy_scan, :types, MapSet.new())
    edit_type = opts |> Keyword.get(:edit_type, "") |> flow_policy_clean_form_value()

    types =
      active_types
      |> Enum.concat(MapSet.to_list(configured_types))
      |> maybe_include_policy_edit_type(edit_type)
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.uniq()
      |> Enum.sort()

    %{
      policies: Enum.map(types, &flow_policy_row(&1, configured_types)),
      editor: flow_policy_editor_data(edit_type),
      flash: Keyword.get(opts, :flash),
      active_types: active_types,
      configured_types: MapSet.size(configured_types),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      policy_scan: Map.delete(policy_scan, :types),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc """
  Applies the Flow policy editor form.

  The form posts the full global retry/retention fields. Before writing the
  policy record we rebuild the existing state override list, so changing the
  global defaults from the dashboard does not silently delete per-state policy.
  """
  @spec apply_flow_policy_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_flow_policy_form(params) when is_map(params) do
    with {:ok, type} <- flow_policy_required_form_value(params, "type", "flow type"),
         {:ok, state} <- flow_policy_optional_form_value(params, "state"),
         {:ok, retry} <- flow_policy_form_retry_opts(params),
         {:ok, retention} <- flow_policy_form_retention_opts(params),
         {:ok, existing_opts} <- flow_policy_existing_set_opts(type),
         opts = flow_policy_merge_form_opts(existing_opts, state, retry, retention),
         {:ok, _policy} <- FerricStore.flow_policy_set(type, opts) do
      {:ok, type}
    else
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def apply_flow_policy_form(_params), do: {:error, "ERR policy form must be a map"}

  @doc """
  Applies the Flow detail rewind form.

  Dashboard rewind is event-based on purpose: the form does not accept an
  arbitrary target state, and this handler re-reads the selected history event
  before mutating the Flow. That keeps the screen constrained to states that this
  specific Flow has already reached.
  """
  @spec apply_flow_rewind_form(map()) ::
          {:ok, binary(), binary() | nil} | {:error, binary()}
  def apply_flow_rewind_form(params) when is_map(params) do
    with :ok <- flow_rewind_confirmed(params),
         {:ok, id} <- flow_rewind_required_form_value(params, "id", "flow id"),
         partition_key = normalize_flow_partition_query(Map.get(params, "partition_key")),
         {:ok, to_event} <- flow_rewind_required_form_value(params, "to_event", "target event"),
         {:ok, run_at_ms} <- flow_rewind_optional_non_neg_integer(params, "run_at_ms"),
         {:ok, record} <- flow_rewind_current_record(id, partition_key),
         {:ok, _target_state} <- flow_rewind_existing_target_state(id, partition_key, to_event),
         opts =
           flow_rewind_opts(partition_key,
             to_event: to_event,
             run_at_ms: run_at_ms,
             expect_state: flow_record_state(record)
           ),
         :ok <- flow_rewind_apply(id, opts) do
      {:ok, id, flow_detail_url_partition_key(partition_key)}
    else
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
    end
  end

  def apply_flow_rewind_form(_params), do: {:error, "ERR rewind form must be a map"}

  @spec flow_rewind_confirmed(map()) :: :ok | {:error, binary()}
  defp flow_rewind_confirmed(params) do
    case Map.get(params, "confirm_rewind") do
      value when value in ["true", "on", "yes", "1"] ->
        :ok

      _ ->
        {:error, "ERR rewind requires confirm_rewind=true after reviewing the selected event"}
    end
  end

  @doc """
  Converts a Flow policy editor query string into a small flash map.
  """
  @spec flow_policy_flash_from_query(binary()) :: map() | nil
  def flow_policy_flash_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    case Map.get(params, "status") do
      "ok" ->
        type = params |> Map.get("type", "") |> flow_policy_clean_form_value()
        %{kind: :ok, message: "Policy saved", type: type}

      "error" ->
        message =
          params
          |> Map.get("message", "Policy update failed")
          |> flow_policy_clean_form_value()

        %{kind: :error, message: message}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  @doc """
  Renders the FerricFlow policy page.
  """
  @spec render_flow_policies_page(map()) :: binary()
  def render_flow_policies_page(data) do
    render_template(template_flow_policies(%{data: data}))
  end

  @doc """
  Collects FerricFlow retention cleanup status for the maintenance page.

  The dashboard preview is intentionally sampled. The actual cleanup command is
  still the source of truth and runs through the same durable Flow command path
  as a RESP `FLOW.RETENTION_CLEANUP`.
  """
  @spec collect_flow_retention_page(keyword()) :: map()
  def collect_flow_retention_page(opts \\ []) when is_list(opts) do
    now_ms = System.system_time(:millisecond)

    limit =
      flow_retention_limit!(Keyword.get(opts, :limit, @flow_dashboard_retention_default_limit))

    records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    candidates = flow_retention_candidates(records, now_ms)
    terminal_sampled = Enum.count(records, &flow_retention_terminal_record?/1)

    %{
      now_ms: now_ms,
      limit: limit,
      sample_limit: @flow_dashboard_sample_limit,
      total_sampled: length(records),
      terminal_sampled: terminal_sampled,
      active_sampled: max(length(records) - terminal_sampled, 0),
      eligible_sampled: length(candidates),
      candidates:
        candidates
        |> Enum.take(min(limit, @flow_dashboard_retention_candidate_preview_limit)),
      storage: collect_storage_summary(),
      projection: collect_flow_projection_health(),
      flash: Keyword.get(opts, :flash),
      generated_at_ms: now_ms
    }
  end

  @doc """
  Applies the retention maintenance form.

  Dry-run only builds the sampled preview. Cleanup calls the real Flow cleanup
  command with the supplied limit.
  """
  @spec apply_flow_retention_form(map()) ::
          {:ok, :dry_run, map()} | {:ok, :cleanup, map()} | {:error, binary()}
  def apply_flow_retention_form(params) when is_map(params) do
    with {:ok, limit} <- flow_retention_form_limit(Map.get(params, "limit")) do
      case Map.get(params, "action", "dry_run") do
        "dry_run" ->
          {:ok, :dry_run, %{limit: limit}}

        "cleanup" ->
          with :ok <- flow_retention_cleanup_confirmed(params) do
            case flow_dashboard_retention_cleanup(limit: limit) do
              {:ok, result} when is_map(result) ->
                {:ok, :cleanup, flow_retention_cleanup_counts(result, limit)}

              {:error, reason} when is_binary(reason) ->
                {:error, reason}

              {:error, reason} ->
                {:error, inspect(reason)}

              other ->
                {:error, "ERR unexpected retention cleanup result: #{inspect(other, limit: 8)}"}
            end
          end

        _other ->
          {:error, "ERR retention action must be dry_run or cleanup"}
      end
    end
  end

  def apply_flow_retention_form(_params), do: {:error, "ERR retention form must be a map"}

  @spec flow_retention_cleanup_confirmed(map()) :: :ok | {:error, binary()}
  defp flow_retention_cleanup_confirmed(params) do
    case Map.get(params, "confirm_cleanup") do
      value when value in ["true", "on", "yes", "1"] ->
        :ok

      _ ->
        {:error, "ERR cleanup requires confirm_cleanup=true after reviewing the sample preview"}
    end
  end

  @doc false
  @spec flow_retention_flash_from_query(binary()) :: map() | nil
  def flow_retention_flash_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    case Map.get(params, "status") do
      "dry_run" ->
        limit = flow_retention_limit!(Map.get(params, "limit"))
        %{kind: :dry_run, message: "Dry run ready", limit: limit}

      "ok" ->
        %{
          kind: :ok,
          message: "Cleanup completed",
          limit: flow_retention_limit!(Map.get(params, "limit")),
          counts: %{
            flows: flow_retention_query_integer(params, "flows"),
            history: flow_retention_query_integer(params, "history"),
            values: flow_retention_query_integer(params, "values")
          }
        }

      "error" ->
        message =
          params
          |> Map.get("message", "Retention cleanup failed")
          |> flow_policy_clean_form_value()

        %{kind: :error, message: message}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  @doc """
  Renders the FerricFlow retention maintenance page.
  """
  @spec render_flow_retention_page(map()) :: binary()
  def render_flow_retention_page(data) do
    render_template(template_flow_retention(%{data: data}))
  end

  @doc """
  Builds the JSON payload used by the live FerricFlow dashboard shell.
  """
  @spec live_flow_payload(map()) :: map()
  def live_flow_payload(data) do
    %{
      generated_at_ms: Map.get(data, :generated_at_ms, System.system_time(:millisecond)),
      components: render_flow_live_components(data)
    }
  end

  @doc """
  Builds a live component payload for dashboard API paths.
  """
  @spec live_payload(binary()) :: {:ok, map()} | :not_found
  def live_payload("slowlog") do
    data = collect_slowlog_page()

    live_component_payload(%{
      "slowlog_summary" => render_slowlog_summary(data.slowlog),
      "slowlog_table" => render_slowlog_table(data.slowlog)
    })
  end

  def live_payload("merge") do
    data = collect_merge_page()

    live_component_payload(%{
      "merge_summary" => render_merge_summary(data.merge),
      "merge_table" => render_merge_table(data.merge)
    })
  end

  def live_payload("raft") do
    data = collect_raft_page()

    live_component_payload(%{
      "cluster_info" => render_cluster_info(data.cluster),
      "consensus_summary" => render_consensus_summary(data.raft_shards),
      "raft_table" => render_raft_table(data.raft_shards)
    })
  end

  def live_payload("clients") do
    data = collect_clients_page()

    live_component_payload(%{
      "clients_summary" => render_clients_summary(data.connections, data.clients),
      "clients_table" => render_clients_table(data.clients)
    })
  end

  def live_payload("storage") do
    data = collect_storage_page()

    live_component_payload(%{
      "storage_summary" => render_storage_summary(data),
      "storage_table" => render_storage_table(data.shards)
    })
  end

  def live_payload("keyspace") do
    data = collect_keyspace_page()

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("keyspace?" <> query) do
    data = collect_keyspace_page(URI.decode_query(query))

    live_component_payload(%{
      "keyspace_inspector" => render_keyspace_inspector(data.inspected),
      "keyspace_table" => render_keyspace_table(data)
    })
  end

  def live_payload("commands") do
    data = collect_commands_page()

    live_component_payload(%{
      "commands_summary" => render_commands_summary(data),
      "commands_slowlog" => render_command_slowlog_table(data)
    })
  end

  def live_payload("reads") do
    data = collect_reads_page()

    live_component_payload(%{
      "reads_summary" => render_reads_summary(data),
      "reads_prefixes" => render_read_prefix_table(data)
    })
  end

  def live_payload("prefixes") do
    data = collect_prefixes_page()

    live_component_payload(%{
      "prefixes_summary" => render_prefixes_summary(data),
      "prefixes_table" => render_prefixes_table(data)
    })
  end

  def live_payload("flow/states") do
    live_flow_states_payload("")
  end

  def live_payload("flow/states?" <> query) do
    live_flow_states_payload(query)
  end

  def live_payload("flow/workers") do
    data = collect_flow_workers_page()

    live_component_payload(%{
      "flow_workers_chart" => render_flow_workers_chart(data.workers),
      "flow_workers" => render_flow_workers(data.workers),
      "flow_running_records" =>
        render_flow_running_records(data.running_records, data.total_sampled, data.sample_limit)
    })
  end

  def live_payload("flow/due") do
    data = collect_flow_due_page()

    live_component_payload(%{
      "flow_due_chart" => render_flow_due_chart(data.due_now, data.scheduled),
      "flow_due_now" =>
        render_flow_due_records("Due Now", data.due_now, data.total_sampled, data.sample_limit),
      "flow_scheduled" =>
        render_flow_due_records(
          "Scheduled Future",
          data.scheduled,
          data.total_sampled,
          data.sample_limit
        )
    })
  end

  def live_payload("flow/signals") do
    live_flow_signals_payload("")
  end

  def live_payload("flow/signals?" <> query) do
    live_flow_signals_payload(query)
  end

  def live_payload("flow/projections"), do: :not_found

  def live_payload("flow/value?" <> query) do
    live_flow_value_payload(query)
  end

  def live_payload("flow/value") do
    live_flow_value_payload("")
  end

  def live_payload("flow/" <> encoded_id) do
    {id, opts} = decode_flow_detail_request(encoded_id)
    data = collect_flow_detail_page(id, Keyword.put(opts, :values, false))

    live_component_payload(%{
      "flow_detail" => render_flow_detail(data),
      "flow_debug" => render_flow_debug(data),
      "flow_history" =>
        render_flow_history_timeline(
          data.history,
          data.history_status,
          Map.get(data, :history_page)
        ),
      "flow_timeline_chart" => render_flow_timeline_chart(data.history)
    })
  end

  def live_payload(_path), do: :not_found

  @spec live_flow_value_payload(binary()) :: {:ok, map()}
  defp live_flow_value_payload(query) do
    params = URI.decode_query(query)
    ref = params |> Map.get("ref", "") |> String.trim()
    flow_id = params |> Map.get("flow", "") |> String.trim()
    partition_key = params |> Map.get("partition_key", "") |> String.trim()

    cond do
      ref == "" ->
        {:ok, live_flow_value_error("", "missing value ref")}

      flow_id == "" ->
        {:ok, live_flow_value_error(ref, "missing flow id")}

      true ->
        opts =
          if partition_key == "" do
            [values: false]
          else
            [values: false, partition_key: partition_key]
          end

        data = collect_flow_detail_page(flow_id, opts)
        visible_refs = flow_detail_value_refs(data.record, data.history) |> MapSet.new(& &1.ref)

        if MapSet.member?(visible_refs, ref) do
          live_flow_value_payload_from_ref(ref)
        else
          {:ok, live_flow_value_error(ref, "value ref is not visible on this Flow detail page")}
        end
    end
  rescue
    error ->
      {:ok, live_flow_value_error("", "value lookup failed: #{inspect(error, limit: 5)}")}
  catch
    :exit, error ->
      {:ok, live_flow_value_error("", "value lookup exited: #{inspect(error, limit: 5)}")}
  end

  @spec live_flow_value_payload_from_ref(binary()) :: {:ok, map()}
  defp live_flow_value_payload_from_ref(ref) do
    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_value_mget([ref]) end,
           timeout_ms,
           :value
         ) do
      {:ok, {:ok, [value]}} ->
        {:ok,
         %{
           generated_at_ms: System.system_time(:millisecond),
           status: "ok",
           ref: ref,
           value: flow_value_preview(value)
         }}

      {:ok, {:ok, _values}} ->
        {:ok, live_flow_value_error(ref, "unexpected value result count")}

      {:ok, {:error, reason}} ->
        {:ok, live_flow_value_error(ref, "value lookup failed: #{inspect(reason, limit: 5)}")}

      {:ok, _other} ->
        {:ok, live_flow_value_error(ref, "unexpected value lookup result")}

      {:error, :timeout} ->
        {:ok, live_flow_value_error(ref, "value lookup timed out")}

      {:error, reason} ->
        {:ok, live_flow_value_error(ref, "value lookup failed: #{inspect(reason, limit: 5)}")}
    end
  end

  @spec live_flow_value_error(binary(), binary()) :: map()
  defp live_flow_value_error(ref, message) do
    %{
      generated_at_ms: System.system_time(:millisecond),
      status: "error",
      ref: ref,
      error: message,
      value: message
    }
  end

  defp live_flow_signals_payload(query) do
    data = collect_flow_signals_page(flow_signals_opts_from_query(query))

    live_component_payload(%{
      "flow_signals_table" =>
        render_flow_signals_table(
          data.signals,
          data.total_sampled,
          data.filtered_sampled,
          data.sample_limit,
          data.filters
        )
    })
  end

  defp live_flow_states_payload(query) do
    data = collect_flow_states_page(flow_states_opts_from_query(query))

    live_component_payload(%{
      "flow_states_chart" => render_flow_states_chart(data.states),
      "flow_states_table" =>
        render_flow_states_table(
          data.states,
          data.total_sampled,
          data.filtered_sampled,
          data.sample_limit,
          data.filters
        ),
      "flow_recent_records" => render_flow_recent_records(data.records, data.limit)
    })
  end

  defp decode_flow_detail_request(encoded_id_with_query) do
    {encoded_id, query} =
      case String.split(encoded_id_with_query, "?", parts: 2) do
        [encoded_id, query] -> {encoded_id, query}
        [encoded_id] -> {encoded_id, ""}
      end

    {URI.decode(encoded_id), flow_detail_opts_from_query(query)}
  end

  @doc false
  @spec flow_detail_opts_from_query(binary()) :: keyword()
  def flow_detail_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:flash, flow_detail_flash_from_params(params))
    |> maybe_put_query_opt(
      :history_count,
      normalize_flow_history_count(Map.get(params, "history_count"))
    )
    |> maybe_put_query_opt(
      :history_before,
      normalize_flow_history_cursor(Map.get(params, "history_before"))
    )
    |> maybe_put_query_opt(:history_after, normalize_flow_history_after_cursor(params))
    |> Enum.reverse()
  end

  def flow_detail_opts_from_query(_query), do: []

  @spec flow_detail_flash_from_params(map()) :: map() | nil
  defp flow_detail_flash_from_params(params) do
    case Map.get(params, "status") do
      "rewound" ->
        %{kind: :ok, message: "Flow rewound"}

      "error" ->
        message =
          params
          |> Map.get("message", "Flow action failed")
          |> flow_policy_clean_form_value()

        %{kind: :error, message: if(message == "", do: "Flow action failed", else: message)}

      _ ->
        nil
    end
  rescue
    _ -> nil
  end

  @doc "Collects data for the Flow state-centric page."
  @spec collect_flow_states_page(keyword()) :: map()
  def collect_flow_states_page(opts \\ []) do
    filters = flow_state_filters_from_opts(opts)
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    available_types = flow_available_types(records)
    terminal_records = collect_flow_states_terminal_records(filters, available_types)

    type_records =
      records |> merge_flow_records(terminal_records) |> filter_flow_records_by_type(filters.type)

    filtered_records = filter_flow_records(type_records, filters)

    %{
      states: flow_state_summaries(filtered_records),
      records: flow_recent_records(filtered_records, filters.limit),
      available_types: flow_available_types(type_records ++ records),
      available_states:
        flow_available_states(type_records) |> maybe_include_flow_state(filters.state),
      filters: filters,
      type_filter: filters.type,
      state_filter: filters.state,
      name_filter: filters.q,
      range_filter: filters.range,
      from_ms: filters.from_ms,
      to_ms: filters.to_ms,
      limit: filters.limit,
      total_sampled: length(type_records),
      filtered_sampled: length(filtered_records),
      sample_limit: max(@flow_dashboard_sample_limit, length(type_records))
    }
  end

  @doc false
  @spec flow_states_opts_from_query(binary()) :: keyword()
  def flow_states_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:state, normalize_flow_state_filter(Map.get(params, "state")))
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:range, normalize_flow_range_filter(Map.get(params, "range")))
    |> maybe_put_query_opt(
      :from_ms,
      parse_flow_time_filter(
        Map.get(params, "from_ms") || Map.get(params, "from") || Map.get(params, "from_at")
      )
    )
    |> maybe_put_query_opt(
      :to_ms,
      parse_flow_time_filter(
        Map.get(params, "to_ms") || Map.get(params, "to") || Map.get(params, "to_at")
      )
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> Enum.reverse()
  end

  def flow_states_opts_from_query(_query), do: []

  @spec collect_flow_states_terminal_records(map(), [binary()]) :: [map()]
  defp collect_flow_states_terminal_records(filters, available_types) do
    terminal_states = flow_states_terminal_fetch_states(filters)

    if terminal_states == [] do
      []
    else
      filters
      |> flow_states_terminal_fetch_types(available_types)
      |> flow_fetch_terminal_records(
        terminal_states,
        max(filters.limit, @flow_dashboard_sample_limit)
      )
    end
  end

  @spec flow_states_terminal_fetch_states(map()) :: [binary()]
  defp flow_states_terminal_fetch_states(%{state: state}) when state in @flow_terminal_states,
    do: [state]

  defp flow_states_terminal_fetch_states(%{state: nil, type: type})
       when is_binary(type) and type != "",
       do: @flow_terminal_states

  defp flow_states_terminal_fetch_states(_filters), do: []

  @spec flow_states_terminal_fetch_types(map(), [binary()]) :: [binary()]
  defp flow_states_terminal_fetch_types(%{type: type}, _available_types)
       when is_binary(type) and type != "",
       do: [type]

  defp flow_states_terminal_fetch_types(_filters, available_types), do: available_types

  @spec flow_fetch_terminal_records([binary()], [binary()], pos_integer()) :: [map()]
  defp flow_fetch_terminal_records(types, terminal_states, limit) when limit > 0 do
    types
    |> Enum.reduce_while({[], limit}, fn type, {acc, remaining} ->
      if remaining <= 0 do
        {:halt, {acc, 0}}
      else
        records = flow_fetch_terminal_records_for_type(type, terminal_states, remaining)
        {:cont, {prepend_flow_dashboard_chunk(records, acc), max(remaining - length(records), 0)}}
      end
    end)
    |> elem(0)
    |> flatten_flow_dashboard_chunks()
    |> Enum.take(limit)
  end

  defp flow_fetch_terminal_records(_types, _terminal_states, _limit), do: []

  @spec flow_fetch_terminal_records_for_type(binary(), [binary()], pos_integer()) :: [map()]
  defp flow_fetch_terminal_records_for_type(type, terminal_states, limit) do
    terminal_states
    |> Enum.reduce_while({[], limit}, fn state, {acc, remaining} ->
      if remaining <= 0 do
        {:halt, {acc, 0}}
      else
        case flow_dashboard_terminal_records(type, state, remaining) do
          {:ok, records} ->
            {:cont,
             {prepend_flow_dashboard_chunk(records, acc), max(remaining - length(records), 0)}}

          {:error, _reason} ->
            {:cont, {acc, remaining}}
        end
      end
    end)
    |> elem(0)
    |> flatten_flow_dashboard_chunks()
  end

  @spec flow_dashboard_terminal_records(binary(), binary(), pos_integer()) ::
          {:ok, [map()]} | {:error, term()}
  defp flow_dashboard_terminal_records(type, state, limit) do
    opts = [
      state: state,
      count: limit,
      include_cold: true,
      consistent_projection: true
    ]

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_list(type, opts) end,
           flow_dashboard_list_fetch_timeout_ms(),
           :terminal_records
         ) do
      {:ok, {:ok, records}} when is_list(records) -> {:ok, records}
      {:ok, {:error, reason}} -> {:error, reason}
      {:ok, other} -> {:error, {:unexpected_flow_list_result, other}}
      {:error, reason} -> {:error, reason}
    end
  rescue
    reason -> {:error, reason}
  catch
    :exit, reason -> {:error, {:exit, reason}}
  end

  @doc "Renders the Flow state-centric page."
  @spec render_flow_states_page(map()) :: binary()
  def render_flow_states_page(data) do
    render_template(template_flow_states(%{data: data}))
  end

  @doc false
  @spec flow_states_page_filters(map()) :: map()
  def flow_states_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: Map.get(data, :type_filter),
      state: Map.get(data, :state_filter),
      q: Map.get(data, :name_filter),
      range: Map.get(data, :range_filter),
      from_ms: Map.get(data, :from_ms),
      to_ms: Map.get(data, :to_ms),
      limit: Map.get(data, :limit, @flow_dashboard_recent_limit)
    })
  end

  @doc false
  @spec flow_states_page_limit(map()) :: pos_integer()
  def flow_states_page_limit(data) when is_map(data),
    do: Map.get(data, :limit, @flow_dashboard_recent_limit)

  @doc "Collects data for the Flow workers and leases page."
  @spec collect_flow_workers_page() :: map()
  def collect_flow_workers_page do
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)

    %{
      workers: flow_worker_summaries(records),
      running_records: Enum.filter(records, &(flow_record_state(&1) == "running")),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit
    }
  end

  @doc "Renders the Flow workers and leases page."
  @spec render_flow_workers_page(map()) :: binary()
  def render_flow_workers_page(data) do
    render_template(template_flow_workers(%{data: data}))
  end

  @doc "Collects data for the Flow due and scheduled work page."
  @spec collect_flow_due_page() :: map()
  def collect_flow_due_page do
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)

    %{
      due_now: Enum.filter(records, &flow_due_now?/1),
      scheduled: records |> Enum.filter(&flow_scheduled_future?/1) |> flow_recent_records(80),
      total_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit
    }
  end

  @doc "Renders the Flow due and scheduled work page."
  @spec render_flow_due_page(map()) :: binary()
  def render_flow_due_page(data) do
    render_template(template_flow_due(%{data: data}))
  end

  @doc "Collects failed, stuck, and expired-lease Flow work for operator recovery."
  @spec collect_flow_failures_page(keyword()) :: map()
  def collect_flow_failures_page(opts \\ []) when is_list(opts) do
    filters = flow_failures_filters_from_opts(opts)
    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)

    available_types =
      sampled_records
      |> flow_available_types()
      |> maybe_include_flow_type(filters.type)

    {queried_records, exact_scan_status} =
      if filters.scan_exact do
        flow_recovery_query_records(filters, available_types)
      else
        {[], %{failures: :skipped, stuck: :skipped}}
      end

    records =
      sampled_records
      |> merge_flow_records(queried_records)
      |> filter_flow_records_by_type(filters.type)
      |> filter_flow_records_by_partition(filters.partition_key)
      |> filter_flow_records_by_name(filters.q)

    candidates =
      records
      |> Enum.filter(&flow_recovery_candidate?/1)
      |> Enum.sort_by(&flow_recovery_sort_key/1)
      |> Enum.take(filters.limit)

    %{
      candidates: candidates,
      summary: flow_recovery_summary(candidates),
      filters: filters,
      available_types: available_types,
      total_sampled: length(sampled_records),
      filtered_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      exact_scan_status: exact_scan_status,
      flash: Keyword.get(opts, :flash),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc false
  @spec flow_failures_opts_from_query(binary()) :: keyword()
  def flow_failures_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:scan_exact, normalize_flow_boolean_filter(Map.get(params, "exact")))
    |> maybe_put_query_opt(:flash, flow_failures_flash_from_params(params))
    |> Enum.reverse()
  end

  def flow_failures_opts_from_query(_query), do: []

  @doc false
  @spec flow_failures_flash_from_query(binary()) :: map() | nil
  def flow_failures_flash_from_query(query) when is_binary(query) do
    query
    |> URI.decode_query()
    |> flow_failures_flash_from_params()
  rescue
    _ -> nil
  end

  def flow_failures_flash_from_query(_query), do: nil

  @doc false
  @spec flow_failures_page_filters(map()) :: map()
  def flow_failures_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      partition_key: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_exact: false
    })
  end

  @doc "Runs the explicit recovery form on the Flow failures page."
  @spec apply_flow_failures_form(map()) :: {:ok, map()} | {:error, binary()}
  def apply_flow_failures_form(params) when is_map(params) do
    with "reclaim" <- Map.get(params, "action", "reclaim"),
         :ok <- flow_failures_reclaim_confirmed(params),
         {:ok, type} <- flow_dashboard_required_form_value(params, "type", "flow type"),
         {:ok, worker} <- flow_dashboard_optional_form_value(params, "worker"),
         {:ok, limit} <- flow_dashboard_form_positive_integer(params, "limit", 25, 200),
         {:ok, lease_ms} <-
           flow_dashboard_form_positive_integer(params, "lease_ms", 30_000, 3_600_000),
         partition_key = normalize_flow_partition_query(Map.get(params, "partition_key")),
         opts =
           [
             worker: worker || "dashboard-recovery",
             limit: limit,
             lease_ms: lease_ms
           ]
           |> maybe_put_query_opt(:partition_key, partition_key)
           |> Enum.reverse(),
         {:ok, reclaimed} <- flow_dashboard_flow_reclaim(type, opts) do
      {:ok, %{type: type, reclaimed: length(reclaimed), worker: opts[:worker]}}
    else
      other when other not in [{:error, nil}, nil] and is_binary(other) ->
        {:error, "ERR unsupported recovery action #{other}"}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}

      other ->
        {:error, "ERR unexpected recovery result: #{inspect(other, limit: 8)}"}
    end
  end

  def apply_flow_failures_form(_params), do: {:error, "ERR recovery form must be a map"}

  @spec flow_failures_reclaim_confirmed(map()) :: :ok | {:error, binary()}
  defp flow_failures_reclaim_confirmed(params) do
    case Map.get(params, "confirm_reclaim") do
      value when value in ["true", "on", "yes", "1"] ->
        :ok

      _ ->
        {:error, "ERR reclaim requires confirm_reclaim=true after reviewing expired leases"}
    end
  end

  @doc "Renders the Flow failures and recovery page."
  @spec render_flow_failures_page(map()) :: binary()
  def render_flow_failures_page(data) do
    render_template(template_flow_failures(%{data: data}))
  end

  @doc "Collects Flow lineage records by parent, root, or correlation id."
  @spec collect_flow_lineage_page(keyword()) :: map()
  def collect_flow_lineage_page(opts \\ []) when is_list(opts) do
    filters = flow_lineage_filters_from_opts(opts)
    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    result = flow_lineage_query_result(filters)

    %{
      filters: filters,
      result: result,
      records: Map.get(result, :records, []),
      summary: flow_lineage_summary(Map.get(result, :records, [])),
      hints: flow_lineage_hints(sampled_records),
      total_sampled: length(sampled_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc false
  @spec flow_lineage_opts_from_query(binary()) :: keyword()
  def flow_lineage_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:mode, normalize_flow_lineage_mode(Map.get(params, "mode")))
    |> maybe_put_query_opt(:target, normalize_flow_name_filter(Map.get(params, "id")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> Enum.reverse()
  end

  def flow_lineage_opts_from_query(_query), do: []

  @doc "Renders the Flow lineage page."
  @spec render_flow_lineage_page(map()) :: binary()
  def render_flow_lineage_page(data) do
    render_template(template_flow_lineage(%{data: data}))
  end

  @doc "Collects a safe Flow query explorer result."
  @spec collect_flow_query_page(keyword()) :: map()
  def collect_flow_query_page(opts \\ []) when is_list(opts) do
    filters = flow_query_filters_from_opts(opts)
    sampled_records = collect_flow_records_sample(@flow_dashboard_sample_limit)

    %{
      filters: filters,
      result: flow_query_execute(filters),
      available_types: flow_available_types(sampled_records),
      total_sampled: length(sampled_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc false
  @spec flow_query_opts_from_query(binary()) :: keyword()
  def flow_query_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:kind, normalize_flow_query_kind(Map.get(params, "kind")))
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:state, normalize_flow_state_filter(Map.get(params, "state")))
    |> maybe_put_query_opt(:id, normalize_flow_name_filter(Map.get(params, "id")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:from_ms, parse_flow_time_filter(Map.get(params, "from")))
    |> maybe_put_query_opt(:to_ms, parse_flow_time_filter(Map.get(params, "to")))
    |> maybe_put_query_opt(:rev, normalize_flow_boolean_filter(Map.get(params, "rev")))
    |> Enum.reverse()
  end

  def flow_query_opts_from_query(_query), do: []

  @doc "Renders the Flow query explorer page."
  @spec render_flow_query_page(map()) :: binary()
  def render_flow_query_page(data) do
    render_template(template_flow_query(%{data: data}))
  end

  @doc "Collects recent Flow signal events from a bounded Flow sample."
  @spec collect_flow_signals_page(keyword()) :: map()
  def collect_flow_signals_page(opts \\ []) when is_list(opts) do
    filters = flow_signals_filters_from_opts(opts)
    records = collect_flow_records_sample(@flow_dashboard_sample_limit)
    type_records = filter_flow_records_by_type(records, filters.type)
    filtered_records = filter_flow_records_by_name(type_records, filters.q)

    signals =
      if filters.scan_history do
        filtered_records
        |> flow_recent_records(@flow_dashboard_signal_flow_fetch_limit)
        |> Enum.flat_map(&flow_signal_rows_for_record/1)
        |> filter_flow_signal_rows(filters)
        |> Enum.sort_by(&flow_signal_sort_key/1, :desc)
        |> Enum.take(filters.limit)
      else
        []
      end

    %{
      signals: signals,
      filters: filters,
      available_types: flow_available_types(records),
      total_sampled: length(records),
      filtered_sampled: length(filtered_records),
      sample_limit: @flow_dashboard_sample_limit,
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc false
  @spec flow_signals_opts_from_query(binary()) :: keyword()
  def flow_signals_opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(:signal, normalize_flow_name_filter(Map.get(params, "signal")))
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:scan_history, normalize_flow_boolean_filter(Map.get(params, "scan")))
    |> Enum.reverse()
  end

  def flow_signals_opts_from_query(_query), do: []

  @doc "Renders the Flow signals page."
  @spec render_flow_signals_page(map()) :: binary()
  def render_flow_signals_page(data) do
    render_template(template_flow_signals(%{data: data}))
  end

  @doc false
  @spec flow_signals_page_filters(map()) :: map()
  def flow_signals_page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      signal: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_history: false
    })
  end

  @spec flow_failures_filters_from_opts(keyword()) :: map()
  defp flow_failures_filters_from_opts(opts) when is_list(opts) do
    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      scan_exact: normalize_flow_boolean_filter(Keyword.get(opts, :scan_exact))
    }
  end

  @spec flow_failures_flash_from_params(map()) :: map() | nil
  defp flow_failures_flash_from_params(params) when is_map(params) do
    case Map.get(params, "status") do
      "reclaimed" ->
        %{
          kind: :ok,
          message:
            "Reclaimed #{flow_retention_query_integer(params, "count")} expired lease(s) for #{Map.get(params, "type", "Flow")}"
        }

      "error" ->
        %{kind: :error, message: Map.get(params, "message", "Recovery action failed")}

      _ ->
        nil
    end
  end

  @spec flow_recovery_query_records(map(), [binary()]) ::
          {[map()],
           %{
             failures: :ok | :skipped | {:error, term()},
             stuck: :ok | :skipped | {:error, term()}
           }}
  defp flow_recovery_query_records(%{type: type} = filters, _available_types)
       when is_binary(type) and type != "" do
    flow_recovery_query_records_for_types([type], filters)
  end

  defp flow_recovery_query_records(filters, available_types) do
    available_types
    |> Enum.take(16)
    |> flow_recovery_query_records_for_types(filters)
  end

  @spec flow_recovery_query_records_for_types([binary()], map()) ::
          {[map()],
           %{
             failures: :ok | :skipped | {:error, term()},
             stuck: :ok | :skipped | {:error, term()}
           }}
  defp flow_recovery_query_records_for_types(types, filters) do
    timeout_ms = flow_dashboard_list_fetch_timeout_ms()

    opts =
      [
        count: filters.limit,
        include_cold: true,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    Enum.reduce(types, {[], %{failures: :skipped, stuck: :skipped}}, fn type,
                                                                        {acc_records, acc_status} ->
      {failures, failures_status} =
        flow_recovery_exact_source(fn -> flow_dashboard_flow_failures(type, opts) end, timeout_ms)

      {stuck, stuck_status} =
        flow_recovery_exact_source(
          fn -> flow_dashboard_flow_stuck(type, Keyword.put(opts, :older_than_ms, 0)) end,
          timeout_ms
        )

      {
        acc_records ++ failures ++ stuck,
        %{
          failures: flow_recovery_merge_status(acc_status.failures, failures_status),
          stuck: flow_recovery_merge_status(acc_status.stuck, stuck_status)
        }
      }
    end)
  end

  @spec flow_recovery_exact_source((-> term()), non_neg_integer()) ::
          {[map()], :ok | {:error, term()}}
  defp flow_recovery_exact_source(fun, timeout_ms) do
    case bounded_dashboard_call(fun, timeout_ms, :flow_recovery_exact) do
      {:ok, {:ok, records}} when is_list(records) ->
        {records, :ok}

      {:ok, {:error, reason}} ->
        {[], {:error, reason}}

      {:error, reason} ->
        {[], {:error, reason}}

      other ->
        {[], {:error, {:unexpected, other}}}
    end
  end

  @spec flow_recovery_merge_status(
          :ok | :skipped | {:error, term()},
          :ok | {:error, term()}
        ) :: :ok | :skipped | {:error, term()}
  defp flow_recovery_merge_status({:error, _} = error, _next), do: error
  defp flow_recovery_merge_status(_previous, {:error, _} = error), do: error
  defp flow_recovery_merge_status(:skipped, :ok), do: :ok
  defp flow_recovery_merge_status(:ok, :ok), do: :ok

  @spec flow_recovery_candidate?(map()) :: boolean()
  defp flow_recovery_candidate?(record) do
    flow_failed?(record) or flow_expired_lease?(record) or flow_max_attempts_reached?(record)
  end

  @spec flow_recovery_sort_key(map()) :: {integer(), integer(), binary()}
  defp flow_recovery_sort_key(record) do
    priority =
      cond do
        flow_expired_lease?(record) -> 0
        flow_failed?(record) -> 1
        flow_max_attempts_reached?(record) -> 2
        true -> 3
      end

    {priority, -flow_record_updated_at_ms(record), flow_record_id(record)}
  end

  @spec flow_recovery_summary([map()]) :: map()
  defp flow_recovery_summary(records) do
    %{
      total: length(records),
      failed: Enum.count(records, &flow_failed?/1),
      expired_leases: Enum.count(records, &flow_expired_lease?/1),
      maxed: Enum.count(records, &flow_max_attempts_reached?/1)
    }
  end

  @spec flow_recovery_reason(map()) :: binary()
  defp flow_recovery_reason(record) do
    cond do
      flow_expired_lease?(record) -> "expired running lease"
      flow_failed?(record) -> "terminal failed"
      flow_max_attempts_reached?(record) -> "retry attempts exhausted"
      flow_retrying?(record) -> "retrying"
      true -> "needs attention"
    end
  end

  @spec maybe_include_flow_type([binary()], binary() | nil) :: [binary()]
  defp maybe_include_flow_type(types, type) when is_binary(type) and type != "" do
    types
    |> Kernel.++([type])
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp maybe_include_flow_type(types, _type), do: types

  @spec flow_dashboard_required_form_value(map(), binary(), binary()) ::
          {:ok, binary()} | {:error, binary()}
  defp flow_dashboard_required_form_value(params, key, label) do
    case flow_dashboard_optional_form_value(params, key) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR #{label} is required"}
    end
  end

  @spec flow_dashboard_optional_form_value(map(), binary()) :: {:ok, binary() | nil}
  defp flow_dashboard_optional_form_value(params, key) when is_map(params) do
    value =
      params
      |> Map.get(key, "")
      |> to_string()
      |> String.trim()

    {:ok, if(value == "", do: nil, else: value)}
  end

  @spec flow_dashboard_form_positive_integer(map(), binary(), pos_integer(), pos_integer()) ::
          {:ok, pos_integer()} | {:error, binary()}
  defp flow_dashboard_form_positive_integer(params, key, default, max_value) do
    value = params |> Map.get(key, "") |> to_string() |> String.trim()

    case value do
      "" ->
        {:ok, default}

      _ ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= 1 and parsed <= max_value ->
            {:ok, parsed}

          {parsed, ""} when parsed > max_value ->
            {:error, "ERR #{key} exceeds maximum #{max_value}"}

          _ ->
            {:error, "ERR #{key} must be a positive integer"}
        end
    end
  end

  @spec flow_lineage_filters_from_opts(keyword()) :: map()
  defp flow_lineage_filters_from_opts(opts) when is_list(opts) do
    %{
      mode: normalize_flow_lineage_mode(Keyword.get(opts, :mode)),
      target: normalize_flow_name_filter(Keyword.get(opts, :target)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit))
    }
  end

  @spec normalize_flow_lineage_mode(term()) :: binary()
  defp normalize_flow_lineage_mode("parent"), do: "parent"
  defp normalize_flow_lineage_mode("root"), do: "root"
  defp normalize_flow_lineage_mode("correlation"), do: "correlation"
  defp normalize_flow_lineage_mode(_mode), do: "root"

  @spec flow_lineage_query_result(map()) :: map()
  defp flow_lineage_query_result(%{target: nil}) do
    %{status: :idle, records: [], command: "FLOW.BY_ROOT", message: "Enter a lineage id"}
  end

  defp flow_lineage_query_result(%{target: target, mode: mode} = filters) do
    opts =
      [
        count: filters.limit,
        include_cold: true,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    {command, fun} =
      case mode do
        "parent" ->
          {"FLOW.BY_PARENT", fn -> flow_dashboard_flow_by_parent(target, opts) end}

        "correlation" ->
          {"FLOW.BY_CORRELATION", fn -> flow_dashboard_flow_by_correlation(target, opts) end}

        _ ->
          {"FLOW.BY_ROOT", fn -> flow_dashboard_flow_by_root(target, opts) end}
      end

    case bounded_dashboard_call(fun, flow_dashboard_list_fetch_timeout_ms(), :lineage) do
      {:ok, {:ok, records}} when is_list(records) ->
        %{status: :ok, command: command, records: records, message: "#{length(records)} records"}

      {:ok, {:error, reason}} ->
        %{status: :error, command: command, records: [], message: inspect(reason)}

      {:error, :timeout} ->
        %{status: :timeout, command: command, records: [], message: "query timed out"}

      {:error, reason} ->
        %{status: :error, command: command, records: [], message: inspect(reason)}

      _ ->
        %{status: :error, command: command, records: [], message: "unexpected query result"}
    end
  end

  @spec flow_lineage_summary([map()]) :: map()
  defp flow_lineage_summary(records) do
    terminal = Enum.count(records, &(flow_record_state(&1) in @flow_terminal_states))

    %{
      total: length(records),
      active: max(length(records) - terminal, 0),
      terminal: terminal,
      failed: Enum.count(records, &flow_failed?/1)
    }
  end

  @spec flow_lineage_hints([map()]) :: [map()]
  defp flow_lineage_hints(records) do
    records
    |> Enum.flat_map(fn record ->
      [
        %{mode: "root", label: "root", id: flow_record_root_id(record)},
        %{mode: "parent", label: "parent", id: flow_record_parent_id(record)},
        %{mode: "correlation", label: "correlation", id: flow_record_correlation_id(record)}
      ]
    end)
    |> Enum.filter(&(is_binary(&1.id) and &1.id != ""))
    |> Enum.uniq_by(fn hint -> {hint.mode, hint.id} end)
    |> Enum.take(8)
  end

  @spec flow_query_filters_from_opts(keyword()) :: map()
  defp flow_query_filters_from_opts(opts) when is_list(opts) do
    %{
      kind: normalize_flow_query_kind(Keyword.get(opts, :kind)),
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      state: normalize_flow_state_filter(Keyword.get(opts, :state)),
      id: normalize_flow_name_filter(Keyword.get(opts, :id)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      from_ms: Keyword.get(opts, :from_ms),
      to_ms: Keyword.get(opts, :to_ms),
      rev: Keyword.get(opts, :rev) == true
    }
  end

  @spec normalize_flow_query_kind(term()) :: binary()
  defp normalize_flow_query_kind("terminals"), do: "terminals"
  defp normalize_flow_query_kind("failures"), do: "failures"
  defp normalize_flow_query_kind("stuck"), do: "stuck"
  defp normalize_flow_query_kind("history"), do: "history"
  defp normalize_flow_query_kind("by_parent"), do: "by_parent"
  defp normalize_flow_query_kind("by_root"), do: "by_root"
  defp normalize_flow_query_kind("by_correlation"), do: "by_correlation"
  defp normalize_flow_query_kind(_kind), do: "list"

  @spec normalize_flow_boolean_filter(term()) :: boolean()
  defp normalize_flow_boolean_filter(value) when value in [true, "true", "1", "on", "yes"],
    do: true

  defp normalize_flow_boolean_filter(_value), do: false

  @spec flow_query_execute(map()) :: map()
  defp flow_query_execute(filters) do
    case flow_query_plan(filters) do
      {:ok, command, fun} ->
        case bounded_dashboard_call(fun, flow_dashboard_list_fetch_timeout_ms(), :query) do
          {:ok, {:ok, rows}} when is_list(rows) ->
            %{status: :ok, command: command, rows: rows, message: "#{length(rows)} row(s)"}

          {:ok, {:ok, row}} ->
            %{status: :ok, command: command, rows: List.wrap(row), message: "1 row"}

          {:ok, {:error, reason}} ->
            %{status: :error, command: command, rows: [], message: inspect(reason)}

          {:error, :timeout} ->
            %{status: :timeout, command: command, rows: [], message: "query timed out"}

          {:error, reason} ->
            %{status: :error, command: command, rows: [], message: inspect(reason)}

          _other ->
            %{status: :error, command: command, rows: [], message: "unexpected query result"}
        end

      {:idle, command, message} ->
        %{status: :idle, command: command, rows: [], message: message}
    end
  end

  @spec flow_query_plan(map()) :: {:ok, binary(), (-> term())} | {:idle, binary(), binary()}
  defp flow_query_plan(%{kind: kind, type: type})
       when kind in ["list", "terminals", "failures", "stuck"] and
              (not is_binary(type) or type == "") do
    {:idle, flow_query_kind_command(kind), "Enter a workflow type"}
  end

  defp flow_query_plan(%{kind: kind, id: id} = _filters)
       when kind in ["history", "by_parent", "by_root", "by_correlation"] and
              (not is_binary(id) or id == "") do
    {:idle, flow_query_kind_command(kind), "Enter an id"}
  end

  defp flow_query_plan(%{kind: "history", id: id} = filters) do
    opts =
      [count: filters.limit, values: false, consistent_projection: true]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    {:ok, "FLOW.HISTORY", fn -> flow_dashboard_flow_history(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_parent", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_PARENT", fn -> flow_dashboard_flow_by_parent(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_root", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_ROOT", fn -> flow_dashboard_flow_by_root(id, opts) end}
  end

  defp flow_query_plan(%{kind: "by_correlation", id: id} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.BY_CORRELATION", fn -> flow_dashboard_flow_by_correlation(id, opts) end}
  end

  defp flow_query_plan(%{kind: "terminals", type: type} = filters) do
    opts = flow_query_terminal_opts(filters)
    {:ok, "FLOW.TERMINALS", fn -> flow_dashboard_flow_terminals(type, opts) end}
  end

  defp flow_query_plan(%{kind: "failures", type: type} = filters) do
    opts = flow_query_terminal_opts(filters)
    {:ok, "FLOW.FAILURES", fn -> flow_dashboard_flow_failures(type, opts) end}
  end

  defp flow_query_plan(%{kind: "stuck", type: type} = filters) do
    opts = flow_query_index_opts(filters)
    {:ok, "FLOW.STUCK", fn -> flow_dashboard_flow_stuck(type, opts) end}
  end

  defp flow_query_plan(%{type: type} = filters) do
    opts =
      flow_query_index_opts(filters)
      |> maybe_put_query_opt(:state, filters.state)

    {:ok, "FLOW.LIST", fn -> flow_dashboard_flow_list(type, opts) end}
  end

  @spec flow_query_index_opts(map()) :: keyword()
  defp flow_query_index_opts(filters) do
    [
      count: filters.limit,
      include_cold: true,
      consistent_projection: true
    ]
    |> maybe_put_query_opt(:partition_key, filters.partition_key)
    |> maybe_put_query_opt(:from_ms, filters.from_ms)
    |> maybe_put_query_opt(:to_ms, filters.to_ms)
    |> maybe_put_query_opt(:rev, if(filters.rev, do: true, else: nil))
    |> Enum.reverse()
  end

  @spec flow_query_terminal_opts(map()) :: keyword()
  defp flow_query_terminal_opts(filters) do
    filters
    |> flow_query_index_opts()
    |> maybe_put_query_opt(:state, filters.state)
  end

  @spec flow_query_kind_command(binary()) :: binary()
  defp flow_query_kind_command("terminals"), do: "FLOW.TERMINALS"
  defp flow_query_kind_command("failures"), do: "FLOW.FAILURES"
  defp flow_query_kind_command("stuck"), do: "FLOW.STUCK"
  defp flow_query_kind_command("history"), do: "FLOW.HISTORY"
  defp flow_query_kind_command("by_parent"), do: "FLOW.BY_PARENT"
  defp flow_query_kind_command("by_root"), do: "FLOW.BY_ROOT"
  defp flow_query_kind_command("by_correlation"), do: "FLOW.BY_CORRELATION"
  defp flow_query_kind_command(_kind), do: "FLOW.LIST"

  @doc false
  @spec collect_flow_projection_health() :: map()
  def collect_flow_projection_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms, 0),
      history_flush_interval_ms:
        Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms, 0),
      metrics: collect_flow_projection_metrics()
    }
  end

  @doc """
  Collects record and history data for one Flow detail page.
  """
  @spec collect_flow_detail_page(binary(), keyword()) :: map()
  def collect_flow_detail_page(id, opts \\ [])

  def collect_flow_detail_page(id, opts) when is_binary(id) and is_list(opts) do
    partition_key = flow_detail_partition_key(opts)
    history_page_opts = flow_detail_history_page_opts(opts)
    {record_status, record} = flow_detail_record(id, partition_key)
    {history_status, history, history_page} = flow_detail_history(id, record, history_page_opts)

    {values_status, value_refs, values_by_ref} =
      if Keyword.get(opts, :values, true) do
        flow_detail_values(record, history)
      else
        {:skipped, [], %{}}
      end

    record_partition_key = if is_map(record), do: flow_record_partition_key(record), else: nil
    detail_partition_key = flow_detail_url_partition_key(partition_key || record_partition_key)
    history_page = flow_detail_history_page_links(id, detail_partition_key, history_page)

    %{
      id: id,
      partition_key: detail_partition_key,
      record: record,
      record_status: record_status,
      history: history,
      history_status: history_status,
      history_page: history_page,
      flash: Keyword.get(opts, :flash),
      value_refs: value_refs,
      values_by_ref: values_by_ref,
      values_status: values_status,
      waiting_reason: flow_waiting_reason(record),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc """
  Renders the Flow detail page.
  """
  @spec render_flow_detail_page(map()) :: binary()
  def render_flow_detail_page(data) do
    render_template(template_flow_detail(%{data: data}))
  end

  @spec render_template(binary()) :: binary()
  defp render_template(html), do: String.trim_leading(html)

  @spec live_component_payload(%{binary() => binary()}) :: {:ok, map()}
  defp live_component_payload(components) do
    {:ok, %{generated_at_ms: System.system_time(:millisecond), components: components}}
  end

  @spec render_overview_live_components(dashboard_data()) :: %{binary() => binary()}
  defp render_overview_live_components(data) do
    %{
      "top_bar" => render_top_bar(data),
      "sidebar" => render_sidebar(data, "overview"),
      "content" => render_overview_content(data),
      "footer" => render_footer(data)
    }
  end

  @spec render_overview_content(dashboard_data()) :: binary()
  defp render_overview_content(data) do
    """
    #{render_cache_performance(data.hotcold)}
    #{render_lifecycle(data.lifecycle)}
    #{render_shards(data.shards)}
    #{render_memory_alert(data.memory)}
    #{render_connections(data.connections)}
    """
  end

  @spec render_flow_live_components(map()) :: %{binary() => binary()}
  defp render_flow_live_components(data) do
    %{
      "flow_overview" =>
        render_flow_overview(data.summary, data.filtered_sampled, data.sample_limit),
      "flow_issue_cards" => render_flow_issue_cards(data.summary),
      "flow_projection_health" =>
        render_flow_projection_health(
          Map.get(data, :projection, default_flow_projection_health())
        ),
      "flow_state_breakdown" => render_flow_state_breakdown(data.types),
      "flow_workers" => render_flow_workers(data.workers),
      "flow_recent_records" => render_flow_recent_records(data.records)
    }
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

      %{
        index: index,
        status: status,
        keys: keys,
        ets_memory_bytes: ets_mem,
        disk_bytes: disk_bytes
      }
    end)
  end

  @spec collect_hotcold() :: hotcold_data()
  defp collect_hotcold do
    rate = :persistent_term.get(:ferricstore_read_sample_rate, 100)
    _hits_sampled = Stats.keyspace_hits()
    misses_sampled = Stats.keyspace_misses()
    hot_sampled = Stats.total_hot_reads()
    cold_sampled = Stats.total_cold_reads()

    # hot_reads, keyspace_hits, and keyspace_misses are sampled; cold_reads are exact.
    hot_est = hot_sampled * rate
    misses_est = misses_sampled * rate
    # NOT sampled -- called on every cold read
    cold_exact = cold_sampled
    total_hits = hot_est + cold_exact
    total_lookups = total_hits + misses_est

    uptime = max(Stats.uptime_seconds(), 1)

    %{
      hot_read_pct: Stats.hot_read_pct(),
      cold_reads_per_sec: Stats.cold_reads_per_second(),
      total_hot: hot_est,
      total_cold: cold_exact,
      total_hits: total_hits,
      total_misses: misses_est,
      total_lookups: total_lookups,
      hit_ratio:
        if(total_lookups > 0, do: Float.round(total_hits / total_lookups * 100, 1), else: 0.0),
      ram_ratio: if(total_hits > 0, do: Float.round(hot_est / total_hits * 100, 1), else: 0.0),
      disk_ratio:
        if(total_hits > 0, do: Float.round(cold_exact / total_hits * 100, 1), else: 0.0),
      sample_rate: rate,
      hits_per_sec: Float.round(total_hits / uptime, 1),
      misses_per_sec: Float.round(misses_est / uptime, 1),
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
    {total_disk, _, _} = scan_storage_tree(data_dir)
    %{total_disk_bytes: total_disk}
  end

  # Scans a shard directory for disk usage and file counts.
  # Returns {total_bytes, data_file_count, hint_file_count}.
  @spec scan_shard_dir(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  defp scan_shard_dir(shard_dir), do: scan_storage_tree(shard_dir)

  @spec scan_storage_tree(binary()) :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}
  defp scan_storage_tree(path) when is_binary(path) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: size}} ->
        file = Path.basename(path)

        {size, if(String.ends_with?(file, ".log"), do: 1, else: 0),
         if(String.ends_with?(file, ".hint"), do: 1, else: 0)}

      {:ok, %{type: :directory}} ->
        case Ferricstore.FS.ls(path) do
          {:ok, files} ->
            Enum.reduce(files, {0, 0, 0}, fn file, {bytes, data, hints} ->
              {child_bytes, child_data, child_hints} = scan_storage_tree(Path.join(path, file))
              {bytes + child_bytes, data + child_data, hints + child_hints}
            end)

          {:error, _reason} ->
            {0, 0, 0}
        end

      {:ok, _other} ->
        {0, 0, 0}

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

  @spec keyspace_filters(keyword() | map()) :: map()
  defp keyspace_filters(opts) do
    key = dashboard_param(opts, "key") |> String.trim()
    prefix = dashboard_param(opts, "prefix") |> String.trim()

    %{
      key: key,
      prefix: prefix,
      include_internal: truthy_dashboard_param?(dashboard_param(opts, "include_internal")),
      limit:
        opts
        |> dashboard_param("limit")
        |> parse_bounded_int(@keyspace_dashboard_default_limit, 1, @keyspace_dashboard_max_limit)
    }
  end

  @spec collect_keyspace_rows(map()) :: {[map()], non_neg_integer()}
  defp collect_keyspace_rows(%{key: key} = filters) when key != "" do
    rows =
      0..(shard_count() - 1)
      |> Enum.flat_map(fn index ->
        keydir = :"keydir_#{index}"

        [key, CompoundKey.type_key(key), CompoundKey.list_meta_key(key)]
        |> Enum.flat_map(&lookup_keyspace_row(keydir, index, &1))
      end)
      |> Enum.uniq_by(& &1.physical_key)
      |> Enum.reject(&(not filters.include_internal and &1.internal? and &1.key != key))
      |> Enum.take(filters.limit)

    {rows, length(rows)}
  end

  defp collect_keyspace_rows(filters) do
    {rows, scanned} =
      Enum.reduce_while(0..(shard_count() - 1), {[], 0}, fn index, {rows, scanned} ->
        remaining = filters.limit - length(rows)

        if remaining <= 0 do
          {:halt, {rows, scanned}}
        else
          keydir = :"keydir_#{index}"
          {shard_rows, shard_scanned} = sample_keyspace_rows(keydir, index, filters, remaining)
          {:cont, {rows ++ shard_rows, scanned + shard_scanned}}
        end
      end)

    {Enum.take(rows, filters.limit), scanned}
  end

  defp lookup_keyspace_row(keydir, index, key) do
    try do
      keydir
      |> :ets.lookup(key)
      |> Enum.map(&keyspace_entry_row(index, &1))
    rescue
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  defp sample_keyspace_rows(keydir, index, filters, remaining) do
    match_spec = [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6", :"$7"}, [], [:"$_"]}]

    try do
      case :ets.select(keydir, match_spec, @keyspace_dashboard_select_batch) do
        :"$end_of_table" ->
          {[], 0}

        {entries, continuation} ->
          continue_keyspace_rows(entries, continuation, index, filters, remaining, [], 0)
      end
    rescue
      ArgumentError -> {[], 0}
    catch
      :exit, _ -> {[], 0}
    end
  end

  defp continue_keyspace_rows(entries, continuation, index, filters, remaining, rows, scanned) do
    {rows, scanned} =
      Enum.reduce_while(entries, {rows, scanned}, fn entry, {acc, scan_acc} ->
        row = keyspace_entry_row(index, entry)

        cond do
          length(acc) >= remaining ->
            {:halt, {acc, scan_acc}}

          keyspace_row_matches?(row, filters) ->
            {:cont, {[row | acc], scan_acc + 1}}

          true ->
            {:cont, {acc, scan_acc + 1}}
        end
      end)

    cond do
      length(rows) >= remaining ->
        {Enum.reverse(rows), scanned}

      continuation == :"$end_of_table" ->
        {Enum.reverse(rows), scanned}

      true ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            {Enum.reverse(rows), scanned}

          {next_entries, next_continuation} ->
            continue_keyspace_rows(
              next_entries,
              next_continuation,
              index,
              filters,
              remaining,
              rows,
              scanned
            )
        end
    end
  end

  defp keyspace_entry_row(
         index,
         {physical_key, value, expire_at_ms, lfu, file_id, offset, value_size}
       ) do
    %{
      key: keyspace_logical_key(physical_key),
      physical_key: physical_key,
      shard: index,
      type: keyspace_entry_type(physical_key, value),
      ttl: keyspace_ttl_label(expire_at_ms),
      size: keyspace_size_label(value, value_size),
      location: keyspace_location_label(value, file_id),
      lfu: lfu,
      offset: offset,
      internal?: CompoundKey.internal_key?(physical_key)
    }
  end

  defp keyspace_logical_key(<<"T:", logical::binary>>), do: logical
  defp keyspace_logical_key(key), do: CompoundKey.extract_redis_key(key)

  defp keyspace_row_matches?(row, filters) do
    internal_ok? = filters.include_internal or not row.internal?
    prefix_ok? = filters.prefix == "" or String.starts_with?(row.key, filters.prefix)
    internal_ok? and prefix_ok?
  end

  defp keyspace_entry_type(<<"T:", _rest::binary>>, value) when is_binary(value), do: value
  defp keyspace_entry_type(<<"H:", _rest::binary>>, _value), do: "hash field"
  defp keyspace_entry_type(<<"L:", _rest::binary>>, _value), do: "list element"
  defp keyspace_entry_type(<<"LM:", _rest::binary>>, _value), do: "list metadata"
  defp keyspace_entry_type(<<"S:", _rest::binary>>, _value), do: "set member"
  defp keyspace_entry_type(<<"Z:", _rest::binary>>, _value), do: "zset member"
  defp keyspace_entry_type(<<"X:", _rest::binary>>, _value), do: "stream record"
  defp keyspace_entry_type(<<"V:", _rest::binary>>, _value), do: "flow value"
  defp keyspace_entry_type(<<"VM:", _rest::binary>>, _value), do: "flow value metadata"
  defp keyspace_entry_type(<<"PM:", _rest::binary>>, _value), do: "promotion marker"
  defp keyspace_entry_type(_key, _value), do: "string"

  defp keyspace_ttl_label(0), do: "none"

  defp keyspace_ttl_label(expire_at_ms) when is_integer(expire_at_ms) do
    remaining = expire_at_ms - System.system_time(:millisecond)
    if remaining > 0, do: format_duration_ms(remaining), else: "expired"
  end

  defp keyspace_ttl_label(_), do: "-"

  defp keyspace_size_label(value, _value_size) when is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{size: logical_size}} -> "#{format_bytes(logical_size)} blob"
      :error -> format_bytes(byte_size(value))
    end
  end

  defp keyspace_size_label(_value, value_size) when is_integer(value_size) and value_size >= 0,
    do: format_bytes(value_size)

  defp keyspace_size_label(_value, _value_size), do: "-"

  defp keyspace_location_label(value, _file_id) when is_binary(value) do
    if BlobRef.ref?(value), do: "blob ref", else: "hot"
  end

  defp keyspace_location_label(_value, :pending), do: "pending"
  defp keyspace_location_label(_value, {:flow_history, _file_id}), do: "flow history"
  defp keyspace_location_label(_value, {:waraft_segment, _index}), do: "segment cold"
  defp keyspace_location_label(_value, {:waraft_projection, _index}), do: "projection cold"
  defp keyspace_location_label(_value, {:waraft_apply_projection, _index}), do: "projection cold"

  defp keyspace_location_label(_value, file_id) when is_integer(file_id) and file_id >= 0,
    do: "bitcask cold"

  defp keyspace_location_label(_value, _file_id), do: "unknown"

  defp inspect_keyspace_key("", _rows), do: nil

  defp inspect_keyspace_key(key, rows) do
    case Enum.find(rows, &(&1.key == key or &1.physical_key == key)) do
      nil ->
        %{key: key, found?: false, type: "none", ttl: "-", size: "-", location: "-", shard: "-"}

      row ->
        %{
          key: key,
          found?: true,
          type: row.type,
          ttl: row.ttl,
          size: row.size,
          location: row.location,
          shard: row.shard
        }
    end
  end

  defp dashboard_param(opts, key) when is_map(opts), do: Map.get(opts, key, "")

  defp dashboard_param(opts, key) when is_list(opts) do
    Keyword.get(opts, String.to_atom(key), Keyword.get(opts, key, ""))
  end

  defp dashboard_param(_opts, _key), do: ""

  defp truthy_dashboard_param?(value) when value in [true, "true", "1", "on", "yes"], do: true
  defp truthy_dashboard_param?(_value), do: false

  defp parse_bounded_int(value, default, min_value, max_value) do
    parsed =
      cond do
        is_integer(value) ->
          value

        is_binary(value) ->
          case Integer.parse(value) do
            {int, ""} -> int
            _ -> default
          end

        true ->
          default
      end

    parsed |> max(min_value) |> min(max_value)
  end

  defp group_slowlog_by_command(entries) do
    entries
    |> Enum.group_by(fn entry ->
      case entry.command do
        [cmd | _] -> cmd |> to_string() |> String.upcase()
        _ -> "(unknown)"
      end
    end)
    |> Enum.map(fn {command, grouped} ->
      total = Enum.reduce(grouped, 0, fn entry, acc -> acc + entry.duration_us end)
      count = length(grouped)

      %{
        command: command,
        count: count,
        worst_us: Enum.reduce(grouped, 0, fn entry, acc -> max(acc, entry.duration_us) end),
        avg_us: if(count > 0, do: div(total, count), else: 0)
      }
    end)
    |> Enum.sort_by(& &1.worst_us, :desc)
  end

  defp kv_command_groups do
    [
      %{
        name: "Strings",
        purpose: "Primary KV read/write path.",
        commands: ~w(GET MGET SET MSET DEL EXISTS TTL PTTL EXPIRE PERSIST TYPE)
      },
      %{
        name: "Structured Values",
        purpose: "Redis-compatible compound primitives stored as internal keys.",
        commands:
          ~w(HGET HSET HMGET HGETALL LPUSH RPUSH LPOP RPOP SADD SMEMBERS ZADD ZRANGE XADD XREAD)
      },
      %{
        name: "Large / Cold Values",
        purpose: "Debug large values, sendfile candidates, and cold reads.",
        commands: ~w(GET MGET STRLEN FERRICSTORE.KEY_INFO FERRICSTORE.HOTNESS)
      },
      %{
        name: "Operational",
        purpose: "Observability and maintenance commands used by the dashboard.",
        commands: ~w(INFO SLOWLOG CONFIG MEMORY CLIENT SCAN)
      }
    ]
  end

  @spec collect_flow_summary() :: map()
  defp collect_flow_summary do
    records = collect_flow_records_sample(160)
    types = flow_type_summaries(records)
    flow_page_summary(types, records)
  end

  @spec collect_flow_records_sample(pos_integer()) :: [map()]
  defp collect_flow_records_sample(limit) when is_integer(limit) and limit > 0 do
    sc = max(shard_count(), 1)
    per_shard = max(1, div(limit + sc - 1, sc))

    0..(sc - 1)
    |> Enum.flat_map(&collect_flow_records_from_keydir(&1, per_shard))
    |> Enum.take(limit)
  end

  defp collect_flow_records_sample(_limit), do: []

  @spec collect_flow_records_from_keydir(non_neg_integer(), pos_integer()) :: [map()]
  defp collect_flow_records_from_keydir(index, per_shard) do
    keydir = :"keydir_#{index}"

    try do
      collect_flow_records_from_keydir_select(
        keydir,
        per_shard,
        max(@flow_dashboard_keydir_scan_floor, per_shard * @flow_dashboard_keydir_scan_multiplier)
      )
    rescue
      ArgumentError -> []
    catch
      :exit, _ -> []
    end
  end

  @spec collect_flow_records_from_keydir_select(atom(), pos_integer(), pos_integer()) :: [map()]
  defp collect_flow_records_from_keydir_select(keydir, wanted, scan_limit) do
    match_spec = [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}]

    case :ets.select(keydir, match_spec, @flow_dashboard_keydir_select_batch) do
      :"$end_of_table" ->
        []

      {keys, continuation} ->
        collect_flow_records_from_keydir_continue(
          keys,
          continuation,
          wanted,
          scan_limit,
          [],
          0,
          0
        )
    end
  end

  defp collect_flow_records_from_keydir_continue(
         keys,
         continuation,
         wanted,
         scan_limit,
         records,
         record_count,
         scanned
       ) do
    {records, record_count} =
      Enum.reduce_while(keys, {records, record_count}, fn key, {acc, count} ->
        cond do
          count >= wanted ->
            {:halt, {acc, count}}

          record = flow_record_from_state_key(key) ->
            {:cont, {[record | acc], count + 1}}

          true ->
            {:cont, {acc, count}}
        end
      end)

    scanned = scanned + length(keys)

    cond do
      record_count >= wanted or scanned >= scan_limit ->
        Enum.reverse(records)

      continuation == :"$end_of_table" ->
        Enum.reverse(records)

      true ->
        case :ets.select(continuation) do
          :"$end_of_table" ->
            Enum.reverse(records)

          {next_keys, next_continuation} ->
            collect_flow_records_from_keydir_continue(
              next_keys,
              next_continuation,
              wanted,
              scan_limit,
              records,
              record_count,
              scanned
            )
        end
    end
  end

  @spec flow_record_from_state_key(binary() | nil) :: map() | nil
  defp flow_record_from_state_key(key) when is_binary(key) do
    if Ferricstore.Flow.Keys.state_key?(key) do
      case FerricStore.get(key) do
        {:ok, value} when is_binary(value) ->
          value
          |> safe_decode_flow_record()
          |> case do
            nil -> nil
            record -> Map.put_new(record, :dashboard_state_key, key)
          end

        _ ->
          nil
      end
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp flow_record_from_state_key(_key), do: nil

  @spec safe_decode_flow_record(binary()) :: map() | nil
  defp safe_decode_flow_record(value) do
    case Ferricstore.Flow.decode_record(value) do
      record when is_map(record) -> record
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  @spec flow_type_summaries([map()]) :: [map()]
  defp flow_type_summaries(records) do
    records
    |> Enum.group_by(&flow_record_type/1)
    |> Enum.reject(fn {type, _records} -> type in [nil, ""] end)
    |> Enum.map(fn {type, type_records} ->
      exact_info = safe_flow_info(type)
      counts = flow_counts_for_type(type_records, exact_info)

      counts
      |> Map.put(:type, type)
      |> Map.put(:sampled, length(type_records))
      |> Map.put(:exact, Map.get(counts, :count_source) == :exact)
    end)
    |> Enum.sort_by(fn type ->
      -(Map.get(type, :active, 0) + Map.get(type, :failed, 0) + Map.get(type, :queued, 0))
    end)
  end

  @spec safe_flow_info(binary()) :: map() | nil
  defp safe_flow_info(type) do
    case FerricStore.flow_info(type) do
      {:ok, info} when is_map(info) -> info
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  @spec flow_counts_for_type([map()], map() | nil) :: map()
  defp flow_counts_for_type(records, info) when is_map(info) do
    states = flow_state_counts(records)
    sampled = flow_counts_for_type(records, nil)
    queued = flow_count(info, :queued)
    running = flow_count(info, :running)
    completed = flow_count(info, :completed)
    failed = flow_count(info, :failed)
    cancelled = flow_count(info, :cancelled)
    terminal = completed + failed + cancelled
    total = queued + running + terminal

    if total < sampled.total do
      sampled
    else
      %{
        total: total,
        active: queued + running,
        queued: queued,
        running: running,
        completed: completed,
        failed: failed,
        cancelled: cancelled,
        terminal: terminal,
        inflight: flow_count(info, :inflight),
        states: states,
        count_source: :exact
      }
    end
  end

  defp flow_counts_for_type(records, _info) do
    states = flow_state_counts(records)
    queued = Map.get(states, "queued", 0)
    running = Map.get(states, "running", 0)
    completed = Map.get(states, "completed", 0)
    failed = Map.get(states, "failed", 0)
    cancelled = Map.get(states, "cancelled", 0)
    terminal = completed + failed + cancelled
    total = length(records)

    %{
      total: total,
      active: max(total - terminal, 0),
      queued: queued,
      running: running,
      completed: completed,
      failed: failed,
      cancelled: cancelled,
      terminal: terminal,
      inflight: running,
      states: states,
      count_source: :sampled
    }
  end

  @spec flow_count(map(), atom()) :: non_neg_integer()
  defp flow_count(map, key) when is_map(map) do
    case flow_field(map, key, 0) do
      n when is_integer(n) and n >= 0 -> n
      _ -> 0
    end
  end

  @spec flow_state_counts([map()]) :: %{binary() => non_neg_integer()}
  defp flow_state_counts(records) do
    Enum.reduce(records, %{}, fn record, acc ->
      Map.update(acc, flow_record_state(record), 1, &(&1 + 1))
    end)
  end

  @spec flow_available_types([map()]) :: [binary()]
  defp flow_available_types(records) do
    records
    |> Enum.map(&flow_record_type/1)
    |> Enum.reject(&(&1 in ["", "unknown"]))
    |> Enum.uniq()
    |> Enum.sort()
  end

  @spec collect_flow_policy_type_scan(pos_integer()) :: map()
  defp collect_flow_policy_type_scan(limit) when is_integer(limit) and limit > 0 do
    sc = max(shard_count(), 1)

    0..(sc - 1)
    |> Enum.reduce_while(%{types: MapSet.new(), scanned_entries: 0, truncated: false}, fn
      _index, %{scanned_entries: scanned} = acc when scanned >= limit ->
        {:halt, %{acc | truncated: true}}

      index, acc ->
        remaining = max(limit - acc.scanned_entries, 0)
        scan = collect_flow_policy_types_from_keydir(index, remaining)

        next = %{
          types: MapSet.union(acc.types, scan.types),
          scanned_entries: acc.scanned_entries + scan.scanned_entries,
          truncated: acc.truncated or scan.truncated
        }

        if next.truncated, do: {:halt, next}, else: {:cont, next}
    end)
  end

  defp collect_flow_policy_type_scan(_limit),
    do: %{types: MapSet.new(), scanned_entries: 0, truncated: false}

  @spec collect_flow_policy_types_from_keydir(non_neg_integer(), non_neg_integer()) :: map()
  defp collect_flow_policy_types_from_keydir(_index, limit) when limit <= 0,
    do: %{types: MapSet.new(), scanned_entries: 0, truncated: true}

  defp collect_flow_policy_types_from_keydir(index, limit) do
    keydir = :"keydir_#{index}"
    batch = min(@flow_dashboard_policy_key_select_batch, limit)

    try do
      keydir
      |> :ets.select(flow_keydir_key_select_spec(), batch)
      |> collect_flow_policy_types_select(MapSet.new(), 0, limit)
    rescue
      ArgumentError -> %{types: MapSet.new(), scanned_entries: 0, truncated: false}
    catch
      :exit, _ -> %{types: MapSet.new(), scanned_entries: 0, truncated: false}
    end
  end

  defp collect_flow_policy_types_select(:"$end_of_table", types, scanned, _limit) do
    %{types: types, scanned_entries: scanned, truncated: false}
  end

  defp collect_flow_policy_types_select({keys, continuation}, types, scanned, limit) do
    scanned = scanned + length(keys)

    types =
      Enum.reduce(keys, types, fn key, acc ->
        case flow_policy_type_from_key(key) do
          nil -> acc
          type -> MapSet.put(acc, type)
        end
      end)

    if scanned >= limit do
      %{types: types, scanned_entries: scanned, truncated: true}
    else
      continuation
      |> :ets.select()
      |> collect_flow_policy_types_select(types, scanned, limit)
    end
  end

  defp collect_flow_policy_types_select(keys, types, scanned, _limit) when is_list(keys) do
    types =
      Enum.reduce(keys, types, fn key, acc ->
        case flow_policy_type_from_key(key) do
          nil -> acc
          type -> MapSet.put(acc, type)
        end
      end)

    %{types: types, scanned_entries: scanned + length(keys), truncated: false}
  end

  @spec flow_keydir_key_select_spec() :: list()
  defp flow_keydir_key_select_spec do
    [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}]
  end

  @spec flow_policy_type_from_key(term()) :: binary() | nil
  defp flow_policy_type_from_key(key) when is_binary(key) do
    prefix = Ferricstore.Flow.Keys.policy_key("")

    cond do
      not Ferricstore.Flow.Keys.policy_key?(key) ->
        nil

      key == prefix ->
        nil

      true ->
        String.replace_prefix(key, prefix, "")
    end
  end

  defp flow_policy_type_from_key(_key), do: nil

  @spec flow_policy_row(binary(), MapSet.t()) :: map()
  defp flow_policy_row(type, configured_types) do
    source = flow_policy_source(type, configured_types)

    case FerricStore.flow_policy_get(type) do
      {:ok, policy} when is_map(policy) ->
        %{
          type: type,
          source: source,
          retry: Map.get(policy, :retry, %{}),
          retention: Map.get(policy, :retention, %{}),
          states: flow_policy_state_rows(Map.get(policy, :states, %{})),
          error: nil
        }

      {:error, reason} ->
        %{
          type: type,
          source: source,
          retry: %{},
          retention: %{},
          states: [],
          error: reason
        }
    end
  rescue
    error ->
      flow_policy_error_row(type, configured_types, Exception.message(error))
  catch
    :exit, reason ->
      flow_policy_error_row(type, configured_types, inspect(reason))
  end

  @spec flow_policy_source(binary(), MapSet.t()) :: binary()
  defp flow_policy_source(type, configured_types),
    do: if(MapSet.member?(configured_types, type), do: "configured", else: "default")

  @spec flow_policy_error_row(binary(), MapSet.t(), binary()) :: map()
  defp flow_policy_error_row(type, configured_types, error) do
    %{
      type: type,
      source: flow_policy_source(type, configured_types),
      retry: %{},
      retention: %{},
      states: [],
      error: error
    }
  end

  @spec flow_policy_state_rows(map()) :: [map()]
  defp flow_policy_state_rows(states) when is_map(states) do
    states
    |> Enum.map(fn {state, policy} ->
      %{
        state: to_string(state),
        retry: Map.get(policy, :retry, %{}),
        retention: Map.get(policy, :retention, %{})
      }
    end)
    |> Enum.sort_by(& &1.state)
  end

  defp flow_policy_state_rows(_states), do: []

  @spec maybe_include_policy_edit_type([binary()], binary()) :: [binary()]
  defp maybe_include_policy_edit_type(types, ""), do: types
  defp maybe_include_policy_edit_type(types, type), do: [type | types]

  @spec flow_policy_editor_data(binary() | nil) :: map()
  defp flow_policy_editor_data(type) do
    type = flow_policy_clean_form_value(type || "")

    policy =
      case type do
        "" ->
          flow_policy_default_response(type)

        _ ->
          case FerricStore.flow_policy_get(type) do
            {:ok, policy} when is_map(policy) -> policy
            _ -> flow_policy_default_response(type)
          end
      end

    retry = Map.get(policy, :retry, Ferricstore.Flow.RetryPolicy.default())
    backoff = flow_policy_field(retry, :backoff, Ferricstore.Flow.RetryPolicy.default().backoff)
    retention = Map.get(policy, :retention, Ferricstore.Flow.RetryPolicy.default_retention())

    %{
      type: type,
      state: "",
      max_retries: flow_policy_field(retry, :max_retries, 3),
      backoff_kind: flow_policy_field(backoff, :kind, :exponential),
      base_ms: flow_policy_field(backoff, :base_ms, 1_000),
      max_ms: flow_policy_field(backoff, :max_ms, 30_000),
      jitter_pct: flow_policy_field(backoff, :jitter_pct, 20),
      exhausted_to: flow_policy_field(retry, :exhausted_to, "failed"),
      retention_ttl_ms: flow_policy_field(retention, :ttl_ms, 604_800_000),
      history_max_events: flow_policy_field(retention, :history_max_events, 100_000)
    }
  end

  @spec flow_policy_default_response(binary()) :: map()
  defp flow_policy_default_response(type) do
    %{
      type: type,
      retry: Ferricstore.Flow.RetryPolicy.default(),
      retention:
        Ferricstore.Flow.RetryPolicy.default_retention()
        |> Map.delete(:history_hot_max_events),
      states: %{}
    }
  end

  @spec flow_policy_existing_set_opts(binary()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_existing_set_opts(type) do
    case FerricStore.flow_policy_get(type) do
      {:ok, policy} when is_map(policy) ->
        {:ok, flow_policy_response_to_set_opts(policy)}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end

  @spec flow_policy_response_to_set_opts(map()) :: keyword()
  defp flow_policy_response_to_set_opts(policy) do
    [
      retry: flow_policy_retry_to_set_opts(Map.get(policy, :retry, %{})),
      retention: flow_policy_retention_to_set_opts(Map.get(policy, :retention, %{})),
      states: flow_policy_states_to_set_opts(Map.get(policy, :states, %{}))
    ]
  end

  @spec flow_policy_states_to_set_opts(map() | term()) :: list()
  defp flow_policy_states_to_set_opts(states) when is_map(states) do
    Enum.map(states, fn {state, policy} ->
      {to_string(state),
       [
         retry: flow_policy_retry_to_set_opts(Map.get(policy, :retry, %{})),
         retention: flow_policy_retention_to_set_opts(Map.get(policy, :retention, %{}))
       ]}
    end)
  end

  defp flow_policy_states_to_set_opts(_states), do: []

  @spec flow_policy_retry_to_set_opts(map()) :: keyword()
  defp flow_policy_retry_to_set_opts(retry) when is_map(retry) do
    backoff = flow_policy_field(retry, :backoff, %{})

    [
      max_retries: flow_policy_field(retry, :max_retries, 3),
      backoff: [
        kind: flow_policy_field(backoff, :kind, :exponential),
        base_ms: flow_policy_field(backoff, :base_ms, 1_000),
        max_ms: flow_policy_field(backoff, :max_ms, 30_000),
        jitter_pct: flow_policy_field(backoff, :jitter_pct, 20)
      ],
      exhausted_to: flow_policy_field(retry, :exhausted_to, "failed")
    ]
  end

  defp flow_policy_retry_to_set_opts(_retry),
    do: flow_policy_retry_to_set_opts(Ferricstore.Flow.RetryPolicy.default())

  @spec flow_policy_retention_to_set_opts(map()) :: keyword()
  defp flow_policy_retention_to_set_opts(retention) when is_map(retention) do
    [
      ttl_ms: flow_policy_field(retention, :ttl_ms, 604_800_000),
      history_max_events: flow_policy_field(retention, :history_max_events, 100_000)
    ]
  end

  defp flow_policy_retention_to_set_opts(_retention),
    do: flow_policy_retention_to_set_opts(Ferricstore.Flow.RetryPolicy.default_retention())

  @spec flow_policy_merge_form_opts(keyword(), binary() | nil, keyword(), keyword()) :: keyword()
  defp flow_policy_merge_form_opts(existing_opts, nil, retry, retention) do
    existing_opts
    |> Keyword.put(:retry, retry)
    |> Keyword.put(:retention, retention)
  end

  defp flow_policy_merge_form_opts(existing_opts, state, retry, retention)
       when is_binary(state) do
    states =
      existing_opts
      |> Keyword.get(:states, [])
      |> flow_policy_put_state_policy(state, retry: retry, retention: retention)

    Keyword.put(existing_opts, :states, states)
  end

  @spec flow_policy_put_state_policy(list(), binary(), keyword()) :: list()
  defp flow_policy_put_state_policy(states, state, policy) do
    states
    |> Enum.reject(fn {existing_state, _policy} -> existing_state == state end)
    |> Kernel.++([{state, policy}])
  end

  @spec flow_policy_form_retry_opts(map()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_form_retry_opts(params) do
    with {:ok, max_retries} <- flow_policy_form_integer(params, "max_retries", 0),
         {:ok, kind} <- flow_policy_form_backoff_kind(params),
         {:ok, base_ms} <- flow_policy_form_integer(params, "base_ms", 0),
         {:ok, max_ms} <- flow_policy_form_integer(params, "max_ms", 0),
         {:ok, jitter_pct} <- flow_policy_form_integer(params, "jitter_pct", 0),
         {:ok, exhausted_to} <-
           flow_policy_required_form_value(params, "exhausted_to", "exhausted_to") do
      {:ok,
       [
         max_retries: max_retries,
         backoff: [kind: kind, base_ms: base_ms, max_ms: max_ms, jitter_pct: jitter_pct],
         exhausted_to: exhausted_to
       ]}
    end
  end

  @spec flow_policy_form_retention_opts(map()) :: {:ok, keyword()} | {:error, binary()}
  defp flow_policy_form_retention_opts(params) do
    with {:ok, ttl_ms} <- flow_policy_form_integer(params, "retention_ttl_ms", 1),
         {:ok, history_max_events} <- flow_policy_form_integer(params, "history_max_events", 1) do
      {:ok,
       [
         ttl_ms: ttl_ms,
         history_max_events: history_max_events
       ]}
    end
  end

  @spec flow_policy_form_backoff_kind(map()) :: {:ok, atom()} | {:error, binary()}
  defp flow_policy_form_backoff_kind(params) do
    case params
         |> Map.get("backoff_kind", "")
         |> flow_policy_clean_form_value()
         |> String.downcase() do
      "none" -> {:ok, :none}
      "fixed" -> {:ok, :fixed}
      "linear" -> {:ok, :linear}
      "exponential" -> {:ok, :exponential}
      _ -> {:error, "ERR flow retry backoff kind must be none, fixed, linear, or exponential"}
    end
  end

  @spec flow_policy_form_integer(map(), binary(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  defp flow_policy_form_integer(params, field, min) do
    value = params |> Map.get(field, "") |> flow_policy_clean_form_value()

    case Integer.parse(value) do
      {integer, ""} when integer >= min ->
        {:ok, integer}

      _ ->
        {:error, "ERR #{String.replace(field, "_", " ")} must be an integer >= #{min}"}
    end
  end

  @spec flow_policy_required_form_value(map(), binary(), binary()) ::
          {:ok, binary()} | {:error, binary()}
  defp flow_policy_required_form_value(params, field, label) do
    case flow_policy_clean_form_value(Map.get(params, field, "")) do
      "" -> {:error, "ERR #{label} is required"}
      value -> {:ok, value}
    end
  end

  @spec flow_policy_optional_form_value(map(), binary()) :: {:ok, binary() | nil}
  defp flow_policy_optional_form_value(params, field) do
    case flow_policy_clean_form_value(Map.get(params, field, "")) do
      "" -> {:ok, nil}
      value -> {:ok, value}
    end
  end

  @spec flow_policy_clean_form_value(term()) :: binary()
  defp flow_policy_clean_form_value(value) when is_binary(value), do: String.trim(value)
  defp flow_policy_clean_form_value(value), do: value |> to_string() |> String.trim()

  @spec flow_retention_form_limit(term()) :: {:ok, pos_integer()} | {:error, binary()}
  defp flow_retention_form_limit(nil), do: {:ok, @flow_dashboard_retention_default_limit}
  defp flow_retention_form_limit(""), do: {:ok, @flow_dashboard_retention_default_limit}

  defp flow_retention_form_limit(value) when is_integer(value) do
    if value >= 1 and value <= @flow_dashboard_retention_max_limit do
      {:ok, value}
    else
      {:error, "ERR cleanup limit must be between 1 and #{@flow_dashboard_retention_max_limit}"}
    end
  end

  defp flow_retention_form_limit(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {integer, ""} -> flow_retention_form_limit(integer)
      _ -> {:error, "ERR cleanup limit must be an integer"}
    end
  end

  defp flow_retention_form_limit(_value), do: {:error, "ERR cleanup limit must be an integer"}

  @spec flow_retention_limit!(term()) :: pos_integer()
  defp flow_retention_limit!(value) do
    case flow_retention_form_limit(value) do
      {:ok, limit} -> limit
      {:error, _reason} -> @flow_dashboard_retention_default_limit
    end
  end

  @spec flow_retention_candidates([map()], integer()) :: [map()]
  defp flow_retention_candidates(records, now_ms) do
    records
    |> Enum.filter(&flow_retention_candidate?(&1, now_ms))
    |> Enum.sort_by(&flow_retention_until_ms/1, :asc)
  end

  @spec flow_retention_candidate?(map(), integer()) :: boolean()
  defp flow_retention_candidate?(record, now_ms) do
    case flow_retention_until_ms(record) do
      until_ms when is_integer(until_ms) ->
        flow_retention_terminal_record?(record) and until_ms <= now_ms

      _ ->
        false
    end
  end

  @spec flow_retention_terminal_record?(map()) :: boolean()
  defp flow_retention_terminal_record?(record) do
    record
    |> flow_record_state()
    |> String.downcase()
    |> then(&(&1 in @flow_terminal_states))
  end

  @spec flow_retention_until_ms(map()) :: integer() | nil
  defp flow_retention_until_ms(record) do
    flow_first_integer(record, [
      :terminal_retention_until_ms,
      :retention_until_ms,
      :expires_at_ms,
      :expire_at_ms
    ])
  end

  @spec flow_dashboard_retention_cleanup(keyword()) :: {:ok, map()} | {:error, term()}
  defp flow_dashboard_retention_cleanup(opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_retention_cleanup_fun) do
      fun when is_function(fun, 1) -> fun.(opts)
      _ -> FerricStore.flow_retention_cleanup(opts)
    end
  end

  @spec flow_retention_cleanup_counts(map(), pos_integer()) :: map()
  defp flow_retention_cleanup_counts(result, limit) do
    %{
      limit: limit,
      flows: flow_retention_count(result, :flows),
      history: flow_retention_count(result, :history),
      values: flow_retention_count(result, :values)
    }
  end

  @spec flow_retention_count(map(), atom()) :: non_neg_integer()
  defp flow_retention_count(result, key) do
    case Map.get(result, key, Map.get(result, Atom.to_string(key), 0)) do
      count when is_integer(count) and count >= 0 -> count
      _ -> 0
    end
  end

  @spec flow_retention_query_integer(map(), binary()) :: non_neg_integer()
  defp flow_retention_query_integer(params, key) do
    case Integer.parse(Map.get(params, key, "0")) do
      {integer, ""} when integer >= 0 -> integer
      _ -> 0
    end
  end

  @spec filter_flow_records_by_type([map()], binary() | nil) :: [map()]
  defp filter_flow_records_by_type(records, nil), do: records

  defp filter_flow_records_by_type(records, type_filter) when is_binary(type_filter) do
    Enum.filter(records, &(flow_record_type(&1) == type_filter))
  end

  @spec filter_flow_records_by_partition([map()], binary() | nil) :: [map()]
  defp filter_flow_records_by_partition(records, nil), do: records

  defp filter_flow_records_by_partition(records, partition_key) when is_binary(partition_key) do
    Enum.filter(records, &(flow_record_partition_key(&1) == partition_key))
  end

  @spec flow_overview_filters_from_opts(keyword()) :: map()
  defp flow_overview_filters_from_opts(opts) when is_list(opts) do
    %{
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key))
    }
  end

  @spec filter_flow_records([map()], map()) :: [map()]
  defp filter_flow_records(records, filters) when is_map(filters) do
    records
    |> filter_flow_records_by_state(Map.get(filters, :state))
    |> filter_flow_records_by_name(Map.get(filters, :q))
    |> filter_flow_records_by_updated_range(Map.get(filters, :from_ms), Map.get(filters, :to_ms))
  end

  @spec filter_flow_records_by_state([map()], binary() | nil) :: [map()]
  defp filter_flow_records_by_state(records, nil), do: records

  defp filter_flow_records_by_state(records, state) when is_binary(state) do
    Enum.filter(records, &(flow_record_state(&1) == state))
  end

  @spec filter_flow_records_by_name([map()], binary() | nil) :: [map()]
  defp filter_flow_records_by_name(records, nil), do: records

  defp filter_flow_records_by_name(records, query) when is_binary(query) do
    needle = String.downcase(query)

    Enum.filter(records, fn record ->
      record
      |> flow_record_id()
      |> String.downcase()
      |> String.contains?(needle)
    end)
  end

  @spec filter_flow_records_by_updated_range([map()], integer() | nil, integer() | nil) :: [map()]
  defp filter_flow_records_by_updated_range(records, nil, nil), do: records

  defp filter_flow_records_by_updated_range(records, from_ms, to_ms) do
    Enum.filter(records, fn record ->
      updated_at = flow_record_updated_at_ms(record)
      after_from? = not is_integer(from_ms) or updated_at >= from_ms
      before_to? = not is_integer(to_ms) or updated_at <= to_ms
      after_from? and before_to?
    end)
  end

  @spec merge_flow_records([map()], [map()]) :: [map()]
  defp merge_flow_records(records, []), do: records

  defp merge_flow_records(records, extra_records) do
    (extra_records ++ records)
    |> Enum.reduce({[], MapSet.new()}, fn record, {acc, seen} ->
      key = flow_record_identity(record)

      if MapSet.member?(seen, key) do
        {acc, seen}
      else
        {[record | acc], MapSet.put(seen, key)}
      end
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  @spec flow_record_identity(map()) :: {binary(), binary() | nil}
  defp flow_record_identity(record),
    do: {flow_record_id(record), flow_record_partition_key(record)}

  @spec prepend_flow_dashboard_chunk([map()], list()) :: list()
  defp prepend_flow_dashboard_chunk([], acc), do: acc
  defp prepend_flow_dashboard_chunk(records, acc), do: [records | acc]

  @spec flatten_flow_dashboard_chunks(list()) :: [map()]
  defp flatten_flow_dashboard_chunks(chunks) do
    chunks
    |> Enum.reverse()
    |> List.flatten()
  end

  @spec flow_available_states([map()]) :: [binary()]
  defp flow_available_states(records) do
    records
    |> Enum.map(&flow_record_state/1)
    |> Enum.reject(&(&1 in ["", "unknown"]))
    |> Enum.uniq()
    |> Enum.sort()
  end

  @spec maybe_include_flow_state([binary()], binary() | nil) :: [binary()]
  defp maybe_include_flow_state(states, state) when is_binary(state) and state != "" do
    states
    |> Kernel.++([state])
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp maybe_include_flow_state(states, _state), do: states

  @spec flow_state_filters_from_opts(keyword()) :: map()
  defp flow_state_filters_from_opts(opts) when is_list(opts) do
    range = normalize_flow_range_filter(Keyword.get(opts, :range))
    {from_ms, to_ms} = flow_time_bounds_from_opts(opts, range)

    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      state: normalize_flow_state_filter(Keyword.get(opts, :state)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      range: range,
      from_ms: from_ms,
      to_ms: to_ms,
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit))
    }
  end

  @spec flow_signals_filters_from_opts(keyword()) :: map()
  defp flow_signals_filters_from_opts(opts) when is_list(opts) do
    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      signal: normalize_flow_name_filter(Keyword.get(opts, :signal)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      scan_history: normalize_flow_boolean_filter(Keyword.get(opts, :scan_history))
    }
  end

  @spec flow_signal_rows_for_record(map()) :: [map()]
  defp flow_signal_rows_for_record(record) when is_map(record) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)

    opts =
      [
        count: @flow_dashboard_signal_history_count,
        values: false,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, flow_detail_url_partition_key(partition_key))

    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_history(id, opts) end,
           timeout_ms,
           :signals_history
         ) do
      {:ok, {:ok, history}} when is_list(history) -> flow_signal_rows(record, history)
      _ -> []
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  @spec flow_signal_rows(map(), list()) :: [map()]
  defp flow_signal_rows(record, history) when is_map(record) and is_list(history) do
    history
    |> flow_history_timeline_rows()
    |> Enum.filter(&flow_signal_event?(&1.fields))
    |> Enum.map(fn row ->
      %{
        id: flow_record_id(record),
        partition_key: flow_detail_url_partition_key(flow_record_partition_key(record)),
        type: flow_record_type(record),
        event_id: to_string(row.event_id),
        time_ms: row.time_ms,
        signal: flow_field_string(row.fields, :signal, "-"),
        from_state: row.from_state,
        to_state: row.to_state,
        fields: row.fields,
        record: record
      }
    end)
  end

  defp flow_signal_rows(_record, _history), do: []

  @spec flow_signal_event?(map()) :: boolean()
  defp flow_signal_event?(fields) do
    fields
    |> flow_field_string(:event, flow_field_string(fields, :action, ""))
    |> String.downcase() == "signaled"
  end

  @spec filter_flow_signal_rows([map()], map()) :: [map()]
  defp filter_flow_signal_rows(rows, filters) when is_map(filters) do
    signal_filter = Map.get(filters, :signal)

    case signal_filter do
      nil ->
        rows

      signal when is_binary(signal) ->
        needle = String.downcase(signal)

        Enum.filter(rows, fn row ->
          row.signal
          |> to_string()
          |> String.downcase()
          |> String.contains?(needle)
        end)
    end
  end

  @spec flow_signal_sort_key(map()) :: {integer(), binary()}
  defp flow_signal_sort_key(row) do
    {Map.get(row, :time_ms) || -1, Map.get(row, :event_id, "")}
  end

  @spec flow_time_bounds_from_opts(keyword(), binary() | nil) ::
          {integer() | nil, integer() | nil}
  defp flow_time_bounds_from_opts(_opts, range) when is_binary(range) do
    {System.system_time(:millisecond) - flow_time_range_duration_ms(range), nil}
  end

  defp flow_time_bounds_from_opts(opts, _range) do
    {parse_flow_time_filter(Keyword.get(opts, :from_ms)),
     parse_flow_time_filter(Keyword.get(opts, :to_ms))}
  end

  @spec maybe_put_query_opt(keyword(), atom(), term()) :: keyword()
  defp maybe_put_query_opt(opts, _key, nil), do: opts
  defp maybe_put_query_opt(opts, key, value), do: [{key, value} | opts]

  @spec normalize_flow_partition_query(term()) :: binary() | nil
  defp normalize_flow_partition_query(partition_key) when is_binary(partition_key) do
    partition_key = String.trim(partition_key)
    if partition_key == "", do: nil, else: partition_key
  end

  defp normalize_flow_partition_query(_partition_key), do: nil

  @spec normalize_flow_history_count(term()) :: pos_integer()
  defp normalize_flow_history_count(count) when is_integer(count) do
    count
    |> max(1)
    |> min(@flow_dashboard_history_max_count)
  end

  defp normalize_flow_history_count(count) when is_binary(count) do
    case Integer.parse(String.trim(count)) do
      {parsed, ""} -> normalize_flow_history_count(parsed)
      _ -> @flow_dashboard_history_default_count
    end
  end

  defp normalize_flow_history_count(_count), do: @flow_dashboard_history_default_count

  @spec normalize_flow_history_cursor(term()) :: binary() | nil
  defp normalize_flow_history_cursor(cursor) when is_binary(cursor) do
    cursor = String.trim(cursor)
    if cursor == "", do: nil, else: cursor
  end

  defp normalize_flow_history_cursor(_cursor), do: nil

  @spec normalize_flow_history_after_cursor(map()) :: binary() | nil
  defp normalize_flow_history_after_cursor(%{
         "history_before" => before,
         "history_after" => after_cursor
       })
       when is_binary(before) do
    case normalize_flow_history_cursor(before) do
      nil -> normalize_flow_history_cursor(after_cursor)
      _before -> nil
    end
  end

  defp normalize_flow_history_after_cursor(params) when is_map(params) do
    normalize_flow_history_cursor(Map.get(params, "history_after"))
  end

  @spec normalize_flow_type_filter(term()) :: binary() | nil
  defp normalize_flow_type_filter(type) when is_binary(type) do
    type = String.trim(type)

    case String.downcase(type) do
      "" -> nil
      "all" -> nil
      _ -> type
    end
  end

  defp normalize_flow_type_filter(_type), do: nil

  @spec normalize_flow_state_filter(term()) :: binary() | nil
  defp normalize_flow_state_filter(state) when is_binary(state) do
    state = String.trim(state)

    case String.downcase(state) do
      "" -> nil
      "all" -> nil
      _ -> state
    end
  end

  defp normalize_flow_state_filter(_state), do: nil

  @spec normalize_flow_name_filter(term()) :: binary() | nil
  defp normalize_flow_name_filter(query) when is_binary(query) do
    query = String.trim(query)
    if query == "", do: nil, else: query
  end

  defp normalize_flow_name_filter(_query), do: nil

  @spec normalize_flow_range_filter(term()) :: binary() | nil
  defp normalize_flow_range_filter(range) when is_binary(range) do
    range = String.trim(range)

    if flow_time_range_duration_ms(range) > 0 do
      range
    else
      nil
    end
  end

  defp normalize_flow_range_filter(_range), do: nil

  @spec flow_time_range_duration_ms(binary() | nil) :: non_neg_integer()
  defp flow_time_range_duration_ms("5m"), do: 5 * 60 * 1_000
  defp flow_time_range_duration_ms("15m"), do: 15 * 60 * 1_000
  defp flow_time_range_duration_ms("1h"), do: 60 * 60 * 1_000
  defp flow_time_range_duration_ms("6h"), do: 6 * 60 * 60 * 1_000
  defp flow_time_range_duration_ms("24h"), do: 24 * 60 * 60 * 1_000
  defp flow_time_range_duration_ms(_range), do: 0

  @spec normalize_flow_limit_filter(term()) :: pos_integer()
  defp normalize_flow_limit_filter(limit) when is_integer(limit) do
    limit
    |> max(1)
    |> min(@flow_dashboard_max_recent_limit)
  end

  defp normalize_flow_limit_filter(limit) when is_binary(limit) do
    case Integer.parse(String.trim(limit)) do
      {parsed, ""} -> normalize_flow_limit_filter(parsed)
      _ -> @flow_dashboard_recent_limit
    end
  end

  defp normalize_flow_limit_filter(_limit), do: @flow_dashboard_recent_limit

  @spec parse_flow_time_filter(term()) :: integer() | nil
  defp parse_flow_time_filter(value) when is_integer(value), do: value

  defp parse_flow_time_filter(value) when is_binary(value) do
    value = String.trim(value)

    cond do
      value == "" ->
        nil

      true ->
        parse_flow_time_integer(value) || parse_flow_time_iso8601(value)
    end
  end

  defp parse_flow_time_filter(_value), do: nil

  @spec parse_flow_time_integer(binary()) :: integer() | nil
  defp parse_flow_time_integer(value) do
    case Integer.parse(value) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  @spec parse_flow_time_iso8601(binary()) :: integer() | nil
  defp parse_flow_time_iso8601(value) do
    value = String.replace(value, " ", "T")
    value = normalize_flow_datetime_local(value)

    with {:ok, datetime, _offset} <- DateTime.from_iso8601(value) do
      DateTime.to_unix(datetime, :millisecond)
    else
      _ ->
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive} ->
            naive
            |> DateTime.from_naive!("Etc/UTC")
            |> DateTime.to_unix(:millisecond)

          _ ->
            nil
        end
    end
  end

  @spec normalize_flow_datetime_local(binary()) :: binary()
  defp normalize_flow_datetime_local(value) do
    cond do
      String.match?(value, ~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/) ->
        value <> ":00"

      true ->
        value
    end
  end

  @spec flow_page_summary([map()], [map()]) :: map()
  defp flow_page_summary(types, records) do
    base = %{
      types: length(types),
      total: 0,
      active: 0,
      queued: 0,
      running: 0,
      terminal: 0,
      failed: 0,
      cancelled: 0,
      inflight: 0
    }

    totals =
      Enum.reduce(types, base, fn type, acc ->
        acc
        |> Map.update!(:total, &(&1 + Map.get(type, :total, 0)))
        |> Map.update!(:active, &(&1 + Map.get(type, :active, 0)))
        |> Map.update!(:queued, &(&1 + Map.get(type, :queued, 0)))
        |> Map.update!(:running, &(&1 + Map.get(type, :running, 0)))
        |> Map.update!(:terminal, &(&1 + Map.get(type, :terminal, 0)))
        |> Map.update!(:failed, &(&1 + Map.get(type, :failed, 0)))
        |> Map.update!(:cancelled, &(&1 + Map.get(type, :cancelled, 0)))
        |> Map.update!(:inflight, &(&1 + Map.get(type, :inflight, 0)))
      end)

    totals
    |> Map.put(:due_now_sampled, Enum.count(records, &flow_due_now?/1))
    |> Map.put(:expired_leases_sampled, Enum.count(records, &flow_expired_lease?/1))
    |> Map.put(:sampled_records, length(records))
  end

  @spec flow_recent_records([map()], pos_integer()) :: [map()]
  defp flow_recent_records(records, limit) do
    records
    |> Enum.sort_by(&flow_record_updated_at_ms/1, :desc)
    |> Enum.take(limit)
  end

  @spec flow_worker_summaries([map()]) :: [map()]
  defp flow_worker_summaries(records) do
    records
    |> Enum.filter(&(flow_record_state(&1) == "running"))
    |> Enum.group_by(fn record -> flow_record_worker(record) || "unknown" end)
    |> Enum.map(fn {worker, worker_records} ->
      %{
        worker: worker,
        running: length(worker_records),
        expired: Enum.count(worker_records, &flow_expired_lease?/1),
        oldest_lease_ms: flow_oldest_lease_ms(worker_records)
      }
    end)
    |> Enum.sort_by(fn worker -> {-worker.running, worker.worker} end)
  end

  @spec flow_state_summaries([map()]) :: [map()]
  defp flow_state_summaries(records) do
    records
    |> Enum.group_by(fn record -> {flow_record_type(record), flow_record_state(record)} end)
    |> Enum.map(fn {{type, state}, state_records} ->
      due_now = Enum.count(state_records, &flow_due_now?/1)
      expired = Enum.count(state_records, &flow_expired_lease?/1)
      retrying = Enum.count(state_records, &flow_retrying?/1)
      failed = Enum.count(state_records, &flow_failed?/1)
      max_attempts_reached = Enum.count(state_records, &flow_max_attempts_reached?/1)
      oldest_due_ms = flow_oldest_due_ms(state_records)

      %{
        type: type,
        state: state,
        count: length(state_records),
        due_now: due_now,
        running: Enum.count(state_records, &(flow_record_state(&1) == "running")),
        retrying: retrying,
        failed: failed,
        expired_leases: expired,
        max_attempts_reached: max_attempts_reached,
        oldest_due_ms: oldest_due_ms
      }
    end)
    |> Enum.sort_by(fn state ->
      {-state.due_now, -state.expired_leases, -state.failed, -state.retrying, state.type,
       state.state}
    end)
  end

  @spec flow_oldest_due_ms([map()]) :: non_neg_integer()
  defp flow_oldest_due_ms(records) do
    now = System.system_time(:millisecond)

    records
    |> Enum.filter(&flow_due_now?/1)
    |> Enum.map(&flow_record_run_at_ms/1)
    |> Enum.filter(&is_integer/1)
    |> case do
      [] -> 0
      due_times -> max(0, now - Enum.min(due_times))
    end
  end

  @spec flow_scheduled_future?(map()) :: boolean()
  defp flow_scheduled_future?(record) do
    state = flow_record_state(record)
    run_at = flow_record_run_at_ms(record)

    state not in @flow_terminal_states and state != "running" and is_integer(run_at) and
      run_at > System.system_time(:millisecond)
  end

  @spec collect_flow_projection_metrics() :: [map()]
  defp collect_flow_projection_metrics do
    Ferricstore.Metrics.scrape()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.filter(&String.contains?(&1, "ferricstore_flow_lmdb"))
    |> Enum.take(80)
    |> Enum.map(&parse_metric_line/1)
    |> Enum.map(&normalize_flow_projection_metric/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  @doc false
  @spec default_flow_projection_health() :: map()
  def default_flow_projection_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: 0,
      history_flush_interval_ms: 0,
      metrics: []
    }
  end

  @spec normalize_flow_projection_metric(map()) :: map()
  defp normalize_flow_projection_metric(%{name: name} = metric) when is_binary(name) do
    %{metric | name: String.replace(name, "mirror", "projection")}
  end

  defp normalize_flow_projection_metric(metric), do: metric

  @spec parse_metric_line(binary()) :: map()
  defp parse_metric_line(line) do
    case String.split(line, " ", parts: 2) do
      [name, value] -> %{name: name, value: value}
      [name] -> %{name: name, value: ""}
    end
  end

  defp projection_metric_rows(metrics) do
    metrics
    |> Enum.reduce(%{}, fn metric, acc ->
      shard = projection_metric_shard(metric.name)
      field = projection_metric_field(metric.name)
      value = numeric_metric_value(metric.value)

      row =
        Map.get(acc, shard, %{
          shard: shard,
          replay_safe_index: 0,
          requested_index: 0,
          lag: 0,
          pending_ops: 0,
          oldest_pending_age_us: 0,
          degraded: 0,
          persist_failures: 0,
          enqueue_failures: 0,
          flush_failures: 0,
          failures: 0
        })

      row =
        row
        |> Map.put(field, value)
        |> then(fn row ->
          failures =
            Map.get(row, :persist_failures, 0) + Map.get(row, :enqueue_failures, 0) +
              Map.get(row, :flush_failures, 0) + Map.get(row, :degraded, 0)

          %{row | failures: failures}
        end)

      Map.put(acc, shard, row)
    end)
    |> Map.values()
    |> Enum.sort_by(& &1.shard)
  end

  defp flow_projection_rollup(metrics) do
    rows = projection_metric_rows(metrics)

    totals =
      Enum.reduce(
        rows,
        %{lag: 0, pending_ops: 0, oldest_pending_age_us: 0, degraded: 0, failures: 0},
        fn row, acc ->
          %{
            lag: acc.lag + row.lag,
            pending_ops: acc.pending_ops + row.pending_ops,
            oldest_pending_age_us: max(acc.oldest_pending_age_us, row.oldest_pending_age_us),
            degraded: acc.degraded + row.degraded,
            failures: acc.failures + row.failures
          }
        end
      )

    health =
      cond do
        totals.failures > 0 -> "failures"
        totals.degraded > 0 -> "degraded"
        totals.lag > 0 or totals.pending_ops > 0 -> "pending"
        true -> "healthy"
      end

    Map.merge(totals, %{health: health, shards: length(rows)})
  end

  defp flow_projection_health_class(%{failures: failures, degraded: degraded})
       when failures > 0 or degraded > 0,
       do: "c-red"

  defp flow_projection_health_class(%{lag: lag, pending_ops: pending_ops})
       when lag > 0 or pending_ops > 0,
       do: "c-yellow"

  defp flow_projection_health_class(_rollup), do: "c-green"

  defp projection_metric_shard(name) do
    case Regex.run(~r/shard_index="([^"]+)"/, name) do
      [_all, shard] -> shard
      _ -> "all"
    end
  end

  defp projection_metric_field(name) do
    base = String.split(name, "{", parts: 2) |> List.first()

    cond do
      String.ends_with?(base, "_replay_safe_index") -> :replay_safe_index
      String.ends_with?(base, "_replay_safe_requested_index") -> :requested_index
      String.ends_with?(base, "_replay_safe_lag") -> :lag
      String.ends_with?(base, "_replay_safe_persist_failures_total") -> :persist_failures
      String.ends_with?(base, "_projection_enqueue_failures_total") -> :enqueue_failures
      String.ends_with?(base, "_projection_degraded") -> :degraded
      String.ends_with?(base, "_writer_pending_ops") -> :pending_ops
      String.ends_with?(base, "_writer_oldest_pending_age_us") -> :oldest_pending_age_us
      String.ends_with?(base, "_writer_flush_failures_total") -> :flush_failures
      true -> :unknown
    end
  end

  @spec flow_detail_partition_key(keyword()) :: binary() | nil
  defp flow_detail_partition_key(opts) do
    case Keyword.get(opts, :partition_key) do
      partition_key when is_binary(partition_key) and partition_key != "" -> partition_key
      _ -> nil
    end
  end

  defp flow_detail_url_partition_key(partition_key) when is_binary(partition_key) do
    if Ferricstore.Flow.Keys.auto_partition_key?(partition_key), do: nil, else: partition_key
  end

  defp flow_detail_url_partition_key(_partition_key), do: nil

  @spec flow_rewind_required_form_value(map(), binary(), binary()) ::
          {:ok, binary()} | {:error, binary()}
  defp flow_rewind_required_form_value(params, key, label) do
    case Map.get(params, key) do
      value when is_binary(value) ->
        value = String.trim(value)
        if value == "", do: {:error, "ERR #{label} is required"}, else: {:ok, value}

      _ ->
        {:error, "ERR #{label} is required"}
    end
  end

  @spec flow_rewind_optional_non_neg_integer(map(), binary()) ::
          {:ok, non_neg_integer() | nil} | {:error, binary()}
  defp flow_rewind_optional_non_neg_integer(params, key) do
    case Map.get(params, key) do
      nil ->
        {:ok, nil}

      "" ->
        {:ok, nil}

      value when is_binary(value) ->
        value = String.trim(value)

        case Integer.parse(value) do
          {parsed, ""} when parsed >= 0 -> {:ok, parsed}
          _ -> {:error, "ERR #{key} must be a non-negative integer"}
        end

      value when is_integer(value) and value >= 0 ->
        {:ok, value}

      _ ->
        {:error, "ERR #{key} must be a non-negative integer"}
    end
  end

  @spec flow_rewind_current_record(binary(), binary() | nil) :: {:ok, map()} | {:error, binary()}
  defp flow_rewind_current_record(id, partition_key) do
    case FerricStore.flow_get(id, flow_dashboard_get_opts(partition_key)) do
      {:ok, %{} = record} -> {:ok, record}
      {:ok, nil} -> {:error, "ERR flow not found"}
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
      other -> {:error, "ERR unexpected flow_get result: #{inspect(other, limit: 5)}"}
    end
  end

  @spec flow_rewind_existing_target_state(binary(), binary() | nil, binary()) ::
          {:ok, binary()} | {:error, binary()}
  defp flow_rewind_existing_target_state(id, partition_key, to_event) do
    opts =
      flow_rewind_opts(partition_key,
        count: 1,
        values: false,
        consistent_projection: true,
        from_event: to_event,
        to_event: to_event
      )

    case FerricStore.flow_history(id, opts) do
      {:ok, [{event_id, fields}]} when is_map(fields) ->
        if to_string(event_id) == to_event do
          case flow_history_current_state(fields) do
            "" -> {:error, "ERR flow rewind target event has no state"}
            state -> {:ok, state}
          end
        else
          {:error, "ERR flow rewind target event is not in this flow history"}
        end

      {:ok, _events} ->
        {:error, "ERR flow rewind target event is not in this flow history"}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}

      other ->
        {:error, "ERR unexpected flow_history result: #{inspect(other, limit: 5)}"}
    end
  end

  @spec flow_rewind_opts(binary() | nil, keyword()) :: keyword()
  defp flow_rewind_opts(partition_key, opts) do
    opts =
      Enum.reject(opts, fn {_key, value} -> is_nil(value) end)

    case partition_key do
      key when is_binary(key) and key != "" -> Keyword.put(opts, :partition_key, key)
      _ -> opts
    end
  end

  @spec flow_rewind_apply(binary(), keyword()) :: :ok | {:error, binary()}
  defp flow_rewind_apply(id, opts) do
    case FerricStore.flow_rewind(id, opts) do
      :ok -> :ok
      {:ok, _record} -> :ok
      {:error, reason} when is_binary(reason) -> {:error, reason}
      {:error, reason} -> {:error, inspect(reason)}
      other -> {:error, "ERR unexpected flow_rewind result: #{inspect(other, limit: 5)}"}
    end
  end

  @spec flow_detail_record(binary(), binary() | nil) ::
          {atom() | {:error, term()} | {:exit, term()}, map() | nil}
  defp flow_detail_record(id, partition_key) do
    sampled =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> Enum.find(fn record ->
        flow_record_id(record) == id and flow_detail_partition_match?(record, partition_key)
      end)

    case sampled do
      %{} = record ->
        {:ok, record}

      nil ->
        timeout_ms = flow_dashboard_detail_fetch_timeout_ms()
        opts = flow_dashboard_get_opts(partition_key)

        case bounded_dashboard_call(
               fn -> flow_dashboard_flow_get(id, opts) end,
               timeout_ms,
               :record
             ) do
          {:ok, {:ok, %{} = record}} -> {:ok, record}
          {:ok, {:ok, nil}} -> {:not_found, nil}
          {:ok, {:error, reason}} -> {{:error, reason}, nil}
          {:ok, _other} -> {{:error, :unexpected_flow_get_result}, nil}
          {:error, :timeout} -> {:timeout, nil}
          {:error, reason} -> {{:error, reason}, nil}
        end
    end
  rescue
    reason -> {{:error, reason}, nil}
  catch
    :exit, reason -> {{:exit, reason}, nil}
  end

  defp flow_detail_partition_match?(_record, nil), do: true

  defp flow_detail_partition_match?(record, partition_key) do
    flow_record_partition_key(record) == partition_key
  end

  defp flow_dashboard_get_opts(nil), do: [payload: false]
  defp flow_dashboard_get_opts(partition_key), do: [payload: false, partition_key: partition_key]

  @spec flow_detail_history_page_opts(keyword()) :: map()
  defp flow_detail_history_page_opts(opts) when is_list(opts) do
    before = normalize_flow_history_cursor(Keyword.get(opts, :history_before))

    after_cursor =
      if is_nil(before), do: normalize_flow_history_cursor(Keyword.get(opts, :history_after))

    %{
      count: normalize_flow_history_count(Keyword.get(opts, :history_count)),
      before: before,
      after_cursor: after_cursor,
      has_older: false,
      has_newer: false,
      oldest_event_id: nil,
      newest_event_id: nil
    }
  end

  @spec flow_detail_history(binary(), map() | nil, map()) ::
          {atom() | {:error, term()} | {:exit, term()}, list(), map()}
  defp flow_detail_history(_id, nil, page), do: {:skipped, [], page}

  defp flow_detail_history(id, record, page) do
    opts =
      case flow_record_partition_key(record) do
        partition_key when is_binary(partition_key) and partition_key != "" ->
          [
            count: flow_detail_history_fetch_count(page),
            values: false,
            consistent_projection: true,
            partition_key: partition_key
          ]

        _ ->
          [
            count: flow_detail_history_fetch_count(page),
            values: false,
            consistent_projection: true
          ]
      end
      |> flow_detail_history_cursor_opts(page)

    timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

    case bounded_dashboard_call(
           fn -> flow_dashboard_flow_history(id, opts) end,
           timeout_ms,
           :history
         ) do
      {:ok, {:ok, history}} when is_list(history) ->
        {history, page} = flow_detail_history_page(history, page)
        {:ok, history, page}

      {:ok, {:error, reason}} ->
        {{:error, reason}, [], page}

      {:ok, _other} ->
        {{:error, :unexpected_flow_history_result}, [], page}

      {:error, :timeout} ->
        {:timeout, [], page}

      {:error, reason} ->
        {{:error, reason}, [], page}
    end
  rescue
    reason -> {{:error, reason}, [], page}
  catch
    :exit, reason -> {{:exit, reason}, [], page}
  end

  defp flow_detail_history_fetch_count(%{
         before: before,
         after_cursor: after_cursor,
         count: count
       })
       when is_binary(before) or is_binary(after_cursor),
       do: count + 2

  defp flow_detail_history_fetch_count(%{count: count}), do: count + 1

  defp flow_detail_history_cursor_opts(opts, %{before: before}) when is_binary(before) do
    opts
    |> Keyword.put(:to_event, before)
    |> Keyword.put(:rev, true)
  end

  defp flow_detail_history_cursor_opts(opts, %{after_cursor: after_cursor})
       when is_binary(after_cursor),
       do: Keyword.put(opts, :from_event, after_cursor)

  defp flow_detail_history_cursor_opts(opts, _page), do: opts

  defp flow_detail_history_page(history, %{before: before, count: count} = page)
       when is_binary(before) do
    older_desc =
      history
      |> flow_history_drop_event(before)

    has_older = length(older_desc) > count
    page_events = older_desc |> Enum.take(count) |> Enum.reverse()

    {page_events,
     %{
       page
       | has_older: has_older,
         has_newer: page_events != [],
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_detail_history_page(history, %{after_cursor: after_cursor, count: count} = page)
       when is_binary(after_cursor) do
    newer_asc = flow_history_drop_event(history, after_cursor)
    has_newer = length(newer_asc) > count
    page_events = Enum.take(newer_asc, count)

    {page_events,
     %{
       page
       | has_older: page_events != [],
         has_newer: has_newer,
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_detail_history_page(history, %{count: count} = page) do
    has_older = length(history) > count
    page_events = Enum.take(history, -count)

    {page_events,
     %{
       page
       | has_older: has_older,
         has_newer: false,
         oldest_event_id: flow_history_page_oldest_event_id(page_events),
         newest_event_id: flow_history_page_newest_event_id(page_events)
     }}
  end

  defp flow_history_drop_event(history, event_id) do
    Enum.reject(history, fn entry ->
      entry
      |> normalize_flow_history_entry()
      |> elem(0)
      |> to_string() == event_id
    end)
  end

  defp flow_history_page_oldest_event_id([]), do: nil

  defp flow_history_page_oldest_event_id(history) do
    history
    |> List.first()
    |> normalize_flow_history_entry()
    |> elem(0)
    |> to_string()
  end

  defp flow_history_page_newest_event_id([]), do: nil

  defp flow_history_page_newest_event_id(history) do
    history
    |> List.last()
    |> normalize_flow_history_entry()
    |> elem(0)
    |> to_string()
  end

  defp flow_detail_history_page_links(id, partition_key, page) when is_map(page) do
    page
    |> Map.put(:id, id)
    |> Map.put(:partition_key, partition_key)
    |> Map.put(:older_url, flow_detail_history_older_url(id, partition_key, page))
    |> Map.put(:newer_url, flow_detail_history_newer_url(id, partition_key, page))
    |> Map.put(:current_live_params, flow_detail_history_current_params(page))
  end

  defp flow_detail_history_older_url(id, partition_key, %{
         has_older: true,
         oldest_event_id: oldest,
         count: count
       })
       when is_binary(oldest) do
    flow_detail_path(id, partition_key, %{
      "history_before" => oldest,
      "history_count" => count
    })
  end

  defp flow_detail_history_older_url(_id, _partition_key, _page), do: nil

  defp flow_detail_history_newer_url(id, partition_key, %{
         has_newer: true,
         newest_event_id: newest,
         count: count
       })
       when is_binary(newest) do
    flow_detail_path(id, partition_key, %{
      "history_after" => newest,
      "history_count" => count
    })
  end

  defp flow_detail_history_newer_url(_id, _partition_key, _page), do: nil

  defp flow_detail_history_current_params(%{before: before, count: count})
       when is_binary(before) do
    %{"history_before" => before, "history_count" => count}
  end

  defp flow_detail_history_current_params(%{after_cursor: after_cursor, count: count})
       when is_binary(after_cursor) do
    %{"history_after" => after_cursor, "history_count" => count}
  end

  defp flow_detail_history_current_params(%{count: @flow_dashboard_history_default_count}),
    do: %{}

  defp flow_detail_history_current_params(%{count: count}), do: %{"history_count" => count}

  defp flow_dashboard_detail_fetch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :flow_dashboard_detail_fetch_timeout_ms,
      @flow_dashboard_detail_fetch_timeout_ms
    )
  end

  defp flow_dashboard_list_fetch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :flow_dashboard_list_fetch_timeout_ms,
      @flow_dashboard_list_fetch_timeout_ms
    )
  end

  defp flow_dashboard_flow_get(id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_get_fun) do
      fun when is_function(fun, 2) -> fun.(id, opts)
      fun when is_function(fun, 1) -> fun.(id)
      _ -> FerricStore.flow_get(id, opts)
    end
  end

  defp flow_dashboard_flow_history(id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_history_fun) do
      fun when is_function(fun, 2) -> fun.(id, opts)
      _ -> FerricStore.flow_history(id, opts)
    end
  end

  defp flow_dashboard_flow_list(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_list_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_list(type, opts)
    end
  end

  defp flow_dashboard_flow_terminals(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_terminals_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_terminals(type, opts)
    end
  end

  defp flow_dashboard_flow_failures(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_failures_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_failures(type, opts)
    end
  end

  defp flow_dashboard_flow_stuck(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_stuck_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_stuck(type, opts)
    end
  end

  defp flow_dashboard_flow_reclaim(type, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_reclaim_fun) do
      fun when is_function(fun, 2) -> fun.(type, opts)
      _ -> FerricStore.flow_reclaim(type, opts)
    end
  end

  defp flow_dashboard_flow_by_parent(parent_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_parent_fun) do
      fun when is_function(fun, 2) -> fun.(parent_id, opts)
      _ -> FerricStore.flow_by_parent(parent_id, opts)
    end
  end

  defp flow_dashboard_flow_by_root(root_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_root_fun) do
      fun when is_function(fun, 2) -> fun.(root_id, opts)
      _ -> FerricStore.flow_by_root(root_id, opts)
    end
  end

  defp flow_dashboard_flow_by_correlation(correlation_id, opts) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_by_correlation_fun) do
      fun when is_function(fun, 2) -> fun.(correlation_id, opts)
      _ -> FerricStore.flow_by_correlation(correlation_id, opts)
    end
  end

  @spec flow_detail_values(map() | nil, list()) ::
          {atom() | {:error, term()} | {:exit, term()}, [map()], %{binary() => term()}}
  defp flow_detail_values(nil, _history), do: {:skipped, [], %{}}

  defp flow_detail_values(record, history) do
    value_refs =
      record
      |> flow_detail_value_refs(history)
      |> Enum.take(@flow_dashboard_value_ref_limit)

    refs = Enum.map(value_refs, & &1.ref)

    if refs == [] do
      {:ok, value_refs, %{}}
    else
      timeout_ms = flow_dashboard_detail_fetch_timeout_ms()

      case bounded_dashboard_call(
             fn -> flow_dashboard_flow_value_mget(refs) end,
             timeout_ms,
             :values
           ) do
        {:ok, {:ok, values}} when is_list(values) and length(values) == length(refs) ->
          {:ok, value_refs, Map.new(Enum.zip(refs, values))}

        {:ok, {:ok, values}} when is_list(values) ->
          {{:error, :unexpected_flow_value_count}, value_refs, %{}}

        {:ok, {:error, reason}} ->
          {{:error, reason}, value_refs, %{}}

        {:ok, _other} ->
          {{:error, :unexpected_flow_value_result}, value_refs, %{}}

        {:error, :timeout} ->
          {:timeout, value_refs, %{}}

        {:error, reason} ->
          {{:error, reason}, value_refs, %{}}
      end
    end
  rescue
    reason -> {{:error, reason}, [], %{}}
  catch
    :exit, reason -> {{:exit, reason}, [], %{}}
  end

  defp flow_dashboard_flow_value_mget(refs) do
    case Application.get_env(:ferricstore, :flow_dashboard_flow_value_mget_fun) do
      fun when is_function(fun, 1) -> fun.(refs)
      _ -> FerricStore.flow_value_mget(refs)
    end
  end

  defp bounded_dashboard_call(fun, timeout_ms, operation) when is_function(fun, 0) do
    started_at = System.monotonic_time()
    task = Task.async(fun)

    result =
      case Task.yield(task, timeout_ms) do
        {:ok, value} ->
          {:ok, value}

        {:exit, reason} ->
          {:error, {:exit, reason}}

        nil ->
          Task.shutdown(task, :brutal_kill)
          {:error, :timeout}
      end

    emit_dashboard_flow_lookup(operation, result, timeout_ms, started_at)
    result
  end

  @spec emit_dashboard_flow_lookup(atom(), term(), non_neg_integer(), integer()) :: :ok
  defp emit_dashboard_flow_lookup(operation, result, timeout_ms, started_at) do
    :telemetry.execute(
      [:ferricstore, :dashboard, :flow, :lookup],
      %{
        duration_us:
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond),
        timeout_ms: timeout_ms
      },
      %{operation: operation, result: dashboard_flow_lookup_result(result)}
    )
  end

  defp dashboard_flow_lookup_result({:ok, _value}), do: :ok
  defp dashboard_flow_lookup_result({:error, :timeout}), do: :timeout
  defp dashboard_flow_lookup_result({:error, {:exit, _reason}}), do: :exit
  defp dashboard_flow_lookup_result({:error, _reason}), do: :error

  @spec flow_waiting_reason(map() | nil) :: binary()
  defp flow_waiting_reason(nil), do: "flow not found"

  defp flow_waiting_reason(record) do
    state = flow_record_state(record)
    now = System.system_time(:millisecond)
    run_at = flow_record_run_at_ms(record)
    worker = flow_record_worker(record)

    cond do
      state in @flow_terminal_states ->
        "terminal: #{state}"

      state == "running" and flow_expired_lease?(record) ->
        "lease expired; reclaimable by workers"

      state == "running" and is_binary(worker) and worker != "" ->
        "leased by #{worker}"

      state == "running" ->
        "running without worker metadata"

      is_integer(run_at) and run_at > now ->
        "scheduled for future"

      state == "queued" ->
        "due now, waiting for worker claim"

      true ->
        "waiting in #{state}"
    end
  end

  @spec flow_due_now?(map()) :: boolean()
  defp flow_due_now?(record) do
    state = flow_record_state(record)
    run_at = flow_record_run_at_ms(record)

    state not in @flow_terminal_states and state != "running" and is_integer(run_at) and
      run_at <= System.system_time(:millisecond)
  end

  @spec flow_retrying?(map()) :: boolean()
  defp flow_retrying?(record) do
    flow_record_attempts(record) > 0 and flow_record_state(record) not in @flow_terminal_states
  end

  @spec flow_failed?(map()) :: boolean()
  defp flow_failed?(record), do: flow_record_state(record) == "failed"

  @spec flow_max_attempts_reached?(map()) :: boolean()
  defp flow_max_attempts_reached?(record) do
    attempts = flow_record_attempts(record)

    case flow_record_max_attempts(record) do
      max_attempts when is_integer(max_attempts) and max_attempts >= 0 and attempts > 0 ->
        attempts >= max_attempts

      _ ->
        false
    end
  end

  @spec flow_expired_lease?(map()) :: boolean()
  defp flow_expired_lease?(record) do
    flow_record_state(record) == "running" and
      case flow_record_lease_expires_at_ms(record) do
        n when is_integer(n) and n > 0 -> n <= System.system_time(:millisecond)
        _ -> false
      end
  end

  @spec flow_oldest_lease_ms([map()]) :: non_neg_integer()
  defp flow_oldest_lease_ms(records) do
    now = System.system_time(:millisecond)

    records
    |> Enum.map(&flow_record_lease_expires_at_ms/1)
    |> Enum.filter(&is_integer/1)
    |> case do
      [] -> 0
      expiries -> max(0, now - Enum.min(expiries))
    end
  end

  @spec flow_record_id(map()) :: binary()
  defp flow_record_id(record), do: flow_field_string(record, :id, "")

  @spec flow_record_type(map()) :: binary()
  defp flow_record_type(record), do: flow_field_string(record, :type, "")

  @spec flow_record_state(map()) :: binary()
  defp flow_record_state(record), do: flow_field_string(record, :state, "unknown")

  @spec flow_record_worker(map()) :: binary() | nil
  defp flow_record_worker(record) do
    case flow_first_non_empty_binary(record, [:worker, :lease_owner]) do
      worker when is_binary(worker) -> worker
      _ -> nil
    end
  end

  @spec flow_first_non_empty_binary(map(), [atom()]) :: binary() | nil
  defp flow_first_non_empty_binary(record, keys) do
    Enum.find_value(keys, fn key ->
      case flow_field(record, key, nil) do
        value when is_binary(value) and value != "" -> value
        _ -> nil
      end
    end)
  end

  @spec flow_record_partition_key(map()) :: binary() | nil
  defp flow_record_partition_key(record) do
    case flow_field(record, :partition_key, nil) do
      partition_key when is_binary(partition_key) and partition_key != "" -> partition_key
      _ -> nil
    end
  end

  @spec flow_record_parent_id(map()) :: binary() | nil
  defp flow_record_parent_id(record) do
    flow_first_non_empty_binary(record, [:parent_flow_id, :parent_id])
  end

  @spec flow_record_root_id(map()) :: binary() | nil
  defp flow_record_root_id(record) do
    flow_first_non_empty_binary(record, [:root_flow_id, :root_id])
  end

  @spec flow_record_correlation_id(map()) :: binary() | nil
  defp flow_record_correlation_id(record) do
    flow_first_non_empty_binary(record, [:correlation_id, :correlation])
  end

  @spec flow_record_run_at_ms(map()) :: integer() | nil
  defp flow_record_run_at_ms(record) do
    flow_first_integer(record, [:run_at_ms, :next_run_at_ms, :due_at_ms])
  end

  @spec flow_record_updated_at_ms(map()) :: integer()
  defp flow_record_updated_at_ms(record) do
    flow_first_integer(record, [:updated_at_ms, :created_at_ms, :run_at_ms]) || 0
  end

  @spec flow_record_lease_expires_at_ms(map()) :: integer() | nil
  defp flow_record_lease_expires_at_ms(record) do
    flow_first_integer(record, [:lease_expires_at_ms, :lease_deadline_ms, :lease_until_ms])
  end

  @spec flow_record_attempts(map()) :: non_neg_integer()
  defp flow_record_attempts(record) do
    case flow_first_integer(record, [:attempts, :attempt]) do
      attempts when is_integer(attempts) and attempts > 0 -> attempts
      _ -> 0
    end
  end

  @spec flow_record_max_attempts(map()) :: integer() | nil
  defp flow_record_max_attempts(record) do
    flow_first_integer(record, [:max_attempts, :max_retries, :retry_max_retries])
  end

  @spec flow_first_integer(map(), [atom()]) :: integer() | nil
  defp flow_first_integer(record, keys) do
    Enum.find_value(keys, fn key ->
      case flow_field(record, key, nil) do
        n when is_integer(n) -> n
        _ -> nil
      end
    end)
  end

  @spec flow_field(map(), atom(), term()) :: term()
  defp flow_field(map, key, default) when is_map(map) and is_atom(key) do
    string_key = Atom.to_string(key)

    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, string_key, default)
    end
  end

  @spec flow_field_string(map(), atom(), binary()) :: binary()
  defp flow_field_string(map, key, default) do
    case flow_field(map, key, default) do
      value when is_binary(value) -> value
      value when is_atom(value) -> Atom.to_string(value)
      value when is_integer(value) -> Integer.to_string(value)
      _ -> default
    end
  end

  @spec collect_raft_shards() :: [raft_shard_data()]
  defp collect_raft_shards do
    Enum.map(0..(shard_count() - 1), fn i ->
      collect_waraft_overview(i)
    end)
  end

  defp collect_waraft_overview(i) do
    case RaftCluster.members(i, 1_000) do
      {:ok, members, leader} ->
        {last_applied, term} =
          case WARaftBackend.storage_position(i) do
            {:ok, {:raft_log_pos, index, position_term}}
            when is_integer(index) and is_integer(position_term) ->
              {index, position_term}

            _other ->
              {0, 0}
          end

        %{
          shard: i,
          status: :ok,
          leader: leader,
          current_term: term,
          commit_index: last_applied,
          last_applied: last_applied,
          log_size: 0,
          members: members
        }

      _error ->
        %{
          shard: i,
          status: :unavailable,
          leader: nil,
          current_term: 0,
          commit_index: 0,
          last_applied: 0,
          log_size: 0,
          members: []
        }
    end
  catch
    :exit, _ ->
      %{
        shard: i,
        status: :unavailable,
        leader: nil,
        current_term: 0,
        commit_index: 0,
        last_applied: 0,
        log_size: 0,
        members: []
      }
  end

  @spec collect_client_list() :: [client_data()]
  defp collect_client_list do
    try do
      summaries = FerricstoreServer.Connection.Registry.summaries()

      if summaries != [] do
        collect_client_list_from_registry(summaries)
      else
        collect_client_list_from_ranch()
      end
    catch
      _, _ -> []
    end
  end

  @spec collect_client_list_from_registry([map()]) :: [client_data()]
  defp collect_client_list_from_registry(summaries) do
    now = System.monotonic_time(:millisecond)

    Enum.map(summaries, fn summary ->
      created =
        case Map.get(summary, :created_at_ms) do
          value when is_integer(value) -> value
          _ -> now
        end

      %{
        pid: Map.get(summary, :pid, self()),
        client_id: Map.get(summary, :client_id),
        client_name: Map.get(summary, :client_name),
        username: Map.get(summary, :username),
        peer: Map.get(summary, :peer, "unknown"),
        age_seconds: max(0, div(now - created, 1000)),
        flags: Map.get(summary, :flags, "")
      }
    end)
  end

  @spec collect_client_list_from_ranch() :: [client_data()]
  defp collect_client_list_from_ranch do
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
                |> then(fn f ->
                  if state && Map.get(state, :multi_state) == :queuing, do: ["M" | f], else: f
                end)
                |> then(fn f ->
                  if state && Map.get(state, :pubsub_channels), do: ["S" | f], else: f
                end)
                |> then(fn f ->
                  if state && Map.get(state, :tracking) && Map.get(state.tracking, :enabled),
                    do: ["T" | f],
                    else: f
                end)

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

    # Hit rate is neutral until there are actual read samples.
    hit_color =
      if hotcold_has_samples?(hotcold), do: hit_rate_color(hotcold.hit_ratio), else: "#8b949e"

    hit_value =
      if hotcold_has_samples?(hotcold), do: "#{hotcold.hit_ratio}%", else: "No read samples"

    # Memory bar
    mem_pct = if memory.max_bytes > 0, do: Float.round(memory.ratio * 100, 1), else: 0.0
    mem_bar_color = mem_bar_color(mem_pct)
    mem_bar_width = min(mem_pct, 100)
    memory_limit = if memory.max_bytes > 0, do: format_bytes(memory.max_bytes), else: "unlimited"

    # Cluster info
    cluster_label =
      case cluster.cluster_mode do
        :standalone -> "single-member WARaft"
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
        <span class="val" style="color:#{hit_color};">#{escape(hit_value)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Memory</span>
        <span class="val" style="font-size:0.85rem;">#{format_bytes(memory.total_bytes)} / #{memory_limit}</span>
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
    has_samples = hotcold_has_samples?(data)
    hit_color = if has_samples, do: hit_rate_color(data.hit_ratio), else: "#8b949e"
    hit_value = if has_samples, do: "#{data.hit_ratio}%", else: "No read samples"

    # RAM bar color -- always green (fast path)
    # Disk bar color -- orange (slow path)
    ram_bar_width = min(data.ram_ratio, 100)
    disk_bar_width = min(data.disk_ratio, 100)

    """
    <div class="section-title">Cache Performance</div>
    <div class="cache-hero">
      <div class="hit-rate-card">
        <div class="hit-rate-num" style="color:#{hit_color};">#{escape(hit_value)}</div>
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
            <div class="source-name">RAM #{sampled_tag(data.sample_rate)} #{info_icon("Served from ETS in-memory cache. Estimated from 1:#{data.sample_rate} sampling. Latency: about 1-5 microseconds.")}</div>
            <div class="source-detail">fast path (~1-5us)</div>
          </div>
          <div class="source-pct c-green">#{data.ram_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{ram_bar_width}%;background:#3fb950;"></div></div>
        <div class="source-row">
          <div>
            <div class="source-name">Disk #{info_icon("Required Bitcask disk read. This is an exact count, not sampled. Latency is usually about 50-200 microseconds. High disk ratio means memory pressure is evicting hot keys.")}</div>
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
    keydir_pct =
      if data.keydir_max_ram > 0, do: Float.round(data.keydir_ratio * 100, 1), else: 0.0

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
          :warning ->
            "Consider increasing max_memory or reviewing eviction policy."

          :pressure ->
            "Eviction active. Keys are being removed under #{escape(Atom.to_string(data.eviction_policy))} policy."

          :reject ->
            "Writes are being rejected. Increase max_memory immediately."

          _ ->
            ""
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
      <span>Hit/miss stats estimated from 1:#{sample_rate} sampling &middot; Live updates patch changed components</span>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Sub-page content sections
  # ---------------------------------------------------------------------------

  @spec render_ops_summary(binary(), [map()]) :: binary()
  defp render_ops_summary(title, cards) do
    card_html =
      Enum.map_join(cards, "\n", fn card ->
        value_class = Map.get(card, :class, "")

        detail_html =
          cond do
            html = Map.get(card, :detail_html) ->
              ~s(<div class="ops-summary-detail">#{html}</div>)

            (detail = Map.get(card, :detail, "")) != "" ->
              ~s(<div class="ops-summary-detail">#{escape(detail)}</div>)

            true ->
              ""
          end

        """
        <div class="ops-summary-card">
          <div class="ops-summary-label">#{escape(Map.fetch!(card, :label))}</div>
          <div class="ops-summary-value #{escape_attr(value_class)}">#{escape(Map.fetch!(card, :value))}</div>
          #{detail_html}
        </div>
        """
      end)

    """
    <div class="section-title">#{escape(title)}</div>
    <div class="ops-summary-grid">
      #{card_html}
    </div>
    """
  end

  @spec render_slowlog_summary([slowlog_entry()]) :: binary()
  defp render_slowlog_summary(entries) do
    count = length(entries)
    total_us = Enum.reduce(entries, 0, fn entry, acc -> acc + max(entry.duration_us, 0) end)
    worst_us = Enum.reduce(entries, 0, fn entry, acc -> max(acc, max(entry.duration_us, 0)) end)
    avg_us = if count > 0, do: div(total_us, count), else: 0

    render_ops_summary("Slow Log Summary", [
      %{label: "Entries", value: format_number(count)},
      %{
        label: "Worst",
        value: format_duration_us(worst_us),
        class: slowlog_duration_class(worst_us)
      },
      %{label: "Avg", value: format_duration_us(avg_us)},
      %{label: "Total Time", value: format_duration_us(total_us)}
    ])
  end

  @spec slowlog_duration_class(non_neg_integer()) :: binary()
  defp slowlog_duration_class(duration_us) when duration_us >= 1_000_000, do: "c-red"
  defp slowlog_duration_class(duration_us) when duration_us >= 100_000, do: "c-yellow"
  defp slowlog_duration_class(_duration_us), do: ""

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

  @spec render_merge_summary([merge_status()]) :: binary()
  defp render_merge_summary(merges) do
    total = length(merges)
    active = Enum.count(merges, & &1.merging)
    total_merges = Enum.reduce(merges, 0, fn m, acc -> acc + max(m.merge_count, 0) end)

    reclaimed =
      Enum.reduce(merges, 0, fn m, acc -> acc + max(m.total_bytes_reclaimed, 0) end)

    latest_merge =
      merges
      |> Enum.map(& &1.last_merge_at)
      |> Enum.filter(&is_integer/1)
      |> Enum.max(fn -> nil end)

    latest_label =
      case latest_merge do
        nil -> "never"
        timestamp_ms -> format_timestamp_ms(timestamp_ms)
      end

    render_ops_summary("Merge Summary", [
      %{
        label: "Active Shards",
        value: "#{format_number(active)} / #{format_number(total)}",
        class: if(active > 0, do: "c-yellow", else: "")
      },
      %{label: "Total Reclaimed", value: format_bytes(reclaimed)},
      %{label: "Total Merges", value: format_number(total_merges)},
      %{label: "Last Merge", value: latest_label}
    ])
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
          ~s[<p style="color:#8b949e; margin: 8px 0; font-size:0.82rem;">All namespaces using built-in default window (1ms)</p>]

        _ ->
          rows =
            Enum.map_join(entries, "\n", fn entry ->
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
                <td>#{changed_at_str}</td>
                <td>#{escape(entry.changed_by)}</td>
              </tr>
              """
            end)

          """
          <table>
            <thead>
              <tr><th>Prefix</th><th>Window (ms)</th><th>Changed At</th><th>Changed By</th></tr>
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

  @spec config_command_reference() :: [config_command_entry()]
  defp config_command_reference do
    [
      %{
        command: "CONFIG GET <pattern>",
        scope: "current node",
        mutability: "read-only",
        notes: "Reads runtime parameters. Supports Redis-style * and ? patterns."
      },
      %{
        command: "CONFIG SET <key> <value>",
        scope: "current node",
        mutability: "read-write",
        notes: "Updates supported runtime parameters. Use CONFIG REWRITE to persist them."
      },
      %{
        command: "CONFIG GET LOCAL <key>",
        scope: "current node",
        mutability: "read-only",
        notes: "Reads node-local ephemeral settings."
      },
      %{
        command: "CONFIG SET LOCAL log_level <level>",
        scope: "current node",
        mutability: "node-local",
        notes: "Sets logger level: debug, info, notice, warning, or error."
      },
      %{
        command: "CONFIG RESETSTAT",
        scope: "current node",
        mutability: "admin",
        notes: "Resets command stats and slowlog."
      },
      %{
        command: "CONFIG REWRITE",
        scope: "current node",
        mutability: "persist",
        notes: "Writes runtime config values to the configured config file."
      },
      %{
        command: "FERRICSTORE.CONFIG GET [prefix]",
        scope: "namespace runtime",
        mutability: "read-only",
        notes: "Shows all namespace commit-window overrides or one prefix."
      },
      %{
        command: "FERRICSTORE.CONFIG SET <prefix> window_ms <ms>",
        scope: "namespace runtime",
        mutability: "read-write",
        notes: "Sets a per-prefix commit window override in milliseconds."
      },
      %{
        command: "FERRICSTORE.CONFIG RESET [prefix]",
        scope: "namespace runtime",
        mutability: "read-write",
        notes: "Clears one namespace override, or all overrides when prefix is omitted."
      }
    ]
  end

  @spec runtime_config_parameter_reference() :: [config_parameter_entry()]
  defp runtime_config_parameter_reference do
    read_write = [
      {"maxmemory-policy", "Eviction/rejection policy used when memory pressure is high."},
      {"notify-keyspace-events", "Redis-compatible keyspace notification setting."},
      {"slowlog-log-slower-than", "Slowlog threshold in microseconds."},
      {"slowlog-max-len", "Maximum slowlog entries kept in memory."},
      {"hz", "Background maintenance frequency."},
      {"keydir-max-ram", "Maximum keydir memory target."},
      {"hot-cache-max-ram", "Maximum hot value cache memory target."},
      {"hot-cache-min-ram", "Minimum hot value cache memory target."},
      {"hot-cache-max-value-size", "Largest value eligible for hot cache storage."}
    ]

    read_only = [
      {"maxmemory", "Configured process memory ceiling."},
      {"maxclients", "Configured client connection ceiling."},
      {"tcp-port", "TCP listener port."},
      {"data-dir", "Persistent storage directory."},
      {"tls-port", "TLS listener port."},
      {"tls-cert-file", "TLS certificate path."},
      {"tls-key-file", "TLS private-key path."},
      {"tls-ca-cert-file", "TLS CA path."},
      {"require-tls", "Whether cleartext client connections are rejected."}
    ]

    legacy = [
      {"timeout", "Redis-compatible setting accepted for client compatibility."},
      {"tcp-keepalive", "Redis-compatible setting accepted for client compatibility."},
      {"databases", "Redis-compatible setting accepted for client compatibility."},
      {"bind", "Redis-compatible setting accepted for client compatibility."},
      {"port", "Redis-compatible setting accepted for client compatibility."},
      {"save", "Redis-compatible setting accepted for client compatibility."},
      {"appendonly", "Redis-compatible setting accepted for client compatibility."},
      {"loglevel", "Redis-compatible setting accepted for client compatibility."},
      {"requirepass", "Redis-compatible setting accepted for client compatibility."}
    ]

    local = [
      {"log_level", "Node-local Logger level. Not persisted or replicated."}
    ]

    Enum.map(read_write, &config_parameter_entry(&1, "runtime", "read-write")) ++
      Enum.map(read_only, &config_parameter_entry(&1, "runtime", "read-only")) ++
      Enum.map(legacy, &config_parameter_entry(&1, "redis-compatible", "read-write")) ++
      Enum.map(local, &config_parameter_entry(&1, "current node", "node-local"))
  end

  @spec config_parameter_entry({binary(), binary()}, binary(), binary()) ::
          config_parameter_entry()
  defp config_parameter_entry({parameter, notes}, scope, mutability) do
    %{parameter: parameter, scope: scope, mutability: mutability, notes: notes}
  end

  @spec render_config_commands([config_command_entry()]) :: binary()
  defp render_config_commands(commands) do
    render_config_command_table("Configuration Commands", commands)
  end

  @spec render_config_command_table(binary(), [config_command_entry()]) :: binary()
  defp render_config_command_table(title, commands) do
    rows =
      Enum.map_join(commands, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.command)}</td>
          <td>#{escape(entry.scope)}</td>
          <td><span class="badge badge-idle">#{escape(entry.mutability)}</span></td>
          <td>#{escape(entry.notes)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">#{escape(title)} <span class="badge badge-idle">#{length(commands)}</span></div>
    <table>
      <thead>
        <tr><th>Command</th><th>Scope</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_config_parameters([config_parameter_entry()]) :: binary()
  defp render_config_parameters(parameters) do
    read_write = Enum.count(parameters, &(&1.mutability == "read-write"))
    read_only = Enum.count(parameters, &(&1.mutability == "read-only"))
    node_local = Enum.count(parameters, &(&1.mutability == "node-local"))

    rows =
      Enum.map_join(parameters, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.parameter)}</td>
          <td>#{escape(entry.scope)}</td>
          <td><span class="badge badge-idle">#{escape(entry.mutability)}</span></td>
          <td>#{escape(entry.notes)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Runtime Parameters <span class="badge badge-idle">read-write #{read_write}</span> <span class="badge badge-idle">read-only #{read_only}</span> <span class="badge badge-idle">node-local #{node_local}</span></div>
    <table>
      <thead>
        <tr><th>Parameter</th><th>Scope</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_cluster_info(cluster_data()) :: binary()
  defp render_cluster_info(cluster) do
    node_str = Atom.to_string(cluster.node_name)

    cluster_badge =
      case cluster.cluster_mode do
        :standalone ->
          ~s(<span class="badge badge-idle">single-member WARaft</span>)

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

  @spec render_consensus_summary([raft_shard_data()]) :: binary()
  defp render_consensus_summary(raft_shards) do
    total = length(raft_shards)
    healthy = Enum.count(raft_shards, &(&1.status == :ok))
    leaders = Enum.count(raft_shards, &match?({_name, _node}, &1.leader))

    max_lag =
      Enum.reduce(raft_shards, 0, fn shard, acc ->
        max(acc, max(shard.commit_index - shard.last_applied, 0))
      end)

    render_ops_summary("WARaft Consensus Summary", [
      %{
        label: "Healthy Shards",
        value: "#{format_number(healthy)} / #{format_number(total)}",
        class: if(healthy == total, do: "c-green", else: "c-red")
      },
      %{
        label: "Max Apply Lag",
        value: format_number(max_lag),
        class: consensus_lag_class(max_lag)
      },
      %{label: "Leaders", value: "#{format_number(leaders)} / #{format_number(total)}"}
    ])
  end

  @spec consensus_lag_class(non_neg_integer()) :: binary()
  defp consensus_lag_class(lag) when lag > 1_000, do: "c-red"
  defp consensus_lag_class(lag) when lag > 100, do: "c-yellow"
  defp consensus_lag_class(_lag), do: "c-green"

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
            nil ->
              ~s(<span class="c-muted">none</span>)

            {name, leader_node} ->
              is_local = leader_node == node()
              class = if is_local, do: "c-green", else: ""
              leader_str = "#{name}@#{leader_node}"

              ~s(<span class="#{class} mono" title="#{escape_attr(leader_str)}">#{escape(short_consensus_member(name, leader_node))}</span>)
          end

        members_str =
          case rs.members do
            [] ->
              "-"

            members ->
              Enum.map_join(members, ", ", fn {name, n} -> short_consensus_member(name, n) end)
          end

        lag = rs.commit_index - rs.last_applied

        lag_class =
          cond do
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
          <td class="mono" style="font-size:0.75rem;">#{escape(members_str)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Per-Shard WARaft State #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Leader</th><th>Term</th><th>Commit Idx</th><th>Applied Idx</th><th>Members</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec short_consensus_member(term(), term()) :: binary()
  defp short_consensus_member(name, node_name) do
    name_str = to_string(name)
    node_str = to_string(node_name)

    shard_label =
      case Regex.run(~r/_(\d+)$/, name_str) do
        [_, shard] -> "shard-" <> shard
        _ -> name_str
      end

    shard_label <> " @ " <> node_str
  end

  @spec render_clients_summary(connections_data(), [client_data()]) :: binary()
  defp render_clients_summary(conns, clients) do
    oldest_age =
      clients
      |> Enum.map(& &1.age_seconds)
      |> Enum.max(fn -> 0 end)

    pubsub = Enum.count(clients, &String.contains?(&1.flags, "S"))
    transactions = Enum.count(clients, &String.contains?(&1.flags, "M"))

    render_ops_summary("Client Summary", [
      %{label: "Active", value: format_number(conns.active)},
      %{
        label: "Blocked",
        value: format_number(conns.blocked),
        class: if(conns.blocked > 0, do: "c-yellow", else: "")
      },
      %{label: "Tracking", value: format_number(conns.tracking), detail: "#{pubsub} Pub/Sub"},
      %{
        label: "Transactions",
        value: format_number(transactions),
        detail: "Oldest #{format_uptime(oldest_age)}"
      }
    ])
  end

  @spec render_clients_table([client_data()]) :: binary()
  defp render_clients_table(clients) do
    rows =
      case clients do
        [] ->
          ~s(<tr><td colspan="6" class="c-muted">No active connections</td></tr>)

        _ ->
          Enum.map_join(clients, "\n", fn c ->
            id = Map.get(c, :client_id)
            id_str = if is_integer(id), do: Integer.to_string(id), else: inspect(c.pid)
            name = Map.get(c, :client_name) || "-"
            user = Map.get(c, :username) || "default"

            """
            <tr>
              <td class="mono">#{escape(id_str)}</td>
              <td>#{escape(name)}</td>
              <td class="mono">#{escape(user)}</td>
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
        <tr><th>ID</th><th>Name</th><th>User</th><th>Client Address</th><th>Age</th><th>Flags</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
        <div style="margin-top:8px; font-size:0.72rem; color:#8b949e;">
      Flags: M=in MULTI transaction, S=subscribed (pub/sub), T=tracking enabled
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- FerricFlow Sub-pages
  # ---------------------------------------------------------------------------

  @spec render_flow_overview(map(), non_neg_integer(), pos_integer()) :: binary()
  defp render_flow_overview(summary, total_sampled, sample_limit) do
    """
    <div class="section-title">Flow Overview <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <div class="flow-card-grid">
      #{render_flow_stat_card("Types", Map.get(summary, :types, 0), "discovered workflow types")}
      #{render_flow_stat_card("Active", Map.get(summary, :active, 0), "queued + running")}
      #{render_flow_stat_card("Queued", Map.get(summary, :queued, 0), "ready or scheduled")}
      #{render_flow_stat_card("Running", Map.get(summary, :running, 0), "leased by workers")}
      #{render_flow_stat_card("Failed", Map.get(summary, :failed, 0), "terminal failures")}
      #{render_flow_stat_card("Inflight", Map.get(summary, :inflight, 0), "index-backed lease count")}
    </div>
    """
  end

  @spec render_flow_stat_card(binary(), non_neg_integer() | binary(), binary()) :: binary()
  defp render_flow_stat_card(label, value, detail) do
    rendered_value =
      case value do
        value when is_binary(value) -> escape(value)
        value when is_integer(value) -> format_number(value)
        value -> escape(to_string(value))
      end

    """
    <div class="flow-card">
      <div class="flow-card-label">#{escape(label)}</div>
      <div class="flow-card-value">#{rendered_value}</div>
      <div class="flow-card-detail">#{escape(detail)}</div>
    </div>
    """
  end

  @spec render_flow_subnav(binary()) :: binary()
  defp render_flow_subnav(active) do
    groups = [
      {"Monitor",
       [
         {"overview", "/dashboard/flow", "Overview",
          "Flow summary, projection health, and recent records"},
         {"states", "/dashboard/flow/states", "States",
          "Filter Flow records by type, state, time, and ID"},
         {"workers", "/dashboard/flow/workers", "Workers", "Worker leases and running work"},
         {"due", "/dashboard/flow/due", "Due", "Claimable and expired Flow work"}
       ]},
      {"Debug",
       [
         {"failures", "/dashboard/flow/failures", "Failures",
          "Failed, stuck, and expired-lease recovery"},
         {"lineage", "/dashboard/flow/lineage", "Lineage",
          "Parent, root, and correlation queries"},
         {"query", "/dashboard/flow/query", "Query", "Bounded Flow query explorer"},
         {"signals", "/dashboard/flow/signals", "Signals", "Recent FLOW.SIGNAL events"}
       ]},
      {"Operate",
       [
         {"policies", "/dashboard/flow/policies", "Policies",
          "Retry and retention policy editor"},
         {"retention", "/dashboard/flow/retention", "Retention",
          "Terminal cleanup and disk-pressure maintenance"}
       ]}
    ]

    links =
      Enum.map_join(groups, "\n", fn {group_label, items} ->
        rendered_items =
          Enum.map_join(items, "\n", fn {key, href, label, title} ->
            active_class = if key == active, do: " active", else: ""
            current = if key == active, do: ~s( aria-current="page"), else: ""

            ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
          end)

        """
        <div class="flow-tab-group">
          <span class="flow-tab-group-label">#{escape(group_label)}</span>
          <div class="flow-tab-group-links">#{rendered_items}</div>
        </div>
        """
      end)

    """
    <div class="flow-nav-row">
      <div class="flow-tabs">
        #{links}
      </div>
      <form class="flow-search" action="/dashboard/flow/lookup" method="get" aria-label="Flow lookup">
        <input class="flow-search-input mono" type="search" name="id" placeholder="Search flow ID" autocomplete="off" aria-label="Flow ID" title="Open a flow by ID.">
        <input class="flow-search-input mono" type="search" name="partition_key" placeholder="Partition key" autocomplete="off" aria-label="Partition key" title="With a Flow ID, scopes the detail lookup. Without a Flow ID, filters the overview to this partition.">
        <button class="flow-search-button" type="submit" title="Open a flow by ID or filter overview by partition">Search</button>
      </form>
    </div>
    """
  end

  @spec render_flow_failures_flash(map()) :: binary()
  defp render_flow_failures_flash(%{flash: %{kind: :ok, message: message}}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  defp render_flow_failures_flash(%{flash: %{kind: :error, message: message}}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  defp render_flow_failures_flash(_data), do: ""

  @spec render_flow_exact_scan_status(map()) :: binary()
  defp render_flow_exact_scan_status(data) do
    filters = flow_failures_page_filters(data)

    if Map.get(filters, :scan_exact, false) do
      status = Map.get(data, :exact_scan_status, %{failures: :skipped, stuck: :skipped})

      status
      |> Enum.flat_map(fn {source, source_status} ->
        case source_status do
          {:error, reason} ->
            [
              """
              <div class="flow-alert flow-alert-error">
                Exact scan issue: #{flow_recovery_source_command(source)} failed with #{escape(inspect(reason, limit: 8))}. Sampled rows are still shown; zero candidates is not authoritative.
              </div>
              """
            ]

          _ ->
            []
        end
      end)
      |> Enum.join("")
    else
      ""
    end
  end

  defp flow_recovery_source_command(:failures), do: "FLOW.FAILURES"
  defp flow_recovery_source_command(:stuck), do: "FLOW.STUCK"
  defp flow_recovery_source_command(source), do: source |> to_string() |> String.upcase()

  @spec render_flow_failures_controls(map()) :: binary()
  defp render_flow_failures_controls(data),
    do: component_flow_failures_controls(%{data: data})

  @spec render_flow_recovery_actions(map()) :: binary()
  defp render_flow_recovery_actions(data),
    do: component_flow_recovery_actions(%{data: data})

  @spec render_flow_failures_summary(map()) :: binary()
  defp render_flow_failures_summary(data),
    do: component_flow_failures_summary(%{data: data})

  @spec render_flow_failures_table(map()) :: binary()
  defp render_flow_failures_table(data),
    do: component_flow_failures_table(%{data: data})

  @spec render_flow_lineage_controls(map()) :: binary()
  defp render_flow_lineage_controls(data),
    do: component_flow_lineage_controls(%{data: data})

  @spec render_flow_lineage_summary(map()) :: binary()
  defp render_flow_lineage_summary(data),
    do: component_flow_lineage_summary(%{data: data})

  @spec render_flow_lineage_graph(map()) :: binary()
  defp render_flow_lineage_graph(data),
    do: component_flow_lineage_graph(%{data: data})

  @spec render_flow_lineage_table(map()) :: binary()
  defp render_flow_lineage_table(data),
    do: component_flow_lineage_table(%{data: data})

  @spec render_flow_query_controls(map()) :: binary()
  defp render_flow_query_controls(data),
    do: component_flow_query_controls(%{data: data})

  @spec render_flow_query_result(map()) :: binary()
  defp render_flow_query_result(data),
    do: component_flow_query_result(%{data: data})

  @spec render_flow_query_kind_help(binary()) :: binary()
  defp render_flow_query_kind_help(kind) do
    doc = flow_query_kind_doc(kind)

    """
    <div class="flow-query-help" data-flow-query-help>
      <div class="flow-query-help-main">
        <span class="flow-query-command" data-flow-query-help-command>#{escape(doc.command)}</span>
        <span data-flow-query-help-purpose>#{escape(doc.purpose)}</span>
      </div>
      <div class="flow-query-help-detail" data-flow-query-help-detail>#{escape(doc.detail)}</div>
    </div>
    """
  end

  @spec render_flow_query_type_field(map()) :: binary()
  defp render_flow_query_type_field(%{type: type} = filters) do
    kinds = ~w(list terminals failures stuck)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-query-field" data-flow-query-field="type" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Workflow Type
      <input class="flow-search-input mono" name="type" value="#{escape_attr(type || "")}" placeholder="email"#{disabled}>
      <span class="flow-field-help">Required. Scopes the query to one Flow type.</span>
    </label>
    """
  end

  @spec render_flow_query_state_field(map()) :: binary()
  defp render_flow_query_state_field(%{state: state} = filters) do
    kinds = ~w(list terminals failures)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-query-field" data-flow-query-field="state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State
      <input class="flow-search-input mono" name="state" value="#{escape_attr(state || "")}" placeholder="optional"#{disabled}>
      <span class="flow-field-help">Optional state filter for this type.</span>
    </label>
    """
  end

  @spec render_flow_query_id_field(map()) :: binary()
  defp render_flow_query_id_field(%{kind: kind, id: id} = filters) do
    kinds = ~w(history by_parent by_root by_correlation)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    doc = flow_query_kind_doc(kind)

    """
    <label class="flow-query-field" data-flow-query-field="id" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <span data-flow-query-id-label>#{escape(Map.get(doc, :id_label, "Flow ID"))}</span>
      <input class="flow-search-input mono" name="id" value="#{escape_attr(id || "")}" placeholder="#{escape_attr(Map.get(doc, :id_placeholder, "workflow id"))}" data-flow-query-id-input#{disabled}>
      <span class="flow-field-help" data-flow-query-id-help>#{escape(Map.get(doc, :id_help, "Required id for this query."))}</span>
    </label>
    """
  end

  @spec render_flow_query_partition_field(map()) :: binary()
  defp render_flow_query_partition_field(%{partition_key: partition_key}) do
    """
    <label class="flow-query-field">
      Partition
      <input class="flow-search-input mono" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="optional">
      <span class="flow-field-help">Optional. Use it when the workflow id or index is partition-scoped.</span>
    </label>
    """
  end

  @spec render_flow_query_time_fields(map()) :: binary()
  defp render_flow_query_time_fields(filters) do
    kinds = ~w(list terminals failures stuck by_parent by_root by_correlation)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-query-field" data-flow-query-field="from" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      From UTC
      <input class="flow-search-input mono flow-filter-time" type="datetime-local" name="from" step="60" value="#{escape_attr(flow_filter_time_value(filters.from_ms))}" title="Optional start time for index queries"#{disabled}>
      <span class="flow-field-help">Optional lower bound for indexed query time.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="to" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      To UTC
      <input class="flow-search-input mono flow-filter-time" type="datetime-local" name="to" step="60" value="#{escape_attr(flow_filter_time_value(filters.to_ms))}" title="Optional end time for index queries"#{disabled}>
      <span class="flow-field-help">Optional upper bound for indexed query time.</span>
    </label>
    """
  end

  @spec render_flow_query_direction_field(map()) :: binary()
  defp render_flow_query_direction_field(filters) do
    kinds = ~w(list terminals failures stuck by_parent by_root by_correlation)
    checked = if filters.rev, do: "checked", else: ""
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-check-label flow-query-check" title="Newest records first for query APIs that support reverse order" data-flow-query-field="direction" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <input type="checkbox" name="rev" value="true" #{checked}#{disabled}>
      Newest first
    </label>
    """
  end

  @spec render_flow_query_dynamic_script() :: binary()
  defp render_flow_query_dynamic_script do
    docs_json = Jason.encode!(flow_query_kind_docs())

    """
    <script>
    (() => {
      const form = document.currentScript.closest(".flow-policy-panel")?.querySelector("[data-flow-query-form]");
      if (!form) return;
      const docs = #{docs_json};
      const select = form.querySelector("[data-flow-query-kind]");
      const help = form.closest(".flow-policy-panel")?.querySelector("[data-flow-query-help]");
      const idLabel = form.querySelector("[data-flow-query-id-label]");
      const idInput = form.querySelector("[data-flow-query-id-input]");
      const idHelp = form.querySelector("[data-flow-query-id-help]");
      const setText = (selector, value) => {
        const node = help && help.querySelector(selector);
        if (node) node.textContent = value || "";
      };
      const allowed = (node, kind) => (node.dataset.flowQueryKinds || "").split(" ").includes(kind);
      const update = () => {
        const kind = select?.value || "list";
        const doc = docs[kind] || docs.list;
        form.querySelectorAll("[data-flow-query-kinds]").forEach((field) => {
          const visible = allowed(field, kind);
          field.hidden = !visible;
          field.querySelectorAll("input, select, textarea").forEach((input) => {
            input.disabled = !visible;
          });
        });
        setText("[data-flow-query-help-command]", doc.command);
        setText("[data-flow-query-help-purpose]", doc.purpose);
        setText("[data-flow-query-help-detail]", doc.detail);
        if (idLabel) idLabel.textContent = doc.id_label || "Flow ID";
        if (idInput) idInput.placeholder = doc.id_placeholder || "workflow id";
        if (idHelp) idHelp.textContent = doc.id_help || "Required id for this query.";
      };
      select?.addEventListener("change", update);
      update();
    })();
    </script>
    """
  end

  @spec flow_query_hidden_attr(map(), [binary()]) :: binary()
  defp flow_query_hidden_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " hidden")

  @spec flow_query_disabled_attr(map(), [binary()]) :: binary()
  defp flow_query_disabled_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " disabled")

  @spec flow_query_kinds_attr([binary()]) :: binary()
  defp flow_query_kinds_attr(kinds), do: kinds |> Enum.join(" ") |> escape_attr()

  @spec render_flow_type_options([binary()], binary() | nil) :: binary()
  defp render_flow_type_options(types, selected_type) do
    all_selected = if selected_type in [nil, ""], do: " selected", else: ""

    all =
      ~s(<option value=""#{all_selected}>All types</option>)

    options =
      types
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.map_join("\n", fn type ->
        selected = if type == selected_type, do: " selected", else: ""
        ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    all <> "\n" <> options
  end

  @spec render_flow_lineage_mode_options(binary()) :: binary()
  defp render_flow_lineage_mode_options(selected_mode) do
    [
      {"root", "Root"},
      {"parent", "Parent"},
      {"correlation", "Correlation"}
    ]
    |> Enum.map_join("\n", fn {mode, label} ->
      selected = if mode == selected_mode, do: " selected", else: ""
      ~s(<option value="#{mode}"#{selected}>#{label}</option>)
    end)
  end

  @spec render_flow_query_kind_options(binary()) :: binary()
  defp render_flow_query_kind_options(selected_kind) do
    flow_query_kind_options()
    |> Enum.map_join("\n", fn {kind, label} ->
      selected = if kind == selected_kind, do: " selected", else: ""
      ~s(<option value="#{kind}"#{selected}>#{label}</option>)
    end)
  end

  @spec flow_query_kind_options() :: [{binary(), binary()}]
  defp flow_query_kind_options do
    [
      {"list", "FLOW.LIST"},
      {"terminals", "FLOW.TERMINALS"},
      {"failures", "FLOW.FAILURES"},
      {"stuck", "FLOW.STUCK"},
      {"history", "FLOW.HISTORY"},
      {"by_parent", "FLOW.BY_PARENT"},
      {"by_root", "FLOW.BY_ROOT"},
      {"by_correlation", "FLOW.BY_CORRELATION"}
    ]
  end

  @spec flow_query_kind_doc(binary()) :: map()
  defp flow_query_kind_doc(kind) do
    docs = flow_query_kind_docs()
    Map.get(docs, kind, Map.fetch!(docs, "list"))
  end

  @spec flow_query_kind_docs() :: map()
  defp flow_query_kind_docs do
    %{
      "list" => %{
        command: "FLOW.LIST",
        purpose: "List workflows by type.",
        detail:
          "Use optional state, partition, time range, and direction filters to keep the result bounded."
      },
      "terminals" => %{
        command: "FLOW.TERMINALS",
        purpose: "List terminal workflows for a type.",
        detail:
          "Use this to audit completed, failed, or cancelled workflow retention and terminal distribution."
      },
      "failures" => %{
        command: "FLOW.FAILURES",
        purpose: "List failed workflows for a type.",
        detail:
          "Use this to inspect failure pressure before retrying, rewinding, or running retention cleanup."
      },
      "stuck" => %{
        command: "FLOW.STUCK",
        purpose: "Find running workflows whose leases or progress look stale.",
        detail:
          "State is intentionally hidden here; this query is driven by type, partition, and indexed time bounds."
      },
      "history" => %{
        command: "FLOW.HISTORY",
        purpose: "Load a bounded history page for one workflow.",
        detail: "Use the Flow detail page for event pagination and value inspection.",
        id_label: "Flow ID",
        id_placeholder: "workflow id",
        id_help: "Required. The workflow whose history should be loaded."
      },
      "by_parent" => %{
        command: "FLOW.BY_PARENT",
        purpose: "List workflows created under one parent.",
        detail: "Use this for fanout debugging when one workflow spawned many children.",
        id_label: "Parent ID",
        id_placeholder: "parent workflow id",
        id_help: "Required. Matches workflows whose parent_id equals this value."
      },
      "by_root" => %{
        command: "FLOW.BY_ROOT",
        purpose: "List workflows in one root lineage.",
        detail: "Use this to inspect the full tree that belongs to one root workflow.",
        id_label: "Root ID",
        id_placeholder: "root workflow id",
        id_help: "Required. Matches workflows whose root_id equals this value."
      },
      "by_correlation" => %{
        command: "FLOW.BY_CORRELATION",
        purpose: "List workflows sharing one correlation id.",
        detail:
          "Use this for request, tenant, IoT fanout, or external job correlation debugging.",
        id_label: "Correlation ID",
        id_placeholder: "correlation id",
        id_help: "Required. Matches workflows whose correlation_id equals this value."
      }
    }
  end

  @spec render_flow_overview_filter(map()) :: binary()
  defp render_flow_overview_filter(data) when is_map(data) do
    filters = flow_page_filters(data)

    case Map.get(filters, :partition_key) do
      partition_key when is_binary(partition_key) and partition_key != "" ->
        filtered = Map.get(data, :filtered_sampled, 0)
        total = Map.get(data, :total_sampled, filtered)

        """
        <div class="flow-filter-summary">
          Showing partition <span class="mono">#{escape(partition_key)}</span>
          <span class="badge badge-idle">#{format_number(filtered)} / #{format_number(total)} sampled</span>
          <a class="flow-filter-clear" href="/dashboard/flow" title="Clear the partition filter">Clear</a>
        </div>
        """

      _ ->
        ""
    end
  end

  @spec flow_overview_live_url(map()) :: binary()
  defp flow_overview_live_url(filters) when is_map(filters) do
    partition_key = Map.get(filters, :partition_key)

    case partition_key do
      key when is_binary(key) and key != "" ->
        "/dashboard/api/flow?" <> URI.encode_query(%{"partition_key" => key})

      _ ->
        "/dashboard/api/flow"
    end
  end

  @spec render_flow_policy_editor(map()) :: binary()
  defp render_flow_policy_editor(data) do
    editor = Map.get(data, :editor, flow_policy_editor_data(""))
    flash = render_flow_policy_flash(Map.get(data, :flash))

    """
    <div id="flow-policy-editor" class="flow-policy-panel">
      <div class="section-title">Create / Update Policy #{info_icon("Policies affect new Flow work and retry scheduling. Existing Flow records keep their durable state.")}</div>
      #{flash}
      <form class="flow-policy-form" action="/dashboard/flow/policies" method="post">
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Type</span>
            <input class="flow-search-input mono" type="text" name="type" value="#{escape_attr(editor.type)}" autocomplete="off" required title="Flow type this policy applies to">
          </label>
          <label class="flow-policy-field">
            <span>State override</span>
            <input class="flow-search-input mono" type="text" name="state" value="#{escape_attr(editor.state)}" autocomplete="off" placeholder="optional" title="Optional state-specific override for this type">
          </label>
          <label class="flow-policy-field">
            <span>Max retries</span>
            <input class="flow-search-input mono" type="number" name="max_retries" min="0" value="#{editor.max_retries}" required title="Maximum FLOW.RETRY attempts before the workflow is exhausted">
          </label>
          <label class="flow-policy-field">
            <span>Backoff</span>
            #{render_flow_policy_backoff_select(editor.backoff_kind)}
          </label>
          <label class="flow-policy-field">
            <span>Base ms</span>
            <input class="flow-search-input mono" type="number" name="base_ms" min="0" value="#{editor.base_ms}" required title="Initial retry delay in milliseconds">
          </label>
          <label class="flow-policy-field">
            <span>Max ms</span>
            <input class="flow-search-input mono" type="number" name="max_ms" min="0" value="#{editor.max_ms}" required title="Maximum retry delay in milliseconds">
          </label>
          <label class="flow-policy-field">
            <span>Jitter %</span>
            <input class="flow-search-input mono" type="number" name="jitter_pct" min="0" max="100" value="#{editor.jitter_pct}" required title="Randomized retry delay percentage to avoid synchronized retries">
          </label>
          <label class="flow-policy-field">
            <span>Exhausted to</span>
            <input class="flow-search-input mono" type="text" name="exhausted_to" value="#{escape_attr(editor.exhausted_to)}" autocomplete="off" required title="Terminal state used when retry attempts are exhausted">
          </label>
          <label class="flow-policy-field">
            <span>Retention ttl ms</span>
            <input class="flow-search-input mono" type="number" name="retention_ttl_ms" min="1" value="#{editor.retention_ttl_ms}" required title="How long terminal state, history, and generated values are retained">
          </label>
          <label class="flow-policy-field">
            <span>Max history</span>
            <input class="flow-search-input mono" type="number" name="history_max_events" min="1" value="#{editor.history_max_events}" required title="Maximum durable history events retained before cleanup can trim old events">
          </label>
        </div>
        #{render_flow_policy_preview(editor)}
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit" title="Save this Flow policy">Save Policy</button>
        </div>
      </form>
    </div>
    """
  end

  @spec render_flow_policy_flash(map() | nil) :: binary()
  defp render_flow_policy_flash(%{kind: :ok, message: message, type: type}) do
    suffix = if type in [nil, ""], do: "", else: " for #{type}"
    ~s(<div class="flow-alert flow-alert-ok">#{escape(message <> suffix)}</div>)
  end

  defp render_flow_policy_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  defp render_flow_policy_flash(_flash), do: ""

  @spec render_flow_policy_preview(map()) :: binary()
  defp render_flow_policy_preview(editor) do
    scope =
      case Map.get(editor, :state, "") do
        state when is_binary(state) and state != "" -> "state override: #{state}"
        _ -> "global defaults for this type"
      end

    ttl = format_duration_ms(Map.get(editor, :retention_ttl_ms, 0))
    history = format_number(Map.get(editor, :history_max_events, 0))

    """
    <div class="flow-policy-preview">
      <div class="flow-policy-preview-title">Review before saving</div>
      <div>Scope: <span class="mono">#{escape(scope)}</span></div>
      <div>Retry: #{format_number(Map.get(editor, :max_retries, 0))} attempts, #{escape(to_string(Map.get(editor, :backoff_kind, :exponential)))} backoff, exhausted to <span class="mono">#{escape(to_string(Map.get(editor, :exhausted_to, "failed")))}</span></div>
      <div>Retention: keep terminal Flow records for #{escape(ttl)} and retain up to #{history} history events before cleanup.</div>
      <div class="flow-filter-note">Requires +FLOW.POLICY.SET. The save operation writes durable policy config; active Flow records keep their current state.</div>
    </div>
    """
  end

  @spec render_flow_policy_backoff_select(atom() | binary()) :: binary()
  defp render_flow_policy_backoff_select(current) do
    current = current |> to_string() |> String.downcase()

    options =
      Enum.map_join(~w(none fixed linear exponential), "\n", fn kind ->
        selected = if kind == current, do: ~s( selected), else: ""
        ~s(<option value="#{kind}"#{selected}>#{String.capitalize(kind)}</option>)
      end)

    ~s(<select class="flow-search-input" name="backoff_kind" title="Retry delay strategy">#{options}</select>)
  end

  @spec render_flow_policy_commands() :: binary()
  defp render_flow_policy_commands do
    render_config_command_table("Flow Policy Commands", flow_policy_command_reference())
  end

  @spec flow_policy_command_reference() :: [config_command_entry()]
  defp flow_policy_command_reference do
    [
      %{
        command: "FLOW.POLICY.SET <type> MAX_RETRIES <n> BACKOFF <kind>",
        scope: "Flow type",
        mutability: "read-write",
        notes:
          "Sets retry defaults for new work of a Flow type. BACKOFF is NONE, FIXED, LINEAR, or EXPONENTIAL."
      },
      %{
        command: "FLOW.POLICY.SET <type> RETENTION_TTL_MS <ms>",
        scope: "Flow type",
        mutability: "read-write",
        notes:
          "Controls how long terminal Flow state, history, and generated values are retained."
      },
      %{
        command: "FLOW.POLICY.GET <type> [STATE <state>]",
        scope: "Flow type",
        mutability: "read-only",
        notes:
          "Reads the effective retry and retention policy, including defaults and state overrides."
      }
    ]
  end

  @spec render_flow_retention_summary(map()) :: binary()
  defp render_flow_retention_summary(data) do
    storage = Map.get(data, :storage, %{})
    projection = Map.get(data, :projection, default_flow_projection_health())

    metrics =
      case Map.get(projection, :metrics, %{}) do
        metric_map when is_map(metric_map) -> metric_map
        _ -> %{}
      end

    pending =
      case Map.get(metrics, :lmdb_pending, Map.get(metrics, "lmdb_pending", 0)) do
        value when is_integer(value) and value >= 0 -> value
        _ -> 0
      end

    """
    <div class="section-title">Sample Preview <span class="badge badge-idle">sampled #{format_number(Map.get(data, :total_sampled, 0))} / #{format_number(Map.get(data, :sample_limit, @flow_dashboard_sample_limit))}</span></div>
    <div class="flow-card-grid">
      #{render_flow_stat_card("Eligible", Map.get(data, :eligible_sampled, 0), "expired terminal Flow records in sample")}
      #{render_flow_stat_card("Terminal", Map.get(data, :terminal_sampled, 0), "completed, failed, or cancelled records")}
      #{render_flow_stat_card("Active", Map.get(data, :active_sampled, 0), "not eligible for retention cleanup")}
      #{render_flow_stat_card("Disk", format_bytes(Map.get(storage, :total_disk_bytes, 0)), "current data directory footprint")}
      #{render_flow_stat_card("Query Index Lag", pending, "cold query-index work pending before cleanup")}
    </div>
    """
  end

  @spec render_flow_retention_controls(map()) :: binary()
  defp render_flow_retention_controls(data) do
    limit = Map.get(data, :limit, @flow_dashboard_retention_default_limit)
    flash = render_flow_retention_flash(Map.get(data, :flash))

    """
    <div id="flow-retention-maintenance" class="flow-policy-panel">
      <div class="section-title">Retention Cleanup #{info_icon("Deletes terminal Flow state, history, and generated values whose retention TTL has expired. Active Flow records are not touched.")}</div>
      #{flash}
      <div class="pressure-alert level-warning">
        <div class="pressure-details">
          Dry Run only previews sampled eligible records. Run Cleanup executes the durable FLOW.RETENTION_CLEANUP command globally with the supplied limit. A sampled preview of zero is not proof that global cleanup will remove zero rows.
        </div>
      </div>
      <form class="flow-policy-form" action="/dashboard/flow/retention" method="post">
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Limit</span>
            <input class="flow-search-input mono" type="number" name="limit" min="1" max="#{@flow_dashboard_retention_max_limit}" value="#{limit}" required title="Maximum terminal Flow records to clean in this command">
          </label>
        </div>
        <label class="flow-check-label" title="Required before the destructive cleanup command is accepted.">
          <input type="checkbox" name="confirm_cleanup" value="true">
          I reviewed the sample preview and understand cleanup is global.
        </label>
        <div class="flow-filter-note">Requires +FLOW.RETENTION_CLEANUP. Use Dry Run first; cleanup is intentionally separate from the sampled preview.</div>
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit" name="action" value="dry_run" title="Preview eligible terminal records without deleting data">Dry Run</button>
          <button class="flow-search-button flow-danger-button" type="submit" name="action" value="cleanup" title="Run durable retention cleanup now">Run Cleanup</button>
        </div>
      </form>
    </div>
    """
  end

  @spec render_flow_retention_flash(map() | nil) :: binary()
  defp render_flow_retention_flash(%{kind: :dry_run, limit: limit}) do
    ~s(<div class="flow-alert flow-alert-ok">Dry run ready for limit #{format_number(limit)}. No data was removed.</div>)
  end

  defp render_flow_retention_flash(%{kind: :ok, counts: counts, limit: limit}) do
    message =
      "Cleanup completed: #{format_number(Map.get(counts, :flows, 0))} flows, " <>
        "#{format_number(Map.get(counts, :history, 0))} history rows, " <>
        "#{format_number(Map.get(counts, :values, 0))} values removed (limit #{format_number(limit)})."

    ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)
  end

  defp render_flow_retention_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  defp render_flow_retention_flash(_flash), do: ""

  @spec render_flow_retention_commands() :: binary()
  defp render_flow_retention_commands do
    render_config_command_table("Flow Retention Commands", flow_retention_command_reference())
  end

  @spec flow_retention_command_reference() :: [config_command_entry()]
  defp flow_retention_command_reference do
    [
      %{
        command: "FLOW.RETENTION_CLEANUP [LIMIT <n>]",
        scope: "Flow data",
        mutability: "read-write",
        notes:
          "Deletes expired terminal Flow records, their durable history, generated payload/result/error values, and shared value links."
      },
      %{
        command: "FLOW.POLICY.SET <type> RETENTION_TTL_MS <ms>",
        scope: "Flow type",
        mutability: "read-write",
        notes:
          "Sets how long terminal Flow data is retained before cleanup is allowed to remove it."
      }
    ]
  end

  @spec render_flow_retention_candidates(map()) :: binary()
  defp render_flow_retention_candidates(data) do
    candidates = Map.get(data, :candidates, [])
    now_ms = Map.get(data, :now_ms, System.system_time(:millisecond))

    rows =
      case candidates do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">No expired terminal Flow records found in the dashboard sample.</td>
          </tr>
          """

        _ ->
          Enum.map_join(candidates, "\n", &render_flow_retention_candidate_row(&1, now_ms))
      end

    """
    <div class="section-title">Sampled Cleanup Candidates <span class="badge badge-idle">#{format_number(length(candidates))}</span></div>
    <table>
      <thead>
        <tr>
          <th>Flow</th>
          <th>Type</th>
          <th>State</th>
          <th>Partition</th>
          <th>Attempts</th>
          <th>Retention Until</th>
          <th>Expired For</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_retention_candidate_row(map(), integer()) :: binary()
  defp render_flow_retention_candidate_row(record, now_ms) do
    id = flow_record_id(record)
    partition_key = flow_record_partition_key(record)
    retention_until = flow_retention_until_ms(record)
    expired_for = if is_integer(retention_until), do: max(now_ms - retention_until, 0), else: 0
    href = flow_detail_path(id, flow_detail_url_partition_key(partition_key))

    """
    <tr>
      <td><a class="mono" href="#{href}">#{escape(id)}</a></td>
      <td class="mono">#{escape(flow_record_type(record))}</td>
      <td><span class="flow-pill flow-pill-terminal">#{escape(flow_record_state(record))}</span></td>
      <td class="mono">#{escape(partition_key || "-")}</td>
      <td>#{format_number(flow_record_attempts(record))}</td>
      <td>#{format_timestamp_ms_or_dash(retention_until)}</td>
      <td>#{format_duration_ms(expired_for)}</td>
      <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
    </tr>
    """
  end

  @spec render_flow_policies_table([map()], map()) :: binary()
  defp render_flow_policies_table(policies, policy_scan) do
    rows =
      case policies do
        [] ->
          """
          <tr>
            <td colspan="8" class="c-muted">No Flow types or policy overrides found in the current sample.</td>
          </tr>
          """

        _ ->
          Enum.map_join(policies, "\n", &render_flow_policy_row/1)
      end

    scan_note = render_flow_policy_scan_note(policy_scan)

    """
    <div class="section-title">Current Flow Policies <span class="badge badge-idle">#{format_number(length(policies))}</span></div>
    #{scan_note}
    <table>
      <thead>
        <tr>
          <th>Type</th>
          <th>Source</th>
          <th>Retries</th>
          <th>Backoff</th>
          <th>Exhausted To</th>
          <th>Retention</th>
          <th>State Overrides</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_policy_scan_note(map()) :: binary()
  defp render_flow_policy_scan_note(policy_scan) do
    scanned = Map.get(policy_scan, :scanned_entries, 0)
    truncated = Map.get(policy_scan, :truncated, false)

    suffix =
      if truncated do
        " Scan hit the dashboard safety limit; create a Flow record for a type if its policy is not visible."
      else
        ""
      end

    """
    <div class="flow-help">
      Shows effective policies for sampled active Flow types plus configured policy keys discovered from a bounded keydir scan
      (#{format_number(scanned)} entries inspected).#{escape(suffix)}
    </div>
    """
  end

  @spec render_flow_policy_row(map()) :: binary()
  defp render_flow_policy_row(%{error: error} = row) when is_binary(error) do
    """
    <tr>
      <td class="mono">#{escape(row.type)}</td>
      <td><span class="badge badge-pressure">error</span></td>
      <td colspan="6" class="c-red">#{escape(error)}</td>
    </tr>
    """
  end

  defp render_flow_policy_row(row) do
    retry = Map.get(row, :retry, %{})
    retention = Map.get(row, :retention, %{})

    """
    <tr>
      <td class="mono">#{escape(row.type)}</td>
      <td><span class="badge #{flow_policy_source_class(row.source)}">#{escape(row.source)}</span></td>
      <td>#{format_number(flow_policy_field(retry, :max_retries, 0))}</td>
      <td>#{escape(flow_policy_backoff_summary(flow_policy_field(retry, :backoff, %{})))}</td>
      <td class="mono">#{escape(to_string(flow_policy_field(retry, :exhausted_to, "failed")))}</td>
      <td>#{escape(flow_policy_retention_summary(retention))}</td>
      <td>#{render_flow_policy_state_overrides(Map.get(row, :states, []))}</td>
      <td><a class="flow-search-button flow-policy-action" href="#{flow_policy_edit_url(row.type)}">Edit</a></td>
    </tr>
    """
  end

  @spec flow_policy_edit_url(binary()) :: binary()
  defp flow_policy_edit_url(type) do
    "/dashboard/flow/policies?" <> URI.encode_query(%{"edit" => type}) <> "#flow-policy-editor"
  end

  @spec flow_policy_source_class(binary()) :: binary()
  defp flow_policy_source_class("configured"), do: "badge-ok"
  defp flow_policy_source_class(_source), do: "badge-idle"

  @spec render_flow_policy_state_overrides([map()]) :: binary()
  defp render_flow_policy_state_overrides([]), do: ~s(<span class="c-muted">-</span>)

  defp render_flow_policy_state_overrides(states) do
    preview =
      states
      |> Enum.take(@flow_dashboard_policy_state_preview_limit)
      |> Enum.map_join("", fn state ->
        retry = Map.get(state, :retry, %{})
        retention = Map.get(state, :retention, %{})

        title =
          "max retries #{flow_policy_field(retry, :max_retries, 0)}, " <>
            flow_policy_retention_summary(retention)

        ~s(<span class="flow-pill" title="#{escape_attr(title)}">#{escape(state.state)}</span>)
      end)

    extra = length(states) - @flow_dashboard_policy_state_preview_limit

    if extra > 0 do
      preview <> ~s(<span class="flow-pill">+#{format_number(extra)}</span>)
    else
      preview
    end
  end

  @spec flow_policy_backoff_summary(map() | term()) :: binary()
  defp flow_policy_backoff_summary(backoff) when is_map(backoff) do
    kind = flow_policy_field(backoff, :kind, :none)
    base_ms = flow_policy_field(backoff, :base_ms, 0)
    max_ms = flow_policy_field(backoff, :max_ms, base_ms)
    jitter = flow_policy_field(backoff, :jitter_pct, 0)

    case kind do
      :none ->
        "none"

      "none" ->
        "none"

      _ ->
        "#{kind} #{format_duration_ms(base_ms)} (max #{format_duration_ms(max_ms)}, jitter #{jitter}%)"
    end
  end

  defp flow_policy_backoff_summary(_backoff), do: "-"

  @spec flow_policy_retention_summary(map()) :: binary()
  defp flow_policy_retention_summary(retention) when is_map(retention) do
    ttl_ms = flow_policy_field(retention, :ttl_ms, 0)
    max = flow_policy_field(retention, :history_max_events, 0)

    "#{format_duration_ms(ttl_ms)} retention, history max #{format_number(max)}"
  end

  defp flow_policy_retention_summary(_retention), do: "-"

  @spec flow_policy_field(map(), atom(), term()) :: term()
  defp flow_policy_field(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp flow_policy_field(_map, _key, default), do: default

  @spec flow_states_live_url(binary() | nil | map()) :: binary()
  defp flow_states_live_url(nil), do: "/dashboard/api/flow/states"

  defp flow_states_live_url(filters) when is_map(filters) do
    case flow_states_filter_query(filters) do
      "" -> "/dashboard/api/flow/states"
      query -> "/dashboard/api/flow/states?" <> query
    end
  end

  defp flow_states_live_url(type) when is_binary(type) do
    "/dashboard/api/flow/states?" <> URI.encode_query(%{"type" => type})
  end

  @spec flow_states_filter_query(map()) :: binary()
  defp flow_states_filter_query(filters) when is_map(filters) do
    range = Map.get(filters, :range)

    []
    |> maybe_put_query_param("type", Map.get(filters, :type))
    |> maybe_put_query_param("state", Map.get(filters, :state))
    |> maybe_put_query_param("q", Map.get(filters, :q))
    |> maybe_put_query_param("range", range)
    |> maybe_put_query_param("from_ms", if(range, do: nil, else: Map.get(filters, :from_ms)))
    |> maybe_put_query_param("to_ms", if(range, do: nil, else: Map.get(filters, :to_ms)))
    |> maybe_put_query_param("limit", flow_filter_limit_query_value(Map.get(filters, :limit)))
    |> Enum.reverse()
    |> URI.encode_query()
  end

  @spec flow_signals_live_url(map()) :: binary()
  defp flow_signals_live_url(filters) when is_map(filters) do
    case flow_signals_filter_query(filters) do
      "" -> "/dashboard/api/flow/signals"
      query -> "/dashboard/api/flow/signals?" <> query
    end
  end

  @spec flow_signals_filter_query(map()) :: binary()
  defp flow_signals_filter_query(filters) when is_map(filters) do
    []
    |> maybe_put_query_param("type", Map.get(filters, :type))
    |> maybe_put_query_param("signal", Map.get(filters, :signal))
    |> maybe_put_query_param("q", Map.get(filters, :q))
    |> maybe_put_query_param("scan", if(Map.get(filters, :scan_history), do: "true", else: nil))
    |> maybe_put_query_param("limit", flow_filter_limit_query_value(Map.get(filters, :limit)))
    |> Enum.reverse()
    |> URI.encode_query()
  end

  @spec maybe_put_query_param([{binary(), binary()}], binary(), term()) :: [{binary(), binary()}]
  defp maybe_put_query_param(params, _key, nil), do: params
  defp maybe_put_query_param(params, _key, ""), do: params

  defp maybe_put_query_param(params, key, value) when is_integer(value),
    do: [{key, Integer.to_string(value)} | params]

  defp maybe_put_query_param(params, key, value), do: [{key, to_string(value)} | params]

  @spec flow_filter_limit_query_value(term()) :: pos_integer() | nil
  defp flow_filter_limit_query_value(@flow_dashboard_recent_limit), do: nil
  defp flow_filter_limit_query_value(limit) when is_integer(limit), do: limit
  defp flow_filter_limit_query_value(_limit), do: nil

  @spec render_flow_issue_cards(map()) :: binary()
  defp render_flow_issue_cards(summary) do
    due_now = Map.get(summary, :due_now_sampled, 0)
    expired = Map.get(summary, :expired_leases_sampled, 0)
    failed = Map.get(summary, :failed, 0)

    if due_now == 0 and expired == 0 and failed == 0 do
      ""
    else
      render_flow_issue_cards(due_now, expired, failed)
    end
  end

  defp render_flow_issue_cards(due_now, expired, failed) do
    due_class = if due_now > 0, do: "badge-warning", else: "badge-ok"
    expired_class = if expired > 0, do: "badge-pressure", else: "badge-ok"
    failed_class = if failed > 0, do: "badge-pressure", else: "badge-ok"

    """
    <div class="section-title">Task Issues</div>
    <div class="flow-issue-row">
      <div class="flow-issue"><span class="badge #{due_class}">#{format_number(due_now)}</span><span>due now in sample</span></div>
      <div class="flow-issue"><span class="badge #{expired_class}">#{format_number(expired)}</span><span>expired leases in sample</span></div>
      <div class="flow-issue"><span class="badge #{failed_class}">#{format_number(failed)}</span><span>failed terminal flows</span></div>
    </div>
    """
  end

  @spec render_flow_states_chart([map()]) :: binary()
  defp render_flow_states_chart(states) do
    rows =
      states
      |> Enum.take(16)
      |> Enum.map(fn state ->
        %{
          label: "#{state.type}:#{state.state}",
          values: [
            {"Due", state.due_now, "bar-yellow"},
            {"Running", state.running, "bar-green"},
            {"Retry", Map.get(state, :retrying, 0), "bar-blue"},
            {"Failed", Map.get(state, :failed, 0), "bar-red"},
            {"Expired", state.expired_leases, "bar-red"}
          ]
        }
      end)

    """
    <div class="section-title">State Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">State pressure</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  @spec render_flow_workers_chart([map()]) :: binary()
  defp render_flow_workers_chart(workers) do
    rows =
      workers
      |> Enum.take(20)
      |> Enum.map(fn worker ->
        %{
          label: worker.worker,
          values: [
            {"Running", worker.running, "bar-green"},
            {"Expired", worker.expired, "bar-red"}
          ]
        }
      end)

    """
    <div class="section-title">Worker Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Lease health by worker</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  @spec render_flow_due_chart([map()], [map()]) :: binary()
  defp render_flow_due_chart(due_now, scheduled) do
    rows = [
      %{
        label: "Claim readiness",
        values: [
          {"Due now", length(due_now), "bar-yellow"},
          {"Scheduled", length(scheduled), "bar-blue"}
        ]
      }
    ]

    """
    <div class="section-title">Due Charts</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">Due vs scheduled</div>
        #{render_bar_chart(rows)}
      </div>
    </div>
    """
  end

  @spec render_flow_timeline_chart(list()) :: binary()
  defp render_flow_timeline_chart(history) do
    timeline =
      history
      |> flow_history_timeline_rows()
      |> Enum.take(@flow_dashboard_timeline_chart_max_events)
      |> Enum.reverse()

    """
    <div class="section-title">Timeline Chart</div>
    <div class="chart-grid">
      <div class="chart-card">
        <div class="chart-title">State graph</div>
        #{render_timeline_sequence(timeline)}
      </div>
    </div>
    """
  end

  defp render_bar_chart([]), do: ~s(<div class="chart-empty">No chart data</div>)

  defp render_bar_chart(rows) do
    max_value =
      rows
      |> Enum.flat_map(& &1.values)
      |> Enum.map(fn {_label, value, _class} -> numeric_metric_value(value) end)
      |> Enum.max(fn -> 0 end)
      |> max(1)

    row_html =
      Enum.map_join(rows, "\n", fn row ->
        bars =
          Enum.map_join(row.values, "\n", fn {label, value, class} ->
            value = numeric_metric_value(value)
            width = max(2, round(value / max_value * 100))

            """
            <div class="chart-bar-line">
              <span class="chart-bar-label">#{escape(label)}</span>
              <span class="chart-bar-track"><span class="chart-bar-fill #{class}" style="width: #{width}%"></span></span>
              <span class="chart-bar-value">#{format_number(value)}</span>
            </div>
            """
          end)

        """
        <div class="chart-row">
          <div class="chart-row-label">#{escape(row.label)}</div>
          <div class="chart-row-bars">#{bars}</div>
        </div>
        """
      end)

    ~s(<div class="chart-bars">#{row_html}</div>)
  end

  defp render_timeline_sequence([]), do: ~s(<div class="chart-empty">No timeline events</div>)

  defp render_timeline_sequence(timeline) do
    rows = flow_timeline_duration_rows(timeline)
    states = flow_timeline_states(rows)
    layout = flow_timeline_graph_layout(rows, states)
    points = flow_timeline_graph_points(rows, states, layout)

    lane_html = render_flow_timeline_lanes(states, layout)
    axis_html = render_flow_timeline_axis(layout)
    path_html = render_flow_timeline_path(points)
    duration_html = render_flow_timeline_duration_segments(points)
    transition_html = render_flow_timeline_transition_segments(points)
    node_html = render_flow_timeline_nodes(points)
    caption = "#{length(rows)} events on this page · click a node to jump to the event row"

    """
    <div class="flow-timeline-graph">
      <div class="flow-timeline-scroll">
        <svg class="flow-timeline-svg" viewBox="0 0 #{layout.width} #{layout.height}" width="#{layout.width}" height="#{layout.height}" role="img" aria-label="Flow state timeline graph">
          <rect class="flow-timeline-bg" x="0" y="0" width="#{layout.width}" height="#{layout.height}" rx="8"></rect>
          #{lane_html}
          #{axis_html}
          #{duration_html}
          #{transition_html}
          #{path_html}
          #{node_html}
        </svg>
      </div>
      <div class="flow-timeline-caption">#{escape(caption)}</div>
    </div>
    """
  end

  @spec flow_timeline_states([map()]) :: [binary()]
  defp flow_timeline_states(rows) do
    states =
      rows
      |> Enum.map(&flow_timeline_state_label/1)
      |> Enum.reject(&(&1 in ["", "-"]))
      |> Enum.uniq()

    case states do
      [] -> ["event"]
      _ -> states
    end
  end

  @spec flow_timeline_graph_layout([map()], [binary()]) :: map()
  defp flow_timeline_graph_layout(rows, states) do
    count = length(rows)
    lane_count = max(length(states), 1)
    left = 132
    right = 52
    top = 42
    bottom = 52
    lane_gap = 66
    step = flow_timeline_graph_step(count)
    plot_width = max(640, max(count - 1, 1) * step)

    times =
      rows
      |> Enum.map(& &1.time_ms)
      |> Enum.filter(&is_integer/1)

    min_time = Enum.min(times, fn -> nil end)
    max_time = Enum.max(times, fn -> nil end)

    %{
      width: left + plot_width + right,
      height: top + bottom + max(lane_count - 1, 0) * lane_gap,
      left: left,
      right: right,
      top: top,
      bottom: bottom,
      lane_gap: lane_gap,
      plot_width: plot_width,
      count: count,
      min_time: min_time,
      max_time: max_time
    }
  end

  defp flow_timeline_graph_step(count) when count > 60, do: 38
  defp flow_timeline_graph_step(count) when count > 40, do: 46
  defp flow_timeline_graph_step(count) when count > 20, do: 58
  defp flow_timeline_graph_step(_count), do: 88

  @spec flow_timeline_graph_points([map()], [binary()], map()) :: [map()]
  defp flow_timeline_graph_points(rows, states, layout) do
    state_index = states |> Enum.with_index() |> Map.new()
    count = length(rows)

    rows
    |> Enum.with_index()
    |> Enum.map(fn {row, index} ->
      state = flow_timeline_state_label(row)
      lane = Map.get(state_index, state, 0)

      row
      |> Map.put(:state, state)
      |> Map.put(:x, flow_timeline_x(row, index, count, layout))
      |> Map.put(:y, flow_timeline_y(lane, layout))
    end)
  end

  defp flow_timeline_x(row, index, count, %{min_time: min_time, max_time: max_time} = layout)
       when is_integer(min_time) and is_integer(max_time) and max_time > min_time do
    case row.time_ms do
      time when is_integer(time) ->
        layout.left + round((time - min_time) / max(max_time - min_time, 1) * layout.plot_width)

      _ ->
        flow_timeline_index_x(index, count, layout)
    end
  end

  defp flow_timeline_x(_row, index, count, layout),
    do: flow_timeline_index_x(index, count, layout)

  defp flow_timeline_index_x(index, count, layout) do
    layout.left + round(index / max(count - 1, 1) * layout.plot_width)
  end

  defp flow_timeline_y(lane, layout), do: layout.top + lane * layout.lane_gap

  defp render_flow_timeline_lanes(states, layout) do
    states
    |> Enum.with_index()
    |> Enum.map_join("\n", fn {state, index} ->
      y = flow_timeline_y(index, layout)
      x2 = layout.width - layout.right + 12

      """
      <g class="flow-timeline-lane">
        <line x1="#{layout.left}" y1="#{y}" x2="#{x2}" y2="#{y}"></line>
        <text class="flow-timeline-lane-label" x="#{layout.left - 14}" y="#{y + 4}" text-anchor="end">#{escape(state)}</text>
      </g>
      """
    end)
  end

  defp render_flow_timeline_axis(%{min_time: nil}), do: ""

  defp render_flow_timeline_axis(%{min_time: min_time, max_time: max_time} = layout) do
    baseline_y = layout.height - layout.bottom + 20
    max_time = max_time || min_time

    ticks =
      if max_time > min_time do
        [min_time, min_time + div(max_time - min_time, 2), max_time]
      else
        [min_time]
      end

    ticks
    |> Enum.uniq()
    |> Enum.map_join("\n", fn tick ->
      x =
        layout.left +
          if max_time > min_time do
            round((tick - min_time) / max(max_time - min_time, 1) * layout.plot_width)
          else
            0
          end

      """
      <g class="flow-timeline-axis">
        <line x1="#{x}" y1="#{layout.top - 16}" x2="#{x}" y2="#{baseline_y - 8}"></line>
        <text class="flow-timeline-axis-label" x="#{x}" y="#{baseline_y}" text-anchor="middle">#{escape(format_timeline_timestamp_ms(tick))}</text>
      </g>
      """
    end)
  end

  defp render_flow_timeline_path([]), do: ""

  defp render_flow_timeline_path(points) do
    d =
      points
      |> Enum.with_index()
      |> Enum.map_join(" ", fn {point, index} ->
        prefix = if index == 0, do: "M", else: "L"
        "#{prefix} #{point.x} #{point.y}"
      end)

    ~s(<path class="flow-timeline-path" d="#{d}"></path>)
  end

  defp render_flow_timeline_duration_segments(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map_join("\n", fn [point, next_point] ->
      class = flow_timeline_bar_class(point)
      duration = format_duration_ms(Map.get(point, :duration_ms, 0))
      title = flow_timeline_event_title(point)

      """
      <line class="flow-timeline-duration-segment #{class}" x1="#{point.x}" y1="#{point.y}" x2="#{next_point.x}" y2="#{point.y}">
        <title>#{escape(title <> " · held " <> duration)}</title>
      </line>
      """
    end)
  end

  defp render_flow_timeline_transition_segments(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.map_join("\n", fn [point, next_point] ->
      mid_x = round((point.x + next_point.x) / 2)

      d =
        "M #{point.x} #{point.y} C #{mid_x} #{point.y} #{mid_x} #{next_point.y} #{next_point.x} #{next_point.y}"

      ~s(<path class="flow-timeline-transition" d="#{d}"></path>)
    end)
  end

  defp render_flow_timeline_nodes(points) do
    dense? = length(points) > 28

    points
    |> Enum.map_join("\n", fn point ->
      anchor = flow_history_event_anchor(point.event_id)
      title = flow_timeline_event_title(point)
      label = flow_timeline_node_label_text(point)
      node_class = flow_timeline_node_class(point)
      label_html = if dense?, do: "", else: render_flow_timeline_node_label(point, label)

      """
      <a href="##{anchor}" class="flow-timeline-node-link">
        <circle class="flow-timeline-node #{node_class}" cx="#{point.x}" cy="#{point.y}" r="7">
          <title>#{escape(title)}</title>
        </circle>
      </a>
      #{label_html}
      """
    end)
  end

  defp render_flow_timeline_node_label(point, label) do
    y = point.y - 13

    ~s(<text class="flow-timeline-node-label" x="#{point.x + 10}" y="#{y}">#{escape(label)}</text>)
  end

  defp flow_timeline_node_label_text(row) do
    label = flow_history_event_label(row.fields)

    case label do
      "Retry" -> "Retry"
      "Failed" -> "Failed"
      "Completed" -> "Completed"
      "Cancelled" -> "Cancelled"
      _ -> flow_timeline_state_label(row)
    end
  end

  defp flow_timeline_node_class(row) do
    fields = row.fields

    cond do
      flow_history_terminal_event?(fields) -> "flow-timeline-node-terminal"
      flow_history_event_label(fields) == "Retry" -> "flow-timeline-node-retry"
      flow_history_event_label(fields) == "Failed" -> "flow-timeline-node-failed"
      true -> "flow-timeline-node-normal"
    end
  end

  @spec flow_timeline_duration_rows([map()]) :: [map()]
  defp flow_timeline_duration_rows(timeline) do
    timeline
    |> Enum.with_index()
    |> Enum.map(fn {row, index} ->
      next_row = Enum.at(timeline, index + 1)
      Map.put(row, :duration_ms, flow_timeline_duration_ms(row, next_row))
    end)
  end

  defp flow_timeline_duration_ms(%{time_ms: start_ms}, %{time_ms: end_ms})
       when is_integer(start_ms) and is_integer(end_ms) and end_ms >= start_ms do
    end_ms - start_ms
  end

  defp flow_timeline_duration_ms(_row, _next_row), do: 0

  @spec flow_timeline_bar_class(map()) :: binary()
  defp flow_timeline_bar_class(row) do
    fields = row.fields

    cond do
      flow_history_terminal_event?(fields) -> "bar-green"
      flow_history_event_label(fields) == "Retry" -> "bar-red"
      flow_history_event_label(fields) == "Failed" -> "bar-red"
      true -> "bar-blue"
    end
  end

  @spec flow_timeline_state_label(map()) :: binary()
  defp flow_timeline_state_label(row) do
    case row.to_state do
      state when is_binary(state) and state != "" -> state
      _ -> flow_timeline_previous_state_label(row)
    end
  end

  defp flow_timeline_previous_state_label(row) do
    case Map.get(row, :from_state) do
      state when is_binary(state) and state != "" -> state
      _ -> flow_history_event_label(row.fields)
    end
  end

  @spec flow_timeline_event_title(map()) :: binary()
  defp flow_timeline_event_title(row) do
    [
      to_string(row.event_id),
      format_timestamp_ms_or_dash(row.time_ms),
      flow_history_event_label(row.fields),
      flow_history_state_move(row),
      "duration #{format_duration_ms(Map.get(row, :duration_ms, 0))}"
    ]
    |> Enum.reject(&(&1 in ["", "-"]))
    |> Enum.join(" · ")
  end

  @spec info_icon(binary()) :: binary()
  defp info_icon(text) do
    attr = escape_attr(text)

    ~s(<span class="info-icon" tabindex="0" role="img" aria-label="#{attr}" data-tooltip="#{attr}" title="#{attr}">i</span>)
  end

  defp numeric_metric_value(value) when is_integer(value), do: value
  defp numeric_metric_value(value) when is_float(value), do: round(value)

  defp numeric_metric_value(value) when is_binary(value) do
    case Float.parse(value) do
      {parsed, _rest} -> round(parsed)
      :error -> 0
    end
  end

  defp numeric_metric_value(_value), do: 0

  @spec render_flow_type_filter(map()) :: binary()
  defp render_flow_type_filter(data) do
    filters =
      Map.get(data, :filters, %{
        type: Map.get(data, :type_filter),
        state: nil,
        q: nil,
        range: nil,
        from_ms: nil,
        to_ms: nil,
        limit: @flow_dashboard_recent_limit
      })

    type_filter = Map.get(filters, :type)
    state_filter = Map.get(filters, :state)
    name_filter = Map.get(filters, :q)
    range_filter = Map.get(filters, :range)
    available_types = Map.get(data, :available_types, [])
    available_states = Map.get(data, :available_states, [])

    type_options =
      [nil | available_types]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(type_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All types</option>)

        type ->
          selected = if type == type_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    state_options =
      [nil | available_states]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(state_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All states</option>)

        state ->
          selected = if state == state_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(state)}"#{selected}>#{escape(state)}</option>)
      end)

    range_options = render_flow_range_options(range_filter)

    custom_from_value =
      if range_filter, do: "", else: flow_filter_time_value(Map.get(filters, :from_ms))

    custom_to_value =
      if range_filter, do: "", else: flow_filter_time_value(Map.get(filters, :to_ms))

    clear =
      if flow_filter_active?(filters) do
        ~s(<a class="flow-filter-clear" href="/dashboard/flow/states" title="Clear Flow state filters">Clear</a>)
      else
        ""
      end

    filtered_sampled = Map.get(data, :filtered_sampled, Map.get(data, :total_sampled, 0))
    total_sampled = Map.get(data, :total_sampled, filtered_sampled)
    limit = Map.get(filters, :limit, @flow_dashboard_recent_limit)

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/flow/states" method="get">
        <label for="flow-state-type-filter">Type</label>
        <select id="flow-state-type-filter" class="flow-search-input" name="type" title="Filter by workflow type">
          #{type_options}
        </select>
        <label for="flow-state-state-filter">State</label>
        <select id="flow-state-state-filter" class="flow-search-input" name="state" title="Filter by current workflow state">
          #{state_options}
        </select>
        <label for="flow-state-name-filter">ID</label>
        <input id="flow-state-name-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        <label for="flow-state-range-filter">Updated</label>
        <select id="flow-state-range-filter" class="flow-search-input flow-filter-range" name="range" title="Use a quick sliding window or Custom for From/To">
          #{range_options}
        </select>
        <label for="flow-state-from-filter">From UTC</label>
        <input id="flow-state-from-filter" class="flow-search-input mono flow-filter-time" type="datetime-local" name="from" step="60" value="#{escape_attr(custom_from_value)}" title="Custom UTC start time, used when Updated is All time">
        <label for="flow-state-to-filter">To UTC</label>
        <input id="flow-state-to-filter" class="flow-search-input mono flow-filter-time" type="datetime-local" name="to" step="60" value="#{escape_attr(custom_to_value)}" title="Custom UTC end time, used when Updated is All time">
        <label for="flow-state-limit-filter">Recent Limit</label>
        <input id="flow-state-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum recent records shown below">
        <button class="flow-search-button" type="submit" title="Apply Flow state filters">Apply</button>
        #{clear}
      </form>
      <div class="flow-filter-note">
        Showing #{escape(flow_filter_summary(filters))} · #{format_number(filtered_sampled)} / #{format_number(total_sampled)} sampled records
        #{info_icon("Updated quick ranges are sliding windows and override custom From/To. Custom times are interpreted as UTC. Limit applies to Recent Flow Records only.")}
      </div>
    </div>
    """
  end

  @spec render_flow_signals_filter(map()) :: binary()
  defp render_flow_signals_filter(data) do
    filters = flow_signals_page_filters(data)
    type_filter = Map.get(filters, :type)
    signal_filter = Map.get(filters, :signal)
    name_filter = Map.get(filters, :q)
    available_types = Map.get(data, :available_types, [])

    type_options =
      [nil | available_types]
      |> Enum.map_join("\n", fn
        nil ->
          selected = if is_nil(type_filter), do: " selected", else: ""
          ~s(<option value=""#{selected}>All types</option>)

        type ->
          selected = if type == type_filter, do: " selected", else: ""
          ~s(<option value="#{escape_attr(type)}"#{selected}>#{escape(type)}</option>)
      end)

    clear =
      if flow_signal_filter_active?(filters) do
        ~s(<a class="flow-filter-clear" href="/dashboard/flow/signals" title="Clear Flow signal filters">Clear</a>)
      else
        ""
      end

    filtered_sampled = Map.get(data, :filtered_sampled, Map.get(data, :total_sampled, 0))
    total_sampled = Map.get(data, :total_sampled, filtered_sampled)
    limit = Map.get(filters, :limit, @flow_dashboard_recent_limit)
    scan_checked = if Map.get(filters, :scan_history, false), do: " checked", else: ""

    """
    <div class="flow-filter-panel">
      <form class="flow-filter-form" action="/dashboard/flow/signals" method="get">
        <label for="flow-signal-type-filter">Type</label>
        <select id="flow-signal-type-filter" class="flow-search-input" name="type" title="Filter signals by workflow type">
          #{type_options}
        </select>
        <label for="flow-signal-name-filter">Signal</label>
        <input id="flow-signal-name-filter" class="flow-search-input mono" type="search" name="signal" value="#{escape_attr(signal_filter || "")}" placeholder="contains" title="Filter by signal name substring">
        <label for="flow-signal-id-filter">Flow ID</label>
        <input id="flow-signal-id-filter" class="flow-search-input mono" type="search" name="q" value="#{escape_attr(name_filter || "")}" placeholder="contains" title="Filter by Flow ID substring">
        <label for="flow-signal-limit-filter">Limit</label>
        <input id="flow-signal-limit-filter" class="flow-search-input mono flow-filter-limit" type="number" name="limit" min="1" max="#{@flow_dashboard_max_recent_limit}" value="#{limit}" title="Maximum signal rows shown below">
        <label class="flow-check-label" title="Read recent Flow histories for the sampled flows. This is intentionally opt-in because it can be expensive under load.">
          <input type="checkbox" name="scan" value="true"#{scan_checked}> Scan histories
        </label>
        <button class="flow-search-button" type="submit" title="Apply Flow signal filters">Apply</button>
        #{clear}
      </form>
      <div class="flow-filter-note">
        Showing #{escape(flow_signals_filter_summary(filters))} · #{format_number(filtered_sampled)} / #{format_number(total_sampled)} sampled records
        #{info_icon("Default view avoids history scans so the dashboard stays cheap during soak. Enable Scan histories to inspect recent sampled history, or use Flow detail for full paginated history.")}
      </div>
    </div>
    """
  end

  @spec render_flow_range_options(binary() | nil) :: binary()
  defp render_flow_range_options(selected_range) do
    Enum.map_join(@flow_dashboard_time_range_options, "\n", fn {range, label} ->
      value = range || ""
      selected = if range == selected_range, do: " selected", else: ""
      ~s(<option value="#{escape_attr(value)}"#{selected}>#{escape(label)}</option>)
    end)
  end

  @spec flow_filter_active?(map()) :: boolean()
  defp flow_filter_active?(filters) do
    Enum.any?([:type, :state, :q, :range, :from_ms, :to_ms], fn key ->
      case Map.get(filters, key) do
        nil -> false
        "" -> false
        _ -> true
      end
    end) or Map.get(filters, :limit, @flow_dashboard_recent_limit) != @flow_dashboard_recent_limit
  end

  @spec flow_filter_time_value(integer() | nil) :: binary()
  defp flow_filter_time_value(nil), do: ""

  defp flow_filter_time_value(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} ->
        datetime
        |> DateTime.to_iso8601()
        |> binary_part(0, 16)

      _ ->
        Integer.to_string(value)
    end
  end

  @spec flow_filter_summary(map()) :: binary()
  defp flow_filter_summary(filters) do
    [
      Map.get(filters, :type) || "all types",
      Map.get(filters, :state) || "all states",
      flow_filter_name_label(Map.get(filters, :q)),
      flow_filter_time_label(filters),
      flow_filter_limit_label(Map.get(filters, :limit))
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(" / ")
  end

  @spec flow_filter_name_label(binary() | nil) :: binary()
  defp flow_filter_name_label(nil), do: ""
  defp flow_filter_name_label(query), do: "id contains #{query}"

  @spec flow_signal_filter_active?(map()) :: boolean()
  defp flow_signal_filter_active?(filters) do
    Enum.any?([:type, :signal, :q], fn key ->
      case Map.get(filters, key) do
        nil -> false
        "" -> false
        _ -> true
      end
    end) or Map.get(filters, :limit, @flow_dashboard_recent_limit) != @flow_dashboard_recent_limit or
      Map.get(filters, :scan_history, false)
  end

  @spec flow_signals_filter_summary(map()) :: binary()
  defp flow_signals_filter_summary(filters) do
    [
      Map.get(filters, :type) || "all types",
      flow_signal_name_label(Map.get(filters, :signal)),
      flow_filter_name_label(Map.get(filters, :q)),
      flow_filter_limit_label(Map.get(filters, :limit)),
      flow_signal_scan_label(Map.get(filters, :scan_history, false))
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join(" / ")
  end

  @spec flow_signal_name_label(binary() | nil) :: binary()
  defp flow_signal_name_label(nil), do: ""
  defp flow_signal_name_label(signal), do: "signal contains #{signal}"

  @spec flow_signal_scan_label(boolean()) :: binary()
  defp flow_signal_scan_label(true), do: "history scan enabled"
  defp flow_signal_scan_label(_), do: "history scan off"

  @spec flow_filter_time_range_label(integer() | nil, integer() | nil) :: binary()
  defp flow_filter_time_range_label(nil, nil), do: ""

  defp flow_filter_time_range_label(from_ms, nil),
    do: "updated from #{flow_filter_time_display(from_ms)}"

  defp flow_filter_time_range_label(nil, to_ms),
    do: "updated to #{flow_filter_time_display(to_ms)}"

  defp flow_filter_time_range_label(from_ms, to_ms),
    do: "updated #{flow_filter_time_display(from_ms)}..#{flow_filter_time_display(to_ms)}"

  @spec flow_filter_time_label(map()) :: binary()
  defp flow_filter_time_label(%{range: range}) when is_binary(range) do
    case flow_time_range_label(range) do
      "" -> ""
      label -> "updated #{label}"
    end
  end

  defp flow_filter_time_label(filters) when is_map(filters) do
    flow_filter_time_range_label(Map.get(filters, :from_ms), Map.get(filters, :to_ms))
  end

  @spec flow_time_range_label(binary()) :: binary()
  defp flow_time_range_label("5m"), do: "last 5 minutes"
  defp flow_time_range_label("15m"), do: "last 15 minutes"
  defp flow_time_range_label("1h"), do: "last 1 hour"
  defp flow_time_range_label("6h"), do: "last 6 hours"
  defp flow_time_range_label("24h"), do: "last 24 hours"
  defp flow_time_range_label(_range), do: ""

  @spec flow_filter_time_display(integer()) :: binary()
  defp flow_filter_time_display(value) when is_integer(value), do: flow_filter_time_value(value)

  @spec flow_filter_limit_label(term()) :: binary()
  defp flow_filter_limit_label(@flow_dashboard_recent_limit), do: ""
  defp flow_filter_limit_label(limit) when is_integer(limit), do: "recent limit #{limit}"
  defp flow_filter_limit_label(_limit), do: ""

  @spec render_flow_states_table(
          [map()],
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          map()
        ) :: binary()
  defp render_flow_states_table(
         states,
         total_sampled,
         filtered_sampled,
         sample_limit,
         filters
       ) do
    rows =
      case states do
        [] ->
          ~s(<tr><td colspan="11" class="c-muted">No Flow states discovered for this type filter</td></tr>)

        _ ->
          Enum.map_join(states, "\n", fn state ->
            due_class = if state.due_now > 0, do: "c-yellow", else: ""
            expired_class = if state.expired_leases > 0, do: "c-red", else: ""
            retry_class = if Map.get(state, :retrying, 0) > 0, do: "c-yellow", else: ""
            failed_class = if Map.get(state, :failed, 0) > 0, do: "c-red", else: ""
            maxed_class = if Map.get(state, :max_attempts_reached, 0) > 0, do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{escape(state.type)}</td>
              <td class="#{flow_state_class(state.state)}">#{escape(state.state)}</td>
              <td>#{format_number(state.count)}</td>
              <td class="#{due_class}">#{format_number(state.due_now)}</td>
              <td>#{format_number(state.running)}</td>
              <td class="#{retry_class}">#{format_number(Map.get(state, :retrying, 0))}</td>
              <td class="#{failed_class}">#{format_number(Map.get(state, :failed, 0))}</td>
              <td class="#{expired_class}">#{format_number(state.expired_leases)}</td>
              <td class="#{maxed_class}">#{format_number(Map.get(state, :max_attempts_reached, 0))}</td>
              <td>#{format_duration_ms(state.oldest_due_ms)}</td>
              <td>#{flow_state_operational_hint(state)}</td>
            </tr>
            """
          end)
      end

    filter_label = flow_filter_summary(filters)

    """
    <div class="section-title">Flow States <span class="badge badge-idle">#{escape(filter_label)}</span> <span class="badge badge-idle">sampled #{format_number(filtered_sampled)} / #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr>
          <th>Type</th>
          <th>State</th>
          <th>Sample Count</th>
          <th>Due Now #{info_icon("Non-terminal flows with run_at/next_run_at at or before now. Workers should be able to claim them.")}</th>
          <th>Running #{info_icon("Flows currently leased to workers through FLOW.CLAIM_DUE.")}</th>
          <th>Retrying #{info_icon("Non-terminal flows with attempts > 0. They were retried and may be waiting for their next run time.")}</th>
          <th>Failed #{info_icon("Terminal failed flows. They are not claimable unless user logic rewinds or creates new work.")}</th>
          <th>Expired #{info_icon("Running flows whose lease deadline passed. This is reclaimable work, not a terminal failure.")}</th>
          <th>Maxed #{info_icon("Flows whose attempts reached max_attempts/max_retries in the sampled records.")}</th>
          <th>Oldest Due</th>
          <th>Hint</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec flow_state_operational_hint(map()) :: binary()
  defp flow_state_operational_hint(state) do
    cond do
      state.expired_leases > 0 ->
        ~s(<span class="c-red">leases need reclaim</span>)

      Map.get(state, :failed, 0) > 0 ->
        ~s(<span class="c-red">terminal failed</span>)

      Map.get(state, :max_attempts_reached, 0) > 0 ->
        ~s(<span class="c-red">retry attempts maxed</span>)

      state.due_now > 0 and state.running == 0 ->
        ~s(<span class="c-yellow">due work, no running sample</span>)

      state.due_now > 0 ->
        ~s(<span class="c-yellow">workers should drain</span>)

      Map.get(state, :retrying, 0) > 0 ->
        ~s(<span class="c-yellow">retry backoff/attempts</span>)

      state.state in @flow_terminal_states ->
        ~s(<span class="c-muted">terminal</span>)

      true ->
        ~s(<span class="c-muted">healthy</span>)
    end
  end

  @spec render_flow_state_breakdown([map()]) :: binary()
  defp render_flow_state_breakdown(types) do
    rows =
      case types do
        [] ->
          ~s(<tr><td colspan="10" class="c-muted">No Flow state records discovered</td></tr>)

        _ ->
          Enum.map_join(types, "\n", fn type ->
            exact_badge =
              if Map.get(type, :exact, false) do
                ~s(<span class="badge badge-ok">exact</span>)
              else
                ~s(<span class="badge badge-idle">sample</span>)
              end

            """
            <tr>
              <td class="mono">#{escape(type.type)}</td>
              <td>#{exact_badge}</td>
              <td>#{format_number(type.total)}</td>
              <td>#{format_number(type.active)}</td>
              <td>#{format_number(type.queued)}</td>
              <td>#{format_number(type.running)}</td>
              <td>#{format_number(type.completed)}</td>
              <td class="#{if type.failed > 0, do: "c-red", else: ""}">#{format_number(type.failed)}</td>
              <td>#{format_number(type.cancelled)}</td>
              <td>#{render_flow_custom_states(type.states)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">State Breakdown</div>
    <table>
      <thead>
        <tr><th>Type</th><th>Count Source</th><th>Total</th><th>Active</th><th>Queued</th><th>Running</th><th>Completed</th><th>Failed</th><th>Cancelled</th><th>Observed States</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_custom_states(map()) :: binary()
  defp render_flow_custom_states(states) when is_map(states) do
    states
    |> Enum.sort_by(fn {state, _count} -> state end)
    |> Enum.map_join(" ", fn {state, count} ->
      ~s(<span class="flow-pill">#{escape(state)} #{format_number(count)}</span>)
    end)
  end

  defp render_flow_custom_states(_states), do: ""

  @spec render_flow_workers([map()]) :: binary()
  defp render_flow_workers(workers) do
    rows =
      case workers do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No running Flow leases discovered in sample</td></tr>)

        _ ->
          Enum.map_join(workers, "\n", fn worker ->
            expired_class = if worker.expired > 0, do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{escape(worker.worker)}</td>
              <td>#{format_number(worker.running)}</td>
              <td class="#{expired_class}">#{format_number(worker.expired)}</td>
              <td>#{format_duration_ms(worker.oldest_lease_ms)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Workers / Leases</div>
    <table>
      <thead>
        <tr><th>Worker</th><th>Running</th><th>Expired</th><th>Oldest Expired By</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_running_records([map()], non_neg_integer(), pos_integer()) :: binary()
  defp render_flow_running_records(records, total_sampled, sample_limit) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No running Flow records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            expired_class = if flow_expired_lease?(record), do: "c-red", else: ""

            """
            <tr>
              <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
              <td class="#{expired_class}">#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))}</td>
              <td>#{escape(to_string(flow_field(record, :lease_token, "-")))}</td>
              <td>#{escape(to_string(flow_field(record, :fencing_token, "-")))}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Running Records <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>Worker</th><th>Status</th><th>Lease Expires</th><th>Lease Token</th><th>Fencing</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_signals_table(
          [map()],
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          pos_integer() | nil,
          map()
        ) :: binary()
  defp render_flow_signals_table(signals, total_sampled, filtered_sampled, sample_limit, filters) do
    render_flow_signals_table(
      signals,
      total_sampled,
      filtered_sampled,
      sample_limit,
      filters,
      :page
    )
  end

  @spec render_flow_signals_table(
          [map()],
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          pos_integer() | nil,
          map(),
          :page | :detail
        ) :: binary()
  defp render_flow_signals_table(
         signals,
         total_sampled,
         filtered_sampled,
         sample_limit,
         filters,
         mode
       ) do
    rows =
      case signals do
        [] ->
          colspan = if mode == :detail, do: 6, else: 8

          message =
            if mode == :page and not Map.get(filters, :scan_history, false) do
              "Signal history scan is off. Enable Scan histories to search sampled recent Flow history."
            else
              "No signal events found in loaded history"
            end

          ~s(<tr><td colspan="#{colspan}" class="c-muted">#{escape(message)}</td></tr>)

        _ ->
          Enum.map_join(signals, "\n", &render_flow_signal_row(&1, mode))
      end

    title = flow_signals_table_title(total_sampled, filtered_sampled, sample_limit, filters, mode)

    """
    <div class="section-title">#{title}</div>
    <table>
      <thead>
        #{render_flow_signals_table_head(mode)}
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_signal_row(map(), :page | :detail) :: binary()
  defp render_flow_signal_row(row, :detail) do
    """
    <tr>
      <td class="mono"><a class="flow-event-link" href="#{flow_signal_event_href(row, :detail)}">#{escape(Map.get(row, :event_id, "-"))}</a></td>
      <td>#{format_timestamp_ms_or_dash(Map.get(row, :time_ms))}</td>
      <td class="mono">#{escape(Map.get(row, :signal, "-"))}</td>
      <td>#{flow_signal_state_move_html(row)}</td>
      <td class="mono">#{flow_signal_refs_summary_html(row, :detail)}</td>
      <td><a class="flow-link" href="#{flow_signal_event_href(row, :detail)}">event</a></td>
    </tr>
    """
  end

  defp render_flow_signal_row(row, :page) do
    id = Map.get(row, :id, "")
    partition_key = Map.get(row, :partition_key)

    """
    <tr>
      <td class="mono">#{render_flow_id_link(id, partition_key)}</td>
      <td class="mono">#{escape(Map.get(row, :type, "-"))}</td>
      <td class="mono"><a class="flow-event-link" href="#{flow_signal_event_href(row, :page)}">#{escape(Map.get(row, :event_id, "-"))}</a></td>
      <td>#{format_timestamp_ms_or_dash(Map.get(row, :time_ms))}</td>
      <td class="mono">#{escape(Map.get(row, :signal, "-"))}</td>
      <td>#{flow_signal_state_move_html(row)}</td>
      <td class="mono">#{flow_signal_refs_summary_html(row, :page)}</td>
      <td><a class="flow-link" href="#{flow_signal_event_href(row, :page)}">detail</a></td>
    </tr>
    """
  end

  @spec render_flow_signals_table_head(:page | :detail) :: binary()
  defp render_flow_signals_table_head(:detail) do
    """
    <tr><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Jump</th></tr>
    """
  end

  defp render_flow_signals_table_head(:page) do
    """
    <tr><th>Flow</th><th>Type</th><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Open</th></tr>
    """
  end

  @spec flow_signals_table_title(
          non_neg_integer() | nil,
          non_neg_integer() | nil,
          pos_integer() | nil,
          map(),
          :page | :detail
        ) :: binary()
  defp flow_signals_table_title(nil, _filtered_sampled, _sample_limit, _filters, :detail),
    do: "Signals"

  defp flow_signals_table_title(total_sampled, filtered_sampled, sample_limit, filters, :page)
       when is_integer(total_sampled) and is_integer(filtered_sampled) and
              is_integer(sample_limit) do
    "Flow Signals <span class=\"badge badge-idle\">#{escape(flow_signals_filter_summary(filters))}</span> <span class=\"badge badge-idle\">sampled #{format_number(filtered_sampled)} / #{format_number(total_sampled)} / #{format_number(sample_limit)}</span>"
  end

  defp flow_signals_table_title(
         _total_sampled,
         _filtered_sampled,
         _sample_limit,
         _filters,
         _mode
       ),
       do: "Signals"

  @spec flow_signal_state_move_html(map()) :: binary()
  defp flow_signal_state_move_html(row) do
    from_state = Map.get(row, :from_state, "")
    to_state = Map.get(row, :to_state, "")

    cond do
      is_binary(from_state) and from_state != "" and is_binary(to_state) and to_state != "" and
          from_state != to_state ->
        escape(from_state) <> " -> " <> escape(to_state)

      is_binary(to_state) and to_state != "" ->
        escape(to_state)

      true ->
        "-"
    end
  end

  @spec flow_signal_event_href(map(), :page | :detail) :: binary()
  defp flow_signal_event_href(row, :detail) do
    "#" <> flow_history_event_anchor(Map.get(row, :event_id, "-"))
  end

  defp flow_signal_event_href(row, :page) do
    anchor = flow_history_event_anchor(Map.get(row, :event_id, "-"))
    id = Map.get(row, :id, "")

    case id do
      "" -> "#" <> anchor
      id -> flow_detail_path(id, Map.get(row, :partition_key)) <> "#" <> anchor
    end
  end

  @spec flow_signal_refs_summary_html(map(), :page | :detail) :: binary()
  defp flow_signal_refs_summary_html(row, mode) do
    record = Map.get(row, :record)
    badge_mode = if mode == :page, do: :detail_link, else: :local

    badges =
      row
      |> Map.get(:fields, %{})
      |> flow_value_ref_entries("signal event")
      |> Enum.map(&render_flow_value_ref_badge(record, badge_mode, &1))

    case badges do
      [] -> "-"
      _ -> Enum.join(badges, " ")
    end
  end

  @spec render_flow_due_records(binary(), [map()], non_neg_integer(), pos_integer()) :: binary()
  defp render_flow_due_records(title, records, total_sampled, sample_limit) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No #{escape(String.downcase(title))} records discovered in sample</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            """
            <tr>
              <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="#{flow_state_class(flow_record_state(record))}">#{escape(flow_record_state(record))}</td>
              <td>#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}</td>
              <td>#{escape(to_string(flow_field(record, :priority, 0)))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">#{escape(title)} <span class="badge badge-idle">sampled #{format_number(total_sampled)} / #{format_number(sample_limit)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>State</th><th>Why Waiting</th><th>Run At</th><th>Priority</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_failures_rows([map()]) :: binary()
  defp render_flow_failures_rows([]) do
    ~s(<tr><td colspan="9" class="c-muted">No failed, exhausted, or expired-lease records found in the current bounded view.</td></tr>)
  end

  defp render_flow_failures_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
        <td class="mono">#{escape(flow_record_type(record))}</td>
        <td class="#{flow_state_class(state)}">#{escape(state)}</td>
        <td>#{escape(flow_recovery_reason(record))}</td>
        <td>#{format_number(flow_record_attempts(record))}</td>
        <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
      </tr>
      """
    end)
  end

  @spec render_flow_lineage_hints([map()]) :: binary()
  defp render_flow_lineage_hints([]), do: ""

  defp render_flow_lineage_hints(hints) do
    links =
      Enum.map_join(hints, " ", fn hint ->
        params = %{"mode" => hint.mode, "id" => hint.id}
        href = "/dashboard/flow/lineage?" <> URI.encode_query(params)

        ~s(<a class="flow-pill flow-link" href="#{href}">#{escape(hint.label)} #{escape(hint.id)}</a>)
      end)

    ~s(<div class="flow-section-note">Recent lineage hints: #{links}</div>)
  end

  @spec flow_lineage_result_label(map()) :: binary()
  defp flow_lineage_result_label(%{status: :idle, message: message}), do: message
  defp flow_lineage_result_label(%{status: :ok, command: command}), do: "#{command} result"
  defp flow_lineage_result_label(%{status: status, message: message}), do: "#{status}: #{message}"
  defp flow_lineage_result_label(_result), do: "lineage result"

  @spec render_flow_lineage_nodes([map()], map()) :: binary()
  defp render_flow_lineage_nodes([], filters) do
    target = Map.get(filters, :target)

    if is_binary(target) and target != "" do
      ~s(<div class="flow-lineage-empty">No lineage records matched this query.</div>)
    else
      ~s(<div class="flow-lineage-empty">Choose parent, root, or correlation and enter an id.</div>)
    end
  end

  defp render_flow_lineage_nodes(records, _filters) do
    records
    |> Enum.take(40)
    |> Enum.map_join("\n", fn record ->
      state = flow_record_state(record)

      """
      <a class="flow-lineage-node #{flow_state_class(state)}" href="#{flow_detail_path(flow_record_id(record), flow_record_partition_key(record))}">
        <span class="flow-lineage-node-id">#{escape(flow_record_id(record))}</span>
        <span class="flow-lineage-node-meta">#{escape(flow_record_type(record))} / #{escape(state)}</span>
      </a>
      """
    end)
  end

  @spec render_flow_lineage_rows([map()]) :: binary()
  defp render_flow_lineage_rows([]) do
    ~s(<tr><td colspan="8" class="c-muted">No lineage records loaded.</td></tr>)
  end

  defp render_flow_lineage_rows(records) do
    Enum.map_join(records, "\n", fn record ->
      state = flow_record_state(record)

      """
      <tr>
        <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
        <td class="mono">#{escape(flow_record_type(record))}</td>
        <td class="#{flow_state_class(state)}">#{escape(state)}</td>
        <td class="mono">#{escape(flow_record_parent_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_root_id(record) || "-")}</td>
        <td class="mono">#{escape(flow_record_correlation_id(record) || "-")}</td>
        <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
        <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
      </tr>
      """
    end)
  end

  @spec flow_query_result_command(map()) :: binary()
  defp flow_query_result_command(%{command: command}) when is_binary(command), do: command
  defp flow_query_result_command(_result), do: "FLOW.QUERY"

  @spec render_flow_query_status(map()) :: binary()
  defp render_flow_query_status(%{status: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  defp render_flow_query_status(%{status: :idle, message: message}),
    do: ~s(<div class="flow-section-note">#{escape(message)}</div>)

  defp render_flow_query_status(%{status: _status, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  defp render_flow_query_status(_result), do: ""

  @spec render_flow_query_rows(map()) :: binary()
  defp render_flow_query_rows(%{rows: []}) do
    ~s(<tr><td colspan="6" class="c-muted">No rows.</td></tr>)
  end

  defp render_flow_query_rows(%{command: "FLOW.HISTORY", rows: rows}) do
    Enum.map_join(rows, "\n", fn entry ->
      {event_id, fields} = normalize_flow_history_entry(entry)

      """
      <tr>
        <td class="mono">#{escape(to_string(event_id))}</td>
        <td class="mono">history</td>
        <td>#{flow_history_action_html(fields)}</td>
        <td>#{format_timestamp_ms_or_dash(flow_history_event_time_ms(event_id, fields))}</td>
        <td class="mono">#{escape(flow_history_worker_summary(fields))}</td>
        <td>#{flow_history_refs_summary_html(fields)}</td>
      </tr>
      """
    end)
  end

  defp render_flow_query_rows(%{rows: rows}) do
    Enum.map_join(rows, "\n", fn
      record when is_map(record) ->
        state = flow_record_state(record)

        """
        <tr>
          <td class="mono">#{render_flow_id_link(flow_record_id(record), flow_record_partition_key(record))}</td>
          <td class="mono">#{escape(flow_record_type(record))}</td>
          <td class="#{flow_state_class(state)}">#{escape(state)}</td>
          <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
          <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
          <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
        </tr>
        """

      other ->
        """
        <tr>
          <td class="mono">#{escape(inspect(other, limit: 5))}</td>
          <td colspan="5" class="c-muted">non-record result</td>
        </tr>
        """
    end)
  end

  @spec render_flow_projection_health(map()) :: binary()
  defp render_flow_projection_health(data) do
    data = Map.merge(default_flow_projection_health(), data)
    rollup = flow_projection_rollup(Map.get(data, :metrics, []))
    health_class = flow_projection_health_class(rollup)

    """
    <div class="section-title">Projection Health</div>
    <div class="flow-card-grid">
      <div class="flow-card">
        <div class="flow-card-label">LMDB</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{escape(to_string(data.lmdb_projection))}</div>
        <div class="flow-card-detail">cold/query projection runs after durable Flow writes</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Health</div>
        <div class="flow-card-value #{health_class}" style="font-size:1.2rem;">#{escape(rollup.health)}</div>
        <div class="flow-card-detail">#{format_number(rollup.shards)} shard projection row(s)</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Lag</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_number(rollup.lag)}</div>
        <div class="flow-card-detail">requested index minus durable projected index</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Pending</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_number(rollup.pending_ops)}</div>
        <div class="flow-card-detail">writer queue ops, oldest #{format_number(rollup.oldest_pending_age_us)}us</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Failures</div>
        <div class="flow-card-value #{if rollup.failures > 0, do: "c-red", else: "c-green"}" style="font-size:1.2rem;">#{format_number(rollup.failures)}</div>
        <div class="flow-card-detail">enqueue, flush, persist, or degraded projection events</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Flush Windows</div>
        <div class="flow-card-value" style="font-size:1.2rem;">#{format_duration_ms(data.lmdb_flush_interval_ms)} / #{format_duration_ms(data.history_flush_interval_ms)}</div>
        <div class="flow-card-detail">state and history projector batching</div>
      </div>
    </div>
    """
  end

  @spec render_flow_recent_records([map()], pos_integer() | nil) :: binary()
  defp render_flow_recent_records(records, limit \\ nil) do
    rows =
      case records do
        [] ->
          ~s(<tr><td colspan="10" class="c-muted">No Flow records discovered</td></tr>)

        _ ->
          Enum.map_join(records, "\n", fn record ->
            id = flow_record_id(record)
            state = flow_record_state(record)
            state_class = flow_state_class(state)
            status = flow_record_status_label(record)

            """
            <tr>
              <td class="mono">#{render_flow_id_link(id, flow_record_partition_key(record))}</td>
              <td class="mono">#{escape(flow_record_type(record))}</td>
              <td class="#{state_class}">#{escape(state)}</td>
              <td>#{escape(status)}</td>
              <td>#{format_number(flow_record_attempts(record))}</td>
              <td class="mono">#{escape(flow_record_worker(record) || "-")}</td>
              <td>#{escape(flow_waiting_reason(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_run_at_ms(record))}</td>
              <td>#{format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))}</td>
              <td>#{render_flow_value_ref_badges(record, :detail_link)}</td>
            </tr>
            """
          end)
      end

    limit_badge =
      case limit do
        limit when is_integer(limit) ->
          ~s( <span class="badge badge-idle">limit #{format_number(limit)}</span>)

        _ ->
          ""
      end

    """
    <div class="section-title">Recent Flow Records#{limit_badge}</div>
    <table>
      <thead>
        <tr><th>ID</th><th>Type</th><th>State</th><th>Status</th><th>Attempts</th><th>Worker</th><th>Why Waiting</th><th>Run At</th><th>Updated</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec flow_record_status_label(map()) :: binary()
  defp flow_record_status_label(record) do
    state = flow_record_state(record)

    cond do
      state in @flow_terminal_states -> "terminal"
      flow_expired_lease?(record) -> "expired lease"
      state == "running" -> "running"
      flow_retrying?(record) -> "retrying"
      flow_scheduled_future?(record) -> "scheduled"
      flow_due_now?(record) -> "due"
      true -> "active"
    end
  end

  @spec render_flow_detail_flash(map()) :: binary()
  defp render_flow_detail_flash(%{flash: %{kind: :ok, message: message}})
       when is_binary(message) do
    ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)
  end

  defp render_flow_detail_flash(%{flash: %{kind: :error, message: message}})
       when is_binary(message) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  defp render_flow_detail_flash(_data), do: ""

  @spec render_flow_detail(map()) :: binary()
  defp render_flow_detail(%{record: nil} = data) do
    reason =
      case Map.get(data, :record_status) do
        :timeout ->
          "Flow lookup timed out. The Flow record may still exist, but the dashboard did not wait for a slow FLOW.GET path."

        {:error, error} ->
          "Flow lookup failed: #{inspect(error, limit: 5)}"

        {:exit, error} ->
          "Flow lookup exited: #{inspect(error, limit: 5)}"

        _ ->
          "Flow #{data.id} was not found in the hot state sample or default Flow lookup."
      end

    """
    <div class="section-title">Flow Detail</div>
    <div class="pressure-alert level-warning">
      <div class="pressure-details">#{escape(reason)}</div>
    </div>
    """
  end

  defp render_flow_detail(data) do
    record = data.record
    state = flow_record_state(record)

    """
    <div class="section-title">Flow Detail <span class="badge #{flow_state_badge_class(state)}">#{escape(state)}</span></div>
    <div class="flow-detail-grid">
      <div class="flow-card flow-card-wide">
        <div class="flow-card-label">ID</div>
        <div class="flow-card-value mono" style="font-size:1rem;">#{escape(flow_record_id(record))}</div>
        <div class="flow-card-detail">type #{escape(flow_record_type(record))}</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Why waiting</div>
        <div class="flow-card-value" style="font-size:1rem;">#{escape(data.waiting_reason)}</div>
        <div class="flow-card-detail">computed from current durable state</div>
      </div>
      <div class="flow-card">
        <div class="flow-card-label">Fencing</div>
        <div class="flow-card-value">#{escape(to_string(flow_field(record, :fencing_token, "-")))}</div>
        <div class="flow-card-detail">lease safety token</div>
      </div>
    </div>
    #{render_flow_detail_table(record)}
    #{render_flow_detail_signals(data)}
    #{render_flow_rewind_action(data)}
    """
  end

  @spec render_flow_detail_signals(map()) :: binary()
  defp render_flow_detail_signals(%{record: %{} = record, history: history})
       when is_list(history) do
    rows = flow_signal_rows(record, history)

    render_flow_signals_table(rows, nil, nil, nil, %{}, :detail)
  end

  defp render_flow_detail_signals(_data), do: ""

  @spec render_flow_detail_table(map()) :: binary()
  defp render_flow_detail_table(record) do
    fields = [
      {"Type", flow_record_type(record)},
      {"State", flow_record_state(record)},
      {"Partition", flow_record_partition_key(record) || "auto/global"},
      {"Worker", flow_record_worker(record) || "-"},
      {"Priority", flow_field(record, :priority, 0)},
      {"Attempts", flow_field(record, :attempts, flow_field(record, :attempt, 0))},
      {"Run At", format_timestamp_ms_or_dash(flow_record_run_at_ms(record))},
      {"Lease Expires", format_timestamp_ms_or_dash(flow_record_lease_expires_at_ms(record))},
      {"Updated", format_timestamp_ms_or_dash(flow_record_updated_at_ms(record))},
      {"Parent", flow_field(record, :parent_flow_id, "-")},
      {"Root", flow_field(record, :root_flow_id, "-")},
      {"Correlation", flow_field(record, :correlation_id, "-")},
      {"Value Refs", {:safe, render_flow_value_ref_badges(record)}}
    ]

    rows =
      Enum.map_join(fields, "\n", fn {label, value} ->
        rendered =
          case value do
            {:safe, html} when is_binary(html) -> html
            value when is_binary(value) -> escape(value)
            value when is_integer(value) -> Integer.to_string(value)
            value -> to_string(value)
          end

        """
        <tr>
          <td class="c-muted">#{escape(label)}</td>
          <td class="mono">#{rendered}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Current State</div>
    <table>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec render_flow_rewind_action(map()) :: binary()
  defp render_flow_rewind_action(%{record: %{} = record} = data) do
    targets = flow_rewind_targets(Map.get(data, :history, []))
    id = flow_record_id(record)

    partition_key =
      flow_detail_url_partition_key(
        Map.get(data, :partition_key) || flow_record_partition_key(record)
      )

    action = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1) <> "/rewind"
    partition_input = render_flow_rewind_partition_input(partition_key)

    {select, button_attrs} =
      case targets do
        [] ->
          {~s(<select class="flow-search-input mono" name="to_event" title="Loaded history has no state event to rewind to" disabled><option>No rewind target in loaded history</option></select>),
           ~s( disabled title="No rewind target in loaded history")}

        _ ->
          {~s(<select class="flow-search-input mono" name="to_event" title="Choose one of this flow&#39;s loaded history events">#{render_flow_rewind_options(targets)}</select>),
           ~s( title="Create a durable rewind to the selected event")}
      end

    """
    <div class="flow-policy-panel">
      <div class="section-title">Rewind #{info_icon("Rewind creates a durable FLOW.REWIND command to move this flow back to a selected state from its own loaded history.")}</div>
      <form class="flow-policy-form" action="#{escape_attr(action)}" method="post">
        <input type="hidden" name="id" value="#{escape_attr(id)}">
        #{partition_input}
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Target event</span>
            #{select}
          </label>
          <label class="flow-policy-field">
            <span>Run at ms</span>
            <input class="flow-search-input mono" type="number" name="run_at_ms" min="0" placeholder="current" title="Optional run_at override in milliseconds; blank keeps the current scheduler choice">
          </label>
        </div>
        <div class="flow-policy-actions">
          <label class="flow-check-label" title="Required before the dashboard sends FLOW.REWIND.">
            <input type="checkbox" name="confirm_rewind" value="true">
            I reviewed the target event and understand this creates a new rewind event.
          </label>
          <button class="flow-search-button flow-danger-button" type="submit"#{button_attrs}>Rewind</button>
        </div>
      </form>
    </div>
    """
  end

  defp render_flow_rewind_action(_data), do: ""

  @spec render_flow_rewind_partition_input(binary() | nil) :: binary()
  defp render_flow_rewind_partition_input(partition_key)
       when is_binary(partition_key) and partition_key != "" do
    ~s(<input type="hidden" name="partition_key" value="#{escape_attr(partition_key)}">)
  end

  defp render_flow_rewind_partition_input(_partition_key), do: ""

  @spec flow_rewind_targets(list()) :: [map()]
  defp flow_rewind_targets(history) do
    history
    |> flow_history_timeline_rows()
    |> Enum.filter(fn row ->
      event_id = to_string(row.event_id)
      is_binary(row.to_state) and row.to_state != "" and event_id != "" and event_id != "-"
    end)
    |> Enum.uniq_by(fn row -> to_string(row.event_id) end)
  end

  @spec render_flow_rewind_options([map()]) :: binary()
  defp render_flow_rewind_options(targets) do
    Enum.map_join(targets, "\n", fn row ->
      event_id = to_string(row.event_id)
      state = row.to_state
      label = "#{state} / #{flow_history_event_label(row.fields)} / #{event_id}"

      ~s(<option value="#{escape_attr(event_id)}">#{escape(label)}</option>)
    end)
  end

  @spec render_flow_value_store(map()) :: binary()
  defp render_flow_value_store(%{record: nil}),
    do: ~s(<div id="flow-value-store" hidden aria-hidden="true"></div>)

  defp render_flow_value_store(data) do
    refs = Map.get(data, :value_refs, [])
    values_by_ref = Map.get(data, :values_by_ref, %{})
    status = Map.get(data, :values_status, :ok)

    rows =
      Enum.map_join(refs, "\n", fn entry ->
        anchor = flow_value_ref_anchor(entry.ref)
        preview = flow_value_store_preview(status, values_by_ref, entry.ref)
        label = escape_attr(entry.label)
        ref = escape_attr(entry.ref)

        """
        <div id="#{anchor}" class="flow-value-row" data-flow-value-ref="#{ref}" data-flow-value-label="#{label}">
          <pre class="flow-value-preview" data-flow-value-preview>#{escape(preview)}</pre>
        </div>
        """
      end)

    limit_note =
      if length(refs) >= @flow_dashboard_value_ref_limit do
        ~s( data-flow-value-limit-note="Showing first #{format_number(@flow_dashboard_value_ref_limit)} refs.")
      else
        ""
      end

    """
    <div id="flow-value-store" hidden aria-hidden="true"#{limit_note}>
      #{rows}
    </div>
    """
  end

  @spec flow_value_store_preview(atom() | {:error, term()} | {:exit, term()}, map(), binary()) ::
          binary()
  defp flow_value_store_preview(:ok, values_by_ref, ref) do
    flow_value_preview(Map.get(values_by_ref, ref, :not_loaded))
  end

  defp flow_value_store_preview(:skipped, _values_by_ref, _ref),
    do: "Value is not loaded on this page."

  defp flow_value_store_preview(:timeout, _values_by_ref, _ref), do: "Value lookup timed out."

  defp flow_value_store_preview({:error, reason}, _values_by_ref, _ref) do
    "Value lookup failed: #{inspect(reason, limit: 5)}"
  end

  defp flow_value_store_preview({:exit, reason}, _values_by_ref, _ref) do
    "Value lookup exited: #{inspect(reason, limit: 5)}"
  end

  defp flow_value_store_preview(_status, _values_by_ref, _ref),
    do: "Value is not loaded on this page."

  @spec render_flow_value_modal() :: binary()
  defp render_flow_value_modal do
    """
    <div id="flow-value-modal" class="flow-value-modal" hidden role="dialog" aria-modal="true" aria-labelledby="flow-value-modal-title">
      <div class="flow-value-modal-backdrop" data-flow-value-modal-close></div>
      <div class="flow-value-modal-panel">
        <div class="flow-value-modal-header">
          <div>
            <div id="flow-value-modal-title" class="section-title">Value Inspector</div>
            <div id="flow-value-modal-ref" class="flow-value-modal-ref mono"></div>
          </div>
          <button class="flow-value-modal-close" type="button" data-flow-value-modal-close title="Close value inspector">Close</button>
        </div>
        <pre id="flow-value-modal-body" class="flow-value-modal-body"></pre>
        <div class="flow-value-modal-actions">
          <button id="flow-value-modal-copy" class="flow-search-button" type="button" title="Copy the displayed value">Copy</button>
          <span id="flow-value-modal-copy-status" class="c-muted"></span>
        </div>
      </div>
    </div>
    """
  end

  @spec render_flow_debug(map()) :: binary()
  defp render_flow_debug(%{record: nil}) do
    """
    <div class="section-title">Debug Inspector</div>
    <div class="pressure-alert level-warning">
      <div class="pressure-details">No current Flow record is available to inspect.</div>
    </div>
    """
  end

  defp render_flow_debug(data) do
    record = data.record
    history = Map.get(data, :history, [])

    cards = [
      {"Execution", flow_execution_debug_summary(record), Map.get(data, :waiting_reason, "-")},
      {"Lease", flow_lease_debug_summary(record), flow_lease_debug_detail(record)},
      {"Values", flow_values_debug_summary(record),
       "payload/result/error/named value references"},
      {"History", flow_history_debug_summary(history),
       "latest events loaded for this detail view"}
    ]

    card_html =
      Enum.map_join(cards, "\n", fn {label, value, detail} ->
        """
        <div class="flow-card">
          <div class="flow-card-label">#{escape(label)}</div>
          <div class="flow-card-value" style="font-size:1rem;">#{escape(value)}</div>
          <div class="flow-card-detail">#{escape(detail)}</div>
        </div>
        """
      end)

    rows =
      flow_debug_rows(record, history)
      |> Enum.map_join("\n", fn {label, value} ->
        """
        <tr>
          <td class="c-muted">#{escape(label)}</td>
          <td class="mono">#{escape(value)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Debug Inspector</div>
    <div class="flow-card-grid">
      #{card_html}
    </div>
    <table>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  @spec flow_debug_rows(map(), list()) :: [{binary(), binary()}]
  defp flow_debug_rows(record, history) do
    [
      {"Identity", flow_debug_identity(record)},
      {"Storage", flow_debug_storage(record)},
      {"Run Timing", flow_run_debug_summary(record)},
      {"Lease", flow_lease_debug_summary(record)},
      {"Retry", flow_retry_debug_summary(record)},
      {"Values", flow_values_debug_detail(record)},
      {"Last Event", flow_last_event_debug_summary(history)}
    ] ++ flow_value_ref_debug_rows(record)
  end

  @spec flow_debug_identity(map()) :: binary()
  defp flow_debug_identity(record) do
    "type=#{flow_record_type(record)} id=#{flow_record_id(record)} state=#{flow_record_state(record)}"
  end

  @spec flow_debug_storage(map()) :: binary()
  defp flow_debug_storage(record) do
    partition = flow_record_partition_key(record) || "auto/global"
    "partition=#{partition} projection=lagged"
  end

  @spec flow_execution_debug_summary(map()) :: binary()
  defp flow_execution_debug_summary(record) do
    state = flow_record_state(record)

    cond do
      state in @flow_terminal_states -> "terminal #{state}"
      state == "running" -> "running"
      flow_due_now?(record) -> "claimable now"
      true -> "waiting"
    end
  end

  @spec flow_run_debug_summary(map()) :: binary()
  defp flow_run_debug_summary(record) do
    now = System.system_time(:millisecond)

    case flow_record_run_at_ms(record) do
      run_at when is_integer(run_at) and run_at > now ->
        "scheduled in #{format_duration_ms(run_at - now)} at #{format_timestamp_ms_or_dash(run_at)}"

      run_at when is_integer(run_at) and run_at > 0 ->
        "due since #{format_duration_ms(now - run_at)} at #{format_timestamp_ms_or_dash(run_at)}"

      _ ->
        "no run_at metadata"
    end
  end

  @spec flow_lease_debug_summary(map()) :: binary()
  defp flow_lease_debug_summary(record) do
    now = System.system_time(:millisecond)

    case {flow_record_state(record), flow_record_lease_expires_at_ms(record)} do
      {"running", expires_at} when is_integer(expires_at) and expires_at > now ->
        "running until #{format_timestamp_ms_or_dash(expires_at)}"

      {"running", expires_at} when is_integer(expires_at) and expires_at > 0 ->
        "expired #{format_duration_ms(now - expires_at)} ago"

      {"running", _} ->
        "running without lease expiry"

      {_state, _expires_at} ->
        "not leased"
    end
  end

  @spec flow_lease_debug_detail(map()) :: binary()
  defp flow_lease_debug_detail(record) do
    worker = flow_record_worker(record) || "-"
    token = flow_debug_value_or_dash(flow_field(record, :lease_token, nil))
    "worker=#{worker} token=#{token}"
  end

  @spec flow_debug_value_or_dash(term()) :: binary()
  defp flow_debug_value_or_dash(nil), do: "-"
  defp flow_debug_value_or_dash(""), do: "-"
  defp flow_debug_value_or_dash(value) when is_binary(value), do: value
  defp flow_debug_value_or_dash(value) when is_atom(value), do: Atom.to_string(value)
  defp flow_debug_value_or_dash(value) when is_integer(value), do: Integer.to_string(value)
  defp flow_debug_value_or_dash(value), do: inspect(value, limit: 5)

  @spec flow_retry_debug_summary(map()) :: binary()
  defp flow_retry_debug_summary(record) do
    attempts = flow_field(record, :attempts, flow_field(record, :attempt, 0))
    max_attempts = flow_field(record, :max_attempts, "-")
    exhausted_to = flow_field(record, :exhausted_to, "-")
    "attempts=#{attempts} max=#{max_attempts} exhausted_to=#{exhausted_to}"
  end

  @spec flow_values_debug_summary(map()) :: binary()
  defp flow_values_debug_summary(record) do
    "#{length(flow_value_ref_debug_rows(record))} refs"
  end

  @spec flow_values_debug_detail(map()) :: binary()
  defp flow_values_debug_detail(record) do
    flow_value_ref_debug_rows(record)
    |> Enum.map_join(", ", fn {label, _value} -> label end)
    |> case do
      "" -> "none"
      labels -> labels
    end
  end

  @spec flow_detail_value_refs(map(), list()) :: [map()]
  defp flow_detail_value_refs(record, history) do
    history_refs =
      Enum.flat_map(history, fn entry ->
        {event_id, fields} = normalize_flow_history_entry(entry)
        flow_value_ref_entries(fields, "event #{event_id}")
      end)

    (flow_value_ref_entries(record, "current state") ++ history_refs)
    |> dedupe_flow_value_refs()
  end

  @spec dedupe_flow_value_refs([map()]) :: [map()]
  defp dedupe_flow_value_refs(entries) do
    entries
    |> Enum.reduce({MapSet.new(), []}, fn entry, {seen, acc} ->
      if MapSet.member?(seen, entry.ref) do
        {seen, acc}
      else
        {MapSet.put(seen, entry.ref), [entry | acc]}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  @spec flow_value_ref_debug_rows(map()) :: [{binary(), binary()}]
  defp flow_value_ref_debug_rows(record) do
    base_refs =
      [
        {"payload_ref", flow_field(record, :payload_ref, nil)},
        {"result_ref", flow_field(record, :result_ref, nil)},
        {"error_ref", flow_field(record, :error_ref, nil)}
      ]
      |> Enum.filter(fn {_label, ref} -> is_binary(ref) and ref != "" end)

    named_refs =
      record
      |> flow_named_value_refs()
      |> Enum.map(fn {name, ref} -> {"value:#{to_string(name)}", ref} end)
      |> Enum.sort_by(fn {name, _ref} -> name end)

    base_refs ++ named_refs
  end

  @spec flow_value_ref_entries(map(), binary()) :: [map()]
  defp flow_value_ref_entries(record, source) do
    base_refs =
      [
        {"payload", flow_field(record, :payload_ref, nil)},
        {"result", flow_field(record, :result_ref, nil)},
        {"error", flow_field(record, :error_ref, nil)}
      ]
      |> Enum.flat_map(fn {label, ref} ->
        case ref do
          ref when is_binary(ref) and ref != "" ->
            [%{label: label, ref: ref, source: source}]

          _ ->
            []
        end
      end)

    named_refs =
      record
      |> flow_named_value_refs()
      |> Enum.map(fn {name, ref} -> %{label: to_string(name), ref: ref, source: source} end)
      |> Enum.sort_by(& &1.label)

    base_refs ++ named_refs
  end

  @spec flow_named_value_refs(map()) :: [{term(), binary()}]
  defp flow_named_value_refs(record) do
    record
    |> flow_field(:value_refs, flow_field(record, :values_refs, %{}))
    |> normalize_flow_named_value_refs()
  end

  @spec normalize_flow_named_value_refs(term()) :: [{term(), binary()}]
  defp normalize_flow_named_value_refs(refs) when is_map(refs) do
    Enum.flat_map(refs, fn {name, ref} ->
      case normalize_flow_value_ref(ref) do
        ref when is_binary(ref) and ref != "" -> [{name, ref}]
        _ -> []
      end
    end)
  end

  defp normalize_flow_named_value_refs(refs) when is_binary(refs) do
    case Jason.decode(refs) do
      {:ok, decoded} -> normalize_flow_named_value_refs(decoded)
      _ -> []
    end
  end

  defp normalize_flow_named_value_refs(_refs), do: []

  @spec normalize_flow_value_ref(term()) :: binary() | nil
  defp normalize_flow_value_ref(ref) when is_binary(ref), do: ref

  defp normalize_flow_value_ref(ref) when is_map(ref) do
    flow_field(ref, :ref, nil)
  end

  defp normalize_flow_value_ref(_ref), do: nil

  @spec flow_history_debug_summary(list()) :: binary()
  defp flow_history_debug_summary(history), do: "#{length(history)} events"

  @spec flow_last_event_debug_summary(list()) :: binary()
  defp flow_last_event_debug_summary([]), do: "none"

  defp flow_last_event_debug_summary(history) do
    {event_id, fields} =
      history
      |> List.last()
      |> normalize_flow_history_entry()

    "#{event_id}: #{flow_history_event_label(fields)} #{flow_history_state_move(fields)}"
  end

  @spec render_flow_history_timeline(
          list(),
          atom() | {:error, term()} | {:exit, term()},
          map() | nil
        ) ::
          binary()
  defp render_flow_history_timeline(history, status, page) do
    rows =
      cond do
        status == :timeout ->
          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: FLOW.HISTORY timed out.</td></tr>)

        match?({:error, _}, status) ->
          {_tag, reason} = status

          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: #{escape(inspect(reason, limit: 5))}</td></tr>)

        match?({:exit, _}, status) ->
          {_tag, reason} = status

          ~s(<tr><td colspan="8" class="c-muted">History temporarily unavailable: #{escape(inspect(reason, limit: 5))}</td></tr>)

        history == [] ->
          ~s(<tr><td colspan="8" class="c-muted">No history events found yet</td></tr>)

        true ->
          history
          |> flow_history_timeline_rows()
          |> Enum.map_join("\n", fn row ->
            fields = row.fields
            anchor = flow_history_event_anchor(row.event_id)

            """
            <tr id="#{anchor}" class="timeline-event-row">
              <td class="mono"><a class="flow-event-link" href="##{anchor}">#{escape(to_string(row.event_id))}</a></td>
              <td>#{format_timestamp_ms_or_dash(row.time_ms)}</td>
              <td>#{flow_history_action_html(fields)}</td>
              <td>#{escape(flow_history_state_move(row))}</td>
              <td>#{escape(flow_history_version_summary(fields))}</td>
              <td>#{escape(flow_history_attempt_summary(fields))}</td>
              <td class="mono">#{escape(flow_history_worker_summary(fields))}</td>
              <td class="mono">#{flow_history_refs_summary_html(fields)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Timeline</div>
    #{render_flow_history_pagination(page)}
    <table>
      <thead>
        <tr><th>Event</th><th>Time</th><th>Action</th><th>State Change</th><th>Version</th><th>Attempts</th><th>Worker</th><th>Values</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  defp render_flow_history_pagination(nil), do: ""

  defp render_flow_history_pagination(page) when is_map(page) do
    newer = render_flow_history_page_link("Newer", Map.get(page, :newer_url))
    older = render_flow_history_page_link("Older", Map.get(page, :older_url))
    count = Map.get(page, :count, @flow_dashboard_history_default_count)

    count_links =
      [50, 100, 250]
      |> Enum.map_join(" ", fn option ->
        class =
          if option == count do
            "flow-history-count flow-history-count-active"
          else
            "flow-history-count"
          end

        ~s(<a class="#{class}" href="#{flow_detail_history_count_url(page, option)}">#{option}</a>)
      end)

    """
    <div class="flow-history-controls">
      <div class="flow-history-pages">
        #{newer}
        #{older}
      </div>
      <div class="flow-history-counts">
        <span class="c-muted">History page</span>
        #{count_links}
      </div>
    </div>
    """
  end

  defp render_flow_history_page_link(label, url) when is_binary(url) and url != "" do
    ~s(<a class="flow-history-page-link" href="#{escape(url)}">#{label}</a>)
  end

  defp render_flow_history_page_link(label, _url) do
    ~s(<span class="flow-history-page-link flow-history-page-disabled">#{label}</span>)
  end

  defp flow_detail_history_count_url(%{id: id, partition_key: partition_key}, count),
    do: flow_detail_path(id, partition_key, %{"history_count" => count})

  defp flow_detail_history_count_url(_page, count), do: "?history_count=#{count}"

  @spec render_flow_id_link(binary(), binary() | nil) :: binary()
  defp render_flow_id_link(id, partition_key) do
    href = flow_detail_path(id, flow_detail_url_partition_key(partition_key))
    ~s(<a class="flow-link" href="#{href}">#{escape(id)}</a>)
  end

  @spec flow_detail_path(binary(), binary() | nil) :: binary()
  defp flow_detail_path(id, partition_key), do: flow_detail_path(id, partition_key, %{})

  @spec flow_detail_path(binary(), binary() | nil, map()) :: binary()
  defp flow_detail_path(id, partition_key, params) when is_map(params) do
    path = "/dashboard/flow/" <> URI.encode(id, &URI.char_unreserved?/1)
    params = flow_detail_query_params(partition_key, params)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  @spec flow_detail_live_url(binary(), binary() | nil, map() | nil) :: binary()
  defp flow_detail_live_url(id, partition_key, history_page) do
    path = "/dashboard/api/flow/" <> URI.encode(id, &URI.char_unreserved?/1)

    history_params =
      if is_map(history_page), do: Map.get(history_page, :current_live_params, %{}), else: %{}

    params = flow_detail_query_params(partition_key, history_params)

    if map_size(params) == 0, do: path, else: path <> "?" <> URI.encode_query(params)
  end

  defp flow_detail_query_params(partition_key, params) do
    params =
      params
      |> Enum.reduce(%{}, fn
        {_key, nil}, acc ->
          acc

        {key, value}, acc when is_atom(key) ->
          Map.put(acc, Atom.to_string(key), value)

        {key, value}, acc ->
          Map.put(acc, to_string(key), value)
      end)

    case partition_key do
      key when is_binary(key) and key != "" -> Map.put(params, "partition_key", key)
      _ -> params
    end
  end

  @spec render_flow_value_ref_badges(map(), :local | :detail_link) :: binary()
  defp render_flow_value_ref_badges(record, mode \\ :local) do
    badges =
      record
      |> flow_value_ref_entries("current state")
      |> Enum.map(&render_flow_value_ref_badge(record, mode, &1))

    case badges do
      [] -> ~s(<span class="c-muted">none</span>)
      _ -> Enum.join(badges, " ")
    end
  end

  @spec render_flow_value_ref_badge(map() | nil, :local | :detail_link, map()) :: binary()
  defp render_flow_value_ref_badge(record, mode, %{label: label, ref: ref}) do
    anchor = flow_value_ref_anchor(ref)
    href = flow_value_ref_href(record, mode, anchor)
    title = "Open #{label} value"

    ~s(<a class="flow-pill flow-value-ref-link" href="#{escape_attr(href)}" title="#{escape_attr(title)}" aria-label="#{escape_attr(title)}" data-flow-value-ref="#{escape_attr(ref)}" data-flow-value-label="#{escape_attr(label)}">#{escape(label)}</a>)
  end

  @spec flow_value_ref_href(map() | nil, :local | :detail_link, binary()) :: binary()
  defp flow_value_ref_href(record, :detail_link, anchor) when is_map(record) do
    id = flow_record_id(record)
    partition_key = flow_detail_url_partition_key(flow_record_partition_key(record))
    flow_detail_path(id, partition_key) <> "##{anchor}"
  end

  defp flow_value_ref_href(_record, _mode, anchor), do: "##{anchor}"

  @spec normalize_flow_history_entry(term()) :: {term(), map()}
  defp normalize_flow_history_entry({event_id, fields}) when is_map(fields),
    do: {event_id, fields}

  defp normalize_flow_history_entry({event_id, fields}) when is_list(fields),
    do: {event_id, Map.new(fields)}

  defp normalize_flow_history_entry(entry), do: {"-", %{raw: inspect(entry, limit: 5)}}

  @spec flow_history_timeline_rows(list()) :: [map()]
  defp flow_history_timeline_rows(history) do
    history
    |> Enum.map(&normalize_flow_history_entry/1)
    |> Enum.sort_by(fn {event_id, fields} ->
      {flow_history_event_time_ms(event_id, fields), to_string(event_id)}
    end)
    |> Enum.map_reduce(nil, fn {event_id, fields}, previous_state ->
      current_state = flow_history_current_state(fields)
      from_state = flow_history_previous_state(fields, previous_state)

      row = %{
        event_id: event_id,
        fields: fields,
        time_ms: flow_history_event_time_ms(event_id, fields),
        from_state: from_state,
        to_state: current_state
      }

      next_state =
        case current_state do
          "" -> previous_state
          state -> state
        end

      {row, next_state}
    end)
    |> elem(0)
    |> Enum.reverse()
  end

  @spec flow_history_event_anchor(term()) :: binary()
  defp flow_history_event_anchor(event_id) do
    encoded =
      event_id
      |> to_string()
      |> Base.url_encode64(padding: false)

    "flow-event-" <> encoded
  end

  @spec flow_value_ref_anchor(binary()) :: binary()
  defp flow_value_ref_anchor(ref) do
    encoded = Base.url_encode64(ref, padding: false)
    "flow-value-" <> encoded
  end

  @spec flow_value_preview(term()) :: binary()
  defp flow_value_preview(:not_loaded), do: "not loaded"
  defp flow_value_preview(nil), do: "missing"

  defp flow_value_preview(value) when is_binary(value) do
    if String.valid?(value) do
      flow_truncate_preview(value)
    else
      value
      |> inspect(limit: :infinity, printable_limit: @flow_dashboard_value_preview_bytes)
      |> flow_truncate_preview()
    end
  end

  defp flow_value_preview(value) do
    value
    |> inspect(pretty: true, limit: 50, printable_limit: @flow_dashboard_value_preview_bytes)
    |> flow_truncate_preview()
  end

  @spec flow_truncate_preview(binary()) :: binary()
  defp flow_truncate_preview(value) when is_binary(value) do
    if String.length(value) > @flow_dashboard_value_preview_bytes do
      String.slice(value, 0, @flow_dashboard_value_preview_bytes) <> "\n... truncated ..."
    else
      value
    end
  end

  @spec flow_history_event_time_ms(term(), map()) :: integer() | nil
  defp flow_history_event_time_ms(event_id, fields) do
    flow_first_integer(fields, [:at, :updated_at_ms, :created_at_ms, :run_at_ms]) ||
      flow_history_event_id_time_ms(event_id)
  end

  @spec flow_history_event_id_time_ms(term()) :: integer() | nil
  defp flow_history_event_id_time_ms(event_id) do
    event_id
    |> to_string()
    |> String.split("-", parts: 2)
    |> List.first()
    |> case do
      part when is_binary(part) ->
        case Integer.parse(part) do
          {parsed, _rest} -> parsed
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @spec flow_history_current_state(map()) :: binary()
  defp flow_history_current_state(fields) do
    flow_field_string(fields, :to_state, flow_field_string(fields, :state, ""))
  end

  @spec flow_history_previous_state(map(), binary() | nil) :: binary()
  defp flow_history_previous_state(fields, previous_state) do
    flow_field_string(fields, :from_state, previous_state || "")
  end

  @spec flow_history_event_label(map()) :: binary()
  defp flow_history_event_label(fields) do
    raw = flow_field_string(fields, :event, flow_field_string(fields, :action, "event"))

    case String.downcase(raw) do
      "create" -> "Created"
      "created" -> "Created"
      "transition" -> "Transitioned"
      "transitioned" -> "Transitioned"
      "retry" -> "Retry"
      "retried" -> "Retry"
      "complete" -> "Completed"
      "completed" -> "Completed"
      "fail" -> "Failed"
      "failed" -> "Failed"
      "cancel" -> "Cancelled"
      "canceled" -> "Cancelled"
      "cancelled" -> "Cancelled"
      "claim" -> "Claimed"
      "claimed" -> "Claimed"
      other -> other |> String.replace("_", " ") |> String.capitalize()
    end
  end

  @spec flow_history_action_html(map()) :: binary()
  defp flow_history_action_html(fields) do
    label = flow_history_event_label(fields)

    terminal_badge =
      if flow_history_terminal_event?(fields) do
        ~s( <span class="flow-pill">terminal</span>)
      else
        ""
      end

    escape(label) <> terminal_badge
  end

  @spec flow_history_terminal_event?(map()) :: boolean()
  defp flow_history_terminal_event?(fields) do
    event =
      fields
      |> flow_field_string(:event, flow_field_string(fields, :action, ""))
      |> String.downcase()

    state =
      fields
      |> flow_history_current_state()
      |> String.downcase()

    event in ["completed", "complete", "failed", "fail", "cancelled", "canceled", "cancel"] or
      state in @flow_terminal_states
  end

  @spec flow_history_state_move(map()) :: binary()
  defp flow_history_state_move(%{from_state: from_state, to_state: to_state}) do
    cond do
      is_binary(from_state) and from_state != "" and is_binary(to_state) and to_state != "" and
          from_state != to_state ->
        from_state <> " -> " <> to_state

      is_binary(to_state) and to_state != "" ->
        to_state

      true ->
        "-"
    end
  end

  defp flow_history_state_move(fields) do
    from_state = flow_field_string(fields, :from_state, "")
    to_state = flow_field_string(fields, :to_state, flow_field_string(fields, :state, ""))

    cond do
      from_state != "" and to_state != "" -> from_state <> " -> " <> to_state
      to_state != "" -> to_state
      true -> "-"
    end
  end

  @spec flow_history_version_summary(map()) :: binary()
  defp flow_history_version_summary(fields) do
    ["version", "fencing_token"]
    |> flow_history_key_value_summary(fields)
  end

  @spec flow_history_attempt_summary(map()) :: binary()
  defp flow_history_attempt_summary(fields) do
    ["attempts", "max_attempts"]
    |> flow_history_key_value_summary(fields)
  end

  @spec flow_history_worker_summary(map()) :: binary()
  defp flow_history_worker_summary(fields) do
    flow_first_non_empty_binary(fields, [:worker, :lease_owner]) || "-"
  end

  @spec flow_history_refs_summary_html(map()) :: binary()
  defp flow_history_refs_summary_html(fields) do
    badges =
      fields
      |> flow_value_ref_entries("history event")
      |> Enum.map(&render_flow_value_ref_badge(nil, :local, &1))

    case badges do
      [] -> "-"
      _ -> Enum.join(badges, " ")
    end
  end

  @spec flow_history_key_value_summary([binary()], map()) :: binary()
  defp flow_history_key_value_summary(keys, fields) do
    keys
    |> Enum.flat_map(fn key ->
      atom_key = String.to_existing_atom(key)

      case flow_field(fields, atom_key, nil) do
        nil -> []
        "" -> []
        value -> ["#{key}=#{value}"]
      end
    end)
    |> case do
      [] -> "-"
      parts -> Enum.join(parts, ", ")
    end
  end

  @spec flow_state_class(binary()) :: binary()
  defp flow_state_class("failed"), do: "c-red"
  defp flow_state_class("cancelled"), do: "c-yellow"
  defp flow_state_class("running"), do: "c-green"
  defp flow_state_class(_state), do: ""

  @spec flow_state_badge_class(binary()) :: binary()
  defp flow_state_badge_class("failed"), do: "badge-pressure"
  defp flow_state_badge_class("cancelled"), do: "badge-warning"
  defp flow_state_badge_class(state) when state in @flow_terminal_states, do: "badge-ok"
  defp flow_state_badge_class("running"), do: "badge-merging"
  defp flow_state_badge_class(_state), do: "badge-idle"

  # ---------------------------------------------------------------------------
  # HTML rendering -- KV Keyspace / Commands / Read Path Sub-pages
  # ---------------------------------------------------------------------------

  @spec render_keyspace_controls(map()) :: binary()
  defp render_keyspace_controls(data) do
    filters = Map.get(data, :filters, %{})
    key = Map.get(filters, :key, "")
    prefix = Map.get(filters, :prefix, "")
    limit = Map.get(filters, :limit, @keyspace_dashboard_default_limit)
    checked = if Map.get(filters, :include_internal, false), do: " checked", else: ""

    """
    <div class="kv-panel">
      <form class="flow-filter-form" action="/dashboard/keyspace" method="get">
        <label>Exact key
          <input class="flow-search-input mono" type="search" name="key" value="#{escape_attr(key)}" placeholder="user:123">
        </label>
        <label>Prefix
          <input class="flow-search-input mono" type="search" name="prefix" value="#{escape_attr(prefix)}" placeholder="tenant:">
        </label>
        <label>Limit
          <input class="flow-search-input flow-filter-limit" type="number" min="1" max="#{@keyspace_dashboard_max_limit}" name="limit" value="#{limit}">
        </label>
        <label class="flow-check-label" title="Show internal compound keys used by hashes, sets, zsets, streams, Flow values, and metadata.">
          <input type="checkbox" name="include_internal" value="true"#{checked}> Internal
        </label>
        <button class="flow-search-button" type="submit">Search</button>
        <a class="flow-filter-clear" href="/dashboard/keyspace">Clear</a>
      </form>
      <div class="flow-filter-note">Requires +SCAN for samples. Exact key inspection requires +GET and key read access.</div>
    </div>
    """
  end

  @spec render_keyspace_inspector(map() | nil) :: binary()
  defp render_keyspace_inspector(nil), do: ""

  defp render_keyspace_inspector(%{found?: false, key: key}) do
    """
    <div class="kv-inspector">
      <div class="section-title">Key Inspector</div>
      <div class="flow-alert flow-alert-error">No live key metadata found for <code>#{escape(key)}</code>.</div>
      <div class="flow-filter-note">Requires +GET and read access to the selected key.</div>
    </div>
    """
  end

  defp render_keyspace_inspector(inspected) do
    """
    <div class="kv-inspector">
      #{render_ops_summary("Key Inspector", [%{label: "Key", value: inspected.key}, %{label: "Type", value: inspected.type}, %{label: "Shard", value: "Shard #{inspected.shard}"}, %{label: "Location", value: inspected.location, detail: "TTL #{inspected.ttl} · #{inspected.size}"}])}
      <div class="flow-filter-note">Requires +GET and read access to the selected key.</div>
    </div>
    """
  end

  @spec render_keyspace_table(map()) :: binary()
  defp render_keyspace_table(data) do
    rows = Map.get(data, :rows, [])
    sampled = Map.get(data, :total_sampled, length(rows))

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="8" class="c-muted">No key metadata matched this query.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            internal =
              if Map.get(row, :internal?, false) do
                ~s(<span class="badge badge-idle">internal</span>)
              else
                ""
              end

            """
            <tr>
              <td class="mono">#{escape(Map.get(row, :key, ""))} #{internal}</td>
              <td>#{escape(Map.get(row, :type, "-"))}</td>
              <td>#{Map.get(row, :shard, "-")}</td>
              <td>#{escape(Map.get(row, :location, "-"))}</td>
              <td>#{escape(Map.get(row, :size, "-"))}</td>
              <td>#{escape(Map.get(row, :ttl, "-"))}</td>
              <td>#{format_number(Map.get(row, :lfu, 0))}</td>
              <td class="mono">#{escape(Map.get(row, :physical_key, ""))}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Key Metadata <span class="badge badge-idle">sampled #{format_number(sampled)}</span></div>
    <table>
      <thead>
        <tr><th>Logical Key</th><th>Type</th><th>Shard</th><th>Location</th><th>Size</th><th>TTL</th><th>LFU</th><th>Physical Key</th></tr>
      </thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  @spec render_commands_summary(map()) :: binary()
  defp render_commands_summary(data) do
    summary = Map.get(data, :summary, %{})

    render_ops_summary("Command Summary", [
      %{label: "Commands", value: format_number(Map.get(summary, :total_commands, 0))},
      %{label: "Ops/Sec", value: to_string(Map.get(summary, :ops_per_sec, 0.0))},
      %{label: "Slowlog", value: format_number(Map.get(summary, :slowlog_entries, 0))},
      %{label: "Slowest", value: format_duration_us(Map.get(summary, :slowest_us, 0))}
    ])
  end

  @spec render_command_slowlog_table(map()) :: binary()
  defp render_command_slowlog_table(data) do
    rows = Map.get(data, :slow_by_command, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No slow commands recorded.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.command)}</td>
              <td>#{format_number(row.count)}</td>
              <td>#{format_duration_us(row.worst_us)}</td>
              <td>#{format_duration_us(row.avg_us)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Slow Log By Command</div>
    <table>
      <thead><tr><th>Command</th><th>Entries</th><th>Worst</th><th>Average</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  @spec render_kv_command_reference(map()) :: binary()
  defp render_kv_command_reference(data) do
    groups = Map.get(data, :command_groups, kv_command_groups())

    body =
      Enum.map_join(groups, "\n", fn group ->
        commands =
          group.commands
          |> Enum.map_join(" ", &~s(<span class="flow-pill mono">#{escape(&1)}</span>))

        """
        <div class="kv-command-group">
          <div class="kv-command-title">#{escape(group.name)}</div>
          <div class="kv-command-purpose">#{escape(group.purpose)}</div>
          <div>#{commands}</div>
        </div>
        """
      end)

    """
    <div class="section-title">Command Groups</div>
    <div class="kv-command-grid">#{body}</div>
    """
  end

  @spec render_reads_summary(map()) :: binary()
  defp render_reads_summary(data) do
    hotcold = Map.fetch!(data, :hotcold)
    hot_reads = Map.get(hotcold, :hot_reads, Map.get(hotcold, :total_hot, 0))
    cold_reads = Map.get(hotcold, :cold_reads, Map.get(hotcold, :total_cold, 0))

    render_ops_summary("Read Path Summary", [
      %{label: "Hit Rate", value: "#{hotcold.hit_ratio}%"},
      %{
        label: "Hot Reads",
        value: format_number(hot_reads),
        detail_html: "sampled #{sampled_tag(Map.get(hotcold, :sample_rate, 1))}"
      },
      %{
        label: "Cold Reads",
        value: format_number(cold_reads),
        detail: "#{Map.get(hotcold, :cold_reads_per_sec, 0.0)}/sec"
      },
      %{label: "Misses", value: format_number(Map.get(hotcold, :total_misses, 0))}
    ])
  end

  @spec render_read_prefix_table(map()) :: binary()
  defp render_read_prefix_table(data) do
    rows = Map.get(data, :prefixes, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No sampled read pressure yet.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.prefix)}</td>
              <td>#{format_number(row.hot_reads)}</td>
              <td>#{format_number(row.cold_reads)}</td>
              <td>#{Float.round(row.cold_pct, 1)}%</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Prefix Read Pressure</div>
    <table>
      <thead><tr><th>Prefix</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th><th>Cold %</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Storage Sub-page
  # ---------------------------------------------------------------------------

  @spec render_storage_summary(map()) :: binary()
  defp render_storage_summary(data) do
    shards = Map.get(data, :shards, [])
    data_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.data_file_count end)
    hint_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.hint_file_count end)

    largest =
      Enum.max_by(shards, & &1.disk_bytes, fn -> %{index: "-", disk_bytes: 0} end)

    render_ops_summary("Storage Summary", [
      %{label: "Total Disk", value: format_bytes(data.total_disk_bytes)},
      %{label: "Total Files", value: format_number(data.total_files)},
      %{
        label: "Largest Shard",
        value: "Shard #{largest.index}",
        detail: format_bytes(largest.disk_bytes)
      },
      %{
        label: "Data Files",
        value: format_number(data_files),
        detail: "#{format_number(hint_files)} Hint Files"
      }
    ])
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
        ~s(<div style="margin-top:8px; font-size:0.72rem; color:#8b949e;">Sampled #{format_number(data.total_sampled)} keys from keydir ETS tables</div>)
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

  @spec render_prefixes_summary(map()) :: binary()
  defp render_prefixes_summary(data) do
    total_indexed = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.keys end)
    hot_reads = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.hot_reads end)
    cold_reads = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.cold_reads end)

    render_ops_summary("Prefix Summary", [
      %{label: "Sampled Keys", value: format_number(data.total_sampled)},
      %{label: "Indexed Keys", value: format_number(total_indexed)},
      %{label: "Hot Reads", value: format_number(hot_reads)},
      %{label: "Cold Reads", value: format_number(cold_reads)}
    ])
  end

  # ---------------------------------------------------------------------------
  # Shared HTML scaffolding
  # ---------------------------------------------------------------------------

  @spec page_head(String.t(), non_neg_integer() | nil) :: binary()
  defp page_head(title, refresh_seconds) do
    page_head(title, refresh_seconds, [])
  end

  @spec page_head(String.t(), non_neg_integer() | nil, keyword()) :: binary()
  defp page_head(title, refresh_seconds, opts) do
    _poll_interval_hint = refresh_seconds
    _chartjs_removed = Keyword.get(opts, :chartjs, false)

    """
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <meta name="description" content="FerricStore operational dashboard">
      <title>#{escape(title)}</title>
      <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, monospace; background: #0d1117; color: #c9d1d9; padding: 0; min-height: 100vh; }
        [data-live-component] { display: contents; }

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
        .page-intro { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; margin: 0 0 16px; color: #9da7b3; line-height: 1.45; }
        .page-intro-title { color: #f0f6fc; font-weight: 700; margin-bottom: 4px; }
        .kv-panel, .kv-inspector { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; margin-bottom: 16px; }
        .kv-command-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-bottom: 16px; }
        .kv-command-group { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 12px 14px; min-width: 0; }
        .kv-command-title { color: #f0f6fc; font-weight: 700; margin-bottom: 4px; }
        .kv-command-purpose { color: #8b949e; font-size: 0.78rem; margin-bottom: 8px; }

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

        /* Operational summary cards */
        .ops-summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 16px; }
        .ops-summary-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; min-width: 0; }
        .ops-summary-label { font-size: 0.68rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
        .ops-summary-value { font-size: 1.45rem; font-weight: 800; color: #f0f6fc; overflow-wrap: anywhere; }
        .ops-summary-detail { color: #8b949e; font-size: 0.72rem; margin-top: 4px; overflow-wrap: anywhere; }

        /* FerricFlow */
        .flow-card-grid, .flow-detail-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 14px; margin-bottom: 16px; }
        .flow-detail-grid { grid-template-columns: minmax(260px, 2fr) repeat(auto-fit, minmax(180px, 1fr)); }
        .flow-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px 18px; min-width: 0; }
        .flow-card-wide { grid-column: span 2; }
        .flow-card-label { font-size: 0.68rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 6px; }
        .flow-card-value { font-size: 1.7rem; font-weight: 800; color: #f0f6fc; overflow-wrap: anywhere; }
        .flow-card-detail { font-size: 0.72rem; color: #8b949e; margin-top: 4px; }
        .flow-nav-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; margin: 4px 0 18px; }
        .flow-tabs { display: flex; gap: 10px; flex-wrap: wrap; margin: 4px 0 18px; }
        .flow-nav-row .flow-tabs { margin: 0; }
        .flow-tab-group { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; padding: 5px 6px; border: 1px solid #21262d; border-radius: 8px; background: rgba(22, 27, 34, 0.55); }
        .flow-tab-group-label { color: #8b949e; font-size: 0.62rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 0 3px; }
        .flow-tab-group-links { display: flex; gap: 6px; flex-wrap: wrap; }
        .flow-tab { display: inline-flex; align-items: center; border: 1px solid #30363d; background: #161b22; color: #c9d1d9; text-decoration: none; border-radius: 999px; padding: 6px 12px; font-size: 0.78rem; }
        .flow-tab:hover { background: #1c2128; }
        .flow-tab.active { color: #58a6ff; border-color: #1f6feb; background: #0f1b2d; font-weight: 700; }
        .flow-search { display: flex; align-items: center; gap: 6px; min-width: min(100%, 360px); }
        .flow-search-input { flex: 1; min-width: 0; height: 32px; background: #0d1117; color: #c9d1d9; border: 1px solid #30363d; border-radius: 6px; padding: 0 10px; font-size: 0.78rem; }
        .flow-search-input:focus { outline: none; border-color: #1f6feb; box-shadow: 0 0 0 2px rgba(31, 111, 235, 0.18); }
        .flow-search-button { height: 32px; border: 1px solid #30363d; background: #21262d; color: #f0f6fc; border-radius: 6px; padding: 0 12px; font-size: 0.78rem; cursor: pointer; }
        .flow-search-button:hover { background: #30363d; }
        .flow-danger-button { border-color: rgba(248, 81, 73, 0.55); color: #ffb3ad; }
        .flow-danger-button:hover { background: rgba(248, 81, 73, 0.14); }
        .flow-filter-panel { display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; margin-bottom: 16px; }
        .flow-filter-form { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
        .flow-filter-form label { font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
        .flow-filter-form select { min-width: 220px; }
        .flow-filter-form input[type="search"] { min-width: 160px; }
        .flow-query-help { display: grid; gap: 5px; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; color: #9da7b3; font-size: 0.8rem; }
        .flow-query-help-main { display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; color: #c9d1d9; }
        .flow-query-command { color: #79c0ff; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-weight: 700; }
        .flow-query-help-detail { color: #8b949e; line-height: 1.4; }
        .flow-query-field { display: grid; gap: 4px; min-width: 170px; }
        .flow-query-field[hidden], .flow-query-check[hidden] { display: none !important; }
        .flow-query-field .flow-search-input { width: 100%; }
        .flow-field-help { color: #6e7681; font-size: 0.66rem; text-transform: none; letter-spacing: 0; line-height: 1.25; max-width: 220px; }
        .flow-query-check { align-self: end; height: 32px; }
        .flow-filter-range { flex: 0 0 150px; max-width: 150px; min-width: 150px; }
        .flow-filter-time { flex: 0 0 172px; max-width: 172px; min-width: 172px; }
        .flow-filter-limit { flex: 0 0 78px; max-width: 78px; }
        .flow-filter-clear { color: #79c0ff; text-decoration: none; font-size: 0.78rem; }
        .flow-filter-clear:hover { text-decoration: underline; }
        .flow-filter-note { color: #8b949e; font-size: 0.78rem; }
        .flow-check-label { display: inline-flex; align-items: center; gap: 6px; color: #8b949e; }
        .flow-check-label input { margin: 0; accent-color: #1f6feb; }
        .flow-policy-panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px 16px; margin-bottom: 18px; }
        .flow-policy-panel .section-title { margin-top: 0; }
        .flow-policy-form { display: grid; gap: 12px; }
        .flow-policy-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; align-items: end; }
        .flow-policy-field { display: grid; gap: 5px; min-width: 0; }
        .flow-policy-field span { color: #8b949e; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
        .flow-policy-actions { display: flex; justify-content: flex-end; }
        .flow-policy-action { display: inline-flex; align-items: center; justify-content: center; text-decoration: none; }
        .flow-policy-preview { display: grid; gap: 5px; color: #c9d1d9; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; padding: 10px 12px; font-size: 0.8rem; }
        .flow-policy-preview-title { color: #79c0ff; font-size: 0.68rem; text-transform: uppercase; letter-spacing: 0.5px; }
        .flow-alert { border-radius: 6px; padding: 8px 10px; margin-bottom: 12px; font-size: 0.8rem; }
        .flow-alert-ok { background: rgba(35, 134, 54, 0.18); border: 1px solid rgba(35, 134, 54, 0.55); color: #a5d6a7; }
        .flow-alert-error { background: rgba(248, 81, 73, 0.14); border: 1px solid rgba(248, 81, 73, 0.55); color: #ffb3ad; }
        .flow-issue-row { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
        .flow-issue { display: flex; align-items: center; gap: 8px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 10px 12px; font-size: 0.82rem; color: #c9d1d9; }
        .flow-pill { display: inline-block; background: #21262d; color: #8b949e; border: 1px solid #30363d; border-radius: 999px; padding: 1px 7px; font-size: 0.68rem; margin: 1px 2px 1px 0; white-space: nowrap; }
        .flow-pill.flow-value-ref-link { color: #f0f6fc; }
        .flow-link { color: #79c0ff; text-decoration: none; }
        .flow-link:hover { text-decoration: underline; }
        .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 16px; }
        .chart-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; min-height: 220px; }
        .chart-title { font-size: 0.75rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 12px; }
        .chart-card canvas { width: 100% !important; max-height: 220px; }
        .chart-empty { color: #8b949e; padding: 28px 0; text-align: center; }
        .chart-bars { display: grid; gap: 14px; }
        .chart-row { display: grid; grid-template-columns: minmax(120px, 220px) 1fr; gap: 14px; align-items: start; }
        .chart-row-label { color: #f0f6fc; font-weight: 600; overflow-wrap: anywhere; }
        .chart-row-bars { display: grid; gap: 6px; }
        .chart-bar-line { display: grid; grid-template-columns: 76px 1fr 64px; gap: 10px; align-items: center; }
        .chart-bar-label { color: #9da7b3; font-size: 0.78rem; }
        .chart-bar-value { color: #f0f6fc; font-size: 0.78rem; text-align: right; }
        .chart-bar-track { height: 10px; border-radius: 999px; background: #0d1117; overflow: hidden; border: 1px solid #30363d; }
        .chart-bar-fill { display: block; height: 100%; border-radius: 999px; min-width: 2px; }
        .bar-green { background: #3fb950; }
        .bar-yellow { background: #d29922; }
        .bar-red { background: #f85149; }
        .bar-blue { background: #58a6ff; }
        .flow-timeline-graph { display: grid; gap: 8px; min-width: 0; }
        .flow-timeline-scroll { overflow-x: auto; overflow-y: hidden; border: 1px solid #30363d; border-radius: 8px; background: #0d1117; }
        .flow-timeline-svg { display: block; min-width: 100%; }
        .flow-timeline-bg { fill: #0d1117; }
        .flow-timeline-lane line { stroke: #21262d; stroke-width: 1; }
        .flow-timeline-lane-label { fill: #c9d1d9; font-size: 12px; font-weight: 700; }
        .flow-timeline-axis line { stroke: #21262d; stroke-width: 1; stroke-dasharray: 3 6; }
        .flow-timeline-axis-label { fill: #6e7681; font-size: 10px; font-variant-numeric: tabular-nums; }
        .flow-timeline-path { fill: none; stroke: rgba(121, 192, 255, 0.24); stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
        .flow-timeline-transition { fill: none; stroke: rgba(139, 148, 158, 0.34); stroke-width: 2; stroke-linecap: round; stroke-dasharray: 4 5; }
        .flow-timeline-duration-segment { stroke-width: 7; stroke-linecap: round; opacity: 0.86; }
        .flow-timeline-duration-segment.bar-green { stroke: #3fb950; }
        .flow-timeline-duration-segment.bar-yellow { stroke: #d29922; }
        .flow-timeline-duration-segment.bar-red { stroke: #f85149; }
        .flow-timeline-duration-segment.bar-blue { stroke: #58a6ff; }
        .flow-timeline-node { stroke: #0d1117; stroke-width: 3; transition: r 0.12s ease, stroke 0.12s ease; }
        .flow-timeline-node-link:hover .flow-timeline-node, .flow-timeline-node-link:focus .flow-timeline-node { r: 10; stroke: #f0f6fc; }
        .flow-timeline-node-normal { fill: #58a6ff; }
        .flow-timeline-node-terminal { fill: #3fb950; }
        .flow-timeline-node-retry { fill: #f85149; }
        .flow-timeline-node-failed { fill: #f85149; }
        .flow-timeline-node-label { fill: #9da7b3; font-size: 10px; font-weight: 600; pointer-events: none; }
        .flow-timeline-caption { color: #8b949e; font-size: 0.74rem; }
        .timeline-event-row:target { outline: 2px solid #58a6ff; outline-offset: -2px; background: #10243a; }
        .flow-history-controls { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin: 8px 0 12px; flex-wrap: wrap; }
        .flow-history-pages, .flow-history-counts { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
        .flow-history-page-link, .flow-history-count { display: inline-flex; align-items: center; justify-content: center; min-height: 28px; border: 1px solid #30363d; border-radius: 6px; padding: 0 10px; color: #f0f6fc; background: #21262d; text-decoration: none; font-size: 0.76rem; }
        .flow-history-page-link:hover, .flow-history-page-link:focus, .flow-history-count:hover, .flow-history-count:focus { border-color: #58a6ff; color: #f0f6fc; background: #161b22; }
        .flow-history-page-disabled { color: #6e7681; background: #0d1117; cursor: default; }
        .flow-history-count-active { border-color: #58a6ff; color: #79c0ff; background: #10243a; }
        .flow-value-row:target { outline: 2px solid #58a6ff; outline-offset: -2px; background: #10243a; }
        .flow-event-link { color: #79c0ff; text-decoration: none; }
        .flow-event-link:hover, .flow-event-link:focus { text-decoration: underline; }
        .flow-value-ref-link { color: #f0f6fc; text-decoration: none; }
        .flow-value-ref-link:hover, .flow-value-ref-link:focus { border-color: #58a6ff; color: #f0f6fc; }
        .flow-value-preview { margin: 0; max-width: 520px; max-height: 220px; overflow: auto; white-space: pre-wrap; overflow-wrap: anywhere; color: #c9d1d9; font-size: 0.76rem; line-height: 1.4; }
        .flow-value-modal[hidden] { display: none; }
        .flow-value-modal { position: fixed; inset: 0; z-index: 1000; display: grid; place-items: center; padding: 24px; }
        .flow-value-modal-backdrop { position: absolute; inset: 0; background: rgba(1, 4, 9, 0.76); }
        .flow-value-modal-panel { position: relative; width: min(920px, 100%); max-height: min(760px, calc(100vh - 48px)); display: flex; flex-direction: column; gap: 12px; background: #161b22; border: 1px solid #30363d; border-radius: 8px; box-shadow: 0 18px 60px rgba(0, 0, 0, 0.44); padding: 16px; }
        .flow-value-modal-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; }
        .flow-value-modal-header .section-title { margin-bottom: 4px; }
        .flow-value-modal-ref { color: #8b949e; font-size: 0.76rem; overflow-wrap: anywhere; }
        .flow-value-modal-close { height: 32px; border: 1px solid #30363d; background: #21262d; color: #f0f6fc; border-radius: 6px; padding: 0 12px; font-size: 0.78rem; cursor: pointer; }
        .flow-value-modal-close:hover { background: #30363d; }
        .flow-value-modal-body { flex: 1; min-height: 220px; max-height: 560px; overflow: auto; margin: 0; padding: 12px; border: 1px solid #30363d; border-radius: 6px; background: #0d1117; color: #c9d1d9; white-space: pre-wrap; overflow-wrap: anywhere; font-size: 0.8rem; line-height: 1.45; }
        .flow-value-modal-actions { display: flex; align-items: center; gap: 10px; }
        .flow-section-note { color: #8b949e; font-size: 0.78rem; margin: -4px 0 10px; }
        .flow-lineage-map { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; margin-bottom: 16px; }
        .flow-lineage-node { display: grid; gap: 4px; min-width: 0; padding: 10px 12px; border: 1px solid #30363d; border-radius: 8px; background: #161b22; color: #f0f6fc; text-decoration: none; }
        .flow-lineage-node:hover, .flow-lineage-node:focus { border-color: #58a6ff; background: #10243a; }
        .flow-lineage-node-id { font-family: 'SFMono-Regular', Consolas, monospace; overflow-wrap: anywhere; }
        .flow-lineage-node-meta { color: #8b949e; font-size: 0.76rem; }
        .flow-lineage-empty { color: #8b949e; background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px 16px; }

        /* Compact table */
        table { width: 100%; border-collapse: collapse; background: #161b22; border: 1px solid #30363d; border-radius: 6px; overflow: hidden; font-size: 0.82rem; }
        th { background: #21262d; color: #8b949e; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 6px 12px; text-align: left; }
        td { padding: 6px 12px; border-top: 1px solid #21262d; }
        tr:hover td { background: #1c2128; }

        /* Status colors */
        .c-green { color: #3fb950; }
        .c-yellow { color: #d29922; }
        .c-red { color: #f85149; }
        .c-muted { color: #8b949e; }

        /* Badges */
        .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.7rem; font-weight: 600; }
        .badge-ok { background: #238636; color: #fff; }
        .badge-warning { background: #9e6a03; color: #fff; }
        .badge-pressure { background: #da3633; color: #fff; }
        .badge-reject { background: #8b1a1a; color: #fff; }
        .badge-merging { background: #1f6feb; color: #fff; }
        .badge-idle { background: #30363d; color: #c9d1d9; }

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
        .sidebar .nav-section { font-size: 0.65rem; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; padding: 16px 16px 4px; }
        .main-content { flex: 1; min-width: 0; }

        /* Sub-page header (still used inside main-content on sub-pages) */
        .subpage-header { display: flex; align-items: center; gap: 16px; padding: 12px 20px; background: #0d1117; border-bottom: 1px solid #30363d; }
        .subpage-title { font-size: 1.1rem; font-weight: 700; color: #f0f6fc; }

        /* Footer */
        .footer { position: fixed; bottom: 0; left: 0; right: 0; background: #0d1117; border-top: 1px solid #21262d; padding: 6px 20px; font-size: 0.7rem; color: #8b949e; display: flex; justify-content: space-between; flex-wrap: wrap; gap: 8px; }

        /* Tooltip */
        .info-icon { position: relative; display: inline-block; width: 14px; height: 14px; border-radius: 50%; background: #30363d; color: #8b949e; font-size: 10px; text-align: center; line-height: 14px; cursor: help; margin-left: 4px; vertical-align: middle; outline: none; }
        .info-icon:hover,
        .info-icon:focus { background: #58a6ff; color: #0d1117; }
        .info-icon::after { content: attr(data-tooltip); position: absolute; left: 50%; bottom: calc(100% + 8px); transform: translateX(-50%); z-index: 50; width: max-content; min-width: 180px; max-width: 280px; padding: 8px 10px; border-radius: 6px; border: 1px solid #30363d; background: #0d1117; color: #f0f6fc; box-shadow: 0 8px 24px rgba(1, 4, 9, 0.45); font-size: 0.72rem; line-height: 1.35; font-weight: 400; white-space: normal; text-align: left; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
        .info-icon::before { content: ""; position: absolute; left: 50%; bottom: calc(100% + 3px); transform: translateX(-50%) rotate(45deg); z-index: 51; width: 8px; height: 8px; background: #0d1117; border-right: 1px solid #30363d; border-bottom: 1px solid #30363d; visibility: hidden; opacity: 0; pointer-events: none; transition: opacity 120ms ease, visibility 120ms ease; }
        .info-icon:hover::after,
        .info-icon:focus::after,
        .info-icon:hover::before,
        .info-icon:focus::before { visibility: visible; opacity: 1; }

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
          .flow-card-wide { grid-column: span 1; }
          .flow-nav-row { align-items: stretch; }
          .flow-tabs, .flow-search, .flow-filter-form, .flow-policy-actions { width: 100%; }
          .flow-search, .flow-filter-form { align-items: stretch; }
          .flow-filter-form label, .flow-filter-form select, .flow-filter-form input, .flow-filter-form button, .flow-search-input, .flow-search-button { width: 100%; max-width: none; }
          table { display: block; overflow-x: auto; white-space: nowrap; }
        }
      </style>
      #{dashboard_live_script()}
    </head>
    """
  end

  @spec render_live_component(binary(), binary()) :: binary()
  defp render_live_component(name, html) do
    ~s(<div data-live-component="#{escape_attr(name)}">#{html}</div>)
  end

  @spec dashboard_live_script() :: binary()
  defp dashboard_live_script do
    """
      <script id="dashboard-live.js">
        (function () {
          function onReady(fn) {
            if (document.readyState === "loading") {
              document.addEventListener("DOMContentLoaded", fn, { once: true });
            } else {
              fn();
            }
          }

          function findComponent(name) {
            var nodes = document.querySelectorAll("[data-live-component]");
            for (var i = 0; i < nodes.length; i += 1) {
              if (nodes[i].getAttribute("data-live-component") === name) {
                return nodes[i];
              }
            }
            return null;
          }

          function dashboardInteractionPaused() {
            var modal = document.getElementById("flow-value-modal");
            if (modal && !modal.hidden) { return true; }

            var active = document.activeElement;
            if (!active || !active.closest) { return false; }
            return !!active.closest("input, textarea, select, button, [data-dashboard-live-pause]");
          }

          function patchComponents(components) {
            if (!components || dashboardInteractionPaused()) { return; }
            Object.keys(components).forEach(function (name) {
              var target = findComponent(name);
              var nextHtml = components[name];
              if (!target || typeof nextHtml !== "string") { return; }
              if (target.innerHTML !== nextHtml) {
                target.innerHTML = nextHtml;
              }
            });
          }

          function setupFlowValueInspector() {
            var modal = document.getElementById("flow-value-modal");
            if (!modal || modal.dataset.bound === "1") { return; }
            modal.dataset.bound = "1";

            var refNode = document.getElementById("flow-value-modal-ref");
            var bodyNode = document.getElementById("flow-value-modal-body");
            var copyButton = document.getElementById("flow-value-modal-copy");
            var copyStatus = document.getElementById("flow-value-modal-copy-status");

            function setCopyStatus(text) {
              if (copyStatus) { copyStatus.textContent = text || ""; }
            }

            function closeModal() {
              modal.hidden = true;
              setCopyStatus("");
            }

            function fallbackCopy(text) {
              var textarea = document.createElement("textarea");
              textarea.value = text;
              textarea.setAttribute("readonly", "readonly");
              textarea.style.position = "fixed";
              textarea.style.left = "-9999px";
              document.body.appendChild(textarea);
              textarea.select();
              try {
                document.execCommand("copy");
                setCopyStatus("Copied");
              } catch (_error) {
                setCopyStatus("Copy failed");
              } finally {
                document.body.removeChild(textarea);
              }
            }

            function copyValue() {
              var text = bodyNode ? bodyNode.textContent : "";
              if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(text)
                  .then(function () { setCopyStatus("Copied"); })
                  .catch(function () { fallbackCopy(text); });
              } else {
                fallbackCopy(text);
              }
            }

            function openFromRow(row, link) {
              var preview = row ? row.querySelector("[data-flow-value-preview]") : null;
              var ref = link.getAttribute("data-flow-value-ref") || (row && row.getAttribute("data-flow-value-ref")) || link.getAttribute("title") || "";
              var label = link.getAttribute("data-flow-value-label") || (row && row.getAttribute("data-flow-value-label")) || link.textContent || "value";
              var value = preview ? preview.textContent : "Value is not loaded on this page.";

              if (refNode) { refNode.textContent = label + " · " + ref; }
              if (bodyNode) { bodyNode.textContent = value; }
              setCopyStatus("");
              modal.hidden = false;
              if (copyButton) { copyButton.focus(); }
              return true;
            }

            function flowValueAnchorFromHref(link) {
              var href = link.getAttribute("href") || "";
              if (href.charAt(0) === "#") { return href.slice(1); }

              try {
                var url = new URL(href, window.location.href);
                if (url.pathname !== window.location.pathname || !url.hash) { return ""; }
                return decodeURIComponent(url.hash.slice(1));
              } catch (_error) {
                return "";
              }
            }

            function flowValueRefFromAnchor(anchor) {
              if (!anchor || anchor.indexOf("flow-value-") !== 0) { return ""; }

              try {
                var encoded = anchor.slice("flow-value-".length).replace(/-/g, "+").replace(/_/g, "/");
                while (encoded.length % 4 !== 0) { encoded += "="; }
                var binary = atob(encoded);
                var escaped = "";
                for (var i = 0; i < binary.length; i += 1) {
                  escaped += "%" + ("00" + binary.charCodeAt(i).toString(16)).slice(-2);
                }
                return decodeURIComponent(escaped);
              } catch (_error) {
                return "";
              }
            }

            function flowValueRequestUrl(ref, link) {
              var sourceUrl;

              try {
                var href = link && link.getAttribute ? link.getAttribute("href") : "";
                sourceUrl = new URL(href || window.location.href, window.location.href);
              } catch (_error) {
                sourceUrl = new URL(window.location.href);
              }

              var match = sourceUrl.pathname.match(/^\\/dashboard\\/flow\\/(.+)$/);
              if (!match) { return ""; }

              var params = new URLSearchParams();
              params.set("flow", decodeURIComponent(match[1]));
              params.set("ref", ref);

              var partition = sourceUrl.searchParams.get("partition_key");
              if (!partition) {
                partition = new URLSearchParams(window.location.search).get("partition_key");
              }
              if (partition) { params.set("partition_key", partition); }

              return "/dashboard/api/flow/value?" + params.toString();
            }

            function openFromRef(ref, label, link) {
              var url = flowValueRequestUrl(ref, link);
              if (!ref || !url) { return false; }

              if (refNode) { refNode.textContent = (label || "value") + " · " + ref; }
              if (bodyNode) { bodyNode.textContent = "Loading value..."; }
              setCopyStatus("");
              modal.hidden = false;
              if (copyButton) { copyButton.focus(); }

              fetch(url, {
                cache: "no-store",
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (!response.ok) { throw new Error("value request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  if (!payload || payload.status !== "ok") {
                    throw new Error((payload && payload.error) || "value unavailable");
                  }
                  if (bodyNode) { bodyNode.textContent = payload.value || ""; }
                })
                .catch(function (error) {
                  if (bodyNode) { bodyNode.textContent = error.message || "Value unavailable."; }
                });

              return true;
            }

            function findValueLinkForAnchor(anchor) {
              var links = document.querySelectorAll(".flow-value-ref-link");
              for (var i = 0; i < links.length; i += 1) {
                if (flowValueAnchorFromHref(links[i]) === anchor) { return links[i]; }
              }
              return null;
            }

            function openFromLink(link) {
              var anchor = flowValueAnchorFromHref(link);
              if (!anchor) { return false; }

              var row = document.getElementById(anchor);
              if (row) { return openFromRow(row, link); }

              return openFromRef(
                link.getAttribute("data-flow-value-ref") || flowValueRefFromAnchor(anchor),
                link.getAttribute("data-flow-value-label") || link.textContent || "value",
                link
              );
            }

            function openFromHash() {
              var hash = window.location.hash || "";
              if (hash.length < 2) { return; }

              var anchor = decodeURIComponent(hash.slice(1));
              var row = document.getElementById(anchor);
              var link = findValueLinkForAnchor(anchor);

              if (!row || !row.hasAttribute("data-flow-value-ref")) {
                var ref = link ? link.getAttribute("data-flow-value-ref") : flowValueRefFromAnchor(anchor);
                if (ref) {
                  openFromRef(
                    ref,
                    (link && (link.getAttribute("data-flow-value-label") || link.textContent)) || "value",
                    link
                  );
                }
                return;
              }

              if (!link) {
                link = {
                  getAttribute: function (name) {
                    if (name === "data-flow-value-ref" || name === "title") { return row.getAttribute("data-flow-value-ref"); }
                    if (name === "data-flow-value-label") { return row.getAttribute("data-flow-value-label"); }
                    return "";
                  },
                  textContent: row.getAttribute("data-flow-value-label") || "value"
                };
              }

              openFromRow(row, link);
            }

            document.addEventListener("click", function (event) {
              var closeTarget = event.target.closest("[data-flow-value-modal-close]");
              if (closeTarget) {
                event.preventDefault();
                closeModal();
                return;
              }

              var link = event.target.closest(".flow-value-ref-link");
              if (link && openFromLink(link)) {
                event.preventDefault();
              }
            });

            document.addEventListener("keydown", function (event) {
              if (event.key === "Escape" && !modal.hidden) {
                event.preventDefault();
                closeModal();
              }
            });

            if (copyButton) {
              copyButton.addEventListener("click", copyValue);
            }

            window.addEventListener("hashchange", openFromHash);
            openFromHash();
          }

          onReady(function () {
            setupFlowValueInspector();

            var root = document.body;
            if (!root || !root.dataset || !root.dataset.dashboardLiveUrl) { return; }

            var url = root.dataset.dashboardLiveUrl;
            var intervalMs = parseInt(root.dataset.dashboardLiveIntervalMs || "2000", 10);
            if (!Number.isFinite(intervalMs) || intervalMs < 500) { intervalMs = 2000; }

            var inFlight = false;

            function tick() {
              if (document.hidden || inFlight) { return; }
              inFlight = true;

              fetch(url, {
                cache: "no-store",
                headers: { "accept": "application/json" }
              })
                .then(function (response) {
                  if (!response.ok) { throw new Error("dashboard live request failed"); }
                  return response.json();
                })
                .then(function (payload) {
                  patchComponents(payload.components);
                  root.dataset.dashboardLiveLastUpdateMs = String(payload.generated_at_ms || Date.now());
                })
                .catch(function () {
                  root.dataset.dashboardLiveError = "1";
                })
                .finally(function () {
                  inFlight = false;
                });
            }

            window.setInterval(tick, intervalMs);
          });
        }());
      </script>
    """
  end

  @spec render_subpage_header(String.t()) :: binary()
  defp render_subpage_header(title) do
    """
    <div class="subpage-header">
      <h1 class="subpage-title">#{escape(title)}</h1>
    </div>
    """
  end

  @spec render_page_intro(binary(), binary()) :: binary()
  defp render_page_intro(title, body) do
    """
    <section class="page-intro" aria-label="#{escape_attr(title)} page purpose">
      <div class="page-intro-title">#{escape(title)}</div>
      <p>#{escape(body)}</p>
    </section>
    """
  end

  @spec render_kv_subnav(binary()) :: binary()
  defp render_kv_subnav(active) do
    items = [
      {"keyspace", "/dashboard/keyspace", "Keyspace", "Find keys and inspect metadata"},
      {"reads", "/dashboard/reads", "Read Path", "Hot-cache and cold-read health"},
      {"commands", "/dashboard/commands", "Commands", "Traffic, slowlog, and command groups"},
      {"prefixes", "/dashboard/prefixes", "Prefixes", "Sampled prefix distribution"},
      {"storage", "/dashboard/storage", "Storage", "Disk files, segments, and shard usage"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label, title} ->
        active_class = if key == active, do: " active", else: ""
        current = if key == active, do: ~s( aria-current="page"), else: ""

        ~s(<a class="flow-tab#{active_class}" href="#{href}"#{current} title="#{escape_attr(title)}">#{escape(label)}</a>)
      end)

    ~s(<nav class="flow-tabs" aria-label="KV dashboard sections">#{links}</nav>)
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
    flow_active = Map.get(data.flow_summary, :active, 0)
    flow_badge = if flow_active > 0, do: "#{flow_active}", else: ""

    sidebar_html(active, %{
      "slowlog" => slowlog_badge,
      "merge" => merge_badge,
      "flow" => flow_badge,
      "config" => config_badge,
      "clients" => conns_badge,
      "storage" => storage_badge,
      "keyspace" => "",
      "reads" => "",
      "commands" => ""
    })
  end

  # Sidebar without live data (used on sub-pages to avoid expensive data collection)
  @spec render_sidebar_static(String.t()) :: binary()
  defp render_sidebar_static(active) do
    sidebar_html(active, %{
      "slowlog" => "",
      "merge" => "",
      "flow" => "",
      "config" => "",
      "clients" => "",
      "storage" => "",
      "keyspace" => "",
      "reads" => "",
      "commands" => ""
    })
  end

  defp sidebar_html(active, badges) do
    items = [
      {"overview", "/dashboard", "Overview"},
      {"flow", "/dashboard/flow", "FerricFlow"},
      {"keyspace", "/dashboard/keyspace", "Keyspace"},
      {"reads", "/dashboard/reads", "Read Path"},
      {"commands", "/dashboard/commands", "Commands"},
      {"slowlog", "/dashboard/slowlog", "Slow Log"},
      {"merge", "/dashboard/merge", "Merge Status"},
      {"storage", "/dashboard/storage", "Storage"},
      {"raft", "/dashboard/raft", "Consensus"},
      {"config", "/dashboard/config", "Config"},
      {"clients", "/dashboard/clients", "Clients"},
      {"prefixes", "/dashboard/prefixes", "Key Prefixes"}
    ]

    links =
      Enum.map_join(items, "\n", fn {key, href, label} ->
        active_class = if key == active, do: " active", else: ""
        current_attr = if key == active, do: ~s( aria-current="page"), else: ""
        badge_val = Map.get(badges, key, "")

        badge_html =
          if badge_val != "" and badge_val != nil do
            ~s(<span class="nav-badge">#{escape(to_string(badge_val))}</span>)
          else
            ""
          end

        ~s(<a class="#{active_class}" href="#{href}"#{current_attr}><span class="nav-label">#{escape(label)}</span>#{badge_html}</a>)
      end)

    """
    <nav class="sidebar" aria-label="Dashboard sections">
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
  # 1:1 = every request, no sampling
  defp sampled_tag(1), do: ""

  defp sampled_tag(rate) do
    ~s(<span class="sampled-tag" title="Estimated from 1:#{rate} sampling">~1:#{rate}</span>)
  end

  @spec hotcold_has_samples?(map()) :: boolean()
  defp hotcold_has_samples?(hotcold) do
    case Map.get(hotcold, :total_lookups, 0) do
      value when is_number(value) -> value > 0
      _ -> false
    end
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

  @spec format_duration_us(non_neg_integer()) :: binary()
  defp format_duration_us(duration_us) do
    "#{Float.round(duration_us / 1000.0, 2)} ms"
  end

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

  @spec format_timestamp_ms_or_dash(integer() | nil) :: binary()
  defp format_timestamp_ms_or_dash(timestamp_ms)
       when is_integer(timestamp_ms) and timestamp_ms > 0 do
    format_timestamp_ms(timestamp_ms)
  rescue
    _ -> "-"
  end

  defp format_timestamp_ms_or_dash(_timestamp_ms), do: "-"

  @spec format_timeline_timestamp_ms(integer() | nil) :: binary()
  defp format_timeline_timestamp_ms(timestamp_ms)
       when is_integer(timestamp_ms) and timestamp_ms > 0 do
    timestamp_ms
    |> DateTime.from_unix!(:millisecond)
    |> Calendar.strftime("%H:%M:%S")
  rescue
    _ -> "-"
  end

  defp format_timeline_timestamp_ms(_timestamp_ms), do: "-"

  @spec format_duration_ms(non_neg_integer()) :: binary()
  defp format_duration_ms(ms) when ms >= 86_400_000 do
    "#{Float.round(ms / 86_400_000, 1)}d"
  end

  defp format_duration_ms(ms) when ms >= 3_600_000 do
    "#{Float.round(ms / 3_600_000, 1)}h"
  end

  defp format_duration_ms(ms) when ms >= 60_000 do
    "#{Float.round(ms / 60_000, 1)}m"
  end

  defp format_duration_ms(ms) when ms >= 1_000 do
    "#{Float.round(ms / 1_000, 1)}s"
  end

  defp format_duration_ms(ms), do: "#{ms}ms"

  @spec escape(binary()) :: binary()
  defp escape(str) do
    str
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
  end

  @spec escape_attr(binary()) :: binary()
  defp escape_attr(str), do: escape(str)

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
