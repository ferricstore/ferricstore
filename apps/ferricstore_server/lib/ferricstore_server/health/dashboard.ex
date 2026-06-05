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

  alias FerricstoreServer.Health.Dashboard.Data.{KV, Operational}
  alias FerricstoreServer.Health.Dashboard.LivePayload
  alias FerricstoreServer.Health.Dashboard.Flow.{Browse, Detail, PolicyRetention, Projection, Query, Recovery}

  require EEx
  require Logger

  import FerricstoreServer.Health.Dashboard.DoctorSupport
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.Layout
  import FerricstoreServer.Health.Dashboard.Render.Admin
  import FerricstoreServer.Health.Dashboard.Render.DoctorPages
  import FerricstoreServer.Health.Dashboard.Render.FlowCharts
  import FerricstoreServer.Health.Dashboard.Render.FlowComponents
  import FerricstoreServer.Health.Dashboard.Render.FlowDetail
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, except: [flow_signal_rows: 2]
  import FerricstoreServer.Health.Dashboard.Render.FlowOverview
  import FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy, except: [flow_policy_editor_data: 1]

  import FerricstoreServer.Health.Dashboard.Render.FlowTables,
    except: [default_flow_projection_health: 0]

  import FerricstoreServer.Health.Dashboard.Render.KVPages, except: [kv_command_groups: 0]
  import FerricstoreServer.Health.Dashboard.Render.Overview
  import FerricstoreServer.Health.Dashboard.Render.Prefixes

  # ---------------------------------------------------------------------------

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
    :template_doctor,
    Path.join(@templates_dir, "doctor.html.eex"),
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
  @typedoc "Dashboard data map containing all sections."
  @type dashboard_data :: %{
          overview: overview_data(),
          shards: [shard_data()],
          hotcold: hotcold_data(),
          memory: memory_data(),
          connections: connections_data(),
          slowlog: [slowlog_entry()],
          merge: [merge_status()],
          namespace_config: [Ferricstore.NamespaceConfig.ns_entry()],
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
          top_prefixes: [Ferricstore.Stats.hotness_entry()]
        }

  @typedoc "Memory pressure data."
  @type memory_data :: %{
          total_bytes: non_neg_integer(),
          max_bytes: non_neg_integer(),
          ratio: float(),
          pressure_level: Ferricstore.MemoryGuard.pressure_level(),
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
          namespace_config: [Ferricstore.NamespaceConfig.ns_entry()],
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
    Operational.collect_dashboard(collect_flow_summary())
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
  def live_overview_payload(data), do: LivePayload.overview_payload(data)

  # ---------------------------------------------------------------------------
  # Public API -- Slow Log Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the slow log sub-page.
  """
  @spec collect_slowlog_page() :: %{slowlog: [slowlog_entry()]}
  def collect_slowlog_page do
    Operational.collect_slowlog_page()
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
    Operational.collect_merge_page()
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
    Operational.collect_config_page()
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
    Operational.collect_raft_page()
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
    Operational.collect_clients_page()
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
    Operational.collect_storage_page()
  end

  @doc """
  Renders the storage sub-page.
  """
  @spec render_storage_page(map()) :: binary()
  def render_storage_page(data) do
    render_template(template_storage(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Doctor Admin Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects doctor diagnostics through the same Redis command handler exposed to
  administrators. The dashboard must not bypass command/ACL semantics for
  repair-oriented tools.
  """
  @spec collect_doctor_page(map() | keyword()) :: map()
  def collect_doctor_page(opts \\ %{}) do
    check = doctor_command(["CHECK"])
    jobs = doctor_command(["LIST"])

    %{
      check: check,
      jobs: Map.get(jobs, "jobs", []),
      flash: doctor_flash(opts),
      command_reference: doctor_command_reference()
    }
  end

  @doc """
  Renders the doctor admin page.
  """
  @spec render_doctor_page(map()) :: binary()
  def render_doctor_page(data) do
    render_template(template_doctor(%{data: data}))
  end

  @doc false
  def apply_doctor_form(params) when is_map(params) do
    case Map.get(params, "action", "") do
      "start_check" ->
        scope = Map.get(params, "scope", "ALL")
        normalize_doctor_form_result(doctor_command(["START", "CHECK", "SCOPE", scope]))

      "repair_flow_lmdb" ->
        normalize_doctor_form_result(
          doctor_command(["START", "REPAIR", "PROJECTIONS", "SCOPE", "FLOW_LMDB"])
        )

      "cancel" ->
        job_id = params |> Map.get("job_id", "") |> String.trim()

        if job_id == "" do
          {:error, "missing doctor job id"}
        else
          normalize_doctor_form_result(doctor_command(["CANCEL", job_id]))
        end

      _ ->
        {:error, "unknown doctor action"}
    end
  end

  # ---------------------------------------------------------------------------
  # Public API -- Prefixes Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the key prefixes sub-page.
  """
  @spec collect_prefixes_page() :: %{prefixes: [map()], total_sampled: non_neg_integer()}
  def collect_prefixes_page do
    KV.collect_prefixes_page()
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
    KV.collect_keyspace_page(opts)
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
    KV.collect_commands_page()
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
    KV.collect_reads_page()
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
  def collect_flow_page(opts \\ []) when is_list(opts), do: Browse.collect_overview_page(opts)

  @doc false
  @spec flow_opts_from_query(binary()) :: keyword()
  def flow_opts_from_query(query), do: Browse.overview_opts_from_query(query)

  @doc false
  @spec flow_page_filters(map()) :: map()
  def flow_page_filters(data), do: Browse.overview_page_filters(data)

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
    PolicyRetention.collect_policies_page(opts)
  end

  @doc """
  Applies the Flow policy editor form.

  The form posts the full global retry/retention fields. Before writing the
  policy record we rebuild the existing state override list, so changing the
  global defaults from the dashboard does not silently delete per-state policy.
  """
  @spec apply_flow_policy_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_flow_policy_form(params) when is_map(params) do
    PolicyRetention.apply_policy_form(params)
  end

  def apply_flow_policy_form(params), do: PolicyRetention.apply_policy_form(params)

  @doc """
  Applies the Flow detail rewind form.

  Dashboard rewind is event-based on purpose: the form does not accept an
  arbitrary target state, and this handler re-reads the selected history event
  before mutating the Flow. That keeps the screen constrained to states that this
  specific Flow has already reached.
  """
  @spec apply_flow_rewind_form(map()) ::
          {:ok, binary(), binary() | nil} | {:error, binary()}
  def apply_flow_rewind_form(params), do: Detail.apply_rewind_form(params)

  @doc """
  Converts a Flow policy editor query string into a small flash map.
  """
  @spec flow_policy_flash_from_query(binary()) :: map() | nil
  def flow_policy_flash_from_query(query) when is_binary(query) do
    PolicyRetention.policy_flash_from_query(query)
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
    PolicyRetention.collect_retention_page(opts)
  end

  @doc """
  Applies the retention maintenance form.

  Dry-run only builds the sampled preview. Cleanup calls the real Flow cleanup
  command with the supplied limit.
  """
  @spec apply_flow_retention_form(map()) ::
          {:ok, :dry_run, map()} | {:ok, :cleanup, map()} | {:error, binary()}
  def apply_flow_retention_form(params) when is_map(params) do
    PolicyRetention.apply_retention_form(params)
  end

  def apply_flow_retention_form(params), do: PolicyRetention.apply_retention_form(params)

  @doc false
  @spec flow_retention_flash_from_query(binary()) :: map() | nil
  def flow_retention_flash_from_query(query) when is_binary(query) do
    PolicyRetention.retention_flash_from_query(query)
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
  def live_flow_payload(data), do: LivePayload.flow_payload(data)

  @doc """
  Builds a live component payload for dashboard API paths.
  """
  @spec live_payload(binary()) :: {:ok, map()} | :not_found
  @spec live_payload(binary(), keyword() | map()) :: {:ok, map()} | :not_found
  def live_payload(path), do: LivePayload.live_payload(path)
  def live_payload(path, opts), do: LivePayload.live_payload(path, opts)

  @doc false
  @spec flow_detail_opts_from_query(binary()) :: keyword()
  def flow_detail_opts_from_query(query), do: Detail.opts_from_query(query)

  @doc "Collects data for the Flow state-centric page."
  @spec collect_flow_states_page(keyword()) :: map()
  def collect_flow_states_page(opts \\ []), do: Browse.collect_states_page(opts)

  @doc false
  @spec flow_states_opts_from_query(binary()) :: keyword()
  def flow_states_opts_from_query(query), do: Browse.states_opts_from_query(query)

  @doc "Renders the Flow state-centric page."
  @spec render_flow_states_page(map()) :: binary()
  def render_flow_states_page(data) do
    render_template(template_flow_states(%{data: data}))
  end

  @doc false
  @spec flow_states_page_filters(map()) :: map()
  def flow_states_page_filters(data), do: Browse.states_page_filters(data)

  @doc false
  @spec flow_states_page_limit(map()) :: pos_integer()
  def flow_states_page_limit(data), do: Browse.states_page_limit(data)

  @doc "Collects data for the Flow workers and leases page."
  @spec collect_flow_workers_page(keyword()) :: map()
  def collect_flow_workers_page(opts \\ []), do: Browse.collect_workers_page(opts)

  @doc "Renders the Flow workers and leases page."
  @spec render_flow_workers_page(map()) :: binary()
  def render_flow_workers_page(data) do
    render_template(template_flow_workers(%{data: data}))
  end

  @doc "Collects data for the Flow due and scheduled work page."
  @spec collect_flow_due_page(keyword()) :: map()
  def collect_flow_due_page(opts \\ []), do: Browse.collect_due_page(opts)

  @doc "Renders the Flow due and scheduled work page."
  @spec render_flow_due_page(map()) :: binary()
  def render_flow_due_page(data) do
    render_template(template_flow_due(%{data: data}))
  end

  @doc "Collects failed, stuck, and expired-lease Flow work for operator recovery."
  @spec collect_flow_failures_page(keyword()) :: map()
  def collect_flow_failures_page(opts \\ []) when is_list(opts), do: Recovery.collect_page(opts)

  @doc false
  @spec flow_failures_opts_from_query(binary()) :: keyword()
  def flow_failures_opts_from_query(query), do: Recovery.opts_from_query(query)

  @doc false
  @spec flow_failures_flash_from_query(binary()) :: map() | nil
  def flow_failures_flash_from_query(query), do: Recovery.flash_from_query(query)

  @doc false
  @spec flow_failures_page_filters(map()) :: map()
  def flow_failures_page_filters(data), do: Recovery.page_filters(data)

  @doc "Runs the explicit recovery form on the Flow failures page."
  @spec apply_flow_failures_form(map()) :: {:ok, map()} | {:error, binary()}
  def apply_flow_failures_form(params), do: Recovery.apply_form(params)

  @doc "Renders the Flow failures and recovery page."
  @spec render_flow_failures_page(map()) :: binary()
  def render_flow_failures_page(data) do
    render_template(template_flow_failures(%{data: data}))
  end

  @doc "Collects Flow lineage records by parent, root, or correlation id."
  @spec collect_flow_lineage_page(keyword()) :: map()
  def collect_flow_lineage_page(opts \\ []) when is_list(opts), do: Query.collect_lineage_page(opts)

  @doc false
  @spec flow_lineage_opts_from_query(binary()) :: keyword()
  def flow_lineage_opts_from_query(query), do: Query.lineage_opts_from_query(query)

  @doc "Renders the Flow lineage page."
  @spec render_flow_lineage_page(map()) :: binary()
  def render_flow_lineage_page(data) do
    render_template(template_flow_lineage(%{data: data}))
  end

  @doc "Collects a safe Flow query explorer result."
  @spec collect_flow_query_page(keyword()) :: map()
  def collect_flow_query_page(opts \\ []) when is_list(opts), do: Query.collect_query_page(opts)

  @doc false
  @spec flow_query_opts_from_query(binary()) :: keyword()
  def flow_query_opts_from_query(query), do: Query.query_opts_from_query(query)

  @doc "Renders the Flow query explorer page."
  @spec render_flow_query_page(map()) :: binary()
  def render_flow_query_page(data) do
    render_template(template_flow_query(%{data: data}))
  end

  @doc "Collects recent Flow signal events from a bounded Flow sample."
  @spec collect_flow_signals_page(keyword()) :: map()
  def collect_flow_signals_page(opts \\ []) when is_list(opts), do: Query.collect_signals_page(opts)

  @doc false
  @spec flow_signals_opts_from_query(binary()) :: keyword()
  def flow_signals_opts_from_query(query), do: Query.signals_opts_from_query(query)

  @doc "Renders the Flow signals page."
  @spec render_flow_signals_page(map()) :: binary()
  def render_flow_signals_page(data) do
    render_template(template_flow_signals(%{data: data}))
  end

  @doc false
  @spec flow_signals_page_filters(map()) :: map()
  def flow_signals_page_filters(data), do: Query.signals_page_filters(data)

  @doc false
  @spec collect_flow_projection_health() :: map()
  def collect_flow_projection_health do
    Projection.collect_health()
  end

  @doc false
  @spec default_flow_projection_health() :: map()
  def default_flow_projection_health do
    Projection.default_health()
  end

  @doc """
  Collects record and history data for one Flow detail page.
  """
  @spec collect_flow_detail_page(binary(), keyword()) :: map()
  def collect_flow_detail_page(id, opts \\ [])
  def collect_flow_detail_page(id, opts) when is_binary(id) and is_list(opts), do: Detail.collect_page(id, opts)

  @doc """
  Renders the Flow detail page.
  """
  @spec render_flow_detail_page(map()) :: binary()
  def render_flow_detail_page(data) do
    render_template(template_flow_detail(%{data: data}))
  end

  @spec render_template(binary()) :: binary()
  defp render_template(html), do: String.trim_leading(html)

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

  # ---------------------------------------------------------------------------
  # HTML rendering -- Main Dashboard
  # ---------------------------------------------------------------------------


end
