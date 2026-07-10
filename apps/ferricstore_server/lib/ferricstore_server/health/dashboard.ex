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
  Liveness and readiness probes use the independent
  `FerricstoreServer.Health.ProbeEndpoint` listener so slow dashboard requests
  cannot exhaust probe connections.
  """

  alias FerricstoreServer.Health.Dashboard.Data.{KV, Messaging, Operational, Security}
  alias FerricstoreServer.Health.Dashboard.LivePayload
  alias FerricstoreServer.Health.Dashboard.Templates
  alias FerricstoreServer.Health.Dashboard.Types

  alias FerricstoreServer.Health.Dashboard.Flow.{
    Browse,
    Detail,
    Governance,
    PolicyRetention,
    Projection,
    Query,
    Recovery,
    Schedules
  }

  require Logger

  import FerricstoreServer.Health.Dashboard.DoctorSupport
  import FerricstoreServer.Health.Dashboard.Flow.Sample

  # ---------------------------------------------------------------------------
  # Public API -- Main Dashboard
  # ---------------------------------------------------------------------------

  @doc """
  Collects all dashboard data from running subsystems.
  """
  @spec collect() :: Types.dashboard_data()
  def collect do
    Operational.collect_dashboard(collect_flow_summary())
  end

  @doc """
  Renders the main dashboard page as a complete HTML document.
  """
  @spec render(Types.dashboard_data()) :: binary()
  def render(data) do
    render_template(Templates.overview(%{data: data}))
  end

  @doc """
  Builds the JSON payload used by the live overview dashboard shell.

  Values are HTML component fragments keyed by stable component names. The
  browser patches only components whose HTML changed, preserving scroll,
  selected text, and browser paint state.
  """
  @spec live_overview_payload(Types.dashboard_data()) :: map()
  def live_overview_payload(data), do: LivePayload.overview_payload(data)

  # ---------------------------------------------------------------------------
  # Public API -- Slow Log Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the slow log sub-page.
  """
  @spec collect_slowlog_page() :: %{slowlog: [Types.slowlog_entry()]}
  def collect_slowlog_page do
    Operational.collect_slowlog_page()
  end

  @doc """
  Renders the slow log sub-page.
  """
  @spec render_slowlog_page(%{slowlog: [Types.slowlog_entry()]}) :: binary()
  def render_slowlog_page(data) do
    render_template(Templates.slowlog(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Merge Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the merge status sub-page.
  """
  @spec collect_merge_page() :: %{merge: [Types.merge_status()]}
  def collect_merge_page do
    Operational.collect_merge_page()
  end

  @doc """
  Renders the merge status sub-page.
  """
  @spec render_merge_page(%{merge: [Types.merge_status()]}) :: binary()
  def render_merge_page(data) do
    render_template(Templates.merge(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Namespace Config Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the namespace config sub-page.
  """
  @spec collect_config_page() :: Types.config_page_data()
  def collect_config_page do
    Operational.collect_config_page()
  end

  @doc """
  Renders the namespace config sub-page (no auto-refresh).
  """
  @spec render_config_page(Types.config_page_data() | map()) :: binary()
  def render_config_page(data) do
    render_template(Templates.config(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Management Capabilities Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects the read-only management capability contract exposed by this server.
  """
  @spec collect_capabilities_page() :: map()
  def collect_capabilities_page do
    %{
      capabilities: FerricStore.ManagementCapabilities.capabilities(),
      command_reference: management_capabilities_command_reference(),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @doc """
  Renders the management capabilities sub-page.
  """
  @spec render_capabilities_page(map()) :: binary()
  def render_capabilities_page(data) do
    render_template(Templates.capabilities(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Security / ACL Diagnostics Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects OSS ACL diagnostics and effective access checks for operators.
  """
  @spec collect_security_page(keyword() | map()) :: map()
  def collect_security_page(opts \\ []) do
    Security.collect_page(opts)
  end

  @doc """
  Renders the read-only security diagnostics sub-page.
  """
  @spec render_security_page(map()) :: binary()
  def render_security_page(data) do
    render_template(Templates.security(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Raft Consensus Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the Raft consensus sub-page.
  """
  @spec collect_raft_page() :: %{
          raft_shards: [Types.raft_shard_data()],
          cluster: Types.cluster_data()
        }
  def collect_raft_page do
    Operational.collect_raft_page()
  end

  @doc """
  Renders the Raft consensus sub-page.
  """
  @spec render_raft_page(%{
          raft_shards: [Types.raft_shard_data()],
          cluster: Types.cluster_data()
        }) :: binary()
  def render_raft_page(data) do
    render_template(Templates.raft(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Client List Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects data for the client list sub-page.
  """
  @spec collect_clients_page() :: %{
          clients: [Types.client_data()],
          connections: Types.connections_data()
        }
  def collect_clients_page do
    Operational.collect_clients_page()
  end

  @doc """
  Renders the client list sub-page.
  """
  @spec render_clients_page(%{
          clients: [Types.client_data()],
          connections: Types.connections_data()
        }) ::
          binary()
  def render_clients_page(data) do
    render_template(Templates.clients(%{data: data}))
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
    render_template(Templates.storage(%{data: data}))
  end

  # ---------------------------------------------------------------------------
  # Public API -- Doctor Admin Sub-page
  # ---------------------------------------------------------------------------

  @doc """
  Collects doctor diagnostics through the same command handler exposed to
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
    render_template(Templates.doctor(%{data: data}))
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
    render_template(Templates.prefixes(%{data: data}))
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
    render_template(Templates.keyspace(%{data: data}))
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
    render_template(Templates.commands(%{data: data}))
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
    render_template(Templates.reads(%{data: data}))
  end

  @doc """
  Collects metadata-only stream append and mutation activity.
  """
  @spec collect_streams_page(keyword() | map()) :: map()
  def collect_streams_page(opts \\ []) do
    Messaging.collect_streams_page(opts)
  end

  @doc """
  Renders the stream activity page.
  """
  @spec render_streams_page(map()) :: binary()
  def render_streams_page(data) do
    render_template(Templates.streams(%{data: data}))
  end

  @doc """
  Collects metadata-only Pub/Sub subscription and publish activity.
  """
  @spec collect_pubsub_page(keyword() | map()) :: map()
  def collect_pubsub_page(opts \\ []) do
    Messaging.collect_pubsub_page(opts)
  end

  @doc """
  Renders the Pub/Sub activity page.
  """
  @spec render_pubsub_page(map()) :: binary()
  def render_pubsub_page(data) do
    render_template(Templates.pubsub(%{data: data}))
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
    render_template(Templates.flow(%{data: data}))
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
  Applies the Flow detail signal form.
  """
  @spec apply_flow_signal_form(map()) ::
          {:ok, binary(), binary() | nil} | {:error, binary()}
  def apply_flow_signal_form(params), do: Detail.apply_signal_form(params)

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
    render_template(Templates.flow_policies(%{data: data}))
  end

  @doc """
  Collects FerricFlow retention cleanup status for the maintenance page.

  The dashboard preview is intentionally sampled. The actual cleanup command is
  still the source of truth and runs through the same durable Flow command path
  as a protocol `FLOW.RETENTION_CLEANUP`.
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
    render_template(Templates.flow_retention(%{data: data}))
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
    render_template(Templates.flow_states(%{data: data}))
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
    render_template(Templates.flow_workers(%{data: data}))
  end

  @doc "Collects data for the Flow due and scheduled work page."
  @spec collect_flow_due_page(keyword()) :: map()
  def collect_flow_due_page(opts \\ []), do: Browse.collect_due_page(opts)

  @doc "Renders the Flow due and scheduled work page."
  @spec render_flow_due_page(map()) :: binary()
  def render_flow_due_page(data) do
    render_template(Templates.flow_due(%{data: data}))
  end

  @doc "Collects durable FerricFlow schedules for operators."
  @spec collect_flow_schedules_page(keyword()) :: map()
  def collect_flow_schedules_page(opts \\ []), do: Schedules.collect_page(opts)

  @doc false
  @spec flow_schedules_opts_from_query(binary()) :: keyword()
  def flow_schedules_opts_from_query(query), do: Schedules.opts_from_query(query)

  @doc "Renders the durable FerricFlow schedules page."
  @spec render_flow_schedules_page(map()) :: binary()
  def render_flow_schedules_page(data) do
    render_template(Templates.flow_schedules(%{data: data}))
  end

  @doc """
  Collects governance approval, budget, and limit records for the Flow admin page.
  """
  @spec collect_flow_governance_page(keyword()) :: map()
  def collect_flow_governance_page(opts \\ []), do: Governance.collect_page(opts)

  @doc false
  @spec flow_governance_opts_from_query(binary()) :: keyword()
  def flow_governance_opts_from_query(query), do: Governance.opts_from_query(query)

  @doc false
  @spec apply_flow_governance_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_flow_governance_form(params), do: Governance.apply_form(params)

  @doc false
  @spec flow_governance_form_command(map()) :: binary()
  def flow_governance_form_command(params), do: Governance.form_command(params)

  @doc """
  Renders the FerricFlow governance page.
  """
  @spec render_flow_governance_page(map()) :: binary()
  def render_flow_governance_page(data) do
    render_template(Templates.flow_governance(%{data: data}))
  end

  @doc false
  @spec apply_flow_schedule_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_flow_schedule_form(params), do: Schedules.apply_form(params)

  @doc false
  @spec flow_schedule_form_command(map()) :: binary()
  def flow_schedule_form_command(params), do: Schedules.form_command(params)

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
    render_template(Templates.flow_failures(%{data: data}))
  end

  @doc "Collects Flow lineage records by parent, root, or correlation id."
  @spec collect_flow_lineage_page(keyword()) :: map()
  def collect_flow_lineage_page(opts \\ []) when is_list(opts),
    do: Query.collect_lineage_page(opts)

  @doc false
  @spec flow_lineage_opts_from_query(binary()) :: keyword()
  def flow_lineage_opts_from_query(query), do: Query.lineage_opts_from_query(query)

  @doc "Renders the Flow lineage page."
  @spec render_flow_lineage_page(map()) :: binary()
  def render_flow_lineage_page(data) do
    render_template(Templates.flow_lineage(%{data: data}))
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
    render_template(Templates.flow_query(%{data: data}))
  end

  @doc "Collects recent Flow signal events from a bounded Flow sample."
  @spec collect_flow_signals_page(keyword()) :: map()
  def collect_flow_signals_page(opts \\ []) when is_list(opts),
    do: Query.collect_signals_page(opts)

  @doc false
  @spec flow_signals_opts_from_query(binary()) :: keyword()
  def flow_signals_opts_from_query(query), do: Query.signals_opts_from_query(query)

  @doc "Renders the Flow signals page."
  @spec render_flow_signals_page(map()) :: binary()
  def render_flow_signals_page(data) do
    render_template(Templates.flow_signals(%{data: data}))
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

  def collect_flow_detail_page(id, opts) when is_binary(id) and is_list(opts),
    do: Detail.collect_page(id, opts)

  @doc """
  Renders the Flow detail page.
  """
  @spec render_flow_detail_page(map()) :: binary()
  def render_flow_detail_page(data) do
    render_template(Templates.flow_detail(%{data: data}))
  end

  @spec render_template(binary()) :: binary()
  defp render_template(html), do: String.trim_leading(html)

  defp management_capabilities_command_reference do
    [
      %{
        command: "FERRICSTORE.CAPABILITIES",
        scope: "server",
        mutability: "read-only",
        notes:
          "Returns supported control-plane features so SDK callers can enable only safe actions."
      },
      %{
        command: "FERRICSTORE.TELEMETRY CLUSTER",
        scope: "cluster",
        mutability: "read-only",
        notes: "Returns health and capability metadata without payload search."
      },
      %{
        command: "FERRICSTORE.TELEMETRY NAMESPACE <prefix>",
        scope: "namespace",
        mutability: "read-only",
        notes: "Returns safe namespace usage counters when the server supports them."
      },
      %{
        command: "FERRICSTORE.TELEMETRY FLOW.QUERY",
        scope: "flow",
        mutability: "read-only",
        notes: "Returns Flow metadata and attributes only."
      },
      %{
        command: "ACL SETUSER | DELUSER | GETUSER | LIST | SAVE",
        scope: "access control",
        mutability: "conditional",
        notes: "Available only when acl_management is supported by the deployed server."
      },
      %{
        command: "FERRICSTORE.NAMESPACE ENSURE | GET | LIST | DELETE",
        scope: "namespace",
        mutability: "conditional",
        notes: "Available only when namespace_management is supported by the deployed server."
      },
      %{
        command: "FERRICSTORE.QUOTA SET | GET | USAGE",
        scope: "namespace",
        mutability: "conditional",
        notes: "Available only when quota_management is supported by the deployed server."
      }
    ]
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Main Dashboard
  # ---------------------------------------------------------------------------
end
