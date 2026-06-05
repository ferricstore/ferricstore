defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryPolicy do

import FerricstoreServer.Health.Dashboard.Format
import FerricstoreServer.Health.Dashboard.FlowRecord
import FerricstoreServer.Health.Dashboard.QueryParams
import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]
import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [flow_detail_path: 2]
import FerricstoreServer.Health.Dashboard.Render.FlowOverview, only: [render_flow_stat_card: 3]
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters
  import FerricstoreServer.Health.Dashboard.Render.FlowTables

@flow_dashboard_sample_limit 400
@flow_dashboard_recent_limit 40
@flow_dashboard_policy_state_preview_limit 6
@flow_dashboard_retention_default_limit 100
@flow_dashboard_retention_max_limit 10_000

  def flow_policy_editor_data(type) do
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

  defp flow_policy_clean_form_value(value) when is_binary(value), do: String.trim(value)
  defp flow_policy_clean_form_value(value), do: value |> to_string() |> String.trim()

  def render_flow_query_kind_help(kind) do
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

  def render_flow_query_type_field(%{type: type} = filters) do
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

  def render_flow_query_state_field(%{state: state} = filters) do
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

  def render_flow_query_id_field(%{kind: kind, id: id} = filters) do
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

  def render_flow_query_partition_field(%{partition_key: partition_key}) do
    """
    <label class="flow-query-field">
      Partition
      <input class="flow-search-input mono" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="optional">
      <span class="flow-field-help">Optional. Use it when the workflow id or index is partition-scoped.</span>
    </label>
    """
  end

  def render_flow_query_time_fields(filters) do
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

  def render_flow_query_direction_field(filters) do
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

  def render_flow_query_dynamic_script do
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

  def flow_query_hidden_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " hidden")

  def flow_query_disabled_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " disabled")

  def flow_query_kinds_attr(kinds), do: kinds |> Enum.join(" ") |> escape_attr()

  def render_flow_type_options(types, selected_type) do
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

  def render_flow_lineage_mode_options(selected_mode) do
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

  def render_flow_query_kind_options(selected_kind) do
    flow_query_kind_options()
    |> Enum.map_join("\n", fn {kind, label} ->
      selected = if kind == selected_kind, do: " selected", else: ""
      ~s(<option value="#{kind}"#{selected}>#{label}</option>)
    end)
  end

  def flow_query_kind_options do
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

  def flow_query_kind_doc(kind) do
    docs = flow_query_kind_docs()
    Map.get(docs, kind, Map.fetch!(docs, "list"))
  end

  def flow_query_kind_docs do
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

  def render_flow_overview_filter(data) when is_map(data) do
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

  def flow_overview_live_url(filters) when is_map(filters) do
    partition_key = Map.get(filters, :partition_key)

    case partition_key do
      key when is_binary(key) and key != "" ->
        "/dashboard/api/flow?" <> URI.encode_query(%{"partition_key" => key})

      _ ->
        "/dashboard/api/flow"
    end
  end

  def render_flow_policy_editor(data) do
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

  def render_flow_policy_flash(%{kind: :ok, message: message, type: type}) do
    suffix = if type in [nil, ""], do: "", else: " for #{type}"
    ~s(<div class="flow-alert flow-alert-ok">#{escape(message <> suffix)}</div>)
  end

  def render_flow_policy_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  def render_flow_policy_flash(_flash), do: ""

  def render_flow_policy_preview(editor) do
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

  def render_flow_policy_backoff_select(current) do
    current = current |> to_string() |> String.downcase()

    options =
      Enum.map_join(~w(none fixed linear exponential), "\n", fn kind ->
        selected = if kind == current, do: ~s( selected), else: ""
        ~s(<option value="#{kind}"#{selected}>#{String.capitalize(kind)}</option>)
      end)

    ~s(<select class="flow-search-input" name="backoff_kind" title="Retry delay strategy">#{options}</select>)
  end

  def render_flow_policy_commands do
    render_config_command_table("Flow Policy Commands", flow_policy_command_reference())
  end

  def flow_policy_command_reference do
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

  def render_flow_retention_summary(data) do
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

  def render_flow_retention_controls(data) do
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

  def render_flow_retention_flash(%{kind: :dry_run, limit: limit}) do
    ~s(<div class="flow-alert flow-alert-ok">Dry run ready for limit #{format_number(limit)}. No data was removed.</div>)
  end

  def render_flow_retention_flash(%{kind: :ok, counts: counts, limit: limit}) do
    message =
      "Cleanup completed: #{format_number(Map.get(counts, :flows, 0))} flows, " <>
        "#{format_number(Map.get(counts, :history, 0))} history rows, " <>
        "#{format_number(Map.get(counts, :values, 0))} values removed (limit #{format_number(limit)})."

    ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)
  end

  def render_flow_retention_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)
  end

  def render_flow_retention_flash(_flash), do: ""

  def render_flow_retention_commands do
    render_config_command_table("Flow Retention Commands", flow_retention_command_reference())
  end

  def flow_retention_command_reference do
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

  def render_flow_retention_candidates(data) do
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

  def render_flow_retention_candidate_row(record, now_ms) do
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

  def render_flow_policies_table(policies, policy_scan) do
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

  def render_flow_policy_scan_note(policy_scan) do
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

  def render_flow_policy_row(%{error: error} = row) when is_binary(error) do
    """
    <tr>
      <td class="mono">#{escape(row.type)}</td>
      <td><span class="badge badge-pressure">error</span></td>
      <td colspan="6" class="c-red">#{escape(error)}</td>
    </tr>
    """
  end

  def render_flow_policy_row(row) do
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

  def flow_policy_edit_url(type) do
    "/dashboard/flow/policies?" <> URI.encode_query(%{"edit" => type}) <> "#flow-policy-editor"
  end

  def flow_policy_source_class("configured"), do: "badge-ok"
  def flow_policy_source_class(_source), do: "badge-idle"

  def render_flow_policy_state_overrides([]), do: ~s(<span class="c-muted">-</span>)

  def render_flow_policy_state_overrides(states) do
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

  def flow_policy_backoff_summary(backoff) when is_map(backoff) do
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

  def flow_policy_backoff_summary(_backoff), do: "-"

  def flow_policy_retention_summary(retention) when is_map(retention) do
    ttl_ms = flow_policy_field(retention, :ttl_ms, 0)
    max = flow_policy_field(retention, :history_max_events, 0)

    "#{format_duration_ms(ttl_ms)} retention, history max #{format_number(max)}"
  end

  def flow_policy_retention_summary(_retention), do: "-"

  def flow_policy_field(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  def flow_policy_field(_map, _key, default), do: default

  def flow_states_live_url(nil), do: "/dashboard/api/flow/states"

  def flow_states_live_url(filters) when is_map(filters) do
    case flow_states_filter_query(filters) do
      "" -> "/dashboard/api/flow/states"
      query -> "/dashboard/api/flow/states?" <> query
    end
  end

  def flow_states_live_url(type) when is_binary(type) do
    "/dashboard/api/flow/states?" <> URI.encode_query(%{"type" => type})
  end

  def flow_states_filter_query(filters) when is_map(filters) do
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

  def flow_signals_live_url(filters) when is_map(filters) do
    case flow_signals_filter_query(filters) do
      "" -> "/dashboard/api/flow/signals"
      query -> "/dashboard/api/flow/signals?" <> query
    end
  end

  def flow_signals_filter_query(filters) when is_map(filters) do
    []
    |> maybe_put_query_param("type", Map.get(filters, :type))
    |> maybe_put_query_param("signal", Map.get(filters, :signal))
    |> maybe_put_query_param("q", Map.get(filters, :q))
    |> maybe_put_query_param("scan", if(Map.get(filters, :scan_history), do: "true", else: nil))
    |> maybe_put_query_param("limit", flow_filter_limit_query_value(Map.get(filters, :limit)))
    |> Enum.reverse()
    |> URI.encode_query()
  end

  def maybe_put_query_param(params, _key, nil), do: params
  def maybe_put_query_param(params, _key, ""), do: params

  def maybe_put_query_param(params, key, value) when is_integer(value),
    do: [{key, Integer.to_string(value)} | params]

  def maybe_put_query_param(params, key, value), do: [{key, to_string(value)} | params]

  def flow_filter_limit_query_value(@flow_dashboard_recent_limit), do: nil
  def flow_filter_limit_query_value(limit) when is_integer(limit), do: limit
  def flow_filter_limit_query_value(_limit), do: nil
end
