defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryControls do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters

  @flow_dashboard_recent_limit 40

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
    kinds = ~w(list search terminals failures stuck)
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
    kinds = ~w(list search stats terminals failures)
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

  def render_flow_query_attribute_fields(filters) do
    kinds = ~w(list search stats)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    attribute_key = Map.get(filters, :attribute_key) || ""
    attribute_value = Map.get(filters, :attribute_value) || ""

    """
    <label class="flow-query-field" data-flow-query-field="attribute_key" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Attribute key
      <input class="flow-search-input mono" name="attribute_key" value="#{escape_attr(attribute_key)}" placeholder="tenant"#{disabled}>
      <span class="flow-field-help">Optional indexed attribute filter.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="attribute_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Attribute value
      <input class="flow-search-input mono" name="attribute_value" value="#{escape_attr(attribute_value)}" placeholder="acme"#{disabled}>
      <span class="flow-field-help">Used only when attribute key is present.</span>
    </label>
    """
  end

  def render_flow_query_state_meta_fields(filters) do
    kinds = ~w(search)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    state = Map.get(filters, :state_meta_state) || ""
    key = Map.get(filters, :state_meta_key) || ""
    value = Map.get(filters, :state_meta_value) || ""

    """
    <label class="flow-query-field" data-flow-query-field="state_meta_state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State meta state
      <input class="flow-search-input mono" name="state_meta_state" value="#{escape_attr(state)}" placeholder="review"#{disabled}>
      <span class="flow-field-help">Logical state that owns the metadata entry.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="state_meta_key" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State meta key
      <input class="flow-search-input mono" name="state_meta_key" value="#{escape_attr(key)}" placeholder="risk_tier"#{disabled}>
      <span class="flow-field-help">Policy-indexed state metadata key.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="state_meta_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State meta value
      <input class="flow-search-input mono" name="state_meta_value" value="#{escape_attr(value)}" placeholder="high"#{disabled}>
      <span class="flow-field-help">Scalar value for the indexed metadata key.</span>
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
      <input class="flow-search-input mono" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="required for record queries">
      <span class="flow-field-help">Required for bounded record queries; optional for history and aggregate views.</span>
    </label>
    """
  end

  def render_flow_query_time_fields(filters) do
    kinds = ~w(list search terminals failures stuck by_parent by_root by_correlation)
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
    kinds = ~w(list search terminals failures stuck by_parent by_root by_correlation)
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
      {"list", "FLOW.QUERY: list"},
      {"search", "FLOW.QUERY: metadata"},
      {"stats", "FLOW.STATS"},
      {"terminals", "FLOW.QUERY: terminals"},
      {"failures", "FLOW.QUERY: failures"},
      {"stuck", "FLOW.QUERY: expired leases"},
      {"history", "FLOW.HISTORY"},
      {"by_parent", "FLOW.QUERY: parent"},
      {"by_root", "FLOW.QUERY: root"},
      {"by_correlation", "FLOW.QUERY: correlation"}
    ]
  end

  def flow_query_kind_doc(kind) do
    docs = flow_query_kind_docs()
    Map.get(docs, kind, Map.fetch!(docs, "list"))
  end

  def flow_query_kind_docs do
    %{
      "list" => %{
        command: "FLOW.QUERY",
        purpose: "List workflows by type.",
        detail:
          "Use optional state, partition, time range, direction, and attribute filters to keep the result bounded."
      },
      "search" => %{
        command: "FLOW.QUERY",
        purpose: "Search policy-indexed Flow metadata.",
        detail:
          "Use indexed attribute filters and optional indexed state metadata filters. Search is bounded, projection-consistent, and payloads stay unloaded."
      },
      "stats" => %{
        command: "FLOW.STATS",
        purpose: "Count workflows by type and optional filters.",
        detail:
          "Use this before fetching rows when you only need a bounded count for state or attribute filters."
      },
      "terminals" => %{
        command: "FLOW.QUERY",
        purpose: "List terminal workflows for a type.",
        detail:
          "Use this to audit completed, failed, or cancelled workflow retention and terminal distribution."
      },
      "failures" => %{
        command: "FLOW.QUERY",
        purpose: "List failed workflows for a type.",
        detail:
          "Use this to inspect failure pressure before retrying, rewinding, or running retention cleanup."
      },
      "stuck" => %{
        command: "FLOW.QUERY",
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
        command: "FLOW.QUERY",
        purpose: "List workflows created under one parent.",
        detail: "Use this for fanout debugging when one workflow spawned many children.",
        id_label: "Parent ID",
        id_placeholder: "parent workflow id",
        id_help: "Required. Matches workflows whose parent_id equals this value."
      },
      "by_root" => %{
        command: "FLOW.QUERY",
        purpose: "List workflows in one root lineage.",
        detail: "Use this to inspect the full tree that belongs to one root workflow.",
        id_label: "Root ID",
        id_placeholder: "root workflow id",
        id_help: "Required. Matches workflows whose root_id equals this value."
      },
      "by_correlation" => %{
        command: "FLOW.QUERY",
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
