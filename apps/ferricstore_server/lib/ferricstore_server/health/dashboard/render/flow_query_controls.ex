defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryControls do
  alias Ferricstore.Flow.Query.Limits

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams
  import FerricstoreServer.Health.Dashboard.Render.FlowFilters

  @flow_dashboard_recent_limit 40

  def render_flow_query_discovery(discovery) when is_map(discovery) do
    case Map.get(discovery, :status, :idle) do
      :ready ->
        render_ready_flow_query_discovery(discovery)

      :unavailable ->
        render_flow_query_discovery_message(
          "Type options are temporarily unavailable. You can still enter query fields manually."
        )

      :forbidden ->
        case {Map.get(discovery, :required_command), Map.get(discovery, :denied_scope)} do
          {command, _scope} when is_binary(command) ->
            render_flow_query_discovery_message("Type options require +#{command}.")

          {_command, :type} ->
            render_flow_query_discovery_message(
              "Type options are not available for this workflow type."
            )

          {_command, _partition_denied} ->
            render_flow_query_discovery_message(
              "Type options are not available for this partition."
            )
        end

      _idle ->
        render_flow_query_discovery_message(
          "Enter a workflow type, then choose Show options to load its queryable states and metadata."
        )
    end
  end

  def render_flow_query_discovery(_discovery),
    do:
      render_flow_query_discovery_message(
        "Enter a workflow type, then choose Show options to load its queryable states and metadata."
      )

  def render_flow_query_discovery_datalists(discovery) when is_map(discovery) do
    types = Map.get(discovery, :available_types, [])
    lifecycle_states = Map.get(discovery, :lifecycle_states, [])
    workflow_steps = Map.get(discovery, :workflow_steps, [])
    attributes = Map.get(discovery, :indexed_attributes, [])
    attribute_values = Map.get(discovery, :attribute_values, [])
    state_meta_values = Map.get(discovery, :state_meta_values, [])

    state_meta_keys =
      case Map.get(discovery, :indexed_state_meta) do
        key when is_binary(key) and key != "" -> [key]
        _missing -> []
      end

    """
    <datalist id="flow-query-type-options">#{render_datalist_options(types)}</datalist>
    <datalist id="flow-query-lifecycle-state-options">#{render_datalist_options(lifecycle_states)}</datalist>
    <datalist id="flow-query-workflow-step-options">#{render_datalist_options(workflow_steps)}</datalist>
    <datalist id="flow-query-attribute-options">#{render_datalist_options(attributes)}</datalist>
    <datalist id="flow-query-attribute-value-options">#{render_value_datalist_options(attribute_values)}</datalist>
    <datalist id="flow-query-state-meta-key-options">#{render_datalist_options(state_meta_keys)}</datalist>
    <datalist id="flow-query-state-meta-value-options">#{render_value_datalist_options(state_meta_values)}</datalist>
    """
  end

  def render_flow_query_discovery_datalists(_discovery) do
    render_flow_query_discovery_datalists(%{})
  end

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
    kinds = ~w(list search stats terminals failures stuck)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    required = flow_query_required_attr(filters, kinds)

    """
    <label class="flow-query-field" data-flow-query-field="type" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Workflow Type
      <input class="flow-search-input mono" name="type" value="#{escape_attr(type || "")}" placeholder="email" list="flow-query-type-options" autocomplete="off" data-flow-query-required-kinds="#{flow_query_kinds_attr(kinds)}"#{required}#{disabled}>
      <span class="flow-field-help">Workflow type filters records; Partition is the data ACL scope. With a partition, Show options suggests observed types.</span>
    </label>
    """
  end

  def render_flow_query_state_field(%{kind: kind, state: state} = filters) do
    kinds = ~w(list search stats terminals)
    required_kinds = ~w(stats)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    required = flow_query_required_attr(filters, required_kinds)
    doc = flow_query_kind_doc(kind)
    placeholder = Map.get(doc, :state_placeholder, "all states")

    help =
      Map.get(
        doc,
        :state_help,
        "Leave empty to include all states. Terminal queries remain limited to terminal states."
      )

    """
    <label class="flow-query-field" data-flow-query-field="state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Lifecycle state
      <input class="flow-search-input mono" name="state" value="#{escape_attr(state || "")}" placeholder="#{escape_attr(placeholder)}" list="flow-query-lifecycle-state-options" autocomplete="off" data-flow-query-state-input data-flow-query-required-kinds="#{flow_query_kinds_attr(required_kinds)}"#{required}#{disabled}>
      <span class="flow-field-help" data-flow-query-state-help>#{escape(help)}</span>
    </label>
    """
  end

  def render_flow_query_run_state_field(filters) do
    kinds = ~w(list search)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    run_state = Map.get(filters, :run_state) || ""

    """
    <label class="flow-query-field" data-flow-query-field="run_state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Workflow step
      <input class="flow-search-input mono" name="run_state" value="#{escape_attr(run_state)}" placeholder="any step" list="flow-query-workflow-step-options" autocomplete="off"#{disabled}>
      <span class="flow-field-help">Optional logical step while a workflow is running.</span>
    </label>
    """
  end

  def render_flow_query_attribute_fields(filters) do
    kinds = ~w(list search stats)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    attribute_key = Map.get(filters, :attribute_key) || ""
    attribute_value_type = Map.get(filters, :attribute_value_type, "string")

    attribute_value =
      Map.get(filters, :attribute_value_input) ||
        metadata_input_value(Map.get(filters, :attribute_value))

    value_disabled = scalar_value_disabled_attr(disabled, attribute_value_type)

    """
    <label class="flow-query-field" data-flow-query-field="attribute_key" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Attribute key
      <input class="flow-search-input mono" name="attribute_key" value="#{escape_attr(attribute_key)}" placeholder="tenant" list="flow-query-attribute-options" autocomplete="off"#{disabled}>
      <span class="flow-field-help">Optional indexed attribute filter.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="attribute_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}" data-flow-query-scalar-group data-flow-query-scalar-key="attribute_key"#{hidden}>
      Attribute value
      <div class="flow-query-scalar-input">
        <select class="flow-search-input mono" name="attribute_value_type" aria-label="Attribute value type" data-flow-query-scalar-type#{disabled}>#{render_metadata_value_type_options(attribute_value_type)}</select>
        <input class="flow-search-input mono" name="attribute_value" value="#{escape_attr(attribute_value)}" placeholder="acme" data-default-placeholder="acme" list="flow-query-attribute-value-options" autocomplete="off" data-flow-query-scalar-value#{value_disabled}>
      </div>
      <span class="flow-field-help">Typed scalar used only when attribute key is present.</span>
    </label>
    """
  end

  def render_flow_query_state_meta_fields(filters) do
    kinds = ~w(search)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    state = Map.get(filters, :state_meta_state) || ""
    key = Map.get(filters, :state_meta_key) || ""
    value_type = Map.get(filters, :state_meta_value_type, "string")

    value =
      Map.get(filters, :state_meta_value_input) ||
        metadata_input_value(Map.get(filters, :state_meta_value))

    value_disabled = scalar_value_disabled_attr(disabled, value_type)

    """
    <label class="flow-query-field" data-flow-query-field="state_meta_state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State meta state
      <input class="flow-search-input mono" name="state_meta_state" value="#{escape_attr(state)}" placeholder="review" list="flow-query-workflow-step-options" autocomplete="off"#{disabled}>
      <span class="flow-field-help">Logical state that owns the metadata entry.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="state_meta_key" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      State meta key
      <input class="flow-search-input mono" name="state_meta_key" value="#{escape_attr(key)}" placeholder="risk_tier" list="flow-query-state-meta-key-options" autocomplete="off"#{disabled}>
      <span class="flow-field-help">Policy-indexed state metadata key.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="state_meta_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}" data-flow-query-scalar-group data-flow-query-scalar-key="state_meta_key"#{hidden}>
      State meta value
      <div class="flow-query-scalar-input">
        <select class="flow-search-input mono" name="state_meta_value_type" aria-label="State metadata value type" data-flow-query-scalar-type#{disabled}>#{render_metadata_value_type_options(value_type)}</select>
        <input class="flow-search-input mono" name="state_meta_value" value="#{escape_attr(value)}" placeholder="high" data-default-placeholder="high" list="flow-query-state-meta-value-options" autocomplete="off" data-flow-query-scalar-value#{value_disabled}>
      </div>
      <span class="flow-field-help">Typed scalar for the indexed metadata key.</span>
    </label>
    """
  end

  def render_flow_query_id_field(%{kind: kind, id: id} = filters) do
    kinds = ~w(history by_parent by_root by_correlation)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    required = flow_query_required_attr(filters, kinds)
    doc = flow_query_kind_doc(kind)

    """
    <label class="flow-query-field" data-flow-query-field="id" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <span data-flow-query-id-label>#{escape(Map.get(doc, :id_label, "Flow ID"))}</span>
      <input class="flow-search-input mono" name="id" value="#{escape_attr(id || "")}" placeholder="#{escape_attr(Map.get(doc, :id_placeholder, "workflow id"))}" data-flow-query-id-input data-flow-query-required-kinds="#{flow_query_kinds_attr(kinds)}"#{required}#{disabled}>
      <span class="flow-field-help" data-flow-query-id-help>#{escape(Map.get(doc, :id_help, "Required id for this query."))}</span>
    </label>
    """
  end

  def render_flow_query_partition_field(%{kind: kind, partition_key: partition_key} = filters) do
    required_kinds = ~w(list search terminals failures stuck by_parent by_root by_correlation)
    required = flow_query_required_attr(filters, required_kinds)
    doc = flow_query_kind_doc(kind)
    placeholder = Map.get(doc, :partition_placeholder, "required")

    help =
      Map.get(doc, :partition_help, "Required query, routing, and data ACL scope.")

    """
    <label class="flow-query-field">
      Partition
      <input class="flow-search-input mono" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="#{escape_attr(placeholder)}" data-flow-query-partition-input data-flow-query-required-kinds="#{flow_query_kinds_attr(required_kinds)}"#{required}>
      <span class="flow-field-help" data-flow-query-partition-help>#{escape(help)}</span>
    </label>
    """
  end

  def render_flow_query_limit_field(filters) do
    kinds = ~w(list search terminals failures stuck history by_parent by_root by_correlation)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-query-field" data-flow-query-field="limit" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Limit
      <input class="flow-search-input mono flow-filter-limit" type="number" min="1" max="#{Limits.max_results()}" name="limit" value="#{filters.limit}"#{disabled}>
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
      const stateInput = form.querySelector("[data-flow-query-state-input]");
      const stateHelp = form.querySelector("[data-flow-query-state-help]");
      const partitionInput = form.querySelector("[data-flow-query-partition-input]");
      const partitionHelp = form.querySelector("[data-flow-query-partition-help]");
      const scalarGroups = Array.from(form.querySelectorAll("[data-flow-query-scalar-group]"));
      const setText = (selector, value) => {
        const node = help && help.querySelector(selector);
        if (node) node.textContent = value || "";
      };
      const allowed = (node, kind) => (node.dataset.flowQueryKinds || "").split(" ").includes(kind);
      const updateScalarGroup = (group) => {
        const type = group.querySelector("[data-flow-query-scalar-type]");
        const input = group.querySelector("[data-flow-query-scalar-value]");
        const key = form.elements.namedItem(group.dataset.flowQueryScalarKey || "");
        if (!type || !input) return;
        const acceptsValue = type.value !== "null";
        input.disabled = group.hidden || !acceptsValue;
        input.required = !group.hidden && acceptsValue && Boolean(key?.value);
        input.placeholder = acceptsValue ? (input.dataset.defaultPlaceholder || input.placeholder) : "No value";
      };
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
        form.querySelectorAll("[data-flow-query-required-kinds]").forEach((input) => {
          const requiredKinds = (input.dataset.flowQueryRequiredKinds || "").split(" ");
          input.required = requiredKinds.includes(kind);
        });
        setText("[data-flow-query-help-command]", doc.command);
        setText("[data-flow-query-help-purpose]", doc.purpose);
        setText("[data-flow-query-help-detail]", doc.detail);
        if (idLabel) idLabel.textContent = doc.id_label || "Flow ID";
        if (idInput) idInput.placeholder = doc.id_placeholder || "workflow id";
        if (idHelp) idHelp.textContent = doc.id_help || "Required id for this query.";
        if (stateInput) stateInput.placeholder = doc.state_placeholder || "all states";
        if (stateHelp) {
          stateHelp.textContent = doc.state_help || "Leave empty to include all states. Terminal queries remain limited to terminal states.";
        }
        if (partitionInput) {
          partitionInput.placeholder = doc.partition_placeholder || "required";
        }
        if (partitionHelp) {
          partitionHelp.textContent = doc.partition_help || "Required query, routing, and data ACL scope.";
        }
        scalarGroups.forEach(updateScalarGroup);
      };
      select?.addEventListener("change", update);
      scalarGroups.forEach((group) => {
        group.querySelector("[data-flow-query-scalar-type]")?.addEventListener("change", () => updateScalarGroup(group));
        const key = form.elements.namedItem(group.dataset.flowQueryScalarKey || "");
        key?.addEventListener("input", () => updateScalarGroup(group));
      });
      update();
    })();
    </script>
    """
  end

  def render_flow_query_mode_script(active_mode) do
    active = if active_mode == :advanced, do: "advanced", else: "guided"

    """
    <script>
    (() => {
      const panel = document.currentScript.closest(".flow-policy-panel");
      if (!panel) return;
      const tabs = Array.from(panel.querySelectorAll("[data-flow-query-mode-tab]"));
      const modes = panel.querySelectorAll("[data-flow-query-mode]");
      const activate = (name) => {
        tabs.forEach((tab) => {
          const selected = tab.dataset.flowQueryModeTab === name;
          tab.setAttribute("aria-selected", selected ? "true" : "false");
          tab.tabIndex = selected ? 0 : -1;
        });
        modes.forEach((mode) => {
          const selected = mode.dataset.flowQueryMode === name;
          mode.hidden = !selected;
          mode.setAttribute("aria-hidden", selected ? "false" : "true");
        });
      };
      tabs.forEach((tab, index) => {
        tab.addEventListener("click", () => activate(tab.dataset.flowQueryModeTab));
        tab.addEventListener("keydown", (event) => {
          let nextIndex;
          if (event.key === "ArrowRight" || event.key === "ArrowDown") {
            nextIndex = (index + 1) % tabs.length;
          } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
            nextIndex = (index - 1 + tabs.length) % tabs.length;
          } else if (event.key === "Home") {
            nextIndex = 0;
          } else if (event.key === "End") {
            nextIndex = tabs.length - 1;
          } else {
            return;
          }
          event.preventDefault();
          activate(tabs[nextIndex].dataset.flowQueryModeTab);
          tabs[nextIndex].focus();
        });
      });
      activate("#{active}");
    })();
    </script>
    """
  end

  def flow_query_hidden_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " hidden")

  def flow_query_disabled_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: "", else: " disabled")

  def flow_query_required_attr(%{kind: kind}, kinds),
    do: if(kind in kinds, do: " required", else: "")

  def flow_query_kinds_attr(kinds), do: kinds |> Enum.join(" ") |> escape_attr()

  defp render_metadata_value_type_options(selected) do
    [
      {"string", "Text"},
      {"integer", "Integer"},
      {"float", "Decimal"},
      {"boolean", "Boolean"},
      {"null", "Null"}
    ]
    |> Enum.map_join(fn {value, label} ->
      selected_attr = if selected == value, do: " selected", else: ""
      ~s(<option value="#{value}"#{selected_attr}>#{label}</option>)
    end)
  end

  defp scalar_value_disabled_attr(disabled, "null"), do: disabled <> " disabled"
  defp scalar_value_disabled_attr(disabled, _type), do: disabled

  defp metadata_input_value(nil), do: ""
  defp metadata_input_value(value) when is_binary(value), do: value
  defp metadata_input_value(value), do: to_string(value)

  defp render_ready_flow_query_discovery(discovery) do
    type = Map.get(discovery, :type) || ""

    lifecycle_states =
      render_discovery_values(Map.get(discovery, :lifecycle_states, []), "No states found")

    lifecycle_states =
      if Map.get(discovery, :lifecycle_states_truncated?, false),
        do:
          lifecycle_states <>
            ~s(<span class="flow-query-discovery-more">more observed</span>),
        else: lifecycle_states

    workflow_steps =
      render_discovery_values(Map.get(discovery, :workflow_steps, []), "None configured")

    workflow_steps =
      if Map.get(discovery, :workflow_steps_truncated?, false),
        do: workflow_steps <> ~s(<span class="flow-query-discovery-more">more configured</span>),
        else: workflow_steps

    attributes =
      render_discovery_values(
        Map.get(discovery, :indexed_attributes, []),
        "None configured"
      )

    attribute_values =
      render_discovery_value_group(
        "Top attribute values",
        Map.get(discovery, :attribute_values, [])
      )

    state_meta =
      case Map.get(discovery, :indexed_state_meta) do
        key when is_binary(key) and key != "" -> ~s(<code>#{escape(key)}</code>)
        _missing -> ~s(<span class="flow-query-discovery-empty">None configured</span>)
      end

    state_meta_values =
      render_discovery_value_group(
        "Top state metadata values",
        Map.get(discovery, :state_meta_values, [])
      )

    restricted = render_restricted_discovery(Map.get(discovery, :restricted_commands, []))

    type_values =
      if type == "" do
        render_discovery_named_group(
          "Observed workflow types",
          Map.get(discovery, :available_types, []),
          Map.get(discovery, :types_truncated?, false)
        )
      else
        ""
      end

    title =
      if type == "",
        do: "Observed query options",
        else: "Query fields for <code>#{escape(type)}</code>"

    """
    <div class="flow-query-discovery" data-flow-query-discovery>
      <div class="flow-query-discovery-title">#{title}</div>
      <div class="flow-query-discovery-groups">
        #{type_values}
        <div><span>Lifecycle states</span><div>#{lifecycle_states}</div></div>
        <div><span>Workflow steps</span><div>#{workflow_steps}</div></div>
        <div><span>Indexed attributes</span><div>#{attributes}</div></div>
        <div><span>Indexed state metadata</span><div>#{state_meta}</div></div>
        #{attribute_values}
        #{state_meta_values}
        #{restricted}
      </div>
    </div>
    """
  end

  defp render_flow_query_discovery_message(message) do
    """
    <div class="flow-query-discovery flow-query-discovery-message" data-flow-query-discovery>
      #{escape(message)}
    </div>
    """
  end

  defp render_discovery_values(values, empty_message) when is_list(values) do
    case Enum.filter(values, &(is_binary(&1) and &1 != "")) do
      [] ->
        ~s(<span class="flow-query-discovery-empty">#{escape(empty_message)}</span>)

      names ->
        Enum.map_join(names, "", fn name -> ~s(<code>#{escape(name)}</code>) end)
    end
  end

  defp render_discovery_values(_values, empty_message),
    do: ~s(<span class="flow-query-discovery-empty">#{escape(empty_message)}</span>)

  defp render_datalist_options(values) when is_list(values) do
    values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> Enum.uniq()
    |> Enum.map_join(fn value -> ~s(<option value="#{escape_attr(value)}"></option>) end)
  end

  defp render_datalist_options(_values), do: ""

  defp render_value_datalist_options(values) when is_list(values) do
    values
    |> Enum.flat_map(fn
      %{value: value} -> scalar_datalist_value(value)
      _invalid -> []
    end)
    |> render_datalist_options()
  end

  defp render_value_datalist_options(_values), do: ""

  defp scalar_datalist_value(nil), do: []
  defp scalar_datalist_value(value) when is_binary(value) and value == "", do: []
  defp scalar_datalist_value(value) when is_binary(value), do: [value]

  defp scalar_datalist_value(value) when is_integer(value) or is_float(value),
    do: [to_string(value)]

  defp scalar_datalist_value(value) when is_boolean(value), do: [to_string(value)]
  defp scalar_datalist_value(_invalid), do: []

  defp render_discovery_named_group(_label, [], _truncated?), do: ""

  defp render_discovery_named_group(label, values, truncated?) do
    rendered = render_discovery_values(values, "")

    rendered =
      if truncated?,
        do: rendered <> ~s(<span class="flow-query-discovery-more">more observed</span>),
        else: rendered

    ~s(<div><span>#{escape(label)}</span><div>#{rendered}</div></div>)
  end

  defp render_discovery_value_group(_label, []), do: ""

  defp render_discovery_value_group(label, values) do
    rendered =
      Enum.map_join(values, "", fn
        %{value: value, count: count} = entry ->
          count = render_discovery_count(count, Map.get(entry, :approximate) == true)

          ~s(<span class="flow-query-discovery-value"><code>#{escape(scalar_display_value(value))}</code>#{count}</span>)

        _invalid ->
          ""
      end)

    ~s(<div><span>#{escape(label)}</span><div>#{rendered}</div></div>)
  end

  defp render_discovery_count(count, true) do
    value = escape(to_string(count))
    ~s(<small title="Approximate count" aria-label="approximately #{value}">~#{value}</small>)
  end

  defp render_discovery_count(count, false), do: ~s(<small>#{escape(to_string(count))}</small>)

  defp scalar_display_value(nil), do: "NULL"
  defp scalar_display_value(""), do: "(empty text)"
  defp scalar_display_value(value) when is_binary(value), do: value
  defp scalar_display_value(value), do: to_string(value)

  defp render_restricted_discovery(commands) when is_list(commands) do
    if "FLOW.ATTRIBUTE_VALUES" in commands do
      ~s(<div><span>Value discovery</span><div>Top values require <code>+FLOW.ATTRIBUTE_VALUES</code>.</div></div>)
    else
      ""
    end
  end

  defp render_restricted_discovery(_commands), do: ""

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
          "Use a required partition plus optional state, time range, direction, and attribute filters to keep the result bounded."
      },
      "search" => %{
        command: "FLOW.QUERY",
        purpose: "Search policy-indexed Flow metadata.",
        detail:
          "Use a required partition with indexed attribute or state metadata filters. Search is bounded, projection-consistent, and payloads stay unloaded."
      },
      "stats" => %{
        command: "FLOW.STATS",
        purpose: "Count workflows by type and optional filters.",
        detail:
          "Use this before fetching rows when you only need a bounded count for state or attribute filters.",
        state_placeholder: "required",
        state_help: "Required. Counts one state; use state any only with an indexed attribute.",
        partition_placeholder: "optional",
        partition_help: "Optional query and data ACL scope. Empty requires wildcard read access."
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
        partition_placeholder: "optional",
        partition_help: "Optional. Otherwise the Flow ID derives the data ACL scope.",
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
