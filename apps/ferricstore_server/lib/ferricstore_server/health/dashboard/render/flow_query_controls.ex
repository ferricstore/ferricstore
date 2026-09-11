defmodule FerricstoreServer.Health.Dashboard.Render.FlowQueryControls do
  alias Ferricstore.Flow.Query.Limits

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.QueryParams

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
        cond do
          discovery_suggestions?(discovery) ->
            render_ready_flow_query_discovery(discovery)

          is_binary(Map.get(discovery, :type)) and Map.get(discovery, :type) != "" ->
            render_flow_query_discovery_message("Options are not loaded for this workflow type.")

          true ->
            render_flow_query_discovery_message(
              "Enter a workflow type, then choose Show options to load its queryable states and metadata."
            )
        end
    end
  end

  def render_flow_query_discovery(_discovery),
    do:
      render_flow_query_discovery_message(
        "Enter a workflow type, then choose Show options to load its queryable states and metadata."
      )

  def render_flow_query_discovery_datalists(discovery) when is_map(discovery) do
    types = Map.get(discovery, :available_types, [])
    partitions = Map.get(discovery, :available_partitions, [])
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
    <datalist id="flow-query-partition-options">#{render_datalist_options(partitions)}</datalist>
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

  def render_flow_query_type_field(filters), do: render_flow_query_type_field(filters, %{})

  def render_flow_query_type_field(%{type: type} = filters, discovery) do
    kinds = ~w(list search stats terminals failures stuck)
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)
    required = flow_query_required_attr(filters, kinds)

    available_types = Map.get(discovery, :available_types, [])

    placeholder =
      case available_types do
        [first | _] when is_binary(first) and first != "" ->
          "e.g. #{first} (or select from list)"

        _ ->
          "e.g. order_fulfillment (or select from list)"
      end

    """
    <label class="flow-query-field" data-flow-query-field="type" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Workflow Type
      <input class="flow-search-input mono" name="type" value="#{escape_attr(type || "")}" placeholder="#{escape_attr(placeholder)}" list="flow-query-type-options" autocomplete="off" data-flow-query-required-kinds="#{flow_query_kinds_attr(kinds)}"#{required}#{disabled}>
      <span class="flow-field-help">Filters records, not permissions.</span>
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
        "Empty includes all states."
      )

    """
    <label class="flow-query-field" data-flow-query-field="state" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      Runtime status
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
      Workflow state
      <input class="flow-search-input mono" name="run_state" value="#{escape_attr(run_state)}" placeholder="any step" list="flow-query-workflow-step-options" autocomplete="off"#{disabled}>
      <span class="flow-field-help">Logical workflow state while runtime status is running (run_state).</span>
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
      <span class="flow-field-help">Indexed attribute filter; Search requires this or state metadata.</span>
    </label>
    <div class="flow-query-field" data-flow-query-field="attribute_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}" data-flow-query-scalar-group data-flow-query-scalar-key="attribute_key"#{hidden}>
      <label for="flow-query-attribute-value">Attribute value</label>
      <div class="flow-query-scalar-input">
        <select class="flow-search-input mono" name="attribute_value_type" aria-label="Attribute value type" data-flow-query-scalar-type#{disabled}>#{render_metadata_value_type_options(attribute_value_type)}</select>
        <input class="flow-search-input mono" id="flow-query-attribute-value" name="attribute_value" value="#{escape_attr(attribute_value)}" placeholder="acme" data-default-placeholder="acme" list="flow-query-attribute-value-options" autocomplete="off" data-flow-query-scalar-value#{value_disabled}>
      </div>
      <span class="flow-field-help">Typed scalar used only when attribute key is present.</span>
      <span class="flow-field-error" data-flow-query-scalar-error role="status" hidden></span>
    </div>
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
    <div class="flow-query-field" data-flow-query-field="state_meta_value" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}" data-flow-query-scalar-group data-flow-query-scalar-key="state_meta_key"#{hidden}>
      <label for="flow-query-state-meta-value">State meta value</label>
      <div class="flow-query-scalar-input">
        <select class="flow-search-input mono" name="state_meta_value_type" aria-label="State metadata value type" data-flow-query-scalar-type#{disabled}>#{render_metadata_value_type_options(value_type)}</select>
        <input class="flow-search-input mono" id="flow-query-state-meta-value" name="state_meta_value" value="#{escape_attr(value)}" placeholder="high" data-default-placeholder="high" list="flow-query-state-meta-value-options" autocomplete="off" data-flow-query-scalar-value#{value_disabled}>
      </div>
      <span class="flow-field-help">Typed scalar for the indexed metadata key.</span>
      <span class="flow-field-error" data-flow-query-scalar-error role="status" hidden></span>
    </div>
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
      Partition Key
      <input class="flow-search-input mono" name="partition_key" value="#{escape_attr(partition_key || "")}" placeholder="#{escape_attr(placeholder)}" list="flow-query-partition-options" autocomplete="off" data-flow-query-partition-input data-flow-query-required-kinds="#{flow_query_kinds_attr(required_kinds)}"#{required}>
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
    clock = flow_query_clock(filters.kind)

    """
    <label class="flow-query-field" data-flow-query-field="from" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <span data-flow-query-time-from-label>#{clock} from UTC</span>
      #{render_query_time_input(filters, :from, disabled)}
      <span class="flow-field-help" data-flow-query-time-from-help>Optional lower bound for #{String.downcase(clock)}.</span>
    </label>
    <label class="flow-query-field" data-flow-query-field="to" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <span data-flow-query-time-to-label>#{clock} to UTC</span>
      #{render_query_time_input(filters, :to, disabled)}
      <span class="flow-field-help" data-flow-query-time-to-help>Optional upper bound for #{String.downcase(clock)}.</span>
    </label>
    <span class="flow-field-error" data-flow-query-time-error role="status" hidden></span>
    """
  end

  defp render_query_time_input(filters, field, disabled) do
    key = if field == :from, do: :from_ms, else: :to_ms
    parsed = Map.get(filters, key)
    error = filters |> Map.get(:errors, %{}) |> Map.get(field)
    invalid? = not is_nil(error) and is_nil(parsed)

    value =
      if invalid?,
        do: Map.get(Map.get(filters, :draft, %{}), field, ""),
        else: FerricstoreServer.Health.Dashboard.Flow.TimeFilter.input_value(parsed)

    error_id = "flow-query-#{field}-error"
    attrs = if error, do: ~s( aria-invalid="true" aria-describedby="#{error_id}"), else: ""

    feedback =
      if error,
        do:
          ~s(<span id="#{error_id}" class="flow-field-error" role="status">#{escape(error)}</span>),
        else: ""

    ~s(<input class="flow-search-input mono flow-filter-time" type="#{if invalid?, do: "text", else: "datetime-local"}" name="#{field}" step="0.001" value="#{escape_attr(value)}"#{attrs}#{disabled}>#{feedback})
  end

  def render_flow_query_direction_field(filters) do
    kinds = ~w(list search terminals failures stuck by_parent by_root by_correlation)
    checked = if filters.rev, do: "checked", else: ""
    hidden = flow_query_hidden_attr(filters, kinds)
    disabled = flow_query_disabled_attr(filters, kinds)

    """
    <label class="flow-check-label flow-query-check" data-flow-query-field="direction" data-flow-query-kinds="#{flow_query_kinds_attr(kinds)}"#{hidden}>
      <input type="checkbox" name="rev" value="true" #{checked}#{disabled}>
      <span data-flow-query-direction-label>Latest #{String.downcase(flow_query_clock(filters.kind))} first</span>
    </label>
    """
  end

  def flow_query_clock("stuck"), do: "Lease deadline"
  def flow_query_clock(_kind), do: "Updated time"

  def render_flow_query_dynamic_script do
    docs_json = Jason.encode!(flow_query_kind_docs())

    """
    <script>
    (() => {
      const form = document.currentScript.closest("[data-flow-query-workbench]")?.querySelector("[data-flow-query-form]");
      if (!form) return;
      const docs = #{docs_json};
      const select = form.querySelector("[data-flow-query-kind]");
      const help = form.closest("[data-flow-query-workbench]")?.querySelector("[data-flow-query-help]");
      const idLabel = form.querySelector("[data-flow-query-id-label]");
      const idInput = form.querySelector("[data-flow-query-id-input]");
      const idHelp = form.querySelector("[data-flow-query-id-help]");
      const stateInput = form.querySelector("[data-flow-query-state-input]");
      const stateHelp = form.querySelector("[data-flow-query-state-help]");
      const partitionInput = form.querySelector("[data-flow-query-partition-input]");
      const partitionHelp = form.querySelector("[data-flow-query-partition-help]");
      const scalarGroups = Array.from(form.querySelectorAll("[data-flow-query-scalar-group]"));
      const fromTime = form.elements.namedItem("from");
      const toTime = form.elements.namedItem("to");
      const timeError = form.querySelector("[data-flow-query-time-error]");
      const predicates = form.querySelector(".flow-query-advanced");
      const searchRequirement = form.querySelector("[data-flow-query-search-requirement]");
      const attributeKey = form.elements.namedItem("attribute_key");
      const stateMetaKey = form.elements.namedItem("state_meta_key");
      const stateMetaState = form.elements.namedItem("state_meta_state");
      const reveal = (input) => {
        let ancestor = input.parentElement;
        while (ancestor && ancestor !== form) {
          if (ancestor instanceof HTMLDetailsElement) { const details = ancestor; details.open = true; }
          ancestor = ancestor.parentElement;
        }
      };
      form.addEventListener("invalid", (event) => {
        reveal(event.target);
      }, true);
      const validateSearch = () => {
        const search = select?.value === "search";
        const missing = search && !attributeKey?.value.trim() && !(stateMetaKey?.value.trim() && stateMetaState?.value.trim());
        attributeKey?.setCustomValidity(missing ? "Enter an indexed attribute or state metadata predicate." : "");
        if (searchRequirement) searchRequirement.hidden = !search;
      };
      const validateTimeBounds = () => {
        if (!fromTime || !toTime) return;
        const reversed = !fromTime.disabled && !toTime.disabled && fromTime.type === 'datetime-local' && toTime.type === 'datetime-local' && fromTime.value && toTime.value && fromTime.value > toTime.value;
        const message = reversed ? "End UTC must be at or after start UTC." : "";
        toTime.setCustomValidity(message);
        if (timeError) { timeError.textContent = message; timeError.hidden = !message; }
      };
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
        input.required = !group.hidden && acceptsValue && type.value !== "string" && Boolean(key?.value);
        input.placeholder = acceptsValue ? (input.dataset.defaultPlaceholder || input.placeholder) : "No value";
        const value = input.value.trim();
        let message = "";
        if (input.required && !value) message = "Enter a value or select Text for an empty string.";
        if (!input.disabled && key?.value && value) {
          if (type.value === "boolean" && !/^(true|false)$/i.test(value)) message = "Enter true or false.";
          if (type.value === "integer" && !/^[+-]?[0-9]+$/.test(value)) message = "Enter a whole number.";
          if (type.value === "float" && (!/^[+-]?(?:[0-9]+(?:[.][0-9]*)?|[.][0-9]+)(?:[eE][+-]?[0-9]+)?$/.test(value) || !Number.isFinite(Number(value)))) message = "Enter a finite number.";
        }
        input.setCustomValidity(message);
        input.setAttribute("aria-invalid", message ? "true" : "false");
        const error = group.querySelector("[data-flow-query-scalar-error]");
        if (error) {
          error.id ||= "flow-query-" + input.name + "-scalar-error";
          error.textContent = message; error.hidden = !message;
          if (message) input.setAttribute("aria-describedby", error.id);
          else input.removeAttribute("aria-describedby");
        }
      };
      const update = () => {
        const kind = select?.value || "list";
        const doc = docs[kind] || docs.list;
        const clock = kind === "stuck" ? "Lease deadline" : "Updated time";
        [["time-from-label", clock + " from UTC"], ["time-to-label", clock + " to UTC"],
          ["time-from-help", "Optional lower bound for " + clock.toLowerCase() + "."],
          ["time-to-help", "Optional upper bound for " + clock.toLowerCase() + "."],
          ["direction-label", "Latest " + clock.toLowerCase() + " first"]].forEach(([name, value]) => {
          const node = form.querySelector("[data-flow-query-" + name + "]");
          if (node) node.textContent = value;
        });
        if (kind === "search" && predicates) predicates.open = true;
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
          stateHelp.textContent = doc.state_help || "Empty includes all states.";
        }
        if (partitionInput) {
          partitionInput.placeholder = doc.partition_placeholder || "required";
        }
        if (partitionHelp) {
          partitionHelp.textContent = doc.partition_help || "Required query, routing, and data ACL scope.";
        }
        scalarGroups.forEach(updateScalarGroup);
        validateSearch();
        validateTimeBounds();
      };
      select?.addEventListener("change", update);
      [attributeKey, stateMetaKey, stateMetaState].forEach(input => input?.addEventListener("input", validateSearch));
      scalarGroups.forEach((group) => {
        group.querySelector("[data-flow-query-scalar-type]")?.addEventListener("change", () => updateScalarGroup(group));
        const key = form.elements.namedItem(group.dataset.flowQueryScalarKey || "");
        key?.addEventListener("input", () => updateScalarGroup(group));
        group.querySelector("[data-flow-query-scalar-value]")?.addEventListener("input", () => updateScalarGroup(group));
      });
      fromTime?.addEventListener("input", validateTimeBounds);
      toTime?.addEventListener("input", validateTimeBounds);
      [fromTime, toTime].forEach(input => input?.addEventListener('input', () => {
        const error = form.querySelector('#flow-query-' + input.name + '-error');
        if (error) error.hidden = true;
        input.removeAttribute('aria-invalid');
        input.removeAttribute('aria-describedby');
      }));
      update();

      form.querySelectorAll("[data-flow-query-fill]").forEach((choice) => {
        choice.addEventListener("click", () => {
          const input = form.elements.namedItem(choice.dataset.flowQueryFill || "");
          if (input) {
            input.value = choice.dataset.flowQueryValue || "";
            input.dispatchEvent(new Event("input", {bubbles: true}));
            input.focus();
          }
        });
      });
    })();
    </script>
    """
  end

  def render_flow_query_import(%{fql: fql, params_json: params_json}, _kind) do
    draft = Jason.encode!(%{fql: fql, params_json: params_json})

    ~s(<button type="button" class="flow-search-button secondary" data-flow-query-import="#{escape_attr(draft)}">Import submitted Guided query</button><span class="flow-field-help" data-flow-query-import-status aria-live="polite">Replaces the Raw draft only after confirmation.</span>)
  end

  def render_flow_query_import(nil, kind) when kind in ["history", "stats"],
    do:
      ~s(<p class="flow-section-note">This Guided operation uses a native command and has no exact Raw FQL import.</p>)

  def render_flow_query_import(_form, _kind), do: ""

  def workbench_error_attrs(form, field) do
    errors = Map.get(form, :errors, %{})

    if Map.has_key?(errors, field) do
      first = Enum.find([:fql, :params_json], &Map.has_key?(errors, &1))
      focus = if field == first, do: " data-flow-query-first-error autofocus", else: ""
      position = workbench_error_position_attrs(form, field)
      ~s( aria-invalid="true" aria-describedby="#{workbench_error_id(field)}"#{focus}#{position})
    else
      ""
    end
  end

  defp workbench_error_position_attrs(form, field) do
    with %{byte: byte} when is_integer(byte) and byte > 0 <-
           form |> Map.get(:error_positions, %{}) |> Map.get(field),
         text when is_binary(text) <- Map.get(form, field),
         true <- byte <= byte_size(text) + 1,
         <<prefix::binary-size(byte - 1), rest::binary>> <- text,
         true <- String.valid?(prefix) do
      next =
        case String.next_codepoint(rest) do
          {codepoint, _rest} -> codepoint
          nil -> ""
        end

      start = editor_offset(prefix)
      finish = editor_offset(prefix <> next)
      ~s( data-flow-query-error-start="#{start}" data-flow-query-error-end="#{finish}")
    else
      _unpositioned -> ""
    end
  end

  # HTML textareas normalize newlines and index selections in UTF-16 code units.
  defp editor_offset(text) do
    text
    |> String.replace(["\r\n", "\r"], "\n")
    |> :unicode.characters_to_binary(:utf8, {:utf16, :big})
    |> byte_size()
    |> div(2)
  end

  def render_workbench_error(form, field) do
    case form |> Map.get(:errors, %{}) |> Map.get(field) do
      message when is_binary(message) ->
        ~s(<span class="flow-field-error" id="#{workbench_error_id(field)}" data-flow-query-server-error role="status">#{escape(message)}</span>)

      _valid ->
        ""
    end
  end

  defp workbench_error_id(:fql), do: "flow-query-fql-error"
  defp workbench_error_id(:params_json), do: "flow-query-params-json-error"

  def render_flow_query_mode_script(active_mode) do
    active = if active_mode == :advanced, do: "advanced", else: "guided"

    """
    <script>
    (() => {
      const panel = document.currentScript.closest("[data-flow-query-workbench]");
      if (!panel) return;
      const tabs = Array.from(panel.querySelectorAll("[data-flow-query-mode-tab]"));
      const modes = panel.querySelectorAll("[data-flow-query-mode]");
      const importButton = panel.querySelector("[data-flow-query-import]");
      const importStatus = panel.querySelector("[data-flow-query-import-status]");
      const guidedForm = panel.querySelector("[data-flow-query-form]");
      const rawForm = panel.querySelector("[data-flow-query-workbench-form]");
      const forms = {guided: guidedForm, advanced: rawForm};
      const storageKey = "ferricstore.query.inflight-draft.v1";
      const scope = panel.dataset.flowQueryDraftScope;
      const maxDraftBytes = 128 * 1024;
      const draftLifetime = 5 * 60 * 1000;
      const editableFields = (form) => Array.from(form?.elements || []).filter((field) =>
        ["INPUT", "SELECT", "TEXTAREA"].includes(field.tagName) && field.name &&
        !["hidden", "submit", "button", "password"].includes(field.type));
      const snapshot = (form) => editableFields(form).map((field) => ({
        name: field.name, value: field.value, checked: field.checked === true
      }));
      const fingerprint = () => JSON.stringify([snapshot(guidedForm), snapshot(rawForm)]);
      const initialForms = {guided: JSON.stringify(snapshot(guidedForm)), advanced: JSON.stringify(snapshot(rawForm))};
      const initialDraft = fingerprint();
      let allowNavigation = false;
      const dirty = () => fingerprint() !== initialDraft;
      document.addEventListener("dashboard:before-refresh", (event) => {
        if (!dirty()) return;
        if (!window.confirm("Discard unsubmitted query edits and refresh?")) {
          event.preventDefault();
          return;
        }
        allowNavigation = true;
      });
      window.addEventListener("beforeunload", (event) => {
        if (allowNavigation || !dirty()) return;
        event.preventDefault();
        event.returnValue = "";
      });
      window.addEventListener("pageshow", () => { allowNavigation = false; });
      const removeStoredDraft = () => {
        try { sessionStorage.removeItem(storageKey); } catch (_) {}
      };
      // Only the unsubmitted sibling crosses this navigation; consume before parsing or restoring.
      const restoreDraft = (activeMode) => { try {
        const saved = sessionStorage.getItem(storageKey);
        sessionStorage.removeItem(storageKey);
        if (saved && scope && new TextEncoder().encode(saved).length <= maxDraftBytes) {
          const entry = JSON.parse(saved);
          const age = Date.now() - entry.createdAt;
          const form = forms[entry.mode];
          if (entry.scope === scope && Number.isFinite(age) && age >= 0 && age <= draftLifetime &&
              form && entry.mode !== activeMode && Array.isArray(entry.fields) && entry.fields.length <= 64) {
            const fields = editableFields(form);
            if (entry.fields.every((draft) => draft && typeof draft.name === "string" &&
                typeof draft.value === "string" && typeof draft.checked === "boolean" &&
                fields.some((field) => field.name === draft.name))) {
              entry.fields.forEach((draft) => {
                const field = fields.find((candidate) => candidate.name === draft.name);
                field.value = draft.value;
                if (field.type === "checkbox" || field.type === "radio") field.checked = draft.checked;
              });
              fields.forEach((field) => field.dispatchEvent(new Event("change", {bubbles: true})));
            }
          }
        }
      } catch (_) { removeStoredDraft(); } };
      document.addEventListener("submit", (event) => {
        const form = event.target;
        if (!(form instanceof HTMLFormElement) || event.defaultPrevented) return;
        const action = new URL(form.getAttribute("action") || location.href, location.href);
        if (action.origin !== location.origin || action.pathname !== "/dashboard/flow/query") return;
        const submittedMode = form === guidedForm || form.elements.namedItem("surface")?.value === "guided" ? "guided" : "advanced";
        if (form !== guidedForm && form !== rawForm && JSON.stringify(snapshot(forms[submittedMode])) !== initialForms[submittedMode]) {
          const label = submittedMode === "advanced" ? "Raw FQL" : "Guided";
          if (!window.confirm("Discard unsubmitted " + label + " edits and change result page?")) {
            event.preventDefault();
            event.stopImmediatePropagation();
            return;
          }
        }
        const siblingMode = submittedMode === "guided" ? "advanced" : "guided";
        const fields = snapshot(forms[siblingMode]);
        if (!fields.length) return;
        let preserved = false;
        try {
          const entry = JSON.stringify({scope, createdAt: Date.now(), mode: siblingMode, fields});
          if (scope && fields.length <= 64 && new TextEncoder().encode(entry).length <= maxDraftBytes) {
            sessionStorage.setItem(storageKey, entry);
            preserved = sessionStorage.getItem(storageKey) === entry;
          }
        } catch (_) {}
        if (!preserved) {
          removeStoredDraft();
          const label = siblingMode === "advanced" ? "Raw FQL" : "Guided";
          if (!window.confirm("The " + label + " draft cannot be retained for this submission. Discard it and continue?")) {
            event.preventDefault();
            event.stopImmediatePropagation();
          }
        }
        queueMicrotask(() => { allowNavigation = !event.defaultPrevented; });
      }, true);
      if (importButton) {
        const invalidateImport = () => {
          importButton.disabled = true;
          if (importStatus) importStatus.textContent = "Guided draft changed. Submit it before importing.";
        };
        guidedForm?.addEventListener("input", invalidateImport);
        guidedForm?.addEventListener("change", invalidateImport);
        importButton.addEventListener("click", () => {
          if (importButton.disabled || !window.confirm("Replace the Raw FQL draft with the complete submitted Guided query and its parameters?")) return;
          const draft = JSON.parse(importButton.dataset.flowQueryImport);
          const rawForm = panel.querySelector("[data-flow-query-workbench-form]");
          ["fql", "params_json"].forEach((name) => {
            const field = rawForm.elements.namedItem(name);
            field.value = draft[name];
            field.dispatchEvent(new Event("input", {bubbles: true}));
          });
          if (importStatus) importStatus.textContent = "Complete submitted Guided query imported. Raw edits remain independent.";
        });
      }
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

      document.addEventListener("click", (event) => {
        const recover = event.target.closest("[data-flow-query-recover]");
        if (recover) {
          event.preventDefault();
          const mode = recover.dataset.flowQueryRecover;
          activate(mode);
          const form = mode === "advanced" ? rawForm : guidedForm;
          form?.elements.namedItem(recover.dataset.flowQueryRecoverField)?.focus();
          return;
        }
        if (!event.target.closest("[data-flow-query-clear-optional]") || !guidedForm) return;
        activate("guided");
        const kind = guidedForm.elements.namedItem("kind");
        if (kind?.value === "search") kind.value = "list";
        const clear = ["state", "run_state", "attribute_key", "attribute_value", "state_meta_state", "state_meta_key", "state_meta_value", "from", "to", "from_ms", "to_ms"];
        clear.forEach((name) => {
          const input = guidedForm.elements.namedItem(name);
          if (input) input.value = "";
        });
        ["attribute_value_type", "state_meta_value_type"].forEach((name) => {
          const input = guidedForm.elements.namedItem(name);
          if (input) input.value = "string";
        });
        const direction = guidedForm.elements.namedItem("rev");
        if (direction) direction.checked = false;
        Array.from(guidedForm.elements).forEach((input) => {
          input.dispatchEvent(new Event("input", {bubbles: true}));
          input.dispatchEvent(new Event("change", {bubbles: true}));
        });
        const status = document.querySelector("[data-flow-query-recovery-status]");
        if (status) status.textContent = "Optional filters removed from the Guided draft. Results have not changed.";
        guidedForm.elements.namedItem("type")?.focus();
      });

      const copyStatus = panel.querySelector("[data-flow-query-copy-status]");
      const setCopyStatus = (message) => {
        if (copyStatus) copyStatus.textContent = message || "";
      };
      panel.querySelectorAll("[data-flow-query-copy-field]").forEach((button) => {
        button.addEventListener("click", async () => {
          const form = button.closest("form");
          const field = form?.elements.namedItem(button.dataset.flowQueryCopyField || "");
          const text = field?.value || "";
          setCopyStatus("");
          const copied = typeof window.dashboardCopyText === "function" &&
            await window.dashboardCopyText(text, {restoreFocus: button});
          setCopyStatus(copied ? "Copied" : "Copy failed. Select and copy manually.");
        });
      });
      activate("#{active}");
      restoreDraft(tabs.find((tab) => tab.getAttribute("aria-selected") === "true")?.dataset.flowQueryModeTab);
      const firstError = panel.querySelector("[data-flow-query-first-error]");
      firstError?.focus();
      if (firstError?.hasAttribute("data-flow-query-error-start")) {
        const start = Number(firstError.dataset.flowQueryErrorStart);
        const end = Number(firstError.dataset.flowQueryErrorEnd);
        if (Number.isInteger(start) && Number.isInteger(end) && start >= 0 && end >= start && end <= firstError.value.length) {
          firstError.setSelectionRange(start, end);
        }
      }
      panel.querySelectorAll("[data-flow-query-server-error]").forEach((feedback) => {
        const field = Array.from(rawForm?.elements || []).find((input) => input.getAttribute("aria-describedby") === feedback.id);
        field?.addEventListener("input", () => {
          field.removeAttribute("aria-invalid");
          field.removeAttribute("aria-describedby");
          feedback.hidden = true;
        }, {once: true});
      });
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
      render_discovery_values(
        Map.get(discovery, :lifecycle_states, []),
        "No states found",
        "state"
      )

    lifecycle_states =
      if Map.get(discovery, :lifecycle_states_truncated?, false),
        do:
          lifecycle_states <>
            ~s(<span class="flow-query-discovery-more">more observed</span>),
        else: lifecycle_states

    workflow_steps =
      render_discovery_values(
        Map.get(discovery, :workflow_steps, []),
        "None configured",
        "run_state"
      )

    workflow_steps =
      if Map.get(discovery, :workflow_steps_truncated?, false),
        do: workflow_steps <> ~s(<span class="flow-query-discovery-more">more configured</span>),
        else: workflow_steps

    attributes =
      render_discovery_values(
        Map.get(discovery, :indexed_attributes, []),
        "None configured",
        "attribute_key"
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
          Map.get(discovery, :types_truncated?, false),
          "type"
        )
      else
        ""
      end

    partition_values =
      render_discovery_named_group(
        "Observed partitions",
        Map.get(discovery, :available_partitions, []),
        false,
        "partition_key"
      )

    title =
      if type == "",
        do: "Observed query options",
        else: "Query fields for <code>#{escape(type)}</code>"

    open = if Map.get(discovery, :status) == :ready, do: " open", else: ""

    """
    <details class="flow-query-discovery"#{open} data-flow-query-discovery>
      <summary class="flow-query-discovery-summary">
        <span class="flow-query-discovery-title">#{title}</span>
        <span class="flow-query-discovery-hint">Observed values and indexed fields</span>
      </summary>
      <div class="flow-query-discovery-groups">
        #{type_values}
        #{partition_values}
        <div><span>Runtime statuses</span><div>#{lifecycle_states}</div></div>
        <div><span>Workflow states</span><div>#{workflow_steps}</div></div>
        <div><span>Indexed attributes</span><div>#{attributes}</div></div>
        <div><span>Indexed state metadata</span><div>#{state_meta}</div></div>
        #{attribute_values}
        #{state_meta_values}
        #{restricted}
      </div>
    </details>
    """
  end

  defp render_flow_query_discovery_message(message) do
    """
    <div class="flow-query-discovery flow-query-discovery-message" data-flow-query-discovery>
      #{escape(message)}
    </div>
    """
  end

  defp render_discovery_values(values, empty_message, target) when is_list(values) do
    case Enum.filter(values, &(is_binary(&1) and &1 != "")) do
      [] ->
        ~s(<span class="flow-query-discovery-empty">#{escape(empty_message)}</span>)

      names ->
        Enum.map_join(names, "", &render_discovery_choice(&1, target))
    end
  end

  defp render_discovery_values(_values, empty_message, _target),
    do: ~s(<span class="flow-query-discovery-empty">#{escape(empty_message)}</span>)

  defp render_discovery_choice(value, target) do
    ~s(<button type="button" class="flow-query-discovery-choice" data-flow-query-fill="#{escape_attr(target)}" data-flow-query-value="#{escape_attr(value)}">#{escape(value)}</button>)
  end

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

  defp render_discovery_named_group(_label, [], _truncated?, _target), do: ""

  defp render_discovery_named_group(label, values, truncated?, target) do
    rendered = render_discovery_values(values, "", target)

    rendered =
      if truncated?,
        do: rendered <> ~s(<span class="flow-query-discovery-more">more observed</span>),
        else: rendered

    ~s(<div><span>#{escape(label)}</span><div>#{rendered}</div></div>)
  end

  defp discovery_suggestions?(discovery) do
    Enum.any?([:available_types, :available_partitions], fn key ->
      case Map.get(discovery, key, []) do
        [_first | _rest] -> true
        _empty -> false
      end
    end)
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
      {"list", "List workflow runs"},
      {"search", "Search indexed metadata"},
      {"stats", "Count workflow runs"},
      {"terminals", "Find terminal workflows"},
      {"failures", "Find failed workflows"},
      {"stuck", "Find expired leases"},
      {"history", "Inspect workflow history"},
      {"by_parent", "Find direct children"},
      {"by_root", "Find root lineage"},
      {"by_correlation", "Find correlated workflows"}
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
          "Audit completed, failed, or cancelled workflow retention and terminal distribution within one partition."
      },
      "failures" => %{
        command: "FLOW.QUERY",
        purpose: "List failed workflows for a type.",
        detail:
          "Inspect failure pressure before retrying, rewinding, or running retention cleanup."
      },
      "stuck" => %{
        command: "FLOW.QUERY",
        purpose: "Find running workflows with expired leases.",
        detail:
          "Time bounds and ordering use the lease deadline, not the workflow update time. The upper bound never extends beyond now."
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

    predicates =
      for {key, label} <- [type: "type", partition_key: "partition"],
          value = Map.get(filters, key),
          is_binary(value) and value != "" do
        ~s(#{label} <span class="mono">#{escape(value)}</span>)
      end

    if predicates == [] do
      ""
    else
      filtered = Map.get(data, :filtered_sampled, 0)
      total = Map.get(data, :total_sampled, filtered)

      """
      <div class="flow-filter-summary">
        Showing #{Enum.join(predicates, " / ")}
        <span class="badge badge-idle">#{format_number(filtered)} / #{format_number(total)} sampled</span>
        <a class="flow-filter-clear" href="/dashboard/flow" title="Clear all scope filters">Clear</a>
      </div>
      """
    end
  end

  def flow_overview_live_url(filters) when is_map(filters) do
    query =
      filters
      |> Map.take([:type, :partition_key])
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> URI.encode_query()

    "/dashboard/api/flow" <> if(query == "", do: "", else: "?" <> query)
  end

  def flow_states_live_url(nil), do: "/dashboard/api/flow/states"

  def flow_states_live_url(%{errors: errors}) when map_size(errors) > 0, do: ""

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
    |> maybe_put_query_param("partition_key", Map.get(filters, :partition_key))
    |> maybe_put_query_param("q", Map.get(filters, :q))
    |> maybe_put_query_param("sort", Map.get(filters, :sort))
    |> maybe_put_query_param("time_mode", Map.get(filters, :time_mode))
    |> maybe_put_query_param("range", range)
    |> maybe_put_query_param("from_ms", if(range, do: nil, else: Map.get(filters, :from_ms)))
    |> maybe_put_query_param("to_ms", if(range, do: nil, else: Map.get(filters, :to_ms)))
    |> maybe_put_query_param("limit", flow_filter_limit_query_value(Map.get(filters, :limit)))
    |> Enum.reverse()
    |> URI.encode_query()
  end

  def flow_signals_live_url(%{scan_history: true}), do: ""

  def flow_signals_live_url(filters) when is_map(filters) do
    case flow_signals_filter_query(filters) do
      "" -> "/dashboard/api/flow/signals"
      query -> "/dashboard/api/flow/signals?" <> query
    end
  end

  def flow_signals_filter_query(filters) when is_map(filters) do
    []
    |> maybe_put_query_param("type", Map.get(filters, :type))
    |> maybe_put_query_param("partition_key", Map.get(filters, :partition_key))
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
