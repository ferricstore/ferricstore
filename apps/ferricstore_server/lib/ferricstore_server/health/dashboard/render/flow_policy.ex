defmodule FerricstoreServer.Health.Dashboard.Render.FlowPolicy do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]
  alias FerricstoreServer.Health.Dashboard.Flow.PolicyEditor
  alias FerricstoreServer.Health.Dashboard.Render.DurationInput

  @flow_dashboard_policy_state_preview_limit 6

  defdelegate flow_policy_editor_data(type), to: PolicyEditor, as: :load

  def flow_policy_clean_form_value(value) when is_binary(value), do: value
  def flow_policy_clean_form_value(value), do: to_string(value)

  def render_flow_policy_editor(data) do
    editor = Map.get(data, :editor, PolicyEditor.empty())
    flash = render_flow_policy_flash(Map.get(data, :flash))

    content =
      cond do
        get_in(data, [:action_capabilities, :save]) == false ->
          flash <>
            ~s(<p class="flow-section-note">Saving this scope requires +FLOW.POLICY.SET and write access to its workflow type.</p>)

        Map.get(editor, :load_error, false) ->
          ~s(<div class="flow-alert flow-alert-error" role="alert">Policy could not be loaded. Retry loading the selected scope before editing.</div>)

        editor.type == "" ->
          flash

        true ->
          render_loaded_policy_editor(editor, flash)
      end

    """
    <section id="flow-policy-editor" aria-label="Policy editor">
      <form class="flow-search flow-policy-scope" method="get" action="/dashboard/flow/policies#flow-policy-editor" aria-label="Policy scope">
        <label class="flow-policy-field"><span>Workflow type</span><input class="flow-search-input mono" type="text" name="edit" value="#{escape_attr(editor.type)}" required autocomplete="off"></label>
        <label class="flow-policy-field"><span>State</span><input class="flow-search-input mono" type="text" name="edit_state" value="#{escape_attr(editor.state)}" placeholder="Type defaults" autocomplete="off"></label>
        <button class="flow-search-button" type="submit">Load policy</button>
      </form>
      #{content}
    </section>
    """
  end

  defp render_loaded_policy_editor(editor, flash) do
    indexed_attributes = Map.get(editor, :indexed_attributes) || ""
    indexed_state_meta = Map.get(editor, :indexed_state_meta) || ""
    max_active_ms = Map.get(editor, :max_active_ms) || ""

    """
    <div class="flow-policy-panel">
      <h2 class="section-title">Create / Update Policy #{info_icon("Policies affect new Flow work and retry scheduling. Existing Flow records keep their durable state.", "About policy changes")}</h2>
      #{flash}
      #{render_policy_type_scope_note(editor)}
      <form class="flow-policy-form" action="/dashboard/flow/policies" method="post" data-policy-editor data-policy-draft="#{Map.get(editor, :dirty, false)}" data-dashboard-single-submit>
        <input type="hidden" name="expected_generation" value="#{escape_attr(to_string(editor.expected_generation))}">
        <fieldset class="flow-management-group"><legend>Scope</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Type</span>
            <input class="flow-search-input mono" type="text" name="type" value="#{escape_attr(editor.type)}" readonly required title="Loaded Flow type">
          </label>
          <label class="flow-policy-field">
            <span>State override</span>
            <input class="flow-search-input mono" type="text" name="state" value="#{escape_attr(editor.state)}" readonly placeholder="Type defaults" title="Loaded state override">
          </label>
          <label class="flow-policy-field">
            <span>State mode</span>
            #{render_flow_policy_mode_select(Map.get(editor, :mode, :parallel), editor.state == "")}
            #{if editor.state == "", do: "<small>Available on a state override.</small>", else: ""}
          </label>
        </div></fieldset>
        <fieldset class="flow-management-group"><legend>Indexing</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Indexed attrs</span>
            <input class="flow-search-input mono" type="text" name="indexed_attributes" value="#{escape_attr(indexed_attributes)}" autocomplete="off" placeholder="tenant, region" title="Comma-separated type-level indexed attributes used by FLOW.QUERY"#{if editor.state != "", do: " disabled", else: ""}>
          </label>
          <label class="flow-policy-field">
            <span>Indexed state meta</span>
            <input class="flow-search-input mono" type="text" name="indexed_state_meta" value="#{escape_attr(indexed_state_meta)}" autocomplete="off" placeholder="risk_tier" title="Optional type-level state metadata key used by FLOW.QUERY"#{if editor.state != "", do: " disabled", else: ""}>
          </label>
        </div></fieldset>
        <fieldset class="flow-management-group"><legend>Retry</legend>
        <div class="flow-policy-grid">
          <label class="flow-policy-field">
            <span>Max retries</span>
            <input class="flow-search-input mono" type="#{numeric_input_type(editor.max_retries)}" name="max_retries" min="0" value="#{escape_attr(to_string(editor.max_retries))}" required title="Maximum FLOW.RETRY attempts before the workflow is exhausted" aria-describedby="policy-max_retries-error">
            #{numeric_error("max_retries")}
          </label>
          <label class="flow-policy-field">
            <span>Backoff</span>
            #{render_flow_policy_backoff_select(editor.backoff_kind)}
          </label>
          <div class="flow-policy-field">
            <span id="policy-base-ms-label">Initial delay</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="base_ms" data-duration-min="0" value="#{escape_attr(to_string(editor.base_ms))}" required title="Initial retry delay" aria-labelledby="policy-base-ms-label" aria-describedby="policy-base_ms-error">
            #{DurationInput.units("base_ms", Map.get(editor, :base_ms_unit, "milliseconds"))}
            </div>
            #{numeric_error("base_ms")}
          </div>
          <div class="flow-policy-field">
            <span id="policy-max-ms-label">Maximum delay</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="max_ms" data-duration-min="0" value="#{escape_attr(to_string(editor.max_ms))}" required title="Maximum retry delay" aria-labelledby="policy-max-ms-label" aria-describedby="policy-max_ms-error">
            #{DurationInput.units("max_ms", Map.get(editor, :max_ms_unit, "milliseconds"))}
            </div>
            #{numeric_error("max_ms")}
          </div>
          <label class="flow-policy-field">
            <span>Jitter %</span>
            <input class="flow-search-input mono" type="#{numeric_input_type(editor.jitter_pct)}" name="jitter_pct" min="0" max="100" value="#{escape_attr(to_string(editor.jitter_pct))}" required title="Randomized retry delay percentage to avoid synchronized retries" aria-describedby="policy-jitter_pct-error">
            #{numeric_error("jitter_pct")}
          </label>
          <label class="flow-policy-field">
            <span>Exhausted to</span>
            <input class="flow-search-input mono" type="text" name="exhausted_to" value="#{escape_attr(editor.exhausted_to)}" autocomplete="off" required title="Terminal state used when retry attempts are exhausted">
          </label>
        </div></fieldset>
        <fieldset class="flow-management-group"><legend>Retention</legend>
        <div class="flow-policy-grid">
          <div class="flow-policy-field">
            <span id="policy-max-active-label">Maximum active duration</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="max_active_ms" data-duration-min="1" data-duration-max="31536000000" value="#{escape_attr(to_string(max_active_ms))}" placeholder="unlimited" title="Type-wide maximum runtime for new active Flow records; leave blank for unlimited" aria-labelledby="policy-max-active-label" aria-describedby="policy-max_active_ms-error"#{if editor.state != "", do: " disabled", else: ""}>
            #{DurationInput.units("max_active_ms", Map.get(editor, :max_active_ms_unit, "milliseconds"), editor.state != "")}
            </div>
            #{numeric_error("max_active_ms")}
          </div>
          <div class="flow-policy-field">
            <span id="policy-retention-label">Terminal retention</span>
            <div class="flow-duration-control">
            <input class="flow-search-input mono" type="text" inputmode="decimal" name="retention_ttl_ms" data-duration-min="1" value="#{escape_attr(to_string(editor.retention_ttl_ms))}" required title="How long terminal state, history, and generated values are retained" aria-labelledby="policy-retention-label" aria-describedby="policy-retention_ttl_ms-error">
            #{DurationInput.units("retention_ttl_ms", Map.get(editor, :retention_ttl_ms_unit, "milliseconds"))}
            </div>
            #{numeric_error("retention_ttl_ms")}
          </div>
          <label class="flow-policy-field">
            <span>Max history</span>
            <input class="flow-search-input mono" type="#{numeric_input_type(editor.history_max_events)}" name="history_max_events" min="1" value="#{escape_attr(to_string(editor.history_max_events))}" required title="Maximum durable history events retained before cleanup can trim old events" aria-describedby="policy-history_max_events-error">
            #{numeric_error("history_max_events")}
          </label>
        </div></fieldset>
        #{render_flow_policy_preview(editor)}
        <p class="flow-field-error" data-policy-scope-status role="status" hidden>Selection changed. Load policy before saving.</p>
        <p class="flow-section-note" data-policy-dirty-status role="status" hidden>Unsaved changes</p>
        <div class="flow-policy-actions">
          <button class="flow-search-button" type="submit" title="Save this Flow policy">Save Policy</button>
          <a class="flow-link" data-discard-draft href="#{escape_attr(flow_policy_edit_url(editor.type, editor.state))}">Discard changes</a>
        </div>
      </form>
      #{FerricstoreServer.Health.Dashboard.Render.FlowFormScripts.duration_script()}
      #{FerricstoreServer.Health.Dashboard.Render.FlowFormScripts.policy_script()}
    </div>
    """
  end

  defp render_policy_type_scope_note(%{state: ""}), do: ""

  defp render_policy_type_scope_note(editor) do
    url = "/dashboard/flow/policies?" <> URI.encode_query(%{"edit" => editor.type})

    ~s(<p class="flow-section-note">Type-wide settings are unchanged by this state override. <a class="flow-link" href="#{escape_attr(url)}#flow-policy-editor">Edit type defaults</a> for indexes or maximum active duration.</p>)
  end

  defp numeric_input_type(value) do
    case Integer.parse(to_string(value)) do
      {_integer, ""} -> "number"
      _ when value == "" -> "number"
      _ -> "text"
    end
  end

  defp numeric_error(name),
    do: ~s(<small class="flow-field-error" id="policy-#{name}-error" hidden></small>)

  def render_flow_policy_flash(%{kind: :ok, message: message, type: type}) do
    suffix = if type in [nil, ""], do: "", else: " for #{type}"
    ~s(<div class="flow-alert flow-alert-ok">#{escape(message <> suffix)}</div>)
  end

  def render_flow_policy_flash(%{kind: :error, message: message}) do
    ~s(<div class="flow-alert flow-alert-error" id="flow-policy-error" role="alert">#{escape(message)}. Your draft has been retained.</div>)
  end

  def render_flow_policy_flash(_flash), do: ""

  def render_flow_policy_preview(editor) do
    max_active_scope =
      if Map.get(editor, :state, "") == "",
        do: " for each new Flow record of this type.",
        else: "."

    """
    <div class="flow-policy-preview" style="display: none;">
      <div class="flow-policy-preview-title">Review before saving</div>
      <div>Scope: <span class="mono" data-policy-preview="scope"></span></div>
      <div>State mode: <span class="mono" data-policy-preview="mode"></span>. FIFO requires every entering Flow to carry a partition key and rejects priority.</div>
      <div>Indexes: <span class="mono" data-policy-preview="indexes"></span></div>
      <div>Retry: <span data-policy-preview="retry"></span></div>
      <div>Type-level max active: <span class="mono" data-policy-preview="max-active"></span>#{max_active_scope}</div>
      <div>Retention: <span data-policy-preview="retention"></span></div>
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

    invalid = invalid_selection(current, ~w(none fixed linear exponential))

    ~s(<select class="flow-search-input" name="backoff_kind" title="Retry delay strategy">#{invalid}#{options}</select>)
  end

  def render_flow_policy_mode_select(current, disabled \\ false) do
    current = current |> to_string() |> String.downcase()

    [
      {"parallel", "Parallel"},
      {"fifo", "FIFO"}
    ]
    |> Enum.map_join("\n", fn {mode, label} ->
      selected = if mode == current, do: ~s( selected), else: ""
      ~s(<option value="#{mode}"#{selected}>#{label}</option>)
    end)
    |> then(fn options ->
      invalid = invalid_selection(current, ~w(parallel fifo))

      ~s(<select class="flow-search-input" name="mode" title="State-level scheduling mode. FIFO applies only when State override is set."#{if disabled, do: " disabled", else: ""}>#{invalid}#{options}</select>)
    end)
  end

  defp invalid_selection(current, allowed) do
    if current in allowed,
      do: "",
      else:
        ~s(<option value="#{escape_attr(current)}" selected>Invalid selection: #{escape(current)}</option>)
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
        command: "FLOW.POLICY.SET <type> MAX_ACTIVE_MS <ms|INFINITY>",
        scope: "Flow type",
        mutability: "read-write",
        notes:
          "Sets the maximum runtime copied onto new Flow records. INFINITY disables active runtime expiry."
      },
      %{
        command: "FLOW.POLICY.SET <type> STATE <state> MODE FIFO|PARALLEL",
        scope: "Flow state",
        mutability: "read-write",
        notes:
          "Sets state-level scheduling mode. FIFO preserves per-partition order, requires partition keys, and rejects priority."
      },
      %{
        command: "FLOW.POLICY.SET <type> INDEXED_ATTRIBUTES <names> INDEXED_STATE_META <key>",
        scope: "Flow type",
        mutability: "read-write",
        notes: "Configures type-level metadata indexes used by bounded FLOW.QUERY plans."
      },
      %{
        command: "FLOW.POLICY.GET <type> [STATE <state>]",
        scope: "Flow type",
        mutability: "read-only",
        notes:
          "Reads the effective active-runtime, retry, and retention policy, including state overrides."
      }
    ]
  end

  def render_flow_policies_table(policies, policy_scan) do
    rows =
      case policies do
        [] ->
          """
          <tr>
            <td colspan="11" class="c-muted">No Flow types or policy overrides found in the current sample.</td>
          </tr>
          """

        _ ->
          Enum.map_join(policies, "\n", &render_flow_policy_row/1)
      end

    scan_note = render_flow_policy_scan_note(policy_scan)

    """
    <h2 class="section-title">Current Flow Policies <span class="badge badge-idle">#{format_number(length(policies))} loaded</span></h2>
    #{scan_note}
    #{FerricstoreServer.Health.Dashboard.Render.TableFilter.controls("flow-policy-catalog", "Filter loaded policies")}
    <div class="table-scroll" role="region" aria-label="Current workflow policies" tabindex="0"><table id="flow-policy-catalog" class="flow-policy-table">
      <thead>
        <tr>
          <th>Type</th>
          <th>Source</th>
          <th>Generation</th>
          <th>Indexes</th>
          <th>Retries</th>
          <th>Backoff</th>
          <th>Exhausted To</th>
          <th>Max Active</th>
          <th>Retention</th>
          <th>State Overrides</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table></div>
    """
  end

  def render_flow_policy_scan_note(%{restricted: true}) do
    """
    <div class="flow-help">
      Shows effective policies limited to authorized Flow types discovered by the bounded policy scan.
    </div>
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
      <td colspan="9" class="c-red">#{escape(error)}</td>
    </tr>
    """
  end

  def render_flow_policy_row(row) do
    retry = Map.get(row, :retry, %{})
    retention = Map.get(row, :retention, %{})

    """
    <tr>
      <td class="mono"><a class="flow-link" href="#{escape_attr(flow_policy_edit_url(row.type))}" title="Edit policy for #{escape_attr(row.type)}">#{escape(row.type)}</a></td>
      <td><span class="badge #{flow_policy_source_class(row.source)}">#{escape(row.source)}</span></td>
      <td class="mono">#{escape(to_string(Map.get(row, :generation, "unavailable")))}</td>
      <td>#{render_flow_policy_indexes(row)}</td>
      <td>#{format_number(flow_policy_field(retry, :max_retries, 0))}</td>
      <td>#{escape(flow_policy_backoff_summary(flow_policy_field(retry, :backoff, %{})))}</td>
      <td class="mono">#{escape(to_string(flow_policy_field(retry, :exhausted_to, "failed")))}</td>
      <td>#{escape(flow_policy_max_active_summary(Map.get(row, :max_active_ms)))}</td>
      <td>#{escape(flow_policy_retention_summary(retention))}</td>
      <td>#{render_flow_policy_state_overrides(Map.get(row, :states, []), row.type)}</td>
      <td><a class="flow-search-button flow-policy-action" href="#{flow_policy_edit_url(row.type)}">Edit</a></td>
    </tr>
    """
  end

  def flow_policy_edit_url(type, state \\ "") do
    "/dashboard/flow/policies?" <>
      URI.encode_query(%{"edit" => type, "edit_state" => state}) <> "#flow-policy-editor"
  end

  def flow_policy_source_class("configured"), do: "badge-ok"
  def flow_policy_source_class(_source), do: "badge-idle"

  def render_flow_policy_state_overrides(states, type \\ nil)
  def render_flow_policy_state_overrides([], _type), do: ~s(<span class="c-muted">-</span>)

  def render_flow_policy_state_overrides(states, type) do
    render_state = fn state ->
      retry = Map.get(state, :retry, %{})
      retention = Map.get(state, :retention, %{})
      mode = Map.get(state, :mode, :parallel)

      title =
        "#{flow_policy_mode_label(mode)}, max retries #{flow_policy_field(retry, :max_retries, 0)}, " <>
          flow_policy_retention_summary(retention)

      label = "#{escape(state.state)} #{escape(flow_policy_mode_label(mode))}"

      if is_binary(type) do
        ~s(<a class="flow-pill" title="#{escape_attr(title)}" href="#{escape_attr(flow_policy_edit_url(type, state.state))}">#{label}</a>)
      else
        ~s(<span class="flow-pill" title="#{escape_attr(title)}">#{label}</span>)
      end
    end

    preview =
      states
      |> Enum.take(@flow_dashboard_policy_state_preview_limit)
      |> Enum.map_join("", render_state)

    extra = length(states) - @flow_dashboard_policy_state_preview_limit

    if extra > 0 do
      all =
        Enum.map_join(states, "", fn state ->
          ~s(<div data-policy-override-name="#{escape_attr(state.state)}">#{render_state.(state)}</div>)
        end)

      preview <>
        """
        <details class="flow-policy-overrides">
          <summary>+#{format_number(extra)} more; all #{length(states)} overrides</summary>
          <label class="flow-policy-field"><span>Find a state override</span><input class="flow-search-input" type="search" data-policy-override-search autocomplete="off"></label>
          <div class="flow-policy-override-list">#{all}</div>
          <p class="flow-section-note" data-policy-override-empty hidden>No matching state overrides.</p>
        </details>
        <script>
        (() => {
          const panel = document.currentScript.previousElementSibling;
          const search = panel.querySelector('[data-policy-override-search]');
          const rows = Array.from(panel.querySelectorAll('[data-policy-override-name]'));
          search.addEventListener('input', () => {
            const query = search.value.toLocaleLowerCase();
            rows.forEach(row => { row.hidden = !row.dataset.policyOverrideName.toLocaleLowerCase().includes(query); });
            panel.querySelector('[data-policy-override-empty]').hidden = rows.some(row => !row.hidden);
          });
        })();
        </script>
        """
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

  def flow_policy_max_active_summary(value) when is_integer(value) and value > 0,
    do: format_duration_ms(value)

  def flow_policy_max_active_summary(_value), do: "unlimited"

  def render_flow_policy_indexes(row) do
    attrs =
      row
      |> Map.get(:indexed_attributes, [])
      |> List.wrap()
      |> Enum.reject(&(&1 in [nil, ""]))
      |> Enum.map_join(", ", &to_string/1)

    state_meta = Map.get(row, :indexed_state_meta)

    parts =
      []
      |> flow_policy_maybe_index_part("attrs", attrs)
      |> flow_policy_maybe_index_part("state_meta", state_meta)
      |> Enum.reverse()

    case parts do
      [] ->
        ~s(<span class="c-muted">-</span>)

      parts ->
        Enum.map_join(parts, " ", fn part ->
          ~s(<span class="flow-pill">#{escape(part)}</span>)
        end)
    end
  end

  defp flow_policy_maybe_index_part(parts, _label, value) when value in [nil, ""], do: parts
  defp flow_policy_maybe_index_part(parts, label, value), do: ["#{label}: #{value}" | parts]

  def flow_policy_mode_label(:fifo), do: "FIFO"
  def flow_policy_mode_label("fifo"), do: "FIFO"
  def flow_policy_mode_label(_mode), do: "parallel"

  def flow_policy_field(map, key, default) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  def flow_policy_field(_map, _key, default), do: default
end
