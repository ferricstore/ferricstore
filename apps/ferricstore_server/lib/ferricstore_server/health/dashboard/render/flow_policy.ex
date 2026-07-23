defmodule FerricstoreServer.Health.Dashboard.Render.FlowPolicy do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]

  @flow_dashboard_policy_state_preview_limit 6

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

    indexed_attributes =
      policy |> Map.get(:indexed_attributes, []) |> flow_policy_indexed_attributes_string()

    %{
      type: type,
      state: "",
      mode: :parallel,
      indexed_attributes: indexed_attributes,
      indexed_state_meta: flow_policy_field(policy, :indexed_state_meta, "") || "",
      max_retries: flow_policy_field(retry, :max_retries, 3),
      backoff_kind: flow_policy_field(backoff, :kind, :exponential),
      base_ms: flow_policy_field(backoff, :base_ms, 1_000),
      max_ms: flow_policy_field(backoff, :max_ms, 30_000),
      jitter_pct: flow_policy_field(backoff, :jitter_pct, 20),
      exhausted_to: flow_policy_field(retry, :exhausted_to, "failed"),
      max_active_ms: flow_policy_field(policy, :max_active_ms, nil) || "",
      retention_ttl_ms: flow_policy_field(retention, :ttl_ms, 604_800_000),
      history_max_events: flow_policy_field(retention, :history_max_events, 100_000)
    }
  end

  def flow_policy_clean_form_value(value) when is_binary(value), do: String.trim(value)
  def flow_policy_clean_form_value(value), do: value |> to_string() |> String.trim()

  def render_flow_policy_editor(data) do
    editor = Map.get(data, :editor, flow_policy_editor_data(""))
    flash = render_flow_policy_flash(Map.get(data, :flash))
    indexed_attributes = Map.get(editor, :indexed_attributes) || ""
    indexed_state_meta = Map.get(editor, :indexed_state_meta) || ""
    max_active_ms = Map.get(editor, :max_active_ms) || ""

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
            <span>State mode</span>
            #{render_flow_policy_mode_select(Map.get(editor, :mode, :parallel))}
          </label>
          <label class="flow-policy-field">
            <span>Indexed attrs</span>
            <input class="flow-search-input mono" type="text" name="indexed_attributes" value="#{escape_attr(indexed_attributes)}" autocomplete="off" placeholder="tenant, region" title="Comma-separated type-level indexed attributes used by FLOW.QUERY">
          </label>
          <label class="flow-policy-field">
            <span>Indexed state meta</span>
            <input class="flow-search-input mono" type="text" name="indexed_state_meta" value="#{escape_attr(indexed_state_meta)}" autocomplete="off" placeholder="risk_tier" title="Optional type-level state metadata key used by FLOW.QUERY">
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
            <span>Max active ms</span>
            <input class="flow-search-input mono" type="number" name="max_active_ms" min="1" max="31536000000" value="#{escape_attr(to_string(max_active_ms))}" placeholder="unlimited" title="Maximum runtime for new active Flow records; leave blank for unlimited">
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
    mode = flow_policy_mode_label(Map.get(editor, :mode, :parallel))
    indexed_attributes = Map.get(editor, :indexed_attributes) || ""
    indexed_state_meta = Map.get(editor, :indexed_state_meta) || ""

    max_active =
      case Map.get(editor, :max_active_ms) do
        value when is_integer(value) and value > 0 -> format_duration_ms(value)
        _ -> "unlimited"
      end

    """
    <div class="flow-policy-preview">
      <div class="flow-policy-preview-title">Review before saving</div>
      <div>Scope: <span class="mono">#{escape(scope)}</span></div>
      <div>State mode: <span class="mono">#{escape(mode)}</span>. FIFO requires every entering Flow to carry a partition key and rejects priority.</div>
      <div>Indexes: attributes <span class="mono">#{escape(if(indexed_attributes == "", do: "-", else: indexed_attributes))}</span>, state meta <span class="mono">#{escape(if(indexed_state_meta == "", do: "-", else: indexed_state_meta))}</span></div>
      <div>Retry: #{format_number(Map.get(editor, :max_retries, 0))} attempts, #{escape(to_string(Map.get(editor, :backoff_kind, :exponential)))} backoff, exhausted to <span class="mono">#{escape(to_string(Map.get(editor, :exhausted_to, "failed")))}</span></div>
      <div>Max active: <span class="mono">#{escape(max_active)}</span> for each new Flow record of this type.</div>
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

  def render_flow_policy_mode_select(current) do
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
      ~s(<select class="flow-search-input" name="mode" title="State-level scheduling mode. FIFO applies only when State override is set.">#{options}</select>)
    end)
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
            <td colspan="10" class="c-muted">No Flow types or policy overrides found in the current sample.</td>
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
    </table>
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
      <td colspan="8" class="c-red">#{escape(error)}</td>
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
      <td>#{render_flow_policy_indexes(row)}</td>
      <td>#{format_number(flow_policy_field(retry, :max_retries, 0))}</td>
      <td>#{escape(flow_policy_backoff_summary(flow_policy_field(retry, :backoff, %{})))}</td>
      <td class="mono">#{escape(to_string(flow_policy_field(retry, :exhausted_to, "failed")))}</td>
      <td>#{escape(flow_policy_max_active_summary(Map.get(row, :max_active_ms)))}</td>
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
        mode = Map.get(state, :mode, :parallel)

        title =
          "#{flow_policy_mode_label(mode)}, max retries #{flow_policy_field(retry, :max_retries, 0)}, " <>
            flow_policy_retention_summary(retention)

        ~s(<span class="flow-pill" title="#{escape_attr(title)}">#{escape(state.state)} #{escape(flow_policy_mode_label(mode))}</span>)
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

  defp flow_policy_default_response(type) do
    %{
      type: type,
      max_active_ms: nil,
      retry: Ferricstore.Flow.RetryPolicy.default(),
      retention:
        Ferricstore.Flow.RetryPolicy.default_retention()
        |> Map.delete(:history_hot_max_events),
      indexed_attributes: [],
      indexed_state_meta: nil,
      states: %{}
    }
  end

  defp flow_policy_indexed_attributes_string(names) when is_list(names) do
    names
    |> Enum.reject(&(&1 in [nil, ""]))
    |> Enum.map_join(", ", &to_string/1)
  end

  defp flow_policy_indexed_attributes_string(_names), do: ""
end
