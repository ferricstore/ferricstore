defmodule FerricstoreServer.Health.Dashboard.Render.FlowTables do

import FerricstoreServer.Health.Dashboard.Format
import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory
import FerricstoreServer.Health.Dashboard.Render.FlowFilters

@flow_terminal_states ~w(completed failed cancelled)

  def default_flow_projection_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: 0,
      history_flush_interval_ms: 0,
      metrics: []
    }
  end

  def flow_projection_rollup(metrics) do
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

  def flow_projection_health_class(%{failures: failures, degraded: degraded})
      when failures > 0 or degraded > 0,
      do: "c-red"

  def flow_projection_health_class(%{lag: lag, pending_ops: pending_ops})
      when lag > 0 or pending_ops > 0,
      do: "c-yellow"

  def flow_projection_health_class(_rollup), do: "c-green"

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

  defp projection_metric_shard(name) do
    case Regex.run(~r/shard_index="([^"]+)"/, name) do
      [_all, shard] -> shard
      _ -> "all"
    end
  end

  defp projection_metric_field(name) do
    cond do
      String.contains?(name, "replay_safe_index") -> :replay_safe_index
      String.contains?(name, "requested_index") -> :requested_index
      String.contains?(name, "lag") -> :lag
      String.contains?(name, "pending_ops") -> :pending_ops
      String.contains?(name, "oldest_pending_age_us") -> :oldest_pending_age_us
      String.contains?(name, "persist_failures") -> :persist_failures
      String.contains?(name, "enqueue_failures") -> :enqueue_failures
      String.contains?(name, "flush_failures") -> :flush_failures
      String.contains?(name, "degraded") -> :degraded
      true -> :unknown
    end
  end

  defp numeric_metric_value(value) when is_integer(value), do: value
  defp numeric_metric_value(value) when is_float(value), do: round(value)

  defp numeric_metric_value(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, _rest} -> integer
      :error -> 0
    end
  end

  defp numeric_metric_value(_value), do: 0

  def render_flow_states_table(
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

  def flow_state_operational_hint(state) do
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

  def render_flow_state_breakdown(types) do
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

  def render_flow_custom_states(states) when is_map(states) do
    states
    |> Enum.sort_by(fn {state, _count} -> state end)
    |> Enum.map_join(" ", fn {state, count} ->
      ~s(<span class="flow-pill">#{escape(state)} #{format_number(count)}</span>)
    end)
  end

  def render_flow_custom_states(_states), do: ""

  def render_flow_workers(workers) do
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

  def render_flow_running_records(records, total_sampled, sample_limit) do
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

  def render_flow_signals_table(signals, total_sampled, filtered_sampled, sample_limit, filters) do
    render_flow_signals_table(
      signals,
      total_sampled,
      filtered_sampled,
      sample_limit,
      filters,
      :page
    )
  end

  def render_flow_signals_table(
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

  def render_flow_signal_row(row, :detail) do
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

  def render_flow_signal_row(row, :page) do
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

  def render_flow_signals_table_head(:detail) do
    """
    <tr><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Jump</th></tr>
    """
  end

  def render_flow_signals_table_head(:page) do
    """
    <tr><th>Flow</th><th>Type</th><th>Event</th><th>Time</th><th>Signal</th><th>State Change</th><th>Values</th><th>Open</th></tr>
    """
  end

  def flow_signals_table_title(nil, _filtered_sampled, _sample_limit, _filters, :detail),
    do: "Signals"

  def flow_signals_table_title(total_sampled, filtered_sampled, sample_limit, filters, :page)
       when is_integer(total_sampled) and is_integer(filtered_sampled) and
              is_integer(sample_limit) do
    "Flow Signals <span class=\"badge badge-idle\">#{escape(flow_signals_filter_summary(filters))}</span> <span class=\"badge badge-idle\">sampled #{format_number(filtered_sampled)} / #{format_number(total_sampled)} / #{format_number(sample_limit)}</span>"
  end

  def flow_signals_table_title(
         _total_sampled,
         _filtered_sampled,
         _sample_limit,
         _filters,
         _mode
       ),
       do: "Signals"

  def flow_signal_state_move_html(row) do
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

  def flow_signal_event_href(row, :detail) do
    "#" <> flow_history_event_anchor(Map.get(row, :event_id, "-"))
  end

  def flow_signal_event_href(row, :page) do
    anchor = flow_history_event_anchor(Map.get(row, :event_id, "-"))
    id = Map.get(row, :id, "")

    case id do
      "" -> "#" <> anchor
      id -> flow_detail_path(id, Map.get(row, :partition_key)) <> "#" <> anchor
    end
  end

  def flow_signal_refs_summary_html(row, mode) do
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

  def render_flow_due_records(title, records, total_sampled, sample_limit) do
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

  def render_flow_failures_rows([]) do
    ~s(<tr><td colspan="9" class="c-muted">No failed, exhausted, or expired-lease records found in the current bounded view.</td></tr>)
  end

  def render_flow_failures_rows(records) do
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

  def render_flow_lineage_hints([]), do: ""

  def render_flow_lineage_hints(hints) do
    links =
      Enum.map_join(hints, " ", fn hint ->
        params = %{"mode" => hint.mode, "id" => hint.id}
        href = "/dashboard/flow/lineage?" <> URI.encode_query(params)

        ~s(<a class="flow-pill flow-link" href="#{href}">#{escape(hint.label)} #{escape(hint.id)}</a>)
      end)

    ~s(<div class="flow-section-note">Recent lineage hints: #{links}</div>)
  end

  def flow_lineage_result_label(%{status: :idle, message: message}), do: message
  def flow_lineage_result_label(%{status: :ok, command: command}), do: "#{command} result"
  def flow_lineage_result_label(%{status: status, message: message}), do: "#{status}: #{message}"
  def flow_lineage_result_label(_result), do: "lineage result"

  def render_flow_lineage_nodes([], filters) do
    target = Map.get(filters, :target)

    if is_binary(target) and target != "" do
      ~s(<div class="flow-lineage-empty">No lineage records matched this query.</div>)
    else
      ~s(<div class="flow-lineage-empty">Choose parent, root, or correlation and enter an id.</div>)
    end
  end

  def render_flow_lineage_nodes(records, _filters) do
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

  def render_flow_lineage_rows([]) do
    ~s(<tr><td colspan="8" class="c-muted">No lineage records loaded.</td></tr>)
  end

  def render_flow_lineage_rows(records) do
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

  def flow_query_result_command(%{command: command}) when is_binary(command), do: command
  def flow_query_result_command(_result), do: "FLOW.QUERY"

  def render_flow_query_status(%{status: :ok, message: message}),
    do: ~s(<div class="flow-alert flow-alert-ok">#{escape(message)}</div>)

  def render_flow_query_status(%{status: :idle, message: message}),
    do: ~s(<div class="flow-section-note">#{escape(message)}</div>)

  def render_flow_query_status(%{status: _status, message: message}),
    do: ~s(<div class="flow-alert flow-alert-error">#{escape(message)}</div>)

  def render_flow_query_status(_result), do: ""

  def render_flow_query_rows(%{rows: []}) do
    ~s(<tr><td colspan="6" class="c-muted">No rows.</td></tr>)
  end

  def render_flow_query_rows(%{command: "FLOW.HISTORY", rows: rows}) do
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

  def render_flow_query_rows(%{rows: rows}) do
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

  def render_flow_projection_health(data) do
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

  def render_flow_recent_records(records, limit \\ nil) do
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

end
