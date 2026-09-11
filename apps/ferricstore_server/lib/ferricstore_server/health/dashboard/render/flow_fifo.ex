defmodule FerricstoreServer.Health.Dashboard.Render.FlowFifo do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.FlowHistory, only: [render_flow_id_link: 2]
  alias FerricstoreServer.Health.Dashboard.Render.FlowNavigation
  @lane_preview_limit 40

  def render(lanes, total_sampled, sample_limit, coverage \\ %{}) do
    unavailable? = Map.get(coverage, :status, :ok) != :ok

    rows =
      if lanes == [] do
        message =
          if unavailable?,
            do: "FIFO lanes could not be established for types with unavailable policies.",
            else: "No FIFO lanes discovered in the current bounded sample."

        ~s(<tr><td colspan="4" class="c-muted">#{message}</td></tr>)
      else
        lanes |> Enum.take(@lane_preview_limit) |> Enum.map_join("", &lane_row/1)
      end

    limit_note =
      if length(lanes) > @lane_preview_limit do
        ~s(<p class="flow-section-note">Showing #{@lane_preview_limit} of #{length(lanes)} sampled lanes. Narrow the type, state, or partition to inspect another lane.</p>)
      else
        ""
      end

    """
    <h2 class="section-title">FIFO Lanes <span class="badge badge-idle">#{if unavailable?, do: "#{length(lanes)} verified lanes", else: length(lanes)}</span> <span class="badge badge-idle">#{sampled_scan_label(total_sampled, sample_limit)}</span></h2>
    #{coverage_notice(coverage)}
    <p class="flow-section-note">Observed members, not the complete queue. Filters and sample bounds can omit an earlier member or blocker; due time alone does not establish claimability.</p>
    #{limit_note}
    <div class="table-scroll" role="region" aria-label="FIFO lanes" tabindex="0">
      <table class="flow-fifo-table">
        <thead><tr><th scope="col">Lane</th><th scope="col">Observed activity</th><th scope="col">Sample counts</th><th scope="col">Observed members</th></tr></thead>
        <tbody>#{rows}</tbody>
      </table>
    </div>
    """
  end

  defp coverage_notice(%{status: status, unavailable_types: types}) when status != :ok do
    """
    <div class="pressure-alert level-warning" role="status"><div class="pressure-details"><strong>FIFO coverage unavailable#{if status == :partial, do: " for some types", else: ""}</strong><p>Policy lookup failed for <span class="mono">#{types |> Enum.join(", ") |> escape()}</span>. These types may contain FIFO lanes; execution mode is unavailable.</p></div><button type="button" class="flow-search-button" data-dashboard-refresh>Retry current scope</button></div>
    """
  end

  defp coverage_notice(_coverage), do: ""

  defp lane_row(lane) do
    status = Map.get(lane, :head_status, "idle")

    label =
      case status do
        "head claimable" -> "Due in sample"
        "waiting for schedule" -> "Scheduled in sample"
        other -> other
      end

    class = if status == "blocked by expired lease", do: "badge-pressure", else: "badge-idle"
    partition = Map.get(lane, :partition_key)
    head = Map.get(lane, :blocked_by_id) || Map.get(lane, :head_id)

    head_label =
      if Map.get(lane, :blocked_by_id), do: "Observed blocker", else: "Earliest observed"

    """
    <tr>
      <th scope="row" class="flow-fifo-lane">
        <strong class="mono">#{escape(Map.get(lane, :type, ""))}</strong>
        <span class="mono">#{escape(Map.get(lane, :state, ""))}</span>
        <span class="mono c-muted">#{escape(partition || "Unknown partition")}</span>
      </th>
      <td class="flow-fifo-activity">
        <span class="badge #{class}">#{escape(label)}</span>
        <div>#{head_label}: #{flow_link(head, partition)}</div>
        #{blocker_details(lane)}
      </td>
      <td class="flow-fifo-counts">#{Map.get(lane, :running, 0)} leased<br>#{Map.get(lane, :waiting, 0)} waiting<br>#{Map.get(lane, :due, 0)} due</td>
      <td>#{members(lane)}</td>
    </tr>
    """
  end

  defp blocker_details(%{blocked_by_id: id} = lane) when is_binary(id) do
    """
    <div class="c-muted">Worker: <span class="mono">#{escape(Map.get(lane, :blocked_by_worker) || "Unknown")}</span></div>
    <div class="c-muted">Lease expires: #{format_timestamp_ms_or_dash(Map.get(lane, :lease_expires_at_ms))}</div>
    """
  end

  defp blocker_details(_lane), do: ""

  defp members(lane) do
    members = Map.get(lane, :members, [])
    partition = Map.get(lane, :partition_key)
    rows = Enum.map_join(members, "", &member(&1, partition))
    omitted = Map.get(lane, :members_omitted, 0)

    extra =
      if omitted > 0,
        do: ~s(<p class="flow-section-note">#{omitted} more sampled members not shown.</p>),
        else: ""

    order =
      if Map.get(lane, :order_known, false),
        do: "Leased members first, then state-entry sequence.",
        else: "Sequence unavailable for some members; full ordering is unknown."

    key =
      {Map.get(lane, :type), Map.get(lane, :state), partition}
      |> :erlang.term_to_binary()
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    if members == [] do
      case FlowNavigation.lane_path(lane) do
        nil -> flow_link(Map.get(lane, :waiting_head_id), partition)
        path -> ~s(<a class="flow-link" href="#{escape_attr(path)}">Inspect lane</a>)
      end
    else
      """
      <details class="flow-fifo-members" data-dashboard-disclosure-key="#{key}" data-dashboard-live-pause>
        <summary>#{length(members)} observed member#{if length(members) == 1, do: "", else: "s"}</summary>
        <p class="flow-section-note">#{order}</p>
        <ol class="flow-fifo-member-list">#{rows}</ol>
        #{extra}
        #{FlowNavigation.related_runs_link(lane)}
      </details>
      """
    end
  end

  defp member(member, partition) do
    {label, class} =
      case member.status do
        :expired -> {"Lease expired", "badge-pressure"}
        :leased -> {"Leased", "badge-idle"}
        :scheduled -> {"Scheduled", "badge-idle"}
        :due -> {"Due in sample", "badge-idle"}
        _ -> {"Waiting", "badge-idle"}
      end

    sequence =
      if is_integer(member.state_enter_seq),
        do: "Sequence #{member.state_enter_seq}",
        else: "Sequence unavailable"

    timing =
      if member.status in [:leased, :expired],
        do: "Lease: #{format_timestamp_ms_or_dash(member.lease_expires_at_ms)}",
        else: "Due: #{format_timestamp_ms_or_dash(member.run_at_ms)}"

    """
    <li>
      <div class="flow-fifo-member-heading">#{flow_link(member.id, partition)}<span class="badge #{class}">#{label}</span></div>
      <span class="flow-fifo-member-sequence mono">#{sequence}</span>
      <span class="flow-fifo-member-timing">#{timing}</span>
    </li>
    """
  end

  defp flow_link(id, partition)
       when is_binary(id) and id != "" and is_binary(partition) and partition != "",
       do: render_flow_id_link(id, partition)

  defp flow_link(id, _partition) when is_binary(id), do: escape(id)
  defp flow_link(_id, _partition), do: "Unknown"
end
