defmodule FerricstoreServer.Health.Dashboard.Render.MessagingPages do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview

  def render_stream_activity_summary(data) do
    summary = Map.get(data, :summary, %{})

    render_ops_summary("Stream Activity", [
      %{label: "Mutations", value: format_number(Map.get(summary, :mutations, 0))},
      %{label: "Appends", value: format_number(Map.get(summary, :appends, 0))},
      %{label: "Consumer Events", value: format_number(Map.get(summary, :consumer_events, 0))},
      %{label: "Streams", value: format_number(Map.get(summary, :unique_streams, 0))},
      %{label: "Latest", value: timestamp_or_idle(Map.get(summary, :latest_at_us))}
    ])
  end

  def render_stream_top_streams(data) do
    rows = Map.get(data, :top_streams, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No stream mutations recorded yet.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.key)}</td>
              <td>#{format_number(row.mutations)}</td>
              <td>#{format_number(row.appends)}</td>
              <td class="mono">#{escape(row.last_entry_id || "-")}</td>
              <td class="mono">#{timestamp_or_dash(row.last_at_us)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Active Streams <span class="badge badge-idle">metadata only</span></div>
    #{table_scroll("Active streams", """
    <table>
      <thead><tr><th>Stream Key</th><th>Mutations</th><th>Appends</th><th>Last Entry</th><th>Last Seen</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  def render_stream_consumers(data) do
    groups = Map.get(data, :consumer_groups, [])

    body =
      case groups do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No active stream consumer groups loaded.</td></tr>)

        _ ->
          Enum.map_join(groups, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.key)}</td>
              <td class="mono">#{escape(row.group)}</td>
              <td>#{format_number(row.consumers)}</td>
              <td>#{format_number(row.pending)}</td>
              <td class="mono">#{escape(row.last_delivered || "-")}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Stream Consumers <span class="badge badge-idle">consumer groups</span></div>
    #{table_scroll("Stream consumers", """
    <table>
      <thead><tr><th>Stream Key</th><th>Group</th><th>Consumers</th><th>Pending</th><th>Last Delivered</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  def render_stream_waiters(data) do
    waiters = Map.get(data, :waiters, [])

    body =
      case waiters do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No blocked XREAD/XREADGROUP waiters.</td></tr>)

        _ ->
          Enum.map_join(waiters, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.key)}</td>
              <td>#{format_number(row.waiters)}</td>
              <td>#{format_duration_us(row.oldest_wait_us)}</td>
              <td class="mono">#{escape(row.last_seen_id || "-")}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Blocked Readers <span class="badge badge-idle">live waiters</span></div>
    #{table_scroll("Blocked stream readers", """
    <table>
      <thead><tr><th>Stream Key</th><th>Waiters</th><th>Oldest Wait</th><th>Last Seen ID</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  def render_stream_activity_log(data) do
    entries = Map.get(data, :entries, [])
    count_label = if entries == [], do: "none", else: "#{length(entries)} recent"

    body =
      case entries do
        [] ->
          ~s(<tr><td colspan="10" class="c-muted">No stream producer or consumer activity recorded yet.</td></tr>)

        _ ->
          Enum.map_join(entries, "\n", fn entry ->
            """
            <tr>
              <td>#{entry.id}</td>
              <td class="mono">#{timestamp_or_dash(entry.timestamp_us)}</td>
              <td>#{stream_role_badge(entry.role)}</td>
              <td class="mono">#{escape(entry.command)}</td>
              <td class="mono">#{escape(entry.key)}</td>
              <td>#{stream_result_badge(entry.result)}</td>
              <td class="mono">#{escape(entry.entry_id || "-")}</td>
              <td>#{stream_count_label(entry)}</td>
              <td class="mono">#{stream_consumer_label(entry)}</td>
              <td class="mono">#{escape(entry.trim || "-")}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Recent Stream Activity <span class="badge badge-idle">#{escape(count_label)}</span></div>
    #{table_scroll("Recent stream activity", """
    <table>
      <thead>
        <tr><th>ID</th><th>Time</th><th>Role</th><th>Command</th><th>Stream Key</th><th>Result</th><th>Entry ID</th><th>Shape</th><th>Consumer</th><th>Trim</th></tr>
      </thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    <div class="flow-filter-note">Payload field names and values are intentionally not recorded.</div>
    """
  end

  def render_pubsub_summary(data) do
    summary = Map.get(data, :summary, %{})

    render_ops_summary("Pub/Sub Activity", [
      %{label: "Channels", value: format_number(Map.get(summary, :channels, 0))},
      %{label: "Patterns", value: format_number(Map.get(summary, :patterns, 0))},
      %{
        label: "Subscriptions",
        value:
          format_number(
            Map.get(summary, :exact_subscriptions, 0) +
              Map.get(summary, :pattern_subscriptions, 0)
          )
      },
      %{label: "Subscribers", value: format_number(Map.get(summary, :active_subscribers, 0))}
    ])
  end

  def render_pubsub_channels(data) do
    rows = Map.get(data, :channels, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="2" class="c-muted">No active exact channel subscriptions.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.channel)}</td>
              <td>#{format_number(row.subscribers)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Channels <span class="badge badge-idle">active subscriptions</span></div>
    #{table_scroll("Pub/Sub channels", """
    <table>
      <thead><tr><th>Channel</th><th>Subscribers</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  def render_pubsub_patterns(data) do
    rows = Map.get(data, :patterns, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="2" class="c-muted">No active pattern subscriptions.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.pattern)}</td>
              <td>#{format_number(row.subscribers)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Patterns <span class="badge badge-idle">PSUBSCRIBE</span></div>
    #{table_scroll("Pub/Sub patterns", """
    <table>
      <thead><tr><th>Pattern</th><th>Subscribers</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  def render_pubsub_activity(data) do
    entries = Map.get(data, :activity, [])
    count_label = if entries == [], do: "none", else: "#{length(entries)} recent"

    body =
      case entries do
        [] ->
          ~s(<tr><td colspan="7" class="c-muted">No Pub/Sub publish or subscription activity recorded yet.</td></tr>)

        _ ->
          Enum.map_join(entries, "\n", fn entry ->
            """
            <tr>
              <td>#{entry.id}</td>
              <td class="mono">#{timestamp_or_dash(entry.timestamp_us)}</td>
              <td class="mono">#{escape(entry.command)}</td>
              <td>#{pubsub_target_badge(entry.target_type)}</td>
              <td class="mono">#{escape(entry.target)}</td>
              <td>#{pubsub_activity_shape(entry)}</td>
              <td>#{pubsub_publish_delivery(entry)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Recent Pub/Sub Activity <span class="badge badge-idle">#{escape(count_label)}</span></div>
    #{table_scroll("Recent Pub/Sub activity", """
    <table>
      <thead><tr><th>ID</th><th>Time</th><th>Command</th><th>Target Type</th><th>Target</th><th>Shape</th><th>Delivery</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    <div class="flow-filter-note">Published message bodies are intentionally not recorded.</div>
    """
  end

  defp table_scroll(label, table) do
    ~s(<div class="table-scroll" role="region" aria-label="#{escape_attr(label)}" tabindex="0">#{table}</div>)
  end

  defp stream_count_label(%{command: "XADD", field_pairs: n}) when is_integer(n),
    do: "#{format_number(n)} field pairs"

  defp stream_count_label(%{command: command, count: n})
       when command in ["XREAD", "XREADGROUP"] and is_integer(n),
       do: "#{format_number(n)} delivered"

  defp stream_count_label(%{command: "XACK", count: n}) when is_integer(n),
    do: "#{format_number(n)} acked"

  defp stream_count_label(%{command: command, count: n})
       when command in ["XTRIM", "XDEL"] and is_integer(n),
       do: "#{format_number(n)} deleted"

  defp stream_count_label(_entry), do: "-"

  defp stream_consumer_label(%{group: group, consumer: consumer})
       when is_binary(group) and is_binary(consumer),
       do: escape(group <> " / " <> consumer)

  defp stream_consumer_label(%{group: group}) when is_binary(group), do: escape(group)
  defp stream_consumer_label(_entry), do: "-"

  defp stream_role_badge(:producer), do: ~s(<span class="badge badge-ok">producer</span>)
  defp stream_role_badge(:consumer), do: ~s(<span class="badge badge-merging">consumer</span>)
  defp stream_role_badge(_role), do: ~s(<span class="badge badge-idle">maintenance</span>)

  defp stream_result_badge("ok"), do: ~s(<span class="badge badge-ok">ok</span>)

  defp stream_result_badge(result),
    do: ~s(<span class="badge badge-idle">#{escape(result)}</span>)

  defp pubsub_target_badge(:pattern), do: ~s(<span class="badge badge-merging">pattern</span>)
  defp pubsub_target_badge(_type), do: ~s(<span class="badge badge-idle">channel</span>)

  defp pubsub_activity_shape(%{command: "PUBLISH", message_bytes: bytes}) when is_integer(bytes),
    do: "#{format_bytes(bytes)} message"

  defp pubsub_activity_shape(%{targets: targets}) when is_integer(targets),
    do: "#{format_number(targets)} target(s)"

  defp pubsub_activity_shape(_entry), do: "-"

  defp pubsub_publish_delivery(%{command: "PUBLISH", subscribers: subscribers})
       when is_integer(subscribers),
       do: "#{format_number(subscribers)} receiver(s)"

  defp pubsub_publish_delivery(_entry), do: "-"

  defp timestamp_or_idle(nil), do: "idle"
  defp timestamp_or_idle(timestamp_us), do: timestamp_or_dash(timestamp_us)

  defp timestamp_or_dash(timestamp_us) when is_integer(timestamp_us) and timestamp_us > 0 do
    format_timestamp_us(timestamp_us)
  rescue
    _ -> "-"
  end

  defp timestamp_or_dash(_timestamp_us), do: "-"
end
