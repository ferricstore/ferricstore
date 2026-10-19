defmodule FerricstoreServer.Health.Dashboard.Render.MessagingPages do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview
  alias FerricstoreServer.Health.Dashboard.Render.{PageStatus, TableFilter}

  def render_stream_activity_summary(data) do
    summary = Map.get(data, :summary, %{})

    mutations = Map.get(summary, :mutations, 0)
    streams = Map.get(summary, :unique_streams, 0)

    status =
      if mutations in [0, nil] do
        PageStatus.render("streams", :idle, "No visible stream activity in the retained window")
      else
        PageStatus.render(
          "streams",
          :idle,
          "#{format_number(mutations)} mutations across #{format_number(streams)} #{if streams == 1, do: "stream", else: "streams"}",
          [
            "#{format_number(Map.get(summary, :appends, 0))} appends",
            "#{format_number(Map.get(summary, :consumer_events, 0))} consumer events"
          ]
        )
      end

    status <>
      render_ops_summary(
        "Stream Activity",
        [
          %{label: "Mutations", value: format_number(mutations)},
          %{label: "Appends", value: format_number(Map.get(summary, :appends, 0))},
          %{
            label: "Consumer events",
            value: format_number(Map.get(summary, :consumer_events, 0))
          },
          %{label: "Streams", value: format_number(streams)},
          latest_stream_card(Map.get(summary, :latest_at_us))
        ],
        heading: :hidden
      ) <> snapshot_scope_note(:streams)
  end

  def render_stream_top_streams(data) do
    rows = Map.get(data, :top_streams, [])

    body =
      case rows do
        [] ->
          ""

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
    #{filtered_table(rows, "stream-top-table", "Filter loaded streams", "Active streams", """
    <table id="stream-top-table" data-dashboard-row-count="#{length(rows)}">
      <thead><tr><th>Stream key</th><th>Mutations</th><th>Appends</th><th>Last entry</th><th>Last seen</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible stream mutations in the retained activity window.")}
    """
  end

  def render_stream_consumers(data) do
    groups = Map.get(data, :consumer_groups, [])

    body =
      case groups do
        [] ->
          ""

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
    #{filtered_table(groups, "stream-consumers-table", "Filter loaded streams and consumer groups", "Stream consumers", """
    <table id="stream-consumers-table" data-dashboard-row-count="#{length(groups)}">
      <thead><tr><th>Stream key</th><th>Group</th><th>Consumers</th><th>Pending</th><th>Last delivered</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No active stream consumer groups loaded.")}
    """
  end

  def render_stream_waiters(data) do
    waiters = Map.get(data, :waiters, [])

    body =
      case waiters do
        [] ->
          ""

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
    #{filtered_table(waiters, "stream-waiters-table", "Filter loaded blocked stream readers", "Blocked stream readers", """
    <table id="stream-waiters-table" data-dashboard-row-count="#{length(waiters)}">
      <thead><tr><th>Stream key</th><th>Waiters</th><th>Oldest wait</th><th>Last seen ID</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible blocked readers in this bounded snapshot.")}
    """
  end

  def render_stream_activity_log(data) do
    entries = Map.get(data, :entries, [])

    body =
      case entries do
        [] ->
          ""

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
    #{filtered_table(entries, "stream-activity-table", "Filter loaded stream activity", "Recent stream activity", """
    <table id="stream-activity-table" data-dashboard-row-count="#{length(entries)}">
      <thead>
        <tr><th>ID</th><th>Time</th><th>Role</th><th>Command</th><th>Stream key</th><th>Result</th><th>Entry ID</th><th>Shape</th><th>Consumer</th><th>Trim</th></tr>
      </thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible stream activity in the last 128 retained events.")}
    <div class="flow-filter-note">Payload field names and values are intentionally not recorded.</div>
    """
  end

  def render_pubsub_summary(data) do
    summary = Map.get(data, :summary, %{})

    channels = Map.get(summary, :channels, 0)
    patterns = Map.get(summary, :patterns, 0)

    subscriptions =
      Map.get(summary, :exact_subscriptions, 0) + Map.get(summary, :pattern_subscriptions, 0)

    status =
      if subscriptions == 0 do
        PageStatus.render("pubsub", :idle, "No visible subscriptions")
      else
        subscribers =
          case Map.get(summary, :active_subscribers) do
            count when is_integer(count) ->
              ["#{format_number(count)} #{if count == 1, do: "subscriber", else: "subscribers"}"]

            _unknown ->
              []
          end

        PageStatus.render(
          "pubsub",
          :idle,
          "#{format_number(subscriptions)} #{if subscriptions == 1, do: "subscription", else: "subscriptions"} on #{format_number(channels)} #{if channels == 1, do: "channel", else: "channels"} and #{format_number(patterns)} #{if patterns == 1, do: "pattern", else: "patterns"}",
          subscribers
        )
      end

    status <>
      render_ops_summary(
        "Pub/Sub Activity",
        [
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
          %{
            label: "Subscribers",
            value: format_optional_number(Map.get(summary, :active_subscribers, 0))
          }
        ],
        heading: :hidden
      ) <> snapshot_scope_note(:pubsub)
  end

  defp format_optional_number(nil), do: "-"
  defp format_optional_number(value), do: format_number(value)

  def render_pubsub_channels(data) do
    rows = Map.get(data, :channels, [])

    body =
      case rows do
        [] ->
          ""

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
    <h2 class="section-title">Channels <span class="badge badge-idle">active subscriptions</span></h2>
    #{filtered_table(rows, "pubsub-channels-table", "Filter loaded channels", "Pub/Sub channels", """
    <table id="pubsub-channels-table">
      <thead><tr><th>Channel</th><th>Subscribers</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible exact channel subscriptions in this bounded snapshot.")}
    """
  end

  def render_pubsub_patterns(data) do
    rows = Map.get(data, :patterns, [])

    body =
      case rows do
        [] ->
          ""

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
    <h2 class="section-title">Patterns <span class="badge badge-idle">PSUBSCRIBE</span></h2>
    #{filtered_table(rows, "pubsub-patterns-table", "Filter loaded subscription patterns", "Pub/Sub patterns", """
    <table id="pubsub-patterns-table">
      <thead><tr><th>Pattern</th><th>Subscribers</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible pattern subscriptions in this bounded snapshot.")}
    """
  end

  def render_pubsub_activity(data) do
    entries = Map.get(data, :activity, [])
    count_label = if entries == [], do: "none", else: "#{length(entries)} recent"

    body =
      case entries do
        [] ->
          ""

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
    <h2 class="section-title">Recent Pub/Sub activity <span class="badge badge-idle">#{escape(count_label)}</span></h2>
    #{filtered_table(entries, "pubsub-activity-table", "Filter loaded Pub/Sub activity", "Recent Pub/Sub activity", """
    <table id="pubsub-activity-table">
      <thead><tr><th>ID</th><th>Time</th><th>Command</th><th>Target type</th><th>Target</th><th>Shape</th><th>Delivery</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """, "No visible Pub/Sub activity in the last 128 retained events.")}
    <div class="flow-filter-note">Published message bodies are intentionally not recorded.</div>
    """
  end

  # An empty bounded table is a one-line prompt; its filter appears with its rows.
  defp filtered_table([], _id, _filter_label, _region, _table, empty),
    do: ~s(<p class="section-empty">#{escape(empty)}</p>)

  defp filtered_table(_rows, id, filter_label, region, table, _empty),
    do: TableFilter.controls(id, filter_label) <> "\n" <> table_scroll(region, table)

  defp table_scroll(label, table) do
    ~s(<div class="table-scroll" role="region" aria-label="#{escape_attr(label)}" tabindex="0">#{table}</div>)
  end

  defp snapshot_scope_note(:streams),
    do:
      ~s(<p class="flow-section-note">Activity covers the last 128 retained events. Consumer groups and blocked readers show up to 100 visible rows, inspecting at most 10,000 group entries or stream keys. Matching data outside these bounds may be absent.</p>)

  defp snapshot_scope_note(:pubsub),
    do:
      ~s(<p class="flow-section-note">Channel and pattern tables show up to 100 visible rows from at most 10,000 entries per catalog. Activity covers the last 128 retained events. Matching data outside these bounds may be absent.</p>)

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

  defp latest_stream_card(nil),
    do: %{label: "Latest", value: "idle", class: "ops-summary-code c-muted"}

  defp latest_stream_card(timestamp_us) when is_integer(timestamp_us) and timestamp_us > 0 do
    case DateTime.from_unix(timestamp_us, :microsecond) do
      {:ok, datetime} ->
        full = DateTime.to_iso8601(datetime)

        time =
          datetime |> DateTime.truncate(:millisecond) |> DateTime.to_time() |> Time.to_iso8601()

        %{
          label: "Latest",
          value: datetime |> DateTime.to_date() |> Date.to_iso8601(),
          class: "ops-summary-timestamp",
          detail_html:
            ~s(<time class="stream-latest-time" datetime="#{full}" title="#{format_timestamp_us(timestamp_us)}">#{time} UTC</time>)
        }

      {:error, _} ->
        %{label: "Latest", value: "-", class: "ops-summary-code c-muted"}
    end
  end

  defp latest_stream_card(_),
    do: %{label: "Latest", value: "-", class: "ops-summary-code c-muted"}

  defp timestamp_or_dash(timestamp_us) when is_integer(timestamp_us) and timestamp_us > 0 do
    format_timestamp_us(timestamp_us)
  rescue
    _ -> "-"
  end

  defp timestamp_or_dash(_timestamp_us), do: "-"
end
