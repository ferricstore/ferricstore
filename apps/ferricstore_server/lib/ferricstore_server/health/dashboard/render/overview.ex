defmodule FerricstoreServer.Health.Dashboard.Render.Overview do
  import FerricstoreServer.Health.Dashboard.Format

  alias FerricstoreServer.Health.Dashboard.Render.RecentRates

  @doc """
  The overview page body: what needs attention, the headline numbers,
  workflows and shards, and small summaries that link to their pages.
  """
  def render_overview_content(data) do
    """
    #{render_operator_attention(data)}
    #{render_overview_metrics(data)}
    <div class="overview-panels">
      #{render_overview_workflows(Map.get(data, :flow_summary, %{}))}
      #{render_overview_shards(Map.get(data, :shards, []), Map.get(data, :cluster, %{}))}
    </div>
    #{render_overview_cards(data)}
    #{render_memory_alert(Map.get(data, :memory, %{}))}
    """
  end

  # ---------------------------------------------------------------------------
  # Needs attention
  # ---------------------------------------------------------------------------

  def render_operator_attention(data) do
    case operator_attention_items(data) do
      [] ->
        """
        <section class="overview-attention is-healthy" aria-label="Health">
          #{icon(:check)}
          <strong>Healthy</strong>
          <span>No warnings in this snapshot.</span>
        </section>
        """

      items ->
        severity =
          if Enum.any?(items, &(&1.severity == :degraded)), do: "degraded", else: "warning"

        """
        <section class="overview-attention is-#{severity}" aria-labelledby="overview-attention-title">
          <div class="overview-attention-head">
            #{icon(:warning)}
            <h2 id="overview-attention-title">Needs attention</h2>
            <span class="overview-attention-count">#{length(items)}</span>
          </div>
          <ul class="overview-attention-list">#{Enum.map_join(items, "", &render_operator_attention_item/1)}</ul>
        </section>
        """
    end
  end

  defp operator_attention_items(data) do
    []
    |> add_status_attention(Map.get(data, :overview, %{}))
    |> add_policy_migration_attention(Map.get(data, :subsystem_health, %{}))
    |> add_shard_attention(Map.get(data, :shards, []))
    |> add_memory_attention(Map.get(data, :memory, %{}))
    |> add_keydir_attention(Map.get(data, :lifecycle, %{}))
    |> add_flow_attention(Map.get(data, :flow_summary, %{}))
    |> Enum.reverse()
  end

  defp add_status_attention(items, overview) do
    case Map.get(overview, :status, :ok) do
      :ok ->
        items

      _starting ->
        [
          %{
            severity: :warning,
            title: "The node is still starting",
            detail: "Numbers on this page may be incomplete until startup finishes.",
            href: nil,
            action: nil
          }
          | items
        ]
    end
  end

  defp add_keydir_attention(items, lifecycle) do
    if Map.get(lifecycle, :keydir_full, false) do
      [
        %{
          severity: :degraded,
          title: "The keydir is full",
          detail: "New writes are rejected until keys expire or the memory budget is raised.",
          href: "/dashboard/config",
          action: "Review config"
        }
        | items
      ]
    else
      items
    end
  end

  defp add_policy_migration_attention(items, subsystem_health) do
    policy = Map.get(subsystem_health, :policy_migration, %{})
    status = Map.get(policy, :status, :healthy)
    issues = Map.get(policy, :issues, [])

    cond do
      status in [:healthy, :disabled] ->
        items

      issues == [] ->
        [
          %{
            severity: if(status == :warning, do: :warning, else: :degraded),
            title: "Policy migration health unavailable",
            detail: "The policy migration worker has not published a current health snapshot.",
            href: "/dashboard/flow/policies",
            action: "Inspect policies"
          }
          | items
        ]

      true ->
        Enum.reduce(issues, items, fn issue, acc ->
          reason = Map.get(issue, :reason)

          title =
            if reason == :policy_catalog_state_projection_pending do
              "Policy migration waiting on its state projection"
            else
              "Policy migration reported #{format_health_reason(reason)}"
            end

          detail =
            "Shard #{Map.get(issue, :shard, "-")} · #{format_number(Map.get(issue, :occurrences, 1))} occurrence(s)"

          [
            %{
              severity: Map.get(issue, :severity, status),
              title: title,
              detail: detail,
              href: "/dashboard/flow/policies",
              action: "Inspect policies"
            }
            | acc
          ]
        end)
    end
  end

  defp add_shard_attention(items, shards) do
    Enum.reduce(shards, items, fn shard, acc ->
      if Map.get(shard, :status) == "ok" do
        acc
      else
        [
          %{
            severity: :degraded,
            title: "Shard #{Map.get(shard, :index, "-")} is unavailable",
            detail: "The shard process is not reporting an operational state.",
            href: "/dashboard/raft",
            action: "Inspect consensus"
          }
          | acc
        ]
      end
    end)
  end

  defp add_memory_attention(items, memory) do
    case Map.get(memory, :pressure_level, :normal) do
      :unavailable ->
        [
          %{
            severity: :warning,
            title: "Memory telemetry is unavailable",
            detail: "Memory pressure cannot be checked until the memory guard reports again.",
            href: nil,
            action: nil
          }
          | items
        ]

      level when level in [:warning, :pressure, :reject] ->
        [
          %{
            severity: if(level == :warning, do: :warning, else: :degraded),
            title: "Memory guard is #{level}",
            detail: "Admission or writes may be constrained until memory pressure falls.",
            href: "/dashboard",
            action: "Inspect memory"
          }
          | items
        ]

      _ ->
        items
    end
  end

  defp add_flow_attention(items, summary) do
    items =
      case Map.get(summary, :failed, 0) do
        count when is_integer(count) and count > 0 ->
          [
            %{
              severity: :warning,
              title: "#{format_number(count)} sampled failed workflows",
              detail: "Bounded sample; open Failures for focused triage.",
              href: "/dashboard/flow/failures",
              action: "Inspect failures"
            }
            | items
          ]

        _ ->
          items
      end

    case Map.get(summary, :expired_leases_sampled, 0) do
      count when is_integer(count) and count > 0 ->
        noun = if count == 1, do: "lease", else: "leases"

        [
          %{
            severity: :warning,
            title: "#{format_number(count)} sampled expired #{noun}",
            detail: "Expired running work may be eligible for guarded reclaim.",
            href: "/dashboard/flow/failures",
            action: "Inspect recovery"
          }
          | items
        ]

      _ ->
        items
    end
  end

  defp render_operator_attention_item(item) do
    severity = if item.severity == :degraded, do: "degraded", else: "warning"

    action =
      case item do
        %{href: href, action: action} when is_binary(href) and is_binary(action) ->
          ~s(<a class="flow-search-button" href="#{href}">#{escape(action)}</a>)

        _ ->
          ""
      end

    """
    <li class="overview-attention-item is-#{severity}">
      <div><strong>#{escape(item.title)}</strong><span>#{escape(item.detail)}</span></div>
      #{action}
    </li>
    """
  end

  defp format_health_reason(reason) when is_atom(reason),
    do: reason |> Atom.to_string() |> String.replace("_", " ")

  defp format_health_reason(reason) when is_binary(reason), do: reason
  defp format_health_reason(_reason), do: "an operational error"

  # ---------------------------------------------------------------------------
  # Headline numbers
  # ---------------------------------------------------------------------------

  @doc """
  Traffic measured in this tab, cache hit rate, memory and keys. Traffic is
  computed in the browser from the exact counters the page ships, so the
  cards sit inside the recent-rates root.
  """
  def render_overview_metrics(data) do
    RecentRates.render(data, [
      render_hit_rate_card(Map.get(data, :hotcold, %{})),
      render_memory_card(Map.get(data, :memory, %{})),
      render_keys_card(data)
    ])
  end

  defp render_hit_rate_card(hotcold) do
    sample_rate = Map.get(hotcold, :sample_rate, 1)

    if hotcold_has_samples?(hotcold) do
      ratio = Map.get(hotcold, :hit_ratio, 0.0)

      """
      <div class="overview-metric">
        <span class="overview-metric-label">Cache hit rate</span>
        <span class="overview-metric-value">#{ratio}%</span>
        <span class="overview-metric-note">#{escape(hit_source_note(hotcold))}#{sample_note(sample_rate)}</span>
        <div class="overview-meter" aria-hidden="true"><div style="width:#{min(ratio, 100)}%;background:#{hit_rate_color(ratio)};"></div></div>
      </div>
      """
    else
      """
      <div class="overview-metric">
        <span class="overview-metric-label">Cache hit rate</span>
        <span class="overview-metric-value is-empty">No read samples</span>
        <span class="overview-metric-note">Reads are sampled#{sample_note(sample_rate)}</span>
      </div>
      """
    end
  end

  # Where hits came from, only when there are hits to attribute.
  defp hit_source_note(hotcold) do
    hits =
      case Map.get(hotcold, :total_hits) do
        hits when is_number(hits) ->
          hits

        _ ->
          case {Map.get(hotcold, :total_hot), Map.get(hotcold, :total_cold)} do
            {hot, cold} when is_number(hot) and is_number(cold) -> hot + cold
            _ -> 0
          end
      end

    if hits > 0 do
      "#{Map.get(hotcold, :ram_ratio, 0.0)}% of hits from RAM, #{Map.get(hotcold, :disk_ratio, 0.0)}% from disk"
    else
      "No hits sampled yet"
    end
  end

  defp sample_note(rate) when is_integer(rate) and rate > 1, do: " · sampled 1:#{rate}"
  defp sample_note(_rate), do: ""

  defp render_memory_card(memory) do
    rss = Map.get(memory, :rss_bytes, 0)
    max_bytes = Map.get(memory, :max_bytes, 0)

    tracked =
      "Tracked allocations #{format_bytes(Map.get(memory, :total_bytes, 0))} of #{if max_bytes > 0, do: format_bytes(max_bytes), else: "no limit"}"

    if is_integer(rss) and rss > 0 do
      limit = if max_bytes > 0, do: Map.get(memory, :memory_limit, 0), else: 0
      pct = if limit > 0, do: Float.round(Map.get(memory, :rss_ratio, 0.0) * 100, 1), else: 0.0
      of_limit = if limit > 0, do: " of #{format_bytes(limit)}", else: " · no limit"

      """
      <div class="overview-metric">
        <span class="overview-metric-label">Memory</span>
        <span class="overview-metric-value">#{format_bytes(rss)}</span>
        <span class="overview-metric-note">Process RSS#{of_limit}</span>
        <div class="overview-meter" aria-hidden="true"><div style="width:#{min(pct, 100)}%;background:#{mem_bar_color(pct)};"></div></div>
        <span class="overview-metric-note">#{tracked}</span>
      </div>
      """
    else
      """
      <div class="overview-metric">
        <span class="overview-metric-label">Memory</span>
        <span class="overview-metric-value is-empty" title="Process RSS unavailable">Unavailable</span>
        <span class="overview-metric-note">Process RSS unavailable</span>
        <span class="overview-metric-note">#{tracked}</span>
      </div>
      """
    end
  end

  defp render_keys_card(data) do
    overview = Map.get(data, :overview, %{})
    lifecycle = Map.get(data, :lifecycle, %{})
    disk = get_in(data, [:storage_summary, :total_disk_bytes])
    on_disk = if is_integer(disk), do: "#{format_bytes(disk)} on disk · ", else: ""

    """
    <div class="overview-metric">
      <span class="overview-metric-label">Keys</span>
      <span class="overview-metric-value">#{format_grouped(Map.get(overview, :total_keys, 0))}</span>
      <span class="overview-metric-note">#{on_disk}#{format_grouped(Map.get(lifecycle, :expired_total, 0))} expired · #{format_grouped(Map.get(lifecycle, :evicted_total, 0))} evicted</span>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # Workflows and shards
  # ---------------------------------------------------------------------------

  @doc """
  Active sampled runs as one bar: queued, running with a live lease, running
  with an expired lease, and any other active state.
  """
  def render_overview_workflows(summary) do
    active = count(summary, :active)

    body =
      if active == 0 do
        ~s(<p class="overview-empty">No active workflow runs in the sample.</p>)
      else
        queued = count(summary, :queued)
        expired = count(summary, :expired_leases_sampled)
        running = max(count(summary, :running) - expired, 0)
        failed = count(summary, :failed)
        other = max(active - queued - count(summary, :running), 0)

        segments = [
          {"queued", "Queued", queued},
          {"running", "Running", running},
          {"expired", "Lease expired", expired},
          {"other", "Other active", other}
        ]

        label =
          "#{queued} queued, #{running} running, #{expired} with expired leases, #{failed} failed" <>
            if(other > 0, do: ", #{other} other", else: "")

        bar =
          segments
          |> Enum.filter(fn {_key, _label, value} -> value > 0 end)
          |> Enum.map_join("", fn {key, _label, value} ->
            ~s(<span class="overview-state is-#{key}" style="flex: #{value}"></span>)
          end)

        legend =
          (segments ++ [{"failed", "Failed", failed}])
          |> Enum.filter(fn {key, _label, value} -> value > 0 or key in ["queued", "running"] end)
          |> Enum.map_join("", fn {key, name, value} ->
            ~s(<li><span class="overview-swatch is-#{key}" aria-hidden="true"></span>#{name} <strong>#{format_grouped(value)}</strong></li>)
          end)

        types =
          case count(summary, :types) do
            0 -> ""
            1 -> ", across 1 type"
            n -> ", across #{n} types"
          end

        """
        <p class="overview-workflows-total"><strong>#{format_grouped(active)}</strong> active of #{format_grouped(count(summary, :sampled_records))} sampled runs#{types}</p>
        <div class="overview-state-bar" role="img" aria-label="#{label}">#{bar}</div>
        <ul class="overview-legend">#{legend}</ul>
        #{render_top_types(Map.get(summary, :top_types, []))}
        """
      end

    """
    <section class="overview-panel" aria-labelledby="overview-workflows-title">
      <div class="overview-panel-head">
        <h2 id="overview-workflows-title">Workflows</h2>
        <a href="/dashboard/flow">Open workflows</a>
      </div>
      #{body}
      <p class="overview-panel-note">The overview reads a small sample across shards; open Workflows for counts over a wider scan.</p>
    </section>
    """
  end

  defp render_top_types([]), do: ""

  defp render_top_types(types) do
    rows =
      Enum.map_join(types, "", fn type ->
        name = to_string(Map.get(type, :type, ""))
        href = "/dashboard/flow/states?" <> URI.encode_query(%{"type" => name})

        ~s(<tr><td><a href="#{escape_attr(href)}">#{escape(name)}</a></td><td>#{format_grouped(count(type, :queued))}</td><td>#{format_grouped(count(type, :running))}</td><td>#{format_grouped(count(type, :failed))}</td></tr>)
      end)

    """
    <table class="overview-types">
      <thead><tr><th scope="col">Type</th><th scope="col">Queued</th><th scope="col">Running</th><th scope="col">Failed</th></tr></thead>
      <tbody>#{rows}</tbody>
    </table>
    """
  end

  defp count(map, key) do
    case Map.get(map, key, 0) do
      value when is_integer(value) and value > 0 -> value
      _ -> 0
    end
  end

  @doc """
  One cell per shard, shaded by its share of keys, with the down ones marked.
  """
  def render_overview_shards(shards, cluster) do
    total = length(shards)
    healthy = Enum.count(shards, &(Map.get(&1, :status) == "ok"))
    most_keys = shards |> Enum.map(&Map.get(&1, :keys, 0)) |> Enum.max(fn -> 0 end)

    cells =
      Enum.map_join(shards, "", fn shard ->
        index = Map.get(shard, :index, "-")
        keys = Map.get(shard, :keys, 0)
        down? = Map.get(shard, :status) != "ok"
        status = if down?, do: Map.get(shard, :status, "unknown"), else: "ok"
        share = if most_keys > 0, do: keys / most_keys, else: 0.0

        fill =
          if down?,
            do: "",
            else: ~s( style="--shard-load: #{Float.round(0.08 + share * 0.32, 2)}")

        title =
          "Shard #{index}: #{status} · #{format_grouped(keys)} keys · #{format_bytes(Map.get(shard, :ets_memory_bytes, 0))} memory · #{format_bytes(Map.get(shard, :disk_bytes, 0))} disk"

        ~s(<li class="overview-shard#{if down?, do: " is-down", else: ""}" title="#{escape_attr(title)}"#{fill}><span>#{index}</span><strong>#{format_number(keys)}</strong></li>)
      end)

    badge =
      if healthy == total,
        do: ~s(<span class="badge badge-ok">#{healthy} of #{total} healthy</span>),
        else: ~s(<span class="badge badge-warning">#{healthy} of #{total} healthy</span>)

    """
    <section class="overview-panel" aria-labelledby="overview-shards-title">
      <div class="overview-panel-head">
        <h2 id="overview-shards-title">Shards</h2>
        #{badge}
        <a href="/dashboard/raft">Consensus</a>
      </div>
      <p class="overview-panel-note">#{escape(cluster_label(cluster))}. Cells show keys per shard.</p>
      <ol class="overview-shard-grid"#{shard_columns(total)}>#{cells}</ol>
    </section>
    """
  end

  # Even rows when the shard count allows them.
  defp shard_columns(total) when total > 8 and rem(total, 8) == 0,
    do: ~s( style="--shard-columns: 8")

  defp shard_columns(_total), do: ""

  defp cluster_label(cluster) do
    node = cluster |> Map.get(:node_name, "") |> to_string()

    mode =
      case Map.get(cluster, :cluster_mode) do
        :cluster -> "#{Map.get(cluster, :cluster_size, "?")}-node cluster"
        _ -> "single-member WARaft"
      end

    if node == "", do: String.capitalize(mode), else: "#{node} · #{mode}"
  end

  # ---------------------------------------------------------------------------
  # Small cards
  # ---------------------------------------------------------------------------

  def render_overview_cards(data) do
    lifecycle = Map.get(data, :lifecycle, %{})
    connections = Map.get(data, :connections, %{})
    merging = data |> Map.get(:merge, []) |> Enum.count(&Map.get(&1, :merging, false))
    disk = get_in(data, [:storage_summary, :total_disk_bytes])
    keydir_max = Map.get(lifecycle, :keydir_max_ram, 0)

    keydir_used =
      "#{format_bytes(Map.get(lifecycle, :keydir_bytes, 0))} of #{if keydir_max > 0, do: format_bytes(keydir_max), else: "no limit"}"

    """
    <div class="overview-cards">
      #{render_card("Storage", "/dashboard/storage", "Details", [{"On disk", if(is_integer(disk), do: format_bytes(disk), else: "Unavailable")}, {"Merges", if(merging > 0, do: "#{merging} running", else: "idle")}])}
      #{render_card("Keydir", nil, nil, [{"Used", keydir_used}, {"Expired", format_grouped(Map.get(lifecycle, :expired_total, 0))}, {"Evicted", format_grouped(Map.get(lifecycle, :evicted_total, 0))}])}
      #{render_card("Clients", "/dashboard/clients", "List", [{"Connected", format_grouped(Map.get(connections, :active, 0))}, {"Blocked", format_grouped(Map.get(connections, :blocked, 0))}, {"Tracking", format_grouped(Map.get(connections, :tracking, 0))}])}
      #{render_slow_card(data)}
    </div>
    """
  end

  defp render_card(title, href, link, rows) do
    link_html = if href, do: ~s(<a href="#{href}">#{escape(link)}</a>), else: ""

    rows_html =
      Enum.map_join(rows, "", fn {label, value} ->
        ~s(<dt>#{escape(label)}</dt><dd>#{escape(value)}</dd>)
      end)

    """
    <section class="overview-card" aria-label="#{escape_attr(title)}">
      <div class="overview-card-head"><h2>#{escape(title)}</h2>#{link_html}</div>
      <dl>#{rows_html}</dl>
    </section>
    """
  end

  defp render_slow_card(data) do
    entries = Map.get(data, :slowlog, [])

    body =
      cond do
        Map.get(data, :slowlog_status, :ok) not in [:ok, nil] ->
          ~s(<p class="overview-card-note">Slow log unavailable.</p>)

        entries == [] ->
          ~s(<p class="overview-card-note">None logged since start.</p>)

        true ->
          slowest = entries |> Enum.map(&Map.get(&1, :duration_us, 0)) |> Enum.max()

          ~s(<dl><dt>Logged</dt><dd>#{format_grouped(length(entries))}</dd><dt>Slowest</dt><dd>#{format_duration_us(slowest)}</dd></dl>)
      end

    """
    <section class="overview-card" aria-label="Slow commands">
      <div class="overview-card-head"><h2>Slow commands</h2><a href="/dashboard/slowlog">Slow log</a></div>
      #{body}
    </section>
    """
  end

  # ---------------------------------------------------------------------------
  # Memory pressure detail
  # ---------------------------------------------------------------------------

  def render_memory_alert(data) do
    level = Map.get(data, :pressure_level, :ok)

    if level in [:warning, :pressure, :reject] do
      tracked_ratio =
        if Map.get(data, :max_bytes, 0) > 0, do: Map.get(data, :ratio, 0.0), else: 0.0

      rss_ratio = Map.get(data, :rss_ratio, 0.0)
      basis = if rss_ratio > tracked_ratio, do: "Process RSS", else: "Tracked allocations"
      pct = Float.round(max(tracked_ratio, rss_ratio) * 100, 1)
      policy = data |> Map.get(:eviction_policy, :unknown) |> Atom.to_string()

      badge_class =
        case level do
          :warning -> "badge-warning"
          :pressure -> "badge-pressure"
          :reject -> "badge-reject"
        end

      action_text =
        case level do
          :warning ->
            "Inspect process RSS and tracked allocations against their separate budgets."

          :pressure ->
            "Eviction active. Keys are being removed under #{policy} policy."

          :reject ->
            "Writes are being rejected. Inspect the limiting memory budget before adjusting capacity."
        end

      shard_rows =
        data
        |> Map.get(:shards, %{})
        |> Enum.sort_by(fn {index, _} -> index end)
        |> Enum.map_join("\n", fn {index, shard} ->
          shard_pct = Float.round(shard.ratio * 100, 1)

          shard_class =
            cond do
              shard.ratio >= 0.85 -> "c-red"
              shard.ratio >= 0.70 -> "c-yellow"
              true -> ""
            end

          """
          <tr>
            <td>#{index}</td>
            <td>#{format_bytes(shard.bytes)}</td>
            <td class="#{shard_class}">#{shard_pct}%</td>
          </tr>
          """
        end)

      """
      <section class="overview-panel" aria-labelledby="overview-memory-title">
        <div class="overview-panel-head">
          <h2 id="overview-memory-title">Memory pressure</h2>
          <span class="badge #{badge_class}">#{escape(Atom.to_string(level))}</span>
        </div>
        <p class="overview-panel-note">#{basis} at #{pct}% · eviction policy #{escape(policy)}. #{escape(action_text)}</p>
        <div class="overview-meter" aria-hidden="true"><div style="width:#{min(pct, 100)}%;background:#{mem_bar_color(pct)};"></div></div>
        <table>
          <thead>
            <tr><th>Shard</th><th>Bytes</th><th>Usage</th></tr>
          </thead>
          <tbody>
            #{shard_rows}
          </tbody>
        </table>
      </section>
      """
    else
      ""
    end
  end

  def render_footer(data) do
    sample_rate = get_in(data, [:hotcold, :sample_rate]) || 1
    version = Map.get(data.overview, :version, "unknown")
    run_id = data.overview |> Map.get(:run_id, "") |> String.slice(0, 8)
    uptime = data.overview |> Map.get(:uptime_seconds)
    up = if is_integer(uptime), do: "Up #{format_uptime(uptime)} · ", else: ""

    sampling =
      if sample_rate > 1, do: "Hit and miss counts are sampled 1:#{sample_rate}", else: ""

    """
    <footer class="overview-footer">
      <span>#{up}v#{escape(version)} · run #{escape(run_id)}</span>
      <span>#{sampling}</span>
    </footer>
    """
  end

  defp icon(:check),
    do:
      ~s(<svg class="overview-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="9"></circle><path d="m8 12.5 2.8 2.8L16 10"></path></svg>)

  defp icon(:warning),
    do:
      ~s(<svg class="overview-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 3 2.5 20h19Z"></path><path d="M12 10v4.5M12 17.5v.01"></path></svg>)

  # ---------------------------------------------------------------------------
  # HTML rendering -- Sub-page content sections
  # ---------------------------------------------------------------------------

  def render_ops_summary(title, cards, opts \\ []) do
    card_html =
      Enum.map_join(cards, "\n", fn card ->
        value_class = Map.get(card, :class, "")

        detail_html =
          cond do
            html = Map.get(card, :detail_html) ->
              ~s(<div class="ops-summary-detail">#{html}</div>)

            (detail = Map.get(card, :detail, "")) != "" ->
              ~s(<div class="ops-summary-detail">#{escape(detail)}</div>)

            true ->
              ""
          end

        value = Map.fetch!(card, :value)

        classes =
          ["ops-summary-value", value_class, if(summary_text?(value), do: "is-text")]
          |> Enum.reject(&(&1 in [nil, ""]))
          |> Enum.join(" ")

        """
        <div class="ops-summary-card">
          <div class="ops-summary-label">#{escape(Map.fetch!(card, :label))}</div>
          <div class="#{escape_attr(classes)}">#{escape(value)}</div>
          #{detail_html}
        </div>
        """
      end)

    # Pages that open with a status line keep the heading for screen readers only.
    heading_class =
      if Keyword.get(opts, :heading) == :hidden, do: "section-title sr-only", else: "section-title"

    """
    <h2 class="#{heading_class}">#{escape(title)}</h2>
    <div class="ops-summary-grid">
      #{card_html}
    </div>
    """
  end

  # A measured figure starts with a digit; anything else (Unavailable, on,
  # Shard 11) is a word and reads smaller.
  defp summary_text?(value) when is_binary(value), do: not Regex.match?(~r/^-?\d/, value)
  defp summary_text?(_value), do: false
end
