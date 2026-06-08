defmodule FerricstoreServer.Health.Dashboard.Render.Overview do
  import FerricstoreServer.Health.Dashboard.Format

  def render_top_bar(data) do
    overview = data.overview
    hotcold = data.hotcold
    memory = data.memory
    conns = data.connections
    cluster = data.cluster

    # Status dot color
    {dot_class, status_text} =
      cond do
        overview.status != :ok -> {"dot-red", "degraded"}
        memory.pressure_level == :reject -> {"dot-red", "rejecting"}
        memory.pressure_level == :pressure -> {"dot-yellow", "pressure"}
        memory.pressure_level == :warning -> {"dot-yellow", "warning"}
        true -> {"dot-green", "healthy"}
      end

    # Hit rate is neutral until there are actual read samples.
    hit_color =
      if hotcold_has_samples?(hotcold), do: hit_rate_color(hotcold.hit_ratio), else: "#8b949e"

    hit_value =
      if hotcold_has_samples?(hotcold), do: "#{hotcold.hit_ratio}%", else: "No read samples"

    # Memory bar
    mem_pct = if memory.max_bytes > 0, do: Float.round(memory.ratio * 100, 1), else: 0.0
    mem_bar_color = mem_bar_color(mem_pct)
    mem_bar_width = min(mem_pct, 100)
    memory_limit = if memory.max_bytes > 0, do: format_bytes(memory.max_bytes), else: "unlimited"

    # Cluster info
    cluster_label =
      case cluster.cluster_mode do
        :standalone -> "single-member WARaft"
        :cluster -> "#{cluster.cluster_size}-node cluster"
      end

    node_short = cluster.node_name |> Atom.to_string()

    """
    <div class="top-bar">
      <div class="logo"><span class="status-dot #{dot_class}"></span>FerricStore</div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Node</span>
        <span class="val" style="font-size:0.75rem;">#{escape(node_short)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Cluster</span>
        <span class="val" style="font-size:0.85rem;">#{escape(cluster_label)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Status</span>
        <span class="val" style="font-size:0.85rem;">#{escape(status_text)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Ops/sec</span>
        <span class="val">#{format_rate(hotcold.ops_per_sec)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Hit Rate #{sampled_tag(hotcold.sample_rate)}</span>
        <span class="val" style="color:#{hit_color};">#{escape(hit_value)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Memory</span>
        <span class="val" style="font-size:0.85rem;">#{format_bytes(memory.total_bytes)} / #{memory_limit}</span>
        <div class="mem-bar-wrap"><div class="mem-bar-fill" style="width:#{mem_bar_width}%;background:#{mem_bar_color};"></div></div>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Connections</span>
        <span class="val">#{format_number(conns.active)}</span>
      </div>
      <div class="sep"></div>
      <div class="metric">
        <span class="label">Keys</span>
        <span class="val">#{format_number(overview.total_keys)}</span>
      </div>
    </div>
    """
  end

  def render_cache_performance(data) do
    has_samples = hotcold_has_samples?(data)
    hit_color = if has_samples, do: hit_rate_color(data.hit_ratio), else: "#8b949e"
    hit_value = if has_samples, do: "#{data.hit_ratio}%", else: "No read samples"

    # RAM bar color -- always green (fast path)
    # Disk bar color -- orange (slow path)
    ram_bar_width = min(data.ram_ratio, 100)
    disk_bar_width = min(data.disk_ratio, 100)

    """
    <div class="section-title">Cache Performance</div>
    <div class="cache-hero">
      <div class="hit-rate-card">
        <div class="hit-rate-num" style="color:#{hit_color};">#{escape(hit_value)}</div>
        <div class="hit-rate-label">Hit Rate #{sampled_tag(data.sample_rate)}</div>
        <div class="hit-rate-sub">
          <span>#{format_rate(data.hits_per_sec)}</span> hits/sec #{sampled_tag(data.sample_rate)} &middot;
          <span>#{format_rate(data.misses_per_sec)}</span> misses/sec
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Where hits come from</div>
        <div class="source-row">
          <div>
            <div class="source-name">RAM #{sampled_tag(data.sample_rate)} #{info_icon("Served from ETS in-memory cache. Estimated from 1:#{data.sample_rate} sampling. Latency: about 1-5 microseconds.")}</div>
            <div class="source-detail">fast path (~1-5us)</div>
          </div>
          <div class="source-pct c-green">#{data.ram_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{ram_bar_width}%;background:#3fb950;"></div></div>
        <div class="source-row">
          <div>
            <div class="source-name">Disk #{info_icon("Required Bitcask disk read. This is an exact count, not sampled. Latency is usually about 50-200 microseconds. High disk ratio means memory pressure is evicting hot keys.")}</div>
            <div class="source-detail">slow path (~50-200us) &middot; exact</div>
          </div>
          <div class="source-pct c-yellow">#{data.disk_ratio}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{disk_bar_width}%;background:#d29922;"></div></div>
      </div>
    </div>
    """
  end

  def render_lifecycle(data) do
    # Evicted card color
    evicted_color =
      cond do
        data.evicted_per_sec > 100 -> "c-red"
        data.evicted_total > 0 -> "c-yellow"
        true -> ""
      end

    # Keydir capacity bar color and percentage
    keydir_pct =
      if data.keydir_max_ram > 0, do: Float.round(data.keydir_ratio * 100, 1), else: 0.0

    keydir_bar_width = min(keydir_pct, 100)

    keydir_bar_color =
      cond do
        keydir_pct > 90 -> "#f85149"
        keydir_pct > 70 -> "#d29922"
        true -> "#3fb950"
      end

    keydir_pct_class =
      cond do
        keydir_pct > 90 -> "c-red"
        keydir_pct > 70 -> "c-yellow"
        true -> "c-green"
      end

    keydir_full_alert =
      if data.keydir_full do
        """
        <div style="background:#8b1a1a; border:2px solid #f85149; border-radius:8px; padding:12px 16px; margin-bottom:16px; color:#f85149; font-weight:700; font-size:0.85rem;">
          KEYDIR FULL &mdash; new writes are being rejected. Increase max_memory or evict keys.
        </div>
        """
      else
        ""
      end

    """
    <div class="section-title">Key Lifecycle</div>
    #{keydir_full_alert}<div class="cache-hero">
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Expired</div>
        <div class="source-row">
          <div>
            <div class="source-name">Total</div>
          </div>
          <div class="source-pct">#{format_number(data.expired_total)}</div>
        </div>
        <div class="source-row">
          <div>
            <div class="source-name">Rate</div>
          </div>
          <div class="source-pct">#{format_rate(data.expired_per_sec)}/sec</div>
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Evicted</div>
        <div class="source-row">
          <div>
            <div class="source-name">Total</div>
          </div>
          <div class="source-pct #{evicted_color}">#{format_number(data.evicted_total)}</div>
        </div>
        <div class="source-row">
          <div>
            <div class="source-name">Rate</div>
          </div>
          <div class="source-pct #{evicted_color}">#{format_rate(data.evicted_per_sec)}/sec</div>
        </div>
      </div>
      <div class="source-card">
        <div style="font-size:0.75rem; color:#8b949e; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">Keydir Capacity</div>
        <div class="source-row">
          <div>
            <div class="source-name">#{format_bytes(data.keydir_bytes)} / #{format_bytes(data.keydir_max_ram)}</div>
          </div>
          <div class="source-pct #{keydir_pct_class}">#{keydir_pct}%</div>
        </div>
        <div class="source-bar-wrap"><div class="source-bar-fill" style="width:#{keydir_bar_width}%;background:#{keydir_bar_color};"></div></div>
      </div>
    </div>
    """
  end

  def render_shards(shards) do
    all_ok = Enum.all?(shards, fn s -> s.status == "ok" end)

    rows =
      Enum.map_join(shards, "\n", fn shard ->
        status_html =
          case shard.status do
            "ok" -> ~s(<span class="c-green">ok</span>)
            _ -> ~s(<span class="c-red">#{escape(shard.status)}</span>)
          end

        disk_bytes = Map.get(shard, :disk_bytes, 0)

        """
        <tr>
          <td>#{shard.index}</td>
          <td>#{status_html}</td>
          <td>#{format_number(shard.keys)}</td>
          <td>#{format_bytes(shard.ets_memory_bytes)}</td>
          <td>#{format_bytes(disk_bytes)}</td>
        </tr>
        """
      end)

    summary_badge =
      if all_ok do
        ~s(<span class="badge badge-ok">all ok</span>)
      else
        down_count = Enum.count(shards, fn s -> s.status != "ok" end)
        ~s(<span class="badge badge-pressure">#{down_count} down</span>)
      end

    """
    <div class="section-title">Shards #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Keys</th><th>Memory</th><th>Disk</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_memory_alert(data) do
    # Only show the memory section when there is pressure
    if data.pressure_level == :ok do
      ""
    else
      level_str = Atom.to_string(data.pressure_level)
      pct = if data.max_bytes > 0, do: Float.round(data.ratio * 100, 1), else: 0.0
      bar_color = mem_bar_color(pct)
      bar_width = min(pct, 100)

      badge_class =
        case data.pressure_level do
          :warning -> "badge-warning"
          :pressure -> "badge-pressure"
          :reject -> "badge-reject"
          _ -> "badge-idle"
        end

      level_class =
        case data.pressure_level do
          :warning -> "level-warning"
          :pressure -> "level-pressure"
          :reject -> "level-reject"
          _ -> ""
        end

      action_text =
        case data.pressure_level do
          :warning ->
            "Consider increasing max_memory or reviewing eviction policy."

          :pressure ->
            "Eviction active. Keys are being removed under #{escape(Atom.to_string(data.eviction_policy))} policy."

          :reject ->
            "Writes are being rejected. Increase max_memory immediately."

          _ ->
            ""
        end

      shard_rows =
        data.shards
        |> Enum.sort_by(fn {index, _} -> index end)
        |> Enum.map_join("\n", fn {index, shard} ->
          shard_pct = Float.round(shard.ratio * 100, 1)

          shard_class =
            cond do
              shard.ratio >= 0.95 -> "c-red"
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
      <div class="section-title">Memory Pressure <span class="badge #{badge_class}">#{escape(level_str)}</span></div>
      <div class="pressure-alert #{level_class}">
        <div class="pressure-details">
          <span>#{format_bytes(data.total_bytes)}</span> / <span>#{format_bytes(data.max_bytes)}</span> (#{pct}%)
          &middot; Policy: <span>#{escape(Atom.to_string(data.eviction_policy))}</span>
        </div>
        <div class="pressure-bar-wrap"><div class="pressure-bar-fill" style="width:#{bar_width}%;background:#{bar_color};"></div></div>
        <div class="pressure-action">#{action_text}</div>
      </div>
      <table>
        <thead>
          <tr><th>Shard</th><th>Bytes</th><th>Usage</th></tr>
        </thead>
        <tbody>
          #{shard_rows}
        </tbody>
      </table>
      """
    end
  end

  def render_connections(data) do
    blocked_class = if data.blocked > 0, do: "c-yellow", else: ""

    """
    <div class="section-title">Connections</div>
    <div class="conn-row">
      <div class="conn-item">
        <span class="conn-label">Active </span>
        <span class="conn-val">#{format_number(data.active)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Blocked </span>
        <span class="conn-val #{blocked_class}">#{format_number(data.blocked)}</span>
      </div>
      <div class="conn-item">
        <span class="conn-label">Tracking </span>
        <span class="conn-val">#{format_number(data.tracking)}</span>
      </div>
    </div>
    """
  end

  # render_nav_links removed — replaced by sidebar navigation

  def render_footer(data) do
    sample_rate = data.hotcold.sample_rate

    """
    <div class="footer">
      <span>Uptime: #{format_uptime(data.overview.uptime_seconds)} &middot; v0.1.0 &middot; Run #{escape(String.slice(data.overview.run_id, 0, 8))}</span>
      <span>Hit/miss stats estimated from 1:#{sample_rate} sampling &middot; Live updates patch changed components</span>
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Sub-page content sections
  # ---------------------------------------------------------------------------

  def render_ops_summary(title, cards) do
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

        """
        <div class="ops-summary-card">
          <div class="ops-summary-label">#{escape(Map.fetch!(card, :label))}</div>
          <div class="ops-summary-value #{escape_attr(value_class)}">#{escape(Map.fetch!(card, :value))}</div>
          #{detail_html}
        </div>
        """
      end)

    """
    <div class="section-title">#{escape(title)}</div>
    <div class="ops-summary-grid">
      #{card_html}
    </div>
    """
  end
end
