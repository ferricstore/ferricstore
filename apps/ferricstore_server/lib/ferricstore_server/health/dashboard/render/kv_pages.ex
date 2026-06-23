defmodule FerricstoreServer.Health.Dashboard.Render.KVPages do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview

  @keyspace_dashboard_default_limit 50
  @keyspace_dashboard_max_limit 500

  def kv_command_groups do
    [
      %{
        name: "Strings",
        purpose: "Primary KV read/write path.",
        commands: ~w(GET MGET SET MSET DEL EXISTS TTL PTTL EXPIRE PERSIST TYPE)
      },
      %{
        name: "Structured Values",
        purpose: "Compound primitives stored as internal keys.",
        commands:
          ~w(HGET HSET HMGET HGETALL LPUSH RPUSH LPOP RPOP SADD SMEMBERS ZADD ZRANGE XADD XREAD)
      },
      %{
        name: "Large / Cold Values",
        purpose: "Debug large values, cold reads, and native response chunks.",
        commands: ~w(GET MGET STRLEN FERRICSTORE.KEY_INFO FERRICSTORE.HOTNESS)
      },
      %{
        name: "Operational",
        purpose: "Observability and maintenance commands used by the dashboard.",
        commands: ~w(INFO SLOWLOG CONFIG MEMORY CLIENT SCAN)
      }
    ]
  end

  def render_keyspace_controls(data) do
    filters = Map.get(data, :filters, %{})
    key = Map.get(filters, :key, "")
    prefix = Map.get(filters, :prefix, "")
    limit = Map.get(filters, :limit, @keyspace_dashboard_default_limit)
    checked = if Map.get(filters, :include_internal, false), do: " checked", else: ""

    """
    <div class="kv-panel">
      <form class="flow-filter-form" action="/dashboard/keyspace" method="get">
        <label>Exact key
          <input class="flow-search-input mono" type="search" name="key" value="#{escape_attr(key)}" placeholder="user:123">
        </label>
        <label>Prefix
          <input class="flow-search-input mono" type="search" name="prefix" value="#{escape_attr(prefix)}" placeholder="tenant:">
        </label>
        <label>Limit
          <input class="flow-search-input flow-filter-limit" type="number" min="1" max="#{@keyspace_dashboard_max_limit}" name="limit" value="#{limit}">
        </label>
        <label class="flow-check-label" title="Show internal compound keys used by hashes, sets, zsets, streams, Flow values, and metadata.">
          <input type="checkbox" name="include_internal" value="true"#{checked}> Internal
        </label>
        <button class="flow-search-button" type="submit">Search</button>
        <a class="flow-filter-clear" href="/dashboard/keyspace">Clear</a>
      </form>
      <div class="flow-filter-note">Requires +SCAN for samples. Exact key inspection requires +GET and key read access.</div>
    </div>
    """
  end

  def render_keyspace_inspector(nil), do: ""

  def render_keyspace_inspector(%{found?: false, key: key}) do
    """
    <div class="kv-inspector">
      <div class="section-title">Key Inspector</div>
      <div class="flow-alert flow-alert-error">No live key metadata found for <code>#{escape(key)}</code>.</div>
      <div class="flow-filter-note">Requires +GET and read access to the selected key.</div>
    </div>
    """
  end

  def render_keyspace_inspector(inspected) do
    """
    <div class="kv-inspector">
      #{render_ops_summary("Key Inspector", [%{label: "Key", value: inspected.key}, %{label: "Type", value: inspected.type}, %{label: "Shard", value: "Shard #{inspected.shard}"}, %{label: "Location", value: inspected.location, detail: "TTL #{inspected.ttl} · #{inspected.size}"}])}
      <div class="flow-filter-note">Requires +GET and read access to the selected key.</div>
    </div>
    """
  end

  def render_keyspace_table(data) do
    rows = Map.get(data, :rows, [])
    sampled = Map.get(data, :total_sampled, length(rows))

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="8" class="c-muted">No key metadata matched this query.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            internal =
              if Map.get(row, :internal?, false) do
                ~s(<span class="badge badge-idle">internal</span>)
              else
                ""
              end

            """
            <tr>
              <td class="mono">#{escape(Map.get(row, :key, ""))} #{internal}</td>
              <td>#{escape(Map.get(row, :type, "-"))}</td>
              <td>#{Map.get(row, :shard, "-")}</td>
              <td>#{escape(Map.get(row, :location, "-"))}</td>
              <td>#{escape(Map.get(row, :size, "-"))}</td>
              <td>#{escape(Map.get(row, :ttl, "-"))}</td>
              <td>#{format_number(Map.get(row, :lfu, 0))}</td>
              <td class="mono">#{escape(Map.get(row, :physical_key, ""))}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Key Metadata <span class="badge badge-idle">sampled #{format_number(sampled)}</span></div>
    <table>
      <thead>
        <tr><th>Logical Key</th><th>Type</th><th>Shard</th><th>Location</th><th>Size</th><th>TTL</th><th>LFU</th><th>Physical Key</th></tr>
      </thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  def render_commands_summary(data) do
    summary = Map.get(data, :summary, %{})

    render_ops_summary("Command Summary", [
      %{label: "Commands", value: format_number(Map.get(summary, :total_commands, 0))},
      %{label: "Ops/Sec", value: to_string(Map.get(summary, :ops_per_sec, 0.0))},
      %{label: "Slowlog", value: format_number(Map.get(summary, :slowlog_entries, 0))},
      %{label: "Slowest", value: format_duration_us(Map.get(summary, :slowest_us, 0))}
    ])
  end

  def render_command_slowlog_table(data) do
    rows = Map.get(data, :slow_by_command, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No slow commands recorded.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.command)}</td>
              <td>#{format_number(row.count)}</td>
              <td>#{format_duration_us(row.worst_us)}</td>
              <td>#{format_duration_us(row.avg_us)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Slow Log By Command</div>
    <table>
      <thead><tr><th>Command</th><th>Entries</th><th>Worst</th><th>Average</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  def render_kv_command_reference(data) do
    groups = Map.get(data, :command_groups, kv_command_groups())

    body =
      Enum.map_join(groups, "\n", fn group ->
        commands =
          group.commands
          |> Enum.map_join(" ", &~s(<span class="flow-pill mono">#{escape(&1)}</span>))

        """
        <div class="kv-command-group">
          <div class="kv-command-title">#{escape(group.name)}</div>
          <div class="kv-command-purpose">#{escape(group.purpose)}</div>
          <div>#{commands}</div>
        </div>
        """
      end)

    """
    <div class="section-title">Command Groups</div>
    <div class="kv-command-grid">#{body}</div>
    """
  end

  def render_reads_summary(data) do
    hotcold = Map.fetch!(data, :hotcold)
    hot_reads = Map.get(hotcold, :hot_reads, Map.get(hotcold, :total_hot, 0))
    cold_reads = Map.get(hotcold, :cold_reads, Map.get(hotcold, :total_cold, 0))

    render_ops_summary("Read Path Summary", [
      %{label: "Hit Rate", value: "#{hotcold.hit_ratio}%"},
      %{
        label: "Hot Reads",
        value: format_number(hot_reads),
        detail_html: "sampled #{sampled_tag(Map.get(hotcold, :sample_rate, 1))}"
      },
      %{
        label: "Cold Reads",
        value: format_number(cold_reads),
        detail: "#{Map.get(hotcold, :cold_reads_per_sec, 0.0)}/sec"
      },
      %{label: "Misses", value: format_number(Map.get(hotcold, :total_misses, 0))}
    ])
  end

  def render_read_prefix_table(data) do
    rows = Map.get(data, :prefixes, [])

    body =
      case rows do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No sampled read pressure yet.</td></tr>)

        _ ->
          Enum.map_join(rows, "\n", fn row ->
            """
            <tr>
              <td class="mono">#{escape(row.prefix)}</td>
              <td>#{format_number(row.hot_reads)}</td>
              <td>#{format_number(row.cold_reads)}</td>
              <td>#{Float.round(row.cold_pct, 1)}%</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Prefix Read Pressure</div>
    <table>
      <thead><tr><th>Prefix</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th><th>Cold %</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Storage Sub-page
  # ---------------------------------------------------------------------------

  def render_storage_summary(data) do
    shards = Map.get(data, :shards, [])
    data_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.data_file_count end)
    hint_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.hint_file_count end)

    largest =
      Enum.max_by(shards, & &1.disk_bytes, fn -> %{index: "-", disk_bytes: 0} end)

    render_ops_summary("Storage Summary", [
      %{label: "Total Disk", value: format_bytes(data.total_disk_bytes)},
      %{label: "Total Files", value: format_number(data.total_files)},
      %{
        label: "Largest Shard",
        value: "Shard #{largest.index}",
        detail: format_bytes(largest.disk_bytes)
      },
      %{
        label: "Data Files",
        value: format_number(data_files),
        detail: "#{format_number(hint_files)} Hint Files"
      }
    ])
  end

  def render_storage_table(shards) do
    rows =
      Enum.map_join(shards, "\n", fn shard ->
        """
        <tr>
          <td>#{shard.index}</td>
          <td>#{format_bytes(shard.disk_bytes)}</td>
          <td>#{shard.data_file_count}</td>
          <td>#{shard.hint_file_count}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Per-Shard Storage</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Disk Size</th><th>Data Files</th><th>Hint Files</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Doctor Sub-page
  # ---------------------------------------------------------------------------
end
