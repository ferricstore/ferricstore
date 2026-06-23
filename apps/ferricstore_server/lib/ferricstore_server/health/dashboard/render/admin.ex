defmodule FerricstoreServer.Health.Dashboard.Render.Admin do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview

  def render_slowlog_summary(entries) do
    count = length(entries)
    total_us = Enum.reduce(entries, 0, fn entry, acc -> acc + max(entry.duration_us, 0) end)
    worst_us = Enum.reduce(entries, 0, fn entry, acc -> max(acc, max(entry.duration_us, 0)) end)
    avg_us = if count > 0, do: div(total_us, count), else: 0

    render_ops_summary("Slow Log Summary", [
      %{label: "Entries", value: format_number(count)},
      %{
        label: "Worst",
        value: format_duration_us(worst_us),
        class: slowlog_duration_class(worst_us)
      },
      %{label: "Avg", value: format_duration_us(avg_us)},
      %{label: "Total Time", value: format_duration_us(total_us)}
    ])
  end

  def slowlog_duration_class(duration_us) when duration_us >= 1_000_000, do: "c-red"
  def slowlog_duration_class(duration_us) when duration_us >= 100_000, do: "c-yellow"
  def slowlog_duration_class(_duration_us), do: ""

  def render_slowlog_table(entries) do
    count = length(entries)
    count_label = if count == 0, do: "none", else: "#{count} entries"

    rows =
      case entries do
        [] ->
          ~s(<tr><td colspan="4" class="c-muted">No slow commands recorded</td></tr>)

        _ ->
          Enum.map_join(entries, "\n", fn entry ->
            cmd_str = Enum.join(entry.command, " ")
            duration_ms = Float.round(entry.duration_us / 1000.0, 2)
            time_str = format_timestamp_us(entry.timestamp_us)

            """
            <tr>
              <td>#{entry.id}</td>
              <td class="mono">#{escape(time_str)}</td>
              <td>#{duration_ms} ms</td>
              <td class="mono">#{escape(cmd_str)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Slow Log <span class="badge badge-idle">#{escape(count_label)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Time</th><th>Duration</th><th>Command</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_merge_summary(merges) do
    total = length(merges)
    active = Enum.count(merges, & &1.merging)
    total_merges = Enum.reduce(merges, 0, fn m, acc -> acc + max(m.merge_count, 0) end)

    reclaimed =
      Enum.reduce(merges, 0, fn m, acc -> acc + max(m.total_bytes_reclaimed, 0) end)

    latest_merge =
      merges
      |> Enum.map(& &1.last_merge_at)
      |> Enum.filter(&is_integer/1)
      |> Enum.max(fn -> nil end)

    latest_label =
      case latest_merge do
        nil -> "never"
        timestamp_ms -> format_timestamp_ms(timestamp_ms)
      end

    render_ops_summary("Merge Summary", [
      %{
        label: "Active Shards",
        value: "#{format_number(active)} / #{format_number(total)}",
        class: if(active > 0, do: "c-yellow", else: "")
      },
      %{label: "Total Reclaimed", value: format_bytes(reclaimed)},
      %{label: "Total Merges", value: format_number(total_merges)},
      %{label: "Last Merge", value: latest_label}
    ])
  end

  def render_merge_table(merges) do
    active_count = Enum.count(merges, & &1.merging)
    summary_label = if active_count > 0, do: "#{active_count} active", else: "idle"

    rows =
      Enum.map_join(merges, "\n", fn m ->
        status_badge =
          if m.merging do
            ~s(<span class="badge badge-merging">merging</span>)
          else
            ~s(<span class="badge badge-idle">idle</span>)
          end

        last_merge_str =
          case m.last_merge_at do
            nil -> "never"
            ts -> format_timestamp_ms(ts)
          end

        """
        <tr>
          <td>#{m.shard_index}</td>
          <td>#{escape(Atom.to_string(m.mode))}</td>
          <td>#{status_badge}</td>
          <td>#{last_merge_str}</td>
          <td>#{m.merge_count}</td>
          <td>#{format_bytes(m.total_bytes_reclaimed)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Merge Status <span class="badge badge-idle">#{escape(summary_label)}</span></div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Mode</th><th>Status</th><th>Last Merge</th><th>Merges</th><th>Reclaimed</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_config_table(entries) do
    count_label =
      case entries do
        [] -> "defaults"
        list -> "#{length(list)} overrides"
      end

    body =
      case entries do
        [] ->
          ~s[<p style="color:#8b949e; margin: 8px 0; font-size:0.82rem;">All namespaces using built-in default window (1ms)</p>]

        _ ->
          rows =
            Enum.map_join(entries, "\n", fn entry ->
              changed_at_str =
                if entry.changed_at == 0 do
                  "default"
                else
                  entry.changed_at
                  |> DateTime.from_unix!()
                  |> Calendar.strftime("%Y-%m-%d %H:%M:%S")
                end

              """
              <tr>
                <td class="mono">#{escape(entry.prefix)}</td>
                <td>#{entry.window_ms}</td>
                <td>#{changed_at_str}</td>
                <td>#{escape(entry.changed_by)}</td>
              </tr>
              """
            end)

          """
          <table>
            <thead>
              <tr><th>Prefix</th><th>Window (ms)</th><th>Changed At</th><th>Changed By</th></tr>
            </thead>
            <tbody>
              #{rows}
            </tbody>
          </table>
          """
      end

    """
    <div class="section-title">Namespace Config <span class="badge badge-idle">#{escape(count_label)}</span></div>
    #{body}
    """
  end

  def config_command_reference do
    [
      %{
        command: "CONFIG GET <pattern>",
        scope: "current node",
        mutability: "read-only",
        notes: "Reads runtime parameters. Supports Redis-style * and ? patterns."
      },
      %{
        command: "CONFIG SET <key> <value>",
        scope: "current node",
        mutability: "read-write",
        notes: "Updates supported runtime parameters. Use CONFIG REWRITE to persist them."
      },
      %{
        command: "CONFIG GET LOCAL <key>",
        scope: "current node",
        mutability: "read-only",
        notes: "Reads node-local ephemeral settings."
      },
      %{
        command: "CONFIG SET LOCAL log_level <level>",
        scope: "current node",
        mutability: "node-local",
        notes: "Sets logger level: debug, info, notice, warning, or error."
      },
      %{
        command: "CONFIG RESETSTAT",
        scope: "current node",
        mutability: "admin",
        notes: "Resets command stats and slowlog."
      },
      %{
        command: "CONFIG REWRITE",
        scope: "current node",
        mutability: "persist",
        notes: "Writes runtime config values to the configured config file."
      },
      %{
        command: "FERRICSTORE.CONFIG GET [prefix]",
        scope: "namespace runtime",
        mutability: "read-only",
        notes: "Shows all namespace commit-window overrides or one prefix."
      },
      %{
        command: "FERRICSTORE.CONFIG SET <prefix> window_ms <ms>",
        scope: "namespace runtime",
        mutability: "read-write",
        notes: "Sets a per-prefix commit window override in milliseconds."
      },
      %{
        command: "FERRICSTORE.CONFIG RESET [prefix]",
        scope: "namespace runtime",
        mutability: "read-write",
        notes: "Clears one namespace override, or all overrides when prefix is omitted."
      }
    ]
  end

  def runtime_config_parameter_reference do
    read_write = [
      {"maxmemory-policy", "Eviction/rejection policy used when memory pressure is high."},
      {"notify-keyspace-events", "Redis-compatible keyspace notification setting."},
      {"slowlog-log-slower-than", "Slowlog threshold in microseconds."},
      {"slowlog-max-len", "Maximum slowlog entries kept in memory."},
      {"hz", "Background maintenance frequency."},
      {"keydir-max-ram", "Maximum keydir memory target."},
      {"hot-cache-max-ram", "Maximum hot value cache memory target."},
      {"hot-cache-min-ram", "Minimum hot value cache memory target."},
      {"hot-cache-max-value-size", "Largest value eligible for hot cache storage."}
    ]

    read_only = [
      {"maxmemory", "Configured process memory ceiling."},
      {"maxclients", "Configured client connection ceiling."},
      {"native-port", "TCP listener port."},
      {"data-dir", "Persistent storage directory."},
      {"native-tls-port", "TLS listener port."},
      {"native-tls-cert-file", "TLS certificate path."},
      {"native-tls-key-file", "TLS private-key path."},
      {"native-tls-ca-cert-file", "TLS CA path."},
      {"require-tls", "Whether cleartext client connections are rejected."}
    ]

    legacy = [
      {"timeout", "Redis-compatible setting accepted for client compatibility."},
      {"tcp-keepalive", "Redis-compatible setting accepted for client compatibility."},
      {"databases", "Redis-compatible setting accepted for client compatibility."},
      {"bind", "Redis-compatible setting accepted for client compatibility."},
      {"port", "Redis-compatible setting accepted for client compatibility."},
      {"save", "Redis-compatible setting accepted for client compatibility."},
      {"appendonly", "Redis-compatible setting accepted for client compatibility."},
      {"loglevel", "Redis-compatible setting accepted for client compatibility."},
      {"requirepass", "Redis-compatible setting accepted for client compatibility."}
    ]

    local = [
      {"log_level", "Node-local Logger level. Not persisted or replicated."}
    ]

    Enum.map(read_write, &config_parameter_entry(&1, "runtime", "read-write")) ++
      Enum.map(read_only, &config_parameter_entry(&1, "runtime", "read-only")) ++
      Enum.map(legacy, &config_parameter_entry(&1, "redis-compatible", "read-write")) ++
      Enum.map(local, &config_parameter_entry(&1, "current node", "node-local"))
  end

  def config_parameter_entry({parameter, notes}, scope, mutability) do
    %{parameter: parameter, scope: scope, mutability: mutability, notes: notes}
  end

  def render_config_commands(commands) do
    render_config_command_table("Configuration Commands", commands)
  end

  def render_config_command_table(title, commands) do
    rows =
      Enum.map_join(commands, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.command)}</td>
          <td>#{escape(entry.scope)}</td>
          <td><span class="badge badge-idle">#{escape(entry.mutability)}</span></td>
          <td>#{escape(entry.notes)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">#{escape(title)} <span class="badge badge-idle">#{length(commands)}</span></div>
    <table>
      <thead>
        <tr><th>Command</th><th>Scope</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_config_parameters(parameters) do
    read_write = Enum.count(parameters, &(&1.mutability == "read-write"))
    read_only = Enum.count(parameters, &(&1.mutability == "read-only"))
    node_local = Enum.count(parameters, &(&1.mutability == "node-local"))

    rows =
      Enum.map_join(parameters, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.parameter)}</td>
          <td>#{escape(entry.scope)}</td>
          <td><span class="badge badge-idle">#{escape(entry.mutability)}</span></td>
          <td>#{escape(entry.notes)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Runtime Parameters <span class="badge badge-idle">read-write #{read_write}</span> <span class="badge badge-idle">read-only #{read_only}</span> <span class="badge badge-idle">node-local #{node_local}</span></div>
    <table>
      <thead>
        <tr><th>Parameter</th><th>Scope</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def render_cluster_info(cluster) do
    node_str = Atom.to_string(cluster.node_name)

    cluster_badge =
      case cluster.cluster_mode do
        :standalone ->
          ~s(<span class="badge badge-idle">single-member WARaft</span>)

        :cluster ->
          ~s(<span class="badge badge-ok">#{cluster.cluster_size}-node cluster</span>)
      end

    nodes_html =
      if cluster.cluster_size > 1 do
        node_items =
          Enum.map_join(cluster.nodes, "", fn n ->
            is_self = n == cluster.node_name
            class = if is_self, do: "c-green", else: ""
            label = if is_self, do: " (this node)", else: ""

            ~s(<span class="#{class}" style="margin-right:16px;">#{escape(Atom.to_string(n))}#{label}</span>)
          end)

        """
        <div style="margin-top:8px; font-size:0.82rem; color:#8b949e;">
          Nodes: #{node_items}
        </div>
        """
      else
        ""
      end

    """
    <div class="section-title">Cluster #{cluster_badge}</div>
    <div class="conn-row" style="flex-direction:column; align-items:flex-start;">
      <div style="font-size:0.85rem;">
        <span class="conn-label">Node: </span>
        <span class="conn-val mono">#{escape(node_str)}</span>
      </div>
      #{nodes_html}
    </div>
    """
  end

  def render_consensus_summary(raft_shards) do
    total = length(raft_shards)
    healthy = Enum.count(raft_shards, &(&1.status == :ok))
    leaders = Enum.count(raft_shards, &match?({_name, _node}, &1.leader))

    max_lag =
      Enum.reduce(raft_shards, 0, fn shard, acc ->
        max(acc, max(shard.commit_index - shard.last_applied, 0))
      end)

    render_ops_summary("WARaft Consensus Summary", [
      %{
        label: "Healthy Shards",
        value: "#{format_number(healthy)} / #{format_number(total)}",
        class: if(healthy == total, do: "c-green", else: "c-red")
      },
      %{
        label: "Max Apply Lag",
        value: format_number(max_lag),
        class: consensus_lag_class(max_lag)
      },
      %{label: "Leaders", value: "#{format_number(leaders)} / #{format_number(total)}"}
    ])
  end

  def consensus_lag_class(lag) when lag > 1_000, do: "c-red"
  def consensus_lag_class(lag) when lag > 100, do: "c-yellow"
  def consensus_lag_class(_lag), do: "c-green"

  def render_raft_table(raft_shards) do
    ok_count = Enum.count(raft_shards, &(&1.status == :ok))
    total = length(raft_shards)

    summary_badge =
      if ok_count == total do
        ~s(<span class="badge badge-ok">all ok</span>)
      else
        ~s(<span class="badge badge-pressure">#{total - ok_count} unavailable</span>)
      end

    rows =
      Enum.map_join(raft_shards, "\n", fn rs ->
        status_html =
          case rs.status do
            :ok -> ~s(<span class="c-green">ok</span>)
            _ -> ~s(<span class="c-red">unavailable</span>)
          end

        leader_html =
          case rs.leader do
            nil ->
              ~s(<span class="c-muted">none</span>)

            {name, leader_node} ->
              is_local = leader_node == node()
              class = if is_local, do: "c-green", else: ""
              leader_str = "#{name}@#{leader_node}"

              ~s(<span class="#{class} mono" title="#{escape_attr(leader_str)}">#{escape(short_consensus_member(name, leader_node))}</span>)
          end

        members_str =
          case rs.members do
            [] ->
              "-"

            members ->
              Enum.map_join(members, ", ", fn {name, n} -> short_consensus_member(name, n) end)
          end

        lag = rs.commit_index - rs.last_applied

        lag_class =
          cond do
            lag > 1000 -> "c-red"
            lag > 100 -> "c-yellow"
            true -> ""
          end

        """
        <tr>
          <td>#{rs.shard}</td>
          <td>#{status_html}</td>
          <td>#{leader_html}</td>
          <td>#{rs.current_term}</td>
          <td>#{format_number(rs.commit_index)}</td>
          <td class="#{lag_class}">#{format_number(rs.last_applied)}</td>
          <td class="mono" style="font-size:0.75rem;">#{escape(members_str)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Per-Shard WARaft State #{summary_badge}</div>
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Leader</th><th>Term</th><th>Commit Idx</th><th>Applied Idx</th><th>Members</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """
  end

  def short_consensus_member(name, node_name) do
    name_str = to_string(name)
    node_str = to_string(node_name)

    shard_label =
      case Regex.run(~r/_(\d+)$/, name_str) do
        [_, shard] -> "shard-" <> shard
        _ -> name_str
      end

    shard_label <> " @ " <> node_str
  end

  def render_clients_summary(conns, clients) do
    oldest_age =
      clients
      |> Enum.map(& &1.age_seconds)
      |> Enum.max(fn -> 0 end)

    pubsub = Enum.count(clients, &String.contains?(&1.flags, "S"))
    transactions = Enum.count(clients, &String.contains?(&1.flags, "M"))

    render_ops_summary("Client Summary", [
      %{label: "Active", value: format_number(conns.active)},
      %{
        label: "Blocked",
        value: format_number(conns.blocked),
        class: if(conns.blocked > 0, do: "c-yellow", else: "")
      },
      %{label: "Tracking", value: format_number(conns.tracking), detail: "#{pubsub} Pub/Sub"},
      %{
        label: "Transactions",
        value: format_number(transactions),
        detail: "Oldest #{format_uptime(oldest_age)}"
      }
    ])
  end

  def render_clients_table(clients) do
    rows =
      case clients do
        [] ->
          ~s(<tr><td colspan="6" class="c-muted">No active connections</td></tr>)

        _ ->
          Enum.map_join(clients, "\n", fn c ->
            id = Map.get(c, :client_id)
            id_str = if is_integer(id), do: Integer.to_string(id), else: inspect(c.pid)
            name = Map.get(c, :client_name) || "-"
            user = Map.get(c, :username) || "default"

            """
            <tr>
              <td class="mono">#{escape(id_str)}</td>
              <td>#{escape(name)}</td>
              <td class="mono">#{escape(user)}</td>
              <td class="mono">#{escape(c.peer)}</td>
              <td>#{format_uptime(c.age_seconds)}</td>
              <td>#{escape(c.flags)}</td>
            </tr>
            """
          end)
      end

    """
    <div class="section-title">Active Connections <span class="badge badge-idle">#{length(clients)}</span></div>
    <table>
      <thead>
        <tr><th>ID</th><th>Name</th><th>User</th><th>Client Address</th><th>Age</th><th>Flags</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
        <div style="margin-top:8px; font-size:0.72rem; color:#8b949e;">
      Flags: M=in MULTI transaction, S=subscribed (pub/sub), T=tracking enabled
    </div>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- FerricFlow Sub-pages
  # ---------------------------------------------------------------------------
end
