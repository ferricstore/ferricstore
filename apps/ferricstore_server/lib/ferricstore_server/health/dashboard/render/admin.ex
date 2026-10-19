defmodule FerricstoreServer.Health.Dashboard.Render.Admin do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview
  alias FerricstoreServer.Health.Dashboard.Render.{PageStatus, TableFilter, TableValue}

  def slowlog_data(data) do
    %{
      status: Map.get(data, :slowlog_status, :ok),
      entries: Map.get(data, :slowlog, []),
      error: Map.get(data, :slowlog_error)
    }
  end

  def render_slowlog_summary(%{status: :unavailable}) do
    PageStatus.render("slowlog", :warning, "Slow log unavailable", [
      "slow commands could not be collected"
    ]) <>
      render_ops_summary("Slow Log unavailable", [
        %{label: "Entries", value: "Unavailable", class: "c-muted"},
        %{label: "Worst", value: "Unavailable", class: "c-muted"},
        %{label: "Avg", value: "Unavailable", class: "c-muted"}
      ])
  end

  def render_slowlog_summary(%{entries: entries}), do: render_slowlog_summary(entries)

  def render_slowlog_summary(entries) do
    count = length(entries)
    total_us = Enum.reduce(entries, 0, fn entry, acc -> acc + max(entry.duration_us, 0) end)
    worst_us = Enum.reduce(entries, 0, fn entry, acc -> max(acc, max(entry.duration_us, 0)) end)
    avg_us = if count > 0, do: div(total_us, count), else: 0

    status =
      case Enum.max_by(entries, &max(&1.duration_us, 0), fn -> nil end) do
        nil ->
          PageStatus.render("slowlog", :healthy, "No slow commands recorded")

        worst ->
          name = worst |> Map.get(:command, []) |> List.wrap() |> List.first() |> to_string()

          PageStatus.render(
            "slowlog",
            :idle,
            "#{format_number(count)} slow #{if count == 1, do: "command", else: "commands"} recorded",
            [
              "worst #{format_duration_us(worst_us)}#{if name == "", do: "", else: ": " <> name}",
              "average #{format_duration_us(avg_us)}"
            ]
          )
      end

    status <>
      render_ops_summary(
        "Slow Log Summary",
        [
          %{label: "Entries", value: format_number(count)},
          %{
            label: "Worst",
            value: if(count > 0, do: format_duration_us(worst_us), else: "No samples"),
            class:
              if(count > 0,
                do: slowlog_duration_class(worst_us),
                else: "ops-summary-code c-muted"
              )
          },
          %{
            label: "Avg",
            value: if(count > 0, do: format_duration_us(avg_us), else: "No samples"),
            class: if(count > 0, do: "", else: "ops-summary-code c-muted")
          },
          %{label: "Total time", value: format_duration_us(total_us)}
        ],
        heading: :hidden
      )
  end

  def slowlog_duration_class(duration_us) when duration_us >= 1_000_000, do: "c-red"
  def slowlog_duration_class(duration_us) when duration_us >= 100_000, do: "c-yellow"
  def slowlog_duration_class(_duration_us), do: ""

  def render_slowlog_table(%{status: :unavailable} = snapshot) do
    """
    <h2 class="section-title">Slow Log unavailable</h2>
    <div class="flow-alert flow-alert-error" role="status">
      <p>#{escape(Map.get(snapshot, :error) || "Slow commands could not be collected.")}</p>
      <a class="flow-link" href="/dashboard/slowlog">Retry Slow Log</a>
    </div>
    """
  end

  def render_slowlog_table(%{entries: entries}), do: render_slowlog_table(entries)

  def render_slowlog_table([]),
    do: """
    <h2 class="section-title">Slow Log <span class="badge badge-idle">none</span></h2>
    <p class="section-empty">No slow commands recorded.</p>
    """

  def render_slowlog_table(entries) do
    count = length(entries)
    count_label = if count == 0, do: "none", else: "#{count} entries"

    rows =
      Enum.map_join(entries, "\n", fn entry ->
        cmd_str = Enum.join(entry.command, " ")
        duration_ms = Float.round(entry.duration_us / 1000.0, 2)
        time_str = format_timestamp_us(entry.timestamp_us)

        """
        <tr>
          <td>#{entry.id}</td>
          <td class="mono">#{escape(time_str)}</td>
          <td>#{duration_ms} ms</td>
          <td class="mono">#{TableValue.render(cmd_str, "command", nil, entry.id)}</td>
        </tr>
        """
      end)

    """
    <h2 class="section-title">Slow Log <span class="badge badge-idle">#{escape(count_label)}</span></h2>
    #{TableFilter.controls("slowlog-table", "Filter loaded slow commands")}
    #{accessible_table("Slow log entries", """
    <table id="slowlog-table">
      <thead>
        <tr><th>ID</th><th>Time</th><th>Duration</th><th>Command</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """)}
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

    shards = "#{format_number(total)} #{if total == 1, do: "shard", else: "shards"}"

    status =
      cond do
        active > 0 ->
          PageStatus.render(
            "merges",
            :idle,
            "#{format_number(active)} of #{shards} merging now",
            [
              "#{format_number(total_merges)} merges since start",
              "#{format_bytes(reclaimed)} reclaimed"
            ]
          )

        total_merges == 0 ->
          PageStatus.render("merges", :idle, "No merges since start", ["all #{shards} idle"])

        true ->
          ago =
            if is_integer(latest_merge),
              do: [
                "last merge #{format_duration_ms(max(System.system_time(:millisecond) - latest_merge, 0))} ago"
              ],
              else: []

          PageStatus.render(
            "merges",
            :healthy,
            "No merge running",
            ago ++ ["#{format_number(total_merges)} merges reclaimed #{format_bytes(reclaimed)}"]
          )
      end

    status <>
      render_ops_summary(
        "Merge Summary",
        [
          %{
            label: "Active shards",
            value: "#{format_number(active)} / #{format_number(total)}",
            class: if(active > 0, do: "c-yellow", else: "")
          },
          %{label: "Total reclaimed", value: format_bytes(reclaimed)},
          %{label: "Total merges", value: format_number(total_merges)},
          %{label: "Last merge", value: latest_label}
        ],
        heading: :hidden
      )
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

    table = """
    <table>
      <thead>
        <tr><th>Shard</th><th>Mode</th><th>Status</th><th>Last merge</th><th>Merges</th><th>Reclaimed</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """

    # Sixteen identical "idle, never" rows say nothing the status line does not.
    if active_count == 0 and Enum.all?(merges, &(&1.merge_count <= 0)) do
      ~s(<details class="dashboard-disclosure"><summary>Per-shard merge status</summary><div class="dashboard-disclosure-body">#{table}</div></details>)
    else
      """
      <h2 class="section-title">Merge status <span class="badge badge-idle">#{escape(summary_label)}</span></h2>
      #{table}
      """
    end
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
          ~s[<p class="section-empty">All namespaces use the built-in default window (1 ms).</p>]

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
    <h2 class="section-title">Namespace Config <span class="badge badge-idle">#{escape(count_label)}</span></h2>
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
      {"notify-keyspace-events", "Keyspace notification setting."},
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

    redis_compatible = [
      {"timeout", "Redis-compatible CONFIG value retained for client tooling."},
      {"tcp-keepalive", "Redis-compatible CONFIG value retained for client tooling."},
      {"databases", "Redis-compatible CONFIG value retained for client tooling."},
      {"bind", "Redis-compatible CONFIG value retained for client tooling."},
      {"save", "Redis-compatible CONFIG value retained for client tooling."},
      {"appendonly", "Redis-compatible CONFIG value retained for client tooling."},
      {"loglevel", "Redis-compatible CONFIG value retained for client tooling."},
      {"requirepass", "Default-user password used by AUTH and protected-mode checks."}
    ]

    local = [
      {"log_level", "Node-local Logger level. Not persisted or replicated."}
    ]

    Enum.map(read_write, &config_parameter_entry(&1, "runtime", "read-write")) ++
      Enum.map(read_only, &config_parameter_entry(&1, "runtime", "read-only")) ++
      Enum.map(redis_compatible, &config_parameter_entry(&1, "Redis-compatible", "read-write")) ++
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
    <h2 class="section-title">#{escape(title)} <span class="badge badge-idle">#{length(commands)}</span></h2>
    <div class="table-scroll" role="region" aria-label="#{escape(title)}" tabindex="0">
    <table>
      <thead>
        <tr><th>Command</th><th>Scope</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    </div>
    """
  end

  def render_config_parameters(parameters) do
    read_write = Enum.count(parameters, &(&1.mutability == "read-write"))
    read_only = Enum.count(parameters, &(&1.mutability == "read-only"))
    node_local = Enum.count(parameters, &(&1.mutability == "node-local"))

    unavailable = Enum.count(parameters, &is_nil(Map.get(&1, :value)))

    status =
      PageStatus.render(
        "config",
        :idle,
        "#{format_number(length(parameters))} runtime parameters",
        ["#{read_write} read-write", "#{read_only} read-only", "#{node_local} node-local"] ++
          if(unavailable > 0, do: ["#{unavailable} unavailable"], else: [])
      )

    rows =
      Enum.map_join(parameters, "\n", fn entry ->
        """
        <tr>
          <td class="mono">#{escape(entry.parameter)}#{config_origin(entry)}</td>
          <td class="mono">#{config_value(entry.parameter, Map.get(entry, :value))}</td>
          <td><span class="badge badge-idle">#{escape(entry.mutability)}</span></td>
          <td>#{escape(entry.notes)}</td>
        </tr>
        """
      end)

    """
    #{status}
    <h2 class="section-title">Runtime Parameters</h2>
    <p class="flow-section-note">Values come from CONFIG GET unless a row names another source.</p>
    #{TableFilter.controls("config-parameters", "Filter parameters")}
    <div class="table-scroll" role="region" aria-label="Runtime parameters" tabindex="0">
    <table class="config-parameters-table" id="config-parameters">
      <colgroup><col class="config-name"><col class="config-value"><col class="config-mode"><col class="config-notes"></colgroup>
      <thead>
        <tr><th>Parameter</th><th>Effective value</th><th>Mode</th><th>Notes</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    </div>
    """
  end

  # Most parameters are runtime values read with CONFIG GET; only the exceptions say so.
  defp config_origin(entry) do
    scope = Map.get(entry, :scope)
    source = Map.get(entry, :source)

    parts =
      [
        if(scope not in [nil, "runtime"], do: scope),
        if(source not in [nil, "CONFIG GET"], do: source)
      ]
      |> Enum.reject(&is_nil/1)

    if parts == [],
      do: "",
      else: ~s(<span class="flow-run-secondary">#{escape(Enum.join(parts, " · "))}</span>)
  end

  defp config_value("requirepass", _value), do: "Redacted"
  defp config_value(_parameter, nil), do: "Unavailable"

  defp config_value("native-tls-port", value) when value in [0, "0"],
    do: ~s(0 <span class="flow-filter-note">TLS listener not configured</span>)

  defp config_value("notify-keyspace-events", ""),
    do: ~s(Empty string <span class="flow-filter-note">Notifications disabled</span>)

  defp config_value("maxmemory", value) when value in [0, "0"],
    do: ~s(0 bytes <span class="flow-filter-note">No explicit process-memory ceiling</span>)

  defp config_value(parameter, "")
       when parameter in [
              "tls-cert-file",
              "tls-key-file",
              "tls-ca-cert-file",
              "native-tls-cert-file",
              "native-tls-key-file",
              "native-tls-ca-cert-file"
            ],
       do: "Not configured"

  defp config_value(_parameter, ""), do: "Empty string"

  defp config_value(parameter, value)
       when parameter in [
              "keydir-max-ram",
              "hot-cache-max-ram",
              "hot-cache-min-ram",
              "hot-cache-max-value-size",
              "maxmemory"
            ] do
    case Integer.parse(to_string(value)) do
      {bytes, ""} when bytes >= 0 ->
        ~s(#{format_bytes(bytes)} <span class="flow-filter-note">#{bytes} bytes</span>)

      _ ->
        escape(to_string(value))
    end
  end

  defp config_value(_parameter, value), do: escape(to_string(value))

  def render_cluster_info(cluster) do
    mode =
      case cluster.cluster_mode do
        :standalone -> ~s(<span class="badge badge-idle">single-member WARaft</span>)
        :cluster -> ~s(<span class="badge badge-ok">#{cluster.cluster_size}-node cluster</span>)
      end

    nodes = if cluster.cluster_size > 1, do: cluster.nodes, else: [cluster.node_name]

    items =
      Enum.map_join(nodes, "", fn node_name ->
        self? = node_name == cluster.node_name
        label = if self?, do: ~s( <span class="c-muted">this node</span>), else: ""
        ~s(<li><span class="mono">#{escape(Atom.to_string(node_name))}</span>#{label}</li>)
      end)

    note =
      if cluster.cluster_size > 1,
        do: "",
        else:
          ~s(<p class="consensus-card-note">One member keeps one copy of each shard, so there is no failover.</p>)

    """
    <section class="consensus-card" aria-labelledby="consensus-members-title">
      <div class="consensus-card-head"><h2 id="consensus-members-title">Members</h2>#{mode}</div>
      <ul class="consensus-members">#{items}</ul>
      #{note}
    </section>
    """
  end

  @doc """
  One line on whether every shard has a leader and is applying what it
  commits; anything else is named.
  """
  def render_consensus_status(raft_shards) do
    total = length(raft_shards)
    unreadable = Enum.count(raft_shards, &(&1.status != :ok))
    unavailable = Enum.count(raft_shards, &(&1.status == :unavailable))
    leaderless = Enum.count(raft_shards, &(&1.status == :ok and is_nil(&1.leader)))

    behind =
      raft_shards
      |> Enum.map(&{&1.shard, consensus_lag(&1)})
      |> Enum.filter(fn {_shard, lag} -> is_integer(lag) and lag > 100 end)
      |> Enum.sort_by(fn {_shard, lag} -> -lag end)

    largest = behind |> Enum.map(&elem(&1, 1)) |> Enum.max(fn -> 0 end)

    issues =
      [
        unreadable > 0 &&
          "#{unreadable} of #{total} #{plural(total, "shard")} could not be fully read",
        leaderless > 0 &&
          "#{leaderless} #{plural(leaderless, "shard")} #{if leaderless == 1, do: "has", else: "have"} no leader",
        case behind do
          [] ->
            false

          [{shard, lag}] ->
            "Shard #{shard} is #{lag} entries behind"

          many ->
            "#{length(many)} shards are behind on applying, the largest by #{largest} entries"
        end
      ]
      |> Enum.filter(&is_binary/1)

    {state, title, detail} =
      cond do
        total == 0 ->
          {"warning", "No shards reported", "The consensus snapshot is empty."}

        issues == [] ->
          lags = raft_shards |> Enum.map(&consensus_lag/1)

          if Enum.all?(lags, &(&1 == 0)) do
            {"healthy", "All #{total} #{plural(total, "shard")} have a leader and are caught up",
             "Every committed entry is applied to storage."}
          else
            {"healthy", "All #{total} #{plural(total, "shard")} have a leader and are keeping up",
             "The largest apply lag is #{Enum.max(lags)} entries."}
          end

        unavailable > 0 or leaderless > 0 or largest > 1_000 ->
          {"degraded", Enum.join(issues, ". "),
           "Open the shard details below, and run the Doctor checks if this persists."}

        true ->
          {"warning", Enum.join(issues, ". "),
           "Shards normally catch up on their own; if a gap keeps growing, run the Doctor checks."}
      end

    """
    <section class="consensus-status is-#{state}" aria-label="Consensus status">
      <strong>#{escape(title)}</strong>
      <span>#{escape(detail)}</span>
    </section>
    """
  end

  def render_consensus_summary(raft_shards) do
    total = length(raft_shards)
    healthy = Enum.count(raft_shards, &(&1.status == :ok))
    leaders = Enum.count(raft_shards, &match?({_name, _node}, &1.leader))

    lags = raft_shards |> Enum.map(&consensus_lag/1) |> Enum.filter(&is_integer/1)
    complete? = total > 0 and length(lags) == total
    max_lag = Enum.max(lags, fn -> nil end)
    terms = raft_shards |> Enum.map(& &1.current_term) |> Enum.filter(&is_integer/1)

    {lag_value, lag_class, lag_note} =
      if complete?,
        do:
          {Integer.to_string(max_lag), consensus_stat_class(max_lag),
           "commit minus storage-applied index"},
        else: {"Unavailable", " is-unknown", "#{length(lags)} / #{total} shards measured"}

    cards = [
      {"Shards with a leader", "#{leaders} / #{total}",
       if(leaders == total, do: "", else: " is-degraded"), ""},
      {"Largest apply lag", lag_value, lag_class, lag_note},
      {"Highest term",
       if(terms == [], do: "Unavailable", else: Integer.to_string(Enum.max(terms))),
       if(terms == [], do: " is-unknown", else: ""), ""},
      {"Readable shards", "#{healthy} / #{total}",
       if(healthy == total, do: "", else: " is-warning"), ""}
    ]

    items =
      Enum.map_join(cards, "", fn {label, value, class, note} ->
        note_html =
          if note == "",
            do: "",
            else: ~s(<span class="consensus-stat-note">#{escape(note)}</span>)

        """
        <div class="consensus-stat">
          <span class="consensus-stat-label">#{escape(label)}</span>
          <div class="consensus-stat-value#{class}">#{escape(value)}</div>
          #{note_html}
        </div>
        """
      end)

    ~s(<section class="consensus-stats" aria-label="Consensus summary">#{items}</section>)
  end

  defp consensus_stat_class(lag) when lag > 1_000, do: " is-degraded"
  defp consensus_stat_class(lag) when lag > 100, do: " is-warning"
  defp consensus_stat_class(_lag), do: ""

  @doc """
  One tile per shard: its commit index and whether storage keeps up with it.
  """
  def render_consensus_grid(raft_shards) do
    tiles =
      Enum.map_join(raft_shards, "", fn shard ->
        lag = consensus_lag(shard)

        {class, note} =
          cond do
            shard.status != :ok -> {"is-down", Atom.to_string(shard.status)}
            is_nil(shard.leader) -> {"is-down", "no leader"}
            lag == 0 -> {"is-ok", "caught up"}
            is_integer(lag) and lag > 100 -> {"is-behind", "#{lag} behind"}
            is_integer(lag) -> {"is-ok", "#{lag} behind"}
            true -> {"is-down", "lag unknown"}
          end

        leader =
          case shard.leader do
            {name, leader_node} -> "#{name}@#{leader_node}"
            nil -> "none"
          end

        title =
          "Shard #{shard.shard} · leader #{leader} · term #{consensus_index(shard.current_term)} · committed #{consensus_index(shard.commit_index)} · applied #{consensus_index(shard.last_applied)}"

        ~s(<li class="consensus-shard #{class}" title="#{escape_attr(title)}"><span class="consensus-shard-head">Shard #{shard.shard}</span><strong>#{consensus_index(shard.commit_index)}</strong><span class="consensus-shard-note">#{escape(note)}</span></li>)
      end)

    """
    <section class="consensus-card consensus-grid-card" aria-labelledby="consensus-grid-title">
      <div class="consensus-card-head"><h2 id="consensus-grid-title">Shards</h2><span class="consensus-card-note">Commit index per shard</span></div>
      <ol class="consensus-grid"#{consensus_columns(length(raft_shards))}>#{tiles}</ol>
    </section>
    """
  end

  # Even rows when the shard count allows them.
  defp consensus_columns(total) when total > 8 and rem(total, 8) == 0,
    do: ~s( style="--shard-columns: 8")

  defp consensus_columns(_total), do: ""

  defp plural(1, word), do: word
  defp plural(_count, word), do: word <> "s"

  defp consensus_lag(%{commit_index: commit, last_applied: applied})
       when is_integer(commit) and is_integer(applied), do: max(commit - applied, 0)

  defp consensus_lag(_shard), do: nil

  defp consensus_index(value) when is_integer(value), do: Integer.to_string(value)
  defp consensus_index(_value), do: "Unavailable"

  def consensus_lag_class(nil), do: "c-muted"
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
            :partial -> ~s(<span class="c-yellow">partial</span>)
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

              ~s(<span class="#{class} mono">#{TableValue.render(leader_str, "leader", short_consensus_member(name, leader_node), rs.shard)}</span>)
          end

        members_str =
          case rs.members do
            [] ->
              "-"

            members ->
              Enum.map_join(members, ", ", fn {name, n} -> "#{name}@#{n}" end)
          end

        lag_class = consensus_lag_class(consensus_lag(rs))

        """
        <tr>
          <td>#{rs.shard}</td>
          <td>#{status_html}</td>
          <td>#{leader_html}</td>
          <td>#{consensus_index(rs.current_term)}</td>
          <td>#{consensus_index(rs.commit_index)}</td>
          <td class="#{lag_class}">#{consensus_index(rs.last_applied)}</td>
          <td class="mono">#{TableValue.render(members_str, "consensus members", nil, rs.shard)}</td>
        </tr>
        """
      end)

    """
    <p class="flow-section-note">#{summary_badge}</p>
    #{accessible_table("Per-shard consensus state", """
    <table>
      <thead>
        <tr><th>Shard</th><th>Status</th><th>Leader</th><th>Term</th><th>Commit Idx</th><th>Storage Applied Idx</th><th>Members</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """)}
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
    shown? = Map.get(conns, :summary_scope) == :shown

    oldest_age =
      Map.get_lazy(conns, :oldest_age_seconds, fn ->
        clients
        |> Enum.map(& &1.age_seconds)
        |> Enum.max(fn -> 0 end)
      end)

    pubsub =
      Map.get_lazy(conns, :pubsub, fn -> Enum.count(clients, &String.contains?(&1.flags, "S")) end)

    transactions =
      Map.get_lazy(conns, :transactions, fn ->
        Enum.count(clients, &String.contains?(&1.flags, "M"))
      end)

    status =
      cond do
        conns.active == 0 ->
          PageStatus.render("clients", :idle, "No native connections")

        conns.blocked > 0 ->
          PageStatus.render(
            "clients",
            :warning,
            "#{format_number(conns.blocked)} blocked of #{format_number(conns.active)} connections",
            ["blocked clients wait on a blocking command such as XREAD or BLPOP"]
          )

        true ->
          PageStatus.render(
            "clients",
            :healthy,
            "#{format_number(conns.active)} native #{if conns.active == 1, do: "connection", else: "connections"}",
            ["none blocked"]
          )
      end

    status <>
      render_ops_summary(
        "Client Summary",
        [
          %{label: "Active", value: format_number(conns.active)},
          %{
            label: "Blocked",
            value: format_number(conns.blocked),
            class: if(conns.blocked > 0, do: "c-yellow", else: "")
          },
          %{
            label: "Tracking",
            value: format_number(conns.tracking),
            detail: "#{pubsub} #{if shown?, do: "shown ", else: ""}Pub/Sub"
          },
          %{
            label: if(shown?, do: "Shown transactions", else: "Transactions"),
            value: format_number(transactions),
            detail: "Oldest #{if shown?, do: "shown ", else: ""}#{format_uptime(oldest_age)}"
          }
        ],
        heading: :hidden
      )
  end

  def render_clients_page_table(data) do
    coverage = Map.get(data, :client_coverage, %{})
    filters = Map.get(data, :client_filters, %{q: "", cursor: ""})
    clients = Map.get(data, :clients, [])
    total = Map.get(coverage, :total_registered)
    next_cursor = Map.get(coverage, :next_cursor)

    next =
      if next_cursor do
        href =
          "/dashboard/clients?" <> URI.encode_query(%{"q" => filters.q, "cursor" => next_cursor})

        ~s(<a class="flow-link" href="#{escape_attr(href)}">Next connections</a>)
      else
        ""
      end

    coverage_note =
      cond do
        Map.get(coverage, :status) == :invalid_filters ->
          "Search must be 256 characters or fewer; page cursors must be at most 128 bytes. Correct the search and retry."

        Map.get(coverage, :status) == :expired_cursor ->
          "Continuation expired after a connection closed. Restart search to inspect current connections."

        Map.get(coverage, :status) == :unavailable ->
          "Client registry unavailable. Restart the search to retry."

        Map.get(coverage, :complete?, true) ->
          "End of the registry traversal."

        Map.get(coverage, :scanned_count, 0) >= 10_000 ->
          "Scan budget reached. Continue to inspect remaining connections."

        true ->
          "More registered connections remain."
      end

    """
    <form class="flow-filter-form" action="/dashboard/clients" method="get" aria-label="Search registered connections">
      <label class="flow-field">Name, user, address, ID or flags
        <input class="flow-search-input" type="search" name="q" maxlength="256" value="#{escape_attr(filters.q)}">
      </label>
      <button class="flow-search-button" type="submit">Search connections</button>
      <a class="flow-filter-clear" href="/dashboard/clients">Restart search</a>
    </form>
    <p class="flow-filter-note" role="status">#{length(clients)} shown / #{if is_integer(total), do: format_number(total), else: "unknown"} registered. #{coverage_note} Pages are live registry traversals; connections can change between requests.</p>
    #{render_clients_table(clients, if(Map.get(coverage, :status, :ok) != :ok, do: "No page collected; correct the search or restart to retry", else: "No matching connections in this bounded page"))}
    <nav aria-label="Connection pages">#{next}</nav>
    """
  end

  def clients_live_url(data) do
    filters = Map.get(data, :client_filters, %{q: "", cursor: ""})

    query =
      [{"q", filters.q}, {"cursor", filters.cursor}]
      |> Enum.reject(fn {_key, value} -> value == "" end)
      |> URI.encode_query()

    "/dashboard/api/clients" <> if(query == "", do: "", else: "?" <> query)
  end

  def render_clients_table(clients, empty_message \\ "No active connections")

  def render_clients_table([], empty_message),
    do: """
    <h2 class="section-title">Active Connections <span class="badge badge-idle">0</span></h2>
    <p class="section-empty">#{escape(empty_message)}</p>
    """

  def render_clients_table(clients, _empty_message) do
    rows =
      Enum.map_join(clients, "\n", fn c ->
        id = Map.get(c, :client_id)
        id_str = if is_integer(id), do: Integer.to_string(id), else: inspect(c.pid)
        name = Map.get(c, :client_name) || "-"
        user = Map.get(c, :username) || "default"

        """
        <tr>
          <td class="mono">#{TableValue.render(id_str, "client ID", nil, id_str)}</td>
          <td>#{TableValue.render(name, "client name", nil, id_str)}</td>
          <td class="mono">#{TableValue.render(user, "username", nil, id_str)}</td>
          <td class="mono">#{TableValue.render(c.peer, "client address", nil, id_str)}</td>
          <td>#{format_uptime(c.age_seconds)}</td>
          <td>#{TableValue.render(c.flags, "client flags", nil, id_str)}</td>
        </tr>
        """
      end)

    """
    <h2 class="section-title">Active Connections <span class="badge badge-idle">#{length(clients)}</span></h2>
    #{TableFilter.controls("clients-table", "Filter loaded clients by user, name or address")}
    #{accessible_table("Active connections", """
    <table id="clients-table">
      <thead>
        <tr><th>ID</th><th>Name</th><th>User</th><th>Client Address</th><th>Age</th><th>Flags</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """)}
    <p class="flow-section-note">Flags: M=in MULTI transaction, S=subscribed (pub/sub), T=tracking enabled</p>
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- FerricFlow Sub-pages
  # ---------------------------------------------------------------------------
end
