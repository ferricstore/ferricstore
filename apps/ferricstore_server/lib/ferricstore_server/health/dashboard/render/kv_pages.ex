defmodule FerricstoreServer.Health.Dashboard.Render.KVPages do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview
  alias Ferricstore.Commands.Catalog.Entries
  alias FerricstoreServer.Health.Dashboard.Render.TableValue

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
        commands: ~w(HGET HSET HMGET HGETALL LPUSH RPUSH LPOP RPOP SADD SMEMBERS ZADD ZRANGE)
      },
      %{
        name: "Streams",
        purpose: "Append-only stream records, blocking reads, and consumer groups.",
        commands: ~w(XADD XLEN XRANGE XREVRANGE XREAD XTRIM XDEL XINFO XGROUP XREADGROUP XACK)
      },
      %{
        name: "Pub/Sub",
        purpose: "Ephemeral channel fanout and subscription introspection.",
        commands: ~w(PUBLISH PUBSUB SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE)
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
      <div class="kv-query-modes">
      <form class="flow-filter-form" action="/dashboard/keyspace" method="get" aria-label="Exact key inspection">
        <fieldset class="kv-query-mode">
        <legend>Inspect an exact key</legend>
        <input type="hidden" name="mode" value="exact">
        <label>Exact key
          <input class="flow-search-input mono" type="search" name="key" value="#{escape_attr(key)}" placeholder="user:123" required>
        </label>
        <label class="flow-check-label" title="Include compound metadata for readable logical keys. Protected workflow and server records remain hidden.">
          <input type="checkbox" name="include_internal" value="true"#{checked}><span>Compound metadata</span>
        </label>
        <button class="flow-search-button" type="submit">Inspect key</button>
        </fieldset>
      </form>
      <form class="flow-filter-form" action="/dashboard/keyspace" method="get" aria-label="Prefix sample">
        <fieldset class="kv-query-mode">
        <legend>Sample by prefix</legend>
        <input type="hidden" name="mode" value="prefix">
        <label>Prefix
          <input class="flow-search-input mono" type="search" name="prefix" value="#{escape_attr(prefix)}" placeholder="tenant:">
        </label>
        <label>Limit
          <input class="flow-search-input flow-filter-limit" type="number" min="1" max="#{@keyspace_dashboard_max_limit}" name="limit" value="#{limit}">
        </label>
        <label class="flow-check-label" title="Include compound metadata for readable logical keys. Protected workflow and server records remain hidden.">
          <input type="checkbox" name="include_internal" value="true"#{checked}><span>Compound metadata</span>
        </label>
        <button class="flow-search-button" type="submit">Sample keys</button>
        </fieldset>
      </form>
      </div>
      <a class="flow-filter-clear" href="/dashboard/keyspace">Clear both searches</a>
      <div class="flow-filter-note">Requires +SCAN for samples. Exact key inspection requires +GET and key read access.</div>
    </div>
    """
  end

  def render_keyspace_inspector(nil), do: ""

  def render_keyspace_inspector(%{found?: nil, key: key}) do
    """
    <div class="kv-inspector">
      <h2 class="section-title">Key Inspector</h2>
      <div class="flow-alert flow-alert-error" role="status">Key metadata unavailable for <code>#{escape(key)}</code>. A shard could not be inspected; absence is unknown. Submit the exact key again to retry.</div>
    </div>
    """
  end

  def render_keyspace_inspector(%{found?: false, key: key}) do
    """
    <div class="kv-inspector">
      <h2 class="section-title">Key Inspector</h2>
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
    searched? = Map.get(data, :searched?, true)

    body =
      case {searched?, rows} do
        {false, _rows} ->
          ~s(<tr><td colspan="8" class="c-muted">Enter an exact key or prefix to inspect key metadata. Submit an empty search only when you intentionally want a bounded sample.</td></tr>)

        {true, []} ->
          message =
            cond do
              Map.get(data, :collection_status) in [:partial, :unavailable] ->
                "Key metadata unavailable. Retry this search; absence has not been established."

              Map.get(data, :scan_limited?, false) ->
                "Scan budget reached before finding visible matches. Narrow the prefix or inspect an exact key."

              true ->
                "No key metadata matched this query."
            end

          ~s(<tr><td colspan="8" class="c-muted">#{message}</td></tr>)

        {true, _rows} ->
          Enum.map_join(rows, "\n", fn row ->
            internal =
              if Map.get(row, :internal?, false) do
                ~s(<span class="badge badge-idle">internal</span>)
              else
                ""
              end

            """
            <tr>
              <td class="mono"><a class="flow-link" href="#{key_inspector_link(row, data)}">#{escape(Map.get(row, :key, ""))}</a> #{internal}</td>
              <td>#{escape(Map.get(row, :physical_kind, Map.get(row, :type, "-")))}</td>
              <td>#{Map.get(row, :shard, "-")}</td>
              <td>#{escape(Map.get(row, :location, "-"))}</td>
              <td>#{escape(Map.get(row, :size, "-"))}</td>
              <td>#{escape(Map.get(row, :ttl, "-"))}</td>
              <td>#{format_number(Map.get(row, :lfu, 0))}</td>
              <td class="mono">#{render_physical_key(Map.get(row, :physical_key, ""))}</td>
            </tr>
            """
          end)
      end

    badge =
      if searched?, do: "#{format_number(length(rows))} keys returned", else: "awaiting query"

    limited = if Map.get(data, :scan_limited?, false), do: " · scan budget reached", else: ""

    scanned =
      case Map.get(data, :scanned_count) do
        count when is_integer(count) and searched? -> "#{format_number(count)} entries scanned. "
        _ -> ""
      end

    exclusions =
      if searched? and not get_in(data, [:filters, :include_internal]),
        do:
          "Compound metadata is excluded; enable Compound metadata to include it. Protected workflow and server records remain hidden.",
        else: ""

    availability =
      if Map.get(data, :collection_status) in [:partial, :unavailable] do
        label = if rows == [], do: "Key metadata unavailable", else: "Partial key metadata"

        ~s(<div class="flow-alert flow-alert-error" role="status">#{label}. Some shard metadata could not be read. Returned rows remain available; submit the search again to retry.</div>)
      else
        ""
      end

    table = """
    #{availability}
    <table>
      <thead>
        <tr><th>Logical Key</th><th>Physical kind</th><th>Shard</th><th>Location</th><th>Size</th><th>TTL</th><th>LFU</th><th>Physical Key (encoded)</th></tr>
      </thead>
      <tbody>#{body}</tbody>
    </table>
    """

    """
    <h2 class="section-title">Key Metadata <span class="badge badge-idle">#{badge}#{limited}</span></h2>
    #{if scanned != "" or exclusions != "", do: ~s(<p class="flow-filter-note">#{scanned}#{exclusions}</p>), else: ""}
    <p class="flow-filter-note">Physical keys use JSON string notation, including escaped control bytes. Base64 denotes invalid UTF-8.</p>
    #{accessible_table("Key metadata", table)}
    """
  end

  defp render_physical_key(key) do
    encoded =
      if String.valid?(key),
        do: "JSON " <> Jason.encode!(key),
        else: "Base64 " <> Base.encode64(key)

    TableValue.render(encoded, "physical key")
  end

  defp key_inspector_link(row, data) do
    params = %{"mode" => "exact", "key" => Map.get(row, :key, "")}
    filters = Map.get(data, :filters, %{})

    params =
      if Map.get(filters, :include_internal, false),
        do: Map.put(params, "include_internal", "true"),
        else: params

    escape_attr("/dashboard/keyspace?" <> URI.encode_query(params))
  end

  def render_commands_summary(data) do
    summary = Map.get(data, :summary, %{})
    unavailable? = Map.get(summary, :slowlog_status) == :unavailable
    count = Map.get(summary, :slowlog_entries, 0)
    slowest = Map.get(summary, :slowest_us)

    latency =
      cond do
        unavailable? -> "Unavailable"
        not is_integer(count) or count == 0 or not is_number(slowest) -> "No samples"
        true -> format_duration_us(slowest)
      end

    render_ops_summary("Command Summary", [
      %{label: "Commands", value: format_number(Map.get(summary, :total_commands, 0))},
      %{
        label: "Avg commands/sec",
        value: to_string(Map.get(summary, :ops_per_sec, 0.0)),
        detail: "since start"
      },
      %{
        label: "Slowlog",
        value: if(unavailable?, do: "Unavailable", else: format_number(count || 0))
      },
      %{label: "Slowest", value: latency}
    ])
  end

  def render_command_slowlog_table(data) do
    rows = Map.get(data, :slow_by_command, [])

    body =
      case rows do
        [] ->
          message =
            if get_in(data, [:summary, :slowlog_status]) == :unavailable,
              do: "Slow Log unavailable. Retry to collect command samples.",
              else: "No slow commands recorded."

          ~s(<tr><td colspan="4" class="c-muted">#{message}</td></tr>)

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
    <h2 class="section-title">Slow Log By Command</h2>
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
          |> Enum.map_join(" ", &command_reference_link/1)

        """
        <div class="kv-command-group" id="#{command_group_anchor(group.name)}">
          <div class="kv-command-title">#{escape(group.name)}</div>
          <div class="kv-command-purpose">#{escape(group.purpose)}</div>
          <div>#{commands}</div>
        </div>
        """
      end)

    """
    <h2 class="section-title">Command Groups</h2>
    <div class="kv-command-grid">#{body}</div>
    """
  end

  defp command_reference_link(command) do
    summary =
      case Entries.lookup_upper(command) do
        {:ok, entry} -> Map.get(entry, :summary, command)
        :error -> command
      end

    href =
      "https://github.com/ferricstore/ferricstore/blob/main/guides/commands.md#" <>
        command_guide_anchor(command)

    ~s(<a class="flow-pill mono" href="#{href}" title="#{escape_attr(summary)}">#{escape(command)}</a>)
  end

  defp command_guide_anchor(command) when command in ~w(GET MGET SET MSET STRLEN),
    do: "string-commands"

  defp command_guide_anchor(command) when command in ~w(HGET HSET HMGET HGETALL),
    do: "hash-commands"

  defp command_guide_anchor(command) when command in ~w(LPUSH RPUSH LPOP RPOP),
    do: "list-commands"

  defp command_guide_anchor(command) when command in ~w(SADD SMEMBERS), do: "set-commands"
  defp command_guide_anchor(command) when command in ~w(ZADD ZRANGE), do: "sorted-set-commands"
  defp command_guide_anchor("X" <> _command), do: "stream-commands"

  defp command_guide_anchor(command)
       when command in ~w(PUBLISH PUBSUB SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE),
       do: "pubsub-commands"

  defp command_guide_anchor("FERRICSTORE." <> _command), do: "ferricstore-native-commands"

  defp command_guide_anchor(command)
       when command in ~w(DEL EXISTS TTL PTTL EXPIRE PERSIST TYPE SCAN), do: "keygeneric-commands"

  defp command_guide_anchor(_command), do: "server-commands"

  defp command_group_anchor("Pub/Sub"), do: "pubsub"

  defp command_group_anchor(name) when is_binary(name) do
    name
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]+/, "-")
    |> String.trim("-")
    |> escape_attr()
  end

  def render_reads_summary(data) do
    hotcold = Map.fetch!(data, :hotcold)
    hot_reads = Map.get(hotcold, :hot_reads, Map.get(hotcold, :total_hot, 0))
    cold_reads = Map.get(hotcold, :cold_reads, Map.get(hotcold, :total_cold, 0))

    lookups =
      Map.get(
        hotcold,
        :total_lookups,
        hot_reads + cold_reads + Map.get(hotcold, :total_misses, 0)
      )

    render_ops_summary("Read Path Summary", [
      %{
        label: "Hit Rate",
        value: if(lookups == 0, do: "No read samples", else: "#{hotcold.hit_ratio}%")
      },
      %{
        label: "Hot Reads",
        value: format_number(hot_reads),
        detail_html: "sampled #{sampled_tag(Map.get(hotcold, :sample_rate, 1))}"
      },
      %{
        label: "Cold Reads",
        value: format_number(cold_reads),
        detail: "#{Map.get(hotcold, :cold_reads_per_sec, 0.0)}/sec average since start"
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
              <td class="mono">#{TableValue.render(row.prefix, "read prefix")}</td>
              <td>#{format_number(row.hot_reads)}</td>
              <td>#{format_number(row.cold_reads)}</td>
              <td>#{Float.round(row.cold_pct, 1)}%</td>
            </tr>
            """
          end)
      end

    """
    <h2 class="section-title">Prefix Read Pressure</h2>
    #{accessible_table("Prefix read pressure", """
    <table>
      <thead><tr><th>Prefix</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th><th>Cold %</th></tr></thead>
      <tbody>#{body}</tbody>
    </table>
    """)}
    """
  end

  # ---------------------------------------------------------------------------
  # HTML rendering -- Storage Sub-page
  # ---------------------------------------------------------------------------

  def render_storage_summary(data) do
    shards = Map.get(data, :shards, [])
    data_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.data_file_count end)
    hint_files = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.hint_file_count end)
    shard_bytes = Enum.reduce(shards, 0, fn shard, acc -> acc + shard.disk_bytes end)

    largest =
      Enum.max_by(shards, & &1.disk_bytes, fn -> %{index: "-", disk_bytes: 0} end)

    render_ops_summary("Storage Summary", [
      %{
        label: "Data directory",
        value: format_bytes(data.total_disk_bytes),
        detail: "All files, including shard and auxiliary directories"
      },
      %{
        label: "Shard data directories",
        value: format_bytes(shard_bytes),
        detail: "Sum of the shard directories below"
      },
      %{
        label: "Other directories",
        value: format_bytes(max(data.total_disk_bytes - shard_bytes, 0)),
        detail: "Data-directory files outside shard directories"
      },
      %{
        label: "Data + hint files",
        value: format_number(data.total_files),
        detail: "Recognized data and hint files only"
      },
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
    <h2 class="section-title">Per-Shard Storage</h2>
    <p class="flow-filter-note">Directory totals include every file beneath each shard data directory. Files outside those directories are counted in Other directories.</p>
    <table>
      <thead>
        <tr><th>Shard</th><th>Shard directory size</th><th>Data Files</th><th>Hint Files</th></tr>
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
