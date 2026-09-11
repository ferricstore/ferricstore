defmodule FerricstoreServer.Health.Dashboard.Render.RecentRates do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.Format, only: [escape: 1, escape_attr: 1]

  @external_resource Path.join(__DIR__, "recent_rates.js")
  @script File.read!(@external_resource)
  def script, do: @script

  def render(data) do
    counters = [
      {"commands", "Commands/sec", get_in(data, [:overview, :total_commands])},
      {"hits", "Hits/sec (estimated)", get_in(data, [:hotcold, :total_hits])},
      {"misses", "Misses/sec (estimated)", get_in(data, [:hotcold, :total_misses])},
      {"cold", "Disk reads/sec", get_in(data, [:hotcold, :total_cold])},
      {"expired", "Expired/sec", get_in(data, [:lifecycle, :expired_total])},
      {"evicted", "Evicted/sec", get_in(data, [:lifecycle, :evicted_total])}
    ]

    attributes =
      Enum.map_join(counters, " ", fn {key, _label, value} ->
        ~s(data-#{key}="#{if is_integer(value) and value >= 0, do: Integer.to_string(value), else: ""}")
      end)

    values =
      Enum.map_join(counters, "", fn {key, label, _value} ->
        """
        <div class="ops-summary-card">
          <div class="ops-summary-label">#{escape(label)}</div>
          <div class="ops-summary-value c-muted" data-recent-rate="#{key}">Pending</div>
        </div>
        """
      end)

    """
    <section class="recent-rates" aria-label="Recent activity" data-dashboard-recent-rates
      data-at="#{Map.get(data, :generated_at_ms, System.system_time(:millisecond))}"
      data-run="#{escape_attr(get_in(data, [:overview, :run_id]) || "")}" data-sample-rate="#{get_in(data, [:hotcold, :sample_rate]) || ""}" #{attributes}>
      <h2 class="section-title">Recent activity</h2>
      <p class="flow-filter-note" data-recent-status role="status">Waiting for a second sample. Recent window: up to 60 seconds in this tab.</p>
      <div class="ops-summary-grid">#{values}</div>
      <details data-dashboard-disclosure-key="recent-rate-observations">
        <summary>Recent observations</summary>
        <div class="table-scroll" role="region" aria-label="Recent observed rates" tabindex="0">
          <table><thead><tr><th>Observed at (UTC)</th><th>Interval</th><th>Commands/sec</th><th>Hits/sec (est.)</th><th>Misses/sec (est.)</th><th>Disk/sec</th></tr></thead>
          <tbody data-recent-history><tr><td colspan="6">No measured interval yet.</td></tr></tbody></table>
        </div>
      </details>
    </section>
    """
  end
end
