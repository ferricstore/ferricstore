defmodule FerricstoreServer.Health.Dashboard.Render.RecentRates do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.Format, only: [escape_attr: 1, format_rate: 1]

  @external_resource Path.join(__DIR__, "recent_rates.js")
  @script File.read!(@external_resource)
  def script, do: @script

  @doc """
  The headline metrics row. Traffic is measured in this tab from the exact
  counters carried on the root element; `cards` are the other headline cards.
  """
  def render(data, cards \\ []) do
    counters = [
      {"commands", get_in(data, [:overview, :total_commands])},
      {"hits", get_in(data, [:hotcold, :total_hits])},
      {"misses", get_in(data, [:hotcold, :total_misses])},
      {"cold", get_in(data, [:hotcold, :total_cold])},
      {"expired", get_in(data, [:lifecycle, :expired_total])},
      {"evicted", get_in(data, [:lifecycle, :evicted_total])}
    ]

    attributes =
      Enum.map_join(counters, " ", fn {key, value} ->
        ~s(data-#{key}="#{if is_integer(value) and value >= 0, do: Integer.to_string(value), else: ""}")
      end)

    average =
      case get_in(data, [:hotcold, :ops_per_sec]) do
        rate when is_number(rate) ->
          ~s(<span class="overview-metric-note">#{format_rate(rate)}/s average since start</span>)

        _ ->
          ""
      end

    """
    <section class="overview-metrics" aria-label="Recent activity" data-dashboard-recent-rates
      data-at="#{Map.get(data, :generated_at_ms, System.system_time(:millisecond))}"
      data-run="#{escape_attr(get_in(data, [:overview, :run_id]) || "")}" data-sample-rate="#{get_in(data, [:hotcold, :sample_rate]) || ""}" #{attributes}>
      <div class="overview-metric-grid">
        <div class="overview-metric">
          <span class="overview-metric-label">Commands / s</span>
          <span class="overview-metric-value c-muted" data-recent-rate="commands">Pending</span>
          <span class="overview-metric-note"><span data-recent-rate="hits">–</span> hits · <span data-recent-rate="misses">–</span> misses · <span data-recent-rate="cold">–</span> disk reads</span>
          <svg class="overview-spark" viewBox="0 0 120 32" preserveAspectRatio="none" aria-hidden="true" data-recent-spark="commands"><polyline points=""></polyline></svg>
          <span class="overview-metric-note" data-recent-status role="status">Waiting for a second sample. Recent window: up to 60 seconds in this tab.</span>
          #{average}
        </div>
        #{Enum.join(cards)}
      </div>
      <details class="overview-observations" data-dashboard-disclosure-key="recent-rate-observations">
        <summary>Recent activity by interval</summary>
        <div class="table-scroll" role="region" aria-label="Recent observed rates" tabindex="0">
          <table><thead><tr><th>Observed at (UTC)</th><th>Interval</th><th>Commands/sec</th><th>Hits/sec (est.)</th><th>Misses/sec (est.)</th><th>Disk/sec</th></tr></thead>
          <tbody data-recent-history><tr><td colspan="6">No measured interval yet.</td></tr></tbody></table>
        </div>
      </details>
    </section>
    """
  end
end
