defmodule FerricstoreServer.Health.Dashboard.Render.FlowIndexCatalog do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.Format

  def render(%{status: :forbidden}),
    do:
      ~s(<section aria-label="Query index lifecycle"><h2 class="section-title">Query index lifecycle</h2><p class="flow-section-note">Requires +FLOW.QUERY.INDEXES. Ask an administrator for catalog access.</p></section>)

  def render(%{status: :ok, snapshot: snapshot}) do
    rows = Map.get(snapshot, "indexes", [])
    registry = Map.get(snapshot, "registry", %{})

    """
    <section aria-labelledby="query-index-lifecycle-title">
      <h2 class="section-title" id="query-index-lifecycle-title">Query index lifecycle</h2>
      <div class="flow-section-note">Global catalog snapshot: #{format_timestamp_ms_or_dash(snapshot["observed_at_ms"])}. Catalog version #{value(registry["catalog_version"])}; registry epoch #{value(registry["epoch"])}.</div>
      #{services(snapshot)}
      <div class="table-scroll" role="region" aria-label="Query index lifecycle table" tabindex="0"><table class="flow-index-table">
        <thead><tr><th>Index</th><th>Generation</th><th>State</th><th>Validation</th><th>Statistics</th><th>Build progress</th><th>Retirement</th></tr></thead>
        <tbody>#{if rows == [], do: ~s(<tr><td colspan="7">No registered composite indexes in this catalog.</td></tr>), else: Enum.map_join(rows, "", &row/1)}</tbody>
      </table></div>
    </section>
    """
  end

  def render(_),
    do:
      ~s(<section aria-label="Query index lifecycle"><h2 class="section-title">Query index lifecycle</h2><p class="flow-alert flow-alert-error">Index status unavailable. Refresh this page; if it persists, check the query registry and lifecycle services.</p></section>)

  defp row(index) do
    build = Map.get(index, "build", %{})
    validation = Map.get(index, "validation", %{})
    retirement = Map.get(index, "retirement", %{})
    statistics = Map.get(index, "statistics", %{})

    fields =
      Enum.map_join(Map.get(index, "fields", []), ", ", &(&1["name"] <> " " <> &1["direction"]))

    """
    <tr>
      <td class="flow-index-identity mono">#{value(index["id"])}<div class="c-muted">#{escape(fields)}</div></td>
      <td><span class="mono">#{value(index["version"])}</span>#{build_identity(index["build_id"])}</td>
      <td>#{value(index["state"])}<div>#{if index["queryable"] == true, do: "Queryable", else: "Not queryable"}</div></td>
      <td>#{value(validation["status"])}<div>#{value(validation["mismatches"])} mismatches</div>#{validation_failure(validation)}<details class="flow-index-validation"><summary>Validation command</summary><p>Read-only native command for the status and validation report:</p><code>FLOW.QUERY.INDEXES #{value(index["id"])}</code></details></td>
      <td>#{value(statistics["status"])}<div>Oldest sample: #{age(statistics["oldest_age_ms"])}</div></td>
      <td>#{progress(build)} shards<div>#{value(build["scanned_records"])} records scanned</div></td>
      <td>#{value(retirement["status"])}#{if retirement["total_shards"], do: "<div>#{progress(retirement)} shards</div>", else: ""}</td>
    </tr>
    """
  end

  defp build_identity(build_id) when is_binary(build_id) and build_id != "" do
    """
    <details class="flow-index-build-identity">
      <summary>Build identity</summary>
      <div class="mono">#{escape(build_id)}</div>
      <button type="button" class="copy-btn-inline" data-copy-text="#{escape_attr(build_id)}" aria-label="Copy build ID" title="Copy build ID">Copy</button>
    </details>
    """
  end

  defp build_identity(_), do: ""

  defp validation_failure(%{"failure_reason" => reason}) when is_binary(reason),
    do:
      ~s(<div class="c-red">#{escape(reason)}. Check the validation report before retrying queries.</div>)

  defp validation_failure(_), do: ""
  defp progress(data), do: "#{value(data["completed_shards"])} / #{value(data["total_shards"])}"
  defp age(ms) when is_integer(ms), do: format_duration_ms(ms)
  defp age(_), do: "unavailable"
  defp value(nil), do: "unavailable"
  defp value(value), do: value |> to_string() |> escape()

  defp services(snapshot) do
    statuses = Map.get(snapshot, "services", %{})

    labels = [
      {"registry", "Registry"},
      {"lifecycle_worker", "Lifecycle worker"},
      {"statistics_store", "Statistics store"},
      {"statistics_worker", "Statistics worker"}
    ]

    rows =
      Enum.map_join(labels, "", fn {key, label} ->
        ~s(<div data-index-service="#{key}"><dt>#{label}</dt><dd>#{value(statuses[key])}</dd></div>)
      end)

    """
    <dl class="flow-index-services" aria-label="Query index services">#{rows}</dl>
    <p class="flow-section-note">Freshness budget: #{age(snapshot["statistics_max_age_ms"])}. Missing samples are not zero measurements; unavailable services prevent statistics collection.</p>
    """
  end
end
