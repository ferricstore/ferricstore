defmodule FerricstoreServer.Health.Dashboard.Render.Prefixes do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview
  alias FerricstoreServer.Health.Dashboard.Render.TableValue

  def render_prefixes_table(data) do
    prefix_count = length(data.prefixes)
    count_label = if prefix_count == 0, do: "none", else: "#{prefix_count} prefixes"

    rows =
      case data.prefixes do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No User KV keys in this retained sample.</td></tr>)

        _ ->
          Enum.map_join(data.prefixes, "\n", fn p ->
            """
            <tr>
              <td class="mono">#{TableValue.render(p.prefix, "prefix")}</td>
              <td>#{format_number(p.keys)}</td>
              <td>#{p.pct}%</td>
              <td>#{observed_number(p.hot_reads)}</td>
              <td>#{observed_number(p.cold_reads)}</td>
            </tr>
            """
          end)
      end

    sampled_note = """
    <p class="flow-section-note">User KV only; reserved workflow records are excluded. Percentages use #{format_number(data.total_sampled)} sampled keys, not the server total. Read counters are unavailable for untracked prefixes.</p>
    #{if Map.get(data, :scan_limited?, false), do: ~s(<p class="flow-alert flow-alert-warning">Scan budget reached. This prefix sample may be incomplete.</p>), else: ""}
    """

    """
    <h2 class="section-title">Key Prefixes <span class="badge badge-idle">#{escape(count_label)}</span></h2>
    #{accessible_table("Key prefixes", """
    <table>
      <thead>
        <tr><th>Prefix</th><th>Sampled keys</th><th>% of sample</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    """)}
    #{sampled_note}
    """
  end

  def render_prefixes_summary(data) do
    total_indexed = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.keys end)
    hot_reads = observed_total(data.prefixes, :hot_reads)
    cold_reads = observed_total(data.prefixes, :cold_reads)

    render_ops_summary("Prefix Summary", [
      %{label: "Sampled User KV keys", value: format_number(data.total_sampled)},
      %{label: "Displayed keys", value: format_number(total_indexed)},
      %{label: "Displayed hot reads", value: observed_number(hot_reads)},
      %{label: "Displayed cold reads", value: observed_number(cold_reads)}
    ])
  end

  defp observed_number(nil), do: "Unavailable"
  defp observed_number(value), do: format_number(value)

  defp observed_total(rows, field) do
    if Enum.any?(rows, &is_nil(Map.get(&1, field))),
      do: nil,
      else: Enum.reduce(rows, 0, &(Map.fetch!(&1, field) + &2))
  end
end
