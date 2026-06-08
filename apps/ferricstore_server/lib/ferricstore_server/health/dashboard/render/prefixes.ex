defmodule FerricstoreServer.Health.Dashboard.Render.Prefixes do
  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Overview

  def render_prefixes_table(data) do
    prefix_count = length(data.prefixes)
    count_label = if prefix_count == 0, do: "none", else: "#{prefix_count} prefixes"

    rows =
      case data.prefixes do
        [] ->
          ~s(<tr><td colspan="5" class="c-muted">No keys found</td></tr>)

        _ ->
          Enum.map_join(data.prefixes, "\n", fn p ->
            """
            <tr>
              <td class="mono">#{escape(p.prefix)}</td>
              <td>#{format_number(p.keys)}</td>
              <td>#{p.pct}%</td>
              <td>#{format_number(p.hot_reads)}</td>
              <td>#{format_number(p.cold_reads)}</td>
            </tr>
            """
          end)
      end

    sampled_note =
      if data.total_sampled > 0 do
        ~s(<div style="margin-top:8px; font-size:0.72rem; color:#8b949e;">Sampled #{format_number(data.total_sampled)} keys from keydir ETS tables</div>)
      else
        ""
      end

    """
    <div class="section-title">Key Prefixes <span class="badge badge-idle">#{escape(count_label)}</span></div>
    <table>
      <thead>
        <tr><th>Prefix</th><th>Keys</th><th>% of Total</th><th>Hot Reads #{sampled_tag(:persistent_term.get(:ferricstore_read_sample_rate, 100))}</th><th>Cold Reads</th></tr>
      </thead>
      <tbody>
        #{rows}
      </tbody>
    </table>
    #{sampled_note}
    """
  end

  def render_prefixes_summary(data) do
    total_indexed = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.keys end)
    hot_reads = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.hot_reads end)
    cold_reads = Enum.reduce(data.prefixes, 0, fn prefix, acc -> acc + prefix.cold_reads end)

    render_ops_summary("Prefix Summary", [
      %{label: "Sampled Keys", value: format_number(data.total_sampled)},
      %{label: "Indexed Keys", value: format_number(total_indexed)},
      %{label: "Hot Reads", value: format_number(hot_reads)},
      %{label: "Cold Reads", value: format_number(cold_reads)}
    ])
  end
end
