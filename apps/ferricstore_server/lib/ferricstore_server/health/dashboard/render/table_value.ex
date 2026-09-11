defmodule FerricstoreServer.Health.Dashboard.Render.TableValue do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format, only: [escape: 1, escape_attr: 1]

  def render(value, label, preview \\ nil, identity \\ nil) do
    value = to_string(value)
    {start, rest} = String.split_at(value, 36)

    if rest != "" or (is_binary(preview) and preview != value) do
      preview = preview || start <> "..."

      key =
        {label, identity || value}
        |> :erlang.term_to_binary()
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.url_encode64(padding: false)

      """
      <details class="dashboard-table-value" data-dashboard-disclosure-key="#{key}">
        <summary aria-label="#{escape_attr("Inspect full " <> label)}">#{escape(preview)}</summary>
        <pre class="dashboard-table-value-full" tabindex="0" aria-label="#{escape_attr("Full " <> label)}">#{escape(value)}</pre>
      </details>
      """
    else
      escape(value)
    end
  end
end
