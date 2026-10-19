defmodule FerricstoreServer.Health.Dashboard.Render.PageStatus do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format

  @states [:healthy, :warning, :degraded, :idle]

  @doc """
  One line at the top of a page that says what matters: a state, a short title and
  optional details. `details` are plain strings (escaped here); `action` is trusted
  HTML, such as a link built with escaped attributes.
  """
  def render(name, state, title, details \\ [], action \\ "")
      when is_binary(name) and state in @states and is_list(details) do
    detail_html = Enum.map_join(details, "", &~s(<span>#{escape(&1)}</span>))

    action_html =
      if action == "", do: "", else: ~s(<span class="page-status-action">#{action}</span>)

    """
    <section class="page-status #{name}-status is-#{state}" aria-label="#{escape_attr(title)}">
      <strong>#{escape(title)}</strong>#{detail_html}#{action_html}
    </section>
    """
  end

  @doc "Shortens a stored name (a key prefix, a command) for a status line."
  def short(name, max \\ 40) do
    name = to_string(name)
    if String.length(name) > max, do: String.slice(name, 0, max) <> "…", else: name
  end
end
