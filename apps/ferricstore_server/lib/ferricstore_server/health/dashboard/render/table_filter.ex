defmodule FerricstoreServer.Health.Dashboard.Render.TableFilter do
  @moduledoc false
  import FerricstoreServer.Health.Dashboard.Format

  def controls(id, label) do
    """
    <div class="flow-filter-row" data-dashboard-filter-control>
      <label class="flow-field" for="#{escape_attr(id)}-filter">
        <span>#{escape(label)}</span>
        <input class="flow-search-input" type="search" id="#{escape_attr(id)}-filter" data-dashboard-table-filter data-dashboard-filter-target="##{escape_attr(id)}" autocomplete="off">
      </label>
      <span class="flow-filter-note" data-dashboard-filter-status role="status" aria-live="polite"></span>
    </div>
    """
  end
end
