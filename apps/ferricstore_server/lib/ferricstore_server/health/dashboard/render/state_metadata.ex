defmodule FerricstoreServer.Health.Dashboard.Render.StateMetadata do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Format, only: [escape: 1, escape_attr: 1]

  alias FerricstoreServer.Health.Endpoint.FlowPaths

  @preview_limit 32

  def render(record, opts \\ []) do
    selected = Keyword.get(opts, :selected)

    entries =
      record
      |> flow_record_state_meta()
      |> Enum.flat_map(fn {state, meta} ->
        Enum.map(meta, fn {key, value} -> {state, key, value} end)
      end)
      |> Enum.sort_by(fn {state, key, _} -> {{state, key} != selected, state, key} end)

    case entries do
      [] ->
        ~s(<span class="badge badge-idle">none</span>)

      _ ->
        {preview, remaining} = Enum.split(entries, @preview_limit)
        badges(preview, selected) <> overflow(remaining, length(entries), record, opts)
    end
  end

  defp overflow([], _count, _record, _opts), do: ""

  defp overflow(remaining, count, record, opts) do
    if Keyword.get(opts, :compact, false) do
      path =
        FlowPaths.flow_detail_location(flow_record_id(record), flow_record_partition_key(record))

      ~s( <a class="flow-link" href="#{escape_attr(path)}#workflow-data">Inspect all #{count} entries</a>)
    else
      # Only the single-record detail expands already-loaded, storage-bounded metadata.
      ~s(<details class="flow-metadata-overflow dashboard-disclosure"><summary>Show #{length(remaining)} more entries</summary><div class="flow-metadata-entries">#{badges(remaining, nil)}</div></details>)
    end
  end

  defp badges(entries, selected) do
    Enum.map_join(entries, " ", fn {state, key, value} ->
      class = if {state, key} == selected, do: "badge badge-ok", else: "badge badge-idle"
      ~s(<span class="#{class}">#{escape("#{state}.#{key}=#{value_text(value)}")}</span>)
    end)
  end

  defp value_text(value) when is_binary(value), do: value
  defp value_text(value) when is_number(value) or is_boolean(value), do: to_string(value)
  defp value_text(value), do: inspect(value, limit: 10)
end
