defmodule FerricstoreServer.Health.Dashboard.Render.Capabilities do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.Format
  import FerricstoreServer.Health.Dashboard.Render.Admin, only: [render_config_command_table: 2]
  import FerricstoreServer.Health.Dashboard.Render.Overview, only: [render_ops_summary: 2]

  @capability_order [
    :sdk,
    :health,
    :telemetry,
    :acl_management,
    :namespace_management,
    :quota_management,
    :flow_observability
  ]

  def render_management_capabilities(data) when is_map(data) do
    capabilities = Map.get(data, :capabilities, %{})

    """
    #{render_management_capability_summary(capabilities)}
    #{render_management_capability_table(capabilities)}
    #{render_config_command_table("Management Command Contract", Map.get(data, :command_reference, []))}
    """
  end

  def render_management_capability_summary(capabilities) do
    supported = Enum.count(capabilities, fn {_key, value} -> value == true end)
    unsupported = Enum.count(capabilities, fn {_key, value} -> value == false end)

    render_ops_summary("Management Capabilities", [
      %{
        label: "Supported",
        value: format_number(supported),
        class: if(supported > 0, do: "c-green", else: "")
      },
      %{
        label: "Unsupported",
        value: format_number(unsupported),
        class: if(unsupported > 0, do: "c-yellow", else: "")
      },
      %{
        label: "Probe",
        value: "FERRICSTORE.CAPABILITIES",
        detail: "stable read-only SDK/native contract"
      }
    ])
  end

  def render_management_capability_table(capabilities) when is_map(capabilities) do
    rows =
      capabilities
      |> ordered_capability_entries()
      |> Enum.map_join("\n", fn {name, value} ->
        {status, class, detail} = capability_status(value)

        """
        <tr>
          <td class="mono">#{escape(to_string(name))}</td>
          <td><span class="badge #{class}">#{escape(status)}</span></td>
          <td>#{escape(detail)}</td>
        </tr>
        """
      end)

    """
    <div class="section-title">Capability Flags</div>
    #{table_scroll("Capability flags", """
    <table>
    <thead>
    <tr><th>Capability</th><th>Status</th><th>Meaning</th></tr>
    </thead>
    <tbody>
    #{rows}
    </tbody>
    </table>
    """)}
    """
  end

  def render_management_capability_table(_capabilities),
    do: render_management_capability_table(%{})

  defp ordered_capability_entries(capabilities) do
    ordered =
      Enum.flat_map(@capability_order, fn key ->
        value = Map.get(capabilities, key, Map.get(capabilities, Atom.to_string(key), :missing))
        if value == :missing, do: [], else: [{key, value}]
      end)

    known =
      ordered
      |> Enum.flat_map(fn {key, _value} -> [key, to_string(key)] end)
      |> MapSet.new()

    extra =
      capabilities
      |> Enum.reject(fn {key, _value} ->
        MapSet.member?(known, key) or MapSet.member?(known, to_string(key))
      end)
      |> Enum.sort_by(fn {key, _value} -> to_string(key) end)

    ordered ++ extra
  end

  defp capability_status(true),
    do: {"supported", "badge-ok", "This deployment exposes the operation."}

  defp capability_status(false),
    do: {"unsupported", "badge-idle", "Clients must keep this operation disabled."}

  defp capability_status(value), do: {"custom", "badge-idle", inspect(value, limit: 8)}

  defp table_scroll(label, table) do
    """
    <div class="table-scroll" role="region" aria-label="#{escape(label)}" tabindex="0">#{table}</div>
    """
  end
end
