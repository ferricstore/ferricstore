defmodule FerricstoreServer.Connection.Dashboard do
  @moduledoc false

  def summary(state) do
    %{
      client_id: state.client_id,
      client_name: state.client_name,
      username: state.username,
      authenticated: state.authenticated,
      peer: format_peer(state.peer),
      created_at_ms: state.created_at,
      flags: flags(state)
    }
  end

  defp flags(state) do
    []
    |> maybe_flag(state.multi_state == :queuing, "M")
    |> maybe_flag(in_pubsub_mode?(state), "S")
    |> maybe_flag(tracking_enabled?(state), "T")
    |> Enum.reverse()
    |> Enum.join()
  end

  defp maybe_flag(flags, true, flag), do: [flag | flags]
  defp maybe_flag(flags, false, _flag), do: flags

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  defp in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp tracking_enabled?(%{tracking: nil}), do: false

  defp tracking_enabled?(%{tracking: tracking}) when is_map(tracking),
    do: Map.get(tracking, :enabled, false)

  defp tracking_enabled?(_state), do: false

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
