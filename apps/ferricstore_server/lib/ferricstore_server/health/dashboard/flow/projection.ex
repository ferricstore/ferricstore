defmodule FerricstoreServer.Health.Dashboard.Flow.Projection do
  @moduledoc false

  def collect_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: Application.get_env(:ferricstore, :flow_lmdb_flush_interval_ms, 0),
      history_flush_interval_ms:
        Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms, 0),
      metrics: collect_metrics()
    }
  end

  def default_health do
    %{
      lmdb_projection: :lagged,
      lmdb_flush_interval_ms: 0,
      history_flush_interval_ms: 0,
      metrics: []
    }
  end

  defp collect_metrics do
    Ferricstore.Metrics.scrape()
    |> String.split("\n", trim: true)
    |> Enum.reject(&String.starts_with?(&1, "#"))
    |> Enum.filter(&String.contains?(&1, "ferricstore_flow_lmdb"))
    |> Enum.take(80)
    |> Enum.map(&parse_metric_line/1)
    |> Enum.map(&normalize_metric/1)
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp normalize_metric(%{name: name} = metric) when is_binary(name) do
    %{metric | name: String.replace(name, "mirror", "projection")}
  end

  defp normalize_metric(metric), do: metric

  defp parse_metric_line(line) do
    case String.split(line, " ", parts: 2) do
      [name, value] -> %{name: name, value: value}
      [name] -> %{name: name, value: ""}
    end
  end
end
