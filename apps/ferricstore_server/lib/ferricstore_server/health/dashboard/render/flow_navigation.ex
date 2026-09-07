defmodule FerricstoreServer.Health.Dashboard.Render.FlowNavigation do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Format, only: [escape_attr: 1]

  def related_runs_path(record) when is_map(record) do
    type = flow_record_type(record)
    partition = flow_record_partition_key(record)

    if type != "" and is_binary(partition) do
      "/dashboard/flow/query?" <>
        URI.encode_query(%{
          "kind" => "list",
          "type" => type,
          "partition_key" => partition,
          "limit" => "40"
        })
    end
  end

  def related_runs_path(_record), do: nil

  def lane_path(lane) do
    type = Map.get(lane, :type)
    state = Map.get(lane, :state)
    partition = Map.get(lane, :partition_key)

    if Enum.all?([type, state, partition], &(is_binary(&1) and &1 != "")) do
      "/dashboard/flow/states?" <>
        URI.encode_query(%{"type" => type, "state" => state, "partition_key" => partition})
    end
  end

  def related_runs_link(record) do
    case related_runs_path(record) do
      nil ->
        ""

      path ->
        ~s(<a href="#{escape_attr(path)}" title="Runs of this type in the same partition, across all states">Related runs</a>)
    end
  end
end
