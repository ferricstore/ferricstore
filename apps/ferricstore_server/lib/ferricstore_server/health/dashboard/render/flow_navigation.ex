defmodule FerricstoreServer.Health.Dashboard.Render.FlowNavigation do
  @moduledoc false

  import FerricstoreServer.Health.Dashboard.FlowRecord
  import FerricstoreServer.Health.Dashboard.Format, only: [escape: 1, escape_attr: 1]

  alias FerricstoreServer.Health.Endpoint.FlowPaths

  def workflow_reference(id, partition) when is_binary(id) and id != "" do
    if is_binary(partition) and partition != "" do
      path = FlowPaths.flow_detail_location(id, partition)
      ~s(<a class="flow-link" href="#{escape_attr(path)}">#{escape(id)}</a>)
    else
      """
      <span class="mono">#{escape(id)}</span>
      <details class="dashboard-disclosure flow-reference-lookup">
        <summary>Look up workflow</summary>
        <form action="/dashboard/flow/lookup" method="get" class="flow-search" aria-label="Look up #{escape_attr(id)}">
          <input type="hidden" name="id" value="#{escape_attr(id)}">
          <label class="flow-filter-field"><span>Partition key</span><input class="flow-search-input mono" name="partition_key" required></label>
          <button class="flow-search-button" type="submit">Open workflow</button>
        </form>
      </details>
      """
    end
  end

  def workflow_reference(_id, _partition), do: "-"

  def relationship_link(mode, id, record) when mode in ["parent", "root", "correlation"] do
    partition = flow_record_partition_key(record)

    if is_binary(id) and id != "" and is_binary(partition) and partition != "" do
      query = URI.encode_query(%{"mode" => mode, "id" => id, "partition_key" => partition})

      ~s(<a class="flow-link" href="/dashboard/flow/lineage?#{escape_attr(query)}" title="Find #{mode} relationships in this partition">#{escape(id)}</a>)
    else
      escape(id || "-")
    end
  end

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
