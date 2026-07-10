defmodule FerricstoreServer.Health.Endpoint.RouteRequirements do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Flow.Schedules
  alias FerricstoreServer.Health.Endpoint.FlowPaths

  @type requirement :: {binary(), keyword()}

  @spec dashboard_path?(binary()) :: boolean()
  def dashboard_path?("/dashboard"), do: true
  def dashboard_path?("/dashboard?" <> _query), do: true
  def dashboard_path?("/dashboard/" <> _rest), do: true
  def dashboard_path?(_path), do: false

  @spec dashboard_api_path?(binary()) :: boolean()
  def dashboard_api_path?("/dashboard/api"), do: true
  def dashboard_api_path?("/dashboard/api/" <> _rest), do: true
  def dashboard_api_path?(_path), do: false

  @spec dashboard_route_requirement(binary(), binary()) :: requirement()
  def dashboard_route_requirement("GET", path) do
    {clean_path, query} = split_path_query(path)

    case clean_path do
      "/dashboard" -> {"INFO", []}
      "/dashboard/slowlog" -> {"SLOWLOG", []}
      "/dashboard/merge" -> {"INFO", []}
      "/dashboard/config" -> {"CONFIG", []}
      "/dashboard/raft" -> {"CLUSTER.STATUS", []}
      "/dashboard/consensus" -> {"CLUSTER.STATUS", []}
      "/dashboard/clients" -> {"CLIENT.LIST", []}
      "/dashboard/storage" -> {"INFO", []}
      "/dashboard/capabilities" -> {"FERRICSTORE.CAPABILITIES", []}
      "/dashboard/security" -> {"ACL.LIST", []}
      "/dashboard/doctor" -> {"FERRICSTORE.DOCTOR", []}
      "/dashboard/keyspace" -> keyspace_requirement(query)
      "/dashboard/commands" -> {"INFO", []}
      "/dashboard/reads" -> {"INFO", []}
      "/dashboard/streams" -> {"XINFO", []}
      "/dashboard/pubsub" -> {"PUBSUB", []}
      "/dashboard/prefixes" -> {"SCAN", []}
      "/dashboard/flow" -> {"FLOW.LIST", []}
      "/dashboard/flow/lookup" -> flow_lookup_requirement(query)
      "/dashboard/flow/states" -> {"FLOW.LIST", []}
      "/dashboard/flow/workers" -> {"FLOW.LIST", []}
      "/dashboard/flow/due" -> {"FLOW.LIST", []}
      "/dashboard/flow/schedules" -> {"FLOW.SCHEDULE.LIST", key: {"*", :read}}
      "/dashboard/flow/failures" -> flow_index_view_requirement("FLOW.FAILURES", query)
      "/dashboard/flow/lineage" -> flow_lineage_requirement(query)
      "/dashboard/flow/query" -> flow_query_requirement(query)
      "/dashboard/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/flow/policies" -> {"FLOW.POLICY.GET", []}
      "/dashboard/flow/governance" -> {"FLOW.GOVERNANCE.OVERVIEW", key: {"*", :read}}
      "/dashboard/flow/retention" -> {"FLOW.LIST", []}
      "/dashboard/flow/config" -> {"CONFIG", []}
      "/dashboard/flow/projections" -> {"FLOW.LIST", []}
      "/dashboard/api/overview" -> {"INFO", []}
      "/dashboard/api/flow" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/states" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/workers" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/due" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/api/flow/projections" -> {"FLOW.LIST", []}
      "/dashboard/api/flow/value" -> flow_value_requirement(query)
      "/dashboard/api/slowlog" -> {"SLOWLOG", []}
      "/dashboard/api/merge" -> {"INFO", []}
      "/dashboard/api/raft" -> {"CLUSTER.STATUS", []}
      "/dashboard/api/clients" -> {"CLIENT.LIST", []}
      "/dashboard/api/storage" -> {"INFO", []}
      "/dashboard/api/keyspace" -> keyspace_requirement(query)
      "/dashboard/api/commands" -> {"INFO", []}
      "/dashboard/api/reads" -> {"INFO", []}
      "/dashboard/api/streams" -> {"XINFO", []}
      "/dashboard/api/pubsub" -> {"PUBSUB", []}
      "/dashboard/api/prefixes" -> {"SCAN", []}
      _ -> flow_detail_or_default_requirement(clean_path, query)
    end
  end

  def dashboard_route_requirement("POST", path) do
    {clean_path, _query} = split_path_query(path)

    case clean_path do
      "/dashboard/flow/failures" ->
        {"FLOW.RECLAIM", []}

      "/dashboard/flow/policies" ->
        {"FLOW.POLICY.SET", []}

      "/dashboard/flow/retention" ->
        {"FLOW.LIST", []}

      "/dashboard/flow/schedules" ->
        {"FLOW.SCHEDULE.LIST", []}

      "/dashboard/flow/governance" ->
        {"FLOW.GOVERNANCE.OVERVIEW", []}

      "/dashboard/doctor" ->
        {"FERRICSTORE.DOCTOR", []}

      _ ->
        flow_rewind_or_default_requirement(clean_path)
    end
  end

  def dashboard_route_requirement(_method, _path), do: {"*", []}

  @spec flow_retention_form_requirement(map()) :: requirement()
  def flow_retention_form_requirement(%{"action" => "cleanup"}) do
    {"FLOW.RETENTION_CLEANUP", key: {"*", :write}}
  end

  def flow_retention_form_requirement(_params), do: {"FLOW.LIST", []}

  @spec flow_policy_form_requirement(map()) :: requirement()
  def flow_policy_form_requirement(params) do
    type =
      params
      |> Map.get("type", "")
      |> String.trim()

    if type == "" do
      {"FLOW.POLICY.SET", []}
    else
      {"FLOW.POLICY.SET", key: {type, :write}}
    end
  end

  @spec flow_schedule_form_requirement(map()) :: requirement()
  def flow_schedule_form_requirement(params) do
    command = Schedules.form_command(params)
    id = params |> Map.get("id", "") |> String.trim()

    if id == "" do
      {command, []}
    else
      {command, key: {id, :write}}
    end
  end

  @spec flow_reclaim_form_requirement(map()) :: requirement()
  def flow_reclaim_form_requirement(params) do
    partition_key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()

    if partition_key == "" do
      {"FLOW.RECLAIM", key: {"*", :write}}
    else
      {"FLOW.RECLAIM", key: {partition_key, :write}}
    end
  end

  @spec flow_rewind_form_requirement(binary(), map()) :: requirement()
  def flow_rewind_form_requirement(id, params) do
    key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()
      |> case do
        "" -> id
        partition_key -> partition_key
      end

    if key == "" do
      {"FLOW.REWIND", []}
    else
      {"FLOW.REWIND", key: {key, :write}}
    end
  end

  @spec flow_signal_form_requirement(binary(), map()) :: requirement()
  def flow_signal_form_requirement(id, params) do
    key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()
      |> case do
        "" -> id
        partition_key -> partition_key
      end

    if key == "" do
      {"FLOW.SIGNAL", []}
    else
      {"FLOW.SIGNAL", key: {key, :write}}
    end
  end

  @spec flow_governance_form_requirement(map()) :: requirement()
  def flow_governance_form_requirement(%{"action" => "close_circuit"} = params) do
    flow_governance_scope_requirement("FLOW.CIRCUIT.CLOSE", params)
  end

  def flow_governance_form_requirement(%{"action" => "open_circuit"} = params) do
    flow_governance_scope_requirement("FLOW.CIRCUIT.OPEN", params)
  end

  def flow_governance_form_requirement(_params), do: {"FLOW.GOVERNANCE.OVERVIEW", []}

  defp flow_governance_scope_requirement(command, params) do
    scope =
      params
      |> Map.get("scope", "")
      |> String.trim()

    if scope == "" do
      {command, []}
    else
      {command, key: {scope, :write}}
    end
  end

  defp flow_lookup_requirement(query) do
    id =
      query
      |> URI.decode_query()
      |> Map.get("id", "")
      |> String.trim()

    partition_key = flow_partition_key_from_query(query)

    cond do
      id == "" ->
        {"FLOW.GET", []}

      partition_key != "" ->
        {"FLOW.GET", key: {partition_key, :read}}

      true ->
        {"FLOW.GET", key: {id, :read}}
    end
  rescue
    _ -> {"FLOW.GET", []}
  end

  defp flow_detail_or_default_requirement("/dashboard/flow/" <> encoded_id, query) do
    id = URI.decode(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement("/dashboard/api/flow/" <> encoded_id, query) do
    id = URI.decode(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement(_path, _query), do: {"INFO", []}

  defp flow_value_requirement(query) do
    flow_id =
      query
      |> URI.decode_query()
      |> Map.get("flow", "")
      |> String.trim()

    partition_key = flow_partition_key_from_query(query)

    cond do
      flow_id == "" ->
        {"FLOW.GET", []}

      partition_key != "" ->
        {"FLOW.GET", key: {partition_key, :read}}

      true ->
        {"FLOW.GET", key: {flow_id, :read}}
    end
  rescue
    _ -> {"FLOW.GET", []}
  end

  defp keyspace_requirement(query) do
    key =
      query
      |> URI.decode_query()
      |> Map.get("key", "")
      |> String.trim()

    if key == "" do
      {"SCAN", []}
    else
      {"GET", key: {key, :read}}
    end
  rescue
    _ -> {"SCAN", []}
  end

  defp flow_lineage_requirement(query) do
    mode =
      query
      |> URI.decode_query()
      |> Map.get("mode", "root")

    command =
      case mode do
        "parent" -> "FLOW.BY_PARENT"
        "correlation" -> "FLOW.BY_CORRELATION"
        _ -> "FLOW.BY_ROOT"
      end

    case flow_partition_key_from_query(query) do
      "" -> {command, []}
      partition_key -> {command, key: {partition_key, :read}}
    end
  rescue
    _ -> {"FLOW.BY_ROOT", []}
  end

  defp flow_query_requirement(query) do
    params = URI.decode_query(query)
    kind = Map.get(params, "kind", "list")
    command = flow_query_command_requirement(kind)
    partition_key = flow_partition_key_from_query(query)
    type = params |> Map.get("type", "") |> String.trim()

    key =
      params
      |> Map.get("id", "")
      |> String.trim()
      |> flow_acl_key_from_query(query)

    flow_query_key_requirement(command, kind, key, partition_key, type)
  rescue
    _ -> {"FLOW.LIST", []}
  end

  defp flow_index_view_requirement(command, query) do
    params = URI.decode_query(query)

    partition_key =
      params
      |> Map.get("partition_key", "")
      |> String.trim()

    if partition_key == "" do
      {command, key: {"*", :read}}
    else
      {command, key: {partition_key, :read}}
    end
  rescue
    _ -> {command, []}
  end

  defp flow_query_key_requirement(command, "history", "", _partition_key, _type),
    do: {command, []}

  defp flow_query_key_requirement(command, "history", key, _partition_key, _type),
    do: {command, key: {key, :read}}

  defp flow_query_key_requirement(command, kind, _key, partition_key, _type)
       when kind in ["failures", "list", "search", "stats", "stuck", "terminals"] do
    if partition_key == "" do
      {command, key: {"*", :read}}
    else
      {command, key: {partition_key, :read}}
    end
  end

  defp flow_query_key_requirement(command, _kind, _key, "", _type) do
    {command, []}
  end

  defp flow_query_key_requirement(command, _kind, _key, partition_key, _type) do
    {command, key: {partition_key, :read}}
  end

  defp flow_query_command_requirement(kind) when is_binary(kind) do
    case kind do
      "terminals" -> "FLOW.TERMINALS"
      "search" -> "FLOW.SEARCH"
      "failures" -> "FLOW.FAILURES"
      "stuck" -> "FLOW.STUCK"
      "stats" -> "FLOW.STATS"
      "history" -> "FLOW.HISTORY"
      "by_parent" -> "FLOW.BY_PARENT"
      "by_root" -> "FLOW.BY_ROOT"
      "by_correlation" -> "FLOW.BY_CORRELATION"
      _ -> "FLOW.LIST"
    end
  end

  defp flow_query_command_requirement(_kind), do: "FLOW.LIST"

  defp flow_rewind_or_default_requirement("/dashboard/flow/" <> encoded_action) do
    cond do
      match?({:ok, _id}, FlowPaths.decode_flow_rewind_action(encoded_action)) ->
        {"FLOW.REWIND", []}

      match?({:ok, _id}, FlowPaths.decode_flow_signal_action(encoded_action)) ->
        {"FLOW.SIGNAL", []}

      true ->
        {"FLOW.REWIND", []}
    end
  end

  defp flow_rewind_or_default_requirement(_path), do: {"INFO", []}

  defp flow_acl_key_from_query(id, query) do
    case flow_partition_key_from_query(query) do
      "" -> id
      partition_key -> partition_key
    end
  end

  defp flow_partition_key_from_query(query) do
    query
    |> URI.decode_query()
    |> Map.get("partition_key", "")
    |> String.trim()
  rescue
    _ -> ""
  end

  defp split_path_query(path) do
    case String.split(path, "?", parts: 2) do
      [clean_path, query] -> {clean_path, query}
      [clean_path] -> {clean_path, ""}
    end
  end
end
