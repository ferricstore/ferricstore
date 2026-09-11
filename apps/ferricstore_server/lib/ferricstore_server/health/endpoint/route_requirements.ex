defmodule FerricstoreServer.Health.Endpoint.RouteRequirements do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Flow.Schedules
  alias FerricstoreServer.Health.Endpoint.FlowPaths
  alias FerricstoreServer.Health.QueryDecoder

  @type command_requirement :: {binary(), keyword()}
  @type requirement :: command_requirement() | [command_requirement()]

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
  def dashboard_route_requirement(method, path),
    do:
      route_requirement(
        method,
        path,
        if(method in ["GET", "POST"], do: {"INFO", []}, else: {"*", []})
      )

  @doc false
  def known_dashboard_route_requirement(method, path),
    do: route_requirement(method, path, :unsupported)

  defp route_requirement("GET", path, fallback) do
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
      "/dashboard/reads" -> {"INFO", key: {"*", :read}}
      "/dashboard/streams" -> {"XINFO", []}
      "/dashboard/pubsub" -> {"PUBSUB", []}
      "/dashboard/prefixes" -> {"SCAN", key: {"*", :read}}
      "/dashboard/flow" -> {"FLOW.QUERY", []}
      "/dashboard/flow/lookup" -> flow_lookup_requirement(query)
      "/dashboard/flow/states" -> flow_partition_view_requirement("FLOW.QUERY", query)
      "/dashboard/flow/workers" -> {"FLOW.QUERY", []}
      "/dashboard/flow/due" -> {"FLOW.QUERY", []}
      "/dashboard/flow/schedules" -> flow_schedule_page_requirement(query)
      "/dashboard/flow/failures" -> flow_index_view_requirement("FLOW.QUERY", query)
      "/dashboard/flow/lineage" -> flow_partition_view_requirement("FLOW.QUERY", query)
      "/dashboard/flow/query" -> flow_query_page_requirement(query)
      "/dashboard/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/flow/policies" -> {"FLOW.POLICY.GET", []}
      "/dashboard/flow/governance" -> flow_governance_requirement(query)
      "/dashboard/flow/retention" -> {"FLOW.QUERY", []}
      "/dashboard/flow/config" -> {"CONFIG", []}
      "/dashboard/flow/projections" -> {"FLOW.QUERY", []}
      "/dashboard/api/overview" -> {"INFO", []}
      "/dashboard/api/flow" -> {"FLOW.QUERY", []}
      "/dashboard/api/flow/states" -> flow_partition_view_requirement("FLOW.QUERY", query)
      "/dashboard/api/flow/workers" -> {"FLOW.QUERY", []}
      "/dashboard/api/flow/due" -> {"FLOW.QUERY", []}
      "/dashboard/api/flow/signals" -> {"FLOW.HISTORY", []}
      "/dashboard/api/flow/projections" -> {"FLOW.QUERY", []}
      "/dashboard/api/flow/value" -> flow_value_requirement(query)
      "/dashboard/api/slowlog" -> {"SLOWLOG", []}
      "/dashboard/api/merge" -> {"INFO", []}
      "/dashboard/api/raft" -> {"CLUSTER.STATUS", []}
      "/dashboard/api/clients" -> {"CLIENT.LIST", []}
      "/dashboard/api/storage" -> {"INFO", []}
      "/dashboard/api/keyspace" -> keyspace_requirement(query)
      "/dashboard/api/commands" -> {"INFO", []}
      "/dashboard/api/reads" -> {"INFO", key: {"*", :read}}
      "/dashboard/api/streams" -> {"XINFO", []}
      "/dashboard/api/pubsub" -> {"PUBSUB", []}
      "/dashboard/api/prefixes" -> {"SCAN", key: {"*", :read}}
      _ -> flow_detail_or_default_requirement(clean_path, query, fallback)
    end
  end

  defp route_requirement("POST", path, fallback) do
    {clean_path, _query} = split_path_query(path)

    case clean_path do
      "/dashboard/security/users" ->
        {"ACL.SETUSER", []}

      "/dashboard/security/users/state" ->
        {"ACL.SETUSER", []}

      "/dashboard/security/users/password" ->
        {"ACL.SETUSER", []}

      "/dashboard/security/users/rules" ->
        {"ACL.SETUSER", []}

      "/dashboard/security/users/delete" ->
        {"ACL.DELUSER", []}

      "/dashboard/flow/failures" ->
        {"FLOW.RECLAIM", []}

      "/dashboard/flow/query" ->
        {"*", []}

      "/dashboard/flow/policies" ->
        {"FLOW.POLICY.SET", []}

      "/dashboard/flow/retention" ->
        {"FLOW.QUERY", []}

      "/dashboard/flow/schedules" ->
        {"FLOW.SCHEDULE.LIST", []}

      "/dashboard/flow/governance" ->
        {"FLOW.GOVERNANCE.OVERVIEW", []}

      "/dashboard/doctor" ->
        {"FERRICSTORE.DOCTOR", []}

      _ ->
        flow_rewind_or_default_requirement(clean_path, fallback)
    end
  end

  defp route_requirement(_method, _path, fallback), do: fallback

  @spec flow_retention_form_requirement(map()) :: requirement()
  def flow_retention_form_requirement(%{"action" => "cleanup"}) do
    {"FLOW.RETENTION_CLEANUP", key: {"*", :write}}
  end

  def flow_retention_form_requirement(_params), do: {"FLOW.QUERY", []}

  @spec flow_policy_form_requirement(map()) :: requirement()
  def flow_policy_form_requirement(params) do
    type =
      params
      |> Map.get("type", "")

    if type == "" do
      {"FLOW.POLICY.SET", []}
    else
      {"FLOW.POLICY.SET", key: {type, :write}}
    end
  end

  @spec flow_schedule_form_requirement(map()) :: requirement()
  def flow_schedule_form_requirement(params) do
    command = Schedules.form_command(params)

    cond do
      command == "FLOW.SCHEDULE.GET" ->
        {command, []}

      command == "FLOW.SCHEDULE.CREATE" and Schedules.replacement?(params) ->
        [{command, key: {"*", :write}}, {"FLOW.SCHEDULE.GET", []}]

      true ->
        {command, key: {"*", :write}}
    end
  end

  defp flow_schedule_page_requirement(query) do
    command =
      case QueryDecoder.decode(query) do
        %{"id" => id} when is_binary(id) and id != "" -> "FLOW.SCHEDULE.GET"
        _ -> "FLOW.SCHEDULE.LIST"
      end

    {command, key: {"*", :read}}
  end

  @spec flow_reclaim_form_requirement(map()) :: requirement()
  def flow_reclaim_form_requirement(params) do
    partition_key =
      params
      |> Map.get("partition_key", "")

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
    circuit_mutation_requirement("FLOW.CIRCUIT.CLOSE", params)
  end

  def flow_governance_form_requirement(%{"action" => "open_circuit"} = params) do
    circuit_mutation_requirement("FLOW.CIRCUIT.OPEN", params)
  end

  def flow_governance_form_requirement(%{"action" => "approve_approval"} = params) do
    flow_governance_scope_requirement("FLOW.APPROVAL.APPROVE", params, "approval_scope")
  end

  def flow_governance_form_requirement(%{"action" => "reject_approval"} = params) do
    flow_governance_scope_requirement("FLOW.APPROVAL.REJECT", params, "approval_scope")
  end

  def flow_governance_form_requirement(_params), do: {"FLOW.GOVERNANCE.OVERVIEW", []}

  defp circuit_mutation_requirement(command, params) do
    scope = Map.get(params, "scope", "")

    read =
      if scope == "",
        do: {"FLOW.CIRCUIT.GET", []},
        else: {"FLOW.CIRCUIT.GET", key: {scope, :read}}

    [flow_governance_scope_requirement(command, params), read]
  end

  defp flow_governance_scope_requirement(command, params) do
    flow_governance_scope_requirement(command, params, "scope")
  end

  defp flow_governance_scope_requirement(command, params, field) do
    scope =
      params
      |> Map.get(field, "")

    if scope == "" do
      {command, []}
    else
      {command, key: {scope, :write}}
    end
  end

  defp flow_governance_requirement(query) do
    overview = {"FLOW.GOVERNANCE.OVERVIEW", key: {"*", :read}}
    params = QueryDecoder.decode(query)

    required_query_params = ~w(meta_partition_key meta_type meta_state meta_key)

    requirements =
      if Enum.all?(required_query_params, &present_query_param?(params, &1)) and
           is_binary(Map.get(params, "meta_value")) do
        partition_key = params |> Map.fetch!("meta_partition_key")
        [overview, {"FLOW.QUERY", key: {partition_key, :read}}]
      else
        [overview]
      end

    requirements =
      case Map.get(params, "circuit_review_scope") do
        scope when is_binary(scope) and scope != "" ->
          requirements ++ [{"FLOW.CIRCUIT.GET", key: {scope, :read}}]

        _ ->
          requirements
      end

    case requirements do
      [single] -> single
      multiple -> multiple
    end
  rescue
    _ -> {"FLOW.GOVERNANCE.OVERVIEW", key: {"*", :read}}
  end

  defp present_query_param?(params, key) do
    case Map.get(params, key) do
      value when is_binary(value) -> value != ""
      _other -> false
    end
  end

  defp flow_lookup_requirement(query) do
    id =
      query
      |> QueryDecoder.decode()
      |> Map.get("id", "")

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

  defp flow_detail_or_default_requirement("/dashboard/flow/" <> encoded_id, query, _fallback) do
    id = decoded_component_or_empty(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement("/dashboard/api/flow/" <> encoded_id, query, _fallback) do
    id = decoded_component_or_empty(encoded_id)
    {"FLOW.GET", key: {flow_acl_key_from_query(id, query), :read}}
  end

  defp flow_detail_or_default_requirement(_path, _query, fallback), do: fallback

  defp flow_value_requirement(query) do
    flow_id =
      query
      |> QueryDecoder.decode()
      |> Map.get("flow", "")

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
      |> QueryDecoder.decode()
      |> FerricstoreServer.Health.Dashboard.Data.KV.keyspace_filters()
      |> Map.fetch!(:key)

    if key == "" do
      {"SCAN", []}
    else
      {"GET", key: {key, :read}}
    end
  rescue
    _ -> {"SCAN", []}
  end

  defp flow_partition_view_requirement(command, query) do
    case flow_partition_key_from_query(query) do
      "" -> {command, []}
      partition_key -> {command, key: {partition_key, :read}}
    end
  rescue
    _ -> {command, []}
  end

  defp flow_query_requirement(query) do
    params = QueryDecoder.decode(query)
    kind = Map.get(params, "kind", "list")
    command = flow_query_command_requirement(kind)
    partition_key = flow_partition_key_from_query(query)
    type = params |> Map.get("type", "")

    key =
      params
      |> Map.get("id", "")
      |> flow_acl_key_from_query(query)

    flow_query_key_requirement(command, kind, key, partition_key, type)
  rescue
    _ -> {"FLOW.QUERY", []}
  end

  defp flow_query_page_requirement(""), do: {"*", []}
  defp flow_query_page_requirement(query), do: flow_query_requirement(query)

  defp flow_index_view_requirement(command, query) do
    params = QueryDecoder.decode(query)

    partition_key =
      params
      |> Map.get("partition_key", "")

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
      "stats" -> "FLOW.STATS"
      "history" -> "FLOW.HISTORY"
      _ -> "FLOW.QUERY"
    end
  end

  defp flow_query_command_requirement(_kind), do: "FLOW.QUERY"

  defp flow_rewind_or_default_requirement("/dashboard/flow/" <> encoded_action, fallback) do
    cond do
      match?({:ok, _id}, FlowPaths.decode_flow_rewind_action(encoded_action)) ->
        {"FLOW.REWIND", []}

      match?({:ok, _id}, FlowPaths.decode_flow_signal_action(encoded_action)) ->
        {"FLOW.SIGNAL", []}

      true ->
        if fallback == :unsupported, do: :unsupported, else: {"FLOW.REWIND", []}
    end
  end

  defp flow_rewind_or_default_requirement(_path, fallback), do: fallback

  defp flow_acl_key_from_query(id, query) do
    case flow_partition_key_from_query(query) do
      "" -> id
      partition_key -> partition_key
    end
  end

  defp flow_partition_key_from_query(query) do
    query
    |> QueryDecoder.decode()
    |> Map.get("partition_key", "")
  rescue
    _ -> ""
  end

  defp split_path_query(path) do
    case String.split(path, "?", parts: 2) do
      [clean_path, query] -> {clean_path, query}
      [clean_path] -> {clean_path, ""}
    end
  end

  defp decoded_component_or_empty(encoded) do
    case QueryDecoder.decode_component(encoded) do
      {:ok, decoded} -> decoded
      :error -> ""
    end
  end
end
