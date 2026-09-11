defmodule FerricstoreServer.Health.Dashboard.Flow.ManagementActions do
  @moduledoc false
  alias FerricstoreServer.Health.Endpoint.{Auth, RouteRequirements}

  def schedules(username) do
    Map.new(~w(create fire pause resume delete)a, fn action ->
      {action,
       allowed?(
         username,
         requirements(
           "/dashboard/flow/schedules",
           RouteRequirements.flow_schedule_form_requirement(%{"action" => to_string(action)})
         )
       )}
    end)
  end

  def policy(username, type),
    do: %{
      save:
        allowed?(
          username,
          RouteRequirements.flow_policy_form_requirement(%{"type" => type || ""})
        )
    }

  def retention(username),
    do: %{
      cleanup:
        allowed?(
          username,
          requirements(
            "/dashboard/flow/retention",
            RouteRequirements.flow_retention_form_requirement(%{"action" => "cleanup"})
          )
        )
    }

  def governance(username, scope) do
    Map.new(~w(open_circuit close_circuit approve_approval reject_approval)a, fn action ->
      params = %{
        "action" => to_string(action),
        "scope" => scope || "",
        "approval_scope" => scope || ""
      }

      {action,
       allowed?(
         username,
         requirements(
           "/dashboard/flow/governance",
           RouteRequirements.flow_governance_form_requirement(params)
         )
       )}
    end)
  end

  def allowed?(nil, _requirement), do: true
  def allowed?(username, requirement), do: Auth.acl_requirement_allowed?(username, requirement)

  defp requirements(path, action_requirement),
    do: [RouteRequirements.dashboard_route_requirement("POST", path), action_requirement]
end
