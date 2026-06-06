defmodule FerricstoreServer.Health.Endpoint.RouteRequirementsTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.RouteRequirements

  test "dashboard path classifiers identify dashboard and dashboard api paths" do
    assert RouteRequirements.dashboard_path?("/dashboard")
    assert RouteRequirements.dashboard_path?("/dashboard?x=1")
    assert RouteRequirements.dashboard_path?("/dashboard/flow")
    refute RouteRequirements.dashboard_path?("/metrics")

    assert RouteRequirements.dashboard_api_path?("/dashboard/api/flow")
    refute RouteRequirements.dashboard_api_path?("/dashboard/flow")
  end

  test "dashboard_route_requirement maps static dashboard pages to commands" do
    assert RouteRequirements.dashboard_route_requirement("GET", "/dashboard") == {"INFO", []}

    assert RouteRequirements.dashboard_route_requirement("GET", "/dashboard/raft") ==
             {"CLUSTER.STATUS", []}

    assert RouteRequirements.dashboard_route_requirement("GET", "/dashboard/clients") ==
             {"CLIENT.LIST", []}
  end

  test "dashboard_route_requirement scopes keyspace and flow lookup reads" do
    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/keyspace?key=tenant%3A1"
           ) == {"GET", key: {"tenant:1", :read}}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/lookup?id=flow-1&partition_key=tenant-a"
           ) == {"FLOW.GET", key: {"tenant-a", :read}}
  end

  test "dashboard_route_requirement scopes flow index and query pages" do
    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/failures?partition_key=tenant-a"
           ) == {"FLOW.FAILURES", key: {"tenant-a", :read}}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/lineage?mode=parent&partition_key=tenant-a"
           ) == {"FLOW.BY_PARENT", key: {"tenant-a", :read}}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/query?kind=history&id=flow-1"
           ) == {"FLOW.HISTORY", key: {"flow-1", :read}}
  end

  test "dashboard_route_requirement scopes flow detail and value requests" do
    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/flow/flow%2F1?partition_key=tenant-a"
           ) == {"FLOW.GET", key: {"tenant-a", :read}}

    assert RouteRequirements.dashboard_route_requirement(
             "GET",
             "/dashboard/api/flow/value?flow=flow-1"
           ) == {"FLOW.GET", key: {"flow-1", :read}}
  end

  test "dashboard_route_requirement maps POST actions" do
    assert RouteRequirements.dashboard_route_requirement("POST", "/dashboard/flow/failures") ==
             {"FLOW.RECLAIM", []}

    assert RouteRequirements.dashboard_route_requirement("POST", "/dashboard/flow/retention") ==
             {"FLOW.LIST", []}

    assert RouteRequirements.dashboard_route_requirement("POST", "/dashboard/flow/flow-1/rewind") ==
             {"FLOW.REWIND", []}

    assert RouteRequirements.dashboard_route_requirement("POST", "/dashboard/unknown") ==
             {"INFO", []}
  end

  test "form requirements scope destructive flow actions" do
    assert RouteRequirements.flow_retention_form_requirement(%{"action" => "cleanup"}) ==
             {"FLOW.RETENTION_CLEANUP", key: {"*", :write}}

    assert RouteRequirements.flow_policy_form_requirement(%{"type" => "email"}) ==
             {"FLOW.POLICY.SET", key: {"email", :write}}

    assert RouteRequirements.flow_reclaim_form_requirement(%{
             "type" => "email",
             "partition_key" => "tenant-a"
           }) == {"FLOW.RECLAIM", key: {"tenant-a", :write}}

    assert RouteRequirements.flow_rewind_form_requirement("flow-1", %{}) ==
             {"FLOW.REWIND", key: {"flow-1", :write}}
  end
end
