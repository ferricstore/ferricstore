defmodule FerricstoreServer.Health.Endpoint.ForbiddenTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Endpoint.Forbidden

  test "requirement_details includes command rule" do
    assert Forbidden.requirement_details("FLOW.QUERY") == %{
             required_command: "FLOW.QUERY",
             required_acl_rule: "+FLOW.QUERY"
           }
  end

  test "requirement_details includes key rule when present" do
    assert Forbidden.requirement_details({"FLOW.GET", key: {"tenant-a", :read}}) == %{
             required_command: "FLOW.GET",
             required_acl_rule: "+FLOW.GET",
             required_key: "tenant-a",
             required_key_access: "read",
             required_key_rule: "%R~tenant-a"
           }
  end
end
