defmodule FerricstoreServer.Health.Dashboard.AccessTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Access

  test "ACL identity keeps empty and whitespace usernames distinct from open access" do
    assert Access.keyspace_acl_username(%{}) == nil
    assert Access.keyspace_acl_username([]) == nil
    assert Access.keyspace_acl_username(%{"acl_username" => ""}) == ""
    assert Access.keyspace_acl_username(acl_username: " user ") == " user "
  end

  @tag :dashboard_lineage_acl_summary
  test "lineage ACL filtering does not retain the pre-filter record count" do
    result = %{
      records: [%{id: "hidden-flow", partition_key: "tenant-b"}],
      message: "1 records"
    }

    filtered = Access.flow_lineage_filter_result_for_acl(result, "missing-dashboard-user")

    assert filtered.records == []
    assert filtered.message == "0 visible record(s)"
  end
end
