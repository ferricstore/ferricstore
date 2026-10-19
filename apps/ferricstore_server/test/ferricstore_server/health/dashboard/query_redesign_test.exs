defmodule FerricstoreServer.Health.Dashboard.QueryRedesignTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Access

  test "a query that has not run keeps its prompt for signed-in users" do
    idle = %{status: :idle, command: "FLOW.QUERY", rows: [], message: "Enter a partition key"}

    assert Access.flow_query_filter_result_for_acl(idle, "default", "*") == idle
  end
end
