defmodule FerricstoreServer.Health.Dashboard.Flow.CallsTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Health.Dashboard.Flow.Calls

  test "bounded calls isolate worker exceptions from the request process" do
    assert {:error, {:exit, {%RuntimeError{message: "storage failed"}, _stacktrace}}} =
             Calls.bounded_dashboard_call(
               fn -> raise "storage failed" end,
               100,
               :test_lookup
             )
  end
end
