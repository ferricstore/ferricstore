defmodule FerricstoreServer.Health.Dashboard.FlowAclSamplingTest do
  use FerricstoreServer.Test.DashboardCase

  alias FerricstoreServer.Health.Dashboard.Flow.Sample

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    :ok
  end

  test "bounded sampling fills its result limit after applying visibility" do
    suffix = System.unique_integer([:positive])
    denied_id = "dashboard-sample-denied-#{suffix}"
    allowed_id = "dashboard-sample-allowed-#{suffix}"

    assert :ok = FerricStore.flow_create(denied_id, type: "sample-acl", state: "queued")
    assert :ok = FerricStore.flow_create(allowed_id, type: "sample-acl", state: "queued")

    records = Sample.collect_flow_records_sample(1, &(&1.id == allowed_id))

    assert Enum.map(records, & &1.id) == [allowed_id]
  end
end
