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

  test "sampling scan work has one global budget independent of shard count" do
    plans = Enum.map([1, 4, 64, 4_096], &Sample.flow_sample_scan_plan(400, &1))

    assert Enum.map(plans, &Enum.sum/1) |> Enum.uniq() |> length() == 1

    Enum.zip([1, 4, 64, 4_096], plans)
    |> Enum.each(fn {shard_count, plan} ->
      assert length(plan) == shard_count
      assert Enum.max(plan) - Enum.min(plan) <= 1
    end)
  end

  test "sampling scan budget remains bounded for large requested result limits" do
    assert Sample.flow_sample_scan_plan(10_000, 64) |> Enum.sum() == 10_000
  end
end
