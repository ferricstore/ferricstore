defmodule Ferricstore.FlowFacadeCorrectnessTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Store.Router

  test "get fails closed when the persisted Flow record is corrupt" do
    ctx = FerricStore.Instance.get(:default)
    id = unique_flow_id("corrupt-flow-record")
    partition_key = "tenant-corrupt"
    state_key = Keys.state_key(id, partition_key)

    assert :ok = Router.put(ctx, state_key, "not-a-flow-record", 0)

    assert {:error, "ERR invalid flow record"} =
             Flow.get(ctx, id, partition_key: partition_key)
  end

  test "lease governance renewal preserves storage unavailability" do
    source =
      File.read!(Path.expand("../../lib/ferricstore/flow.ex", __DIR__))

    assert source =~
             ~r/defp maybe_renew_governance_limit.*case Router\.flow_get_with_status\(ctx, id, partition_key\).*:unavailable ->\s*\{:error, "ERR storage read failed"\}/s
  end

  test "get rejects payload hydration above the configured ceiling" do
    ctx = FerricStore.Instance.get(:default)

    assert {:error, "ERR flow payload_max_bytes exceeds maximum 65536"} =
             Flow.get(ctx, unique_flow_id("payload-ceiling"), payload_max_bytes: 65_537)
  end
end
