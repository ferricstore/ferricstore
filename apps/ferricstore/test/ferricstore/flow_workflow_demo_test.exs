defmodule Ferricstore.FlowWorkflowDemoTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "demo logs full workflow lifecycle and query results" do
    id = "demo-flow-#{System.unique_integer([:positive])}"
    type = "email"
    partition = "tenant-a"
    correlation = "order-123"
    now = 1_000

    log_step("1 create queued workflow")

    assert {:ok, created} =
             FerricStore.flow_create(id,
               type: type,
               partition_key: partition,
               state: "queued",
               payload_ref: "payload:#{id}",
               correlation_id: correlation,
               run_at_ms: now,
               now_ms: now
             )

    log_flow("created", created)

    log_step("2 worker claims due workflow")

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: partition,
               state: "queued",
               worker: "worker-1",
               lease_ms: 30_000,
               limit: 1,
               now_ms: now
             )

    log_flow("claimed", claimed)

    log_step("3 worker completes workflow")

    assert {:ok, completed} =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               result_ref: "result:#{id}",
               now_ms: now + 10
             )

    log_flow("completed", completed)

    log_step("4 query final state")
    assert {:ok, final} = FerricStore.flow_get(id, partition_key: partition)
    log_query("flow_get", final)

    log_step("5 query completed list")
    assert {:ok, completed_list} = FerricStore.flow_list(type, state: "completed", partition_key: partition, count: 10)
    log_query("flow_list completed", Enum.map(completed_list, &flow_summary/1))

    log_step("6 query by correlation")
    assert {:ok, correlated} = FerricStore.flow_by_correlation(correlation, partition_key: partition, count: 10)
    log_query("flow_by_correlation", Enum.map(correlated, &flow_summary/1))

    log_step("7 query workflow info")
    assert {:ok, info} = FerricStore.flow_info(type, partition_key: partition)
    log_query("flow_info", info)

    log_step("8 query workflow history")
    assert {:ok, history} = FerricStore.flow_history(id, partition_key: partition, count: 10)

    Enum.each(history, fn {event_id, fields} ->
      IO.puts(
        "[flow-demo] history event_id=#{event_id} event=#{fields["event"]} state=#{fields["state"]} worker=#{fields["lease_owner"] || "-"}"
      )
    end)

    log_query("full history", history)

    assert Enum.map(history, fn {_id, fields} -> fields["event"] end) == [
             "created",
             "claimed",
             "completed"
           ]
  end

  defp log_step(step), do: IO.puts("[flow-demo] STEP #{step}")
  defp log_flow(label, flow), do: log_query(label, flow_summary(flow))
  defp log_query(name, value), do: IO.puts("[flow-demo] #{name}: #{inspect(value, pretty: true)}")

  defp flow_summary(flow) do
    Map.take(flow, [
      :id,
      :type,
      :state,
      :version,
      :attempts,
      :lease_owner,
      :lease_token,
      :fencing_token,
      :correlation_id,
      :result_ref
    ])
  end
end
