defmodule Ferricstore.FlowTest.Sections.FlowMaxActiveMsTimesOutFlows do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      test "max_active_ms defaults to infinite and does not affect active flows when unset" do
        id = uid("flow-active-timeout-unset")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: "active-timeout-unset",
                   state: "queued",
                   run_at_ms: now,
                   now_ms: now
                 )

        assert Map.get(created, :max_active_ms) == nil

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 86_400_000)

        assert {:ok, active} = FerricStore.flow_get(id)
        assert active.state == "queued"
        assert active.terminal_retention_until_ms == nil
      end

      test "max_active_ms moves overdue non-terminal flows to failed and starts retention" do
        id = uid("flow-active-timeout")
        partition_key = uid("tenant-active-timeout")
        type = uid("active-timeout-type")
        create_now = System.system_time(:millisecond) + 60_000
        before_timeout = create_now + 499
        timeout_now = create_now + 500

        assert {:ok, policy} =
                 FerricStore.flow_policy_set(type,
                   max_active_ms: 500,
                   retention: [ttl_ms: 1_000]
                 )

        assert policy.max_active_ms == 500

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   partition_key: partition_key,
                   run_at_ms: create_now + 10_000,
                   now_ms: create_now
                 )

        assert created.max_active_ms == 500

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: before_timeout)

        assert {:ok, queued} = FerricStore.flow_get(id, partition_key: partition_key)
        assert queued.state == "queued"

        assert {:ok, %{active_timeouts: 1, flows: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: timeout_now)

        assert {:ok, failed} = FerricStore.flow_get(id, partition_key: partition_key, full: true)
        assert failed.state == "failed"
        assert failed.max_active_ms == 500
        assert failed.updated_at_ms == timeout_now
        assert failed.terminal_retention_until_ms == timeout_now + 1_000
        assert failed.lease_owner == nil
        assert failed.lease_token == nil
        assert failed.next_run_at_ms == nil
        assert failed.error == %{reason: "max_active_ms", max_active_ms: 500}

        assert {:ok, history} =
                 FerricStore.flow_history(id,
                   partition_key: partition_key,
                   count: 10,
                   direction: :forward
                 )

        assert Enum.any?(history, fn {_event_id, event} ->
                 event["event"] == "failed" and event["reason"] == "max_active_ms" and
                   event["max_active_ms"] == "500"
               end)
      end

      test "create max_active_ms overrides the type policy for one flow" do
        id = uid("flow-active-timeout-override")
        type = uid("active-timeout-override-type")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 10_000)

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: 100,
                   run_at_ms: now,
                   now_ms: now
                 )

        assert created.max_active_ms == 100

        assert {:ok, %{active_timeouts: 1}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, failed} = FerricStore.flow_get(id)
        assert failed.state == "failed"
      end

      test "create max_active_ms infinity opts one flow out of the type policy timeout" do
        id = uid("flow-active-timeout-infinity")
        type = uid("active-timeout-infinity-type")
        now = System.system_time(:millisecond) + 60_000

        assert {:ok, _policy} = FerricStore.flow_policy_set(type, max_active_ms: 100)

        assert {:ok, created} =
                 flow_create_and_get(id,
                   type: type,
                   state: "queued",
                   max_active_ms: :infinity,
                   run_at_ms: now,
                   now_ms: now
                 )

        assert Map.get(created, :max_active_ms) == nil

        assert {:ok, %{active_timeouts: 0}} =
                 FerricStore.flow_retention_cleanup(limit: 10, now_ms: now + 100)

        assert {:ok, queued} = FerricStore.flow_get(id)
        assert queued.state == "queued"
      end
    end
  end
end
