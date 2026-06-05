defmodule Ferricstore.FlowBackpressureTest do
  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard
  alias Ferricstore.Flow.Admission
  alias Ferricstore.OperationalGuard
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  def handle_telemetry(event, measurements, metadata, test_pid) do
    send(test_pid, {:flow_telemetry, event, measurements, metadata})
  end

  setup do
    OperationalGuard.reset_for_test()
    Admission.clear_create_pause()
    MemoryGuard.set_keydir_full(false)
    MemoryGuard.set_reject_writes(false)

    on_exit(fn ->
      OperationalGuard.reset_for_test()
      Admission.clear_create_pause()
      MemoryGuard.set_keydir_full(false)
      MemoryGuard.set_reject_writes(false)
    end)

    :ok
  end

  defp attach_flow_telemetry(events) do
    test_pid = self()

    handler_ids =
      Enum.map(events, fn event ->
        handler_id = {__MODULE__, test_pid, event}
        :telemetry.attach(handler_id, event, &__MODULE__.handle_telemetry/4, test_pid)
        handler_id
      end)

    on_exit(fn ->
      Enum.each(handler_ids, &:telemetry.detach/1)
    end)
  end

  test "flow_create rejects new flows under memory overload" do
    ctx = FerricStore.Instance.get(:default)
    MemoryGuard.set_reject_writes(true)

    assert {:error, message} =
             Router.flow_create(ctx, %{
               id: "flow-overload-#{System.unique_integer([:positive])}",
               type: "overload",
               state: "queued"
             })

    assert message =~ "BUSY"
    assert message =~ "overloaded"
    assert message =~ "retry_after_ms="
  end

  test "flow_create_pipeline_batch returns per-item overload errors" do
    ctx = FerricStore.Instance.get(:default)
    MemoryGuard.set_reject_writes(true)

    results =
      Router.flow_create_pipeline_batch(ctx, [
        %{id: "flow-overload-batch-a-#{System.unique_integer([:positive])}"},
        %{id: "flow-overload-batch-b-#{System.unique_integer([:positive])}"}
      ])

    assert [{:error, msg_a}, {:error, msg_b}] = results
    assert msg_a =~ "BUSY"
    assert msg_b =~ "BUSY"
    assert msg_a =~ "retry_after_ms="
  end

  test "flow_create_pipeline_batch rejects malformed attrs without crashing shard" do
    ctx = FerricStore.Instance.get(:default)

    results =
      Router.flow_create_pipeline_batch(ctx, [
        %{id: "flow-malformed-batch-a-#{System.unique_integer([:positive])}"},
        %{
          id: "flow-malformed-batch-b-#{System.unique_integer([:positive])}",
          type: "overload"
        }
      ])

    assert [{:error, msg_a}, {:error, msg_b}] = results
    assert msg_a =~ "ERR flow type"
    assert msg_b =~ "ERR flow state"

    assert :ok = ShardHelpers.wait_shards_alive()
  end

  test "flow_create_many_independent returns top-level overload for SDK backpressure" do
    ctx = FerricStore.Instance.get(:default)
    MemoryGuard.set_reject_writes(true)

    assert {:error, message} =
             Router.flow_create_many_independent(ctx, [
               %{
                 id: "flow-overload-many-a-#{System.unique_integer([:positive])}",
                 type: "overload",
                 state: "queued"
               },
               %{
                 id: "flow-overload-many-b-#{System.unique_integer([:positive])}",
                 type: "overload",
                 state: "queued"
               }
             ])

    assert message =~ "BUSY"
    assert message =~ "overloaded"
    assert message =~ "retry_after_ms="
  end

  test "flow admission gate rejects new creates before memory guard hard reject" do
    ctx = FerricStore.Instance.get(:default)
    Admission.pause_creates(:rss_pressure, 2_000)

    assert {:error, message} =
             Router.flow_create(ctx, %{
               id: "flow-admission-#{System.unique_integer([:positive])}",
               type: "overload",
               state: "queued"
             })

    assert message =~ "BUSY"
    assert message =~ "retry_after_ms=2000"
    assert message =~ "reason=rss_pressure"
  end

  test "flow_create telemetry separates attempts successes and rejections" do
    attach_flow_telemetry([
      [:ferricstore, :flow, :create, :attempt],
      [:ferricstore, :flow, :create, :success],
      [:ferricstore, :flow, :create, :rejected]
    ])

    id = "flow-telemetry-success-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.flow_create(id, type: "telemetry", state: "queued")

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :attempt], %{count: 1},
                    %{result: :ok}}

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :success], %{count: 1},
                    %{result: :ok}}

    refute_receive {:flow_telemetry, [:ferricstore, :flow, :create, :rejected], _measurements,
                    _metadata},
                   50

    MemoryGuard.set_reject_writes(true)
    rejected_id = "flow-telemetry-rejected-#{System.unique_integer([:positive])}"

    assert {:error, message} =
             FerricStore.flow_create(rejected_id, type: "telemetry", state: "queued")

    assert message =~ "BUSY"

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :rejected], %{count: 1},
                    %{reason: _reason}}

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :attempt], %{count: 1},
                    %{result: :error}}

    refute_receive {:flow_telemetry, [:ferricstore, :flow, :create, :success], _measurements,
                    _metadata},
                   50
  end

  test "flow_create_many rejected telemetry counts blocked records" do
    attach_flow_telemetry([[:ferricstore, :flow, :create, :rejected]])

    ctx = FerricStore.Instance.get(:default)
    MemoryGuard.set_reject_writes(true)

    assert {:error, message} =
             Router.flow_create_many_independent(ctx, [
               %{id: "flow-overload-count-a-#{System.unique_integer([:positive])}"},
               %{id: "flow-overload-count-b-#{System.unique_integer([:positive])}"},
               %{id: "flow-overload-count-c-#{System.unique_integer([:positive])}"}
             ])

    assert message =~ "BUSY"

    assert_receive {:flow_telemetry, [:ferricstore, :flow, :create, :rejected], %{count: 3},
                    %{reason: _reason}}
  end
end
