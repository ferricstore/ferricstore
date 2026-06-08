defmodule Ferricstore.Flow.HistoryProjector.TelemetryTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Telemetry

  test "publish_requested_index stores monotonic max per shard" do
    ref = :atomics.new(2, signed: false)
    ctx = %{flow_history_requested_index: ref}

    assert Telemetry.publish_requested_index(ctx, 0, 10) == :ok
    assert Telemetry.publish_requested_index(ctx, 0, 5) == :ok
    assert Telemetry.publish_requested_index(ctx, 1, 7) == :ok

    assert :atomics.get(ref, 1) == 10
    assert :atomics.get(ref, 2) == 7
  end

  test "mark_queue_full increments per shard" do
    ref = :atomics.new(1, signed: false)
    ctx = %{flow_history_projector_queue_full: ref}

    assert Telemetry.mark_queue_full(ctx, 0) == :ok
    assert Telemetry.mark_queue_full(ctx, 0) == :ok

    assert :atomics.get(ref, 1) == 2
  end
end
