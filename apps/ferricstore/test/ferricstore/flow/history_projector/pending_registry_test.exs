defmodule Ferricstore.Flow.HistoryProjector.PendingRegistryTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.PendingRegistry

  test "reserve and release enforce pending capacity" do
    projector = :history_projector_pending_registry_test
    counter = :atomics.new(1, signed: true)

    PendingRegistry.unregister(projector)
    assert PendingRegistry.register(projector, counter, 2) == :ok
    assert PendingRegistry.reserve(projector, 2) == :ok
    assert PendingRegistry.reserve(projector, 1) == {:error, :queue_full, 2, 2}

    assert PendingRegistry.release(%{pending_counter: counter}, 2) == :ok
    assert PendingRegistry.reserve(projector, 1) == :ok

    PendingRegistry.unregister(projector)
  end

  test "replay reservation tracks flushed index and trims range" do
    projector = :history_projector_replay_registry_test

    PendingRegistry.trim_replay_reservation(projector, 1_000_000)
    assert PendingRegistry.replay_reservation_flushed_index(projector) == 0

    assert PendingRegistry.reserve_replay_range(projector, [%{ra_index: 5}, %{ra_index: 7}]) ==
             :ok

    assert PendingRegistry.mark_replay_range_flushed(projector, 6) == :ok
    assert PendingRegistry.replay_reservation_flushed_index(projector) == 6

    assert PendingRegistry.trim_replay_reservation(projector, 7) == :ok
    assert PendingRegistry.replay_reservation_flushed_index(projector) == 0
  end
end
