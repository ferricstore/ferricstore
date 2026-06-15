defmodule Ferricstore.Flow.HistoryProjectorPendingTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Pending

  test "overflow batches drain in insertion order and clear taken rows" do
    projector = unique_projector()
    first = [entry("first")]
    second = [entry("second")]

    assert :ok = Pending.append_overflow(projector, first)
    assert :ok = Pending.append_overflow(projector, second)

    assert Pending.take_overflow(projector) == first ++ second
    assert Pending.take_overflow(projector) == []
  end

  test "overflow can be requeued after a failed reserve" do
    projector = unique_projector()
    entries = [entry("retry")]

    assert :ok = Pending.append_overflow(projector, entries)
    assert Pending.take_overflow(projector) == entries
    assert :ok = Pending.append_overflow(projector, entries)
    assert Pending.take_overflow(projector) == entries
  end

  defp unique_projector do
    :"history_projector_pending_test_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp entry(id) do
    %{
      key: "flow/history/#{id}",
      expire_at_ms: 0,
      history_key: "flow/history/index/#{id}",
      event_id: id,
      event_ms: 1,
      version: 1,
      value: id,
      ra_index: 1
    }
  end
end
