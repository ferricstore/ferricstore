defmodule Ferricstore.Flow.HistoryProjectorPendingTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.HistoryProjector.Pending
  alias Ferricstore.Flow.HistoryProjector.TableOwner

  @overflow_registry :ferricstore_flow_history_projector_overflow

  test "pending admission fails closed until the projector counter is registered" do
    projector = unique_projector()

    assert Pending.reserve_pending(projector, 1) == {:error, :not_registered}
  end

  test "overflow batches drain in insertion order and clear taken rows" do
    projector = unique_projector()
    first = [entry("first")]
    second = [entry("second")]

    assert {:ok, _first_token} = append_ready(projector, first)
    assert {:ok, _second_token} = append_ready(projector, second)

    assert Pending.take_overflow(projector, 10) == {:ok, first ++ second}
    assert Pending.take_overflow(projector, 10) == {:ok, []}
  end

  test "overflow drains are bounded without reordering a split batch" do
    projector = unique_projector()
    entries = Enum.map(1..5, &entry(Integer.to_string(&1)))

    assert {:ok, _first_token} = append_ready(projector, Enum.take(entries, 3))
    assert {:ok, _second_token} = append_ready(projector, Enum.drop(entries, 3))

    assert Pending.take_overflow(projector, 2) == {:ok, Enum.slice(entries, 0, 2)}
    assert Pending.take_overflow(projector, 2) == {:ok, Enum.slice(entries, 2, 2)}
    assert Pending.take_overflow(projector, 2) == {:ok, Enum.slice(entries, 4, 1)}
    assert Pending.take_overflow(projector, 2) == {:ok, []}
  end

  test "overflow can be requeued after a failed reserve" do
    projector = unique_projector()
    entries = [entry("retry")]

    assert {:ok, _token} = append_ready(projector, entries)
    assert Pending.take_overflow(projector, 10) == {:ok, entries}
    assert {:ok, _token} = append_ready(projector, entries)
    assert Pending.take_overflow(projector, 10) == {:ok, entries}
  end

  test "staged overflow is invisible until its replay reservation is committed" do
    projector = unique_projector()
    entries = [entry("staged")]

    assert {:ok, token} = Pending.append_overflow(projector, entries)
    assert Pending.take_overflow(projector, 10) == {:ok, []}
    assert :ok = Pending.commit_overflow(projector, token)
    assert Pending.take_overflow(projector, 10) == {:ok, entries}
  end

  test "a tracked overflow append can be rolled back without touching later rows" do
    projector = unique_projector()
    first = [entry("rollback")]
    second = [entry("keep")]

    assert {:ok, first_token} = append_ready(projector, first)
    assert {:ok, _second_token} = append_ready(projector, second)
    assert :ok = Pending.delete_overflow(projector, first_token)

    assert Pending.take_overflow(projector, 10) == {:ok, second}
  end

  test "overflow survives the process that first creates the registry" do
    projector = unique_projector()
    entries = [entry("owned-by-table-owner")]
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        with {:ok, token} <- Pending.append_overflow(projector, entries),
             :ok <- Pending.commit_overflow(projector, token) do
          send(parent, {:append_result, {:ok, token}})
        end
      end)

    assert_receive {:append_result, {:ok, _token}}
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}

    assert :ets.info(@overflow_registry, :owner) == Process.whereis(TableOwner)
    assert Pending.take_overflow(projector, 10) == {:ok, entries}
  end

  test "discard clears overflow and replay reservation state" do
    projector = unique_projector()
    entries = [%{entry("discarded") | ra_index: 9}]

    assert :ok = Pending.reserve_replay_range(projector, entries)
    assert :ok = Pending.mark_replay_range_flushed(projector, 8)
    assert {:ok, _token} = append_ready(projector, entries)
    assert Pending.replay_reservation_flushed_index(projector) == 8

    assert :ok = Pending.discard(projector)
    assert Pending.take_overflow(projector, 10) == {:ok, []}
    assert Pending.replay_reservation_flushed_index(projector) == 0
  end

  defp unique_projector do
    :"history_projector_pending_test_#{System.unique_integer([:positive, :monotonic])}"
  end

  defp append_ready(projector, entries) do
    with {:ok, token} <- Pending.append_overflow(projector, entries),
         :ok <- Pending.commit_overflow(projector, token) do
      {:ok, token}
    end
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
