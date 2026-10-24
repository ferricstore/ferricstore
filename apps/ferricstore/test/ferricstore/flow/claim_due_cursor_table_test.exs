defmodule Ferricstore.Flow.ClaimDueCursorTableTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  @cursor_table :ferricstore_flow_claim_due_any_cursor

  # A claim without a partition picks its first shard with a counter in a
  # shared table. That table must outlive the worker that claimed first, or a
  # claim running in another worker fails on the vanished table.
  test "the claim cursor table outlives the worker that claimed first" do
    # As on a node where no claim has run yet.
    if :ets.whereis(@cursor_table) != :undefined, do: :ets.delete(@cursor_table)

    parent = self()
    type = "claim-cursor-#{System.unique_integer([:positive])}"

    {worker, monitor} =
      spawn_monitor(fn ->
        send(parent, {:claimed, FerricStore.flow_claim_due(type, worker: "w1", limit: 1)})
      end)

    assert_receive {:claimed, {:ok, []}}, 5_000
    assert_receive {:DOWN, ^monitor, :process, ^worker, :normal}, 5_000

    assert :ets.whereis(@cursor_table) != :undefined
  end
end
