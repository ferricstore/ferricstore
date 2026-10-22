defmodule Ferricstore.Flow.HistoryValueRelease.WorkerTest do
  @moduledoc """
  Apply hints every replica's worker, and a release goes through Raft, so only
  the shard's leader runs it: each other node would read the same history and
  propose the same release again.
  """
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.HistoryValueRelease.Worker
  alias Ferricstore.Raft.LocalLeadership

  test "only the shard's leader releases the hinted Flows" do
    parent = self()

    {:ok, worker} =
      Worker.start_link(
        instance_ctx: %{
          name: :"history_value_release_worker_#{System.unique_integer([:positive])}"
        },
        leads_shard?: fn _ctx, shard_index -> shard_index == 0 end,
        release: fn _ctx, shard_index, flows ->
          send(parent, {:released, shard_index, flows})
          :ok
        end
      )

    GenServer.cast(worker, {:trimmed, 0, "led", nil})
    GenServer.cast(worker, {:trimmed, 1, "followed", "partition"})
    send(worker, :tick)

    assert_receive {:released, 0, [{"led", nil}]}
    refute_receive {:released, 1, _flows}, 200
  end

  test "a single-node instance leads every shard" do
    ctx = FerricStore.Instance.get(:default)

    for shard_index <- 0..(ctx.shard_count - 1) do
      assert LocalLeadership.leads?(ctx, shard_index)
    end
  end
end
