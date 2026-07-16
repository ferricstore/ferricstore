defmodule Ferricstore.Raft.SyncGateManyTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.WARaftBackend.SyncGate

  test "concurrent first entrants share one shard counter" do
    shard_index = System.unique_integer([:positive]) + 100_000
    parent = self()

    entrants =
      for _ <- 1..256 do
        spawn_link(fn ->
          receive do
            :enter ->
              {:ok, token} = SyncGate.enter(shard_index)
              send(parent, {:entered, self(), token})

              receive do
                :leave -> SyncGate.leave(token)
              end
          end
        end)
      end

    Enum.each(entrants, &send(&1, :enter))

    tokens =
      for _ <- entrants do
        assert_receive {:entered, _pid, token}, 5_000
        token
      end

    assert tokens
           |> Enum.map(&elem(&1, 1))
           |> MapSet.new()
           |> MapSet.size() == 1

    Enum.each(entrants, &send(&1, :leave))
  end

  test "multi-shard pause publishes every gate before waiting for active writers" do
    indexes = [30_000, 30_001]
    SyncGate.init_shards(0)

    on_exit(fn -> SyncGate.resume_many(indexes, 1_000) end)

    assert {:ok, held_token} = SyncGate.enter(30_001)
    assert {:ok, pauses} = SyncGate.pause_many(indexes)
    assert Enum.map(pauses, &elem(&1, 0)) == indexes
    assert SyncGate.paused?(30_000)
    assert SyncGate.paused?(30_001)

    parent = self()

    blocked =
      spawn(fn ->
        result = SyncGate.enter(30_000)
        send(parent, {:blocked_enter_result, result})
      end)

    refute_receive {:blocked_enter_result, _result}, 50
    assert Process.alive?(blocked)

    SyncGate.leave(held_token)
    assert :ok = SyncGate.await_many_drained(pauses, 1_000)
    assert :ok = SyncGate.resume_many(indexes, 1_000)
    assert_receive {:blocked_enter_result, {:ok, token}}, 1_000
    SyncGate.leave(token)
  end
end
