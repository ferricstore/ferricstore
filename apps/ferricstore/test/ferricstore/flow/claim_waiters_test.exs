defmodule Ferricstore.Flow.ClaimWaitersTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.ClaimWaiters

  @table :ferricstore_flow_claim_waiters

  setup do
    ClaimWaiters.init()
    :ets.delete_all_objects(@table)

    on_exit(fn ->
      if :ets.whereis(@table) != :undefined do
        :ets.delete_all_objects(@table)
      end
    end)

    :ok
  end

  test "multi-state multi-partition wait keys stay compact" do
    states = Enum.map(1..50, &"state-#{&1}")
    partitions = Enum.map(1..32, &"partition-#{&1}")

    keys = ClaimWaiters.wait_keys("email", states, 0, partitions)

    assert length(keys) <= 4
    refute Enum.any?(keys, fn {_type, state, _priority, _partition} -> state in states end)

    refute Enum.any?(keys, fn {_type, _state, _priority, partition} -> partition in partitions end)
  end

  test "registering a broad claim_due waiter uses bounded ETS rows" do
    states = Enum.map(1..50, &"state-#{&1}")
    partitions = Enum.map(1..32, &"partition-#{&1}")
    keys = ClaimWaiters.wait_keys("email", states, 0, partitions)

    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.total_count() <= 4
  end

  test "compact partition waiter still wakes for matching ready work" do
    keys = ClaimWaiters.wait_keys("email", ["queued", "retry"], 0, ["p1", "p2"])

    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p2", 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
    assert ClaimWaiters.total_count() == 0
  end

  test "ready credits coalesce repeated hints and cap herd wake per bucket" do
    waiters = start_waiters(10, ClaimWaiters.wait_keys("email", "queued", 0, ["p1", "p2"]))

    hints =
      Enum.map(1..100, fn _ ->
        {"email", "queued", 0, "p1", 1}
      end)

    assert ClaimWaiters.notify_ready_many(hints, 4) == 4
    assert_receive_count(4)
    refute_receive {:woke, _pid}, 25
    assert ClaimWaiters.total_count() == 6

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  test "ready credits keep distinct ready buckets independent" do
    p1_waiters = start_waiters(3, ClaimWaiters.wait_keys("email", "queued", 0, "p1"))
    p2_waiters = start_waiters(3, ClaimWaiters.wait_keys("email", "queued", 0, "p2"))

    hints = [
      {"email", "queued", 0, "p1", 10},
      {"email", "queued", 0, "p2", 10}
    ]

    assert ClaimWaiters.notify_ready_many(hints, 2) == 4
    assert_receive_count(4)
    refute_receive {:woke, _pid}, 25
    assert ClaimWaiters.total_count() == 2

    Enum.each(p1_waiters ++ p2_waiters, &Process.exit(&1, :kill))
  end

  test "ready credits use waiter claim limits to avoid herd wakeups" do
    waiters = start_waiters(10, ClaimWaiters.wait_keys("email", "queued", 0, "p1"), limit: 100)

    assert ClaimWaiters.notify_ready_many([{"email", "queued", 0, "p1", 50}], 8) == 1
    assert_receive_count(1)
    refute_receive {:woke, _pid}, 25

    assert ClaimWaiters.notify_ready_many([{"email", "queued", 0, "p1", 250}], 8) == 3
    assert_receive_count(3)
    refute_receive {:woke, _pid}, 25

    assert ClaimWaiters.total_count() == 6

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp start_waiters(count, keys, opts \\ []) do
    parent = self()

    for _ <- 1..count do
      spawn(fn ->
        ClaimWaiters.register(keys, self(), now_ms() + 1_000, opts)
        send(parent, {:registered, self()})

        receive do
          {:flow_claim_due_wake, _key} -> send(parent, {:woke, self()})
        after
          1_000 -> :ok
        end
      end)
    end
    |> tap(fn pids ->
      Enum.each(pids, fn pid -> assert_receive {:registered, ^pid}, 100 end)
    end)
  end

  defp assert_receive_count(0), do: :ok

  defp assert_receive_count(count) do
    assert_receive {:woke, _pid}, 100
    assert_receive_count(count - 1)
  end
end
