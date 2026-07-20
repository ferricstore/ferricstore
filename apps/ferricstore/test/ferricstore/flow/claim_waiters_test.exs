defmodule Ferricstore.Flow.ClaimWaitersTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.{ClaimWaiters, Keys, StorageScope}
  alias Ferricstore.Test.ShardHelpers

  @table :ferricstore_flow_claim_waiters
  @timer_table :ferricstore_flow_claim_waiter_timers
  @source_path Path.expand("../../../lib/ferricstore/flow/claim_waiters.ex", __DIR__)
  @flow_source_path Path.expand("../../../lib/ferricstore/flow.ex", __DIR__)
  @waiter_registration_timeout_ms 5_000

  setup do
    old_max_waiter_rows = Application.get_env(:ferricstore, :flow_claim_due_max_waiter_rows)
    ClaimWaiters.init()
    :ets.delete_all_objects(@table)
    :ets.delete_all_objects(@timer_table)

    on_exit(fn ->
      restore_env(:flow_claim_due_max_waiter_rows, old_max_waiter_rows)

      if :ets.whereis(@table) != :undefined do
        :ets.delete_all_objects(@table)
      end

      if :ets.whereis(@timer_table) != :undefined do
        :ets.delete_all_objects(@timer_table)
      end
    end)

    :ok
  end

  test "register capacity check does not scan all waiters on the hot path" do
    source = File.read!(@source_path)

    [function_source] =
      Regex.run(
        ~r/defp ensure_waiter_capacity\(keys\).*?^  end/ms,
        source
      )

    refute function_source =~ "total_count()",
           "FLOW.CLAIM_DUE BLOCK registration must not prune/scan all waiters on every register"

    refute function_source =~ ":ets.tab2list",
           "FLOW.CLAIM_DUE BLOCK registration must use O(1) size checks and prune only under pressure"
  end

  test "capacity-pressure pruning is bounded and does not scan every waiter row" do
    source = File.read!(@source_path)

    [function_source] =
      Regex.run(
        ~r/defp ensure_waiter_capacity\(keys\).*?^  end/ms,
        source
      )

    refute function_source =~ "prune_stale_entries()",
           "capacity pressure must not run the exact full-table stale waiter prune"

    assert function_source =~ "prune_stale_entries(@capacity_prune_page_size)",
           "capacity pressure should prune a bounded page before rejecting"
  end

  test "scheduled ready cleanup uses a bounded timer-table page" do
    source = File.read!(@source_path)

    [function_source] =
      Regex.run(
        ~r/defp prune_scheduled_ready_without_waiters do.*?^  end/ms,
        source
      )

    refute function_source =~ ":ets.tab2list",
           "FLOW.CLAIM_DUE BLOCK unregister must not copy every delayed wake timer"

    assert source =~ "select_scheduled_ready_timer_keys(@scheduled_prune_page_size)",
           "timer cleanup should inspect a bounded ETS page per unregister/cleanup"
  end

  test "stale waiter pruning does not copy the whole waiter table" do
    source = File.read!(@source_path)

    [function_source] =
      Regex.run(
        ~r/def prune_stale_entries do.*?^  end/ms,
        source
      )

    refute function_source =~ ":ets.tab2list",
           "stale waiter pruning should stream/delete ETS rows instead of copying every waiter"
  end

  test "flow retry wake gate does not scan all blocked waiters" do
    source = File.read!(@flow_source_path)

    refute source =~ "ClaimWaiters.total_count() > 0",
           "Flow write hot paths should use an O(1) waiter presence check, not total_count/0"

    assert source =~ "ClaimWaiters.any_waiters?()"
  end

  test "flow delayed wake gate checks only matching waiter keys" do
    source = File.read!(@flow_source_path)

    refute source =~ "ClaimWaiters.count(&1) > 0",
           "delayed Flow wake checks must not prune/scan all waiters per ready key"

    assert source =~ "&ClaimWaiters.has_live_waiter?/1"
  end

  test "register fails closed when blocked claim_due waiter row cap is reached" do
    Application.put_env(:ferricstore, :flow_claim_due_max_waiter_rows, 1)

    key1 = ClaimWaiters.wait_keys("email", "queued", 0, "p1")
    key2 = ClaimWaiters.wait_keys("email", "queued", 0, "p2")

    assert :ok = ClaimWaiters.register(key1, self(), now_ms() + 1_000)

    assert {:error, "ERR max blocked claim_due waiters reached"} =
             ClaimWaiters.register(key2, self(), now_ms() + 1_000)

    assert ClaimWaiters.total_count() == 1
  end

  test "register rejects broad waiter atomically when row cap cannot fit all keys" do
    Application.put_env(:ferricstore, :flow_claim_due_max_waiter_rows, 1)

    keys = ClaimWaiters.wait_keys("email", "queued", 0, ["p1", "p2"])

    assert {:error, "ERR max blocked claim_due waiters reached"} =
             ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.total_count() == 0
  end

  test "concurrent registrations cannot oversubscribe the waiter row cap" do
    Application.put_env(:ferricstore, :flow_claim_due_max_waiter_rows, 1)
    parent = self()

    contenders =
      for id <- 1..64 do
        spawn(fn ->
          key = {"email", "queued", 0, "p#{id}"}
          send(parent, {:ready, self()})

          receive do
            :register ->
              result = ClaimWaiters.register([key], self(), now_ms() + 1_000)
              send(parent, {:registered, self(), result})

              receive do
                :stop -> ClaimWaiters.unregister([key], self())
              end
          end
        end)
      end

    on_exit(fn -> Enum.each(contenders, &Process.exit(&1, :kill)) end)

    Enum.each(contenders, fn pid -> assert_receive {:ready, ^pid}, 1_000 end)
    Enum.each(contenders, &send(&1, :register))

    results =
      Enum.map(contenders, fn pid ->
        assert_receive {:registered, ^pid, result}, 1_000
        result
      end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert ClaimWaiters.total_count() == 1

    Enum.each(contenders, &send(&1, :stop))
  end

  test "register rejects a direct key set larger than the waiter footprint limit" do
    keys = for id <- 1..65, do: {"email", "queued", 0, "p#{id}"}

    assert {:error, "ERR blocked claim_due waiter has too many keys"} =
             ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.total_count() == 0
  end

  test "multi-state multi-partition wait keys stay compact" do
    states = Enum.map(1..50, &"state-#{&1}")
    partitions = Enum.map(1..32, &"partition-#{&1}")

    keys = ClaimWaiters.wait_keys("email", states, 0, partitions)

    assert length(keys) <= 64
    refute Enum.any?(keys, fn {_type, state, _priority, _partition} -> state in states end)

    assert Enum.any?(keys, fn {_type, _state, _priority, partition} -> partition in partitions end)
  end

  test "registering a broad claim_due waiter uses bounded ETS rows" do
    states = Enum.map(1..50, &"state-#{&1}")
    partitions = Enum.map(1..32, &"partition-#{&1}")
    keys = ClaimWaiters.wait_keys("email", states, 0, partitions)

    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.total_count() <= 64
  end

  test "compact partition waiter still wakes for matching ready work" do
    keys = ClaimWaiters.wait_keys("email", ["queued", "retry"], 0, ["p1", "p2"])

    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p2", 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
    assert ClaimWaiters.total_count() == 0
  end

  test "small partition-list waiters do not wake for unrelated partitions" do
    keys = ClaimWaiters.wait_keys("email", "queued", 0, ["p1", "p2"])

    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p3", 1) == 0
    refute_receive {:flow_claim_due_wake, _key}, 25
    assert ClaimWaiters.total_count() == 2

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p2", 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
    assert ClaimWaiters.total_count() == 0
  end

  test "moderate partition-list waiters stay bounded without waking for unrelated partitions" do
    partitions = Enum.map(1..32, &"p#{&1}")
    keys = ClaimWaiters.wait_keys("email", "queued", 0, partitions)

    assert length(keys) <= 64
    ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "outside", 1) == 0
    refute_receive {:flow_claim_due_wake, _key}, 25
    assert ClaimWaiters.total_count() == length(partitions)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p17", 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
    assert ClaimWaiters.total_count() == 0
  end

  test "broad shared-scope waiters stay compact without cross-scope wakeups" do
    tenant_a_partitions = scoped_auto_partitions(<<11::unsigned-big-64>>)
    tenant_b_partitions = scoped_auto_partitions(<<22::unsigned-big-64>>)
    keys = ClaimWaiters.wait_keys("email", "queued", 0, tenant_a_partitions)

    assert length(keys) == 1
    assert :ok = ClaimWaiters.register(keys, self(), now_ms() + 1_000)

    assert ClaimWaiters.notify_ready("email", "queued", 0, hd(tenant_b_partitions), 1) == 0
    refute_receive {:flow_claim_due_wake, _key}, 25
    assert ClaimWaiters.total_count() == 1

    assert ClaimWaiters.notify_ready("email", "queued", 0, hd(tenant_a_partitions), 1) == 1
    assert_receive {:flow_claim_due_wake, _key}, 100
    assert ClaimWaiters.total_count() == 0
  end

  test "notify skips expired waiters" do
    keys = ClaimWaiters.wait_keys("email", "queued", 0, "p1")

    ClaimWaiters.register(keys, self(), now_ms() - 1)

    assert ClaimWaiters.notify_ready("email", "queued", 0, "p1", 1) == 0
    refute_receive {:flow_claim_due_wake, _key}, 25
    assert ClaimWaiters.total_count() == 0
  end

  test "total_count prunes waiters whose process died before unregistering" do
    keys = ClaimWaiters.wait_keys("email", "queued", 0, "p1")
    parent = self()

    pid =
      spawn(fn ->
        ClaimWaiters.register(keys, self(), now_ms() + 1_000)
        send(parent, {:registered, self()})

        receive do
          :stop -> :ok
        after
          5_000 -> :ok
        end
      end)

    assert_receive {:registered, ^pid}, 100
    assert ClaimWaiters.total_count() == 1

    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 100

    assert ClaimWaiters.total_count() == 0
  end

  test "ready credits coalesce repeated hints and wake available workers for ready work" do
    waiters = start_waiters(10, ClaimWaiters.wait_keys("email", "queued", 0, ["p1", "p2"]))

    hints =
      Enum.map(1..100, fn _ ->
        {"email", "queued", 0, "p1", 1}
      end)

    assert ClaimWaiters.notify_ready_many(hints, 4) == 10
    assert_receive_count(10)
    refute_receive {:woke, _pid}, 25
    assert ClaimWaiters.total_count() == 0

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  test "ready credits keep distinct ready buckets independent" do
    p1_waiters = start_waiters(3, ClaimWaiters.wait_keys("email", "queued", 0, "p1"))
    p2_waiters = start_waiters(3, ClaimWaiters.wait_keys("email", "queued", 0, "p2"))

    hints = [
      {"email", "queued", 0, "p1", 10},
      {"email", "queued", 0, "p2", 10}
    ]

    assert ClaimWaiters.notify_ready_many(hints, 2) == 6
    assert_receive_count(6)
    refute_receive {:woke, _pid}, 25
    assert ClaimWaiters.total_count() == 0

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

  test "large ready batches are not stranded behind the legacy fixed wake cap" do
    waiters = start_waiters(20, ClaimWaiters.wait_keys("email", "queued", 0, "p1"), limit: 1)

    assert ClaimWaiters.notify_ready_many([{"email", "queued", 0, "p1", 20}], 8) == 20
    assert_receive_count(20)
    assert ClaimWaiters.total_count() == 0

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  test "future ready timers coalesce by ready bucket" do
    waiters = start_waiters(10, ClaimWaiters.wait_keys("email", "queued", 0, "p1"), limit: 1)
    due_at = Ferricstore.CommandTime.now_ms() + 40

    for _ <- 1..10 do
      assert :ok = ClaimWaiters.schedule_ready("email", "queued", 0, "p1", due_at, 1)
    end

    assert ClaimWaiters.scheduled_count() == 1
    assert_receive_count(10, 1_000)

    ShardHelpers.eventually(
      fn -> ClaimWaiters.scheduled_count() == 0 end,
      "scheduled ready bucket removed",
      100,
      5
    )

    assert ClaimWaiters.total_count() == 0

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  test "future ready timers use flow wall-clock due times" do
    waiters = start_waiters(1, ClaimWaiters.wait_keys("email", "queued", 0, "p1"), limit: 1)
    due_at = Ferricstore.CommandTime.now_ms() + 30

    assert :ok = ClaimWaiters.schedule_ready("email", "queued", 0, "p1", due_at, 1)
    assert_receive_count(1, 1_000)

    Enum.each(waiters, &Process.exit(&1, :kill))
  end

  test "future ready timers are pruned when the matching waiter unregisters" do
    keys = ClaimWaiters.wait_keys("email", "queued", 0, "p1")
    due_at = Ferricstore.CommandTime.now_ms() + 60_000

    assert :ok = ClaimWaiters.register(keys, self(), now_ms() + 1_000, limit: 1)
    assert :ok = ClaimWaiters.schedule_ready("email", "queued", 0, "p1", due_at, 1)
    assert ClaimWaiters.scheduled_count() == 1

    assert :ok = ClaimWaiters.unregister(keys, self())
    assert ClaimWaiters.scheduled_count() == 0
  end

  test "unregister cancels the underlying delayed wake timer" do
    keys = ClaimWaiters.wait_keys("email", "queued", 0, "p1")
    due_at = Ferricstore.CommandTime.now_ms() + 60_000

    assert :ok = ClaimWaiters.register(keys, self(), now_ms() + 1_000, limit: 1)
    assert :ok = ClaimWaiters.schedule_ready("email", "queued", 0, "p1", due_at, 1)

    assert [{_timer_key, 1, {:once, timer_ref}}] = :ets.tab2list(@timer_table)
    assert is_integer(:erlang.read_timer(timer_ref))

    assert :ok = ClaimWaiters.unregister(keys, self())
    assert :erlang.read_timer(timer_ref) == false
  end

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp scoped_auto_partitions(scope) do
    Enum.map(Keys.auto_partition_keys(), fn logical_partition ->
      assert {:ok, physical_partition} =
               StorageScope.physical_partition_key(logical_partition, scope)

      physical_partition
    end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp start_waiters(count, keys, opts \\ []) do
    parent = self()

    pids =
      for _ <- 1..count do
        spawn(fn ->
          parent_monitor = Process.monitor(parent)

          :ok =
            ClaimWaiters.register(
              keys,
              self(),
              now_ms() + @waiter_registration_timeout_ms + 1_000,
              opts
            )

          send(parent, {:registered, self()})

          receive do
            {:flow_claim_due_wake, _key} ->
              send(parent, {:woke, self()})

            {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
              :ok
          end
        end)
      end

    await_waiter_registrations(pids, @waiter_registration_timeout_ms)
    pids
  end

  defp await_waiter_registrations(pids, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_waiter_registrations(MapSet.new(pids), deadline)
  end

  defp do_await_waiter_registrations(pending, deadline) do
    if MapSet.size(pending) == 0 do
      :ok
    else
      remaining = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:registered, pid} ->
          do_await_waiter_registrations(MapSet.delete(pending, pid), deadline)
      after
        remaining ->
          flunk(
            "#{MapSet.size(pending)} claim waiter registration(s) did not complete within " <>
              "#{@waiter_registration_timeout_ms}ms"
          )
      end
    end
  end

  defp assert_receive_count(0), do: :ok

  defp assert_receive_count(count) do
    assert_receive_count(count, count * 100)
  end

  defp assert_receive_count(count, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_assert_receive_count(count, deadline)
  end

  defp do_assert_receive_count(0, _deadline), do: :ok

  defp do_assert_receive_count(count, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:woke, _pid} -> do_assert_receive_count(count - 1, deadline)
    after
      remaining -> flunk("expected #{count} more claim waiter wake message(s)")
    end
  end
end
