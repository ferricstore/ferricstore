defmodule FerricstoreServer.Native.ResourceBudgetTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.ResourceBudget

  test "enforces limits across owners and reclaims every lease when an owner exits" do
    name = :"native_resource_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 2, chunk_bytes: 8}}
    )

    owner = spawn(fn -> Process.sleep(:infinity) end)
    other = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Enum.each([owner, other], &Process.exit(&1, :kill)) end)

    assert {:ok, lane_token} = ResourceBudget.acquire(name, :lanes, owner, 1)
    assert {:error, {:limit, :lanes}} = ResourceBudget.acquire(name, :lanes, other, 1)
    assert %{lanes: 1} = ResourceBudget.usage(name)

    assert :ok = ResourceBudget.release(name, lane_token)
    assert {:ok, _other_lane_token} = ResourceBudget.acquire(name, :lanes, other, 1)

    assert {:ok, _blocking_token} =
             ResourceBudget.acquire(name, :blocking_requests, owner, 1)

    Process.exit(owner, :kill)

    assert eventually(fn ->
             usage = ResourceBudget.usage(name)
             usage.blocking_requests == 0 and usage.lanes == 1
           end)
  end

  test "indexes leases and waiters by owner for bounded process-down cleanup" do
    name = :"native_resource_owner_index_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 3,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1
       }}
    )

    owner = spawn(fn -> Process.sleep(:infinity) end)
    other = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Enum.each([owner, other], &Process.exit(&1, :kill)) end)

    assert {:ok, owner_first} = ResourceBudget.acquire(name, :executions, owner, 1)
    assert {:ok, owner_second} = ResourceBudget.acquire(name, :executions, owner, 1)
    assert {:ok, other_token} = ResourceBudget.acquire(name, :executions, other, 1)

    state = :sys.get_state(name)

    assert MapSet.new(:ets.lookup(state.budget.owner_leases, owner)) ==
             MapSet.new([{owner, owner_first}, {owner, owner_second}])

    assert :ok = ResourceBudget.release(name, owner_first)

    assert eventually(fn ->
             state = :sys.get_state(name)
             :ets.lookup(state.budget.owner_leases, owner) == [{owner, owner_second}]
           end)

    parent = self()

    waiter =
      spawn(fn ->
        send(
          parent,
          {:waiter_result, self(), ResourceBudget.acquire_wait(name, :executions, self(), 2)}
        )
      end)

    assert eventually(fn ->
             state = :sys.get_state(name)
             Map.get(state.waiting_by_owner, waiter, MapSet.new()) |> MapSet.size() == 1
           end)

    Process.exit(waiter, :kill)

    assert eventually(fn ->
             state = :sys.get_state(name)

             not Map.has_key?(state.waiting_by_owner, waiter) and
               ResourceBudget.waiting(name).executions == 0
           end)

    Process.exit(owner, :kill)

    assert eventually(fn ->
             state = :sys.get_state(name)

             :ets.lookup(state.budget.owner_leases, owner) == [] and
               ResourceBudget.usage(name).executions == 1
           end)

    refute_receive {:waiter_result, ^waiter, _result}
    assert :ok = ResourceBudget.release(name, other_token)
  end

  test "resizing a byte lease is atomic and cannot cross the global ceiling" do
    name = :"native_resource_bytes_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 8}}
    )

    assert {:ok, token} = ResourceBudget.acquire(name, :chunk_bytes, self(), 4)
    assert :ok = ResourceBudget.resize(name, token, 8)
    assert {:error, {:limit, :chunk_bytes}} = ResourceBudget.resize(name, token, 9)
    assert %{chunk_bytes: 8} = ResourceBudget.usage(name)

    assert :ok = ResourceBudget.release(name, token)
    assert %{chunk_bytes: 0} = ResourceBudget.usage(name)
  end

  test "scoped leases enforce limits and release exactly once" do
    name = :"native_resource_scoped_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    assert {:ok, token} = ResourceBudget.acquire_scoped(name, :executions, 1)

    assert {:error, {:limit, :executions}} =
             ResourceBudget.acquire_scoped(name, :executions, 1)

    assert ResourceBudget.usage(name).executions == 1
    parent = self()

    waiter =
      spawn(fn ->
        result = ResourceBudget.acquire_wait(name, :executions, self(), 1)
        send(parent, {:scoped_waiter_admitted, self(), result})

        receive do
          :release ->
            with {:ok, waiter_token} <- result,
                 do: ResourceBudget.release(name, waiter_token)
        end
      end)

    assert eventually(fn -> ResourceBudget.waiting(name).executions == 1 end)
    assert :ok = ResourceBudget.release_scoped(token)
    assert_receive {:scoped_waiter_admitted, ^waiter, {:ok, _waiter_token}}
    send(waiter, :release)
    assert eventually(fn -> ResourceBudget.usage(name).executions == 0 end)
    assert :ok = ResourceBudget.release_scoped(token)
  end

  test "scoped leases cannot be released by another process" do
    name = :"native_resource_scoped_owner_check_#{System.unique_integer([:positive])}"
    start_supervised!({ResourceBudget, name: name})
    assert {:ok, token} = ResourceBudget.acquire_scoped(name, :executions, 1)

    assert {:error, :lease_owner_mismatch} =
             Task.async(fn -> ResourceBudget.release_scoped(token) end) |> Task.await()

    assert ResourceBudget.usage(name).executions == 1
    assert :ok = ResourceBudget.release_scoped(token)
  end

  test "scoped leases are reclaimed when their owner exits" do
    name = :"native_resource_scoped_owner_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       scoped_sweep_interval_ms: 10,
       limits: %{executions: 2, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    parent = self()

    owner =
      spawn(fn ->
        assert {:ok, _first} = ResourceBudget.acquire_scoped(name, :executions, 1)
        assert {:ok, _second} = ResourceBudget.acquire_scoped(name, :executions, 1)
        send(parent, {:scoped_leases_acquired, self()})
        Process.sleep(:infinity)
      end)

    assert_receive {:scoped_leases_acquired, ^owner}
    assert ResourceBudget.usage(name).executions == 2
    Process.exit(owner, :kill)
    assert eventually(fn -> ResourceBudget.usage(name).executions == 0 end)
  end

  test "a saturated scoped acquire reclaims a dead owner before returning busy" do
    name = :"native_resource_scoped_saturated_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget, name: name, scoped_sweep_interval_ms: 60_000, limits: %{executions: 1}}
    )

    parent = self()

    owner =
      spawn(fn ->
        assert {:ok, _token} = ResourceBudget.acquire_scoped(name, :executions, 1)
        send(parent, {:saturated_scoped_lease_acquired, self()})
        Process.sleep(:infinity)
      end)

    assert_receive {:saturated_scoped_lease_acquired, ^owner}
    monitor_ref = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor_ref, :process, ^owner, :killed}

    assert {:ok, replacement} = ResourceBudget.acquire_scoped(name, :executions, 1)
    assert :ok = ResourceBudget.release_scoped(replacement)
    assert ResourceBudget.usage(name).executions == 0
  end

  @tag :lock_free_resource_budget
  test "normal scoped lease accounting does not use the coordinator mailbox" do
    name = :"native_resource_scoped_fast_path_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!({ResourceBudget, name: name, scoped_sweep_interval_ms: 60_000})

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: :sys.resume(pid)
      catch
        :exit, _reason -> :ok
      end
    end)

    :ok = :sys.suspend(pid)
    assert {:ok, token} = ResourceBudget.acquire_scoped(name, :executions, 1)
    assert :ok = ResourceBudget.release_scoped(token)
    assert {:message_queue_len, 0} = Process.info(pid, :message_queue_len)
    :ok = :sys.resume(pid)
  end

  test "stale registry handles are rejected after the coordinator is killed" do
    name = :"native_resource_stale_registry_#{System.unique_integer([:positive])}"
    {:ok, pid} = ResourceBudget.start_link(name: name)
    Process.unlink(pid)
    monitor_ref = Process.monitor(pid)

    on_exit(fn ->
      :persistent_term.erase({ResourceBudget, :budget, name})
      :persistent_term.erase({ResourceBudget, :budget, pid})
    end)

    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor_ref, :process, ^pid, :killed}

    assert {:error, :resource_budget_unavailable} =
             ResourceBudget.acquire_scoped(name, :executions, 1)
  end

  @tag :lock_free_resource_budget
  test "non-waiting accounting does not block on the coordinator mailbox" do
    name = :"native_resource_fast_path_#{System.unique_integer([:positive])}"

    pid =
      start_supervised!(
        {ResourceBudget,
         name: name,
         limits: %{
           executions: 2,
           lanes: 1,
           blocking_requests: 1,
           chunk_streams: 1,
           chunk_bytes: 8,
           inbound_bytes: 8
         }}
      )

    on_exit(fn ->
      try do
        if Process.alive?(pid), do: :sys.resume(pid)
      catch
        :exit, _reason -> :ok
      end
    end)

    :ok = :sys.suspend(pid)

    task =
      Task.async(fn ->
        with {:ok, token} <- ResourceBudget.acquire_wait(name, :executions, self(), 1),
             %{executions: 1} <- ResourceBudget.usage(name),
             :ok <- ResourceBudget.release(name, token) do
          ResourceBudget.usage(name)
        end
      end)

    assert {:ok, %{executions: 0}} = Task.yield(task, 100)
    :ok = :sys.resume(pid)
  end

  @tag :global_inbound_budget
  test "inbound buffers share one byte ceiling across connections" do
    name = :"native_inbound_bytes_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 8
       }}
    )

    owner = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(owner, :kill) end)

    assert {:ok, first} = ResourceBudget.acquire(name, :inbound_bytes, self(), 6)

    assert {:error, {:limit, :inbound_bytes}} =
             ResourceBudget.acquire(name, :inbound_bytes, owner, 3)

    assert :ok = ResourceBudget.release(name, first)
    assert {:ok, second} = ResourceBudget.acquire(name, :inbound_bytes, owner, 3)
    assert :ok = ResourceBudget.release(name, second)
  end

  test "waiters are admitted when release creates capacity without polling" do
    name = :"native_resource_waiters_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    assert {:ok, holder_token} = ResourceBudget.acquire(name, :executions, self(), 1)
    parent = self()

    waiter =
      spawn(fn ->
        result = ResourceBudget.acquire_wait(name, :executions, self(), 1)
        send(parent, {:waiter_admitted, self(), result})

        receive do
          :release ->
            with {:ok, token} <- result, do: ResourceBudget.release(name, token)
        end
      end)

    assert eventually(fn -> ResourceBudget.waiting(name).executions == 1 end)
    refute_receive {:waiter_admitted, ^waiter, _result}, 20

    assert :ok = ResourceBudget.release(name, holder_token)
    assert_receive {:waiter_admitted, ^waiter, {:ok, _token}}, 500
    send(waiter, :release)

    assert eventually(fn -> ResourceBudget.usage(name).executions == 0 end)
  end

  test "dead waiters cannot leave an unbounded saturated FIFO" do
    name = :"native_resource_dead_waiters_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{executions: 1, lanes: 1, blocking_requests: 1, chunk_streams: 1, chunk_bytes: 1}}
    )

    assert {:ok, holder_token} = ResourceBudget.acquire(name, :executions, self(), 1)

    waiters =
      for _ <- 1..100 do
        spawn(fn -> ResourceBudget.acquire_wait(name, :executions, self(), 1) end)
      end

    assert eventually(fn -> ResourceBudget.waiting(name).executions == 100 end)
    Enum.each(waiters, &Process.exit(&1, :kill))
    assert eventually(fn -> ResourceBudget.waiting(name).executions == 0 end)

    assert ResourceBudget.waiter_queue_depths(name).executions <= 64
    assert :ok = ResourceBudget.release(name, holder_token)
  end

  @tag :resource_budget_contention
  test "parallel fast-path acquisitions never oversubscribe the configured limit" do
    name = :"native_resource_contention_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 4,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1
       }}
    )

    active = :atomics.new(2, signed: false)

    1..32
    |> Task.async_stream(
      fn _worker ->
        for _attempt <- 1..100 do
          case ResourceBudget.acquire(name, :executions, self(), 1) do
            {:ok, token} ->
              concurrent = :atomics.add_get(active, 1, 1)
              update_max(active, 2, concurrent)
              Process.sleep(0)
              :atomics.sub(active, 1, 1)
              :ok = ResourceBudget.release(name, token)

            {:error, {:limit, :executions}} ->
              Process.sleep(0)
          end
        end
      end,
      max_concurrency: 32,
      timeout: 5_000,
      ordered: false
    )
    |> Stream.run()

    assert :atomics.get(active, 2) <= 4
    assert ResourceBudget.usage(name).executions == 0
  end

  defp eventually(fun, attempts \\ 50)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, attempts) do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp update_max(atomics, index, value) do
    current = :atomics.get(atomics, index)

    if value > current do
      case :atomics.compare_exchange(atomics, index, current, value) do
        :ok -> :ok
        _changed -> update_max(atomics, index, value)
      end
    else
      :ok
    end
  end
end
