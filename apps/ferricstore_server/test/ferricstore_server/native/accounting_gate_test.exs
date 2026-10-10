defmodule FerricstoreServer.Native.AccountingGateTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.ResourceBudget
  alias FerricstoreServer.Native.ResourceBudget.AccountingGate

  test "reconciliation waits for an in-progress publication and excludes new entrants" do
    gate = AccountingGate.new()
    budget = %{accounting_gate: gate, coordinator: self()}
    parent = self()

    holder =
      Task.async(fn ->
        AccountingGate.run(budget, fn ->
          send(parent, :entered)
          receive do: (:finish -> :ok)
        end)
      end)

    assert_receive :entered

    releaser =
      Task.async(fn ->
        wait_until_frozen(gate)

        entrant =
          Task.async(fn ->
            AccountingGate.run(budget, fn -> send(parent, :new_entrant) end)
          end)

        send(holder.pid, :finish)
        Task.await(entrant)
      end)

    assert {:repaired, from} =
             AccountingGate.reconcile(gate, :initial, fn :initial ->
               assert :atomics.get(gate.frozen, 1) == 1
               assert_receive {:"$gen_call", from, :await_accounting}
               refute_received :new_entrant
               {:repaired, from}
             end)

    GenServer.reply(from, :ok)
    Task.await(holder)
    Task.await(releaser)
    assert_receive :new_entrant
    assert :atomics.get(gate.frozen, 1) == 0
  end

  test "a stalled live mutation defers repair instead of resetting active reservations" do
    gate = AccountingGate.new()
    budget = %{accounting_gate: gate, coordinator: self()}
    parent = self()

    holder =
      Task.async(fn ->
        AccountingGate.run(budget, fn ->
          send(parent, :entered)
          receive do: (:finish -> :ok)
        end)
      end)

    assert_receive :entered
    assert :initial = AccountingGate.reconcile(gate, :initial, fn _ -> flunk("unsafe repair") end)
    assert :atomics.get(gate.frozen, 1) == 0
    send(holder.pid, :finish)
    Task.await(holder)
  end

  test "an interrupted unindexed grant to a surviving owner restores its index and monitor" do
    name = :"budget_orphan_index_#{System.unique_integer([:positive])}"
    coordinator = start_supervised!({ResourceBudget, name: name})
    %{budget: budget} = :sys.get_state(coordinator)
    parent = self()
    owner = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(owner, :kill) end)
    token = make_ref()

    {worker, ref} =
      spawn_monitor(fn ->
        AccountingGate.run(budget, fn ->
          :atomics.add(budget.counters, 1, 1)
          :ets.insert(budget.leases, {token, owner, :executions, 1})
          send(parent, :published)
          Process.exit(self(), :kill)
        end)
      end)

    assert_receive :published
    assert_receive {:DOWN, ^ref, :process, ^worker, :killed}
    assert :ok = GenServer.call(coordinator, {:reclaim_scoped, :all})
    assert [{^owner, ^token}] = :ets.lookup(budget.owner_leases, owner)
    assert ResourceBudget.usage(name).executions == 1
    assert Map.has_key?(:sys.get_state(coordinator).owner_monitors, owner)
    Process.exit(owner, :kill)
    assert eventually(fn -> ResourceBudget.usage(name).executions == 0 end)
  end

  test "death notifications reclaim interrupted reservations without a saturation request" do
    name = :"budget_interrupted_notification_#{System.unique_integer([:positive])}"
    coordinator = start_supervised!({ResourceBudget, name: name, scoped_sweep_interval_ms: 10})
    %{budget: budget} = :sys.get_state(coordinator)

    {worker, ref} =
      spawn_monitor(fn ->
        AccountingGate.run(budget, fn ->
          :atomics.add(budget.counters, 1, 1)
          Process.exit(self(), :kill)
        end)
      end)

    assert_receive {:DOWN, ^ref, :process, ^worker, :killed}
    assert eventually(fn -> ResourceBudget.usage(name).executions == 0 end)
    assert eventually(fn -> :ets.info(budget.accounting_gate.actors, :size) == 0 end)
    assert :ets.info(budget.accounting_gate.interrupted, :size) == 0
  end

  defp wait_until_frozen(gate) do
    if :atomics.get(gate.frozen, 1) == 0 do
      Process.sleep(1)
      wait_until_frozen(gate)
    end
  end

  defp eventually(fun, retries \\ 100)
  defp eventually(fun, 0), do: fun.()

  defp eventually(fun, retries) do
    if fun.() do
      true
    else
      Process.sleep(1)
      eventually(fun, retries - 1)
    end
  end
end
