defmodule FerricstoreServer.Native.ResourceBudget.AccountingGate do
  @moduledoc false

  # Atomics and ETS cannot be updated in one transaction. Track interrupted
  # mutations without serializing normal admission, then freeze new mutations
  # while the coordinator reconstructs counters from surviving leases.

  def new do
    %{
      frozen: :atomics.new(1, signed: false),
      actors: :ets.new(__MODULE__, [:set, :public, read_concurrency: true]),
      interrupted: :ets.new(__MODULE__, [:set])
    }
  end

  def run(%{coordinator: coordinator}, operation) when coordinator == self(), do: operation.()

  def run(%{accounting_gate: gate} = budget, operation) do
    actor = actor(gate, budget.coordinator)
    :atomics.put(actor, 1, 1)

    try do
      if :atomics.get(gate.frozen, 1) == 0 do
        operation.()
      else
        :atomics.put(actor, 1, 0)
        :ok = GenServer.call(budget.coordinator, :await_accounting, :infinity)
        run(budget, operation)
      end
    after
      :atomics.put(actor, 1, 0)
    end
  rescue
    ArgumentError -> {:error, :resource_budget_unavailable}
  catch
    :exit, _reason -> {:error, :resource_budget_unavailable}
  end

  def discover_interruptions(gate) do
    :ets.foldl(
      fn {pid, actor}, interrupted ->
        if Process.alive?(pid) do
          interrupted
        else
          if :atomics.get(actor, 1) == 0 do
            :ets.delete(gate.actors, pid)
            interrupted
          else
            :ets.insert(gate.interrupted, {pid})
            true
          end
        end
      end,
      false,
      gate.actors
    )
  end

  def pending_repair?(gate), do: :ets.info(gate.interrupted, :size) > 0

  def owner_down(gate, pid) do
    case :ets.lookup(gate.actors, pid) do
      [{^pid, actor}] ->
        if :atomics.get(actor, 1) == 0 do
          :ets.delete(gate.actors, pid)
        else
          :ets.insert(gate.interrupted, {pid})
        end

      [] ->
        :ok
    end
  end

  def reconcile(gate, state, repair) do
    # Mark-before-check in run/2 closes the entrant/snapshot race. Never reset
    # counters while a live caller can still publish or release a reservation.
    :atomics.put(gate.frozen, 1, 1)

    try do
      actors = :ets.tab2list(gate.actors)

      if quiescent?(actors, 20) do
        state = repair.(state)

        Enum.each(actors, fn {pid, _actor} ->
          if not Process.alive?(pid) do
            :ets.delete(gate.actors, pid)
            :ets.delete(gate.interrupted, pid)
          end
        end)

        state
      else
        state
      end
    after
      :atomics.put(gate.frozen, 1, 0)
    end
  end

  defp actor(gate, coordinator) do
    key = {__MODULE__, gate.actors}

    case Process.get(key) do
      nil ->
        actor = :atomics.new(1, signed: false)
        # Register the death notification before publishing any mutable state.
        # A caller killed before this cast has not changed accounting yet.
        GenServer.cast(coordinator, {:track_owner, self()})
        true = :ets.insert(gate.actors, {self(), actor})
        Process.put(key, actor)
        actor

      actor ->
        actor
    end
  end

  defp quiescent?(actors, remaining) do
    busy =
      Enum.filter(actors, fn {pid, actor} ->
        :atomics.get(actor, 1) != 0 and Process.alive?(pid)
      end)

    cond do
      busy == [] ->
        true

      remaining == 0 ->
        false

      true ->
        Process.sleep(1)
        quiescent?(busy, remaining - 1)
    end
  end
end
