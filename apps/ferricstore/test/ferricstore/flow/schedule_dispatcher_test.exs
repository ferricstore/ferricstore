defmodule Ferricstore.Flow.Schedule.DispatcherTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Schedule.Dispatcher

  test "claims bounded waves only after the previous wave finishes" do
    {:ok, state} =
      Agent.start_link(fn ->
        %{
          remaining: Enum.to_list(1..7),
          claim_sizes: [],
          active_at_claim: [],
          active: 0,
          max_active: 0
        }
      end)

    claim = fn requested, first? ->
      Agent.get_and_update(state, fn current ->
        {items, remaining} = Enum.split(current.remaining, requested)

        result = {:ok, Enum.map(items, &%{id: &1, first?: first?})}

        next = %{
          current
          | remaining: remaining,
            claim_sizes: current.claim_sizes ++ [requested],
            active_at_claim: current.active_at_claim ++ [current.active]
        }

        {result, next}
      end)
    end

    process = fn %{id: id} ->
      Agent.update(state, fn current ->
        active = current.active + 1
        %{current | active: active, max_active: max(current.max_active, active)}
      end)

      Process.sleep(10)
      Agent.update(state, &%{&1 | active: &1.active - 1})
      {:processed, id}
    end

    assert {:ok, results, nil} = Dispatcher.run(7, 3, 2, claim, process)

    assert Enum.map(results, fn {%{id: id}, result} -> {id, result} end) ==
             Enum.map(1..7, &{&1, {:processed, &1}})

    snapshot = Agent.get(state, & &1)
    assert snapshot.claim_sizes == [3, 3, 1]
    assert snapshot.active_at_claim == [0, 0, 0]
    assert snapshot.max_active == 2
  end

  test "single-record waves avoid task creation" do
    caller = self()
    claim = fn _requested, _first? -> {:ok, [:record]} end
    process = fn :record -> {self(), :done} end

    assert {:ok, [{:record, {^caller, :done}}], nil} =
             Dispatcher.run(1, 16, 8, claim, process)
  end

  test "returns completed work when a later claim fails" do
    {:ok, calls} = Agent.start_link(fn -> 0 end)

    claim = fn _requested, _first? ->
      case Agent.get_and_update(calls, &{&1, &1 + 1}) do
        0 -> {:ok, [1, 2]}
        _ -> {:error, "ERR claim unavailable"}
      end
    end

    assert {:ok, [{1, 2}, {2, 4}], "ERR claim unavailable"} =
             Dispatcher.run(4, 2, 2, claim, &(&1 * 2))
  end
end
