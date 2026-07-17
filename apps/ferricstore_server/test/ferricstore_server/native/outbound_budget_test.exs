defmodule FerricstoreServer.Native.OutboundBudgetTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.{OutboundBudget, ResourceBudget}

  test "reserves local and global bytes atomically and releases both" do
    budget = start_budget(50)
    counter = OutboundBudget.new_counter()

    state = %{
      resource_budget: budget,
      outbound_counter: counter,
      max_outbound_bytes: 100
    }

    assert {:ok, first} = OutboundBudget.reserve_iodata(state, self(), :binary.copy("a", 40))
    assert OutboundBudget.usage(counter) == 40
    assert ResourceBudget.usage(budget).outbound_bytes == 40

    assert {:error, :global_limit} =
             OutboundBudget.reserve_iodata(state, self(), :binary.copy("b", 20))

    assert OutboundBudget.usage(counter) == 40
    assert ResourceBudget.usage(budget).outbound_bytes == 40

    assert :ok = OutboundBudget.release(first)
    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  test "rejects a per-connection overflow without consuming global capacity" do
    budget = start_budget(1_000)
    counter = OutboundBudget.new_counter()

    state = %{
      resource_budget: budget,
      outbound_counter: counter,
      max_outbound_bytes: 32
    }

    assert {:error, :connection_limit} =
             OutboundBudget.reserve_iodata(state, self(), :binary.copy("x", 33))

    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  test "growing a lease reserves only the encoded delta and rolls back rejection" do
    budget = start_budget(50)
    counter = OutboundBudget.new_counter()

    state = %{
      resource_budget: budget,
      outbound_counter: counter,
      max_outbound_bytes: 100
    }

    assert {:ok, initial} = OutboundBudget.reserve_bytes(state, self(), 20)
    assert {:ok, grown} = OutboundBudget.ensure_iodata(initial, :binary.copy("x", 40))
    assert OutboundBudget.usage(counter) == 40
    assert ResourceBudget.usage(budget).outbound_bytes == 40

    assert {:error, :global_limit} =
             OutboundBudget.ensure_iodata(grown, :binary.copy("x", 60))

    assert OutboundBudget.usage(counter) == 40
    assert ResourceBudget.usage(budget).outbound_bytes == 40

    assert :ok = OutboundBudget.release(grown)
    assert OutboundBudget.usage(counter) == 0
    assert ResourceBudget.usage(budget).outbound_bytes == 0
  end

  defp start_budget(outbound_bytes) do
    name = :"native_outbound_budget_#{System.unique_integer([:positive])}"

    start_supervised!(
      {ResourceBudget,
       name: name,
       limits: %{
         executions: 1,
         lanes: 1,
         blocking_requests: 1,
         chunk_streams: 1,
         chunk_bytes: 1,
         inbound_bytes: 1,
         subscription_bytes: 1,
         session_bytes: 1,
         outbound_bytes: outbound_bytes
       }}
    )

    name
  end
end
