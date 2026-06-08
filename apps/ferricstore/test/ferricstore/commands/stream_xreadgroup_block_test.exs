defmodule Ferricstore.Commands.StreamXreadgroupBlockTest do
  @moduledoc """
  TDD tests for XREADGROUP BLOCK support (M4).
  XREADGROUP should accept BLOCK like XREAD does.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream
  alias Ferricstore.Test.MockStore

  defp ustream, do: "xrgb_#{System.unique_integer([:positive, :monotonic])}"

  setup do
    for table <- [Ferricstore.Stream.Meta, Ferricstore.Stream.Groups] do
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end

    if :ets.whereis(:ferricstore_stream_waiters) == :undefined do
      try do
        :ets.new(:ferricstore_stream_waiters, [:duplicate_bag, :public, :named_table])
      rescue
        ArgumentError -> :ok
      end
    else
      :ets.delete_all_objects(:ferricstore_stream_waiters)
    end

    :ok
  end

  describe "XREADGROUP BLOCK parsing" do
    test "XREADGROUP BLOCK parses and returns data when available" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "consumer1", "BLOCK", "0", "STREAMS", key, ">"],
          store
        )

      # Data available — should return immediately, not block
      assert is_list(result)
      assert length(result) == 1
      [[^key, entries]] = result
      assert length(entries) == 1
    end

    test "XREADGROUP BLOCK with COUNT parses correctly" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "c1", "COUNT", "2", "BLOCK", "0", "STREAMS", key, ">"],
          store
        )

      assert is_list(result)
      [[^key, entries]] = result
      assert length(entries) == 2
    end

    test "XREADGROUP BLOCK returns block tuple when no data available" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      # Consume all messages first
      Stream.handle(
        "XREADGROUP",
        ["GROUP", "grp", "c1", "STREAMS", key, ">"],
        store
      )

      # Now BLOCK — no new messages, should signal block
      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "c1", "BLOCK", "100", "STREAMS", key, ">"],
          store
        )

      # Should return {:block, timeout_ms, ...} tuple for the connection layer
      assert is_tuple(result)
      assert elem(result, 0) == :block
      assert elem(result, 1) == 100
    end

    test "XREADGROUP without BLOCK still works (non-blocking)" do
      store = MockStore.make()
      key = ustream()

      Stream.handle("XADD", [key, "1-0", "f", "v"], store)
      Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "c1", "STREAMS", key, ">"],
          store
        )

      assert is_list(result)
      [[^key, entries]] = result
      assert length(entries) == 1
    end

    test "XREADGROUP BLOCK before COUNT works" do
      store = MockStore.make()
      key = ustream()

      for i <- 1..5, do: Stream.handle("XADD", [key, "#{i}-0", "f", "#{i}"], store)
      Stream.handle("XGROUP", ["CREATE", key, "grp", "0"], store)

      result =
        Stream.handle(
          "XREADGROUP",
          ["GROUP", "grp", "c1", "BLOCK", "0", "COUNT", "3", "STREAMS", key, ">"],
          store
        )

      assert is_list(result)
      [[^key, entries]] = result
      assert length(entries) == 3
    end
  end
end
