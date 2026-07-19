defmodule Ferricstore.Flow.FifoLaneTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{FifoLane, Keys}
  alias Ferricstore.Flow.NativeOrderedIndex
  alias Ferricstore.Store.Router

  test "member round-trips an exact 128-bit state-entry sequence and id" do
    sequence = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF
    member = FifoLane.member(sequence, "flow-1")

    assert FifoLane.decode_member(member) == {:ok, {sequence, "flow-1"}}
  end

  test "member byte order preserves sequence order beyond float precision" do
    lower = FifoLane.member(9_007_199_254_740_992, "z")
    higher = FifoLane.member(9_007_199_254_740_993, "a")

    assert lower < higher
  end

  test "native lane rank uses encoded sequence order beyond float precision" do
    native = NativeOrderedIndex.new()
    lane_key = FifoLane.lane_key("orders", "queued", "tenant-1")
    lower = FifoLane.member(9_007_199_254_740_992, "z")
    higher = FifoLane.member(9_007_199_254_740_993, "a")

    assert :ok =
             NativeOrderedIndex.put_entries(native, [
               {lane_key, higher, 0},
               {lane_key, lower, 0}
             ])

    assert [{^lower, score}] = NativeOrderedIndex.rank_range(native, lane_key, 0, 0, false)
    assert score == 0.0
  end

  test "decoder rejects truncation and an empty id" do
    assert :error = FifoLane.decode_member(<<>>)
    assert :error = FifoLane.decode_member(<<1::unsigned-big-128>>)
  end

  test "running records remain in the lane of their logical run state" do
    record = %{
      id: "flow-1",
      type: "orders",
      state: "running",
      run_state: "review",
      partition_key: "tenant-1",
      state_enter_seq: 42
    }

    assert {:ok, %{lane_key: lane_key, member: member}} = FifoLane.identity(record)
    assert lane_key == FifoLane.lane_key("orders", "review", "tenant-1")
    assert {:ok, {42, "flow-1"}} = FifoLane.decode_member(member)
  end

  test "running members sort ahead of queued members while preserving entry order within each class" do
    lane_key = FifoLane.lane_key("orders", "review", "tenant-1")
    native = NativeOrderedIndex.new()

    queued = %{
      id: "queued",
      type: "orders",
      state: "review",
      partition_key: "tenant-1",
      state_enter_seq: 1
    }

    running = %{
      id: "running",
      type: "orders",
      state: "running",
      run_state: "review",
      partition_key: "tenant-1",
      state_enter_seq: 2
    }

    assert {^lane_key, queued_member, 0} = FifoLane.index_entry(queued)
    assert {^lane_key, running_member, -1} = FifoLane.index_entry(running)
    assert :ok = NativeOrderedIndex.put_entries(native, [FifoLane.index_entry(queued)])
    assert :ok = NativeOrderedIndex.put_entries(native, [FifoLane.index_entry(running)])

    assert [{^running_member, running_score}, {^queued_member, queued_score}] =
             NativeOrderedIndex.rank_range(native, lane_key, 0, 1, false)

    assert running_score == -1.0
    assert queued_score == 0.0
  end

  test "native fifo head lookup resolves all lanes and due scores in one request" do
    native = NativeOrderedIndex.new()
    due_key = Keys.due_any_key("orders", 0, "tenant-1")
    second_due_key = Keys.due_key("orders", "second", 0, "tenant-1")
    first_lane = FifoLane.lane_key("orders", "first", "tenant-1")
    second_lane = FifoLane.lane_key("orders", "second", "tenant-1")
    first_member = FifoLane.member(1, "first-flow")
    second_member = FifoLane.member(2, "second-flow")

    assert :ok =
             NativeOrderedIndex.put_entries(native, [
               {first_lane, first_member, 0},
               {second_lane, second_member, -1},
               {due_key, "first-flow", 2_000},
               {due_key, "second-flow", 3_000},
               {second_due_key, "second-flow", 1_500}
             ])

    assert [
             {^first_lane, ^first_member, 2_000.0},
             {^second_lane, ^second_member, 3_000.0}
           ] = NativeOrderedIndex.fifo_lane_heads(native, due_key, [first_lane, second_lane])

    assert [
             {^due_key, ^first_lane, ^first_member, 2_000.0},
             {^second_due_key, ^second_lane, ^second_member, 1_500.0}
           ] =
             NativeOrderedIndex.fifo_lane_heads_many(native, [
               {due_key, first_lane},
               {second_due_key, second_lane}
             ])
  end

  test "terminal records do not have active lane entries" do
    assert nil ==
             FifoLane.index_entry(%{
               id: "flow-1",
               type: "orders",
               state: "completed",
               partition_key: "tenant-1",
               state_enter_seq: 42
             })
  end

  test "lane keys remain colocated with their partition" do
    lane_key = FifoLane.lane_key("orders", "queued", "tenant-1")
    state_tag = Router.extract_hash_tag(Keys.state_key("flow-1", "tenant-1"))

    assert Router.extract_hash_tag(lane_key) == state_tag
  end
end
