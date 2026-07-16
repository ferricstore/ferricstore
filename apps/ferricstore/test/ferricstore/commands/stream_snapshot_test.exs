defmodule Ferricstore.Commands.StreamSnapshotTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Stream.{Groups, Waiters}

  @groups_table Ferricstore.Stream.Groups
  @waiters_table :ferricstore_stream_waiters

  setup do
    _ = Groups.snapshot(0)
    _ = Waiters.snapshot(0)
    :ets.delete_all_objects(@groups_table)
    :ets.delete_all_objects(@waiters_table)
    :ok
  end

  test "waiter snapshot and count avoid whole-table result lists" do
    now = System.monotonic_time(:microsecond)

    for index <- 1..3 do
      :ets.insert(@waiters_table, {"busy", self(), "#{index}-0", now - index})
    end

    for index <- 1..2 do
      :ets.insert(@waiters_table, {"medium", self(), "#{index}-0", now - index})
    end

    :ets.insert(@waiters_table, {"idle", self(), "1-0", now})

    assert 3 == Waiters.count("busy")
    assert [%{key: "busy", waiters: 3}, %{key: "medium", waiters: 2}] = Waiters.snapshot(2)
    assert [] == Waiters.snapshot(0)

    source =
      File.read!(Path.expand("../../../lib/ferricstore/commands/stream/waiters.ex", __DIR__))

    refute source =~ ":ets.tab2list"
    refute source =~ ":ets.match(@stream_waiters_table"
    refute source =~ "Enum.sort_by"
  end

  test "group snapshot keeps only the requested deterministic top rows" do
    :ets.insert(@groups_table, {{"stream-c", "group-c"}, "0-0", %{"c" => %{}}, %{1 => %{}}})

    :ets.insert(
      @groups_table,
      {{"stream-b", "group-b"}, "0-0", %{"b" => %{}}, %{1 => %{}, 2 => %{}}}
    )

    :ets.insert(
      @groups_table,
      {{"stream-a", "group-a"}, "0-0", %{"a" => %{}, "b" => %{}}, %{1 => %{}, 2 => %{}}}
    )

    assert [
             %{key: "stream-a", group: "group-a", pending: 2, consumers: 2},
             %{key: "stream-b", group: "group-b", pending: 2, consumers: 1}
           ] = Groups.snapshot(2)

    assert [] == Groups.snapshot(0)

    source =
      File.read!(Path.expand("../../../lib/ferricstore/commands/stream/groups.ex", __DIR__))

    refute source =~ ":ets.tab2list"
    refute source =~ "Enum.sort_by"
  end

  test "snapshots skip malformed cache keys without discarding healthy rows" do
    now = System.monotonic_time(:microsecond)

    :ets.insert(@waiters_table, {{:malformed}, self(), "1-0", now})
    :ets.insert(@waiters_table, {"healthy-waiter", self(), "1-0", now})

    :ets.insert(
      @groups_table,
      {{{:malformed}, "bad-group"}, "0-0", %{}, %{}}
    )

    :ets.insert(
      @groups_table,
      {{"healthy-stream", "healthy-group"}, "0-0", %{}, %{}}
    )

    assert [%{key: "healthy-waiter", waiters: 1}] = Waiters.snapshot(10)

    assert [%{key: "healthy-stream", group: "healthy-group"}] = Groups.snapshot(10)
  end
end
