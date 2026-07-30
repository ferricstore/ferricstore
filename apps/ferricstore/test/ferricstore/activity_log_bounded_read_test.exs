defmodule Ferricstore.ActivityLogBoundedReadTest do
  use ExUnit.Case, async: false

  alias Ferricstore.PubSub.ActivityLog, as: PubSubActivityLog
  alias Ferricstore.Stream.ActivityLog, as: StreamActivityLog

  setup do
    StreamActivityLog.reset()
    PubSubActivityLog.reset()
    :ok
  end

  test "ordered activity rings read only the requested newest rows" do
    for index <- 1..10 do
      StreamActivityLog.record_xadd("stream:#{index}", "#{index}-0", 1, nil, false)
      PubSubActivityLog.record_publish("channel:#{index}", index, index)
    end

    assert Enum.map(StreamActivityLog.get(3), & &1.key) == [
             "stream:10",
             "stream:9",
             "stream:8"
           ]

    assert Enum.map(PubSubActivityLog.get(3), & &1.target) == [
             "channel:10",
             "channel:9",
             "channel:8"
           ]

    for relative <- [
          "../../lib/ferricstore/stream/activity_log.ex",
          "../../lib/ferricstore/pubsub/activity_log.ex"
        ] do
      source = File.read!(Path.expand(relative, __DIR__))
      refute source =~ ":ets.tab2list"
      refute source =~ "Enum.sort_by"
    end
  end

  test "copies and bounds user-controlled activity labels" do
    oversized = :binary.copy("x", 4_096)
    StreamActivityLog.record_xadd(oversized, oversized, 1, nil, false)
    PubSubActivityLog.record_publish(oversized, 1, 0)

    [stream] = StreamActivityLog.get(1)
    [publish] = PubSubActivityLog.get(1)

    assert byte_size(stream.key) < 512
    assert byte_size(stream.entry_id) < 512
    assert byte_size(publish.target) < 512
    refute stream.key == oversized
    refute publish.target == oversized
  end

  test "batch XADD activity retains only the configured newest rows in order" do
    entries =
      Enum.map(1..600, fn index ->
        {"stream:#{index}", "#{index}-0", 1}
      end)

    assert :ok = StreamActivityLog.record_xadd_many(entries)
    assert StreamActivityLog.len() == 512

    assert Enum.map(StreamActivityLog.get(3), & &1.key) == [
             "stream:600",
             "stream:599",
             "stream:598"
           ]

    assert hd(StreamActivityLog.get(1)).id == 599

    replacement =
      Enum.map(601..1_200, fn index ->
        {"stream:#{index}", "#{index}-0", 1}
      end)

    assert :ok = StreamActivityLog.record_xadd_many(replacement)
    assert StreamActivityLog.len() == 512

    assert Enum.map(StreamActivityLog.get(3), & &1.key) == [
             "stream:1200",
             "stream:1199",
             "stream:1198"
           ]

    assert hd(StreamActivityLog.get(1)).id == 1_199
  end

  test "XADD results are paired and bounded without materializing discarded activity rows" do
    items =
      Enum.map(1..700, fn index ->
        {"stream:#{index}", ["field", Integer.to_string(index)]}
      end)

    results =
      Enum.map(1..700, fn index ->
        if rem(index, 100) == 0, do: {:error, "rejected"}, else: {:ok, "#{index}-0"}
      end)

    assert :ok = StreamActivityLog.record_xadd_results(results, items)
    assert StreamActivityLog.len() == 512

    assert Enum.map(StreamActivityLog.get(3), &{&1.key, &1.entry_id, &1.field_pairs}) == [
             {"stream:699", "699-0", 1},
             {"stream:698", "698-0", 1},
             {"stream:697", "697-0", 1}
           ]
  end

  test "batch XADD activity preserves trim and NOMKSTREAM metadata" do
    assert :ok =
             StreamActivityLog.record_xadd_many([
               {"stream:trimmed", "10-1", 2, {:maxlen, true, 100}, true}
             ])

    assert [entry] = StreamActivityLog.get(1)
    assert entry.key == "stream:trimmed"
    assert entry.entry_id == "10-1"
    assert entry.field_pairs == 2
    assert entry.trim == "MAXLEN ~ 100"
    assert entry.nomkstream == true
  end
end
