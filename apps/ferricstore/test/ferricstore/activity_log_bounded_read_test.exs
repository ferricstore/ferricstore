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
end
