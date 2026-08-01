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

  test "pubsub activity materializes the public entry contract from compact rows" do
    PubSubActivityLog.record_subscription("SUBSCRIBE", :channel, ["alpha", "beta"])
    PubSubActivityLog.record_publish("alpha", 42, 3)

    [publish, subscription] = PubSubActivityLog.get(2)

    assert Map.drop(publish, [:id, :timestamp_us]) == %{
             command: "PUBLISH",
             target_type: :channel,
             target: "alpha",
             targets: 1,
             subscribers: 3,
             message_bytes: 42
           }

    assert Map.drop(subscription, [:id, :timestamp_us]) == %{
             command: "SUBSCRIBE",
             target_type: :channel,
             target: "alpha +1",
             targets: 2,
             subscribers: nil,
             message_bytes: nil
           }
  end

  test "pubsub publish sampling keeps periodic events and all subscription changes" do
    source = File.read!(Path.expand("../../lib/ferricstore/pubsub/activity_log.ex", __DIR__))

    assert source =~ ":pubsub_activity_log_sample_every"
    assert source =~ "configure_publish_sampling"

    on_exit(fn -> PubSubActivityLog.configure_publish_sampling(1) end)

    PubSubActivityLog.configure_publish_sampling(3)
    PubSubActivityLog.reset()

    for index <- 1..7 do
      PubSubActivityLog.record_publish("sampled:#{index}", index, 0)
    end

    PubSubActivityLog.record_subscription("SUBSCRIBE", :channel, ["always-recorded"])

    assert Enum.map(PubSubActivityLog.get(4), &{&1.command, &1.target}) == [
             {"SUBSCRIBE", "always-recorded"},
             {"PUBLISH", "sampled:7"},
             {"PUBLISH", "sampled:4"},
             {"PUBLISH", "sampled:1"}
           ]
  end

  test "batch PubSub activity preserves newest-first order, sizes, counts, and ids" do
    assert :ok =
             PubSubActivityLog.record_publish_batch(
               "orders",
               ["one", "four", "seventeen"],
               [1, 4, 17]
             )

    assert Enum.map(PubSubActivityLog.get(3), fn entry ->
             {entry.id, entry.target, entry.message_bytes, entry.subscribers}
           end) == [
             {2, "orders", 9, 17},
             {1, "orders", 4, 4},
             {0, "orders", 3, 1}
           ]
  end

  test "batch PubSub activity advances sampling exactly once per logical publish" do
    on_exit(fn -> PubSubActivityLog.configure_publish_sampling(1) end)

    PubSubActivityLog.configure_publish_sampling(3)
    PubSubActivityLog.reset()

    messages = Enum.map(1..7, &:binary.copy("x", &1))
    assert :ok = PubSubActivityLog.record_publish_batch("sampled", messages, List.duplicate(0, 7))

    assert Enum.map(PubSubActivityLog.get(3), &{&1.id, &1.message_bytes}) == [
             {2, 7},
             {1, 4},
             {0, 1}
           ]
  end

  test "oversized PubSub activity batches retain only the newest bounded rows" do
    messages = Enum.map(1..600, &:binary.copy("x", &1))

    assert :ok =
             PubSubActivityLog.record_publish_batch(
               "bounded",
               messages,
               Enum.to_list(1..600)
             )

    assert :ets.info(:ferricstore_pubsub_activity_log, :size) == 512
    assert %{id: 599, message_bytes: 600, subscribers: 600} = hd(PubSubActivityLog.get(1))

    assert %{id: 100, message_bytes: 101, subscribers: 101} =
             PubSubActivityLog.get(500) |> List.last()
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
