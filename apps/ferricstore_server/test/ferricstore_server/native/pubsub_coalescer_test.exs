defmodule FerricstoreServer.Native.PubSubCoalescerTest do
  use ExUnit.Case, async: true

  alias FerricstoreServer.Native.Connection.PubSubCoalescer

  test "collects ready PubSub events in mailbox order up to the event limit" do
    first = {:pubsub_message, "channel", "one"}
    second = {:pubsub_message, "channel", "two"}
    third = {:pubsub_pmessage, "channel:*", "channel", "three"}

    send(self(), second)
    send(self(), third)
    send(self(), :after_pubsub_burst)

    assert PubSubCoalescer.collect(first, 2, 1_024) == [first, second]
    assert_receive ^third
    assert_receive :after_pubsub_burst
  end

  test "stops collecting once the estimated byte bound is reached" do
    first = {:pubsub_message, "channel", "one"}
    second = {:pubsub_message, "channel", "two"}

    send(self(), second)

    assert PubSubCoalescer.collect(first, 64, PubSubCoalescer.event_bytes(first)) == [first]
    assert_receive ^second
  end
end
