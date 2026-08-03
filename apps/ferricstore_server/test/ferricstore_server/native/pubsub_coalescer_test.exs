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

    assert PubSubCoalescer.collect(first, 2, 1_024) == {[first, second], nil}
    assert_receive ^third
    assert_receive :after_pubsub_burst
  end

  test "stops collecting once the estimated byte bound is reached" do
    first = {:pubsub_message, "channel", "one"}
    second = {:pubsub_message, "channel", "two"}

    send(self(), second)

    assert PubSubCoalescer.collect(first, 64, PubSubCoalescer.event_bytes(first)) ==
             {[first], nil}

    assert_receive ^second
  end

  test "stops at an earlier control message instead of scanning past it" do
    first = {:pubsub_message, "channel", "one"}
    control = {:acl_invalidate, "alice", 7}
    second = {:pubsub_message, "channel", "two"}

    send(self(), control)
    send(self(), second)

    assert PubSubCoalescer.collect(first, 64, 1_024) == {[first], control}
    assert_receive ^second
  end

  test "a non-positive byte cap disables coalescing without crashing" do
    first = {:pubsub_message, "channel", "one"}
    second = {:pubsub_message, "channel", "two"}

    send(self(), second)

    assert PubSubCoalescer.collect(first, 64, 0) == {[first], nil}
    assert_receive ^second
  end

  test "invalid coalescing limits disable coalescing without crashing" do
    first = {:pubsub_message, "orders", "one"}
    second = {:pubsub_message, "orders", "two"}

    send(self(), second)

    assert PubSubCoalescer.collect(first, :invalid, 1_024) == {[first], nil}
    assert_receive ^second

    send(self(), second)

    assert PubSubCoalescer.collect(first, 64, :invalid) == {[first], nil}
    assert_receive ^second
  end
end
