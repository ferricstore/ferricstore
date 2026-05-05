defmodule FerricstoreServer.Connection.TrackingTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Config
  alias Ferricstore.PubSub
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection.Tracking

  setup do
    Config.set("notify-keyspace-events", "")
    ClientTracking.init_tables()
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)

    on_exit(fn ->
      Config.set("notify-keyspace-events", "")
      ClientTracking.cleanup(self())
    end)

    :ok
  end

  test "XREAD tracks stream keys instead of option tokens" do
    stream_a = "tracking:xread:a"
    stream_b = "tracking:xread:b"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %{tracking: tracking}

    new_state =
      Tracking.maybe_track_read(
        "XREAD",
        ["COUNT", "2", "STREAMS", stream_a, stream_b, "0-0", "$"],
        [[stream_a, []]],
        state
      )

    assert new_state.tracking == tracking
    assert :ets.lookup(:ferricstore_tracking, stream_a) == [{stream_a, self()}]
    assert :ets.lookup(:ferricstore_tracking, stream_b) == [{stream_b, self()}]
    assert :ets.lookup(:ferricstore_tracking, "COUNT") == []
    assert :ets.lookup(:ferricstore_tracking, "STREAMS") == []
  end

  test "XREAD tracking accepts lowercase streams marker" do
    stream = "tracking:xread:lower"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    Tracking.maybe_track_read("XREAD", ["streams", stream, "$"], [[stream, []]], %{
      tracking: tracking
    })

    assert :ets.lookup(:ferricstore_tracking, stream) == [{stream, self()}]
    assert :ets.lookup(:ferricstore_tracking, "streams") == []
  end

  test "malformed XREAD tracking does not record option names as keys" do
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    Tracking.maybe_track_read("XREAD", ["COUNT", "2"], :ok, %{tracking: tracking})

    assert :ets.lookup(:ferricstore_tracking, "COUNT") == []
    assert :ets.lookup(:ferricstore_tracking, "2") == []
  end

  test "XREADGROUP tracks stream keys instead of group option tokens" do
    stream = "tracking:xreadgroup:a"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    Tracking.maybe_track_read(
      "XREADGROUP",
      ["GROUP", "g1", "c1", "COUNT", "1", "STREAMS", stream, ">"],
      [[stream, []]],
      %{tracking: tracking}
    )

    assert :ets.lookup(:ferricstore_tracking, stream) == [{stream, self()}]
    assert :ets.lookup(:ferricstore_tracking, "GROUP") == []
    assert :ets.lookup(:ferricstore_tracking, "g1") == []
    assert :ets.lookup(:ferricstore_tracking, "STREAMS") == []
  end

  test "XINFO tracks the stream key instead of the subcommand token" do
    stream = "tracking:xinfo:a"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    Tracking.maybe_track_read("XINFO", ["STREAM", stream], %{}, %{tracking: tracking})

    assert :ets.lookup(:ferricstore_tracking, stream) == [{stream, self()}]
    assert :ets.lookup(:ferricstore_tracking, "STREAM") == []
  end

  test "OBJECT tracks the key instead of the subcommand token" do
    key = "tracking:object:a"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    Tracking.maybe_track_read("OBJECT", ["ENCODING", key], "raw", %{tracking: tracking})

    assert :ets.lookup(:ferricstore_tracking, key) == [{key, self()}]
    assert :ets.lookup(:ferricstore_tracking, "ENCODING") == []
  end

  test "BITOP invalidates the destination key instead of the operation token" do
    destination = "tracking:bitop:dst"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), destination, tracking)

    Tracking.maybe_notify_tracking("BITOP", ["AND", destination, "a", "b"], 1, %{
      tracking: tracking
    })

    assert_receive {:tracking_invalidation, _payload, [^destination]}
    assert :ets.lookup(:ferricstore_tracking, destination) == []
    assert :ets.lookup(:ferricstore_tracking, "AND") == []
  end

  test "SMOVE invalidates both source and destination sets" do
    source = "tracking:smove:src"
    destination = "tracking:smove:dst"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), source, tracking)
    ClientTracking.track_key(self(), destination, tracking)

    Tracking.maybe_notify_tracking("SMOVE", [source, destination, "member"], 1, %{
      tracking: tracking
    })

    assert_receive {:tracking_invalidation, _payload, [^source]}
    assert_receive {:tracking_invalidation, _payload, [^destination]}
    assert :ets.lookup(:ferricstore_tracking, source) == []
    assert :ets.lookup(:ferricstore_tracking, destination) == []
  end

  test "COPY tracking invalidates destination only when mutation succeeds" do
    source = "tracking:copy:src"
    destination = "tracking:copy:dst"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), destination, tracking)

    Tracking.maybe_notify_tracking("COPY", [source, destination], 0, %{tracking: tracking})

    refute_received {:tracking_invalidation, _, _}
    assert :ets.lookup(:ferricstore_tracking, destination) == [{destination, self()}]

    Tracking.maybe_notify_tracking("COPY", [source, destination], 1, %{tracking: tracking})

    assert_receive {:tracking_invalidation, _payload, [^destination]}
    assert :ets.lookup(:ferricstore_tracking, destination) == []
    assert :ets.lookup(:ferricstore_tracking, source) == []
  end

  test "MSETNX tracking invalidates all keys only when mutation succeeds" do
    key_a = "tracking:msetnx:a"
    key_b = "tracking:msetnx:b"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), key_a, tracking)
    ClientTracking.track_key(self(), key_b, tracking)

    Tracking.maybe_notify_tracking("MSETNX", [key_a, "1", key_b, "2"], 0, %{
      tracking: tracking
    })

    refute_received {:tracking_invalidation, _, _}
    assert :ets.lookup(:ferricstore_tracking, key_a) == [{key_a, self()}]
    assert :ets.lookup(:ferricstore_tracking, key_b) == [{key_b, self()}]

    Tracking.maybe_notify_tracking("MSETNX", [key_a, "1", key_b, "2"], 1, %{
      tracking: tracking
    })

    assert_receive {:tracking_invalidation, _payload, [^key_a]}
    assert_receive {:tracking_invalidation, _payload, [^key_b]}
    assert :ets.lookup(:ferricstore_tracking, key_a) == []
    assert :ets.lookup(:ferricstore_tracking, key_b) == []
  end

  test "RENAMENX tracking invalidates source and destination only when rename succeeds" do
    source = "tracking:renamenx:src"
    destination = "tracking:renamenx:dst"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])
    ClientTracking.track_key(self(), source, tracking)
    ClientTracking.track_key(self(), destination, tracking)

    Tracking.maybe_notify_tracking("RENAMENX", [source, destination], 0, %{tracking: tracking})

    refute_received {:tracking_invalidation, _, _}
    assert :ets.lookup(:ferricstore_tracking, source) == [{source, self()}]
    assert :ets.lookup(:ferricstore_tracking, destination) == [{destination, self()}]

    Tracking.maybe_notify_tracking("RENAMENX", [source, destination], 1, %{tracking: tracking})

    assert_receive {:tracking_invalidation, _payload, [^source]}
    assert_receive {:tracking_invalidation, _payload, [^destination]}
    assert :ets.lookup(:ferricstore_tracking, source) == []
    assert :ets.lookup(:ferricstore_tracking, destination) == []
  end

  test "zero-result no-op writes do not invalidate tracked keys" do
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    commands = [
      {"SETNX", ["tracking:noop:setnx", "v"], "tracking:noop:setnx"},
      {"EXPIRE", ["tracking:noop:expire", "10"], "tracking:noop:expire"},
      {"PEXPIRE", ["tracking:noop:pexpire", "10"], "tracking:noop:pexpire"},
      {"EXPIREAT", ["tracking:noop:expireat", "10"], "tracking:noop:expireat"},
      {"PEXPIREAT", ["tracking:noop:pexpireat", "10"], "tracking:noop:pexpireat"},
      {"PERSIST", ["tracking:noop:persist"], "tracking:noop:persist"},
      {"DEL", ["tracking:noop:del"], "tracking:noop:del"},
      {"UNLINK", ["tracking:noop:unlink"], "tracking:noop:unlink"},
      {"HSETNX", ["tracking:noop:hsetnx", "field", "v"], "tracking:noop:hsetnx"},
      {"HDEL", ["tracking:noop:hdel", "field"], "tracking:noop:hdel"},
      {"SADD", ["tracking:noop:sadd", "member"], "tracking:noop:sadd"},
      {"SREM", ["tracking:noop:srem", "member"], "tracking:noop:srem"},
      {"ZREM", ["tracking:noop:zrem", "member"], "tracking:noop:zrem"},
      {"LREM", ["tracking:noop:lrem", "1", "member"], "tracking:noop:lrem"},
      {"PFADD", ["tracking:noop:pfadd", "member"], "tracking:noop:pfadd"},
      {"XDEL", ["tracking:noop:xdel", "1-0"], "tracking:noop:xdel"},
      {"XTRIM", ["tracking:noop:xtrim", "MAXLEN", "10"], "tracking:noop:xtrim"}
    ]

    Enum.each(commands, fn {_cmd, _args, key} ->
      ClientTracking.track_key(self(), key, tracking)
    end)

    Enum.each(commands, fn {cmd, args, key} ->
      Tracking.maybe_notify_tracking(cmd, args, 0, %{tracking: tracking})

      refute_received {:tracking_invalidation, _, _}
      assert :ets.lookup(:ferricstore_tracking, key) == [{key, self()}]
    end)
  end

  test "COPY keyspace notification fires for destination only on mutation" do
    source = "tracking:keyspace:copy:src"
    destination = "tracking:keyspace:copy:dst"
    Config.set("notify-keyspace-events", "KEg")
    PubSub.subscribe("__keyspace@0__:#{source}", self())
    PubSub.subscribe("__keyspace@0__:#{destination}", self())
    PubSub.subscribe("__keyevent@0__:copy", self())

    Tracking.maybe_notify_keyspace("COPY", [source, destination], 1)

    refute_received {:pubsub_message, "__keyspace@0__:" <> ^source, "copy"}
    assert_received {:pubsub_message, "__keyspace@0__:" <> ^destination, "copy"}
    assert_received {:pubsub_message, "__keyevent@0__:copy", ^destination}
  end

  test "COPY keyspace notification is silent when destination was not modified" do
    source = "tracking:keyspace:copy_noop:src"
    destination = "tracking:keyspace:copy_noop:dst"
    Config.set("notify-keyspace-events", "KEg")
    PubSub.subscribe("__keyspace@0__:#{source}", self())
    PubSub.subscribe("__keyspace@0__:#{destination}", self())
    PubSub.subscribe("__keyevent@0__:copy", self())

    Tracking.maybe_notify_keyspace("COPY", [source, destination], 0)

    refute_received {:pubsub_message, _, _}
  end

  test "MSETNX keyspace notification fires for all keys only when mutation succeeds" do
    key_a = "tracking:keyspace:msetnx:a"
    key_b = "tracking:keyspace:msetnx:b"
    Config.set("notify-keyspace-events", "KE$")
    PubSub.subscribe("__keyspace@0__:#{key_a}", self())
    PubSub.subscribe("__keyspace@0__:#{key_b}", self())
    PubSub.subscribe("__keyevent@0__:mset", self())

    Tracking.maybe_notify_keyspace("MSETNX", [key_a, "1", key_b, "2"], 0)

    refute_received {:pubsub_message, _, _}

    Tracking.maybe_notify_keyspace("MSETNX", [key_a, "1", key_b, "2"], 1)

    assert_received {:pubsub_message, "__keyspace@0__:" <> ^key_a, "mset"}
    assert_received {:pubsub_message, "__keyspace@0__:" <> ^key_b, "mset"}
    assert_received {:pubsub_message, "__keyevent@0__:mset", ^key_a}
    assert_received {:pubsub_message, "__keyevent@0__:mset", ^key_b}
  end

  test "RENAME keyspace notification fires for source and destination" do
    source = "tracking:keyspace:rename:src"
    destination = "tracking:keyspace:rename:dst"
    Config.set("notify-keyspace-events", "KEg")
    PubSub.subscribe("__keyspace@0__:#{source}", self())
    PubSub.subscribe("__keyspace@0__:#{destination}", self())
    PubSub.subscribe("__keyevent@0__:rename", self())

    Tracking.maybe_notify_keyspace("RENAME", [source, destination], :ok)

    assert_received {:pubsub_message, "__keyspace@0__:" <> ^source, "rename"}
    assert_received {:pubsub_message, "__keyspace@0__:" <> ^destination, "rename"}
    assert_received {:pubsub_message, "__keyevent@0__:rename", ^source}
    assert_received {:pubsub_message, "__keyevent@0__:rename", ^destination}
  end

  test "RENAMENX keyspace notification fires only when rename succeeds" do
    source = "tracking:keyspace:renamenx:src"
    destination = "tracking:keyspace:renamenx:dst"
    Config.set("notify-keyspace-events", "KEg")
    PubSub.subscribe("__keyspace@0__:#{source}", self())
    PubSub.subscribe("__keyspace@0__:#{destination}", self())
    PubSub.subscribe("__keyevent@0__:rename", self())

    Tracking.maybe_notify_keyspace("RENAMENX", [source, destination], 0)

    refute_received {:pubsub_message, _, _}

    Tracking.maybe_notify_keyspace("RENAMENX", [source, destination], 1)

    assert_received {:pubsub_message, "__keyspace@0__:" <> ^source, "rename"}
    assert_received {:pubsub_message, "__keyspace@0__:" <> ^destination, "rename"}
    assert_received {:pubsub_message, "__keyevent@0__:rename", ^source}
    assert_received {:pubsub_message, "__keyevent@0__:rename", ^destination}
  end

  test "zero-result no-op writes do not fire keyspace notifications" do
    Config.set("notify-keyspace-events", "KEA")

    commands = [
      {"SETNX", ["tracking:keyspace:noop:setnx", "v"]},
      {"EXPIRE", ["tracking:keyspace:noop:expire", "10"]},
      {"PEXPIRE", ["tracking:keyspace:noop:pexpire", "10"]},
      {"EXPIREAT", ["tracking:keyspace:noop:expireat", "10"]},
      {"PEXPIREAT", ["tracking:keyspace:noop:pexpireat", "10"]},
      {"PERSIST", ["tracking:keyspace:noop:persist"]},
      {"SADD", ["tracking:keyspace:noop:sadd", "member"]},
      {"SREM", ["tracking:keyspace:noop:srem", "member"]},
      {"HDEL", ["tracking:keyspace:noop:hdel", "field"]},
      {"ZREM", ["tracking:keyspace:noop:zrem", "member"]},
      {"LREM", ["tracking:keyspace:noop:lrem", "1", "member"]},
      {"PFADD", ["tracking:keyspace:noop:pfadd", "member"]}
    ]

    Enum.each(commands, fn {cmd, args} ->
      Tracking.maybe_notify_keyspace(cmd, args, 0)
      refute_received {:pubsub_message, _, _}
    end)
  end
end
