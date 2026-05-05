defmodule FerricstoreServer.Connection.TrackingTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection.Tracking

  setup do
    ClientTracking.init_tables()
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)

    on_exit(fn ->
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
end
