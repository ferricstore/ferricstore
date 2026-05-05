defmodule FerricstoreServer.Connection.ReadwriteTrackingTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection.Tracking

  setup do
    ClientTracking.init_tables()
    :ets.delete_all_objects(:ferricstore_tracking)
    :ets.delete_all_objects(:ferricstore_tracking_connections)
    :ok
  end

  test "read-write string commands do not track stale returned values" do
    getset_key = "tracking:readwrite:getset"
    getdel_key = "tracking:readwrite:getdel"
    getex_mutated_key = "tracking:readwrite:getex_mutated"
    getex_missing_key = "tracking:readwrite:getex_missing"
    getex_read_key = "tracking:readwrite:getex_read"
    {:ok, tracking} = ClientTracking.enable(self(), ClientTracking.new_config(), [])

    state = %{tracking: tracking}

    Tracking.maybe_track_read("GETSET", [getset_key, "new"], "old", state)
    Tracking.maybe_track_read("GETDEL", [getdel_key], "old", state)
    Tracking.maybe_track_read("GETEX", [getex_mutated_key, "EX", "10"], "value", state)
    Tracking.maybe_track_read("GETEX", [getex_missing_key, "EX", "10"], nil, state)
    Tracking.maybe_track_read("GETEX", [getex_read_key], "value", state)

    assert :ets.lookup(:ferricstore_tracking, getset_key) == []
    assert :ets.lookup(:ferricstore_tracking, getdel_key) == []
    assert :ets.lookup(:ferricstore_tracking, getex_mutated_key) == []
    assert :ets.lookup(:ferricstore_tracking, getex_missing_key) == [{getex_missing_key, self()}]
    assert :ets.lookup(:ferricstore_tracking, getex_read_key) == [{getex_read_key, self()}]
  end
end
