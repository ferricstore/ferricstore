defmodule Ferricstore.Bench.FlowStateLMDBSoakGuardTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  @moduletag :bench

  test "production soak reports projection, blob protection, and release cursor health" do
    source =
      File.read!(Path.expand("../../../../../bench/flow_state_lmdb_soak.exs", __DIR__))

    assert source =~ "history_pending="
    assert source =~ "history_oldest_lag_ms="
    assert source =~ "history_projection_lag="
    assert source =~ "history_flush_failures="
    assert source =~ "history_queue_full="
    assert source =~ "blob_hardened="
    assert source =~ "blob_hardened_oldest_ms="
    assert source =~ "release_cursor_gap="
  end
end
