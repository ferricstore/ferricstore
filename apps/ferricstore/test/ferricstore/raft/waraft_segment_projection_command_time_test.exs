defmodule Ferricstore.Raft.WARaftSegmentProjectionCommandTimeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.CommandTime
  alias Ferricstore.Raft.WARaftStorage
  alias Ferricstore.Store.{LFU, Promotion}

  test "projection callback is never retried after it raises" do
    command =
      {{:put, "key", "value", 0}, %{hlc_ts: {1_000, 0}, wall_time_ms: 1_000}}

    Process.put(:projection_callback_count, 0)

    assert_raise RuntimeError, "projection failed", fn ->
      WARaftStorage.__with_segment_projection_command_time_for_test__(command, fn ->
        Process.put(
          :projection_callback_count,
          Process.get(:projection_callback_count, 0) + 1
        )

        raise "projection failed"
      end)
    end

    assert Process.get(:projection_callback_count) == 1
  after
    Process.delete(:projection_callback_count)
  end

  @tag :hlc_drift_guard
  test "projection preflight preserves wall-live promotion markers under unsafe stamped drift" do
    keydir = :ets.new(:waraft_projection_marker_time, [:set])
    redis_key = "promoted"
    marker_key = Promotion.marker_key(redis_key)
    :ets.insert(keydir, {marker_key, "hash", 31_000, LFU.initial(), 0, 0, 4})

    assert CommandTime.with_expiry_context(61_000, 1_000, fn ->
             WARaftStorage.__segment_live_promotion_marker_for_test__(
               %{ets: keydir},
               redis_key
             )
           end)
  end

  @tag :hlc_drift_guard
  test "projection preflight preserves wall-live fetch locks under unsafe stamped drift" do
    state = %{fetch_or_compute_locks: %{"key" => {"owner", 31_000}}}

    assert CommandTime.with_expiry_context(61_000, 1_000, fn ->
             WARaftStorage.__segment_project_check_fetch_or_compute_lock_for_test__(
               state,
               "key",
               nil
             )
           end) == {:error, :key_locked}
  end
end
