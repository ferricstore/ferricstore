defmodule Ferricstore.Raft.WARaftSegmentProjectionCommandTimeTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftStorage

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
end
