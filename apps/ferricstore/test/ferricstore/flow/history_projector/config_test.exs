defmodule Ferricstore.Flow.HistoryProjector.ConfigTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.HistoryProjector.Config

  test "default config values match projector defaults" do
    assert Config.default_batch_size() == 25_000
    assert Config.default_flush_interval_ms() == 1_000
    assert Config.default_chunk_interval_ms() == 1
    assert Config.default_max_pending_entries() == 100_000
  end

  test "initial_state builds startup state" do
    counter = :atomics.new(1, signed: true)

    state =
      Config.initial_state(
        :projector,
        3,
        "/tmp/history-shard",
        %{name: :ctx},
        counter,
        123,
        456
      )

    assert state.projector_name == :projector
    assert state.shard_index == 3
    assert state.shard_data_path == "/tmp/history-shard"
    assert state.instance_ctx == %{name: :ctx}
    assert state.pending_counter == counter
    assert state.pending == []
    assert state.pending_count == 0
    assert state.flush_timer == nil
    assert state.requested_index == nil
    assert state.flushed_index == 456
    assert state.max_pending_entries == 123
  end
end
