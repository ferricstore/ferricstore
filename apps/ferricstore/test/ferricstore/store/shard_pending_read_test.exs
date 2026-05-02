defmodule Ferricstore.Store.ShardPendingReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.Shard

  test "cold read timeout removes simple pending read and replies nil" do
    tag = make_ref()
    state = %{flush_in_flight: nil, pending_reads: %{123 => {{self(), tag}, "key"}}}

    assert {:noreply, new_state} = Shard.handle_info({:cold_read_timeout, 123}, state)
    assert new_state.pending_reads == %{}
    assert_receive {^tag, nil}
  end

  test "cold read timeout removes metadata pending read and replies nil" do
    tag = make_ref()
    state = %{flush_in_flight: nil, pending_reads: %{456 => {{self(), tag}, "key", :meta, 99}}}

    assert {:noreply, new_state} = Shard.handle_info({:cold_read_timeout, 456}, state)
    assert new_state.pending_reads == %{}
    assert_receive {^tag, nil}
  end

  test "cold read timeout ignores stale correlation ids" do
    state = %{flush_in_flight: nil, pending_reads: %{}}

    assert {:noreply, ^state} = Shard.handle_info({:cold_read_timeout, 789}, state)
    refute_received _
  end
end
