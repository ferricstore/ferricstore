defmodule Ferricstore.Store.ShardPendingReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
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

  test "cold read timeout removes location-stamped pending read and replies nil" do
    tag = make_ref()
    state = %{flush_in_flight: nil, pending_reads: %{321 => {{self(), tag}, "key", 0, 1, 2, 3}}}

    assert {:noreply, new_state} = Shard.handle_info({:cold_read_timeout, 321}, state)
    assert new_state.pending_reads == %{}
    assert_receive {^tag, nil}
  end

  test "cold read timeout ignores stale correlation ids" do
    state = %{flush_in_flight: nil, pending_reads: %{}}

    assert {:noreply, ^state} = Shard.handle_info({:cold_read_timeout, 789}, state)
    refute_received _
  end

  test "stale async cold read completion does not warm newer cold location" do
    keydir = :ets.new(:"pending_read_#{System.unique_integer([:positive])}", [:set, :public])
    key = "pending:stale-cold-location"
    tag = make_ref()

    state = %{
      flush_in_flight: nil,
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64},
      pending_reads: %{123 => {{self(), tag}, key}}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 8, 40, 3})

      assert {:noreply, new_state} = Shard.handle_info({:tokio_complete, 123, :ok, "old"}, state)
      assert new_state.pending_reads == %{}
      assert_receive {^tag, "old"}
      assert [{^key, nil, 0, _lfu, 8, 40, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end

  test "successful async cold read completion cancels its timeout timer" do
    keydir = :ets.new(:"pending_read_#{System.unique_integer([:positive])}", [:set, :public])
    key = "pending:cancel-success-timeout"
    tag = make_ref()
    timer_ref = Process.send_after(self(), :unexpected_timeout, 60_000)

    state = %{
      flush_in_flight: nil,
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64},
      pending_reads: %{125 => {:pending_read, {{self(), tag}, key, 0, 7, 12, 3}, timer_ref}}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      assert {:noreply, new_state} =
               Shard.handle_info({:tokio_complete, 125, :ok, "value"}, state)

      assert new_state.pending_reads == %{}
      assert_receive {^tag, "value"}
      assert Process.read_timer(timer_ref) == false
      refute_received :unexpected_timeout
    after
      Process.cancel_timer(timer_ref)
      :ets.delete(keydir)
    end
  end

  test "failed async cold read completion cancels its timeout timer" do
    key = "pending:cancel-error-timeout"
    tag = make_ref()
    timer_ref = Process.send_after(self(), :unexpected_timeout, 60_000)

    state = %{
      flush_in_flight: nil,
      pending_reads: %{126 => {:pending_read, {{self(), tag}, key}, timer_ref}}
    }

    try do
      assert {:noreply, new_state} =
               Shard.handle_info({:tokio_complete, 126, :error, :enoent}, state)

      assert new_state.pending_reads == %{}
      assert_receive {^tag, nil}
      assert Process.read_timer(timer_ref) == false
      refute_received :unexpected_timeout
    after
      Process.cancel_timer(timer_ref)
    end
  end

  test "location-stamped async cold read completion warms only matching location" do
    keydir = :ets.new(:"pending_read_#{System.unique_integer([:positive])}", [:set, :public])
    key = "pending:matching-cold-location"
    tag = make_ref()

    state = %{
      flush_in_flight: nil,
      keydir: keydir,
      instance_ctx: %{hot_cache_max_value_size: 64},
      pending_reads: %{124 => {{self(), tag}, key, 0, 7, 12, 3}}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      assert {:noreply, new_state} = Shard.handle_info({:tokio_complete, 124, :ok, "old"}, state)
      assert new_state.pending_reads == %{}
      assert_receive {^tag, "old"}
      assert [{^key, "old", 0, _lfu, 7, 12, 3}] = :ets.lookup(keydir, key)
    after
      :ets.delete(keydir)
    end
  end
end
