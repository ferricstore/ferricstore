defmodule Ferricstore.Store.ShardPendingReadTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.LFU
  alias Ferricstore.Store.Shard

  setup do
    handler_id = {:pending_read_telemetry, self(), make_ref()}
    parent = self()

    :telemetry.attach(
      handler_id,
      [:ferricstore, :bitcask, :pread_corrupt],
      &__MODULE__.handle_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok
  end

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
    shard_data_path = Path.join(System.tmp_dir!(), "pending_read_timeout")

    state = %{
      flush_in_flight: nil,
      shard_data_path: shard_data_path,
      pending_reads: %{321 => {{self(), tag}, "key", 0, 1, 2, 3}}
    }

    assert {:noreply, new_state} = Shard.handle_info({:cold_read_timeout, 321}, state)
    assert new_state.pending_reads == %{}
    assert_receive {^tag, nil}

    assert_receive {:pread_telemetry, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                    %{path: path, reason: :timeout, raw_reason: :timeout}}

    assert path == Path.join(shard_data_path, "00001.log")
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
    shard_data_path = Path.join(System.tmp_dir!(), "pending_read_error")

    state = %{
      flush_in_flight: nil,
      shard_data_path: shard_data_path,
      pending_reads: %{126 => {:pending_read, {{self(), tag}, key, 0, 7, 12, 3}, timer_ref}}
    }

    try do
      assert {:noreply, new_state} =
               Shard.handle_info({:tokio_complete, 126, :error, :enoent}, state)

      assert new_state.pending_reads == %{}
      assert_receive {^tag, nil}
      assert Process.read_timer(timer_ref) == false
      refute_received :unexpected_timeout

      assert_receive {:pread_telemetry, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                      %{path: path, reason: :missing_file, raw_reason: :enoent}}

      assert path == Path.join(shard_data_path, "00007.log")
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

  test "nil async cold read waits briefly for compaction ETS update" do
    keydir = :ets.new(:"pending_read_#{System.unique_integer([:positive])}", [:set, :public])
    key = "pending:compaction-delayed-update"
    tag = make_ref()

    state = %{
      flush_in_flight: nil,
      keydir: keydir,
      shard_data_path: Path.join(System.tmp_dir!(), "pending_read_retry"),
      instance_ctx: %{hot_cache_max_value_size: 64},
      pending_reads: %{127 => {{self(), tag}, key, 0, 7, 12, 3}}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      assert {:noreply, retry_state} = Shard.handle_info({:tokio_complete, 127, :ok, nil}, state)
      assert retry_state.pending_reads == %{}
      refute_receive {^tag, _}, 0

      :ets.insert(keydir, {key, "new", 0, LFU.initial(), 8, 24, 3})

      assert_receive {:cold_read_retry, pending_entry, attempts_left}, 100

      assert {:noreply, final_state} =
               Shard.handle_info({:cold_read_retry, pending_entry, attempts_left}, retry_state)

      assert final_state.pending_reads == %{}
      assert_receive {^tag, "new"}
    after
      :ets.delete(keydir)
    end
  end

  test "nil async cold read retry exhaustion emits telemetry" do
    attach_cold_retry_exhausted_handler()

    keydir = :ets.new(:"pending_read_#{System.unique_integer([:positive])}", [:set, :public])
    key = "pending:compaction-retry-exhausted"
    tag = make_ref()
    shard_data_path = Path.join(System.tmp_dir!(), "pending_read_retry_exhausted")

    state = %{
      index: 2,
      flush_in_flight: nil,
      keydir: keydir,
      shard_data_path: shard_data_path,
      instance_ctx: %{hot_cache_max_value_size: 64},
      pending_reads: %{}
    }

    try do
      :ets.insert(keydir, {key, nil, 0, LFU.initial(), 7, 12, 3})

      pending_entry = {{self(), tag}, key, 0, 7, 12, 3}

      assert {:noreply, ^state} = Shard.handle_info({:cold_read_retry, pending_entry, 0}, state)
      assert_receive {^tag, nil}

      path = Path.join(shard_data_path, "00007.log")

      assert_receive {:pread_telemetry, [:ferricstore, :bitcask, :pread_corrupt], %{count: 1},
                      %{
                        path: ^path,
                        reason: :corrupt_record,
                        raw_reason: :missing_live_cold_entry
                      }}

      assert_receive {:cold_retry_exhausted, [:ferricstore, :store, :cold_read_retry_exhausted],
                      %{count: 1, attempts: 8},
                      %{
                        source: :shard,
                        operation: :get,
                        shard_index: 2,
                        path: ^path,
                        reason: :missing_live_cold_entry
                      }}
    after
      :ets.delete(keydir)
    end
  end

  def handle_telemetry(event, measurements, metadata, parent) do
    send(parent, {:pread_telemetry, event, measurements, metadata})
  end

  defp attach_cold_retry_exhausted_handler do
    parent = self()
    handler_id = {:pending_read_retry_exhausted, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :store, :cold_read_retry_exhausted],
      &__MODULE__.handle_cold_retry_exhausted/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  def handle_cold_retry_exhausted(event, measurements, metadata, parent) do
    send(parent, {:cold_retry_exhausted, event, measurements, metadata})
  end
end
