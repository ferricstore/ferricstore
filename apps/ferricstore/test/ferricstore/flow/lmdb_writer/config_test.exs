defmodule Ferricstore.Flow.LMDBWriter.ConfigTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter.Config

  @config_keys [
    :flow_lmdb_flush_interval_ms,
    :flow_lmdb_flush_jitter_ms,
    :flow_lmdb_flush_quiet_ms,
    :flow_lmdb_flush_max_lag_ms,
    :flow_lmdb_max_batch_ops,
    :flow_lmdb_flush_on_max_ops,
    :flow_lmdb_flush_chunk_ops,
    :flow_lmdb_flush_chunk_pause_ms
  ]

  setup do
    previous = Map.new(@config_keys, &{&1, Application.get_env(:ferricstore, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:ferricstore, key)
        {key, value} -> Application.put_env(:ferricstore, key, value)
      end)
    end)
  end

  test "instance name prefers explicit opts over context" do
    assert Config.instance_name_from_opts(instance_name: :custom, instance_ctx: %{name: :ctx}) ==
             :custom
  end

  test "instance name falls back to context then default" do
    assert Config.instance_name_from_opts(instance_ctx: %{name: :ctx}) == :ctx
    assert Config.instance_name_from_opts([]) == :default
    assert Config.instance_name_from_ctx(%{name: :ctx}) == :ctx
    assert Config.instance_name_from_ctx(%{}) == :default
  end

  test "default writer config matches lagged defaults" do
    assert Config.default_flush_interval_ms() == 500
    assert Config.default_flush_jitter_ms() == 250
    assert Config.default_flush_quiet_ms() == 250
    assert Config.default_flush_max_lag_ms() == 30_000
    assert Config.default_max_ops() == 25_000
    assert Config.default_flush_on_max_ops(:lagged)
    assert Config.default_flush_chunk_ops() == 5_000
    assert Config.default_flush_chunk_pause_ms() == 1
  end

  test "initial_state builds writer startup state" do
    ref = :atomics.new(2, signed: false)

    state =
      Config.initial_state([instance_ctx: %{name: :ctx}], :ctx, 2, "/tmp/ferric-config", ref)

    assert state.instance_name == :ctx
    assert state.shard_index == 2
    assert state.data_dir == "/tmp/ferric-config"
    assert state.shard_data_path == "/tmp/ferric-config/data/shard_2"
    assert state.path == "/tmp/ferric-config/data/shard_2/flow_lmdb"
    assert state.instance_ctx == %{name: :ctx}
    assert state.enqueue_seq == ref
    assert state.pending == []
    assert state.pending_after_flush == []
    assert state.flush_waiters == []
    assert state.flush_chunk_ops == 5_000
    refute state.terminal_atomic_write?
    refute Map.has_key?(state, :terminal_count_cache)
  end

  test "initial_state normalizes malformed runtime limits" do
    Application.put_env(:ferricstore, :flow_lmdb_flush_interval_ms, -1)
    Application.put_env(:ferricstore, :flow_lmdb_flush_jitter_ms, "wide")
    Application.put_env(:ferricstore, :flow_lmdb_flush_quiet_ms, nil)
    Application.put_env(:ferricstore, :flow_lmdb_flush_max_lag_ms, -1)
    Application.put_env(:ferricstore, :flow_lmdb_max_batch_ops, 0)
    Application.put_env(:ferricstore, :flow_lmdb_flush_on_max_ops, "true")
    Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_ops, 0)
    Application.put_env(:ferricstore, :flow_lmdb_flush_chunk_pause_ms, -1)

    state = Config.initial_state([], :default, 0, "/tmp/ferric-config", nil)

    assert state.flush_interval_ms == 500
    assert state.flush_jitter_ms == 250
    assert state.flush_quiet_ms == 250
    assert state.flush_max_lag_ms == 30_000
    assert state.max_ops == 25_000
    assert state.flush_on_max_ops?
    assert state.flush_chunk_ops == 5_000
    assert state.flush_chunk_pause_ms == 1
  end
end
