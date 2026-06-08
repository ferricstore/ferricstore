defmodule Ferricstore.Flow.LMDBWriter.ConfigTest do
  use ExUnit.Case, async: true
  @moduletag :flow

  alias Ferricstore.Flow.LMDBWriter.Config

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
    assert Config.default_flush_on_max_ops(:lagged) == false
    assert Config.default_flush_chunk_ops() == 5_000
    assert Config.default_flush_chunk_pause_ms() == 1
  end

  test "initial_state builds writer startup state" do
    ref = :atomics.new(2, signed: false)
    state = Config.initial_state([instance_ctx: %{name: :ctx}], :ctx, 2, "/tmp/ferric-config", ref)

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
  end
end
