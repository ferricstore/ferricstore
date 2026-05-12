defmodule Ferricstore.Raft.PerfTogglesTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.PerfToggles

  setup do
    keys = [
      :raft_pipeline_priority,
      :raft_direct_batch_commands,
      :raft_compact_hot_batches,
      :raft_put_batch_apply_fast_path,
      :raft_delete_batch_apply_fast_path
    ]

    previous = Map.new(keys, &{&1, Application.fetch_env(:ferricstore, &1)})

    on_exit(fn ->
      Enum.each(previous, fn
        {key, {:ok, value}} -> Application.put_env(:ferricstore, key, value)
        {key, :error} -> Application.delete_env(:ferricstore, key)
      end)
    end)
  end

  test "defaults keep the current optimized path enabled" do
    Application.delete_env(:ferricstore, :raft_pipeline_priority)
    Application.delete_env(:ferricstore, :raft_direct_batch_commands)
    Application.delete_env(:ferricstore, :raft_compact_hot_batches)
    Application.delete_env(:ferricstore, :raft_put_batch_apply_fast_path)
    Application.delete_env(:ferricstore, :raft_delete_batch_apply_fast_path)

    assert PerfToggles.pipeline_priority() == :low
    assert PerfToggles.direct_batch_commands?()
    assert PerfToggles.compact_hot_batches?()
    assert PerfToggles.put_batch_apply_fast_path?()
    assert PerfToggles.delete_batch_apply_fast_path?()
  end

  test "pipeline priority can be forced to normal for regression isolation" do
    Application.put_env(:ferricstore, :raft_pipeline_priority, "normal")

    assert PerfToggles.pipeline_priority() == :normal
  end

  test "direct batch commands can be disabled for regression isolation" do
    Application.put_env(:ferricstore, :raft_direct_batch_commands, "false")

    refute PerfToggles.direct_batch_commands?()
  end

  test "hot batch compaction can be disabled for regression isolation" do
    Application.put_env(:ferricstore, :raft_compact_hot_batches, false)

    refute PerfToggles.compact_hot_batches?()
  end

  test "put_batch apply fast path can be disabled for regression isolation" do
    Application.put_env(:ferricstore, :raft_put_batch_apply_fast_path, "0")

    refute PerfToggles.put_batch_apply_fast_path?()
  end

  test "delete_batch apply fast path can be disabled for regression isolation" do
    Application.put_env(:ferricstore, :raft_delete_batch_apply_fast_path, "0")

    refute PerfToggles.delete_batch_apply_fast_path?()
  end

  test "invalid values fail closed instead of silently changing the hot path" do
    Application.put_env(:ferricstore, :raft_pipeline_priority, "urgent")

    assert_raise ArgumentError, ~r/raft_pipeline_priority/, fn ->
      PerfToggles.pipeline_priority()
    end

    Application.put_env(:ferricstore, :raft_direct_batch_commands, "maybe")

    assert_raise ArgumentError, ~r/raft_direct_batch_commands/, fn ->
      PerfToggles.direct_batch_commands?()
    end
  end
end
