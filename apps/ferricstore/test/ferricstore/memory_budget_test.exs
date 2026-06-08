defmodule Ferricstore.MemoryBudgetTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryBudget

  @gib 1024 * 1024 * 1024
  @mib 1024 * 1024

  describe "adaptive_limits/1" do
    test "sizes WARaft ETS as a bounded per-shard RAM budget" do
      limits =
        MemoryBudget.adaptive_limits(%{
          memory_limit_bytes: 32 * @gib,
          disk_free_bytes: 500 * @gib,
          schedulers_online: 16,
          shard_count: 16
        })

      assert limits.waraft_segment_log_max_ets_bytes == 16 * @mib
      assert limits.waraft_segment_log_min_ets_entries == 512
      assert limits.waraft_segment_log_max_ets_entries == 16_384
      assert limits.waraft_apply_projection_cache_max_entries == 83_886
    end

    test "lets large machines absorb a one-million Flow terminal burst without immediate spill" do
      limits =
        MemoryBudget.adaptive_limits(%{
          memory_limit_bytes: 100 * @gib,
          disk_free_bytes: 500 * @gib,
          schedulers_online: 16,
          shard_count: 16
        })

      assert limits.waraft_apply_projection_cache_max_entries >= 250_000
    end

    test "keeps small machines bounded but still leaves a useful disk-backed tail" do
      limits =
        MemoryBudget.adaptive_limits(%{
          memory_limit_bytes: 1 * @gib,
          disk_free_bytes: 200 * @gib,
          schedulers_online: 2,
          shard_count: 8
        })

      assert limits.waraft_segment_log_max_ets_bytes == 8 * @mib
      assert limits.waraft_segment_log_min_ets_entries == 128
      assert limits.waraft_segment_log_max_ets_entries == 8_192
      assert limits.waraft_apply_projection_cache_max_entries == 5_242
    end

    test "uses disk pressure to reduce async projection queues before memory grows" do
      normal =
        MemoryBudget.adaptive_limits(%{
          memory_limit_bytes: 16 * @gib,
          disk_free_bytes: 200 * @gib,
          schedulers_online: 8,
          shard_count: 8
        })

      low_disk =
        MemoryBudget.adaptive_limits(%{
          memory_limit_bytes: 16 * @gib,
          disk_free_bytes: 4 * @gib,
          schedulers_online: 8,
          shard_count: 8
        })

      assert low_disk.flow_history_projector_max_pending_entries <
               normal.flow_history_projector_max_pending_entries

      assert low_disk.flow_lmdb_writer_max_mailbox_messages <
               normal.flow_lmdb_writer_max_mailbox_messages

      assert low_disk.flow_lmdb_writer_max_enqueue_ops <
               normal.flow_lmdb_writer_max_enqueue_ops

      assert low_disk.waraft_apply_projection_cache_max_entries <
               normal.waraft_apply_projection_cache_max_entries
    end
  end

  describe "limit/2" do
    test "explicit app env overrides adaptive defaults" do
      old_value = Application.get_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops)
      Application.put_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops, 12_345)

      try do
        assert MemoryBudget.limit(:flow_lmdb_writer_max_enqueue_ops, 1) == 12_345
      after
        restore_env(:flow_lmdb_writer_max_enqueue_ops, old_value)
      end
    end

    test "false/off/infinity overrides disable a cap intentionally" do
      old_value = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes)
      Application.put_env(:ferricstore, :waraft_segment_log_max_ets_bytes, "off")

      try do
        assert MemoryBudget.limit(:waraft_segment_log_max_ets_bytes, 1) == :infinity
      after
        restore_env(:waraft_segment_log_max_ets_bytes, old_value)
      end
    end

    test "unknown limits fall back to the caller default" do
      assert MemoryBudget.limit(:not_a_real_budget_key, 99) == 99
    end

    test "invalid explicit values do not accidentally disable a cap" do
      old_value = Application.get_env(:ferricstore, :flow_lmdb_writer_max_mailbox_messages)
      Application.put_env(:ferricstore, :flow_lmdb_writer_max_mailbox_messages, "not-an-int")

      try do
        assert is_integer(MemoryBudget.limit(:flow_lmdb_writer_max_mailbox_messages, 1))
      after
        restore_env(:flow_lmdb_writer_max_mailbox_messages, old_value)
      end
    end

    test "adaptive defaults are cached until reset so hot paths do not rescan hardware" do
      old_memory = Application.get_env(:ferricstore, :max_memory_bytes)
      old_shard_count = Application.get_env(:ferricstore, :shard_count)
      old_limit = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes)

      try do
        Application.put_env(:ferricstore, :max_memory_bytes, 1 * @gib)
        Application.put_env(:ferricstore, :shard_count, 8)
        Application.delete_env(:ferricstore, :waraft_segment_log_max_ets_bytes)
        MemoryBudget.reset_cache()

        first = MemoryBudget.limit(:waraft_segment_log_max_ets_bytes, 1)

        Application.put_env(:ferricstore, :max_memory_bytes, 32 * @gib)
        Application.put_env(:ferricstore, :shard_count, 1)

        assert MemoryBudget.limit(:waraft_segment_log_max_ets_bytes, 1) == first

        MemoryBudget.reset_cache()

        assert MemoryBudget.limit(:waraft_segment_log_max_ets_bytes, 1) != first
      after
        restore_env(:max_memory_bytes, old_memory)
        restore_env(:shard_count, old_shard_count)
        restore_env(:waraft_segment_log_max_ets_bytes, old_limit)
        MemoryBudget.reset_cache()
      end
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
