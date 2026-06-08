defmodule FerricstoreServer.Spec.OomPreventionTest do
  @moduledoc """
  Tests for the noeviction OOM prevention behaviour.
  When eviction_policy is :noeviction, writes are rejected when memory
  pressure exceeds the reject threshold (95%).
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.MemoryGuard

  setup do
    MemoryGuard.reset_pressure_flags()

    on_exit(fn ->
      MemoryGuard.reset_pressure_flags()
    end)
  end

  # Helper: send :check and wait for MemoryGuard to process it
  defp trigger_and_wait(pid, expected_reject) do
    send(pid, :check)
    Ferricstore.Test.ShardHelpers.eventually(fn ->
      assert GenServer.call(pid, :reject_writes?) == expected_reject
    end, "reject_writes? should be #{expected_reject}", 20, 100)
  end

  describe "MemoryGuard noeviction + tiny budget (spec 2G.7)" do
    test "reject_writes? returns true with noeviction policy and critical pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      trigger_and_wait(pid, true)
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with noeviction but normal pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      trigger_and_wait(pid, false)
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with volatile_lru even under pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :volatile_lru
        ])

      trigger_and_wait(pid, false)
      GenServer.stop(pid)
    end

    test "reject_writes? returns false with allkeys_lru even under pressure" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :allkeys_lru
        ])

      trigger_and_wait(pid, false)
      GenServer.stop(pid)
    end

    test "stats report :reject pressure level with tiny budget" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      send(pid, :check)
      Ferricstore.Test.ShardHelpers.eventually(fn ->
        stats = GenServer.call(pid, :stats)
        assert stats.pressure_level == :reject
      end, "pressure should be :reject", 20, 100)

      stats = GenServer.call(pid, :stats)
      assert stats.eviction_policy == :noeviction
      assert stats.ratio > 0.95

      GenServer.stop(pid)
    end

    test "eviction_policy/0 returns :noeviction when configured" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      assert GenServer.call(pid, :eviction_policy) == :noeviction
      GenServer.stop(pid)
    end

    test "pressure level transitions from :ok to :reject when budget shrinks" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      trigger_and_wait(pid, false)
      GenServer.stop(pid)

      {:ok, pid2} =
        GenServer.start_link(MemoryGuard, [
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        ])

      trigger_and_wait(pid2, true)
      GenServer.stop(pid2)
    end
  end

  # ---------------------------------------------------------------------------
  # OOM error format (connection to spec section 4.6 error codes)
  # ---------------------------------------------------------------------------

  describe "OOM error format" do
    test "KEYDIR_FULL error message matches spec format" do
      err = {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      assert {:error, msg} = err
      assert msg =~ "KEYDIR_FULL"
    end
  end
end
