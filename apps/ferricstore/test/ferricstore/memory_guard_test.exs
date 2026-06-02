defmodule Ferricstore.MemoryGuardTest do
  @moduledoc """
  Tests for the `Ferricstore.MemoryGuard` GenServer.

  Covers periodic check execution, telemetry event emission, memory stats
  accuracy, and pressure threshold detection.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.MemoryGuard

  setup do
    on_exit(fn ->
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # MemoryGuard starts and runs periodic checks
  # ---------------------------------------------------------------------------

  describe "MemoryGuard starts and runs periodic checks" do
    test "MemoryGuard is alive after application start" do
      pid = Process.whereis(MemoryGuard)
      assert pid != nil
      assert Process.alive?(pid)
    end

    test "stats/0 returns a valid stats map" do
      stats = MemoryGuard.stats()

      assert is_integer(stats.total_bytes)
      assert stats.total_bytes >= 0
      assert is_integer(stats.max_bytes)
      assert stats.max_bytes > 0
      assert is_float(stats.ratio)
      assert stats.ratio >= 0.0
      assert stats.pressure_level in [:ok, :warning, :pressure, :reject]
      assert is_map(stats.shards)
      assert map_size(stats.shards) == 4
      assert stats.eviction_policy in [:volatile_lru, :allkeys_lru, :volatile_ttl, :noeviction]
    end

    test "stats handles zero configured shards without probing negative shard ids" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1_073_741_824,
          shard_count: 0
        )

      stats = GenServer.call(pid, :stats)

      assert stats.shards == %{}

      GenServer.stop(pid)
    end

    test "eviction_policy/0 returns configured policy" do
      policy = MemoryGuard.eviction_policy()
      assert policy == :volatile_lru
    end

    test "reject_writes?/0 returns false under normal conditions" do
      assert MemoryGuard.reject_writes?() == false
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry events emitted at pressure thresholds
  # ---------------------------------------------------------------------------

  describe "telemetry events emitted at pressure thresholds" do
    test "check event is emitted on periodic check" do
      test_pid = self()
      handler_id = "test-memory-check-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn event, measurements, metadata, config ->
          send(config.test_pid, {:telemetry_event, event, measurements, metadata})
        end,
        %{test_pid: test_pid}
      )

      # Start a dedicated MemoryGuard with a very short interval to trigger quickly
      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 50,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4
        )

      assert_receive {:telemetry_event, [:ferricstore, :memory, :check], measurements, metadata},
                     1000

      assert is_integer(measurements.total_bytes)
      assert metadata.pressure_level in [:ok, :warning, :pressure, :reject]

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end

    test "pressure event is emitted when threshold is crossed" do
      test_pid = self()
      handler_id = "test-memory-pressure-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :pressure],
        fn event, measurements, metadata, config ->
          send(config.test_pid, {:pressure_event, event, measurements, metadata})
        end,
        %{test_pid: test_pid}
      )

      # Start a MemoryGuard with a very small max_memory to force pressure
      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 50,
          # Set max memory very low to trigger pressure
          max_memory_bytes: 1,
          shard_count: 4
        )

      # With max_memory_bytes=1, any ETS data should trigger pressure.
      # Match specifically on per-shard events (contain pressure_level in metadata).
      assert_receive {:pressure_event, [:ferricstore, :memory, :pressure], measurements,
                      %{pressure_level: _}},
                     2000

      assert is_integer(measurements.shard_index)
      assert is_integer(measurements.bytes)
      assert is_float(measurements.ratio)

      GenServer.stop(pid)
      :telemetry.detach(handler_id)

      # Reset global persistent_term keys contaminated by the dedicated instance.
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
    end
  end

  # ---------------------------------------------------------------------------
  # Memory stats accuracy
  # ---------------------------------------------------------------------------

  describe "memory stats accurate" do
    test "per-shard stats reflect actual ETS memory" do
      stats = MemoryGuard.stats()

      # Each shard should have non-negative bytes
      Enum.each(stats.shards, fn {index, shard_stat} ->
        assert is_integer(index)
        assert is_integer(shard_stat.bytes)
        assert shard_stat.bytes >= 0
        assert is_float(shard_stat.ratio)
        assert shard_stat.ratio >= 0.0
      end)
    end

    test "total_bytes is sum of all shard bytes" do
      stats = MemoryGuard.stats()

      expected_total =
        stats.shards
        |> Map.values()
        |> Enum.map(& &1.bytes)
        |> Enum.sum()

      assert stats.total_bytes == expected_total
    end

    test "ratio is total_bytes / max_bytes" do
      stats = MemoryGuard.stats()

      expected_ratio =
        if stats.max_bytes > 0 do
          stats.total_bytes / stats.max_bytes
        else
          0.0
        end

      assert_in_delta stats.ratio, expected_ratio, 0.001
    end

    test "adding data to ETS increases reported memory" do
      stats_before = MemoryGuard.stats()

      # Write some data to the single keydir table (new 4-element format)
      keydir = :keydir_0

      large_entries =
        for i <- 1..100 do
          key = "memguard_test_#{i}"
          value = :binary.copy("X", 1000)
          {key, value, 0}
        end

      Enum.each(large_entries, fn {key, value, expire} ->
        :ets.insert(keydir, {key, value, expire, 5, 0, 0, 0})
      end)

      stats_after = MemoryGuard.stats()

      assert stats_after.total_bytes > stats_before.total_bytes

      # Clean up
      Enum.each(large_entries, fn {key, _, _} ->
        :ets.delete(keydir, key)
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Pressure level classification
  # ---------------------------------------------------------------------------

  describe "pressure level classification" do
    test "normal memory reports :ok" do
      # With default 1GB max, our test data should be well under 70%
      stats = MemoryGuard.stats()
      assert stats.pressure_level == :ok
    end

    test "dedicated MemoryGuard with tiny budget reports :reject" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 1,
          shard_count: 4
        )

      stats = GenServer.call(pid, :stats)
      assert stats.pressure_level == :reject

      GenServer.stop(pid)
    end

    test "max_memory_bytes 0 disables RSS pressure for unlimited deployments" do
      key = {FerricStore.Instance, :default}
      previous = :persistent_term.get(key, :__missing__)

      ref = :atomics.new(4, signed: true)

      :persistent_term.put(key, %{
        keydir_binary_bytes: ref,
        process_rss_fn: fn -> 10_000 end
      })

      on_exit(fn ->
        case previous do
          :__missing__ -> :persistent_term.erase(key)
          value -> :persistent_term.put(key, value)
        end
      end)

      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 60_000,
          max_memory_bytes: 0,
          shard_count: 4
        )

      :sys.replace_state(pid, fn state -> %{state | memory_limit: 1} end)

      stats = GenServer.call(pid, :stats)

      assert stats.rss_bytes == 10_000
      assert stats.rss_ratio == 0.0
      assert stats.rss_pressure_level == :ok
      assert stats.pressure_level == :ok

      GenServer.stop(pid)
    end

    test "reject_writes? is true when policy is noeviction and level is reject" do
      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 50,
          max_memory_bytes: 1,
          shard_count: 4,
          eviction_policy: :noeviction
        )

      # Trigger a check
      send(pid, :check)
      Process.sleep(100)

      # The check should have set last_pressure_level to :reject
      result = GenServer.call(pid, :reject_writes?)
      assert result == true

      GenServer.stop(pid)

      # Reset the global persistent_term that the dedicated instance contaminated.
      # The perform_check/1 in MemoryGuard writes to global :persistent_term keys
      # regardless of which instance runs it, so we must restore them.
      MemoryGuard.set_reject_writes(false)
      MemoryGuard.set_keydir_full(false)
    end
  end

  # ---------------------------------------------------------------------------
  # Configuration
  # ---------------------------------------------------------------------------

  describe "configuration" do
    test "parses macOS sysctl host memory output" do
      assert MemoryGuard.parse_sysctl_memsize("137438953472\n") == 137_438_953_472
      assert MemoryGuard.parse_sysctl_memsize("not-a-number\n") == nil
    end

    test "parses ps rss output as bytes for macOS and Unix fallback" do
      assert MemoryGuard.parse_ps_rss_kb("  85388976\n") == 85_388_976 * 1024
      assert MemoryGuard.parse_ps_rss_kb("not-a-number\n") == nil
    end

    test "custom interval is respected" do
      test_pid = self()
      handler_id = "test-custom-interval-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :memory, :check],
        fn _event, _measurements, _metadata, config ->
          send(config.test_pid, :check_fired)
        end,
        %{test_pid: test_pid}
      )

      {:ok, pid} =
        GenServer.start_link(MemoryGuard,
          interval_ms: 50,
          max_memory_bytes: 1_073_741_824,
          shard_count: 4
        )

      # Should fire at least twice in 200ms with 50ms interval
      assert_receive :check_fired, 500
      assert_receive :check_fired, 500

      GenServer.stop(pid)
      :telemetry.detach(handler_id)
    end
  end
end
