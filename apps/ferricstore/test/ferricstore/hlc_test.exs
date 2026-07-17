defmodule Ferricstore.HLCTest do
  use ExUnit.Case, async: false

  alias Ferricstore.HLC

  # ---------------------------------------------------------------------------
  # Setup
  #
  # The application supervision tree starts Ferricstore.HLC automatically.
  # Since the atomics-based HLC uses a global :persistent_term, we cannot
  # start isolated per-test instances. Instead, we reset the atomics state
  # before each test to avoid cross-test pollution.
  # ---------------------------------------------------------------------------

  setup do
    # Reset the packed atomics slot to 0 so each test starts from a clean
    # slate. Physical and logical are packed into a single 64-bit value.
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    :atomics.put(ref, 1, 0)
    :ok
  end

  # ---------------------------------------------------------------------------
  # now/0 -- basic monotonicity (lock-free path)
  # ---------------------------------------------------------------------------

  describe "now/0" do
    test "does not expose server-argument compatibility overloads" do
      refute function_exported?(HLC, :now, 1)
      refute function_exported?(HLC, :now_ms, 1)
      refute function_exported?(HLC, :update, 2)
      refute function_exported?(HLC, :drift_ms, 1)
      refute function_exported?(HLC, :drift_exceeded?, 1)
    end

    test "returns a 2-tuple {physical_ms, logical}" do
      {physical, logical} = HLC.now()

      assert is_integer(physical)
      assert is_integer(logical)
      assert physical > 0
      assert logical >= 0
    end

    test "returns monotonically increasing values across sequential calls" do
      ts1 = HLC.now()
      ts2 = HLC.now()
      ts3 = HLC.now()

      # Monotonicity: each subsequent timestamp must be strictly greater.
      assert HLC.compare(ts2, ts1) == :gt
      assert HLC.compare(ts3, ts2) == :gt
    end

    test "rapid calls in the same millisecond increment the logical counter" do
      # Call many times rapidly -- at least some will land in the same ms.
      timestamps = for _ <- 1..100, do: HLC.now()

      # All timestamps must be unique.
      assert length(Enum.uniq(timestamps)) == 100

      # Check that when physical ms is repeated, logical counter increments.
      grouped = Enum.group_by(timestamps, fn {phys, _log} -> phys end)

      Enum.each(grouped, fn {_ms, entries} ->
        logicals = Enum.map(entries, fn {_phys, log} -> log end)
        # Logical values within the same ms must be strictly ascending.
        assert logicals == Enum.sort(logicals)
        assert logicals == Enum.uniq(logicals)
      end)
    end

    test "physical component is close to wall clock" do
      wall_before = System.os_time(:millisecond)
      {physical, _logical} = HLC.now()
      wall_after = System.os_time(:millisecond)

      assert physical >= wall_before
      assert physical <= wall_after + 1
    end

    test "saturates instead of wrapping past the maximum packed timestamp" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      max_physical = Bitwise.bsl(1, 48) - 1
      max_logical = Bitwise.bsl(1, 16) - 1
      max_packed = Bitwise.bsl(1, 64) - 1
      :atomics.put(ref, 1, max_packed)

      assert HLC.now() == {max_physical, max_logical}
      assert :atomics.get(ref, 1) == max_packed
    end

    test "concurrent near-limit calls cannot race through the saturation point" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      max_timestamp = {Bitwise.bsl(1, 48) - 1, Bitwise.bsl(1, 16) - 1}
      max_packed = Bitwise.bsl(1, 64) - 1
      :atomics.put(ref, 1, max_packed - 1)

      results =
        1..50
        |> Enum.map(fn _ -> Task.async(&HLC.now/0) end)
        |> Task.await_many()

      assert Enum.all?(results, &(&1 == max_timestamp))
      assert :atomics.get(ref, 1) == max_packed
    end
  end

  describe "supervised restart" do
    test "preserves the last published timestamp when the HLC child crashes" do
      old_pid = Process.whereis(HLC)
      ref = :persistent_term.get(:ferricstore_hlc_ref)

      on_exit(fn ->
        case :persistent_term.get(:ferricstore_hlc_ref, nil) do
          nil -> :ok
          current_ref -> :atomics.put(current_ref, 1, 0)
        end
      end)

      future_ms = System.os_time(:millisecond) + 60_000
      packed = Bitwise.bsl(future_ms, 16) + 42
      :atomics.put(ref, 1, packed)

      Process.exit(old_pid, :kill)
      new_pid = wait_for_hlc_restart(old_pid, 100)

      assert is_pid(new_pid)
      assert :atomics.get(:persistent_term.get(:ferricstore_hlc_ref), 1) >= packed
    end
  end

  # ---------------------------------------------------------------------------
  # now_ms/0 -- monotonic millisecond convenience (lock-free)
  # ---------------------------------------------------------------------------

  describe "now_ms/0" do
    test "returns a single integer (physical component)" do
      ms = HLC.now_ms()
      assert is_integer(ms)
      assert ms > 0
    end

    test "returns monotonically increasing values" do
      ms1 = HLC.now_ms()
      ms2 = HLC.now_ms()

      # In rapid succession the physical ms may be the same, but it should
      # never go backward.
      assert ms2 >= ms1
    end

    test "called 100K times rapidly completes without timeout" do
      start = System.monotonic_time(:millisecond)

      for _ <- 1..100_000, do: HLC.now_ms()

      elapsed = System.monotonic_time(:millisecond) - start
      # Should complete in well under 500ms on modern hardware.
      # Give generous margin for CI machines.
      assert elapsed < 500, "100K now_ms() calls took #{elapsed}ms, expected < 500ms"
    end
  end

  # ---------------------------------------------------------------------------
  # update/1 -- merging remote timestamps
  # ---------------------------------------------------------------------------

  describe "update/1" do
    test "does not call the HLC GenServer on the hot update path" do
      source = File.read!("lib/ferricstore/hlc.ex")

      [update_source] =
        Regex.run(~r/@spec update\(timestamp\(\)\).*?^  end/ms, source)

      assert update_source =~ "hlc_update_packed(remote_ts)"
      refute update_source =~ "GenServer.call"
    end

    test "ignores remote timestamps that do not fit the packed clock" do
      assert :ok = HLC.update({Bitwise.bsl(1, 48), 0})

      {physical, logical} = HLC.now()
      assert physical < Bitwise.bsl(1, 48)
      assert logical < Bitwise.bsl(1, 16)
    end

    test "ignores logical counters that exceed the remaining packed range" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      :atomics.put(ref, 1, 0)

      assert :ok = HLC.update({0, Bitwise.bsl(1, 64)})
      assert :atomics.get(ref, 1) == 0
    end

    test "normalizes remote logical overflow into the physical component" do
      remote_physical = System.os_time(:millisecond) + 50
      remote_timestamp = {remote_physical, Bitwise.bsl(1, 16)}

      assert :ok = HLC.update(remote_timestamp)

      timestamp = HLC.now()
      assert HLC.compare(timestamp, remote_timestamp) == :gt
      assert elem(timestamp, 0) > remote_physical
      assert elem(timestamp, 1) < Bitwise.bsl(1, 16)
    end

    test "preserves causal ordering when normalized remote time ties the wall clock" do
      assert_normalized_remote_wall_tie(20)
    end

    test "with a future remote timestamp advances the local clock" do
      # Get current time.
      {local_phys, _} = HLC.now()

      # Simulate a remote timestamp 1000ms in the future.
      remote_ts = {local_phys + 1_000, 5}
      :ok = HLC.update(remote_ts)

      # After update, now() should be at or beyond the remote timestamp.
      {new_phys, new_log} = HLC.now()
      assert new_phys >= local_phys + 1_000

      # The logical counter should be correct relative to the merge.
      assert new_log >= 0
    end

    test "with a past remote timestamp does not go backward" do
      ts_before = HLC.now()

      # Remote timestamp is way in the past.
      remote_ts = {1_000_000, 0}
      :ok = HLC.update(remote_ts)

      ts_after = HLC.now()
      assert HLC.compare(ts_after, ts_before) == :gt
    end

    test "with equal physical time picks the higher logical counter" do
      {local_phys, _} = HLC.now()

      # Remote has the same physical time but a high logical counter.
      remote_ts = {local_phys, 999}
      :ok = HLC.update(remote_ts)

      {new_phys, new_log} = HLC.now()

      # We should be at least at local_phys with logical > 999.
      assert new_phys >= local_phys

      if new_phys == local_phys do
        assert new_log > 999
      end
    end
  end

  defp assert_normalized_remote_wall_tie(0) do
    flunk("wall clock advanced during every normalized remote timestamp attempt")
  end

  defp assert_normalized_remote_wall_tie(attempts) do
    ref = :persistent_term.get(:ferricstore_hlc_ref)
    :atomics.put(ref, 1, 0)

    wall = System.os_time(:millisecond)
    assert :ok = HLC.update({wall - 1, Bitwise.bsl(1, 16)})
    wall_after = System.os_time(:millisecond)

    if wall_after == wall do
      packed = :atomics.get(ref, 1)
      assert Bitwise.bsr(packed, 16) == wall
      assert Bitwise.band(packed, Bitwise.bsl(1, 16) - 1) == 1
    else
      assert_normalized_remote_wall_tie(attempts - 1)
    end
  end

  # ---------------------------------------------------------------------------
  # drift_ms/0 -- wall clock drift detection (lock-free)
  # ---------------------------------------------------------------------------

  describe "drift_ms/0" do
    test "returns near-zero drift under normal operation" do
      _ts = HLC.now()
      drift = HLC.drift_ms()

      # Under normal conditions, drift should be < 10ms.
      assert drift < 10
    end

    test "returns positive drift after updating with a far-future timestamp" do
      future_ms = System.os_time(:millisecond) + 2_000
      :ok = HLC.update({future_ms, 0})

      drift = HLC.drift_ms()
      # Drift should be roughly 2000ms (minus some wall-clock advancement).
      assert drift >= 1_900
      assert drift <= 2_100
    end

    test "does not reject an idle HLC that is behind the advancing wall clock" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      stale_physical = System.os_time(:millisecond) - 2_000
      :atomics.put(ref, 1, Bitwise.bsl(stale_physical, 16))

      assert HLC.drift_ms() == 0
      refute HLC.drift_exceeded?()
    end
  end

  # ---------------------------------------------------------------------------
  # drift_exceeded?/0
  # ---------------------------------------------------------------------------

  describe "drift_exceeded?/0" do
    test "returns false under normal operation" do
      _ts = HLC.now()
      refute HLC.drift_exceeded?()
    end

    test "returns true when drift exceeds 1000ms" do
      future_ms = System.os_time(:millisecond) + 1_500
      :ok = HLC.update({future_ms, 0})
      assert HLC.drift_exceeded?()
    end
  end

  # ---------------------------------------------------------------------------
  # compare/2 -- timestamp ordering
  # ---------------------------------------------------------------------------

  describe "compare/2" do
    test "compares by physical time first" do
      assert HLC.compare({100, 0}, {200, 0}) == :lt
      assert HLC.compare({200, 0}, {100, 0}) == :gt
    end

    test "compares by logical time when physical is equal" do
      assert HLC.compare({100, 1}, {100, 2}) == :lt
      assert HLC.compare({100, 2}, {100, 1}) == :gt
    end

    test "returns :eq for identical timestamps" do
      assert HLC.compare({100, 5}, {100, 5}) == :eq
    end
  end

  # ---------------------------------------------------------------------------
  # encode_ms/1 -- millisecond component for stream IDs
  # ---------------------------------------------------------------------------

  describe "encode_ms/1" do
    test "returns the physical component of an HLC timestamp" do
      assert HLC.encode_ms({1_234_567_890, 42}) == 1_234_567_890
    end
  end

  # ---------------------------------------------------------------------------
  # Telemetry -- drift warning events
  # ---------------------------------------------------------------------------

  describe "telemetry drift warning" do
    test "emits [:ferricstore, :hlc, :drift_warning] when drift exceeds 500ms" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :hlc, :drift_warning]
        ])

      # Push the HLC 600ms into the future via update/1. The update call
      # itself checks drift and emits telemetry.
      future_ms = System.os_time(:millisecond) + 600
      :ok = HLC.update({future_ms, 0})

      assert_receive {[:ferricstore, :hlc, :drift_warning], ^ref, %{drift_ms: drift}, _meta}
      assert drift >= 500
    end

    test "does not emit warning when drift is within threshold" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:ferricstore, :hlc, :drift_warning]
        ])

      _ts = HLC.now()

      refute_receive {[:ferricstore, :hlc, :drift_warning], ^ref, _, _}, 50
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent access -- the primary motivation for the atomics refactor
  # ---------------------------------------------------------------------------

  describe "concurrent access" do
    test "50 tasks calling now() simultaneously all return valid timestamps" do
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..1_000 do
              HLC.now()
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)
      all_timestamps = List.flatten(results)

      # All timestamps must be valid 2-tuples with positive physical ms.
      Enum.each(all_timestamps, fn {phys, logical} ->
        assert is_integer(phys) and phys > 0
        assert is_integer(logical) and logical >= 0
      end)

      # Total: 50 * 1000 = 50_000 timestamps.
      assert length(all_timestamps) == 50_000
    end

    @tag :bench
    test "now_ms() throughput exceeds 1M calls/sec (no GenServer bottleneck)" do
      # Warm up.
      for _ <- 1..1_000, do: HLC.now_ms()

      iterations = 1_000_000
      start = System.monotonic_time(:microsecond)

      for _ <- 1..iterations, do: HLC.now_ms()

      elapsed_us = System.monotonic_time(:microsecond) - start
      elapsed_sec = elapsed_us / 1_000_000
      throughput = iterations / elapsed_sec

      # On modern hardware this should easily exceed 1M/sec since there is
      # no message passing. Give generous margin for CI.
      assert throughput > 1_000_000,
             "Expected >1M now_ms()/sec, got #{Float.round(throughput / 1_000_000, 2)}M/sec " <>
               "(#{elapsed_us}us for #{iterations} calls)"
    end

    test "concurrent now() calls produce unique timestamps within each caller" do
      # Each task gets its own stream of timestamps; within a single task
      # (sequential calls) all timestamps must be strictly monotonic.
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            timestamps = for _ <- 1..100, do: HLC.now()

            # Within a single task, timestamps must be monotonically increasing.
            timestamps
            |> Enum.chunk_every(2, 1, :discard)
            |> Enum.each(fn [a, b] ->
              assert HLC.compare(b, a) == :gt,
                     "Expected #{inspect(b)} > #{inspect(a)}"
            end)

            timestamps
          end)
        end

      Task.await_many(tasks, 10_000)
    end
  end

  # ---------------------------------------------------------------------------
  # Graceful fallback when GenServer / persistent_term is not available
  # ---------------------------------------------------------------------------

  describe "graceful fallback" do
    test "now() returns wall clock when persistent_term is not set" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      on_exit(fn -> :persistent_term.put(:ferricstore_hlc_ref, ref) end)
      :persistent_term.erase(:ferricstore_hlc_ref)

      wall_before = System.os_time(:millisecond)
      {phys, logical} = HLC.now()
      wall_after = System.os_time(:millisecond)

      assert phys >= wall_before
      assert phys <= wall_after + 1
      assert logical == 0
    end

    test "now_ms() returns wall clock when persistent_term is not set" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      on_exit(fn -> :persistent_term.put(:ferricstore_hlc_ref, ref) end)
      :persistent_term.erase(:ferricstore_hlc_ref)

      wall_before = System.os_time(:millisecond)
      ms = HLC.now_ms()
      wall_after = System.os_time(:millisecond)

      assert ms >= wall_before
      assert ms <= wall_after + 1
    end

    test "drift_ms() returns 0 when persistent_term is not set" do
      ref = :persistent_term.get(:ferricstore_hlc_ref)
      on_exit(fn -> :persistent_term.put(:ferricstore_hlc_ref, ref) end)
      :persistent_term.erase(:ferricstore_hlc_ref)

      assert HLC.drift_ms() == 0
    end
  end

  defp wait_for_hlc_restart(_old_pid, 0), do: nil

  defp wait_for_hlc_restart(old_pid, attempts) do
    case Process.whereis(HLC) do
      pid when is_pid(pid) and pid != old_pid ->
        pid

      _ ->
        Process.sleep(10)
        wait_for_hlc_restart(old_pid, attempts - 1)
    end
  end
end
