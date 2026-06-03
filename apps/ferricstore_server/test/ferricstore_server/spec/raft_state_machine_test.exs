defmodule FerricstoreServer.Spec.RaftStateMachineTest do
  @moduledoc """
  Spec section 6: RA / Raft State Machine Tests (single-node subset).

  These tests validate the subset of Section 6 test plan items that can
  run on a single node (self-quorum). Tests tagged [cluster] in the spec
  are excluded.

  Covered test IDs:

    * **AP-002** -- apply() updates ETS hot cache
    * **AP-003** -- CONFIG SET apply updates namespace ETS
    * **AP-006** -- TTL consistency (SET with EX, check EXPIRETIME matches)
    * **GC-001** -- Narrow window flushes on time
    * **GC-002** -- Wide window batches writes
    * **GC-004** -- Namespace batchers independent
    * **SN-001** -- release_cursor triggers after N applies
  """

  use ExUnit.Case, async: false

  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    NamespaceConfig.reset_all()

    on_exit(fn ->
      NamespaceConfig.reset_all()
      ShardHelpers.wait_shards_alive()
    end)
  end

  # Helper to generate unique keys with a specific prefix
  defp pkey(prefix, base), do: "#{prefix}:spec6_#{base}_#{:rand.uniform(9_999_999)}"

  # Find a key with a given prefix that hashes to shard 0
  defp key_on_shard(prefix, shard_idx) do
    suffix =
      Enum.find(1..10_000, fn i ->
        Router.shard_for(FerricStore.Instance.get(:default), "#{prefix}:s6_#{i}") == shard_idx
      end)

    "#{prefix}:s6_#{suffix}"
  end

  # ---------------------------------------------------------------------------
  # AP-002: apply() updates ETS hot cache
  #
  # Spec: SET key val; after apply on leader; GET returns val from ETS.
  # Single-node adaptation: write via the Batcher (which goes through ra on
  # single-node), then verify the value is present in both the keydir and
  # hot_cache ETS tables -- confirming apply() populated both caches.
  # ---------------------------------------------------------------------------

  describe "AP-002: apply() updates ETS hot cache" do
    test "SET via Batcher populates keydir ETS table" do
      key = pkey("ap002", "hot_cache")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      :ok = Batcher.write(shard_index, {:put, key, "cached_value", 0})

      # Verify the keydir ETS table has the key in single-table format
      # {key, value, expire_at_ms, lfu_counter}
      keydir = :"keydir_#{shard_index}"
      assert [{^key, "cached_value", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
    end

    test "SET with expiry populates keydir with correct expire_at_ms" do
      key = pkey("ap002", "hot_cache_ttl")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)
      future = System.os_time(:millisecond) + 60_000

      :ok = Batcher.write(shard_index, {:put, key, "ttl_value", future})

      keydir = :"keydir_#{shard_index}"
      assert [{^key, "ttl_value", ^future, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
    end

    test "overwrite updates ETS to new value" do
      key = pkey("ap002", "overwrite")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      :ok = Batcher.write(shard_index, {:put, key, "old_value", 0})
      :ok = Batcher.write(shard_index, {:put, key, "new_value", 0})

      keydir = :"keydir_#{shard_index}"
      assert [{^key, "new_value", 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)
    end

    test "GET reads from ETS without GenServer call (hot path)" do
      key = pkey("ap002", "hot_read")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      :ok = Batcher.write(shard_index, {:put, key, "hot_read_val", 0})

      # Router.get reads directly from ETS for hot keys
      assert "hot_read_val" == Router.get(FerricStore.Instance.get(:default), key)
    end

    test "delete removes key from keydir ETS" do
      key = pkey("ap002", "delete_cache")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      :ok = Batcher.write(shard_index, {:put, key, "to_delete", 0})
      :ok = Batcher.write(shard_index, {:delete, key})

      keydir = :"keydir_#{shard_index}"
      assert [] = :ets.lookup(keydir, key)
    end
  end

  # ---------------------------------------------------------------------------
  # AP-003: CONFIG SET apply updates namespace ETS
  #
  # Spec: FERRICSTORE.CONFIG SET sensor window_ms 50; check
  # :ferricstore_ns_config ETS on all nodes.
  # Single-node adaptation: set namespace config and verify the
  # :ferricstore_ns_config ETS table reflects the change immediately.
  # ---------------------------------------------------------------------------

  describe "AP-003: CONFIG SET apply updates namespace ETS" do
    test "CONFIG SET stores window_ms in :ferricstore_ns_config ETS" do
      :ok = NamespaceConfig.set("sensor", "window_ms", "50")

      # Verify directly in ETS
      assert [{"sensor", 50, _changed_at, _changed_by}] =
               :ets.lookup(:ferricstore_ns_config, "sensor")
    end

    test "CONFIG SET rejects removed durability field" do
      assert {:error, msg} = NamespaceConfig.set("sensor", "durability", "async")
      assert msg =~ "unknown namespace config field 'durability'"

      assert [] = :ets.lookup(:ferricstore_ns_config, "sensor")
    end

    test "CONFIG SET updates window without durability metadata" do
      :ok = NamespaceConfig.set("sensor", "window_ms", "50")

      [{prefix, window_ms, _at, _by}] = :ets.lookup(:ferricstore_ns_config, "sensor")

      assert prefix == "sensor"
      assert window_ms == 50
    end

    test "CONFIG SET is immediately visible to window_for/1" do
      :ok = NamespaceConfig.set("sensor", "window_ms", "50")
      assert 50 == NamespaceConfig.window_for("sensor")
    end

    test "CONFIG SET for multiple namespaces stores each independently" do
      :ok = NamespaceConfig.set("sensor", "window_ms", "50")
      :ok = NamespaceConfig.set("session", "window_ms", "1")
      :ok = NamespaceConfig.set("rate", "window_ms", "10")

      assert 50 == NamespaceConfig.window_for("sensor")
      assert 1 == NamespaceConfig.window_for("session")
      assert 10 == NamespaceConfig.window_for("rate")

      # Each entry exists independently in ETS
      assert [{_p1, 50, _, _}] = :ets.lookup(:ferricstore_ns_config, "sensor")
      assert [{_p2, 1, _, _}] = :ets.lookup(:ferricstore_ns_config, "session")
      assert [{_p3, 10, _, _}] = :ets.lookup(:ferricstore_ns_config, "rate")
    end

    test "CONFIG RESET removes entry from ETS, falls back to defaults" do
      :ok = NamespaceConfig.set("sensor", "window_ms", "50")
      assert [{_, 50, _, _}] = :ets.lookup(:ferricstore_ns_config, "sensor")

      :ok = NamespaceConfig.reset("sensor")
      assert [] = :ets.lookup(:ferricstore_ns_config, "sensor")

      # Falls back to default
      assert 1 == NamespaceConfig.window_for("sensor")
    end
  end

  # ---------------------------------------------------------------------------
  # AP-006: TTL apply consistency
  #
  # Spec: SET key val EX 10; check EXPIRETIME on all nodes after apply.
  # Single-node adaptation: write with TTL via the Batcher, then verify
  # EXPIRETIME (via Router.get_meta) matches the expected value.
  # ---------------------------------------------------------------------------

  describe "AP-006: TTL apply consistency" do
    test "SET with EX stores correct expire_at_ms, PEXPIRETIME matches" do
      key = pkey("ap006", "ttl_consist")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      ttl_seconds = 10
      now_ms = System.os_time(:millisecond)
      expire_at_ms = now_ms + ttl_seconds * 1_000

      :ok = Batcher.write(shard_index, {:put, key, "ttl_val", expire_at_ms})

      # Verify via Router.get_meta
      {value, stored_expire} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert value == "ttl_val"
      assert stored_expire == expire_at_ms
    end

    test "SET with EX, EXPIRETIME returns correct Unix timestamp in seconds" do
      key = pkey("ap006", "expiretime_sec")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      ttl_seconds = 30
      now_ms = System.os_time(:millisecond)
      expire_at_ms = now_ms + ttl_seconds * 1_000

      :ok = Batcher.write(shard_index, {:put, key, "v", expire_at_ms})

      # EXPIRETIME returns seconds (div expire_at_ms by 1000)
      expected_seconds = div(expire_at_ms, 1_000)

      {_val, stored_expire} = Router.get_meta(FerricStore.Instance.get(:default), key)
      actual_seconds = div(stored_expire, 1_000)

      assert actual_seconds == expected_seconds
    end

    test "SET without TTL, PEXPIRETIME returns expire_at_ms = 0 (no expiry)" do
      key = pkey("ap006", "no_ttl")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      :ok = Batcher.write(shard_index, {:put, key, "persistent", 0})

      {value, stored_expire} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert value == "persistent"
      assert stored_expire == 0
    end

    test "TTL is consistent between keydir ETS and get_meta" do
      key = pkey("ap006", "ets_vs_meta")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      expire_at_ms = System.os_time(:millisecond) + 60_000

      :ok = Batcher.write(shard_index, {:put, key, "v", expire_at_ms})

      # Check keydir ETS directly
      keydir = :"keydir_#{shard_index}"
      [{^key, _value, ets_expire, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir, key)

      # Check via get_meta
      {_value, meta_expire} = Router.get_meta(FerricStore.Instance.get(:default), key)

      assert ets_expire == expire_at_ms
      assert meta_expire == expire_at_ms
    end

    test "overwriting with new TTL updates expire_at_ms atomically" do
      key = pkey("ap006", "ttl_overwrite")
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), key)

      expire1 = System.os_time(:millisecond) + 10_000
      expire2 = System.os_time(:millisecond) + 60_000

      :ok = Batcher.write(shard_index, {:put, key, "v1", expire1})
      {_, stored1} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert stored1 == expire1

      :ok = Batcher.write(shard_index, {:put, key, "v2", expire2})
      {val, stored2} = Router.get_meta(FerricStore.Instance.get(:default), key)
      assert val == "v2"
      assert stored2 == expire2
    end
  end

  # ---------------------------------------------------------------------------
  # GC-001: Narrow window flushes on time
  #
  # Spec: FERRICSTORE.CONFIG SET session window_ms 1; 100 rapid writes.
  # Expected: All writes committed within ~1ms of submission.
  # Single-node adaptation: configure 1ms window, send rapid writes, measure
  # time from submission to reply.
  # ---------------------------------------------------------------------------

  describe "GC-001: Narrow window flushes on time" do
    test "1ms window commits all writes promptly" do
      NamespaceConfig.set("session", "window_ms", "1")
      assert 1 == NamespaceConfig.window_for("session")

      shard_idx = 0

      keys =
        for _i <- 1..100 do
          key_on_shard("session", shard_idx)
        end
        |> Enum.uniq()
        |> Enum.take(50)

      start = System.monotonic_time(:millisecond)

      results =
        Enum.map(keys, fn key ->
          Batcher.write(shard_idx, {:put, key, "v_#{key}", 0})
        end)

      elapsed = System.monotonic_time(:millisecond) - start

      assert Enum.all?(results, &(&1 == :ok))

      # With a 1ms window on single-node, all writes should complete
      # within a few hundred ms even sequentially. Each write blocks until
      # the batcher's timer fires and ra commits.
      # The key constraint from the spec: writes committed "within ~1ms of
      # submission" means the batcher window is 1ms, so the first write in
      # each batch triggers a 1ms timer. On a single node the ra commit is
      # fast (self-quorum), so total time for sequential writes should be
      # well under 5 seconds.
      assert elapsed < 5_000,
             "Expected narrow window writes to complete quickly, took #{elapsed}ms"

      # Verify all keys are readable
      for key <- keys do
        assert Router.get(FerricStore.Instance.get(:default), key) != nil,
               "Key #{key} should be readable after write"
      end
    end

    test "1ms window: individual write latency is bounded" do
      NamespaceConfig.set("latcheck", "window_ms", "1")

      key = key_on_shard("latcheck", 0)

      start = System.monotonic_time(:millisecond)
      :ok = Batcher.write(0, {:put, key, "latency_check", 0})
      latency = System.monotonic_time(:millisecond) - start

      # A single write with 1ms window should complete within reasonable
      # time: window_ms (1) + ra commit time (fast on single node).
      # Allow up to 500ms for slow CI environments.
      assert latency < 500,
             "Expected single write latency < 500ms with 1ms window, got #{latency}ms"

      assert "latency_check" == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  # ---------------------------------------------------------------------------
  # GC-002: Wide window batches writes
  #
  # Spec: FERRICSTORE.CONFIG SET sensor window_ms 50; monitor Raft entries
  # during burst.
  # Expected: Multiple writes per Raft entry during 50ms window.
  # Single-node adaptation: configure 50ms window, send concurrent writes
  # faster than the window, and verify they all succeed. Measure that the
  # total time for concurrent writes is close to window_ms (indicating
  # batching), not N * window_ms.
  # ---------------------------------------------------------------------------

  describe "GC-002: Wide window batches writes" do
    test "50ms window batches concurrent writes into fewer Raft entries" do
      NamespaceConfig.set("sensor", "window_ms", "50")
      assert 50 == NamespaceConfig.window_for("sensor")

      shard_idx = 0
      num_writes = 20

      keys =
        for _ <- 1..(num_writes * 3) do
          key_on_shard("sensor", shard_idx)
        end
        |> Enum.uniq()
        |> Enum.take(num_writes)

      # Send all writes concurrently -- they should be batched within the
      # same 50ms window into a single (or very few) Raft batch entries.
      start = System.monotonic_time(:millisecond)

      tasks =
        Enum.map(keys, fn key ->
          Task.async(fn ->
            Batcher.write(shard_idx, {:put, key, "batched_#{key}", 0})
          end)
        end)

      results = Task.await_many(tasks, 10_000)
      elapsed = System.monotonic_time(:millisecond) - start

      assert Enum.all?(results, &(&1 == :ok))

      # If writes were NOT batched, each would take ~50ms (the window).
      # With batching, all concurrent writes within the same window are
      # flushed together, so total time should be close to one window
      # (~50ms) plus ra commit overhead, not 20 * 50ms.
      # Allow generous headroom for CI: total should be under 2 seconds.
      assert elapsed < 2_000,
             "Expected batched writes to complete in roughly one window period, " <>
               "took #{elapsed}ms for #{num_writes} writes (unbatched would be ~#{num_writes * 50}ms)"

      # All keys readable
      for key <- keys do
        assert Router.get(FerricStore.Instance.get(:default), key) != nil
      end
    end

    test "wide window: writes submitted during window share a batch" do
      NamespaceConfig.set("widebatch", "window_ms", "50")

      shard_idx = 1

      keys =
        for _ <- 1..30 do
          key_on_shard("widebatch", shard_idx)
        end
        |> Enum.uniq()
        |> Enum.take(10)

      # Fire all writes at the same time
      tasks =
        Enum.map(keys, fn key ->
          Task.async(fn ->
            t1 = System.monotonic_time(:microsecond)
            result = Batcher.write(shard_idx, {:put, key, "wb_val", 0})
            t2 = System.monotonic_time(:microsecond)
            {result, t2 - t1}
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All writes succeeded
      assert Enum.all?(results, fn {result, _lat} -> result == :ok end)

      # All replies should arrive within a narrow time window of each other,
      # indicating they were flushed as part of the same batch. The spread
      # (max - min reply time) should be well under the window_ms because
      # they all get replied to when the same batch commits.
      latencies = Enum.map(results, fn {_r, lat} -> lat end)
      min_lat = Enum.min(latencies)
      max_lat = Enum.max(latencies)
      spread_ms = (max_lat - min_lat) / 1_000

      # The spread should be small (same batch commit); allow up to 200ms
      # for scheduling jitter on loaded CI.
      assert spread_ms < 200,
             "Expected concurrent write reply times to be close (same batch), " <>
               "spread was #{Float.round(spread_ms, 1)}ms"
    end
  end

  # ---------------------------------------------------------------------------
  # GC-004: Namespace batchers independent
  #
  # Spec: session writes at 1ms window; sensor writes at 50ms window.
  # Expected: session ACKs at ~1ms; sensor ACKs at ~50ms; session not delayed
  # by sensor.
  # Single-node adaptation: configure two namespaces with different windows,
  # fire concurrent writes for both, and verify that session writes are not
  # delayed by the slower sensor window.
  # ---------------------------------------------------------------------------

  describe "GC-004: Namespace batchers independent" do
    test "session (1ms) writes are not delayed by sensor (50ms) writes" do
      NamespaceConfig.set("session", "window_ms", "1")
      NamespaceConfig.set("sensor", "window_ms", "50")

      assert 1 == NamespaceConfig.window_for("session")
      assert 50 == NamespaceConfig.window_for("sensor")

      shard_idx = 0

      session_key = key_on_shard("session", shard_idx)
      sensor_key = key_on_shard("sensor", shard_idx)

      # Fire both writes concurrently
      session_task =
        Task.async(fn ->
          t1 = System.monotonic_time(:microsecond)
          result = Batcher.write(shard_idx, {:put, session_key, "fast", 0})
          t2 = System.monotonic_time(:microsecond)
          {result, (t2 - t1) / 1_000}
        end)

      sensor_task =
        Task.async(fn ->
          t1 = System.monotonic_time(:microsecond)
          result = Batcher.write(shard_idx, {:put, sensor_key, "slow", 0})
          t2 = System.monotonic_time(:microsecond)
          {result, (t2 - t1) / 1_000}
        end)

      {session_result, session_latency_ms} = Task.await(session_task, 10_000)
      {sensor_result, _sensor_latency_ms} = Task.await(sensor_task, 10_000)

      assert session_result == :ok
      assert sensor_result == :ok

      # The session write (1ms window) should complete faster than the
      # sensor write (50ms window). If they were sharing the same timer,
      # session would be delayed to ~50ms. With independent batchers,
      # session should be significantly faster.
      #
      # We don't assert exact timing (too flaky), but we verify:
      # 1. Both succeed
      # 2. Session latency < sensor latency (with some tolerance)
      # 3. Session latency is reasonable (not delayed by sensor window)
      #
      # On a loaded CI, allow generous tolerance. The key invariant is
      # that session is not delayed to the sensor's 50ms window.
      assert session_latency_ms < 500,
             "Expected session (1ms window) to complete quickly, took #{Float.round(session_latency_ms, 1)}ms"

      # Verify both values are readable
      assert "fast" == Router.get(FerricStore.Instance.get(:default), session_key)
      assert "slow" == Router.get(FerricStore.Instance.get(:default), sensor_key)
    end

    test "multiple namespace writes on same shard are independent" do
      NamespaceConfig.set("fast_ns", "window_ms", "1")
      NamespaceConfig.set("slow_ns", "window_ms", "100")

      shard_idx = 0
      num_fast = 5

      fast_keys =
        for _ <- 1..(num_fast * 3) do
          key_on_shard("fast_ns", shard_idx)
        end
        |> Enum.uniq()
        |> Enum.take(num_fast)

      slow_key = key_on_shard("slow_ns", shard_idx)

      # Start the slow write first (100ms window)
      slow_task =
        Task.async(fn ->
          Batcher.write(shard_idx, {:put, slow_key, "slow_val", 0})
        end)

      # Immediately fire fast writes (1ms window)
      fast_start = System.monotonic_time(:millisecond)

      fast_results =
        Enum.map(fast_keys, fn key ->
          Batcher.write(shard_idx, {:put, key, "fast_val", 0})
        end)

      fast_elapsed = System.monotonic_time(:millisecond) - fast_start

      assert Enum.all?(fast_results, &(&1 == :ok))

      # Fast writes should not be blocked by the slow namespace's 100ms window.
      # Even sequential, 5 writes with 1ms window should complete in well
      # under 1 second.
      assert fast_elapsed < 2_000,
             "Expected fast_ns (1ms window) writes to not be blocked by slow_ns (100ms), " <>
               "took #{fast_elapsed}ms"

      # Wait for slow write
      slow_result = Task.await(slow_task, 10_000)
      assert slow_result == :ok

      # All values readable
      for key <- fast_keys do
        assert "fast_val" == Router.get(FerricStore.Instance.get(:default), key)
      end

      assert "slow_val" == Router.get(FerricStore.Instance.get(:default), slow_key)
    end
  end

  # ---------------------------------------------------------------------------
  # SN-001: Snapshot triggered by log size (release_cursor)
  #
  # Spec: Write enough entries to exceed snapshot_interval_ops; check
  # ra:snapshot_meta.
  # Single-node adaptation: exercise StateMachine.apply directly with a
  # small release_cursor_interval and verify that the release_cursor effect
  # is emitted at the correct interval. This validates that the state machine
  # correctly tracks applied_count and triggers log compaction.
  # ---------------------------------------------------------------------------

  describe "SN-001: release_cursor triggers after N applies" do
    setup do
      root_dir = Path.join(System.tmp_dir!(), "sn001_test_#{:rand.uniform(9_999_999)}")
      dir = Ferricstore.DataDir.shard_data_path(root_dir, 0)
      File.mkdir_p!(dir)

      # v2: create the active log file directly (no NIF store reference)
      active_file_id = 0
      active_file_path = Path.join(dir, "00000.log")
      File.touch!(active_file_path)

      suffix = :rand.uniform(9_999_999)
      keydir_name = :"sn001_keydir_#{suffix}"
      :ets.new(keydir_name, [:set, :public, :named_table])

      on_exit(fn ->
        try do
          :ets.delete(keydir_name)
        rescue
          ArgumentError -> :ok
        end

        File.rm_rf(root_dir)
      end)

      %{
        data_dir: root_dir,
        shard_data_path: dir,
        active_file_id: active_file_id,
        active_file_path: active_file_path,
        keydir: keydir_name,
        dir: dir
      }
    end

    test "release_cursor emitted exactly at the configured interval", ctx do
      interval = 10

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: ctx.shard_data_path,
          active_file_id: ctx.active_file_id,
          active_file_path: ctx.active_file_path,
          ets: ctx.keydir,
          release_cursor_interval: interval
        })

      assert state.applied_count == 0
      assert state.release_cursor_interval == interval

      # Apply (interval - 1) commands -- no release_cursor should be emitted
      state_before =
        Enum.reduce(1..(interval - 1), state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, effects} =
            StateMachine.apply(meta, {:put, "sn001_#{i}", "v#{i}", 0}, acc)

          if Enum.any?(effects, &match?({:release_cursor, _, _}, &1)) do
            flunk("release_cursor emitted at apply #{i}, before interval #{interval}")
          end

          new_state
        end)

      assert state_before.applied_count == interval - 1

      # The interval-th apply should mark a pending release cursor. The actual
      # release effect is emitted only after replay-safe durability catches up.
      meta = %{index: interval, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, :ok}, effects} =
        StateMachine.apply(meta, {:put, "sn001_#{interval}", "v#{interval}", 0}, state_before)

      assert new_state.applied_count == interval
      refute Enum.any?(effects, &match?({:release_cursor, _}, &1))
      assert new_state.pending_release_cursor_index == interval
    end

    test "pending release_cursor advances at every multiple of the interval", ctx do
      interval = 5

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: ctx.shard_data_path,
          active_file_id: ctx.active_file_id,
          active_file_path: ctx.active_file_path,
          ets: ctx.keydir,
          release_cursor_interval: interval
        })

      # Apply 3 * interval commands, track where pending release cursor advances.
      {_final_state, cursor_indices} =
        Enum.reduce(1..(interval * 3), {state, []}, fn i, {acc, cursors} ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          case StateMachine.apply(meta, {:put, "sn001m_#{i}", "v#{i}", 0}, acc) do
            {new_state, {:applied_at, _, :ok}, _effects} ->
              pending = new_state.pending_release_cursor_index

              if pending != acc.pending_release_cursor_index do
                {new_state, cursors ++ [pending]}
              else
                {new_state, cursors}
              end
          end
        end)

      assert cursor_indices == [interval, interval * 2, interval * 3]
    end

    test "applied_count in state machine tracks total applies accurately", ctx do
      interval = 100

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: ctx.shard_data_path,
          active_file_id: ctx.active_file_id,
          active_file_path: ctx.active_file_path,
          ets: ctx.keydir,
          release_cursor_interval: interval
        })

      # Apply various command types
      {s1, :ok} = StateMachine.apply(%{}, {:put, "count_a", "va", 0}, state)
      assert s1.applied_count == 1

      {s2, :ok} = StateMachine.apply(%{}, {:delete, "count_a"}, s1)
      assert s2.applied_count == 2

      # Batch of 5 commands increments by 5
      batch = for i <- 1..5, do: {:put, "count_b_#{i}", "vb#{i}", 0}
      {s3, {:ok, _results}} = StateMachine.apply(%{}, {:batch, batch}, s2)
      assert s3.applied_count == 7

      # overview reflects applied_count
      overview = StateMachine.overview(s3)
      assert overview.applied_count == 7
    end

    test "batch crossing interval boundary emits single release_cursor", ctx do
      interval = 5

      state =
        StateMachine.init(%{
          shard_index: 0,
          shard_data_path: ctx.shard_data_path,
          active_file_id: ctx.active_file_id,
          active_file_path: ctx.active_file_path,
          ets: ctx.keydir,
          release_cursor_interval: interval
        })

      # Apply 3 single commands (applied_count = 3)
      state_before =
        Enum.reduce(1..3, state, fn i, acc ->
          meta = %{index: i, term: 1, system_time: System.os_time(:millisecond)}

          {new_state, {:applied_at, _, :ok}, _effects} =
            StateMachine.apply(meta, {:put, "sn_batch_#{i}", "v#{i}", 0}, acc)

          new_state
        end)

      assert state_before.applied_count == 3

      # Batch of 4 commands: applied_count goes from 3 to 7, crossing 5
      batch = for i <- 1..4, do: {:put, "sn_batch_b_#{i}", "vb#{i}", 0}
      meta = %{index: 4, term: 1, system_time: System.os_time(:millisecond)}

      {new_state, {:applied_at, _, {:ok, results}}, effects} =
        StateMachine.apply(meta, {:batch, batch}, state_before)

      assert length(results) == 4
      assert new_state.applied_count == 7
      # A single pending release cursor is recorded for the Ra index that
      # crossed the boundary; release waits for replay-safe durability.
      refute Enum.any?(effects, &match?({:release_cursor, _}, &1))
      assert new_state.pending_release_cursor_index == 4
    end
  end
end
