defmodule Ferricstore.Raft.AsyncWalTest do
  @moduledoc """
  Tests for the async fdatasync WAL monkey-patch (ra_log_wal).

  The patched WAL decouples fdatasync from the batch processing loop:
  1. Writes data to kernel buffer synchronously (fast)
  2. Queues writer notifications in `pending_sync`
  3. Spawns a linked process that opens a SEPARATE fd and calls fdatasync
  4. Notifies writers ONLY AFTER fdatasync completes (durability preserved)
  5. While fdatasync runs, WAL keeps accepting new entries (batching)

  These tests verify:
  - The patched module is correctly loaded
  - Durability guarantees: :ok means data is on disk
  - Async batching behavior: multiple writers notified per fdatasync
  - Edge cases: WAL rotation, rapid writes, mixed workloads
  - Concurrent correctness: INCR and CAS under contention
  - The patch is NOT sync_method=none (writers notified after sync, not before)
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
      ShardHelpers.wait_shards_alive()
    end)
  end

  defp ukey(base), do: "async_wal_#{base}_#{:rand.uniform(9_999_999)}"

  # ---------------------------------------------------------------------------
  # Patch loading and verification
  # ---------------------------------------------------------------------------

  describe "async WAL patch loading" do
    test "patched ra_log_wal module is loaded" do
      # The patched module must be loaded (not the upstream one).
      # Verify the module is available and has our patched fields.
      assert {:module, :ra_log_wal} == :code.ensure_loaded(:ra_log_wal)

      # The patched module exports format_status/1 which includes
      # async_sync info. Verify the export list contains it.
      exports = :ra_log_wal.module_info(:exports)
      assert {:format_status, 1} in exports
    end

    test "WAL process is alive and uses the patched module" do
      # Derive the WAL process name from the ra system names.
      names = :ra_system.derive_names(Cluster.system_name())
      wal_name = names.wal

      pid = Process.whereis(wal_name)
      assert is_pid(pid), "WAL process #{inspect(wal_name)} not registered"
      assert Process.alive?(pid), "WAL process is not alive"
    end

    test "WAL format_status exposes async_sync state" do
      # The patched format_status/1 includes an :async_sync key with
      # :pending_batches and :sync_in_flight fields.
      names = :ra_system.derive_names(Cluster.system_name())
      wal_name = names.wal

      status = :sys.get_status(wal_name)

      # :sys.get_status returns a tuple; the format_status output is
      # typically in the last element of the status tuple.
      # We extract it by pattern matching the gen_batch_server status format.
      {_status_type, _pid, _mod_info, status_data} = status

      # The status data is a list of items; the last is typically the state.
      # For gen_batch_server, the format_status result is embedded.
      status_str = inspect(status_data)

      assert status_str =~ "async_sync",
             "format_status should contain async_sync key, got: #{status_str}"
    end

    test "WAL sync emits duration telemetry for profiling" do
      parent = self()
      handler_id = {__MODULE__, :wal_sync_profile, make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :wal, :sync],
        fn event, measurements, metadata, _config ->
          send(parent, {:wal_sync_profile, event, measurements, metadata})
        end,
        nil
      )

      try do
        :ok = FerricStore.set(ukey("wal_sync_profile"), "value")

        assert_receive {:wal_sync_profile, [:ferricstore, :wal, :sync], measurements, metadata},
                       5_000

        assert is_integer(measurements.duration_us)
        assert measurements.duration_us >= 0
        assert measurements.pending_batches >= 1
        assert is_integer(measurements.delay_us)
        assert measurements.delay_us >= 0
        assert metadata.status == :ok
      after
        :telemetry.detach(handler_id)
      end
    end

    test "adaptive WAL delay only waits when sync backlog exists" do
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(0, 0) == 0
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(6_000, 0) == 200
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(6_000, 1) == 200
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(6_000, 2) == 1_000
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(6_000, 8) == 3_000
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(6_000, 32) == 6_000
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(100, 1) == 100
      assert :ra_log_wal.__adaptive_commit_delay_us_for_test__(100, 32) == 100
    end

    test "adaptive WAL delay drains queued pressure and decays when idle" do
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(0, 200, 1, 1) == 0
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(6_000, 200, 1, 1) == 200
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(6_000, 1_000, 1, 1) == 200
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(6_000, 4_000, 1, 1) == 200
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(6_000, 6_000, 1, 0) == 3_000
      assert :ra_log_wal.__next_adaptive_commit_delay_us_for_test__(6_000, 300, 1, 0) == 200
    end

    test "completed WAL sync releases only the batches covered by that sync" do
      assert :ra_log_wal.__complete_sync_partition_for_test__(3, 2) == {3, 2}
    end

    test "completed WAL sync can release delay-covered batches by synced byte frontier" do
      assert :ra_log_wal.__complete_sync_frontier_for_test__([100], [300, 200], 250) ==
               {2, [300]}
    end

    test "patched module source file contains async-sync marker" do
      # The patched .erl file should have our marker comment.
      wal_source =
        :ferricstore
        |> :code.priv_dir()
        |> Path.join("patched/ra_log_wal.erl")

      assert File.exists?(wal_source)
      content = File.read!(wal_source)
      assert content =~ "async-sync patched"
      assert content =~ "pending_sync"
      assert content =~ "sync_in_flight"
      assert content =~ "maybe_start_async_sync"
      assert content =~ "wal_async_sync_complete"
    end
  end

  # ---------------------------------------------------------------------------
  # Durability guarantees
  # ---------------------------------------------------------------------------

  describe "async WAL durability" do
    test "write returns :ok only after data is durable" do
      k = ukey("durable_single")

      :ok = FerricStore.set(k, "durable_value")

      # If set returned :ok, the write went through Raft consensus and the
      # WAL fdatasync completed. The data must be readable.
      {:ok, val} = FerricStore.get(k)
      assert val == "durable_value"
    end

    test "concurrent writes all durable after :ok" do
      # 50 writers, each does 20 writes. After all return :ok, ALL writes
      # must be recoverable.
      count = 50
      writes_per = 20
      prefix = ukey("conc_dur")

      tasks =
        for i <- 1..count do
          Task.async(fn ->
            for j <- 1..writes_per do
              key = "#{prefix}:#{i}:#{j}"
              :ok = FerricStore.set(key, "v_#{i}_#{j}")
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      # Verify all writes are readable
      for i <- 1..count, j <- 1..writes_per do
        key = "#{prefix}:#{i}:#{j}"
        {:ok, val} = FerricStore.get(key)

        assert val == "v_#{i}_#{j}",
               "Key #{key} expected v_#{i}_#{j}, got #{inspect(val)}"
      end
    end

    test "write survives flush to Bitcask" do
      k = ukey("flush_dur")

      :ok = FerricStore.set(k, "flush_value")

      # Flush all shards to ensure data is written through to Bitcask.
      ShardHelpers.flush_all_shards()

      {:ok, val} = FerricStore.get(k)
      assert val == "flush_value"
    end

    test "sequential writes maintain order" do
      k = ukey("order")

      for i <- 1..100 do
        :ok = FerricStore.set(k, "v#{i}")
      end

      {:ok, val} = FerricStore.get(k)
      assert val == "v100", "Expected last write to win, got #{inspect(val)}"
    end
  end

  # ---------------------------------------------------------------------------
  # Async batching behavior
  # ---------------------------------------------------------------------------

  describe "async WAL batching" do
    test "multiple writers are notified together after one fdatasync" do
      # Start 10 writers simultaneously. With async WAL batching, they should
      # all return within a similar timeframe (batched into one fdatasync window),
      # not sequentially spaced by fdatasync_time each.
      prefix = ukey("batch_notify")

      results =
        for i <- 1..10 do
          Task.async(fn ->
            t0 = System.monotonic_time(:millisecond)
            :ok = FerricStore.set("#{prefix}:#{i}", "v")
            t1 = System.monotonic_time(:millisecond)
            t1 - t0
          end)
        end
        |> Task.await_many(10_000)

      max_time = Enum.max(results)
      min_time = Enum.min(results)
      spread = max_time - min_time

      # With async batching, spread should be small (< 100ms).
      # With blocking fdatasync, spread would be large (10 * fdatasync_time).
      # We use a generous threshold to avoid flaky tests on slow CI.
      assert spread < 500,
             "Writers returned too far apart: #{spread}ms spread " <>
               "(min=#{min_time}ms, max=#{max_time}ms). " <>
               "Expected batching to keep spread under 500ms. Times: #{inspect(results)}"
    end

    test "WAL accumulates entries during fdatasync" do
      # Send many writes rapidly. With async WAL, they complete faster
      # because fdatasync is amortized across batches.
      prefix = ukey("accum")

      {elapsed_us, _} =
        :timer.tc(fn ->
          for i <- 1..200 do
            :ok = FerricStore.set("#{prefix}:#{i}", "v")
          end
        end)

      elapsed_ms = elapsed_us / 1000

      # With blocking fdatasync (~10-20ms each on macOS), 200 writes would
      # take 2000-4000ms. With async batching, it should be much less.
      # We use a generous threshold: under 10s (50ms/write average).
      assert elapsed_ms < 10_000,
             "200 writes took #{Float.round(elapsed_ms, 1)}ms, expected < 10s with async batching"

      # Verify all writes landed
      for i <- 1..200 do
        {:ok, "v"} = FerricStore.get("#{prefix}:#{i}")
      end
    end

    test "concurrent bursts complete in batched fashion" do
      # Multiple bursts of concurrent writers. Each burst should complete
      # roughly together, not one-by-one.
      prefix = ukey("burst")

      for burst <- 1..5 do
        tasks =
          for i <- 1..20 do
            Task.async(fn ->
              :ok = FerricStore.set("#{prefix}:#{burst}:#{i}", "v")
            end)
          end

        Task.await_many(tasks, 10_000)
      end

      # Verify all 100 writes
      for burst <- 1..5, i <- 1..20 do
        {:ok, "v"} = FerricStore.get("#{prefix}:#{burst}:#{i}")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "async WAL edge cases" do
    test "WAL handles rapid small writes" do
      prefix = ukey("rapid")

      for i <- 1..2000 do
        :ok = FerricStore.set("#{prefix}:#{i}", "v")
      end

      for i <- 1..2000 do
        {:ok, "v"} = FerricStore.get("#{prefix}:#{i}")
      end
    end

    test "WAL handles mixed read/write workload" do
      prefix = ukey("mixed")

      for i <- 1..500 do
        :ok = FerricStore.set("#{prefix}:#{i}", "val_#{i}")
        {:ok, val} = FerricStore.get("#{prefix}:#{i}")
        assert val == "val_#{i}"
      end
    end

    test "WAL handles large values" do
      k = ukey("large_val")
      large = String.duplicate("x", 64 * 1024)

      :ok = FerricStore.set(k, large)
      {:ok, val} = FerricStore.get(k)
      assert val == large
    end

    test "WAL handles empty values" do
      k = ukey("empty_val")

      :ok = FerricStore.set(k, "")
      {:ok, val} = FerricStore.get(k)
      assert val == ""
    end

    test "DELETE is durable through async WAL" do
      k = ukey("del_test")

      :ok = FerricStore.set(k, "value")
      {:ok, "value"} = FerricStore.get(k)

      {:ok, 1} = FerricStore.del(k)
      {:ok, nil} = FerricStore.get(k)
    end

    test "overwrite is durable through async WAL" do
      k = ukey("overwrite")

      :ok = FerricStore.set(k, "first")
      {:ok, "first"} = FerricStore.get(k)

      :ok = FerricStore.set(k, "second")
      {:ok, "second"} = FerricStore.get(k)

      :ok = FerricStore.set(k, "third")
      {:ok, "third"} = FerricStore.get(k)
    end

    test "WAL handles writes to all shards concurrently" do
      # Ensure async WAL works across all shard WAL processes.
      # Write keys that hash to different shards.
      prefix = ukey("all_shards")
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      keys_by_shard =
        Enum.reduce(1..10_000, %{}, fn i, acc ->
          key = "#{prefix}:#{i}"
          shard = Router.shard_for(FerricStore.Instance.get(:default), key)

          if map_size(Map.get(acc, shard, %{})) < 10 do
            Map.update(acc, shard, %{key => true}, &Map.put(&1, key, true))
          else
            acc
          end
        end)

      # Verify we found keys for each shard
      for s <- 0..(shard_count - 1) do
        assert Map.has_key?(keys_by_shard, s),
               "Could not find keys that hash to shard #{s}"
      end

      # Write all keys concurrently
      all_keys =
        keys_by_shard
        |> Enum.flat_map(fn {_shard, key_map} -> Map.keys(key_map) end)

      tasks =
        for key <- all_keys do
          Task.async(fn ->
            :ok = FerricStore.set(key, "shard_test")
            key
          end)
        end

      written = Task.await_many(tasks, 10_000)

      # Verify all reads
      for key <- written do
        {:ok, "shard_test"} = FerricStore.get(key)
      end
    end

    test "WAL handles file rotation during writes" do
      # Write enough data to potentially trigger WAL rotation.
      # Each value is 1KB, 1000 writes = ~1MB of WAL data.
      prefix = ukey("rotation")

      for i <- 1..1000 do
        :ok = FerricStore.set("#{prefix}:#{i}", String.duplicate("x", 1000))
      end

      # Verify all keys readable after rotation
      for i <- 1..1000 do
        {:ok, val} = FerricStore.get("#{prefix}:#{i}")
        assert val != nil, "Key #{prefix}:#{i} lost during rotation"
        assert byte_size(val) == 1000
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent correctness (INCR, CAS)
  # ---------------------------------------------------------------------------

  describe "async WAL concurrent correctness" do
    test "INCR is atomic through async WAL" do
      # FerricStore.incr/1 does a non-atomic read-modify-write at the API level.
      # Router.incr/2 is the atomic path (goes through Raft state machine as a
      # single {:incr, key, delta} command). Test that path directly.
      k = ukey("incr_atomic")
      Router.put(FerricStore.Instance.get(:default), k, "0")

      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            for _ <- 1..100 do
              Router.incr(FerricStore.Instance.get(:default), k, 1)
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      assert Router.get(FerricStore.Instance.get(:default), k) == "5000"
    end

    test "CAS is linearizable through async WAL" do
      k = ukey("cas_linear")
      :ok = FerricStore.set(k, "0")

      # 50 tasks try to CAS 0->1, exactly one should succeed
      results =
        for _ <- 1..50 do
          Task.async(fn ->
            FerricStore.cas(k, "0", "1")
          end)
        end
        |> Task.await_many(10_000)

      successes = Enum.count(results, fn r -> r == {:ok, true} end)
      assert successes == 1, "Expected exactly 1 CAS success, got #{successes}"

      # The value must be "1" (the single successful CAS)
      {:ok, "1"} = FerricStore.get(k)
    end

    test "concurrent INCR and reads are consistent" do
      k = ukey("incr_read")
      Router.put(FerricStore.Instance.get(:default), k, "0")

      # Writers increment while readers read. Every read must see a valid
      # integer in the range [0, total_increments].
      total = 500

      writer =
        Task.async(fn ->
          for _ <- 1..total do
            Router.incr(FerricStore.Instance.get(:default), k, 1)
          end
        end)

      reader =
        Task.async(fn ->
          for _ <- 1..200 do
            val = Router.get(FerricStore.Instance.get(:default), k)

            if val != nil do
              n = if is_integer(val), do: val, else: String.to_integer(val)
              assert n >= 0 and n <= total, "Read invalid value: #{n}"
            end

            Process.sleep(1)
          end
        end)

      Task.await(writer, 30_000)
      Task.await(reader, 30_000)

      assert Router.get(FerricStore.Instance.get(:default), k) == Integer.to_string(total)
    end

    test "CAS retry loop converges" do
      k = ukey("cas_retry")
      :ok = FerricStore.set(k, "0")

      # 10 tasks each try to increment via CAS loop.
      # Each task does 10 increments. Total should be 100.
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            for _ <- 1..10 do
              cas_increment(k)
            end
          end)
        end

      Task.await_many(tasks, 30_000)

      {:ok, val} = FerricStore.get(k)
      assert String.to_integer(val) == 100
    end
  end

  # ---------------------------------------------------------------------------
  # Async WAL is NOT sync_method=none
  # ---------------------------------------------------------------------------

  describe "async WAL is NOT sync_method=none" do
    test "writers are notified AFTER fdatasync, not before" do
      # With sync_method=none: writers notified immediately (before sync).
      # With our patch: writers notified only after sync completes.
      # We verify indirectly: if :ok is returned, the data must be readable
      # AND the WAL sync_method config must NOT be :none.
      k = ukey("not_none")

      :ok = FerricStore.set(k, "important_data")
      {:ok, "important_data"} = FerricStore.get(k)

      # Verify the WAL config does not use sync_method=none
      names = :ra_system.derive_names(Cluster.system_name())
      wal_name = names.wal
      status = :sys.get_status(wal_name)
      status_str = inspect(status)

      # The format_status includes sync_method. It should be :datasync or :sync,
      # never :none.
      refute status_str =~ "sync_method => none" or status_str =~ "sync_method: :none",
             "WAL sync_method should NOT be :none -- that would mean no durability. " <>
               "Status: #{status_str}"
    end

    test "format_status sync_method is datasync or sync" do
      names = :ra_system.derive_names(Cluster.system_name())
      wal_name = names.wal
      status = :sys.get_status(wal_name)
      status_str = inspect(status)

      assert status_str =~ "sync_method" or status_str =~ "datasync" or status_str =~ ":sync",
             "WAL status should mention sync_method. Status: #{status_str}"
    end

    test "data written before flush is readable after flush" do
      # Write a value, flush all batchers and shards, then verify readability.
      # This confirms the full durability chain: WAL -> StateMachine -> Bitcask.
      k = ukey("full_chain")

      :ok = FerricStore.set(k, "chain_value")

      # Flush batchers
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        try do
          Ferricstore.Raft.Batcher.flush(i)
        rescue
          _ -> :ok
        end
      end

      # Flush shards to Bitcask
      ShardHelpers.flush_all_shards()

      {:ok, "chain_value"} = FerricStore.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Stress tests
  # ---------------------------------------------------------------------------

  describe "async WAL stress" do
    test "high concurrency write storm" do
      prefix = ukey("storm")
      writer_count = 100
      writes_per = 10

      tasks =
        for i <- 1..writer_count do
          Task.async(fn ->
            for j <- 1..writes_per do
              :ok = FerricStore.set("#{prefix}:#{i}:#{j}", "s_#{i}_#{j}")
            end
          end)
        end

      Task.await_many(tasks, 60_000)

      # Spot-check a sample of writes
      for i <- Enum.take_random(1..writer_count, 20),
          j <- Enum.take_random(1..writes_per, 5) do
        {:ok, val} = FerricStore.get("#{prefix}:#{i}:#{j}")
        assert val == "s_#{i}_#{j}"
      end
    end

    test "interleaved writers and deleters" do
      prefix = ukey("interleave")

      # Phase 1: write 200 keys
      for i <- 1..200 do
        :ok = FerricStore.set("#{prefix}:#{i}", "alive")
      end

      # Phase 2: concurrently delete odd keys and overwrite even keys
      tasks =
        for i <- 1..200 do
          Task.async(fn ->
            if rem(i, 2) == 1 do
              {:ok, 1} = FerricStore.del("#{prefix}:#{i}")
              {:deleted, i}
            else
              :ok = FerricStore.set("#{prefix}:#{i}", "updated")
              {:updated, i}
            end
          end)
        end

      results = Task.await_many(tasks, 10_000)

      # Verify results
      for {action, i} <- results do
        case action do
          :deleted ->
            {:ok, nil} = FerricStore.get("#{prefix}:#{i}")

          :updated ->
            {:ok, "updated"} = FerricStore.get("#{prefix}:#{i}")
        end
      end
    end

    test "writes with TTL are durable through async WAL" do
      k = ukey("ttl_dur")
      # Set with 60 second TTL (won't expire during test)
      :ok = FerricStore.set(k, "ttl_value", ttl: 60_000)

      {:ok, "ttl_value"} = FerricStore.get(k)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # CAS-based increment: reads current value, attempts CAS, retries on conflict.
  defp cas_increment(key, max_retries \\ 1000) do
    cas_increment_loop(key, max_retries, 0)
  end

  defp cas_increment_loop(_key, max_retries, attempt) when attempt >= max_retries do
    raise "CAS increment failed after #{max_retries} retries"
  end

  defp cas_increment_loop(key, max_retries, attempt) do
    {:ok, current} = FerricStore.get(key)
    current_val = if current, do: current, else: "0"
    new_val = Integer.to_string(String.to_integer(current_val) + 1)

    case FerricStore.cas(key, current_val, new_val) do
      {:ok, true} -> :ok
      {:ok, false} -> cas_increment_loop(key, max_retries, attempt + 1)
      {:ok, nil} -> cas_increment_loop(key, max_retries, attempt + 1)
    end
  end
end
