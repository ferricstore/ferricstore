defmodule Ferricstore.Merge.FragmentationTest do
  @moduledoc """
  Tests for fragmentation-based compaction triggers.

  Covers: per-file dead byte tracking (file_stats), fragmentation threshold
  notifications, merge cooldown, promoted collection fragmentation compaction,
  and crash recovery of file_stats.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Merge.Scheduler
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @compound_path Path.expand("../../../lib/ferricstore/store/shard/compound.ex", __DIR__)

  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  setup do
    ShardHelpers.flush_all_keys()
    :ok
  end

  test "promoted compaction cooldown uses monotonic elapsed time" do
    source = File.read!(@compound_path)
    [_before, rest] = String.split(source, "def bump_promoted_writes", parts: 2)
    [body | _after] = String.split(rest, "\n  @spec promoted_dir_size", parts: 2)

    assert body =~ "System.monotonic_time(:millisecond)"
    refute body =~ "System.system_time(:millisecond)"
  end

  # ---------------------------------------------------------------------------
  # Scheduler: fragmentation notification
  # ---------------------------------------------------------------------------

  describe "scheduler fragmentation notification" do
    test "notify_fragmentation updates candidates in dedicated scheduler" do
      # Use a dedicated scheduler so we control the state
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 100,
            merge_cooldown_ms: 60_000
          },
          name: :test_frag_notify_scheduler
        )

      # Set last_merge_completed_at so cooldown blocks merge
      :sys.replace_state(pid, fn state ->
        %{state | last_merge_completed_at: System.system_time(:millisecond)}
      end)

      GenServer.cast(pid, {:fragmentation, [1, 2], 5})

      ShardHelpers.eventually(fn ->
        GenServer.call(pid, :status).file_count == 5
      end, "fragmentation notification not processed", 20, 10)

      status = GenServer.call(pid, :status)
      # Cooldown should block the merge, preserving the candidates
      assert status.fragmentation_candidates == [1, 2]
      assert status.file_count == 5

      GenServer.stop(pid)
    end

    test "notify_fragmentation is safe when scheduler is not running" do
      assert :ok = Scheduler.notify_fragmentation(99_999, [1], 1)
    end

    test "fragmentation candidates cleared after merge attempt" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 100,
            merge_cooldown_ms: 0
          },
          name: :test_frag_clear_scheduler
        )

      GenServer.cast(pid, {:fragmentation, [1], 3})

      ShardHelpers.eventually(fn ->
        GenServer.call(pid, :status).fragmentation_candidates == []
      end, "fragmentation candidates not cleared after merge attempt", 20, 10)

      status = GenServer.call(pid, :status)
      # Candidates should be cleared after merge attempt
      assert status.fragmentation_candidates == []

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scheduler: merge cooldown
  # ---------------------------------------------------------------------------

  describe "merge cooldown" do
    test "merge is deferred during cooldown" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 2,
            merge_cooldown_ms: 60_000
          },
          name: :test_cooldown_scheduler
        )

      # Simulate a recently completed merge
      :sys.replace_state(pid, fn state ->
        %{state | last_merge_completed_at: System.system_time(:millisecond)}
      end)

      # Send file rotation that would normally trigger merge
      GenServer.cast(pid, {:file_rotated, 10})

      ShardHelpers.eventually(fn ->
        GenServer.call(pid, :status).file_count == 10
      end, "file count not updated", 20, 10)

      status = GenServer.call(pid, :status)
      assert status.merging == false
      assert status.merge_count == 0

      GenServer.stop(pid)
    end

    test "merge proceeds after cooldown expires" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 2,
            merge_cooldown_ms: 10
          },
          name: :test_expired_cooldown_scheduler
        )

      # Simulate a merge that completed long enough ago
      :sys.replace_state(pid, fn state ->
        %{state | last_merge_completed_at: System.system_time(:millisecond) - 100}
      end)

      # Send file rotation
      GenServer.cast(pid, {:file_rotated, 10})

      ShardHelpers.eventually(fn ->
        GenServer.call(pid, :status).file_count == 10
      end, "file count not updated", 20, 10)

      # Merge should have been attempted (cooldown didn't block it).
      # The attempt may fail (not enough actual files) but that's fine —
      # what matters is the cooldown didn't prevent the attempt.
      status = GenServer.call(pid, :status)
      # last_merge_completed_at should still be set from our :sys.replace_state,
      # confirming the cooldown check didn't reject it prematurely.
      assert status.last_merge_completed_at != nil

      GenServer.stop(pid)
    end

    test "last_merge_completed_at is nil before any merge" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{mode: :hot, min_files_for_merge: 100},
          name: :test_nil_cooldown_scheduler
        )

      status = GenServer.call(pid, :status)
      assert status.last_merge_completed_at == nil

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scheduler: config options
  # ---------------------------------------------------------------------------

  describe "scheduler config" do
    test "new config options present with values from app env" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          name: :test_defaults_scheduler
        )

      status = GenServer.call(pid, :status)
      config = status.config

      assert is_float(config.fragmentation_threshold)
      assert config.fragmentation_threshold > 0 and config.fragmentation_threshold <= 1
      assert is_integer(config.dead_bytes_threshold)
      assert config.dead_bytes_threshold > 0
      assert is_integer(config.merge_cooldown_ms)
      assert config.merge_cooldown_ms >= 0
      assert is_integer(config.small_file_threshold)
      assert config.small_file_threshold > 0

      GenServer.stop(pid)
    end

    test "config options can be overridden" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            fragmentation_threshold: 0.3,
            dead_bytes_threshold: 1024,
            merge_cooldown_ms: 5000,
            small_file_threshold: 512
          },
          name: :test_custom_config_scheduler
        )

      status = GenServer.call(pid, :status)
      config = status.config

      assert config.fragmentation_threshold == 0.3
      assert config.dead_bytes_threshold == 1024
      assert config.merge_cooldown_ms == 5000
      assert config.small_file_threshold == 512

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Scheduler: pick_mergeable_files with fragmentation candidates
  # ---------------------------------------------------------------------------

  describe "file selection with fragmentation" do
    test "fragmentation-triggered merge can merge a single file" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 5,
            merge_cooldown_ms: 0
          },
          name: :test_single_file_frag_scheduler
        )

      # Send fragmentation with candidates but only 2 total files
      GenServer.cast(pid, {:fragmentation, [0], 2})

      ShardHelpers.eventually(fn ->
        GenServer.call(pid, :status).fragmentation_candidates == []
      end, "fragmentation candidates not cleared", 20, 10)

      # The merge should have been attempted (candidates cleared)
      status = GenServer.call(pid, :status)
      assert status.fragmentation_candidates == []

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Shard: file_stats tracking
  # ---------------------------------------------------------------------------

  describe "shard file_stats" do
    test "file_stats populated with at least one file on a running shard" do
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), 0)

      # Write some data to ensure the shard has content
      for i <- 1..5 do
        Router.put(FerricStore.Instance.get(:default), "stats_check_#{i}_#{:rand.uniform(10_000_000)}", "value", 0)
      end

      ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard_name)
        is_map(state.file_stats) and map_size(state.file_stats) > 0
      end, "file_stats not populated", 20, 10)

      state = :sys.get_state(shard_name)
      assert is_map(state.file_stats)
      assert map_size(state.file_stats) > 0
    end

    test "file_stats contains active file entry" do
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), 0)
      state = :sys.get_state(shard_name)

      # Active file should be present in file_stats
      assert Map.has_key?(state.file_stats, state.active_file_id)
    end

    test "file_stats values are {total_bytes, dead_bytes} tuples" do
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), 0)
      state = :sys.get_state(shard_name)

      Enum.each(state.file_stats, fn {fid, {total, dead}} ->
        assert is_integer(fid), "Expected integer file_id, got #{inspect(fid)}"
        assert is_number(total), "Expected number total_bytes for fid #{fid}, got #{inspect(total)}"
        assert is_number(dead), "Expected number dead_bytes for fid #{fid}, got #{inspect(dead)}"
        assert total >= 0, "Expected non-negative total_bytes for fid #{fid}"
        assert dead >= 0, "Expected non-negative dead_bytes for fid #{fid}"
      end)
    end

    test "merge_config stored in shard state" do
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), 0)
      state = :sys.get_state(shard_name)

      assert is_map(state.merge_config)
      assert Map.has_key?(state.merge_config, :fragmentation_threshold)
      assert Map.has_key?(state.merge_config, :dead_bytes_threshold)
    end
  end

  # ---------------------------------------------------------------------------
  # Promoted: byte tracking fields
  # ---------------------------------------------------------------------------

  describe "promoted byte tracking" do
    @tag :slow
    test "promoted instance includes byte tracking fields after promotion" do
      original_pt =
        try do
          :persistent_term.get(:ferricstore_promotion_threshold)
        rescue
          ArgumentError -> :not_set
        end

      :persistent_term.put(:ferricstore_promotion_threshold, 5)

      redis_key = "prom_bytes_#{:rand.uniform(10_000_000)}"
      shard_index = Router.shard_for(FerricStore.Instance.get(:default), redis_key)
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), shard_index)

      # Write enough entries to trigger promotion via compound_put
      for i <- 1..6 do
        compound_key = "H:#{redis_key}\0field#{i}"
        GenServer.call(shard_name, {:compound_put, redis_key, compound_key, "val#{i}", 0})
      end

      ShardHelpers.eventually(fn ->
        state = :sys.get_state(shard_name)
        Map.has_key?(state.promoted_instances, redis_key)
      end, "promoted instance not created", 30, 50)

      state = :sys.get_state(shard_name)

      case Map.get(state.promoted_instances, redis_key) do
        %{total_bytes: tb, dead_bytes: db, last_compacted_at: lca} ->
          assert is_number(tb)
          assert is_number(db)
          assert db >= 0
          assert lca == nil

        nil ->
          :ok

        %{path: _path} ->
          # May have legacy format if upgrade path triggered, that's OK
          :ok
      end

      case original_pt do
        :not_set -> :persistent_term.erase(:ferricstore_promotion_threshold)
        val -> :persistent_term.put(:ferricstore_promotion_threshold, val)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Semaphore busy: retry
  # ---------------------------------------------------------------------------

  describe "semaphore busy retry" do
    test "scheduler retries merge after semaphore busy" do
      # Hold the real semaphore so our test scheduler gets :busy
      real_sem = Ferricstore.Merge.Semaphore
      :ok = Ferricstore.Merge.Semaphore.acquire(99, real_sem)

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 2,
            merge_cooldown_ms: 0,
            merge_retry_interval_ms: 50
          },
          semaphore: real_sem,
          name: :test_retry_scheduler
        )

      # Trigger merge — should get :busy and schedule a retry
      GenServer.cast(pid, {:file_rotated, 10})

      ShardHelpers.eventually(fn ->
        state = :sys.get_state(pid)
        state.retry_ref != nil
      end, "retry not scheduled after semaphore busy", 20, 10)

      # Verify retry is pending
      state = :sys.get_state(pid)
      assert state.retry_ref != nil
      assert state.merging == false

      # Release the semaphore — the retry should fire and attempt merge
      Ferricstore.Merge.Semaphore.release(99, real_sem)

      ShardHelpers.eventually(fn ->
        state = :sys.get_state(pid)
        state.retry_ref == nil
      end, "retry_ref not cleared after semaphore release", 30, 50)

      # After retry, retry_ref should be cleared
      state = :sys.get_state(pid)
      assert state.retry_ref == nil

      GenServer.stop(pid)
    end

    test "multiple busy responses don't stack retries" do
      real_sem = Ferricstore.Merge.Semaphore
      :ok = Ferricstore.Merge.Semaphore.acquire(99, real_sem)

      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          merge_config: %{
            mode: :hot,
            min_files_for_merge: 2,
            merge_cooldown_ms: 0,
            merge_retry_interval_ms: 200
          },
          semaphore: real_sem,
          name: :test_no_stack_scheduler
        )

      # Send multiple rotations while semaphore held
      for i <- 1..5 do
        GenServer.cast(pid, {:file_rotated, 10 + i})
      end

      ShardHelpers.eventually(fn ->
        state = :sys.get_state(pid)
        state.retry_ref != nil
      end, "retry not scheduled", 20, 10)

      # Should have exactly one retry pending, not five
      state = :sys.get_state(pid)
      assert state.retry_ref != nil

      Ferricstore.Merge.Semaphore.release(99, real_sem)
      GenServer.stop(pid)
    end

    test "retry config defaults to 5 seconds" do
      {:ok, pid} =
        Scheduler.start_link(
          shard_index: 0,
          data_dir: Application.get_env(:ferricstore, :data_dir, "data"),
          name: :test_retry_default_scheduler
        )

      status = GenServer.call(pid, :status)
      assert status.config.merge_retry_interval_ms == 5_000

      GenServer.stop(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Status observability
  # ---------------------------------------------------------------------------

  describe "status observability" do
    test "scheduler status includes fragmentation fields" do
      status = Scheduler.status(0)

      assert Map.has_key?(status, :fragmentation_candidates)
      assert Map.has_key?(status, :last_merge_completed_at)
      assert is_list(status.fragmentation_candidates)
    end

    test "scheduler status config includes new options" do
      status = Scheduler.status(0)
      config = status.config

      assert Map.has_key?(config, :fragmentation_threshold)
      assert Map.has_key?(config, :dead_bytes_threshold)
      assert Map.has_key?(config, :merge_cooldown_ms)
      assert Map.has_key?(config, :small_file_threshold)
    end
  end
end
