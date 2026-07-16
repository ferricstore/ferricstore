defmodule FerricstoreServer.ShutdownTest do
  @moduledoc """
  Tests that verify shutdown behaviour for individual shards.

  Covers spec section 2C.6: after stopping a shard, it is no longer alive;
  pending writes are flushed before shutdown (hint/data files exist on disk).

  These tests do NOT stop the entire application (that would break other
  tests). Instead, they exercise individual shard lifecycle by starting
  isolated shard processes under a temporary supervisor and then stopping
  them cleanly, or by verifying that the application-supervised shards
  flush data to disk.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state
  @moduletag :cluster

  alias Ferricstore.DataDir
  alias Ferricstore.Store.Router
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # Reads a key's value directly from .log files using v2 stateless NIFs.
  # Returns {:ok, value} or :not_found.
  defp read_key_from_disk(shard_dir, key) do
    {:ok, files} = File.ls(shard_dir)

    log_files =
      files
      |> Enum.filter(&String.ends_with?(&1, ".log"))
      |> Enum.sort()

    # Scan all log files, collecting (value_offset, log_path) for every
    # non-tombstone entry that matches `key`. The last one wins (newest write).
    entries =
      for log_name <- log_files,
          log_path = Path.join(shard_dir, log_name),
          {:ok, records} <- [NIF.v2_scan_file(log_path)],
          {^key, offset, _vsize, _expire, tombstone} <- records do
        {offset, log_path, tombstone}
      end

    case List.last(entries) do
      {offset, log_path, false} ->
        NIF.v2_pread_at(log_path, offset)

      _ ->
        :not_found
    end
  end

  defp evict_hot_value(ctx, key) do
    shard_idx = Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_idx)

    assert :ets.update_element(keydir, key, {2, nil}),
           "expected keydir entry for #{inspect(key)}"

    :ok
  end

  # ---------------------------------------------------------------------------
  # After stopping a shard, it is no longer alive
  # ---------------------------------------------------------------------------

  describe "shard not alive after stop" do
    @tag :capture_log
    test "killing a shard makes it temporarily unregistered" do
      name = Router.shard_name(FerricStore.Instance.get(:default), 0)
      old_pid = Process.whereis(name)
      assert is_pid(old_pid) and Process.alive?(old_pid)

      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      # The old PID is definitely not alive.
      refute Process.alive?(old_pid),
             "Old shard PID should not be alive after being killed"

      # Wait for the supervisor to restart it so other tests are not affected.
      ShardHelpers.wait_shards_alive()
    end

    @tag :capture_log
    test "stopped shard is restarted by supervisor with a new PID" do
      name = Router.shard_name(FerricStore.Instance.get(:default), 1)
      old_pid = Process.whereis(name)
      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      ShardHelpers.wait_shards_alive()

      new_pid = Process.whereis(name)
      assert is_pid(new_pid)
      assert Process.alive?(new_pid)
      assert new_pid != old_pid, "Restarted shard should have a different PID"
    end
  end

  # ---------------------------------------------------------------------------
  # Pending writes are flushed before shutdown (data files on disk)
  # ---------------------------------------------------------------------------

  describe "pending writes flushed before shutdown" do
    test "data written to shard is recoverable from its durable backend" do
      ctx = FerricStore.Instance.get(:default)
      key = "shutdown_flush_test_#{:rand.uniform(9_999_999)}"
      value = "must_be_on_disk"

      Router.put(ctx, key, value, 0)

      # Explicitly flush to ensure pending async writes hit disk.
      shard_idx = Router.shard_for(ctx, key)
      shard_name = Router.shard_name(ctx, shard_idx)
      :ok = GenServer.call(shard_name, :flush)
      Ferricstore.Store.BitcaskWriter.flush_all()

      # Force the next read through the keydir's direct-log or WARaft segment location.
      assert :ok = evict_hot_value(ctx, key)
      assert Router.get(ctx, key) == value
    end

    test "multiple keys are persisted and recoverable from the durable backend" do
      ctx = FerricStore.Instance.get(:default)
      prefix = "shutdown_multi_#{:rand.uniform(9_999_999)}"

      keys_and_values =
        for i <- 1..20 do
          k = "#{prefix}_#{i}"
          v = "value_#{i}"
          Router.put(ctx, k, v, 0)
          {k, v}
        end

      # Flush all shards to guarantee writes are on disk.
      ShardHelpers.flush_all_shards()

      # Evict hot copies so each read must use its backend-specific durable location.
      for {k, v} <- keys_and_values do
        assert :ok = evict_hot_value(ctx, k)
        assert Router.get(ctx, k) == v
      end
    end

    test "data directory has files for all active shards" do
      data_dir = Application.fetch_env!(:ferricstore, :data_dir)

      # Write at least one key per shard to ensure all directories have data.
      for i <- 0..3 do
        # Find a key that hashes to shard i.
        key =
          Enum.find_value(1..100_000, fn n ->
            k = "shard_data_probe_#{i}_#{n}"
            if Router.shard_for(FerricStore.Instance.get(:default), k) == i, do: k
          end)

        Router.put(FerricStore.Instance.get(:default), key, "shard_#{i}_data", 0)
      end

      ShardHelpers.flush_all_shards()

      for i <- 0..3 do
        shard_dir = DataDir.shard_data_path(data_dir, i)
        assert File.dir?(shard_dir), "Shard #{i} data directory should exist"

        files = File.ls!(shard_dir)
        assert files != [], "Shard #{i} directory should have data files"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Isolated shard: start, write, stop, verify data persists
  # ---------------------------------------------------------------------------

  describe "isolated shard stop flushes data" do
    @tag :capture_log
    test "starting and stopping an isolated shard preserves written data" do
      # Isolated shard tests bypass WARaft consensus for ad-hoc indices.

      # Start a shard outside the application supervisor tree with a
      # unique index and temporary directory to avoid conflicts.
      tmp_dir =
        Path.join(System.tmp_dir!(), "ferricstore_shutdown_iso_#{:rand.uniform(9_999_999)}")

      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      # Use index 99 to avoid colliding with application shards 0-3.
      iso_ctx = minimal_instance_ctx(tmp_dir)
      opts = [index: 99, data_dir: tmp_dir, flush_interval_ms: 1, instance_ctx: iso_ctx]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      # Write data through the shard.
      :ok = GenServer.call(pid, {:put, "iso_key", "iso_val", 0})
      :ok = GenServer.call(pid, :flush)

      # Stop the shard gracefully.
      GenServer.stop(pid, :normal, 5_000)
      refute Process.alive?(pid)

      # Verify data is on disk.
      shard_dir = DataDir.shard_data_path(tmp_dir, 99)
      assert File.dir?(shard_dir)

      {:ok, recovered} = read_key_from_disk(shard_dir, "iso_key")
      assert recovered == "iso_val", "Data should survive shard stop"
    end
  end

  # ---------------------------------------------------------------------------
  # Spec 2C.6: Graceful shutdown lifecycle
  #
  # terminate/2 must: flush pending writes, write hint file, emit telemetry.
  # OTP invokes terminate/2 when the supervisor stops children in reverse
  # order during application shutdown.
  # ---------------------------------------------------------------------------

  describe "terminate/2 flushes pending writes" do
    @tag :capture_log
    test "after shutdown signal, pending writes are flushed" do
      # Isolated shard tests bypass WARaft consensus for ad-hoc indices.

      # Start an isolated shard so we can stop it without affecting the
      # application supervisor tree.
      tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_term_flush_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      iso_ctx = minimal_instance_ctx(tmp_dir)
      opts = [index: 100, data_dir: tmp_dir, flush_interval_ms: 100, instance_ctx: iso_ctx]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      # Write several keys but do NOT call :flush. These remain in the
      # pending list. terminate/2 must flush them to disk.
      for i <- 1..10 do
        :ok = GenServer.call(pid, {:put, "term_key_#{i}", "term_val_#{i}", 0})
      end

      # Graceful stop triggers terminate/2.
      GenServer.stop(pid, :normal, 5_000)
      refute Process.alive?(pid)

      # Verify all keys survived using v2 stateless NIFs.
      shard_dir = DataDir.shard_data_path(tmp_dir, 100)

      for i <- 1..10 do
        {:ok, recovered} = read_key_from_disk(shard_dir, "term_key_#{i}")

        assert recovered == "term_val_#{i}",
               "Key term_key_#{i} should survive graceful shutdown"
      end
    end
  end

  describe "terminate/2 writes hint files" do
    @tag :capture_log
    test "after shutdown, hint files exist on disk" do
      # Isolated shard tests bypass WARaft consensus for ad-hoc indices.

      tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_term_hint_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      iso_ctx = minimal_instance_ctx(tmp_dir)
      opts = [index: 101, data_dir: tmp_dir, flush_interval_ms: 100, instance_ctx: iso_ctx]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      # Write data so the Bitcask log has entries to reference in the hint.
      :ok = GenServer.call(pid, {:put, "hint_key", "hint_val", 0})

      # Graceful stop triggers terminate/2 which must call NIF.write_hint.
      GenServer.stop(pid, :normal, 5_000)

      # The shard data directory should contain at least one .hint file.
      shard_dir = DataDir.shard_data_path(tmp_dir, 101)
      files = File.ls!(shard_dir)
      hint_files = Enum.filter(files, &String.ends_with?(&1, ".hint"))

      assert hint_files != [],
             "Shard directory should contain .hint file(s) after graceful shutdown, " <>
               "got: #{inspect(files)}"
    end
  end

  describe "terminate/2 emits shutdown telemetry" do
    @tag :capture_log
    test "shard terminate/2 emits [:ferricstore, :shard, :shutdown] telemetry" do
      # Isolated shard tests bypass WARaft consensus for ad-hoc indices.

      tmp_dir = Path.join(System.tmp_dir!(), "ferricstore_term_telem_#{:rand.uniform(9_999_999)}")
      File.mkdir_p!(tmp_dir)

      on_exit(fn ->
        File.rm_rf(tmp_dir)
      end)

      # Attach a telemetry handler to capture the shutdown event.
      test_pid = self()
      handler_id = "shutdown_test_#{:rand.uniform(9_999_999)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :shard, :shutdown],
        fn event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event_name, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      iso_ctx = minimal_instance_ctx(tmp_dir)
      opts = [index: 102, data_dir: tmp_dir, flush_interval_ms: 100, instance_ctx: iso_ctx]
      {:ok, pid} = GenServer.start_link(Ferricstore.Store.Shard, opts)

      :ok = GenServer.call(pid, {:put, "telem_key", "telem_val", 0})
      GenServer.stop(pid, :normal, 5_000)

      assert_receive {:telemetry_event, [:ferricstore, :shard, :shutdown], measurements,
                      metadata},
                     1_000

      assert is_integer(measurements.flush_duration_us)
      assert measurements.flush_duration_us >= 0
      assert is_integer(measurements.hint_duration_us)
      assert measurements.hint_duration_us >= 0
      assert metadata.shard_index == 102
    end
  end

  defp minimal_instance_ctx(data_dir) do
    n = 128
    nil_tuple = List.to_tuple(List.duplicate(nil, n))

    %FerricStore.Instance{
      name: :"shutdown_test_#{:erlang.unique_integer([:positive])}",
      data_dir: data_dir,
      shard_count: n,
      slot_map: nil_tuple,
      shard_names: nil_tuple,
      keydir_refs: nil_tuple,
      pressure_flags: :atomics.new(3, signed: false),
      disk_pressure: :atomics.new(n, signed: false),
      checkpoint_flags: :atomics.new(n, signed: false),
      write_version: :counters.new(n, [:write_concurrency]),
      stats_counter: :counters.new(10, [:atomics]),
      lfu_decay_time: 1,
      lfu_log_factor: 10,
      lfu_initial_ref: :atomics.new(2, signed: false),
      hot_cache_max_value_size: 65_536,
      sync_flush_timeout_ms: 5_000,
      max_active_file_size: 64 * 1024 * 1024,
      read_sample_rate: 1,
      eviction_policy: :volatile_lfu,
      max_memory_bytes: 1_073_741_824,
      keydir_max_ram: 256 * 1024 * 1024,
      memory_limit: 1_073_741_824,
      keydir_binary_bytes: :atomics.new(n, signed: true),
      latch_refs: nil_tuple,
      hotness_table: :ets.new(:test_hotness, [:set, :public]),
      config_table: :ets.new(:test_config, [:set, :public]),
      connected_clients_fn: fn -> 0 end,
      process_rss_fn: nil,
      server_info_fn: fn -> %{} end
    }
  end
end
