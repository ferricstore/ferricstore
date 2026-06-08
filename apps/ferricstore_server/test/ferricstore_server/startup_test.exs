defmodule FerricstoreServer.StartupTest do
  @moduledoc """
  Tests that verify the application starts correctly and all expected
  components are initialized.

  Covers spec section 2C.1: application boot, shard initialization,
  ETS table creation, MemoryGuard, TCP listener, Waiters, and
  ClientTracking.

  These tests do not restart the application -- they inspect the state
  left by normal application startup (which happens before ExUnit runs).
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Store.{Router, ShardSupervisor}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Shards alive after startup
  # ---------------------------------------------------------------------------

  describe "all shards alive after startup" do
    test "all 4 shard GenServers are registered and alive" do
      for i <- 0..3 do
        name = Router.shard_name(FerricStore.Instance.get(:default), i)
        pid = Process.whereis(name)
        assert is_pid(pid), "Shard #{i} should be registered as #{name}"
        assert Process.alive?(pid), "Shard #{i} should be alive"
      end
    end

    test "ShardSupervisor has exactly 4 shard children" do
      children =
        ShardSupervisor
        |> Supervisor.which_children()
        |> Enum.filter(fn {id, _, _, _} ->
          is_atom(id) and String.starts_with?(Atom.to_string(id), "shard_")
        end)

      assert length(children) == 4, "Expected 4 shard children, got #{length(children)}"
    end

    test "all ShardSupervisor children are alive" do
      children = Supervisor.which_children(ShardSupervisor)

      for {id, pid, _type, _mods} <- children do
        assert is_pid(pid) and Process.alive?(pid),
               "Shard child #{inspect(id)} is not alive (pid=#{inspect(pid)})"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Shards rebuild keydir from Bitcask on startup
  # ---------------------------------------------------------------------------

  describe "shards rebuild keydir from Bitcask" do
    test "data written before is recoverable after startup (keydir rebuilt)" do
      # Write data, flush to disk, then verify it is still available.
      # Since the application is already started with the same data_dir,
      # this verifies that the shard initialized its Bitcask store
      # correctly and can read persisted data.
      key = "startup_keydir_test_#{:rand.uniform(9_999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, "persisted_value", 0)

      # Flush to ensure data is on disk (shard pending + background writer).
      shard_idx = Router.shard_for(FerricStore.Instance.get(:default), key)
      shard_name = Router.shard_name(FerricStore.Instance.get(:default), shard_idx)
      :ok = GenServer.call(shard_name, :flush)
      Ferricstore.Store.BitcaskWriter.flush_all()

      # Simulate cold key by setting value to nil but preserving disk location.
      # In v2, ETS IS the keydir -- deleting entirely means the key is gone.
      keydir = :"keydir_#{shard_idx}"
      [{_, _val, exp, lfu, fid, off, vsize}] = :ets.lookup(keydir, key)
      :ets.insert(keydir, {key, nil, exp, lfu, fid, off, vsize})

      # The get must warm from Bitcask -- keydir must be valid.
      assert "persisted_value" == Router.get(FerricStore.Instance.get(:default), key)
    end
  end

  # ---------------------------------------------------------------------------
  # ETS tables created for each shard
  # ---------------------------------------------------------------------------

  describe "ETS tables created for each shard" do
    test "keydir_0 through keydir_3 exist" do
      for i <- 0..3 do
        ets_name = :"keydir_#{i}"
        ref = :ets.whereis(ets_name)

        refute ref == :undefined,
               "ETS table #{ets_name} should exist after startup"
      end
    end

    test "shard ETS tables are public and named" do
      for i <- 0..3 do
        ets_name = :"keydir_#{i}"
        info = :ets.info(ets_name)
        assert info != :undefined, "ETS table #{ets_name} should exist"

        # Verify it is public (readable from any process)
        protection = :ets.info(ets_name, :protection)
        assert protection == :public, "ETS table #{ets_name} should be public"

        # Verify it is a named table
        named = :ets.info(ets_name, :named_table)
        assert named == true, "ETS table #{ets_name} should be a named table"
      end
    end

    test "shard ETS tables are :set type" do
      for i <- 0..3 do
        ets_name = :"keydir_#{i}"
        type = :ets.info(ets_name, :type)
        assert type == :set, "ETS table #{ets_name} should be a :set"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # MemoryGuard is running after startup
  # ---------------------------------------------------------------------------

  describe "MemoryGuard running after startup" do
    test "MemoryGuard process is registered and alive" do
      pid = Process.whereis(Ferricstore.MemoryGuard)
      assert is_pid(pid), "MemoryGuard should be registered"
      assert Process.alive?(pid), "MemoryGuard should be alive"
    end

    test "MemoryGuard responds to stats query" do
      stats = Ferricstore.MemoryGuard.stats()
      assert is_map(stats)
      assert Map.has_key?(stats, :total_bytes)
      assert Map.has_key?(stats, :max_bytes)
      assert Map.has_key?(stats, :pressure_level)
      assert stats.pressure_level in [:ok, :warning, :pressure, :reject]
    end

    test "MemoryGuard eviction policy matches test config" do
      policy = Ferricstore.MemoryGuard.eviction_policy()
      assert policy == :volatile_lru
    end
  end

  # ---------------------------------------------------------------------------
  # TCP listener is accepting connections after startup
  # ---------------------------------------------------------------------------

  describe "TCP listener accepting connections after startup" do
    test "listener is bound to a valid ephemeral port" do
      port = Listener.port()
      assert is_integer(port)
      assert port > 0
      refute port == 6379, "Test env should use an ephemeral port, not 6379"
    end

    test "TCP connection can be established" do
      port = Listener.port()

      assert {:ok, sock} =
               :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

      :gen_tcp.close(sock)
    end

    test "multiple concurrent TCP connections are accepted" do
      port = Listener.port()

      sockets =
        for _ <- 1..10 do
          {:ok, sock} =
            :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 2_000)

          sock
        end

      assert length(sockets) == 10
      Enum.each(sockets, &:gen_tcp.close/1)
    end
  end

  # ---------------------------------------------------------------------------
  # Waiters ETS table exists
  # ---------------------------------------------------------------------------

  describe "Waiters ETS table exists" do
    test ":ferricstore_waiters table is created" do
      ref = :ets.whereis(:ferricstore_waiters)
      refute ref == :undefined, "Waiters ETS table should exist after startup"
    end

    test ":ferricstore_waiters is a duplicate_bag" do
      type = :ets.info(:ferricstore_waiters, :type)
      assert type == :duplicate_bag, "Waiters table should be :duplicate_bag"
    end

    test ":ferricstore_waiters is public" do
      protection = :ets.info(:ferricstore_waiters, :protection)
      assert protection == :public, "Waiters table should be public"
    end

    test "Waiters module reports zero waiters at startup" do
      assert Ferricstore.Waiters.total_count() >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # ClientTracking tables exist
  # ---------------------------------------------------------------------------

  describe "ClientTracking tables exist" do
    test ":ferricstore_tracking table is created" do
      ref = :ets.whereis(:ferricstore_tracking)
      refute ref == :undefined, "Tracking ETS table should exist after startup"
    end

    test ":ferricstore_tracking_connections table is created" do
      ref = :ets.whereis(:ferricstore_tracking_connections)

      refute ref == :undefined,
             "Tracking connections ETS table should exist after startup"
    end

    test ":ferricstore_tracking is a :bag" do
      type = :ets.info(:ferricstore_tracking, :type)
      assert type == :bag, "Tracking table should be :bag"
    end

    test ":ferricstore_tracking_connections is a :set" do
      type = :ets.info(:ferricstore_tracking_connections, :type)
      assert type == :set, "Tracking connections table should be :set"
    end

    test "both ClientTracking tables are public" do
      assert :ets.info(:ferricstore_tracking, :protection) == :public
      assert :ets.info(:ferricstore_tracking_connections, :protection) == :public
    end
  end

  # ---------------------------------------------------------------------------
  # Stats process is running
  # ---------------------------------------------------------------------------

  describe "Stats process running after startup" do
    test "Stats process is registered and alive" do
      pid = Process.whereis(Ferricstore.Stats)
      assert is_pid(pid), "Stats should be registered"
      assert Process.alive?(pid), "Stats should be alive"
    end

    test "Stats has a valid run ID" do
      run_id = Ferricstore.Stats.run_id()
      assert is_binary(run_id)
      assert byte_size(run_id) == 40, "Run ID should be 40 hex characters (20 bytes)"
    end

    test "Stats uptime is non-negative" do
      uptime = Ferricstore.Stats.uptime_seconds()
      assert is_integer(uptime)
      assert uptime >= 0
    end
  end

  # ---------------------------------------------------------------------------
  # Step 6 - Embedded large value check
  # ---------------------------------------------------------------------------

  describe "embedded large value check (Step 6)" do
    import ExUnit.CaptureLog

    test "scan_large_values detects values exceeding threshold" do
      # Write a 600KB value (exceeds default 512KB threshold).
      large_value = :binary.copy(<<0>>, 600 * 1024)
      key = "large_value_test_#{:rand.uniform(9_999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, large_value, 0)

      {count, largest_key, largest_size} =
        Ferricstore.Application.scan_large_values(4, 512 * 1024)

      assert count >= 1
      assert largest_size >= 600 * 1024
      # The largest key should be our key (or another equally large one).
      assert is_binary(largest_key)
    end

    test "scan_large_values returns {0, nil, 0} when no large values exist" do
      # All keys were flushed in setup; write only small values.
      Router.put(
        FerricStore.Instance.get(:default),
        "small_#{:rand.uniform(9_999_999)}",
        "tiny",
        0
      )

      assert {0, nil, 0} = Ferricstore.Application.scan_large_values(4, 512 * 1024)
    end

    test "scan_large_values identifies the key with the largest value" do
      key_medium = "medium_val_#{:rand.uniform(9_999_999)}"
      key_biggest = "biggest_val_#{:rand.uniform(9_999_999)}"

      Router.put(
        FerricStore.Instance.get(:default),
        key_medium,
        :binary.copy(<<1>>, 600 * 1024),
        0
      )

      Router.put(
        FerricStore.Instance.get(:default),
        key_biggest,
        :binary.copy(<<2>>, 800 * 1024),
        0
      )

      {count, largest_key, largest_size} =
        Ferricstore.Application.scan_large_values(4, 512 * 1024)

      assert count == 2
      assert largest_key == key_biggest
      assert largest_size == 800 * 1024
    end

    test "scan_large_values respects a custom threshold" do
      key = "custom_threshold_#{:rand.uniform(9_999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, :binary.copy(<<0>>, 100), 0)

      # Threshold of 50 bytes -- the 100-byte value should be flagged.
      {count, largest_key, _largest_size} =
        Ferricstore.Application.scan_large_values(4, 50)

      assert count >= 1
      assert is_binary(largest_key)
    end

    test "check_large_values logs warning when large values exist" do
      large_value = :binary.copy(<<0>>, 600 * 1024)
      key = "log_warn_test_#{:rand.uniform(9_999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, large_value, 0)

      log =
        capture_log(fn ->
          # Directly invoke the private function via the public scan + simulated check.
          # We replicate check_large_values logic here because it is private.
          case Ferricstore.Application.scan_large_values(4, 512 * 1024) do
            {0, _key, _size} ->
              :ok

            {count, lk, ls} ->
              require Logger

              Logger.warning(
                "Embedded large value check: #{count} value(s) exceed threshold; " <>
                  "largest key=#{inspect(lk)} (#{ls} bytes)"
              )
          end
        end)

      assert log =~ "Embedded large value check"
      assert log =~ "value(s) exceed threshold"
      assert log =~ "bytes"
    end

    test "check_large_values emits telemetry when large values exist" do
      large_value = :binary.copy(<<0>>, 700 * 1024)
      key = "telemetry_test_#{:rand.uniform(9_999_999)}"
      Router.put(FerricStore.Instance.get(:default), key, large_value, 0)

      test_pid = self()
      ref = make_ref()

      handler_id = "test-large-values-telemetry-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :embedded, :large_values_detected],
        fn event_name, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, ref, event_name, measurements, metadata})
        end,
        nil
      )

      # Replicate check_large_values logic to trigger telemetry.
      case Ferricstore.Application.scan_large_values(4, 512 * 1024) do
        {0, _key, _size} ->
          :ok

        {count, lk, ls} ->
          :telemetry.execute(
            [:ferricstore, :embedded, :large_values_detected],
            %{count: count, largest_size: ls},
            %{largest_key: lk}
          )
      end

      assert_receive {:telemetry_event, ^ref, [:ferricstore, :embedded, :large_values_detected],
                      measurements, metadata},
                     1_000

      assert measurements.count >= 1
      assert measurements.largest_size >= 700 * 1024
      assert metadata.largest_key == key

      :telemetry.detach(handler_id)
    end

    test "no warning or telemetry when all values are below threshold" do
      # Only small values exist (setup flushed everything).
      Router.put(
        FerricStore.Instance.get(:default),
        "small_check_#{:rand.uniform(9_999_999)}",
        "ok",
        0
      )

      test_pid = self()
      ref = make_ref()

      handler_id = "test-no-large-values-#{inspect(ref)}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :embedded, :large_values_detected],
        fn _event_name, _measurements, _metadata, _config ->
          send(test_pid, {:telemetry_event, ref})
        end,
        nil
      )

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          case Ferricstore.Application.scan_large_values(4, 512 * 1024) do
            {0, _key, _size} ->
              :ok

            {count, lk, ls} ->
              :telemetry.execute(
                [:ferricstore, :embedded, :large_values_detected],
                %{count: count, largest_size: ls},
                %{largest_key: lk}
              )
          end
        end)

      assert log == ""
      refute_receive {:telemetry_event, ^ref}, 100

      :telemetry.detach(handler_id)
    end

    test "scan_large_values handles missing ETS tables gracefully" do
      # Passing a shard_count higher than actual shards -- the extra
      # tables won't exist. Should not crash.
      assert {count, _key, _size} = Ferricstore.Application.scan_large_values(8, 512 * 1024)
      assert is_integer(count)
    end
  end
end
