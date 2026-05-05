defmodule FerricstoreServer.ClientTrackingTest do
  @moduledoc """
  Tests for the `FerricstoreServer.ClientTracking` module.

  Covers default tracking mode, BCAST mode, OPTIN/OPTOUT modes, NOLOOP,
  invalidation dispatch, and the CLIENT TRACKING/CACHING command handlers.
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias FerricstoreServer.ClientTracking

  setup do
    # Ensure tracking tables exist
    ClientTracking.init_tables()

    # Clean up any leftover state from previous tests
    try do
      :ets.delete_all_objects(:ferricstore_tracking)
      :ets.delete_all_objects(:ferricstore_tracking_connections)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  # ---------------------------------------------------------------------------
  # Basic enable/disable
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKING ON enables tracking" do
    test "default config has tracking disabled" do
      config = ClientTracking.new_config()
      assert config.enabled == false
      assert config.mode == :default
      assert config.prefixes == []
      assert config.redirect == nil
      assert config.optin == false
      assert config.optout == false
      assert config.noloop == false
    end

    test "enable/3 sets enabled to true" do
      config = ClientTracking.new_config()
      {:ok, new_config} = ClientTracking.enable(self(), config)

      assert new_config.enabled == true
      assert new_config.mode == :default
    end

    test "enable/3 with BCAST mode" do
      config = ClientTracking.new_config()
      {:ok, new_config} = ClientTracking.enable(self(), config, mode: :bcast)

      assert new_config.enabled == true
      assert new_config.mode == :bcast
    end

    test "enable/3 with prefixes" do
      config = ClientTracking.new_config()

      {:ok, new_config} =
        ClientTracking.enable(self(), config,
          mode: :bcast,
          prefixes: ["user:", "session:"]
        )

      assert new_config.prefixes == ["user:", "session:"]
    end

    test "enable/3 with OPTIN" do
      config = ClientTracking.new_config()
      {:ok, new_config} = ClientTracking.enable(self(), config, optin: true)

      assert new_config.optin == true
      assert new_config.caching == false
    end

    test "enable/3 with OPTOUT" do
      config = ClientTracking.new_config()
      {:ok, new_config} = ClientTracking.enable(self(), config, optout: true)

      assert new_config.optout == true
    end

    test "enable/3 with NOLOOP" do
      config = ClientTracking.new_config()
      {:ok, new_config} = ClientTracking.enable(self(), config, noloop: true)

      assert new_config.noloop == true
    end

    test "enable/3 rejects OPTIN + OPTOUT" do
      config = ClientTracking.new_config()
      {:error, msg} = ClientTracking.enable(self(), config, optin: true, optout: true)

      assert msg =~ "mutually exclusive"
    end

    test "enable/3 rejects OPTIN + BCAST" do
      config = ClientTracking.new_config()
      {:error, msg} = ClientTracking.enable(self(), config, mode: :bcast, optin: true)

      assert msg =~ "not compatible with BCAST"
    end

    test "enable/3 rejects OPTOUT + BCAST" do
      config = ClientTracking.new_config()
      {:error, msg} = ClientTracking.enable(self(), config, mode: :bcast, optout: true)

      assert msg =~ "not compatible with BCAST"
    end
  end

  describe "CLIENT TRACKING OFF disables tracking" do
    test "disable/2 sets enabled to false and clears state" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config, noloop: true)
      assert enabled_config.enabled == true

      {:ok, disabled_config} = ClientTracking.disable(self(), enabled_config)
      assert disabled_config.enabled == false
      assert disabled_config.prefixes == []
      assert disabled_config.redirect == nil
    end

    test "disable/2 removes tracked keys from ETS" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config)

      # Track a key
      ClientTracking.track_key(self(), "mykey", enabled_config)

      # Verify the key is tracked
      assert :ets.lookup(:ferricstore_tracking, "mykey") == [{"mykey", self()}]

      # Disable tracking
      ClientTracking.disable(self(), enabled_config)

      # Key should no longer be tracked
      assert :ets.lookup(:ferricstore_tracking, "mykey") == []
    end
  end

  # ---------------------------------------------------------------------------
  # Key tracking and invalidation (default mode)
  # ---------------------------------------------------------------------------

  describe "tracked key modification sends invalidation" do
    test "track_key/3 records key for enabled connection" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config)

      ClientTracking.track_key(self(), "key1", enabled_config)

      entries = :ets.lookup(:ferricstore_tracking, "key1")
      assert length(entries) == 1
      assert {"key1", self()} in entries
    end

    test "track_key/3 is no-op when tracking is disabled" do
      config = ClientTracking.new_config()
      ClientTracking.track_key(self(), "key1", config)

      assert :ets.lookup(:ferricstore_tracking, "key1") == []
    end

    test "notify_key_modified/3 sends invalidation to tracking connections" do
      # Spawn a "tracked connection" process
      test_pid = self()

      tracked_pid =
        spawn(fn ->
          receive do
            {:send_invalidation, msg} ->
              send(test_pid, {:got_invalidation, msg})
          end
        end)

      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(tracked_pid, config)

      # Track the key from the tracked connection
      ClientTracking.track_key(tracked_pid, "mykey", enabled_config)

      # Writer modifies the key
      writer_pid = spawn(fn -> :timer.sleep(1000) end)

      sender = fn pid, msg, _keys ->
        send(pid, {:send_invalidation, msg})
        :ok
      end

      ClientTracking.notify_key_modified("mykey", writer_pid, sender)

      # Verify the tracked connection received the invalidation
      assert_receive {:got_invalidation, _msg}, 1000
    end

    test "invalidation removes tracked key (one-shot)" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config)

      ClientTracking.track_key(self(), "mykey", enabled_config)
      assert :ets.lookup(:ferricstore_tracking, "mykey") != []

      writer_pid = spawn(fn -> :timer.sleep(1000) end)
      sender = fn _pid, _msg -> :ok end

      ClientTracking.notify_key_modified("mykey", writer_pid, sender)

      # After invalidation, the tracking entry should be removed
      assert :ets.lookup(:ferricstore_tracking, "mykey") == []
    end

    test "track_keys/3 tracks multiple keys" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config)

      ClientTracking.track_keys(self(), ["k1", "k2", "k3"], enabled_config)

      assert :ets.lookup(:ferricstore_tracking, "k1") == [{"k1", self()}]
      assert :ets.lookup(:ferricstore_tracking, "k2") == [{"k2", self()}]
      assert :ets.lookup(:ferricstore_tracking, "k3") == [{"k3", self()}]
    end

    test "notify_keys_modified/3 coalesces default-mode invalidations per connection" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config)

      ClientTracking.track_keys(self(), ["bulk1", "bulk2"], enabled_config)

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, _msg, keys -> send(self(), {:sent, pid, keys}) end

      ClientTracking.notify_keys_modified(["bulk1", "bulk2"], writer, sender)

      assert_receive {:sent, pid, ["bulk1", "bulk2"]}
      assert pid == self()
      refute_receive {:sent, _, _}
      assert :ets.lookup(:ferricstore_tracking, "bulk1") == []
      assert :ets.lookup(:ferricstore_tracking, "bulk2") == []
    end

    test "notify_keys_modified/3 honors NOLOOP while coalescing default-mode keys" do
      config = ClientTracking.new_config()
      {:ok, enabled_config} = ClientTracking.enable(self(), config, noloop: true)

      ClientTracking.track_keys(self(), ["self_bulk1", "self_bulk2"], enabled_config)

      sender = fn pid, _msg, keys -> send(self(), {:sent, pid, keys}) end

      ClientTracking.notify_keys_modified(["self_bulk1", "self_bulk2"], self(), sender)

      refute_receive {:sent, _, _}
      assert :ets.lookup(:ferricstore_tracking, "self_bulk1") == []
      assert :ets.lookup(:ferricstore_tracking, "self_bulk2") == []
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple connections tracking same key
  # ---------------------------------------------------------------------------

  describe "multiple connections tracking same key" do
    test "both connections receive invalidation" do
      test_pid = self()

      conn1 =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:conn1_invalidated})
          end
        end)

      conn2 =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:conn2_invalidated})
          end
        end)

      config1 = ClientTracking.new_config()
      {:ok, enabled1} = ClientTracking.enable(conn1, config1)
      ClientTracking.track_key(conn1, "shared_key", enabled1)

      config2 = ClientTracking.new_config()
      {:ok, enabled2} = ClientTracking.enable(conn2, config2)
      ClientTracking.track_key(conn2, "shared_key", enabled2)

      # Verify both are tracking
      entries = :ets.lookup(:ferricstore_tracking, "shared_key")
      assert length(entries) == 2

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      ClientTracking.notify_key_modified("shared_key", writer, sender)

      assert_receive {:conn1_invalidated}, 1000
      assert_receive {:conn2_invalidated}, 1000
    end
  end

  # ---------------------------------------------------------------------------
  # BCAST mode
  # ---------------------------------------------------------------------------

  describe "BCAST mode sends invalidation for prefix matches" do
    test "prefix match triggers invalidation" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:bcast_invalidated})
          end
        end)

      config = ClientTracking.new_config()

      {:ok, _enabled} =
        ClientTracking.enable(conn, config, mode: :bcast, prefixes: ["user:"])

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      # Write a key that matches the prefix
      ClientTracking.notify_key_modified("user:42", writer, sender)

      assert_receive {:bcast_invalidated}, 1000
    end

    test "prefix mismatch does not trigger invalidation" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:bcast_invalidated})
          after
            100 -> send(test_pid, {:no_invalidation})
          end
        end)

      config = ClientTracking.new_config()

      {:ok, _enabled} =
        ClientTracking.enable(conn, config, mode: :bcast, prefixes: ["user:"])

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      # Write a key that does NOT match the prefix
      ClientTracking.notify_key_modified("order:99", writer, sender)

      assert_receive {:no_invalidation}, 500
    end

    test "empty prefix list matches all keys" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:bcast_invalidated})
          end
        end)

      config = ClientTracking.new_config()
      {:ok, _enabled} = ClientTracking.enable(conn, config, mode: :bcast, prefixes: [])

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      ClientTracking.notify_key_modified("anything:here", writer, sender)

      assert_receive {:bcast_invalidated}, 1000
    end

    test "notify_keys_modified/3 coalesces BCAST invalidations per connection" do
      config = ClientTracking.new_config()
      {:ok, _enabled} = ClientTracking.enable(self(), config, mode: :bcast, prefixes: ["user:"])

      writer = spawn(fn -> :timer.sleep(1000) end)
      sender = fn pid, _msg, keys -> send(self(), {:sent, pid, keys}) end

      ClientTracking.notify_keys_modified(["user:1", "order:1", "user:2"], writer, sender)

      assert_receive {:sent, pid, ["user:1", "user:2"]}
      assert pid == self()
      refute_receive {:sent, _, _}
    end

    test "BCAST mode does not track individual keys" do
      config = ClientTracking.new_config()
      {:ok, bcast_config} = ClientTracking.enable(self(), config, mode: :bcast)

      # track_key should be a no-op in BCAST mode
      result = ClientTracking.track_key(self(), "mykey", bcast_config)
      assert result == bcast_config
      assert :ets.lookup(:ferricstore_tracking, "mykey") == []
    end
  end

  # ---------------------------------------------------------------------------
  # OPTIN mode
  # ---------------------------------------------------------------------------

  describe "OPTIN mode only tracks after CLIENT CACHING yes" do
    test "key is not tracked without CLIENT CACHING yes" do
      config = ClientTracking.new_config()
      {:ok, optin_config} = ClientTracking.enable(self(), config, optin: true)

      # caching should be false initially in OPTIN mode
      assert optin_config.caching == false

      # Track attempt should be no-op
      result = ClientTracking.track_key(self(), "mykey", optin_config)
      assert result == optin_config
      assert :ets.lookup(:ferricstore_tracking, "mykey") == []
    end

    test "key is tracked after CLIENT CACHING yes" do
      config = ClientTracking.new_config()
      {:ok, optin_config} = ClientTracking.enable(self(), config, optin: true)

      # Enable caching
      {:ok, caching_config} = ClientTracking.set_caching(optin_config, true)
      assert caching_config.caching == true

      # Now track should work
      result = ClientTracking.track_key(self(), "mykey", caching_config)
      assert :ets.lookup(:ferricstore_tracking, "mykey") == [{"mykey", self()}]

      # Caching flag should reset to false after one tracked command
      assert result.caching == false
    end

    test "track_keys/3 tracks every key for one CLIENT CACHING yes command" do
      config = ClientTracking.new_config()
      {:ok, optin_config} = ClientTracking.enable(self(), config, optin: true)
      {:ok, caching_config} = ClientTracking.set_caching(optin_config, true)

      result = ClientTracking.track_keys(self(), ["multi1", "multi2", "multi3"], caching_config)

      assert :ets.lookup(:ferricstore_tracking, "multi1") == [{"multi1", self()}]
      assert :ets.lookup(:ferricstore_tracking, "multi2") == [{"multi2", self()}]
      assert :ets.lookup(:ferricstore_tracking, "multi3") == [{"multi3", self()}]
      assert result.caching == false
    end

    test "CLIENT CACHING fails when tracking is disabled" do
      config = ClientTracking.new_config()
      {:error, msg} = ClientTracking.set_caching(config, true)
      assert msg =~ "CLIENT CACHING can be called only after"
    end

    test "CLIENT CACHING fails without OPTIN or OPTOUT mode" do
      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(self(), config)
      {:error, msg} = ClientTracking.set_caching(enabled, true)
      assert msg =~ "OPTIN or OPTOUT mode"
    end
  end

  # ---------------------------------------------------------------------------
  # OPTOUT mode
  # ---------------------------------------------------------------------------

  describe "OPTOUT mode tracks by default, skips on CLIENT CACHING no" do
    test "key is tracked by default in OPTOUT mode" do
      config = ClientTracking.new_config()
      {:ok, optout_config} = ClientTracking.enable(self(), config, optout: true)

      ClientTracking.track_key(self(), "mykey", optout_config)
      assert :ets.lookup(:ferricstore_tracking, "mykey") == [{"mykey", self()}]
    end

    test "CLIENT CACHING no skips tracking for next command" do
      config = ClientTracking.new_config()
      {:ok, optout_config} = ClientTracking.enable(self(), config, optout: true)

      {:ok, skip_config} = ClientTracking.set_caching(optout_config, false)
      assert skip_config.caching == false

      # This track should be skipped
      result = ClientTracking.track_key(self(), "skipped_key", skip_config)
      assert :ets.lookup(:ferricstore_tracking, "skipped_key") == []

      # After skipping, caching should reset to true
      assert result.caching == true

      # Next track should work normally
      ClientTracking.track_key(self(), "next_key", result)
      assert :ets.lookup(:ferricstore_tracking, "next_key") == [{"next_key", self()}]
    end

    test "track_keys/3 skips every key for one CLIENT CACHING no command" do
      config = ClientTracking.new_config()
      {:ok, optout_config} = ClientTracking.enable(self(), config, optout: true)
      {:ok, skip_config} = ClientTracking.set_caching(optout_config, false)

      result = ClientTracking.track_keys(self(), ["skip1", "skip2", "skip3"], skip_config)

      assert :ets.lookup(:ferricstore_tracking, "skip1") == []
      assert :ets.lookup(:ferricstore_tracking, "skip2") == []
      assert :ets.lookup(:ferricstore_tracking, "skip3") == []
      assert result.caching == true
    end
  end

  # ---------------------------------------------------------------------------
  # NOLOOP
  # ---------------------------------------------------------------------------

  describe "NOLOOP does not self-invalidate" do
    test "writer with NOLOOP does not receive own invalidation" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:self_invalidated})
          after
            200 -> send(test_pid, {:no_self_invalidation})
          end
        end)

      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(conn, config, noloop: true)

      # conn tracks a key and then modifies it itself
      ClientTracking.track_key(conn, "my_key", enabled)

      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      # conn is both the tracker and the writer
      ClientTracking.notify_key_modified("my_key", conn, sender)

      assert_receive {:no_self_invalidation}, 500
    end

    test "writer without NOLOOP does receive own invalidation" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:self_invalidated})
          after
            200 -> send(test_pid, {:no_self_invalidation})
          end
        end)

      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(conn, config, noloop: false)

      ClientTracking.track_key(conn, "my_key", enabled)

      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      ClientTracking.notify_key_modified("my_key", conn, sender)

      assert_receive {:self_invalidated}, 500
    end
  end

  # ---------------------------------------------------------------------------
  # NOLOOP in BCAST mode
  # ---------------------------------------------------------------------------

  describe "NOLOOP in BCAST mode" do
    test "writer with NOLOOP in BCAST mode does not receive own invalidation" do
      test_pid = self()

      conn =
        spawn(fn ->
          receive do
            {:send_invalidation, _msg} -> send(test_pid, {:self_invalidated})
          after
            200 -> send(test_pid, {:no_self_invalidation})
          end
        end)

      config = ClientTracking.new_config()

      {:ok, _enabled} =
        ClientTracking.enable(conn, config, mode: :bcast, noloop: true, prefixes: ["user:"])

      sender = fn pid, msg, _keys -> send(pid, {:send_invalidation, msg}) && :ok end

      # conn writes a key matching its own prefix
      ClientTracking.notify_key_modified("user:42", conn, sender)

      assert_receive {:no_self_invalidation}, 500
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT TRACKINGINFO
  # ---------------------------------------------------------------------------

  describe "CLIENT TRACKINGINFO returns current state" do
    test "returns flags and redirect info" do
      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(self(), config, optin: true, noloop: true)

      info = ClientTracking.tracking_info(enabled)

      assert "optin" in info["flags"]
      assert "noloop" in info["flags"]
      assert info["redirect"] == -1
      assert info["prefixes"] == []
    end

    test "returns prefixes for BCAST mode" do
      config = ClientTracking.new_config()

      {:ok, enabled} =
        ClientTracking.enable(self(), config, mode: :bcast, prefixes: ["user:", "session:"])

      info = ClientTracking.tracking_info(enabled)
      assert info["prefixes"] == ["user:", "session:"]
    end

    test "returns off flags when no special modes are active" do
      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(self(), config)

      info = ClientTracking.tracking_info(enabled)
      assert info["flags"] == ["off"]
    end
  end

  # ---------------------------------------------------------------------------
  # CLIENT GETREDIR
  # ---------------------------------------------------------------------------

  describe "CLIENT GETREDIR" do
    test "returns 0 when no redirect is set" do
      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(self(), config)

      assert ClientTracking.get_redirect(enabled) == 0
    end
  end

  # ---------------------------------------------------------------------------
  # Invalidation message encoding
  # ---------------------------------------------------------------------------

  describe "encode_invalidation/1" do
    test "encodes a single key invalidation" do
      msg = ClientTracking.encode_invalidation(["mykey"]) |> IO.iodata_to_binary()

      # RESP3 push: >2\r\n+invalidate\r\n*1\r\n$5\r\nmykey\r\n
      assert msg =~ ">2\r\n"
      assert msg =~ "+invalidate\r\n"
      assert msg =~ "$5\r\nmykey\r\n"
    end

    test "encodes multiple key invalidation" do
      msg = ClientTracking.encode_invalidation(["k1", "k2"]) |> IO.iodata_to_binary()

      assert msg =~ ">2\r\n"
      assert msg =~ "+invalidate\r\n"
      assert msg =~ "*2\r\n"
      assert msg =~ "$2\r\nk1\r\n"
      assert msg =~ "$2\r\nk2\r\n"
    end
  end

  # ---------------------------------------------------------------------------
  # Cleanup
  # ---------------------------------------------------------------------------

  describe "cleanup/1" do
    test "removes all tracking state for a connection" do
      config = ClientTracking.new_config()
      {:ok, enabled} = ClientTracking.enable(self(), config)

      ClientTracking.track_key(self(), "k1", enabled)
      ClientTracking.track_key(self(), "k2", enabled)

      assert :ets.lookup(:ferricstore_tracking, "k1") != []
      assert :ets.lookup(:ferricstore_tracking, "k2") != []
      assert :ets.lookup(:ferricstore_tracking_connections, self()) != []

      ClientTracking.cleanup(self())

      assert :ets.lookup(:ferricstore_tracking, "k1") == []
      assert :ets.lookup(:ferricstore_tracking, "k2") == []
      assert :ets.lookup(:ferricstore_tracking_connections, self()) == []
    end

    test "cleanup is safe to call when tables don't exist or are empty" do
      # Should not raise
      assert ClientTracking.cleanup(self()) == :ok
    end
  end
end
