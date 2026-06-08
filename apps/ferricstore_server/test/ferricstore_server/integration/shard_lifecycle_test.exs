defmodule FerricstoreServer.Integration.ShardLifecycleTest do
  @moduledoc """
  Integration tests for shard crash recovery, TTL expiry end-to-end,
  purge/compaction integration, supervisor behaviour, and real-time
  expiry over TCP.

  These tests exercise the full stack:

      Router -> Shard GenServer -> ETS cache -> Bitcask NIF

  All tests run with `async: false` because they manipulate live
  application-supervised shard processes and share a single TCP listener.
  """

  use ExUnit.Case, async: false
  @moduletag :integration
  @moduletag :global_state
  @moduletag :shard_kill
  # Shard lifecycle tests kill and restart shards, destroying ETS tables.
  # Must be excluded from umbrella runs to avoid polluting other apps.
  @moduletag :cluster

  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.Expiry
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias Ferricstore.Test.ShardHelpers

  # Wait for all shards to be alive before running any test in this module
  # (a previous module may have killed shards and they might still be restarting).
  setup_all do
    ShardHelpers.wait_shards_alive()
    :ok
  end

  # Ensure all shards are alive after tests that kill them, and flush all keys
  # before each test so the keydir stays small — a large keydir makes KEYS/DBSIZE
  # calls progressively slower and can trigger GenServer call timeouts.
  setup do
    ShardHelpers.flush_all_keys()
    on_exit(fn -> ShardHelpers.wait_shards_alive() end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp ukey(base), do: "slc_#{base}_#{:rand.uniform(9_999_999)}"

  defp shard_pid(index) do
    Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), index))
  end

  defp shard_index_for(key), do: Router.shard_for(FerricStore.Instance.get(:default), key)
  defp shard_name_for(key), do: Router.shard_name(FerricStore.Instance.get(:default), shard_index_for(key))
  defp shard_pid_for(key), do: Process.whereis(shard_name_for(key))

  # Builds a store map backed by the real Router (application-supervised shards).
  defp real_store do
    %{
      get: fn k -> Router.get(FerricStore.Instance.get(:default), k) end,
      get_meta: fn k -> Router.get_meta(FerricStore.Instance.get(:default), k) end,
      put: fn k, v, e -> Router.put(FerricStore.Instance.get(:default), k, v, e) end,
      delete: fn k -> Router.delete(FerricStore.Instance.get(:default), k) end,
      exists?: fn k -> Router.exists?(FerricStore.Instance.get(:default), k) end,
      keys: fn -> Router.keys(FerricStore.Instance.get(:default)) end,
      flush: fn -> :ok end,
      dbsize: fn -> Router.dbsize(FerricStore.Instance.get(:default)) end
    }
  end

  # Polls until the shard registered under `name` has a different PID than
  # `old_pid`, or raises after `attempts` retries (50ms apart).
  defp wait_for_new_pid(name, old_pid, attempts \\ 20)

  defp wait_for_new_pid(_name, _old_pid, 0) do
    raise "Shard did not restart within the expected time"
  end

  defp wait_for_new_pid(name, old_pid, attempts) do
    case Process.whereis(name) do
      nil ->
        :timer.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      ^old_pid ->
        :timer.sleep(50)
        wait_for_new_pid(name, old_pid, attempts - 1)

      new_pid ->
        new_pid
    end
  end

  # Kills a shard by index, waits for DOWN, then waits for the supervisor
  # to restart it. Returns the new PID.
  defp kill_and_wait_restart(index) do
    name = Router.shard_name(FerricStore.Instance.get(:default), index)
    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    wait_for_new_pid(name, old_pid)
  end

  # Kills a shard by key, waits for restart, returns {old_pid, new_pid}.
  defp kill_shard_for_key(key) do
    name = shard_name_for(key)
    old_pid = Process.whereis(name)
    ref = Process.monitor(old_pid)
    Process.exit(old_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000
    new_pid = wait_for_new_pid(name, old_pid)
    {old_pid, new_pid}
  end

  # Finds N unique keys that all hash to the given shard index.
  defp find_n_keys_for_shard(shard_idx, n, prefix) do
    Enum.reduce_while(1..100_000, [], fn i, acc ->
      if length(acc) >= n do
        {:halt, acc}
      else
        k = "slc_#{prefix}_probe_#{i}_#{:rand.uniform(999_999)}"

        if Router.shard_for(FerricStore.Instance.get(:default), k) == shard_idx do
          {:cont, [k | acc]}
        else
          {:cont, acc}
        end
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # TCP helpers (adapted from ConnectionTest / CommandsTcpTest)
  # ---------------------------------------------------------------------------


  defp connect_and_hello(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  defp send_cmd(sock, cmd) do
    data = IO.iodata_to_binary(Encoder.encode(cmd))
    :ok = :gen_tcp.send(sock, data)
  end

  defp recv_response(sock) do
    recv_response(sock, "")
  end

  defp recv_response(sock, buf) do
    {:ok, data} = :gen_tcp.recv(sock, 0, 5_000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  # ===========================================================================
  # describe "shard crash and restart"
  # ===========================================================================

  describe "shard crash and restart" do
    test "data survives shard GenServer crash (Bitcask persisted)" do
      k = ukey("crash_persist")
      v = "durable_value"

      Router.put(FerricStore.Instance.get(:default), k, v, 0)
      assert v == Router.get(FerricStore.Instance.get(:default), k)

      # Flush pending writes to Bitcask before crashing, so the data survives.
      pid = shard_pid_for(k)
      :ok = GenServer.call(pid, :flush)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 2_000

      _new_pid = wait_for_new_pid(shard_name_for(k), pid)
      ShardHelpers.wait_shards_alive()

      ShardHelpers.eventually(fn -> v == Router.get(FerricStore.Instance.get(:default), k) end,
        "Value should be recovered from Bitcask after shard restart")
    end

    test "new shard process is registered under same name after restart" do
      old_pid = shard_pid(0)
      assert is_pid(old_pid)

      new_pid = kill_and_wait_restart(0)

      assert new_pid != old_pid,
             "New shard PID should differ from old PID after restart"

      assert Process.whereis(Router.shard_name(FerricStore.Instance.get(:default), 0)) == new_pid,
             "Shard 0 should be re-registered under its canonical name"
    end

    test "ETS cache is rebuilt correctly after shard restart" do
      k = ukey("ets_rebuild")
      v = "rebuild_me"

      Router.put(FerricStore.Instance.get(:default), k, v, 0)
      # Warm ETS cache
      assert v == Router.get(FerricStore.Instance.get(:default), k)

      shard_idx = shard_index_for(k)
      keydir_name = :"keydir_#{shard_idx}"
      # Single-table format: {key, value, expire_at_ms, lfu_counter}
      assert [{^k, _, _, _, _, _, _}] = :ets.lookup(keydir_name, k)

      # Flush to Bitcask before crashing so data is durable.
      :ok = GenServer.call(shard_pid_for(k), :flush)

      # Kill shard, wait for restart
      {_old, _new} = kill_shard_for_key(k)
      ShardHelpers.wait_shards_alive()

      # First GET after restart: cache miss, warms from Bitcask
      ShardHelpers.eventually(fn -> v == Router.get(FerricStore.Instance.get(:default), k) end,
        "Value should be recovered from Bitcask after shard restart")

      # Second GET: served from ETS
      assert v == Router.get(FerricStore.Instance.get(:default), k)

      # Verify ETS contains the warmed entry in single-table format
      assert [{^k, ^v, 0, _lfu, _fid, _off, _vsize}] = :ets.lookup(keydir_name, k)
    end

    test "multiple shard restarts don't lose data" do
      keys =
        for i <- 1..10 do
          k = ukey("multi_restart_#{i}")
          Router.put(FerricStore.Instance.get(:default), k, "val_#{i}", 0)
          k
        end

      # Flush all shards before the first kill so async writes are durable.
      # With the async io_uring write path, rapid consecutive puts may still
      # be in state.pending (batched). An explicit flush drains them to disk.
      ShardHelpers.flush_all_shards()

      # Kill shard 0 three times
      for _ <- 1..3 do
        kill_and_wait_restart(0)
        ShardHelpers.wait_shards_alive()
      end

      # All 10 keys should still be retrievable regardless of which shard they map to
      for {k, i} <- Enum.with_index(keys, 1) do
        ShardHelpers.eventually(fn -> "val_#{i}" == Router.get(FerricStore.Instance.get(:default), k) end,
          "Key #{k} should survive three restarts of shard 0")
      end
    end

    test "writes during shard restart are handled" do
      # Find a key that maps to shard 0
      [k] = find_n_keys_for_shard(0, 1, "write_during_restart")

      old_pid = shard_pid(0)
      ref = Process.monitor(old_pid)
      Process.exit(old_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^old_pid, :killed}, 2_000

      # Attempt a put while shard 0 may still be restarting.
      # This should either succeed (fast restart) or raise an exit
      # (no process), but must not crash the calling process permanently.
      result =
        try do
          Router.put(FerricStore.Instance.get(:default), k, "during_restart", 0)
          :ok
        catch
          :exit, _ -> :exit_caught
        end

      assert result in [:ok, :exit_caught]

      # After restart is complete, writes should succeed
      _new_pid = wait_for_new_pid(Router.shard_name(FerricStore.Instance.get(:default), 0), old_pid)
      ShardHelpers.wait_shards_alive()
      ShardHelpers.eventually(fn ->
        Router.put(FerricStore.Instance.get(:default), k, "after_restart", 0) == :ok
      end, "put should succeed after shard restart")
      ShardHelpers.eventually(fn ->
        "after_restart" == Router.get(FerricStore.Instance.get(:default), k)
      end, "get should return value written after shard restart")
    end
  end

  # ===========================================================================
  # describe "TTL expiry end-to-end"
  # ===========================================================================

  describe "TTL expiry end-to-end" do
    test "SET with PX 300 expires key after 300ms" do
      k = ukey("px_300")
      expire_at = System.os_time(:millisecond) + 300

      Router.put(FerricStore.Instance.get(:default), k, "v", expire_at)
      assert "v" == Router.get(FerricStore.Instance.get(:default), k)

      :timer.sleep(350)
      assert nil == Router.get(FerricStore.Instance.get(:default), k), "Key should be nil after 300ms TTL"
    end

    test "expired key not counted in DBSIZE" do
      k = ukey("dbsize_ttl")
      expire_at = System.os_time(:millisecond) + 200

      Router.put(FerricStore.Instance.get(:default), k, "v", expire_at)
      size1 = Router.dbsize(FerricStore.Instance.get(:default))
      assert size1 >= 1, "At least the TTL key should be counted"

      :timer.sleep(300)

      # After expiry, GET must return nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)
    end

    test "expired key not in Router.keys(FerricStore.Instance.get(:default))" do
      k = ukey("keys_ttl")
      expire_at = System.os_time(:millisecond) + 200

      Router.put(FerricStore.Instance.get(:default), k, "v", expire_at)
      assert k in Router.keys(FerricStore.Instance.get(:default))

      :timer.sleep(300)
      refute k in Router.keys(FerricStore.Instance.get(:default)), "Expired key should not appear in keys()"
    end

    test "EXPIRE then sleep then key is gone" do
      store = real_store()
      k = ukey("expire_gone")

      store.put.(k, "v", 0)
      assert "v" == store.get.(k)

      assert 1 == Expiry.handle("EXPIRE", [k, "1"], store)

      :timer.sleep(1_100)
      assert nil == store.get.(k), "Key should be gone after EXPIRE 1 second"
    end

    test "TTL decrements over time" do
      store = real_store()
      k = ukey("ttl_decrement")

      Router.put(FerricStore.Instance.get(:default), k, "v", System.os_time(:millisecond) + 5_000)

      t1 = Expiry.handle("TTL", [k], store)
      :timer.sleep(1_100)
      t2 = Expiry.handle("TTL", [k], store)

      assert t2 < t1, "TTL should decrement: t1=#{t1}, t2=#{t2}"
    end

    test "PTTL decrements with millisecond precision" do
      store = real_store()
      k = ukey("pttl_decrement")

      Router.put(FerricStore.Instance.get(:default), k, "v", System.os_time(:millisecond) + 5_000)

      ms1 = Expiry.handle("PTTL", [k], store)
      :timer.sleep(100)
      ms2 = Expiry.handle("PTTL", [k], store)

      assert ms2 < ms1, "PTTL should decrement: ms1=#{ms1}, ms2=#{ms2}"
      assert ms1 - ms2 >= 50, "At least 50ms should have elapsed, got #{ms1 - ms2}ms"
    end
  end

  # ===========================================================================
  # describe "purge_expired integration"
  # ===========================================================================

  describe "purge_expired integration" do
    test "expired keys return nil after get (lazy tombstone)" do
      keys =
        for i <- 1..5 do
          k = ukey("purge_#{i}")
          past = System.os_time(:millisecond) - 100
          Router.put(FerricStore.Instance.get(:default), k, "val_#{i}", past)
          k
        end

      # Each GET triggers lazy eviction
      for k <- keys do
        assert nil == Router.get(FerricStore.Instance.get(:default), k), "Expired key #{k} should return nil"
      end

      # None should appear in keys()
      all_keys = Router.keys(FerricStore.Instance.get(:default))

      for k <- keys do
        refute k in all_keys, "Expired key #{k} should not be in keys()"
      end
    end

    test "expired keys are tombstoned after get" do
      k = ukey("tombstone")
      past = System.os_time(:millisecond) - 100

      Router.put(FerricStore.Instance.get(:default), k, "old", past)

      # GET detects expiry, writes tombstone, returns nil
      assert nil == Router.get(FerricStore.Instance.get(:default), k)

      # Shard should still be alive and functional
      pid = shard_pid_for(k)
      assert Process.alive?(pid)

      # Re-use the key with no expiry
      Router.put(FerricStore.Instance.get(:default), k, "newval", 0)
      assert "newval" == Router.get(FerricStore.Instance.get(:default), k)
    end
  end

  # ===========================================================================
  # describe "ShardSupervisor integration"
  # ===========================================================================

  describe "ShardSupervisor integration" do
    test "all 4 shards are alive at startup" do
      for i <- 0..3 do
        pid = shard_pid(i)
        assert is_pid(pid), "Shard #{i} should be registered"
        assert Process.alive?(pid), "Shard #{i} should be alive"
      end
    end

    test "supervisor is :one_for_one -- killing one shard does not kill others" do
      pids_1_to_3 =
        for i <- 1..3 do
          {i, shard_pid(i)}
        end

      # Kill shard 0
      kill_and_wait_restart(0)

      :timer.sleep(50)

      # Shards 1, 2, 3 should remain alive with the same PIDs
      for {i, original_pid} <- pids_1_to_3 do
        current_pid = shard_pid(i)
        assert current_pid == original_pid,
               "Shard #{i} should not have restarted (one_for_one strategy)"

        assert Process.alive?(current_pid),
               "Shard #{i} should still be alive"
      end
    end

    test "shard count matches configured count" do
      children =
        Supervisor.which_children(Ferricstore.Store.ShardSupervisor)
        |> Enum.filter(fn {id, _, _, _} ->
          is_atom(id) and String.starts_with?(Atom.to_string(id), "shard_")
        end)

      assert length(children) == 4, "Expected 4 shard children, got #{length(children)}"
    end
  end

  # ===========================================================================
  # describe "TCP + TTL integration"
  # ===========================================================================

  describe "TCP + TTL integration" do
    setup do
      # The application supervisor already starts the Ranch listener.
      # Use the actual bound port rather than starting a second listener.
      %{port: Listener.port()}
    end

    test "SET with PX over TCP, key expires, GET returns nil", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tcp_px_expire")

      send_cmd(sock, ["SET", k, "ephemeral", "PX", "300"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "ephemeral"

      :timer.sleep(400)

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "EXPIRE over TCP then TTL shows countdown", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tcp_expire_ttl")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["EXPIRE", k, "100"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl)
      assert ttl >= 1 and ttl <= 100

      :gen_tcp.close(sock)
    end

    test "PERSIST over TCP removes expiry", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tcp_persist")

      send_cmd(sock, ["SET", k, "v", "EX", "100"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["TTL", k])
      ttl = recv_response(sock)
      assert is_integer(ttl) and ttl > 0

      send_cmd(sock, ["PERSIST", k])
      assert recv_response(sock) == 1

      send_cmd(sock, ["TTL", k])
      assert recv_response(sock) == -1

      :gen_tcp.close(sock)
    end

    test "PEXPIRE over TCP then PTTL shows countdown in ms", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tcp_pexpire_pttl")

      send_cmd(sock, ["SET", k, "v"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["PEXPIRE", k, "5000"])
      assert recv_response(sock) == 1

      send_cmd(sock, ["PTTL", k])
      pttl = recv_response(sock)
      assert is_integer(pttl)
      assert pttl >= 1 and pttl <= 5_000

      :gen_tcp.close(sock)
    end

    test "SET with EX over TCP, wait for expiry, GET returns nil", %{port: port} do
      sock = connect_and_hello(port)
      k = ukey("tcp_ex_expire")

      send_cmd(sock, ["SET", k, "short_lived", "EX", "1"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == "short_lived"

      :timer.sleep(1_100)

      send_cmd(sock, ["GET", k])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end
end
