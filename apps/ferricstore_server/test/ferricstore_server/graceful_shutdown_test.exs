defmodule FerricstoreServer.GracefulShutdownTest do
  @moduledoc """
  Tests that the TCP server shuts down gracefully and resumes correctly.

  Each test runs in a fresh temp data directory to avoid contamination
  from other tests.
  """

  use ExUnit.Case, async: false
  @moduletag :shard_kill

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias Ferricstore.Test.ShardHelpers
  alias FerricstoreServer.Listener

  setup do
    ctx = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      ShardHelpers.teardown_isolated_data_dir(ctx)
      Listener.ensure_started()
    end)
  end

  # ---------------------------------------------------------------------------
  # TCP helpers
  # ---------------------------------------------------------------------------

  defp get_port do
    case Process.get(:cached_port) do
      nil ->
        port = :ranch.get_port(Listener)
        Process.put(:cached_port, port)
        port
      port ->
        port
    end
  end

  defp connect do
    port = get_port()
    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw], 5_000)
    send_cmd(sock, ["HELLO", "3"])
    _resp = recv_response(sock)
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
    {:ok, data} = :gen_tcp.recv(sock, 0, 10_000)
    combined = buf <> data

    case Parser.parse(combined) do
      {:ok, [val], _rest} -> val
      {:ok, [val | _], _rest} -> val
      {:ok, [], _rest} -> recv_response(sock, combined)
    end
  end

  defp tcp_set(sock, key, value) do
    send_cmd(sock, ["SET", key, value])
    recv_response(sock)
  end

  defp tcp_get(sock, key) do
    send_cmd(sock, ["GET", key])
    recv_response(sock)
  end

  defp shutdown_and_restart do
    Ferricstore.Application.prep_stop(nil)

    shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

    for i <- 0..(shard_count - 1) do
      name = Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), i)
      pid = Process.whereis(name)

      if pid && Process.alive?(pid) do
        ref = Process.monitor(pid)
        Process.exit(pid, :kill)
        receive do {:DOWN, ^ref, _, _, _} -> :ok after 5_000 -> :ok end
      end
    end

    # Wait for full readiness
    ShardHelpers.eventually(fn ->
      shard_count_val = :persistent_term.get(:ferricstore_shard_count, 4)

      Enum.all?(0..(shard_count_val - 1), fn i ->
        pid = Process.whereis(Ferricstore.Store.Router.shard_name(FerricStore.Instance.get(:default), i))
        alive = is_pid(pid) and Process.alive?(pid)

        alive and try do
          match?({:ok, {:raft_log_pos, _, _}}, Ferricstore.Raft.WARaftBackend.storage_position(i))
        catch
          :exit, _ -> false
        end
      end) and try do
        Ferricstore.Store.Router.put(FerricStore.Instance.get(:default), "__readiness_probe__", "ok", 0)
        Ferricstore.Store.Router.delete(FerricStore.Instance.get(:default), "__readiness_probe__")
        true
      catch
        :exit, _ -> false
      end
    end, "full write path should be ready after restart", 300, 200)

    Ferricstore.Health.set_ready(true)
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "data written via TCP survives graceful shutdown" do
    test "SET then shutdown then GET returns the value" do
      sock = connect()
      {:simple, "OK"} = tcp_set(sock, "tcp_gsd_1", "hello_shutdown")
      :gen_tcp.close(sock)

      ShardHelpers.flush_all_shards()
      ShardHelpers.compact_wal()
      shutdown_and_restart()

      ShardHelpers.eventually(fn ->
        sock2 = connect()
        result = tcp_get(sock2, "tcp_gsd_1")
        :gen_tcp.close(sock2)
        assert result == "hello_shutdown"
      end, "GET after restart should return value", 10, 500)
    end

    test "multiple keys from different connections survive" do
      for i <- 1..10 do
        sock = connect()
        {:simple, "OK"} = tcp_set(sock, "tcp_gsd_multi_#{i}", "val_#{i}")
        :gen_tcp.close(sock)
      end

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock = connect()

      for i <- 1..10 do
        assert tcp_get(sock, "tcp_gsd_multi_#{i}") == "val_#{i}",
               "key tcp_gsd_multi_#{i} should survive"
      end

      :gen_tcp.close(sock)
    end

    test "INCR counter via TCP survives restart" do
      sock = connect()
      tcp_set(sock, "tcp_gsd_counter", "0")

      for _ <- 1..25 do
        send_cmd(sock, ["INCR", "tcp_gsd_counter"])
        recv_response(sock)
      end

      :gen_tcp.close(sock)

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock2 = connect()
      result = tcp_get(sock2, "tcp_gsd_counter")
      :gen_tcp.close(sock2)

      assert result == "25"
    end
  end

  describe "server accepts connections after restart" do
    test "new connection works after shutdown + restart" do
      shutdown_and_restart()

      sock = connect()
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}
      :gen_tcp.close(sock)
    end

    test "SET and GET work on fresh connection after restart" do
      shutdown_and_restart()

      sock = connect()
      {:simple, "OK"} = tcp_set(sock, "tcp_gsd_post_restart", "fresh_data")
      assert tcp_get(sock, "tcp_gsd_post_restart") == "fresh_data"
      :gen_tcp.close(sock)
    end
  end

  describe "data from before and after restart coexist" do
    test "old and new keys both readable" do
      sock = connect()
      {:simple, "OK"} = tcp_set(sock, "tcp_gsd_old", "before")
      :gen_tcp.close(sock)

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock2 = connect()
      {:simple, "OK"} = tcp_set(sock2, "tcp_gsd_new", "after")

      assert tcp_get(sock2, "tcp_gsd_old") == "before"
      assert tcp_get(sock2, "tcp_gsd_new") == "after"
      :gen_tcp.close(sock2)
    end

    test "overwrite old key after restart" do
      sock = connect()
      {:simple, "OK"} = tcp_set(sock, "tcp_gsd_overwrite", "v1")
      :gen_tcp.close(sock)

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock2 = connect()
      {:simple, "OK"} = tcp_set(sock2, "tcp_gsd_overwrite", "v2")
      assert tcp_get(sock2, "tcp_gsd_overwrite") == "v2"
      :gen_tcp.close(sock2)
    end
  end

  describe "health endpoint reflects shutdown state" do
    test "health returns starting during shutdown" do
      Ferricstore.Health.set_ready(false)
      health = Ferricstore.Health.check()
      assert health.status == :starting
      Ferricstore.Health.set_ready(true)
    end

    test "health returns ok after restart" do
      shutdown_and_restart()
      health = Ferricstore.Health.check()
      assert health.status == :ok
    end
  end

  describe "two full shutdown-restart cycles via TCP" do
    test "data survives two cycles" do
      sock = connect()
      {:simple, "OK"} = tcp_set(sock, "tcp_gsd_cycle", "cycle1")
      :gen_tcp.close(sock)

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock2 = connect()
      assert tcp_get(sock2, "tcp_gsd_cycle") == "cycle1"
      {:simple, "OK"} = tcp_set(sock2, "tcp_gsd_cycle", "cycle2")
      :gen_tcp.close(sock2)

      ShardHelpers.flush_all_shards()
      shutdown_and_restart()

      sock3 = connect()
      assert tcp_get(sock3, "tcp_gsd_cycle") == "cycle2"
      :gen_tcp.close(sock3)
    end
  end
end
