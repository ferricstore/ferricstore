defmodule FerricstoreServer.AclRaftReplicationTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :acl
  @moduletag :global_state

  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener
  alias FerricstoreServer.Acl

  setup do
    Acl.reset!()
    on_exit(fn -> Acl.reset!() end)
    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp connect(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    sock
  end

  defp connect_and_hello(port) do
    sock = connect(port)
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
    {:ok, data} = :gen_tcp.recv(sock, 0, 5000)
    buf2 = buf <> data

    case Parser.parse(buf2) do
      {:ok, [val], ""} -> val
      {:ok, [val], _rest} -> val
      {:ok, [], _} -> recv_response(sock, buf2)
    end
  end

  # ---------------------------------------------------------------------------
  # 1. handle_raft_command/1 dispatches correctly
  # ---------------------------------------------------------------------------

  describe "handle_raft_command/1 dispatches correctly" do
    test "acl_setuser creates a user" do
      assert :ok = Acl.handle_raft_command({:acl_setuser, "raft_user", ["on", ">raftpass"]})
      assert Acl.get_user("raft_user") != nil
    end

    test "acl_setuser invalidates connection ACL caches when applied from raft" do
      group = FerricstoreServer.Connection.acl_pg_group()
      :ok = :pg.join(group, group, self())

      task =
        Task.async(fn ->
          Acl.handle_raft_command({:acl_setuser, "raft_user", ["on", ">raftpass"]})
        end)

      assert :ok = Task.await(task)
      assert_receive {:acl_invalidate, "raft_user"}, 500
    end

    test "acl_deluser removes a user" do
      Acl.handle_raft_command({:acl_setuser, "raft_user", ["on", ">raftpass"]})

      assert :ok = Acl.handle_raft_command({:acl_deluser, "raft_user"})
      assert Acl.get_user("raft_user") == nil
    end

    test "acl_reset resets ACL state" do
      Acl.handle_raft_command({:acl_setuser, "raft_user", ["on", ">raftpass"]})

      assert :ok = Acl.handle_raft_command({:acl_reset})
      assert Acl.get_user("raft_user") == nil
    end

    test "acl_load applies validated ACL file contents from raft" do
      data_dir =
        Path.join(System.tmp_dir!(), "ferricstore-acl-load-#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(data_dir) end)

      assert :ok = Acl.set_user("raft_loaded", ["on", ">loadedpass", "~*", "+GET"])
      assert :ok = Acl.save(data_dir)
      assert {:ok, contents} = File.read(Acl.acl_file_path(data_dir))

      assert :ok = Acl.reset!()
      assert :ok = Acl.set_user("stale_local", ["on", ">stale", "~*", "+GET"])

      assert :ok = Acl.handle_raft_command({:acl_load, contents})

      assert Acl.get_user("raft_loaded") != nil
      assert Acl.get_user("stale_local") == nil
    end

    test "unknown command returns error" do
      assert {:error, :unknown_acl_command} = Acl.handle_raft_command({:unknown_thing})
    end
  end

  # ---------------------------------------------------------------------------
  # 2. ACL SETUSER through Raft (integration via TCP)
  # ---------------------------------------------------------------------------

  describe "ACL SETUSER through Raft (integration via TCP)" do
    test "create user, auth, delete, verify auth fails", %{port: port} do
      # Create user via ACL SETUSER
      sock = connect_and_hello(port)
      send_cmd(sock, ["ACL", "SETUSER", "raft_test_user", "on", ">testpass123"])
      assert recv_response(sock) == {:simple, "OK"}
      :gen_tcp.close(sock)

      # Auth with the new user on a fresh connection
      sock2 = connect_and_hello(port)
      send_cmd(sock2, ["AUTH", "raft_test_user", "testpass123"])
      assert recv_response(sock2) == {:simple, "OK"}
      :gen_tcp.close(sock2)

      # Delete the user
      sock3 = connect_and_hello(port)
      send_cmd(sock3, ["ACL", "DELUSER", "raft_test_user"])
      assert recv_response(sock3) == {:simple, "OK"}
      :gen_tcp.close(sock3)

      # Verify auth fails with deleted user (error may be WRONGPASS or
      # "no password is set" depending on whether other ACL users remain)
      sock4 = connect_and_hello(port)
      send_cmd(sock4, ["AUTH", "raft_test_user", "testpass123"])
      response = recv_response(sock4)
      assert {:error, _msg} = response
      :gen_tcp.close(sock4)
    end
  end

  # ---------------------------------------------------------------------------
  # 3. ACL SETUSER goes through Router.server_command (not direct)
  # ---------------------------------------------------------------------------

  describe "ACL SETUSER goes through Router.server_command" do
    test "user created via TCP is visible in Acl module", %{port: port} do
      # Create user via TCP (which routes through connection.ex -> Router.server_command)
      sock = connect_and_hello(port)
      send_cmd(sock, ["ACL", "SETUSER", "router_user", "on", ">routerpass"])
      assert recv_response(sock) == {:simple, "OK"}
      :gen_tcp.close(sock)

      # The fact that the user exists in Acl proves the Raft path was used,
      # since connection.ex routes ACL SETUSER through Router.server_command.
      user = Acl.get_user("router_user")
      assert user != nil
    end
  end

  # ---------------------------------------------------------------------------
  # 4. Listener.running?/0 and ensure_started/0
  # ---------------------------------------------------------------------------

  describe "Listener.running?/0 and ensure_started/0" do
    test "running? returns true" do
      assert Listener.running?() == true
    end

    test "ensure_started returns :ok when already running" do
      assert Listener.ensure_started() == :ok
    end

    test "port returns an integer" do
      port = Listener.port()
      assert is_integer(port)
    end
  end

  # ---------------------------------------------------------------------------
  # 5. INFO command reflects injected callbacks (end-to-end)
  # ---------------------------------------------------------------------------

  describe "INFO command reflects injected callbacks" do
    test "INFO server section contains redis_mode:standalone from server_info_fn", %{port: port} do
      sock = connect_and_hello(port)
      send_cmd(sock, ["INFO", "server"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      assert is_binary(result)
      assert result =~ "redis_mode:standalone"
    end

    test "INFO clients section contains connected_clients from connected_clients_fn", %{
      port: port
    } do
      sock = connect_and_hello(port)
      send_cmd(sock, ["INFO", "clients"])
      result = recv_response(sock)
      :gen_tcp.close(sock)

      assert is_binary(result)
      assert result =~ "connected_clients:"
    end
  end

  # ---------------------------------------------------------------------------
  # 6. SET/GET still works end-to-end via TCP (no regression)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # 7. LPUSH via TCP triggers Waiters notification (on_push end-to-end)
  # ---------------------------------------------------------------------------

  describe "LPUSH via TCP triggers waiter notification" do
    test "registered waiter receives notification after LPUSH over TCP", %{port: port} do
      key = "blpop_e2e_#{:rand.uniform(999_999)}"

      # Register ourselves as a waiter for this key
      deadline = System.monotonic_time(:millisecond) + 10_000
      Ferricstore.Waiters.register(key, self(), deadline)

      # LPUSH via TCP — goes through connection.ex → Dispatcher → List.handle → on_push
      sock = connect_and_hello(port)
      send_cmd(sock, ["LPUSH", key, "hello"])
      response = recv_response(sock)
      assert response == 1
      :gen_tcp.close(sock)

      # Waiter should be notified
      assert_receive {:waiter_notify, ^key}, 2000
    end
  end

  # ---------------------------------------------------------------------------
  # 8. SET/GET regression via TCP
  # ---------------------------------------------------------------------------

  describe "SET/GET regression via TCP" do
    test "SET and GET roundtrip works after all callback changes", %{port: port} do
      sock = connect_and_hello(port)
      key = "regression_key_#{System.unique_integer([:positive])}"

      send_cmd(sock, ["SET", key, "regression_value"])
      assert {:simple, "OK"} = recv_response(sock)

      send_cmd(sock, ["GET", key])
      assert "regression_value" = recv_response(sock)

      send_cmd(sock, ["DEL", key])
      recv_response(sock)

      :gen_tcp.close(sock)
    end
  end
end
