defmodule FerricstoreServer.Commands.AuthTest do
  @moduledoc false
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Config
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias FerricstoreServer.Listener

  # Reset requirepass after each test to avoid contaminating other tests.
  setup do
    FerricstoreServer.Acl.reset!()

    on_exit(fn ->
      Config.set("requirepass", "")
      FerricstoreServer.Acl.reset!()
    end)

    %{port: Listener.port()}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

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

  defp connect_raw(port) do
    {:ok, sock} =
      :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, packet: :raw])

    sock
  end

  defp connect_and_hello(port) do
    sock = connect_raw(port)
    send_cmd(sock, ["HELLO", "3"])
    _greeting = recv_response(sock)
    sock
  end

  # ---------------------------------------------------------------------------
  # AUTH without requirepass
  # ---------------------------------------------------------------------------

  describe "AUTH without requirepass set" do
    test "AUTH returns error when no password is configured", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "somepassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no password is set"

      :gen_tcp.close(sock)
    end

    test "AUTH with username returns error when no password is configured", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "somepassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "no password is set"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # AUTH with requirepass
  # ---------------------------------------------------------------------------

  describe "AUTH with requirepass set" do
    setup %{port: port} do
      Config.set("requirepass", "secret123")
      %{port: port}
    end

    test "AUTH with correct password returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "AUTH with wrong password returns WRONGPASS error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "wrongpassword"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "AUTH with username and correct password returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "AUTH with username and wrong password returns WRONGPASS", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "default", "bad"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "commands are rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["SET", "key", "value"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "GET is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["GET", "key"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "PING is rejected before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["PING"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end

    test "HELLO is allowed before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["HELLO", "3"])
      response = recv_response(sock)

      # HELLO returns the greeting map, not an error
      assert is_map(response)
      assert response["server"] == "ferricstore"

      :gen_tcp.close(sock)
    end

    test "CLIENT HELLO is allowed before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["CLIENT", "HELLO", "3"])
      response = recv_response(sock)

      assert is_map(response)
      assert response["server"] == "ferricstore"
      assert response["proto"] == 3

      :gen_tcp.close(sock)
    end

    test "QUIT is allowed before authentication", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["QUIT"])
      response = recv_response(sock)

      assert response == {:simple, "OK"}
      # Socket should be closed by the server
    end

    test "commands work after successful AUTH", %{port: port} do
      sock = connect_and_hello(port)

      # Authenticate
      send_cmd(sock, ["AUTH", "secret123"])
      assert recv_response(sock) == {:simple, "OK"}

      # Now commands should work
      send_cmd(sock, ["PING"])
      assert recv_response(sock) == {:simple, "PONG"}

      send_cmd(sock, ["SET", "auth_test_key", "value"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["GET", "auth_test_key"])
      assert recv_response(sock) == "value"

      :gen_tcp.close(sock)
    end

    test "AUTH with no args returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL WHOAMI
  # ---------------------------------------------------------------------------

  describe "ACL WHOAMI" do
    test "ACL WHOAMI returns 'default'", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "WHOAMI"])
      assert recv_response(sock) == "default"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LIST
  # ---------------------------------------------------------------------------

  describe "ACL LIST" do
    test "ACL LIST returns default user rule", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "LIST"])
      response = recv_response(sock)

      assert is_list(response)
      default_rule = Enum.find(response, &String.contains?(&1, "user default"))
      assert default_rule, "ACL LIST should contain a rule for the default user"
      assert default_rule =~ "+@all"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL GETUSER
  # ---------------------------------------------------------------------------

  describe "ACL GETUSER" do
    test "ACL GETUSER default returns user info", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "GETUSER", "default"])
      response = recv_response(sock)

      assert is_list(response)
      assert "flags" in response
      assert "commands" in response

      :gen_tcp.close(sock)
    end

    test "ACL GETUSER unknown user returns nil", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "GETUSER", "unknownuser"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL CAT
  # ---------------------------------------------------------------------------

  describe "ACL CAT" do
    test "ACL CAT returns category list", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "CAT"])
      response = recv_response(sock)

      assert is_list(response)
      assert response != []
      assert "read" in response
      assert "write" in response
      assert "admin" in response

      :gen_tcp.close(sock)
    end

    test "ACL CAT with specific category returns list", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "CAT", "read"])
      response = recv_response(sock)

      assert is_list(response)

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LOG
  # ---------------------------------------------------------------------------

  describe "ACL LOG" do
    test "ACL LOG returns empty list after reset", %{port: port} do
      sock = connect_and_hello(port)

      # Reset first to clear any entries from prior tests
      send_cmd(sock, ["ACL", "LOG", "RESET"])
      recv_response(sock)

      send_cmd(sock, ["ACL", "LOG"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end

    test "ACL LOG RESET returns OK", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "LOG", "RESET"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "ACL LOG with count returns empty list after reset", %{port: port} do
      sock = connect_and_hello(port)

      # Reset first to clear any entries from prior tests
      send_cmd(sock, ["ACL", "LOG", "RESET"])
      recv_response(sock)

      send_cmd(sock, ["ACL", "LOG", "10"])
      assert recv_response(sock) == []

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL unknown subcommand
  # ---------------------------------------------------------------------------

  describe "ACL unknown" do
    test "ACL with unknown subcommand returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "BADSUBCMD"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "unknown subcommand"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL gated by auth
  # ---------------------------------------------------------------------------

  describe "ACL gated by authentication" do
    test "ACL is rejected before authentication when requirepass is set", %{port: port} do
      Config.set("requirepass", "secret123")
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "WHOAMI"])
      response = recv_response(sock)

      assert {:error, msg} = response
      assert msg =~ "NOAUTH"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL SETUSER — creates a new user
  # ---------------------------------------------------------------------------

  describe "ACL SETUSER" do
    setup %{port: port} do
      on_exit(fn -> FerricstoreServer.Acl.reset!() end)
      %{port: port}
    end

    test "ACL SETUSER creates a new user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "alice"])
      assert recv_response(sock) == {:simple, "OK"}

      # Verify user exists via ACL GETUSER.
      send_cmd(sock, ["ACL", "GETUSER", "alice"])
      response = recv_response(sock)
      assert is_list(response)
      assert "flags" in response

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER with >password sets password", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "bob", "on", ">s3cret"])
      assert recv_response(sock) == {:simple, "OK"}

      # Verify the user has a password set.
      send_cmd(sock, ["ACL", "GETUSER", "bob"])
      response = recv_response(sock)
      assert is_list(response)

      # The "passwords" field should contain the SHA-256 hash.
      passwords_idx = Enum.find_index(response, &(&1 == "passwords"))
      passwords = Enum.at(response, passwords_idx + 1)
      assert length(passwords) == 1

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER with on enables the user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "carol", "on"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "GETUSER", "carol"])
      response = recv_response(sock)
      flags_idx = Enum.find_index(response, &(&1 == "flags"))
      flags = Enum.at(response, flags_idx + 1)
      assert "on" in flags

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER with off disables the user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "dan", "off"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "GETUSER", "dan"])
      response = recv_response(sock)
      flags_idx = Enum.find_index(response, &(&1 == "flags"))
      flags = Enum.at(response, flags_idx + 1)
      assert "off" in flags

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER with multiple rules applies all of them", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "eve", "on", ">pass123", "~*", "+@all"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "GETUSER", "eve"])
      response = recv_response(sock)

      flags_idx = Enum.find_index(response, &(&1 == "flags"))
      flags = Enum.at(response, flags_idx + 1)
      assert "on" in flags

      commands_idx = Enum.find_index(response, &(&1 == "commands"))
      commands = Enum.at(response, commands_idx + 1)
      assert commands == "+@all"

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER updates an existing user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "frank", "on", ">oldpass"])
      assert recv_response(sock) == {:simple, "OK"}

      # Update password.
      send_cmd(sock, ["ACL", "SETUSER", "frank", ">newpass"])
      assert recv_response(sock) == {:simple, "OK"}

      # User should still be enabled (state preserved from prior SETUSER).
      send_cmd(sock, ["ACL", "GETUSER", "frank"])
      response = recv_response(sock)
      flags_idx = Enum.find_index(response, &(&1 == "flags"))
      flags = Enum.at(response, flags_idx + 1)
      assert "on" in flags

      :gen_tcp.close(sock)
    end

    test "ACL SETUSER with no username returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # AUTH with ACL-created users
  # ---------------------------------------------------------------------------

  describe "AUTH with ACL users" do
    setup %{port: port} do
      on_exit(fn ->
        FerricstoreServer.Acl.reset!()
        Config.set("requirepass", "")
      end)

      %{port: port}
    end

    test "AUTH with new ACL user works", %{port: port} do
      # Create user with password via ACL.
      FerricstoreServer.Acl.set_user("testuser", ["on", ">testpass"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "testuser", "testpass"])
      assert recv_response(sock) == {:simple, "OK"}

      :gen_tcp.close(sock)
    end

    test "AUTH with wrong password for ACL user is rejected", %{port: port} do
      FerricstoreServer.Acl.set_user("testuser2", ["on", ">correctpass"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "testuser2", "wrongpass"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "AUTH with disabled ACL user is rejected", %{port: port} do
      FerricstoreServer.Acl.set_user("disableduser", ["off", ">somepass"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "disableduser", "somepass"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "AUTH with nonexistent user is rejected", %{port: port} do
      # Set requirepass so AUTH is accepted at all.
      Config.set("requirepass", "irrelevant")

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "nosuchuser", "pass"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "WRONGPASS"

      :gen_tcp.close(sock)
    end

    test "ACL WHOAMI reflects authenticated user", %{port: port} do
      FerricstoreServer.Acl.set_user("whoami_user", ["on", ">mypass"])

      sock = connect_and_hello(port)

      send_cmd(sock, ["AUTH", "whoami_user", "mypass"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "WHOAMI"])
      assert recv_response(sock) == "whoami_user"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL DELUSER
  # ---------------------------------------------------------------------------

  describe "ACL DELUSER" do
    setup %{port: port} do
      on_exit(fn -> FerricstoreServer.Acl.reset!() end)
      %{port: port}
    end

    test "ACL DELUSER removes a user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "temp_user", "on"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "DELUSER", "temp_user"])
      assert recv_response(sock) == {:simple, "OK"}

      # Verify user is gone.
      send_cmd(sock, ["ACL", "GETUSER", "temp_user"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end

    test "ACL DELUSER cannot remove default user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "DELUSER", "default"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "default"

      :gen_tcp.close(sock)
    end

    test "ACL DELUSER with nonexistent user returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "DELUSER", "no_such_user"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "does not exist"

      :gen_tcp.close(sock)
    end

    test "ACL DELUSER with no username returns error", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "DELUSER"])
      response = recv_response(sock)
      assert {:error, msg} = response
      assert msg =~ "wrong number of arguments"

      :gen_tcp.close(sock)
    end

    test "ACL DELUSER with multiple users is atomic when a later delete errors", %{port: port} do
      admin = connect_and_hello(port)
      alice = connect_and_hello(port)

      send_cmd(admin, ["ACL", "SETUSER", "alice", "on", ">pass", "~*", "+@all"])
      assert recv_response(admin) == {:simple, "OK"}

      send_cmd(alice, ["AUTH", "alice", "pass"])
      assert recv_response(alice) == {:simple, "OK"}

      send_cmd(admin, ["ACL", "DELUSER", "alice", "default"])
      assert {:error, msg} = recv_response(admin)
      assert msg =~ "default"

      assert FerricstoreServer.Acl.get_user("alice") != nil

      send_cmd(alice, ["PING"])
      assert recv_response(alice) == {:simple, "PONG"}

      :gen_tcp.close(admin)
      :gen_tcp.close(alice)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL LIST shows created users
  # ---------------------------------------------------------------------------

  describe "ACL LIST with custom users" do
    setup %{port: port} do
      on_exit(fn -> FerricstoreServer.Acl.reset!() end)
      %{port: port}
    end

    test "ACL LIST shows newly created users", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "alice", "on", ">pass", "~*", "+@all"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "LIST"])
      response = recv_response(sock)

      assert is_list(response)
      assert length(response) == 2

      assert Enum.any?(response, &(&1 =~ "user default"))
      assert Enum.any?(response, &(&1 =~ "user alice"))

      :gen_tcp.close(sock)
    end

    test "ACL LIST no longer shows deleted users", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "temp", "on"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "DELUSER", "temp"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "LIST"])
      response = recv_response(sock)

      assert length(response) == 1
      assert hd(response) =~ "user default"

      :gen_tcp.close(sock)
    end
  end

  # ---------------------------------------------------------------------------
  # ACL GETUSER shows user rules
  # ---------------------------------------------------------------------------

  describe "ACL GETUSER for ACL-created users" do
    setup %{port: port} do
      on_exit(fn -> FerricstoreServer.Acl.reset!() end)
      %{port: port}
    end

    test "ACL GETUSER shows user rules for ACL-created user", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "grace", "on", ">mypass", "~cache:*", "+get", "+set"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "GETUSER", "grace"])
      response = recv_response(sock)
      assert is_list(response)

      # Verify structure.
      assert "flags" in response
      assert "passwords" in response
      assert "commands" in response
      assert "keys" in response

      # Verify the user is enabled.
      flags_idx = Enum.find_index(response, &(&1 == "flags"))
      flags = Enum.at(response, flags_idx + 1)
      assert "on" in flags

      # Verify password is set (hashed).
      passwords_idx = Enum.find_index(response, &(&1 == "passwords"))
      passwords = Enum.at(response, passwords_idx + 1)
      assert length(passwords) == 1

      # Verify keys pattern.
      keys_idx = Enum.find_index(response, &(&1 == "keys"))
      keys_str = Enum.at(response, keys_idx + 1)
      assert keys_str =~ "cache:*"

      :gen_tcp.close(sock)
    end

    test "ACL GETUSER and LIST show explicit denied commands", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "deny_get", "on", "nopass", "~*", "+@all", "-get"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "GETUSER", "deny_get"])
      response = recv_response(sock)
      commands_idx = Enum.find_index(response, &(&1 == "commands"))
      commands = Enum.at(response, commands_idx + 1)
      assert commands =~ "+@all"
      assert commands =~ "-get"

      send_cmd(sock, ["ACL", "LIST"])
      list = recv_response(sock)
      assert Enum.any?(list, &String.contains?(&1, "user deny_get"))
      assert Enum.any?(list, &String.contains?(&1, "-get"))

      :gen_tcp.close(sock)
    end

    test "ACL GETUSER for nonexistent user returns nil", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "GETUSER", "nonexistent"])
      assert recv_response(sock) == nil

      :gen_tcp.close(sock)
    end
  end

  describe "ACL persistence commands" do
    setup %{port: port} do
      on_exit(fn -> FerricstoreServer.Acl.reset!() end)
      %{port: port}
    end

    test "ACL SAVE and LOAD are available over RESP", %{port: port} do
      sock = connect_and_hello(port)

      send_cmd(sock, ["ACL", "SETUSER", "persisted_user", "on", "nopass", "~*", "+get"])
      assert recv_response(sock) == {:simple, "OK"}

      send_cmd(sock, ["ACL", "SAVE"])
      assert recv_response(sock) == {:simple, "OK"}

      FerricstoreServer.Acl.reset!()
      assert FerricstoreServer.Acl.get_user("persisted_user") == nil

      send_cmd(sock, ["ACL", "LOAD"])
      assert recv_response(sock) == {:simple, "OK"}
      assert FerricstoreServer.Acl.get_user("persisted_user") != nil

      :gen_tcp.close(sock)
    end
  end
end
