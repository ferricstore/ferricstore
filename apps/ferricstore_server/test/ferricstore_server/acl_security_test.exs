defmodule FerricstoreServer.AclSecurityTest do
  @moduledoc """
  Security-focused tests for the ACL Phase 1 hardening.

  Covers:
    1. Password hashing (PBKDF2-SHA256 via :crypto -- no plaintext in ETS)
    2. denied_commands set (explicit denials work when commands is :all)
    3. Protected mode (non-localhost rejected until ACL users configured)
    4. max_acl_users safety limit
    5. ACL LOG for command denials

  These tests are `async: false` because they share the global ACL ETS table
  and call `Acl.reset!/0` in setup to prevent cross-test contamination.
  """

  use ExUnit.Case, async: false
  @moduletag :acl
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Native.Session
  alias Ferricstore.AuditLog

  setup do
    Acl.reset!()
    AuditLog.reset()
    Application.put_env(:ferricstore, :audit_log_enabled, true)

    on_exit(fn ->
      Application.put_env(:ferricstore, :audit_log_enabled, false)
      Application.delete_env(:ferricstore, :max_acl_users)
      Application.delete_env(:ferricstore, :protected_mode)
      Ferricstore.Config.set("requirepass", "")
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Fix 1: Password hashing (PBKDF2-SHA256)
  # ---------------------------------------------------------------------------

  describe "password hashing" do
    test "password stored as hash, not plaintext" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      user = Acl.get_user("alice")

      # Password field should NOT be the plaintext string
      refute user.password == "s3cret"
      # It should be a non-nil binary (the base64-encoded salt+hash)
      assert is_binary(user.password)
      assert byte_size(user.password) > 0
    end

    test "auth with correct password succeeds" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert {:ok, "alice"} = Acl.authenticate("alice", "s3cret")
    end

    test "auth with wrong password fails" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert {:error, msg} = Acl.authenticate("alice", "wrong_password")
      assert msg =~ "WRONGPASS"
    end

    test "ETS table does not contain plaintext password" do
      assert :ok = Acl.set_user("alice", ["on", ">my_secret_password"])

      # Directly inspect the ETS table
      [{_name, user}] = :ets.lookup(:ferricstore_acl, "alice")

      refute user.password == "my_secret_password"
      assert is_binary(user.password)
    end

    test "same password produces different hashes (salt randomness)" do
      assert :ok = Acl.set_user("alice", ["on", ">same_pass"])
      user1 = Acl.get_user("alice")

      # Delete and recreate to get a new salt
      assert :ok = Acl.del_user("alice")
      assert :ok = Acl.set_user("alice", ["on", ">same_pass"])
      user2 = Acl.get_user("alice")

      # The stored hashes should be different because the salt is random
      refute user1.password == user2.password

      # But both should still authenticate correctly
      assert {:ok, "alice"} = Acl.authenticate("alice", "same_pass")
    end

    test "unicode password hashing works" do
      unicode_pass = "пароль_密码_パスワード"
      assert :ok = Acl.set_user("alice", ["on", ">" <> unicode_pass])

      user = Acl.get_user("alice")
      refute user.password == unicode_pass

      assert {:ok, "alice"} = Acl.authenticate("alice", unicode_pass)
      assert {:error, _} = Acl.authenticate("alice", "wrong")
    end

    test "empty password hashing works" do
      assert :ok = Acl.set_user("alice", ["on", ">"])
      user = Acl.get_user("alice")

      # Even empty password should be hashed (not stored as "")
      refute user.password == ""
      assert is_binary(user.password)

      assert {:ok, "alice"} = Acl.authenticate("alice", "")
      assert {:error, _} = Acl.authenticate("alice", "notempty")
    end

    test "very long password hashing works" do
      long_pass = String.duplicate("x", 10_000)
      assert :ok = Acl.set_user("alice", ["on", ">" <> long_pass])
      assert {:ok, "alice"} = Acl.authenticate("alice", long_pass)
      assert {:error, _} = Acl.authenticate("alice", "short")
    end

    test "password update replaces old hash" do
      assert :ok = Acl.set_user("alice", ["on", ">first"])
      assert {:ok, "alice"} = Acl.authenticate("alice", "first")

      assert :ok = Acl.set_user("alice", [">second"])
      assert {:error, _} = Acl.authenticate("alice", "first")
      assert {:ok, "alice"} = Acl.authenticate("alice", "second")
    end

    test "nopass clears the hash and allows any password" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert :ok = Acl.set_user("alice", ["nopass"])

      user = Acl.get_user("alice")
      assert user.password == nil

      assert {:ok, "alice"} = Acl.authenticate("alice", "anything")
    end

    test "hash_for_display returns SHA-256 hex of the original password hash" do
      assert :ok = Acl.set_user("alice", ["on", ">mypass"])
      info = Acl.get_user_info("alice")
      pw_idx = Enum.find_index(info, &(&1 == "passwords"))
      passwords = Enum.at(info, pw_idx + 1)

      assert length(passwords) == 1
      [hash] = passwords
      # Should be a 64-char lowercase hex string (SHA-256 digest display)
      assert byte_size(hash) == 64
      assert String.match?(hash, ~r/^[0-9a-f]{64}$/)
    end
  end

  # ---------------------------------------------------------------------------
  # Fix 2: denied_commands set
  # ---------------------------------------------------------------------------

  describe "denied_commands" do
    test "+@all -FLUSHDB denies FLUSHDB" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-FLUSHDB"])
      assert {:error, msg} = Acl.check_command("alice", "FLUSHDB")
      assert msg =~ "NOPERM"
      assert msg =~ "flushdb"
    end

    test "+@all -FLUSHDB still allows other commands" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-FLUSHDB"])
      assert :ok = Acl.check_command("alice", "GET")
      assert :ok = Acl.check_command("alice", "SET")
      assert :ok = Acl.check_command("alice", "CONFIG")
    end

    test "+@all -@dangerous denies all dangerous commands" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-@dangerous"])

      dangerous = ~w(FLUSHDB FLUSHALL DEBUG CONFIG KEYS SHUTDOWN)

      for cmd <- dangerous do
        assert {:error, msg} = Acl.check_command("alice", cmd),
               "Expected #{cmd} to be denied"

        assert msg =~ "NOPERM"
      end
    end

    test "+@all -@dangerous still allows non-dangerous commands" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-@dangerous"])
      assert :ok = Acl.check_command("alice", "GET")
      assert :ok = Acl.check_command("alice", "SET")
      assert :ok = Acl.check_command("alice", "HGET")
    end

    test "+@all -@dangerous +DEBUG allows DEBUG (explicit allow overrides category deny)" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-@dangerous", "+DEBUG"])

      # DEBUG was denied by -@dangerous but then explicitly re-allowed by +DEBUG
      assert :ok = Acl.check_command("alice", "DEBUG")

      # Other dangerous commands are still denied
      assert {:error, _} = Acl.check_command("alice", "FLUSHDB")
      assert {:error, _} = Acl.check_command("alice", "FLUSHALL")
    end

    test "-@all +GET allows only GET (basic allowlist)" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+GET"])
      assert :ok = Acl.check_command("alice", "GET")
      assert {:error, _} = Acl.check_command("alice", "SET")
      assert {:error, _} = Acl.check_command("alice", "DEL")
    end

    test "denied_commands is case-insensitive" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-flushdb"])
      assert {:error, _} = Acl.check_command("alice", "FLUSHDB")
      assert {:error, _} = Acl.check_command("alice", "flushdb")
    end

    test "default user has empty denied_commands" do
      user = Acl.get_user("default")
      assert user.denied_commands == MapSet.new()
    end

    test "new user defaults to empty denied_commands" do
      assert :ok = Acl.set_user("alice", [])
      user = Acl.get_user("alice")
      assert user.denied_commands == MapSet.new()
    end

    test "new user with password starts with no command or key permissions" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])

      assert {:ok, "alice"} = Acl.authenticate("alice", "s3cret")
      assert {:error, _} = Acl.check_command("alice", "GET")
      assert {:error, _} = Acl.check_key_access("alice", "any:key", :read)

      user = Acl.get_user("alice")
      assert user.commands == MapSet.new()
      assert user.keys == []
    end

    test "new user requires explicit command and key grants" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret", "+GET", "~cache:*"])

      assert :ok = Acl.check_command("alice", "GET")
      assert {:error, _} = Acl.check_command("alice", "SET")
      assert :ok = Acl.check_key_access("alice", "cache:item", :read)
      assert {:error, _} = Acl.check_key_access("alice", "other:item", :read)
    end

    test "multiple denied commands accumulate" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-FLUSHDB", "-FLUSHALL", "-DEBUG"])

      assert {:error, _} = Acl.check_command("alice", "FLUSHDB")
      assert {:error, _} = Acl.check_command("alice", "FLUSHALL")
      assert {:error, _} = Acl.check_command("alice", "DEBUG")
      assert :ok = Acl.check_command("alice", "GET")
    end

    test "denied_commands persists across set_user updates" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-FLUSHDB"])
      # Update a different field
      assert :ok = Acl.set_user("alice", [">newpass"])

      # denied_commands should still be in effect
      assert {:error, _} = Acl.check_command("alice", "FLUSHDB")
    end
  end

  # ---------------------------------------------------------------------------
  # Fix 3: Protected mode
  # ---------------------------------------------------------------------------

  describe "protected mode" do
    test "protected_mode? returns true when config is true" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert Acl.protected_mode?() == true
    end

    test "protected_mode? defaults to true" do
      Application.delete_env(:ferricstore, :protected_mode)

      assert Acl.protected_mode?() == true
    end

    test "protected_mode? returns false when config is false" do
      Application.put_env(:ferricstore, :protected_mode, false)

      assert Acl.protected_mode?() == false
    end

    test "protected_mode? can be set to true" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert Acl.protected_mode?() == true
    end

    test "has_configured_users? returns false when only default user exists" do
      refute Acl.has_configured_users?()
    end

    test "has_configured_users? returns false when ACL ETS table is temporarily unavailable" do
      on_exit(fn ->
        FerricstoreServer.Acl.Tables.new_active_table()
        FerricstoreServer.Acl.Tables.insert_default_user()
      end)

      table = FerricstoreServer.Acl.Tables.active_table()
      :ets.delete(table)

      refute Acl.has_configured_users?()
    end

    test "has_configured_users? returns true when default user has a password" do
      assert :ok = Acl.set_user("default", ["on", ">s3cret"])
      assert Acl.has_configured_users?()
    end

    test "has_configured_users? returns true when requirepass is configured" do
      assert :ok = Ferricstore.Config.set("requirepass", "s3cret")
      assert Acl.has_configured_users?()
    end

    test "has_configured_users? returns true when non-default user with password exists" do
      assert :ok = Acl.set_user("admin", ["on", ">s3cret"])
      assert Acl.has_configured_users?()
    end

    test "has_configured_users? returns false for disabled user with password" do
      assert :ok = Acl.set_user("admin", ["off", ">s3cret"])
      refute Acl.has_configured_users?()
    end

    test "has_configured_users? returns false for user without password" do
      assert :ok = Acl.set_user("admin", ["on", "nopass"])
      refute Acl.has_configured_users?()
    end

    test "localhost? recognizes IPv4 loopback" do
      assert Acl.localhost?({{127, 0, 0, 1}, 12_345})
    end

    test "localhost? recognizes IPv6 loopback" do
      assert Acl.localhost?({{0, 0, 0, 0, 0, 0, 0, 1}, 12_345})
    end

    test "localhost? rejects non-loopback IPv4" do
      refute Acl.localhost?({{192, 168, 1, 1}, 12_345})
      refute Acl.localhost?({{10, 0, 0, 1}, 12_345})
    end

    test "localhost? rejects non-loopback IPv6" do
      refute Acl.localhost?({{0, 0, 0, 0, 0, 0, 0, 2}, 12_345})
    end

    test "check_protected_mode allows localhost regardless of config" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert :ok = Acl.check_protected_mode({{127, 0, 0, 1}, 12_345})
    end

    test "check_protected_mode rejects non-localhost when no configured users" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert {:error, msg} = Acl.check_protected_mode({{192, 168, 1, 1}, 12_345})
      assert msg =~ "DENIED"
      assert msg =~ "protected mode"
    end

    test "check_protected_mode allows non-localhost after creating ACL user" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert :ok = Acl.set_user("admin", ["on", ">s3cret"])
      assert :ok = Acl.check_protected_mode({{192, 168, 1, 1}, 12_345})
    end

    test "check_protected_mode allows non-localhost when default user is passworded" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert :ok = Acl.set_user("default", ["on", ">s3cret"])
      assert :ok = Acl.check_protected_mode({{192, 168, 1, 1}, 12_345})
    end

    test "check_protected_mode allows non-localhost when requirepass is configured" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert :ok = Ferricstore.Config.set("requirepass", "s3cret")
      assert :ok = Acl.check_protected_mode({{192, 168, 1, 1}, 12_345})
    end

    test "check_protected_mode allows everything when protected_mode is false" do
      Application.put_env(:ferricstore, :protected_mode, false)

      assert :ok = Acl.check_protected_mode({{192, 168, 1, 1}, 12_345})
    end

    test "check_protected_mode with nil peer is treated as non-localhost" do
      Application.put_env(:ferricstore, :protected_mode, true)

      assert {:error, _} = Acl.check_protected_mode(nil)
    end
  end

  # ---------------------------------------------------------------------------
  # Fix 4: max_acl_users
  # ---------------------------------------------------------------------------

  describe "max_acl_users" do
    test "creating user beyond limit returns error" do
      Application.put_env(:ferricstore, :max_acl_users, 3)

      # "default" already occupies 1 slot
      assert :ok = Acl.set_user("user1", ["on"])
      assert :ok = Acl.set_user("user2", ["on"])

      # Table now has 3 users (default + user1 + user2) = at limit
      assert {:error, msg} = Acl.set_user("user3", ["on"])
      assert msg =~ "max ACL users reached"
    end

    test "updating existing user within limit works" do
      Application.put_env(:ferricstore, :max_acl_users, 2)

      assert :ok = Acl.set_user("alice", ["on"])
      # Table has 2 users (default + alice) = at limit

      # Updating an existing user should still work
      assert :ok = Acl.set_user("alice", ["off"])
      user = Acl.get_user("alice")
      assert user.enabled == false
    end

    test "default limit is 10000" do
      Application.delete_env(:ferricstore, :max_acl_users)

      # Create a few users -- should not hit the 10000 limit
      for i <- 1..5 do
        assert :ok = Acl.set_user("user_#{i}", ["on"])
      end
    end

    test "deleting a user frees a slot" do
      Application.put_env(:ferricstore, :max_acl_users, 3)

      assert :ok = Acl.set_user("user1", ["on"])
      assert :ok = Acl.set_user("user2", ["on"])
      # At limit (3 users)

      assert {:error, _} = Acl.set_user("user3", ["on"])

      # Delete one to free a slot
      assert :ok = Acl.del_user("user1")

      # Now we can create user3
      assert :ok = Acl.set_user("user3", ["on"])
    end

    test "limit of 1 only allows default user" do
      Application.put_env(:ferricstore, :max_acl_users, 1)

      assert {:error, msg} = Acl.set_user("alice", ["on"])
      assert msg =~ "max ACL users reached"
    end
  end

  # ---------------------------------------------------------------------------
  # Fix 5: ACL LOG for command denials
  # ---------------------------------------------------------------------------

  describe "ACL LOG denials" do
    test "denied command is logged to audit log" do
      assert :ok = Acl.set_user("alice", ["on", ">pass", "-@all", "+GET"])

      # This should be denied
      {:error, _} = Acl.check_command("alice", "SET")

      # Now log the denial (this is what connection.ex will call)
      Acl.log_command_denied("alice", "SET", "127.0.0.1:1234", 42)
      Process.sleep(10)

      entries = AuditLog.get()
      denied_entries = Enum.filter(entries, fn {_, _, type, _} -> type == :command_denied end)
      assert denied_entries != []

      {_, _, :command_denied, details} = hd(denied_entries)
      assert details.username == "alice"
      assert details.command == "SET"
    end

    test "log entry has correct username, command, and client info" do
      Acl.log_command_denied("bob", "FLUSHDB", "10.0.0.1:5678", 99)
      Process.sleep(10)

      [{_, _, :command_denied, details}] = AuditLog.get()
      assert details.username == "bob"
      assert details.command == "FLUSHDB"
      assert details.client_ip == "10.0.0.1:5678"
      assert details.client_id == 99
    end

    test "multiple denials are all logged" do
      Acl.log_command_denied("alice", "SET", "127.0.0.1:1234", 1)
      Acl.log_command_denied("alice", "DEL", "127.0.0.1:1234", 1)
      Acl.log_command_denied("bob", "FLUSHDB", "10.0.0.1:5678", 2)
      Process.sleep(20)

      entries = AuditLog.get()
      denied_entries = Enum.filter(entries, fn {_, _, type, _} -> type == :command_denied end)
      assert length(denied_entries) == 3
    end

    test "native session channel ACL denials are logged to audit log" do
      assert :ok =
               Acl.set_user("subscriber", [
                 "on",
                 "nopass",
                 "-@all",
                 "+psubscribe",
                 "&tenant:a:*"
               ])

      state = %{
        username: "subscriber",
        authenticated: true,
        require_auth: false,
        acl_cache: ConnAuth.build_acl_cache("subscriber"),
        peer: {{127, 0, 0, 1}, 5_678},
        client_id: 123
      }

      assert {:noperm, payload, _state} =
               Session.execute(%{"command" => "PSUBSCRIBE", "args" => ["tenant:*"]}, state)

      assert payload =~ "channels mentioned"

      Process.sleep(10)

      [{_, _, :command_denied, details}] = AuditLog.get()
      assert details.username == "subscriber"
      assert details.command == "PSUBSCRIBE"
      assert details.client_ip == "127.0.0.1:5678"
      assert details.client_id == 123
    end

    test "denial log is a no-op when audit logging is disabled" do
      Application.put_env(:ferricstore, :audit_log_enabled, false)

      Acl.log_command_denied("alice", "SET", "127.0.0.1:1234", 1)
      Process.sleep(10)

      assert AuditLog.len() == 0
    end
  end
end
