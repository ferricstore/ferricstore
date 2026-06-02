defmodule FerricstoreServer.AclTest do
  @moduledoc """
  Unit tests for the FerricstoreServer.Acl GenServer module.

  Tests the ACL module directly (not via TCP), covering user creation,
  modification, deletion, authentication, password handling, key patterns,
  command permissions, and edge cases.

  These tests are `async: false` because they share the global ACL ETS table
  and call `Acl.reset!/0` in setup to prevent cross-test contamination.
  """

  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl

  setup do
    Acl.reset!()
    :ok
  end

  # Helper: extracts the glob strings from compiled key patterns.
  defp key_globs(patterns) when is_list(patterns) do
    Enum.map(patterns, fn {glob, _mode, _regex} -> glob end)
  end

  # ---------------------------------------------------------------------------
  # init/0 — default user
  # ---------------------------------------------------------------------------

  describe "init creates default user" do
    test "default user exists after init" do
      user = Acl.get_user("default")
      assert user != nil
    end

    test "default user is enabled" do
      %{enabled: enabled} = Acl.get_user("default")
      assert enabled == true
    end

    test "default user has no password" do
      %{password: password} = Acl.get_user("default")
      assert password == nil
    end

    test "default user has all commands" do
      %{commands: commands} = Acl.get_user("default")
      assert commands == :all
    end

    test "default user has all keys" do
      %{keys: keys} = Acl.get_user("default")
      assert keys == :all
    end

    test "list_users includes default user after init" do
      users = Acl.list_users()
      assert length(users) == 1
      assert hd(users) =~ "user default on"
    end
  end

  # ---------------------------------------------------------------------------
  # set_user/2 — creating and updating users
  # ---------------------------------------------------------------------------

  describe "set_user/2 creates users" do
    test "creates a new user with on modifier" do
      assert :ok = Acl.set_user("alice", ["on"])
      user = Acl.get_user("alice")
      assert user.enabled == true
    end

    test "creates a new user with off modifier" do
      assert :ok = Acl.set_user("bob", ["off"])
      user = Acl.get_user("bob")
      assert user.enabled == false
    end

    test "new user defaults to disabled" do
      assert :ok = Acl.set_user("charlie", [])
      user = Acl.get_user("charlie")
      assert user.enabled == false
    end

    test "new user defaults to no password" do
      assert :ok = Acl.set_user("charlie", [])
      user = Acl.get_user("charlie")
      assert user.password == nil
    end

    test "new user defaults to no commands" do
      assert :ok = Acl.set_user("charlie", [])
      user = Acl.get_user("charlie")
      assert user.commands == MapSet.new()
    end

    test "new user defaults to empty denied_commands" do
      assert :ok = Acl.set_user("charlie", [])
      user = Acl.get_user("charlie")
      assert user.denied_commands == MapSet.new()
    end

    test "new user defaults to no keys" do
      assert :ok = Acl.set_user("charlie", [])
      user = Acl.get_user("charlie")
      assert user.keys == []
    end

    test "rejects unknown concrete command names" do
      assert {:error, msg} = Acl.set_user("typo", ["on", "-@all", "+gett"])
      assert msg =~ "Unknown command 'gett'"
      assert Acl.get_user("typo") == nil
    end
  end

  describe "set_user/2 with password modifier" do
    test ">password sets the user's password (stored as hash, not plaintext)" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      user = Acl.get_user("alice")
      # Password is hashed -- should not be plaintext
      refute user.password == "s3cret"
      assert is_binary(user.password)
      assert byte_size(user.password) > 0
      # But authentication should work
      assert {:ok, "alice"} = Acl.authenticate("alice", "s3cret")
    end

    test "nopass clears the password" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert :ok = Acl.set_user("alice", ["nopass"])
      user = Acl.get_user("alice")
      assert user.password == nil
    end

    test "resetpass clears the password" do
      assert :ok = Acl.set_user("alice", ["on", ">mypassword"])
      assert :ok = Acl.set_user("alice", ["resetpass"])
      user = Acl.get_user("alice")
      assert user.password == nil
    end

    test "password can be updated by calling set_user again" do
      assert :ok = Acl.set_user("alice", ["on", ">first"])
      assert :ok = Acl.set_user("alice", [">second"])
      # Old password should fail, new one should work
      assert {:error, _} = Acl.authenticate("alice", "first")
      assert {:ok, "alice"} = Acl.authenticate("alice", "second")
    end

    test "empty password via > modifier" do
      assert :ok = Acl.set_user("alice", ["on", ">"])
      user = Acl.get_user("alice")
      # Even empty password is hashed
      refute user.password == ""
      assert is_binary(user.password)
      # But authentication with "" should work
      assert {:ok, "alice"} = Acl.authenticate("alice", "")
      assert {:error, _} = Acl.authenticate("alice", "notempty")
    end

    test "very long password is accepted" do
      long_pass = String.duplicate("a", 10_000)
      assert :ok = Acl.set_user("alice", ["on", ">" <> long_pass])
      user = Acl.get_user("alice")
      # Password is hashed
      refute user.password == long_pass
      assert {:ok, "alice"} = Acl.authenticate("alice", long_pass)
    end

    test "unicode password is accepted" do
      unicode_pass = "пароль_密码_パスワード"
      assert :ok = Acl.set_user("alice", ["on", ">" <> unicode_pass])
      user = Acl.get_user("alice")
      # Password is hashed
      refute user.password == unicode_pass
      assert {:ok, "alice"} = Acl.authenticate("alice", unicode_pass)
    end
  end

  describe "set_user/2 with key pattern modifier" do
    test "~pattern adds a key pattern" do
      assert :ok = Acl.set_user("alice", ["on", "~cache:*"])
      user = Acl.get_user("alice")
      assert key_globs(user.keys) == ["cache:*"]
    end

    test "multiple key patterns accumulate" do
      assert :ok = Acl.set_user("alice", ["on", "~cache:*", "~session:*"])
      user = Acl.get_user("alice")
      assert key_globs(user.keys) == ["cache:*", "session:*"]
    end

    test "many key patterns preserve order without per-pattern list appends" do
      source =
        Path.join([__DIR__, "../../lib/ferricstore_server/acl.ex"])
        |> Path.expand()
        |> File.read!()

      refute source =~ "patterns ++ [compiled_pattern]"

      patterns = Enum.map(1..128, &"tenant:#{&1}:*")
      rules = ["on" | Enum.map(patterns, &"~#{&1}")]

      assert :ok = Acl.set_user("alice", rules)
      user = Acl.get_user("alice")
      assert key_globs(user.keys) == patterns
    end

    test "allkeys sets keys to :all" do
      assert :ok = Acl.set_user("alice", ["on", "~cache:*"])
      assert :ok = Acl.set_user("alice", ["allkeys"])
      user = Acl.get_user("alice")
      assert user.keys == :all
    end

    test "resetkeys and allkeys discard queued key patterns in order" do
      assert :ok = Acl.set_user("alice", ["on", "~before:*", "resetkeys", "~after:*"])
      assert key_globs(Acl.get_user("alice").keys) == ["after:*"]

      assert :ok = Acl.set_user("bob", ["on", "~before:*", "allkeys", "~after:*"])
      assert key_globs(Acl.get_user("bob").keys) == ["after:*"]
    end

    test "~* on new user replaces default :all with explicit pattern" do
      assert :ok = Acl.set_user("alice", ["on", "~*"])
      user = Acl.get_user("alice")
      # When keys is :all and we add a pattern, it becomes a list
      assert key_globs(user.keys) == ["*"]
    end
  end

  describe "set_user/2 with command modifier" do
    test "+command allows a specific command" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+get"])
      user = Acl.get_user("alice")
      assert MapSet.member?(user.commands, "GET")
    end

    test "+@all allows all commands" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+@all"])
      user = Acl.get_user("alice")
      assert user.commands == :all
    end

    test "allcommands allows all commands" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "allcommands"])
      user = Acl.get_user("alice")
      assert user.commands == :all
    end

    test "-command denies a specific command from MapSet" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+get", "+set", "-get"])
      user = Acl.get_user("alice")
      refute MapSet.member?(user.commands, "GET")
      assert MapSet.member?(user.commands, "SET")
    end

    test "-@all clears all command permissions" do
      assert :ok = Acl.set_user("alice", ["on", "-@all"])
      user = Acl.get_user("alice")
      assert user.commands == MapSet.new()
    end

    test "+command is case-insensitive (stored uppercased)" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+get"])
      user = Acl.get_user("alice")
      assert MapSet.member?(user.commands, "GET")
      refute MapSet.member?(user.commands, "get")
    end

    test "+command accepts Redis subcommand pipe syntax without granting parent command" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+acl|whoami"])

      user = Acl.get_user("alice")
      assert MapSet.member?(user.commands, "ACL.WHOAMI")
      assert :ok = Acl.check_command("alice", "ACL.WHOAMI")
      assert {:error, _} = Acl.check_command("alice", "ACL.SETUSER")
    end

    test "+parent command grants known ACL subcommands for existing admin rules" do
      assert :ok = Acl.set_user("alice", ["on", "-@all", "+acl"])

      assert :ok = Acl.check_command("alice", "ACL.SETUSER")
      assert :ok = Acl.check_command("alice", "ACL.GETUSER")
    end

    test "+command rejects unknown Redis subcommand pipe syntax" do
      assert {:error, msg} = Acl.set_user("alice", ["on", "-@all", "+acl|notacommand"])
      assert msg =~ "Unknown command 'acl|notacommand'"
      assert Acl.get_user("alice") == nil
    end

    test "-command from :all adds to denied_commands" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "-get"])
      user = Acl.get_user("alice")
      # Still :all, but GET is in denied_commands
      assert user.commands == :all
      assert MapSet.member?(user.denied_commands, "GET")
      # check_command should deny GET
      assert {:error, _} = Acl.check_command("alice", "GET")
      # Other commands are still allowed
      assert :ok = Acl.check_command("alice", "SET")
    end

    test "+command when already :all is a no-op" do
      assert :ok = Acl.set_user("alice", ["on", "+@all", "+get"])
      user = Acl.get_user("alice")
      assert user.commands == :all
    end
  end

  describe "set_user/2 with combined rules" do
    test "applies all rules in order" do
      assert :ok =
               Acl.set_user("alice", [
                 "on",
                 ">s3cret",
                 "~cache:*",
                 "-@all",
                 "+get",
                 "+set"
               ])

      user = Acl.get_user("alice")
      assert user.enabled == true
      # Password is hashed, not plaintext
      refute user.password == "s3cret"
      assert is_binary(user.password)
      assert {:ok, "alice"} = Acl.authenticate("alice", "s3cret")
      assert key_globs(user.keys) == ["cache:*"]
      assert MapSet.member?(user.commands, "GET")
      assert MapSet.member?(user.commands, "SET")
      assert MapSet.size(user.commands) == 2
    end

    test "updates existing user preserving unchanged fields" do
      assert :ok = Acl.set_user("alice", ["on", ">pass1", "~*"])
      assert :ok = Acl.set_user("alice", [">pass2"])

      # Password updated -- old fails, new works
      assert {:error, _} = Acl.authenticate("alice", "pass1")
      assert {:ok, "alice"} = Acl.authenticate("alice", "pass2")
      # Other fields preserved
      user = Acl.get_user("alice")
      assert user.enabled == true
    end
  end

  describe "set_user/2 with invalid rules" do
    test "invalid modifier returns error" do
      result = Acl.set_user("alice", ["invalid_modifier"])
      assert {:error, msg} = result
      assert msg =~ "Syntax error"
    end

    test "invalid rule does not partially apply previous rules" do
      # First create alice
      assert :ok = Acl.set_user("alice", ["on", ">original"])

      # Try to update with an invalid rule after a valid one
      result = Acl.set_user("alice", ["off", "invalid_modifier"])
      assert {:error, _} = result

      # alice should still be unchanged (the set_user call failed atomically)
      # Note: The implementation applies rules sequentially and returns error on first bad rule.
      # Since the GenServer returns {:error, ...} on bad rules, the ETS insert is skipped.
      user = Acl.get_user("alice")
      assert user.enabled == true
      # Password is hashed, so verify via authentication instead of direct comparison
      assert {:ok, "alice"} = Acl.authenticate("alice", "original")
    end
  end

  # ---------------------------------------------------------------------------
  # del_user/1
  # ---------------------------------------------------------------------------

  describe "del_user/1" do
    test "deletes an existing user" do
      assert :ok = Acl.set_user("alice", ["on"])
      assert :ok = Acl.del_user("alice")
      assert Acl.get_user("alice") == nil
    end

    test "cannot delete the default user" do
      result = Acl.del_user("default")
      assert {:error, msg} = result
      assert msg =~ "default"
      assert msg =~ "cannot be removed"
    end

    test "returns error for non-existent user" do
      result = Acl.del_user("nonexistent")
      assert {:error, msg} = result
      assert msg =~ "does not exist"
    end

    test "deleted user is removed from list_users" do
      assert :ok = Acl.set_user("alice", ["on"])
      assert :ok = Acl.del_user("alice")
      users = Acl.list_users()
      refute Enum.any?(users, &(&1 =~ "alice"))
    end
  end

  # ---------------------------------------------------------------------------
  # list_users/0
  # ---------------------------------------------------------------------------

  describe "list_users/0" do
    test "returns all users in Redis format" do
      assert :ok = Acl.set_user("alice", ["on", ">pass", "~cache:*"])
      assert :ok = Acl.set_user("bob", ["off"])

      users = Acl.list_users()
      assert length(users) == 3

      # Check they are all strings
      assert Enum.all?(users, &is_binary/1)
    end

    test "users are sorted alphabetically by name" do
      assert :ok = Acl.set_user("zara", ["on"])
      assert :ok = Acl.set_user("alice", ["on"])
      assert :ok = Acl.set_user("bob", ["on"])

      users = Acl.list_users()

      names =
        Enum.map(users, fn s ->
          # Extract username from "user <name> on/off ..."
          s |> String.split(" ") |> Enum.at(1)
        end)

      assert names == Enum.sort(names)
    end

    test "format includes user prefix, name, enabled flag, keys, channels, commands" do
      users = Acl.list_users()
      default_entry = hd(users)

      assert default_entry =~ "user default"
      assert default_entry =~ "on"
      assert default_entry =~ "~*"
      assert default_entry =~ "&*"
      assert default_entry =~ "+@all"
    end

    test "disabled user shows off flag" do
      assert :ok = Acl.set_user("alice", ["off"])
      users = Acl.list_users()
      alice_entry = Enum.find(users, &(&1 =~ "alice"))
      assert alice_entry =~ "off"
    end
  end

  # ---------------------------------------------------------------------------
  # get_user/1
  # ---------------------------------------------------------------------------

  describe "get_user/1" do
    test "returns user map for existing user" do
      user = Acl.get_user("default")
      assert is_map(user)
      assert Map.has_key?(user, :enabled)
      assert Map.has_key?(user, :password)
      assert Map.has_key?(user, :commands)
      assert Map.has_key?(user, :keys)
    end

    test "returns nil for non-existent user" do
      assert Acl.get_user("nonexistent") == nil
    end
  end

  # ---------------------------------------------------------------------------
  # get_user_info/1
  # ---------------------------------------------------------------------------

  describe "get_user_info/1" do
    test "returns Redis-compatible flat list for existing user" do
      info = Acl.get_user_info("default")
      assert is_list(info)

      # The format is: ["flags", [flags], "passwords", [passwords], "commands", cmd_str, "keys", keys_str, "channels", "&*"]
      assert "flags" in info
      assert "passwords" in info
      assert "commands" in info
      assert "keys" in info
      assert "channels" in info
    end

    test "returns nil for non-existent user" do
      assert Acl.get_user_info("nonexistent") == nil
    end

    test "enabled user has 'on' flag" do
      info = Acl.get_user_info("default")
      flags_idx = Enum.find_index(info, &(&1 == "flags"))
      flags = Enum.at(info, flags_idx + 1)
      assert "on" in flags
    end

    test "disabled user has 'off' flag" do
      assert :ok = Acl.set_user("alice", ["off"])
      info = Acl.get_user_info("alice")
      flags_idx = Enum.find_index(info, &(&1 == "flags"))
      flags = Enum.at(info, flags_idx + 1)
      assert "off" in flags
    end

    test "user with no password has empty passwords list" do
      info = Acl.get_user_info("default")
      pw_idx = Enum.find_index(info, &(&1 == "passwords"))
      passwords = Enum.at(info, pw_idx + 1)
      assert passwords == []
    end

    test "user with password has SHA-256 hash in passwords list" do
      assert :ok = Acl.set_user("alice", ["on", ">mypass"])
      info = Acl.get_user_info("alice")
      pw_idx = Enum.find_index(info, &(&1 == "passwords"))
      passwords = Enum.at(info, pw_idx + 1)
      assert length(passwords) == 1

      # Verify it's a hex-encoded SHA-256 hash (64 chars)
      [hash] = passwords
      assert byte_size(hash) == 64
      assert String.match?(hash, ~r/^[0-9a-f]{64}$/)
    end

    test "user with :all commands shows +@all" do
      info = Acl.get_user_info("default")
      cmd_idx = Enum.find_index(info, &(&1 == "commands"))
      commands = Enum.at(info, cmd_idx + 1)
      assert commands == "+@all"
    end

    test "user with :all keys shows ~*" do
      info = Acl.get_user_info("default")
      keys_idx = Enum.find_index(info, &(&1 == "keys"))
      keys = Enum.at(info, keys_idx + 1)
      assert keys == "~*"
    end

    test "channels always shows &*" do
      info = Acl.get_user_info("default")
      ch_idx = Enum.find_index(info, &(&1 == "channels"))
      channels = Enum.at(info, ch_idx + 1)
      assert channels == "&*"
    end

    test "channels reports explicit channel ACL patterns" do
      assert :ok = Acl.set_user("subscriber", ["on", "nopass", "-@all", "+subscribe", "&news:*"])

      info = Acl.get_user_info("subscriber")
      ch_idx = Enum.find_index(info, &(&1 == "channels"))
      channels = Enum.at(info, ch_idx + 1)

      assert channels == "&news:*"
      assert Enum.any?(Acl.list_users(), &String.contains?(&1, "&news:*"))
    end
  end

  # ---------------------------------------------------------------------------
  # authenticate/2
  # ---------------------------------------------------------------------------

  describe "authenticate/2" do
    test "succeeds for user with matching password" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert {:ok, "alice"} = Acl.authenticate("alice", "s3cret")
    end

    test "fails for wrong password" do
      assert :ok = Acl.set_user("alice", ["on", ">s3cret"])
      assert {:error, msg} = Acl.authenticate("alice", "wrong")
      assert msg =~ "WRONGPASS"
    end

    test "fails for non-existent user" do
      assert {:error, msg} = Acl.authenticate("nonexistent", "pass")
      assert msg =~ "WRONGPASS"
    end

    test "disabled user cannot authenticate" do
      assert :ok = Acl.set_user("alice", ["off", ">s3cret"])
      assert {:error, msg} = Acl.authenticate("alice", "s3cret")
      assert msg =~ "WRONGPASS"
    end

    test "user with no password (nopass) accepts any password" do
      assert :ok = Acl.set_user("alice", ["on", "nopass"])
      assert {:ok, "alice"} = Acl.authenticate("alice", "anything")
    end

    test "default user with no password accepts any password" do
      assert {:ok, "default"} = Acl.authenticate("default", "any_password")
    end

    test "default user with no password accepts empty string" do
      assert {:ok, "default"} = Acl.authenticate("default", "")
    end

    test "unicode password authentication" do
      unicode_pass = "пароль_密码_パスワード"
      assert :ok = Acl.set_user("alice", ["on", ">" <> unicode_pass])
      assert {:ok, "alice"} = Acl.authenticate("alice", unicode_pass)
      assert {:error, _} = Acl.authenticate("alice", "wrong")
    end

    test "very long password authentication" do
      long_pass = String.duplicate("x", 10_000)
      assert :ok = Acl.set_user("alice", ["on", ">" <> long_pass])
      assert {:ok, "alice"} = Acl.authenticate("alice", long_pass)
    end

    test "empty password via > modifier" do
      assert :ok = Acl.set_user("alice", ["on", ">"])
      # Password is "" — only "" should match
      assert {:ok, "alice"} = Acl.authenticate("alice", "")
      assert {:error, _} = Acl.authenticate("alice", "notempty")
    end

    test "resetpass clears password and user becomes nopass" do
      assert :ok = Acl.set_user("alice", ["on", ">secret"])
      assert :ok = Acl.set_user("alice", ["resetpass"])
      # After resetpass, password is nil, so any password is accepted (nopass mode)
      assert {:ok, "alice"} = Acl.authenticate("alice", "anything")
    end
  end

  # ---------------------------------------------------------------------------
  # check_permission/2
  # ---------------------------------------------------------------------------

  describe "check_permission/2" do
    test "returns :ok for enabled user" do
      assert :ok = Acl.check_permission("default", "GET")
    end

    test "returns error for non-existent user" do
      assert {:error, msg} = Acl.check_permission("ghost", "GET")
      assert msg =~ "NOPERM"
      assert msg =~ "ghost"
    end

    test "returns error for disabled user" do
      assert :ok = Acl.set_user("alice", ["off"])
      assert {:error, msg} = Acl.check_permission("alice", "GET")
      assert msg =~ "NOPERM"
      assert msg =~ "disabled"
    end
  end

  # ---------------------------------------------------------------------------
  # reset!/0
  # ---------------------------------------------------------------------------

  describe "reset!/0" do
    test "removes all users except default" do
      assert :ok = Acl.set_user("alice", ["on"])
      assert :ok = Acl.set_user("bob", ["on"])
      assert :ok = Acl.reset!()

      users = Acl.list_users()
      assert length(users) == 1
      assert hd(users) =~ "user default"
    end

    test "default user is re-initialized after reset" do
      # Modify default user
      assert :ok = Acl.set_user("default", ["off"])
      assert :ok = Acl.reset!()

      user = Acl.get_user("default")
      assert user.enabled == true
      assert user.password == nil
      assert user.commands == :all
      assert user.keys == :all
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "empty username is allowed" do
      assert :ok = Acl.set_user("", ["on"])
      user = Acl.get_user("")
      assert user != nil
      assert user.enabled == true
    end

    test "username with special characters is allowed" do
      assert :ok = Acl.set_user("user@domain.com", ["on"])
      assert Acl.get_user("user@domain.com") != nil
    end

    test "username with spaces is allowed" do
      assert :ok = Acl.set_user("user name", ["on"])
      assert Acl.get_user("user name") != nil
    end

    test "username is case-sensitive" do
      assert :ok = Acl.set_user("Alice", ["on"])
      assert :ok = Acl.set_user("alice", ["off"])

      assert Acl.get_user("Alice").enabled == true
      assert Acl.get_user("alice").enabled == false
    end

    test "creating same user twice updates rather than duplicates" do
      assert :ok = Acl.set_user("alice", ["on", ">first"])
      assert :ok = Acl.set_user("alice", [">second"])

      # Verify old password fails and new password works
      assert {:error, _} = Acl.authenticate("alice", "first")
      assert {:ok, "alice"} = Acl.authenticate("alice", "second")
      # Verify only one alice in the user list
      users = Acl.list_users()
      alice_count = Enum.count(users, &(&1 =~ "user alice"))
      assert alice_count == 1
    end

    test "password with > character inside" do
      assert :ok = Acl.set_user("alice", ["on", ">pass>word"])
      # Password is hashed, not stored as plaintext
      assert {:ok, "alice"} = Acl.authenticate("alice", "pass>word")
    end

    test "set_user with many rules in one call" do
      rules = ["on", ">pass", "~prefix:*", "~other:*", "-@all", "+get", "+set", "+del"]
      assert :ok = Acl.set_user("alice", rules)

      user = Acl.get_user("alice")
      assert user.enabled == true
      # Password is hashed
      assert is_binary(user.password)
      assert {:ok, "alice"} = Acl.authenticate("alice", "pass")
      assert key_globs(user.keys) == ["prefix:*", "other:*"]
      assert MapSet.size(user.commands) == 3
    end
  end

  # ---------------------------------------------------------------------------
  # check_key_access/3 — key pattern enforcement
  # ---------------------------------------------------------------------------

  describe "check_key_access/3" do
    test "default user (~*) allows all keys" do
      assert :ok = Acl.check_key_access("default", "any:key", :read)
      assert :ok = Acl.check_key_access("default", "any:key", :write)
    end

    test "user with ~cache:* allows matching keys" do
      Acl.set_user("alice", ["on", "~cache:*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "cache:foo", :read)
      assert :ok = Acl.check_key_access("alice", "cache:bar:baz", :write)
    end

    test "user with ~cache:* denies non-matching keys" do
      Acl.set_user("alice", ["on", "~cache:*", "+@all"])

      assert {:error, "NOPERM" <> _} =
               Acl.check_key_access("alice", "session:foo", :read)
    end

    test "multiple patterns — any match allows" do
      Acl.set_user("alice", ["on", "~cache:*", "~session:*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "cache:x", :read)
      assert :ok = Acl.check_key_access("alice", "session:y", :write)

      assert {:error, _} = Acl.check_key_access("alice", "user:z", :read)
    end

    test "%R~ pattern allows reads but denies writes" do
      Acl.set_user("alice", ["on", "%R~readonly:*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "readonly:key1", :read)

      assert {:error, "NOPERM" <> _} =
               Acl.check_key_access("alice", "readonly:key1", :write)
    end

    test "%W~ pattern allows writes but denies reads" do
      Acl.set_user("alice", ["on", "%W~writeonly:*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "writeonly:key1", :write)

      assert {:error, "NOPERM" <> _} =
               Acl.check_key_access("alice", "writeonly:key1", :read)
    end

    test "combined %R~ and %W~ patterns" do
      Acl.set_user("alice", ["on", "%R~read:*", "%W~write:*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "read:x", :read)
      assert :ok = Acl.check_key_access("alice", "write:x", :write)

      assert {:error, _} = Acl.check_key_access("alice", "read:x", :write)
      assert {:error, _} = Acl.check_key_access("alice", "write:x", :read)
    end

    test "~* explicit pattern allows all keys" do
      Acl.set_user("alice", ["on", "~*", "+@all"])
      assert :ok = Acl.check_key_access("alice", "anything:at:all", :read)
      assert :ok = Acl.check_key_access("alice", "anything:at:all", :write)
    end

    test "disabled user is denied" do
      Acl.set_user("alice", ["off", "~*", "+@all"])

      assert {:error, "NOPERM" <> _} =
               Acl.check_key_access("alice", "key", :read)
    end

    test "nonexistent user is denied" do
      assert {:error, "NOPERM" <> _} =
               Acl.check_key_access("nobody", "key", :read)
    end

    test "? glob matches single character" do
      Acl.set_user("alice", ["on", "~user:?", "+@all"])
      assert :ok = Acl.check_key_access("alice", "user:a", :read)
      assert {:error, _} = Acl.check_key_access("alice", "user:ab", :read)
    end

    test "[abc] glob matches character set" do
      Acl.set_user("alice", ["on", "~key:[abc]", "+@all"])
      assert :ok = Acl.check_key_access("alice", "key:a", :read)
      assert :ok = Acl.check_key_access("alice", "key:b", :read)
      assert {:error, _} = Acl.check_key_access("alice", "key:d", :read)
    end

    test "resetkeys clears all patterns" do
      Acl.set_user("alice", ["on", "~cache:*", "resetkeys", "+@all"])
      user = Acl.get_user("alice")
      assert user.keys == []

      assert {:error, _} = Acl.check_key_access("alice", "cache:foo", :read)
    end
  end

  # ---------------------------------------------------------------------------
  # compile_glob/1 — glob pattern compilation
  # ---------------------------------------------------------------------------

  describe "compile_glob/1" do
    test "* matches everything" do
      regex = Acl.compile_glob("*")
      assert Regex.match?(regex, "anything")
      assert Regex.match?(regex, "")
    end

    test "prefix:* matches prefix keys" do
      regex = Acl.compile_glob("cache:*")
      assert Regex.match?(regex, "cache:foo")
      assert Regex.match?(regex, "cache:")
      refute Regex.match?(regex, "session:foo")
    end

    test "? matches single character" do
      regex = Acl.compile_glob("k?y")
      assert Regex.match?(regex, "key")
      assert Regex.match?(regex, "kxy")
      refute Regex.match?(regex, "keey")
    end

    test "[abc] matches character class" do
      regex = Acl.compile_glob("[abc]")
      assert Regex.match?(regex, "a")
      assert Regex.match?(regex, "b")
      refute Regex.match?(regex, "d")
    end

    test "literal special regex chars are escaped" do
      regex = Acl.compile_glob("foo.bar")
      assert Regex.match?(regex, "foo.bar")
      refute Regex.match?(regex, "fooXbar")
    end

    test "exact key match (no wildcards)" do
      regex = Acl.compile_glob("mykey")
      assert Regex.match?(regex, "mykey")
      refute Regex.match?(regex, "mykey2")
      refute Regex.match?(regex, "xmykey")
    end
  end
end
