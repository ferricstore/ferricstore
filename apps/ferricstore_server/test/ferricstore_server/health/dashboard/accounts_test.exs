defmodule FerricstoreServer.Health.Dashboard.AccountsTest do
  use ExUnit.Case, async: false

  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Health.Dashboard.Accounts

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    Acl.reset!()

    on_exit(fn -> Acl.reset!() end)

    :ok
  end

  test "bootstrap validates a strong confirmed password before mutating ACL state" do
    assert Accounts.bootstrap_available?()

    assert {:error, message} =
             Accounts.bootstrap(%{
               "password" => "short",
               "password_confirmation" => "different"
             })

    assert message =~ "match"
    assert Acl.get_user("default").password == nil

    assert {:ok, "default"} =
             Accounts.bootstrap(%{
               "password" => "a-long-bootstrap-password",
               "password_confirmation" => "a-long-bootstrap-password"
             })

    refute Accounts.bootstrap_available?()
  end

  test "password minimum is measured in characters while allocation remains byte-bounded" do
    six_multibyte_characters = :binary.copy(<<0xC3, 0xA9>>, 6)

    assert byte_size(six_multibyte_characters) == 12

    assert {:error, message} =
             Accounts.bootstrap(%{
               "password" => six_multibyte_characters,
               "password_confirmation" => six_multibyte_characters
             })

    assert message =~ "12 characters"
    assert Acl.get_user("default").password == nil
  end

  test "creates full administrators and scoped observers without plaintext persistence" do
    assert {:ok, "admin"} =
             Accounts.create_user("default", %{
               "username" => "admin",
               "password" => "admin-password-123",
               "password_confirmation" => "admin-password-123",
               "role" => "admin"
             })

    assert :ok = Acl.check_command("admin", "ACL.SETUSER")
    assert :ok = Acl.check_key_access("admin", "any:key", :write)

    assert {:ok, "observer"} =
             Accounts.create_user("default", %{
               "username" => "observer",
               "password" => "observer-password-123",
               "password_confirmation" => "observer-password-123",
               "role" => "observer",
               "key_pattern" => "tenant-a:*",
               "channel_pattern" => "tenant-a:*"
             })

    assert :ok = Acl.check_command("observer", "GET")
    assert {:error, _reason} = Acl.check_command("observer", "SET")
    assert :ok = Acl.check_key_access("observer", "tenant-a:key", :read)
    assert {:error, _reason} = Acl.check_key_access("observer", "tenant-b:key", :read)
    assert {:error, _reason} = Acl.check_key_access("observer", "tenant-a:key", :write)
  end

  test "custom account modifiers cannot smuggle password or state changes" do
    for modifier <- [">hidden-password", "nopass", "resetpass", "off", "on"] do
      assert {:error, message} =
               Accounts.create_user("default", %{
                 "username" => "custom-#{modifier}",
                 "password" => "custom-password-123",
                 "password_confirmation" => "custom-password-123",
                 "role" => "custom",
                 "modifiers" => modifier
               })

      assert message =~ "separate controls"
    end
  end

  test "duplicate creation never changes the existing account password" do
    params = %{
      "username" => "duplicate",
      "password" => "original-password-123",
      "password_confirmation" => "original-password-123",
      "role" => "admin"
    }

    assert {:ok, "duplicate"} = Accounts.create_user("default", params)

    assert {:error, message} =
             Accounts.create_user("default", %{
               params
               | "password" => "replacement-password-123",
                 "password_confirmation" => "replacement-password-123"
             })

    assert message =~ "already exists"
    assert {:ok, "duplicate"} = Acl.authenticate("duplicate", "original-password-123")
    assert {:error, _reason} = Acl.authenticate("duplicate", "replacement-password-123")
  end

  test "dashboard safeguards the recovery account and the current principal" do
    assert {:error, default_message} = Accounts.set_enabled("default", "default", false)
    assert default_message =~ "recovery"

    assert :ok = Acl.set_user("operator", ["on", ">operator-password-123", "+@all"])

    assert {:error, self_message} = Accounts.set_enabled("operator", "operator", false)
    assert self_message =~ "own account"

    assert {:error, delete_message} = Accounts.delete_user("operator", "operator")
    assert delete_message =~ "own account"
    assert Acl.get_user("operator") != nil
  end

  test "password resets revoke the old credential and require confirmation" do
    assert :ok = Acl.set_user("member", ["on", ">old-password-123"])

    assert {:error, _message} =
             Accounts.reset_password("default", %{
               "username" => "member",
               "password" => "new-password-123",
               "password_confirmation" => "not-the-same"
             })

    assert {:ok, "member"} = Acl.authenticate("member", "old-password-123")

    assert {:ok, "member"} =
             Accounts.reset_password("default", %{
               "username" => "member",
               "password" => "new-password-123",
               "password_confirmation" => "new-password-123"
             })

    assert {:error, _reason} = Acl.authenticate("member", "old-password-123")
    assert {:ok, "member"} = Acl.authenticate("member", "new-password-123")
  end

  test "applies non-credential ACL modifiers through the advanced editor" do
    assert :ok = Acl.set_user("member", ["on", ">member-password-123", "-@all"])

    assert {:ok, "member"} =
             Accounts.apply_modifiers("default", %{
               "username" => "member",
               "modifiers" => "+GET\n%R~tenant-a:*"
             })

    assert :ok = Acl.check_command("member", "GET")
    assert :ok = Acl.check_key_access("member", "tenant-a:key", :read)

    assert {:error, message} =
             Accounts.apply_modifiers("default", %{
               "username" => "member",
               "modifiers" => ">hidden-password"
             })

    assert message =~ "separate controls"
  end

  test "preserves exact ACL usernames and pattern bytes" do
    username = " exact dashboard user "
    key_pattern = " exact key "
    channel_pattern = " exact channel "

    assert {:ok, ^username} =
             Accounts.create_user("default", %{
               "username" => username,
               "password" => "exact-password-123",
               "password_confirmation" => "exact-password-123",
               "role" => "observer",
               "key_pattern" => key_pattern,
               "channel_pattern" => channel_pattern
             })

    assert Acl.get_user(username) != nil
    assert :ok = Acl.check_key_access(username, key_pattern, :read)
    assert Acl.channel_matches_any?(channel_pattern, Acl.get_user(username).channels)

    replacement_pattern = " replacement key "

    assert {:ok, ^username} =
             Accounts.apply_modifiers("default", %{
               "username" => username,
               "modifiers" => "%R~#{replacement_pattern}"
             })

    assert :ok = Acl.check_key_access(username, replacement_pattern, :read)
  end
end
