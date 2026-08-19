defmodule FerricstoreServer.AclExpiryTest do
  use ExUnit.Case, async: false

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.{FileParser, Formatter}
  alias FerricstoreServer.Connection.Auth
  alias FerricstoreServer.Management.ACL, as: ManagementACL

  setup do
    Acl.reset!()
    on_exit(&Acl.reset!/0)
    :ok
  end

  test "expiring ACL credentials fail authentication and cached commands after their deadline" do
    expires_at_ms = System.system_time(:millisecond) + 60_000

    assert :ok =
             Acl.set_user("platform_cli_expiry", [
               "on",
               ">temporary-secret",
               "expireat:#{expires_at_ms}",
               "+@all",
               "~*"
             ])

    assert {:ok, "platform_cli_expiry"} =
             Acl.authenticate("platform_cli_expiry", "temporary-secret")

    cache = Auth.build_acl_cache("platform_cli_expiry")
    assert :ok = Auth.check_command_cached(cache, "PING")

    expired_at_ms = System.system_time(:millisecond) - 1

    assert :ok =
             Acl.set_user("platform_cli_expiry", ["expireat:#{expired_at_ms}"])

    expired_cache = Map.put(cache, :expires_at_ms, expired_at_ms)

    assert {:error, _reason} = Acl.authenticate("platform_cli_expiry", "temporary-secret")
    assert {:error, _reason} = Acl.check_permission("platform_cli_expiry", "PING")
    assert {:error, _reason} = Acl.check_command("platform_cli_expiry", "PING")
    assert {:error, _reason} = Acl.check_key_access("platform_cli_expiry", "key", :read)

    assert {:error, "NOPERM user session expired or user was deleted"} =
             Auth.check_command_cached(expired_cache, "PING")
  end

  test "expiry survives the durable catalog and ACL file codecs" do
    expires_at_ms = System.system_time(:millisecond) + 60_000

    assert {:ok, encoded} =
             ManagementACL.prepare_catalog_value(nil, "platform_cli_codec", [
               "on",
               ">temporary-secret",
               "expireat:#{expires_at_ms}",
               "+PING",
               "~tenant:a:*"
             ])

    assert {:ok, %{expires_at_ms: ^expires_at_ms} = user} =
             ManagementACL.decode_catalog_value(encoded)

    line = Formatter.format_user_for_file({"platform_cli_codec", user})

    assert {:ok, [{"platform_cli_codec", %{expires_at_ms: ^expires_at_ms}}]} =
             FileParser.parse(line)
  end

  test "persist removes a previously configured credential deadline" do
    assert :ok =
             Acl.set_user("platform_cli_persistent", [
               "on",
               ">secret",
               "expireat:0",
               "+PING",
               "~*"
             ])

    assert {:error, _reason} = Acl.authenticate("platform_cli_persistent", "secret")
    assert :ok = Acl.set_user("platform_cli_persistent", ["persist"])

    assert {:ok, "platform_cli_persistent"} =
             Acl.authenticate("platform_cli_persistent", "secret")
  end
end
