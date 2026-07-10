defmodule FerricstoreServer.AclAuthTimingTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Password

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    Acl.reset!()

    on_exit(fn ->
      Acl.reset!()
    end)

    :ok
  end

  test "authentication performs one password verification regardless of user state" do
    test_pid = self()
    verifier = verifier_spy(test_pid)

    assert :ok = Acl.set_user("known", ["on", ">secret", "~*", "+@all"])
    assert :ok = Acl.set_user("disabled", ["off", ">secret", "~*", "+@all"])
    assert :ok = Acl.set_user("passwordless", ["on", "nopass", "~*", "+@all"])

    assert {:error, _reason} = Acl.authenticate("missing", "wrong", verifier)
    missing_hash = assert_single_verification("wrong")

    assert {:error, _reason} = Acl.authenticate("disabled", "wrong", verifier)
    disabled_hash = assert_single_verification("wrong")

    assert {:ok, "passwordless"} = Acl.authenticate("passwordless", "anything", verifier)
    passwordless_hash = assert_single_verification("anything")

    assert {:ok, "known"} = Acl.authenticate("known", "secret", verifier)
    known_hash = assert_single_verification("secret")

    assert missing_hash == disabled_hash
    assert missing_hash == passwordless_hash
    refute missing_hash == known_hash
  end

  defp assert_single_verification(password) do
    assert_receive {:password_verified, ^password, stored_hash}
    refute_receive {:password_verified, _password, _stored_hash}
    stored_hash
  end

  defp verifier_spy(test_pid) do
    fn password, stored_hash ->
      send(test_pid, {:password_verified, password, stored_hash})
      Password.verify(password, stored_hash)
    end
  end
end
