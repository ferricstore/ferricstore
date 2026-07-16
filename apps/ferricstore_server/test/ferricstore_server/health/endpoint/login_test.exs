defmodule FerricstoreServer.Health.Endpoint.LoginTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
  alias FerricstoreServer.Acl.Password
  alias FerricstoreServer.Health.Endpoint.Login

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    Acl.reset!()

    on_exit(fn ->
      Acl.reset!()
    end)

    :ok
  end

  test "location encodes sanitized next path" do
    assert Login.location("/dashboard/flow?type=email") ==
             "/dashboard/login?next=%2Fdashboard%2Fflow%3Ftype%3Demail"
  end

  test "sanitize_next only allows dashboard-local paths" do
    assert Login.sanitize_next("/dashboard/flow") == "/dashboard/flow"
    assert Login.sanitize_next("/dashboard/login") == "/dashboard"
    assert Login.sanitize_next("//evil.test/dashboard") == "/dashboard"
    assert Login.sanitize_next("/not-dashboard") == "/dashboard"
    assert Login.sanitize_next("/dashboard\nx") == "/dashboard"
    assert Login.sanitize_next(nil) == "/dashboard"
  end

  test "render_page escapes error and next values" do
    html = Login.render_page("/dashboard/flow?x=\"bad\"", "<bad>")

    assert html =~ "&lt;bad&gt;"
    assert html =~ "/dashboard/flow?x=&quot;bad&quot;"
    assert html =~ "FerricStore Dashboard"
  end

  test "authenticate delegates to shared ACL authentication" do
    assert :ok = Acl.set_user("known", ["on", ">secret", "~*", "+@all"])

    assert {:error, _reason} = Login.authenticate("missing", "wrong")
    assert {:ok, "known"} = Login.authenticate("known", "secret")
  end

  test "successful dashboard authentication upgrades unversioned PBKDF2 hashes" do
    stored_hash = unversioned_pbkdf2_hash("secret")

    contents = "user default on nopass ~* &* +@all\nuser known on ##{stored_hash} ~* &* +@all\n"
    assert :ok = Acl.load_contents(contents)

    assert {:ok, "known", _auth_epoch} = Login.authenticate_session("known", "secret")
    assert String.starts_with?(Acl.get_user("known").password, "pbkdf2-sha256$")
  end

  @tag :dashboard_auth_epoch_race
  test "authentication fails closed when the verified ACL snapshot changes" do
    assert :ok = Acl.set_user("known", ["on", ">old-secret", "~*", "+@all"])
    initial_epoch = Acl.get_user("known").auth_epoch

    verifier = fn candidate, stored_hash ->
      assert Password.verify(candidate, stored_hash)
      assert :ok = Acl.set_user("known", ["on", ">new-secret", "~*", "+@all"])
      true
    end

    assert {:error, reason} = Login.authenticate_session("known", "old-secret", verifier)
    assert reason =~ "WRONGPASS"
    assert Acl.get_user("known").auth_epoch > initial_epoch
  end

  defp unversioned_pbkdf2_hash(password) do
    salt = String.duplicate("s", 16)
    hash = :crypto.pbkdf2_hmac(:sha256, password, salt, 100_000, 32)
    Base.encode64(salt <> hash)
  end
end
