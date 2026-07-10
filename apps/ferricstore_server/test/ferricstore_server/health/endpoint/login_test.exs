defmodule FerricstoreServer.Health.Endpoint.LoginTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricstoreServer.Acl
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
end
