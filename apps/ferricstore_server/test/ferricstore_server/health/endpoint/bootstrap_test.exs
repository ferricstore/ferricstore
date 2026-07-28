defmodule FerricstoreServer.Health.Endpoint.BootstrapTest do
  use ExUnit.Case, async: false

  @moduletag :global_state
  @runtime_config Path.expand("../../../../../../config/runtime.exs", __DIR__)

  alias FerricstoreServer.Health.Endpoint.Bootstrap

  setup do
    previous_token = Application.get_env(:ferricstore, :dashboard_bootstrap_token)
    previous_trust_proxy = Application.get_env(:ferricstore, :dashboard_trust_proxy_headers)
    previous_trusted_proxies = Application.get_env(:ferricstore, :dashboard_trusted_proxies)
    Application.delete_env(:ferricstore, :dashboard_bootstrap_token)
    Application.delete_env(:ferricstore, :dashboard_trust_proxy_headers)
    Application.delete_env(:ferricstore, :dashboard_trusted_proxies)

    on_exit(fn ->
      restore_env(:dashboard_bootstrap_token, previous_token)
      restore_env(:dashboard_trust_proxy_headers, previous_trust_proxy)
      restore_env(:dashboard_trusted_proxies, previous_trusted_proxies)
    end)

    :ok
  end

  test "loopback bootstrap does not require an external token" do
    assert :ok = Bootstrap.authorize({127, 0, 0, 1}, %{}, "")
    assert :ok = Bootstrap.authorize({0, 0, 0, 0, 0, 0, 0, 1}, %{}, "")
  end

  test "remote bootstrap requires the configured one-time token" do
    token = String.duplicate("t", 32)
    Application.put_env(:ferricstore, :dashboard_bootstrap_token, token)

    assert {:error, _message} = Bootstrap.authorize({198, 51, 100, 9}, %{}, "")
    assert {:error, _message} = Bootstrap.authorize({198, 51, 100, 9}, %{}, "wrong")
    assert :ok = Bootstrap.authorize({198, 51, 100, 9}, %{}, token)
  end

  test "remote bootstrap fails closed when no token is configured" do
    assert {:error, message} = Bootstrap.authorize({198, 51, 100, 9}, %{}, "anything")
    assert message =~ "not configured"
  end

  test "a loopback reverse proxy never receives the direct-loopback token exemption" do
    token = String.duplicate("t", 32)
    Application.put_env(:ferricstore, :dashboard_bootstrap_token, token)
    Application.put_env(:ferricstore, :dashboard_trust_proxy_headers, true)
    Application.put_env(:ferricstore, :dashboard_trusted_proxies, ["127.0.0.1/32"])

    for headers <- [
          %{"x-forwarded-proto" => "https"},
          %{"x-forwarded-proto" => "https", "x-forwarded-for" => "198.51.100.9"}
        ] do
      assert Bootstrap.token_required?({127, 0, 0, 1}, headers)
      assert {:error, _message} = Bootstrap.authorize({127, 0, 0, 1}, headers, "")
      assert :ok = Bootstrap.authorize({127, 0, 0, 1}, headers, token)
    end
  end

  test "setup redirects are restricted to dashboard-local destinations" do
    assert Bootstrap.location("/dashboard/security") ==
             "/dashboard/setup?next=%2Fdashboard%2Fsecurity"

    assert Bootstrap.location("https://attacker.example") ==
             "/dashboard/setup?next=%2Fdashboard"
  end

  test "setup page shares the accessible auth surface and only requests remote tokens" do
    local_html = Bootstrap.render_page("/dashboard", "<invalid>", false)
    remote_html = Bootstrap.render_page("/dashboard", nil, true)

    assert local_html =~ "Create the recovery administrator"
    assert local_html =~ ~s(class="auth-context")
    assert local_html =~ ~s(role="alert")
    assert local_html =~ "&lt;invalid&gt;"
    refute local_html =~ ~s(name="bootstrap_token")

    assert remote_html =~ ~s(name="bootstrap_token")
    assert remote_html =~ "mounted secret file"
  end

  test "production config reads a bounded bootstrap token from a secret file" do
    token = " " <> String.duplicate("t", 32) <> " "
    path = temporary_secret_file(token <> "\r\n")
    put_system_env("FERRICSTORE_DASHBOARD_BOOTSTRAP_TOKEN_FILE", path)

    assert runtime_ferricstore_config()[:dashboard_bootstrap_token] == token
  end

  test "production config rejects short and oversized bootstrap token files" do
    for {contents, expected_message} <- [
          {String.duplicate("t", 31) <> "\n", "must contain between 32 and 4096 bytes"},
          {String.duplicate("t", 4_097), "must contain between 32 and 4096 bytes"}
        ] do
      path = temporary_secret_file(contents)
      put_system_env("FERRICSTORE_DASHBOARD_BOOTSTRAP_TOKEN_FILE", path)

      assert_raise RuntimeError, ~r/#{expected_message}/, fn ->
        Config.Reader.read!(@runtime_config, env: :prod)
      end
    end
  end

  test "production config fails when the bootstrap token file cannot be read" do
    path = Path.join(System.tmp_dir!(), "missing-ferricstore-bootstrap-token")
    File.rm(path)
    put_system_env("FERRICSTORE_DASHBOARD_BOOTSTRAP_TOKEN_FILE", path)

    assert_raise RuntimeError, ~r/could not be read/, fn ->
      Config.Reader.read!(@runtime_config, env: :prod)
    end
  end

  defp runtime_ferricstore_config do
    @runtime_config
    |> Config.Reader.read!(env: :prod)
    |> Keyword.get_values(:ferricstore)
    |> Enum.reduce([], &Keyword.merge(&2, &1))
  end

  defp temporary_secret_file(contents) do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-bootstrap-token-#{System.unique_integer([:positive, :monotonic])}"
      )

    File.write!(path, contents, [:exclusive])
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp put_system_env(name, value) do
    previous = System.get_env(name)
    System.put_env(name, value)
    on_exit(fn -> restore_system_env(name, previous) end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp restore_system_env(name, nil), do: System.delete_env(name)
  defp restore_system_env(name, value), do: System.put_env(name, value)
end
