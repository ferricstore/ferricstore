defmodule FerricstoreHttp.AuthTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Auth.Identity
  alias FerricstoreHttp.Auth.Providers.Basic

  defmodule RecordingProvider do
    @behaviour FerricstoreHttp.Auth.Provider

    @impl FerricstoreHttp.Auth.Provider
    def authenticate(request, opts), do: {:ok, {:custom_identity, request, opts}}
  end

  test "authenticates standard Basic credentials through the configured backend" do
    authorization = "Basic " <> Base.encode64("worker:secret")

    request = %{
      authorization: authorization,
      peer: {{127, 0, 0, 1}, 12_345},
      backend: FerricstoreHttp.TestBackend
    }

    assert {:ok,
            %Identity{
              session: {:session, {{127, 0, 0, 1}, 12_345}},
              subject: "worker"
            }} = Basic.authenticate(request, [])
  end

  test "treats the Basic authentication scheme case-insensitively" do
    encoded = Base.encode64("worker:secret")

    request = %{
      authorization: "basic " <> encoded,
      peer: :peer,
      backend: FerricstoreHttp.TestBackend
    }

    assert {:ok, ^encoded} = Basic.cache_key(request, [])

    assert {:ok, %Identity{session: {:session, :peer}, subject: "worker"}} =
             Basic.authenticate(request, [])
  end

  test "derives a cheap opaque cache input before validating Basic credentials" do
    request = %{
      authorization: "Basic not-base64",
      peer: :peer,
      backend: FerricstoreHttp.TestBackend
    }

    assert {:ok, "not-base64"} = Basic.cache_key(request, [])
    assert {:error, :unauthenticated} = Basic.authenticate(request, [])
  end

  test "preserves every colon after the username as part of the password" do
    request = %{
      authorization: "Basic " <> Base.encode64("worker:secret"),
      peer: :peer,
      backend: FerricstoreHttp.TestBackend
    }

    assert {:ok, %Identity{session: {:session, :peer}, subject: "worker"}} =
             Basic.authenticate(request, [])

    request = %{request | authorization: "Basic " <> Base.encode64("worker:secret:extra")}
    assert {:error, :unauthenticated} = Basic.authenticate(request, [])
  end

  test "custom providers receive normalized metadata and configured options" do
    assert {:ok, config} =
             Config.new(
               backend: FerricstoreHttp.TestBackend,
               auth_provider: RecordingProvider,
               auth_options: [audience: "functions"]
             )

    assert {:ok,
            %Identity{
              session:
                {:custom_identity,
                 %{
                   authorization: "Bearer workload-token",
                   peer: :peer,
                   backend: FerricstoreHttp.TestBackend,
                   headers: %{}
                 }, [audience: "functions"]}
            }} =
             Auth.authenticate("Bearer workload-token", :peer, config)
  end

  test "request identity headers are ignored unless explicitly trusted" do
    headers = %{
      "x-ferricstore-subject" => "function-17",
      "x-ferricstore-scopes" => "invoke values:read"
    }

    assert {:ok, untrusted_config} =
             Config.new(backend: FerricstoreHttp.TestBackend, auth_cache_enabled: false)

    authorization = "Basic " <> Base.encode64("worker:secret")

    assert {:ok, %Auth.Context{subject: "worker", scopes: []}} =
             Auth.resolve(authorization, :peer, headers, untrusted_config)

    assert {:ok, trusted_config} =
             Config.new(
               backend: FerricstoreHttp.TestBackend,
               auth_cache_enabled: false,
               trust_context_headers: true
             )

    assert {:ok,
            %Auth.Context{
              subject: "function-17",
              scopes: ["invoke", "values:read"]
            }} = Auth.resolve(authorization, :peer, headers, trusted_config)
  end

  test "rejects missing, malformed, and invalid credentials uniformly" do
    for authorization <- [
          nil,
          "Bearer token",
          "Basic invalid",
          "Basic " <> Base.encode64(":secret"),
          "Basic " <> Base.encode64("worker:wrong")
        ] do
      request = %{authorization: authorization, peer: :peer, backend: FerricstoreHttp.TestBackend}
      assert {:error, :unauthenticated} = Basic.authenticate(request, [])
    end
  end
end
