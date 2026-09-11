defmodule FerricstoreHttp.ToolingContractTest do
  use ExUnit.Case, async: true

  @app_root Path.expand("../..", __DIR__)
  @repo_root Path.expand("../../../..", __DIR__)

  test "HTTP is an umbrella application with only an in-repository server dependency" do
    app_mix = File.read!(Path.join(@app_root, "mix.exs"))

    assert app_mix =~ ~s(app: :ferricstore_http)
    assert app_mix =~ ~s({:ferricstore_server, in_umbrella: true})
    assert app_mix =~ ~s(build_path: "../../_build")
    assert app_mix =~ ~s(config_path: "../../config/config.exs")
    assert app_mix =~ ~s(lockfile: "../../mix.lock")
    refute app_mix =~ "FERRICSTORE_OSS_PATH"
    refute app_mix =~ "github: \"ferricstore/ferricstore\""
  end

  test "the OSS release owns HTTP and Credo scans every umbrella application" do
    root_mix = File.read!(Path.join(@repo_root, "mix.exs"))
    credo = File.read!(Path.join(@repo_root, ".credo.exs"))
    http_credo = File.read!(Path.join(@repo_root, ".credo_http.exs"))

    assert root_mix =~ ~s(ferricstore_http: :permanent)
    assert root_mix =~ ~s("quality.http")
    assert credo =~ ~s("apps/*/lib/")
    assert credo =~ ~s("apps/*/test/")
    assert http_credo =~ ~s("apps/ferricstore_http/lib/")
    assert http_credo =~ ~s("apps/ferricstore_http/test/")
  end

  test "the release image contains the HTTP application and its TLS trust store" do
    dockerfile = File.read!(Path.join(@repo_root, "Dockerfile"))

    assert dockerfile =~ "COPY apps/ferricstore_http/mix.exs"
    assert dockerfile =~ "COPY apps/ferricstore_http/lib"
    assert dockerfile =~ "ca-certificates"
    assert dockerfile =~ "FERRICSTORE_HTTP_ENABLED=false"
    assert dockerfile =~ "EXPOSE 6388 6380 6381 8080"
  end
end
