defmodule FerricstoreHttp.RepositoryContractTest do
  use ExUnit.Case, async: true

  @repo_root Path.expand("../../../..", __DIR__)

  test "CI runs the HTTP suite against the in-repository command gateway" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    assert workflow =~ "test-http-ubuntu:"
    assert workflow =~ "mix quality.http"
  end

  test "CI runs the real Python SDK through the TLS HTTP listener" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    assert workflow =~ "test-http-python-sdk-ubuntu:"
    assert workflow =~ "repository: ferricstore/ferricstore-python"
    assert workflow =~ "FERRICSTORE_PYTHON_SDK_PATH"
    assert workflow =~ "--include python_sdk_integration"
  end

  test "CI runs every other official SDK through the TLS HTTP listener" do
    workflow = File.read!(Path.join(@repo_root, ".github/workflows/test.yml"))

    assert workflow =~ "test-http-official-sdks-ubuntu:"

    for repository <- [
          "ferricstore/ferricstore-go",
          "ferricstore/ferricstore-typescript",
          "ferricstore/ferricstore-elixir",
          "ferricstore/ferricstore-java"
        ] do
      assert workflow =~ "repository: #{repository}"
    end

    for variable <- [
          "FERRICSTORE_GO_SDK_PATH",
          "FERRICSTORE_TYPESCRIPT_SDK_PATH",
          "FERRICSTORE_ELIXIR_SDK_PATH",
          "FERRICSTORE_JAVA_SDK_PATH"
        ] do
      assert workflow =~ variable
    end

    assert workflow =~ "--include sdk_integration"
    assert workflow =~ "--include java_sdk_integration"
  end
end
