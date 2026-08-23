defmodule FerricstoreHttp.JavaSdkIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :java_sdk_integration

  alias FerricstoreHttp.Test.HttpHelpers
  alias FerricstoreServer.Acl.CatalogProjector

  test "real Java 17 SDK exercises the complete HTTP command suite through HTTPS" do
    java_sdk_path = System.fetch_env!("FERRICSTORE_JAVA_SDK_PATH")

    maven =
      System.get_env("FERRICSTORE_MAVEN_EXECUTABLE") || System.find_executable("mvn") ||
        flunk("Maven is required")

    pom = Path.join(java_sdk_path, "pom.xml")
    assert File.regular?(pom), "Java SDK not found at #{java_sdk_path}"

    :ok = CatalogProjector.mark_ready()
    unique = System.unique_integer([:positive, :monotonic])
    username = "java-http-#{unique}"
    password = "java-http-secret-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               "resetpass",
               ">#{password}",
               "resetkeys",
               "+@all",
               "~*"
             ])

    tls_files = HttpHelpers.tls_files()
    on_exit(fn -> File.rm_rf!(tls_files.directory) end)

    HttpHelpers.start_server(
      backend: FerricstoreHttp.Backends.Ferricstore,
      max_connections: 128,
      max_in_flight_requests: 128,
      max_body_bytes: 1_048_576,
      tls: [enabled: true, certfile: tls_files.certfile, keyfile: tls_files.keyfile]
    )

    {output, status} =
      System.cmd(
        maven,
        [
          "-B",
          "-pl",
          "ferricstore-java",
          "-Dtest=FerricStoreIntegrationTest",
          "test"
        ],
        cd: java_sdk_path,
        env: [
          {"FERRICSTORE_INTEGRATION", "1"},
          {"FERRICSTORE_URL", "https://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
          {"FERRICSTORE_USERNAME", username},
          {"FERRICSTORE_PASSWORD", password},
          {"FERRICSTORE_CA_FILE", tls_files.cafile}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "BUILD SUCCESS"
    assert output =~ "FerricStoreIntegrationTest"
  end
end
