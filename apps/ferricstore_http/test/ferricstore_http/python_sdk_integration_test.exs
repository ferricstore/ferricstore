defmodule FerricstoreHttp.PythonSdkIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :python_sdk_integration

  alias Ferricstore.Flow.Query.IndexRegistry
  alias FerricstoreHttp.Test.HttpHelpers
  alias FerricstoreServer.Acl.CatalogProjector

  test "real Python SDK covers every workflow command plus HTTP transport paths" do
    python_sdk_path = System.fetch_env!("FERRICSTORE_PYTHON_SDK_PATH")

    python =
      System.get_env("FERRICSTORE_PYTHON_EXECUTABLE") || System.find_executable("python3") ||
        flunk("python3 is required")

    http_adapter = Path.join(python_sdk_path, "src/ferricstore/http_adapter.py")
    assert File.regular?(http_adapter), "HTTP-enabled Python SDK not found at #{python_sdk_path}"

    :ok = CatalogProjector.mark_ready()
    assert_query_indexes_ready()

    unique = System.unique_integer([:positive, :monotonic])
    username = "python-http-#{unique}"
    password = "python-http-secret-#{unique}"
    key_prefix = "python:http:#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               "resetpass",
               ">#{password}",
               "resetkeys",
               "+@all",
               "~#{key_prefix}:*"
             ])

    tls_files = HttpHelpers.tls_files()
    on_exit(fn -> File.rm_rf!(tls_files.directory) end)

    HttpHelpers.start_server(
      backend: FerricstoreHttp.Backends.Ferricstore,
      max_connections: 64,
      max_in_flight_requests: 64,
      tls: [enabled: true, certfile: tls_files.certfile, keyfile: tls_files.keyfile]
    )

    script = Path.expand("../../../../scripts/http/python_sdk_integration.py", __DIR__)

    {output, status} =
      System.cmd(python, [script],
        cd: python_sdk_path,
        env: [
          {"PYTHONPATH", Path.join(python_sdk_path, "src")},
          {"FERRICSTORE_HTTP_INTEGRATION_URL",
           "https://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
          {"FERRICSTORE_HTTP_INTEGRATION_USERNAME", username},
          {"FERRICSTORE_HTTP_INTEGRATION_PASSWORD", password},
          {"FERRICSTORE_HTTP_INTEGRATION_KEY_PREFIX", key_prefix},
          {"FERRICSTORE_HTTP_INTEGRATION_CA_FILE", tls_files.cafile}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ ~s("suite": "python_sdk_http_integration")
    assert output =~ ~s("status": "ok")
    assert output =~ ~s("workflow_commands": 67)

    command_surface =
      output
      |> String.split("\n", trim: true)
      |> Enum.find(&String.contains?(&1, ~s("suite": "python_sdk_http_integration")))
      |> Jason.decode!()

    expected_workflow_commands = MapSet.new(command_surface["workflow_command_names"])
    assert MapSet.size(expected_workflow_commands) == 67

    integration_test =
      Path.join(python_sdk_path, "tests/integration/test_ferricstore_integration.py")

    workflow_username = "python-http-workflows-#{unique}"
    workflow_password = "python-http-workflows-secret-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(workflow_username, [
               "on",
               "resetpass",
               ">#{workflow_password}",
               "resetkeys",
               "+@all",
               "~*"
             ])

    selected_tests = [
      "#{integration_test}::test_real_ferricstore_command_and_flow_cycle",
      "#{integration_test}::test_real_ferricstore_native_protocol_flow_admin_surface",
      "#{integration_test}::test_real_ferricstore_flow_state_machine_and_repair_surface"
    ]

    observed_commands_file = Path.join(tls_files.directory, "observed-workflow-commands.txt")

    {workflow_output, workflow_status} =
      System.cmd(python, ["-m", "pytest", "-q" | selected_tests],
        cd: python_sdk_path,
        env: [
          {"PYTHONPATH", Path.join(python_sdk_path, "src")},
          {"FERRICSTORE_INTEGRATION", "1"},
          {"FERRICSTORE_SKIP_CATALOG_COVERAGE", "1"},
          {"FERRICSTORE_HTTP_COMMAND_COVERAGE", "1"},
          {"FERRICSTORE_OBSERVED_COMMANDS_FILE", observed_commands_file},
          {"FERRICSTORE_URL", "https://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
          {"FERRICSTORE_USERNAME", workflow_username},
          {"FERRICSTORE_PASSWORD", workflow_password},
          {"FERRICSTORE_CA_FILE", tls_files.cafile}
        ],
        stderr_to_stdout: true
      )

    assert workflow_status == 0, workflow_output
    assert workflow_output =~ "3 passed"

    observed_workflow_commands =
      observed_commands_file
      |> File.read!()
      |> String.split("\n", trim: true)
      |> Enum.filter(&String.starts_with?(&1, "FLOW."))
      |> MapSet.new()

    assert observed_workflow_commands == expected_workflow_commands,
           "valid workflow scenarios missed: #{inspect(MapSet.difference(expected_workflow_commands, observed_workflow_commands) |> Enum.sort())}"
  end

  defp assert_query_indexes_ready do
    deadline = System.monotonic_time(:millisecond) + 30_000
    wait_for_query_indexes(deadline, :not_checked)
  end

  defp wait_for_query_indexes(deadline, _last_status) do
    status = IndexRegistry.overview(IndexRegistry)

    case status do
      {:ok, %{indexes: [_ | _] = indexes}} ->
        if Enum.all?(indexes, &(&1.state == :active)) do
          :ok
        else
          retry_query_index_readiness(deadline, status)
        end

      _not_ready ->
        retry_query_index_readiness(deadline, status)
    end
  end

  defp retry_query_index_readiness(deadline, last_status) do
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(100)
      wait_for_query_indexes(deadline, last_status)
    else
      flunk("Flow query indexes did not become active: #{inspect(last_status)}")
    end
  end
end
