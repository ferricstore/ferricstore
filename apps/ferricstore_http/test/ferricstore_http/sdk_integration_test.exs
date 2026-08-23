defmodule FerricstoreHttp.SdkIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :sdk_integration
  @moduletag timeout: 300_000

  alias FerricstoreHttp.RecordingFerricstoreBackend
  alias FerricstoreHttp.Test.HttpHelpers
  alias FerricstoreServer.Acl.CatalogProjector

  @flow_commands MapSet.new(~w(
    FLOW.APPROVAL.APPROVE FLOW.APPROVAL.GET FLOW.APPROVAL.LIST FLOW.APPROVAL.REJECT
    FLOW.APPROVAL.REQUEST FLOW.ATTRIBUTES FLOW.ATTRIBUTE_VALUES FLOW.BUDGET.COMMIT
    FLOW.BUDGET.GET FLOW.BUDGET.LIST FLOW.BUDGET.RELEASE FLOW.BUDGET.RESERVE
    FLOW.CANCEL FLOW.CANCEL_MANY FLOW.CIRCUIT.CLOSE FLOW.CIRCUIT.GET FLOW.CIRCUIT.OPEN
    FLOW.CLAIM_DUE FLOW.COMPLETE FLOW.COMPLETE_MANY FLOW.CREATE FLOW.CREATE_MANY
    FLOW.EFFECT.COMPENSATE FLOW.EFFECT.CONFIRM FLOW.EFFECT.FAIL FLOW.EFFECT.GET
    FLOW.EFFECT.RESERVE FLOW.EXTEND_LEASE FLOW.FAIL FLOW.FAIL_MANY FLOW.GET
    FLOW.GOVERNANCE.LEDGER FLOW.GOVERNANCE.OVERVIEW FLOW.HISTORY FLOW.INFO
    FLOW.LIMIT.GET FLOW.LIMIT.LEASE FLOW.LIMIT.LIST FLOW.LIMIT.RELEASE FLOW.LIMIT.SPEND
    FLOW.POLICY.GET FLOW.POLICY.SET FLOW.QUERY FLOW.QUERY.INDEXES FLOW.RECLAIM
    FLOW.RETENTION_CLEANUP FLOW.RETRY FLOW.RETRY_MANY FLOW.REWIND FLOW.RUN_STEPS_MANY
    FLOW.SCHEDULE.CREATE FLOW.SCHEDULE.DELETE FLOW.SCHEDULE.FIRE
    FLOW.SCHEDULE.FIRE_DUE FLOW.SCHEDULE.GET FLOW.SCHEDULE.LIST FLOW.SCHEDULE.PAUSE
    FLOW.SCHEDULE.RESUME FLOW.SIGNAL FLOW.SPAWN_CHILDREN FLOW.START_AND_CLAIM
    FLOW.STATS FLOW.STEP_CONTINUE FLOW.TRANSITION FLOW.TRANSITION_MANY
    FLOW.VALUE.MGET FLOW.VALUE.PUT
  ))

  test "Go SDK runs valid workflows and the complete Flow surface over TLS HTTP/2" do
    sdk_path = sdk_path!("FERRICSTORE_GO_SDK_PATH", "http_executor.go")
    context = start_http_fixture("go")

    run!(
      sdk_executable("FERRICSTORE_GO_EXECUTABLE", "go"),
      [
        "test",
        "-tags",
        "integration",
        "-run",
        "TestIntegration(KVAndFlowRoundTrip|FlowStateMachineRepairAndIndexes|FlowAttributesSchedulesAndGovernance)$",
        "."
      ],
      sdk_path,
      context
    )

    assert_complete_flow_surface("Go")
  end

  test "Elixir SDK runs valid workflows and the complete Flow surface over TLS HTTP/2" do
    sdk_path = sdk_path!("FERRICSTORE_ELIXIR_SDK_PATH", "lib/ferric_store/http/client.ex")
    context = start_http_fixture("elixir")

    run!(
      sdk_executable("FERRICSTORE_ELIXIR_EXECUTABLE", "elixir"),
      [
        "-S",
        "mix",
        "test",
        "--only",
        "http_sdk_integration",
        "test/ferric_store/client_integration_test.exs"
      ],
      sdk_path,
      context
    )

    assert_complete_flow_surface("Elixir")
  end

  test "TypeScript SDK runs valid workflows and the complete Flow surface over TLS HTTP/2" do
    sdk_path = sdk_path!("FERRICSTORE_TYPESCRIPT_SDK_PATH", "src/http-adapter.ts")
    context = start_http_fixture("typescript")

    run!(
      "npm",
      [
        "exec",
        "--",
        "vitest",
        "run",
        "tests/integration/live-http-command-surface.test.ts",
        "tests/integration/live-store-flow.test.ts",
        "tests/integration/live-governance-workflow.test.ts",
        "-t",
        "complete typed Flow command catalog|Flow state-machine repair and index commands|fused Flow, schedule, query, and governance helpers|queue and workflow wrappers"
      ],
      sdk_path,
      context
    )

    assert_complete_flow_surface("TypeScript")
  end

  defp start_http_fixture(prefix) do
    :ok = CatalogProjector.mark_ready()
    start_supervised!(RecordingFerricstoreBackend)
    :ok = RecordingFerricstoreBackend.reset()
    unique = System.unique_integer([:positive, :monotonic])
    username = "#{prefix}-http-#{unique}"
    password = "#{prefix}-http-secret-#{unique}"

    assert :ok =
             FerricstoreServer.Acl.set_user(username, [
               "on",
               "resetpass",
               ">#{password}",
               "resetkeys",
               "+@all",
               "~*"
             ])

    files = HttpHelpers.sdk_tls_files()
    on_exit(fn -> File.rm_rf!(files.directory) end)

    HttpHelpers.start_server(
      backend: RecordingFerricstoreBackend,
      http2_enabled: true,
      max_body_bytes: 16 * 1_024 * 1_024,
      max_connections: 128,
      max_in_flight_requests: 128,
      request_timeout_ms: 120_000,
      tls: [enabled: true, certfile: files.certfile, keyfile: files.keyfile]
    )

    [
      {"FERRICSTORE_URL", "https://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
      {"FERRICSTORE_TEST_URL", "https://127.0.0.1:#{FerricstoreHttp.Listener.port()}"},
      {"FERRICSTORE_USERNAME", username},
      {"FERRICSTORE_PASSWORD", password},
      {"FERRICSTORE_CA_FILE", files.cafile},
      {"FERRICSTORE_HTTP2", "true"},
      {"FERRICSTORE_SKIP_COMMAND_COVERAGE", "1"},
      {"MIX_ENV", "test"}
    ]
  end

  defp assert_complete_flow_surface(sdk) do
    observed =
      RecordingFerricstoreBackend.commands()
      |> Enum.filter(&String.starts_with?(&1, "FLOW."))
      |> MapSet.new()

    assert observed == @flow_commands,
           "#{sdk} HTTP workflows missed #{inspect(MapSet.difference(@flow_commands, observed) |> Enum.sort())} and added #{inspect(MapSet.difference(observed, @flow_commands) |> Enum.sort())}"
  end

  defp sdk_path!(variable, marker) do
    path = System.fetch_env!(variable)
    assert File.regular?(Path.join(path, marker)), "HTTP-enabled SDK not found at #{path}"
    path
  end

  defp sdk_executable(variable, default) do
    System.get_env(variable) || default
  end

  defp run!(command, args, directory, environment) do
    executable = System.find_executable(command) || flunk("#{command} is required")

    {output, status} =
      System.cmd(executable, args,
        cd: directory,
        env: environment,
        stderr_to_stdout: true
      )

    assert status == 0, output
  end
end
