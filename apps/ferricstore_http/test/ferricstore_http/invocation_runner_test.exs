defmodule FerricstoreHttp.Invocations.RunnerTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Auth, Config, ControlledTestBackend}
  alias FerricstoreHttp.Invocations.{Definition, Runner}

  defmodule SuccessfulTarget do
    @behaviour FerricstoreHttp.Target

    @impl FerricstoreHttp.Target
    def invoke(target, request, opts) do
      send(Process.whereis(:invocation_runner_test), {:target_invoked, target, request, opts})
      {:ok, %{"delivered" => true}}
    end
  end

  defmodule RetryTarget do
    @behaviour FerricstoreHttp.Target

    @impl FerricstoreHttp.Target
    def invoke(_target, _request, _opts), do: {:retry, %{"code" => "temporary"}}
  end

  defmodule BlockingTarget do
    @behaviour FerricstoreHttp.Target

    @impl FerricstoreHttp.Target
    def invoke(_target, %{"invocation_id" => id}, _opts) do
      test_pid = Process.whereis(:invocation_runner_test)
      send(test_pid, {:target_started, id, self()})

      receive do
        :release -> {:ok, %{"delivered" => true}}
      end
    end
  end

  defmodule HangingTarget do
    @behaviour FerricstoreHttp.Target

    @impl FerricstoreHttp.Target
    def invoke(_target, _request, _opts), do: Process.sleep(:infinity)
  end

  setup do
    Process.register(self(), :invocation_runner_test)
    start_supervised!(ControlledTestBackend)

    on_exit(fn ->
      if Process.whereis(:invocation_runner_test), do: Process.unregister(:invocation_runner_test)
    end)

    :ok
  end

  test "claims an invocation, calls its HTTP target adapter, and completes the Flow" do
    ControlledTestBackend.reset(%{execute_result: &backend_response/3})
    definition = definition()

    assert {:ok, %{definitions: 1, claimed: 1, completed: 1, retried: 0, failed: 0, errors: 0}} =
             Runner.run_definition(definition, context(), config(), target: SuccessfulTarget)

    assert_receive {:target_invoked, target, request, target_opts}
    assert target["url"] == "http://target.local/invoke"
    assert request["invocation_id"] == "inv-1"
    assert request["payload"] == %{"to" => "test@example.com"}
    assert target_opts[:max_response_bytes] == 1_048_576

    assert Enum.any?(ControlledTestBackend.calls(), fn
             {:execute_batch, :runner_session,
              [%{"command" => "FLOW.CLAIM_DUE", "payload" => payload}], _opts} ->
               payload["lease_ms"] >= 91_000

             _call ->
               false
           end)

    assert Enum.any?(ControlledTestBackend.calls(), fn
             {:execute_batch, :runner_session,
              [%{"command" => "FLOW.COMPLETE", "payload" => payload}], _opts} ->
               payload["id"] == "inv-1" and
                 Jason.decode!(payload["result"]) == %{"delivered" => true}

             _call ->
               false
           end)
  end

  test "reschedules transient target failures" do
    ControlledTestBackend.reset(%{execute_result: &backend_response/3})

    assert {:ok, %{claimed: 1, completed: 0, retried: 1, failed: 0, errors: 0}} =
             Runner.run_definition(definition(), context(), config(), target: RetryTarget)

    assert Enum.any?(ControlledTestBackend.calls(), fn
             {:execute_batch, :runner_session,
              [%{"command" => "FLOW.RETRY", "payload" => payload}], _opts} ->
               payload["id"] == "inv-1" and is_integer(payload["run_at_ms"])

             _call ->
               false
           end)
  end

  test "starts every claimed job concurrently within a lease-safe window" do
    ControlledTestBackend.reset(%{execute_result: &two_job_backend_response/3})

    run =
      Task.async(fn ->
        Runner.run_definition(definition(), context(), config(), target: BlockingTarget)
      end)

    assert_receive {:target_started, first_id, first_target}, 1_000
    assert_receive {:target_started, second_id, second_target}, 1_000
    assert MapSet.new([first_id, second_id]) == MapSet.new(["inv-1", "inv-2"])

    send(first_target, :release)
    send(second_target, :release)

    assert {:ok, %{claimed: 2, completed: 2, errors: 0}} = Task.await(run, 2_000)
  end

  test "kills a target adapter that exceeds the configured job deadline" do
    ControlledTestBackend.reset(%{execute_result: &backend_response/3})

    run =
      Task.async(fn ->
        Runner.run_definition(definition(), context(), short_timeout_config(),
          target: HangingTarget
        )
      end)

    assert {:ok, %{claimed: 1, completed: 0, retried: 0, failed: 0, errors: 1}} =
             Task.await(run, 1_000)
  end

  test "does not claim any Flow when the invocation partition catalog is empty" do
    ControlledTestBackend.reset(%{execute_result: &empty_partition_backend_response/3})

    assert {:ok, %{definitions: 1, claimed: 0, completed: 0, errors: 0}} =
             Runner.run_definition(definition(), context(), config(), target: SuccessfulTarget)

    refute Enum.any?(ControlledTestBackend.calls(), fn
             {:execute_batch, :runner_session, [%{"command" => "FLOW.CLAIM_DUE"}], _opts} ->
               true

             _call ->
               false
           end)

    refute_receive {:target_invoked, _target, _request, _opts}
  end

  test "does not dispatch a claimed Flow belonging to a different invocation definition" do
    ControlledTestBackend.reset(%{execute_result: &mismatched_definition_backend_response/3})

    assert {:ok, %{claimed: 1, completed: 0, retried: 0, failed: 0, errors: 1}} =
             Runner.run_definition(definition(), context(), config(), target: SuccessfulTarget)

    refute_receive {:target_invoked, _target, _request, _opts}

    refute Enum.any?(ControlledTestBackend.calls(), fn
             {:execute_batch, :runner_session,
              [%{"command" => command, "payload" => %{"id" => "inv-1"}}], _opts}
             when command in ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL"] ->
               true

             _call ->
               false
           end)
  end

  defp backend_response(_session, [["INVOCATION.PARTITION.LIST", "send-email"]], _opts),
    do: ok(["queue:a"])

  defp backend_response(
         _session,
         [%{"command" => "FLOW.CLAIM_DUE"}],
         _opts
       ) do
    ok([
      %{
        "id" => "inv-1",
        "state" => "running",
        "partition_key" => "queue:a",
        "lease_token" => "lease-1",
        "fencing_token" => 2
      }
    ])
  end

  defp backend_response(_session, [%{"command" => "FLOW.GET"}], _opts) do
    ok(%{
      "id" => "inv-1",
      "state" => "running",
      "partition_key" => "queue:a",
      "attributes" => %{"invocation_name" => "send-email"},
      "payload" => Jason.encode!(%{"to" => "test@example.com"})
    })
  end

  defp backend_response(_session, [%{"command" => command}], _opts)
       when command in ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL"],
       do: ok(%{"id" => "inv-1"})

  defp empty_partition_backend_response(
         _session,
         [["INVOCATION.PARTITION.LIST", "send-email"]],
         _opts
       ),
       do: ok([])

  defp empty_partition_backend_response(session, commands, opts),
    do: backend_response(session, commands, opts)

  defp mismatched_definition_backend_response(
         _session,
         [["INVOCATION.PARTITION.LIST", "send-email"]],
         _opts
       ),
       do: ok(["queue:a"])

  defp mismatched_definition_backend_response(
         _session,
         [%{"command" => "FLOW.GET"}],
         _opts
       ) do
    ok(%{
      "id" => "inv-1",
      "state" => "running",
      "partition_key" => "queue:a",
      "attributes" => %{"invocation_name" => "different-definition"}
    })
  end

  defp mismatched_definition_backend_response(session, commands, opts),
    do: backend_response(session, commands, opts)

  defp two_job_backend_response(
         _session,
         [["INVOCATION.PARTITION.LIST", "send-email"]],
         _opts
       ),
       do: ok(["queue:a"])

  defp two_job_backend_response(_session, [%{"command" => "FLOW.CLAIM_DUE"}], _opts) do
    ok(
      Enum.map(1..2, fn index ->
        %{
          "id" => "inv-#{index}",
          "state" => "running",
          "partition_key" => "queue:a",
          "lease_token" => "lease-#{index}",
          "fencing_token" => index
        }
      end)
    )
  end

  defp two_job_backend_response(
         _session,
         [%{"command" => "FLOW.GET", "payload" => %{"id" => id}}],
         _opts
       ),
       do:
         ok(%{
           "id" => id,
           "state" => "running",
           "partition_key" => "queue:a",
           "attributes" => %{"invocation_name" => "send-email"}
         })

  defp two_job_backend_response(
         _session,
         [%{"command" => command, "payload" => %{"id" => id}}],
         _opts
       )
       when command in ["FLOW.COMPLETE", "FLOW.RETRY", "FLOW.FAIL"],
       do: ok(%{"id" => id})

  defp ok(value), do: {:ok, [%{status: :ok, value: value}]}

  defp definition do
    {:ok, definition} =
      Definition.new(%{
        name: "send-email",
        target: %{kind: "http_endpoint", url: "http://target.local/invoke"}
      })

    definition
  end

  defp context, do: %Auth.Context{session: :runner_session, subject: "runner"}

  defp config do
    {:ok, config} = Config.new(backend: ControlledTestBackend)
    config
  end

  defp short_timeout_config do
    {:ok, config} =
      Config.new(
        backend: ControlledTestBackend,
        request_timeout_ms: 10,
        runner_target_timeout_ms: 20
      )

    config
  end
end
