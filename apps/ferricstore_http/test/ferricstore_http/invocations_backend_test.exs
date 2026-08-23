defmodule FerricstoreHttp.Invocations.BackendTest do
  use ExUnit.Case, async: false

  alias FerricstoreHttp.{Auth, Config, ControlledTestBackend, Deadline}
  alias FerricstoreHttp.Backends.Ferricstore
  alias FerricstoreHttp.Invocations.Backend

  setup do
    start_supervised!(ControlledTestBackend)
    :ok
  end

  test "attaches trusted request context to invocation creation" do
    ControlledTestBackend.reset(%{
      execute_result: {:ok, [%{status: :ok, value: %{"invocation_id" => "inv-1"}}]}
    })

    config = config()
    context = context()
    deadline = Deadline.new(1_000)

    assert {:ok, %{"invocation_id" => "inv-1"}} =
             Backend.invocation_create(
               context,
               "send-email",
               %{"payload" => %{"to" => "test@example.com"}},
               config,
               deadline: deadline,
               idempotency_key: "request-1"
             )

    assert [
             {:execute_batch, :opaque_session, [["INVOCATION.CREATE", "send-email", encoded]],
              opts}
           ] = ControlledTestBackend.calls()

    assert %{
             "attrs" => %{"payload" => %{"to" => "test@example.com"}},
             "context" => %{
               "subject" => "function-1",
               "scopes" => ["invoke"]
             },
             "idempotency_key" => "request-1"
           } = Jason.decode!(encoded)

    assert opts[:request_context] == %{
             "subject" => "function-1",
             "scopes" => ["invoke"]
           }

    assert is_integer(opts[:deadline_ms])
  end

  test "uses native Flow descriptors for claims and value writes" do
    ControlledTestBackend.reset(%{
      execute_result: {:ok, [%{status: :ok, value: []}]}
    })

    assert {:ok, []} =
             Backend.flow_claim_due(context(), "invocation:send-email", config(),
               state: "queued",
               worker: "runner-1",
               lease_ms: 5_000,
               limit: 7,
               partition_keys: ["queue:a"]
             )

    assert [
             {:execute_batch, :opaque_session,
              [
                %{
                  "command" => "FLOW.CLAIM_DUE",
                  "opcode" => 0x0203,
                  "payload" => %{
                    "type" => "invocation:send-email",
                    "states" => ["queued"],
                    "worker" => "runner-1",
                    "lease_ms" => 5_000,
                    "limit" => 7,
                    "partition_keys" => ["queue:a"]
                  }
                }
              ], _opts}
           ] = ControlledTestBackend.calls()
  end

  test "normalizes a missing invocation command extension" do
    for result <- [
          {:error, {:malformed_command, 0}},
          {:ok, [%{status: :error, value: "ERR unsupported INVOCATION.CREATE"}]}
        ] do
      ControlledTestBackend.reset(%{execute_result: result})

      assert {:error, :invocations_unavailable} =
               Backend.invocation_create(context(), "send-email", %{}, config())
    end
  end

  test "preserves ACL denial status as a forbidden invocation error" do
    ControlledTestBackend.reset(%{
      execute_result: {:ok, [%{status: :noperm, value: "NOPERM command denied"}]}
    })

    assert {:error, :forbidden} = Backend.invocation_get(context(), "inv-1", config())
  end

  test "the production adapter prepares every Flow descriptor used by invocations" do
    terminal = %{
      "id" => "inv-1",
      "lease_token" => "lease-1",
      "fencing_token" => 1,
      "now_ms" => 1
    }

    descriptors = [
      descriptor("FLOW.GET", 0x0202, %{"id" => "inv-1"}),
      descriptor("FLOW.CLAIM_DUE", 0x0203, %{
        "type" => "invocation:send-email",
        "states" => ["queued"],
        "worker" => "runner-1",
        "lease_ms" => 30_000,
        "limit" => 10
      }),
      descriptor("FLOW.COMPLETE", 0x0204, Map.put(terminal, "result", "{}")),
      descriptor("FLOW.RETRY", 0x0206, Map.put(terminal, "error", "{}")),
      descriptor("FLOW.FAIL", 0x0207, Map.put(terminal, "error", "{}")),
      descriptor("FLOW.VALUE.PUT", 0x020B, %{"value" => "{}"}),
      descriptor("FLOW.VALUE.MGET", 0x020C, %{"refs" => ["ref-1"]})
    ]

    assert {:ok, _prepared} = Ferricstore.prepare_batch(descriptors, deadline_ms: 0)
  end

  defp config do
    {:ok, config} = Config.new(backend: ControlledTestBackend)
    config
  end

  defp context do
    %Auth.Context{
      session: :opaque_session,
      subject: "function-1",
      scopes: ["invoke"]
    }
  end

  defp descriptor(command, opcode, payload),
    do: %{"command" => command, "opcode" => opcode, "payload" => payload}
end
