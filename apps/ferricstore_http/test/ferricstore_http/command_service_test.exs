defmodule FerricstoreHttp.CommandServiceTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.{BinaryEnvelope, CommandService, Config, Deadline}

  defmodule PreparedBatchingUnavailableBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, :session}

    @impl FerricstoreHttp.Backend
    def execute_batch(:session, commands, _opts) do
      {:ok, Enum.map(commands, fn ["PING"] -> %{status: :ok, value: "PONG"} end)}
    end

    @impl FerricstoreHttp.Backend
    def prepare_batch(_commands, _opts), do: raise("must use the compatible fallback")

    @impl FerricstoreHttp.Backend
    def execute_prepared_batches(_session, _batches, _opts),
      do: raise("must use the compatible fallback")

    @impl FerricstoreHttp.Backend
    def prepared_batching_supported?, do: false

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  defmodule DiagnosticBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, :session}

    @impl FerricstoreHttp.Backend
    def execute_batch(:session, _commands, _opts) do
      {:ok,
       [
         %{
           status: :error,
           value: %{
             "code" => "query_projection_changed",
             "message" => "ERR Flow visibility projection changed during the query",
             "detail" => "The projection moved while this request was executing.",
             "position" => %{"byte" => 17, "line" => 2, "column" => 4},
             "context" => %{
               "source" => "runs",
               "filters" => ["region", %{"field" => "state", "active" => true}]
             },
             "retryable" => true,
             "safe_to_retry" => true,
             "retry_after_ms" => 0
           }
         }
       ]}
    end

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  defmodule BlockingBatchBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, self()}

    @impl FerricstoreHttp.Backend
    def execute_batch(test_pid, commands, _opts) do
      send(test_pid, {:execute_batch, commands})

      {:ok,
       Enum.map(commands, fn
         ["PING"] -> %{status: :ok, value: "PONG"}
         ["BLPOP" | _args] -> %{status: :ok, value: nil}
       end)}
    end

    @impl FerricstoreHttp.Backend
    def prepare_batch(_commands, _opts), do: raise("blocking requests must not be coalesced")

    @impl FerricstoreHttp.Backend
    def execute_prepared_batches(_session, _batches, _opts),
      do: raise("blocking requests must not be coalesced")

    @impl FerricstoreHttp.Backend
    def prepared_batching_supported?, do: true

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  setup do
    {:ok, config} = Config.new(backend: FerricstoreHttp.TestBackend)
    %{config: config, session: {:session, :peer}, deadline: Deadline.new(1_000)}
  end

  test "executes the SDK-compatible binary JSON envelope", context do
    bytes = <<0, 255, 1>>

    envelope = %{
      "encoding" => BinaryEnvelope.encoding(),
      "commands" => BinaryEnvelope.encode([["ECHO", bytes]])
    }

    assert {:ok,
            %{
              "encoding" => "ferricstore-json-v1",
              "results" => [%{"status" => "ok", "value" => encoded}]
            }} =
             CommandService.execute(envelope, context.session, context.config, context.deadline)

    assert {:ok, ^bytes} = BinaryEnvelope.decode(encoded)
  end

  test "preserves ordered command errors", context do
    envelope = %{"commands" => [["PING"], ["UNKNOWN"]]}

    assert {:ok,
            %{
              "results" => [
                %{"status" => "ok", "value" => "PONG"},
                %{"status" => "error", "error" => %{"code" => "error"}}
              ]
            }} =
             CommandService.execute(envelope, context.session, context.config, context.deadline)
  end

  test "preserves bounded structured command diagnostics for SDK retry decisions" do
    {:ok, config} = Config.new(backend: DiagnosticBackend)

    assert {:ok,
            %{
              "results" => [
                %{
                  "status" => "error",
                  "error" => %{
                    "code" => "query_projection_changed",
                    "message" => "ERR Flow visibility projection changed during the query",
                    "detail" => "The projection moved while this request was executing.",
                    "position" => %{"byte" => 17, "line" => 2, "column" => 4},
                    "context" => %{
                      "source" => "runs",
                      "filters" => ["region", %{"field" => "state", "active" => true}]
                    },
                    "retryable" => true,
                    "safe_to_retry" => true,
                    "retry_after_ms" => 0
                  }
                }
              ]
            }} =
             CommandService.execute(
               %{"commands" => [["FLOW.QUERY"]]},
               :session,
               config,
               Deadline.new(1_000)
             )
  end

  test "rejects malformed envelopes before dispatch", context do
    assert {:error, :malformed_envelope} =
             CommandService.execute(%{}, context.session, context.config, context.deadline)

    assert {:error, :malformed_binary_envelope} =
             CommandService.execute(
               %{
                 "encoding" => BinaryEnvelope.encoding(),
                 "commands" => %{"$ferricstore_bytes" => "bad"}
               },
               context.session,
               context.config,
               context.deadline
             )
  end

  test "passes blocking commands through one ordered request batch" do
    {:ok, config} = Config.new(backend: BlockingBatchBackend)

    assert {:ok,
            %{
              "results" => [
                %{"status" => "ok", "value" => "PONG"},
                %{"status" => "ok", "value" => nil},
                %{"status" => "ok", "value" => "PONG"}
              ]
            }} =
             CommandService.execute(
               %{"commands" => [["PING"], ["BLPOP", "queue", "0"], ["PING"]]},
               self(),
               config,
               Deadline.new(1_000)
             )

    assert_received {:execute_batch, [["PING"], ["BLPOP", "queue", "0"], ["PING"]]}
  end

  test "does not coalesce a blocking request with another HTTP request" do
    {:ok, config} = Config.new(backend: BlockingBatchBackend)
    {:ok, blocking} = CommandService.prepare(%{"commands" => [["BLPOP", "queue", "0"]]})
    {:ok, ping} = CommandService.prepare(%{"commands" => [["PING"]]})
    deadline = Deadline.new(1_000)

    assert [
             {:ok, %{"results" => [%{"status" => "ok", "value" => nil}]}},
             {:ok, %{"results" => [%{"status" => "ok", "value" => "PONG"}]}}
           ] =
             CommandService.execute_many([{blocking, deadline}, {ping, deadline}], self(), config)

    assert_received {:execute_batch, [["BLPOP", "queue", "0"]]}
    assert_received {:execute_batch, [["PING"]]}
  end

  test "prepares and executes multiple request envelopes without losing their boundaries",
       context do
    assert {:ok, ping} = CommandService.prepare(%{"commands" => [["PING"]]})

    assert {:ok, echo} =
             CommandService.prepare(%{"commands" => [["ECHO", "one"], ["ECHO", "two"]]})

    assert CommandService.command_count(ping) == 1
    assert CommandService.command_count(echo) == 2

    assert [
             {:ok, %{"results" => [%{"status" => "ok", "value" => "PONG"}]}},
             {:ok,
              %{
                "results" => [
                  %{"status" => "ok", "value" => "one"},
                  %{"status" => "ok", "value" => "two"}
                ]
              }}
           ] =
             CommandService.execute_many(
               [{ping, context.deadline}, {echo, context.deadline}],
               context.session,
               context.config
             )
  end

  test "falls back safely when an installed backend does not support prepared batching" do
    {:ok, config} = Config.new(backend: PreparedBatchingUnavailableBackend)
    {:ok, first} = CommandService.prepare(%{"commands" => [["PING"]]})
    {:ok, second} = CommandService.prepare(%{"commands" => [["PING"]]})
    deadline = Deadline.new(1_000)

    assert [
             {:ok, %{"results" => [%{"status" => "ok", "value" => "PONG"}]}},
             {:ok, %{"results" => [%{"status" => "ok", "value" => "PONG"}]}}
           ] =
             CommandService.execute_many(
               [{first, deadline}, {second, deadline}],
               :session,
               config
             )
  end
end
