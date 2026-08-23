defmodule FerricstoreHttp.Invocations.ResultValuesTest do
  use ExUnit.Case, async: true

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Invocations.{Service, Values}

  defmodule MissingReferenceBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, :session}

    @impl FerricstoreHttp.Backend
    def execute_batch(_session, [command], _opts) do
      {:ok, [%{status: :ok, value: response(command)}]}
    end

    @impl FerricstoreHttp.Backend
    def ready?, do: true

    defp response(["INVOCATION.GET", id]),
      do: %{
        "id" => id,
        "name" => "send-email",
        "flow_type" => "invocation:send-email",
        "value_policy" => %{"refs" => %{"allowed_read_names" => "*"}}
      }

    defp response(%{"command" => "FLOW.GET", "payload" => %{"id" => "missing-result"}}),
      do: %{"id" => "missing-result", "state" => "completed", "result_ref" => "missing-ref"}

    defp response(%{"command" => "FLOW.GET", "payload" => %{"id" => "missing-value"}}),
      do: %{
        "id" => "missing-value",
        "state" => "completed",
        "value_refs" => %{"receipt" => "missing-ref"}
      }

    defp response(%{"command" => "FLOW.VALUE.MGET"}), do: [nil]
  end

  defmodule BatchBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, :session}

    @impl FerricstoreHttp.Backend
    def execute_batch(test_pid, [command], _opts) do
      send(test_pid, {:backend_command, command})
      {:ok, [%{status: :ok, value: response(command)}]}
    end

    @impl FerricstoreHttp.Backend
    def ready?, do: true

    defp response(["INVOCATION.GET", id]),
      do: %{
        "id" => id,
        "name" => "send-email",
        "flow_type" => "invocation:send-email",
        "value_policy" => %{"refs" => %{"allowed_read_names" => "*"}}
      }

    defp response(%{"command" => "FLOW.GET", "payload" => %{"id" => id}}),
      do: %{
        "id" => id,
        "state" => "completed",
        "values" => %{"inline" => %{"ok" => true}},
        "value_refs" => %{"first" => "ref-1", "second" => "ref-2"}
      }

    defp response(%{"command" => "FLOW.VALUE.MGET", "payload" => %{"refs" => refs}}),
      do: Enum.map(refs, &("value-for-" <> &1))
  end

  defmodule RejectUnexpectedBackend do
    @behaviour FerricstoreHttp.Backend

    @impl FerricstoreHttp.Backend
    def authenticate(_username, _password, _opts), do: {:ok, :session}

    @impl FerricstoreHttp.Backend
    def execute_batch(_session, _commands, _opts),
      do: raise("invalid value names must be rejected before backend execution")

    @impl FerricstoreHttp.Backend
    def ready?, do: true
  end

  setup do
    {:ok, config} = Config.new(backend: MissingReferenceBackend)
    %{config: config, context: %Auth.Context{session: :session}}
  end

  test "reports a missing result reference instead of returning null", context do
    assert {:error, :value_not_found} =
             Service.result("missing-result", context.context, context.config)
  end

  test "reports a missing scoped value reference instead of returning null", context do
    assert {:error, :value_not_found} =
             Values.get("missing-value", "receipt", context.context, context.config)
  end

  test "rejects malformed names before loading invocation state", context do
    for names <- [[%{"name" => "receipt"}], [""], [String.duplicate("x", 4_097)]] do
      assert {:error, :value_names_required} =
               Values.batch("missing-value", names, context.context, context.config)
    end
  end

  test "bounds batch names before loading state", context do
    names = Enum.map(1..(context.config.max_batch_commands + 1), &"value-#{&1}")

    assert {:error, :value_names_required} =
             Values.batch("missing-value", names, context.context, context.config)
  end

  test "rejects oversized names on every single-value route before backend execution" do
    {:ok, config} = Config.new(backend: RejectUnexpectedBackend)
    context = %Auth.Context{session: :session}
    oversized = String.duplicate("x", 4_097)

    assert {:error, :value_name_required} = Values.get("inv-1", oversized, context, config)
    assert {:error, :value_name_required} = Values.content("inv-1", oversized, context, config)

    assert {:error, :value_name_required} =
             Values.put("inv-1", %{"name" => oversized, "value" => "data"}, context, config)
  end

  test "deduplicates names and fetches all referenced values in one backend command" do
    {:ok, config} = Config.new(backend: BatchBackend, max_batch_commands: 4)
    context = %Auth.Context{session: self()}

    assert {:ok, %{"values" => values}} =
             Values.batch(
               "inv-1",
               ["first", "first", "inline", "second"],
               context,
               config
             )

    assert Map.keys(values) |> Enum.sort() == ["first", "inline", "second"]
    assert values["first"]["bytes_base64"] == Base.encode64("value-for-ref-1")
    assert values["second"]["bytes_base64"] == Base.encode64("value-for-ref-2")
    assert values["inline"]["json"] == %{"ok" => true}

    assert_received {:backend_command,
                     %{
                       "command" => "FLOW.VALUE.MGET",
                       "payload" => %{"refs" => ["ref-1", "ref-2"]}
                     }}

    refute_receive {:backend_command, %{"command" => "FLOW.VALUE.MGET"}}
  end
end
