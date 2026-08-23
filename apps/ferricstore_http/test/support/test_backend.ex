defmodule FerricstoreHttp.TestBackend do
  @moduledoc false

  @behaviour FerricstoreHttp.Backend

  @impl FerricstoreHttp.Backend
  def authenticate("worker", "secret", opts), do: {:ok, {:session, Keyword.get(opts, :peer)}}
  def authenticate(_username, _password, _opts), do: {:error, :unauthenticated}

  @impl FerricstoreHttp.Backend
  def execute_batch({:session, _peer}, commands, _opts) do
    results =
      Enum.map(commands, fn
        ["PING"] ->
          %{status: :ok, value: "PONG"}

        ["ECHO", value] ->
          %{status: :ok, value: value}

        ["INVOCATION.CREATE", name, _envelope] ->
          %{
            status: :ok,
            value: %{
              "invocation_id" => "inv-test",
              "name" => name,
              "state" => "queued",
              "flow_type" => "invocation:#{name}"
            }
          }

        ["INVOCATION.GET", id] ->
          %{
            status: :ok,
            value: %{
              "id" => id,
              "name" => "send-email",
              "flow_type" => "invocation:send-email",
              "value_policy" => %{
                "refs" => %{
                  "allowed_read_names" => "*",
                  "allowed_write_names" => "*"
                }
              }
            }
          }

        ["INVOCATION.DEFINITION.GET", name] ->
          %{
            status: :ok,
            value: %{
              "name" => name,
              "flow_type" => "invocation:#{name}",
              "refs" => %{"allowed_read_names" => "*", "allowed_write_names" => "*"}
            }
          }

        %{"command" => "FLOW.GET", "payload" => %{"id" => id}} ->
          %{
            status: :ok,
            value: %{
              "id" => id,
              "state" => "completed",
              "result" => Jason.encode!(%{"delivered" => true}),
              "values" => %{"receipt" => Jason.encode!(%{"accepted" => true})}
            }
          }

        [command | _args] ->
          %{status: :error, value: "ERR unsupported #{command}"}

        %{"command" => command} ->
          %{status: :error, value: "ERR unsupported #{command}"}
      end)

    {:ok, results}
  end

  @impl FerricstoreHttp.Backend
  def prepare_batch(commands, opts), do: {:ok, {commands, opts}}

  @impl FerricstoreHttp.Backend
  def execute_prepared_batches(session, batches, _opts) do
    results =
      Enum.map(batches, fn {commands, opts} ->
        {:ok, values} = execute_batch(session, commands, opts)
        values
      end)

    {:ok, results}
  end

  @impl FerricstoreHttp.Backend
  def ready?, do: true
end

defmodule FerricstoreHttp.ControlledTestBackend do
  @moduledoc false

  use Agent

  @behaviour FerricstoreHttp.Backend

  def start_link(_opts) do
    Agent.start_link(fn -> initial_state() end, name: __MODULE__)
  end

  def reset(overrides \\ %{}) do
    Agent.update(__MODULE__, fn _state -> Map.merge(initial_state(), overrides) end)
  end

  def calls, do: Agent.get(__MODULE__, &Enum.reverse(&1.calls))

  def put(key, value), do: Agent.update(__MODULE__, &Map.put(&1, key, value))

  @impl FerricstoreHttp.Backend
  def authenticate(username, password, opts) do
    Agent.get_and_update(__MODULE__, fn state ->
      result =
        case state.auth_result do
          :credentials -> authenticate_credentials(username, password, opts)
          configured -> configured
        end

      {result, %{state | calls: [{:authenticate, username, password, opts} | state.calls]}}
    end)
  end

  @impl FerricstoreHttp.Backend
  def execute_batch(session, commands, opts) do
    Agent.get_and_update(__MODULE__, fn state ->
      result =
        case state.execute_result do
          :commands -> execute_commands(commands)
          operation when is_function(operation, 3) -> operation.(session, commands, opts)
          configured -> configured
        end

      {result, %{state | calls: [{:execute_batch, session, commands, opts} | state.calls]}}
    end)
  end

  @impl FerricstoreHttp.Backend
  def prepare_batch(commands, opts), do: {:ok, {commands, opts}}

  @impl FerricstoreHttp.Backend
  def execute_prepared_batches(session, batches, opts) do
    Agent.get_and_update(__MODULE__, fn state ->
      result =
        case state.execute_result do
          :commands ->
            {:ok,
             Enum.map(batches, fn {commands, _request_opts} ->
               {:ok, values} = execute_commands(commands)
               values
             end)}

          configured ->
            configured
        end

      call = {:execute_prepared_batches, session, batches, opts}
      {result, %{state | calls: [call | state.calls]}}
    end)
  end

  @impl FerricstoreHttp.Backend
  def ready?, do: Agent.get(__MODULE__, & &1.ready)

  defp initial_state do
    %{ready: true, auth_result: :credentials, execute_result: :commands, calls: []}
  end

  defp authenticate_credentials("worker", "secret:with:colon", opts),
    do: {:ok, {:session, Keyword.fetch!(opts, :peer)}}

  defp authenticate_credentials(_username, _password, _opts), do: {:error, :unauthenticated}

  defp execute_commands(commands) do
    results =
      Enum.map(commands, fn
        ["PING"] -> %{status: :ok, value: "PONG"}
        ["ECHO", value] -> %{status: :ok, value: value}
        ["ERROR", message] -> %{status: :upstream_error, value: message}
        ["ATOM_ERROR", value] -> %{status: :error, value: value}
        ["OPAQUE_ERROR"] -> %{status: :error, value: %{private: "must not leak"}}
        [command | _args] -> %{status: :error, value: "ERR unsupported #{command}"}
      end)

    {:ok, results}
  end
end
