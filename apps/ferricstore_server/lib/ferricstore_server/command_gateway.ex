defmodule FerricstoreServer.CommandGateway do
  @moduledoc """
  Public transport-neutral boundary for stateless FerricStore command batches.

  Protocol applications pass an opaque session from
  `FerricstoreServer.AuthenticationGateway` and raw command arrays such as
  `["SET", "key", "value"]`. Execution reuses the canonical native generic
  command path, including parsing, command/key ACL checks, deadlines, resource
  limits, cluster write routing, and result normalization.

  Reads retain FerricStore's embedded/local read semantics. A protocol that
  promises leader reads must add that routing policy before calling this local
  execution boundary.

  Connection-scoped transactions, subscriptions, blocking commands, and client
  state are deliberately rejected. Those require a persistent native session.
  """

  alias FerricstoreServer.AuthenticationGateway
  alias FerricstoreServer.AuthenticationGateway.Session
  alias FerricstoreServer.Native.{Blocking, Commands, ResourceBudget}
  alias FerricstoreServer.Native.Session, as: NativeSession

  @command_exec_opcode 0x0100
  @default_max_commands 1_000
  @connection_scoped_commands MapSet.new(~w(AUTH CLIENT HELLO QUIT SELECT))

  @type command :: [term()]
  @type result :: %{status: atom(), value: term()}
  @type error ::
          :reauthentication_required
          | :resource_budget_unavailable
          | {:busy, binary()}
          | {:invalid_batch, binary()}
          | {:invalid_deadline, term()}
          | {:malformed_command, non_neg_integer()}
          | {:too_many_commands, pos_integer()}
          | {:unsupported_command, non_neg_integer(), binary()}

  @doc """
  Executes an ordered, stateless command batch.

  Command failures are returned in place inside the successful batch result.
  Top-level errors mean no command was submitted.
  """
  @spec execute_batch(Session.t(), [command()], keyword()) ::
          {:ok, [result()]} | {:error, error()}
  def execute_batch(session, commands, opts \\ [])

  def execute_batch(%Session{} = session, commands, opts)
      when is_list(commands) and is_list(opts) do
    with {:ok, session} <- AuthenticationGateway.validate(session),
         {:ok, max_commands} <- max_commands(opts),
         :ok <- check_batch_size(commands, max_commands),
         {:ok, deadline_ms} <- deadline_ms(opts),
         {:ok, planned} <- plan(commands),
         {:ok, token} <- acquire_execution_slot() do
      try do
        {:ok, execute_planned(planned, session, deadline_ms, opts)}
      after
        ResourceBudget.release(token)
      end
    end
  end

  def execute_batch(%Session{}, _commands, _opts),
    do: {:error, {:invalid_batch, "commands and options must be lists"}}

  def execute_batch(_session, _commands, _opts), do: {:error, :reauthentication_required}

  defp plan(commands) do
    commands
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn
      {[command | args], index}, {:ok, planned} when is_binary(command) and is_list(args) ->
        normalized = String.upcase(command)

        if unsupported_command?(normalized) do
          {:halt, {:error, {:unsupported_command, index, normalized}}}
        else
          payload = %{"command" => command, "args" => args}
          {:cont, {:ok, [payload | planned]}}
        end

      {_malformed, index}, {:ok, _planned} ->
        {:halt, {:error, {:malformed_command, index}}}
    end)
    |> case do
      {:ok, planned} -> {:ok, Enum.reverse(planned)}
      {:error, _reason} = error -> error
    end
  end

  defp execute_planned(planned, session, deadline_ms, opts) do
    state = command_state(session, opts)

    Enum.map(planned, fn payload ->
      payload =
        if deadline_ms == 0, do: payload, else: Map.put(payload, "deadline_ms", deadline_ms)

      {status, value, _state} = Commands.execute(@command_exec_opcode, payload, state)
      %{status: status, value: value}
    end)
  end

  defp command_state(%Session{} = session, opts) do
    store = Keyword.get_lazy(opts, :store, fn -> FerricStore.Instance.get(:default) end)

    %{
      client_id: System.unique_integer([:positive, :monotonic]),
      client_name: Keyword.get(opts, :client_name, "protocol-gateway"),
      username: session.username,
      authenticated: true,
      require_auth: false,
      acl_cache: session.acl_cache,
      peer: session.peer,
      created_at: session.authenticated_at_ms,
      instance_ctx: store,
      stats_counter: store.stats_counter,
      compression: :none,
      compact_flow_responses: false,
      compact_response_codecs: MapSet.new(),
      subscribed_events: MapSet.new(),
      flow_wake_subscriptions: MapSet.new()
    }
  end

  defp unsupported_command?(command) do
    MapSet.member?(@connection_scoped_commands, command) or
      NativeSession.session_command?(command) or Blocking.blocking_command?(command)
  end

  defp max_commands(opts) do
    case Keyword.get(opts, :max_commands, @default_max_commands) do
      max when is_integer(max) and max > 0 ->
        {:ok, max}

      invalid ->
        {:error,
         {:invalid_batch, "max_commands must be a positive integer, got #{inspect(invalid)}"}}
    end
  end

  defp check_batch_size(commands, max_commands) do
    result =
      Enum.reduce_while(commands, 0, fn _command, count ->
        if count == max_commands do
          {:halt, :too_many}
        else
          {:cont, count + 1}
        end
      end)

    case result do
      :too_many -> {:error, {:too_many_commands, max_commands}}
      _count -> :ok
    end
  end

  defp deadline_ms(opts) do
    case Keyword.get(opts, :deadline_ms, 0) do
      value when is_integer(value) and value >= 0 -> {:ok, value}
      invalid -> {:error, {:invalid_deadline, invalid}}
    end
  end

  defp acquire_execution_slot do
    case ResourceBudget.acquire(:executions, self(), 1) do
      {:ok, token} -> {:ok, token}
      {:error, {:limit, :executions}} -> {:error, {:busy, "global execution limit exceeded"}}
      {:error, _reason} -> {:error, :resource_budget_unavailable}
    end
  end
end
