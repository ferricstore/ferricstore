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

  Connection-scoped transactions, subscriptions, and client state are
  deliberately rejected. Blocking commands may participate in one sequential
  request batch, but that request cannot be coalesced with another request. The
  request process owns each blocking worker, so cancellation and absolute
  deadlines tear down its waiter and budget leases.
  """

  alias FerricstoreServer.AuthenticationGateway
  alias FerricstoreServer.AuthenticationGateway.Session
  alias FerricstoreServer.Native.{Blocking, Commands, ResourceBudget}
  alias FerricstoreServer.Native.Session, as: NativeSession

  defmodule NativeCommand do
    @moduledoc """
    Validated structured command accepted by the transport-neutral gateway.

    Transport adapters must construct this value through
    `FerricstoreServer.CommandGateway.native_command/3`; wire envelopes do not
    belong in the shared execution boundary.
    """

    @enforce_keys [:name, :opcode, :payload]
    defstruct @enforce_keys

    @opaque t :: %__MODULE__{
              name: binary(),
              opcode: non_neg_integer(),
              payload: map()
            }
  end

  defmodule PreparedBatch do
    @moduledoc false

    @enforce_keys [:planned, :deadline_ms]
    defstruct @enforce_keys

    @type planned_command ::
            {:command_exec, map()} | {:blocking, map()} | {:native, non_neg_integer(), map()}

    @opaque t :: %__MODULE__{
              planned: [planned_command()],
              deadline_ms: non_neg_integer()
            }
  end

  @command_exec_opcode 0x0100
  @default_max_commands 1_000
  @deadline_wait_chunk_ms 60_000
  @connection_scoped_commands MapSet.new(~w(AUTH CLIENT HELLO QUIT SELECT))

  @type command :: [term()] | NativeCommand.t()
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
  Creates a validated native command for stateless execution.

  The command name and opcode must identify the same known native command.
  Connection-scoped commands are never accepted. Payload schema, key
  discovery, and ACL validation still run through the canonical native command
  planner before execution.
  """
  @spec native_command(binary(), non_neg_integer(), map()) ::
          {:ok, NativeCommand.t()} | {:error, :invalid_native_command}
  def native_command(command, opcode, payload)
      when is_binary(command) and is_integer(opcode) and opcode >= 0 and is_map(payload) do
    case normalize_command(command) do
      {:ok, normalized} ->
        if structured_native_command?(normalized, opcode) do
          {:ok, %NativeCommand{name: normalized, opcode: opcode, payload: payload}}
        else
          {:error, :invalid_native_command}
        end

      :error ->
        {:error, :invalid_native_command}
    end
  end

  def native_command(_command, _opcode, _payload), do: {:error, :invalid_native_command}

  @doc """
  Validates and plans one request batch without executing it.

  The returned value is opaque and can be combined with other independently
  prepared request batches by `execute_prepared_batches/3`.
  """
  @spec prepare_batch([command()], keyword()) ::
          {:ok, PreparedBatch.t()} | {:error, error()}
  def prepare_batch(commands, opts \\ [])

  def prepare_batch(commands, opts) when is_list(commands) and is_list(opts) do
    with {:ok, max_commands} <- max_commands(opts),
         :ok <- check_batch_size(commands, max_commands),
         {:ok, deadline_ms} <- deadline_ms(opts),
         {:ok, planned} <- plan(commands) do
      {:ok, %PreparedBatch{planned: planned, deadline_ms: deadline_ms}}
    end
  end

  def prepare_batch(_commands, _opts),
    do: {:error, {:invalid_batch, "commands and options must be lists"}}

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
         {:ok, prepared} <- prepare_batch(commands, opts),
         {:ok, [results]} <- execute_prepared_batches_validated(session, [prepared], opts) do
      {:ok, results}
    end
  end

  def execute_batch(%Session{}, _commands, _opts),
    do: {:error, {:invalid_batch, "commands and options must be lists"}}

  def execute_batch(_session, _commands, _opts), do: {:error, :reauthentication_required}

  @doc """
  Executes independently prepared request batches under one session validation
  and one global execution-budget lease.

  Request boundaries, command ordering, and each request's absolute deadline
  remain intact. Every value must originate from `prepare_batch/2`; otherwise
  no request is submitted.
  """
  @spec execute_prepared_batches(Session.t(), [PreparedBatch.t()], keyword()) ::
          {:ok, [[result()]]} | {:error, error()}
  def execute_prepared_batches(session, batches, opts \\ [])

  def execute_prepared_batches(%Session{} = session, batches, opts)
      when is_list(batches) and is_list(opts) do
    with {:ok, session} <- AuthenticationGateway.validate(session),
         :ok <- validate_prepared_batches(batches),
         :ok <- validate_combined_request_shape(batches) do
      execute_prepared_batches_validated(session, batches, opts)
    end
  end

  def execute_prepared_batches(%Session{}, _batches, _opts),
    do: {:error, {:invalid_batch, "prepared batches are invalid"}}

  def execute_prepared_batches(_session, _batches, _opts),
    do: {:error, :reauthentication_required}

  defp execute_prepared_batches_validated(_session, [], _opts), do: {:ok, []}

  defp execute_prepared_batches_validated(session, batches, opts) do
    with {:ok, token} <- acquire_execution_slot() do
      try do
        state = command_state(session, opts)

        {:ok,
         Enum.map(batches, fn %PreparedBatch{planned: planned, deadline_ms: deadline_ms} ->
           execute_planned(planned, state, deadline_ms)
         end)}
      after
        ResourceBudget.release_scoped(token)
      end
    end
  end

  defp validate_prepared_batches(batches) do
    if Enum.all?(batches, &valid_prepared_batch?/1) do
      :ok
    else
      {:error, {:invalid_batch, "prepared batches are invalid"}}
    end
  end

  defp valid_prepared_batch?(%PreparedBatch{planned: planned, deadline_ms: deadline_ms})
       when is_list(planned) and is_integer(deadline_ms) and deadline_ms >= 0 do
    Enum.all?(planned, &valid_planned_command?/1)
  end

  defp valid_prepared_batch?(_invalid), do: false

  defp valid_planned_command?({:command_exec, %{"command" => command, "args" => args} = payload})
       when is_binary(command) and is_list(args) and map_size(payload) == 2 do
    case normalize_command(command) do
      {:ok, normalized} ->
        not unsupported_command?(normalized) and not Blocking.blocking_command?(normalized)

      :error ->
        false
    end
  end

  defp valid_planned_command?({:blocking, %{"command" => command, "args" => args} = payload})
       when is_binary(command) and is_list(args) and map_size(payload) == 2 do
    case normalize_command(command) do
      {:ok, normalized} -> Blocking.blocking_command?(normalized)
      :error -> false
    end
  end

  defp valid_planned_command?({:native, opcode, payload})
       when is_integer(opcode) and opcode >= 0 and is_map(payload) do
    case Commands.command_name(opcode) do
      command when is_binary(command) -> structured_native_command?(command, opcode)
      _unknown -> false
    end
  end

  defp valid_planned_command?(_invalid), do: false

  defp plan(commands) do
    commands
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn
      {%NativeCommand{name: command, opcode: opcode, payload: payload}, index}, {:ok, planned} ->
        if structured_native_command?(command, opcode) do
          {:cont, {:ok, [{:native, opcode, payload} | planned]}}
        else
          {:halt, {:error, {:malformed_command, index}}}
        end

      {[command | args], index}, {:ok, planned} when is_binary(command) and is_list(args) ->
        case normalize_command(command) do
          {:ok, normalized} ->
            cond do
              unsupported_command?(normalized) ->
                {:halt, {:error, {:unsupported_command, index, normalized}}}

              Blocking.blocking_command?(normalized) ->
                payload = %{"command" => command, "args" => args}
                {:cont, {:ok, [{:blocking, payload} | planned]}}

              true ->
                payload = %{"command" => command, "args" => args}
                {:cont, {:ok, [{:command_exec, payload} | planned]}}
            end

          :error ->
            {:halt, {:error, {:malformed_command, index}}}
        end

      {_malformed, index}, {:ok, _planned} ->
        {:halt, {:error, {:malformed_command, index}}}
    end)
    |> case do
      {:ok, planned} ->
        {:ok, Enum.reverse(planned)}

      {:error, _reason} = error ->
        error
    end
  end

  defp execute_planned(planned, state, deadline_ms) do
    Enum.map(planned, fn
      {:command_exec, payload} ->
        payload = add_deadline(payload, deadline_ms)
        {status, value, _state} = Commands.execute(@command_exec_opcode, payload, state)
        %{status: status, value: value}

      {:blocking, payload} ->
        execute_blocking(payload, state, deadline_ms)

      {:native, opcode, payload} ->
        payload = add_deadline(payload, deadline_ms)
        {status, value, _state} = Commands.execute(opcode, payload, state)
        %{status: status, value: value}
    end)
  end

  defp add_deadline(payload, deadline_ms) do
    payload =
      if deadline_ms == 0, do: payload, else: Map.put(payload, "deadline_ms", deadline_ms)

    payload
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
      NativeSession.session_command?(command)
  end

  defp normalize_command(command) do
    if String.valid?(command), do: {:ok, String.upcase(command)}, else: :error
  end

  defp validate_combined_request_shape([_, _ | _] = batches) do
    if Enum.any?(batches, &blocking_batch?/1) do
      {:error, {:invalid_batch, "a blocking request cannot be combined with other requests"}}
    else
      :ok
    end
  end

  defp validate_combined_request_shape(_zero_or_one_batch), do: :ok

  defp blocking_batch?(%PreparedBatch{planned: planned}) do
    Enum.any?(planned, fn
      {:blocking, _payload} -> true
      _non_blocking -> false
    end)
  end

  defp blocking_batch?(_batch), do: false

  defp execute_blocking(payload, state, deadline_ms) do
    if deadline_expired?(deadline_ms) do
      deadline_exceeded_result()
    else
      meta = %{gateway_request: make_ref()}

      case Blocking.start_request(payload, state, meta) do
        {:ok, pid, monitor_ref} ->
          await_blocking_result(pid, monitor_ref, meta, deadline_ms)

        {:error, status, value} ->
          %{status: status, value: value}
      end
    end
  end

  defp await_blocking_result(pid, monitor_ref, meta, 0) do
    receive do
      {:native_blocking_response, ^meta, ^pid, status, value} ->
        Process.demonitor(monitor_ref, [:flush])
        %{status: status, value: value}

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        blocking_worker_exit_result(reason)
    end
  end

  defp await_blocking_result(pid, monitor_ref, meta, deadline_ms) do
    timeout_ms =
      deadline_ms
      |> Kernel.-(System.system_time(:millisecond))
      |> max(0)
      |> min(@deadline_wait_chunk_ms)

    receive do
      {:native_blocking_response, ^meta, ^pid, status, value} ->
        Process.demonitor(monitor_ref, [:flush])
        %{status: status, value: value}

      {:DOWN, ^monitor_ref, :process, ^pid, reason} ->
        blocking_worker_exit_result(reason)
    after
      timeout_ms ->
        if deadline_expired?(deadline_ms) do
          stop_blocking_worker(pid, monitor_ref, meta)
          deadline_exceeded_result()
        else
          await_blocking_result(pid, monitor_ref, meta, deadline_ms)
        end
    end
  end

  defp stop_blocking_worker(pid, monitor_ref, meta) do
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^monitor_ref, :process, ^pid, _reason} -> :ok
    after
      1_000 -> Process.demonitor(monitor_ref, [:flush])
    end

    receive do
      {:native_blocking_response, ^meta, ^pid, _status, _value} -> :ok
    after
      0 -> :ok
    end
  end

  defp deadline_expired?(0), do: false
  defp deadline_expired?(deadline_ms), do: deadline_ms <= System.system_time(:millisecond)

  defp deadline_exceeded_result,
    do: %{status: :error, value: %{"code" => "deadline_exceeded"}}

  defp blocking_worker_exit_result(:normal),
    do: %{status: :error, value: "ERR blocking worker exited without a response"}

  defp blocking_worker_exit_result(_reason),
    do: %{status: :error, value: "ERR internal server error"}

  defp structured_native_command?(command, opcode) do
    Commands.command_name(opcode) == command and not unsupported_command?(command)
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
    case ResourceBudget.acquire_scoped(:executions, 1) do
      {:ok, token} -> {:ok, token}
      {:error, {:limit, :executions}} -> {:error, {:busy, "global execution limit exceeded"}}
      {:error, _reason} -> {:error, :resource_budget_unavailable}
    end
  end
end
