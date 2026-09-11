defmodule FerricstoreHttp.Invocations.Backend do
  @moduledoc "In-process command boundary for invocation and Flow operations."

  alias FerricstoreHttp.{Auth, Config, Deadline}

  @invocation_error_patterns [
    {"invalid_invocation_name", :invalid_invocation_name},
    {"invalid invocation name", :invalid_invocation_name},
    {"definition_not_found", :definition_not_found},
    {"definition not found", :definition_not_found},
    {"invocation_not_found", :invocation_not_found},
    {"invocation not found", :invocation_not_found},
    {"invocation_disabled", :invocation_disabled},
    {"idempotency_key_required", :idempotency_key_required},
    {"idempotency_conflict", :idempotency_conflict},
    {"subject_required", :subject_required},
    {"payload_too_large", :payload_too_large},
    {"forbidden", :forbidden},
    {"reauthentication_required", :reauthentication_required},
    {"reauthentication required", :reauthentication_required},
    {"unknown command", :invocations_unavailable},
    {"unsupported command", :invocations_unavailable},
    {"err unsupported invocation.", :invocations_unavailable}
  ]

  @spec definition_get(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def definition_get(context, name, config, opts \\ []),
    do: command(context, ["INVOCATION.DEFINITION.GET", name], config, opts)

  @spec definition_list(Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def definition_list(context, config, opts \\ []),
    do: command(context, ["INVOCATION.DEFINITION.LIST"], config, opts)

  @spec definition_put(Auth.Context.t(), map(), Config.t(), keyword()) ::
          :ok | {:error, term()}
  def definition_put(context, definition, config, opts \\ []) do
    case command(
           context,
           ["INVOCATION.DEFINITION.PUT", Jason.encode!(definition)],
           config,
           opts
         ) do
      {:ok, _value} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @spec invocation_create(Auth.Context.t(), binary(), map(), Config.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def invocation_create(context, name, attrs, config, opts \\ []) do
    envelope =
      %{"attrs" => attrs, "context" => request_context(context)}
      |> put_if_present("idempotency_key", Keyword.get(opts, :idempotency_key))

    command(
      context,
      ["INVOCATION.CREATE", name, Jason.encode!(envelope)],
      config,
      Keyword.put(opts, :request_context, request_context(context))
    )
  end

  @spec invocation_get(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def invocation_get(context, id, config, opts \\ []),
    do: command(context, ["INVOCATION.GET", id], config, opts)

  @spec invocation_partition_list(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, [binary()]} | {:error, term()}
  def invocation_partition_list(context, name, config, opts \\ []) do
    command(context, ["INVOCATION.PARTITION.LIST", name], config, opts)
  end

  @spec flow_get(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def flow_get(context, id, config, opts \\ []) do
    payload =
      %{"id" => id}
      |> put_from_opts(opts, "partition_key", :partition_key)
      |> put_from_opts(opts, "payload", :payload)
      |> put_from_opts(opts, "values", :values)

    native(context, "FLOW.GET", 0x0202, payload, config, opts)
  end

  @spec flow_claim_due(Auth.Context.t(), binary(), Config.t(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def flow_claim_due(context, type, config, opts) do
    payload =
      %{
        "type" => type,
        "worker" => Keyword.fetch!(opts, :worker),
        "lease_ms" => Keyword.get(opts, :lease_ms, 30_000),
        "limit" => Keyword.get(opts, :limit, 1)
      }
      |> put_claim_states(opts)
      |> put_from_opts(opts, "partition_keys", :partition_keys)

    native(context, "FLOW.CLAIM_DUE", 0x0203, payload, config, opts)
  end

  def flow_complete(context, id, config, opts),
    do: terminal(context, id, {"FLOW.COMPLETE", 0x0204}, {"result", :result}, config, opts)

  def flow_retry(context, id, config, opts) do
    payload =
      terminal_payload(id, opts)
      |> put_from_opts(opts, "error", :error)
      |> put_from_opts(opts, "run_at_ms", :run_at_ms)

    native(context, "FLOW.RETRY", 0x0206, payload, config, opts)
  end

  def flow_fail(context, id, config, opts),
    do: terminal(context, id, {"FLOW.FAIL", 0x0207}, {"error", :error}, config, opts)

  def flow_value_put(context, value, config, opts) do
    payload =
      %{"value" => value}
      |> put_from_opts(opts, "partition_key", :partition_key)
      |> put_from_opts(opts, "owner_flow_id", :owner_flow_id)
      |> put_from_opts(opts, "name", :name)

    native(context, "FLOW.VALUE.PUT", 0x020B, payload, config, opts)
  end

  def flow_value_mget(_context, [], _config, _opts), do: {:ok, []}

  def flow_value_mget(context, refs, config, opts) do
    payload =
      %{"refs" => refs}
      |> put_from_opts(opts, "max_bytes", :max_bytes)

    native(context, "FLOW.VALUE.MGET", 0x020C, payload, config, opts)
  end

  defp terminal(context, id, {command, opcode}, {field, option}, config, opts) do
    payload = terminal_payload(id, opts) |> put_from_opts(opts, field, option)
    native(context, command, opcode, payload, config, opts)
  end

  defp terminal_payload(id, opts) do
    %{
      "id" => id,
      "lease_token" => Keyword.fetch!(opts, :lease_token),
      "fencing_token" => Keyword.fetch!(opts, :fencing_token),
      "now_ms" => System.system_time(:millisecond)
    }
    |> put_from_opts(opts, "partition_key", :partition_key)
  end

  defp native(context, command_name, opcode, payload, config, opts) do
    command = %{"command" => command_name, "opcode" => opcode, "payload" => payload}
    command(context, command, config, opts)
  end

  defp command(%Auth.Context{} = context, command, %Config{} = config, opts) do
    backend_opts =
      [deadline_ms: deadline_ms(opts)]
      |> put_option(
        :request_context,
        Keyword.get(opts, :request_context, request_context(context))
      )
      |> put_option(:store, Keyword.get(opts, :store))

    case config.backend.execute_batch(context.session, [command], backend_opts) do
      {:ok, [%{status: :ok, value: value}]} -> {:ok, stringify(value)}
      {:ok, [%{status: status, value: reason}]} -> {:error, normalize_status(status, reason)}
      {:error, reason} -> {:error, normalize_error(reason)}
      _invalid -> {:error, :invalid_backend_response}
    end
  end

  defp normalize_status(:noperm, _reason), do: :forbidden
  defp normalize_status(_status, reason), do: normalize_error(reason)

  defp deadline_ms(opts) do
    case Keyword.get(opts, :deadline) do
      nil -> Keyword.get(opts, :deadline_ms, 0)
      deadline -> Deadline.system_ms(deadline)
    end
  end

  defp request_context(%Auth.Context{} = context) do
    %{"subject" => context.subject, "scopes" => context.scopes}
    |> Enum.reject(fn {_key, value} -> value in [nil, []] end)
    |> Map.new()
  end

  defp normalize_error({:malformed_command, _index}), do: :invocations_unavailable

  defp normalize_error({:unsupported_command, _index, _command}),
    do: :invocations_unavailable

  defp normalize_error(reason) when is_atom(reason), do: reason

  defp normalize_error(reason) when is_binary(reason) do
    normalized = String.downcase(reason)

    case Enum.find(@invocation_error_patterns, fn {pattern, _error} ->
           String.contains?(normalized, pattern)
         end) do
      {_pattern, error} -> error
      nil -> reason
    end
  end

  defp normalize_error(_reason), do: :backend_error

  defp stringify(%{} = map) do
    Map.new(map, fn {key, value} -> {to_string(key), stringify(value)} end)
  end

  defp stringify(values) when is_list(values), do: Enum.map(values, &stringify/1)
  defp stringify(value), do: value

  defp put_from_opts(map, opts, key, option),
    do: put_if_present(map, key, Keyword.get(opts, option))

  defp put_claim_states(map, opts) do
    case {Keyword.get(opts, :state), Keyword.get(opts, :states)} do
      {state, _states} when is_binary(state) -> Map.put(map, "states", [state])
      {_state, states} when is_list(states) -> Map.put(map, "states", states)
      _missing -> map
    end
  end

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp put_option(opts, _key, nil), do: opts
  defp put_option(opts, key, value), do: Keyword.put(opts, key, value)
end
