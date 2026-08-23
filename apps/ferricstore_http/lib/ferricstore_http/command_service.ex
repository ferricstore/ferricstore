defmodule FerricstoreHttp.CommandService do
  @moduledoc """
  Validates HTTP command envelopes and delegates one ordered batch to FerricStore.
  """

  alias FerricstoreHttp.{Backend, BinaryEnvelope, CommandError, Config, Deadline}

  @binary_encoding "ferricstore-json-v1"
  @compact_encoding "ferricstore-msgpack-v1"
  @blocking_commands MapSet.new(~w(BLPOP BRPOP BLMOVE BLMPOP XREAD XREADGROUP))

  defmodule Prepared do
    @moduledoc false

    @enforce_keys [:commands, :encoding, :count]
    defstruct @enforce_keys
  end

  @opaque prepared :: %Prepared{
            commands: [Backend.command()],
            encoding: atom(),
            count: non_neg_integer()
          }

  @spec prepare(map()) :: {:ok, prepared()} | {:error, term()}
  def prepare(envelope) do
    with {:ok, commands, encoding} <- commands(envelope) do
      {:ok, %Prepared{commands: commands, encoding: encoding, count: length(commands)}}
    end
  end

  @spec command_count(prepared()) :: non_neg_integer()
  def command_count(%Prepared{count: count}), do: count

  @spec execute(map(), term(), Config.t(), Deadline.t()) ::
          {:ok, map()} | {:error, term()}
  def execute(envelope, session, %Config{} = config, deadline) do
    with {:ok, prepared} <- prepare(envelope) do
      execute_prepared(prepared, session, config, deadline)
    end
  end

  @spec execute_prepared(prepared(), term(), Config.t(), Deadline.t()) ::
          {:ok, map()} | {:error, term()}
  def execute_prepared(%Prepared{} = prepared, session, %Config{} = config, deadline) do
    with %Prepared{commands: commands, encoding: encoding} <- prepared,
         {:ok, results} <-
           config.backend.execute_batch(session, commands,
             deadline_ms: Deadline.system_ms(deadline),
             max_commands: config.max_batch_commands
           ) do
      {:ok, response(results, encoding)}
    end
  end

  @spec execute_many([{prepared(), Deadline.t()}], term(), Config.t()) ::
          [{:ok, map()} | {:error, term()}]
  def execute_many([], _session, %Config{}), do: []

  def execute_many(requests, session, %Config{} = config) when is_list(requests) do
    if batch_capable?(config.backend) and length(requests) > 1 and
         not includes_blocking_request?(requests) do
      execute_combined(requests, session, config)
    else
      Enum.map(requests, fn {prepared, deadline} ->
        execute_prepared(prepared, session, config, deadline)
      end)
    end
  end

  defp execute_combined(requests, session, config) do
    {ready, completed} = prepare_backend_batches(requests, config)

    case ready do
      [] ->
        ordered_results(completed, length(requests))

      _prepared ->
        batches = Enum.map(ready, & &1.backend_batch)

        result = config.backend.execute_prepared_batches(session, batches, [])
        complete_combined(result, ready, completed, length(requests))
    end
  end

  defp prepare_backend_batches(requests, config) do
    requests
    |> Enum.with_index()
    |> Enum.reduce({[], %{}}, fn {{prepared, deadline}, index}, {ready, completed} ->
      opts = [
        deadline_ms: Deadline.system_ms(deadline),
        max_commands: config.max_batch_commands
      ]

      case config.backend.prepare_batch(prepared.commands, opts) do
        {:ok, backend_batch} ->
          request = %{index: index, prepared: prepared, backend_batch: backend_batch}
          {[request | ready], completed}

        {:error, _reason} = error ->
          {ready, Map.put(completed, index, error)}

        _invalid ->
          {ready, Map.put(completed, index, {:error, :invalid_backend_response})}
      end
    end)
    |> then(fn {ready, completed} -> {Enum.reverse(ready), completed} end)
  end

  defp complete_combined({:ok, batch_results}, ready, completed, request_count)
       when is_list(batch_results) and length(batch_results) == length(ready) do
    completed =
      Enum.zip(ready, batch_results)
      |> Enum.reduce(completed, fn {request, results}, completed ->
        result = normalize_combined_result(request.prepared, results)
        Map.put(completed, request.index, result)
      end)

    ordered_results(completed, request_count)
  end

  defp complete_combined({:error, _reason} = error, ready, completed, request_count) do
    completed = Enum.reduce(ready, completed, &Map.put(&2, &1.index, error))
    ordered_results(completed, request_count)
  end

  defp complete_combined(_invalid, ready, completed, request_count) do
    complete_combined(
      {:error, :invalid_backend_response},
      ready,
      completed,
      request_count
    )
  end

  defp normalize_combined_result(%Prepared{count: count, encoding: encoding}, results)
       when is_list(results) and length(results) == count do
    {:ok, response(results, encoding)}
  end

  defp normalize_combined_result(_prepared, _invalid),
    do: {:error, :invalid_backend_response}

  defp ordered_results(completed, request_count) do
    Enum.map(0..(request_count - 1), &Map.fetch!(completed, &1))
  end

  defp batch_capable?(backend) do
    function_exported?(backend, :prepare_batch, 2) and
      function_exported?(backend, :execute_prepared_batches, 3) and
      prepared_batching_supported?(backend)
  end

  defp prepared_batching_supported?(backend) do
    not function_exported?(backend, :prepared_batching_supported?, 0) or
      backend.prepared_batching_supported?()
  end

  defp commands(%{"commands" => encoded, "encoding" => encoding})
       when encoding == @binary_encoding do
    case BinaryEnvelope.decode(encoded) do
      {:ok, commands} when is_list(commands) -> {:ok, commands, :binary_json}
      _invalid -> {:error, :malformed_binary_envelope}
    end
  end

  defp commands(%{"commands" => commands, "encoding" => @compact_encoding})
       when is_list(commands),
       do: {:ok, commands, :msgpack}

  defp commands(%{"commands" => commands}) when is_list(commands),
    do: {:ok, commands, :legacy_json}

  defp commands(_invalid), do: {:error, :malformed_envelope}

  defp includes_blocking_request?(requests) do
    Enum.any?(requests, fn
      {%Prepared{commands: commands}, _deadline} -> Enum.any?(commands, &blocking_command?/1)
      _invalid -> false
    end)
  end

  defp blocking_command?([command | _args]) when is_binary(command),
    do: MapSet.member?(@blocking_commands, String.upcase(command, :ascii))

  defp blocking_command?(_command), do: false

  defp response(results, :binary_json) do
    %{
      "encoding" => BinaryEnvelope.encoding(),
      "results" => Enum.map(results, &normalize_result(&1, true))
    }
  end

  defp response(results, :msgpack) do
    %{
      "encoding" => @compact_encoding,
      "results" => Enum.map(results, &normalize_result(&1, false))
    }
  end

  defp response(results, :legacy_json) do
    %{"results" => Enum.map(results, &normalize_result(&1, false))}
  end

  defp normalize_result(%{status: :ok, value: value}, binary_safe?) do
    value = if binary_safe?, do: BinaryEnvelope.encode(value), else: value
    %{"status" => "ok", "value" => value}
  end

  defp normalize_result(%{status: status, value: value}, _binary_safe?) do
    %{
      "status" => "error",
      "error" => CommandError.normalize(status, value)
    }
  end
end
