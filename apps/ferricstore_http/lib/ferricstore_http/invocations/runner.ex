defmodule FerricstoreHttp.Invocations.Runner do
  @moduledoc "Claims due invocation Flow records and executes supported targets."

  use GenServer

  require Logger

  alias FerricstoreHttp.{Auth, Config, Deadline}
  alias FerricstoreHttp.Invocations.{Backend, Definition, DefinitionStore, SystemSession}
  alias FerricstoreHttp.Targets.{EgressPolicy, HttpEndpoint}

  @empty_stats %{definitions: 0, claimed: 0, completed: 0, retried: 0, failed: 0, errors: 0}
  @job_timeout_margin_ms 250

  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config),
    do: GenServer.start_link(__MODULE__, config, name: __MODULE__)

  @spec run_once(Config.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def run_once(%Config{} = config, opts \\ []) do
    config
    |> do_run_once(opts)
    |> maybe_refresh_context(config, opts)
  end

  defp do_run_once(config, opts) do
    with {:ok, context} <- context(opts),
         {:ok, definitions} <- DefinitionStore.list(context, config, backend_opts(config)) do
      definitions
      |> Enum.filter(&runnable?/1)
      |> run_definitions(context, config, opts)
    end
  end

  defp maybe_refresh_context({:error, :reauthentication_required}, config, opts) do
    if Keyword.has_key?(opts, :context) do
      {:error, :reauthentication_required}
    else
      with {:ok, context} <- SystemSession.refresh() do
        do_run_once(config, Keyword.put(opts, :context, context))
      end
    end
  end

  defp maybe_refresh_context(result, _config, _opts), do: result

  @spec run_definition(Definition.t(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def run_definition(%Definition{} = definition, context, config, opts \\ []) do
    if runnable?(definition) do
      claim_and_run(definition, context, config, opts)
    else
      {:ok, @empty_stats}
    end
  end

  @impl GenServer
  def init(%Config{} = config), do: {:ok, schedule(config, 0)}

  @impl GenServer
  def handle_info(:poll, config) do
    config
    |> run_once()
    |> log_run_error()

    {:noreply, schedule(config, config.runner_poll_interval_ms)}
  end

  defp claim_and_run(definition, context, config, opts) do
    case Backend.invocation_partition_list(
           context,
           definition.name,
           config,
           backend_opts(config)
         ) do
      {:ok, []} ->
        {:ok, empty_stats()}

      {:ok, partition_keys} when is_list(partition_keys) ->
        case Backend.flow_claim_due(
               context,
               definition.flow_type,
               config,
               claim_opts(definition, config, opts, partition_keys)
             ) do
          {:ok, jobs} when is_list(jobs) ->
            stats = %{empty_stats() | claimed: length(jobs)}

            result =
              jobs
              |> run_jobs(definition, context, config, opts)
              |> Enum.reduce({:ok, stats}, &accumulate_job_result/2)

            log_definition_stats(definition, result)
            result

          {:error, _reason} = error ->
            error

          other ->
            {:error, {:unexpected_claim_response, other}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp run_job(definition, job, context, config, opts) do
    target = Keyword.get(opts, :target, HttpEndpoint)

    case load_record(job, context, config) do
      {:ok, record} ->
        if invocation_record?(record, definition) do
          invoke_target(target, definition, job, record, context, config)
        else
          {:error, :invocation_identity_mismatch}
        end

      {:error, reason} ->
        retry_job(definition, job, reason, context, config)
    end
  end

  defp invoke_target(target, definition, job, record, context, config) do
    request = build_request(definition, job, record)

    case target.invoke(definition.target, request, target_opts(definition, config)) do
      {:ok, result} -> complete_job(definition, job, result, context, config)
      {:retry, reason} -> retry_job(definition, job, reason, context, config)
      {:error, reason} -> fail_job(job, reason, context, config)
      _invalid -> fail_job(job, :invalid_target_response, context, config)
    end
  end

  defp invocation_record?(%{"attributes" => attributes}, definition) when is_map(attributes),
    do: attributes["invocation_name"] == definition.name

  defp invocation_record?(_record, _definition), do: false

  defp run_jobs([], _definition, _context, _config, _opts), do: []

  defp run_jobs(jobs, definition, context, config, opts) do
    jobs
    |> Task.async_stream(
      &run_job(definition, &1, context, config, opts),
      max_concurrency: length(jobs),
      ordered: false,
      timeout: job_timeout_ms(definition, config),
      on_timeout: :kill_task
    )
    |> Enum.map(fn
      {:ok, result} -> result
      {:exit, _reason} -> {:error, :runner_job_crashed}
    end)
  end

  defp load_record(%{"id" => id} = job, context, config) do
    opts =
      config
      |> backend_opts()
      |> Keyword.put(:payload, true)
      |> maybe_put_partition(job["partition_key"])

    case Backend.flow_get(context, id, config, opts) do
      {:ok, nil} -> {:error, :invocation_not_found}
      {:ok, %{} = record} -> {:ok, record}
      {:error, _reason} -> {:error, :backend_error}
    end
  end

  defp load_record(_job, _context, _config), do: {:error, :invalid_claim_record}

  defp build_request(definition, job, record) do
    %{
      "invocation_id" => job["id"] || record["id"],
      "name" => definition.name,
      "flow_type" => definition.flow_type,
      "state" => record["state"] || job["state"],
      "partition_key" => job["partition_key"] || record["partition_key"],
      "payload" => decode_json_field(record["payload"]),
      "value_refs" => record["value_refs"],
      "attributes" => record["attributes"],
      "correlation_id" => record["correlation_id"]
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp complete_job(definition, job, result, context, config) do
    with {:ok, result} <- encode_result(definition, result),
         {:ok, id, opts} <- terminal_opts(job, config),
         {:ok, _value} <-
           Backend.flow_complete(context, id, config, Keyword.put(opts, :result, result)) do
      {:completed, id}
    else
      {:error, :result_too_large} ->
        fail_job(job, :result_too_large, context, config)

      {:error, _reason} = error ->
        error
    end
  end

  defp retry_job(definition, job, reason, context, config) do
    with {:ok, id, opts} <- terminal_opts(job, config),
         opts =
           opts
           |> Keyword.put(:error, encode_error(reason))
           |> Keyword.put(:run_at_ms, now_ms() + retry_delay_ms(definition, config)),
         {:ok, _value} <- Backend.flow_retry(context, id, config, opts) do
      {:retried, id}
    end
  end

  defp fail_job(job, reason, context, config) do
    with {:ok, id, opts} <- terminal_opts(job, config),
         {:ok, _value} <-
           Backend.flow_fail(context, id, config, Keyword.put(opts, :error, encode_error(reason))) do
      {:failed, id}
    end
  end

  defp terminal_opts(job, config) do
    with {:ok, id} <- fetch_job(job, "id"),
         {:ok, lease_token} <- fetch_job(job, "lease_token"),
         {:ok, fencing_token} <- fetch_job(job, "fencing_token") do
      opts =
        config
        |> backend_opts()
        |> Keyword.put(:lease_token, lease_token)
        |> Keyword.put(:fencing_token, fencing_token)
        |> maybe_put_partition(job["partition_key"])

      {:ok, id, opts}
    end
  end

  defp fetch_job(job, key) do
    case Map.fetch(job, key) do
      {:ok, value} when not is_nil(value) -> {:ok, value}
      _missing -> {:error, :invalid_claim_record}
    end
  end

  defp claim_opts(definition, config, opts, partition_keys) do
    [
      state: definition.runner["state"] || definition.initial_state,
      worker: definition.runner["worker_id"] || Keyword.get(opts, :worker_id, config.runner_id),
      lease_ms: safe_lease_ms(definition, config),
      limit: positive_int(definition.runner["claim_limit"], config.runner_default_claim_limit),
      payload: true,
      values: true,
      deadline: Deadline.new(config.request_timeout_ms)
    ]
    |> maybe_put_partition_keys(partition_keys)
  end

  defp runnable?(%Definition{enabled?: true, target: %{"kind" => "http_endpoint"}}), do: true
  defp runnable?(%Definition{}), do: false

  defp target_opts(definition, config) do
    [
      timeout: positive_int(definition.target["timeout_ms"], config.runner_target_timeout_ms),
      egress_policy: EgressPolicy.from_config(config),
      max_response_bytes: config.target_max_response_bytes
    ]
  end

  defp retry_delay_ms(definition, config),
    do: positive_int(definition.runner["retry_delay_ms"], config.runner_default_retry_delay_ms)

  defp job_timeout_ms(definition, config) do
    target_timeout =
      positive_int(definition.target["timeout_ms"], config.runner_target_timeout_ms)

    target_timeout + config.request_timeout_ms * 2 + @job_timeout_margin_ms
  end

  defp safe_lease_ms(definition, config) do
    configured = positive_int(definition.runner["lease_ms"], config.runner_default_lease_ms)

    target_timeout =
      positive_int(definition.target["timeout_ms"], config.runner_target_timeout_ms)

    max(configured, target_timeout + config.request_timeout_ms * 2 + 1_000)
  end

  defp encode_result(definition, result) do
    result = if is_binary(result), do: result, else: Jason.encode!(result)

    if byte_size(result) <= Definition.max_result_bytes(definition),
      do: {:ok, result},
      else: {:error, :result_too_large}
  end

  defp encode_error(reason), do: reason |> sanitize_reason() |> Jason.encode!()

  defp sanitize_reason(%{} = reason) do
    reason
    |> Map.take(["code", "status"])
    |> Map.put_new("code", "target_error")
  end

  defp sanitize_reason(reason) when is_atom(reason), do: %{"code" => Atom.to_string(reason)}
  defp sanitize_reason(_reason), do: %{"code" => "target_error"}

  defp decode_json_field(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> value
    end
  end

  defp decode_json_field(value), do: value

  defp context(opts) do
    case Keyword.get(opts, :context) do
      %Auth.Context{} = context -> {:ok, context}
      nil -> SystemSession.context()
    end
  end

  defp backend_opts(config), do: [deadline: Deadline.new(config.request_timeout_ms)]

  defp maybe_put_partition(opts, nil), do: opts
  defp maybe_put_partition(opts, value), do: Keyword.put(opts, :partition_key, value)
  defp maybe_put_partition_keys(opts, values), do: Keyword.put(opts, :partition_keys, values)

  defp positive_int(value, _default) when is_integer(value) and value > 0, do: value

  defp positive_int(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> parsed
      _invalid -> default
    end
  end

  defp positive_int(_value, default), do: default

  defp accumulate_job_result({:completed, _id}, {:ok, stats}), do: {:ok, bump(stats, :completed)}
  defp accumulate_job_result({:retried, _id}, {:ok, stats}), do: {:ok, bump(stats, :retried)}
  defp accumulate_job_result({:failed, _id}, {:ok, stats}), do: {:ok, bump(stats, :failed)}

  defp accumulate_job_result(
         {:error, :reauthentication_required},
         {:ok, _stats}
       ),
       do: {:error, :reauthentication_required}

  defp accumulate_job_result({:error, _reason}, {:ok, stats}), do: {:ok, bump(stats, :errors)}
  defp accumulate_job_result(_result, {:error, _reason} = error), do: error

  defp run_definitions(definitions, context, config, opts) do
    Enum.reduce_while(definitions, {:ok, @empty_stats}, fn definition, {:ok, stats} ->
      case merge_stats_result(stats, run_definition(definition, context, config, opts)) do
        {:error, :reauthentication_required} = error -> {:halt, error}
        {:ok, _stats} = result -> {:cont, result}
      end
    end)
  end

  defp merge_stats_result(stats, {:ok, definition_stats}),
    do: {:ok, Map.merge(stats, definition_stats, fn _key, left, right -> left + right end)}

  defp merge_stats_result(_stats, {:error, :reauthentication_required}),
    do: {:error, :reauthentication_required}

  defp merge_stats_result(stats, {:error, _reason}), do: {:ok, bump(stats, :errors)}

  defp log_definition_stats(definition, {:ok, stats}) do
    if stats.claimed > 0 or stats.errors > 0 do
      Logger.info(
        "Invocation runner definition=#{definition.name} claimed=#{stats.claimed} " <>
          "completed=#{stats.completed} retried=#{stats.retried} failed=#{stats.failed} " <>
          "errors=#{stats.errors}"
      )
    end
  end

  defp empty_stats, do: %{@empty_stats | definitions: 1}
  defp bump(stats, key), do: Map.update!(stats, key, &(&1 + 1))

  defp schedule(config, after_ms) do
    Process.send_after(self(), :poll, after_ms)
    config
  end

  defp log_run_error({:ok, _stats}), do: :ok

  defp log_run_error({:error, reason}),
    do: Logger.error("Invocation runner failed: #{inspect(reason)}")

  defp now_ms, do: System.system_time(:millisecond)
end
