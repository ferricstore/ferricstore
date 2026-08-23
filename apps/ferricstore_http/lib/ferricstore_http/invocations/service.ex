defmodule FerricstoreHttp.Invocations.Service do
  @moduledoc "Asynchronous invocation operations backed by Flow records."

  alias FerricstoreHttp.{Auth, Config}
  alias FerricstoreHttp.Invocations.Backend

  @terminal_states ~w(completed failed cancelled)

  @spec create(binary(), map(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def create(name, attrs, %Auth.Context{} = context, %Config{} = config, opts \\ [])
      when is_map(attrs) do
    Backend.invocation_create(context, name, attrs, config,
      deadline: Keyword.get(opts, :deadline),
      idempotency_key: idempotency_key(attrs, opts)
    )
  end

  @spec metadata(binary(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def metadata(id, context, config, opts \\ []) do
    case Backend.invocation_get(context, id, config, opts) do
      {:ok, nil} -> {:error, :invocation_not_found}
      {:ok, %{} = invocation} -> {:ok, invocation_metadata(invocation)}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_invocation_metadata}
    end
  end

  @spec get(binary(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def get(id, context, config, opts \\ []) do
    with {:ok, %{record: record}} <- scope(id, context, config, opts) do
      {:ok, record}
    end
  end

  @spec scope(binary(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, %{metadata: map(), record: map()}} | {:error, term()}
  def scope(id, context, config, opts \\ []) do
    with {:ok, metadata} <- metadata(id, context, config, opts) do
      load_scope_record(id, metadata, context, config, opts)
    end
  end

  defp load_scope_record(id, metadata, context, config, opts) do
    flow_opts =
      [payload: true, values: true, deadline: Keyword.get(opts, :deadline)]
      |> put_if_present(:partition_key, metadata["partition_key"])

    case Backend.flow_get(context, id, config, flow_opts) do
      {:ok, nil} -> {:error, :invocation_not_found}
      {:ok, %{} = record} -> {:ok, %{metadata: metadata, record: normalize_record(record)}}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_invocation_record}
    end
  end

  @spec result(binary(), Auth.Context.t(), Config.t(), keyword()) ::
          {:ok, map()} | {:pending, map()} | {:error, term()}
  def result(id, context, config, opts \\ []) do
    with {:ok, record} <- get(id, context, config, opts) do
      if terminal?(record) do
        read_result(record, context, config, opts)
      else
        {:pending, %{"invocation_id" => id, "state" => record["state"]}}
      end
    end
  end

  defp read_result(%{"result" => result} = record, _context, _config, _opts) do
    {:ok,
     %{
       "invocation_id" => record["id"],
       "state" => record["state"],
       "result" => result
     }}
  end

  defp read_result(%{"result_ref" => ref} = record, context, config, opts)
       when is_binary(ref) do
    case Backend.flow_value_mget(context, [ref], config, opts) do
      {:ok, [nil]} ->
        {:error, :value_not_found}

      {:ok, [result]} ->
        {:ok,
         %{
           "invocation_id" => record["id"],
           "state" => record["state"],
           "result" => decode_json_value(result)
         }}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_invocation_result}
    end
  end

  defp read_result(%{"error" => error} = record, _context, _config, _opts) do
    {:ok, %{"invocation_id" => record["id"], "state" => record["state"], "error" => error}}
  end

  defp read_result(record, _context, _config, _opts),
    do: {:ok, %{"invocation_id" => record["id"], "state" => record["state"]}}

  defp idempotency_key(attrs, opts) do
    normalize_idempotency_key(attrs["idempotency_key"]) ||
      normalize_idempotency_key(Keyword.get(opts, :idempotency_key))
  end

  defp normalize_idempotency_key(value) when value in [nil, ""], do: nil
  defp normalize_idempotency_key(value) when is_binary(value), do: value
  defp normalize_idempotency_key(_value), do: nil

  defp invocation_metadata(invocation) do
    index = Map.get(invocation, "index", %{})

    index
    |> Map.merge(Map.take(invocation, ~w(name flow_type partition_key subject value_policy)))
    |> Map.put_new("id", invocation["id"] || invocation["invocation_id"])
  end

  defp normalize_record(record) do
    record
    |> parse_json_field("payload")
    |> parse_json_field("result")
    |> parse_json_field("error")
  end

  defp parse_json_field(record, field) do
    case record[field] do
      value when is_binary(value) ->
        case Jason.decode(value) do
          {:ok, decoded} -> Map.put(record, field, decoded)
          {:error, _reason} -> record
        end

      _other ->
        record
    end
  end

  defp decode_json_value(value) when is_binary(value) do
    case Jason.decode(value) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> value
    end
  end

  defp decode_json_value(value), do: value

  defp terminal?(record),
    do: record["state"] in @terminal_states or record["run_state"] in @terminal_states

  defp put_if_present(opts, _key, nil), do: opts
  defp put_if_present(opts, key, value), do: Keyword.put(opts, key, value)
end
