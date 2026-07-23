defmodule FerricStore.Flow.QueryEngine do
  @moduledoc """
  Installable execution boundary for canonical Flow queries.

  OSS uses the bounded cost-aware planner. Extensions may inject trusted
  mandatory scope, authorization, budgets, and index metadata through the
  existing instance behaviours after parsing and parameter binding.
  """

  alias Ferricstore.Flow.Query.{Error, ExecutionContext, Request, Surface}

  @type context :: FerricStore.Instance.t() | ExecutionContext.t() | map()
  @type result :: {:ok, term()} | {:error, term()}
  @type capability_manifest :: %{
          request_contract: binary() | nil,
          result_contract: binary() | nil,
          explain_contract: binary() | nil,
          capabilities: [binary()],
          language_versions: [binary()],
          shapes: [binary()]
        }

  @callback execute(context(), Request.t()) :: result()
  @callback capabilities() :: capability_manifest()
  @optional_callbacks capabilities: 0

  @default_implementation FerricStore.Flow.QueryEngine.Default
  @capability_manifest_keys [
    :request_contract,
    :result_contract,
    :explain_contract,
    :capabilities,
    :language_versions,
    :shapes
  ]
  @unavailable_capabilities %{
    request_contract: nil,
    result_contract: nil,
    explain_contract: nil,
    capabilities: [],
    language_versions: [],
    shapes: []
  }

  @spec execute(context(), Request.t(), keyword()) :: result()
  def execute(ctx, %Request{} = request, opts \\ []) when is_list(opts) do
    implementation = implementation(ctx, opts)

    try do
      implementation
      |> apply(:execute, [ctx, request])
      |> normalize_result()
    rescue
      _error -> {:error, :query_engine_failure}
    catch
      _kind, _reason -> {:error, :query_engine_failure}
    end
  end

  @doc false
  @spec implementation(context(), keyword()) :: module()
  def implementation(ctx, opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) || context_implementation(ctx)
  end

  @doc false
  @spec configured_implementation(keyword()) :: module()
  def configured_implementation(opts) when is_list(opts) do
    opts
    |> Keyword.get(
      :query_engine,
      Application.get_env(:ferricstore, __MODULE__, @default_implementation)
    )
    |> validate_implementation!()
  end

  @spec capabilities(context()) :: capability_manifest()
  def capabilities(%{query_capabilities: capabilities}),
    do: validate_capability_manifest!(capabilities)

  def capabilities(ctx) do
    ctx
    |> implementation()
    |> capabilities_for()
  end

  @doc """
  Returns the immutable FerricStore instance carried by an execution context.

  Query-engine implementations should use this accessor instead of depending
  on the protocol layer's context wrapper.
  """
  @spec instance_context(context()) :: FerricStore.Instance.t() | map()
  def instance_context(ctx), do: ExecutionContext.instance_ctx(ctx)

  @doc """
  Returns trusted request authority carried by the execution context.

  Embedded calls and native requests without trusted authority return an empty
  map.
  """
  @spec request_context(context()) :: map()
  def request_context(%ExecutionContext{request_context: request_context}), do: request_context
  def request_context(%{request_context: %{} = request_context}), do: request_context
  def request_context(%{"request_context" => %{} = request_context}), do: request_context
  def request_context(_ctx), do: %{}

  @doc """
  Returns the absolute Unix-millisecond request deadline, when one was supplied.

  Engines should convert this value to a monotonic deadline once at query
  initialization and use that value for subsequent budget checks.
  """
  @spec deadline_ms(context()) :: pos_integer() | nil
  def deadline_ms(%ExecutionContext{deadline_ms: deadline_ms}), do: deadline_ms

  def deadline_ms(%{deadline_ms: deadline_ms})
      when is_integer(deadline_ms) and deadline_ms > 0,
      do: deadline_ms

  def deadline_ms(_ctx), do: nil

  @doc false
  @spec capabilities_for(module()) :: capability_manifest()
  def capabilities_for(implementation) when is_atom(implementation) do
    manifest =
      if Code.ensure_loaded?(implementation) and
           function_exported?(implementation, :capabilities, 0) do
        implementation.capabilities()
      else
        @unavailable_capabilities
      end

    validate_capability_manifest!(manifest)
  rescue
    _error ->
      raise ArgumentError, "query capability manifest is invalid"
  catch
    _kind, _reason ->
      raise ArgumentError, "query capability manifest is invalid"
  end

  @doc false
  @spec validate_implementation!(term()) :: module()
  def validate_implementation!(implementation) when is_atom(implementation) do
    if Code.ensure_loaded?(implementation) and function_exported?(implementation, :execute, 2) do
      implementation
    else
      raise ArgumentError,
            "query_engine must be a loaded module exporting execute/2, got: #{inspect(implementation)}"
    end
  end

  def validate_implementation!(implementation) do
    raise ArgumentError,
          "query_engine must be a loaded module exporting execute/2, got: #{inspect(implementation)}"
  end

  defp validate_capability_manifest!(%{} = manifest) do
    request_contract = Map.get(manifest, :request_contract)
    result_contract = Map.get(manifest, :result_contract)
    explain_contract = Map.get(manifest, :explain_contract)
    capabilities = Map.get(manifest, :capabilities)
    language_versions = Map.get(manifest, :language_versions)
    shapes = Map.get(manifest, :shapes)

    if Map.keys(manifest) |> Enum.sort() == Enum.sort(@capability_manifest_keys) and
         valid_request_contract?(request_contract) and
         valid_optional_contract?(result_contract) and
         valid_optional_contract?(explain_contract) and
         valid_string_list?(capabilities) and
         valid_string_list?(language_versions) and
         valid_string_list?(shapes) and
         Surface.supported_language_versions?(language_versions) and
         Surface.supported_shapes?(shapes) and
         coherent_capability_surface?(
           request_contract,
           result_contract,
           explain_contract,
           capabilities,
           language_versions,
           shapes
         ) do
      %{
        request_contract: request_contract,
        result_contract: result_contract,
        explain_contract: explain_contract,
        capabilities: capabilities,
        language_versions: language_versions,
        shapes: shapes
      }
    else
      raise ArgumentError, "query capability manifest is invalid"
    end
  end

  defp validate_capability_manifest!(_manifest) do
    raise ArgumentError, "query capability manifest is invalid"
  end

  defp valid_optional_contract?(nil), do: true
  defp valid_optional_contract?(value), do: valid_manifest_string?(value)

  defp valid_request_contract?(nil), do: true
  defp valid_request_contract?(value), do: value == Surface.request_contract()

  defp valid_string_list?(values) when is_list(values) and length(values) <= 32 do
    Enum.all?(values, &valid_manifest_string?/1) and length(Enum.uniq(values)) == length(values)
  end

  defp valid_string_list?(_values), do: false

  defp valid_manifest_string?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= 128

  defp coherent_capability_surface?(nil, nil, nil, [], [], []), do: true

  defp coherent_capability_surface?(
         request_contract,
         result_contract,
         explain_contract,
         capabilities,
         language_versions,
         shapes
       ) do
    is_binary(request_contract) and is_binary(result_contract) and
      (is_nil(explain_contract) or is_binary(explain_contract)) and
      capabilities != [] and language_versions != [] and shapes != []
  end

  defp context_implementation(%{query_engine: implementation}) when is_atom(implementation),
    do: implementation

  defp context_implementation(%ExecutionContext{instance_ctx: instance_ctx}),
    do: context_implementation(instance_ctx)

  defp context_implementation(_ctx), do: @default_implementation

  defp normalize_result({:ok, _value} = result), do: result

  defp normalize_result({:error, %Error{} = error} = result) do
    if Error.valid?(error), do: result, else: {:error, :query_engine_failure}
  end

  defp normalize_result({:error, reason} = result) when is_atom(reason) do
    if Error.known?(reason), do: result, else: {:error, :query_engine_failure}
  end

  defp normalize_result(_invalid), do: {:error, :query_engine_failure}
end

defmodule FerricStore.Flow.QueryEngine.Default do
  @moduledoc false

  @behaviour FerricStore.Flow.QueryEngine

  @impl true
  def execute(ctx, request), do: Ferricstore.Flow.Query.PlannerEngine.execute(ctx, request)

  @impl true
  def capabilities, do: Ferricstore.Flow.Query.Surface.default_capability_manifest()
end
