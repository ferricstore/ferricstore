defmodule FerricStore.ManagementCapabilities do
  @moduledoc """
  Public management capability contract.

  Control-plane clients should use `FERRICSTORE.CAPABILITIES` over the SDK/native
  protocol. Deployments can override this module through application config.
  """

  @type capabilities :: %{
          required(:sdk) => boolean(),
          required(:health) => boolean(),
          required(:telemetry) => boolean(),
          required(:acl_management) => boolean(),
          required(:namespace_management) => boolean(),
          required(:quota_management) => boolean(),
          required(:flow_observability) => boolean(),
          optional(atom() | binary()) => term()
        }

  @callback capabilities(keyword()) :: capabilities()

  @default %{
    sdk: true,
    health: true,
    telemetry: true,
    acl_management: false,
    namespace_management: false,
    quota_management: false,
    flow_observability: true
  }

  @required_flags Map.keys(@default)

  @doc "Returns the OSS baseline capability map."
  @spec default() :: capabilities()
  def default, do: @default

  @doc "Returns the configured server management capabilities."
  @spec capabilities(keyword()) :: capabilities()
  def capabilities(opts \\ []) when is_list(opts) do
    implementation = opts |> implementation() |> validate_implementation!()

    implementation
    |> apply(:capabilities, [opts])
    |> normalize_capabilities(implementation)
  end

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.ManagementCapabilities.Default)
  end

  defp validate_implementation!(implementation) when is_atom(implementation) do
    if Code.ensure_loaded?(implementation) and
         function_exported?(implementation, :capabilities, 1) do
      implementation
    else
      raise ArgumentError,
            "management capabilities implementation #{inspect(implementation)} must export capabilities/1"
    end
  end

  defp validate_implementation!(implementation) do
    raise ArgumentError,
          "management capabilities implementation #{inspect(implementation)} must export capabilities/1"
  end

  defp normalize_capabilities(capabilities, _implementation) when is_map(capabilities) do
    normalized = Map.merge(@default, capabilities)

    case Enum.find(@required_flags, &(not is_boolean(Map.fetch!(normalized, &1)))) do
      nil ->
        normalized

      capability ->
        raise ArgumentError,
              "management capability #{inspect(capability)} must be a boolean, got: " <>
                inspect(Map.fetch!(normalized, capability))
    end
  end

  defp normalize_capabilities(capabilities, implementation) do
    raise ArgumentError,
          "management capabilities implementation #{inspect(implementation)} must return a map, got: " <>
            inspect(capabilities)
  end
end

defmodule FerricStore.ManagementCapabilities.Default do
  @moduledoc false

  @behaviour FerricStore.ManagementCapabilities

  @impl true
  def capabilities(_opts \\ []), do: FerricStore.ManagementCapabilities.default()
end
