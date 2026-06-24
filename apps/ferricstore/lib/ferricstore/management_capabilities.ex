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

  @doc "Returns the OSS baseline capability map."
  @spec default() :: capabilities()
  def default, do: @default

  @doc "Returns the configured server management capabilities."
  @spec capabilities(keyword()) :: capabilities()
  def capabilities(opts \\ []) when is_list(opts) do
    opts
    |> implementation()
    |> apply(:capabilities, [opts])
    |> normalize_capabilities()
  end

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.ManagementCapabilities.Default)
  end

  defp normalize_capabilities(capabilities) when is_map(capabilities) do
    Map.merge(@default, capabilities)
  end
end

defmodule FerricStore.ManagementCapabilities.Default do
  @moduledoc false

  @behaviour FerricStore.ManagementCapabilities

  @impl true
  def capabilities(_opts \\ []), do: FerricStore.ManagementCapabilities.default()
end
