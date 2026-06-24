defmodule FerricStore.Management.Telemetry do
  @moduledoc """
  Stable safe-read telemetry contract for control-plane clients.

  This boundary returns metadata and attributes only. Payload search is not part
  of the default contract.
  """

  @type result :: {:ok, term()} | {:error, term()}

  @callback cluster_info(keyword()) :: result()
  @callback namespace_usage(binary(), keyword()) :: result()
  @callback flow_query(map(), keyword()) :: result()
  @callback flow_history(binary(), keyword()) :: result()

  @spec cluster_info(keyword()) :: result()
  def cluster_info(opts \\ []), do: implementation(opts).cluster_info(opts)

  @spec namespace_usage(binary(), keyword()) :: result()
  def namespace_usage(prefix, opts \\ []), do: implementation(opts).namespace_usage(prefix, opts)

  @spec flow_query(map(), keyword()) :: result()
  def flow_query(attrs, opts \\ []), do: implementation(opts).flow_query(attrs, opts)

  @spec flow_history(binary(), keyword()) :: result()
  def flow_history(id, opts \\ []), do: implementation(opts).flow_history(id, opts)

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.Management.Telemetry.Default)
  end
end

defmodule FerricStore.Management.Telemetry.Default do
  @moduledoc false

  @behaviour FerricStore.Management.Telemetry

  @impl true
  def cluster_info(_opts) do
    {:ok,
     %{
       capabilities: FerricStore.ManagementCapabilities.capabilities(),
       health: FerricStore.health()
     }}
  end

  @impl true
  def namespace_usage(prefix, _opts) do
    {:ok,
     %{
       prefix: prefix,
       keys: nil,
       bytes: nil,
       ops_per_sec: nil,
       flow_count: nil
     }}
  end

  @impl true
  def flow_query(_attrs, _opts), do: {:ok, []}

  @impl true
  def flow_history(_id, _opts), do: {:ok, []}
end
