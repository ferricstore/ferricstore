defmodule FerricStore.Management.Namespace do
  @moduledoc """
  Stable namespace metadata contract for FerricStore control planes.

  Deployments can use this boundary for tenant limits, isolation policy,
  durability policy, and other scoped metadata.
  """

  @type prefix :: binary()
  @type result :: :ok | {:ok, term()} | {:error, term()}

  @callback ensure_namespace(prefix(), keyword()) :: result()
  @callback get_namespace(prefix()) :: result()
  @callback list_namespaces() :: result()
  @callback delete_namespace(prefix(), keyword()) :: result()

  @spec ensure_namespace(prefix(), keyword()) :: result()
  def ensure_namespace(prefix, opts \\ []),
    do: implementation(opts).ensure_namespace(prefix, opts)

  @spec get_namespace(prefix()) :: result()
  def get_namespace(prefix), do: implementation([]).get_namespace(prefix)

  @spec list_namespaces() :: result()
  def list_namespaces, do: implementation([]).list_namespaces()

  @spec delete_namespace(prefix(), keyword()) :: result()
  def delete_namespace(prefix, opts \\ []),
    do: implementation(opts).delete_namespace(prefix, opts)

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.Management.Namespace.Unsupported)
  end
end

defmodule FerricStore.Management.Namespace.Unsupported do
  @moduledoc false

  @behaviour FerricStore.Management.Namespace

  @impl true
  def ensure_namespace(_prefix, _opts), do: {:error, :unsupported}

  @impl true
  def get_namespace(_prefix), do: {:error, :unsupported}

  @impl true
  def list_namespaces, do: {:error, :unsupported}

  @impl true
  def delete_namespace(_prefix, _opts), do: {:error, :unsupported}
end
