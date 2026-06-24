defmodule FerricStore.Management.ACL do
  @moduledoc """
  Stable ACL management contract for FerricStore control planes.

  The default implementation is intentionally unsupported. Deployments can
  provide a configured implementation through application config.
  """

  @type username :: binary()
  @type acl_rule :: binary()
  @type result :: :ok | {:ok, term()} | {:error, term()}

  @callback set_user(username(), [acl_rule()], keyword()) :: result()
  @callback del_user(username(), keyword()) :: result()
  @callback get_user(username(), keyword()) :: result()
  @callback list_users(keyword()) :: result()
  @callback save(keyword()) :: result()

  @spec set_user(username(), [acl_rule()], keyword()) :: result()
  def set_user(username, rules, opts \\ []),
    do: implementation(opts).set_user(username, rules, opts)

  @spec del_user(username(), keyword()) :: result()
  def del_user(username, opts \\ []), do: implementation(opts).del_user(username, opts)

  @spec get_user(username(), keyword()) :: result()
  def get_user(username, opts \\ []), do: implementation(opts).get_user(username, opts)

  @spec list_users(keyword()) :: result()
  def list_users(opts \\ []), do: implementation(opts).list_users(opts)

  @spec save(keyword()) :: result()
  def save(opts \\ []), do: implementation(opts).save(opts)

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.Management.ACL.Unsupported)
  end
end

defmodule FerricStore.Management.ACL.Unsupported do
  @moduledoc false

  @behaviour FerricStore.Management.ACL

  @impl true
  def set_user(_username, _rules, _opts), do: {:error, :unsupported}

  @impl true
  def del_user(_username, _opts), do: {:error, :unsupported}

  @impl true
  def get_user(_username, _opts), do: {:error, :unsupported}

  @impl true
  def list_users(_opts), do: {:error, :unsupported}

  @impl true
  def save(_opts), do: {:error, :unsupported}
end
