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
  @callback del_users([username()], keyword()) :: result()
  @callback get_user(username(), keyword()) :: result()
  @callback list_users(keyword()) :: result()
  @callback save(keyword()) :: result()
  @callback load(keyword()) :: result()
  @callback categories(keyword()) :: result()
  @callback category_commands(binary(), keyword()) :: result()
  @callback log(pos_integer() | :all | :reset, keyword()) :: result()
  @callback dryrun(username(), binary(), [binary()], keyword()) :: result()
  @optional_callbacks categories: 1, category_commands: 2, log: 2, dryrun: 4

  @spec set_user(username(), [acl_rule()], keyword()) :: result()
  def set_user(username, rules, opts \\ []),
    do: implementation(opts).set_user(username, rules, opts)

  @spec del_user(username(), keyword()) :: result()
  def del_user(username, opts \\ []), do: implementation(opts).del_user(username, opts)

  @spec del_users([username()], keyword()) :: result()
  def del_users(usernames, opts \\ []), do: implementation(opts).del_users(usernames, opts)

  @spec get_user(username(), keyword()) :: result()
  def get_user(username, opts \\ []), do: implementation(opts).get_user(username, opts)

  @spec list_users(keyword()) :: result()
  def list_users(opts \\ []), do: implementation(opts).list_users(opts)

  @spec save(keyword()) :: result()
  def save(opts \\ []), do: implementation(opts).save(opts)

  @spec load(keyword()) :: result()
  def load(opts \\ []), do: implementation(opts).load(opts)

  @doc "ACL CAT without a category: the category names."
  @spec categories(keyword()) :: result()
  def categories(opts \\ []), do: optional(opts, :categories, [opts])

  @doc "ACL CAT category: the commands in it."
  @spec category_commands(binary(), keyword()) :: result()
  def category_commands(category, opts \\ []),
    do: optional(opts, :category_commands, [category, opts])

  @doc "ACL LOG: the latest `count` (or all) denials, or `:reset` to clear them."
  @spec log(pos_integer() | :all | :reset, keyword()) :: result()
  def log(count, opts \\ []), do: optional(opts, :log, [count, opts])

  @doc "ACL DRYRUN: whether `username` may run `command` with `args`."
  @spec dryrun(username(), binary(), [binary()], keyword()) :: result()
  def dryrun(username, command, args, opts \\ []),
    do: optional(opts, :dryrun, [username, command, args, opts])

  defp optional(opts, function, args) do
    module = implementation(opts)
    Code.ensure_loaded(module)

    if function_exported?(module, function, length(args)),
      do: apply(module, function, args),
      else: {:error, :unsupported}
  end

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
  def del_users(_usernames, _opts), do: {:error, :unsupported}

  @impl true
  def get_user(_username, _opts), do: {:error, :unsupported}

  @impl true
  def list_users(_opts), do: {:error, :unsupported}

  @impl true
  def save(_opts), do: {:error, :unsupported}

  @impl true
  def load(_opts), do: {:error, :unsupported}
end
