defmodule FerricStore.ResourceLimits do
  @moduledoc """
  Stable resource-limit contract for FerricStore control planes and write paths.

  The public management commands use `set_limit/3`, `get_limit/2`, and
  `usage/2`. Internal enforcement points can use `check/4`, `reserve/4`, and
  `release/2` without coupling storage code to any specific limit backend.
  """

  @type scope :: binary() | map()
  @type resource :: atom() | binary()
  @type amount :: non_neg_integer()
  @type limit_spec :: map()
  @type reservation :: term()
  @type result :: :ok | {:ok, term()} | {:error, term()}

  @callback set_limit(scope(), limit_spec(), keyword()) :: result()
  @callback get_limit(scope(), keyword()) :: result()
  @callback usage(scope(), keyword()) :: result()
  @callback check(scope(), resource(), amount(), keyword()) :: :ok | {:error, term()}
  @callback reserve(scope(), resource(), amount(), keyword()) ::
              {:ok, reservation()} | {:error, term()}
  @callback release(reservation(), keyword()) :: :ok | {:error, term()}

  @spec set_limit(scope(), limit_spec(), keyword()) :: result()
  def set_limit(scope, limit_spec, opts \\ []),
    do: implementation(opts).set_limit(scope, limit_spec, opts)

  @spec get_limit(scope(), keyword()) :: result()
  def get_limit(scope, opts \\ []), do: implementation(opts).get_limit(scope, opts)

  @spec usage(scope(), keyword()) :: result()
  def usage(scope, opts \\ []), do: implementation(opts).usage(scope, opts)

  @spec check(scope(), resource(), amount(), keyword()) :: :ok | {:error, term()}
  def check(scope, resource, amount, opts \\ []),
    do: implementation(opts).check(scope, resource, amount, opts)

  @spec reserve(scope(), resource(), amount(), keyword()) ::
          {:ok, reservation()} | {:error, term()}
  def reserve(scope, resource, amount, opts \\ []),
    do: implementation(opts).reserve(scope, resource, amount, opts)

  @spec release(reservation(), keyword()) :: :ok | {:error, term()}
  def release(reservation, opts \\ []), do: implementation(opts).release(reservation, opts)

  @doc false
  @spec implementation(keyword()) :: module()
  def implementation(opts \\ []) when is_list(opts) do
    Keyword.get(opts, :impl) ||
      Application.get_env(:ferricstore, __MODULE__, FerricStore.ResourceLimits.Default)
  end
end

defmodule FerricStore.ResourceLimits.Default do
  @moduledoc false

  @behaviour FerricStore.ResourceLimits

  @impl true
  def set_limit(_scope, _limit_spec, _opts), do: {:error, :unsupported}

  @impl true
  def get_limit(_scope, _opts), do: {:error, :unsupported}

  @impl true
  def usage(scope, _opts) do
    {:ok,
     %{
       scope: scope,
       usage: %{
         keys: nil,
         bytes: nil,
         ops_per_sec: nil,
         flow_count: nil
       }
     }}
  end

  @impl true
  def check(_scope, _resource, _amount, _opts), do: :ok

  @impl true
  def reserve(_scope, _resource, _amount, _opts), do: {:ok, nil}

  @impl true
  def release(_reservation, _opts), do: :ok
end
