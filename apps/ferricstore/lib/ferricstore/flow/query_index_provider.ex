defmodule FerricStore.Flow.QueryIndexProvider do
  @moduledoc """
  Immutable instance boundary for the versioned composite-index registry.

  Providers publish one bounded snapshot per LMDB writer expansion batch.
  Building and validating indexes remain in the projection set for online
  migration. Retiring indexes leave the projection set before their writer
  fence and physical cleanup, while only active indexes are visible to queries.
  """

  alias Ferricstore.Flow.Query.{RegisteredIndex, RegistrySnapshot}

  @default_implementation FerricStore.Flow.QueryIndexProvider.Disabled

  @callback snapshot(FerricStore.Instance.t() | map(), non_neg_integer()) ::
              {:ok, RegistrySnapshot.t()} | {:error, term()}
  @callback child_specs(FerricStore.Instance.t() | map()) :: [Supervisor.child_spec()]
  @optional_callbacks child_specs: 1

  @spec configured_implementation(keyword()) :: module()
  def configured_implementation(opts) when is_list(opts) do
    opts
    |> Keyword.get(
      :query_index_provider,
      Application.get_env(:ferricstore, __MODULE__, @default_implementation)
    )
    |> validate_implementation!()
  end

  @spec validate_implementation!(term()) :: module()
  def validate_implementation!(implementation) when is_atom(implementation) do
    if Code.ensure_loaded?(implementation) and function_exported?(implementation, :snapshot, 2) do
      implementation
    else
      raise ArgumentError,
            "query_index_provider must be a loaded module exporting snapshot/2, got: #{inspect(implementation)}"
    end
  end

  def validate_implementation!(implementation) do
    raise ArgumentError,
          "query_index_provider must be a loaded module exporting snapshot/2, got: #{inspect(implementation)}"
  end

  @spec snapshot(FerricStore.Instance.t() | map(), non_neg_integer()) ::
          {:ok, RegistrySnapshot.t()} | {:error, atom()}
  def snapshot(ctx, shard_index) when is_integer(shard_index) and shard_index >= 0 do
    implementation = implementation(ctx)

    try do
      case implementation.snapshot(ctx, shard_index) do
        {:ok, %RegistrySnapshot{} = snapshot} ->
          case RegistrySnapshot.validate(snapshot) do
            :ok -> {:ok, snapshot}
            {:error, _reason} = error -> error
          end

        {:error, _reason} ->
          {:error, :query_index_provider_failure}

        _invalid ->
          {:error, :invalid_query_index_snapshot}
      end
    rescue
      _error -> {:error, :query_index_provider_failure}
    catch
      _kind, _reason -> {:error, :query_index_provider_failure}
    end
  end

  def snapshot(_ctx, _shard_index), do: {:error, :invalid_query_index_snapshot}

  @spec projection_definitions(FerricStore.Instance.t() | map(), non_neg_integer()) ::
          {:ok, [Ferricstore.Flow.Query.IndexDefinition.t()]} | {:error, atom()}
  def projection_definitions(ctx, shard_index) do
    with {:ok, snapshot} <- snapshot(ctx, shard_index) do
      definitions =
        snapshot.indexes
        |> Enum.filter(&RegisteredIndex.projection?/1)
        |> Enum.map(& &1.definition)

      {:ok, definitions}
    end
  end

  @spec active_indexes(FerricStore.Instance.t() | map(), non_neg_integer()) ::
          {:ok, [RegisteredIndex.t()]} | {:error, atom()}
  def active_indexes(ctx, shard_index) do
    with {:ok, snapshot} <- snapshot(ctx, shard_index) do
      {:ok, Enum.filter(snapshot.indexes, &RegisteredIndex.queryable?/1)}
    end
  end

  @spec child_specs(FerricStore.Instance.t() | map()) ::
          {:ok, [Supervisor.child_spec()]} | {:error, :query_index_provider_failure}
  def child_specs(ctx) do
    implementation = implementation(ctx)

    try do
      specs =
        if function_exported?(implementation, :child_specs, 1),
          do: implementation.child_specs(ctx),
          else: []

      if is_list(specs) and length(specs) <= 16,
        do: {:ok, Enum.map(specs, &Supervisor.child_spec(&1, []))},
        else: {:error, :query_index_provider_failure}
    rescue
      _error -> {:error, :query_index_provider_failure}
    catch
      _kind, _reason -> {:error, :query_index_provider_failure}
    end
  end

  @spec enabled?(FerricStore.Instance.t() | map()) :: boolean()
  def enabled?(ctx), do: implementation(ctx) != @default_implementation

  defp implementation(%{query_index_provider: implementation}) when is_atom(implementation),
    do: implementation

  defp implementation(_ctx), do: @default_implementation
end

defmodule FerricStore.Flow.QueryIndexProvider.Disabled do
  @moduledoc false

  @behaviour FerricStore.Flow.QueryIndexProvider

  @impl true
  def snapshot(_ctx, _shard_index),
    do: {:ok, Ferricstore.Flow.Query.RegistrySnapshot.empty()}
end
