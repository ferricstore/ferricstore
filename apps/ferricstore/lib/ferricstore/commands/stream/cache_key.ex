defmodule Ferricstore.Commands.Stream.CacheKey do
  @moduledoc false

  alias Ferricstore.Store.LocalTxStore

  @type scope :: term()
  @type t :: binary() | {scope(), binary()}

  @spec build(term(), binary()) :: t()
  def build(%FerricStore.Instance{name: name}, key) when is_binary(key), do: {name, key}

  def build(%LocalTxStore{instance_ctx: instance_ctx}, key), do: build(instance_ctx, key)

  def build(%{instance_ctx: %FerricStore.Instance{} = instance_ctx}, key),
    do: build(instance_ctx, key)

  def build(%{cache_scope: scope}, key) when is_binary(key), do: {scope, key}
  def build(_store, key) when is_binary(key), do: key

  @spec raw(term()) :: binary() | nil
  def raw({_scope, key}) when is_binary(key), do: key
  def raw(key) when is_binary(key), do: key
  def raw(_invalid), do: nil

  @spec scope(term()) :: {:ok, scope()} | :unscoped
  def scope(%FerricStore.Instance{name: name}), do: {:ok, name}
  def scope(%LocalTxStore{instance_ctx: instance_ctx}), do: scope(instance_ctx)

  def scope(%{instance_ctx: %FerricStore.Instance{} = instance_ctx}),
    do: scope(instance_ctx)

  def scope(%{cache_scope: scope}), do: {:ok, scope}
  def scope(_store), do: :unscoped

  @spec in_scope?(t(), scope()) :: boolean()
  def in_scope?({scope, key}, scope) when is_binary(key), do: true
  def in_scope?(_cache_key, _scope), do: false
end
