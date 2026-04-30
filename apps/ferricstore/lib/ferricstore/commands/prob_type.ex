defmodule Ferricstore.Commands.ProbType do
  @moduledoc false

  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @wrongtype "WRONGTYPE Operation against a key holding the wrong kind of value"

  @spec check_expected(binary(), atom(), map()) :: :ok | {:error, binary()}
  def check_expected(key, expected, store) do
    case stored_type(key, store) do
      nil -> :ok
      ^expected -> :ok
      _other -> {:error, @wrongtype}
    end
  end

  @spec check_create(binary(), atom(), map()) :: :ok | {:error, :exists | binary()}
  def check_create(key, expected, store) do
    case stored_type(key, store) do
      nil -> :ok
      ^expected -> {:error, :exists}
      _other -> {:error, @wrongtype}
    end
  end

  @spec register(map(), binary(), {atom(), map()}) :: :ok
  def register(%FerricStore.Instance{}, _key, _meta), do: :ok
  def register(%{prob_write: write_fn}, _key, _meta) when is_function(write_fn), do: :ok

  def register(store, key, meta) when is_map(store) do
    if Map.has_key?(store, :put) do
      Ops.put(store, key, :erlang.term_to_binary(meta), 0)
    end

    :ok
  end

  def register(_store, _key, _meta), do: :ok

  defp stored_type(key, store) do
    case raw_value_type(key, store) do
      nil -> registry_type(key, store)
      type -> type
    end
  end

  defp raw_value_type(key, store) do
    cond do
      has_get?(store) ->
        store
        |> Ops.get(key)
        |> decode_raw_type()

      true ->
        nil
    end
  rescue
    _ -> nil
  end

  defp decode_raw_type(nil), do: nil

  defp decode_raw_type(value) when is_binary(value) do
    try do
      value
      |> :erlang.binary_to_term()
      |> decode_term_type()
    rescue
      _ -> :string
    end
  end

  defp decode_raw_type(value), do: decode_term_type(value)

  defp decode_term_type({:bloom_meta, _}), do: :bloom
  defp decode_term_type({:cms_meta, _}), do: :cms
  defp decode_term_type({:cuckoo_meta, _}), do: :cuckoo
  defp decode_term_type({:topk_meta, _}), do: :topk
  defp decode_term_type({:topk_path, _}), do: :topk
  defp decode_term_type(nil), do: nil
  defp decode_term_type(_), do: :other

  defp registry_type(key, store) do
    if has_type_registry?(store) do
      case TypeRegistry.get_type(key, store) do
        "none" -> nil
        _type -> :other
      end
    end
  rescue
    _ -> nil
  end

  defp has_get?(%FerricStore.Instance{}), do: true
  defp has_get?(%Ferricstore.Store.LocalTxStore{}), do: true
  defp has_get?(store) when is_map(store), do: Map.has_key?(store, :get)
  defp has_get?(_store), do: false

  defp has_type_registry?(%FerricStore.Instance{}), do: true
  defp has_type_registry?(%Ferricstore.Store.LocalTxStore{}), do: true

  defp has_type_registry?(store) when is_map(store) do
    Map.has_key?(store, :compound_get) and Map.has_key?(store, :get)
  end

  defp has_type_registry?(_store), do: false
end
