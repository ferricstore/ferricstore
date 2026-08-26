defmodule Ferricstore.Store.StringRead do
  @moduledoc false

  alias Ferricstore.Store.{CompoundKey, ReadResult, Router}

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  @spec batch_results(FerricStore.Instance.t(), [binary()], [term()]) :: [
          {:ok, term()} | {:error, binary()}
        ]
  def batch_results(ctx, keys, values) do
    keys
    |> Enum.zip(values)
    |> Enum.map(fn {key, value} ->
      result_with_lookup(key, value, fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end)
    end)
  end

  @doc false
  @spec result_with_lookup(binary(), term(), (binary(), binary() -> term())) ::
          {:ok, term()} | {:error, binary()}
  def result_with_lookup(
        _key,
        {:error, {:storage_read_failed, _reason}} = failure,
        _compound_get
      ),
      do: ReadResult.command_error(failure)

  def result_with_lookup(key, <<131, _rest::binary>> = value, compound_get) do
    case compound_data_structure_status(key, compound_get) do
      :compound -> @wrongtype_error
      :plain -> {:ok, value}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  def result_with_lookup(_key, value, _compound_get) when not is_nil(value),
    do: {:ok, value}

  def result_with_lookup(key, nil, compound_get) do
    case compound_data_structure_status(key, compound_get) do
      :compound -> @wrongtype_error
      :plain -> {:ok, nil}
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp compound_data_structure_status(key, compound_get) do
    type_key = CompoundKey.type_key(key)
    list_meta_key = CompoundKey.list_meta_key(key)

    case compound_get.(key, type_key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        case compound_get.(key, list_meta_key) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> :plain
          _list_meta -> :compound
        end

      _type_marker ->
        :compound
    end
  end
end
