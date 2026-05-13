defmodule Ferricstore.Store.BlobValue do
  @moduledoc """
  Value-level glue for large payload blob side-channel storage.

  Bitcask stores a fixed `BlobRef` only when the instance threshold is enabled
  and the persisted value is large enough. Values that already look like an
  encoded `BlobRef` are also externalized so arbitrary user bytes cannot be
  confused with an internal pointer on read.
  """

  alias Ferricstore.Store.{BlobRef, BlobStore}

  @doc "Returns the configured blob threshold, or 0 when disabled."
  @spec threshold(term()) :: non_neg_integer()
  def threshold(%{blob_side_channel_threshold_bytes: threshold})
      when is_integer(threshold) and threshold > 0,
      do: threshold

  def threshold(_ctx), do: 0

  @doc "Externalizes `value` to the blob store when it crosses the threshold."
  @spec maybe_externalize(binary(), non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def maybe_externalize(data_dir, shard_index, threshold, value)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(threshold) and threshold > 0 and is_binary(value) do
    if externalize?(value, threshold) do
      with {:ok, ref} <- BlobStore.put(data_dir, shard_index, value) do
        {:ok, BlobRef.encode!(ref)}
      end
    else
      {:ok, value}
    end
  end

  def maybe_externalize(_data_dir, _shard_index, _threshold, value) when is_binary(value),
    do: {:ok, value}

  @doc "Externalizes a batch of values with one blob append/fsync when needed."
  @spec maybe_externalize_many(binary(), non_neg_integer(), non_neg_integer(), [binary()]) ::
          {:ok, [binary()]} | {:error, term()}
  def maybe_externalize_many(data_dir, shard_index, threshold, values)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(threshold) and threshold > 0 and is_list(values) do
    externalize_many(data_dir, shard_index, threshold, values)
  end

  def maybe_externalize_many(_data_dir, _shard_index, _threshold, values) when is_list(values) do
    if Enum.all?(values, &is_binary/1) do
      {:ok, values}
    else
      {:error, :invalid_blob_payload}
    end
  end

  @doc """
  Materializes an encoded blob ref when the side-channel is enabled.

  When the threshold is disabled, ref-shaped user bytes are ordinary values.
  This keeps arbitrary 48-byte payloads from being misread as internal refs.
  """
  @spec maybe_materialize(binary(), non_neg_integer(), non_neg_integer(), term()) ::
          {:ok, term()} | {:error, term()}
  def maybe_materialize(data_dir, shard_index, threshold, value)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(threshold) and threshold > 0 and is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, ref} -> BlobStore.get(data_dir, shard_index, ref)
      :error -> {:ok, value}
    end
  end

  def maybe_materialize(_data_dir, _shard_index, _threshold, value), do: {:ok, value}

  defp externalize?(value, threshold) do
    byte_size(value) >= threshold or BlobRef.ref?(value)
  end

  defp externalize_many(data_dir, shard_index, threshold, values) do
    values
    |> Enum.reduce_while({:ok, [], []}, fn
      value, {:ok, prepared, payloads} when is_binary(value) ->
        if externalize?(value, threshold) do
          {:cont, {:ok, [{:external, value} | prepared], [value | payloads]}}
        else
          {:cont, {:ok, [{:value, value} | prepared], payloads}}
        end

      _invalid, {:ok, _prepared, _payloads} ->
        {:halt, {:error, :invalid_blob_payload}}
    end)
    |> case do
      {:ok, _prepared, []} ->
        {:ok, values}

      {:ok, prepared, external_payloads} ->
        with {:ok, refs} <-
               BlobStore.put_many(data_dir, shard_index, Enum.reverse(external_payloads)) do
          {:ok, inflate_externalized_values(Enum.reverse(prepared), refs)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp inflate_externalized_values(prepared, refs) do
    {values, []} =
      Enum.map_reduce(prepared, refs, fn
        {:external, _value}, [ref | rest] -> {BlobRef.encode!(ref), rest}
        {:value, value}, refs -> {value, refs}
      end)

    values
  end
end
