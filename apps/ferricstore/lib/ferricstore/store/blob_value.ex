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
    if BlobRef.encoded_size?(byte_size(value)) do
      case BlobRef.decode(value) do
        {:ok, ref} -> BlobStore.get(data_dir, shard_index, ref)
        :error -> {:ok, value}
      end
    else
      {:ok, value}
    end
  end

  def maybe_materialize(_data_dir, _shard_index, _threshold, value), do: {:ok, value}

  @doc """
  Materializes a batch of values while loading each exact encoded blob ref once.

  The result stays per-entry so one corrupt or missing blob ref only affects the
  values that point at that exact ref.
  """
  @spec maybe_materialize_many(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [term()]
        ) :: [{:ok, term()} | {:error, term()}]
  @spec maybe_materialize_many(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [term()],
          (binary(), non_neg_integer(), BlobRef.t() -> {:ok, binary()} | {:error, term()})
        ) :: [{:ok, term()} | {:error, term()}]
  def maybe_materialize_many(data_dir, shard_index, threshold, values)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(threshold) and threshold > 0 and is_list(values) do
    {prepared, unique_refs} = prepare_materialize_batch(values)
    loaded_refs = load_materialize_refs(data_dir, shard_index, unique_refs)
    materialize_prepared(prepared, loaded_refs)
  end

  def maybe_materialize_many(_data_dir, _shard_index, _threshold, values) when is_list(values) do
    Enum.map(values, &{:ok, &1})
  end

  def maybe_materialize_many(data_dir, shard_index, threshold, values, loader)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_integer(threshold) and threshold > 0 and is_list(values) and
             is_function(loader, 3) do
    {prepared, unique_refs} = prepare_materialize_batch(values)
    loaded_refs = load_materialize_refs(data_dir, shard_index, unique_refs, loader)
    materialize_prepared(prepared, loaded_refs)
  end

  def maybe_materialize_many(_data_dir, _shard_index, _threshold, values, _loader)
      when is_list(values) do
    Enum.map(values, &{:ok, &1})
  end

  defp load_materialize_refs(data_dir, shard_index, unique_refs) do
    refs = Enum.map(unique_refs, fn {_encoded_ref, ref} -> ref end)
    results = BlobStore.get_many(data_dir, shard_index, refs)

    unique_refs
    |> Enum.zip(results)
    |> Map.new(fn {{encoded_ref, _ref}, result} ->
      {encoded_ref, normalize_load_result(result)}
    end)
  end

  defp load_materialize_refs(data_dir, shard_index, unique_refs, loader) do
    Enum.reduce(unique_refs, %{}, fn {encoded_ref, ref}, acc ->
      Map.put(acc, encoded_ref, normalize_load_result(loader.(data_dir, shard_index, ref)))
    end)
  end

  defp materialize_prepared(prepared, loaded_refs) do
    Enum.map(prepared, fn
      {:ref, encoded_ref} -> Map.fetch!(loaded_refs, encoded_ref)
      {:value, value} -> {:ok, value}
    end)
  end

  defp externalize?(value, threshold) do
    size = byte_size(value)
    size >= threshold or (BlobRef.encoded_size?(size) and BlobRef.ref?(value))
  end

  defp prepare_materialize_batch(values) do
    {prepared, unique_refs, _seen} =
      Enum.reduce(values, {[], [], MapSet.new()}, fn
        value, {prepared, unique_refs, seen} when is_binary(value) ->
          if BlobRef.encoded_size?(byte_size(value)) do
            case BlobRef.decode(value) do
              {:ok, ref} ->
                if MapSet.member?(seen, value) do
                  {[{:ref, value} | prepared], unique_refs, seen}
                else
                  {[{:ref, value} | prepared], [{value, ref} | unique_refs],
                   MapSet.put(seen, value)}
                end

              :error ->
                {[{:value, value} | prepared], unique_refs, seen}
            end
          else
            {[{:value, value} | prepared], unique_refs, seen}
          end

        value, {prepared, unique_refs, seen} ->
          {[{:value, value} | prepared], unique_refs, seen}
      end)

    {Enum.reverse(prepared), Enum.reverse(unique_refs)}
  end

  defp normalize_load_result({:ok, _value} = ok), do: ok
  defp normalize_load_result({:error, _reason} = error), do: error
  defp normalize_load_result(other), do: {:error, other}

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
