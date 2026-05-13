defmodule Ferricstore.Raft.BlobCommand do
  @moduledoc """
  Prepares large-value Raft commands for blob side-channel replication.

  The ref-only command shapes are only safe for a single-member Raft group: the
  local apply side can validate that the blob file already exists before it
  stores the ref in Bitcask. Multi-member groups still need a blob transfer
  protocol before followers can apply refs without the original payload.
  """

  alias Ferricstore.Store.{BlobRef, BlobStore, BlobValue}

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:put_batch, [{binary(), binary(), non_neg_integer()}]}
          | term()

  @doc """
  Returns a command that can be submitted to Raft.

  When side-channel storage is disabled, or the Raft group has more than one
  member, commands are returned unchanged. In one-node mode, large values are
  written to the blob store first and Raft receives only the small encoded ref.
  """
  @spec prepare(map(), non_neg_integer(), command(), keyword()) ::
          {:ok, command()} | {:error, term()}
  def prepare(ctx, shard_index, command, opts \\ []) do
    threshold = BlobValue.threshold(ctx)

    cond do
      threshold <= 0 ->
        {:ok, command}

      not Keyword.get(opts, :single_member?, false) ->
        {:ok, command}

      true ->
        prepare_enabled(ctx, shard_index, threshold, command)
    end
  end

  @doc """
  Returns true when `command` contains a value that would use the blob
  side-channel if the Raft group is eligible.

  Batcher uses this as a cheap hot-path guard so enabling a large-value
  threshold does not force a Ra membership lookup for every tiny SET.
  """
  @spec side_channel_candidate?(map(), command()) :: boolean()
  def side_channel_candidate?(ctx, command) do
    threshold = BlobValue.threshold(ctx)
    threshold > 0 and command_candidate?(command, threshold)
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:put, key, value, exp})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_binary(value) do
    case prepare_value(data_dir, shard_index, threshold, value) do
      {:ok, {^value, :value}} -> {:ok, {:put, key, value, exp}}
      {:ok, {encoded_ref, :blob_ref}} -> {:ok, {:put_blob_ref, key, encoded_ref, exp}}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:put_batch, entries})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_list(entries) do
    with {:ok, prepared, externalized?} <-
           prepare_batch_entries(data_dir, shard_index, threshold, entries) do
      if externalized? do
        {:ok, {:put_blob_batch, prepared}}
      else
        {:ok, {:put_batch, entries}}
      end
    end
  end

  defp prepare_enabled(%{data_dir: data_dir}, shard_index, threshold, {:batch, commands})
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_list(commands) do
    with {:ok, prepared_commands} <-
           prepare_generic_batch_commands(data_dir, shard_index, threshold, commands) do
      {:ok, {:batch, prepared_commands}}
    end
  end

  defp prepare_enabled(_ctx, _shard_index, _threshold, command), do: {:ok, command}

  defp command_candidate?({:put, _key, value, _expire_at_ms}, threshold)
       when is_binary(value) do
    externalize?(value, threshold)
  end

  defp command_candidate?({:put_batch, entries}, threshold) when is_list(entries) do
    Enum.any?(entries, fn
      {_key, value, _expire_at_ms} when is_binary(value) -> externalize?(value, threshold)
      _other -> false
    end)
  end

  defp command_candidate?({:batch, commands}, threshold) when is_list(commands) do
    Enum.any?(commands, &command_candidate?(&1, threshold))
  end

  defp command_candidate?(_command, _threshold), do: false

  defp prepare_batch_entries(data_dir, shard_index, threshold, entries) do
    Enum.reduce_while(entries, {:ok, [], false}, fn
      {key, value, expire_at_ms}, {:ok, acc, externalized?}
      when is_binary(key) and is_binary(value) ->
        case prepare_value(data_dir, shard_index, threshold, value) do
          {:ok, {prepared_value, kind}} ->
            next_externalized? = externalized? or kind == :blob_ref
            {:cont, {:ok, [{key, prepared_value, expire_at_ms, kind} | acc], next_externalized?}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      _invalid, {:ok, _acc, _externalized?} ->
        {:halt, {:error, :invalid_put_batch_entry}}
    end)
    |> case do
      {:ok, prepared, externalized?} -> {:ok, Enum.reverse(prepared), externalized?}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_generic_batch_commands(data_dir, shard_index, threshold, commands) do
    Enum.reduce_while(commands, {:ok, []}, fn
      {:put, key, value, expire_at_ms}, {:ok, acc}
      when is_binary(key) and is_binary(value) ->
        case prepare_value(data_dir, shard_index, threshold, value) do
          {:ok, {^value, :value}} ->
            {:cont, {:ok, [{:put, key, value, expire_at_ms} | acc]}}

          {:ok, {encoded_ref, :blob_ref}} ->
            {:cont, {:ok, [{:put_blob_ref, key, encoded_ref, expire_at_ms} | acc]}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      command, {:ok, acc} ->
        {:cont, {:ok, [command | acc]}}
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_value(data_dir, shard_index, threshold, value) do
    if externalize?(value, threshold) do
      with {:ok, ref} <- BlobStore.put(data_dir, shard_index, value) do
        {:ok, {BlobRef.encode!(ref), :blob_ref}}
      end
    else
      {:ok, {value, :value}}
    end
  end

  defp externalize?(value, threshold) do
    byte_size(value) >= threshold or BlobRef.ref?(value)
  end
end
