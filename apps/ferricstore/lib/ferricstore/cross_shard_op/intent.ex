defmodule Ferricstore.CrossShardOp.Intent do
  @moduledoc false

  @max_keys 20

  @spec validate(term(), term()) :: {:ok, [binary()]} | {:error, :invalid_cross_shard_intent}
  def validate(
        owner_ref,
        %{
          command: command,
          keys: keys,
          value_hashes: value_hashes,
          status: :executing,
          created_at: created_at
        }
      )
      when is_reference(owner_ref) and is_atom(command) and is_map(keys) and map_size(keys) > 0 and
             map_size(keys) <= @max_keys and
             is_map(value_hashes) and map_size(value_hashes) > 0 and
             map_size(value_hashes) <= @max_keys and is_integer(created_at) and created_at >= 0 do
    with {:ok, described_keys} <- flatten_described_keys(keys),
         token_keys = Map.keys(value_hashes),
         true <- Enum.all?(token_keys, &is_binary/1),
         true <- MapSet.new(described_keys) == MapSet.new(token_keys) do
      {:ok, token_keys}
    else
      _invalid -> {:error, :invalid_cross_shard_intent}
    end
  end

  def validate(_owner_ref, _intent), do: {:error, :invalid_cross_shard_intent}

  defp flatten_described_keys(keys) do
    case Enum.reduce_while(keys, {:ok, [], 0}, fn
           {_name, key}, {:ok, acc, count} when is_binary(key) and count < @max_keys ->
             {:cont, {:ok, [key | acc], count + 1}}

           {_name, key_list}, {:ok, acc, count} when is_list(key_list) ->
             append_key_list(key_list, acc, count)

           _entry, _acc ->
             {:halt, {:error, :invalid_cross_shard_intent}}
         end) do
      {:ok, described_keys, _count} -> {:ok, described_keys}
      {:error, :invalid_cross_shard_intent} = error -> error
    end
  end

  defp append_key_list([], acc, count), do: {:cont, {:ok, acc, count}}

  defp append_key_list([key | rest], acc, count)
       when is_binary(key) and count < @max_keys do
    append_key_list(rest, [key | acc], count + 1)
  end

  defp append_key_list(_invalid_or_too_large, _acc, _count),
    do: {:halt, {:error, :invalid_cross_shard_intent}}
end
