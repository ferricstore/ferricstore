defmodule Ferricstore.Flow.RetentionCleanupMember do
  @moduledoc false

  def encode(index_key, owned_key) when is_binary(index_key) and is_binary(owned_key) do
    :erlang.term_to_binary({index_key, owned_key})
  end

  def decode(value) when is_binary(value) do
    case :erlang.binary_to_term(value, [:safe]) do
      {index_key, owned_key} when is_binary(index_key) and is_binary(owned_key) ->
        {:ok, {index_key, owned_key}}

      _invalid ->
        :error
    end
  rescue
    _ -> :error
  end

  def decode(_value), do: :error
end
