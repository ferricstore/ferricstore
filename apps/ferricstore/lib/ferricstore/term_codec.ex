defmodule Ferricstore.TermCodec do
  @moduledoc false

  @type decode_error :: {:error, :invalid_external_term}

  @spec encode(term()) :: binary()
  def encode(term), do: :erlang.term_to_binary(term, [:deterministic])

  @spec decode(term()) :: {:ok, term()} | decode_error()
  def decode(<<131, 80, _compressed::binary>>), do: {:error, :invalid_external_term}

  def decode(binary) when is_binary(binary) do
    case :erlang.binary_to_term(binary, [:safe, :used]) do
      {term, used} when used == byte_size(binary) -> {:ok, term}
      _invalid -> {:error, :invalid_external_term}
    end
  rescue
    ArgumentError -> {:error, :invalid_external_term}
  end

  def decode(_binary), do: {:error, :invalid_external_term}
end
