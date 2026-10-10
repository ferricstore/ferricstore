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

  @doc """
  Decodes a term this cluster wrote itself (the replicated Raft log).

  Unlike `decode/1`, this may create atoms: replicated commands carry user
  terms such as payload maps with atom keys, and a restarted VM must replay
  them before those atoms exist again. Never use it for client input.
  """
  @spec decode_trusted(term()) :: {:ok, term()} | decode_error()
  def decode_trusted(<<131, 80, _compressed::binary>>), do: {:error, :invalid_external_term}

  def decode_trusted(binary) when is_binary(binary) do
    case :erlang.binary_to_term(binary, [:used]) do
      {term, used} when used == byte_size(binary) -> {:ok, term}
      _invalid -> {:error, :invalid_external_term}
    end
  rescue
    ArgumentError -> {:error, :invalid_external_term}
  end

  def decode_trusted(_binary), do: {:error, :invalid_external_term}
end
