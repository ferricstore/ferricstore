defmodule Ferricstore.FetchOrCompute.Outcome do
  @moduledoc false

  @prefix "FC:"
  @version 1
  @max_error_bytes 65_536

  @spec key(binary()) :: binary()
  def key(key) when is_binary(key), do: @prefix <> :crypto.hash(:sha256, key)

  @spec encode_error(binary()) :: {:ok, binary()} | {:error, binary()}
  def encode_error(error) when is_binary(error) and byte_size(error) <= @max_error_bytes do
    {:ok, <<@version, byte_size(error)::unsigned-big-32, error::binary>>}
  end

  def encode_error(_error), do: {:error, "ERR fetch_or_compute error is too large"}

  @spec decode_error(binary()) :: {:ok, binary()} | {:error, binary()}
  def decode_error(<<@version, size::unsigned-big-32, error::binary-size(size)>>)
      when size <= @max_error_bytes do
    {:ok, error}
  end

  def decode_error(_encoded), do: {:error, "ERR invalid fetch_or_compute outcome"}
end
