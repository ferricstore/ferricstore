defmodule Ferricstore.Flow.Digest do
  @moduledoc false

  # Digests embedded in Flow keys: unpadded URL-safe base64 of SHA-256.
  #
  # The data format of the node's data directory decides the length
  # (Ferricstore.DataFormat): format 1, used before short digests, keeps all
  # 32 bytes (43 characters); format 2 keeps the first 16 bytes (22 characters).
  # A format 1 digest therefore converts to the format 2 digest of the same
  # value without knowing the value (convert_legacy/1).
  #
  # Validation accepts a canonical digest of either length, so a node can read
  # and reserve keys of both formats.

  @format_key {__MODULE__, :format}
  @default_format 2

  @spec format() :: 1 | 2
  def format, do: :persistent_term.get(@format_key, @default_format)

  @doc false
  # Set once at boot (and by the offline converter); only writes on change, so
  # the global persistent_term update happens at most once per switch.
  @spec put_format(1 | 2) :: :ok
  def put_format(format) when format in [1, 2] do
    if format() != format, do: :persistent_term.put(@format_key, format)
    :ok
  end

  @spec length() :: 22 | 43
  def length, do: chars(format())

  defp chars(1), do: 43
  defp chars(2), do: 22

  @spec encode(binary()) :: binary()
  def encode(value) when is_binary(value), do: encode(value, format())

  defp encode(value, 1), do: Base.url_encode64(:crypto.hash(:sha256, value), padding: false)

  defp encode(value, 2) do
    <<prefix::binary-size(16), _rest::binary>> = :crypto.hash(:sha256, value)
    Base.url_encode64(prefix, padding: false)
  end

  @doc false
  # The format 2 digest of the value a canonical format 1 digest was made from.
  @spec convert_legacy(binary()) :: {:ok, binary()} | :error
  def convert_legacy(digest) when is_binary(digest) and byte_size(digest) == 43 do
    with true <- canonical?(digest),
         {:ok, <<prefix::binary-size(16), _rest::binary>>} <-
           Base.url_decode64(digest, padding: false) do
      {:ok, Base.url_encode64(prefix, padding: false)}
    else
      _invalid -> :error
    end
  end

  def convert_legacy(_digest), do: :error

  # The last character of a 32-byte digest carries 4 data bits and of a 16-byte
  # digest 2 data bits; only characters with the remaining low bits clear are
  # canonical. Checked without decoding: this runs for every key classified.
  @spec canonical?(term()) :: boolean()
  def canonical?(<<body::binary-size(42), last>>) when last in ~c"AEIMQUYcgkosw048",
    do: url_safe_base64?(body)

  def canonical?(<<body::binary-size(21), last>>) when last in ~c"AQgw",
    do: url_safe_base64?(body)

  def canonical?(_digest), do: false

  defp url_safe_base64?(<<char, rest::binary>>)
       when char in ?A..?Z or char in ?a..?z or char in ?0..?9 or char == ?- or char == ?_,
       do: url_safe_base64?(rest)

  defp url_safe_base64?(<<>>), do: true
  defp url_safe_base64?(_invalid), do: false
end
