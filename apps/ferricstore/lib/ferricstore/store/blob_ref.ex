defmodule Ferricstore.Store.BlobRef do
  @moduledoc """
  Fixed-size content-addressed reference for future side-channel blob storage.

  Bitcask and Raft should store this small binary, not the large payload bytes.
  The external blob file path is derived from the checksum, which keeps refs
  deterministic and prevents path traversal.
  """

  @enforce_keys [:checksum, :size]
  defstruct version: 1, checksum: nil, size: 0

  @type t :: %__MODULE__{
          version: 1,
          checksum: <<_::256>>,
          size: non_neg_integer()
        }

  @max_u64 18_446_744_073_709_551_615
  @encoded_size 48

  @doc "Returns the fixed encoded byte size stored in Bitcask."
  @spec encoded_size() :: pos_integer()
  def encoded_size, do: @encoded_size

  @doc "Builds a content-addressed ref from payload bytes."
  @spec from_payload(binary()) :: t()
  def from_payload(payload) when is_binary(payload) do
    %__MODULE__{
      checksum: :crypto.hash(:sha256, payload),
      size: byte_size(payload)
    }
  end

  @doc "Encodes a ref as the exact fixed binary stored in Bitcask."
  @spec encode!(t()) :: binary()
  def encode!(%__MODULE__{version: 1, size: size, checksum: checksum})
      when is_integer(size) and size >= 0 and size <= @max_u64 and
             is_binary(checksum) and byte_size(checksum) == 32 do
    <<0, ?F, ?S, ?B, ?L, ?O, ?B, 1, size::unsigned-big-64, checksum::binary>>
  end

  def encode!(_ref) do
    raise ArgumentError,
          "invalid blob ref; expected version=1, non-negative u64 size, and 32-byte checksum"
  end

  @doc "Decodes a Bitcask value when it is exactly a blob ref."
  @spec decode(binary()) :: {:ok, t()} | :error
  def decode(<<0, ?F, ?S, ?B, ?L, ?O, ?B, 1, size::unsigned-big-64, checksum::binary-size(32)>>) do
    {:ok, %__MODULE__{checksum: checksum, size: size}}
  end

  def decode(_value), do: :error

  @doc "Returns true when `value` is an encoded blob ref."
  @spec ref?(term()) :: boolean()
  def ref?(value) when is_binary(value), do: match?({:ok, _ref}, decode(value))
  def ref?(_value), do: false

  @doc "Verifies that payload bytes match a ref's checksum and size."
  @spec verify_payload?(t(), binary()) :: boolean()
  def verify_payload?(%__MODULE__{size: size, checksum: checksum}, payload)
      when is_binary(payload) and is_binary(checksum) do
    byte_size(payload) == size and :crypto.hash(:sha256, payload) == checksum
  end

  def verify_payload?(_ref, _payload), do: false

  @doc "Returns the shard-local relative blob path for a ref."
  @spec relative_path(t()) :: binary()
  def relative_path(%__MODULE__{checksum: checksum}) when is_binary(checksum) do
    validate_checksum!(checksum)
    hex = Base.encode16(checksum, case: :lower)
    Path.join([binary_part(hex, 0, 2), hex <> ".blob"])
  end

  @doc "Returns the canonical blob file path for a ref."
  @spec path(binary(), non_neg_integer(), t()) :: binary()
  def path(data_dir, shard_index, %__MODULE__{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    Path.join(Ferricstore.DataDir.blob_shard_path(data_dir, shard_index), relative_path(ref))
  end

  defp validate_checksum!(checksum) when byte_size(checksum) == 32, do: :ok

  defp validate_checksum!(_checksum) do
    raise ArgumentError, "invalid blob ref checksum; expected 32 bytes"
  end
end
