defmodule Ferricstore.Store.BlobRef do
  @moduledoc """
  Fixed-size side-channel reference for large-value blob storage.

  Version 1 refs are legacy content-addressed files. Version 2 refs point at a
  payload range in a shard append segment and carry a checksum so materialized
  reads and apply-time validation can still prove byte integrity.
  """

  @enforce_keys [:checksum, :size]
  defstruct version: 1, checksum: nil, size: 0, segment_id: nil, offset: nil

  @type t :: %__MODULE__{
          version: 1 | 2,
          checksum: <<_::256>>,
          size: non_neg_integer(),
          segment_id: non_neg_integer() | nil,
          offset: non_neg_integer() | nil
        }

  @max_u64 18_446_744_073_709_551_615
  @legacy_encoded_size 48
  @segment_encoded_size 64

  @doc "Returns the fixed encoded byte size stored in Bitcask for new refs."
  @spec encoded_size() :: pos_integer()
  def encoded_size, do: @segment_encoded_size

  @doc "Returns true when a persisted value size can hold an encoded blob ref."
  @spec encoded_size?(term()) :: boolean()
  def encoded_size?(size), do: size in [@legacy_encoded_size, @segment_encoded_size]

  @doc "Builds a content-addressed ref from payload bytes."
  @spec from_payload(binary()) :: t()
  def from_payload(payload) when is_binary(payload) do
    %__MODULE__{
      checksum: :crypto.hash(:sha256, payload),
      size: byte_size(payload)
    }
  end

  @doc "Builds an append-segment ref from payload bytes and its segment location."
  @spec from_segment(binary(), non_neg_integer(), non_neg_integer()) :: t()
  def from_segment(payload, segment_id, offset)
      when is_binary(payload) and is_integer(segment_id) and segment_id >= 0 and
             is_integer(offset) and offset >= 0 do
    %__MODULE__{
      version: 2,
      checksum: :crypto.hash(:sha256, payload),
      size: byte_size(payload),
      segment_id: segment_id,
      offset: offset
    }
  end

  @doc "Encodes a ref as the exact fixed binary stored in Bitcask."
  @spec encode!(t()) :: binary()
  def encode!(%__MODULE__{version: 1, size: size, checksum: checksum})
      when is_integer(size) and size >= 0 and size <= @max_u64 and
             is_binary(checksum) and byte_size(checksum) == 32 do
    <<0, ?F, ?S, ?B, ?L, ?O, ?B, 1, size::unsigned-big-64, checksum::binary>>
  end

  def encode!(%__MODULE__{
        version: 2,
        size: size,
        segment_id: segment_id,
        offset: offset,
        checksum: checksum
      })
      when is_integer(size) and size >= 0 and size <= @max_u64 and
             is_integer(segment_id) and segment_id >= 0 and segment_id <= @max_u64 and
             is_integer(offset) and offset >= 0 and offset <= @max_u64 and
             is_binary(checksum) and byte_size(checksum) == 32 do
    <<0, ?F, ?S, ?B, ?L, ?O, ?B, 2, size::unsigned-big-64, segment_id::unsigned-big-64,
      offset::unsigned-big-64, checksum::binary>>
  end

  def encode!(_ref) do
    raise ArgumentError,
          "invalid blob ref; expected a supported version, non-negative u64 fields, and 32-byte checksum"
  end

  @doc "Decodes a Bitcask value when it is exactly a blob ref."
  @spec decode(binary()) :: {:ok, t()} | :error
  def decode(<<0, ?F, ?S, ?B, ?L, ?O, ?B, 1, size::unsigned-big-64, checksum::binary-size(32)>>) do
    {:ok, %__MODULE__{checksum: checksum, size: size}}
  end

  def decode(
        <<0, ?F, ?S, ?B, ?L, ?O, ?B, 2, size::unsigned-big-64, segment_id::unsigned-big-64,
          offset::unsigned-big-64, checksum::binary-size(32)>>
      ) do
    {:ok,
     %__MODULE__{
       version: 2,
       checksum: checksum,
       size: size,
       segment_id: segment_id,
       offset: offset
     }}
  end

  def decode(_value), do: :error

  @doc "Returns true when `value` is an encoded blob ref."
  @spec ref?(term()) :: boolean()
  def ref?(value) when is_binary(value) do
    encoded_size?(byte_size(value)) and match?({:ok, _ref}, decode(value))
  end

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
  def relative_path(%__MODULE__{version: 1, checksum: checksum}) when is_binary(checksum) do
    validate_checksum!(checksum)
    hex = Base.encode16(checksum, case: :lower)
    Path.join([binary_part(hex, 0, 2), hex <> ".blob"])
  end

  def relative_path(%__MODULE__{version: 2, segment_id: segment_id})
      when is_integer(segment_id) and segment_id >= 0 do
    Path.join(["segments", segment_filename(segment_id)])
  end

  @doc "Returns the canonical blob file path for a ref."
  @spec path(binary(), non_neg_integer(), t()) :: binary()
  def path(data_dir, shard_index, %__MODULE__{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    Path.join(Ferricstore.DataDir.blob_shard_path(data_dir, shard_index), relative_path(ref))
  end

  @doc "Returns the canonical append-segment filename for a segment id."
  @spec segment_filename(non_neg_integer()) :: binary()
  def segment_filename(segment_id) when is_integer(segment_id) and segment_id >= 0 do
    segment_id
    |> Integer.to_string()
    |> String.pad_leading(20, "0")
    |> Kernel.<>(".bloblog")
  end

  defp validate_checksum!(checksum) when byte_size(checksum) == 32, do: :ok

  defp validate_checksum!(_checksum) do
    raise ArgumentError, "invalid blob ref checksum; expected 32 bytes"
  end
end
