defmodule Ferricstore.Flow.LMDB.ValueLocator do
  @moduledoc false

  alias Ferricstore.TermCodec

  @max_u64 18_446_744_073_709_551_615

  def encode(expire_at_ms, file_id, offset, value_size)
      when is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_u64 and
             is_integer(offset) and offset >= 0 and offset <= @max_u64 and
             is_integer(value_size) and value_size >= 0 and value_size <= @max_u64,
      do: encode_validated(expire_at_ms, file_id, offset, value_size)

  def encode(_expire_at_ms, _file_id, _offset, _value_size),
    do: raise(ArgumentError, "LMDB value locator metadata is invalid")

  defp encode_validated(expire_at_ms, file_id, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
       do: TermCodec.encode({:flow_value_locator, 1, expire_at_ms, file_id, offset, value_size})

  defp encode_validated(expire_at_ms, {:flow_history, file_id} = source, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
       do: TermCodec.encode({:flow_value_locator, 1, expire_at_ms, source, offset, value_size})

  defp encode_validated(
         expire_at_ms,
         {tag, index} = source,
         offset,
         value_size
       )
       when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
              index > 0 and index <= @max_u64,
       do: TermCodec.encode({:flow_value_locator, 1, expire_at_ms, source, offset, value_size})

  defp encode_validated(_expire_at_ms, _file_id, _offset, _value_size),
    do: raise(ArgumentError, "LMDB value locator metadata is invalid")

  def valid_file_id?(file_id)
      when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
      do: true

  def valid_file_id?({:flow_history, file_id})
      when is_integer(file_id) and file_id >= 0 and file_id <= @max_u64,
      do: true

  def valid_file_id?({tag, index})
      when tag in [:waraft_segment, :waraft_apply_projection] and is_integer(index) and
             index > 0 and index <= @max_u64,
      do: true

  def valid_file_id?(_file_id), do: false
end
