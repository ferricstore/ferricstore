defmodule Ferricstore.Flow.Query.MemoryBudget do
  @moduledoc false

  # FSF5 records are schema-bounded. The smallest valid record is the worst
  # compact-bytes-to-heap ratio; 64x covers it with room for VM layout changes.
  @maximum_record_decode_expansion 64
  @record_storage_copies 1
  @record_reservation_factor @maximum_record_decode_expansion + @record_storage_copies
  @spec term_bytes(term()) :: non_neg_integer()
  defdelegate term_bytes(term), to: Ferricstore.TermMemory, as: :bytes

  @spec decoded_record_reservation(non_neg_integer()) :: non_neg_integer()
  def decoded_record_reservation(encoded_bytes)
      when is_integer(encoded_bytes) and encoded_bytes >= 0,
      do: encoded_bytes * @record_reservation_factor

  @spec encoded_record_input_bytes(integer()) :: non_neg_integer()
  def encoded_record_input_bytes(available_bytes)
      when is_integer(available_bytes) and available_bytes > 0,
      do: div(available_bytes, @record_reservation_factor)

  def encoded_record_input_bytes(_available_bytes), do: 0
end
