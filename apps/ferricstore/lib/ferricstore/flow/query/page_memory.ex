defmodule Ferricstore.Flow.Query.PageMemory do
  @moduledoc false

  # Bounds the outer entry/list layout, three binary headers, one boxed expiry,
  # and the covering-map header under Ferricstore.TermMemory's 64-bit model.
  @retained_entry_structure_bytes 576
  @retained_covering_field_bytes 96
  @retained_binary_reference_bytes 64

  @spec retained_bytes([map()], non_neg_integer()) :: non_neg_integer()
  def retained_bytes(entries, scanned_bytes)
      when is_list(entries) and is_integer(scanned_bytes) and scanned_bytes >= 0 do
    {measured_storage_bytes, measured_retained_bytes} = measure(entries)
    measured_retained_bytes - measured_storage_bytes + scanned_bytes
  end

  @spec measure([map()]) :: {non_neg_integer(), non_neg_integer()}
  def measure(entries) when is_list(entries) do
    Enum.reduce(entries, {0, 0}, fn entry, {storage_bytes, retained_bytes} ->
      entry_storage_bytes = entry.storage_bytes
      expansion = @retained_entry_structure_bytes + covering_expansion(entry)

      {storage_bytes + entry_storage_bytes, retained_bytes + entry_storage_bytes + expansion}
    end)
  end

  @spec estimated_bytes(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          non_neg_integer()
  def estimated_bytes(entry_count, average_storage_bytes, covering_field_count)
      when is_integer(entry_count) and entry_count >= 0 and is_integer(average_storage_bytes) and
             average_storage_bytes >= 0 and is_integer(covering_field_count) and
             covering_field_count >= 0 do
    entry_count *
      (average_storage_bytes + @retained_entry_structure_bytes +
         covering_field_count * @retained_covering_field_bytes)
  end

  defp covering_expansion(%{id: id, covering_record: covering_record})
       when is_binary(id) and is_map(covering_record) do
    map_size(covering_record) * @retained_covering_field_bytes + byte_size(id) +
      @retained_binary_reference_bytes
  end

  defp covering_expansion(_entry), do: 0
end
