defmodule Ferricstore.Flow.RecordIdentity do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, StorageScope}

  @max_key_bytes 65_535

  @spec owns_state_key?(map(), binary()) :: boolean()
  def owns_state_key?(%{id: id} = record, state_key)
      when is_binary(id) and id != "" and byte_size(id) <= @max_key_bytes and
             is_binary(state_key) and state_key != "" and
             byte_size(state_key) <= @max_key_bytes do
    with {:ok, ^id} <- Keys.run_id_from_state_key(state_key),
         {:ok, physical_partition} <- StorageScope.physical_partition_key(record),
         true <- bounded_partition?(physical_partition) do
      Keys.state_key(id, physical_partition) == state_key
    else
      _invalid -> false
    end
  rescue
    _error -> false
  catch
    _kind, _reason -> false
  end

  def owns_state_key?(_record, _state_key), do: false

  defp bounded_partition?(nil), do: true

  defp bounded_partition?(partition),
    do: is_binary(partition) and byte_size(partition) <= @max_key_bytes
end
