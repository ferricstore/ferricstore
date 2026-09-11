defmodule Ferricstore.Flow.Query.QueryRow do
  @moduledoc false

  alias Ferricstore.Flow.{Locator, StorageScope}

  @terminal_states ["completed", "failed", "cancelled"]

  @enforce_keys [:state_key, :record, :locator, :expire_at_ms]
  defstruct [:state_key, :record, :locator, :expire_at_ms, scope_prefix: nil]

  @type t :: %__MODULE__{
          state_key: binary(),
          record: map(),
          locator: Locator.t(),
          expire_at_ms: non_neg_integer(),
          scope_prefix: binary() | nil
        }

  @doc false
  @spec internal_record(t()) :: {:ok, map()} | :error
  def internal_record(%__MODULE__{} = row) do
    with {:ok, record} <- restore_physical_partition(row.record, row.scope_prefix) do
      {:ok, restore_terminal_retention(record, row.expire_at_ms)}
    end
  end

  def internal_record(_row), do: :error

  defp restore_physical_partition(record, nil) when is_map(record), do: {:ok, record}

  defp restore_physical_partition(%{partition_key: partition_key} = record, scope_prefix)
       when is_binary(partition_key) and is_binary(scope_prefix) do
    case StorageScope.physical_partition_key(partition_key, scope_prefix) do
      {:ok, physical_partition} -> {:ok, Map.put(record, :partition_key, physical_partition)}
      {:error, _reason} -> :error
    end
  end

  defp restore_physical_partition(_record, _scope_prefix), do: :error

  defp restore_terminal_retention(%{state: state} = record, expire_at_ms)
       when state in @terminal_states and is_integer(expire_at_ms) and expire_at_ms > 0,
       do: Map.put(record, :terminal_retention_until_ms, expire_at_ms)

  defp restore_terminal_retention(record, _expire_at_ms), do: record
end
