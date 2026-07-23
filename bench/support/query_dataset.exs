defmodule Ferricstore.Bench.QueryDataset do
  @moduledoc false

  @tenant "benchmark-tenant"

  @spec tenant() :: binary()
  def tenant, do: @tenant

  @spec records(pos_integer()) :: [map()]
  def records(count) when is_integer(count) and count > 0 do
    Enum.map(1..count, fn ordinal ->
      %{
        id: "run-#{String.pad_leading(Integer.to_string(ordinal), 12, "0")}",
        type: if(rem(ordinal, 3) == 0, do: "invoice", else: "workflow"),
        state: Enum.at(["failed", "running", "completed"], rem(div(ordinal - 1, 3), 3)),
        version: 1,
        priority: rem(ordinal, 10),
        partition_key: @tenant,
        created_at_ms: ordinal - 1,
        updated_at_ms: ordinal,
        lease_deadline_ms: ordinal,
        attempts: rem(ordinal, 4)
      }
    end)
  end
end
