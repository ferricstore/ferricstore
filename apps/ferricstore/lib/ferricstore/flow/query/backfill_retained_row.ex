defmodule Ferricstore.Flow.Query.BackfillRetainedRow do
  @moduledoc false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Query.QueryRow

  def validate_park(%QueryRow{} = row, blob) when is_binary(blob) do
    case LMDB.decode_cold_park(blob) do
      {:ok, park} ->
        # Park locators may still be logical WAL addresses. Identity and version
        # must agree; their resolved physical offsets need not be identical.
        if park.state_key == row.state_key and
             park.locator.flow_id == row.locator.flow_id and
             park.locator.version == row.locator.version do
          :ok
        else
          {:error, :query_backfill_concurrent_change}
        end

      _invalid ->
        {:error, :corrupt_query_backfill_record}
    end
  end
end
