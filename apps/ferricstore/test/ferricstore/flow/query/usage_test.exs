defmodule Ferricstore.Flow.Query.UsageTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.Query.{Budget, Usage}

  test "validates one canonical usage contract for records and counts" do
    records = usage(3)
    assert Usage.valid?(records, Budget.default(), :records)

    count = %{
      usage(1)
      | hydrated_records: 0,
        scanned_entries: 0,
        scanned_bytes: 0,
        residual_checks: 0
    }

    assert Usage.valid?(count, Budget.default(), :count)

    refute Usage.valid?(%{count | result_records: 0}, Budget.default(), :count)

    refute Usage.valid?(
             Map.put(records, :provider_private_counter, 1),
             Budget.default(),
             :records
           )

    refute Usage.valid?(
             %{records | response_bytes: Budget.default().response_bytes + 1},
             Budget.default(),
             :records
           )
  end

  test "rejects impossible page and residual-check accounting" do
    baseline = usage(3)

    refute Usage.valid?(%{baseline | range_pages: 5}, Budget.default(), :records)
    refute Usage.valid?(%{baseline | residual_checks: 37}, Budget.default(), :records)
  end

  defp usage(result_records) do
    %{
      range_seeks: 1,
      range_pages: 1,
      scanned_entries: 3,
      scanned_bytes: 300,
      hydrated_records: 3,
      residual_checks: 3,
      duplicate_entries: 0,
      result_records: result_records,
      response_bytes: 1_024,
      memory_high_water_bytes: 2_048,
      wall_time_us: 500
    }
  end
end
