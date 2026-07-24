defmodule Ferricstore.Flow.Query.Usage do
  @moduledoc false

  alias Ferricstore.Flow.Query.Limits
  alias Ferricstore.Flow.Query.Budget

  @fields [
    :range_seeks,
    :range_pages,
    :scanned_entries,
    :scanned_bytes,
    :hydrated_records,
    :residual_checks,
    :duplicate_entries,
    :result_records,
    :response_bytes,
    :memory_high_water_bytes,
    :wall_time_us
  ]
  @field_count length(@fields)
  @maximum_predicates Limits.max_predicates()
  @maximum_native_integer 0x7FFF_FFFF_FFFF_FFFF

  @doc false
  @spec fields() :: [atom()]
  def fields, do: @fields

  @spec valid?(term(), Budget.t(), :records | :count) :: boolean()
  def valid?(usage, %Budget{} = budget, kind)
      when is_map(usage) and kind in [:records, :count] do
    canonical_usage?(usage) and within_budget?(usage, budget) and internally_consistent?(usage) and
      valid_result_usage?(usage, kind)
  end

  def valid?(_usage, _budget, _kind), do: false

  defp canonical_usage?(usage) do
    map_size(usage) == @field_count and
      Enum.all?(@fields, fn field -> nonnegative_integer?(Map.get(usage, field)) end)
  end

  defp within_budget?(usage, budget) do
    usage.range_seeks <= budget.range_seeks and
      usage.scanned_entries <= budget.scan_entries and
      usage.scanned_bytes <= budget.scan_bytes and
      usage.hydrated_records <= budget.hydrated_records and
      usage.result_records <= budget.result_records and
      usage.response_bytes <= budget.response_bytes and
      usage.memory_high_water_bytes <= budget.executor_memory_bytes and
      usage.wall_time_us <= budget.wall_time_ms * 1_000
  end

  defp internally_consistent?(usage) do
    usage.hydrated_records <= usage.scanned_entries and
      usage.duplicate_entries <= usage.scanned_entries and
      usage.range_pages <= usage.scanned_entries + usage.range_seeks and
      usage.residual_checks <= usage.hydrated_records * @maximum_predicates
  end

  defp valid_result_usage?(%{result_records: 1}, :count), do: true
  defp valid_result_usage?(usage, :records), do: usage.result_records <= usage.hydrated_records
  defp valid_result_usage?(_usage, _kind), do: false

  defp nonnegative_integer?(value),
    do: is_integer(value) and value >= 0 and value <= @maximum_native_integer
end
