defmodule Ferricstore.Flow.Query.Budget do
  @moduledoc false

  @enforce_keys [
    :range_seeks,
    :scan_entries,
    :scan_bytes,
    :hydrated_records,
    :result_records,
    :response_bytes,
    :planner_memory_bytes,
    :executor_memory_bytes,
    :wall_time_ms
  ]

  @defaults [
    range_seeks: 32,
    scan_entries: 50_000,
    scan_bytes: 64 * 1_024 * 1_024,
    hydrated_records: 50_000,
    result_records: 100,
    response_bytes: 512 * 1_024,
    planner_memory_bytes: 4 * 1_024 * 1_024,
    executor_memory_bytes: 16 * 1_024 * 1_024,
    wall_time_ms: 750
  ]
  @maximums Map.new(@defaults)

  defstruct @defaults

  @type t :: %__MODULE__{
          range_seeks: pos_integer(),
          scan_entries: pos_integer(),
          scan_bytes: pos_integer(),
          hydrated_records: pos_integer(),
          result_records: pos_integer(),
          response_bytes: pos_integer(),
          planner_memory_bytes: pos_integer(),
          executor_memory_bytes: pos_integer(),
          wall_time_ms: pos_integer()
        }

  @spec default() :: t()
  def default, do: struct(__MODULE__)

  @spec new(map() | keyword()) :: {:ok, t()} | {:error, :invalid_query_budget}
  def new(overrides) when is_list(overrides), do: overrides |> Map.new() |> new()

  def new(%{} = overrides) do
    allowed = Map.keys(Map.from_struct(default()))

    if Enum.all?(Map.keys(overrides), &(&1 in allowed)) do
      budget = struct(default(), overrides)

      if valid?(budget), do: {:ok, budget}, else: {:error, :invalid_query_budget}
    else
      {:error, :invalid_query_budget}
    end
  end

  def new(_overrides), do: {:error, :invalid_query_budget}

  @spec lower(t(), map() | keyword()) :: {:ok, t()} | {:error, :invalid_query_budget}
  def lower(%__MODULE__{} = ceiling, requested) when is_list(requested),
    do: lower(ceiling, Map.new(requested))

  def lower(%__MODULE__{} = ceiling, %{} = requested) do
    with {:ok, candidate} <- new(Map.merge(Map.from_struct(ceiling), requested)),
         true <-
           Enum.all?(Map.from_struct(candidate), fn {field, value} ->
             value <= Map.fetch!(ceiling, field)
           end) do
      {:ok, candidate}
    else
      _invalid -> {:error, :invalid_query_budget}
    end
  end

  def lower(%__MODULE__{}, _requested), do: {:error, :invalid_query_budget}

  defp valid?(%__MODULE__{} = budget) do
    budget
    |> Map.from_struct()
    |> Enum.all?(fn {field, value} ->
      is_integer(value) and value > 0 and value <= Map.fetch!(@maximums, field)
    end)
  end
end
