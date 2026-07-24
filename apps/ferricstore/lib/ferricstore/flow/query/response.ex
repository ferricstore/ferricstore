defmodule Ferricstore.Flow.Query.Response do
  @moduledoc false

  alias Ferricstore.{Flow.Query.Limits, NativeValueCodec}
  alias Ferricstore.Flow.Query.{Budget, Usage}

  @contract "ferric.flow.query.result/v1"
  @quality_fields [:coverage, :exactness, :freshness, :pagination]
  @maximum_cursor_bytes Limits.max_cursor_bytes()
  @maximum_count 0x7FFF_FFFF_FFFF_FFFF

  @spec contract() :: binary()
  def contract, do: @contract

  @doc false
  @spec quality_fields() :: [atom()]
  def quality_fields, do: @quality_fields

  @spec build([map()], boolean(), binary() | nil, map(), map(), Budget.t()) ::
          {:ok, map()} | {:error, atom()}
  def build(records, has_more, cursor, quality, usage, %Budget{} = budget)
      when is_list(records) and is_boolean(has_more) and is_map(quality) and is_map(usage) do
    with :ok <- validate_page(has_more, cursor),
         :ok <- validate_records(records, usage, budget),
         :ok <- validate_usage(usage, budget, :records),
         :ok <- validate_quality(quality) do
      response = %{
        version: @contract,
        records: records,
        page: %{has_more: has_more, cursor: cursor},
        quality: quality,
        usage: usage
      }

      settle_size(response, budget)
    end
  end

  def build(_records, _has_more, _cursor, _quality, _usage, _budget),
    do: {:error, :query_engine_failure}

  @spec build_count(non_neg_integer(), map(), map(), Budget.t()) ::
          {:ok, map()} | {:error, atom()}
  def build_count(count, quality, usage, %Budget{} = budget)
      when is_integer(count) and count >= 0 and count <= @maximum_count and is_map(quality) and
             is_map(usage) do
    with :ok <- validate_usage(usage, budget, :count),
         :ok <- validate_quality(quality) do
      %{
        version: @contract,
        result: %{kind: "count", value: count},
        quality: quality,
        usage: usage
      }
      |> settle_size(budget)
    end
  end

  def build_count(_count, _quality, _usage, _budget), do: {:error, :query_engine_failure}

  defp settle_size(response, budget) do
    with {:ok, size} <- encoded_size(response) do
      if size <= budget.response_bytes,
        do: {:ok, put_in(response.usage.response_bytes, size)},
        else: {:error, :query_response_budget_exceeded}
    end
  end

  defp validate_page(false, nil), do: :ok

  defp validate_page(true, cursor)
       when is_binary(cursor) and cursor != "" and byte_size(cursor) <= @maximum_cursor_bytes,
       do: :ok

  defp validate_page(_has_more, _cursor), do: {:error, :query_engine_failure}

  defp validate_records(records, usage, budget) do
    if Enum.all?(records, &is_map/1) and length(records) == Map.get(usage, :result_records) and
         length(records) <= budget.result_records,
       do: :ok,
       else: {:error, :query_engine_failure}
  end

  defp validate_usage(usage, budget, kind) do
    if Usage.valid?(usage, budget, kind), do: :ok, else: {:error, :query_engine_failure}
  end

  defp validate_quality(quality) do
    valid =
      Map.keys(quality) |> Enum.sort() == Enum.sort(@quality_fields) and
        Enum.all?(@quality_fields, fn field ->
          value = Map.get(quality, field)
          is_binary(value) and value != "" and byte_size(value) <= 64
        end)

    if valid, do: :ok, else: {:error, :query_engine_failure}
  end

  defp encoded_size(term) do
    {:ok, NativeValueCodec.encoded_size(term)}
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end
end
