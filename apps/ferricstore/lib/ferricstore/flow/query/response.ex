defmodule Ferricstore.Flow.Query.Response do
  @moduledoc false

  alias Ferricstore.NativeValueCodec
  alias Ferricstore.Flow.Query.{Budget, Cursor, PreparedResponse, Quality, ResultCodec, Usage}

  @contract "ferric.flow.query.result/v1"
  @maximum_count 0x7FFF_FFFF_FFFF_FFFF

  @spec contract() :: binary()
  def contract, do: @contract

  @doc false
  @spec quality_fields() :: [atom()]
  def quality_fields, do: Quality.fields()

  @spec build([map()], boolean(), binary() | nil, map(), map(), Budget.t()) ::
          {:ok, map() | PreparedResponse.t()} | {:error, atom()}
  def build(records, has_more, cursor, quality, usage, budget),
    do: build(records, has_more, cursor, quality, usage, budget, :native_value)

  @spec build(
          [map()],
          boolean(),
          binary() | nil,
          map(),
          map(),
          Budget.t(),
          :native_value | :flow_query_result_v1
        ) :: {:ok, map() | PreparedResponse.t()} | {:error, atom()}
  def build(records, has_more, cursor, quality, usage, %Budget{} = budget, response_codec)
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

      settle_size(response, budget, response_codec)
    end
  end

  def build(_records, _has_more, _cursor, _quality, _usage, _budget, _response_codec),
    do: {:error, :query_engine_failure}

  @spec build_count(non_neg_integer(), map(), map(), Budget.t()) ::
          {:ok, map() | PreparedResponse.t()} | {:error, atom()}
  def build_count(count, quality, usage, budget),
    do: build_count(count, quality, usage, budget, :native_value)

  @spec build_count(
          non_neg_integer(),
          map(),
          map(),
          Budget.t(),
          :native_value | :flow_query_result_v1
        ) :: {:ok, map() | PreparedResponse.t()} | {:error, atom()}
  def build_count(count, quality, usage, %Budget{} = budget, response_codec)
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
      |> settle_size(budget, response_codec)
    end
  end

  def build_count(_count, _quality, _usage, _budget, _response_codec),
    do: {:error, :query_engine_failure}

  defp settle_size(response, budget, :native_value) do
    with {:ok, size} <- encoded_size(response, :native_value) do
      if size <= budget.response_bytes,
        do: {:ok, put_in(response.usage.response_bytes, size)},
        else: {:error, :query_response_budget_exceeded}
    end
  end

  defp settle_size(response, budget, :flow_query_result_v1) do
    with {:ok, payload, size} <- ResultCodec.encode_with_size(response),
         true <- size <= budget.response_bytes,
         response = put_in(response.usage.response_bytes, size),
         {:ok, prepared} <- PreparedResponse.new(response, payload) do
      {:ok, prepared}
    else
      false -> {:error, :query_response_budget_exceeded}
      :error -> {:error, :query_engine_failure}
    end
  end

  defp settle_size(_response, _budget, _response_codec),
    do: {:error, :query_engine_failure}

  defp validate_page(false, nil), do: :ok

  defp validate_page(true, cursor)
       when is_binary(cursor),
       do: if(Cursor.valid_shape?(cursor), do: :ok, else: {:error, :query_engine_failure})

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
    if Quality.valid?(quality), do: :ok, else: {:error, :query_engine_failure}
  end

  defp encoded_size(term, :native_value) do
    {:ok, NativeValueCodec.encoded_size(term)}
  rescue
    _error -> {:error, :query_engine_failure}
  catch
    _kind, _reason -> {:error, :query_engine_failure}
  end
end
