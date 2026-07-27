defmodule Ferricstore.Flow.Query.ResultCodec do
  @moduledoc false

  import Bitwise

  alias Ferricstore.Flow.Query.{Cursor, Limits, Quality, Usage}
  alias Ferricstore.NativeValueCodec

  @tag 0xA0
  @name "flow_query_result_v1"
  @contract "ferric.flow.query.result/v1"
  @page_kind 0
  @count_kind 1
  @nil_length 0xFFFF_FFFF
  @maximum_native_integer 0x7FFF_FFFF_FFFF_FFFF
  @maximum_records Limits.max_results()

  @quality_fields Quality.fields()

  @usage_fields Usage.fields()

  @record_fields [
    :id,
    :type,
    :state,
    :version,
    :priority,
    :partition_key,
    :created_at_ms,
    :updated_at_ms,
    :next_run_at_ms,
    :lease_deadline_ms,
    :attempts,
    :run_state,
    :max_active_ms,
    :parent_flow_id,
    :root_flow_id,
    :correlation_id,
    :attributes,
    :state_meta,
    :event_id,
    :fields
  ]
  @record_atom_indexes @record_fields |> Enum.with_index() |> Map.new()
  @record_string_indexes Map.new(@record_atom_indexes, fn {field, index} ->
                           {Atom.to_string(field), index}
                         end)

  @common_bytes 1 + 1 + length(@quality_fields) + 8 * length(@usage_fields)

  @spec tag() :: 0xA0
  def tag, do: @tag

  @doc false
  @spec name() :: binary()
  def name, do: @name

  @doc false
  @spec contract() :: binary()
  def contract, do: @contract

  @doc false
  @spec record_fields() :: [atom()]
  def record_fields, do: @record_fields

  @doc false
  @spec quality_fields() :: [atom()]
  def quality_fields, do: @quality_fields

  @doc false
  @spec usage_fields() :: [atom()]
  def usage_fields, do: @usage_fields

  @spec encode(term()) :: binary() | nil
  def encode(response) do
    case encode_with_size(response) do
      {:ok, payload, _bytes} -> payload
      :error -> nil
    end
  end

  @doc false
  @spec encode_with_size(term()) :: {:ok, binary(), non_neg_integer()} | :error
  def encode_with_size(response) do
    case encode_result(response) do
      {:ok, payload} ->
        encoded = IO.iodata_to_binary(payload)
        {:ok, encoded, byte_size(encoded)}

      :error ->
        :error
    end
  rescue
    _error -> :error
  catch
    _kind, _reason -> :error
  end

  @spec encoded_size(term()) :: non_neg_integer() | nil
  def encoded_size(response) do
    case measure_result(response) do
      {:ok, bytes} -> bytes
      :error -> nil
    end
  rescue
    _error -> nil
  catch
    _kind, _reason -> nil
  end

  defp encode_result(response) when is_map(response) and map_size(response) == 5,
    do: encode_page(response)

  defp encode_result(response) when is_map(response) and map_size(response) == 4,
    do: encode_count(response)

  defp encode_result(_response), do: :error

  defp measure_result(response) when is_map(response) and map_size(response) == 5,
    do: measure_page(response)

  defp measure_result(response) when is_map(response) and map_size(response) == 4,
    do: measure_count(response)

  defp measure_result(_response), do: :error

  defp measure_page(response) do
    with {:ok, @contract} <- fetch_exact(response, :version),
         {:ok, records} when is_list(records) <- fetch_exact(response, :records),
         {:ok, page} when is_map(page) and map_size(page) == 2 <- fetch_exact(response, :page),
         {:ok, quality} when is_map(quality) <- fetch_exact(response, :quality),
         {:ok, usage} when is_map(usage) <- fetch_exact(response, :usage),
         {:ok, _quality_payload} <- encode_quality(quality),
         {:ok, _usage_values} <- usage_values(usage, {:records, length(records)}),
         {:ok, _page_payload, page_bytes} <- encode_page_metadata(page),
         {:ok, records_bytes} <- measure_records(records) do
      {:ok, @common_bytes + page_bytes + 4 + records_bytes}
    else
      _invalid -> :error
    end
  end

  defp measure_count(response) do
    with {:ok, @contract} <- fetch_exact(response, :version),
         {:ok, result} when is_map(result) and map_size(result) == 2 <-
           fetch_exact(response, :result),
         {:ok, "count"} <- fetch_exact(result, :kind),
         {:ok, count}
         when is_integer(count) and count >= 0 and count <= @maximum_native_integer <-
           fetch_exact(result, :value),
         {:ok, quality} when is_map(quality) <- fetch_exact(response, :quality),
         {:ok, usage} when is_map(usage) <- fetch_exact(response, :usage),
         {:ok, _quality_payload} <- encode_quality(quality),
         {:ok, _usage_values} <- usage_values(usage, :count) do
      {:ok, @common_bytes + 8}
    else
      _invalid -> :error
    end
  end

  defp encode_page(response) do
    with {:ok, @contract} <- fetch_exact(response, :version),
         {:ok, records} when is_list(records) <- fetch_exact(response, :records),
         {:ok, page} when is_map(page) and map_size(page) == 2 <- fetch_exact(response, :page),
         {:ok, quality} when is_map(quality) <- fetch_exact(response, :quality),
         {:ok, usage} when is_map(usage) <- fetch_exact(response, :usage),
         {:ok, quality_payload} <- encode_quality(quality),
         {:ok, usage_values} <- usage_values(usage, {:records, length(records)}),
         {:ok, page_payload, page_bytes} <- encode_page_metadata(page),
         {:ok, records_payload, record_count, records_bytes} <- encode_records(records) do
      payload_bytes = @common_bytes + page_bytes + 4 + records_bytes

      {:ok,
       [
         <<@tag, @page_kind>>,
         quality_payload,
         encode_usage(usage_values, payload_bytes),
         page_payload,
         <<record_count::unsigned-32>>,
         records_payload
       ]}
    else
      _invalid -> :error
    end
  end

  defp encode_count(response) do
    with {:ok, @contract} <- fetch_exact(response, :version),
         {:ok, result} when is_map(result) and map_size(result) == 2 <-
           fetch_exact(response, :result),
         {:ok, "count"} <- fetch_exact(result, :kind),
         {:ok, count}
         when is_integer(count) and count >= 0 and count <= @maximum_native_integer <-
           fetch_exact(result, :value),
         {:ok, quality} when is_map(quality) <- fetch_exact(response, :quality),
         {:ok, usage} when is_map(usage) <- fetch_exact(response, :usage),
         {:ok, quality_payload} <- encode_quality(quality),
         {:ok, usage_values} <- usage_values(usage, :count) do
      payload_bytes = @common_bytes + 8

      {:ok,
       [
         <<@tag, @count_kind>>,
         quality_payload,
         encode_usage(usage_values, payload_bytes),
         <<count::unsigned-64>>
       ]}
    else
      _invalid -> :error
    end
  end

  defp encode_quality(quality), do: Quality.encode(quality)

  defp usage_values(usage, shape) when map_size(usage) == length(@usage_fields) do
    @usage_fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, acc} ->
      case fetch_exact(usage, field) do
        {:ok, value}
        when is_integer(value) and value >= 0 and value <= @maximum_native_integer ->
          {:cont, {:ok, [value | acc]}}

        _invalid ->
          {:halt, :error}
      end
    end)
    |> case do
      {:ok, reversed} ->
        values = Enum.reverse(reversed)

        canonical =
          if Enum.all?(@usage_fields, &Map.has_key?(usage, &1)),
            do: usage,
            else: Map.new(Enum.zip(@usage_fields, values))

        if usage_contract_valid?(canonical, shape), do: {:ok, values}, else: :error

      :error ->
        :error
    end
  end

  defp usage_values(_usage, _kind), do: :error

  defp usage_contract_valid?(usage, {:records, record_count})
       when is_integer(record_count) and record_count >= 0,
       do: Usage.contract_valid?(usage, :records) and usage.result_records == record_count

  defp usage_contract_valid?(usage, :count), do: Usage.contract_valid?(usage, :count)
  defp usage_contract_valid?(_usage, _shape), do: false

  defp encode_usage(values, response_bytes) do
    @usage_fields
    |> Enum.zip(values)
    |> Enum.map(fn
      {:response_bytes, _previous} -> <<response_bytes::unsigned-64>>
      {_field, value} -> <<value::unsigned-64>>
    end)
  end

  defp encode_page_metadata(page) do
    with {:ok, has_more} when is_boolean(has_more) <- fetch_exact(page, :has_more),
         {:ok, cursor} <- fetch_exact(page, :cursor) do
      encode_page_metadata(has_more, cursor)
    else
      _invalid -> :error
    end
  end

  defp encode_page_metadata(false, nil),
    do: {:ok, <<0, @nil_length::unsigned-32>>, 5}

  defp encode_page_metadata(true, cursor)
       when is_binary(cursor) do
    if Cursor.valid_shape?(cursor) do
      cursor_bytes = byte_size(cursor)
      {:ok, [<<1, cursor_bytes::unsigned-32>>, cursor], 5 + cursor_bytes}
    else
      :error
    end
  end

  defp encode_page_metadata(_has_more, _cursor), do: :error

  defp encode_records(records) when length(records) <= @maximum_records do
    records
    |> Enum.reduce_while({:ok, [], 0}, fn record, {:ok, acc, bytes} ->
      case encode_record(record) do
        {:ok, payload, record_bytes} ->
          {:cont, {:ok, [payload | acc], bytes + record_bytes}}

        :error ->
          {:halt, :error}
      end
    end)
    |> case do
      {:ok, reversed, bytes} -> {:ok, Enum.reverse(reversed), length(records), bytes}
      :error -> :error
    end
  end

  defp encode_records(_records), do: :error

  defp measure_records(records) when length(records) <= @maximum_records do
    Enum.reduce_while(records, {:ok, 0}, fn record, {:ok, bytes} ->
      case measure_record(record) do
        {:ok, record_bytes} -> {:cont, {:ok, bytes + record_bytes}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp measure_records(_records), do: :error

  defp measure_record(record)
       when is_map(record) and map_size(record) <= length(@record_fields) do
    record
    |> Enum.reduce_while({:ok, 0, 4}, fn {field, value}, {:ok, bitmap, bytes} ->
      case record_field_index(field) do
        {:ok, index} when band(bitmap, 1 <<< index) == 0 ->
          {:cont, {:ok, bitmap ||| 1 <<< index, bytes + NativeValueCodec.encoded_size(value)}}

        _unknown_or_duplicate ->
          {:halt, :error}
      end
    end)
    |> case do
      {:ok, _bitmap, bytes} -> {:ok, bytes}
      :error -> :error
    end
  end

  defp measure_record(_record), do: :error

  defp encode_record(record) when is_map(record) and map_size(record) <= length(@record_fields) do
    record
    |> Enum.reduce_while({:ok, 0, [], 0}, fn {field, value}, {:ok, bitmap, values, bytes} ->
      case record_field_index(field) do
        {:ok, index} when band(bitmap, 1 <<< index) == 0 ->
          encoded = NativeValueCodec.encode(value)

          {:cont,
           {:ok, bitmap ||| 1 <<< index, [{index, encoded} | values], bytes + byte_size(encoded)}}

        _unknown_or_duplicate ->
          {:halt, :error}
      end
    end)
    |> case do
      {:ok, bitmap, indexed_values, bytes} ->
        values = indexed_values |> Enum.sort() |> Enum.map(&elem(&1, 1))
        {:ok, [<<bitmap::unsigned-32>>, values], bytes + 4}

      _invalid ->
        :error
    end
  end

  defp encode_record(_record), do: :error

  defp record_field_index(field) when is_atom(field), do: Map.fetch(@record_atom_indexes, field)

  defp record_field_index(field) when is_binary(field),
    do: Map.fetch(@record_string_indexes, field)

  defp record_field_index(_field), do: :error

  defp fetch_exact(map, field) do
    string_field = Atom.to_string(field)

    case {Map.fetch(map, field), Map.fetch(map, string_field)} do
      {{:ok, _atom_value}, {:ok, _string_value}} -> :error
      {{:ok, value}, :error} -> {:ok, value}
      {:error, {:ok, value}} -> {:ok, value}
      {:error, :error} -> :missing
    end
  end
end
