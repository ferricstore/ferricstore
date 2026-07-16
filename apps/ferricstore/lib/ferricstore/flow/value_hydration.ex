defmodule Ferricstore.Flow.ValueHydration do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.Router

  def payload_result(_ctx, {:ok, nil}, _payload_return), do: {:ok, nil}

  def payload_result(ctx, {:ok, record}, payload_return) when is_map(record) do
    {:ok, hd(payload_records(ctx, [record], payload_return))}
  end

  def payload_result(_ctx, other, _payload_return), do: other

  def payload_records(_ctx, records, %{enabled?: false}), do: records
  def payload_records(_ctx, [], _payload_return), do: []

  def payload_records(ctx, records, %{enabled?: true, max_bytes: max_bytes}) do
    ref_entries =
      records
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, idx} ->
        [
          {idx, :payload, Map.get(record, :payload_ref)},
          {idx, :result, Map.get(record, :result_ref)},
          {idx, :error, Map.get(record, :error_ref)}
        ]
      end)

    fetchable_refs =
      ref_entries
      |> Enum.map(fn {_idx, _kind, ref} -> ref end)
      |> Enum.uniq()
      |> Enum.filter(fn ref ->
        is_binary(ref) and ref != "" and byte_size(ref) <= Router.max_key_size()
      end)

    values =
      fetchable_refs
      |> then(fn refs ->
        ValueStore.raw_mget_with_file_refs(ctx, refs, file_ref_payload_threshold(max_bytes))
      end)
      |> map_fetched_values(fetchable_refs)

    Enum.map(records, fn record ->
      Enum.reduce([:payload, :result, :error], record, fn kind, acc ->
        ref = Map.get(acc, value_ref_field(kind))
        apply_value_result(acc, kind, ref, Map.get(values, ref), max_bytes)
      end)
    end)
  end

  def named_value_result({:ok, nil}, _ctx, _names), do: {:ok, nil}

  def named_value_result({:ok, record}, ctx, names) when is_map(record) do
    {:ok, hd(named_value_records(ctx, [record], names))}
  end

  def named_value_result(other, _ctx, _names), do: other

  def named_value_records(_ctx, records, nil), do: records
  def named_value_records(_ctx, [], _names), do: []

  def named_value_records(ctx, records, :all) do
    names =
      records
      |> Enum.flat_map(fn record -> Map.keys(Codec.flow_record_value_refs(record)) end)
      |> Enum.uniq()

    named_value_records(ctx, records, names)
  end

  def named_value_records(_ctx, records, []), do: records

  def named_value_records(ctx, records, names) when is_list(names) do
    ref_entries =
      records
      |> Enum.with_index()
      |> Enum.flat_map(fn {record, idx} ->
        refs = Codec.flow_record_value_refs(record)

        Enum.flat_map(names, fn name ->
          case Map.get(refs, name) do
            %{ref: ref} when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            %{"ref" => ref} when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            ref when is_binary(ref) and ref != "" -> [{idx, name, ref}]
            _other -> []
          end
        end)
      end)

    fetchable_refs =
      ref_entries
      |> Enum.map(fn {_idx, _name, ref} -> ref end)
      |> Enum.uniq()
      |> Enum.filter(fn ref -> byte_size(ref) <= Router.max_key_size() end)

    values =
      fetchable_refs
      |> then(&ValueStore.raw_mget(ctx, &1))
      |> map_fetched_values(fetchable_refs)

    hydrate_named_values(records, ref_entries, values)
  end

  @doc false
  def __map_fetched_values_for_test__(refs, results), do: map_fetched_values(results, refs)

  defp map_fetched_values(results, refs) do
    case BatchResult.map_exact(refs, results, fn ref, value -> {ref, value} end) do
      {:ok, entries} ->
        Map.new(entries)

      {:error, _reason} ->
        Map.new(refs, fn ref ->
          {ref, {:error, {:storage_read_failed, :batch_result_mismatch}}}
        end)
    end
  end

  @doc false
  def __hydrate_named_values_for_test__(records, ref_entries, values),
    do: hydrate_named_values(records, ref_entries, values)

  defp hydrate_named_values(records, ref_entries, values) do
    decoded_values = Map.new(values, fn {ref, value} -> {ref, decode_named_value(value)} end)

    {values_by_record, errors_by_record} =
      Enum.reduce(ref_entries, {%{}, %{}}, fn {idx, name, ref}, {value_acc, error_acc} ->
        case Map.get(decoded_values, ref, :missing) do
          {:ok, value} ->
            next_values =
              Map.update(value_acc, idx, %{name => value}, &Map.put(&1, name, value))

            {next_values, error_acc}

          {:error, message} ->
            {value_acc, Map.put_new(error_acc, idx, message)}

          :missing ->
            {value_acc, error_acc}
        end
      end)

    records
    |> Enum.with_index()
    |> Enum.map(fn {record, idx} ->
      record
      |> maybe_put_named_values(Map.get(values_by_record, idx))
      |> maybe_put_named_values_error(Map.get(errors_by_record, idx))
    end)
  end

  defp decode_named_value(value) when is_binary(value) do
    case Codec.decode_value_result(value) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, :invalid_flow_value} -> {:error, "ERR invalid flow value"}
    end
  end

  defp decode_named_value({:error, {:storage_read_failed, _reason}}),
    do: {:error, "ERR storage read failed"}

  defp decode_named_value(_value), do: :missing

  defp maybe_put_named_values(record, values) when is_map(values) and map_size(values) > 0,
    do: Map.put(record, :values, values)

  defp maybe_put_named_values(record, _values), do: record

  defp maybe_put_named_values_error(record, nil), do: record
  defp maybe_put_named_values_error(record, message), do: Map.put(record, :values_error, message)

  @doc false
  def __apply_value_result_for_test__(record, kind, ref, value, max_bytes),
    do: apply_value_result(record, kind, ref, value, max_bytes)

  defp apply_value_result(record, _kind, nil, _value, _max_bytes), do: record
  defp apply_value_result(record, _kind, "", _value, _max_bytes), do: record

  defp apply_value_result(record, kind, ref, value, max_bytes) when is_binary(ref) do
    if byte_size(ref) > Router.max_key_size() do
      Map.put(record, value_error_field(kind), "ERR #{kind}_ref key too large")
    else
      apply_value_result_for_valid_ref(record, kind, value, max_bytes)
    end
  end

  defp apply_value_result(record, _kind, _ref, _other, _max_bytes), do: record

  defp apply_value_result_for_valid_ref(record, kind, nil, _max_bytes) do
    record
    |> Map.put(kind, nil)
    |> Map.put(value_missing_field(kind), true)
  end

  defp apply_value_result_for_valid_ref(
         record,
         kind,
         {:error, {:storage_read_failed, _reason}},
         _max_bytes
       ) do
    Map.put(record, value_error_field(kind), "ERR storage read failed")
  end

  defp apply_value_result_for_valid_ref(
         record,
         kind,
         {:file_ref, _path, _offset, size},
         _max_bytes
       ) do
    record
    |> Map.put(value_omitted_field(kind), true)
    |> Map.put(value_size_field(kind), size)
  end

  defp apply_value_result_for_valid_ref(record, kind, encoded_value, max_bytes)
       when is_binary(encoded_value) and byte_size(encoded_value) > max_bytes do
    record
    |> Map.put(value_omitted_field(kind), true)
    |> Map.put(value_size_field(kind), byte_size(encoded_value))
  end

  defp apply_value_result_for_valid_ref(record, kind, encoded_value, _max_bytes)
       when is_binary(encoded_value) do
    case Codec.decode_value_result(encoded_value) do
      {:ok, decoded} ->
        record
        |> Map.put(kind, decoded)
        |> Map.put(value_size_field(kind), byte_size(encoded_value))

      {:error, :invalid_flow_value} ->
        Map.put(record, value_error_field(kind), "ERR invalid flow value")
    end
  end

  defp apply_value_result_for_valid_ref(record, _kind, _other, _max_bytes), do: record

  defp value_ref_field(:payload), do: :payload_ref
  defp value_ref_field(:result), do: :result_ref
  defp value_ref_field(:error), do: :error_ref

  defp value_error_field(:payload), do: :payload_error
  defp value_error_field(:result), do: :result_error
  defp value_error_field(:error), do: :error_error

  defp value_missing_field(:payload), do: :payload_missing
  defp value_missing_field(:result), do: :result_missing
  defp value_missing_field(:error), do: :error_missing

  defp value_omitted_field(:payload), do: :payload_omitted
  defp value_omitted_field(:result), do: :result_omitted
  defp value_omitted_field(:error), do: :error_omitted

  defp value_size_field(:payload), do: :payload_size
  defp value_size_field(:result), do: :result_size
  defp value_size_field(:error), do: :error_size

  defp file_ref_payload_threshold(max_bytes) when max_bytes < 1, do: 1
  defp file_ref_payload_threshold(max_bytes), do: max_bytes + 1
end
