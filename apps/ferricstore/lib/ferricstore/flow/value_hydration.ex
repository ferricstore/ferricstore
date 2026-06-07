defmodule Ferricstore.Flow.ValueHydration do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.Router

  @value_bin_magic "FSV2"

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
      ctx
      |> ValueStore.raw_mget_with_file_refs(fetchable_refs, file_ref_payload_threshold(max_bytes))
      |> Enum.zip(fetchable_refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

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
      ctx
      |> ValueStore.raw_mget(fetchable_refs)
      |> Enum.zip(fetchable_refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    values_by_record =
      Enum.reduce(ref_entries, %{}, fn {idx, name, ref}, acc ->
        case Map.get(values, ref) do
          value when is_binary(value) ->
            Map.update(acc, idx, %{name => Codec.decode_value(value)}, fn existing ->
              Map.put(existing, name, Codec.decode_value(value))
            end)

          _other ->
            acc
        end
      end)

    records
    |> Enum.with_index()
    |> Enum.map(fn {record, idx} ->
      case Map.get(values_by_record, idx) do
        values when is_map(values) and map_size(values) > 0 -> Map.put(record, :values, values)
        _other -> record
      end
    end)
  end

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
         {:file_ref, _path, _offset, size},
         _max_bytes
       ) do
    record
    |> Map.put(value_omitted_field(kind), true)
    |> Map.put(value_size_field(kind), value_user_size_from_file_size(size))
  end

  defp apply_value_result_for_valid_ref(record, kind, encoded_value, max_bytes)
       when is_binary(encoded_value) do
    {decoded, size} = Codec.decode_value_with_user_size(encoded_value)

    if size <= max_bytes do
      record
      |> Map.put(kind, decoded)
      |> Map.put(value_size_field(kind), size)
    else
      record
      |> Map.put(value_omitted_field(kind), true)
      |> Map.put(value_size_field(kind), size)
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

  defp file_ref_payload_threshold(max_bytes) do
    max_bytes + value_codec_overhead_bytes() + 1
  end

  defp value_codec_overhead_bytes, do: byte_size(@value_bin_magic) + 1

  defp value_user_size_from_file_size(size) when is_integer(size) and size >= 0 do
    max(0, size - value_codec_overhead_bytes())
  end

  defp value_user_size_from_file_size(size), do: size
end
