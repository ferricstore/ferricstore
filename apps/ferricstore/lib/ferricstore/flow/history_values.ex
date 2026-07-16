defmodule Ferricstore.Flow.HistoryValues do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.ValueStore
  alias Ferricstore.Store.Router

  def hydrate(history, _ctx, %{enabled?: false}), do: history
  def hydrate([], _ctx, _value_return), do: []

  def hydrate(history, ctx, %{enabled?: true, max_bytes: max_bytes}) do
    refs =
      history
      |> Enum.flat_map(fn {_event_id, fields} ->
        ["payload_ref", "result_ref", "error_ref"]
        |> Enum.map(&Map.get(fields, &1))
      end)
      |> Enum.uniq()
      |> Enum.filter(fn ref ->
        is_binary(ref) and ref != "" and byte_size(ref) <= Router.max_key_size()
      end)

    values =
      ctx
      |> ValueStore.raw_mget_with_file_refs(refs, file_ref_payload_threshold(max_bytes))
      |> Enum.zip(refs)
      |> Map.new(fn {value, ref} -> {ref, value} end)

    Enum.map(history, fn {event_id, fields} ->
      hydrated =
        Enum.reduce(["payload", "result", "error"], fields, fn kind, acc ->
          ref = Map.get(acc, kind <> "_ref")
          apply_value_result(acc, kind, ref, Map.get(values, ref), max_bytes)
        end)

      {event_id, hydrated}
    end)
  end

  @doc false
  def __apply_value_result_for_test__(fields, kind, ref, value, max_bytes),
    do: apply_value_result(fields, kind, ref, value, max_bytes)

  defp apply_value_result(fields, _kind, nil, _value, _max_bytes), do: fields
  defp apply_value_result(fields, _kind, "", _value, _max_bytes), do: fields

  defp apply_value_result(fields, kind, ref, value, max_bytes) when is_binary(ref) do
    if byte_size(ref) > Router.max_key_size() do
      Map.put(fields, kind <> "_error", "ERR #{kind}_ref key too large")
    else
      apply_value_result_for_valid_ref(fields, kind, value, max_bytes)
    end
  end

  defp apply_value_result(fields, _kind, _ref, _value, _max_bytes), do: fields

  defp apply_value_result_for_valid_ref(fields, kind, nil, _max_bytes) do
    fields
    |> Map.put(kind, nil)
    |> Map.put(kind <> "_missing", true)
  end

  defp apply_value_result_for_valid_ref(
         fields,
         kind,
         {:error, {:storage_read_failed, _reason}},
         _max_bytes
       ) do
    Map.put(fields, kind <> "_error", "ERR storage read failed")
  end

  defp apply_value_result_for_valid_ref(
         fields,
         kind,
         {:file_ref, _path, _offset, size},
         _max_bytes
       ) do
    fields
    |> Map.put(kind <> "_omitted", true)
    |> Map.put(kind <> "_size", size)
  end

  defp apply_value_result_for_valid_ref(fields, kind, encoded_value, max_bytes)
       when is_binary(encoded_value) and byte_size(encoded_value) > max_bytes do
    fields
    |> Map.put(kind <> "_omitted", true)
    |> Map.put(kind <> "_size", byte_size(encoded_value))
  end

  defp apply_value_result_for_valid_ref(fields, kind, encoded_value, _max_bytes)
       when is_binary(encoded_value) do
    case Codec.decode_value_result(encoded_value) do
      {:ok, decoded} ->
        fields
        |> Map.put(kind, decoded)
        |> Map.put(kind <> "_size", byte_size(encoded_value))

      {:error, :invalid_flow_value} ->
        Map.put(fields, kind <> "_error", "ERR invalid flow value")
    end
  end

  defp apply_value_result_for_valid_ref(fields, _kind, _value, _max_bytes), do: fields

  defp file_ref_payload_threshold(max_bytes) when max_bytes < 1, do: 1
  defp file_ref_payload_threshold(max_bytes), do: max_bytes + 1
end
