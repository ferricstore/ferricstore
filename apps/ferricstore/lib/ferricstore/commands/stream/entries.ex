defmodule Ferricstore.Commands.Stream.Entries do
  @moduledoc false

  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult}
  alias Ferricstore.TermCodec

  @spec entry_key(binary(), binary()) :: binary()
  def entry_key(stream_key, id_str) do
    CompoundKey.stream_prefix(stream_key) <> id_str
  end

  @spec delete_keys(binary(), [binary()]) :: [binary()]
  def delete_keys(stream_key, ids) do
    prefix = prefix(stream_key)
    Enum.map(ids, &(prefix <> &1))
  end

  @spec existing_ids([binary()], [term()], [binary()]) :: [binary()]
  def existing_ids([_id | ids], [nil | raws], acc) do
    existing_ids(ids, raws, acc)
  end

  def existing_ids([id | ids], [_raw | raws], acc) do
    existing_ids(ids, raws, [id | acc])
  end

  def existing_ids(_ids, _raws, acc), do: Enum.reverse(acc)

  @spec put(map(), binary(), binary(), binary()) :: :ok | {:error, term()}
  def put(store, stream_key, compound_key, encoded) do
    if Ops.has_compound?(store) do
      Ops.compound_put(store, stream_key, compound_key, encoded, 0)
    else
      Ops.put(store, compound_key, encoded, 0)
    end
  end

  @spec batch_get(map(), binary(), [binary()]) :: [term()]
  def batch_get(store, stream_key, compound_keys) do
    values =
      if Ops.has_compound?(store) do
        Ops.compound_batch_get(store, stream_key, compound_keys)
      else
        Ops.batch_get(store, compound_keys)
      end

    normalize_batch_cardinality(values, compound_keys)
  end

  @spec delete(map(), binary(), [binary()]) :: :ok | {:error, term()}
  def delete(_store, _stream_key, []), do: :ok

  def delete(store, stream_key, compound_keys) do
    if Ops.has_compound?(store) do
      Ops.compound_batch_delete(store, stream_key, compound_keys)
    else
      Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
        case Ops.delete(store, compound_key) do
          :ok -> {:cont, :ok}
          {:error, _} = error -> {:halt, error}
        end
      end)
    end
  end

  @spec scan(map(), binary()) :: [term()] | ReadResult.failure()
  def scan(store, stream_key) do
    Ops.compound_scan(store, stream_key, prefix(stream_key))
  end

  @spec ids_for(map(), binary()) :: [binary()] | ReadResult.failure()
  def ids_for(store, stream_key) do
    fields_for(store, stream_key)
  end

  @spec fields_for(map(), binary()) :: [binary()] | ReadResult.failure()
  def fields_for(store, stream_key) do
    Ops.compound_fields(store, stream_key, prefix(stream_key))
  end

  @spec count(map(), binary()) :: non_neg_integer() | ReadResult.failure()
  def count(store, stream_key) do
    Ops.compound_count(store, stream_key, prefix(stream_key))
  end

  @spec decode_entry(binary(), term()) :: [binary()] | nil
  def decode_entry(_id, nil), do: nil

  def decode_entry(id, raw) do
    case decode_fields(raw) do
      {:ok, fields} -> [id | fields]
      :error -> nil
    end
  end

  @spec decode_indexed([{binary(), binary()}], binary(), map()) ::
          [[binary()]] | {:error, binary()}
  def decode_indexed([], _stream_key, _store), do: []

  def decode_indexed(index_entries, stream_key, store) do
    {compound_keys, ids} = indexed_keys_and_ids(index_entries, [], [])
    raw_values = batch_get(store, stream_key, compound_keys)

    case ReadResult.first_failure(raw_values) do
      nil -> decode_indexed_raw(ids, raw_values, [])
      failure -> ReadResult.command_error(failure)
    end
  end

  @spec decode_fields(term()) :: {:ok, [binary()]} | :error
  def decode_fields(raw) when is_binary(raw) do
    case Ferricstore.Flow.decode_history_fields(raw) do
      [_ | _] = fields -> decode_field_list(fields)
      _ -> decode_term_fields(raw)
    end
  end

  def decode_fields(_), do: :error

  defp prefix(stream_key) do
    CompoundKey.stream_prefix(stream_key)
  end

  defp indexed_keys_and_ids([], compound_keys, ids) do
    {Enum.reverse(compound_keys), Enum.reverse(ids)}
  end

  defp indexed_keys_and_ids([{id_str, compound_key} | rest], compound_keys, ids) do
    indexed_keys_and_ids(rest, [compound_key | compound_keys], [id_str | ids])
  end

  defp decode_indexed_raw([id_str | ids], [raw | raws], acc) when is_binary(raw) do
    case decode_fields(raw) do
      {:ok, fields} -> decode_indexed_raw(ids, raws, [[id_str | fields] | acc])
      :error -> decode_indexed_raw(ids, raws, acc)
    end
  end

  defp decode_indexed_raw([_id_str | ids], [_raw | raws], acc) do
    decode_indexed_raw(ids, raws, acc)
  end

  defp decode_indexed_raw(_ids, _raws, acc) do
    Enum.reverse(acc)
  end

  defp decode_term_fields(raw) do
    case TermCodec.decode(raw) do
      {:ok, fields} when is_list(fields) -> decode_field_list(fields)
      _other -> :error
    end
  end

  defp decode_field_list([_field, _value | _rest] = fields) do
    if valid_field_pairs?(fields), do: {:ok, fields}, else: :error
  end

  defp decode_field_list(_fields), do: :error

  defp normalize_batch_cardinality(values, compound_keys) when is_list(values) do
    if same_length?(values, compound_keys) do
      values
    else
      batch_cardinality_failures(compound_keys)
    end
  end

  defp normalize_batch_cardinality(_values, compound_keys),
    do: batch_cardinality_failures(compound_keys)

  defp batch_cardinality_failures(compound_keys) do
    List.duplicate(ReadResult.failure(:batch_result_length_mismatch), length(compound_keys))
  end

  defp same_length?([], []), do: true
  defp same_length?([_ | values], [_ | keys]), do: same_length?(values, keys)
  defp same_length?(_values, _keys), do: false

  defp valid_field_pairs?([]), do: true

  defp valid_field_pairs?([field, value | rest]) when is_binary(field) and is_binary(value),
    do: valid_field_pairs?(rest)

  defp valid_field_pairs?(_fields), do: false
end
