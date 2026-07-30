defmodule Ferricstore.Commands.Stream.Entries do
  @moduledoc false

  alias Ferricstore.Commands.Stream.ID
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, Router}
  alias Ferricstore.TermCodec

  @max_u64 18_446_744_073_709_551_615
  @range_page_size 4096

  @spec entry_key(binary(), binary()) :: binary()
  def entry_key(stream_key, id_str) do
    CompoundKey.stream_prefix(stream_key) <> id_str
  end

  @doc false
  @spec entry_key_and_id(binary(), non_neg_integer(), non_neg_integer()) ::
          {binary(), binary()}
  def entry_key_and_id(stream_key, ms, seq)
      when is_binary(stream_key) and is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 do
    entry_prefix = CompoundKey.stream_prefix(stream_key)
    ms_binary = Integer.to_string(ms)
    sequence_binary = Integer.to_string(seq)
    entry_key = entry_prefix <> ms_binary <> "-" <> sequence_binary
    id_size = byte_size(ms_binary) + 1 + byte_size(sequence_binary)
    id = binary_part(entry_key, byte_size(entry_prefix), id_size)
    {id, entry_key}
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

  @doc false
  @spec range_page(
          map(),
          binary(),
          :min | ID.stream_id(),
          :max | ID.stream_id(),
          pos_integer()
        ) :: [[binary()]] | {:error, binary()}
  def range_page(store, stream_key, range_start, range_end, count) do
    range_page(store, stream_key, prefix(stream_key), range_start, range_end, count)
  end

  @doc false
  @spec range_page(
          map(),
          binary(),
          binary(),
          :min | ID.stream_id(),
          :max | ID.stream_id(),
          pos_integer()
        ) :: [[binary()]] | {:error, binary()}
  def range_page(store, stream_key, entry_prefix, range_start, range_end, count) do
    cursor = range_cursor(range_start)

    case fetch_single_range_page(store, stream_key, entry_prefix, cursor, count) do
      {:ok, pairs} when is_list(pairs) ->
        decode_range_pairs(pairs, range_start, range_end, [])

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec typed_range_page(
          FerricStore.Instance.t(),
          binary(),
          binary(),
          binary(),
          :min | ID.stream_id(),
          :max | ID.stream_id(),
          pos_integer()
        ) :: {:ok, term(), [[binary()]]} | {:error, term()}
  def typed_range_page(
        store,
        stream_key,
        type_key,
        entry_prefix,
        range_start,
        range_end,
        count
      ) do
    cursor = range_cursor(range_start)

    case Router.stream_typed_range_page(
           store,
           stream_key,
           type_key,
           entry_prefix,
           cursor,
           count
         ) do
      {:ok, {marker, pairs}} when is_list(pairs) ->
        case decode_range_pairs(pairs, range_start, range_end, []) do
          {:error, _reason} = error -> error
          entries -> {:ok, marker, entries}
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec typed_raw_range_page(
          FerricStore.Instance.t(),
          binary(),
          binary(),
          binary(),
          :min | ID.stream_id(),
          :max | ID.stream_id(),
          pos_integer()
        ) :: {:ok, term(), [{binary(), binary()}]} | {:error, term()}
  def typed_raw_range_page(
        store,
        stream_key,
        type_key,
        entry_prefix,
        range_start,
        range_end,
        count
      ) do
    cursor = range_cursor(range_start)

    case Router.stream_typed_range_page(
           store,
           stream_key,
           type_key,
           entry_prefix,
           cursor,
           count
         ) do
      {:ok, {marker, pairs}} when is_list(pairs) ->
        case raw_range_pairs(pairs, range_start, range_end, []) do
          {:error, _reason} = error -> error
          pairs -> {:ok, marker, pairs}
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_single_range_page(
         %FerricStore.Instance{} = store,
         stream_key,
         entry_prefix,
         cursor,
         count
       ) do
    Router.stream_range_page(store, stream_key, entry_prefix, cursor, count)
  end

  defp fetch_single_range_page(store, stream_key, entry_prefix, cursor, count) do
    case fetch_range_page(store, stream_key, entry_prefix, cursor, count) do
      {:ok, {_next_cursor, pairs}} -> {:ok, pairs}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec range_pages(map(), binary(), :min | ID.stream_id(), :max | ID.stream_id()) ::
          [[binary()]] | {:error, binary()}
  def range_pages(store, stream_key, range_start, range_end) do
    range_pages(store, stream_key, prefix(stream_key), range_start, range_end)
  end

  @doc false
  @spec range_pages(map(), binary(), binary(), :min | ID.stream_id(), :max | ID.stream_id()) ::
          [[binary()]] | {:error, binary()}
  def range_pages(store, stream_key, entry_prefix, range_start, range_end) do
    do_range_pages(
      store,
      stream_key,
      entry_prefix,
      range_start,
      range_end,
      range_cursor(range_start),
      []
    )
  end

  @doc false
  @spec reverse_full_range(map(), binary(), non_neg_integer(), non_neg_integer() | :infinity) ::
          [[binary()]] | {:error, binary()}
  def reverse_full_range(_store, _stream_key, _total, 0), do: []
  def reverse_full_range(_store, _stream_key, 0, _count), do: []

  def reverse_full_range(store, stream_key, total, count) do
    reverse_full_range(store, stream_key, prefix(stream_key), total, count)
  end

  @doc false
  @spec reverse_full_range(
          map(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer() | :infinity
        ) :: [[binary()]] | {:error, binary()}
  def reverse_full_range(_store, _stream_key, _entry_prefix, _total, 0), do: []
  def reverse_full_range(_store, _stream_key, _entry_prefix, 0, _count), do: []

  def reverse_full_range(store, stream_key, entry_prefix, total, count) do
    requested = if count == :infinity, do: total, else: min(count, total)
    start = total - requested

    case Ops.compound_scan_slice(store, stream_key, entry_prefix, start, requested, total) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      pairs when is_list(pairs) ->
        decode_reverse_range_pairs(pairs, [])
    end
  end

  @doc false
  @spec typed_reverse_full_range(
          FerricStore.Instance.t(),
          binary(),
          binary(),
          binary(),
          pos_integer(),
          non_neg_integer() | :infinity
        ) :: {:ok, term(), [[binary()]]} | {:error, term()}
  def typed_reverse_full_range(
        store,
        stream_key,
        type_key,
        entry_prefix,
        total,
        count
      )
      when is_integer(total) and total > 0 do
    requested = if count == :infinity, do: total, else: min(count, total)
    start = total - requested

    case Router.stream_typed_reverse_slice(
           store,
           stream_key,
           type_key,
           entry_prefix,
           start,
           requested,
           total
         ) do
      {:ok, {marker, pairs}} when is_list(pairs) ->
        {:ok, marker, decode_reverse_range_pairs(pairs, [])}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec typed_raw_reverse_full_range(
          FerricStore.Instance.t(),
          binary(),
          binary(),
          binary(),
          pos_integer(),
          pos_integer()
        ) :: {:ok, term(), [{binary(), binary()}]} | {:error, term()}
  def typed_raw_reverse_full_range(
        store,
        stream_key,
        type_key,
        entry_prefix,
        total,
        count
      )
      when is_integer(total) and total > 0 and is_integer(count) and count > 0 do
    requested = min(count, total)
    start = total - requested

    case Router.stream_typed_reverse_slice(
           store,
           stream_key,
           type_key,
           entry_prefix,
           start,
           requested,
           total
         ) do
      {:ok, {marker, pairs}} when is_list(pairs) ->
        {:ok, marker, Enum.reverse(pairs)}

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp do_range_pages(store, stream_key, entry_prefix, range_start, range_end, cursor, acc) do
    case fetch_range_page(store, stream_key, entry_prefix, cursor, @range_page_size) do
      {:ok, {next_cursor, pairs}} when is_list(pairs) ->
        page = decode_range_pairs(pairs, range_start, range_end, [])
        acc = Enum.reverse(page, acc)

        if next_cursor == 0 or range_page_reaches_end?(pairs, range_end) do
          Enum.reverse(acc)
        else
          do_range_pages(
            store,
            stream_key,
            entry_prefix,
            range_start,
            range_end,
            next_cursor,
            acc
          )
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_range_page(store, stream_key, entry_prefix, cursor, count) do
    Ops.compound_scan_page(
      store,
      stream_key,
      entry_prefix,
      cursor,
      count,
      nil,
      false
    )
  end

  defp range_page_reaches_end?(_pairs, :max), do: false
  defp range_page_reaches_end?([], _range_end), do: false

  defp range_page_reaches_end?(pairs, range_end) do
    case pairs |> List.last() |> elem(0) |> ID.parse_full_id() do
      {:ok, id} -> ID.compare(id, range_end) != :lt
      {:error, _message} -> true
    end
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
  def decode_fields(<<"FSH2", _rest::binary>> = raw) do
    raw
    |> Ferricstore.Flow.decode_history_fields()
    |> decode_field_list()
  end

  # The common two-field Stream row has one fixed ETF schema. Matching the
  # entire external term keeps trailing bytes and non-binary fields excluded;
  # every other durable shape still uses the generic safe decoder below.
  def decode_fields(
        <<131, 108, 0, 0, 0, 2, 109, field_len::unsigned-big-32, field::binary-size(field_len),
          109, value_len::unsigned-big-32, value::binary-size(value_len), 106>>
      ) do
    {:ok, [field, value]}
  end

  def decode_fields(raw) when is_binary(raw), do: decode_term_fields(raw)

  def decode_fields(_), do: :error

  @doc false
  @spec encode_fields([binary()]) :: binary()
  def encode_fields([field, value]) when is_binary(field) and is_binary(value) do
    <<131, 108, 0, 0, 0, 2, 109, byte_size(field)::unsigned-big-32, field::binary, 109,
      byte_size(value)::unsigned-big-32, value::binary, 106>>
  end

  def encode_fields(fields) do
    # The accepted Stream schema is an ordered list containing only binaries,
    # so ETF has one canonical byte representation without deterministic map
    # sorting. Keep the legacy durable bytes while avoiding that generic work
    # for every appended entry.
    :erlang.term_to_binary(fields)
  end

  defp prefix(stream_key) do
    CompoundKey.stream_prefix(stream_key)
  end

  defp range_cursor(:min), do: 0
  defp range_cursor({0, 0}), do: 0
  defp range_cursor({ms, 0}) when ms > 0, do: {:after, {ms - 1, @max_u64}}
  defp range_cursor({ms, seq}) when seq > 0, do: {:after, {ms, seq - 1}}

  defp raw_range_pairs(pairs, _range_start, :max, _acc), do: pairs

  defp raw_range_pairs([], _range_start, _range_end, acc), do: Enum.reverse(acc)

  defp raw_range_pairs([{id_str, _raw} = pair | rest], range_start, range_end, acc) do
    case ID.parse_full_id(id_str) do
      {:ok, id} ->
        cond do
          ID.in_range?(id, range_start, range_end) ->
            raw_range_pairs(rest, range_start, range_end, [pair | acc])

          ID.compare(id, range_end) == :gt ->
            Enum.reverse(acc)

          true ->
            raw_range_pairs(rest, range_start, range_end, acc)
        end

      {:error, _message} ->
        ReadResult.command_error(ReadResult.failure({:corrupt_stream_id, id_str}))
    end
  end

  defp decode_range_pairs([], _range_start, _range_end, acc), do: Enum.reverse(acc)

  # The catalog cursor already seeks to the numeric lower bound. With an open
  # upper bound every returned catalog member is therefore in range; avoid
  # parsing the same ID again on the common XRANGE/XREVRANGE paths.
  defp decode_range_pairs(
         [
           {id_str,
            <<131, 108, 0, 0, 0, 2, 109, field_len::unsigned-big-32,
              field::binary-size(field_len), 109, value_len::unsigned-big-32,
              value::binary-size(value_len), 106>>}
           | rest
         ],
         _range_start,
         :max,
         acc
       ) do
    decode_range_pairs(rest, :min, :max, [[id_str, field, value] | acc])
  end

  defp decode_range_pairs([{id_str, raw} | rest], _range_start, :max, acc) do
    case decode_fields(raw) do
      {:ok, fields} -> decode_range_pairs(rest, :min, :max, [[id_str | fields] | acc])
      :error -> decode_range_pairs(rest, :min, :max, acc)
    end
  end

  defp decode_range_pairs([{id_str, raw} | rest], range_start, range_end, acc) do
    case ID.parse_full_id(id_str) do
      {:ok, id} ->
        cond do
          not ID.in_range?(id, range_start, range_end) ->
            if range_end != :max and ID.compare(id, range_end) == :gt do
              Enum.reverse(acc)
            else
              decode_range_pairs(rest, range_start, range_end, acc)
            end

          true ->
            case decode_fields(raw) do
              {:ok, fields} ->
                decode_range_pairs(rest, range_start, range_end, [[id_str | fields] | acc])

              :error ->
                decode_range_pairs(rest, range_start, range_end, acc)
            end
        end

      {:error, _message} ->
        ReadResult.command_error(ReadResult.failure({:corrupt_stream_id, id_str}))
    end
  end

  defp decode_reverse_range_pairs([], acc), do: acc

  defp decode_reverse_range_pairs(
         [
           {id_str,
            <<131, 108, 0, 0, 0, 2, 109, field_len::unsigned-big-32,
              field::binary-size(field_len), 109, value_len::unsigned-big-32,
              value::binary-size(value_len), 106>>}
           | rest
         ],
         acc
       ) do
    decode_reverse_range_pairs(rest, [[id_str, field, value] | acc])
  end

  defp decode_reverse_range_pairs([{id_str, raw} | rest], acc) do
    case decode_fields(raw) do
      {:ok, fields} -> decode_reverse_range_pairs(rest, [[id_str | fields] | acc])
      :error -> decode_reverse_range_pairs(rest, acc)
    end
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

  defp decode_field_list([field, value] = fields) when is_binary(field) and is_binary(value),
    do: {:ok, fields}

  defp decode_field_list([_field, _value]), do: :error

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
