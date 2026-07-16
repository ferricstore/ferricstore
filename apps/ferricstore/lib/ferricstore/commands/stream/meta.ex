defmodule Ferricstore.Commands.Stream.Meta do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, Entries, Groups, ID, Index}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, TypeRegistry}
  alias Ferricstore.TermCodec

  @meta_table Ferricstore.Stream.Meta

  @spec ensure_read_type(binary(), map()) :: :ok | {:error, binary()}
  def ensure_read_type(key, store) do
    ensure_table()

    case lookup_local(key, store) do
      [_entry] ->
        ensure_live(key, store)

      [] ->
        if Ops.has_compound?(store),
          do: TypeRegistry.command_check_type(key, :stream, store),
          else: :ok
    end
  end

  @spec entries(binary(), map()) :: [tuple()] | ReadResult.failure()
  def entries(key, store) do
    ensure_table()

    case lookup_local(key, store) do
      [] -> rebuild_entries(key, store)
      entries -> entries
    end
  end

  @spec xadd_entries(binary(), map()) :: [tuple()] | ReadResult.failure()
  def xadd_entries(key, store) do
    ensure_table()

    case lookup_local(key, store) do
      [] ->
        case type_marker_status(key, store) do
          {:ok, true} -> rebuild_entries(key, store, true)
          {:ok, false} -> []
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end

      entries ->
        entries
    end
  end

  @spec type_marker_status(binary(), map()) :: {:ok, boolean()} | ReadResult.failure()
  def type_marker_status(key, store) do
    if Ops.has_compound?(store) do
      case Ops.compound_get(store, key, CompoundKey.type_key(key)) do
        {:error, {:storage_read_failed, _reason}} = failure -> failure
        "stream" -> {:ok, true}
        _missing_or_other_type -> {:ok, false}
      end
    else
      {:ok, false}
    end
  end

  @spec durable_entry(binary(), map()) ::
          {non_neg_integer(), binary(), binary(), non_neg_integer(), non_neg_integer()}
          | nil
          | ReadResult.failure()
  def durable_entry(key, store) do
    case Ops.compound_get(store, key, CompoundKey.stream_meta_key(key)) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      raw -> decode(raw)
    end
  end

  @spec put_local(
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) :: true
  def put_local(key, len, first, last, ms, seq) do
    put_local(key, len, first, last, ms, seq, nil)
  end

  @spec put_local(
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          term()
        ) :: true
  def put_local(key, len, first, last, ms, seq, store) do
    ensure_table()
    :ets.insert(@meta_table, {CacheKey.build(store, key), len, first, last, ms, seq})
  end

  @spec put(
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          map()
        ) ::
          :ok | {:error, term()}
  def put(key, len, first, last, ms, seq, store) do
    case persist(key, len, first, last, ms, seq, store) do
      result when result in [:ok, true] ->
        put_local(key, len, first, last, ms, seq, store)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  @spec cleanup_local(binary()) :: true
  def cleanup_local(stream_key), do: cleanup_local(stream_key, nil)

  @spec cleanup_local(binary(), term()) :: true
  def cleanup_local(stream_key, store) do
    ensure_table()
    :ets.delete(@meta_table, CacheKey.build(store, stream_key))
    Groups.delete_local(stream_key, store)
    Index.clear(stream_key, store)
  end

  defp ensure_live(key, store) do
    if Ops.has_compound?(store) do
      case type_marker_status(key, store) do
        {:ok, true} ->
          :ok

        {:ok, false} ->
          case TypeRegistry.command_get_type(key, store) do
            "none" ->
              cleanup_local(key, store)
              :ok

            {:error, _reason} = error ->
              error

            _other_type ->
              TypeRegistry.command_check_type(key, :stream, store)
          end

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)
      end
    else
      :ok
    end
  end

  defp rebuild_entries(key, store) do
    case type_marker_status(key, store) do
      {:ok, marker_present?} -> rebuild_entries(key, store, marker_present?)
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  defp rebuild_entries(key, store, marker_present?) do
    if Ops.has_compound?(store) do
      case Entries.fields_for(store, key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        fields when is_list(fields) ->
          case sorted_valid_ids(fields) do
            {:ok, ids} ->
              rebuild_from_fields(key, store, ids, marker_present?)

            {:error, {:storage_read_failed, _reason}} = failure ->
              ReadResult.command_error(failure)
          end
      end
    else
      []
    end
  end

  defp sorted_valid_ids(ids) do
    ids
    |> Enum.reduce_while({:ok, []}, fn id, {:ok, parsed} ->
      case ID.parse_full_id(id) do
        {:ok, parsed_id} -> {:cont, {:ok, [{parsed_id, id} | parsed]}}
        {:error, _message} -> {:halt, ReadResult.failure({:corrupt_stream_id, id})}
      end
    end)
    |> case do
      {:ok, parsed} ->
        sorted = parsed |> Enum.sort_by(&elem(&1, 0)) |> Enum.map(&elem(&1, 1))
        {:ok, sorted}

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure
    end
  end

  defp rebuild_from_fields(key, store, [], marker_present?) do
    case durable_entry(key, store) do
      nil ->
        if marker_present? do
          put_local(key, 0, "0-0", "0-0", 0, 0, store)
          lookup_local(key, store)
        else
          []
        end

      {len, first, last, ms, seq} ->
        put_local(key, len, first, last, ms, seq, store)
        lookup_local(key, store)

      {:error, {:storage_read_failed, _reason}} = failure ->
        failure
    end
  end

  defp rebuild_from_fields(key, store, ids, _marker_present?) do
    first = List.first(ids)
    last = List.last(ids)
    {last_ms, last_seq} = ID.parse_id!(last)

    case put(key, length(ids), first, last, last_ms, last_seq, store) do
      :ok -> lookup_local(key, store)
      {:error, _reason} = error -> error
    end
  end

  defp persist(key, len, first, last, ms, seq, store) do
    if Ops.has_compound?(store) do
      encoded = TermCodec.encode({:stream_meta, len, first, last, ms, seq})
      Ops.compound_put(store, key, CompoundKey.stream_meta_key(key), encoded, 0)
    else
      :ok
    end
  end

  defp decode(nil), do: nil

  defp decode(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_meta, len, first, last, ms, seq}}
      when is_integer(len) and len >= 0 and is_binary(first) and is_binary(last) and
             is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 ->
        validate_decoded_meta(len, first, last, ms, seq)

      _ ->
        invalid_metadata()
    end
  end

  defp decode(_raw), do: invalid_metadata()

  defp validate_decoded_meta(len, first, last, ms, seq) do
    with {:ok, first_id} <- ID.parse_full_id(first),
         {:ok, last_id} <- ID.parse_full_id(last),
         true <- last_id == {ms, seq},
         true <- valid_meta_range?(len, first, first_id, last_id) do
      {len, first, last, ms, seq}
    else
      _invalid -> invalid_metadata()
    end
  end

  defp invalid_metadata, do: ReadResult.failure(:invalid_stream_metadata)

  defp valid_meta_range?(0, "0-0", _first_id, _last_id), do: true

  defp valid_meta_range?(len, _first, first_id, last_id) when len > 0,
    do: ID.compare(first_id, last_id) != :gt

  defp valid_meta_range?(_len, _first, _first_id, _last_id), do: false

  defp lookup_local(key, store) do
    cache_key = CacheKey.build(store, key)

    case :ets.lookup(@meta_table, cache_key) do
      [{^cache_key, len, first, last, ms, seq}] -> [{key, len, first, last, ms, seq}]
      [] -> []
    end
  end

  defp ensure_table do
    case :ets.whereis(@meta_table) do
      :undefined ->
        try do
          :ets.new(@meta_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
