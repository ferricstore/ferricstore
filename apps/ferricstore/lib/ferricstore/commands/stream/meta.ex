defmodule Ferricstore.Commands.Stream.Meta do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, Entries, Groups, ID, Index, Tables}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult, Router, TypeRegistry}
  alias Ferricstore.TermCodec

  @meta_table Ferricstore.Stream.Meta

  @spec ensure_read_type(binary(), map()) :: :ok | {:error, binary()}
  def ensure_read_type(key, store) do
    ensure_read_type(key, CompoundKey.type_key(key), store)
  end

  @doc false
  @spec ensure_read_type(binary(), binary(), map()) :: :ok | {:error, binary()}
  def ensure_read_type(key, type_key, store) when is_binary(type_key) do
    ensure_table()

    case lookup_local(key, store) do
      [_entry] ->
        case ensure_live_status(key, type_key, store) do
          status when status in [:live, :missing] -> :ok
          {:error, _reason} = error -> error
        end

      [] ->
        if Ops.has_compound?(store),
          do: TypeRegistry.command_check_type(key, :stream, store),
          else: :ok
    end
  end

  @doc false
  @spec checked_entries(binary(), map()) :: [tuple()] | {:error, term()}
  def checked_entries(key, store) do
    checked_entries(key, CompoundKey.type_key(key), store)
  end

  @doc false
  @spec checked_entries(binary(), binary(), map()) :: [tuple()] | {:error, term()}
  def checked_entries(key, type_key, store) when is_binary(type_key) do
    ensure_table()

    case lookup_local(key, store) do
      [entry] ->
        case ensure_live_status(key, type_key, store) do
          :live -> [entry]
          :missing -> []
          {:error, _reason} = error -> error
        end

      [] ->
        with :ok <- ensure_missing_read_type(key, store) do
          entries(key, store)
        end
    end
  end

  @doc false
  @spec ensure_read_marker(binary(), term(), map()) :: :ok | {:error, term()}
  def ensure_read_marker(_key, "stream", _store), do: :ok

  def ensure_read_marker(_key, {:error, {:storage_read_failed, _reason}} = failure, _store),
    do: ReadResult.command_error(failure)

  def ensure_read_marker(key, _missing_or_other_type, store) do
    case TypeRegistry.command_get_type(key, store) do
      "none" ->
        cleanup_stale_local_if_present(key, store)
        :ok

      {:error, _reason} = error ->
        error

      _other_type ->
        TypeRegistry.command_check_type(key, :stream, store)
    end
  end

  defp cleanup_stale_local_if_present(key, store) do
    ensure_table()

    if :ets.member(@meta_table, CacheKey.build(store, key)) do
      cleanup_local(key, store)
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
    type_marker_status(key, CompoundKey.type_key(key), store)
  end

  @doc false
  @spec type_marker_status(binary(), binary(), map()) ::
          {:ok, boolean()} | ReadResult.failure()
  def type_marker_status(key, type_key, store) do
    if Ops.has_compound?(store) do
      case stream_type_marker_get(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure -> failure
        "stream" -> {:ok, true}
        _missing_or_other_type -> {:ok, false}
      end
    else
      {:ok, false}
    end
  end

  defp stream_type_marker_get(%FerricStore.Instance{} = store, key, type_key),
    do: Router.stream_type_marker_get(store, key, type_key)

  defp stream_type_marker_get(store, key, type_key),
    do: Ops.compound_get(store, key, type_key)

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

  @doc false
  @spec serialized_entry(binary(), map()) ::
          {non_neg_integer(), binary(), binary(), non_neg_integer(), non_neg_integer()}
          | {:error, term()}
  def serialized_entry(key, store) do
    raw = Ops.compound_get(store, key, CompoundKey.stream_meta_key(key))

    case raw do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      raw ->
        case decode_versioned(raw) do
          {:ok, 2, meta} -> meta
          {:ok, 1, legacy_meta} -> rebuild_serialized_entry(key, legacy_meta, store)
          nil -> rebuild_serialized_entry(key, nil, store)
          {:error, _reason} -> rebuild_serialized_entry(key, nil, store)
        end
    end
  end

  @doc false
  @spec serialized_put_entry(
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) :: {binary(), binary(), 0}
  def serialized_put_entry(key, len, first, last, ms, seq) do
    serialized_put_entry_with_key(CompoundKey.stream_meta_key(key), len, first, last, ms, seq)
  end

  @doc false
  @spec serialized_put_entry_with_key(
          binary(),
          non_neg_integer(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) :: {binary(), binary(), 0}
  def serialized_put_entry_with_key(meta_key, len, first, last, ms, seq)
      when is_binary(meta_key) do
    # This closed schema contains only an atom, integers, binaries, and a tuple,
    # whose ordinary ETF encoding is byte-identical to deterministic ETF without
    # paying the generic deterministic-term traversal on every Stream append.
    encoded = :erlang.term_to_binary({:stream_meta, 2, len, first, last, ms, seq})
    {meta_key, encoded, 0}
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

  @doc false
  @spec put_local_many([
          {CacheKey.t(), non_neg_integer(), binary(), binary(), non_neg_integer(),
           non_neg_integer()}
        ]) :: true
  def put_local_many(entries) when is_list(entries) do
    ensure_table()
    :ets.insert(@meta_table, entries)
  end

  @doc false
  @spec cached_entry(binary(), term()) ::
          {:ok, {non_neg_integer(), binary(), binary(), non_neg_integer(), non_neg_integer()}}
          | :miss
  def cached_entry(key, store) when is_binary(key) do
    ensure_table()
    cache_key = CacheKey.build(store, key)

    case :ets.lookup(@meta_table, cache_key) do
      [{^cache_key, len, first, last, ms, seq}] ->
        {:ok, {len, first, last, ms, seq}}

      [] ->
        :miss
    end
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

  @doc false
  @spec empty_entry_bytes(binary()) :: pos_integer()
  def empty_entry_bytes(key) when is_binary(key) do
    encoded = TermCodec.encode({:stream_meta, 0, "0-0", "0-0", 0, 0})
    byte_size(CompoundKey.stream_meta_key(key)) + byte_size(encoded)
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

  defp ensure_live_status(key, type_key, store) do
    if Ops.has_compound?(store) do
      case type_marker_status(key, type_key, store) do
        {:ok, true} ->
          :live

        {:ok, false} ->
          case TypeRegistry.command_get_type(key, store) do
            "none" ->
              cleanup_local(key, store)
              :missing

            {:error, _reason} = error ->
              error

            _other_type ->
              case TypeRegistry.command_check_type(key, :stream, store) do
                :ok -> :live
                {:error, _reason} = error -> error
              end
          end

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)
      end
    else
      :live
    end
  end

  defp ensure_missing_read_type(key, store) do
    if Ops.has_compound?(store),
      do: TypeRegistry.command_check_type(key, :stream, store),
      else: :ok
  end

  defp rebuild_entries(key, store) do
    case type_marker_status(key, store) do
      {:ok, marker_present?} -> rebuild_entries(key, store, marker_present?)
      {:error, {:storage_read_failed, _reason}} = failure -> failure
    end
  end

  defp rebuild_entries(key, store, marker_present?) do
    if Ops.has_compound?(store) do
      case durable_versioned_entry(key, store) do
        {:ok, 2, {len, first, last, ms, seq}} when marker_present? ->
          put_local(key, len, first, last, ms, seq, store)
          lookup_local(key, store)

        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        _legacy_missing_or_invalid ->
          rebuild_entries_from_fields(key, store, marker_present?)
      end
    else
      []
    end
  end

  defp rebuild_entries_from_fields(key, store, marker_present?) do
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
    case decode_versioned(raw) do
      {:ok, _version, meta} -> meta
      {:error, _reason} = error -> error
      nil -> nil
    end
  end

  defp decode(_raw), do: invalid_metadata()

  defp decode_versioned(nil), do: nil

  defp decode_versioned(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_meta, len, first, last, ms, seq}} ->
        versioned_meta(1, len, first, last, ms, seq)

      {:ok, {:stream_meta, 2, len, first, last, ms, seq}} ->
        versioned_meta(2, len, first, last, ms, seq)

      _ ->
        invalid_metadata()
    end
  end

  defp decode_versioned(_raw), do: invalid_metadata()

  defp durable_versioned_entry(key, store) do
    case Ops.compound_get(store, key, CompoundKey.stream_meta_key(key)) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      raw -> decode_versioned(raw)
    end
  end

  defp versioned_meta(version, len, first, last, ms, seq)
       when is_integer(len) and len >= 0 and is_binary(first) and is_binary(last) and
              is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 do
    case validate_decoded_meta(version, len, first, last, ms, seq) do
      {:error, _reason} = error -> error
      meta -> {:ok, version, meta}
    end
  end

  defp versioned_meta(_version, _len, _first, _last, _ms, _seq), do: invalid_metadata()

  defp rebuild_serialized_entry(key, legacy_meta, store) do
    case Entries.fields_for(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      ids when is_list(ids) ->
        with {:ok, sorted_ids} <- sorted_valid_ids(ids) do
          serialized_meta_from_ids(sorted_ids, legacy_meta)
        end
    end
  end

  defp serialized_meta_from_ids([], nil), do: {0, "0-0", "0-0", 0, 0}

  defp serialized_meta_from_ids([], {_len, _first, _last, ms, seq}),
    do: {0, "0-0", "0-0", ms, seq}

  defp serialized_meta_from_ids(ids, legacy_meta) do
    first = List.first(ids)
    physical_last = List.last(ids)
    physical_last_id = ID.parse_id!(physical_last)

    {last_ms, last_seq} =
      case legacy_meta do
        {_len, _first, _legacy_last, legacy_ms, legacy_seq}
        when {legacy_ms, legacy_seq} >= physical_last_id ->
          {legacy_ms, legacy_seq}

        _ ->
          physical_last_id
      end

    {length(ids), first, physical_last, last_ms, last_seq}
  end

  defp validate_decoded_meta(1, len, first, last, ms, seq) do
    with {:ok, first_id} <- ID.parse_full_id(first),
         {:ok, last_id} <- ID.parse_full_id(last),
         true <- last_id == {ms, seq},
         true <- valid_meta_range?(len, first, first_id, last_id) do
      {len, first, last, ms, seq}
    else
      _invalid -> invalid_metadata()
    end
  end

  defp validate_decoded_meta(2, len, first, last, ms, seq) do
    with {:ok, first_id} <- ID.parse_full_id(first),
         {:ok, last_id} <- ID.parse_full_id(last),
         true <- valid_v2_meta_range?(len, first, last, first_id, last_id, {ms, seq}) do
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

  defp valid_v2_meta_range?(0, "0-0", "0-0", _first_id, _last_id, generated_id),
    do: generated_id >= {0, 0}

  defp valid_v2_meta_range?(len, _first, _last, first_id, last_id, generated_id)
       when len > 0 do
    ID.compare(first_id, last_id) != :gt and ID.compare(last_id, generated_id) != :gt
  end

  defp valid_v2_meta_range?(
         _len,
         _first,
         _last,
         _first_id,
         _last_id,
         _generated_id
       ),
       do: false

  defp lookup_local(key, store) do
    cache_key = CacheKey.build(store, key)

    case :ets.lookup(@meta_table, cache_key) do
      [{^cache_key, len, first, last, ms, seq}] -> [{key, len, first, last, ms, seq}]
      [] -> []
    end
  end

  defp ensure_table do
    Tables.ensure_all()
  end
end
