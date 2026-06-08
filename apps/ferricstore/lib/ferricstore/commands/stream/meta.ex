defmodule Ferricstore.Commands.Stream.Meta do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{Entries, Groups, ID, Index}
  alias Ferricstore.Store.{CompoundKey, Ops, TypeRegistry}

  @meta_table Ferricstore.Stream.Meta

  @spec ensure_read_type(binary(), map()) :: :ok | {:error, binary()}
  def ensure_read_type(key, store) do
    ensure_table()

    case :ets.lookup(@meta_table, key) do
      [_entry] ->
        ensure_live(key, store)

      [] ->
        if Ops.has_compound?(store), do: TypeRegistry.check_type(key, :stream, store), else: :ok
    end
  end

  @spec entries(binary(), map()) :: [tuple()]
  def entries(key, store) do
    ensure_table()

    case :ets.lookup(@meta_table, key) do
      [] -> rebuild_entries(key, store)
      entries -> entries
    end
  end

  @spec xadd_entries(binary(), map()) :: [tuple()]
  def xadd_entries(key, store) do
    ensure_table()

    case :ets.lookup(@meta_table, key) do
      [] ->
        if type_marker?(key, store), do: rebuild_entries(key, store), else: []

      entries ->
        entries
    end
  end

  @spec type_marker?(binary(), map()) :: boolean()
  def type_marker?(key, store) do
    Ops.has_compound?(store) and
      Ops.compound_get(store, key, CompoundKey.type_key(key)) == "stream"
  end

  @spec durable_entry(binary(), map()) ::
          {non_neg_integer(), binary(), binary(), non_neg_integer(), non_neg_integer()} | nil
  def durable_entry(key, store) do
    store
    |> Ops.compound_get(key, CompoundKey.stream_meta_key(key))
    |> decode()
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
    ensure_table()
    :ets.insert(@meta_table, {key, len, first, last, ms, seq})
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
    put_local(key, len, first, last, ms, seq)
    persist(key, len, first, last, ms, seq, store)
  end

  @spec cleanup_local(binary()) :: true
  def cleanup_local(stream_key) do
    ensure_table()
    :ets.delete(@meta_table, stream_key)
    Groups.delete_local(stream_key)
    Index.clear(stream_key)
  end

  defp ensure_live(key, store) do
    cond do
      not Ops.has_compound?(store) ->
        :ok

      type_marker?(key, store) ->
        :ok

      TypeRegistry.get_type(key, store) == "none" ->
        cleanup_local(key)
        :ok

      true ->
        TypeRegistry.check_type(key, :stream, store)
    end
  end

  defp rebuild_entries(key, store) do
    if Ops.has_compound?(store) do
      ids =
        store
        |> Entries.fields_for(key)
        |> Enum.sort_by(&ID.parse_id!/1)

      case ids do
        [] ->
          case durable_entry(key, store) do
            nil ->
              if type_marker?(key, store) do
                put_local(key, 0, "0-0", "0-0", 0, 0)
                :ets.lookup(@meta_table, key)
              else
                []
              end

            {len, first, last, ms, seq} ->
              put_local(key, len, first, last, ms, seq)
              :ets.lookup(@meta_table, key)
          end

        _ ->
          first = List.first(ids)
          last = List.last(ids)
          {last_ms, last_seq} = ID.parse_id!(last)
          put(key, length(ids), first, last, last_ms, last_seq, store)
          :ets.lookup(@meta_table, key)
      end
    else
      []
    end
  end

  defp persist(key, len, first, last, ms, seq, store) do
    if Ops.has_compound?(store) do
      encoded = :erlang.term_to_binary({:stream_meta, len, first, last, ms, seq})
      Ops.compound_put(store, key, CompoundKey.stream_meta_key(key), encoded, 0)
    else
      :ok
    end
  end

  defp decode(nil), do: nil

  defp decode(raw) when is_binary(raw) do
    case :erlang.binary_to_term(raw, [:safe]) do
      {:stream_meta, len, first, last, ms, seq}
      when is_integer(len) and len >= 0 and is_binary(first) and is_binary(last) and
             is_integer(ms) and ms >= 0 and is_integer(seq) and seq >= 0 ->
        {len, first, last, ms, seq}

      _ ->
        nil
    end
  rescue
    _ -> nil
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
