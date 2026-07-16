defmodule Ferricstore.Commands.Stream.Groups do
  @moduledoc false

  alias Ferricstore.Commands.Stream.{CacheKey, ID}
  alias Ferricstore.Store.{CompoundKey, Ops, ReadResult}
  alias Ferricstore.TermCodec

  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks

  @spec lookup(map(), binary(), binary()) ::
          :missing | {:ok, binary(), map(), map()} | {:error, binary()}
  def lookup(store, key, group) do
    ensure_table()
    cache_key = CacheKey.build(store, key)

    case :ets.lookup(@groups_table, {cache_key, group}) do
      [{{^cache_key, ^group}, last_delivered, consumers, pending}] ->
        {:ok, last_delivered, consumers, pending}

      [] ->
        load_persisted(store, key, group)
    end
  end

  @spec persist(map(), binary(), binary(), binary(), map(), map()) :: :ok | {:error, term()}
  def persist(store, key, group, last_delivered, consumers, pending) do
    ensure_table()

    if Ops.has_compound?(store) do
      encoded = encode_state(last_delivered, consumers, pending)

      case Ops.compound_put(store, key, group_key(key, group), encoded, 0) do
        :ok ->
          cache(store, key, group, last_delivered, consumers, pending)
          :ok

        {:error, _reason} = error ->
          error
      end
    else
      cache(store, key, group, last_delivered, consumers, pending)
      :ok
    end
  end

  @spec count(binary(), map()) :: non_neg_integer() | ReadResult.failure()
  def count(key, store) do
    if Ops.has_compound?(store) do
      Ops.compound_count(store, key, CompoundKey.stream_group_prefix(key))
    else
      ensure_table()
      cache_key = CacheKey.build(store, key)

      :ets.foldl(
        fn
          {{^cache_key, _group}, _last, _consumers, _pending}, acc -> acc + 1
          _, acc -> acc
        end,
        0,
        @groups_table
      )
    end
  end

  @spec delete_local(binary()) :: true
  def delete_local(stream_key), do: delete_local(stream_key, nil)

  @spec delete_local(binary(), term()) :: true
  def delete_local(stream_key, store) do
    ensure_table()

    :ets.match_delete(
      @groups_table,
      {{CacheKey.build(store, stream_key), :_}, :_, :_, :_}
    )
  end

  @spec delete(map(), binary(), binary()) :: :ok | {:error, term()}
  def delete(store, key, group) do
    ensure_table()

    result =
      if Ops.has_compound?(store) do
        Ops.compound_delete(store, key, group_key(key, group))
      else
        :ok
      end

    case result do
      :ok ->
        :ets.delete(@groups_table, {CacheKey.build(store, key), group})
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  @spec snapshot(non_neg_integer()) :: [map()]
  def snapshot(limit \\ 100) do
    ensure_table()
    limit = max(limit, 0)

    if limit == 0 do
      []
    else
      :ets.foldl(
        fn
          {{cache_key, group}, last_delivered, consumers, pending}, acc
          when is_binary(group) and is_map(consumers) and is_map(pending) ->
            case CacheKey.raw(cache_key) do
              key when is_binary(key) ->
                row = %{
                  key: key,
                  group: group,
                  last_delivered: last_delivered,
                  consumers: map_size(consumers),
                  pending: map_size(pending)
                }

                insert_group_snapshot(row, cache_key, acc, limit)

              nil ->
                acc
            end

          _invalid, acc ->
            acc
        end,
        :gb_sets.empty(),
        @groups_table
      )
      |> :gb_sets.to_list()
      |> Enum.map(&elem(&1, 1))
    end
  rescue
    _ -> []
  catch
    :exit, _ -> []
  end

  defp insert_group_snapshot(row, cache_key, acc, limit) do
    rank = {-row.pending, -row.consumers, row.key, row.group, cache_key}
    ranked = :gb_sets.add({rank, row}, acc)

    if :gb_sets.size(ranked) > limit do
      :gb_sets.del_element(:gb_sets.largest(ranked), ranked)
    else
      ranked
    end
  end

  @spec with_lock(binary(), binary(), (-> result)) :: result when result: term()
  def with_lock(key, group, fun) when is_function(fun, 0),
    do: with_lock(nil, key, group, fun)

  @spec with_lock(term(), binary(), binary(), (-> result)) :: result when result: term()
  def with_lock(store, key, group, fun) when is_function(fun, 0) do
    lock = {CacheKey.build(store, key), group}
    acquire_lock(lock)

    try do
      fun.()
    after
      release_lock(lock)
    end
  end

  defp load_persisted(store, key, group) do
    if Ops.has_compound?(store) do
      case Ops.compound_get(store, key, group_key(key, group)) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        nil ->
          :missing

        raw ->
          case decode_state(raw) do
            {:ok, last_delivered, consumers, pending} ->
              cache(store, key, group, last_delivered, consumers, pending)
              {:ok, last_delivered, consumers, pending}

            :error ->
              ReadResult.command_error(ReadResult.failure(:invalid_stream_group_state))
          end
      end
    else
      :missing
    end
  end

  defp cache(store, key, group, last_delivered, consumers, pending) do
    :ets.insert(
      @groups_table,
      {{CacheKey.build(store, key), group}, last_delivered, consumers, pending}
    )
  end

  defp group_key(stream_key, group) do
    CompoundKey.stream_group(stream_key, group)
  end

  defp encode_state(last_delivered, consumers, pending) do
    TermCodec.encode({:stream_group, 1, last_delivered, consumers, pending})
  end

  defp decode_state(raw) when is_binary(raw) do
    case TermCodec.decode(raw) do
      {:ok, {:stream_group, 1, last_delivered, consumers, pending}}
      when is_binary(last_delivered) and is_map(consumers) and is_map(pending) ->
        if valid_state?(last_delivered, consumers, pending) do
          {:ok, last_delivered, consumers, pending}
        else
          :error
        end

      _other ->
        :error
    end
  end

  defp decode_state(_raw), do: :error

  defp valid_state?(last_delivered, consumers, pending) do
    valid_stream_id?(last_delivered) and valid_consumers?(consumers) and
      valid_pending?(pending)
  end

  defp valid_consumers?(consumers) do
    Enum.all?(consumers, fn
      {consumer, seen_at_ms}
      when is_binary(consumer) and is_integer(seen_at_ms) and seen_at_ms >= 0 ->
        true

      _invalid ->
        false
    end)
  end

  defp valid_pending?(pending) do
    Enum.all?(pending, fn
      {id, {consumer, delivered_at_ms}}
      when is_binary(id) and is_binary(consumer) and is_integer(delivered_at_ms) and
             delivered_at_ms >= 0 ->
        valid_stream_id?(id)

      _invalid ->
        false
    end)
  end

  defp valid_stream_id?(id), do: match?({:ok, {_ms, _seq}}, ID.parse_full_id(id))

  defp acquire_lock(lock) do
    ensure_lock_table()

    case :ets.insert_new(@group_locks_table, {lock, self()}) do
      true ->
        :ok

      false ->
        wait_for_lock(lock)
    end
  end

  defp wait_for_lock(lock) do
    case :ets.lookup(@group_locks_table, lock) do
      [{^lock, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          receive do
          after
            1 -> :ok
          end
        else
          :ets.select_delete(@group_locks_table, [{{lock, holder}, [], [true]}])
        end

      _other ->
        :ok
    end

    acquire_lock(lock)
  end

  defp release_lock(lock) do
    ensure_lock_table()
    :ets.select_delete(@group_locks_table, [{{lock, self()}, [], [true]}])
    :ok
  end

  defp ensure_table do
    case :ets.whereis(@groups_table) do
      :undefined ->
        try do
          :ets.new(@groups_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end

  defp ensure_lock_table do
    case :ets.whereis(@group_locks_table) do
      :undefined ->
        try do
          :ets.new(@group_locks_table, [:set, :public, :named_table])
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
