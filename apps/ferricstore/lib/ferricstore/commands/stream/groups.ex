defmodule Ferricstore.Commands.Stream.Groups do
  @moduledoc false

  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.CompoundKey

  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks

  @spec lookup(map(), binary(), binary()) ::
          :missing | {:ok, binary(), map(), map()}
  def lookup(store, key, group) do
    ensure_table()

    case :ets.lookup(@groups_table, {key, group}) do
      [{{^key, ^group}, last_delivered, consumers, pending}] ->
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
          cache(key, group, last_delivered, consumers, pending)
          :ok

        {:error, _reason} = error ->
          error
      end
    else
      cache(key, group, last_delivered, consumers, pending)
      :ok
    end
  end

  @spec count(binary(), map()) :: non_neg_integer()
  def count(key, store) do
    if Ops.has_compound?(store) do
      Ops.compound_count(store, key, CompoundKey.stream_group_prefix(key))
    else
      ensure_table()

      :ets.foldl(
        fn
          {{^key, _group}, _last, _consumers, _pending}, acc -> acc + 1
          _, acc -> acc
        end,
        0,
        @groups_table
      )
    end
  end

  @spec delete_local(binary()) :: true
  def delete_local(stream_key) do
    ensure_table()
    :ets.match_delete(@groups_table, {{stream_key, :_}, :_, :_, :_})
  end

  @spec with_lock(binary(), binary(), (-> result)) :: result when result: term()
  def with_lock(key, group, fun) when is_function(fun, 0) do
    lock = {key, group}
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
        nil ->
          :missing

        raw ->
          case decode_state(raw) do
            {:ok, last_delivered, consumers, pending} ->
              cache(key, group, last_delivered, consumers, pending)
              {:ok, last_delivered, consumers, pending}

            :error ->
              :missing
          end
      end
    else
      :missing
    end
  end

  defp cache(key, group, last_delivered, consumers, pending) do
    :ets.insert(@groups_table, {{key, group}, last_delivered, consumers, pending})
  end

  defp group_key(stream_key, group) do
    CompoundKey.stream_group(stream_key, group)
  end

  defp encode_state(last_delivered, consumers, pending) do
    :erlang.term_to_binary({:stream_group, 1, last_delivered, consumers, pending})
  end

  defp decode_state(raw) when is_binary(raw) do
    case :erlang.binary_to_term(raw, [:safe]) do
      {:stream_group, 1, last_delivered, consumers, pending}
      when is_binary(last_delivered) and is_map(consumers) and is_map(pending) ->
        {:ok, last_delivered, consumers, pending}

      _other ->
        :error
    end
  rescue
    _ -> :error
  end

  defp decode_state(_raw), do: :error

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
