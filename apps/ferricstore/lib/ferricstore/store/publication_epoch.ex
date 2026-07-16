defmodule Ferricstore.Store.PublicationEpoch do
  @moduledoc false

  @type token ::
          :noop
          | {reference(), pos_integer(), non_neg_integer()}
          | {reference(), pos_integer(), non_neg_integer(), :ets.tid() | atom(), term(), pid()}

  @spec begin_write(map(), non_neg_integer()) :: token()
  def begin_write(%{publication_epoch: ref, latch_refs: latch_refs}, shard_index)
      when is_reference(ref) and is_tuple(latch_refs) and is_integer(shard_index) and
             shard_index >= 0 and shard_index < tuple_size(latch_refs) do
    position = shard_index + 1
    latch_table = elem(latch_refs, shard_index)
    latch_key = {__MODULE__, :writer, shard_index}

    case acquire_writer_latch(latch_table, latch_key) do
      :ok ->
        repair_orphaned_odd_epoch(ref, position)
        odd_epoch = begin_atomic_write(ref, position)
        {ref, position, odd_epoch, latch_table, latch_key, self()}

      :unavailable ->
        {ref, position, begin_atomic_write(ref, position)}

      :reentrant ->
        raise ArgumentError, "publication writes cannot be nested for the same shard"
    end
  end

  def begin_write(%{publication_epoch: ref}, shard_index)
      when is_reference(ref) and is_integer(shard_index) and shard_index >= 0 do
    position = shard_index + 1
    {ref, position, begin_atomic_write(ref, position)}
  end

  def begin_write(_ctx, _shard_index), do: :noop

  @spec end_write(token()) :: :ok
  def end_write(:noop), do: :ok

  def end_write({ref, position, odd_epoch, latch_table, latch_key, owner}) do
    finish_atomic_write(ref, position, odd_epoch)
    delete_writer_latch(latch_table, latch_key, owner)
    :ok
  end

  def end_write({ref, position, odd_epoch}) do
    finish_atomic_write(ref, position, odd_epoch)
  end

  @spec end_write_if_open(token()) :: :ok
  def end_write_if_open(token), do: end_write(token)

  @spec with_write(map(), non_neg_integer(), (-> result)) :: result when result: term()
  def with_write(ctx, shard_index, fun) when is_function(fun, 0) do
    token = begin_write(ctx, shard_index)

    try do
      fun.()
    after
      end_write(token)
    end
  end

  @spec read(map(), [non_neg_integer()], (-> result)) :: result when result: term()
  def read(%{publication_epoch: ref, latch_refs: latch_refs}, shard_indexes, fun)
      when is_reference(ref) and is_tuple(latch_refs) and is_list(shard_indexes) and
             is_function(fun, 0) do
    indexes = Enum.uniq(shard_indexes)
    read_stable(ref, read_descriptors(indexes, latch_refs), fun)
  end

  def read(%{publication_epoch: ref}, shard_indexes, fun)
      when is_reference(ref) and is_list(shard_indexes) and is_function(fun, 0) do
    indexes = Enum.uniq(shard_indexes)
    read_stable(ref, read_descriptors(indexes, nil), fun)
  end

  def read(_ctx, _shard_indexes, fun) when is_function(fun, 0), do: fun.()

  @spec reset(map(), non_neg_integer()) :: :ok
  def reset(%{publication_epoch: ref, latch_refs: latch_refs}, shard_index)
      when is_reference(ref) and is_tuple(latch_refs) and is_integer(shard_index) and
             shard_index >= 0 and shard_index < tuple_size(latch_refs) do
    position = shard_index + 1
    epoch = :atomics.get(ref, position)
    if rem(epoch, 2) == 1, do: :atomics.put(ref, position, epoch + 1)

    latch_table = elem(latch_refs, shard_index)
    latch_key = {__MODULE__, :writer, shard_index}
    delete_any_writer_latch(latch_table, latch_key)
    :ok
  end

  def reset(%{publication_epoch: ref}, shard_index)
      when is_reference(ref) and is_integer(shard_index) and shard_index >= 0 do
    position = shard_index + 1
    epoch = :atomics.get(ref, position)
    if rem(epoch, 2) == 1, do: :atomics.put(ref, position, epoch + 1)
    :ok
  end

  def reset(_ctx, _shard_index), do: :ok

  defp begin_atomic_write(ref, position) do
    epoch = :atomics.get(ref, position)

    cond do
      rem(epoch, 2) == 1 ->
        :erlang.yield()
        begin_atomic_write(ref, position)

      :atomics.compare_exchange(ref, position, epoch, epoch + 1) == :ok ->
        epoch + 1

      true ->
        begin_atomic_write(ref, position)
    end
  end

  defp read_stable(ref, descriptors, fun) do
    before = read_epochs(ref, descriptors, [])

    if Enum.any?(before, &(rem(&1, 2) == 1)) do
      repair_dead_writer_epochs(ref, descriptors, before)
      :erlang.yield()
      read_stable(ref, descriptors, fun)
    else
      result = fun.()

      if before == read_epochs(ref, descriptors, []) do
        result
      else
        read_stable(ref, descriptors, fun)
      end
    end
  end

  defp read_epochs(_ref, [], acc), do: Enum.reverse(acc)

  defp read_epochs(ref, [{position, _latch_table, _latch_key} | rest], acc),
    do: read_epochs(ref, rest, [:atomics.get(ref, position) | acc])

  defp read_descriptors(indexes, latch_refs) do
    Enum.map(indexes, fn shard_index ->
      latch_table =
        if is_tuple(latch_refs) and shard_index >= 0 and shard_index < tuple_size(latch_refs),
          do: elem(latch_refs, shard_index),
          else: nil

      {shard_index + 1, latch_table, {__MODULE__, :writer, shard_index}}
    end)
  end

  defp acquire_writer_latch(latch_table, latch_key) do
    case :ets.insert_new(latch_table, {latch_key, self()}) do
      true ->
        :ok

      false ->
        case :ets.lookup(latch_table, latch_key) do
          [{^latch_key, owner}] when owner == self() ->
            :reentrant

          [{^latch_key, owner}] when is_pid(owner) ->
            if Process.alive?(owner) do
              :erlang.yield()
            else
              :ets.delete_object(latch_table, {latch_key, owner})
            end

            acquire_writer_latch(latch_table, latch_key)

          [stale] ->
            :ets.delete_object(latch_table, stale)
            acquire_writer_latch(latch_table, latch_key)

          [] ->
            acquire_writer_latch(latch_table, latch_key)
        end
    end
  rescue
    ArgumentError -> :unavailable
  end

  defp repair_orphaned_odd_epoch(ref, position) do
    epoch = :atomics.get(ref, position)

    if rem(epoch, 2) == 1 do
      _ = :atomics.compare_exchange(ref, position, epoch, epoch + 1)
    end

    :ok
  end

  defp finish_atomic_write(ref, position, odd_epoch) do
    case :atomics.compare_exchange(ref, position, odd_epoch, odd_epoch + 1) do
      :ok -> :ok
      current when current == odd_epoch + 1 -> :ok
      _other -> :ok
    end
  end

  defp repair_dead_writer_epochs(ref, descriptors, epochs) do
    descriptors
    |> Enum.zip(epochs)
    |> Enum.each(fn
      {{position, latch_table, latch_key}, epoch}
      when rem(epoch, 2) == 1 and not is_nil(latch_table) ->
        repair_dead_writer_epoch(ref, position, epoch, latch_table, latch_key)

      _stable_or_untracked ->
        :ok
    end)
  end

  defp repair_dead_writer_epoch(ref, position, epoch, latch_table, latch_key) do
    case :ets.lookup(latch_table, latch_key) do
      [{^latch_key, owner}] when is_pid(owner) ->
        unless Process.alive?(owner) do
          _ = :atomics.compare_exchange(ref, position, epoch, epoch + 1)
          :ets.delete_object(latch_table, {latch_key, owner})
        end

      _missing_or_invalid ->
        _ = :atomics.compare_exchange(ref, position, epoch, epoch + 1)
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp delete_writer_latch(latch_table, latch_key, owner) do
    :ets.delete_object(latch_table, {latch_key, owner})
    :ok
  rescue
    ArgumentError -> :ok
  end

  defp delete_any_writer_latch(latch_table, latch_key) do
    :ets.delete(latch_table, latch_key)
    :ok
  rescue
    ArgumentError -> :ok
  end
end
