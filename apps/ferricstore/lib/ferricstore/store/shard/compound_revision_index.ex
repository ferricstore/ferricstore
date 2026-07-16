defmodule Ferricstore.Store.Shard.CompoundRevisionIndex do
  @moduledoc false

  @epoch_key :"$ferricstore_compound_revision_epoch"

  @type table_ref :: atom() | :ets.tid() | nil

  @spec table_name(atom() | binary(), non_neg_integer()) :: atom()
  def table_name(instance_name, shard_index),
    do: :"ferricstore_compound_revision_#{instance_name}_#{shard_index}"

  @spec ensure_table!(atom()) :: atom()
  def ensure_table!(table_name) when is_atom(table_name) do
    case :ets.whereis(table_name) do
      :undefined ->
        :ets.new(table_name, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto},
          {:decentralized_counters, true}
        ])

      _tid ->
        table_name
    end

    :ets.insert_new(table_name, {@epoch_key, new_epoch()})
    table_name
  end

  @spec put(table_ref(), binary(), non_neg_integer()) :: :ok
  def put(table, key, revision)
      when is_binary(key) and is_integer(revision) and revision >= 0 do
    case table_ref(table) do
      :undefined ->
        :ok

      tid ->
        case :ets.lookup(tid, key) do
          [{^key, current}] when is_integer(current) and current >= revision -> :ok
          _older_or_missing -> :ets.insert(tid, {key, revision})
        end
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def put(_table, _key, _revision), do: :ok

  @spec delete(table_ref(), binary()) :: :ok
  def delete(table, key) when is_binary(key) do
    case table_ref(table) do
      :undefined -> :ok
      tid -> :ets.delete(tid, key)
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  def delete(_table, _key), do: :ok

  @spec revision_token(table_ref(), binary()) ::
          {:ok, {pos_integer(), non_neg_integer()}} | :missing | :unavailable
  def revision_token(table, key) when is_binary(key) do
    case table_ref(table) do
      :undefined ->
        :unavailable

      tid ->
        with [{@epoch_key, epoch}] when is_integer(epoch) and epoch > 0 <-
               :ets.lookup(tid, @epoch_key),
             [{^key, revision}] when is_integer(revision) and revision >= 0 <-
               :ets.lookup(tid, key) do
          {:ok, {epoch, revision}}
        else
          [] -> :missing
          _invalid -> :unavailable
        end
    end
  rescue
    ArgumentError -> :unavailable
  end

  def revision_token(_table, _key), do: :unavailable

  defp table_ref(nil), do: :undefined
  defp table_ref(table) when is_reference(table), do: table

  defp table_ref(table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined -> :undefined
      tid -> tid
    end
  end

  defp new_epoch, do: :erlang.unique_integer([:monotonic, :positive])
end
