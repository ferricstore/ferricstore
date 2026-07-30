defmodule Ferricstore.Commands.Stream.Tables do
  @moduledoc false

  @meta_table Ferricstore.Stream.Meta
  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks
  @index_table Ferricstore.Stream.Index
  @stream_waiters_table :ferricstore_stream_waiters
  @startup_ready_key {__MODULE__, :startup_ready}

  @spec init_all() :: :ok
  def init_all do
    ensure_all_tables()
    :persistent_term.put(@startup_ready_key, true)
    :ok
  end

  @spec ensure_all() :: :ok
  def ensure_all do
    if :persistent_term.get(@startup_ready_key, false) do
      :ok
    else
      ensure_all_tables()
    end
  end

  @spec clear_startup_ready() :: :ok
  def clear_startup_ready do
    :persistent_term.erase(@startup_ready_key)
    :ok
  end

  defp ensure_all_tables do
    ensure(@meta_table, [
      :set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, :auto}
    ])

    ensure(@groups_table, [:set, :public, :named_table])
    ensure(@group_locks_table, [:set, :public, :named_table])
    ensure(@stream_waiters_table, [:duplicate_bag, :public, :named_table])

    ensure(@index_table, [
      :ordered_set,
      :public,
      :named_table,
      {:read_concurrency, true},
      {:write_concurrency, true}
    ])

    :ok
  end

  defp ensure(table, opts) do
    case :ets.whereis(table) do
      :undefined ->
        try do
          :ets.new(table, opts)
        rescue
          ArgumentError -> :ok
        end

      _ref ->
        :ok
    end
  end
end
