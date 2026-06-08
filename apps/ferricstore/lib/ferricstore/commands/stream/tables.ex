defmodule Ferricstore.Commands.Stream.Tables do
  @moduledoc false

  @meta_table Ferricstore.Stream.Meta
  @groups_table Ferricstore.Stream.Groups
  @group_locks_table Ferricstore.Stream.GroupLocks
  @index_table Ferricstore.Stream.Index
  @stream_waiters_table :ferricstore_stream_waiters

  @spec ensure_all() :: :ok
  def ensure_all do
    ensure(@meta_table, [:set, :public, :named_table])
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
