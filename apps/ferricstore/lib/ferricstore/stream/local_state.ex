defmodule Ferricstore.Stream.LocalState do
  @moduledoc false

  @tables [
    Ferricstore.Stream.Meta,
    Ferricstore.Stream.Groups,
    Ferricstore.Stream.Index,
    :ferricstore_stream_waiters
  ]

  @spec clear() :: :ok
  def clear do
    Enum.each(@tables, fn table ->
      if :ets.whereis(table) != :undefined do
        :ets.delete_all_objects(table)
      end
    end)

    :ok
  end
end
