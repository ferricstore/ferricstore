defmodule FerricstoreServer.Acl.Tables do
  @moduledoc false

  @table :ferricstore_acl
  @active_table_key :ferricstore_acl_active_table
  @swap_new_table :ferricstore_acl_new_swap
  @retired_table_grace_ms 5_000
  @retired_swap_tables [
    :ferricstore_acl_old_swap_0,
    :ferricstore_acl_old_swap_1,
    :ferricstore_acl_old_swap_2,
    :ferricstore_acl_old_swap_3,
    :ferricstore_acl_old_swap_4,
    :ferricstore_acl_old_swap_5,
    :ferricstore_acl_old_swap_6,
    :ferricstore_acl_old_swap_7
  ]

  @spec new_active_table() :: :ets.tid()
  def new_active_table do
    table = :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    :persistent_term.put(@active_table_key, table)
    table
  end

  @spec active_table() :: :ets.tid() | atom()
  def active_table do
    table = :persistent_term.get(@active_table_key, @table)

    case :ets.info(table) do
      :undefined -> @table
      _info -> table
    end
  rescue
    ArgumentError -> @table
  end

  @spec insert_default_user() :: true
  def insert_default_user do
    :ets.insert(
      active_table(),
      {"default",
       %{
         enabled: true,
         password: nil,
         commands: :all,
         denied_commands: MapSet.new(),
         keys: :all,
         channels: :all
       }}
    )
  end

  @spec replace_acl_snapshot([{binary(), map()}]) :: :ok
  def replace_acl_snapshot(users) do
    cleanup_named_table(@swap_new_table)
    new_table = :ets.new(@swap_new_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(new_table, users)
    maybe_run_load_swap_hook()
    swap_active_table(new_table)
  end

  @spec cleanup_new_swap_table() :: :ok
  def cleanup_new_swap_table, do: cleanup_named_table(@swap_new_table)

  @spec cleanup_retired_tables() :: :ok
  def cleanup_retired_tables do
    Enum.each(@retired_swap_tables, &cleanup_named_table/1)
  end

  @spec cleanup_named_table(atom()) :: :ok
  def cleanup_named_table(name) do
    case :ets.whereis(name) do
      :undefined -> :ok
      _tid -> :ets.delete(name)
    end
  end

  @spec retired_table?(atom()) :: boolean()
  def retired_table?(name), do: name in @retired_swap_tables

  defp maybe_run_load_swap_hook do
    case :persistent_term.get(:ferricstore_acl_before_load_swap_hook, nil) do
      nil -> :ok
      fun when is_function(fun, 0) -> fun.()
    end
  end

  defp swap_active_table(new_table) do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.rename(new_table, @table)

      _tid ->
        retired_table = next_retired_table_name()
        :ets.rename(@table, retired_table)
        :ets.rename(new_table, @table)
        schedule_retired_table_cleanup(retired_table)
    end

    new_active = :ets.whereis(@table)
    :persistent_term.put(@active_table_key, new_active)
    :ok
  end

  defp next_retired_table_name do
    case Enum.find(@retired_swap_tables, &(:ets.whereis(&1) == :undefined)) do
      nil ->
        table_name = hd(@retired_swap_tables)
        cleanup_named_table(table_name)
        table_name

      table_name ->
        table_name
    end
  end

  defp schedule_retired_table_cleanup(table_name) do
    Process.send_after(self(), {:cleanup_acl_retired_table, table_name}, @retired_table_grace_ms)
    :ok
  end
end
