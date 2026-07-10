defmodule FerricstoreServer.Acl.Tables do
  @moduledoc false

  @table :ferricstore_acl
  @active_table_key :ferricstore_acl_active_table
  @configured_user_key :ferricstore_acl_configured_user
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
    put_configured_user_witness(nil)
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

  @spec insert_default_user(non_neg_integer()) :: true
  def insert_default_user(auth_epoch \\ 0) do
    :ets.insert(
      active_table(),
      {"default",
       %{
         enabled: true,
         auth_epoch: auth_epoch,
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
    witness = configured_user_witness(users)
    cleanup_named_table(@swap_new_table)
    new_table = :ets.new(@swap_new_table, [:set, :public, :named_table, read_concurrency: true])
    :ets.insert(new_table, users)
    maybe_run_load_swap_hook()
    swap_active_table(new_table)
    put_configured_user_witness(witness)
  end

  @doc false
  @spec configured_user?(:ets.tid() | atom()) :: boolean()
  def configured_user?(table) do
    case :persistent_term.get(@configured_user_key, nil) do
      username when is_binary(username) ->
        case :ets.lookup(table, username) do
          [{^username, user}] -> configured_user_record?(user)
          _missing -> false
        end

      _none ->
        false
    end
  rescue
    ArgumentError -> false
  end

  @doc false
  @spec update_configured_user_witness(binary(), map()) :: :ok
  def update_configured_user_witness(username, user) when is_binary(username) and is_map(user) do
    if configured_user_record?(user) do
      put_configured_user_witness(username)
    else
      remove_configured_user_witnesses([username])
    end
  end

  @doc false
  @spec remove_configured_user_witnesses([binary()]) :: :ok
  def remove_configured_user_witnesses(usernames) when is_list(usernames) do
    case :persistent_term.get(@configured_user_key, nil) do
      username when is_binary(username) ->
        if username in usernames, do: refresh_configured_user_witness(), else: :ok

      _none ->
        :ok
    end
  end

  @doc false
  @spec clear_configured_user_witness() :: :ok
  def clear_configured_user_witness, do: put_configured_user_witness(nil)

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

  defp refresh_configured_user_witness do
    match_spec = [
      {{:"$1", %{enabled: true, password: :"$2"}}, [{:is_binary, :"$2"}], [:"$1"]}
    ]

    witness =
      case :ets.select(active_table(), match_spec, 1) do
        {[username], _continuation} -> username
        :"$end_of_table" -> nil
      end

    put_configured_user_witness(witness)
  rescue
    ArgumentError -> put_configured_user_witness(nil)
  end

  defp configured_user_witness(users) do
    Enum.find_value(users, fn
      {username, user} when is_binary(username) ->
        if configured_user_record?(user), do: username

      _invalid ->
        nil
    end)
  end

  defp configured_user_record?(%{enabled: true, password: password}) when is_binary(password),
    do: true

  defp configured_user_record?(_user), do: false

  defp put_configured_user_witness(username) when is_binary(username) or is_nil(username) do
    :persistent_term.put(@configured_user_key, username)
    :ok
  end
end
