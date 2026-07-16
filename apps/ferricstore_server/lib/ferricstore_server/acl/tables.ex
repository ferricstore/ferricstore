defmodule FerricstoreServer.Acl.Tables do
  @moduledoc false

  @table :ferricstore_acl
  @active_table_key :ferricstore_acl_active_table
  @configured_user_key :ferricstore_acl_configured_user
  @retired_table_grace_ms 5_000

  @spec new_active_table() :: :ets.tid()
  def new_active_table do
    table = :ets.new(@table, [:set, :public, read_concurrency: true])
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
    new_table = :ets.new(@table, [:set, :public, read_concurrency: true])
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

  @doc false
  @spec cleanup_retired_table(:ets.tid()) :: :ok
  def cleanup_retired_table(table) do
    cond do
      table == active_table() -> :ok
      :ets.info(table) == :undefined -> :ok
      true -> :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end

  defp maybe_run_load_swap_hook do
    case :persistent_term.get(:ferricstore_acl_before_load_swap_hook, nil) do
      nil -> :ok
      fun when is_function(fun, 0) -> fun.()
    end
  end

  defp swap_active_table(new_table) do
    previous_table = active_table()
    :persistent_term.put(@active_table_key, new_table)
    schedule_retired_table_cleanup(previous_table)
    :ok
  end

  defp schedule_retired_table_cleanup(table) do
    if :ets.info(table) != :undefined do
      Process.send_after(
        self(),
        {:cleanup_acl_retired_table, table},
        @retired_table_grace_ms
      )
    end

    :ok
  rescue
    ArgumentError -> :ok
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
