defmodule FerricstoreServer.Acl.Protection do
  @moduledoc false

  alias Ferricstore.AuditLog
  alias FerricstoreServer.Acl.Tables

  @spec protected_mode?() :: boolean()
  def protected_mode? do
    Application.get_env(:ferricstore, :protected_mode, true)
  end

  @spec has_configured_users?() :: boolean()
  def has_configured_users? do
    requirepass_configured?() or
      configured_users_in_table?(Tables.active_table())
  end

  @spec localhost?({:inet.ip_address(), :inet.port_number()} | nil) :: boolean()
  def localhost?(nil), do: false
  def localhost?({{127, 0, 0, 1}, _port}), do: true
  def localhost?({{0, 0, 0, 0, 0, 0, 0, 1}, _port}), do: true
  def localhost?(_peer), do: false

  @spec check_protected_mode({:inet.ip_address(), :inet.port_number()} | nil) ::
          :ok | {:error, binary()}
  def check_protected_mode(peer) do
    if protected_mode?() and not has_configured_users?() and not localhost?(peer) do
      {:error,
       "DENIED Redis is running in protected mode because protected mode is enabled and no password is configured for the default user. Connections are only accepted from localhost."}
    else
      :ok
    end
  end

  @spec log_command_denied(binary(), binary(), binary(), term()) :: :ok
  def log_command_denied(username, command, client_ip, client_id) do
    AuditLog.log(:command_denied, %{
      username: username,
      command: command,
      client_ip: client_ip,
      client_id: client_id,
      timestamp: System.system_time(:millisecond)
    })
  end

  defp requirepass_configured? do
    app_configured?() or runtime_configured?()
  end

  defp app_configured? do
    Application.get_env(:ferricstore, :requirepass) not in [nil, ""]
  end

  defp runtime_configured? do
    Ferricstore.Config.get_value("requirepass") not in [nil, ""]
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp configured_users_in_table?(table) do
    if table_exists?(table) do
      Enum.any?(:ets.tab2list(table), fn
        {_name, %{enabled: true, password: password}} when is_binary(password) -> true
        _ -> false
      end)
    else
      false
    end
  end

  defp table_exists?(table) do
    :ets.info(table) != :undefined
  rescue
    ArgumentError -> false
  end
end
