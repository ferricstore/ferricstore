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
      configured_users_in_table?()
  end

  @spec localhost?({:inet.ip_address(), :inet.port_number()} | nil) :: boolean()
  def localhost?(nil), do: false
  def localhost?({{127, 0, 0, 1}, _port}), do: true
  def localhost?({{0, 0, 0, 0, 0, 0, 0, 1}, _port}), do: true
  def localhost?(_peer), do: false

  @spec check_protected_mode({:inet.ip_address(), :inet.port_number()} | nil) ::
          :ok | {:error, binary()}
  def check_protected_mode(peer) do
    if protected_mode?() and not localhost?(peer) and not has_configured_users?() do
      {:error,
       "DENIED FerricStore is running in protected mode because protected mode is enabled and no password is configured for the default user. Connections are only accepted from localhost."}
    else
      :ok
    end
  end

  @doc """
  Logs a denied command to the ACL LOG (always) and the audit log (when it
  is on). `reason` is the denial's NOPERM text: it tells a key or channel
  denial from a command one.
  """
  @spec log_command_denied(binary(), binary(), binary(), term(), binary() | nil) :: :ok
  def log_command_denied(username, command, client_ip, client_id, reason \\ nil) do
    FerricstoreServer.Acl.Log.record(
      denial_reason(reason),
      String.downcase(to_string(command)),
      username,
      "id=#{client_id} addr=#{client_ip}"
    )

    AuditLog.log(:command_denied, %{
      username: username,
      command: command,
      client_ip: client_ip,
      client_id: client_id,
      timestamp: System.system_time(:millisecond)
    })
  end

  defp denial_reason(reason) when is_binary(reason) do
    cond do
      String.contains?(reason, "one of the keys") -> "key"
      String.contains?(reason, "one of the channels") -> "channel"
      true -> "command"
    end
  end

  defp denial_reason(_reason), do: "command"

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

  defp configured_users_in_table? do
    Tables.read(fn table -> Tables.configured_user?(table) end)
  rescue
    ArgumentError -> false
  end
end
