# Suppress function clause grouping warnings (clauses added by different agents)
defmodule FerricstoreServer.Commands.Client do
  @moduledoc """
  Handles native protocol CLIENT metadata commands.

  Legacy client-tracking invalidation was removed with the text protocol
  listener. Native SDKs should use protocol event subscriptions instead of
  CLIENT TRACKING/CACHING.
  """

  @doc """
  Handles a CLIENT subcommand.

  ## Parameters

    - `subcmd` - Uppercased subcommand name (e.g. `"ID"`, `"SETNAME"`)
    - `args` - List of string arguments after the subcommand
    - `conn_state` - Per-connection state map
    - `store` - Injected store map (unused by most CLIENT commands)

  ## Returns

  A tuple `{result, updated_conn_state}` when the command mutates connection state
  (e.g. SETNAME), or `{result, conn_state}` for read-only commands.
  """
  @spec handle(binary(), [binary()], map(), map()) :: {term(), map()}
  def handle(subcmd, args, conn_state, store)

  def handle("ID", [], conn_state, _store) do
    {conn_state.client_id, conn_state}
  end

  def handle("ID", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|id' command"}, conn_state}
  end

  def handle("SETNAME", [name], conn_state, _store) do
    if has_invalid_name_chars?(name) do
      {{:error, "ERR Client names cannot contain spaces, newlines or special characters."},
       conn_state}
    else
      {:ok, %{conn_state | client_name: name}}
    end
  end

  def handle("SETNAME", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|setname' command"}, conn_state}
  end

  def handle("GETNAME", [], conn_state, _store) do
    {conn_state.client_name, conn_state}
  end

  def handle("GETNAME", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|getname' command"}, conn_state}
  end

  def handle("INFO", [], conn_state, _store) do
    {format_client_info(conn_state), conn_state}
  end

  def handle("INFO", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|info' command"}, conn_state}
  end

  def handle("LIST", args, conn_state, _store) when args in [[], ["TYPE", "normal"]] do
    info = format_client_info(conn_state)
    {info, conn_state}
  end

  def handle("LIST", ["TYPE", type], conn_state, _store)
      when type in ~w(master replica pubsub) do
    {"", conn_state}
  end

  def handle("LIST", ["TYPE", bad], conn_state, _store) do
    {{:error, "ERR Unknown client type '#{bad}'"}, conn_state}
  end

  def handle("LIST", _args, conn_state, _store) do
    {{:error, "ERR syntax error"}, conn_state}
  end

  def handle("TRACKING", _args, conn_state, _store) do
    {{:error,
      "ERR CLIENT TRACKING is not supported by the native protocol; use event subscriptions"},
     conn_state}
  end

  def handle("CACHING", _args, conn_state, _store) do
    {{:error, "ERR CLIENT CACHING is not supported by the native protocol"}, conn_state}
  end

  def handle("TRACKINGINFO", [], conn_state, _store) do
    {%{enabled: false, protocol: "ferric", replacement: "SUBSCRIBE_EVENTS"}, conn_state}
  end

  def handle("TRACKINGINFO", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|trackinginfo' command"}, conn_state}
  end

  def handle("GETREDIR", [], conn_state, _store) do
    {0, conn_state}
  end

  def handle("GETREDIR", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|getredir' command"}, conn_state}
  end

  def handle("KILL", ["ID", id], conn_state, _store) do
    with {client_id, ""} <- Integer.parse(id),
         :ok <-
           FerricstoreServer.Connection.Registry.kill(
             client_id,
             Map.get(conn_state, :conn_pid, self())
           ) do
      {:ok, conn_state}
    else
      :error ->
        {{:error, "ERR value is not an integer or out of range"}, conn_state}

      {:error, :self} ->
        {{:error, "ERR I won't kill myself"}, conn_state}

      {:error, :not_found} ->
        {{:error, "ERR No such client"}, conn_state}

      {_client_id, _rest} ->
        {{:error, "ERR value is not an integer or out of range"}, conn_state}
    end
  end

  def handle("KILL", _args, conn_state, _store) do
    {{:error, "ERR syntax error"}, conn_state}
  end

  def handle("PAUSE", _args, conn_state, _store) do
    {{:error, "ERR CLIENT PAUSE is not supported by the native protocol"}, conn_state}
  end

  def handle("UNPAUSE", _args, conn_state, _store) do
    {{:error, "ERR CLIENT UNPAUSE is not supported by the native protocol"}, conn_state}
  end

  def handle("NO-EVICT", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {{:error, "ERR CLIENT NO-EVICT is not supported by the native protocol"}, conn_state}
  end

  def handle("NO-EVICT", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|no-evict' command"}, conn_state}
  end

  def handle("NO-TOUCH", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {{:error, "ERR CLIENT NO-TOUCH is not supported by the native protocol"}, conn_state}
  end

  def handle("NO-TOUCH", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|no-touch' command"}, conn_state}
  end

  def handle(subcmd, _args, conn_state, _store) do
    {{:error, "ERR unknown subcommand '#{subcmd}'. Try CLIENT HELP."}, conn_state}
  end

  # -- Private helpers --------------------------------------------------------

  defp format_client_info(conn_state) do
    id = conn_state.client_id
    name = conn_state.client_name || ""

    {addr, fd} =
      case conn_state do
        %{peer: {ip, port}} ->
          ip_str = :inet.ntoa(ip) |> to_string()
          {"#{ip_str}:#{port}", 0}

        _ ->
          {"unknown:0", 0}
      end

    age = div(System.monotonic_time(:millisecond) - conn_state.created_at, 1000)
    "id=#{id} addr=#{addr} fd=#{fd} name=#{name} age=#{age}\n"
  end

  # Redis rejects client names containing spaces or any byte < 0x20
  # (control characters: newlines, tabs, null bytes, etc.)
  defp has_invalid_name_chars?(name) do
    name
    |> :binary.bin_to_list()
    |> Enum.any?(fn byte -> byte <= 0x20 end)
  end
end
