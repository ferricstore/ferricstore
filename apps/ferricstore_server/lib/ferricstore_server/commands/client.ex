# Suppress function clause grouping warnings (clauses added by different agents)
defmodule FerricstoreServer.Commands.Client do
  @moduledoc """
  Handles Redis CLIENT subcommands: ID, SETNAME, GETNAME, INFO, LIST,
  TRACKING, CACHING, TRACKINGINFO, GETREDIR.

  Unlike most command modules, CLIENT commands require access to per-connection
  state (client ID, client name, address, creation time). The connection layer
  passes this state via a `conn_state` map with the following shape:

      %{
        client_id: pos_integer(),
        client_name: binary() | nil,
        created_at: integer(),         # monotonic ms
        peer: {ip_tuple, port},
        conn_pid: pid(),               # the connection process pid
        tracking: ClientTracking.tracking_config()
      }

  ## Supported subcommands

    * `CLIENT ID` -- returns the unique client connection ID
    * `CLIENT SETNAME name` -- sets the connection name
    * `CLIENT GETNAME` -- returns the connection name or nil
    * `CLIENT INFO` -- returns info about the current connection
    * `CLIENT LIST [TYPE normal|master|replica|pubsub]` -- returns info about all connections
    * `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]`
    * `CLIENT CACHING YES|NO` -- control per-command caching in OPTIN/OPTOUT mode
    * `CLIENT TRACKINGINFO` -- returns the current tracking configuration
    * `CLIENT GETREDIR` -- returns the redirect target client ID or 0
  """

  alias FerricstoreServer.ClientTracking

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

  def handle("TRACKING", [toggle | opts], conn_state, _store) do
    case String.upcase(toggle) do
      "ON" -> do_tracking_on(opts, conn_state)
      "OFF" -> do_tracking_off(conn_state)
      _ -> {{:error, "ERR syntax error: CLIENT TRACKING requires ON or OFF"}, conn_state}
    end
  end

  def handle("TRACKING", [], conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|tracking' command"}, conn_state}
  end

  def handle("CACHING", [value], conn_state, _store) do
    case String.upcase(value) do
      "YES" ->
        tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())

        case ClientTracking.set_caching(tracking, true) do
          {:ok, new_tracking} -> {:ok, %{conn_state | tracking: new_tracking}}
          {:error, _msg} = err -> {err, conn_state}
        end

      "NO" ->
        tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())

        case ClientTracking.set_caching(tracking, false) do
          {:ok, new_tracking} -> {:ok, %{conn_state | tracking: new_tracking}}
          {:error, _msg} = err -> {err, conn_state}
        end

      _ ->
        {{:error, "ERR syntax error: CLIENT CACHING requires YES or NO"}, conn_state}
    end
  end

  def handle("CACHING", [], conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|caching' command"}, conn_state}
  end

  def handle("CACHING", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|caching' command"}, conn_state}
  end

  def handle("TRACKINGINFO", [], conn_state, _store) do
    tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())
    {ClientTracking.tracking_info(tracking), conn_state}
  end

  def handle("TRACKINGINFO", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|trackinginfo' command"}, conn_state}
  end

  def handle("GETREDIR", [], conn_state, _store) do
    tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())
    {ClientTracking.get_redirect(tracking), conn_state}
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

  def handle("PAUSE", _args, conn_state, _store), do: {:ok, conn_state}
  def handle("UNPAUSE", _args, conn_state, _store), do: {:ok, conn_state}

  def handle("NO-EVICT", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {:ok, conn_state}
  end

  def handle("NO-EVICT", _args, conn_state, _store) do
    {{:error, "ERR wrong number of arguments for 'client|no-evict' command"}, conn_state}
  end

  def handle("NO-TOUCH", [flag], conn_state, _store) when flag in ~w(ON OFF on off) do
    {:ok, conn_state}
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

  defp do_tracking_on(raw_opts, conn_state) do
    conn_pid = Map.get(conn_state, :conn_pid, self())
    tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())

    with {:ok, parsed} <- parse_opts(raw_opts),
         :ok <- validate_prefix_bcast(parsed) do
      kw = [
        mode: if(parsed.bcast, do: :bcast, else: :default),
        prefixes: parsed.prefixes,
        redirect: parsed.redirect,
        optin: parsed.optin,
        optout: parsed.optout,
        noloop: parsed.noloop
      ]

      case ClientTracking.enable(conn_pid, tracking, kw) do
        {:ok, new_tracking} -> {:ok, %{conn_state | tracking: new_tracking}}
        {:error, _} = err -> {err, conn_state}
      end
    else
      {:error, _} = err -> {err, conn_state}
    end
  end

  defp validate_prefix_bcast(parsed) do
    if parsed.prefixes != [] and not parsed.bcast do
      {:error, "ERR PREFIX requires BCAST mode to be enabled"}
    else
      :ok
    end
  end

  defp do_tracking_off(conn_state) do
    conn_pid = Map.get(conn_state, :conn_pid, self())
    tracking = Map.get(conn_state, :tracking, ClientTracking.new_config())
    {:ok, new_tracking} = ClientTracking.disable(conn_pid, tracking)
    {:ok, %{conn_state | tracking: new_tracking}}
  end

  defp parse_opts(raw_opts) do
    do_parse_opts(raw_opts, %{
      bcast: false,
      prefixes: [],
      redirect: nil,
      optin: false,
      optout: false,
      noloop: false
    })
  end

  defp do_parse_opts([], acc), do: {:ok, acc}

  defp do_parse_opts([token | rest], acc) do
    case String.upcase(token) do
      "BCAST" -> do_parse_opts(rest, %{acc | bcast: true})
      "OPTIN" -> do_parse_opts(rest, %{acc | optin: true})
      "OPTOUT" -> do_parse_opts(rest, %{acc | optout: true})
      "NOLOOP" -> do_parse_opts(rest, %{acc | noloop: true})
      "PREFIX" -> parse_prefix(rest, acc)
      "REDIRECT" -> parse_redirect(rest, acc)
      _ -> {:error, "ERR syntax error: unrecognized CLIENT TRACKING option '#{token}'"}
    end
  end

  defp parse_prefix([prefix | rest], acc) do
    do_parse_opts(rest, %{acc | prefixes: acc.prefixes ++ [prefix]})
  end

  defp parse_prefix([], _acc) do
    {:error, "ERR syntax error: PREFIX requires a value"}
  end

  defp parse_redirect([id_str | rest], acc) do
    pid = do_resolve_redirect(id_str)
    do_parse_opts(rest, %{acc | redirect: pid})
  end

  defp parse_redirect([], _acc) do
    {:error, "ERR syntax error: REDIRECT requires a client ID"}
  end

  defp do_resolve_redirect(id_str) do
    raw =
      id_str
      |> String.replace_leading("#PID", "")
      |> String.to_charlist()

    :erlang.list_to_pid(raw)
  rescue
    _ -> nil
  end

  # Redis rejects client names containing spaces or any byte < 0x20
  # (control characters: newlines, tabs, null bytes, etc.)
  defp has_invalid_name_chars?(name) do
    name
    |> :binary.bin_to_list()
    |> Enum.any?(fn byte -> byte <= 0x20 end)
  end
end
