defmodule FerricstoreServer.Connection.Auth do
  @moduledoc """
  Auth and ACL command handling extracted from Connection.

  All functions accept and return the connection state as a map/struct.
  """

  alias Ferricstore.AuditLog
  alias FerricstoreServer.Acl.CommandCategories
  alias FerricstoreServer.Resp.Encoder

  @global_keyspace_enumeration_commands ~w(KEYS SCAN RANDOMKEY DBSIZE)
  @acl_subcommands ~w(CAT DELUSER GETUSER LIST LOAD LOG SAVE SETUSER WHOAMI)

  # ── AUTH dispatch ──────────────────────────────────────────────────────

  @spec dispatch_auth([binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_auth([], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}),
     state}
  end

  def dispatch_auth([_, _, _ | _], state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'auth' command"}),
     state}
  end

  def dispatch_auth(args, state) do
    {username, password} =
      case args do
        [pass] -> {"default", pass}
        [user, pass] -> {user, pass}
      end

    requirepass = Ferricstore.Config.get_value("requirepass")
    acl_user = FerricstoreServer.Acl.get_user(username)
    client_ip = format_peer(state.peer)

    # Determine whether any auth source is configured for this user.
    has_acl_password = acl_user != nil and acl_user.password != nil
    has_requirepass = requirepass != nil and requirepass != ""

    do_dispatch_auth(
      has_acl_password,
      has_requirepass,
      username,
      password,
      requirepass,
      client_ip,
      state
    )
  end

  # ── ACL subcommand dispatch ────────────────────────────────────────────

  @spec dispatch_acl(binary(), [binary()], map()) ::
          {:continue, iodata(), map()} | {:quit, iodata(), map()}
  def dispatch_acl("WHOAMI", _, state) do
    {:continue, Encoder.encode(state.username), state}
  end

  def dispatch_acl("LIST", _, state) do
    {:continue, Encoder.encode(FerricstoreServer.Acl.list_users()), state}
  end

  def dispatch_acl("SAVE", [], state) do
    case FerricstoreServer.Acl.save() do
      :ok ->
        {:continue, Encoder.encode(:ok), state}

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  def dispatch_acl("SAVE", _args, state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl|save' command"}),
     state}
  end

  def dispatch_acl("LOAD", [], state) do
    with {:ok, contents} <- FerricstoreServer.Acl.load_file_contents(),
         {:ok, ctx} <- default_instance_ctx() do
      case Ferricstore.Store.Router.server_command(ctx, {:acl_load, contents}) do
        :ok ->
          broadcast_acl_invalidation(:all)
          {:continue, Encoder.encode(:ok), maybe_refresh_acl_cache(state, :all)}

        {:error, reason} ->
          {:continue, Encoder.encode({:error, reason}), state}
      end
    else
      :error ->
        loading(state)

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  def dispatch_acl("LOAD", _args, state) do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'acl|load' command"}),
     state}
  end

  def dispatch_acl("SETUSER", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'acl|setuser' command"}), state}
  end

  def dispatch_acl("SETUSER", [username | rules], state) do
    # Route through Raft so the mutation is replicated to all nodes.
    with {:ok, ctx} <- default_instance_ctx() do
      result = Ferricstore.Store.Router.server_command(ctx, {:acl_setuser, username, rules})

      case result do
        :ok ->
          broadcast_acl_invalidation(username)

          new_state =
            if username == state.username do
              refresh_acl_session(state)
            else
              state
            end

          {:continue, Encoder.encode(:ok), new_state}

        {:error, reason} ->
          {:continue, Encoder.encode({:error, reason}), state}
      end
    else
      :error -> loading(state)
    end
  end

  def dispatch_acl("DELUSER", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'acl|deluser' command"}), state}
  end

  def dispatch_acl("DELUSER", usernames, state) do
    with {:ok, ctx} <- default_instance_ctx() do
      case Ferricstore.Store.Router.server_command(ctx, {:acl_delusers, usernames}) do
        :ok ->
          Enum.each(usernames, &broadcast_acl_invalidation/1)
          state = Enum.reduce(usernames, state, &maybe_refresh_acl_cache(&2, &1))
          {:continue, Encoder.encode(:ok), state}

        {:error, reason} ->
          {:continue, Encoder.encode({:error, reason}), state}
      end
    else
      :error -> loading(state)
    end
  end

  def dispatch_acl("CAT", [], state) do
    {:continue, Encoder.encode(CommandCategories.category_names_lower()), state}
  end

  def dispatch_acl("CAT", [category | _], state) do
    case CommandCategories.category_commands(category) do
      {:ok, commands} ->
        commands =
          commands
          |> MapSet.to_list()
          |> Enum.map(&String.downcase/1)
          |> Enum.sort()

        {:continue, Encoder.encode(commands), state}

      :error ->
        {:continue,
         Encoder.encode(
           {:error,
            "ERR Unknown category '#{category}'. Try ACL CAT without arguments for a list."}
         ), state}
    end
  end

  def dispatch_acl("LOG", ["RESET" | _], state) do
    AuditLog.reset()
    {:continue, Encoder.encode(:ok), state}
  end

  def dispatch_acl("LOG", ["COUNT", count_str | _], state) do
    case Integer.parse(count_str) do
      {count, ""} when count >= 0 ->
        entries = AuditLog.get(count) |> AuditLog.format_entries()
        {:continue, Encoder.encode(entries), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR value is not an integer or out of range"}),
         state}
    end
  end

  def dispatch_acl("LOG", [], state) do
    entries = AuditLog.get() |> AuditLog.format_entries()
    {:continue, Encoder.encode(entries), state}
  end

  def dispatch_acl("LOG", _, state) do
    entries = AuditLog.get() |> AuditLog.format_entries()
    {:continue, Encoder.encode(entries), state}
  end

  def dispatch_acl("GETUSER", [username | _], state) do
    case FerricstoreServer.Acl.get_user_info(username) do
      nil ->
        {:continue, Encoder.encode(nil), state}

      info ->
        {:continue, Encoder.encode(info), state}
    end
  end

  def dispatch_acl("GETUSER", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'acl|getuser' command"}), state}
  end

  def dispatch_acl(_, _, state) do
    {:continue,
     Encoder.encode(
       {:error, "ERR unknown subcommand or wrong number of arguments for 'acl' command"}
     ), state}
  end

  # ── ACL cache ──────────────────────────────────────────────────────────

  @spec user_requires_auth?(binary()) :: boolean()
  def user_requires_auth?(username) do
    case FerricstoreServer.Acl.get_user(username) do
      %{password: password} when is_binary(password) -> true
      _ -> false
    end
  end

  @spec build_acl_cache(binary()) :: map() | :full_access | :denied
  def build_acl_cache(username) do
    case FerricstoreServer.Acl.get_user(username) do
      nil ->
        if username == "default", do: :full_access, else: :denied

      user ->
        denied = Map.get(user, :denied_commands, MapSet.new())
        channels = Map.get(user, :channels, :all)

        if user.enabled and user.commands == :all and
             MapSet.size(denied) == 0 and user.keys == :all and channels == :all do
          :full_access
        else
          %{
            commands: user.commands,
            denied_commands: denied,
            keys: user.keys,
            channels: channels,
            enabled: user.enabled
          }
        end
    end
  end

  # ── Command permission checks (called from pipeline dispatch path) ───

  @spec check_command_cached(map() | :full_access | :denied | nil, binary()) ::
          :ok | {:error, binary()}

  # ACL WHOAMI only returns the authenticated username, but disabled/deleted
  # users must still fail closed after ACL invalidation refreshes the cache.
  def check_command_cached(:full_access, "ACL.WHOAMI"), do: :ok
  def check_command_cached(%{enabled: true}, "ACL.WHOAMI"), do: :ok

  # Deleted user, unknown user, or missing cache -- deny all commands.
  def check_command_cached(:denied, _cmd),
    do: {:error, "NOPERM user session expired or user was deleted"}

  def check_command_cached(nil, _cmd),
    do: {:error, "NOPERM user session expired or user was deleted"}

  # Fast path: unrestricted user — single atom comparison, zero MapSet/map ops.
  def check_command_cached(:full_access, _cmd), do: :ok

  # Fast path: full-access user with no denied commands — skip all MapSet ops.
  def check_command_cached(
        %{commands: :all, denied_commands: %MapSet{map: denied_map}, enabled: true},
        _cmd
      )
      when map_size(denied_map) == 0 do
    :ok
  end

  def check_command_cached(cache, cmd) do
    cond do
      not cache.enabled ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      cache.commands == :all and not cached_command_denied?(cache.denied_commands, cmd) ->
        :ok

      cache.commands == :all ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      cached_command_allowed?(cache.commands, cmd) and
          not cached_command_denied?(cache.denied_commands, cmd) ->
        :ok

      true ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
    end
  end

  # ── Key pattern checks ─────────────────────────────────────────────────

  @spec check_keys_cached(map() | :full_access | :denied | nil, binary(), [binary()]) ::
          :ok | {:error, binary()}
  def check_keys_cached(:denied, _cmd, _keys),
    do: {:error, "NOPERM user session expired or user was deleted"}

  def check_keys_cached(nil, _cmd, _keys),
    do: {:error, "NOPERM user session expired or user was deleted"}

  def check_keys_cached(:full_access, _cmd, _keys), do: :ok
  def check_keys_cached(%{keys: :all}, _cmd, _keys), do: :ok

  def check_keys_cached(%{keys: patterns}, cmd, []) when is_list(patterns) do
    if global_keyspace_enumeration_command?(cmd) and
         not unrestricted_read_key_patterns?(patterns) do
      {:error,
       "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
    else
      :ok
    end
  end

  def check_keys_cached(%{keys: patterns}, cmd, keys) when is_list(keys) do
    case keys do
      [] ->
        :ok

      keys ->
        case key_access_plan(cmd, keys) do
          {:mixed, plan} -> check_key_access_plan(plan, patterns)
          {:uniform, access_type} -> check_all_keys(keys, access_type, patterns)
        end
    end
  end

  @spec check_channels_cached(map() | :full_access | :denied | nil, [binary()]) ::
          :ok | {:error, binary()}
  def check_channels_cached(:denied, _channels),
    do: {:error, "NOPERM user session expired or user was deleted"}

  def check_channels_cached(nil, _channels),
    do: {:error, "NOPERM user session expired or user was deleted"}

  def check_channels_cached(:full_access, _channels), do: :ok
  def check_channels_cached(%{channels: :all}, _channels), do: :ok
  def check_channels_cached(%{channels: patterns}, []) when is_list(patterns), do: :ok

  def check_channels_cached(%{channels: patterns}, channels)
      when is_list(patterns) and is_list(channels) do
    if Enum.all?(channels, &FerricstoreServer.Acl.channel_matches_any?(&1, patterns)) do
      :ok
    else
      {:error,
       "NOPERM this user has no permissions to access one of the channels mentioned in the command"}
    end
  end

  # ── Helpers (public for Connection to call) ─────────────────────────────

  @spec check_all_keys([binary()], :read | :write | :rw, [FerricstoreServer.Acl.key_pattern()]) ::
          :ok | {:error, binary()}
  def check_all_keys([], _access_type, _patterns), do: :ok

  def check_all_keys([key | rest], access_type, patterns) do
    types_to_check =
      case access_type do
        :rw -> [:read, :write]
        other -> [other]
      end

    all_pass =
      Enum.all?(types_to_check, fn t ->
        FerricstoreServer.Acl.key_matches_any?(key, t, patterns)
      end)

    if all_pass do
      check_all_keys(rest, access_type, patterns)
    else
      {:error,
       "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
    end
  end

  @spec command_access_type(binary()) :: :read | :write | :rw
  def command_access_type(cmd), do: CommandCategories.command_access_type(cmd)

  defp key_access_plan(cmd, keys) when is_binary(cmd) do
    case String.upcase(cmd) do
      # Source/destination commands must not treat every key as a write key:
      # the command reads source data and writes only the destination.
      "COPY" ->
        source_destination_plan(keys, :read)

      # BITOP reads every source key and writes only the destination key.
      # Keeping this as a per-command plan avoids forcing source keys to have
      # write access while also preventing write-only source patterns from
      # leaking read access.
      cmd when cmd in ~w(BITOP PFMERGE SDIFFSTORE SINTERSTORE SUNIONSTORE GEOSEARCHSTORE
                         CMS.MERGE TDIGEST.MERGE ZINTERSTORE ZUNIONSTORE) ->
        destination_sources_plan(keys)

      cmd when cmd in ~w(RENAME RENAMENX LMOVE BLMOVE RPOPLPUSH SMOVE) ->
        source_destination_plan(keys, :read_write)

      _ ->
        {:uniform, command_access_type(cmd)}
    end
  end

  defp key_access_plan(cmd, _keys), do: {:uniform, command_access_type(cmd)}

  defp destination_sources_plan([dest | sources]),
    do: {:mixed, [{dest, :write} | Enum.map(sources, &{&1, :read})]}

  defp destination_sources_plan(_keys), do: {:mixed, []}

  defp source_destination_plan([source, destination | _rest], :read),
    do: {:mixed, [{source, :read}, {destination, :write}]}

  defp source_destination_plan([source, destination | _rest], :read_write),
    do: {:mixed, [{source, :read}, {source, :write}, {destination, :write}]}

  defp source_destination_plan(_keys, _source_access), do: {:mixed, []}

  defp check_key_access_plan([], _patterns), do: :ok

  defp check_key_access_plan([{key, access_type} | rest], patterns) do
    if FerricstoreServer.Acl.key_matches_any?(key, access_type, patterns) do
      check_key_access_plan(rest, patterns)
    else
      {:error,
       "NOPERM this user has no permissions to access one of the keys mentioned in the command"}
    end
  end

  defp global_keyspace_enumeration_command?(cmd),
    do: String.upcase(cmd) in @global_keyspace_enumeration_commands

  defp unrestricted_read_key_patterns?(patterns) do
    Enum.any?(patterns, fn
      {"*", mode, _regex} when mode in [:rw, :read] -> true
      _other -> false
    end)
  end

  @doc false
  @spec acl_command_name(binary(), [binary()], term()) :: binary()
  def acl_command_name("CLIENT", ["HELLO" | _rest], _ast), do: "HELLO"

  def acl_command_name("CLIENT", [subcmd | _rest], _ast) when is_binary(subcmd) do
    "CLIENT." <> String.upcase(subcmd)
  end

  def acl_command_name("ACL", [subcmd | _rest], _ast) when is_binary(subcmd) do
    subcmd = String.upcase(subcmd)

    if subcmd in @acl_subcommands do
      "ACL." <> subcmd
    else
      "ACL"
    end
  end

  def acl_command_name(cmd, _args, _ast), do: cmd

  defp cached_command_denied?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (cached_parent_supported?(cmd) and MapSet.member?(commands, cached_command_parent(cmd)))
  end

  defp cached_command_allowed?(commands, cmd) do
    MapSet.member?(commands, cmd) or
      (cached_parent_supported?(cmd) and MapSet.member?(commands, cached_command_parent(cmd)))
  end

  defp cached_parent_supported?(cmd) do
    case cached_command_parent(cmd) do
      nil -> false
      parent -> MapSet.member?(CommandCategories.acl_supported_commands(), parent)
    end
  end

  defp cached_command_parent(cmd) do
    case String.split(cmd, ".", parts: 2) do
      [parent, _subcommand] -> parent
      _ -> nil
    end
  end

  # ── ACL invalidation broadcasting ──────────────────────────────────────

  @acl_pg_group :ferricstore_acl_connections

  @spec broadcast_acl_invalidation(binary() | :all) :: :ok
  def broadcast_acl_invalidation(username) do
    members =
      try do
        :pg.get_members(@acl_pg_group, @acl_pg_group)
      catch
        :error, _ -> []
      end

    for pid <- members, pid != self() do
      send(pid, {:acl_invalidate, username})
    end

    :ok
  end

  @spec maybe_refresh_acl_cache(map(), binary() | :all) :: map()
  def maybe_refresh_acl_cache(state, :all),
    do: refresh_acl_session(state)

  def maybe_refresh_acl_cache(state, invalidated_username) do
    if invalidated_username == state.username do
      refresh_acl_session(state)
    else
      state
    end
  end

  @spec refresh_acl_session(map()) :: map()
  def refresh_acl_session(state) do
    %{
      state
      | acl_cache: build_acl_cache(state.username),
        require_auth: user_requires_auth?(state.username)
    }
  end

  # ── Internal helpers ───────────────────────────────────────────────────

  @doc false
  def constant_time_equal?(a, b) when is_binary(a) and is_binary(b) do
    hash_a = :crypto.hash(:sha256, a)
    hash_b = :crypto.hash(:sha256, b)
    :crypto.hash_equals(hash_a, hash_b)
  end

  @doc false
  def do_acl_auth(username, password, client_ip, state) do
    case FerricstoreServer.Acl.authenticate(username, password) do
      {:ok, ^username} ->
        AuditLog.log(:auth_success, %{username: username, client_ip: client_ip})
        new_cache = build_acl_cache(username)

        {:continue, Encoder.encode(:ok),
         %{
           state
           | authenticated: true,
             username: username,
             acl_cache: new_cache,
             require_auth: user_requires_auth?(username)
         }}

      {:error, reason} ->
        AuditLog.log(:auth_failure, %{username: username, client_ip: client_ip})
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  # ── Private ────────────────────────────────────────────────────────────

  defp do_dispatch_auth(false, false, _user, _pass, _rp, _ip, state) do
    {:continue,
     Encoder.encode(
       {:error,
        "ERR Client sent AUTH, but no password is set. Did you mean ACL SETUSER with >password?"}
     ), state}
  end

  defp do_dispatch_auth(true, _has_rp, username, password, _rp, client_ip, state) do
    do_acl_auth(username, password, client_ip, state)
  end

  defp do_dispatch_auth(false, true, "default", password, requirepass, client_ip, state) do
    if constant_time_equal?(password, requirepass) do
      AuditLog.log(:auth_success, %{username: "default", client_ip: client_ip})
      new_cache = build_acl_cache("default")

      {:continue, Encoder.encode(:ok),
       %{
         state
         | authenticated: true,
           username: "default",
           acl_cache: new_cache,
           require_auth: user_requires_auth?("default")
       }}
    else
      AuditLog.log(:auth_failure, %{username: "default", client_ip: client_ip})

      {:continue,
       Encoder.encode({:error, "WRONGPASS invalid username-password pair or user is disabled."}),
       state}
    end
  end

  defp do_dispatch_auth(false, true, username, password, _rp, client_ip, state) do
    do_acl_auth(username, password, client_ip, state)
  end

  defp default_instance_ctx do
    {:ok, FerricStore.Instance.get(:default)}
  rescue
    ArgumentError -> :error
  end

  defp loading(state) do
    {:continue, Encoder.encode({:error, "LOADING FerricStore is initializing"}), state}
  end

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
