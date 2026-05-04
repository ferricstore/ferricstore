defmodule FerricstoreServer.Connection.Auth do
  @moduledoc """
  Auth and ACL command handling extracted from Connection.

  All functions accept and return the connection state as a map/struct.
  """

  alias Ferricstore.AuditLog
  alias FerricstoreServer.Resp.Encoder

  # O(1) MapSet lookups for hot-path classification.
  @read_cmds_set MapSet.new(~w(GET MGET GETRANGE STRLEN GETEX GETDEL GETSET
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER
    ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE
    TYPE EXISTS TTL PTTL EXPIRETIME PEXPIRETIME
    GETBIT BITCOUNT BITPOS PFCOUNT
    OBJECT SUBSTR
    GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XINFO
    JSON.GET JSON.TYPE JSON.STRLEN JSON.OBJKEYS JSON.OBJLEN JSON.ARRLEN JSON.MGET))

  @write_cmds_set MapSet.new(~w(SET SETNX SETEX PSETEX MSET MSETNX APPEND SETRANGE
    INCR DECR INCRBY DECRBY INCRBYFLOAT
    DEL UNLINK
    EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
    RENAME RENAMENX COPY
    HSET HDEL HINCRBY HINCRBYFLOAT HSETNX
    LPUSH RPUSH LPOP RPOP LSET LINSERT LTRIM LREM LMOVE LPUSHX RPUSHX
    SADD SREM SPOP SMOVE SDIFFSTORE SINTERSTORE SUNIONSTORE
    ZADD ZREM ZINCRBY ZPOPMIN ZPOPMAX
    SETBIT BITOP PFADD PFMERGE
    GEOADD GEOSEARCHSTORE
    XADD XTRIM XDEL
    JSON.SET JSON.DEL JSON.NUMINCRBY JSON.TOGGLE JSON.CLEAR JSON.ARRAPPEND
    GETSET GETDEL
    CAS LOCK UNLOCK EXTEND))

  @read_write_cmds_set MapSet.new(~w(GETSET GETDEL GETEX CAS))

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

  def dispatch_acl("SETUSER", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'acl|setuser' command"}), state}
  end

  def dispatch_acl("SETUSER", [username | rules], state) do
    # Route through Raft so the mutation is replicated to all nodes.
    ctx = FerricStore.Instance.get(:default)
    result = Ferricstore.Store.Router.server_command(ctx, {:acl_setuser, username, rules})

    case result do
      :ok ->
        broadcast_acl_invalidation(username)

        new_state =
          if username == state.username do
            %{state | acl_cache: build_acl_cache(username)}
          else
            state
          end

        {:continue, Encoder.encode(:ok), new_state}

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  def dispatch_acl("DELUSER", [], state) do
    {:continue,
     Encoder.encode({:error, "ERR wrong number of arguments for 'acl|deluser' command"}), state}
  end

  def dispatch_acl("DELUSER", usernames, state) do
    ctx = FerricStore.Instance.get(:default)

    results =
      Enum.map(usernames, fn username ->
        Ferricstore.Store.Router.server_command(ctx, {:acl_deluser, username})
      end)

    case Enum.find(results, fn r -> match?({:error, _}, r) end) do
      nil ->
        Enum.each(usernames, &broadcast_acl_invalidation/1)
        {:continue, Encoder.encode(:ok), state}

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  def dispatch_acl("CAT", _, state) do
    cats =
      ~w(keyspace read write set sortedset list hash string bitmap hyperloglog geo stream pubsub admin fast slow blocking dangerous connection transaction server generic)

    {:continue, Encoder.encode(cats), state}
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

  @spec build_acl_cache(binary()) :: map() | :full_access | :denied | nil
  def build_acl_cache(username) do
    case FerricstoreServer.Acl.get_user(username) do
      nil ->
        if username == "default", do: :full_access, else: :denied

      user ->
        denied = Map.get(user, :denied_commands, MapSet.new())

        if user.enabled and user.commands == :all and
             MapSet.size(denied) == 0 and user.keys == :all do
          :full_access
        else
          %{
            commands: user.commands,
            denied_commands: denied,
            keys: user.keys,
            enabled: user.enabled
          }
        end
    end
  end

  # ── Command permission checks (called from pipeline dispatch path) ───

  @spec check_command_cached(map() | :full_access | :denied | nil, binary()) ::
          :ok | {:error, binary()}

  # Deleted user or unknown user — deny all commands.
  def check_command_cached(:denied, _cmd),
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

      cache.commands == :all and not MapSet.member?(cache.denied_commands, cmd) ->
        :ok

      cache.commands == :all ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}

      MapSet.member?(cache.commands, cmd) and
          not MapSet.member?(cache.denied_commands, cmd) ->
        :ok

      true ->
        {:error,
         "NOPERM this user has no permissions to run the '#{String.downcase(cmd)}' command"}
    end
  end

  # ── Key pattern checks ─────────────────────────────────────────────────

  @spec check_keys_cached(map() | :full_access | nil, binary(), [binary()]) ::
          :ok | {:error, binary()}
  def check_keys_cached(nil, _cmd, _keys), do: :ok
  def check_keys_cached(:full_access, _cmd, _keys), do: :ok
  def check_keys_cached(%{keys: :all}, _cmd, _keys), do: :ok

  def check_keys_cached(%{keys: patterns}, cmd, keys) when is_list(keys) do
    case keys do
      [] ->
        :ok

      keys ->
        cmd
        |> command_access_type()
        |> then(&check_all_keys(keys, &1, patterns))
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
  def command_access_type(cmd) do
    cond do
      MapSet.member?(@read_write_cmds_set, cmd) -> :rw
      MapSet.member?(@read_cmds_set, cmd) -> :read
      MapSet.member?(@write_cmds_set, cmd) -> :write
      # Default to :rw for unknown commands — most conservative.
      true -> :rw
    end
  end

  # ── ACL invalidation broadcasting ──────────────────────────────────────

  @acl_pg_group :ferricstore_acl_connections

  @spec broadcast_acl_invalidation(binary()) :: :ok
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

  @spec maybe_refresh_acl_cache(map(), binary()) :: map()
  def maybe_refresh_acl_cache(state, invalidated_username) do
    if invalidated_username == state.username do
      %{state | acl_cache: build_acl_cache(state.username)}
    else
      state
    end
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
         %{state | authenticated: true, username: username, acl_cache: new_cache}}

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
       %{state | authenticated: true, username: "default", acl_cache: new_cache}}
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

  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"
end
