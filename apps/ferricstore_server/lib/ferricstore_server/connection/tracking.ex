defmodule FerricstoreServer.Connection.Tracking do
  @moduledoc "Client-side key tracking registration, invalidation dispatch, and keyspace notification firing."

  alias Ferricstore.KeyspaceNotifications
  alias FerricstoreServer.ClientTracking

  # Commands that read keys and should trigger client tracking registration.
  @read_cmds ~w(GET MGET GETRANGE STRLEN GETEX GETDEL GETSET
    HGET HMGET HGETALL HKEYS HVALS HLEN HEXISTS HRANDFIELD HSCAN HSTRLEN
    LRANGE LLEN LINDEX LPOS
    SMEMBERS SISMEMBER SMISMEMBER SCARD SRANDMEMBER
    ZSCORE ZRANK ZREVRANK ZRANGE ZCARD ZCOUNT ZRANDMEMBER ZMSCORE
    TYPE EXISTS TTL PTTL EXPIRETIME PEXPIRETIME
    GETBIT BITCOUNT BITPOS PFCOUNT
    OBJECT SUBSTR
    GEOHASH GEOPOS GEODIST GEOSEARCH
    XLEN XRANGE XREVRANGE XREAD XREADGROUP XINFO)

  # Commands that write keys and should trigger client tracking invalidation.
  @write_cmds ~w(SET SETNX SETEX PSETEX MSET MSETNX APPEND SETRANGE
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
    GETSET GETDEL GETEX
    CAS LOCK UNLOCK EXTEND)

  # O(1) MapSet lookups for hot-path classification.
  @write_cmds_set MapSet.new(@write_cmds)

  # Commands where integer 0 means "no key was mutated". Keep this narrow:
  # some Redis commands return 0 while still changing data (for example SETBIT
  # returns the old bit), so only commands with count/conditional semantics go
  # here.
  @zero_result_noop_cmds ~w(SETNX MSETNX
    DEL UNLINK
    EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST
    RENAMENX COPY
    HSETNX HDEL
    LPUSHX RPUSHX
    SADD SREM SMOVE
    ZREM
    LREM
    PFADD
    XDEL XTRIM)

  @nil_result_noop_cmds ~w(GETDEL LPOP RPOP SPOP)
  @empty_list_result_noop_cmds ~w(LPOP RPOP SPOP ZPOPMIN ZPOPMAX)

  # Maps command names to their keyspace notification event names.
  # Only fires on successful results (not errors).
  @keyspace_events %{
    "SET" => "set",
    "SETNX" => "set",
    "SETEX" => "set",
    "PSETEX" => "set",
    "MSET" => "mset",
    "MSETNX" => "mset",
    "APPEND" => "append",
    "GETSET" => "getset",
    "GETDEL" => "getdel",
    "GETEX" => "getex",
    "SETRANGE" => "setrange",
    "INCR" => "incr",
    "DECR" => "decr",
    "INCRBY" => "incrby",
    "DECRBY" => "decrby",
    "INCRBYFLOAT" => "incrbyfloat",
    "DEL" => "del",
    "UNLINK" => "del",
    "SETBIT" => "setbit",
    "BITOP" => "bitop",
    "EXPIRE" => "expire",
    "PEXPIRE" => "pexpire",
    "EXPIREAT" => "expireat",
    "PEXPIREAT" => "pexpireat",
    "PERSIST" => "persist",
    "RENAME" => "rename",
    "RENAMENX" => "rename",
    "LPUSH" => "lpush",
    "RPUSH" => "rpush",
    "LPOP" => "lpop",
    "RPOP" => "rpop",
    "LSET" => "lset",
    "LINSERT" => "linsert",
    "LTRIM" => "ltrim",
    "LREM" => "lrem",
    "LMOVE" => "lmove",
    "SADD" => "sadd",
    "SREM" => "srem",
    "SPOP" => "spop",
    "SMOVE" => "smove",
    "SDIFFSTORE" => "sdiffstore",
    "SINTERSTORE" => "sinterstore",
    "SUNIONSTORE" => "sunionstore",
    "HSET" => "hset",
    "HSETNX" => "hset",
    "HDEL" => "hdel",
    "HINCRBY" => "hincrby",
    "HINCRBYFLOAT" => "hincrbyfloat",
    "LPUSHX" => "lpush",
    "RPUSHX" => "rpush",
    "ZADD" => "zadd",
    "ZREM" => "zrem",
    "ZINCRBY" => "zincrby",
    "ZPOPMIN" => "zpopmin",
    "ZPOPMAX" => "zpopmax",
    "PFADD" => "pfadd",
    "PFMERGE" => "pfmerge",
    "GEOADD" => "geoadd",
    "GEOSEARCHSTORE" => "geosearchstore",
    "COPY" => "copy",
    "XADD" => "xadd",
    "XDEL" => "xdel",
    "XTRIM" => "xtrim"
  }

  @doc false
  @spec maybe_notify_keyspace(binary(), [binary()], term()) :: :ok
  def maybe_notify_keyspace(cmd, _args, 0) when cmd in @zero_result_noop_cmds, do: :ok
  def maybe_notify_keyspace(cmd, _args, nil) when cmd in @nil_result_noop_cmds, do: :ok
  def maybe_notify_keyspace(cmd, _args, []) when cmd in @empty_list_result_noop_cmds, do: :ok
  def maybe_notify_keyspace("GETEX", [_key], _result), do: :ok
  def maybe_notify_keyspace("GETEX", _args, nil), do: :ok

  def maybe_notify_keyspace(cmd, args, result) do
    case Map.get(@keyspace_events, cmd) do
      nil -> :ok
      event -> do_notify_keyspace(cmd, event, args, result)
    end
  end

  # For DEL/UNLINK with multiple keys, notify per key
  @doc false
  def do_notify_keyspace(cmd, event, keys, count)
      when cmd in ~w(DEL UNLINK) and is_integer(count) and count > 0 do
    Enum.each(keys, fn key -> KeyspaceNotifications.notify(key, event) end)
  end

  # For MSET, notify per key
  def do_notify_keyspace("MSET", event, args, :ok) do
    args
    |> Enum.chunk_every(2)
    |> Enum.each(fn [key, _val] -> KeyspaceNotifications.notify(key, event) end)
  end

  def do_notify_keyspace("MSETNX", event, args, 1) do
    args
    |> Enum.chunk_every(2)
    |> Enum.each(fn [key, _val] -> KeyspaceNotifications.notify(key, event) end)
  end

  def do_notify_keyspace("MSETNX", _event, _args, _result), do: :ok

  def do_notify_keyspace("COPY", event, [_source, destination | _], 1) do
    KeyspaceNotifications.notify(destination, event)
  end

  def do_notify_keyspace("COPY", _event, _args, _result), do: :ok

  def do_notify_keyspace("RENAME", event, [source, destination], :ok) do
    KeyspaceNotifications.notify(source, event)
    KeyspaceNotifications.notify(destination, event)
  end

  def do_notify_keyspace("RENAMENX", event, [source, destination], 1) do
    KeyspaceNotifications.notify(source, event)
    KeyspaceNotifications.notify(destination, event)
  end

  def do_notify_keyspace("RENAMENX", _event, _args, _result), do: :ok

  def do_notify_keyspace("LMOVE", event, [source, destination | _], :ok) do
    KeyspaceNotifications.notify(source, event)
    KeyspaceNotifications.notify(destination, event)
  end

  def do_notify_keyspace("SMOVE", event, [source, destination | _], result)
      when result not in [0, nil] do
    KeyspaceNotifications.notify(source, event)
    KeyspaceNotifications.notify(destination, event)
  end

  def do_notify_keyspace("SMOVE", _event, _args, _result), do: :ok

  def do_notify_keyspace("BITOP", event, [_operation, destination | _], _result) do
    KeyspaceNotifications.notify(destination, event)
  end

  # Single-key commands: first arg is the key. Skip errors.
  def do_notify_keyspace(_cmd, _event, _args, {:error, _}), do: :ok
  def do_notify_keyspace(_cmd, _event, [], _result), do: :ok

  def do_notify_keyspace(_cmd, event, [key | _], _result) do
    KeyspaceNotifications.notify(key, event)
  end

  @doc false
  @spec tracking_socket_sender() :: (pid(), iodata(), [binary()] -> :ok)
  def tracking_socket_sender do
    fn target_pid, iodata, keys ->
      send(target_pid, {:tracking_invalidation, iodata, keys})
      :ok
    end
  end

  # After a successful read command, register the read key(s) for tracking.
  # Only called when tracking is enabled on the connection.
  # Returns the (potentially updated) connection state.
  @doc false
  @spec maybe_track_read(binary(), [binary()], term(), map()) :: map()
  def maybe_track_read(_cmd, _args, _result, %{tracking: %{enabled: false}} = state), do: state
  def maybe_track_read(_cmd, _args, _result, %{tracking: nil} = state), do: state
  def maybe_track_read(_cmd, _args, {:error, _}, state), do: state
  def maybe_track_read(cmd, _args, _result, state) when cmd in ~w(GETSET GETDEL), do: state

  def maybe_track_read("GETEX", [_key, _option | _opts], result, state) when result != nil,
    do: state

  def maybe_track_read(cmd, args, _result, state) when cmd in @read_cmds do
    conn_pid = self()

    case cmd do
      "MGET" ->
        new_tracking = ClientTracking.track_keys(conn_pid, args, state.tracking)
        %{state | tracking: new_tracking}

      c when c in ~w(EXISTS PFCOUNT) ->
        new_tracking = ClientTracking.track_keys(conn_pid, args, state.tracking)
        %{state | tracking: new_tracking}

      "HMGET" ->
        # HMGET key field [field ...] -- track the top-level key
        case args do
          [key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end

      c when c in ~w(XREAD XREADGROUP) ->
        new_tracking =
          ClientTracking.track_keys(conn_pid, xread_stream_keys(args), state.tracking)

        %{state | tracking: new_tracking}

      "XINFO" ->
        case args do
          [_subcommand, key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end

      "OBJECT" ->
        case args do
          [_subcommand, key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end

      _ ->
        # Single-key commands: first arg is the key
        case args do
          [key | _] ->
            new_tracking = ClientTracking.track_key(conn_pid, key, state.tracking)
            %{state | tracking: new_tracking}

          _ ->
            state
        end
    end
  end

  def maybe_track_read(_cmd, _args, _result, state), do: state

  defp xread_stream_keys(args) do
    with stream_idx when is_integer(stream_idx) <-
           Enum.find_index(args, &(String.upcase(&1) == "STREAMS")),
         stream_args when stream_args != [] <- Enum.drop(args, stream_idx + 1) do
      Enum.take(stream_args, div(length(stream_args), 2))
    else
      _ -> []
    end
  end

  # After a successful write command, notify all tracking connections.
  # This can be called from any process (connection process or Task).
  @doc false
  @spec maybe_notify_tracking(binary(), [binary()], term(), map()) :: :ok
  def maybe_notify_tracking(_cmd, _args, {:error, _}, _state), do: :ok
  def maybe_notify_tracking(cmd, _args, 0, _state) when cmd in @zero_result_noop_cmds, do: :ok
  def maybe_notify_tracking(cmd, _args, nil, _state) when cmd in @nil_result_noop_cmds, do: :ok

  def maybe_notify_tracking(cmd, _args, [], _state) when cmd in @empty_list_result_noop_cmds,
    do: :ok

  def maybe_notify_tracking("GETEX", [_key], _result, _state), do: :ok
  def maybe_notify_tracking("GETEX", _args, nil, _state), do: :ok

  def maybe_notify_tracking(cmd, args, _result, _state) do
    if MapSet.member?(@write_cmds_set, cmd) do
      do_notify_tracking(cmd, args)
    else
      :ok
    end
  end

  defp do_notify_tracking(cmd, args) do
    writer_pid = self()
    sender = tracking_socket_sender()

    case cmd do
      c when c in ~w(MSET MSETNX) ->
        keys =
          args
          |> Enum.chunk_every(2)
          |> Enum.map(fn [key | _] -> key end)

        ClientTracking.notify_keys_modified(keys, writer_pid, sender)

      c when c in ~w(DEL UNLINK) ->
        ClientTracking.notify_keys_modified(args, writer_pid, sender)

      c when c in ~w(RENAME RENAMENX) ->
        notify_tracking_keys(args, :all, writer_pid, sender)

      "LMOVE" ->
        notify_tracking_keys(args, :all, writer_pid, sender)

      "SMOVE" ->
        notify_tracking_keys(args, :all, writer_pid, sender)

      "COPY" ->
        notify_tracking_keys(args, :destination, writer_pid, sender)

      "BITOP" ->
        notify_tracking_keys(args, :second, writer_pid, sender)

      _ ->
        notify_tracking_keys(args, :first, writer_pid, sender)
    end
  end

  defp notify_tracking_keys([src, dst | _], :all, writer_pid, sender) do
    ClientTracking.notify_keys_modified([src, dst], writer_pid, sender)
  end

  defp notify_tracking_keys([_src, dst | _], :destination, writer_pid, sender) do
    ClientTracking.notify_key_modified(dst, writer_pid, sender)
  end

  defp notify_tracking_keys([_first, key | _], :second, writer_pid, sender) do
    ClientTracking.notify_key_modified(key, writer_pid, sender)
  end

  defp notify_tracking_keys([key | _], :first, writer_pid, sender) do
    ClientTracking.notify_key_modified(key, writer_pid, sender)
  end

  defp notify_tracking_keys(_, _, _, _), do: :ok
end
