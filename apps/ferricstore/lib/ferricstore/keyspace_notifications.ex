defmodule Ferricstore.KeyspaceNotifications do
  @moduledoc """
  Emits keyspace and keyevent notifications via PubSub.

  When the `notify-keyspace-events` configuration value is set (non-empty),
  key mutation operations fire pub/sub messages on two channel families:

    * `__keyspace@0__:<key>` -- carries the event name as the message
    * `__keyevent@0__:<event>` -- carries the key name as the message

  ## Configuration flags

  The `notify-keyspace-events` config value is a string of flag characters:

    * `K` -- enable `__keyspace@0__:<key>` channel
    * `E` -- enable `__keyevent@0__:<event>` channel
    * `g` -- generic commands: DEL, EXPIRE, RENAME, PERSIST, COPY
    * `$` -- string commands: SET, INCR, APPEND, GETSET, GETEX, SETRANGE, SETBIT, BITOP, PF*
    * `h` -- hash commands: HSET, HDEL, HINCRBY
    * `l` -- list commands: LPUSH, LPOP, LREM, LMOVE
    * `s` -- set commands: SADD, SREM, SPOP, SMOVE
    * `z` -- sorted set commands: ZADD, ZREM, ZINCRBY, ZPOP*
    * `t` -- stream commands: XADD, XDEL, XTRIM
    * `x` -- expired events
    * `A` -- alias for all event types

  At least one of `K` or `E` must be present along with at least one event
  type flag for notifications to fire.

  ## Examples

      # Enable keyspace + keyevent for all event types:
      Ferricstore.Config.set("notify-keyspace-events", "KEA")

      # Enable only keyevent for string commands:
      Ferricstore.Config.set("notify-keyspace-events", "E$")
  """

  @event_flags %{
    "del" => "g",
    "expire" => "g",
    "pexpire" => "g",
    "expireat" => "g",
    "pexpireat" => "g",
    "rename" => "g",
    "persist" => "g",
    "copy" => "g",
    "set" => "$",
    "mset" => "$",
    "append" => "$",
    "getset" => "$",
    "getdel" => "$",
    "getex" => "$",
    "setrange" => "$",
    "incr" => "$",
    "incrby" => "$",
    "incrbyfloat" => "$",
    "decr" => "$",
    "decrby" => "$",
    "setbit" => "$",
    "bitop" => "$",
    "pfadd" => "$",
    "pfmerge" => "$",
    "hset" => "h",
    "hdel" => "h",
    "hincrby" => "h",
    "hincrbyfloat" => "h",
    "lpush" => "l",
    "rpush" => "l",
    "lpop" => "l",
    "rpop" => "l",
    "lset" => "l",
    "linsert" => "l",
    "ltrim" => "l",
    "lrem" => "l",
    "lmove" => "l",
    "sadd" => "s",
    "srem" => "s",
    "spop" => "s",
    "smove" => "s",
    "sdiffstore" => "s",
    "sinterstore" => "s",
    "sunionstore" => "s",
    "zadd" => "z",
    "zrem" => "z",
    "zincrby" => "z",
    "zpopmin" => "z",
    "zpopmax" => "z",
    "geoadd" => "z",
    "geosearchstore" => "z",
    "xadd" => "t",
    "xdel" => "t",
    "xtrim" => "t",
    "expired" => "x"
  }

  # The event a successful command fires (Redis's names).
  @command_events %{
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
    "COPY" => "copy",
    "LPUSH" => "lpush",
    "RPUSH" => "rpush",
    "LPUSHX" => "lpush",
    "RPUSHX" => "rpush",
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
    "ZADD" => "zadd",
    "ZREM" => "zrem",
    "ZINCRBY" => "zincrby",
    "ZPOPMIN" => "zpopmin",
    "ZPOPMAX" => "zpopmax",
    "PFADD" => "pfadd",
    "PFMERGE" => "pfmerge",
    "GEOADD" => "geoadd",
    "GEOSEARCHSTORE" => "geosearchstore",
    "XADD" => "xadd",
    "XDEL" => "xdel",
    "XTRIM" => "xtrim"
  }

  # Results that mean the command changed nothing. Kept narrow: some commands
  # answer 0 while still changing data (SETBIT returns the old bit, HSET and
  # ZADD count only new members).
  @zero_result_noop ~w(SETNX MSETNX DEL UNLINK EXPIRE PEXPIRE EXPIREAT PEXPIREAT
    PERSIST RENAMENX COPY HSETNX HDEL LPUSHX RPUSHX SADD SREM SMOVE ZREM LREM PFADD
    XDEL XTRIM)
  @nil_result_noop ~w(GETDEL LPOP RPOP SPOP SET)
  @empty_list_result_noop ~w(LPOP RPOP SPOP ZPOPMIN ZPOPMAX)

  @doc "Whether `notify-keyspace-events` turns any notifications on."
  @spec enabled?() :: boolean()
  def enabled?, do: :persistent_term.get(:ferricstore_keyspace_events, "") != ""

  @doc """
  Fires the notifications for a command a client ran: `command` is its upper
  case name, `args` its arguments, `result` its reply. Does nothing when
  notifications are off, or when the command failed or changed nothing.
  Never raises: a notification failure must not fail the command.
  """
  @spec after_command(binary(), [term()], term()) :: :ok
  def after_command(command, args, result) do
    if enabled?() do
      notify_command(command, args, result)
    end

    :ok
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp notify_command(_command, _args, {:error, _reason}), do: :ok
  defp notify_command(command, _args, 0) when command in @zero_result_noop, do: :ok
  defp notify_command(command, _args, {:ok, 0}) when command in @zero_result_noop, do: :ok
  defp notify_command(command, _args, []) when command in @empty_list_result_noop, do: :ok
  defp notify_command("GETEX", [_key], _result), do: :ok
  defp notify_command("GETEX", _args, nil), do: :ok

  # SET answers nil when NX or XX kept it from writing, unless GET asked for
  # the old value, which is nil for a new key that SET did write.
  defp notify_command("SET", args, nil) do
    if Enum.any?(args, &(is_binary(&1) and String.upcase(&1) == "GET")),
      do: notify_keys(args, "set"),
      else: :ok
  end

  defp notify_command(command, _args, nil) when command in @nil_result_noop, do: :ok

  defp notify_command(command, args, result) do
    case Map.fetch(@command_events, command) do
      {:ok, event} -> notify_event(command, event, args, result)
      :error -> :ok
    end
  end

  defp notify_event(command, event, keys, _result) when command in ~w(DEL UNLINK),
    do: Enum.each(keys, &notify_key(&1, event))

  defp notify_event(command, event, args, _result) when command in ~w(MSET MSETNX) do
    args
    |> Enum.chunk_every(2)
    |> Enum.each(fn
      [key, _value] -> notify_key(key, event)
      _odd -> :ok
    end)
  end

  defp notify_event("COPY", event, [_source, destination | _rest], _result),
    do: notify_key(destination, event)

  defp notify_event(command, event, [source, destination | _rest], _result)
       when command in ~w(RENAME RENAMENX LMOVE SMOVE) do
    notify_key(source, event)
    notify_key(destination, event)
  end

  defp notify_event(command, event, [destination | _rest], _result)
       when command in ~w(SDIFFSTORE SINTERSTORE SUNIONSTORE GEOSEARCHSTORE PFMERGE),
       do: notify_key(destination, event)

  defp notify_event("BITOP", event, [_operation, destination | _rest], _result),
    do: notify_key(destination, event)

  defp notify_event(_command, event, [key | _rest], _result), do: notify_key(key, event)
  defp notify_event(_command, _event, _args, _result), do: :ok

  defp notify_keys([key | _rest], event), do: notify_key(key, event)
  defp notify_keys(_args, _event), do: :ok

  defp notify_key(key, event) when is_binary(key), do: notify(key, event)
  defp notify_key(_key, _event), do: :ok

  @doc """
  Fires keyspace and/or keyevent notifications for a key mutation.

  Reads the `notify-keyspace-events` config to determine which channels
  to publish to. Does nothing if notifications are disabled (empty config)
  or if the event type is not covered by the configured flags.

  ## Parameters

    * `key` - the key that was mutated
    * `event` - the event name (e.g. `"set"`, `"del"`, `"expire"`)
    * `flags` - (optional) override config flags; if `nil`, reads from
      `Ferricstore.Config`

  ## Returns

  `:ok`
  """
  @spec notify(binary(), binary(), binary() | nil) :: :ok
  def notify(key, event, flags \\ nil) do
    # Read from persistent_term (~5ns) instead of ETS Config.get_value (~100-300ns).
    # The persistent_term is updated by Config.apply_side_effect when
    # CONFIG SET notify-keyspace-events is called.
    config_flags = flags || :persistent_term.get(:ferricstore_keyspace_events, "")

    if config_flags != "" and should_notify?(event, config_flags) do
      if String.contains?(config_flags, "K") do
        Ferricstore.PubSub.publish("__keyspace@0__:#{key}", event)
      end

      if String.contains?(config_flags, "E") do
        Ferricstore.PubSub.publish("__keyevent@0__:#{event}", key)
      end
    end

    :ok
  end

  @doc """
  Determines whether a notification should fire for the given event
  based on the configured flags.

  ## Parameters

    * `event` - the event name
    * `flags` - the flag string from config

  ## Returns

  `true` if the event matches the configured flags, `false` otherwise.
  """
  @spec should_notify?(binary(), binary()) :: boolean()
  def should_notify?(event, flags) do
    has_channel? = String.contains?(flags, "K") or String.contains?(flags, "E")

    has_channel? and
      (String.contains?(flags, "A") or
         case Map.get(@event_flags, event) do
           nil -> false
           flag -> String.contains?(flags, flag)
         end)
  end
end
