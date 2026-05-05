defmodule Ferricstore.KeyspaceNotifications do
  @moduledoc """
  Emits Redis-compatible keyspace and keyevent notifications via PubSub.

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
