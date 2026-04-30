defmodule Ferricstore.Commands.Generic do
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops

  @moduledoc """
  Handles Redis generic key commands: TYPE, UNLINK, RENAME, RENAMENX, COPY,
  RANDOMKEY, SCAN, EXPIRETIME, PEXPIRETIME, OBJECT, WAIT.

  These commands operate on keys regardless of value type. Each handler takes
  the uppercased command name, a list of string arguments, and an injected
  store map. Returns plain Elixir terms -- the connection layer handles RESP
  encoding.

  ## Supported commands

    * `TYPE key` -- returns the type of key ("string" for existing, "none" for missing)
    * `UNLINK key [key ...]` -- async DEL; returns count of deleted keys
    * `RENAME key newkey` -- rename key, error if source missing
    * `RENAMENX key newkey` -- rename only if newkey doesn't exist (1 = renamed, 0 = not)
    * `COPY source destination [REPLACE]` -- copy value+TTL (1 = success, 0 = failure)
    * `RANDOMKEY` -- return a random key, or nil if DB is empty
    * `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` -- cursor-based key iteration
    * `EXPIRETIME key` -- absolute Unix timestamp (seconds) when key expires (-1 / -2)
    * `PEXPIRETIME key` -- absolute Unix timestamp (milliseconds) when key expires (-1 / -2)
    * `OBJECT ENCODING key` -- returns actual encoding based on key type
    * `OBJECT HELP` -- returns list of OBJECT subcommands
    * `OBJECT FREQ key` -- returns decayed LFU access frequency counter
    * `OBJECT IDLETIME key` -- returns idle seconds derived from LFU ldt
    * `OBJECT REFCOUNT key` -- always returns 1
    * `WAIT numreplicas timeout` -- returns 0 immediately (no replication)
  """

  alias Ferricstore.CrossShardOp

  @doc """
  Handles a generic key command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"TYPE"`, `"RENAME"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `get_meta`, `put`, `delete`,
      `exists?`, `keys` callbacks

  ## Returns

  Plain Elixir term: string, integer, list, nil, `{:simple, string}`, or
  `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # TYPE
  # ---------------------------------------------------------------------------

  def handle("TYPE", [key], store) do
    {:simple, Ferricstore.Store.TypeRegistry.get_type(key, store)}
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  # ---------------------------------------------------------------------------
  # UNLINK (same semantics as DEL -- async reclaim deferred to merge)
  # ---------------------------------------------------------------------------

  def handle("UNLINK", [], _store) do
    {:error, "ERR wrong number of arguments for 'unlink' command"}
  end

  def handle("UNLINK", keys, store) do
    # UNLINK has the same semantics as DEL for data-structure cleanup;
    # async reclaim is deferred to merge.
    Ferricstore.Commands.Strings.handle("DEL", keys, store)
  end

  # ---------------------------------------------------------------------------
  # RENAME
  # ---------------------------------------------------------------------------

  def handle("RENAME", [key, newkey], store) do
    CrossShardOp.execute(
      [{key, :read_write}, {newkey, :write}],
      fn unified_store ->
        case Ops.get_meta(unified_store, key) do
          nil ->
            {:error, "ERR no such key"}

          {value, expire_at_ms} ->
            Ops.put(unified_store, newkey, value, expire_at_ms)

            if key != newkey do
              Ops.delete(unified_store, key)
            end

            :ok
        end
      end,
      intent: %{command: :rename, keys: %{source: key, dest: newkey}, value_hashes: %{}},
      store: store
    )
  end

  def handle("RENAME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'rename' command"}
  end

  # ---------------------------------------------------------------------------
  # RENAMENX
  # ---------------------------------------------------------------------------

  def handle("RENAMENX", [key, newkey], store) do
    CrossShardOp.execute(
      [{key, :read_write}, {newkey, :write}],
      fn unified_store ->
        case Ops.get_meta(unified_store, key) do
          nil ->
            {:error, "ERR no such key"}

          {_value, _expire_at_ms} when key == newkey ->
            # Same key -- always 0 since destination "exists"
            0

          {value, expire_at_ms} ->
            if Ops.exists?(unified_store, newkey) do
              0
            else
              Ops.put(unified_store, newkey, value, expire_at_ms)
              Ops.delete(unified_store, key)
              1
            end
        end
      end,
      intent: %{command: :renamenx, keys: %{source: key, dest: newkey}, value_hashes: %{}},
      store: store
    )
  end

  def handle("RENAMENX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'renamenx' command"}
  end

  # ---------------------------------------------------------------------------
  # COPY
  # ---------------------------------------------------------------------------

  def handle("COPY", [source, destination | opts], store) do
    case parse_copy_opts(opts) do
      {:ok, replace?} ->
        CrossShardOp.execute(
          [{source, :read}, {destination, :write}],
          fn unified_store ->
            do_copy(source, destination, replace?, unified_store)
          end,
          intent: %{command: :copy, keys: %{source: source, dest: destination}, value_hashes: %{}},
          store: store
        )

      {:error, _} = err ->
        err
    end
  end

  def handle("COPY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'copy' command"}
  end

  # ---------------------------------------------------------------------------
  # RANDOMKEY
  # ---------------------------------------------------------------------------

  def handle("RANDOMKEY", [], store) do
    case Ops.keys(store) do
      [] -> nil
      keys -> Enum.random(keys)
    end
  end

  def handle("RANDOMKEY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'randomkey' command"}
  end

  # ---------------------------------------------------------------------------
  # SCAN
  # ---------------------------------------------------------------------------

  def handle("SCAN", [cursor_str | opts], store) do
    with {:ok, match_pattern, count, type_filter} <- parse_scan_opts(opts) do
      do_scan(cursor_str, match_pattern, count, type_filter, store)
    end
  end

  def handle("SCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'scan' command"}
  end

  # ---------------------------------------------------------------------------
  # EXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("EXPIRETIME", [key], store) do
    case key_meta(store, key) do
      nil -> -2
      0 -> -1
      expire_at_ms -> div(expire_at_ms, 1_000)
    end
  end

  def handle("EXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'expiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # PEXPIRETIME
  # ---------------------------------------------------------------------------

  def handle("PEXPIRETIME", [key], store) do
    case key_meta(store, key) do
      nil -> -2
      0 -> -1
      expire_at_ms -> expire_at_ms
    end
  end

  def handle("PEXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pexpiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # OBJECT -- subcommand is case-insensitive (uppercased before dispatch)
  # ---------------------------------------------------------------------------

  def handle("OBJECT", [], _store) do
    {:error, "ERR wrong number of arguments for 'object' command"}
  end

  def handle("OBJECT", [subcmd | rest], store) do
    do_object(String.upcase(subcmd), rest, store)
  end

  # ---------------------------------------------------------------------------
  # WAIT
  # ---------------------------------------------------------------------------

  def handle("WAIT", [_numreplicas, _timeout], _store) do
    # No replication support yet -- return 0 immediately.
    0
  end

  def handle("WAIT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'wait' command"}
  end

  defp key_meta(store, key) do
    case Ops.get_meta(store, key) do
      nil -> compound_expire_at_ms(store, key)
      {_value, expire_at_ms} -> expire_at_ms
    end
  end

  defp compound_expire_at_ms(store, key) do
    if Ops.has_compound?(store) do
      case Ops.compound_get_meta(store, key, CompoundKey.type_key(key)) do
        nil ->
          case Ops.compound_get_meta(store, key, CompoundKey.list_meta_key(key)) do
            nil -> nil
            {_meta, expire_at_ms} -> expire_at_ms
          end

        {_type, expire_at_ms} ->
          expire_at_ms
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- OBJECT subcommands
  # ---------------------------------------------------------------------------

  defp do_object("ENCODING", [key], store) do
    if Ops.exists?(store, key) do
      case Ferricstore.Store.TypeRegistry.get_type(key, store) do
        "hash" ->
          "hashtable"

        "list" ->
          "quicklist"

        "set" ->
          "hashtable"

        "zset" ->
          "skiplist"

        "stream" ->
          "stream"

        "string" ->
          value = Ops.get(store, key)
          if value != nil and byte_size(value) <= 44, do: "embstr", else: "raw"

        _other ->
          "raw"
      end
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("HELP", [], _store) do
    [
      "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "ENCODING <key>",
      "  Return the kind of internal representation the Redis object stored at <key> is using.",
      "FREQ <key>",
      "  Return the logarithmic access frequency counter of a Redis object stored at <key>.",
      "HELP",
      "  Return subcommand help summary.",
      "IDLETIME <key>",
      "  Return the idle time of a Redis object stored at <key>.",
      "REFCOUNT <key>",
      "  Return the reference count of the object stored at <key>."
    ]
  end

  defp do_object("FREQ", [key], store) do
    if Ops.exists?(store, key) do
      ctx = FerricStore.Instance.get(:default)
      idx = Ferricstore.Store.Router.shard_for(ctx, key)
      keydir = Ferricstore.Store.Router.resolve_keydir(ctx, idx)

      case :ets.lookup(keydir, key) do
        [{^key, _val, _exp, packed_lfu, _fid, _off, _vsize}] ->
          Ferricstore.Store.LFU.effective_counter(packed_lfu)

        _ ->
          0
      end
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("IDLETIME", [key], store) do
    if Ops.exists?(store, key) do
      ctx = FerricStore.Instance.get(:default)
      idx = Ferricstore.Store.Router.shard_for(ctx, key)
      keydir = Ferricstore.Store.Router.resolve_keydir(ctx, idx)

      case :ets.lookup(keydir, key) do
        [{^key, _val, _exp, packed_lfu, _fid, _off, _vsize}] ->
          {ldt, _counter} = Ferricstore.Store.LFU.unpack(packed_lfu)
          now_min = Ferricstore.Store.LFU.now_minutes()
          elapsed = Ferricstore.Store.LFU.elapsed_minutes(now_min, ldt)
          elapsed * 60

        _ ->
          0
      end
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object("REFCOUNT", [key], store) do
    if Ops.exists?(store, key) do
      1
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object(subcmd, _rest, _store) do
    {:error,
     "ERR unknown subcommand or wrong number of arguments for '#{String.downcase(subcmd)}' command"}
  end

  # ---------------------------------------------------------------------------
  # Private -- COPY helpers
  # ---------------------------------------------------------------------------

  defp parse_copy_opts([]), do: {:ok, false}

  defp parse_copy_opts([opt]) do
    if String.upcase(opt) == "REPLACE" do
      {:ok, true}
    else
      {:error, "ERR syntax error"}
    end
  end

  defp parse_copy_opts(_) do
    {:error, "ERR syntax error"}
  end

  defp do_copy(source, destination, replace?, store) do
    case Ops.get_meta(store, source) do
      nil ->
        {:error, "ERR no such key"}

      {value, expire_at_ms} ->
        if not replace? and Ops.exists?(store, destination) do
          {:error, "ERR target key already exists"}
        else
          Ops.put(store, destination, value, expire_at_ms)
          1
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- SCAN option parsing and execution
  # ---------------------------------------------------------------------------

  defp parse_scan_opts(opts), do: parse_scan_opts(opts, nil, 10, nil)

  defp parse_scan_opts([], match, count, type), do: {:ok, match, count, type}

  defp parse_scan_opts([opt, value | rest], match, count, type) do
    case String.upcase(opt) do
      "MATCH" ->
        parse_scan_opts(rest, value, count, type)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 ->
            parse_scan_opts(rest, match, n, type)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end

      "TYPE" ->
        parse_scan_opts(rest, match, count, String.downcase(value))

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp parse_scan_opts([_ | _], _match, _count, _type) do
    {:error, "ERR syntax error"}
  end

  defp do_scan(cursor_str, match_pattern, count, type_filter, store) do
    alias Ferricstore.Store.CompoundKey

    all_keys =
      Ops.keys(store)
      |> CompoundKey.user_visible_keys()
      |> filter_by_type(type_filter, store)
      |> filter_by_match(match_pattern)
      |> Enum.sort()

    # Cursor "0" means start from the beginning. Otherwise, cursor is the last
    # key seen -- find the first key strictly after it alphabetically.
    remaining =
      if cursor_str == "0" do
        all_keys
      else
        Enum.drop_while(all_keys, fn k -> k <= cursor_str end)
      end

    {batch, rest} = Enum.split(remaining, count)

    next_cursor =
      case {batch, rest} do
        {[], _} -> "0"
        {_, []} -> "0"
        _ -> List.last(batch)
      end

    [next_cursor, batch]
  end

  defp filter_by_type(keys, nil, _store), do: keys

  defp filter_by_type(keys, type_filter, store) do
    alias Ferricstore.Store.TypeRegistry

    Enum.filter(keys, fn key ->
      TypeRegistry.get_type(key, store) == type_filter
    end)
  end

  defp filter_by_match(keys, nil), do: keys

  defp filter_by_match(keys, pattern) do
    Enum.filter(keys, &Ferricstore.GlobMatcher.match?(&1, pattern))
  end
end
