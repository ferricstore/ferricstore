defmodule Ferricstore.Commands.Generic do
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

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
    {:simple, TypeRegistry.get_type(key, store)}
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
        case key_entry(unified_store, key) do
          nil ->
            {:error, "ERR no such key"}

          entry ->
            rename_entry(key, newkey, entry, unified_store)
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
        case key_entry(unified_store, key) do
          nil ->
            {:error, "ERR no such key"}

          _entry when key == newkey ->
            # Same key -- always 0 since destination "exists"
            0

          entry ->
            if key_exists?(unified_store, newkey) do
              0
            else
              rename_entry(key, newkey, entry, unified_store)
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
    case Ops.expire_at_ms(store, key) do
      nil -> compound_expire_at_ms(store, key)
      expire_at_ms -> expire_at_ms
    end
  end

  defp compound_expire_at_ms(store, key) do
    if Ops.has_compound?(store) do
      case Ops.compound_get_meta(store, key, CompoundKey.type_key(key)) do
        nil ->
          case Ops.compound_get_meta(store, key, CompoundKey.list_meta_key(key)) do
            nil -> nil
            {_meta, expire_at_ms} -> live_compound_expire_at_ms(store, key, "list", expire_at_ms)
          end

        {type, expire_at_ms} ->
          live_compound_expire_at_ms(store, key, type, expire_at_ms)
      end
    end
  end

  defp live_compound_expire_at_ms(store, key, expected_type, expire_at_ms) do
    if TypeRegistry.get_type(key, store) == expected_type do
      expire_at_ms
    end
  end

  # ---------------------------------------------------------------------------
  # Private -- OBJECT subcommands
  # ---------------------------------------------------------------------------

  defp do_object("ENCODING", [key], store) do
    case TypeRegistry.get_type(key, store) do
      "none" ->
        {:error, "ERR no such key"}

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
        string_encoding(store, key)

      _other ->
        "raw"
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
    if object_exists?(store, key) do
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
    if object_exists?(store, key) do
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
    if object_exists?(store, key) do
      1
    else
      {:error, "ERR no such key"}
    end
  end

  defp do_object(subcmd, _rest, _store) do
    {:error,
     "ERR unknown subcommand or wrong number of arguments for '#{String.downcase(subcmd)}' command"}
  end

  defp string_encoding(store, key) do
    case Ops.value_size(store, key) do
      size when is_integer(size) and size > 44 ->
        "raw"

      _small_or_unknown ->
        value = Ops.get(store, key)

        cond do
          int_encoded_string?(value) -> "int"
          value != nil and byte_size(value) <= 44 -> "embstr"
          true -> "raw"
        end
    end
  end

  defp object_exists?(store, key) do
    TypeRegistry.get_type(key, store) != "none"
  end

  defp int_encoded_string?(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int >= @min_int64 and int <= @max_int64 and Integer.to_string(int) == value

      _ ->
        false
    end
  end

  defp int_encoded_string?(_value), do: false

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
    case key_entry(store, source) do
      nil ->
        {:error, "ERR no such key"}

      entry ->
        if not replace? and key_exists?(store, destination) do
          0
        else
          if source != destination do
            delete_key(destination, store)
          end

          copy_entry(source, destination, entry, store)
          1
        end
    end
  end

  defp key_exists?(store, key), do: TypeRegistry.get_type(key, store) != "none"

  defp key_entry(store, key) do
    case Ops.get_meta(store, key) do
      nil -> compound_entry(store, key)
      {value, expire_at_ms} -> {:plain, value, expire_at_ms}
    end
  end

  defp compound_entry(store, key) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get_meta(store, key, type_key) do
        nil ->
          list_meta_key = CompoundKey.list_meta_key(key)

          case Ops.compound_get_meta(store, key, list_meta_key) do
            nil -> nil
            {_meta, expire_at_ms} -> live_compound_entry(store, key, "list", expire_at_ms)
          end

        {type, expire_at_ms} ->
          live_compound_entry(store, key, type, expire_at_ms)
      end
    end
  end

  defp live_compound_entry(store, key, expected_type, expire_at_ms) do
    if TypeRegistry.get_type(key, store) == expected_type do
      {:compound, expected_type, expire_at_ms}
    end
  end

  defp rename_entry(source, destination, _entry, _store) when source == destination, do: :ok

  defp rename_entry(source, destination, entry, store) do
    delete_key(destination, store)
    copy_entry(source, destination, entry, store)
    delete_key(source, store)
  end

  defp copy_entry(_source, destination, {:plain, value, expire_at_ms}, store) do
    Ops.put(store, destination, value, expire_at_ms)
  end

  defp copy_entry(source, destination, {:compound, type, _expire_at_ms}, store) do
    copy_compound_meta(source, destination, type, store)
    copy_compound_entries(source, destination, type, store)
  end

  defp delete_key(key, store) do
    Ferricstore.Commands.Strings.handle("DEL", [key], store)
  end

  defp copy_compound_meta(source, destination, type, store) do
    copy_compound_key(
      source,
      destination,
      CompoundKey.type_key(source),
      CompoundKey.type_key(destination),
      store,
      type
    )

    if type == "list" do
      copy_compound_key(
        source,
        destination,
        CompoundKey.list_meta_key(source),
        CompoundKey.list_meta_key(destination),
        store
      )
    end
  end

  defp copy_compound_entries(source, destination, type, store) do
    source_prefix = compound_prefix(type, source)
    destination_prefix = compound_prefix(type, destination)

    store
    |> Ops.compound_scan(source, source_prefix)
    |> Enum.each(fn {sub_key, _value} ->
      source_key = scanned_compound_key(source_prefix, sub_key)
      destination_key = scanned_compound_key(destination_prefix, sub_key)
      copy_compound_key(source, destination, source_key, destination_key, store)
    end)
  end

  defp copy_compound_key(
         source,
         destination,
         source_key,
         destination_key,
         store,
         fallback_value \\ nil
       ) do
    case Ops.compound_get_meta(store, source, source_key) do
      nil ->
        if fallback_value != nil do
          Ops.compound_put(store, destination, destination_key, fallback_value, 0)
        end

      {value, expire_at_ms} ->
        Ops.compound_put(store, destination, destination_key, value, expire_at_ms)
    end
  end

  defp scanned_compound_key(prefix, key) do
    if String.starts_with?(key, prefix), do: key, else: prefix <> key
  end

  defp compound_prefix("hash", key), do: CompoundKey.hash_prefix(key)
  defp compound_prefix("list", key), do: CompoundKey.list_prefix(key)
  defp compound_prefix("set", key), do: CompoundKey.set_prefix(key)
  defp compound_prefix("zset", key), do: CompoundKey.zset_prefix(key)

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
    Enum.filter(keys, fn key ->
      TypeRegistry.get_type(key, store) == type_filter
    end)
  end

  defp filter_by_match(keys, nil), do: keys

  defp filter_by_match(keys, pattern) do
    Enum.filter(keys, &Ferricstore.GlobMatcher.match?(&1, pattern))
  end
end
