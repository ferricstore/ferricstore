# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Strings do
  alias Ferricstore.Store.Ops
  alias Ferricstore.CrossShardOp
  @moduledoc """
  Handles Redis string commands.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

  ## Supported commands

    * `GET key` — returns the value or `nil`
    * `SET key value [EX secs | PX ms | EXAT unix-sec | PXAT unix-ms] [NX | XX] [GET] [KEEPTTL]` — sets a key with optional expiry/conditions
    * `DEL key [key ...]` — deletes keys, returns count deleted
    * `EXISTS key [key ...]` — returns count of existing keys
    * `MGET key [key ...]` — returns list of values (nil for missing)
    * `MSET key value [key value ...]` — sets multiple keys atomically
    * `INCR key` — increment integer value by 1
    * `DECR key` — decrement integer value by 1
    * `INCRBY key increment` — increment integer value by given amount
    * `DECRBY key decrement` — decrement integer value by given amount
    * `INCRBYFLOAT key increment` — increment float value by given amount
    * `APPEND key value` — append to value, return new length
    * `STRLEN key` — return byte length of value
    * `GETSET key value` — set key, return old value
    * `GETDEL key` — get value and delete atomically
    * `GETEX key [EX s | PX ms | EXAT ts | PXAT ms-ts | PERSIST]` — get and update TTL
    * `SETNX key value` — set if not exists
    * `SETEX key seconds value` — set with expiry in seconds
    * `PSETEX key milliseconds value` — set with expiry in milliseconds
    * `GETRANGE key start end` — return substring by byte range
    * `SETRANGE key offset value` — overwrite part of string at offset
    * `MSETNX key value [key value ...]` — set multiple only if none exist
  """

  @doc """
  Handles a string command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"GET"`, `"SET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get`, `put`, `delete`, `exists?` callbacks
      and atomic operations like `incr`, `append`, etc.

  ## Returns

  Plain Elixir term: `:ok`, `nil`, integer, string, list, or `{:error, message}`.
  """
  @max_key_bytes 65_535

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  @wrongtype_error {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

  # ---------------------------------------------------------------------------
  # TYPE -- delegated here from tests; canonical handler is Generic
  # ---------------------------------------------------------------------------

  def handle("TYPE", [key], store) do
    {:simple, Ferricstore.Store.TypeRegistry.get_type(key, store)}
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  def handle("GET", [""], _store), do: {:error, "ERR empty key"}
  def handle("GET", [key], _store) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}

  def handle("GET", [key], store) do
    case Ops.get(store, key) do
      nil ->
        # Plain key is nil. Check if this is a data structure key (compound keys).
        if Ops.has_compound?(store) do
          type_key = Ferricstore.Store.CompoundKey.type_key(key)
          case Ops.compound_get(store, key, type_key) do
            nil -> nil
            _type_str -> @wrongtype_error
          end
        else
          nil
        end
      value when is_binary(value) -> maybe_check_type(value)
      other -> other
    end
  end

  def handle("GET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'get' command"}
  end

  def handle("SET", ["", _value | _opts], _store), do: {:error, "ERR empty key"}
  def handle("SET", [key, _value | _opts], _store) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}
  def handle("SET", [key, value | opts], store), do: do_set(key, value, opts, store)

  def handle("SET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'set' command"}
  end

  def handle("DEL", [], _store) do
    {:error, "ERR wrong number of arguments for 'del' command"}
  end

  def handle("DEL", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      if do_del_key(key, store), do: acc + 1, else: acc
    end)
  end

  def handle("EXISTS", [], _store) do
    {:error, "ERR wrong number of arguments for 'exists' command"}
  end

  def handle("EXISTS", keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      exists = Ops.exists?(store, key)
      # Also check TypeRegistry for compound-key-based data structures
      # (lists, hashes, sets, zsets) that don't use the plain key store.
      exists = exists or (Ops.has_compound?(store) and
        Ops.compound_get(store, key, Ferricstore.Store.CompoundKey.type_key(key)) != nil)
      if exists, do: acc + 1, else: acc
    end)
  end

  def handle("MGET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mget' command"}
  end

  def handle("MGET", keys, store), do: Enum.map(keys, &Ops.get(store, &1))

  def handle("MSET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mset' command"}
  end

  def handle("MSET", args, store) do
    if even_length?(args) do
      # Direct recursive processing avoids chunked enumeration intermediate lists.
      case mset_validate(args) do
        :ok ->
          mset_exec(args, store)
          :ok

        {:error, _} = err ->
          err
      end
    else
      {:error, "ERR wrong number of arguments for 'mset' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # INCR / DECR / INCRBY / DECRBY
  # ---------------------------------------------------------------------------

  def handle("INCR", [key], store), do: Ops.incr(store, key, 1)
  def handle("INCR", _args, _store), do: {:error, "ERR wrong number of arguments for 'incr' command"}

  def handle("DECR", [key], store), do: Ops.incr(store, key, -1)
  def handle("DECR", _args, _store), do: {:error, "ERR wrong number of arguments for 'decr' command"}

  # Redis range: [-2^63, 2^63-1] for integer operations.
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

  def handle("INCRBY", [key, delta_str], store) do
    case Integer.parse(delta_str) do
      {delta, ""} when delta >= @min_int64 and delta <= @max_int64 ->
        Ops.incr(store, key, delta)

      {_delta, ""} ->
        {:error, "ERR value is not an integer or out of range"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("INCRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incrby' command"}

  def handle("DECRBY", [key, delta_str], store) do
    case Integer.parse(delta_str) do
      {delta, ""} when delta >= @min_int64 and delta <= @max_int64 ->
        Ops.incr(store, key, -delta)

      {_delta, ""} ->
        {:error, "ERR value is not an integer or out of range"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("DECRBY", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'decrby' command"}

  # ---------------------------------------------------------------------------
  # INCRBYFLOAT
  # ---------------------------------------------------------------------------

  def handle("INCRBYFLOAT", [key, delta_str], store) do
    case parse_float_arg(delta_str) do
      {:ok, delta} ->
        case Ops.incr_float(store, key, delta) do
          {:ok, new_val} when is_float(new_val) ->
            Ferricstore.Store.ValueCodec.format_float(new_val)
          {:ok, new_str} when is_binary(new_str) -> new_str
          {:error, _} = err -> err
        end

      :error ->
        {:error, "ERR value is not a valid float"}
    end
  end

  def handle("INCRBYFLOAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incrbyfloat' command"}

  # ---------------------------------------------------------------------------
  # APPEND
  # ---------------------------------------------------------------------------

  def handle("APPEND", [key, value], store) do
    {:ok, new_len} = Ops.append(store, key, value)
    new_len
  end

  def handle("APPEND", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'append' command"}

  # ---------------------------------------------------------------------------
  # STRLEN
  # ---------------------------------------------------------------------------

  def handle("STRLEN", [key], store) do
    case Ops.get(store, key) do
      nil -> 0
      v when is_integer(v) -> byte_size(Integer.to_string(v))
      v when is_float(v) -> byte_size(Float.to_string(v))
      v -> byte_size(v)
    end
  end

  def handle("STRLEN", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'strlen' command"}

  # ---------------------------------------------------------------------------
  # GETSET (deprecated but supported)
  # ---------------------------------------------------------------------------

  def handle("GETSET", [key, value], store), do: Ops.getset(store, key, value)

  def handle("GETSET", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getset' command"}

  # ---------------------------------------------------------------------------
  # GETDEL
  # ---------------------------------------------------------------------------

  def handle("GETDEL", [key], store), do: Ops.getdel(store, key)

  def handle("GETDEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getdel' command"}

  # ---------------------------------------------------------------------------
  # GETEX
  # ---------------------------------------------------------------------------

  def handle("GETEX", [key], store), do: Ops.get(store, key)

  def handle("GETEX", [key | opts], store), do: do_getex(key, opts, store)

  def handle("GETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getex' command"}

  # ---------------------------------------------------------------------------
  # SETNX
  # ---------------------------------------------------------------------------

  def handle("SETNX", [key, value], store) do
    opts = %{expire_at_ms: 0, nx: true, xx: false, get: false, keepttl: false}

    case Ops.set(store, key, value, opts) do
      :ok -> 1
      nil -> 0
      {:error, _} = err -> err
    end
  end

  def handle("SETNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setnx' command"}

  # ---------------------------------------------------------------------------
  # SETEX
  # ---------------------------------------------------------------------------

  def handle("SETEX", [key, secs_str, value], store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        expire_at_ms = Ferricstore.HLC.now_ms() + secs * 1_000
        Ops.put(store, key, value, expire_at_ms)

      {_secs, ""} ->
        {:error, "ERR invalid expire time in 'setex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setex' command"}

  # ---------------------------------------------------------------------------
  # PSETEX
  # ---------------------------------------------------------------------------

  def handle("PSETEX", [key, ms_str, value], store) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 ->
        expire_at_ms = Ferricstore.HLC.now_ms() + ms
        Ops.put(store, key, value, expire_at_ms)

      {_ms, ""} ->
        {:error, "ERR invalid expire time in 'psetex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("PSETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'psetex' command"}

  # ---------------------------------------------------------------------------
  # GETRANGE
  # ---------------------------------------------------------------------------

  def handle("GETRANGE", [key, start_str, end_str], store) do
    with {start_idx, ""} <- Integer.parse(start_str),
         {end_idx, ""} <- Integer.parse(end_str) do
      case Ops.get(store, key) do
        nil -> ""
        v when is_integer(v) -> do_getrange(Integer.to_string(v), start_idx, end_idx)
        v when is_float(v) -> do_getrange(Float.to_string(v), start_idx, end_idx)
        value -> do_getrange(value, start_idx, end_idx)
      end
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("GETRANGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getrange' command"}

  # ---------------------------------------------------------------------------
  # SETRANGE
  # ---------------------------------------------------------------------------

  # Redis caps SETRANGE offset at 512MB (536_870_911 = 2^29 - 1).
  @max_setrange_offset 536_870_911

  def handle("SETRANGE", [key, offset_str, value], store) do
    case Integer.parse(offset_str) do
      {offset, ""} when offset >= 0 and offset <= @max_setrange_offset ->
        {:ok, new_len} = Ops.setrange(store, key, offset, value)
        new_len

      {offset, ""} when offset > @max_setrange_offset ->
        {:error, "ERR string exceeds maximum allowed size (512MB)"}

      {_offset, ""} ->
        {:error, "ERR offset is out of range"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("SETRANGE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setrange' command"}

  # ---------------------------------------------------------------------------
  # MSETNX
  # ---------------------------------------------------------------------------

  def handle("MSETNX", [], _store),
    do: {:error, "ERR wrong number of arguments for 'msetnx' command"}

  def handle("MSETNX", args, store) do
    if even_length?(args) do
      keys = extract_keys(args)

      CrossShardOp.execute(
        Enum.map(keys, &{&1, :write}),
        fn unified_store ->
          if msetnx_any_exists?(args, unified_store) do
            0
          else
            mset_exec(args, unified_store)
            1
          end
        end,
        intent: %{command: :msetnx, keys: %{targets: keys}},
        store: store
      )
    else
      {:error, "ERR wrong number of arguments for 'msetnx' command"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — GETEX option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_getex(key, opts, store) do
    case parse_getex_opts(opts) do
      {:ok, expire_at_ms} ->
        Ops.getex(store, key, expire_at_ms)

      {:error, _} = err ->
        err
    end
  end

  defp parse_getex_opts(["PERSIST"]), do: {:ok, 0}

  defp parse_getex_opts(["EX", secs_str]) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        {:ok, Ferricstore.HLC.now_ms() + secs * 1_000}

      {_secs, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PX", ms_str]) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 ->
        {:ok, Ferricstore.HLC.now_ms() + ms}

      {_ms, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["EXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts * 1_000}

      {_ts, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PXAT", ts_str]) do
    case Integer.parse(ts_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts}

      {_ts, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(_) do
    {:error, "ERR syntax error"}
  end

  # ---------------------------------------------------------------------------
  # Private — GETRANGE substring extraction
  # ---------------------------------------------------------------------------

  defp do_getrange(value, start_idx, end_idx) do
    len = byte_size(value)

    # Normalise negative indices
    start_norm = if start_idx < 0, do: max(len + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: len + end_idx, else: end_idx

    # Clamp to bounds
    start_clamped = min(start_norm, len)
    end_clamped = min(end_norm, len - 1)

    if start_clamped > end_clamped do
      ""
    else
      count = end_clamped - start_clamped + 1
      binary_part(value, start_clamped, count)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — float argument parsing
  # ---------------------------------------------------------------------------

  defp parse_float_arg(str) do
    # Try integer first (Redis considers "10" valid for INCRBYFLOAT)
    case Integer.parse(str) do
      {val, ""} ->
        {:ok, val * 1.0}

      _ ->
        case Float.parse(str) do
          {val, ""} ->
            # Reject inf/nan
            cond do
              val == :infinity -> :error
              val == :neg_infinity -> :error
              true -> {:ok, val}
            end

          _ ->
            :error
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Private — SET option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_set(key, value, opts, store) do
    with {:ok, parsed} <- parse_set_opts(opts) do
      if parsed.nx or parsed.xx or parsed.get or parsed.keepttl do
        Ops.set(store, key, value, parsed)
      else
        Ops.put(store, key, value, parsed.expire_at_ms)
      end
    end
  end

  # Accumulator map for SET option parsing. All fields start at their defaults.
  @set_opts_default %{expire_at_ms: 0, nx: false, xx: false, get: false, keepttl: false, has_expiry: false}

  defp parse_set_opts(opts), do: parse_set_opts(opts, @set_opts_default)

  defp parse_set_opts([], acc) do
    if acc.nx and acc.xx do
      {:error, "ERR XX and NX options at the same time are not compatible"}
    else
      {:ok, acc}
    end
  end

  defp parse_set_opts(["NX" | rest], acc) do
    parse_set_opts(rest, %{acc | nx: true})
  end

  defp parse_set_opts(["XX" | rest], acc) do
    parse_set_opts(rest, %{acc | xx: true})
  end

  defp parse_set_opts(["GET" | rest], acc) do
    parse_set_opts(rest, %{acc | get: true})
  end

  defp parse_set_opts(["KEEPTTL" | rest], acc) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      parse_set_opts(rest, %{acc | keepttl: true, has_expiry: true})
    end
  end

  defp parse_set_opts(["EX", secs_str | rest], acc) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      with {secs, ""} <- Integer.parse(secs_str),
           true <- secs > 0 do
        parse_set_opts(rest, %{acc | expire_at_ms: Ferricstore.HLC.now_ms() + secs * 1000, has_expiry: true})
      else
        false -> {:error, "ERR invalid expire time in 'set' command"}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  defp parse_set_opts(["PX", ms_str | rest], acc) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      with {ms, ""} <- Integer.parse(ms_str),
           true <- ms > 0 do
        parse_set_opts(rest, %{acc | expire_at_ms: Ferricstore.HLC.now_ms() + ms, has_expiry: true})
      else
        false -> {:error, "ERR invalid expire time in 'set' command"}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  defp parse_set_opts(["EXAT", ts_str | rest], acc) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      with {ts, ""} <- Integer.parse(ts_str),
           true <- ts > 0 do
        parse_set_opts(rest, %{acc | expire_at_ms: ts * 1000, has_expiry: true})
      else
        false -> {:error, "ERR invalid expire time in 'set' command"}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  defp parse_set_opts(["PXAT", ts_str | rest], acc) do
    if acc.has_expiry do
      {:error, "ERR syntax error"}
    else
      with {ts, ""} <- Integer.parse(ts_str),
           true <- ts > 0 do
        parse_set_opts(rest, %{acc | expire_at_ms: ts, has_expiry: true})
      else
        false -> {:error, "ERR invalid expire time in 'set' command"}
        _ -> {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  defp parse_set_opts([unknown | _rest], _acc) do
    {:error, "ERR syntax error, option '#{unknown}' not recognized"}
  end

  # ---------------------------------------------------------------------------
  # Private — MSET/MSETNX helpers (direct recursion, no chunked enumeration)
  # ---------------------------------------------------------------------------

  # Validates all keys in a flat [k, v, k, v, ...] list without creating
  # intermediate chunk lists.
  defp mset_validate([]), do: :ok

  defp mset_validate([k, _v | rest]) do
    if k == "" or byte_size(k) > @max_key_bytes do
      {:error, "ERR key too large or empty"}
    else
      mset_validate(rest)
    end
  end

  # Executes MSET by walking the flat [k, v, k, v, ...] list directly.
  defp mset_exec([], _store), do: :ok

  defp mset_exec([k, v | rest], store) do
    Ops.put(store, k, v, 0)
    mset_exec(rest, store)
  end

  # Checks if any key in a flat [k, v, k, v, ...] list already exists.
  defp msetnx_any_exists?([], _store), do: false

  defp msetnx_any_exists?([k, _v | rest], store) do
    if Ops.exists?(store, k), do: true, else: msetnx_any_exists?(rest, store)
  end

  # Extracts keys from a flat [k, v, k, v, ...] list.
  defp extract_keys([]), do: []
  defp extract_keys([k, _v | rest]), do: [k | extract_keys(rest)]

  # O(n/2) parity check without computing full length.
  defp even_length?([]), do: true
  defp even_length?([_, _ | rest]), do: even_length?(rest)
  defp even_length?(_), do: false

  # ---------------------------------------------------------------------------
  # Private — type checking for GET
  # ---------------------------------------------------------------------------

  # Detects if a stored binary is actually a serialized non-string type
  # (list, hash, set, zset). If so, returns WRONGTYPE error instead of the
  # raw binary. This matches Redis behaviour where GET on a non-string key
  # returns a WRONGTYPE error.
  #
  # Peeks at the ETF header bytes to identify tuple tags without deserializing
  # the entire payload. This avoids multi-MB heap spikes for large data
  # structures (e.g., a hash with 10K fields stored as ETF).
  #
  # ETF format for a 2-tuple like {:list, payload}:
  #   131 = ETF version tag
  #   104 = SMALL_TUPLE_EXT (arity < 256)
  #   2   = arity (2-tuple)
  #   100 = ATOM_EXT (followed by 2-byte length + atom bytes)
  #         or 119 = SMALL_ATOM_UTF8_EXT (1-byte length + atom bytes)
  #         or 118 = ATOM_UTF8_EXT (2-byte length + atom bytes)
  #         or 115 = SMALL_ATOM_EXT (1-byte length + atom bytes)
  defp maybe_check_type(<<131, 104, 2, rest::binary>> = value) do
    case extract_etf_atom_name(rest) do
      name when name in ["list", "hash", "set", "zset"] -> @wrongtype_error
      _ -> value
    end
  end

  # LARGE_TUPLE_EXT (arity 2) - same check for large tuples
  defp maybe_check_type(<<131, 105, 0, 0, 0, 2, rest::binary>> = value) do
    case extract_etf_atom_name(rest) do
      name when name in ["list", "hash", "set", "zset"] -> @wrongtype_error
      _ -> value
    end
  end

  defp maybe_check_type(value), do: value

  # Extracts the atom name from the beginning of an ETF-encoded atom.
  # Returns the atom name as a string, or nil if unrecognized format.
  # ATOM_EXT (tag 100): 2-byte big-endian length + atom bytes (Latin1)
  defp extract_etf_atom_name(<<100, len::16, name::binary-size(len), _::binary>>), do: name
  # SMALL_ATOM_UTF8_EXT (tag 119): 1-byte length + atom bytes (UTF8)
  defp extract_etf_atom_name(<<119, len::8, name::binary-size(len), _::binary>>), do: name
  # ATOM_UTF8_EXT (tag 118): 2-byte length + atom bytes (UTF8)
  defp extract_etf_atom_name(<<118, len::16, name::binary-size(len), _::binary>>), do: name
  # SMALL_ATOM_EXT (tag 115): 1-byte length + atom bytes (Latin1)
  defp extract_etf_atom_name(<<115, len::8, name::binary-size(len), _::binary>>), do: name
  defp extract_etf_atom_name(_), do: nil

  # ---------------------------------------------------------------------------
  # Private — DEL key deletion (plain + compound)
  # ---------------------------------------------------------------------------

  # Deletes a single key, handling both plain string keys and data structure
  # keys that use compound sub-keys. Returns `true` if the key existed and
  # was deleted, `false` otherwise.
  defp do_del_key(key, store) do
    alias Ferricstore.Store.{CompoundKey, TypeRegistry}

    # Check for data structure type metadata when compound operations are
    # available (the store has compound_get). When they are not available
    # (e.g. raw Router-based store without data structure support), fall
    # through to plain key deletion.
    has_compound? = Ops.has_compound?(store)

    if has_compound? do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get(store, key, type_key) do
        nil ->
          # No type metadata -- plain string key
          if Ops.exists?(store, key) do
            Ops.delete(store, key)
            true
          else
            false
          end

        type_str ->
          # Data structure key -- delete compound sub-keys, then type metadata.
          # Lists store data as serialized Erlang terms in the plain key store,
          # so we must also delete the plain key for list types.
          prefix =
            case type_str do
              "hash" -> CompoundKey.hash_prefix(key)
              "list" -> CompoundKey.list_prefix(key)
              "set" -> CompoundKey.set_prefix(key)
              "zset" -> CompoundKey.zset_prefix(key)
            end

          Ops.compound_delete_prefix(store, key, prefix)
          if type_str == "list" do
            meta_key = CompoundKey.list_meta_key(key)
            Ops.compound_delete(store, key, meta_key)
          end
          TypeRegistry.delete_type(key, store)
          true
      end
    else
      if Ops.exists?(store, key) do
        Ops.delete(store, key)
        true
      else
        false
      end
    end
  end
end
