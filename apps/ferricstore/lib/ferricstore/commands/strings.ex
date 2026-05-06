# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Strings do
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK}
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

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

  def handle("GET", [key], store), do: get_key(key, store)

  def handle("GET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'get' command"}
  end

  def handle("SET", ["", _value | _opts], _store), do: {:error, "ERR empty key"}

  def handle("SET", [key, _value | _opts], _store) when byte_size(key) > 65_535,
    do: {:error, "ERR key too large"}

  def handle("SET", [key, value | opts], store), do: do_set(key, value, opts, store)

  def handle("SET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'set' command"}
  end

  @set_opts_default %{
    expire_at_ms: 0,
    nx: false,
    xx: false,
    get: false,
    keepttl: false,
    has_expiry: false
  }

  def handle("DEL", [], _store) do
    {:error, "ERR wrong number of arguments for 'del' command"}
  end

  def handle("DEL", keys, store), do: del_keys(keys, store)

  def handle("EXISTS", [], _store) do
    {:error, "ERR wrong number of arguments for 'exists' command"}
  end

  def handle("EXISTS", keys, store), do: exists_keys(keys, store)

  def handle("MGET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mget' command"}
  end

  def handle("MGET", keys, store), do: mget_keys(keys, store)

  def handle("MSET", [], _store) do
    {:error, "ERR wrong number of arguments for 'mset' command"}
  end

  def handle("MSET", args, store), do: mset_args(args, store)

  # ---------------------------------------------------------------------------
  # INCR / DECR / INCRBY / DECRBY
  # ---------------------------------------------------------------------------

  def handle("INCR", [key], store), do: incr_key(key, store)

  def handle("INCR", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incr' command"}

  def handle("DECR", [key], store), do: decr_key(key, store)

  def handle("DECR", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'decr' command"}

  # Redis range: [-2^63, 2^63-1] for integer operations.
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808

  def handle("INCRBY", [key, delta_str], store) do
    case Integer.parse(delta_str) do
      {delta, ""} when delta >= @min_int64 and delta <= @max_int64 ->
        incr_string_key(key, delta, store)

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
        incr_string_key(key, -delta, store)

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
        incr_string_key_float(key, delta, store)

      :error ->
        {:error, "ERR value is not a valid float"}
    end
  end

  def handle("INCRBYFLOAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'incrbyfloat' command"}

  # ---------------------------------------------------------------------------
  # APPEND
  # ---------------------------------------------------------------------------

  def handle("APPEND", [key, value], store), do: append_value(key, value, store)

  def handle("APPEND", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'append' command"}

  # ---------------------------------------------------------------------------
  # STRLEN
  # ---------------------------------------------------------------------------

  def handle("STRLEN", [key], store), do: strlen_key(key, store)

  def handle("STRLEN", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'strlen' command"}

  # ---------------------------------------------------------------------------
  # GETSET (deprecated but supported)
  # ---------------------------------------------------------------------------

  def handle("GETSET", [key, value], store), do: getset_value(key, value, store)

  def handle("GETSET", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getset' command"}

  # ---------------------------------------------------------------------------
  # GETDEL
  # ---------------------------------------------------------------------------

  def handle("GETDEL", [key], store), do: getdel_key(key, store)

  def handle("GETDEL", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getdel' command"}

  # ---------------------------------------------------------------------------
  # GETEX
  # ---------------------------------------------------------------------------

  def handle("GETEX", [key], store) do
    case read_string_value(key, store) do
      {:value, value} -> value
      :missing -> nil
      @wrongtype_error -> @wrongtype_error
    end
  end

  def handle("GETEX", [key | opts], store), do: do_getex(key, opts, store)

  def handle("GETEX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'getex' command"}

  # ---------------------------------------------------------------------------
  # SETNX
  # ---------------------------------------------------------------------------

  def handle("SETNX", [key, value], store), do: setnx_value(key, value, store)

  def handle("SETNX", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'setnx' command"}

  # ---------------------------------------------------------------------------
  # SETEX
  # ---------------------------------------------------------------------------

  def handle("SETEX", [key, secs_str, value], store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        setex_parsed(key, secs, value, store)

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
        psetex_parsed(key, ms, value, store)

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
      getrange_parsed(key, start_idx, end_idx, store)
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
        setrange_parsed(key, offset, value, store)

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

  def handle("MSETNX", args, store), do: msetnx_args(args, store)

  @doc false
  def handle_ast({:get, key}, store) when is_binary(key), do: get_key(key, store)

  def handle_ast({:get, _args}, _store),
    do: {:error, "ERR wrong number of arguments for 'get' command"}

  def handle_ast({:del, keys}, store) when is_list(keys) and keys != [], do: del_keys(keys, store)

  def handle_ast({:exists, keys}, store) when is_list(keys) and keys != [],
    do: exists_keys(keys, store)

  def handle_ast({:mget, keys}, store) when is_list(keys) and keys != [],
    do: mget_keys(keys, store)

  def handle_ast({:mset, args}, store), do: mset_args(args, store)
  def handle_ast({:incr, key}, store), do: incr_key(key, store)
  def handle_ast({:decr, key}, store), do: decr_key(key, store)
  def handle_ast({:append, key, value}, store), do: append_value(key, value, store)
  def handle_ast({:strlen, key}, store), do: strlen_key(key, store)
  def handle_ast({:getset, key, value}, store), do: getset_value(key, value, store)
  def handle_ast({:getdel, key}, store), do: getdel_key(key, store)
  def handle_ast({:setnx, key, value}, store), do: setnx_value(key, value, store)
  def handle_ast({:msetnx, args}, store), do: msetnx_args(args, store)

  def handle_ast({:set, "", _value}, _store), do: {:error, "ERR empty key"}
  def handle_ast({:set, "", _value, _opts}, _store), do: {:error, "ERR empty key"}

  def handle_ast({:incrby, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:decrby, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:incrbyfloat, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:getex, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:setex, _key, {:error, reason}, _value}, _store), do: {:error, reason}
  def handle_ast({:psetex, _key, {:error, reason}, _value}, _store), do: {:error, reason}
  def handle_ast({:getrange, _key, {:error, reason}, _stop}, _store), do: {:error, reason}
  def handle_ast({:getrange, _key, _start, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:setrange, _key, {:error, reason}, _value}, _store), do: {:error, reason}

  def handle_ast({:incrby, key, delta}, store) when is_integer(delta),
    do: incr_string_key(key, delta, store)

  def handle_ast({:decrby, key, delta}, store) when is_integer(delta),
    do: incr_string_key(key, -delta, store)

  def handle_ast({:incrbyfloat, key, delta}, store) when is_float(delta),
    do: incr_string_key_float(key, delta, store)

  def handle_ast({:getex, key}, store) do
    case read_string_value(key, store) do
      {:value, value} -> value
      :missing -> nil
      @wrongtype_error -> @wrongtype_error
    end
  end

  def handle_ast({:getex, key, :persist}, store), do: getex_parsed(key, 0, store)

  def handle_ast({:getex, key, {:ex, secs}}, store) when is_integer(secs),
    do: getex_parsed(key, CommandTime.now_ms() + secs * 1_000, store)

  def handle_ast({:getex, key, {:px, ms}}, store) when is_integer(ms),
    do: getex_parsed(key, CommandTime.now_ms() + ms, store)

  def handle_ast({:getex, key, {:exat, ts}}, store) when is_integer(ts),
    do: getex_parsed(key, ts * 1_000, store)

  def handle_ast({:getex, key, {:pxat, ts}}, store) when is_integer(ts),
    do: getex_parsed(key, ts, store)

  def handle_ast({:setex, key, seconds, value}, store) when is_integer(seconds) do
    if seconds > 0 do
      setex_parsed(key, seconds, value, store)
    else
      {:error, "ERR invalid expire time in 'setex' command"}
    end
  end

  def handle_ast({:psetex, key, ms, value}, store) when is_integer(ms) do
    if ms > 0 do
      psetex_parsed(key, ms, value, store)
    else
      {:error, "ERR invalid expire time in 'psetex' command"}
    end
  end

  def handle_ast({:getrange, key, start_idx, end_idx}, store)
      when is_integer(start_idx) and is_integer(end_idx),
      do: getrange_parsed(key, start_idx, end_idx, store)

  def handle_ast({:setrange, key, offset, value}, store) when is_integer(offset) do
    cond do
      offset >= 0 and offset <= @max_setrange_offset ->
        setrange_parsed(key, offset, value, store)

      offset > @max_setrange_offset ->
        {:error, "ERR string exceeds maximum allowed size (512MB)"}

      true ->
        {:error, "ERR offset is out of range"}
    end
  end

  def handle_ast({:set, key, _value}, _store) when byte_size(key) > 65_535,
    do: {:error, "ERR key too large"}

  def handle_ast({:set, key, _value, _opts}, _store) when byte_size(key) > 65_535,
    do: {:error, "ERR key too large"}

  def handle_ast({:set, key, value}, store),
    do: do_set_parsed(key, value, @set_opts_default, store)

  def handle_ast({:set, _key, _value, {:error, reason}}, _store) when is_binary(reason),
    do: {:error, reason}

  def handle_ast({:set, key, value, opts}, store) when is_list(opts) do
    with {:ok, parsed} <- set_opts_from_ast(opts) do
      do_set_parsed(key, value, parsed, store)
    end
  end

  def handle_ast(_ast, _store), do: {:error, "ERR wrong number of arguments for 'set' command"}

  defp get_key("", _store), do: {:error, "ERR empty key"}
  defp get_key(key, _store) when byte_size(key) > 65_535, do: {:error, "ERR key too large"}

  defp get_key(key, store) do
    case read_string_value(key, store) do
      {:value, value} -> value
      :missing -> nil
      @wrongtype_error -> @wrongtype_error
    end
  end

  defp del_keys(keys, store) do
    keys
    |> Enum.reduce_while({:ok, 0}, fn key, {:ok, acc} ->
      case do_del_key(key, store) do
        true -> {:cont, {:ok, acc + 1}}
        false -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, count} -> count
      {:error, _reason} = error -> error
    end
  end

  defp exists_keys(keys, store) do
    Enum.reduce(keys, 0, fn key, acc ->
      exists = Ops.exists?(store, key)
      # Also check TypeRegistry for compound-key-based data structures
      # (lists, hashes, sets, zsets) that don't use the plain key store.
      exists =
        exists or
          (Ops.has_compound?(store) and TypeRegistry.get_type(key, store) != "none")

      if exists, do: acc + 1, else: acc
    end)
  end

  defp mget_keys(keys, store), do: Ops.batch_get(store, keys)

  defp mset_args(args, store) do
    if even_length?(args) do
      case mset_validate(args) do
        :ok -> mset_exec(args, store)
        {:error, _} = err -> err
      end
    else
      {:error, "ERR wrong number of arguments for 'mset' command"}
    end
  end

  defp incr_key(key, store), do: incr_string_key(key, 1, store)
  defp decr_key(key, store), do: incr_string_key(key, -1, store)

  defp append_value(key, value, store) do
    case ensure_string_key(key, store) do
      :ok ->
        case Ops.append(store, key, value) do
          {:ok, new_len} -> new_len
          {:error, _} = err -> err
        end

      @wrongtype_error ->
        @wrongtype_error
    end
  end

  defp strlen_key(key, store) do
    case read_string_size(key, store) do
      :missing -> 0
      @wrongtype_error -> @wrongtype_error
      {:size, size} -> size
    end
  end

  defp getset_value(key, value, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.getset(store, key, value)
      @wrongtype_error -> @wrongtype_error
    end
  end

  defp getdel_key(key, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.getdel(store, key)
      @wrongtype_error -> @wrongtype_error
    end
  end

  defp setnx_value(key, value, store) do
    opts = %{expire_at_ms: 0, nx: true, xx: false, get: false, keepttl: false}

    if compound_data_structure_key?(key, store) do
      0
    else
      case Ops.set(store, key, value, opts) do
        :ok -> 1
        nil -> 0
        {:error, _} = err -> err
      end
    end
  end

  defp msetnx_args(args, store) do
    if even_length?(args) do
      keys = extract_keys(args)

      CrossShardOp.execute(
        Enum.map(keys, &{&1, :write}),
        fn unified_store ->
          if msetnx_any_exists?(args, unified_store) do
            0
          else
            case mset_exec(args, unified_store) do
              :ok -> 1
              {:error, _} = err -> err
            end
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
        getex_parsed(key, expire_at_ms, store)

      {:error, _} = err ->
        err
    end
  end

  defp incr_string_key(key, delta, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.incr(store, key, delta)
      @wrongtype_error -> @wrongtype_error
    end
  end

  defp incr_string_key_float(key, delta, store) do
    incr_result =
      case ensure_string_key(key, store) do
        :ok -> Ops.incr_float(store, key, delta)
        @wrongtype_error -> @wrongtype_error
      end

    case incr_result do
      {:ok, new_val} when is_float(new_val) ->
        Ferricstore.Store.ValueCodec.format_float(new_val)

      {:ok, new_str} when is_binary(new_str) ->
        new_str

      {:error, _} = err ->
        err
    end
  end

  defp getex_parsed(key, expire_at_ms, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.getex(store, key, expire_at_ms)
      @wrongtype_error -> @wrongtype_error
    end
  end

  defp parse_getex_opts(["PERSIST"]), do: {:ok, 0}

  defp parse_getex_opts(["EX", secs_str]) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 ->
        {:ok, CommandTime.now_ms() + secs * 1_000}

      {_secs, ""} ->
        {:error, "ERR invalid expire time in 'getex' command"}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_getex_opts(["PX", ms_str]) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms > 0 ->
        {:ok, CommandTime.now_ms() + ms}

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

  defp metadata_value_size(%FerricStore.Instance{} = store, key), do: Ops.value_size(store, key)

  defp metadata_value_size(%Ferricstore.Store.LocalTxStore{} = store, key),
    do: Ops.value_size(store, key)

  defp metadata_value_size(%{value_size: value_size}, key) when is_function(value_size, 1),
    do: value_size.(key)

  defp metadata_value_size(_store, _key), do: :unknown

  defp getrange_parsed(key, start_idx, end_idx, store) do
    case metadata_value_size(store, key) do
      size when is_integer(size) ->
        if getrange_empty_for_size?(size, start_idx, end_idx) do
          if compound_data_structure_key?(key, store), do: @wrongtype_error, else: ""
        else
          read_getrange_value(key, start_idx, end_idx, store)
        end

      _unknown_or_missing ->
        read_getrange_value(key, start_idx, end_idx, store)
    end
  end

  defp read_getrange_value(key, start_idx, end_idx, store) do
    case Ops.getrange(store, key, start_idx, end_idx) do
      nil ->
        if compound_data_structure_key?(key, store), do: @wrongtype_error, else: ""

      value ->
        value
    end
  end

  defp getrange_empty_for_size?(size, start_idx, end_idx) do
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    start_clamped > end_clamped
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
      do_set_parsed(key, value, parsed, store)
    end
  end

  defp setex_parsed(key, secs, value, store) do
    expire_at_ms = CommandTime.now_ms() + secs * 1_000
    replace_string_key(key, value, expire_at_ms, store)
  end

  defp psetex_parsed(key, ms, value, store) do
    expire_at_ms = CommandTime.now_ms() + ms
    replace_string_key(key, value, expire_at_ms, store)
  end

  defp setrange_parsed(key, offset, value, store) do
    case ensure_string_key(key, store) do
      :ok ->
        case Ops.setrange(store, key, offset, value) do
          {:ok, new_len} -> new_len
          {:error, _} = err -> err
        end

      @wrongtype_error ->
        @wrongtype_error
    end
  end

  defp do_set_parsed(key, value, parsed, store) do
    cond do
      parsed.get and compound_data_structure_key?(key, store) ->
        @wrongtype_error

      parsed.nx and compound_data_structure_key?(key, store) ->
        nil

      compound_data_structure_key?(key, store) ->
        replace_string_key(key, value, parsed.expire_at_ms, store)

      parsed.nx or parsed.xx or parsed.get or parsed.keepttl ->
        Ops.set(store, key, value, parsed)

      true ->
        replace_string_key(key, value, parsed.expire_at_ms, store)
    end
  end

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
        parse_set_opts(rest, %{
          acc
          | expire_at_ms: CommandTime.now_ms() + secs * 1000,
            has_expiry: true
        })
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
        parse_set_opts(rest, %{acc | expire_at_ms: CommandTime.now_ms() + ms, has_expiry: true})
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

  defp set_opts_from_ast(opts), do: set_opts_from_ast(opts, @set_opts_default)

  defp set_opts_from_ast([], acc), do: {:ok, acc}
  defp set_opts_from_ast([:nx | rest], acc), do: set_opts_from_ast(rest, %{acc | nx: true})
  defp set_opts_from_ast([:xx | rest], acc), do: set_opts_from_ast(rest, %{acc | xx: true})
  defp set_opts_from_ast([:get | rest], acc), do: set_opts_from_ast(rest, %{acc | get: true})

  defp set_opts_from_ast([:keepttl | rest], acc) do
    set_opts_from_ast(rest, %{acc | keepttl: true, has_expiry: true})
  end

  defp set_opts_from_ast([{:ex, seconds} | rest], acc) when is_integer(seconds) do
    set_opts_from_ast(rest, %{
      acc
      | expire_at_ms: CommandTime.now_ms() + seconds * 1000,
        has_expiry: true
    })
  end

  defp set_opts_from_ast([{:px, ms} | rest], acc) when is_integer(ms) do
    set_opts_from_ast(rest, %{acc | expire_at_ms: CommandTime.now_ms() + ms, has_expiry: true})
  end

  defp set_opts_from_ast([{:exat, seconds} | rest], acc) when is_integer(seconds) do
    set_opts_from_ast(rest, %{acc | expire_at_ms: seconds * 1000, has_expiry: true})
  end

  defp set_opts_from_ast([{:pxat, ms} | rest], acc) when is_integer(ms) do
    set_opts_from_ast(rest, %{acc | expire_at_ms: ms, has_expiry: true})
  end

  defp set_opts_from_ast(_opts, _acc), do: {:error, "ERR syntax error"}

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

  defp mset_exec([], _store), do: :ok

  defp mset_exec(args, %FerricStore.Instance{} = store) do
    Ops.batch_put(store, mset_pairs(args))
  end

  defp mset_exec(args, store) do
    if mset_needs_compound_cleanup?(args, store) do
      mset_exec_sequential(args, store)
    else
      Ops.batch_put(store, mset_pairs(args))
    end
  end

  defp mset_needs_compound_cleanup?([], _store), do: false

  defp mset_needs_compound_cleanup?([k, _v | rest], store) do
    compound_data_structure_key?(k, store) or mset_needs_compound_cleanup?(rest, store)
  end

  defp mset_pairs([]), do: []
  defp mset_pairs([k, v | rest]), do: [{k, v} | mset_pairs(rest)]

  # Fallback path preserves per-key compound cleanup for stores that cannot
  # provide string-batch replacement semantics themselves.
  defp mset_exec_sequential([], _store), do: :ok

  defp mset_exec_sequential([k, v | rest], store) do
    case replace_string_key(k, v, 0, store) do
      :ok -> mset_exec_sequential(rest, store)
      {:error, _} = err -> err
    end
  end

  # Checks if any key in a flat [k, v, k, v, ...] list already exists.
  defp msetnx_any_exists?([], _store), do: false

  defp msetnx_any_exists?([k, _v | rest], store) do
    if Ops.exists?(store, k) or compound_data_structure_key?(k, store),
      do: true,
      else: msetnx_any_exists?(rest, store)
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

  defp read_string_value(key, store) do
    case Ops.get(store, key) do
      nil ->
        if compound_data_structure_key?(key, store), do: @wrongtype_error, else: :missing

      value when is_binary(value) ->
        {:value, value}

      other ->
        {:value, other}
    end
  end

  defp ensure_string_key(key, store) do
    if compound_data_structure_key?(key, store), do: @wrongtype_error, else: :ok
  end

  defp read_string_size(key, store) do
    case Ops.value_size(store, key) do
      nil ->
        if compound_data_structure_key?(key, store), do: @wrongtype_error, else: :missing

      size ->
        {:size, size}
    end
  end

  defp replace_string_key(key, value, expire_at_ms, store) do
    with :ok <- clear_compound_data_structure(key, store) do
      Ops.put(store, key, value, expire_at_ms)
    end
  end

  defp compound_data_structure_key?(key, store) do
    Ops.has_compound?(store) and
      compound_type_marker?(key, store) and
      TypeRegistry.get_type(key, store) != "none"
  end

  defp compound_type_marker?(key, store) do
    # Raw type markers can outlive all fields when the last field expires.
    # Keep the cheap no-marker fast path, then let TypeRegistry clean stale markers.
    Ops.compound_get(store, key, CompoundKey.type_key(key)) != nil
  end

  defp clear_compound_data_structure(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get(store, key, type_key) do
        nil ->
          :ok

        type ->
          with :ok <- clear_compound_prefix(key, type, store),
               :ok <- Ops.compound_delete(store, key, type_key) do
            :ok
          end
      end
    else
      :ok
    end
  end

  defp clear_compound_prefix(key, "hash", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.hash_prefix(key))

  defp clear_compound_prefix(key, "list", store) do
    with :ok <- Ops.compound_delete_prefix(store, key, CompoundKey.list_prefix(key)),
         :ok <- Ops.compound_delete(store, key, CompoundKey.list_meta_key(key)) do
      :ok
    end
  end

  defp clear_compound_prefix(key, "set", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.set_prefix(key))

  defp clear_compound_prefix(key, "zset", store),
    do: Ops.compound_delete_prefix(store, key, CompoundKey.zset_prefix(key))

  defp clear_compound_prefix(_key, _type, _store), do: :ok

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
          # No type metadata -- plain string key, or a stream that only has
          # X:<key>\0 compound entries plus local ETS metadata.
          case maybe_delete_stream_key(key, store) do
            true -> true
            false -> delete_plain_key_if_exists(key, store)
            {:error, _reason} = error -> error
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
              "stream" -> stream_prefix(key)
              _unknown -> nil
            end

          case delete_compound_key_data(key, type_str, prefix, store) do
            :ok -> true
            {:error, _reason} = error -> error
          end
      end
    else
      if Ops.exists?(store, key) do
        case Ops.delete(store, key) do
          :ok -> true
          {:error, _reason} = error -> error
        end
      else
        false
      end
    end
  end

  defp delete_plain_key_if_exists(key, store) do
    if Ops.exists?(store, key) do
      case maybe_delete_prob_file(key, store) do
        :ok ->
          case Ops.delete(store, key) do
            :ok -> true
            {:error, _reason} = error -> error
          end

        {:error, _reason} = error ->
          error
      end
    else
      false
    end
  end

  defp delete_compound_key_data(key, type_str, prefix, store) do
    with :ok <- delete_compound_prefix_if_present(key, prefix, store),
         :ok <- delete_list_meta_if_needed(key, type_str, store),
         :ok <- delete_stream_metadata_if_needed(key, type_str),
         :ok <- TypeRegistry.delete_type(key, store) do
      :ok
    end
  end

  defp delete_compound_prefix_if_present(_key, nil, _store), do: :ok

  defp delete_compound_prefix_if_present(key, prefix, store) do
    Ops.compound_delete_prefix(store, key, prefix)
  end

  defp delete_list_meta_if_needed(key, "list", store) do
    Ops.compound_delete(store, key, CompoundKey.list_meta_key(key))
  end

  defp delete_list_meta_if_needed(_key, _type_str, _store), do: :ok

  defp delete_stream_metadata_if_needed(key, "stream") do
    cleanup_stream_metadata(key)
    :ok
  end

  defp delete_stream_metadata_if_needed(_key, _type_str), do: :ok

  defp maybe_delete_prob_file(_key, %FerricStore.Instance{}), do: :ok
  defp maybe_delete_prob_file(_key, %Ferricstore.Store.LocalTxStore{}), do: :ok
  defp maybe_delete_prob_file(_key, %{prob_write: write_fn}) when is_function(write_fn), do: :ok

  defp maybe_delete_prob_file(key, store) when is_map(store) do
    case prob_type(key, store) do
      :bloom -> Bloom.nif_delete(key, store)
      :cms -> CMS.nif_delete(key, store)
      :cuckoo -> Cuckoo.nif_delete(key, store)
      :topk -> TopK.nif_delete(key, store)
      nil -> :ok
    end
  end

  defp maybe_delete_prob_file(_key, _store), do: :ok

  defp prob_type(key, store) do
    store
    |> Ops.get(key)
    |> decode_prob_meta()
  rescue
    _ -> nil
  end

  defp decode_prob_meta(value) when is_binary(value) do
    try do
      value
      |> :erlang.binary_to_term([:safe])
      |> decode_prob_meta()
    rescue
      _ -> nil
    end
  end

  defp decode_prob_meta({:bloom_meta, _}), do: :bloom
  defp decode_prob_meta({:cms_meta, _}), do: :cms
  defp decode_prob_meta({:cuckoo_meta, _}), do: :cuckoo
  defp decode_prob_meta({:topk_meta, _}), do: :topk
  defp decode_prob_meta({:topk_path, _}), do: :topk
  defp decode_prob_meta(_), do: nil

  defp maybe_delete_stream_key(key, store) do
    prefix = stream_prefix(key)

    if Ops.compound_scan(store, key, prefix) != [] or stream_metadata_exists?(key) do
      case Ops.compound_delete_prefix(store, key, prefix) do
        :ok ->
          cleanup_stream_metadata(key)
          true

        {:error, _reason} = error ->
          error
      end
    else
      false
    end
  end

  defp stream_prefix(key), do: "X:" <> key <> <<0>>

  defp stream_metadata_exists?(key) do
    table = Ferricstore.Stream.Meta
    :ets.whereis(table) != :undefined and :ets.lookup(table, key) != []
  end

  defp cleanup_stream_metadata(key) do
    meta_table = Ferricstore.Stream.Meta
    groups_table = Ferricstore.Stream.Groups
    waiters_table = :ferricstore_stream_waiters

    if :ets.whereis(meta_table) != :undefined do
      :ets.delete(meta_table, key)
    end

    if :ets.whereis(groups_table) != :undefined do
      :ets.match_delete(groups_table, {{key, :_}, :_, :_, :_})
    end

    if :ets.whereis(waiters_table) != :undefined do
      :ets.match_delete(waiters_table, {key, :_, :_, :_})
    end

    :ok
  end
end
