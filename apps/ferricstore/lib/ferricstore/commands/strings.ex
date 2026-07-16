# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Strings do
  alias Ferricstore.Commands.ExpiryTime
  alias Ferricstore.Commands.Strings.{Compound, Delete, GetEx, MSet, Range, SetOptions}
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.TypeRegistry

  @moduledoc """
  Handles Redis string commands.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles wire encoding.

  ## Supported commands

    * `GET key` — returns the value or `nil`
    * `SET key value [EX secs | PX ms | EXAT unix-sec | PXAT unix-ms] [NX | XX] [GET] [KEEPTTL]` — sets a key with optional expiry/conditions
    * `DEL key [key ...]` — deletes keys, returns count deleted
    * `EXISTS key [key ...]` — returns count of existing keys
    * `MGET key [key ...]` — returns list of values (nil for missing)
    * `MSET key value [key value ...]` — atomically sets keys in one hash slot
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
    case TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      type -> {:simple, type}
    end
  end

  def handle("TYPE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'type' command"}
  end

  def handle("GET", [key], store), do: get_key(key, store)

  def handle("GET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'get' command"}
  end

  def handle("SET", ["", _value | _opts], _store), do: {:error, "ERR empty key"}

  def handle("SET", [key, _value | _opts], _store) when byte_size(key) > @max_key_bytes,
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
      {:error, _reason} = error -> error
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
      {:error, _reason} = error -> error
    end
  end

  def handle_ast({:getex, key, :persist}, store), do: GetEx.getex_parsed(key, 0, store)

  def handle_ast({:getex, key, {:ex, secs}}, store) when is_integer(secs) and secs > 0,
    do: getex_with_expiry(key, secs, 1_000, :relative, store)

  def handle_ast({:getex, key, {:px, ms}}, store) when is_integer(ms) and ms > 0,
    do: getex_with_expiry(key, ms, 1, :relative, store)

  def handle_ast({:getex, key, {:exat, ts}}, store) when is_integer(ts) and ts > 0,
    do: getex_with_expiry(key, ts, 1_000, :absolute, store)

  def handle_ast({:getex, key, {:pxat, ts}}, store) when is_integer(ts) and ts > 0,
    do: getex_with_expiry(key, ts, 1, :absolute, store)

  def handle_ast({:getex, _key, {mode, value}}, _store)
      when mode in [:ex, :px, :exat, :pxat] and is_integer(value),
      do: {:error, "ERR invalid expire time in 'getex' command"}

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

  def handle_ast({:set, key, _value}, _store) when byte_size(key) > @max_key_bytes,
    do: {:error, "ERR key too large"}

  def handle_ast({:set, key, _value, _opts}, _store) when byte_size(key) > @max_key_bytes,
    do: {:error, "ERR key too large"}

  def handle_ast({:set, key, value}, store),
    do: do_set_parsed(key, value, @set_opts_default, store)

  def handle_ast({:set, _key, _value, {:error, reason}}, _store) when is_binary(reason),
    do: {:error, reason}

  def handle_ast({:set, key, value, opts}, store) when is_list(opts) do
    with {:ok, parsed} <- SetOptions.from_ast(opts, @set_opts_default) do
      do_set_parsed(key, value, parsed, store)
    end
  end

  def handle_ast(_ast, _store), do: {:error, "ERR wrong number of arguments for 'set' command"}

  defp get_key("", _store), do: {:error, "ERR empty key"}

  defp get_key(key, _store) when byte_size(key) > @max_key_bytes,
    do: {:error, "ERR key too large"}

  defp get_key(key, store) do
    case read_string_value(key, store) do
      {:value, value} -> value
      :missing -> nil
      @wrongtype_error -> @wrongtype_error
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec get_bounded(binary(), term(), non_neg_integer() | :unlimited) ::
          {:ok, binary() | nil} | {:error, binary() | :response_byte_limit}
  def get_bounded("", _store, _limit), do: {:error, "ERR empty key"}

  def get_bounded(key, _store, _limit) when byte_size(key) > @max_key_bytes,
    do: {:error, "ERR key too large"}

  def get_bounded(key, store, limit) do
    case Ops.get_bounded(store, key, limit) do
      {:ok, nil} ->
        case Compound.data_structure_status(key, store) do
          :compound ->
            @wrongtype_error

          :plain ->
            {:ok, nil}

          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)
        end

      {:ok, value} ->
        {:ok, value}

      {:error, :response_byte_limit} = error ->
        error

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp del_keys(keys, store) do
    keys
    |> Enum.reduce_while({:ok, 0}, fn key, {:ok, acc} ->
      case Delete.do_del_key(key, store) do
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
    keys
    |> Enum.reduce_while({:ok, 0}, fn key, {:ok, count} ->
      cond do
        Ops.exists?(store, key) ->
          {:cont, {:ok, count + 1}}

        not Ops.has_compound?(store) ->
          {:cont, {:ok, count}}

        true ->
          case TypeRegistry.get_type(key, store) do
            {:error, {:storage_read_failed, _reason}} = failure -> {:halt, failure}
            "none" -> {:cont, {:ok, count}}
            _type -> {:cont, {:ok, count + 1}}
          end
      end
    end)
    |> case do
      {:ok, count} -> count
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
    end
  end

  defp mget_keys(keys, store) do
    values = Ops.batch_get(store, keys)

    case ReadResult.first_failure(values) do
      nil -> values
      failure -> ReadResult.command_error(failure)
    end
  end

  defp mset_args(args, store), do: MSet.mset_args(args, store)

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

      {:error, _reason} = error ->
        error
    end
  end

  defp strlen_key(key, store) do
    case read_string_size(key, store) do
      :missing -> 0
      @wrongtype_error -> @wrongtype_error
      {:error, _reason} = error -> error
      {:size, size} -> size
    end
  end

  defp getset_value(key, value, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.getset(store, key, value)
      @wrongtype_error -> @wrongtype_error
      {:error, _reason} = error -> error
    end
  end

  defp getdel_key(key, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.getdel(store, key)
      @wrongtype_error -> @wrongtype_error
      {:error, _reason} = error -> error
    end
  end

  defp setnx_value(key, value, store) do
    opts = %{expire_at_ms: 0, nx: true, xx: false, get: false, keepttl: false}

    case Compound.data_structure_status(key, store) do
      :compound ->
        0

      :plain ->
        case Ops.set(store, key, value, opts) do
          :ok -> 1
          nil -> 0
          {:error, _} = err -> err
        end

      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)
    end
  end

  defp msetnx_args(args, store), do: MSet.msetnx_args(args, store)

  # ---------------------------------------------------------------------------
  # Private — GETEX option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_getex(key, opts, store), do: GetEx.do_getex(key, opts, store)

  defp getex_with_expiry(key, value, multiplier, mode, store) do
    case expiry_time(value, multiplier, mode) do
      {:ok, expire_at_ms} -> GetEx.getex_parsed(key, expire_at_ms, store)
      :error -> integer_range_error()
    end
  end

  defp incr_string_key(key, delta, store) do
    case ensure_string_key(key, store) do
      :ok -> Ops.incr(store, key, delta)
      @wrongtype_error -> @wrongtype_error
      {:error, _reason} = error -> error
    end
  end

  defp incr_string_key_float(key, delta, store) do
    incr_result =
      case ensure_string_key(key, store) do
        :ok -> Ops.incr_float(store, key, delta)
        @wrongtype_error -> @wrongtype_error
        {:error, _reason} = error -> error
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

  # ---------------------------------------------------------------------------
  # Private — GETRANGE substring extraction
  # ---------------------------------------------------------------------------

  defp getrange_parsed(key, start_idx, end_idx, store),
    do: Range.getrange_parsed(key, start_idx, end_idx, store)

  # ---------------------------------------------------------------------------
  # Private — float argument parsing
  # ---------------------------------------------------------------------------

  defp parse_float_arg(str) do
    case Float.parse(str) do
      {val, ""} when is_float(val) -> {:ok, val}
      _ -> :error
    end
  rescue
    ArgumentError -> :error
  end

  # ---------------------------------------------------------------------------
  # Private — SET option parsing and execution
  # ---------------------------------------------------------------------------

  defp do_set(key, value, [], store), do: do_set_parsed(key, value, @set_opts_default, store)

  defp do_set(key, value, opts, store) do
    with {:ok, parsed} <- SetOptions.parse(opts, @set_opts_default) do
      do_set_parsed(key, value, parsed, store)
    end
  end

  defp setex_parsed(key, secs, value, store) do
    case ExpiryTime.relative(secs, 1_000) do
      {:ok, expire_at_ms} -> replace_string_key(key, value, expire_at_ms, store)
      :error -> integer_range_error()
    end
  end

  defp psetex_parsed(key, ms, value, store) do
    case ExpiryTime.relative(ms, 1) do
      {:ok, expire_at_ms} -> replace_string_key(key, value, expire_at_ms, store)
      :error -> integer_range_error()
    end
  end

  defp expiry_time(value, multiplier, :relative), do: ExpiryTime.relative(value, multiplier)
  defp expiry_time(value, multiplier, :absolute), do: ExpiryTime.absolute(value, multiplier)

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}

  defp setrange_parsed(key, offset, value, store) do
    case ensure_string_key(key, store) do
      :ok ->
        case Ops.setrange(store, key, offset, value) do
          {:ok, new_len} -> new_len
          {:error, _} = err -> err
        end

      @wrongtype_error ->
        @wrongtype_error

      {:error, _reason} = error ->
        error
    end
  end

  defp do_set_parsed(key, value, parsed, store) do
    case Compound.data_structure_status(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      :compound when parsed.get ->
        @wrongtype_error

      :compound when parsed.nx ->
        nil

      :compound ->
        replace_string_key(key, value, parsed.expire_at_ms, store)

      :plain when parsed.nx or parsed.xx or parsed.get or parsed.keepttl ->
        Ops.set(store, key, value, parsed)

      :plain ->
        replace_string_key(key, value, parsed.expire_at_ms, store)
    end
  end

  # ---------------------------------------------------------------------------
  # Private — MSET/MSETNX helpers (direct recursion, no chunked enumeration)
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Private — type checking for GET
  # ---------------------------------------------------------------------------

  defp read_string_value(key, store) do
    case Ops.get(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        case Compound.data_structure_status(key, store) do
          :compound ->
            @wrongtype_error

          :plain ->
            :missing

          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)
        end

      value when is_binary(value) ->
        {:value, value}

      other ->
        {:value, other}
    end
  end

  def ensure_string_key(key, store) do
    Compound.ensure_string_key(key, store)
  end

  defp read_string_size(key, store) do
    case Ops.value_size(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        case Compound.data_structure_status(key, store) do
          :compound ->
            @wrongtype_error

          :plain ->
            :missing

          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)
        end

      size ->
        {:size, size}
    end
  end

  def replace_string_key(key, value, expire_at_ms, store) do
    Compound.replace_string_key(key, value, expire_at_ms, store)
  end
end
