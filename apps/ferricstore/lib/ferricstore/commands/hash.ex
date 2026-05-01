defmodule Ferricstore.Commands.Hash do
  @moduledoc """
  Handles Redis hash commands: HSET, HGET, HDEL, HMGET, HGETALL, HLEN,
  HEXISTS, HKEYS, HVALS, HSETNX, HINCRBY, HINCRBYFLOAT, HEXPIRE, HTTL,
  HPERSIST, HSCAN, HRANDFIELD.

  Each hash field is stored as an individual compound key entry in the
  shared shard Bitcask:

      H:redis_key\\0field_name -> value

  This allows individual field access without reading or deserializing
  the entire hash. HGETALL scans all entries matching the hash prefix.

  ## Hash Field TTL (Redis 7.4+)

  Individual hash fields can have per-field expiry via `HEXPIRE`, `HTTL`,
  and `HPERSIST`. The expiry is stored as the `expire_at_ms` timestamp on
  each compound key entry.

  ## Type Enforcement

  All hash commands check the type metadata for the key. If the key
  already exists as a different data type (list, set, zset), a
  WRONGTYPE error is returned.
  """

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops
  alias Ferricstore.Store.TypeRegistry

  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @overflow_error "ERR increment or decrement would overflow"

  @doc """
  Handles a hash command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"HSET"`, `"HGET"`)
    - `args` - List of string arguments
    - `store` - Injected store map with compound key callbacks

  ## Returns

  Plain Elixir term: integer, string, list, nil, or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  # ---------------------------------------------------------------------------
  # HSET key field value [field value ...]
  # ---------------------------------------------------------------------------

  def handle("HSET", [key, _f, _v | _] = args, store) do
    [_ | field_value_pairs] = args

    if even_length?(field_value_pairs) do
      with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
        hset_pairs(field_value_pairs, key, store, 0)
      end
    else
      {:error, "ERR wrong number of arguments for 'hset' command"}
    end
  end

  def handle("HSET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hset' command"}
  end

  # ---------------------------------------------------------------------------
  # HGET key field
  # ---------------------------------------------------------------------------

  def handle("HGET", [key, field], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)
      Ops.compound_get(store, key, compound_key)
    end
  end

  def handle("HGET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hget' command"}
  end

  # ---------------------------------------------------------------------------
  # HDEL key field [field ...]
  # ---------------------------------------------------------------------------

  def handle("HDEL", [key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      deleted =
        Enum.reduce(fields, 0, fn field, acc ->
          compound_key = CompoundKey.hash_field(key, field)

          if Ops.compound_get(store, key, compound_key) != nil do
            Ops.compound_delete(store, key, compound_key)
            acc + 1
          else
            acc
          end
        end)

      maybe_cleanup_empty_hash(key, deleted, store)
      deleted
    end
  end

  def handle("HDEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hdel' command"}
  end

  # ---------------------------------------------------------------------------
  # HMGET key field [field ...]
  # ---------------------------------------------------------------------------

  def handle("HMGET", [key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_keys = Enum.map(fields, &CompoundKey.hash_field(key, &1))
      Ops.compound_batch_get(store, key, compound_keys)
    end
  end

  def handle("HMGET", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hmget' command"}
  end

  # ---------------------------------------------------------------------------
  # HGETALL key
  # ---------------------------------------------------------------------------

  def handle("HGETALL", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)

      # Return flat list [field1, value1, field2, value2, ...]
      Enum.flat_map(pairs, fn {field, value} -> [field, value] end)
    end
  end

  def handle("HGETALL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hgetall' command"}
  end

  # ---------------------------------------------------------------------------
  # HLEN key
  # ---------------------------------------------------------------------------

  def handle("HLEN", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      Ops.compound_count(store, key, prefix)
    end
  end

  def handle("HLEN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hlen' command"}
  end

  # ---------------------------------------------------------------------------
  # HEXISTS key field
  # ---------------------------------------------------------------------------

  def handle("HEXISTS", [key, field], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      if Ops.compound_get(store, key, compound_key) != nil do
        1
      else
        0
      end
    end
  end

  def handle("HEXISTS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hexists' command"}
  end

  # ---------------------------------------------------------------------------
  # HKEYS key
  # ---------------------------------------------------------------------------

  def handle("HKEYS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      Enum.map(pairs, fn {field, _value} -> field end)
    end
  end

  def handle("HKEYS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hkeys' command"}
  end

  # ---------------------------------------------------------------------------
  # HVALS key
  # ---------------------------------------------------------------------------

  def handle("HVALS", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      Enum.map(pairs, fn {_field, value} -> value end)
    end
  end

  def handle("HVALS", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hvals' command"}
  end

  # ---------------------------------------------------------------------------
  # HSETNX key field value
  # ---------------------------------------------------------------------------

  def handle("HSETNX", [key, field, value], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      if Ops.compound_get(store, key, compound_key) != nil do
        0
      else
        Ops.compound_put(store, key, compound_key, value, 0)
        1
      end
    end
  end

  def handle("HSETNX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hsetnx' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBY key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBY", [key, field, increment_str], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store),
         {increment, ""} <- Integer.parse(increment_str) do
      do_hincrby(key, field, increment, store)
    else
      {:error, _} = err -> err
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("HINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBYFLOAT key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBYFLOAT", [key, field, increment_str], store) do
    with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
      case Float.parse(increment_str) do
        {increment, ""} ->
          compound_key = CompoundKey.hash_field(key, field)
          current = Ops.compound_get(store, key, compound_key)

          case parse_float_value(current) do
            {:ok, current_float} ->
              new_val = current_float + increment
              result_str = format_float(new_val)
              Ops.compound_put(store, key, compound_key, result_str, 0)
              result_str

            :error ->
              {:error, "ERR hash value is not a valid float"}
          end

        :error ->
          {:error, "ERR value is not a valid float"}
      end
    end
  end

  def handle("HINCRBYFLOAT", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hincrbyfloat' command"}
  end

  # ---------------------------------------------------------------------------
  # HEXPIRE key seconds FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Sets a TTL (in seconds) on individual hash fields.
  # Returns: 1 = expiry set, -2 = field/key does not exist.
  def handle("HEXPIRE", [key, seconds_str, "FIELDS", count_str | fields], store) do
    with {:ok, seconds} <- parse_positive_integer(seconds_str, "seconds"),
         {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        expire_at_ms = CommandTime.now_ms() + seconds * 1000

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {value, _old_expire} ->
              Ops.compound_put(store, key, compound_key, value, expire_at_ms)
              1
          end
        end)
      end
    end
  end

  def handle("HEXPIRE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hexpire' command"}
  end

  # ---------------------------------------------------------------------------
  # HTTL key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Returns remaining TTL (seconds) for individual hash fields.
  # Returns: TTL >= 0, -1 = no expiry, -2 = field/key does not exist.
  def handle("HTTL", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        now = CommandTime.now_ms()

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {_value, expire_at_ms} ->
              remaining_ms = expire_at_ms - now
              if remaining_ms > 0, do: div(remaining_ms, 1000), else: -2
          end
        end)
      end
    end
  end

  def handle("HTTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'httl' command"}
  end

  # ---------------------------------------------------------------------------
  # HPERSIST key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Removes expiry from individual hash fields, making them persistent.
  # Returns: 1 = expiry removed, -1 = no expiry set, -2 = field/key does not exist.
  def handle("HPERSIST", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {value, _expire_at_ms} ->
              Ops.compound_put(store, key, compound_key, value, 0)
              1
          end
        end)
      end
    end
  end

  def handle("HPERSIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hpersist' command"}
  end

  # ---------------------------------------------------------------------------
  # HPEXPIRE key milliseconds FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Sets a TTL (in milliseconds) on individual hash fields.
  # Returns: 1 = expiry set, -2 = field/key does not exist.
  def handle("HPEXPIRE", [key, ms_str, "FIELDS", count_str | fields], store) do
    with {:ok, ms} <- parse_positive_integer(ms_str, "milliseconds"),
         {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        expire_at_ms = CommandTime.now_ms() + ms

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {value, _old_expire} ->
              Ops.compound_put(store, key, compound_key, value, expire_at_ms)
              1
          end
        end)
      end
    end
  end

  def handle("HPEXPIRE", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hpexpire' command"}
  end

  # ---------------------------------------------------------------------------
  # HPTTL key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Returns remaining TTL (milliseconds) for individual hash fields.
  # Returns: TTL >= 0, -1 = no expiry, -2 = field/key does not exist.
  def handle("HPTTL", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        now = CommandTime.now_ms()

        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {_value, expire_at_ms} ->
              remaining_ms = expire_at_ms - now
              if remaining_ms > 0, do: remaining_ms, else: -2
          end
        end)
      end
    end
  end

  def handle("HPTTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hpttl' command"}
  end

  # ---------------------------------------------------------------------------
  # HEXPIRETIME key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Returns the absolute Unix timestamp (seconds) at which each field expires.
  # Returns: timestamp >= 0, -1 = no expiry, -2 = field/key does not exist.
  def handle("HEXPIRETIME", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              -2

            {_value, 0} ->
              -1

            {_value, expire_at_ms} ->
              div(expire_at_ms, 1000)
          end
        end)
      end
    end
  end

  def handle("HEXPIRETIME", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hexpiretime' command"}
  end

  # ---------------------------------------------------------------------------
  # HGETDEL key FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Atomically gets the values of the specified fields and deletes them.
  # Returns a list of values (nil for missing fields).
  def handle("HGETDEL", [key, "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        results =
          Enum.map(fields, fn field ->
            compound_key = CompoundKey.hash_field(key, field)

            case Ops.compound_get(store, key, compound_key) do
              nil ->
                nil

              value ->
                Ops.compound_delete(store, key, compound_key)
                value
            end
          end)

        # Clean up type metadata if hash is now empty
        deleted_count = Enum.count(results, &(&1 != nil))
        maybe_cleanup_empty_hash(key, deleted_count, store)

        results
      end
    end
  end

  def handle("HGETDEL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hgetdel' command"}
  end

  # ---------------------------------------------------------------------------
  # HGETEX key [PERSIST|EX sec|PX ms|EXAT ts|PXAT ms_ts] FIELDS count field [field ...]
  # ---------------------------------------------------------------------------

  # Gets the values of the specified fields and optionally modifies their expiry.
  def handle("HGETEX", [key, mode | rest], store) when mode in ~w(EX PX EXAT PXAT) do
    case rest do
      [value_str, "FIELDS", count_str | fields] ->
        with {:ok, expire_at_ms} <- parse_expiry_mode(mode, value_str),
             {:ok, count} <- parse_positive_integer(count_str, "count"),
             :ok <- validate_field_count(count, fields) do
          with :ok <- TypeRegistry.check_type(key, :hash, store) do
            Enum.map(fields, fn field ->
              compound_key = CompoundKey.hash_field(key, field)

              case Ops.compound_get_meta(store, key, compound_key) do
                nil ->
                  nil

                {value, _old_expire} ->
                  Ops.compound_put(store, key, compound_key, value, expire_at_ms)
                  value
              end
            end)
          end
        end

      _ ->
        {:error, "ERR wrong number of arguments for 'hgetex' command"}
    end
  end

  def handle("HGETEX", [key, "PERSIST", "FIELDS", count_str | fields], store) do
    with {:ok, count} <- parse_positive_integer(count_str, "count"),
         :ok <- validate_field_count(count, fields) do
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        Enum.map(fields, fn field ->
          compound_key = CompoundKey.hash_field(key, field)

          case Ops.compound_get_meta(store, key, compound_key) do
            nil ->
              nil

            {value, _old_expire} ->
              Ops.compound_put(store, key, compound_key, value, 0)
              value
          end
        end)
      end
    end
  end

  def handle("HGETEX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hgetex' command"}
  end

  # ---------------------------------------------------------------------------
  # HSETEX key seconds field value [field value ...]
  # ---------------------------------------------------------------------------

  # Sets field-value pairs in a hash with a per-field TTL in seconds.
  # Returns the number of NEW fields added (not updated).
  def handle("HSETEX", [key, seconds_str, _f, _v | _] = args, store) do
    [_, _ | field_value_pairs] = args

    with {:ok, seconds} <- parse_positive_integer(seconds_str, "seconds") do
      if even_length?(field_value_pairs) do
        with :ok <- TypeRegistry.check_or_set(key, :hash, store) do
          expire_at_ms = CommandTime.now_ms() + seconds * 1000
          hset_pairs_with_ttl(field_value_pairs, key, store, expire_at_ms, 0)
        end
      else
        {:error, "ERR wrong number of arguments for 'hsetex' command"}
      end
    end
  end

  def handle("HSETEX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hsetex' command"}
  end

  # ---------------------------------------------------------------------------
  # HSTRLEN key field
  # ---------------------------------------------------------------------------

  def handle("HSTRLEN", [key, field], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      case Ops.compound_get(store, key, compound_key) do
        nil -> 0
        value -> byte_size(value)
      end
    end
  end

  def handle("HSTRLEN", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hstrlen' command"}
  end

  # ---------------------------------------------------------------------------
  # HSCAN key cursor [MATCH pattern] [COUNT count]
  # ---------------------------------------------------------------------------

  def handle("HSCAN", [key, cursor_str | opts], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store),
         {:ok, cursor} <- parse_cursor(cursor_str),
         {:ok, match_pattern, count} <- parse_hscan_opts(opts) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)

      filtered =
        case match_pattern do
          nil ->
            pairs

          pattern ->
            Enum.filter(pairs, fn {field, _value} ->
              Ferricstore.GlobMatcher.match?(field, pattern)
            end)
        end

      {next_cursor, batch} = paginate(filtered, cursor, count)
      elements = Enum.flat_map(batch, fn {field, value} -> [field, value] end)
      [next_cursor, elements]
    end
  end

  def handle("HSCAN", [_key], _store) do
    {:error, "ERR wrong number of arguments for 'hscan' command"}
  end

  def handle("HSCAN", [], _store) do
    {:error, "ERR wrong number of arguments for 'hscan' command"}
  end

  # ---------------------------------------------------------------------------
  # HRANDFIELD key [count [WITHVALUES]]
  # ---------------------------------------------------------------------------

  def handle("HRANDFIELD", [key], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)

      case pairs do
        [] ->
          nil

        _ ->
          {field, _value} = Enum.random(pairs)
          field
      end
    end
  end

  def handle("HRANDFIELD", [key, count_str], store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      case Integer.parse(count_str) do
        {count, ""} ->
          prefix = CompoundKey.hash_prefix(key)
          pairs = Ops.compound_scan(store, key, prefix)
          select_random_fields(pairs, count, false)

        _ ->
          {:error, "ERR value is not an integer or out of range"}
      end
    end
  end

  def handle("HRANDFIELD", [key, count_str, withvalues_str], store) do
    if String.upcase(withvalues_str) != "WITHVALUES" do
      {:error, "ERR syntax error"}
    else
      with :ok <- TypeRegistry.check_type(key, :hash, store) do
        case Integer.parse(count_str) do
          {count, ""} ->
            prefix = CompoundKey.hash_prefix(key)
            pairs = Ops.compound_scan(store, key, prefix)
            select_random_fields(pairs, count, true)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end
      end
    end
  end

  def handle("HRANDFIELD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hrandfield' command"}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp do_hincrby(key, field, increment, store) do
    compound_key = CompoundKey.hash_field(key, field)
    current = Ops.compound_get(store, key, compound_key)

    case parse_integer_value(current) do
      {:ok, current_int} ->
        case checked_integer_add(current_int, increment) do
          {:ok, new_val} ->
            Ops.compound_put(store, key, compound_key, Integer.to_string(new_val), 0)
            new_val

          :overflow ->
            {:error, @overflow_error}
        end

      :error ->
        {:error, "ERR hash value is not an integer"}
    end
  end

  defp maybe_cleanup_empty_hash(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_hash(key, _deleted, store) do
    prefix = CompoundKey.hash_prefix(key)

    if Ops.compound_count(store, key, prefix) == 0 do
      TypeRegistry.delete_type(key, store)
    end
  end

  # Walk flat [field, value, field, value, ...] list directly without
  # Enum.chunk_every intermediate allocation.
  defp hset_pairs([], _key, _store, acc), do: acc

  defp hset_pairs([field, value | rest], key, store, acc) do
    compound_key = CompoundKey.hash_field(key, field)
    existing = Ops.compound_get(store, key, compound_key)
    Ops.compound_put(store, key, compound_key, value, 0)
    new_acc = if existing == nil, do: acc + 1, else: acc
    hset_pairs(rest, key, store, new_acc)
  end

  # Same as hset_pairs but with per-field TTL.
  defp hset_pairs_with_ttl([], _key, _store, _expire_at_ms, acc), do: acc

  defp hset_pairs_with_ttl([field, value | rest], key, store, expire_at_ms, acc) do
    compound_key = CompoundKey.hash_field(key, field)
    existing = Ops.compound_get(store, key, compound_key)
    Ops.compound_put(store, key, compound_key, value, expire_at_ms)
    new_acc = if existing == nil, do: acc + 1, else: acc
    hset_pairs_with_ttl(rest, key, store, expire_at_ms, new_acc)
  end

  # O(n/2) parity check without computing full length.
  defp even_length?([]), do: true
  defp even_length?([_, _ | rest]), do: even_length?(rest)
  defp even_length?(_), do: false

  defp parse_integer_value(nil), do: {:ok, 0}

  defp parse_integer_value(str) when is_binary(str) do
    case Integer.parse(str) do
      {int, ""} -> {:ok, int}
      _ -> :error
    end
  end

  defp checked_integer_add(value, increment) do
    result = value + increment

    if result > @max_int64 or result < @min_int64 do
      :overflow
    else
      {:ok, result}
    end
  end

  defp parse_float_value(nil), do: {:ok, 0.0}

  defp parse_float_value(str) when is_binary(str) do
    case Float.parse(str) do
      {float, ""} ->
        {:ok, float}

      _ ->
        case Integer.parse(str) do
          {int, ""} -> {:ok, int * 1.0}
          _ -> :error
        end
    end
  end

  defp format_float(val) when is_float(val) do
    :erlang.float_to_binary(val, [:compact, decimals: 17])
  end

  defp parse_positive_integer(str, label) do
    case Integer.parse(str) do
      {int, ""} when int > 0 -> {:ok, int}
      _ -> {:error, "ERR #{label} is not a positive integer"}
    end
  end

  # Parses expiry mode + value to an absolute expire_at_ms timestamp.
  defp parse_expiry_mode("EX", value_str) do
    case Integer.parse(value_str) do
      {seconds, ""} when seconds > 0 ->
        {:ok, CommandTime.now_ms() + seconds * 1000}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_expiry_mode("PX", value_str) do
    case Integer.parse(value_str) do
      {ms, ""} when ms > 0 ->
        {:ok, CommandTime.now_ms() + ms}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_expiry_mode("EXAT", value_str) do
    case Integer.parse(value_str) do
      {ts, ""} when ts > 0 ->
        {:ok, ts * 1000}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_expiry_mode("PXAT", value_str) do
    case Integer.parse(value_str) do
      {ts_ms, ""} when ts_ms > 0 ->
        {:ok, ts_ms}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp validate_field_count(0, []), do: :ok
  defp validate_field_count(n, [_ | rest]) when n > 0, do: validate_field_count(n - 1, rest)

  defp validate_field_count(_, _),
    do: {:error, "ERR number of fields does not match the count argument"}

  # ---------------------------------------------------------------------------
  # HSCAN helpers
  # ---------------------------------------------------------------------------

  defp parse_cursor(cursor_str) do
    case Integer.parse(cursor_str) do
      {cursor, ""} when cursor >= 0 -> {:ok, cursor}
      _ -> {:error, "ERR invalid cursor"}
    end
  end

  defp parse_hscan_opts(opts), do: do_parse_hscan_opts(opts, nil, 10)

  defp do_parse_hscan_opts([], match, count), do: {:ok, match, count}

  defp do_parse_hscan_opts([opt, value | rest], match, count) do
    case String.upcase(opt) do
      "MATCH" ->
        do_parse_hscan_opts(rest, value, count)

      "COUNT" ->
        case Integer.parse(value) do
          {n, ""} when n > 0 -> do_parse_hscan_opts(rest, match, n)
          _ -> {:error, "ERR value is not an integer or out of range"}
        end

      _ ->
        {:error, "ERR syntax error"}
    end
  end

  defp do_parse_hscan_opts([_ | _], _match, _count) do
    {:error, "ERR syntax error"}
  end

  defp paginate(items, cursor, count) do
    rest = Enum.drop(items, cursor)

    case rest do
      [] ->
        {"0", []}

      _ ->
        {batch, remainder} = Enum.split(rest, count)

        case remainder do
          [] -> {"0", batch}
          _ -> {Integer.to_string(cursor + length(batch)), batch}
        end
    end
  end

  defp select_random_fields(pairs, count, with_values) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_values do
          Enum.flat_map(selected, fn {field, value} -> [field, value] end)
        else
          Enum.map(selected, fn {field, _value} -> field end)
        end

      count < 0 ->
        abs_count = abs(count)

        if pairs == [] do
          []
        else
          # Convert to tuple for O(1) random access instead of O(n) Enum.random on list
          tuple = List.to_tuple(pairs)
          size = tuple_size(tuple)
          selected = for _ <- 1..abs_count, do: elem(tuple, :rand.uniform(size) - 1)

          if with_values do
            Enum.flat_map(selected, fn {field, value} -> [field, value] end)
          else
            Enum.map(selected, fn {field, _value} -> field end)
          end
        end
    end
  end
end
