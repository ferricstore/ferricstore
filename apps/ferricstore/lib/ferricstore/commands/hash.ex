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
      hset_fields(key, field_value_pairs, store)
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

  def handle("HDEL", [key | fields], store) when fields != [],
    do: hdel_args([key | fields], store)

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
      hash_pairs_to_flat_list(pairs)
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
    hsetnx_field(key, field, value, store)
  end

  def handle("HSETNX", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hsetnx' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBY key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBY", [key, field, increment_str], store) do
    with {:ok, increment} <- parse_hincrby_increment(increment_str),
         type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      do_hincrby(key, field, increment, store, type_status)
    end
  end

  def handle("HINCRBY", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hincrby' command"}
  end

  # ---------------------------------------------------------------------------
  # HINCRBYFLOAT key field increment
  # ---------------------------------------------------------------------------

  def handle("HINCRBYFLOAT", [key, field, increment_str], store) do
    with {:ok, increment} <- parse_hincrbyfloat_increment(increment_str),
         type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      hincrbyfloat_field(key, field, increment, store, type_status)
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

        {unique_fields, compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        entries =
          existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

        case Ops.compound_batch_put(store, key, entries) do
          :ok ->
            Enum.map(fields, fn field ->
              case Map.fetch!(metas_by_field, field) do
                nil ->
                  -2

                {_value, _old_expire} ->
                  1
              end
            end)

          {:error, _} = err ->
            err
        end
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

        {_unique_fields, _compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        Enum.map(fields, fn field ->
          case Map.fetch!(metas_by_field, field) do
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
        {unique_fields, compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        entries = persistent_hash_field_entries(unique_fields, compound_keys, metas_by_field, [])

        case Ops.compound_batch_put(store, key, entries) do
          :ok ->
            Enum.map(fields, fn field ->
              case Map.fetch!(metas_by_field, field) do
                nil ->
                  -2

                {_value, 0} ->
                  -1

                {_value, _expire_at_ms} ->
                  1
              end
            end)

          {:error, _} = err ->
            err
        end
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

        {unique_fields, compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        entries =
          existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

        case Ops.compound_batch_put(store, key, entries) do
          :ok ->
            Enum.map(fields, fn field ->
              case Map.fetch!(metas_by_field, field) do
                nil ->
                  -2

                {_value, _old_expire} ->
                  1
              end
            end)

          {:error, _} = err ->
            err
        end
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

        {_unique_fields, _compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        Enum.map(fields, fn field ->
          case Map.fetch!(metas_by_field, field) do
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
        {_unique_fields, _compound_keys, metas_by_field} =
          batch_hash_field_metas(fields, key, store)

        Enum.map(fields, fn field ->
          case Map.fetch!(metas_by_field, field) do
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
      hgetdel_fields(key, fields, store)
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
            hgetex_fields(fields, key, store, expire_at_ms)
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
        hgetex_fields(fields, key, store, 0)
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
        expire_at_ms = CommandTime.now_ms() + seconds * 1000
        hset_fields_with_ttl(key, field_value_pairs, store, expire_at_ms)
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
      elements = hash_pairs_to_flat_list(batch)
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
          select_random_hash_fields(key, count, false, store)

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
            select_random_hash_fields(key, count, true, store)

          _ ->
            {:error, "ERR value is not an integer or out of range"}
        end
      end
    end
  end

  def handle("HRANDFIELD", _args, _store) do
    {:error, "ERR wrong number of arguments for 'hrandfield' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:hset, args}, store), do: hset_args(args, store)
  def handle_ast({:hdel, args}, store), do: hdel_args(args, store)
  def handle_ast({:hmget, args}, store), do: hmget_args(args, store)

  def handle_ast({:hget, key, field}, store), do: hget_field(key, field, store)
  def handle_ast({:hgetall, key}, store), do: hgetall_key(key, store)
  def handle_ast({:hexists, key, field}, store), do: hexists_field(key, field, store)
  def handle_ast({:hkeys, key}, store), do: hkeys_key(key, store)
  def handle_ast({:hvals, key}, store), do: hvals_key(key, store)
  def handle_ast({:hlen, key}, store), do: hlen_key(key, store)

  def handle_ast({:hsetnx, key, field, value}, store),
    do: hsetnx_field(key, field, value, store)

  def handle_ast({:hstrlen, key, field}, store), do: hstrlen_field(key, field, store)
  def handle_ast({:hscan, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hscan, key, cursor, opts}, store), do: hscan_typed(key, cursor, opts, store)

  def handle_ast({:hincrby, _key, _field, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:hincrby, key, field, increment}, store) when is_integer(increment) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      do_hincrby(key, field, increment, store, type_status)
    end
  end

  def handle_ast({:hincrbyfloat, _key, _field, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:hincrbyfloat, key, field, increment}, store) when is_float(increment) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      hincrbyfloat_field(key, field, increment, store, type_status)
    end
  end

  def handle_ast({:hrandfield, key}, store), do: hrandfield_one(key, store)
  def handle_ast({:hrandfield, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:hrandfield, key, count}, store) when is_integer(count),
    do: hrandfield_parsed(key, count, false, store)

  def handle_ast({:hrandfield, _key, _count, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:hrandfield, key, count, :withvalues}, store) when is_integer(count),
    do: hrandfield_parsed(key, count, true, store)

  def handle_ast({:hexpire, _key, {:error, reason}, _fields}, _store), do: {:error, reason}
  def handle_ast({:hpexpire, _key, {:error, reason}, _fields}, _store), do: {:error, reason}
  def handle_ast({:hsetex, _key, {:error, reason}, _pairs}, _store), do: {:error, reason}
  def handle_ast({:hgetex, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hgetex, _key, {:error, reason}, _fields}, _store), do: {:error, reason}
  def handle_ast({:httl, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hpersist, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hpttl, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hexpiretime, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:hgetdel, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:hexpire, key, seconds, fields}, store)
      when is_integer(seconds) and is_list(fields) do
    hash_expire_fields(key, fields, CommandTime.now_ms() + seconds * 1000, store)
  end

  def handle_ast({:hpexpire, key, ms, fields}, store) when is_integer(ms) and is_list(fields) do
    hash_expire_fields(key, fields, CommandTime.now_ms() + ms, store)
  end

  def handle_ast({:httl, key, fields}, store) when is_list(fields),
    do: hash_ttl_fields(key, fields, :seconds, store)

  def handle_ast({:hpttl, key, fields}, store) when is_list(fields),
    do: hash_ttl_fields(key, fields, :milliseconds, store)

  def handle_ast({:hpersist, key, fields}, store) when is_list(fields),
    do: hash_persist_fields(key, fields, store)

  def handle_ast({:hexpiretime, key, fields}, store) when is_list(fields),
    do: hash_expiretime_fields(key, fields, store)

  def handle_ast({:hgetdel, key, fields}, store) when is_list(fields),
    do: hgetdel_fields(key, fields, store)

  def handle_ast({:hgetex, key, :persist, fields}, store) when is_list(fields),
    do: hgetex_parsed(key, fields, 0, store)

  def handle_ast({:hgetex, key, {:ex, seconds}, fields}, store)
      when is_integer(seconds) and is_list(fields),
      do: hgetex_parsed(key, fields, CommandTime.now_ms() + seconds * 1000, store)

  def handle_ast({:hgetex, key, {:px, ms}, fields}, store)
      when is_integer(ms) and is_list(fields),
      do: hgetex_parsed(key, fields, CommandTime.now_ms() + ms, store)

  def handle_ast({:hgetex, key, {:exat, ts}, fields}, store)
      when is_integer(ts) and is_list(fields),
      do: hgetex_parsed(key, fields, ts * 1000, store)

  def handle_ast({:hgetex, key, {:pxat, ts}, fields}, store)
      when is_integer(ts) and is_list(fields),
      do: hgetex_parsed(key, fields, ts, store)

  def handle_ast({:hsetex, key, seconds, field_value_pairs}, store)
      when is_integer(seconds) and is_list(field_value_pairs) do
    if even_length?(field_value_pairs) do
      expire_at_ms = CommandTime.now_ms() + seconds * 1000
      hset_fields_with_ttl(key, field_value_pairs, store, expire_at_ms)
    else
      {:error, "ERR wrong number of arguments for 'hsetex' command"}
    end
  end

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported hash command AST"}

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp hset_args([key, _f, _v | _] = args, store) do
    [_ | field_value_pairs] = args

    if even_length?(field_value_pairs) do
      hset_fields(key, field_value_pairs, store)
    else
      {:error, "ERR wrong number of arguments for 'hset' command"}
    end
  end

  defp hset_args(_args, _store), do: {:error, "ERR wrong number of arguments for 'hset' command"}

  defp hset_fields(key, field_value_pairs, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      hset_pairs(field_value_pairs, key, store, type_status)
    end
  end

  defp hset_fields_with_ttl(key, field_value_pairs, store, expire_at_ms) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      hset_pairs_with_ttl(field_value_pairs, key, store, expire_at_ms, type_status)
    end
  end

  defp hget_field(key, field, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)
      Ops.compound_get(store, key, compound_key)
    end
  end

  defp hdel_args([key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_keys =
        fields
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.hash_field(key, &1))

      metas = Ops.compound_batch_get_meta(store, key, compound_keys)
      deleted_entries = hash_deleted_entries(compound_keys, metas, [])

      deleted = length(deleted_entries)

      with :ok <- delete_hash_fields_and_cleanup(key, deleted_entries, deleted, store) do
        deleted
      end
    end
  end

  defp hdel_args(_args, _store), do: {:error, "ERR wrong number of arguments for 'hdel' command"}

  defp hash_deleted_entries([_compound_key | compound_keys], [nil | metas], acc) do
    hash_deleted_entries(compound_keys, metas, acc)
  end

  defp hash_deleted_entries([compound_key | compound_keys], [{value, expire_at_ms} | metas], acc) do
    hash_deleted_entries(compound_keys, metas, [{compound_key, value, expire_at_ms} | acc])
  end

  defp hash_deleted_entries([compound_key | _compound_keys], [], _acc) do
    raise KeyError, key: compound_key, term: %{}
  end

  defp hash_deleted_entries(_compound_keys, _metas, acc), do: Enum.reverse(acc)

  defp hmget_args([key | fields], store) when fields != [] do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_keys = Enum.map(fields, &CompoundKey.hash_field(key, &1))
      Ops.compound_batch_get(store, key, compound_keys)
    end
  end

  defp hmget_args(_args, _store),
    do: {:error, "ERR wrong number of arguments for 'hmget' command"}

  defp hgetall_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      hash_pairs_to_flat_list(pairs)
    end
  end

  defp hlen_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      Ops.compound_count(store, key, prefix)
    end
  end

  defp hexists_field(key, field, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)
      if Ops.compound_get(store, key, compound_key) != nil, do: 1, else: 0
    end
  end

  defp hkeys_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      Enum.map(pairs, fn {field, _value} -> field end)
    end
  end

  defp hvals_key(key, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      prefix = CompoundKey.hash_prefix(key)
      pairs = Ops.compound_scan(store, key, prefix)
      Enum.map(pairs, fn {_field, value} -> value end)
    end
  end

  defp hsetnx_field(key, field, value, store) do
    with type_status when type_status in [:ok, {:ok, :created}] <-
           TypeRegistry.check_or_set_status(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      if Ops.compound_get(store, key, compound_key) != nil do
        0
      else
        write_hash_field(store, key, compound_key, value, 1, type_status)
      end
    end
  end

  defp hstrlen_field(key, field, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_key = CompoundKey.hash_field(key, field)

      case Ops.compound_get(store, key, compound_key) do
        nil -> 0
        value -> byte_size(value)
      end
    end
  end

  defp hrandfield_one(key, store) do
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

  defp hscan_typed(key, cursor, opts, store) when is_integer(cursor) and cursor >= 0 do
    with :ok <- TypeRegistry.check_type(key, :hash, store),
         {:ok, match_pattern, count} <- typed_scan_opts(opts) do
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
      elements = hash_pairs_to_flat_list(batch)
      [next_cursor, elements]
    end
  end

  defp hscan_typed(_key, _cursor, _opts, _store), do: {:error, "ERR invalid cursor"}

  defp select_random_hash_fields(_key, 0, _with_values, _store), do: []

  defp select_random_hash_fields(key, count, with_values, store) do
    prefix = CompoundKey.hash_prefix(key)
    pairs = Ops.compound_scan(store, key, prefix)
    select_random_fields(pairs, count, with_values)
  end

  defp typed_scan_opts(opts), do: typed_scan_opts(opts, nil, 10)

  defp typed_scan_opts([], match_pattern, count), do: {:ok, match_pattern, count}

  defp typed_scan_opts([{:match, pattern} | rest], _match_pattern, count),
    do: typed_scan_opts(rest, pattern, count)

  defp typed_scan_opts([{:count, count} | rest], match_pattern, _count)
       when is_integer(count) and count > 0,
       do: typed_scan_opts(rest, match_pattern, count)

  defp typed_scan_opts(_opts, _match_pattern, _count), do: {:error, "ERR syntax error"}

  defp parse_hincrby_increment(increment_str) do
    case Integer.parse(increment_str) do
      {increment, ""} when increment >= @min_int64 and increment <= @max_int64 ->
        {:ok, increment}

      {_increment, ""} ->
        {:error, @overflow_error}

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp parse_hincrbyfloat_increment(increment_str) do
    case Float.parse(increment_str) do
      {increment, ""} -> {:ok, increment}
      :error -> {:error, "ERR value is not a valid float"}
    end
  end

  defp do_hincrby(key, field, increment, store, type_status) do
    compound_key = CompoundKey.hash_field(key, field)
    current = Ops.compound_get(store, key, compound_key)

    case parse_integer_value(current) do
      {:ok, current_int} ->
        case checked_integer_add(current_int, increment) do
          {:ok, new_val} ->
            write_hash_field(
              store,
              key,
              compound_key,
              Integer.to_string(new_val),
              new_val,
              type_status
            )

          :overflow ->
            {:error, @overflow_error}
        end

      :error ->
        {:error, "ERR hash value is not an integer"}
    end
  end

  defp hincrbyfloat_field(key, field, increment, store, type_status) do
    compound_key = CompoundKey.hash_field(key, field)
    current = Ops.compound_get(store, key, compound_key)

    case parse_float_value(current) do
      {:ok, current_float} ->
        new_val = current_float + increment
        result_str = format_float(new_val)
        write_hash_field(store, key, compound_key, result_str, result_str, type_status)

      :error ->
        {:error, "ERR hash value is not a valid float"}
    end
  end

  defp write_hash_field(store, key, compound_key, value, success, type_status) do
    case Ops.compound_put(store, key, compound_key, value, 0) do
      :ok -> success
      true -> success
      {:error, _reason} = error -> rollback_new_hash_type_marker(key, store, type_status, error)
      other -> {:error, other}
    end
  end

  defp hrandfield_parsed(key, count, with_values, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      select_random_hash_fields(key, count, with_values, store)
    end
  end

  defp hash_expire_fields(key, fields, expire_at_ms, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {unique_fields, compound_keys, metas_by_field} = batch_hash_field_metas(fields, key, store)

      entries =
        existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

      case Ops.compound_batch_put(store, key, entries) do
        :ok ->
          Enum.map(fields, fn field ->
            case Map.fetch!(metas_by_field, field) do
              nil -> -2
              {_value, _old_expire} -> 1
            end
          end)

        {:error, _} = err ->
          err
      end
    end
  end

  defp hash_ttl_fields(key, fields, unit, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      now = CommandTime.now_ms()

      {_unique_fields, _compound_keys, metas_by_field} =
        batch_hash_field_metas(fields, key, store)

      Enum.map(fields, fn field ->
        case Map.fetch!(metas_by_field, field) do
          nil ->
            -2

          {_value, 0} ->
            -1

          {_value, expire_at_ms} ->
            remaining_ms = expire_at_ms - now

            cond do
              remaining_ms <= 0 -> -2
              unit == :seconds -> div(remaining_ms, 1000)
              true -> remaining_ms
            end
        end
      end)
    end
  end

  defp hash_persist_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {unique_fields, compound_keys, metas_by_field} = batch_hash_field_metas(fields, key, store)

      entries = persistent_hash_field_entries(unique_fields, compound_keys, metas_by_field, [])

      case Ops.compound_batch_put(store, key, entries) do
        :ok ->
          Enum.map(fields, fn field ->
            case Map.fetch!(metas_by_field, field) do
              nil -> -2
              {_value, 0} -> -1
              {_value, _expire_at_ms} -> 1
            end
          end)

        {:error, _} = err ->
          err
      end
    end
  end

  defp hash_expiretime_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      {_unique_fields, _compound_keys, metas_by_field} =
        batch_hash_field_metas(fields, key, store)

      Enum.map(fields, fn field ->
        case Map.fetch!(metas_by_field, field) do
          nil -> -2
          {_value, 0} -> -1
          {_value, expire_at_ms} -> div(expire_at_ms, 1000)
        end
      end)
    end
  end

  defp hgetdel_fields(key, fields, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      compound_keys =
        fields
        |> Enum.uniq()
        |> Enum.map(&CompoundKey.hash_field(key, &1))

      metas = Ops.compound_batch_get_meta(store, key, compound_keys)
      metas_by_key = hash_metas_by_key(compound_keys, metas, %{})
      {results, deleted_entries} = hgetdel_results(fields, key, metas_by_key, [], %{}, [])

      deleted_count = length(deleted_entries)

      with :ok <- delete_hash_fields_and_cleanup(key, deleted_entries, deleted_count, store) do
        results
      end
    end
  end

  defp hash_metas_by_key([compound_key | compound_keys], [meta | metas], acc) do
    hash_metas_by_key(compound_keys, metas, Map.put(acc, compound_key, meta))
  end

  defp hash_metas_by_key(_compound_keys, _metas, acc), do: acc

  defp hgetdel_results([], _key, _metas_by_key, results, _deleted, deleted_entries) do
    {Enum.reverse(results), Enum.reverse(deleted_entries)}
  end

  defp hgetdel_results([field | fields], key, metas_by_key, results, deleted, deleted_entries) do
    compound_key = CompoundKey.hash_field(key, field)

    cond do
      Map.has_key?(deleted, compound_key) ->
        hgetdel_results(fields, key, metas_by_key, [nil | results], deleted, deleted_entries)

      is_nil(Map.get(metas_by_key, compound_key)) ->
        hgetdel_results(fields, key, metas_by_key, [nil | results], deleted, deleted_entries)

      true ->
        {value, expire_at_ms} = Map.fetch!(metas_by_key, compound_key)

        hgetdel_results(
          fields,
          key,
          metas_by_key,
          [value | results],
          Map.put(deleted, compound_key, true),
          [{compound_key, value, expire_at_ms} | deleted_entries]
        )
    end
  end

  defp hgetex_parsed(key, fields, expire_at_ms, store) do
    with :ok <- TypeRegistry.check_type(key, :hash, store) do
      hgetex_fields(fields, key, store, expire_at_ms)
    end
  end

  defp maybe_cleanup_empty_hash(_key, 0, _store), do: :ok

  defp maybe_cleanup_empty_hash(key, _deleted, store) do
    prefix = CompoundKey.hash_prefix(key)

    if Ops.compound_count(store, key, prefix) == 0 do
      TypeRegistry.delete_type(key, store)
    else
      :ok
    end
  end

  defp delete_hash_fields_and_cleanup(key, deleted_entries, deleted_count, store) do
    deleted_keys =
      Enum.map(deleted_entries, fn {compound_key, _value, _expire_at_ms} -> compound_key end)

    case Ops.compound_batch_delete(store, key, deleted_keys) do
      :ok ->
        case maybe_cleanup_empty_hash(key, deleted_count, store) do
          :ok -> :ok
          {:error, _} = error -> rollback_deleted_hash_fields(key, deleted_entries, store, error)
        end

      {:error, _} = err ->
        err
    end
  end

  defp rollback_deleted_hash_fields(_key, [], _store, write_error), do: write_error

  defp rollback_deleted_hash_fields(key, deleted_entries, store, write_error) do
    case Ops.compound_batch_put(store, key, deleted_entries) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:hash_delete_rollback_failed, write_error, rollback_error}}
    end
  end

  defp hset_pairs(field_value_pairs, key, store, type_status) do
    {fields, values_by_field} = collapse_field_values(field_value_pairs, [], %{})
    compound_keys = Enum.map(fields, &CompoundKey.hash_field(key, &1))
    existing_values = Ops.compound_batch_get(store, key, compound_keys)

    {added, entries} =
      hash_put_entries(fields, compound_keys, existing_values, values_by_field, 0)

    case Ops.compound_batch_put(store, key, entries) do
      :ok -> added
      {:error, _} = err -> rollback_new_hash_type_marker(key, store, type_status, err)
    end
  end

  defp rollback_new_hash_type_marker(key, store, {:ok, :created}, write_error) do
    case TypeRegistry.delete_type(key, store) do
      :ok ->
        write_error

      {:error, _} = rollback_error ->
        {:error, {:hash_type_marker_rollback_failed, write_error, rollback_error}}
    end
  end

  defp rollback_new_hash_type_marker(_key, _store, :ok, write_error), do: write_error

  defp collapse_field_values([], fields_rev, values_by_field) do
    {Enum.reverse(fields_rev), values_by_field}
  end

  defp collapse_field_values([field, value | rest], fields_rev, values_by_field) do
    next_fields_rev =
      if Map.has_key?(values_by_field, field) do
        fields_rev
      else
        [field | fields_rev]
      end

    collapse_field_values(rest, next_fields_rev, Map.put(values_by_field, field, value))
  end

  defp hgetex_fields(fields, key, store, expire_at_ms) do
    {unique_fields, compound_keys, metas_by_field} = batch_hash_field_metas(fields, key, store)

    entries =
      existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms)

    case Ops.compound_batch_put(store, key, entries) do
      :ok ->
        Enum.map(fields, fn field ->
          case Map.fetch!(metas_by_field, field) do
            nil -> nil
            {value, _old_expire} -> value
          end
        end)

      {:error, _} = err ->
        err
    end
  end

  defp batch_hash_field_metas(fields, key, store) do
    unique_fields = Enum.uniq(fields)
    compound_keys = Enum.map(unique_fields, &CompoundKey.hash_field(key, &1))

    metas = Ops.compound_batch_get_meta(store, key, compound_keys)
    metas_by_field = hash_field_metas_by_field(unique_fields, metas, %{})

    {unique_fields, compound_keys, metas_by_field}
  end

  defp hash_field_metas_by_field([field | fields], [meta | metas], acc) do
    hash_field_metas_by_field(fields, metas, Map.put(acc, field, meta))
  end

  defp hash_field_metas_by_field(_fields, _metas, acc), do: acc

  defp persistent_hash_field_entries(
         [field | fields],
         [compound_key | compound_keys],
         metas_by_field,
         acc
       ) do
    next_acc =
      case Map.fetch!(metas_by_field, field) do
        {value, expire_at_ms} when expire_at_ms != 0 -> [{compound_key, value, 0} | acc]
        _nil_or_persistent -> acc
      end

    persistent_hash_field_entries(fields, compound_keys, metas_by_field, next_acc)
  end

  defp persistent_hash_field_entries(_fields, _compound_keys, _metas_by_field, acc),
    do: Enum.reverse(acc)

  # Same as hset_pairs but with per-field TTL.
  defp hset_pairs_with_ttl(field_value_pairs, key, store, expire_at_ms, type_status) do
    {fields, values_by_field} = collapse_field_values(field_value_pairs, [], %{})
    compound_keys = Enum.map(fields, &CompoundKey.hash_field(key, &1))
    existing_values = Ops.compound_batch_get(store, key, compound_keys)

    {added, entries} =
      hash_put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms)

    case Ops.compound_batch_put(store, key, entries) do
      :ok -> added
      {:error, _} = err -> rollback_new_hash_type_marker(key, store, type_status, err)
    end
  end

  defp existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms) do
    existing_hash_field_entries(unique_fields, compound_keys, metas_by_field, expire_at_ms, [])
  end

  defp existing_hash_field_entries(
         [field | fields],
         [compound_key | compound_keys],
         metas_by_field,
         expire_at_ms,
         acc
       ) do
    next_acc =
      case Map.fetch!(metas_by_field, field) do
        {value, _old_expire} -> [{compound_key, value, expire_at_ms} | acc]
        nil -> acc
      end

    existing_hash_field_entries(fields, compound_keys, metas_by_field, expire_at_ms, next_acc)
  end

  defp existing_hash_field_entries(_fields, _compound_keys, _metas_by_field, _expire_at_ms, acc),
    do: Enum.reverse(acc)

  defp hash_put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms) do
    hash_put_entries(fields, compound_keys, existing_values, values_by_field, expire_at_ms, 0, [])
  end

  defp hash_put_entries([], [], _existing_values, _values_by_field, _expire_at_ms, added, entries) do
    {added, Enum.reverse(entries)}
  end

  defp hash_put_entries(
         [field | fields],
         [compound_key | compound_keys],
         [nil | existing_values],
         values_by_field,
         expire_at_ms,
         added,
         entries
       ) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    hash_put_entries(
      fields,
      compound_keys,
      existing_values,
      values_by_field,
      expire_at_ms,
      added + 1,
      [entry | entries]
    )
  end

  defp hash_put_entries(
         [field | fields],
         [compound_key | compound_keys],
         [_existing | existing_values],
         values_by_field,
         expire_at_ms,
         added,
         entries
       ) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    hash_put_entries(
      fields,
      compound_keys,
      existing_values,
      values_by_field,
      expire_at_ms,
      added,
      [entry | entries]
    )
  end

  defp hash_put_entries(
         [field | fields],
         [compound_key | compound_keys],
         [],
         values_by_field,
         expire_at_ms,
         added,
         entries
       ) do
    entry = {compound_key, Map.fetch!(values_by_field, field), expire_at_ms}

    hash_put_entries(fields, compound_keys, [], values_by_field, expire_at_ms, added, [
      entry | entries
    ])
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

  defp hash_pairs_to_flat_list(pairs), do: hash_pairs_to_flat_list(pairs, [])

  defp hash_pairs_to_flat_list([{field, value} | pairs], acc) do
    hash_pairs_to_flat_list(pairs, [value, field | acc])
  end

  defp hash_pairs_to_flat_list([], acc), do: Enum.reverse(acc)

  defp select_random_fields(pairs, count, with_values) do
    cond do
      count == 0 ->
        []

      count > 0 ->
        selected = Enum.take_random(pairs, count)

        if with_values do
          hash_pairs_to_flat_list(selected)
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
            hash_pairs_to_flat_list(selected)
          else
            Enum.map(selected, fn {field, _value} -> field end)
          end
        end
    end
  end
end
