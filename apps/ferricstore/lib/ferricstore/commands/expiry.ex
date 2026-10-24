defmodule Ferricstore.Commands.Expiry do
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.Commands.ExpiryTime
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.{Ops, ReadResult, TypeRegistry}
  alias Ferricstore.Transaction.SingleCommand

  @probabilistic_types ["bloom", "cms", "cuckoo", "topk"]

  @moduledoc """
  Handles Redis expiry commands: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles wire encoding.

  ## Supported commands

    * `EXPIRE key seconds` — set TTL in seconds, returns 1 on success / 0 if key missing
    * `PEXPIRE key milliseconds` — set TTL in milliseconds
    * `EXPIREAT key unix-timestamp` — set absolute expiry (seconds since epoch)
    * `PEXPIREAT key unix-timestamp-ms` — set absolute expiry (milliseconds since epoch)
    * `TTL key` — remaining TTL in seconds (-1 = no expiry, -2 = key missing)
    * `PTTL key` — remaining TTL in milliseconds
    * `PERSIST key` — remove expiry, returns 1 if removed / 0 otherwise
  """

  @doc """
  Handles an expiry command.

  ## Parameters

    - `cmd` - Uppercased command name (e.g. `"EXPIRE"`, `"TTL"`)
    - `args` - List of string arguments
    - `store` - Injected store map with `get_meta`, `put` callbacks

  ## Returns

  Plain Elixir term: integer or `{:error, message}`.
  """
  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("EXPIRE", [key, secs_str], store),
    do: set_expiry_seconds(key, secs_str, :none, store)

  def handle("EXPIRE", [key, secs_str | flags], store) when flags != [] do
    with {:ok, f} <- parse_flags(flags), do: set_expiry_seconds(key, secs_str, f, store)
  end

  def handle("EXPIRE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'expire' command"}

  def handle("PEXPIRE", [key, ms_str], store), do: set_expiry_ms(key, ms_str, :none, store)

  def handle("PEXPIRE", [key, ms_str | flags], store) when flags != [] do
    with {:ok, f} <- parse_flags(flags), do: set_expiry_ms(key, ms_str, f, store)
  end

  def handle("PEXPIRE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'pexpire' command"}

  def handle("EXPIREAT", [key, ts_str], store),
    do: set_expiry_at_seconds(key, ts_str, :none, store)

  def handle("EXPIREAT", [key, ts_str | flags], store) when flags != [] do
    with {:ok, f} <- parse_flags(flags), do: set_expiry_at_seconds(key, ts_str, f, store)
  end

  def handle("EXPIREAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'expireat' command"}

  def handle("PEXPIREAT", [key, ts_str], store), do: set_expiry_at_ms(key, ts_str, :none, store)

  def handle("PEXPIREAT", [key, ts_str | flags], store) when flags != [] do
    with {:ok, f} <- parse_flags(flags), do: set_expiry_at_ms(key, ts_str, f, store)
  end

  def handle("PEXPIREAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'pexpireat' command"}

  def handle("TTL", [key], store), do: get_ttl_seconds(key, store)

  def handle("TTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'ttl' command"}
  end

  def handle("PTTL", [key], store), do: get_ttl_ms(key, store)

  def handle("PTTL", _args, _store) do
    {:error, "ERR wrong number of arguments for 'pttl' command"}
  end

  def handle("PERSIST", [key], store), do: do_persist(key, store)

  def handle("PERSIST", _args, _store) do
    {:error, "ERR wrong number of arguments for 'persist' command"}
  end

  @doc false
  def handle_ast(ast, store)

  def handle_ast({:expire, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:pexpire, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:expireat, _key, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:pexpireat, _key, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({:expire, key, secs}, store) when is_integer(secs),
    do: set_expiry_seconds_parsed(key, secs, :none, store)

  def handle_ast({:pexpire, key, ms}, store) when is_integer(ms),
    do: set_expiry_ms_parsed(key, ms, :none, store)

  def handle_ast({:expireat, key, ts}, store) when is_integer(ts),
    do: set_expiry_at_seconds_parsed(key, ts, :none, store)

  def handle_ast({:pexpireat, key, ts}, store) when is_integer(ts),
    do: set_expiry_at_ms_parsed(key, ts, :none, store)

  def handle_ast({:expire, _key, _secs, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:pexpire, _key, _ms, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:expireat, _key, _ts, {:error, reason}}, _store), do: {:error, reason}
  def handle_ast({:pexpireat, _key, _ts, {:error, reason}}, _store), do: {:error, reason}

  def handle_ast({tag, _key, {:error, reason}, _flag}, _store)
      when tag in [:expire, :pexpire, :expireat, :pexpireat],
      do: {:error, reason}

  def handle_ast({:expire, key, secs, flag}, store) when is_integer(secs),
    do: set_expiry_seconds_parsed(key, secs, flag, store)

  def handle_ast({:pexpire, key, ms, flag}, store) when is_integer(ms),
    do: set_expiry_ms_parsed(key, ms, flag, store)

  def handle_ast({:expireat, key, ts, flag}, store) when is_integer(ts),
    do: set_expiry_at_seconds_parsed(key, ts, flag, store)

  def handle_ast({:pexpireat, key, ts, flag}, store) when is_integer(ts),
    do: set_expiry_at_ms_parsed(key, ts, flag, store)

  def handle_ast({:ttl, key}, store), do: get_ttl_seconds(key, store)
  def handle_ast({:pttl, key}, store), do: get_ttl_ms(key, store)
  def handle_ast({:persist, key}, store), do: do_persist(key, store)

  def handle_ast(_ast, _store), do: {:error, "ERR unsupported expiry command AST"}

  # ---------------------------------------------------------------------------
  # Private — EXPIRE / PEXPIRE (relative)
  # ---------------------------------------------------------------------------

  defp set_expiry_seconds(key, secs_str, flag, store),
    do: with_integer(secs_str, &set_expiry_seconds_parsed(key, &1, flag, store))

  defp set_expiry_ms(key, ms_str, flag, store),
    do: with_integer(ms_str, &set_expiry_ms_parsed(key, &1, flag, store))

  defp set_expiry_seconds_parsed(key, secs, flag, store),
    do: apply_expiry_at(key, ExpiryTime.relative(secs, 1_000), "expire", flag, store)

  defp set_expiry_ms_parsed(key, ms, flag, store),
    do: apply_expiry_at(key, ExpiryTime.relative(ms, 1), "pexpire", flag, store)

  # ---------------------------------------------------------------------------
  # Private — EXPIREAT / PEXPIREAT (absolute)
  # ---------------------------------------------------------------------------

  defp set_expiry_at_seconds(key, ts_str, flag, store),
    do: with_integer(ts_str, &set_expiry_at_seconds_parsed(key, &1, flag, store))

  defp set_expiry_at_ms(key, ts_str, flag, store),
    do: with_integer(ts_str, &set_expiry_at_ms_parsed(key, &1, flag, store))

  defp set_expiry_at_seconds_parsed(key, ts, flag, store),
    do: apply_expiry_at(key, ExpiryTime.absolute(ts, 1_000), "expireat", flag, store)

  defp set_expiry_at_ms_parsed(key, ts, flag, store),
    do: apply_expiry_at(key, ExpiryTime.absolute(ts, 1), "pexpireat", flag, store)

  defp with_integer(raw, fun) do
    case ExpiryTime.parse_integer(raw) do
      {:ok, value} -> fun.(value)
      :error -> integer_range_error()
    end
  end

  # A time that does not fit answers Redis's "invalid expire time" error.
  defp apply_expiry_at(key, {:ok, expire_at_ms}, _command, flag, store),
    do: apply_expiry(key, expire_at_ms, flag, store)

  defp apply_expiry_at(_key, :error, command, _flag, _store),
    do: {:error, "ERR invalid expire time in '#{command}' command"}

  defp integer_range_error, do: {:error, "ERR value is not an integer or out of range"}

  defp durable_raft_store?(%FerricStore.Instance{} = ctx),
    do: Ferricstore.Store.Router.durable_raft?(ctx)

  defp durable_raft_store?(_store), do: true

  defp rewrite_plain(key, value, expire_at_ms, store) do
    with :ok <- refuse_probabilistic(key, store),
         do: write_plain_expiry(key, value, expire_at_ms, store)
  end

  # On the instance the expiry changes in one replicated entry, GETEX's, which
  # keeps whatever value the key holds when it applies. Writing back the value
  # read before undid any write in between: INCR racing EXPIRE lost most of
  # its increments. Inside MULTI the handler runs in the state machine, where
  # nothing comes in between, and keeps writing the value back.
  defp write_plain_expiry(key, value, expire_at_ms, %FerricStore.Instance{} = ctx) do
    case Ops.getex(ctx, key, expire_at_ms) do
      nil ->
        0

      # A Bloom, Cuckoo, Count-Min or Top-K key holds its structure's metadata
      # under a type GETEX refuses; where it takes an expiry at all (a
      # standalone instance), the value is written back with it as before.
      {:error, "WRONGTYPE" <> _rest} ->
        write_plain_expiry(key, value, expire_at_ms, :rewrite, ctx)

      {:error, _reason} = error ->
        error

      _value ->
        1
    end
  end

  defp write_plain_expiry(key, value, expire_at_ms, store),
    do: write_plain_expiry(key, value, expire_at_ms, :rewrite, store)

  defp write_plain_expiry(key, value, expire_at_ms, :rewrite, store) do
    case Ops.put(store, key, value, expire_at_ms) do
      :ok -> 1
      {:error, _} = error -> error
    end
  end

  # A Bloom, Cuckoo, Count-Min or Top-K key holds the structure's metadata;
  # on the durable Raft path, rewriting it as a string with a new expiry
  # deleted the structure. Until the expiry can change in place there, refuse
  # wherever writes may differ from older releases (the instance, and MULTI
  # at cluster feature level 5); a transaction below that level writes as
  # older releases do. A standalone instance keeps the structure, so it
  # still takes the expiry.
  defp refuse_probabilistic(key, store) do
    if Ops.level_5_writes?(store) and durable_raft_store?(store) do
      case TypeRegistry.get_type(key, store) do
        type when type in @probabilistic_types ->
          {:error, "ERR expiry is not supported on #{type} keys"}

        {:error, {:storage_read_failed, _reason}} = failure ->
          ReadResult.command_error(failure)

        _type ->
          :ok
      end
    else
      :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Private — apply expiry to existing key
  # ---------------------------------------------------------------------------

  defp delete_if_exists(key, store) do
    Ferricstore.Commands.Strings.handle_ast({:del, [key]}, store)
  end

  defp apply_expiry(key, expire_at_ms, :none, store) do
    if expire_at_ms <= CommandTime.now_ms() do
      delete_if_exists(key, store)
    else
      apply_expiry_with_flag(key, expire_at_ms, :none, store)
    end
  end

  defp apply_expiry(key, expire_at_ms, flag, store),
    do: apply_expiry_with_flag(key, expire_at_ms, flag, store)

  defp apply_expiry_with_flag(key, expire_at_ms, flag, store) do
    case ttl_expire_at_ms(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        0

      old_exp ->
        if flag_allows?(flag, old_exp, expire_at_ms) do
          if expire_at_ms <= CommandTime.now_ms() do
            delete_if_exists(key, store)
          else
            apply_expiry_after_flag_check(key, expire_at_ms, store)
          end
        else
          0
        end
    end
  end

  defp apply_expiry_after_flag_check(key, expire_at_ms, store) do
    case key_meta(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        0

      {:plain, value, _old_exp} ->
        rewrite_plain(key, value, expire_at_ms, store)

      {:compound, type, _old_exp} ->
        set_collection_expiry(key, type, expire_at_ms, store)
    end
  end

  # Flag checks: NX (only if no expiry), XX (only if has expiry),
  # GT (only if new > current), LT (only if new < current). XX can join GT or
  # LT, and then both must allow the new expiry.
  defp flag_allows?(flags, old, new) when is_list(flags),
    do: Enum.all?(flags, &flag_allows?(&1, old, new))

  defp flag_allows?(:none, _old, _new), do: true
  defp flag_allows?(:nx, 0, _new), do: true
  defp flag_allows?(:nx, _old, _new), do: false
  defp flag_allows?(:xx, 0, _new), do: false
  defp flag_allows?(:xx, _old, _new), do: true
  # Redis treats persistent keys as having infinite TTL for GT/LT comparisons.
  defp flag_allows?(:gt, 0, _new), do: false
  defp flag_allows?(:gt, old, new), do: new > old
  defp flag_allows?(:lt, 0, _new), do: true
  defp flag_allows?(:lt, old, new), do: new < old

  @doc """
  Parses EXPIRE's option flags as Redis 7 does: NX, GT and LT are mutually
  exclusive, XX joins GT or LT, and a repeated flag counts once.
  """
  @spec parse_flags([binary()]) :: {:ok, atom() | [atom()]} | {:error, binary()}
  def parse_flags([_ | _] = flags) do
    with {:ok, parsed} <- parse_each_flag(flags, []) do
      parsed = Enum.uniq(parsed)

      cond do
        :nx in parsed and length(parsed) > 1 ->
          {:error, "ERR NX and XX, GT or LT options at the same time are not compatible"}

        :gt in parsed and :lt in parsed ->
          {:error, "ERR GT and LT options at the same time are not compatible"}

        match?([_flag], parsed) ->
          {:ok, hd(parsed)}

        true ->
          {:ok, parsed}
      end
    end
  end

  defp parse_each_flag([], acc), do: {:ok, Enum.reverse(acc)}

  defp parse_each_flag([flag | rest], acc) when is_binary(flag) do
    case parse_flag(flag) do
      {:ok, parsed} -> parse_each_flag(rest, [parsed | acc])
      :error -> {:error, "ERR Unsupported option #{flag}"}
    end
  end

  defp parse_each_flag([flag | _rest], _acc),
    do: {:error, "ERR Unsupported option #{inspect(flag)}"}

  defp parse_flag(str) do
    case String.upcase(str) do
      "NX" -> {:ok, :nx}
      "XX" -> {:ok, :xx}
      "GT" -> {:ok, :gt}
      "LT" -> {:ok, :lt}
      _ -> :error
    end
  end

  # ---------------------------------------------------------------------------
  # Private — TTL / PTTL queries
  # ---------------------------------------------------------------------------

  defp get_ttl_seconds(key, store) do
    case ttl_expire_at_ms(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      nil -> -2
      0 -> -1
      # Rounded to the nearest second, as Redis does.
      exp -> div(max(0, exp - CommandTime.now_ms()) + 500, 1_000)
    end
  end

  defp get_ttl_ms(key, store) do
    case ttl_expire_at_ms(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure -> ReadResult.command_error(failure)
      nil -> -2
      0 -> -1
      exp -> max(0, exp - CommandTime.now_ms())
    end
  end

  # ---------------------------------------------------------------------------
  # Private — PERSIST (remove expiry)
  # ---------------------------------------------------------------------------

  defp do_persist(key, store) do
    case ttl_expire_at_ms(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        0

      0 ->
        0

      _exp ->
        persist_after_ttl_check(key, store)
    end
  end

  defp persist_after_ttl_check(key, store) do
    case key_meta(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      nil ->
        0

      {_kind, _value_or_type, 0} ->
        0

      {:plain, value, _exp} ->
        rewrite_plain(key, value, 0, store)

      {:compound, type, _exp} ->
        set_collection_expiry(key, type, 0, store)
    end
  end

  defp key_meta(key, store) do
    case Ops.get_meta(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      nil -> compound_meta(key, store)
      {value, expire_at_ms} -> {:plain, value, expire_at_ms}
    end
  end

  defp ttl_expire_at_ms(key, store) do
    case Ops.expire_at_ms(store, key) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        failure

      nil ->
        case compound_meta(key, store) do
          {:error, {:storage_read_failed, _reason}} = failure -> failure
          nil -> nil
          {:compound, _type, expire_at_ms} -> expire_at_ms
        end

      expire_at_ms ->
        expire_at_ms
    end
  end

  @doc false
  # The expiry a list, hash, set or sorted set keeps on its own metadata, or
  # nil when `key` holds none of them.
  @spec collection_expire_at_ms(binary(), map()) ::
          non_neg_integer() | nil | ReadResult.failure()
  def collection_expire_at_ms(key, store) do
    case compound_meta(key, store) do
      {:compound, _type, expire_at_ms} -> expire_at_ms
      nil_or_failure -> nil_or_failure
    end
  end

  defp compound_meta(key, store) do
    if Ops.has_compound?(store) do
      type_key = CompoundKey.type_key(key)

      case Ops.compound_get_meta(store, key, type_key) do
        {:error, {:storage_read_failed, _reason}} = failure ->
          failure

        nil ->
          list_meta_key = CompoundKey.list_meta_key(key)

          case Ops.compound_get_meta(store, key, list_meta_key) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            nil -> nil
            {_meta, expire_at_ms} -> {:compound, "list", expire_at_ms}
          end

        {type, expire_at_ms} ->
          {:compound, type, expire_at_ms}
      end
    end
  end

  # A collection's expiry lives on every member, so changing it rewrites them
  # all. Read here and written back, that undid any member changed or added in
  # between: INCRBY racing EXPIRE kept a fraction of its increments and RPUSH
  # lost elements. Where it can, it runs instead as a one-command transaction,
  # which the shard's state machine applies with nothing in between (inside
  # MULTI the handler already runs there).
  defp set_collection_expiry(key, type, expire_at_ms, store) do
    {command, args} =
      if expire_at_ms == 0,
        do: {"PERSIST", [key]},
        else: {"PEXPIREAT", [key, Integer.to_string(expire_at_ms)]}

    case SingleCommand.run(store, command, args) do
      :fallback ->
        case expire_compound_key(key, type, expire_at_ms, store) do
          :ok -> 1
          {:error, _} = error -> ReadResult.command_result(error)
        end

      result ->
        result
    end
  end

  defp expire_compound_key(key, type, expire_at_ms, store) do
    with {:ok, entries} <- CompoundSnapshot.value_snapshot(key, type, store) do
      expiring_entries =
        Enum.map(entries, fn {compound_key, value, _old_expire_at_ms} ->
          {compound_key, value, expire_at_ms}
        end)

      Ops.compound_batch_put(store, key, expiring_entries)
    end
  end
end
