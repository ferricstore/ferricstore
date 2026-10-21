defmodule Ferricstore.Commands.Expiry do
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.CompoundSnapshot
  alias Ferricstore.Commands.ExpiryTime
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.{Ops, ReadResult}

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
        case Ops.put(store, key, value, expire_at_ms) do
          :ok -> 1
          {:error, _} = error -> error
        end

      {:compound, type, _old_exp} ->
        case expire_compound_key(key, type, expire_at_ms, store) do
          :ok -> 1
          {:error, _} = error -> ReadResult.command_result(error)
        end
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
        case Ops.put(store, key, value, 0) do
          :ok -> 1
          {:error, _} = error -> error
        end

      {:compound, type, _exp} ->
        case expire_compound_key(key, type, 0, store) do
          :ok -> 1
          {:error, _} = error -> ReadResult.command_result(error)
        end
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
