defmodule Ferricstore.Commands.Expiry do
  alias Ferricstore.CommandTime
  alias Ferricstore.Store.Ops

  @moduledoc """
  Handles Redis expiry commands: EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST.

  Each handler takes the uppercased command name, a list of string arguments,
  and an injected store map. Returns plain Elixir terms — the connection layer
  handles RESP encoding.

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

  def handle("EXPIRE", [key, secs_str, flag], store) do
    case parse_flag(flag) do
      {:ok, f} -> set_expiry_seconds(key, secs_str, f, store)
      :error -> {:error, "ERR Unsupported option #{flag}"}
    end
  end

  def handle("EXPIRE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'expire' command"}

  def handle("PEXPIRE", [key, ms_str], store), do: set_expiry_ms(key, ms_str, :none, store)

  def handle("PEXPIRE", [key, ms_str, flag], store) do
    case parse_flag(flag) do
      {:ok, f} -> set_expiry_ms(key, ms_str, f, store)
      :error -> {:error, "ERR Unsupported option #{flag}"}
    end
  end

  def handle("PEXPIRE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'pexpire' command"}

  def handle("EXPIREAT", [key, ts_str], store),
    do: set_expiry_at_seconds(key, ts_str, :none, store)

  def handle("EXPIREAT", [key, ts_str, flag], store) do
    case parse_flag(flag) do
      {:ok, f} -> set_expiry_at_seconds(key, ts_str, f, store)
      :error -> {:error, "ERR Unsupported option #{flag}"}
    end
  end

  def handle("EXPIREAT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'expireat' command"}

  def handle("PEXPIREAT", [key, ts_str], store), do: set_expiry_at_ms(key, ts_str, :none, store)

  def handle("PEXPIREAT", [key, ts_str, flag], store) do
    case parse_flag(flag) do
      {:ok, f} -> set_expiry_at_ms(key, ts_str, f, store)
      :error -> {:error, "ERR Unsupported option #{flag}"}
    end
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

  # ---------------------------------------------------------------------------
  # Private — EXPIRE / PEXPIRE (relative)
  # ---------------------------------------------------------------------------

  defp set_expiry_seconds(key, secs_str, flag, store) do
    case Integer.parse(secs_str) do
      {secs, ""} when secs <= 0 ->
        delete_if_exists(key, store)

      {secs, ""} ->
        apply_expiry(key, CommandTime.now_ms() + secs * 1_000, flag, store)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp set_expiry_ms(key, ms_str, flag, store) do
    case Integer.parse(ms_str) do
      {ms, ""} when ms <= 0 ->
        delete_if_exists(key, store)

      {ms, ""} ->
        apply_expiry(key, CommandTime.now_ms() + ms, flag, store)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — EXPIREAT / PEXPIREAT (absolute)
  # ---------------------------------------------------------------------------

  defp set_expiry_at_seconds(key, ts_str, flag, store) do
    case Integer.parse(ts_str) do
      {ts, ""} ->
        expire_at_ms = ts * 1_000

        if expire_at_ms <= CommandTime.now_ms() do
          delete_if_exists(key, store)
        else
          apply_expiry(key, expire_at_ms, flag, store)
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp set_expiry_at_ms(key, ts_str, flag, store) do
    case Integer.parse(ts_str) do
      {ts, ""} ->
        if ts <= CommandTime.now_ms() do
          delete_if_exists(key, store)
        else
          apply_expiry(key, ts, flag, store)
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — apply expiry to existing key
  # ---------------------------------------------------------------------------

  defp delete_if_exists(key, store) do
    case Ops.get_meta(store, key) do
      nil ->
        0

      _ ->
        Ops.delete(store, key)
        1
    end
  end

  defp apply_expiry(key, expire_at_ms, flag, store) do
    case Ops.get_meta(store, key) do
      nil ->
        0

      {value, old_exp} ->
        if flag_allows?(flag, old_exp, expire_at_ms) do
          Ops.put(store, key, value, expire_at_ms)
          1
        else
          0
        end
    end
  end

  # Flag checks: NX (only if no expiry), XX (only if has expiry),
  # GT (only if new > current), LT (only if new < current).
  defp flag_allows?(:none, _old, _new), do: true
  defp flag_allows?(:nx, 0, _new), do: true
  defp flag_allows?(:nx, _old, _new), do: false
  defp flag_allows?(:xx, 0, _new), do: false
  defp flag_allows?(:xx, _old, _new), do: true
  defp flag_allows?(:gt, 0, _new), do: true
  defp flag_allows?(:gt, old, new), do: new > old
  defp flag_allows?(:lt, 0, _new), do: true
  defp flag_allows?(:lt, old, new), do: new < old

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
    case Ops.get_meta(store, key) do
      nil -> -2
      {_, 0} -> -1
      {_, exp} -> max(0, div(exp - CommandTime.now_ms(), 1_000))
    end
  end

  defp get_ttl_ms(key, store) do
    case Ops.get_meta(store, key) do
      nil -> -2
      {_, 0} -> -1
      {_, exp} -> max(0, exp - CommandTime.now_ms())
    end
  end

  # ---------------------------------------------------------------------------
  # Private — PERSIST (remove expiry)
  # ---------------------------------------------------------------------------

  defp do_persist(key, store) do
    case Ops.get_meta(store, key) do
      nil ->
        0

      {_, 0} ->
        0

      {value, _exp} ->
        Ops.put(store, key, value, 0)
        1
    end
  end
end
