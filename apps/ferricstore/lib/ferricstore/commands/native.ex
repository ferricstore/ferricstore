# Suppress function clause grouping warnings (clauses added by different agents)
defmodule Ferricstore.Commands.Native do
  @moduledoc """
  Handles FerricStore-native commands that go beyond the Redis command set.

  ## Supported commands

    * `CAS key expected new [EX seconds]` -- compare-and-swap
    * `LOCK key owner ttl_ms` -- acquire a distributed lock
    * `UNLOCK key owner` -- release a distributed lock
    * `EXTEND key owner ttl_ms` -- extend lock TTL
    * `RATELIMIT.ADD key window_ms max [count]` -- sliding window rate limiter
    * `KEY_INFO key` -- returns diagnostic metadata about a key
  """

  alias Ferricstore.CommandTime
  alias Ferricstore.Store.Router

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CAS", [key, expected, new_value], _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.cas(ctx, key, expected, new_value, nil)
  end

  def handle("CAS", [key, expected, new_value, "EX", secs_str], _store) do
    ctx = FerricStore.Instance.get(:default)

    case Integer.parse(secs_str) do
      {secs, ""} when secs > 0 -> Router.cas(ctx, key, expected, new_value, secs * 1_000)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("CAS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cas' command"}

  def handle("LOCK", [key, owner, ttl_ms_str], _store) do
    ctx = FerricStore.Instance.get(:default)

    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 -> Router.lock(ctx, key, owner, ttl_ms)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("LOCK", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'lock' command"}

  def handle("UNLOCK", [key, owner], _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.unlock(ctx, key, owner)
  end

  def handle("UNLOCK", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'unlock' command"}

  def handle("EXTEND", [key, owner, ttl_ms_str], _store) do
    ctx = FerricStore.Instance.get(:default)

    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 -> Router.extend(ctx, key, owner, ttl_ms)
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("EXTEND", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'extend' command"}

  def handle("RATELIMIT.ADD", [key, wms, max_str], _store),
    do: do_ratelimit_add(key, wms, max_str, "1")

  def handle("RATELIMIT.ADD", [key, wms, max_str, cnt], _store),
    do: do_ratelimit_add(key, wms, max_str, cnt)

  def handle("RATELIMIT.ADD", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'ratelimit.add' command"}

  def handle("KEY_INFO", [key], _store), do: do_key_info(key)

  def handle("KEY_INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'key_info' command"}

  def handle("FETCH_OR_COMPUTE", [key, ttl], _store), do: do_fetch_or_compute(key, ttl, "")

  def handle("FETCH_OR_COMPUTE", [key, ttl, hint], _store),
    do: do_fetch_or_compute(key, ttl, hint)

  def handle("FETCH_OR_COMPUTE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute' command"}

  def handle("FETCH_OR_COMPUTE_RESULT", [key, value, ttl_ms_str], _store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms >= 0 ->
        Ferricstore.FetchOrCompute.fetch_or_compute_result(key, value, ttl_ms)

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("FETCH_OR_COMPUTE_RESULT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_result' command"}

  def handle("FETCH_OR_COMPUTE_ERROR", [key, msg], _store),
    do: Ferricstore.FetchOrCompute.fetch_or_compute_error(key, msg)

  def handle("FETCH_OR_COMPUTE_ERROR", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_error' command"}

  @spec handle_ast(term(), map()) :: term()
  def handle_ast({:cas, {:error, _} = err}, _store), do: err

  def handle_ast({:cas, key, expected, new_value, ttl_ms}, _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.cas(ctx, key, expected, new_value, ttl_ms)
  end

  def handle_ast({:cas, _args}, _store),
    do: {:error, "ERR wrong number of arguments for 'cas' command"}

  def handle_ast({:lock, {:error, _} = err}, _store), do: err
  def handle_ast({:lock, _key, _owner, {:error, _} = err}, _store), do: err

  def handle_ast({:lock, key, owner, ttl_ms}, _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.lock(ctx, key, owner, ttl_ms)
  end

  def handle_ast({:unlock, {:error, _} = err}, _store), do: err

  def handle_ast({:unlock, key, owner}, _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.unlock(ctx, key, owner)
  end

  def handle_ast({:extend, {:error, _} = err}, _store), do: err
  def handle_ast({:extend, _key, _owner, {:error, _} = err}, _store), do: err

  def handle_ast({:extend, key, owner, ttl_ms}, _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.extend(ctx, key, owner, ttl_ms)
  end

  def handle_ast({:ratelimit_add, {:error, _} = err}, _store), do: err

  def handle_ast({:ratelimit_add, key, window_ms, max, count}, _store) do
    ctx = FerricStore.Instance.get(:default)
    Router.ratelimit_add(ctx, key, window_ms, max, count)
  end

  def handle_ast({:ferricstore_key_info, {:error, _} = err}, _store), do: err
  def handle_ast({:ferricstore_key_info, key}, _store), do: do_key_info(key)

  def handle_ast({:fetch_or_compute, _key, {:error, _} = err}, _store), do: err

  def handle_ast({:fetch_or_compute, key, ttl_ms, hint}, _store),
    do: do_fetch_or_compute_ast(key, ttl_ms, hint)

  def handle_ast({:fetch_or_compute_result, _key, {:error, _} = err}, _store), do: err

  def handle_ast({:fetch_or_compute_result, key, value, ttl_ms}, _store),
    do: Ferricstore.FetchOrCompute.fetch_or_compute_result(key, value, ttl_ms)

  def handle_ast({:fetch_or_compute_error, key, msg}, _store),
    do: Ferricstore.FetchOrCompute.fetch_or_compute_error(key, msg)

  def handle_ast({tag, _args}, _store)
      when tag in ~w(lock unlock extend ratelimit_add fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a do
    {:error,
     "ERR wrong number of arguments for '#{String.replace(to_string(tag), "_", ".")}' command"}
  end

  defp do_ratelimit_add(key, wms, max_str, cnt) do
    with {w, ""} <- Integer.parse(wms),
         true <- w > 0,
         {m, ""} <- Integer.parse(max_str),
         true <- m > 0,
         {c, ""} <- Integer.parse(cnt),
         true <- c > 0 do
      ctx = FerricStore.Instance.get(:default)
      Router.ratelimit_add(ctx, key, w, m, c)
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp do_key_info(key) do
    ctx = FerricStore.Instance.get(:default)
    idx = Router.shard_for(ctx, key)
    keydir = Router.resolve_keydir(ctx, idx)
    now = CommandTime.now_ms()

    store = %{
      get: fn k -> Router.get(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_count: fn redis_key, prefix ->
        Router.compound_count(ctx, redis_key, prefix)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end
    }

    type = Ferricstore.Store.TypeRegistry.get_type(key, store)
    alive? = type != "none"

    {value_size, expire_at_ms, hot_status} = resolve_key_info(alive?, keydir, key, now)
    ttl_ms = compute_ttl_ms(alive?, expire_at_ms, now)

    [
      "type",
      type,
      "value_size",
      Integer.to_string(value_size),
      "ttl_ms",
      Integer.to_string(ttl_ms),
      "hot_cache_status",
      hot_status,
      "last_write_shard",
      Integer.to_string(idx)
    ]
  end

  defp resolve_key_info(false, _keydir, _key, _now), do: {0, 0, "cold"}
  defp resolve_key_info(true, keydir, key, now), do: ets_key_info(keydir, key, now)

  defp compute_ttl_ms(false, _expire_at_ms, _now), do: -2
  defp compute_ttl_ms(true, 0, _now), do: -1
  defp compute_ttl_ms(true, expire_at_ms, now), do: max(expire_at_ms - now, 0)

  # 7-tuple keydir lookup: {key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  defp ets_key_info(keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {byte_size(value), 0, "hot"}

        [{^key, nil, 0, _lfu, _fid, _off, vsize}] ->
          {vsize, 0, "cold"}

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {byte_size(value), exp, "hot"}

        [{^key, nil, exp, _lfu, _fid, _off, vsize}] when exp > now ->
          {vsize, exp, "cold"}

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          {0, 0, "cold"}

        [] ->
          {0, 0, "cold"}
      end
    rescue
      ArgumentError -> {0, 0, "cold"}
    end
  end

  defp do_fetch_or_compute(key, ttl_ms_str, hint) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 ->
        case Ferricstore.FetchOrCompute.fetch_or_compute(key, ttl_ms, hint) do
          {:hit, v} -> ["hit", v]
          {:compute, ch} -> ["compute", ch]
          {:ok, v} -> ["hit", v]
          {:error, reason} -> {:error, "ERR compute failed: " <> reason}
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp do_fetch_or_compute_ast(key, ttl_ms, hint) do
    case Ferricstore.FetchOrCompute.fetch_or_compute(key, ttl_ms, hint) do
      {:hit, v} -> ["hit", v]
      {:compute, ch} -> ["compute", ch]
      {:ok, v} -> ["hit", v]
      {:error, reason} -> {:error, "ERR compute failed: " <> reason}
    end
  end
end
