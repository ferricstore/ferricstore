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
  alias Ferricstore.Store.{LocalTxStore, Ops, ReadResult, Router}

  @max_int64 9_223_372_036_854_775_807
  # Expiries are signed 64-bit values and HLC physical time occupies 48 bits.
  @max_ttl_ms @max_int64 - 281_474_976_710_656 + 1
  @max_window_ms div(@max_ttl_ms, 2)

  @spec handle(binary(), [binary()], map()) :: term()
  def handle(cmd, args, store)

  def handle("CAS", [key, expected, new_value], store),
    do: Ops.cas(store, key, expected, new_value, nil)

  def handle("CAS", [key, expected, new_value, option, secs_str], store) do
    if String.upcase(option) == "EX" do
      case Integer.parse(secs_str) do
        {secs, ""} when secs > 0 and secs <= div(@max_ttl_ms, 1_000) ->
          Ops.cas(store, key, expected, new_value, secs * 1_000)

        _ ->
          integer_error()
      end
    else
      {:error, "ERR wrong number of arguments for 'cas' command"}
    end
  end

  def handle("CAS", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'cas' command"}

  def handle("LOCK", [key, owner, ttl_ms_str], store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 and ttl_ms <= @max_ttl_ms ->
        Ops.lock(store, key, owner, ttl_ms)

      _ ->
        integer_error()
    end
  end

  def handle("LOCK", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'lock' command"}

  def handle("UNLOCK", [key, owner], store), do: Ops.unlock(store, key, owner)

  def handle("UNLOCK", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'unlock' command"}

  def handle("EXTEND", [key, owner, ttl_ms_str], store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 and ttl_ms <= @max_ttl_ms ->
        Ops.extend(store, key, owner, ttl_ms)

      _ ->
        integer_error()
    end
  end

  def handle("EXTEND", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'extend' command"}

  def handle("RATELIMIT.ADD", [key, wms, max_str], store),
    do: do_ratelimit_add(store, key, wms, max_str, "1")

  def handle("RATELIMIT.ADD", [key, wms, max_str, cnt], store),
    do: do_ratelimit_add(store, key, wms, max_str, cnt)

  def handle("RATELIMIT.ADD", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'ratelimit.add' command"}

  def handle("KEY_INFO", [key], store), do: do_key_info(key, store)

  def handle("KEY_INFO", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'key_info' command"}

  def handle("FETCH_OR_COMPUTE", [key, ttl], store),
    do: do_fetch_or_compute(store, key, ttl, "")

  def handle("FETCH_OR_COMPUTE", [key, ttl, hint], store),
    do: do_fetch_or_compute(store, key, ttl, hint)

  def handle("FETCH_OR_COMPUTE", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute' command"}

  def handle("FETCH_OR_COMPUTE_RESULT", [key, token, value, ttl_ms_str], store) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms >= 0 and ttl_ms <= @max_ttl_ms ->
        Ferricstore.FetchOrCompute.fetch_or_compute_result(
          fetch_or_compute_ctx(store),
          key,
          value,
          token,
          ttl_ms
        )

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  def handle("FETCH_OR_COMPUTE_RESULT", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_result' command"}

  def handle("FETCH_OR_COMPUTE_ERROR", [key, token, msg], store),
    do:
      Ferricstore.FetchOrCompute.fetch_or_compute_error(
        fetch_or_compute_ctx(store),
        key,
        token,
        msg
      )

  def handle("FETCH_OR_COMPUTE_ERROR", _args, _store),
    do: {:error, "ERR wrong number of arguments for 'fetch_or_compute_error' command"}

  @spec handle_ast(term(), map()) :: term()
  def handle_ast({:cas, {:error, _} = err}, _store), do: err

  def handle_ast({:cas, key, expected, new_value, ttl_ms}, store)
      when is_nil(ttl_ms) or
             (is_integer(ttl_ms) and ttl_ms > 0 and ttl_ms <= @max_ttl_ms),
      do: Ops.cas(store, key, expected, new_value, ttl_ms)

  def handle_ast({:cas, _key, _expected, _new_value, _ttl_ms}, _store),
    do: integer_error()

  def handle_ast({:cas, _args}, _store),
    do: {:error, "ERR wrong number of arguments for 'cas' command"}

  def handle_ast({:lock, {:error, _} = err}, _store), do: err
  def handle_ast({:lock, _key, _owner, {:error, _} = err}, _store), do: err

  def handle_ast({:lock, key, owner, ttl_ms}, store)
      when is_integer(ttl_ms) and ttl_ms > 0 and ttl_ms <= @max_ttl_ms,
      do: Ops.lock(store, key, owner, ttl_ms)

  def handle_ast({:lock, _key, _owner, _ttl_ms}, _store), do: integer_error()

  def handle_ast({:unlock, {:error, _} = err}, _store), do: err

  def handle_ast({:unlock, key, owner}, store), do: Ops.unlock(store, key, owner)

  def handle_ast({:extend, {:error, _} = err}, _store), do: err
  def handle_ast({:extend, _key, _owner, {:error, _} = err}, _store), do: err

  def handle_ast({:extend, key, owner, ttl_ms}, store)
      when is_integer(ttl_ms) and ttl_ms > 0 and ttl_ms <= @max_ttl_ms,
      do: Ops.extend(store, key, owner, ttl_ms)

  def handle_ast({:extend, _key, _owner, _ttl_ms}, _store), do: integer_error()

  def handle_ast({:ratelimit_add, {:error, _} = err}, _store), do: err

  def handle_ast({:ratelimit_add, key, window_ms, max, count}, store)
      when is_integer(window_ms) and window_ms > 0 and window_ms <= @max_window_ms and
             is_integer(max) and max > 0 and max <= @max_int64 and is_integer(count) and
             count > 0 and count <= @max_int64,
      do: Ops.ratelimit_add(store, key, window_ms, max, count)

  def handle_ast({:ratelimit_add, _key, _window_ms, _max, _count}, _store),
    do: integer_error()

  def handle_ast({:ferricstore_key_info, {:error, _} = err}, _store), do: err
  def handle_ast({:ferricstore_key_info, key}, store), do: do_key_info(key, store)

  def handle_ast({:fetch_or_compute, _key, {:error, _} = err}, _store), do: err

  def handle_ast({:fetch_or_compute, key, ttl_ms, hint}, store)
      when is_integer(ttl_ms) and ttl_ms > 0 and ttl_ms <= @max_ttl_ms,
      do: do_fetch_or_compute_ast(store, key, ttl_ms, hint)

  def handle_ast({:fetch_or_compute, _key, _ttl_ms, _hint}, _store), do: integer_error()

  def handle_ast({:fetch_or_compute_result, _key, {:error, _} = err}, _store), do: err

  def handle_ast({:fetch_or_compute_result, key, token, value, ttl_ms}, store)
      when is_integer(ttl_ms) and ttl_ms >= 0 and ttl_ms <= @max_ttl_ms,
      do:
        Ferricstore.FetchOrCompute.fetch_or_compute_result(
          fetch_or_compute_ctx(store),
          key,
          value,
          token,
          ttl_ms
        )

  def handle_ast({:fetch_or_compute_result, _key, _token, _value, _ttl_ms}, _store),
    do: integer_error()

  def handle_ast({:fetch_or_compute_error, key, token, msg}, store),
    do:
      Ferricstore.FetchOrCompute.fetch_or_compute_error(
        fetch_or_compute_ctx(store),
        key,
        token,
        msg
      )

  def handle_ast({tag, _args}, _store)
      when tag in ~w(lock unlock extend ratelimit_add fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a do
    {:error,
     "ERR wrong number of arguments for '#{String.replace(to_string(tag), "_", ".")}' command"}
  end

  defp do_ratelimit_add(store, key, wms, max_str, cnt) do
    with {w, ""} <- Integer.parse(wms),
         true <- w > 0 and w <= @max_window_ms,
         {m, ""} <- Integer.parse(max_str),
         true <- m > 0 and m <= @max_int64,
         {c, ""} <- Integer.parse(cnt),
         true <- c > 0 and c <= @max_int64 do
      Ops.ratelimit_add(store, key, w, m, c)
    else
      _ -> {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp integer_error, do: {:error, "ERR value is not an integer or out of range"}

  defp do_key_info(key, store) do
    ctx = key_info_ctx(store)
    idx = Router.shard_for(ctx, key)
    keydir = Router.resolve_keydir(ctx, idx)
    now = CommandTime.now_ms()

    case Ferricstore.Store.TypeRegistry.get_type(key, store) do
      {:error, {:storage_read_failed, _reason}} = failure ->
        ReadResult.command_error(failure)

      type when is_binary(type) ->
        alive? = type != "none"

        {stored_size, expire_at_ms, hot_status} = resolve_key_info(alive?, keydir, key, now)

        case key_info_value_size(type, ctx, key, stored_size) do
          value_size when is_integer(value_size) and value_size >= 0 ->
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

          {:error, {:storage_read_failed, _reason}} = failure ->
            ReadResult.command_error(failure)

          _invalid ->
            {:error, "ERR storage read failed"}
        end

      _invalid ->
        {:error, "ERR storage read failed"}
    end
  end

  defp key_info_ctx(store), do: command_instance_ctx(store)

  defp resolve_key_info(false, _keydir, _key, _now), do: {0, 0, "cold"}
  defp resolve_key_info(true, keydir, key, now), do: ets_key_info(keydir, key, now)

  defp key_info_value_size("string", ctx, key, stored_size) do
    case Router.value_size(ctx, key) do
      nil -> stored_size
      result -> result
    end
  end

  defp key_info_value_size(_type, _ctx, _key, stored_size), do: stored_size

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

  defp do_fetch_or_compute(store, key, ttl_ms_str, hint) do
    case Integer.parse(ttl_ms_str) do
      {ttl_ms, ""} when ttl_ms > 0 and ttl_ms <= @max_ttl_ms ->
        case Ferricstore.FetchOrCompute.fetch_or_compute(
               fetch_or_compute_ctx(store),
               key,
               ttl_ms,
               hint
             ) do
          {:hit, v} -> ["hit", v]
          {:compute, ch, token} -> ["compute", ch, token]
          {:ok, v} -> ["hit", v]
          {:error, reason} -> fetch_or_compute_error(reason)
        end

      _ ->
        {:error, "ERR value is not an integer or out of range"}
    end
  end

  defp do_fetch_or_compute_ast(store, key, ttl_ms, hint) do
    case Ferricstore.FetchOrCompute.fetch_or_compute(
           fetch_or_compute_ctx(store),
           key,
           ttl_ms,
           hint
         ) do
      {:hit, v} -> ["hit", v]
      {:compute, ch, token} -> ["compute", ch, token]
      {:ok, v} -> ["hit", v]
      {:error, reason} -> fetch_or_compute_error(reason)
    end
  end

  defp fetch_or_compute_error(reason) when is_binary(reason),
    do: {:error, "ERR compute failed: " <> reason}

  defp fetch_or_compute_error(reason),
    do: {:error, "ERR compute failed: " <> inspect(reason, limit: 20, printable_limit: 256)}

  defp fetch_or_compute_ctx(store), do: command_instance_ctx(store)

  defp command_instance_ctx(%FerricStore.Instance{} = ctx), do: ctx

  defp command_instance_ctx(%LocalTxStore{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

  defp command_instance_ctx(%{__instance_ctx__: %FerricStore.Instance{} = ctx}), do: ctx
  defp command_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx
  defp command_instance_ctx(_store), do: FerricStore.Instance.get(:default)
end
