defmodule FerricStore.Impl do
  @moduledoc "Elixir-native implementation of all FerricStore data-type operations, delegating to Router and command handlers."

  alias Ferricstore.HLC
  alias Ferricstore.Store.Router
  alias Ferricstore.Commands.{Bloom, CMS, Cuckoo, TopK, TDigest}

  # ---------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------

  @spec set(FerricStore.Instance.t(), binary(), binary(), keyword()) ::
          :ok | {:ok, binary() | nil} | {:ok, boolean()} | {:error, term()}
  def set(ctx, key, value, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, 0)
    nx = Keyword.get(opts, :nx, false)
    xx = Keyword.get(opts, :xx, false)
    get = Keyword.get(opts, :get, false)
    keepttl = Keyword.get(opts, :keepttl, false)
    exat = Keyword.get(opts, :exat)
    pxat = Keyword.get(opts, :pxat)

    expire_at_ms = resolve_expire_at(keepttl, pxat, exat, ttl)
    do_set(ctx, key, value, expire_at_ms, get, nx, xx)
  end

  defp resolve_expire_at(true, _pxat, _exat, _ttl), do: :keepttl
  defp resolve_expire_at(_kttl, pxat, _exat, _ttl) when pxat != nil, do: pxat
  defp resolve_expire_at(_kttl, _pxat, exat, _ttl) when exat != nil, do: exat * 1000
  defp resolve_expire_at(_kttl, _pxat, _exat, ttl) when ttl > 0, do: HLC.now_ms() + ttl
  defp resolve_expire_at(_kttl, _pxat, _exat, _ttl), do: 0

  defp do_set(ctx, key, value, expire_at_ms, true, nx, xx) do
    old = Router.get(ctx, key)

    if (nx and old != nil) or (xx and old == nil) do
      {:ok, old}
    else
      case Router.put(ctx, key, value, expire_at_ms) do
        :ok -> {:ok, old}
        {:error, _reason} = error -> error
      end
    end
  end

  defp do_set(ctx, key, value, expire_at_ms, _get, true, _xx) do
    if Router.exists?(ctx, key) do
      {:ok, false}
    else
      case Router.put(ctx, key, value, expire_at_ms) do
        :ok -> {:ok, true}
        {:error, _reason} = error -> error
      end
    end
  end

  defp do_set(ctx, key, value, expire_at_ms, _get, _nx, true) do
    if Router.exists?(ctx, key) do
      Router.put(ctx, key, value, expire_at_ms)
    else
      :ok
    end
  end

  defp do_set(ctx, key, value, expire_at_ms, _get, _nx, _xx) do
    Router.put(ctx, key, value, expire_at_ms)
  end

  @spec get(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, binary() | nil}
  def get(ctx, key, _opts \\ []) do
    {:ok, Router.get(ctx, key)}
  end

  @spec del(FerricStore.Instance.t(), [binary()]) :: {:ok, non_neg_integer()} | {:error, term()}
  def del(ctx, keys) when is_list(keys) do
    Enum.reduce_while(keys, 0, fn key, count ->
      case Router.delete(ctx, key) do
        :ok -> {:cont, count + 1}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      count when is_integer(count) -> {:ok, count}
      {:error, _reason} = error -> error
    end
  end

  @spec exists?(FerricStore.Instance.t(), binary()) :: {:ok, boolean()}
  def exists?(ctx, key) do
    {:ok, Router.exists?(ctx, key)}
  end

  @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
          {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    Router.incr(ctx, key, delta)
  end

  @spec incr_float(FerricStore.Instance.t(), binary(), float()) ::
          {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    Router.incr_float(ctx, key, delta)
  end

  @spec mget(FerricStore.Instance.t(), [binary()]) :: {:ok, [binary() | nil]}
  def mget(ctx, keys) do
    results = Router.batch_get(ctx, keys)
    {:ok, results}
  end

  @spec mset(FerricStore.Instance.t(), [{binary(), binary()}]) :: :ok | {:error, term()}
  def mset(ctx, pairs) do
    Enum.reduce_while(pairs, :ok, fn {key, value}, :ok ->
      case Router.put(ctx, key, value, 0) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    Router.append(ctx, key, suffix)
  end

  @spec strlen(FerricStore.Instance.t(), binary()) :: {:ok, non_neg_integer()}
  def strlen(ctx, key) do
    case Router.get(ctx, key) do
      nil -> {:ok, 0}
      val when is_binary(val) -> {:ok, byte_size(val)}
      val -> {:ok, byte_size(to_string(val))}
    end
  end

  @spec getset(FerricStore.Instance.t(), binary(), binary()) :: {:ok, binary() | nil}
  def getset(ctx, key, value) do
    old = Router.getset(ctx, key, value)
    {:ok, old}
  end

  @spec getdel(FerricStore.Instance.t(), binary()) :: {:ok, binary() | nil}
  def getdel(ctx, key) do
    old = Router.getdel(ctx, key)
    {:ok, old}
  end

  @spec getex(FerricStore.Instance.t(), binary(), keyword()) :: {:ok, binary() | nil}
  def getex(ctx, key, opts) do
    ttl = Keyword.get(opts, :ttl, 0)
    expire_at_ms = if ttl > 0, do: HLC.now_ms() + ttl, else: 0
    val = Router.getex(ctx, key, expire_at_ms)
    {:ok, val}
  end

  @spec setnx(FerricStore.Instance.t(), binary(), binary()) :: {:ok, boolean()} | {:error, term()}
  def setnx(ctx, key, value) do
    if Router.exists?(ctx, key) do
      {:ok, false}
    else
      case Router.put(ctx, key, value, 0) do
        :ok -> {:ok, true}
        {:error, _reason} = error -> error
      end
    end
  end

  @spec setex(FerricStore.Instance.t(), binary(), pos_integer(), binary()) ::
          :ok | {:error, term()}
  def setex(ctx, key, seconds, value) do
    expire_at_ms = HLC.now_ms() + seconds * 1000
    Router.put(ctx, key, value, expire_at_ms)
  end

  @spec psetex(FerricStore.Instance.t(), binary(), pos_integer(), binary()) ::
          :ok | {:error, term()}
  def psetex(ctx, key, milliseconds, value) do
    expire_at_ms = HLC.now_ms() + milliseconds
    Router.put(ctx, key, value, expire_at_ms)
  end

  @spec getrange(FerricStore.Instance.t(), binary(), integer(), integer()) :: {:ok, binary()}
  def getrange(ctx, key, start, stop) do
    case Router.get(ctx, key) do
      nil ->
        {:ok, ""}

      val ->
        len = byte_size(val)
        s = if start < 0, do: max(len + start, 0), else: min(start, len)
        e = if stop < 0, do: max(len + stop, 0), else: min(stop, len - 1)
        if s > e, do: {:ok, ""}, else: {:ok, binary_part(val, s, e - s + 1)}
    end
  end

  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    Router.setrange(ctx, key, offset, value)
  end

  # ---------------------------------------------------------------
  # TTL / expiry
  # ---------------------------------------------------------------

  @spec expire(FerricStore.Instance.t(), binary(), integer()) :: 0 | 1 | {:error, binary()}
  def expire(ctx, key, seconds) do
    store = build_store(ctx)
    Ferricstore.Commands.Expiry.handle("PEXPIRE", [key, to_string(seconds * 1000)], store)
  end

  @spec pexpire(FerricStore.Instance.t(), binary(), integer()) :: 0 | 1 | {:error, binary()}
  def pexpire(ctx, key, milliseconds) do
    store = build_store(ctx)
    Ferricstore.Commands.Expiry.handle("PEXPIRE", [key, to_string(milliseconds)], store)
  end

  @spec ttl(FerricStore.Instance.t(), binary()) :: {:ok, integer()}
  def ttl(ctx, key) do
    store = build_store(ctx)

    case Ferricstore.Commands.Expiry.handle("PTTL", [key], store) do
      ms when is_integer(ms) and ms > 0 -> {:ok, ms}
      -1 -> {:ok, -1}
      -2 -> {:ok, -2}
      other -> {:ok, other}
    end
  end

  @spec pttl(FerricStore.Instance.t(), binary()) :: {:ok, integer()}
  def pttl(ctx, key) do
    ttl(ctx, key)
  end

  @spec persist(FerricStore.Instance.t(), binary()) :: 0 | 1
  def persist(ctx, key) do
    store = build_store(ctx)
    Ferricstore.Commands.Expiry.handle("PERSIST", [key], store)
  end

  # ---------------------------------------------------------------
  # Hash
  # ---------------------------------------------------------------

  @spec hset(FerricStore.Instance.t(), binary(), map()) :: {:ok, term()} | {:error, binary()}
  def hset(ctx, key, fields) when is_map(fields) do
    store = build_store(ctx)
    args = [key | Enum.flat_map(fields, fn {k, v} -> [to_string(k), to_string(v)] end)]
    result = Ferricstore.Commands.Hash.handle("HSET", args, store)
    wrap_result(result)
  end

  @spec hget(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, binary() | nil} | {:error, binary()}
  def hget(ctx, key, field) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Hash.handle("HGET", [key, to_string(field)], store)
    wrap_result(result)
  end

  @spec hgetall(FerricStore.Instance.t(), binary()) :: {:ok, map()} | {:error, binary()}
  def hgetall(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Hash.handle("HGETALL", [key], store)

    case result do
      list when is_list(list) ->
        map = list |> Enum.chunk_every(2) |> Enum.into(%{}, fn [k, v] -> {k, v} end)
        {:ok, map}

      {:error, _} = err ->
        err

      _ ->
        {:ok, %{}}
    end
  end

  @spec hdel(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def hdel(ctx, key, fields) when is_list(fields) do
    store = build_store(ctx)
    args = [key | Enum.map(fields, &to_string/1)]
    result = Ferricstore.Commands.Hash.handle("HDEL", args, store)
    wrap_result(result)
  end

  @spec hexists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, boolean()} | {:error, binary()}
  def hexists(ctx, key, field) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Hash.handle("HEXISTS", [key, to_string(field)], store)

    case result do
      {:error, _} = err -> err
      value -> {:ok, value == 1}
    end
  end

  @spec hlen(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def hlen(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Hash.handle("HLEN", [key], store)
    wrap_result(result)
  end

  @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
          integer() | {:error, binary()}
  def hincrby(ctx, key, field, amount) do
    store = build_store(ctx)
    Ferricstore.Commands.Hash.handle("HINCRBY", [key, to_string(field), to_string(amount)], store)
  end

  # ---------------------------------------------------------------
  # Set
  # ---------------------------------------------------------------

  @spec sadd(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def sadd(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = Ferricstore.Commands.Set.handle("SADD", args, store)
    wrap_result(result)
  end

  @spec srem(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def srem(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = Ferricstore.Commands.Set.handle("SREM", args, store)
    wrap_result(result)
  end

  @spec smembers(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def smembers(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Set.handle("SMEMBERS", [key], store)
    wrap_result(result)
  end

  @spec sismember(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, boolean()} | {:error, binary()}
  def sismember(ctx, key, member) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Set.handle("SISMEMBER", [key, to_string(member)], store)

    case result do
      {:error, _} = err -> err
      value -> {:ok, value == 1}
    end
  end

  @spec scard(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def scard(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.Set.handle("SCARD", [key], store)
    wrap_result(result)
  end

  @spec spop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def spop(ctx, key, count) do
    store = build_store(ctx)

    if count == 1 do
      result = Ferricstore.Commands.Set.handle("SPOP", [key], store)
      wrap_result(result)
    else
      result = Ferricstore.Commands.Set.handle("SPOP", [key, to_string(count)], store)
      wrap_result(result)
    end
  end

  # ---------------------------------------------------------------
  # List
  # ---------------------------------------------------------------

  @spec lpush(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def lpush(ctx, key, values) when is_list(values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    result = Ferricstore.Commands.List.handle("LPUSH", args, store)
    wrap_result(result)
  end

  @spec rpush(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def rpush(ctx, key, values) when is_list(values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    result = Ferricstore.Commands.List.handle("RPUSH", args, store)
    wrap_result(result)
  end

  @spec lpop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def lpop(ctx, key, count) do
    store = build_store(ctx)

    if count == 1 do
      result = Ferricstore.Commands.List.handle("LPOP", [key], store)
      wrap_result(result)
    else
      result = Ferricstore.Commands.List.handle("LPOP", [key, to_string(count)], store)
      wrap_result(result)
    end
  end

  @spec rpop(FerricStore.Instance.t(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, binary()}
  def rpop(ctx, key, count) do
    store = build_store(ctx)

    if count == 1 do
      result = Ferricstore.Commands.List.handle("RPOP", [key], store)
      wrap_result(result)
    else
      result = Ferricstore.Commands.List.handle("RPOP", [key, to_string(count)], store)
      wrap_result(result)
    end
  end

  @spec lrange(FerricStore.Instance.t(), binary(), integer(), integer()) ::
          {:ok, term()} | {:error, binary()}
  def lrange(ctx, key, start, stop) do
    store = build_store(ctx)

    result =
      Ferricstore.Commands.List.handle("LRANGE", [key, to_string(start), to_string(stop)], store)

    wrap_result(result)
  end

  @spec llen(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def llen(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.List.handle("LLEN", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # Sorted Set
  # ---------------------------------------------------------------

  @spec zadd(FerricStore.Instance.t(), binary(), [{number(), binary()}]) ::
          {:ok, term()} | {:error, binary()}
  def zadd(ctx, key, members) when is_list(members) do
    store = build_store(ctx)

    args = [
      key
      | Enum.flat_map(members, fn {score, member} -> [to_string(score), to_string(member)] end)
    ]

    result = Ferricstore.Commands.SortedSet.handle("ZADD", args, store)
    wrap_result(result)
  end

  @spec zcard(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def zcard(ctx, key) do
    store = build_store(ctx)
    result = Ferricstore.Commands.SortedSet.handle("ZCARD", [key], store)
    wrap_result(result)
  end

  @spec zscore(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def zscore(ctx, key, member) do
    store = build_store(ctx)
    result = Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, to_string(member)], store)
    wrap_result(result)
  end

  @spec zrange(FerricStore.Instance.t(), binary(), integer(), integer(), keyword()) ::
          {:ok, term()} | {:error, binary()}
  def zrange(ctx, key, start, stop, opts) do
    store = build_store(ctx)
    args = [key, to_string(start), to_string(stop)]
    args = if Keyword.get(opts, :withscores, false), do: args ++ ["WITHSCORES"], else: args
    result = Ferricstore.Commands.SortedSet.handle("ZRANGE", args, store)
    wrap_result(result)
  end

  @spec zrem(FerricStore.Instance.t(), binary(), [binary()]) :: {:ok, term()} | {:error, binary()}
  def zrem(ctx, key, members) when is_list(members) do
    store = build_store(ctx)
    args = [key | Enum.map(members, &to_string/1)]
    result = Ferricstore.Commands.SortedSet.handle("ZREM", args, store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # Bloom filter
  # ---------------------------------------------------------------

  @spec bf_reserve(FerricStore.Instance.t(), binary(), number(), pos_integer()) ::
          :ok | {:error, binary()}
  def bf_reserve(ctx, key, error_rate, capacity) do
    store = build_prob_store(ctx, key)
    Bloom.handle("BF.RESERVE", [key, to_string(error_rate), to_string(capacity)], store)
  end

  @spec bf_add(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_add(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.ADD", [key, element], store)
    wrap_result(result)
  end

  @spec bf_madd(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def bf_madd(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.MADD", [key | elements], store)
    wrap_result(result)
  end

  @spec bf_exists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def bf_exists(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.EXISTS", [key, element], store)
    wrap_result(result)
  end

  @spec bf_mexists(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def bf_mexists(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.MEXISTS", [key | elements], store)
    wrap_result(result)
  end

  @spec bf_card(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_card(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.CARD", [key], store)
    wrap_result(result)
  end

  @spec bf_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def bf_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Bloom.handle("BF.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # CMS
  # ---------------------------------------------------------------

  @spec cms_initbydim(FerricStore.Instance.t(), binary(), pos_integer(), pos_integer()) ::
          :ok | {:error, binary()}
  def cms_initbydim(ctx, key, width, depth) do
    store = build_prob_store(ctx, key)
    CMS.handle("CMS.INITBYDIM", [key, to_string(width), to_string(depth)], store)
  end

  @spec cms_initbyprob(FerricStore.Instance.t(), binary(), number(), number()) ::
          :ok | {:error, binary()}
  def cms_initbyprob(ctx, key, error, probability) do
    store = build_prob_store(ctx, key)
    CMS.handle("CMS.INITBYPROB", [key, to_string(error), to_string(probability)], store)
  end

  @spec cms_incrby(FerricStore.Instance.t(), binary(), [{binary(), pos_integer()}]) ::
          {:ok, term()} | {:error, binary()}
  def cms_incrby(ctx, key, pairs) do
    store = build_prob_store(ctx, key)
    args = [key | Enum.flat_map(pairs, fn {elem, count} -> [elem, to_string(count)] end)]
    result = CMS.handle("CMS.INCRBY", args, store)
    wrap_result(result)
  end

  @spec cms_query(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def cms_query(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = CMS.handle("CMS.QUERY", [key | elements], store)
    wrap_result(result)
  end

  @spec cms_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def cms_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = CMS.handle("CMS.INFO", [key], store)
    wrap_result(result)
  end

  @spec cms_merge(FerricStore.Instance.t(), binary(), [binary()], keyword()) ::
          :ok | {:error, binary()}
  def cms_merge(ctx, dest, sources, _opts \\ []) do
    store = build_prob_store(ctx, dest)
    args = [dest, to_string(length(sources))] ++ sources
    CMS.handle("CMS.MERGE", args, store)
  end

  # ---------------------------------------------------------------
  # Cuckoo
  # ---------------------------------------------------------------

  @spec cf_reserve(FerricStore.Instance.t(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def cf_reserve(ctx, key, capacity) do
    store = build_prob_store(ctx, key)
    Cuckoo.handle("CF.RESERVE", [key, to_string(capacity)], store)
  end

  @spec cf_add(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_add(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.ADD", [key, element], store)
    wrap_result(result)
  end

  @spec cf_addnx(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_addnx(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.ADDNX", [key, element], store)
    wrap_result(result)
  end

  @spec cf_del(FerricStore.Instance.t(), binary(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_del(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.DEL", [key, element], store)
    wrap_result(result)
  end

  @spec cf_exists(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_exists(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.EXISTS", [key, element], store)
    wrap_result(result)
  end

  @spec cf_mexists(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def cf_mexists(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.MEXISTS", [key | elements], store)
    wrap_result(result)
  end

  @spec cf_count(FerricStore.Instance.t(), binary(), binary()) ::
          {:ok, term()} | {:error, binary()}
  def cf_count(ctx, key, element) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.COUNT", [key, element], store)
    wrap_result(result)
  end

  @spec cf_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def cf_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = Cuckoo.handle("CF.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # TopK
  # ---------------------------------------------------------------

  @spec topk_reserve(FerricStore.Instance.t(), binary(), pos_integer()) ::
          :ok | {:error, binary()}
  def topk_reserve(ctx, key, k) do
    store = build_prob_store(ctx, key)
    TopK.handle("TOPK.RESERVE", [key, to_string(k)], store)
  end

  @spec topk_add(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def topk_add(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = TopK.handle("TOPK.ADD", [key | elements], store)
    wrap_result(result)
  end

  @spec topk_query(FerricStore.Instance.t(), binary(), [binary()]) ::
          {:ok, term()} | {:error, binary()}
  def topk_query(ctx, key, elements) do
    store = build_prob_store(ctx, key)
    result = TopK.handle("TOPK.QUERY", [key | elements], store)
    wrap_result(result)
  end

  @spec topk_list(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def topk_list(ctx, key) do
    store = build_prob_store(ctx, key)
    result = TopK.handle("TOPK.LIST", [key], store)
    wrap_result(result)
  end

  @spec topk_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def topk_info(ctx, key) do
    store = build_prob_store(ctx, key)
    result = TopK.handle("TOPK.INFO", [key], store)
    wrap_result(result)
  end

  # ---------------------------------------------------------------
  # TDigest
  # ---------------------------------------------------------------

  @spec tdigest_create(FerricStore.Instance.t(), binary(), keyword()) :: :ok | {:error, binary()}
  def tdigest_create(ctx, key, opts \\ []) do
    store = build_store(ctx)
    args = [key]

    args =
      case Keyword.get(opts, :compression) do
        nil -> args
        c -> args ++ ["COMPRESSION", to_string(c)]
      end

    TDigest.handle("TDIGEST.CREATE", args, store)
  end

  @spec tdigest_add(FerricStore.Instance.t(), binary(), [number()]) :: :ok | {:error, binary()}
  def tdigest_add(ctx, key, values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    TDigest.handle("TDIGEST.ADD", args, store)
  end

  @spec tdigest_quantile(FerricStore.Instance.t(), binary(), [number()]) ::
          {:ok, term()} | {:error, binary()}
  def tdigest_quantile(ctx, key, quantiles) do
    store = build_store(ctx)
    args = [key | Enum.map(quantiles, &to_string/1)]
    result = TDigest.handle("TDIGEST.QUANTILE", args, store)
    wrap_result(result)
  end

  @spec tdigest_cdf(FerricStore.Instance.t(), binary(), [number()]) ::
          {:ok, term()} | {:error, binary()}
  def tdigest_cdf(ctx, key, values) do
    store = build_store(ctx)
    args = [key | Enum.map(values, &to_string/1)]
    result = TDigest.handle("TDIGEST.CDF", args, store)
    wrap_result(result)
  end

  @spec tdigest_min(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_min(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle("TDIGEST.MIN", [key], store)
    wrap_result(result)
  end

  @spec tdigest_max(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_max(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle("TDIGEST.MAX", [key], store)
    wrap_result(result)
  end

  @spec tdigest_info(FerricStore.Instance.t(), binary()) :: {:ok, term()} | {:error, binary()}
  def tdigest_info(ctx, key) do
    store = build_store(ctx)
    result = TDigest.handle("TDIGEST.INFO", [key], store)
    wrap_result(result)
  end

  @spec tdigest_reset(FerricStore.Instance.t(), binary()) :: :ok | {:error, binary()}
  def tdigest_reset(ctx, key) do
    store = build_store(ctx)
    TDigest.handle("TDIGEST.RESET", [key], store)
  end

  # ---------------------------------------------------------------
  # Server / utility
  # ---------------------------------------------------------------

  @spec keys(FerricStore.Instance.t(), keyword()) :: {:ok, [binary()]}
  def keys(ctx, _opts \\ []) do
    {:ok, Router.keys(ctx)}
  end

  @spec dbsize(FerricStore.Instance.t()) :: {:ok, non_neg_integer()}
  def dbsize(ctx) do
    {:ok, Router.dbsize(ctx)}
  end

  @spec flushdb(FerricStore.Instance.t()) :: :ok | {:error, term()}
  def flushdb(ctx) do
    Router.keys(ctx) |> Enum.each(&Router.delete(ctx, &1))
    # Clean prob dirs across all shards. Fsync each prob dir after the
    # rm loop so the removals survive a crash.
    Enum.reduce_while(0..(ctx.shard_count - 1), :ok, fn i, :ok ->
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, i)
      prob_dir = Path.join(shard_path, "prob")

      case clear_prob_dir(prob_dir) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp clear_prob_dir(prob_dir) do
    case Ferricstore.FS.ls(prob_dir) do
      {:ok, []} ->
        :ok

      {:ok, files} ->
        with :ok <- delete_prob_files(prob_dir, files),
             :ok <- prob_fsync_dir(prob_dir, :flush_prob_dir) do
          :ok
        end

      {:error, _reason} ->
        :ok
    end
  end

  defp delete_prob_files(prob_dir, files) do
    Enum.reduce_while(files, :ok, fn file, :ok ->
      case Ferricstore.FS.rm(Path.join(prob_dir, file)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:delete_prob_file_failed, file, reason}}}
      end
    end)
  end

  defp prob_fsync_dir(path, phase) do
    case prob_fsync_dir_result(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  defp prob_fsync_dir_result(path) do
    case Process.get(:ferricstore_prob_command_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end
  end

  # ---------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------

  defp build_store(ctx) do
    %{
      get: fn key -> Router.get(ctx, key) end,
      get_meta: fn key -> Router.get_meta(ctx, key) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      expire_at_ms: fn key -> Router.expire_at_ms(ctx, key) end,
      value_size: fn key -> Router.value_size(ctx, key) end,
      put: fn key, value, exp -> Router.put(ctx, key, value, exp) end,
      delete: fn key -> Router.delete(ctx, key) end,
      exists?: fn key -> Router.exists?(ctx, key) end,
      keys: fn -> Router.keys(ctx) end,
      flush: fn -> flushdb(ctx) end,
      dbsize: fn -> Router.dbsize(ctx) end,
      incr: fn key, delta -> Router.incr(ctx, key, delta) end,
      incr_float: fn key, delta -> Router.incr_float(ctx, key, delta) end,
      append: fn key, suffix -> Router.append(ctx, key, suffix) end,
      getset: fn key, value -> Router.getset(ctx, key, value) end,
      getdel: fn key -> Router.getdel(ctx, key) end,
      getex: fn key, exp -> Router.getex(ctx, key, exp) end,
      setrange: fn key, offset, value -> Router.setrange(ctx, key, offset, value) end,
      cas: fn key, exp, new_val, ttl -> Router.cas(ctx, key, exp, new_val, ttl) end,
      lock: fn key, owner, ttl -> Router.lock(ctx, key, owner, ttl) end,
      unlock: fn key, owner -> Router.unlock(ctx, key, owner) end,
      extend: fn key, owner, ttl -> Router.extend(ctx, key, owner, ttl) end,
      ratelimit_add: fn key, w, m, c -> Router.ratelimit_add(ctx, key, w, m, c) end,
      list_op: fn key, op -> Router.list_op(ctx, key, op) end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end,
      prob_dir: fn ->
        # For compound store, use shard 0's prob dir as default
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        Path.join(shard_path, "prob")
      end,
      prob_dir_for_key: fn key ->
        idx = Router.shard_for(ctx, key)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        Path.join(shard_path, "prob")
      end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_scan: fn redis_key, prefix ->
        Router.compound_scan(ctx, redis_key, prefix)
      end,
      compound_count: fn redis_key, prefix ->
        Router.compound_count(ctx, redis_key, prefix)
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end

  defp build_prob_store(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    %{
      get: fn k -> Router.get(ctx, k) end,
      get_meta: fn k -> Router.get_meta(ctx, k) end,
      batch_get: fn keys -> Router.batch_get(ctx, keys) end,
      expire_at_ms: fn k -> Router.expire_at_ms(ctx, k) end,
      value_size: fn k -> Router.value_size(ctx, k) end,
      put: fn k, value, exp -> Router.put(ctx, k, value, exp) end,
      delete: fn k -> Router.delete(ctx, k) end,
      exists?: fn k -> Router.exists?(ctx, k) end,
      keys: fn -> Router.keys(ctx) end,
      prob_dir: fn -> Path.join(shard_path, "prob") end,
      prob_dir_for_key: fn k ->
        i = Router.shard_for(ctx, k)
        sp = Ferricstore.DataDir.shard_data_path(ctx.data_dir, i)
        Path.join(sp, "prob")
      end,
      prob_write: fn cmd -> Router.prob_write(ctx, cmd) end
    }
  end

  defp wrap_result({:error, _} = err), do: err
  defp wrap_result(result), do: {:ok, result}
end
