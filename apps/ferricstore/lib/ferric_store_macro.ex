defmodule FerricStore.Macro do
  @moduledoc "Generates the full FerricStore public API (get, set, del, hash, set, list, sorted set, probabilistic, etc.) for `use FerricStore` modules."

  @doc """
  Generates all FerricStore API functions for a module.

  Each generated function resolves the instance context via
  `__instance__/0` and delegates to `FerricStore.Impl`.

  Usage:

      defmodule MyApp.Cache do
        use FerricStore,
          data_dir: "/data/cache",
          shard_count: 4
      end

      MyApp.Cache.set("key", "value")
      {:ok, "value"} = MyApp.Cache.get("key")
  """

  defmacro __using__(opts) do
    # credo:disable-for-next-line Credo.Check.Refactor.LongQuoteBlocks
    quote do
      @ferricstore_opts unquote(opts)

      def child_spec(overrides \\ []) do
        opts = Keyword.merge(@ferricstore_opts, overrides)

        %{
          id: __MODULE__,
          start: {FerricStore.Instance.Supervisor, :start_link, [__MODULE__, opts]},
          type: :supervisor
        }
      end

      def start_link(overrides \\ []) do
        opts = Keyword.merge(@ferricstore_opts, overrides)
        FerricStore.Instance.Supervisor.start_link(__MODULE__, opts)
      end

      def stop do
        name = :"#{__MODULE__}.Supervisor"

        if pid = Process.whereis(name) do
          try do
            Supervisor.stop(pid)
          catch
            :exit,
            {{:shutdown, {:sys, :terminate, [^pid, :normal, :infinity]}},
             {GenServer, :stop, [^pid, :normal, :infinity]}} ->
              :ok

            :exit, {:noproc, {GenServer, :stop, [^pid, :normal, :infinity]}} ->
              :ok
          end

          FerricStore.Instance.cleanup(__MODULE__)
        end

        :ok
      end

      @doc false
      def __instance__ do
        FerricStore.Instance.get(__MODULE__)
      end

      # ---------------------------------------------------------------
      # Core key-value operations
      # ---------------------------------------------------------------

      def set(key, value, opts \\ []) do
        FerricStore.Impl.set(__instance__(), key, value, opts)
      end

      def get(key, opts \\ []) do
        FerricStore.Impl.get(__instance__(), key, opts)
      end

      def del(key) when is_binary(key), do: del([key])

      def del(keys) when is_list(keys) do
        FerricStore.Impl.del(__instance__(), keys)
      end

      def exists?(key) do
        FerricStore.Impl.exists?(__instance__(), key)
      end

      def incr(key), do: incr_by(key, 1)
      def decr(key), do: incr_by(key, -1)
      def decr_by(key, amount) when is_integer(amount), do: incr_by(key, -amount)

      def incr_by(key, amount) when is_integer(amount) do
        FerricStore.Impl.incr(__instance__(), key, amount)
      end

      def incr_by_float(key, amount) when is_number(amount) do
        FerricStore.Impl.incr_float(__instance__(), key, amount)
      end

      def mget(keys) when is_list(keys) do
        FerricStore.Impl.mget(__instance__(), keys)
      end

      def mset(pairs) when is_map(pairs) do
        FerricStore.Impl.mset(__instance__(), pairs)
      end

      def append(key, suffix) do
        FerricStore.Impl.append(__instance__(), key, suffix)
      end

      def strlen(key) do
        FerricStore.Impl.strlen(__instance__(), key)
      end

      def getset(key, value) do
        FerricStore.Impl.getset(__instance__(), key, value)
      end

      def getdel(key) do
        FerricStore.Impl.getdel(__instance__(), key)
      end

      def getex(key, opts \\ []) do
        FerricStore.Impl.getex(__instance__(), key, opts)
      end

      def setnx(key, value) do
        FerricStore.Impl.setnx(__instance__(), key, value)
      end

      def setex(key, seconds, value) do
        FerricStore.Impl.setex(__instance__(), key, seconds, value)
      end

      def psetex(key, milliseconds, value) do
        FerricStore.Impl.psetex(__instance__(), key, milliseconds, value)
      end

      def getrange(key, start, stop) do
        FerricStore.Impl.getrange(__instance__(), key, start, stop)
      end

      def setrange(key, offset, value) do
        FerricStore.Impl.setrange(__instance__(), key, offset, value)
      end

      # ---------------------------------------------------------------
      # TTL / expiry
      # ---------------------------------------------------------------

      def expire(key, seconds) do
        FerricStore.Impl.expire(__instance__(), key, seconds)
      end

      def pexpire(key, milliseconds) do
        FerricStore.Impl.pexpire(__instance__(), key, milliseconds)
      end

      def ttl(key) do
        FerricStore.Impl.ttl(__instance__(), key)
      end

      def pttl(key) do
        FerricStore.Impl.pttl(__instance__(), key)
      end

      def persist(key) do
        FerricStore.Impl.persist(__instance__(), key)
      end

      # ---------------------------------------------------------------
      # Hash
      # ---------------------------------------------------------------

      def hset(key, fields) when is_map(fields) do
        FerricStore.Impl.hset(__instance__(), key, fields)
      end

      def hget(key, field) do
        FerricStore.Impl.hget(__instance__(), key, field)
      end

      def hgetall(key) do
        FerricStore.Impl.hgetall(__instance__(), key)
      end

      def hdel(key, fields) when is_list(fields) do
        FerricStore.Impl.hdel(__instance__(), key, fields)
      end

      def hexists(key, field) do
        FerricStore.Impl.hexists(__instance__(), key, field)
      end

      def hlen(key) do
        FerricStore.Impl.hlen(__instance__(), key)
      end

      def hincrby(key, field, amount) do
        FerricStore.Impl.hincrby(__instance__(), key, field, amount)
      end

      # ---------------------------------------------------------------
      # Set
      # ---------------------------------------------------------------

      def sadd(key, members) when is_list(members) do
        FerricStore.Impl.sadd(__instance__(), key, members)
      end

      def srem(key, members) when is_list(members) do
        FerricStore.Impl.srem(__instance__(), key, members)
      end

      def smembers(key) do
        FerricStore.Impl.smembers(__instance__(), key)
      end

      def sismember(key, member) do
        FerricStore.Impl.sismember(__instance__(), key, member)
      end

      def scard(key) do
        FerricStore.Impl.scard(__instance__(), key)
      end

      def spop(key, count \\ 1) do
        FerricStore.Impl.spop(__instance__(), key, count)
      end

      # ---------------------------------------------------------------
      # List
      # ---------------------------------------------------------------

      def lpush(key, values) when is_list(values) do
        FerricStore.Impl.lpush(__instance__(), key, values)
      end

      def rpush(key, values) when is_list(values) do
        FerricStore.Impl.rpush(__instance__(), key, values)
      end

      def lpop(key, count \\ 1) do
        FerricStore.Impl.lpop(__instance__(), key, count)
      end

      def rpop(key, count \\ 1) do
        FerricStore.Impl.rpop(__instance__(), key, count)
      end

      def lrange(key, start, stop) do
        FerricStore.Impl.lrange(__instance__(), key, start, stop)
      end

      def llen(key) do
        FerricStore.Impl.llen(__instance__(), key)
      end

      # ---------------------------------------------------------------
      # Sorted Set
      # ---------------------------------------------------------------

      def zadd(key, members) when is_list(members) do
        FerricStore.Impl.zadd(__instance__(), key, members)
      end

      def zcard(key) do
        FerricStore.Impl.zcard(__instance__(), key)
      end

      def zscore(key, member) do
        FerricStore.Impl.zscore(__instance__(), key, member)
      end

      def zrange(key, start, stop, opts \\ []) do
        FerricStore.Impl.zrange(__instance__(), key, start, stop, opts)
      end

      def zrem(key, members) when is_list(members) do
        FerricStore.Impl.zrem(__instance__(), key, members)
      end

      # ---------------------------------------------------------------
      # Probabilistic: Bloom
      # ---------------------------------------------------------------

      def bf_reserve(key, error_rate, capacity) do
        FerricStore.Impl.bf_reserve(__instance__(), key, error_rate, capacity)
      end

      def bf_add(key, element) do
        FerricStore.Impl.bf_add(__instance__(), key, element)
      end

      def bf_madd(key, elements) when is_list(elements) do
        FerricStore.Impl.bf_madd(__instance__(), key, elements)
      end

      def bf_exists(key, element) do
        FerricStore.Impl.bf_exists(__instance__(), key, element)
      end

      def bf_mexists(key, elements) when is_list(elements) do
        FerricStore.Impl.bf_mexists(__instance__(), key, elements)
      end

      def bf_card(key) do
        FerricStore.Impl.bf_card(__instance__(), key)
      end

      def bf_info(key) do
        FerricStore.Impl.bf_info(__instance__(), key)
      end

      # ---------------------------------------------------------------
      # Probabilistic: CMS
      # ---------------------------------------------------------------

      def cms_initbydim(key, width, depth) do
        FerricStore.Impl.cms_initbydim(__instance__(), key, width, depth)
      end

      def cms_initbyprob(key, error, probability) do
        FerricStore.Impl.cms_initbyprob(__instance__(), key, error, probability)
      end

      def cms_incrby(key, pairs) when is_list(pairs) do
        FerricStore.Impl.cms_incrby(__instance__(), key, pairs)
      end

      def cms_query(key, elements) when is_list(elements) do
        FerricStore.Impl.cms_query(__instance__(), key, elements)
      end

      def cms_info(key) do
        FerricStore.Impl.cms_info(__instance__(), key)
      end

      def cms_merge(dest, sources, opts \\ []) do
        FerricStore.Impl.cms_merge(__instance__(), dest, sources, opts)
      end

      # ---------------------------------------------------------------
      # Probabilistic: Cuckoo
      # ---------------------------------------------------------------

      def cf_reserve(key, capacity) do
        FerricStore.Impl.cf_reserve(__instance__(), key, capacity)
      end

      def cf_add(key, element) do
        FerricStore.Impl.cf_add(__instance__(), key, element)
      end

      def cf_addnx(key, element) do
        FerricStore.Impl.cf_addnx(__instance__(), key, element)
      end

      def cf_del(key, element) do
        FerricStore.Impl.cf_del(__instance__(), key, element)
      end

      def cf_exists(key, element) do
        FerricStore.Impl.cf_exists(__instance__(), key, element)
      end

      def cf_mexists(key, elements) when is_list(elements) do
        FerricStore.Impl.cf_mexists(__instance__(), key, elements)
      end

      def cf_count(key, element) do
        FerricStore.Impl.cf_count(__instance__(), key, element)
      end

      def cf_info(key) do
        FerricStore.Impl.cf_info(__instance__(), key)
      end

      # ---------------------------------------------------------------
      # Probabilistic: TopK
      # ---------------------------------------------------------------

      def topk_reserve(key, k) do
        FerricStore.Impl.topk_reserve(__instance__(), key, k)
      end

      def topk_add(key, elements) when is_list(elements) do
        FerricStore.Impl.topk_add(__instance__(), key, elements)
      end

      def topk_query(key, elements) when is_list(elements) do
        FerricStore.Impl.topk_query(__instance__(), key, elements)
      end

      def topk_list(key) do
        FerricStore.Impl.topk_list(__instance__(), key)
      end

      def topk_info(key) do
        FerricStore.Impl.topk_info(__instance__(), key)
      end

      # ---------------------------------------------------------------
      # Probabilistic: TDigest
      # ---------------------------------------------------------------

      def tdigest_create(key, opts \\ []) do
        FerricStore.Impl.tdigest_create(__instance__(), key, opts)
      end

      def tdigest_add(key, values) when is_list(values) do
        FerricStore.Impl.tdigest_add(__instance__(), key, values)
      end

      def tdigest_quantile(key, quantiles) when is_list(quantiles) do
        FerricStore.Impl.tdigest_quantile(__instance__(), key, quantiles)
      end

      def tdigest_cdf(key, values) when is_list(values) do
        FerricStore.Impl.tdigest_cdf(__instance__(), key, values)
      end

      def tdigest_min(key), do: FerricStore.Impl.tdigest_min(__instance__(), key)
      def tdigest_max(key), do: FerricStore.Impl.tdigest_max(__instance__(), key)
      def tdigest_info(key), do: FerricStore.Impl.tdigest_info(__instance__(), key)
      def tdigest_reset(key), do: FerricStore.Impl.tdigest_reset(__instance__(), key)

      # ---------------------------------------------------------------
      # Flow
      # ---------------------------------------------------------------

      def flow_create(id, opts) do
        FerricStore.Impl.flow_create(__instance__(), id, opts)
      end

      def flow_create_many(partition_key, items, opts \\ []) do
        FerricStore.Impl.flow_create_many(__instance__(), partition_key, items, opts)
      end

      def flow_spawn_children(parent_id, children, opts \\ []) do
        FerricStore.Impl.flow_spawn_children(__instance__(), parent_id, children, opts)
      end

      def flow_get(id, opts \\ []) do
        FerricStore.Impl.flow_get(__instance__(), id, opts)
      end

      def flow_claim_due(type, opts) do
        FerricStore.Impl.flow_claim_due(__instance__(), type, opts)
      end

      def flow_reclaim(type, opts) do
        FerricStore.Impl.flow_reclaim(__instance__(), type, opts)
      end

      def flow_complete(id, lease_token, opts \\ []) do
        FerricStore.Impl.flow_complete(__instance__(), id, lease_token, opts)
      end

      def flow_transition(id, from_state, to_state, opts \\ []) do
        FerricStore.Impl.flow_transition(__instance__(), id, from_state, to_state, opts)
      end

      def flow_transition_many(partition_key, from_state, to_state, items, opts \\ []) do
        FerricStore.Impl.flow_transition_many(
          __instance__(),
          partition_key,
          from_state,
          to_state,
          items,
          opts
        )
      end

      def flow_retry(id, lease_token, opts) do
        FerricStore.Impl.flow_retry(__instance__(), id, lease_token, opts)
      end

      def flow_fail(id, lease_token, opts \\ []) do
        FerricStore.Impl.flow_fail(__instance__(), id, lease_token, opts)
      end

      def flow_cancel(id, opts \\ []) do
        FerricStore.Impl.flow_cancel(__instance__(), id, opts)
      end

      def flow_rewind(id, opts) do
        FerricStore.Impl.flow_rewind(__instance__(), id, opts)
      end

      def flow_list(type, opts \\ []) do
        FerricStore.Impl.flow_list(__instance__(), type, opts)
      end

      def flow_by_parent(parent_flow_id, opts \\ []) do
        FerricStore.Impl.flow_by_parent(__instance__(), parent_flow_id, opts)
      end

      def flow_by_root(root_flow_id, opts \\ []) do
        FerricStore.Impl.flow_by_root(__instance__(), root_flow_id, opts)
      end

      def flow_by_correlation(correlation_id, opts \\ []) do
        FerricStore.Impl.flow_by_correlation(__instance__(), correlation_id, opts)
      end

      def flow_info(type, opts \\ []) do
        FerricStore.Impl.flow_info(__instance__(), type, opts)
      end

      def flow_stuck(type, opts \\ []) do
        FerricStore.Impl.flow_stuck(__instance__(), type, opts)
      end

      def flow_history(id, opts \\ []) do
        FerricStore.Impl.flow_history(__instance__(), id, opts)
      end

      # ---------------------------------------------------------------
      # Server / utility
      # ---------------------------------------------------------------

      def keys(opts \\ []) do
        FerricStore.Impl.keys(__instance__(), opts)
      end

      def dbsize do
        FerricStore.Impl.dbsize(__instance__())
      end

      def flushdb do
        FerricStore.Impl.flushdb(__instance__())
      end

      def flushall, do: flushdb()
    end
  end
end
