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

      import FerricStore.API.PublicAccess, only: [defguardedinstance: 2]

      # ---------------------------------------------------------------
      # Core key-value operations
      # ---------------------------------------------------------------

      defguardedinstance(set(key, value, opts \\ []), to: FerricStore.Impl, keys: [key])
      defguardedinstance(get(key, opts \\ []), to: FerricStore.Impl, keys: [key])

      def del(key) when is_binary(key), do: del([key])

      def del(keys) when is_list(keys) do
        FerricStore.API.PublicAccess.call(keys, fn ->
          FerricStore.Impl.del(__instance__(), keys)
        end)
      end

      defguardedinstance(exists?(key), to: FerricStore.Impl, keys: [key])

      def incr(key), do: incr_by(key, 1)
      def decr(key), do: incr_by(key, -1)
      def decr_by(key, amount) when is_integer(amount), do: incr_by(key, -amount)

      def incr_by(key, amount) when is_integer(amount),
        do:
          FerricStore.API.PublicAccess.call([key], fn ->
            FerricStore.Impl.incr(__instance__(), key, amount)
          end)

      def incr_by_float(key, amount) when is_number(amount),
        do:
          FerricStore.API.PublicAccess.call([key], fn ->
            FerricStore.Impl.incr_float(__instance__(), key, amount)
          end)

      def mget(keys) when is_list(keys) do
        FerricStore.API.PublicAccess.call(keys, fn ->
          FerricStore.Impl.mget(__instance__(), keys)
        end)
      end

      def mset(pairs) when is_map(pairs) do
        FerricStore.API.PublicAccess.call(Map.keys(pairs), fn ->
          FerricStore.Impl.mset(__instance__(), pairs)
        end)
      end

      defguardedinstance(append(key, suffix), to: FerricStore.Impl, keys: [key])
      defguardedinstance(strlen(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(getset(key, value), to: FerricStore.Impl, keys: [key])
      defguardedinstance(getdel(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(getex(key, opts \\ []), to: FerricStore.Impl, keys: [key])
      defguardedinstance(setnx(key, value), to: FerricStore.Impl, keys: [key])
      defguardedinstance(setex(key, seconds, value), to: FerricStore.Impl, keys: [key])
      defguardedinstance(psetex(key, milliseconds, value), to: FerricStore.Impl, keys: [key])
      defguardedinstance(getrange(key, start, stop), to: FerricStore.Impl, keys: [key])
      defguardedinstance(setrange(key, offset, value), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # TTL / expiry
      # ---------------------------------------------------------------

      defguardedinstance(expire(key, seconds), to: FerricStore.Impl, keys: [key])
      defguardedinstance(pexpire(key, milliseconds), to: FerricStore.Impl, keys: [key])
      defguardedinstance(ttl(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(pttl(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(persist(key), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Hash
      # ---------------------------------------------------------------

      def hset(key, fields) when is_map(fields) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.hset(__instance__(), key, fields)
        end)
      end

      defguardedinstance(hget(key, field), to: FerricStore.Impl, keys: [key])
      defguardedinstance(hgetall(key), to: FerricStore.Impl, keys: [key])

      def hdel(key, fields) when is_list(fields) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.hdel(__instance__(), key, fields)
        end)
      end

      defguardedinstance(hexists(key, field), to: FerricStore.Impl, keys: [key])
      defguardedinstance(hlen(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(hincrby(key, field, amount), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Set
      # ---------------------------------------------------------------

      def sadd(key, members) when is_list(members) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.sadd(__instance__(), key, members)
        end)
      end

      def srem(key, members) when is_list(members) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.srem(__instance__(), key, members)
        end)
      end

      defguardedinstance(smembers(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(sismember(key, member), to: FerricStore.Impl, keys: [key])
      defguardedinstance(scard(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(spop(key, count \\ 1), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # List
      # ---------------------------------------------------------------

      def lpush(key, values) when is_list(values) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.lpush(__instance__(), key, values)
        end)
      end

      def rpush(key, values) when is_list(values) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.rpush(__instance__(), key, values)
        end)
      end

      defguardedinstance(lpop(key, count \\ 1), to: FerricStore.Impl, keys: [key])
      defguardedinstance(rpop(key, count \\ 1), to: FerricStore.Impl, keys: [key])
      defguardedinstance(lrange(key, start, stop), to: FerricStore.Impl, keys: [key])
      defguardedinstance(llen(key), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Sorted Set
      # ---------------------------------------------------------------

      def zadd(key, members) when is_list(members) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.zadd(__instance__(), key, members)
        end)
      end

      defguardedinstance(zcard(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(zscore(key, member), to: FerricStore.Impl, keys: [key])
      defguardedinstance(zrange(key, start, stop, opts \\ []), to: FerricStore.Impl, keys: [key])

      def zrem(key, members) when is_list(members) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.zrem(__instance__(), key, members)
        end)
      end

      # ---------------------------------------------------------------
      # Probabilistic: Bloom
      # ---------------------------------------------------------------

      defguardedinstance(bf_reserve(key, error_rate, capacity),
        to: FerricStore.Impl,
        keys: [key]
      )

      defguardedinstance(bf_add(key, element), to: FerricStore.Impl, keys: [key])

      def bf_madd(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.bf_madd(__instance__(), key, elements)
        end)
      end

      defguardedinstance(bf_exists(key, element), to: FerricStore.Impl, keys: [key])

      def bf_mexists(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.bf_mexists(__instance__(), key, elements)
        end)
      end

      defguardedinstance(bf_card(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(bf_info(key), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Probabilistic: CMS
      # ---------------------------------------------------------------

      defguardedinstance(cms_initbydim(key, width, depth),
        to: FerricStore.Impl,
        keys: [key]
      )

      defguardedinstance(cms_initbyprob(key, error, probability),
        to: FerricStore.Impl,
        keys: [key]
      )

      def cms_incrby(key, pairs) when is_list(pairs) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.cms_incrby(__instance__(), key, pairs)
        end)
      end

      def cms_query(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.cms_query(__instance__(), key, elements)
        end)
      end

      defguardedinstance(cms_info(key), to: FerricStore.Impl, keys: [key])

      def cms_merge(dest, sources, opts \\ []) do
        FerricStore.API.PublicAccess.call([dest | sources], fn ->
          FerricStore.Impl.cms_merge(__instance__(), dest, sources, opts)
        end)
      end

      # ---------------------------------------------------------------
      # Probabilistic: Cuckoo
      # ---------------------------------------------------------------

      defguardedinstance(cf_reserve(key, capacity), to: FerricStore.Impl, keys: [key])
      defguardedinstance(cf_add(key, element), to: FerricStore.Impl, keys: [key])
      defguardedinstance(cf_addnx(key, element), to: FerricStore.Impl, keys: [key])
      defguardedinstance(cf_del(key, element), to: FerricStore.Impl, keys: [key])
      defguardedinstance(cf_exists(key, element), to: FerricStore.Impl, keys: [key])

      def cf_mexists(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.cf_mexists(__instance__(), key, elements)
        end)
      end

      defguardedinstance(cf_count(key, element), to: FerricStore.Impl, keys: [key])
      defguardedinstance(cf_info(key), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Probabilistic: TopK
      # ---------------------------------------------------------------

      defguardedinstance(topk_reserve(key, k), to: FerricStore.Impl, keys: [key])

      def topk_add(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.topk_add(__instance__(), key, elements)
        end)
      end

      def topk_query(key, elements) when is_list(elements) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.topk_query(__instance__(), key, elements)
        end)
      end

      defguardedinstance(topk_list(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(topk_info(key), to: FerricStore.Impl, keys: [key])

      # ---------------------------------------------------------------
      # Probabilistic: TDigest
      # ---------------------------------------------------------------

      defguardedinstance(tdigest_create(key, opts \\ []), to: FerricStore.Impl, keys: [key])

      def tdigest_add(key, values) when is_list(values) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.tdigest_add(__instance__(), key, values)
        end)
      end

      def tdigest_quantile(key, quantiles) when is_list(quantiles) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.tdigest_quantile(__instance__(), key, quantiles)
        end)
      end

      def tdigest_cdf(key, values) when is_list(values) do
        FerricStore.API.PublicAccess.call([key], fn ->
          FerricStore.Impl.tdigest_cdf(__instance__(), key, values)
        end)
      end

      defguardedinstance(tdigest_min(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(tdigest_max(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(tdigest_info(key), to: FerricStore.Impl, keys: [key])
      defguardedinstance(tdigest_reset(key), to: FerricStore.Impl, keys: [key])

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

      def flow_policy_set(type, opts) do
        FerricStore.Impl.flow_policy_set(__instance__(), type, opts)
      end

      def flow_policy_get(type, opts \\ []) do
        FerricStore.Impl.flow_policy_get(__instance__(), type, opts)
      end

      def flow_claim_due(type, opts) do
        FerricStore.Impl.flow_claim_due(__instance__(), type, opts)
      end

      def flow_reclaim(type, opts) do
        FerricStore.Impl.flow_reclaim(__instance__(), type, opts)
      end

      def flow_extend_lease(id, lease_token, opts \\ []) do
        FerricStore.Impl.flow_extend_lease(__instance__(), id, lease_token, opts)
      end

      def flow_complete(id, lease_token, opts \\ []) do
        FerricStore.Impl.flow_complete(__instance__(), id, lease_token, opts)
      end

      def flow_transition(id, from_state, to_state, opts \\ []) do
        FerricStore.Impl.flow_transition(__instance__(), id, from_state, to_state, opts)
      end

      def flow_schedule_create(id, opts) do
        FerricStore.Impl.flow_schedule_create(__instance__(), id, opts)
      end

      def flow_schedule_get(id, opts \\ []) do
        FerricStore.Impl.flow_schedule_get(__instance__(), id, opts)
      end

      def flow_schedule_fire(id, opts \\ []) do
        FerricStore.Impl.flow_schedule_fire(__instance__(), id, opts)
      end

      def flow_schedule_pause(id, opts \\ []) do
        FerricStore.Impl.flow_schedule_pause(__instance__(), id, opts)
      end

      def flow_schedule_resume(id, opts \\ []) do
        FerricStore.Impl.flow_schedule_resume(__instance__(), id, opts)
      end

      def flow_schedule_list(opts \\ []) do
        FerricStore.Impl.flow_schedule_list(__instance__(), opts)
      end

      def flow_schedule_delete(id, opts \\ []) do
        FerricStore.Impl.flow_schedule_delete(__instance__(), id, opts)
      end

      def flow_schedule_fire_due(opts \\ []) do
        FerricStore.Impl.flow_schedule_fire_due(__instance__(), opts)
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

      def flow_retention_cleanup(opts \\ []) do
        FerricStore.Impl.flow_retention_cleanup(__instance__(), opts)
      end

      def flow_rewind(id, opts) do
        FerricStore.Impl.flow_rewind(__instance__(), id, opts)
      end

      def flow_list(type, opts \\ []) do
        FerricStore.Impl.flow_list(__instance__(), type, opts)
      end

      def flow_search(opts \\ []) do
        FerricStore.Impl.flow_search(__instance__(), opts)
      end

      def flow_attributes(type, opts \\ []) do
        FerricStore.Impl.flow_attributes(__instance__(), type, opts)
      end

      def flow_attribute_values(type, attr_name, opts \\ []) do
        FerricStore.Impl.flow_attribute_values(__instance__(), type, attr_name, opts)
      end

      def flow_effect_reserve(id, effect_key, effect_type, opts \\ []) do
        FerricStore.Impl.flow_effect_reserve(
          __instance__(),
          id,
          effect_key,
          effect_type,
          opts
        )
      end

      def flow_effect_confirm(id, effect_key, opts \\ []) do
        FerricStore.Impl.flow_effect_confirm(__instance__(), id, effect_key, opts)
      end

      def flow_effect_fail(id, effect_key, opts \\ []) do
        FerricStore.Impl.flow_effect_fail(__instance__(), id, effect_key, opts)
      end

      def flow_effect_compensate(id, effect_key, opts \\ []) do
        FerricStore.Impl.flow_effect_compensate(__instance__(), id, effect_key, opts)
      end

      def flow_effect_get(id, effect_key, opts \\ []) do
        FerricStore.Impl.flow_effect_get(__instance__(), id, effect_key, opts)
      end

      def flow_governance_ledger(id, opts \\ []) do
        FerricStore.Impl.flow_governance_ledger(__instance__(), id, opts)
      end

      def flow_approval_request(id, opts \\ []) do
        FerricStore.Impl.flow_approval_request(__instance__(), id, opts)
      end

      def flow_approval_approve(id, opts \\ []) do
        FerricStore.Impl.flow_approval_approve(__instance__(), id, opts)
      end

      def flow_approval_reject(id, opts \\ []) do
        FerricStore.Impl.flow_approval_reject(__instance__(), id, opts)
      end

      def flow_approval_get(id, opts \\ []) do
        FerricStore.Impl.flow_approval_get(__instance__(), id, opts)
      end

      def flow_approval_list(opts \\ []) do
        FerricStore.Impl.flow_approval_list(__instance__(), opts)
      end

      def flow_governance_overview(opts \\ []) do
        FerricStore.Impl.flow_governance_overview(__instance__(), opts)
      end

      def flow_circuit_open(scope, opts \\ []) do
        FerricStore.Impl.flow_circuit_open(__instance__(), scope, opts)
      end

      def flow_circuit_close(scope, opts \\ []) do
        FerricStore.Impl.flow_circuit_close(__instance__(), scope, opts)
      end

      def flow_circuit_get(scope, opts \\ []) do
        FerricStore.Impl.flow_circuit_get(__instance__(), scope, opts)
      end

      def flow_circuit_list(opts \\ []) do
        FerricStore.Impl.flow_circuit_list(__instance__(), opts)
      end

      def flow_budget_reserve(scope, amount, opts \\ []) do
        FerricStore.Impl.flow_budget_reserve(__instance__(), scope, amount, opts)
      end

      def flow_budget_commit(scope, reservation_id, actual_amount, opts \\ []) do
        FerricStore.Impl.flow_budget_commit(
          __instance__(),
          scope,
          reservation_id,
          actual_amount,
          opts
        )
      end

      def flow_budget_release(scope, reservation_id, opts \\ []) do
        FerricStore.Impl.flow_budget_release(__instance__(), scope, reservation_id, opts)
      end

      def flow_budget_get(scope, opts \\ []) do
        FerricStore.Impl.flow_budget_get(__instance__(), scope, opts)
      end

      def flow_budget_list(opts \\ []) do
        FerricStore.Impl.flow_budget_list(__instance__(), opts)
      end

      def flow_limit_lease(scope, opts \\ []) do
        FerricStore.Impl.flow_limit_lease(__instance__(), scope, opts)
      end

      def flow_limit_spend(scope, opts \\ []) do
        FerricStore.Impl.flow_limit_spend(__instance__(), scope, opts)
      end

      def flow_limit_release(scope, opts \\ []) do
        FerricStore.Impl.flow_limit_release(__instance__(), scope, opts)
      end

      def flow_limit_get(scope, opts \\ []) do
        FerricStore.Impl.flow_limit_get(__instance__(), scope, opts)
      end

      def flow_limit_list(opts \\ []) do
        FerricStore.Impl.flow_limit_list(__instance__(), opts)
      end

      def flow_stats(type, opts \\ []) do
        FerricStore.Impl.flow_stats(__instance__(), type, opts)
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
