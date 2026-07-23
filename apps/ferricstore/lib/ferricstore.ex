defmodule FerricStore do
  @moduledoc """
  Module-based cache instances for FerricStore.

  Each module that calls `use FerricStore` gets a local direct cache instance
  with its own shards, ETS tables, data directory, and config. The default
  application instance owns the Raft system.

  ## Usage

      defmodule MyApp.Cache do
        use FerricStore,
          data_dir: "/data/cache",
          shard_count: 4,
          max_memory: "1GB"
      end

      # In your supervision tree:
      children = [MyApp.Cache]

      # Then use it:
      MyApp.Cache.set("key", "value")
      {:ok, "value"} = MyApp.Cache.get("key")

  ## Multiple instances

      defmodule MyApp.Sessions do
        use FerricStore,
          data_dir: "/data/sessions",
          shard_count: 2
      end

      MyApp.Cache.set("page:home", html)
      MyApp.Sessions.set("sess:abc", session_data)

  ## Options

    * `:data_dir` — base directory for Bitcask data files (required)
    * `:shard_count` — number of shards (default: 4)
    * `:max_memory_bytes` — maximum memory budget (default: 1GB)
    * `:keydir_max_ram` — maximum ETS keydir memory (default: 256MB)
    * `:eviction_policy` — `:volatile_lfu` | `:allkeys_lfu` | `:noeviction` (default: `:volatile_lfu`)
    * `:hot_cache_max_value_size` — max value size for ETS caching (default: 65536)
    * `:read_sample_rate` — LFU sampling rate (default: 100)
    * `:flow_retention_sweeper` — per-instance Flow timeout/retention sweeper
      options such as `:initial_delay_ms`, `:interval_ms`, and `:limit`
  """

  defmacro __using__(opts) do
    quote do
      use FerricStore.Macro, unquote(opts)
    end
  end

  # ---------------------------------------------------------------------------
  # Readiness
  # ---------------------------------------------------------------------------

  @doc """
  Blocks until FerricStore is fully ready to serve requests.

  Polls `Health.check/0` until all shards are alive and all Raft leaders
  are elected. Returns `:ok` when ready, raises on timeout.

  Call this in your application's `start/2` after FerricStore is in your
  supervision tree, or in test setup, to ensure writes won't fail.

  ## Options

    * `:timeout` - max milliseconds to wait (default: 30_000)
    * `:interval` - polling interval in ms (default: 100)

  ## Examples

      # In your Application.start/2:
      def start(_type, _args) do
        children = [
          {FerricStore, []},
          MyApp.Repo,
          MyAppWeb.Endpoint
        ]
        opts = [strategy: :one_for_one, name: MyApp.Supervisor]
        {:ok, pid} = Supervisor.start_link(children, opts)

        FerricStore.await_ready()
        {:ok, pid}
      end

      # With custom timeout:
      FerricStore.await_ready(timeout: 60_000)

  """
  @spec await_ready(keyword()) :: :ok
  def await_ready(opts \\ []) do
    {timeout, interval} = validate_await_ready_options!(opts)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_ready(deadline, interval)
  end

  defp validate_await_ready_options!(opts) when is_list(opts) do
    case Keyword.validate(opts, timeout: 30_000, interval: 100) do
      {:ok, validated} ->
        timeout = Keyword.fetch!(validated, :timeout)
        interval = Keyword.fetch!(validated, :interval)

        if is_integer(timeout) and timeout >= 0 and is_integer(interval) and interval > 0 do
          {timeout, interval}
        else
          raise ArgumentError,
                "await_ready options require a non-negative integer timeout and positive integer interval"
        end

      {:error, invalid} ->
        raise ArgumentError, "invalid await_ready options: #{inspect(invalid)}"
    end
  end

  defp validate_await_ready_options!(_opts) do
    raise ArgumentError, "await_ready options must be a keyword list"
  end

  defp do_await_ready(deadline, interval) do
    case Ferricstore.Health.check() do
      %{status: :ok} ->
        :ok

      _ ->
        remaining = deadline - System.monotonic_time(:millisecond)

        if remaining <= 0 do
          raise "FerricStore not ready within timeout. Check shard health with FerricStore.health()"
        end

        Process.sleep(min(interval, remaining))
        do_await_ready(deadline, interval)
    end
  end

  @doc """
  Returns the current health status without blocking.

  ## Examples

      iex> FerricStore.health()
      %{status: :ok, shard_count: 4, shards: [...], uptime_seconds: 120}

  """
  @spec health() :: Ferricstore.Health.health_result()
  def health do
    Ferricstore.Health.check()
  end

  @doc """
  Returns `true` if FerricStore is ready to serve requests.

  ## Examples

      iex> FerricStore.ready?()
      true

  """
  @spec ready?() :: boolean()
  def ready? do
    Ferricstore.Health.ready?()
  end

  @doc """
  Gracefully shuts down FerricStore, flushing all pending data to disk.

  Flushes Raft batchers, BitcaskWriters, shard pending writes, and
  triggers a WAL rollover. Call before stopping the application to
  ensure zero data loss.

  ## Examples

      FerricStore.shutdown()
      Application.stop(:ferricstore)

  """
  @spec shutdown() :: :ok
  def shutdown do
    Ferricstore.Application.prep_stop(nil)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @type key :: binary()
  @type value :: binary()
  @type write_error :: {:error, binary() | {:timeout, :unknown_outcome}}
  @type set_opts :: [
          ttl: non_neg_integer(),
          exat: pos_integer(),
          pxat: pos_integer(),
          nx: boolean(),
          xx: boolean(),
          get: boolean(),
          keepttl: boolean(),
          cache: atom()
        ]
  @type get_opts :: [cache: atom()]
  @type cas_opts :: [ttl: non_neg_integer()]
  @type fetch_or_compute_opts :: [ttl: pos_integer(), hint: binary()]
  @type zrange_opts :: [withscores: boolean()]

  # ---------------------------------------------------------------------------
  # Strings
  # ---------------------------------------------------------------------------

  # ---------------------------------------------------------------------------
  # Embedded FerricStore API
  # ---------------------------------------------------------------------------

  alias FerricStore.API.Bitmap, as: BitmapAPI
  alias FerricStore.API.Generic, as: GenericAPI
  alias FerricStore.API.Geo, as: GeoAPI
  alias FerricStore.API.Hashes, as: HashesAPI
  alias FerricStore.API.HyperLogLog, as: HyperLogLogAPI
  alias FerricStore.API.Lists, as: ListsAPI
  alias FerricStore.API.Locks, as: LocksAPI
  alias FerricStore.API.Probabilistic, as: ProbabilisticAPI
  alias FerricStore.API.Sets, as: SetsAPI
  alias FerricStore.API.SortedSets, as: SortedSetsAPI
  alias FerricStore.API.Streams, as: StreamsAPI
  alias FerricStore.API.Strings, as: StringsAPI
  alias FerricStore.API.System, as: SystemAPI
  import FerricStore.API.PublicAccess, only: [defguardeddelegate: 2]

  @spec set(key(), value(), set_opts()) :: :ok | {:ok, value() | nil} | nil | write_error()
  defguardeddelegate(set(key, value, opts \\ []), to: StringsAPI, keys: [key])
  defguardeddelegate(get(key, opts \\ []), to: StringsAPI, keys: [key])
  defguardeddelegate(del(key), to: StringsAPI, keys: FerricStore.API.PublicAccess.keys(key))
  defguardeddelegate(incr(key), to: StringsAPI, keys: [key])
  defguardeddelegate(decr(key), to: StringsAPI, keys: [key])
  defguardeddelegate(decr_by(key, amount), to: StringsAPI, keys: [key])
  defguardeddelegate(incr_by(key, amount), to: StringsAPI, keys: [key])
  defguardeddelegate(incr_by_float(key, amount), to: StringsAPI, keys: [key])
  defguardeddelegate(mget(keys), to: StringsAPI, keys: keys)

  defguardeddelegate(mset(pairs),
    to: StringsAPI,
    keys: FerricStore.API.PublicAccess.pair_keys(pairs)
  )

  defguardeddelegate(append(key, suffix), to: StringsAPI, keys: [key])
  defguardeddelegate(strlen(key), to: StringsAPI, keys: [key])
  defguardeddelegate(getset(key, value), to: StringsAPI, keys: [key])
  defguardeddelegate(getdel(key), to: StringsAPI, keys: [key])
  defguardeddelegate(getex(key, opts \\ []), to: StringsAPI, keys: [key])
  defguardeddelegate(setnx(key, value), to: StringsAPI, keys: [key])
  defguardeddelegate(setex(key, seconds, value), to: StringsAPI, keys: [key])
  defguardeddelegate(psetex(key, milliseconds, value), to: StringsAPI, keys: [key])
  defguardeddelegate(getrange(key, start, stop), to: StringsAPI, keys: [key])
  defguardeddelegate(setrange(key, offset, value), to: StringsAPI, keys: [key])

  defguardeddelegate(msetnx(pairs),
    to: StringsAPI,
    keys: FerricStore.API.PublicAccess.pair_keys(pairs)
  )

  defguardeddelegate(hset(key, fields), to: HashesAPI, keys: [key])
  defguardeddelegate(hget(key, field), to: HashesAPI, keys: [key])
  defguardeddelegate(hgetall(key), to: HashesAPI, keys: [key])
  defguardeddelegate(hdel(key, fields), to: HashesAPI, keys: [key])
  defguardeddelegate(hexists(key, field), to: HashesAPI, keys: [key])
  defguardeddelegate(hlen(key), to: HashesAPI, keys: [key])
  defguardeddelegate(hkeys(key), to: HashesAPI, keys: [key])
  defguardeddelegate(hvals(key), to: HashesAPI, keys: [key])
  defguardeddelegate(hmget(key, fields), to: HashesAPI, keys: [key])
  defguardeddelegate(hincrby(key, field, amount), to: HashesAPI, keys: [key])
  defguardeddelegate(hincrbyfloat(key, field, amount), to: HashesAPI, keys: [key])
  defguardeddelegate(hsetnx(key, field, value), to: HashesAPI, keys: [key])
  defguardeddelegate(hrandfield(key, count \\ nil), to: HashesAPI, keys: [key])
  defguardeddelegate(hstrlen(key, field), to: HashesAPI, keys: [key])

  defguardeddelegate(lpush(key, elements), to: ListsAPI, keys: [key])
  defguardeddelegate(rpush(key, elements), to: ListsAPI, keys: [key])
  defguardeddelegate(lpop(key, count \\ 1), to: ListsAPI, keys: [key])
  defguardeddelegate(rpop(key, count \\ 1), to: ListsAPI, keys: [key])
  defguardeddelegate(lrange(key, start, stop), to: ListsAPI, keys: [key])
  defguardeddelegate(llen(key), to: ListsAPI, keys: [key])
  defguardeddelegate(lindex(key, index), to: ListsAPI, keys: [key])
  defguardeddelegate(lset(key, index, element), to: ListsAPI, keys: [key])
  defguardeddelegate(lrem(key, count, element), to: ListsAPI, keys: [key])
  defguardeddelegate(linsert(key, direction, pivot, element), to: ListsAPI, keys: [key])

  defguardeddelegate(lmove(source, destination, from_dir, to_dir),
    to: ListsAPI,
    keys: [source, destination]
  )

  defguardeddelegate(lpos(key, element, opts \\ []), to: ListsAPI, keys: [key])

  defguardeddelegate(sadd(key, members), to: SetsAPI, keys: [key])
  defguardeddelegate(srem(key, members), to: SetsAPI, keys: [key])
  defguardeddelegate(smembers(key), to: SetsAPI, keys: [key])
  defguardeddelegate(sismember(key, member), to: SetsAPI, keys: [key])
  defguardeddelegate(scard(key), to: SetsAPI, keys: [key])
  defguardeddelegate(smismember(key, members), to: SetsAPI, keys: [key])
  defguardeddelegate(srandmember(key, count \\ nil), to: SetsAPI, keys: [key])
  defguardeddelegate(spop(key, count \\ nil), to: SetsAPI, keys: [key])
  defguardeddelegate(sdiff(keys), to: SetsAPI, keys: keys)
  defguardeddelegate(sinter(keys), to: SetsAPI, keys: keys)
  defguardeddelegate(sunion(keys), to: SetsAPI, keys: keys)

  defguardeddelegate(sdiffstore(destination, keys),
    to: SetsAPI,
    keys: FerricStore.API.PublicAccess.destination_keys(destination, keys)
  )

  defguardeddelegate(sinterstore(destination, keys),
    to: SetsAPI,
    keys: FerricStore.API.PublicAccess.destination_keys(destination, keys)
  )

  defguardeddelegate(sunionstore(destination, keys),
    to: SetsAPI,
    keys: FerricStore.API.PublicAccess.destination_keys(destination, keys)
  )

  defguardeddelegate(sintercard(keys, opts \\ []), to: SetsAPI, keys: keys)

  defguardeddelegate(zadd(key, score_member_pairs), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zrange(key, start, stop, opts \\ []), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zscore(key, member), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zcard(key), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zrem(key, members), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zrank(key, member), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zrevrank(key, member), to: SortedSetsAPI, keys: [key])

  defguardeddelegate(zrangebyscore(key, min, max, opts \\ []),
    to: SortedSetsAPI,
    keys: [key]
  )

  defguardeddelegate(zcount(key, min, max), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zincrby(key, increment, member), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zrandmember(key, count \\ nil), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zpopmin(key, count \\ 1), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zpopmax(key, count \\ 1), to: SortedSetsAPI, keys: [key])
  defguardeddelegate(zmscore(key, members), to: SortedSetsAPI, keys: [key])

  defguardeddelegate(cas(key, expected, new_value, opts \\ []), to: GenericAPI, keys: [key])
  defguardeddelegate(fetch_or_compute(key, opts), to: GenericAPI, keys: [key])

  defguardeddelegate(fetch_or_compute_result(key, value, opts),
    to: GenericAPI,
    keys: [key]
  )

  defguardeddelegate(fetch_or_compute_error(key, message, opts),
    to: GenericAPI,
    keys: [key]
  )

  defguardeddelegate(exists(key), to: GenericAPI, keys: [key])
  defdelegate keys(pattern \\ "*"), to: GenericAPI
  defdelegate dbsize(), to: GenericAPI
  defdelegate flushdb(), to: GenericAPI
  defguardeddelegate(expire(key, ttl_ms), to: GenericAPI, keys: [key])
  defguardeddelegate(ttl(key), to: GenericAPI, keys: [key])

  defguardeddelegate(copy(source, destination, opts \\ []),
    to: GenericAPI,
    keys: [source, destination]
  )

  defguardeddelegate(rename(source, destination),
    to: GenericAPI,
    keys: [source, destination]
  )

  defguardeddelegate(renamenx(source, destination),
    to: GenericAPI,
    keys: [source, destination]
  )

  defguardeddelegate(type(key), to: GenericAPI, keys: [key])
  defdelegate randomkey(), to: GenericAPI
  defguardeddelegate(persist(key), to: GenericAPI, keys: [key])
  defguardeddelegate(pexpire(key, ttl_ms), to: GenericAPI, keys: [key])
  defguardeddelegate(expireat(key, unix_ts_seconds), to: GenericAPI, keys: [key])
  defguardeddelegate(pexpireat(key, unix_ts_ms), to: GenericAPI, keys: [key])
  defguardeddelegate(expiretime(key), to: GenericAPI, keys: [key])
  defguardeddelegate(pexpiretime(key), to: GenericAPI, keys: [key])
  defguardeddelegate(pttl(key), to: GenericAPI, keys: [key])

  defguardeddelegate(setbit(key, offset, bit_value), to: BitmapAPI, keys: [key])
  defguardeddelegate(getbit(key, offset), to: BitmapAPI, keys: [key])
  defguardeddelegate(bitcount(key, opts \\ []), to: BitmapAPI, keys: [key])

  defguardeddelegate(bitop(op, dest_key, source_keys),
    to: BitmapAPI,
    keys: FerricStore.API.PublicAccess.destination_keys(dest_key, source_keys)
  )

  defguardeddelegate(bitpos(key, bit_value, opts \\ []), to: BitmapAPI, keys: [key])

  defguardeddelegate(xadd(key, fields), to: StreamsAPI, keys: [key])
  defguardeddelegate(xlen(key), to: StreamsAPI, keys: [key])
  defguardeddelegate(xrange(key, start, stop, opts \\ []), to: StreamsAPI, keys: [key])
  defguardeddelegate(xrevrange(key, stop, start, opts \\ []), to: StreamsAPI, keys: [key])
  defguardeddelegate(xtrim(key, opts), to: StreamsAPI, keys: [key])

  defguardeddelegate(bf_reserve(key, error_rate, capacity), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_add(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_madd(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_exists(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_mexists(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_card(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(bf_info(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_reserve(key, capacity), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_add(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_addnx(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_del(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_exists(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_mexists(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_count(key, element), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cf_info(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cms_initbydim(key, width, depth), to: ProbabilisticAPI, keys: [key])

  defguardeddelegate(cms_initbyprob(key, error, probability),
    to: ProbabilisticAPI,
    keys: [key]
  )

  defguardeddelegate(cms_incrby(key, pairs), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cms_query(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(cms_info(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(topk_reserve(key, k), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(topk_add(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(topk_query(key, elements), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(topk_list(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(topk_info(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_create(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_add(key, values), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_quantile(key, quantiles), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_cdf(key, values), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_min(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_max(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_info(key), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_reset(key), to: ProbabilisticAPI, keys: [key])

  defguardeddelegate(tdigest_trimmed_mean(key, lo, hi),
    to: ProbabilisticAPI,
    keys: [key]
  )

  defguardeddelegate(tdigest_rank(key, values), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_revrank(key, values), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_byrank(key, ranks), to: ProbabilisticAPI, keys: [key])
  defguardeddelegate(tdigest_byrevrank(key, ranks), to: ProbabilisticAPI, keys: [key])

  defguardeddelegate(geoadd(key, members), to: GeoAPI, keys: [key])
  defguardeddelegate(geodist(key, member1, member2, unit \\ "m"), to: GeoAPI, keys: [key])
  defguardeddelegate(geohash(key, members), to: GeoAPI, keys: [key])
  defguardeddelegate(geopos(key, members), to: GeoAPI, keys: [key])

  defguardeddelegate(lock(key, owner, ttl_ms), to: LocksAPI, keys: [key])
  defguardeddelegate(unlock(key, owner), to: LocksAPI, keys: [key])
  defguardeddelegate(extend(key, owner, ttl_ms), to: LocksAPI, keys: [key])
  defguardeddelegate(ratelimit_add(key, window_ms, max, count \\ 1), to: LocksAPI, keys: [key])

  defguardeddelegate(pfadd(key, elements), to: HyperLogLogAPI, keys: [key])
  defguardeddelegate(pfcount(keys), to: HyperLogLogAPI, keys: keys)

  defguardeddelegate(pfmerge(dest_key, source_keys),
    to: HyperLogLogAPI,
    keys: FerricStore.API.PublicAccess.destination_keys(dest_key, source_keys)
  )

  defdelegate multi(fun), to: SystemAPI
  defdelegate ping(), to: SystemAPI
  defdelegate echo(message), to: SystemAPI
  @spec flushall() :: :ok | {:error, term()}
  defdelegate flushall(), to: SystemAPI
  defdelegate pipeline(fun), to: SystemAPI
  defguardeddelegate(batch_get(keys), to: SystemAPI, keys: keys)
  defdelegate packed_batch_get(packed_keys), to: SystemAPI
  @spec batch_set([{binary(), binary()}]) :: [:ok | write_error()] | write_error()
  defguardeddelegate(batch_set(kv_pairs),
    to: SystemAPI,
    keys: FerricStore.API.PublicAccess.pair_keys(kv_pairs)
  )

  # ---------------------------------------------------------------------------
  # Flow API
  # ---------------------------------------------------------------------------

  alias FerricStore.API.Flow, as: FlowAPI

  defdelegate flow_create(id, opts), to: FlowAPI
  defdelegate flow_value_put(value, opts \\ []), to: FlowAPI
  defdelegate flow_value_mget(refs, opts \\ []), to: FlowAPI
  defdelegate flow_signal(id, opts), to: FlowAPI
  defdelegate flow_create_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_spawn_children(parent_id, children, opts \\ []), to: FlowAPI
  defdelegate flow_get(id, opts \\ []), to: FlowAPI
  defdelegate flow_query(query, params \\ %{}), to: FlowAPI
  defdelegate flow_policy_set(type, opts), to: FlowAPI
  defdelegate flow_policy_get(type, opts \\ []), to: FlowAPI
  defdelegate flow_claim_due(type, opts), to: FlowAPI
  defdelegate flow_reclaim(type, opts), to: FlowAPI
  defdelegate flow_extend_lease(id, lease_token, opts \\ []), to: FlowAPI
  defdelegate flow_complete(id, lease_token, opts \\ []), to: FlowAPI
  defdelegate flow_complete_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_run_steps_many(items, opts \\ []), to: FlowAPI
  defdelegate flow_transition(id, from_state, to_state, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_create(id, opts), to: FlowAPI
  defdelegate flow_schedule_get(id, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_fire(id, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_pause(id, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_resume(id, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_list(opts \\ []), to: FlowAPI
  defdelegate flow_schedule_delete(id, opts \\ []), to: FlowAPI
  defdelegate flow_schedule_fire_due(opts \\ []), to: FlowAPI
  defdelegate flow_start_and_claim(id, type, initial_state, opts \\ []), to: FlowAPI
  defdelegate flow_step_continue(id, lease_token, from_state, to_state, opts \\ []), to: FlowAPI

  defdelegate flow_transition_many(partition_key, from_state, to_state, items, opts \\ []),
    to: FlowAPI

  defdelegate flow_retry(id, lease_token, opts), to: FlowAPI
  defdelegate flow_retry_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_fail(id, lease_token, opts \\ []), to: FlowAPI
  defdelegate flow_fail_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_cancel(id, opts \\ []), to: FlowAPI
  defdelegate flow_cancel_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_retention_cleanup(opts \\ []), to: FlowAPI
  defdelegate flow_rewind(id, opts), to: FlowAPI
  defdelegate flow_list(type, opts \\ []), to: FlowAPI
  defdelegate flow_search(opts \\ []), to: FlowAPI
  defdelegate flow_attributes(type, opts \\ []), to: FlowAPI
  defdelegate flow_attribute_values(type, attr_name, opts \\ []), to: FlowAPI
  defdelegate flow_effect_reserve(id, effect_key, effect_type, opts \\ []), to: FlowAPI
  defdelegate flow_effect_confirm(id, effect_key, opts \\ []), to: FlowAPI
  defdelegate flow_effect_fail(id, effect_key, opts \\ []), to: FlowAPI
  defdelegate flow_effect_compensate(id, effect_key, opts \\ []), to: FlowAPI
  defdelegate flow_effect_get(id, effect_key, opts \\ []), to: FlowAPI
  defdelegate flow_governance_ledger(id, opts \\ []), to: FlowAPI
  defdelegate flow_approval_request(id, opts \\ []), to: FlowAPI
  defdelegate flow_approval_approve(id, opts \\ []), to: FlowAPI
  defdelegate flow_approval_reject(id, opts \\ []), to: FlowAPI
  defdelegate flow_approval_get(id, opts \\ []), to: FlowAPI
  defdelegate flow_approval_list(opts \\ []), to: FlowAPI
  defdelegate flow_governance_overview(opts \\ []), to: FlowAPI
  defdelegate flow_circuit_open(scope, opts \\ []), to: FlowAPI
  defdelegate flow_circuit_close(scope, opts \\ []), to: FlowAPI
  defdelegate flow_circuit_get(scope, opts \\ []), to: FlowAPI
  defdelegate flow_circuit_list(opts \\ []), to: FlowAPI
  defdelegate flow_budget_reserve(scope, amount, opts \\ []), to: FlowAPI
  defdelegate flow_budget_commit(scope, reservation_id, actual_amount, opts \\ []), to: FlowAPI
  defdelegate flow_budget_release(scope, reservation_id, opts \\ []), to: FlowAPI
  defdelegate flow_budget_get(scope, opts \\ []), to: FlowAPI
  defdelegate flow_budget_list(opts \\ []), to: FlowAPI
  defdelegate flow_limit_lease(scope, opts \\ []), to: FlowAPI
  defdelegate flow_limit_spend(scope, opts \\ []), to: FlowAPI
  defdelegate flow_limit_release(scope, opts \\ []), to: FlowAPI
  defdelegate flow_limit_get(scope, opts \\ []), to: FlowAPI
  defdelegate flow_limit_list(opts \\ []), to: FlowAPI
  defdelegate flow_stats(type, opts \\ []), to: FlowAPI
  defdelegate flow_terminals(type, opts \\ []), to: FlowAPI
  defdelegate flow_failures(type, opts \\ []), to: FlowAPI
  defdelegate flow_by_parent(parent_flow_id, opts \\ []), to: FlowAPI
  defdelegate flow_by_root(root_flow_id, opts \\ []), to: FlowAPI
  defdelegate flow_by_correlation(correlation_id, opts \\ []), to: FlowAPI
  defdelegate flow_info(type, opts \\ []), to: FlowAPI
  defdelegate flow_stuck(type, opts \\ []), to: FlowAPI
  defdelegate flow_history(id, opts \\ []), to: FlowAPI
end
