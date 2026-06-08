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
    timeout = Keyword.get(opts, :timeout, 30_000)
    interval = Keyword.get(opts, :interval, 100)
    deadline = System.monotonic_time(:millisecond) + timeout

    do_await_ready(deadline, interval)
  end

  defp do_await_ready(deadline, interval) do
    case Ferricstore.Health.check() do
      %{status: :ok} ->
        :ok

      _ ->
        if System.monotonic_time(:millisecond) > deadline do
          raise "FerricStore not ready within timeout. Check shard health with FerricStore.health()"
        end

        Process.sleep(interval)
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
  # Embedded Redis-compatible API
  # ---------------------------------------------------------------------------

  alias FerricStore.API.Bitmap, as: BitmapAPI
  alias FerricStore.API.Generic, as: GenericAPI
  alias FerricStore.API.Geo, as: GeoAPI
  alias FerricStore.API.Hashes, as: HashesAPI
  alias FerricStore.API.HyperLogLog, as: HyperLogLogAPI
  alias FerricStore.API.Json, as: JsonAPI
  alias FerricStore.API.Lists, as: ListsAPI
  alias FerricStore.API.Locks, as: LocksAPI
  alias FerricStore.API.Probabilistic, as: ProbabilisticAPI
  alias FerricStore.API.Sets, as: SetsAPI
  alias FerricStore.API.SortedSets, as: SortedSetsAPI
  alias FerricStore.API.Streams, as: StreamsAPI
  alias FerricStore.API.Strings, as: StringsAPI
  alias FerricStore.API.System, as: SystemAPI

  @spec set(key(), value(), set_opts()) :: :ok | {:ok, value() | nil} | nil | write_error()
  defdelegate set(key, value, opts \\ []), to: StringsAPI
  defdelegate get(key, opts \\ []), to: StringsAPI
  defdelegate del(key), to: StringsAPI
  defdelegate incr(key), to: StringsAPI
  defdelegate decr(key), to: StringsAPI
  defdelegate decr_by(key, amount), to: StringsAPI
  defdelegate incr_by(key, amount), to: StringsAPI
  defdelegate incr_by_float(key, amount), to: StringsAPI
  defdelegate mget(keys), to: StringsAPI
  defdelegate mset(pairs), to: StringsAPI
  defdelegate append(key, suffix), to: StringsAPI
  defdelegate strlen(key), to: StringsAPI
  defdelegate getset(key, value), to: StringsAPI
  defdelegate getdel(key), to: StringsAPI
  defdelegate getex(key, opts \\ []), to: StringsAPI
  defdelegate setnx(key, value), to: StringsAPI
  defdelegate setex(key, seconds, value), to: StringsAPI
  defdelegate psetex(key, milliseconds, value), to: StringsAPI
  defdelegate getrange(key, start, stop), to: StringsAPI
  defdelegate setrange(key, offset, value), to: StringsAPI
  defdelegate msetnx(pairs), to: StringsAPI

  defdelegate hset(key, fields), to: HashesAPI
  defdelegate hget(key, field), to: HashesAPI
  defdelegate hgetall(key), to: HashesAPI
  defdelegate hdel(key, fields), to: HashesAPI
  defdelegate hexists(key, field), to: HashesAPI
  defdelegate hlen(key), to: HashesAPI
  defdelegate hkeys(key), to: HashesAPI
  defdelegate hvals(key), to: HashesAPI
  defdelegate hmget(key, fields), to: HashesAPI
  defdelegate hincrby(key, field, amount), to: HashesAPI
  defdelegate hincrbyfloat(key, field, amount), to: HashesAPI
  defdelegate hsetnx(key, field, value), to: HashesAPI
  defdelegate hrandfield(key, count \\ nil), to: HashesAPI
  defdelegate hstrlen(key, field), to: HashesAPI

  defdelegate lpush(key, elements), to: ListsAPI
  defdelegate rpush(key, elements), to: ListsAPI
  defdelegate lpop(key, count \\ 1), to: ListsAPI
  defdelegate rpop(key, count \\ 1), to: ListsAPI
  defdelegate lrange(key, start, stop), to: ListsAPI
  defdelegate llen(key), to: ListsAPI
  defdelegate lindex(key, index), to: ListsAPI
  defdelegate lset(key, index, element), to: ListsAPI
  defdelegate lrem(key, count, element), to: ListsAPI
  defdelegate linsert(key, direction, pivot, element), to: ListsAPI
  defdelegate lmove(source, destination, from_dir, to_dir), to: ListsAPI
  defdelegate lpos(key, element, opts \\ []), to: ListsAPI

  defdelegate sadd(key, members), to: SetsAPI
  defdelegate srem(key, members), to: SetsAPI
  defdelegate smembers(key), to: SetsAPI
  defdelegate sismember(key, member), to: SetsAPI
  defdelegate scard(key), to: SetsAPI
  defdelegate smismember(key, members), to: SetsAPI
  defdelegate srandmember(key, count \\ nil), to: SetsAPI
  defdelegate spop(key, count \\ nil), to: SetsAPI
  defdelegate sdiff(keys), to: SetsAPI
  defdelegate sinter(keys), to: SetsAPI
  defdelegate sunion(keys), to: SetsAPI
  defdelegate sdiffstore(destination, keys), to: SetsAPI
  defdelegate sinterstore(destination, keys), to: SetsAPI
  defdelegate sunionstore(destination, keys), to: SetsAPI
  defdelegate sintercard(keys, opts \\ []), to: SetsAPI

  defdelegate zadd(key, score_member_pairs), to: SortedSetsAPI
  defdelegate zrange(key, start, stop, opts \\ []), to: SortedSetsAPI
  defdelegate zscore(key, member), to: SortedSetsAPI
  defdelegate zcard(key), to: SortedSetsAPI
  defdelegate zrem(key, members), to: SortedSetsAPI
  defdelegate zrank(key, member), to: SortedSetsAPI
  defdelegate zrevrank(key, member), to: SortedSetsAPI
  defdelegate zrangebyscore(key, min, max, opts \\ []), to: SortedSetsAPI
  defdelegate zcount(key, min, max), to: SortedSetsAPI
  defdelegate zincrby(key, increment, member), to: SortedSetsAPI
  defdelegate zrandmember(key, count \\ nil), to: SortedSetsAPI
  defdelegate zpopmin(key, count \\ 1), to: SortedSetsAPI
  defdelegate zpopmax(key, count \\ 1), to: SortedSetsAPI
  defdelegate zmscore(key, members), to: SortedSetsAPI

  defdelegate cas(key, expected, new_value, opts \\ []), to: GenericAPI
  defdelegate fetch_or_compute(key, opts), to: GenericAPI
  defdelegate fetch_or_compute_result(key, value, opts), to: GenericAPI
  defdelegate exists(key), to: GenericAPI
  defdelegate keys(pattern \\ "*"), to: GenericAPI
  defdelegate dbsize(), to: GenericAPI
  defdelegate flushdb(), to: GenericAPI
  defdelegate expire(key, ttl_ms), to: GenericAPI
  defdelegate ttl(key), to: GenericAPI
  defdelegate copy(source, destination, opts \\ []), to: GenericAPI
  defdelegate rename(source, destination), to: GenericAPI
  defdelegate renamenx(source, destination), to: GenericAPI
  defdelegate type(key), to: GenericAPI
  defdelegate randomkey(), to: GenericAPI
  defdelegate persist(key), to: GenericAPI
  defdelegate pexpire(key, ttl_ms), to: GenericAPI
  defdelegate expireat(key, unix_ts_seconds), to: GenericAPI
  defdelegate pexpireat(key, unix_ts_ms), to: GenericAPI
  defdelegate expiretime(key), to: GenericAPI
  defdelegate pexpiretime(key), to: GenericAPI
  defdelegate pttl(key), to: GenericAPI

  defdelegate setbit(key, offset, bit_value), to: BitmapAPI
  defdelegate getbit(key, offset), to: BitmapAPI
  defdelegate bitcount(key, opts \\ []), to: BitmapAPI
  defdelegate bitop(op, dest_key, source_keys), to: BitmapAPI
  defdelegate bitpos(key, bit_value, opts \\ []), to: BitmapAPI

  defdelegate xadd(key, fields), to: StreamsAPI
  defdelegate xlen(key), to: StreamsAPI
  defdelegate xrange(key, start, stop, opts \\ []), to: StreamsAPI
  defdelegate xrevrange(key, stop, start, opts \\ []), to: StreamsAPI
  defdelegate xtrim(key, opts), to: StreamsAPI

  defdelegate bf_reserve(key, error_rate, capacity), to: ProbabilisticAPI
  defdelegate bf_add(key, element), to: ProbabilisticAPI
  defdelegate bf_madd(key, elements), to: ProbabilisticAPI
  defdelegate bf_exists(key, element), to: ProbabilisticAPI
  defdelegate bf_mexists(key, elements), to: ProbabilisticAPI
  defdelegate bf_card(key), to: ProbabilisticAPI
  defdelegate bf_info(key), to: ProbabilisticAPI
  defdelegate cf_reserve(key, capacity), to: ProbabilisticAPI
  defdelegate cf_add(key, element), to: ProbabilisticAPI
  defdelegate cf_addnx(key, element), to: ProbabilisticAPI
  defdelegate cf_del(key, element), to: ProbabilisticAPI
  defdelegate cf_exists(key, element), to: ProbabilisticAPI
  defdelegate cf_mexists(key, elements), to: ProbabilisticAPI
  defdelegate cf_count(key, element), to: ProbabilisticAPI
  defdelegate cf_info(key), to: ProbabilisticAPI
  defdelegate cms_initbydim(key, width, depth), to: ProbabilisticAPI
  defdelegate cms_initbyprob(key, error, probability), to: ProbabilisticAPI
  defdelegate cms_incrby(key, pairs), to: ProbabilisticAPI
  defdelegate cms_query(key, elements), to: ProbabilisticAPI
  defdelegate cms_info(key), to: ProbabilisticAPI
  defdelegate topk_reserve(key, k), to: ProbabilisticAPI
  defdelegate topk_add(key, elements), to: ProbabilisticAPI
  defdelegate topk_query(key, elements), to: ProbabilisticAPI
  defdelegate topk_list(key), to: ProbabilisticAPI
  defdelegate topk_info(key), to: ProbabilisticAPI
  defdelegate tdigest_create(key), to: ProbabilisticAPI
  defdelegate tdigest_add(key, values), to: ProbabilisticAPI
  defdelegate tdigest_quantile(key, quantiles), to: ProbabilisticAPI
  defdelegate tdigest_cdf(key, values), to: ProbabilisticAPI
  defdelegate tdigest_min(key), to: ProbabilisticAPI
  defdelegate tdigest_max(key), to: ProbabilisticAPI
  defdelegate tdigest_info(key), to: ProbabilisticAPI
  defdelegate tdigest_reset(key), to: ProbabilisticAPI
  defdelegate tdigest_trimmed_mean(key, lo, hi), to: ProbabilisticAPI
  defdelegate tdigest_rank(key, values), to: ProbabilisticAPI
  defdelegate tdigest_revrank(key, values), to: ProbabilisticAPI
  defdelegate tdigest_byrank(key, ranks), to: ProbabilisticAPI
  defdelegate tdigest_byrevrank(key, ranks), to: ProbabilisticAPI

  defdelegate geoadd(key, members), to: GeoAPI
  defdelegate geodist(key, member1, member2, unit \\ "m"), to: GeoAPI
  defdelegate geohash(key, members), to: GeoAPI
  defdelegate geopos(key, members), to: GeoAPI

  defdelegate json_set(key, path, value), to: JsonAPI
  defdelegate json_get(key, path \\ "$"), to: JsonAPI
  defdelegate json_del(key, path \\ "$"), to: JsonAPI
  defdelegate json_type(key, path \\ "$"), to: JsonAPI
  defdelegate json_numincrby(key, path, increment), to: JsonAPI
  defdelegate json_arrappend(key, path, values), to: JsonAPI
  defdelegate json_arrlen(key, path \\ "$"), to: JsonAPI
  defdelegate json_strlen(key, path \\ "$"), to: JsonAPI
  defdelegate json_objkeys(key, path \\ "$"), to: JsonAPI
  defdelegate json_objlen(key, path \\ "$"), to: JsonAPI

  defdelegate lock(key, owner, ttl_ms), to: LocksAPI
  defdelegate unlock(key, owner), to: LocksAPI
  defdelegate extend(key, owner, ttl_ms), to: LocksAPI
  defdelegate ratelimit_add(key, window_ms, max, count \\ 1), to: LocksAPI

  defdelegate pfadd(key, elements), to: HyperLogLogAPI
  defdelegate pfcount(keys), to: HyperLogLogAPI
  defdelegate pfmerge(dest_key, source_keys), to: HyperLogLogAPI

  defdelegate multi(fun), to: SystemAPI
  defdelegate ping(), to: SystemAPI
  defdelegate echo(message), to: SystemAPI
  @spec flushall() :: :ok | {:error, term()}
  defdelegate flushall(), to: SystemAPI
  defdelegate pipeline(fun), to: SystemAPI
  defdelegate batch_get(keys), to: SystemAPI
  defdelegate packed_batch_get(packed_keys), to: SystemAPI
  @spec batch_set([{binary(), binary()}]) :: [:ok | write_error()]
  defdelegate batch_set(kv_pairs), to: SystemAPI
  # ---------------------------------------------------------------------------
  # Flow API
  # ---------------------------------------------------------------------------

  alias FerricStore.API.Flow, as: FlowAPI

  defdelegate flow_create(id, opts), to: FlowAPI
  defdelegate flow_value_put(value, opts \\ []), to: FlowAPI
  defdelegate flow_value_mget(refs), to: FlowAPI
  defdelegate flow_signal(id, opts), to: FlowAPI
  defdelegate flow_create_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_spawn_children(parent_id, children, opts \\ []), to: FlowAPI
  defdelegate flow_get(id, opts \\ []), to: FlowAPI
  defdelegate flow_policy_set(type, opts), to: FlowAPI
  defdelegate flow_policy_get(type, opts \\ []), to: FlowAPI
  defdelegate flow_claim_due(type, opts), to: FlowAPI
  defdelegate flow_reclaim(type, opts), to: FlowAPI
  defdelegate flow_extend_lease(id, lease_token, opts \\ []), to: FlowAPI
  defdelegate flow_complete(id, lease_token, opts \\ []), to: FlowAPI
  defdelegate flow_complete_many(partition_key, items, opts \\ []), to: FlowAPI
  defdelegate flow_transition(id, from_state, to_state, opts \\ []), to: FlowAPI

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
  defdelegate flow_terminals(type, opts \\ []), to: FlowAPI
  defdelegate flow_failures(type, opts \\ []), to: FlowAPI
  defdelegate flow_by_parent(parent_flow_id, opts \\ []), to: FlowAPI
  defdelegate flow_by_root(root_flow_id, opts \\ []), to: FlowAPI
  defdelegate flow_by_correlation(correlation_id, opts \\ []), to: FlowAPI
  defdelegate flow_info(type, opts \\ []), to: FlowAPI
  defdelegate flow_stuck(type, opts \\ []), to: FlowAPI
  defdelegate flow_history(id, opts \\ []), to: FlowAPI
end
