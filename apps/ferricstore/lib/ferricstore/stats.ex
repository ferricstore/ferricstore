defmodule Ferricstore.Stats do
  @moduledoc """
  Tracks global server statistics using `:counters` for lock-free atomic increments.

  Maintains the following counters:

    * `total_connections_received` — number of TCP connections accepted since startup
    * `total_commands_processed` — number of commands dispatched since startup
    * `hot_reads` — number of reads served from the ETS hot cache
    * `cold_reads` — number of reads that fell through to Bitcask on disk

  Also stores the server start time and a random run ID (hex string) generated at
  startup.

  ## Hot/cold read tracking

  User-facing KV reads through `Router.get/1` are classified as either *hot*
  (served from ETS) or *cold* (required a Bitcask disk read). Flow, compound
  data-structure, and other internal storage keys are excluded so dashboard
  cache stats describe client KV access instead of background/index reads.
  Counts are tracked at two
  levels:

  1. **Global** — via atomic counters (slots 3 and 4), used for `INFO stats`.
  2. **Per-prefix** — via the `:ferricstore_hotness` ETS table, used by the
     `FERRICSTORE.HOTNESS` command. The prefix is the first colon-delimited
     component of the key, or `"_root"` when no colon is present.

  The per-prefix table is capped at `@max_tracked_prefixes` (1000). Once the
  cap is reached, new prefixes that would exceed it are bucketed under the
  `"_other"` pseudo-prefix to bound memory usage.

  ## Architecture

  Uses a single `:counters` reference with four slots for the hot-path
  counters. The start time and run ID are stored via `:persistent_term` for
  zero-cost reads from any process. The hotness ETS table is a public
  `:set` table allowing concurrent writers from any process.

  ## Usage

      Ferricstore.Stats.incr_connections()
      Ferricstore.Stats.incr_commands()
      Ferricstore.Stats.record_hot_read("user:42")
      Ferricstore.Stats.record_cold_read("user:42")
      Ferricstore.Stats.total_hot_reads()
      Ferricstore.Stats.total_cold_reads()
      Ferricstore.Stats.hotness_top(5)
  """

  use GenServer

  @counter_connections 1
  @counter_commands 2
  @counter_hot_reads 3
  @counter_cold_reads 4
  @counter_active_connections 5
  @counter_keyspace_hits 6
  @counter_keyspace_misses 7
  @counter_expired_keys 8
  @counter_evicted_keys 9
  @counter_keys_with_expiry 10

  @hotness_table :ferricstore_hotness
  @max_tracked_prefixes 1000
  @keyspace_hit_sample_acc {__MODULE__, :keyspace_hit_sample_acc}
  @keyspace_miss_sample_acc {__MODULE__, :keyspace_miss_sample_acc}
  @cache_tracking_disabled {__MODULE__, :cache_tracking_disabled}

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "A prefix hotness entry: `{prefix, hot_count, cold_count, cold_pct}`."
  @type hotness_entry :: {binary(), non_neg_integer(), non_neg_integer(), float()}

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc "Starts the Stats process and initialises counters."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Increments the total connections received counter by 1 and the active
  connection counter by 1.

  Also checks the current active connection count against `maxclients` and
  emits a `[:ferricstore, :connection, :threshold]` telemetry event when
  the count crosses the 80% or 95% thresholds.
  """
  @spec incr_connections() :: :ok
  def incr_connections do
    ref = FerricStore.Instance.get(:default).stats_counter
    :counters.add(ref, @counter_connections, 1)

    try do
      :counters.add(ref, @counter_active_connections, 1)
      check_connection_threshold()
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @doc """
  Decrements the active connection counter by 1.

  Called when a connection closes. The total_connections counter is not
  decremented (it tracks lifetime connections accepted).
  """
  @spec decr_connections() :: :ok
  def decr_connections do
    try do
      :counters.add(
        FerricStore.Instance.get(:default).stats_counter,
        @counter_active_connections,
        -1
      )
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @doc "Returns the current number of active connections."
  @spec active_connections() :: non_neg_integer()
  def active_connections do
    try do
      max(
        0,
        :counters.get(
          FerricStore.Instance.get(:default).stats_counter,
          @counter_active_connections
        )
      )
    rescue
      ArgumentError -> 0
    end
  end

  @doc "Increments the total commands processed counter by 1."
  @spec incr_commands() :: :ok
  def incr_commands do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_commands, 1)
    :ok
  end

  @doc "Increments the total commands processed counter using a cached counter ref."
  @spec incr_commands(reference()) :: :ok
  def incr_commands(counter_ref) do
    :counters.add(counter_ref, @counter_commands, 1)
    :ok
  end

  @doc "Increments the total commands processed counter by N using a cached counter ref."
  @spec incr_commands_by(reference(), pos_integer()) :: :ok
  def incr_commands_by(counter_ref, n) do
    :counters.add(counter_ref, @counter_commands, n)
    :ok
  end

  @doc "Returns the total number of connections received since startup."
  @spec total_connections() :: non_neg_integer()
  def total_connections do
    counter_get(@counter_connections)
  end

  @doc "Returns the total number of commands processed since startup."
  @spec total_commands() :: non_neg_integer()
  def total_commands do
    counter_get(@counter_commands)
  end

  @doc "Increments the keyspace_hits counter by 1."
  @spec incr_keyspace_hits() :: :ok
  def incr_keyspace_hits do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_keyspace_hits, 1)
    :ok
  end

  @doc "Increments the keyspace_hits counter using instance ctx."
  @spec incr_keyspace_hits(FerricStore.Instance.t()) :: :ok
  def incr_keyspace_hits(ctx) do
    :counters.add(ctx.stats_counter, @counter_keyspace_hits, 1)
    :ok
  end

  @doc "Increments the keyspace_hits counter using instance ctx."
  @spec incr_keyspace_hits(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def incr_keyspace_hits(_ctx, 0), do: :ok

  def incr_keyspace_hits(ctx, count) when is_integer(count) and count > 0 do
    :counters.add(ctx.stats_counter, @counter_keyspace_hits, count)
    :ok
  end

  @doc "Increments the keyspace_misses counter by 1."
  @spec incr_keyspace_misses() :: :ok
  def incr_keyspace_misses do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_keyspace_misses, 1)
    :ok
  end

  @doc "Increments the keyspace_misses counter using instance ctx."
  @spec incr_keyspace_misses(FerricStore.Instance.t()) :: :ok
  def incr_keyspace_misses(ctx) do
    :counters.add(ctx.stats_counter, @counter_keyspace_misses, 1)
    :ok
  end

  @doc "Increments the keyspace_misses counter using instance ctx."
  @spec incr_keyspace_misses(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def incr_keyspace_misses(_ctx, 0), do: :ok

  def incr_keyspace_misses(ctx, count) when is_integer(count) and count > 0 do
    :counters.add(ctx.stats_counter, @counter_keyspace_misses, count)
    :ok
  end

  @doc """
  Samples keyspace misses using the instance read sample rate.

  This mirrors hot-read sampling so miss-heavy workloads do not perform one
  atomic counter write per failed lookup. `read_sample_rate <= 1` keeps exact
  accounting for tests and users who explicitly disable sampling.
  """
  @spec sample_keyspace_misses(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def sample_keyspace_misses(ctx, count \\ 1)

  def sample_keyspace_misses(_ctx, 0), do: :ok

  def sample_keyspace_misses(ctx, count) when is_integer(count) and count > 0 do
    if cache_tracking_enabled?() do
      do_sample_keyspace_misses(ctx, count)
    else
      :ok
    end
  end

  @doc false
  @spec sample_keyspace_misses_for_key(FerricStore.Instance.t(), binary(), non_neg_integer()) ::
          :ok
  def sample_keyspace_misses_for_key(ctx, key, count \\ 1)

  def sample_keyspace_misses_for_key(_ctx, _key, 0), do: :ok

  def sample_keyspace_misses_for_key(ctx, key, count)
      when is_binary(key) and is_integer(count) and count > 0 do
    if track_cache_stats?(key) do
      do_sample_keyspace_misses(ctx, count)
    else
      :ok
    end
  end

  @doc """
  Samples keyspace hits using the instance read sample rate.

  Returns the sampled count written to the global counter. Router uses that
  return value to run LFU/hotness updates only when a sample is actually
  flushed. This avoids a PRNG call and a global counter write on every hot GET.
  """
  @spec sample_keyspace_hits(FerricStore.Instance.t(), non_neg_integer()) :: non_neg_integer()
  def sample_keyspace_hits(ctx, count \\ 1)

  def sample_keyspace_hits(_ctx, 0), do: 0

  def sample_keyspace_hits(ctx, count) when is_integer(count) and count > 0 do
    if cache_tracking_enabled?() do
      do_sample_keyspace_hits(ctx, count)
    else
      0
    end
  end

  @doc false
  @spec sample_keyspace_hits_for_key(FerricStore.Instance.t(), binary(), non_neg_integer()) ::
          non_neg_integer()
  def sample_keyspace_hits_for_key(ctx, key, count \\ 1)

  def sample_keyspace_hits_for_key(_ctx, _key, 0), do: 0

  def sample_keyspace_hits_for_key(ctx, key, count)
      when is_binary(key) and is_integer(count) and count > 0 do
    if track_cache_stats?(key) do
      do_sample_keyspace_hits(ctx, count)
    else
      0
    end
  end

  @doc false
  @spec with_cache_tracking_disabled((-> result)) :: result when result: var
  def with_cache_tracking_disabled(fun) when is_function(fun, 0) do
    previous = Process.get(@cache_tracking_disabled, false)
    Process.put(@cache_tracking_disabled, true)

    try do
      fun.()
    after
      if previous do
        Process.put(@cache_tracking_disabled, true)
      else
        Process.delete(@cache_tracking_disabled)
      end
    end
  end

  @doc false
  @spec cache_tracking_enabled?() :: boolean()
  def cache_tracking_enabled? do
    Process.get(@cache_tracking_disabled, false) != true
  end

  @doc false
  @spec cache_tracking_key?(term()) :: boolean()
  def cache_tracking_key?(key) when is_binary(key) do
    not flow_internal_key?(key) and not compound_internal_key?(key)
  end

  def cache_tracking_key?(_key), do: false

  @doc false
  @spec start_keyspace_hit_batch(FerricStore.Instance.t(), non_neg_integer()) ::
          {:exact, non_neg_integer()}
          | {:sampled_no_touch, pos_integer(), non_neg_integer(), non_neg_integer()}
          | {:sampled_touch, pos_integer(), non_neg_integer(), non_neg_integer(), pos_integer()}
  def start_keyspace_hit_batch(ctx, max_hits) when is_integer(max_hits) and max_hits >= 0 do
    if cache_tracking_enabled?() do
      rate = ctx.read_sample_rate

      if rate <= 1 do
        {:exact, 0}
      else
        key = {@keyspace_hit_sample_acc, ctx.stats_counter}
        previous = Process.get(key, 0)
        next_sample_offset = rate - previous

        if max_hits < next_sample_offset do
          {:sampled_no_touch, rate, previous, 0}
        else
          {:sampled_touch, rate, previous, 0, next_sample_offset}
        end
      end
    else
      {:exact, 0}
    end
  end

  @doc false
  @spec finish_keyspace_hit_batch(
          FerricStore.Instance.t(),
          {:exact, non_neg_integer()}
          | {:sampled_no_touch, pos_integer(), non_neg_integer(), non_neg_integer()}
          | {:sampled_touch, pos_integer(), non_neg_integer(), non_neg_integer(), pos_integer()}
        ) :: :ok
  def finish_keyspace_hit_batch(_ctx, {:exact, 0}), do: :ok

  def finish_keyspace_hit_batch(ctx, {:exact, count}) do
    incr_keyspace_hits(ctx, count)
  end

  def finish_keyspace_hit_batch(ctx, {:sampled_no_touch, rate, previous, count}) do
    key = {@keyspace_hit_sample_acc, ctx.stats_counter}
    Process.put(key, rem(previous + count, rate))
    :ok
  end

  def finish_keyspace_hit_batch(ctx, {:sampled_touch, rate, previous, count, _next_sample_offset}) do
    sampled = div(previous + count, rate)

    if sampled > 0 do
      incr_keyspace_hits(ctx, sampled)
    end

    key = {@keyspace_hit_sample_acc, ctx.stats_counter}
    Process.put(key, rem(previous + count, rate))
    :ok
  end

  @doc "Returns the total number of successful key lookups since startup."
  @spec keyspace_hits() :: non_neg_integer()
  def keyspace_hits, do: counter_get(@counter_keyspace_hits)
  @doc "Returns keyspace hits using instance ctx."
  @spec keyspace_hits(FerricStore.Instance.t()) :: non_neg_integer()
  def keyspace_hits(ctx), do: :counters.get(ctx.stats_counter, @counter_keyspace_hits)
  @doc "Returns the total number of failed key lookups since startup."
  @spec keyspace_misses() :: non_neg_integer()
  def keyspace_misses, do: counter_get(@counter_keyspace_misses)
  @doc "Returns keyspace misses using instance ctx."
  @spec keyspace_misses(FerricStore.Instance.t()) :: non_neg_integer()
  def keyspace_misses(ctx), do: :counters.get(ctx.stats_counter, @counter_keyspace_misses)
  @doc "Increments the expired_keys counter by `count`."
  @spec incr_expired_keys(non_neg_integer()) :: :ok
  def incr_expired_keys(0), do: :ok

  def incr_expired_keys(count) when is_integer(count) and count > 0 do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_expired_keys, count)
    :ok
  end

  @doc "Increments the expired_keys counter using instance ctx."
  @spec incr_expired_keys(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def incr_expired_keys(_ctx, 0), do: :ok

  def incr_expired_keys(ctx, count) when is_integer(count) and count > 0 do
    :counters.add(ctx.stats_counter, @counter_expired_keys, count)
    :ok
  end

  @doc "Increments the evicted_keys counter by `count`."
  @spec incr_evicted_keys(non_neg_integer()) :: :ok
  def incr_evicted_keys(0), do: :ok

  def incr_evicted_keys(count) when is_integer(count) and count > 0 do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_evicted_keys, count)
    :ok
  end

  @doc "Increments the evicted_keys counter using instance ctx."
  @spec incr_evicted_keys(FerricStore.Instance.t(), non_neg_integer()) :: :ok
  def incr_evicted_keys(_ctx, 0), do: :ok

  def incr_evicted_keys(ctx, count) when is_integer(count) and count > 0 do
    :counters.add(ctx.stats_counter, @counter_evicted_keys, count)
    :ok
  end

  @doc "Returns expired keys count."
  @spec expired_keys() :: non_neg_integer()
  def expired_keys, do: counter_get(@counter_expired_keys)
  @doc "Returns evicted keys count."
  @spec evicted_keys() :: non_neg_integer()
  def evicted_keys, do: counter_get(@counter_evicted_keys)
  @doc "Returns evicted keys count using instance ctx."
  @spec evicted_keys(FerricStore.Instance.t()) :: non_neg_integer()
  def evicted_keys(ctx), do: :counters.get(ctx.stats_counter, @counter_evicted_keys)
  @doc "Returns expired keys count using instance ctx."
  @spec expired_keys(FerricStore.Instance.t()) :: non_neg_integer()
  def expired_keys(ctx) when is_map(ctx),
    do: :counters.get(ctx.stats_counter, @counter_expired_keys)

  @doc "Increments the keys_with_expiry counter (key gained a TTL)."
  @spec incr_keys_with_expiry() :: :ok
  def incr_keys_with_expiry do
    :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_keys_with_expiry, 1)
    :ok
  end

  @doc "Decrements the keys_with_expiry counter (key lost TTL or was deleted)."
  @spec decr_keys_with_expiry() :: :ok
  def decr_keys_with_expiry do
    ref = FerricStore.Instance.get(:default).stats_counter
    current = :counters.get(ref, @counter_keys_with_expiry)
    if current > 0, do: :counters.add(ref, @counter_keys_with_expiry, -1)
    :ok
  end

  @doc "Returns the number of keys that currently have a TTL set."
  @spec keys_with_expiry() :: non_neg_integer()
  def keys_with_expiry, do: max(0, counter_get(@counter_keys_with_expiry))

  @doc """
  Records a hot read (ETS cache hit) for the given key.

  Increments both the global hot-read counter and the per-prefix hot counter
  in the hotness ETS table.

  ## Parameters

    * `key` — the key that was read from ETS
  """
  @spec record_hot_read(binary()) :: :ok
  def record_hot_read(key) when is_binary(key) do
    if track_cache_stats?(key) do
      :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_hot_reads, 1)
      prefix = extract_prefix(key)
      resolved = resolve_prefix(prefix)
      update_hotness(resolved, :hot)
    end

    :ok
  end

  @doc "Records a hot read using instance ctx."
  @spec record_hot_read(FerricStore.Instance.t(), binary()) :: :ok
  def record_hot_read(ctx, key) when is_binary(key) do
    if track_cache_stats?(key) do
      :counters.add(ctx.stats_counter, @counter_hot_reads, 1)
      prefix = extract_prefix(key)
      resolved = resolve_prefix(prefix)
      update_hotness(resolved, :hot)
    end

    :ok
  end

  @doc """
  Records a cold read (Bitcask disk fallback) for the given key.

  Increments both the global cold-read counter and the per-prefix cold counter
  in the hotness ETS table.

  ## Parameters

    * `key` — the key that required a Bitcask read
  """
  @spec record_cold_read(binary()) :: :ok
  def record_cold_read(key) when is_binary(key) do
    if track_cache_stats?(key) do
      :counters.add(FerricStore.Instance.get(:default).stats_counter, @counter_cold_reads, 1)
      prefix = extract_prefix(key)
      resolved = resolve_prefix(prefix)
      update_hotness(resolved, :cold)
    end

    :ok
  end

  @doc "Records a cold read using instance ctx."
  @spec record_cold_read(FerricStore.Instance.t(), binary()) :: :ok
  def record_cold_read(ctx, key) when is_binary(key) do
    if track_cache_stats?(key) do
      :counters.add(ctx.stats_counter, @counter_cold_reads, 1)
      prefix = extract_prefix(key)
      resolved = resolve_prefix(prefix)
      update_hotness(resolved, :cold)
    end

    :ok
  end

  @doc "Returns the total number of hot reads (ETS cache hits) since startup."
  @spec total_hot_reads() :: non_neg_integer()
  def total_hot_reads, do: counter_get(@counter_hot_reads)
  @spec total_hot_reads(FerricStore.Instance.t()) :: non_neg_integer()
  def total_hot_reads(ctx), do: :counters.get(ctx.stats_counter, @counter_hot_reads)

  @doc "Returns the total number of cold reads (Bitcask fallbacks) since startup."
  @spec total_cold_reads() :: non_neg_integer()
  def total_cold_reads, do: counter_get(@counter_cold_reads)
  @spec total_cold_reads(FerricStore.Instance.t()) :: non_neg_integer()
  def total_cold_reads(ctx), do: :counters.get(ctx.stats_counter, @counter_cold_reads)

  @doc """
  Returns the hot read percentage as a float between 0.0 and 100.0.

  Returns 0.0 when no reads have been recorded.
  """
  @spec hot_read_pct() :: float()
  def hot_read_pct do
    hot = total_hot_reads()
    cold = total_cold_reads()
    total = hot + cold

    if total == 0 do
      0.0
    else
      Float.round(hot / total * 100.0, 2)
    end
  end

  @doc """
  Returns approximate cold reads per second since server startup.

  Uses `uptime_seconds/0` as the denominator. Returns 0.0 when uptime is 0.
  """
  @spec cold_reads_per_second() :: float()
  def cold_reads_per_second do
    uptime = uptime_seconds()

    if uptime == 0 do
      0.0
    else
      Float.round(total_cold_reads() / uptime, 2)
    end
  end

  @doc """
  Returns the top `n` prefixes sorted by cold read count (descending).

  Each entry is a tuple `{prefix, hot_count, cold_count, cold_pct}` where
  `cold_pct` is the percentage of reads for that prefix that were cold.

  ## Parameters

    * `n` — maximum number of entries to return (default: 10)

  ## Examples

      iex> Ferricstore.Stats.hotness_top(5)
      [{"user", 1000, 50, 4.76}, {"session", 500, 200, 28.57}]
  """
  @spec hotness_top(pos_integer()) :: [hotness_entry()]
  def hotness_top(n \\ 10) do
    try do
      :ets.tab2list(@hotness_table)
      |> Enum.map(fn {prefix, hot, cold} ->
        total = hot + cold
        pct = if total == 0, do: 0.0, else: Float.round(cold / total * 100.0, 2)
        {prefix, hot, cold, pct}
      end)
      |> Enum.sort_by(fn {_prefix, _hot, cold, _pct} -> cold end, :desc)
      |> Enum.take(n)
    rescue
      ArgumentError -> []
    end
  end

  @doc """
  Resets all hotness counters (both global and per-prefix).

  Useful for tests and for the `FERRICSTORE.HOTNESS RESET` subcommand.
  """
  @spec reset_hotness() :: :ok
  def reset_hotness do
    :counters.put(FerricStore.Instance.get(:default).stats_counter, @counter_hot_reads, 0)
    :counters.put(FerricStore.Instance.get(:default).stats_counter, @counter_cold_reads, 0)

    try do
      :ets.delete_all_objects(@hotness_table)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @doc "Returns the server uptime in seconds."
  @spec uptime_seconds() :: non_neg_integer()
  def uptime_seconds do
    case :persistent_term.get({__MODULE__, :start_time}, nil) do
      nil -> 0
      start -> div(System.monotonic_time(:millisecond) - start, 1000)
    end
  end

  @doc "Returns the random hex run ID generated at startup."
  @spec run_id() :: binary()
  def run_id do
    :persistent_term.get({__MODULE__, :run_id}, String.duplicate("0", 40))
  end

  @doc "Returns the server start time as a monotonic millisecond timestamp."
  @spec start_time() :: integer()
  def start_time do
    :persistent_term.get({__MODULE__, :start_time}, System.monotonic_time(:millisecond))
  end

  @doc """
  Resets all Stats counters to zero: connections, commands, hot reads,
  and cold reads.

  Also clears the per-prefix hotness table. The run ID and start time
  are **not** reset.

  Used by `CONFIG RESETSTAT` to clear accumulated statistics.
  """
  @spec reset() :: :ok
  def reset do
    ref = FerricStore.Instance.get(:default).stats_counter
    :counters.put(ref, @counter_connections, 0)
    :counters.put(ref, @counter_commands, 0)
    :counters.put(ref, @counter_hot_reads, 0)
    :counters.put(ref, @counter_cold_reads, 0)
    :counters.put(ref, @counter_keyspace_hits, 0)
    :counters.put(ref, @counter_keyspace_misses, 0)
    :counters.put(ref, @counter_expired_keys, 0)
    :counters.put(ref, @counter_evicted_keys, 0)

    try do
      :ets.delete_all_objects(@hotness_table)
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  @doc """
  Extracts the prefix from a key by splitting on the first colon.

  Returns the portion before the first `":"`, or `"_root"` when the key
  contains no colon.

  ## Examples

      iex> Ferricstore.Stats.extract_prefix("user:42")
      "user"

      iex> Ferricstore.Stats.extract_prefix("plain_key")
      "_root"

      iex> Ferricstore.Stats.extract_prefix("a:b:c")
      "a"
  """
  @spec extract_prefix(binary()) :: binary()
  def extract_prefix(key) when is_binary(key) do
    case :binary.split(key, ":") do
      [^key] -> "_root"
      [prefix, _rest] -> prefix
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    # Use the counter ref from the :default Instance (built before Stats starts).
    # No separate counter — Stats and Instance share the same ref.
    _ref = FerricStore.Instance.get(:default).stats_counter
    run_id = :crypto.strong_rand_bytes(20) |> Base.encode16(case: :lower)
    start_time = System.monotonic_time(:millisecond)

    # run_id and start_time are server-level metadata, not per-instance
    :persistent_term.put({__MODULE__, :run_id}, run_id)
    :persistent_term.put({__MODULE__, :start_time}, start_time)

    # Create the per-prefix hotness table if it does not already exist.
    # The table is :public so any process (Router, Shard) can write to it
    # without going through this GenServer.
    case :ets.whereis(@hotness_table) do
      :undefined ->
        :ets.new(@hotness_table, [
          :set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto},
          {:decentralized_counters, true}
        ])

      _ref ->
        :ets.delete_all_objects(@hotness_table)
    end

    {:ok, %{}}
  end

  # ---------------------------------------------------------------------------
  # Private
  # ---------------------------------------------------------------------------

  defp counter_get(index) do
    :counters.get(FerricStore.Instance.get(:default).stats_counter, index)
  rescue
    ArgumentError -> 0
  end

  defp track_cache_stats?(key) when is_binary(key) do
    cache_tracking_enabled?() and cache_tracking_key?(key)
  end

  defp do_sample_keyspace_misses(ctx, count) do
    rate = ctx.read_sample_rate

    if rate <= 1 do
      incr_keyspace_misses(ctx, count)
    else
      key = {@keyspace_miss_sample_acc, ctx.stats_counter}
      total = Process.get(key, 0) + count
      sampled = div(total, rate)
      remainder = rem(total, rate)

      if sampled > 0 do
        incr_keyspace_misses(ctx, sampled)
      end

      Process.put(key, remainder)
      :ok
    end
  end

  defp do_sample_keyspace_hits(ctx, count) do
    rate = ctx.read_sample_rate

    if rate <= 1 do
      incr_keyspace_hits(ctx, count)
      count
    else
      key = {@keyspace_hit_sample_acc, ctx.stats_counter}
      total = Process.get(key, 0) + count
      sampled = div(total, rate)
      remainder = rem(total, rate)

      if sampled > 0 do
        incr_keyspace_hits(ctx, sampled)
      end

      Process.put(key, remainder)
      sampled
    end
  end

  defp flow_internal_key?(<<"f:{", _rest::binary>>), do: true
  defp flow_internal_key?(_key), do: false

  defp compound_internal_key?(<<"XG:", _rest::binary>>), do: true

  defp compound_internal_key?(key) do
    Ferricstore.Store.CompoundKey.internal_key?(key)
  end

  # Resolves the prefix to track: if the hotness table already has this prefix
  # or the table has room, use the prefix as-is. Otherwise bucket into "_other".
  @spec resolve_prefix(binary()) :: binary()
  defp resolve_prefix(prefix) do
    try do
      case :ets.lookup(@hotness_table, prefix) do
        [{^prefix, _, _}] ->
          # Already tracked — use it directly.
          prefix

        [] ->
          # New prefix. Check if we have room.
          size = :ets.info(@hotness_table, :size)

          if size < @max_tracked_prefixes do
            prefix
          else
            "_other"
          end
      end
    rescue
      ArgumentError -> "_other"
    end
  end

  # Atomically increments either the hot or cold counter for a prefix.
  # Uses :ets.update_counter with a default for atomic upsert.
  @spec update_hotness(binary(), :hot | :cold) :: :ok
  defp update_hotness(prefix, :hot) do
    try do
      :ets.update_counter(@hotness_table, prefix, {2, 1}, {prefix, 0, 0})
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  defp update_hotness(prefix, :cold) do
    try do
      :ets.update_counter(@hotness_table, prefix, {3, 1}, {prefix, 0, 0})
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  # Checks if the current active connection count crosses the 80% or 95%
  # thresholds of maxclients and emits a telemetry event if so.
  # Uses the atomic counter for a lock-free, non-blocking check.
  @connection_warn_threshold 0.80
  @connection_critical_threshold 0.95

  defp check_connection_threshold do
    maxclients = Application.get_env(:ferricstore, :maxclients, 10_000)
    active = active_connections()
    ratio = if maxclients > 0, do: active / maxclients, else: 0.0

    cond do
      ratio >= @connection_critical_threshold ->
        :telemetry.execute(
          [:ferricstore, :connection, :threshold],
          %{active: active, max: maxclients, ratio: ratio},
          %{level: :critical}
        )

      ratio >= @connection_warn_threshold ->
        :telemetry.execute(
          [:ferricstore, :connection, :threshold],
          %{active: active, max: maxclients, ratio: ratio},
          %{level: :warning}
        )

      true ->
        :ok
    end
  end
end
