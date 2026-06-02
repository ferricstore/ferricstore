defmodule Ferricstore.MemoryGuard do
  @moduledoc """
  Memory pressure monitor and eviction controller for FerricStore.

  MemoryGuard runs as a GenServer that checks memory usage every 100ms
  (configurable) and takes graduated action based on pressure thresholds.

  ## Data sources

  `compute_stats/1` aggregates memory from multiple sources:

    * **ETS keydir bytes** -- per-shard ETS table memory (`:ets.info(table, :memory)`)
    * **Rust NIF allocator** -- `NIF.rust_allocated_bytes()` via global `AtomicUsize`
      counter in `tracking_alloc.rs`. Returns -1 when tracking is not installed
      (production cdylib), 0+ when active (tests).
    * **Process RSS** -- `process_rss_bytes()` reads `/proc/self/status` (Linux)
      or `ps rss` (macOS/Unix fallback). Includes ETS, NIF allocations, BEAM
      heaps, LMDB/native mappings, and page cache residency. Only used in
      standalone mode (not embedded).
    * **Cgroup limits** -- detects container memory limits via `/sys/fs/cgroup/`
      and uses that as the effective memory ceiling instead of host RAM.

  ## Pressure levels and actions

  | Level      | Threshold | Eviction          | Promotion    | Evict target | Writes     |
  |------------|-----------|-------------------|--------------|--------------|------------|
  | `:ok`      | < 70%     | None              | Hot (normal) | —            | Accepted   |
  | `:warning` | 70-85%    | Gentle (LFU)      | Hot (normal) | Down to 65%  | Accepted   |
  | `:pressure`| 85-95%    | Aggressive (LFU)  | Skip (cold)  | Down to 75%  | Accepted   |
  | `:reject`  | > 95%     | Emergency (LFU)   | Skip (cold)  | Down to 80%  | Accepted*  |

  *Writes rejected only when eviction_policy is `:noeviction`.

  ## Eviction algorithm

  Target-based eviction with LFU ordering:

  1. Compute `bytes_to_free = current_bytes - target_bytes` based on pressure level.
  2. Sample eligible hot entries from ETS (value != nil, not :pending).
  3. Sort by effective LFU counter ascending (lowest frequency first).
  4. Evict by setting value to `nil` in ETS (key stays, disk location stays).
  5. Subtract each evicted value's `value_size` from deficit.
  6. Stop when deficit reaches zero or no more eligible entries.

  Sample sizes scale with pressure:

    * `:warning` -- sample 50 per shard, evict as needed
    * `:pressure` -- sample 200 per shard, evict as needed
    * `:reject` -- sample 1000 per shard, evict as needed

  ## Promotion skip (anti-thrashing)

  At `:pressure` and `:reject` levels, the `skip_promotion` atomics flag is set.
  Cold reads (value=nil in ETS, pread from Bitcask) return the value to the caller
  but do NOT re-cache it in ETS. This prevents evict/re-promote thrashing where
  MemoryGuard evicts values and the next GET immediately re-caches them.

  The flag is read via `MemoryGuard.skip_promotion?()` (~5ns atomics read) by:
    * `Router.warm_ets_after_cold_read/5` (direct ETS read path)
    * `Shard.cold_read_warm_ets/7` (GenServer read path)

  ## Page cache hints (fadvise)

  All pread NIFs (Bitcask cold reads + prob structure reads) use:
    * `FADV_RANDOM` on file open -- disables kernel readahead (hash-indexed access)
    * `FADV_DONTNEED` after pread -- hints kernel to evict pages immediately

  This keeps page cache free for genuinely hot data. Linux-only (no-ops on macOS).

  ## Atomics flags (lock-free hot-path reads)

  Three flags published to atomics via persistent_term (~5ns read):

    * **slot 1: `keydir_full`** -- set at `:reject` (95%). Gates new key writes
      in `Router.check_keydir_full/1`. Updates to existing keys still allowed.
    * **slot 2: `reject_writes`** -- set at `:reject` + `:noeviction` policy.
      Gates ALL writes (even updates) in `Router.check_keydir_full/1`.
    * **slot 3: `skip_promotion`** -- set at `:pressure` (85%). Prevents cold
      reads from re-caching values in ETS.

  ## Eviction policies

    * `:volatile_lfu` (default) -- Evict least frequently used keys with a TTL.
    * `:volatile_lru` -- Evict least recently used keys with a TTL.
    * `:allkeys_lfu` -- Evict least frequently used key regardless of TTL.
    * `:allkeys_lru` -- Evict least recently used key regardless of TTL.
    * `:volatile_ttl` -- Evict the key with the shortest remaining TTL first.
    * `:noeviction` -- Return OOM error when memory is full. No eviction.

  ## Telemetry events

    * `[:ferricstore, :memory, :check]` -- every check cycle (100ms).
      Measurements: `total_bytes`, `rss_bytes`.
      Metadata: `pressure_level`, `ratio`, `max_bytes`, `rss_ratio`.

    * `[:ferricstore, :memory, :pressure]` -- every check, spec 2.4 levels.
      Measurements: `total_bytes`, `max_bytes`, `ratio`.
      Metadata: `level` (`:ok` | `:warn` | `:pressure` | `:full`).

    * `[:ferricstore, :memory, :recovered]` -- once when pressure drops to `:ok`.

    * `[:ferricstore, :memory, :keydir_pressure]` -- when keydir pressure is
      `:pressure` or `:reject`. Includes `keydir_bytes`, `keydir_ratio`.

    * `[:ferricstore, :hot_cache, :limit_reduced]` / `:limit_restored` --
      hot cache budget changes due to pressure level transitions.

  ## Configuration

    * `:memory_guard_interval_ms` -- check interval (default: 100ms)
    * `:max_memory_bytes` -- maximum total memory budget
    * `:keydir_max_ram` -- maximum ETS keydir memory
    * `:eviction_policy` -- eviction policy atom (default: `:volatile_lfu`)
  """

  use GenServer

  alias Ferricstore.HLC

  require Logger

  @check_interval_ms 100

  @warning_threshold 0.70
  @pressure_threshold 0.85
  @reject_threshold 0.95

  @type pressure_level :: :ok | :warning | :pressure | :reject

  @typedoc "Spec 2.4 pressure level names used in telemetry metadata."
  @type spec_level :: :ok | :warn | :pressure | :full

  defstruct [
    :interval_ms,
    :max_memory_bytes,
    :eviction_policy,
    :shard_count,
    :keydir_max_ram,
    :hot_cache_max_ram,
    :hot_cache_min_ram,
    # Effective memory limit: cgroup limit or host RAM.
    # Used for RSS-based pressure detection.
    :memory_limit,
    last_pressure_level: :ok,
    last_hot_cache_budget: nil,
    keydir_pressure_level: :ok
  ]

  @doc "Starts the MemoryGuard GenServer."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Returns the current memory usage stats for all shards."
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc "Returns the current eviction policy."
  @spec eviction_policy() :: atom()
  def eviction_policy do
    GenServer.call(__MODULE__, :eviction_policy)
  end

  @doc """
  Returns true if memory pressure is at or above the reject threshold.

  Reads from `:persistent_term` (~5ns) instead of GenServer.call (~1-5us).
  The value is updated by `perform_check/1` every 100ms.
  """
  @spec reject_writes?() :: boolean()
  def reject_writes? do
    pressure_flag?(2)
  end

  @doc """
  Returns true if keydir memory usage is at or above 95% of `keydir_max_ram`.

  Reads from `:persistent_term` (~5ns) instead of GenServer.call (~1-5us).
  The value is updated by `perform_check/1` every 100ms. The staleness
  window (100ms) is acceptable since memory pressure changes slowly.

  When true, new key writes should be rejected, but updates to existing keys
  are still allowed.
  """
  @spec keydir_full?() :: boolean()
  def keydir_full? do
    pressure_flag?(1)
  end

  @doc """
  Directly sets the keydir_full flag. For use in tests only.
  """
  @spec set_keydir_full(boolean()) :: :ok
  def set_keydir_full(value) do
    ref = FerricStore.Instance.get(:default).pressure_flags
    :atomics.put(ref, 1, if(value, do: 1, else: 0))
    :ok
  end

  @doc """
  Directly sets the reject_writes flag. For use in tests only.
  """
  @spec set_reject_writes(boolean()) :: :ok
  def set_reject_writes(value) do
    ref = FerricStore.Instance.get(:default).pressure_flags
    :atomics.put(ref, 2, if(value, do: 1, else: 0))
    :ok
  end

  @doc """
  Returns true when cold reads should NOT be promoted to hot cache.

  Set at `:pressure` level (85%+). Prevents evict/re-promote thrashing
  where MemoryGuard evicts values and the next GET re-caches them.
  Reads from atomics via persistent_term (~5ns).
  """
  @spec skip_promotion?() :: boolean()
  def skip_promotion? do
    pressure_flag?(3)
  end

  @doc """
  Directly sets the skip_promotion flag. For use in tests only.
  """
  @spec set_skip_promotion(boolean()) :: :ok
  def set_skip_promotion(value) do
    ref = FerricStore.Instance.get(:default).pressure_flags
    :atomics.put(ref, 3, if(value, do: 1, else: 0))
    :ok
  end

  @doc """
  Clears all lock-free memory-pressure flags.

  This is intentionally small and synchronous because write-path guards read
  these atomics directly. Tests and recovery code can use it after artificial
  pressure without relying on stale compatibility keys in `:persistent_term`.
  """
  @spec reset_pressure_flags() :: :ok
  def reset_pressure_flags do
    ref = FerricStore.Instance.get(:default).pressure_flags
    :atomics.put(ref, 1, 0)
    :atomics.put(ref, 2, 0)
    :atomics.put(ref, 3, 0)
    :ok
  end

  @doc """
  Reconfigures MemoryGuard with new budget parameters.

  Accepts a map with optional keys:
    * `:keydir_max_ram` -- maximum keydir ETS memory in bytes
    * `:hot_cache_max_ram` -- maximum hot_cache ETS memory (or `:auto`)
    * `:hot_cache_min_ram` -- minimum hot_cache budget
    * `:max_memory_bytes` -- total memory budget
    * `:eviction_policy` -- eviction policy atom
  """
  @spec reconfigure(map()) :: :ok
  def reconfigure(params) when is_map(params) do
    GenServer.call(__MODULE__, {:reconfigure, params})
  end

  @doc """
  Forces an immediate memory check cycle.

  Useful in tests to synchronously update pressure levels after changing budgets.
  """
  @spec force_check() :: :ok
  def force_check do
    GenServer.call(__MODULE__, :force_check)
  end

  @doc """
  Triggers an immediate memory check + eviction cycle without blocking the caller.

  Called from the write path when a key is rejected due to memory pressure.
  This kicks off eviction immediately rather than waiting up to 100ms for the
  next periodic check. Uses `cast` so the write path returns the error to the
  client without waiting for the eviction to complete.
  """
  @spec nudge() :: :ok
  def nudge do
    GenServer.cast(__MODULE__, :nudge)
  end

  @impl true
  def init(opts) do
    interval_ms = Keyword.get(opts, :interval_ms, default_interval())
    max_memory_bytes = Keyword.get(opts, :max_memory_bytes, default_max_memory())
    eviction_policy = Keyword.get(opts, :eviction_policy, default_eviction_policy())
    shard_count = Keyword.get(opts, :shard_count, default_shard_count())
    keydir_max_ram = Keyword.get(opts, :keydir_max_ram, default_keydir_max_ram())
    hot_cache_min_ram = Application.get_env(:ferricstore, :hot_cache_min_ram, 0)

    initial_budget = hot_cache_budget(max_memory_bytes, :ok)

    memory_limit = detect_memory_limit()

    state = %__MODULE__{
      interval_ms: interval_ms,
      max_memory_bytes: max_memory_bytes,
      eviction_policy: eviction_policy,
      shard_count: shard_count,
      keydir_max_ram: keydir_max_ram,
      hot_cache_max_ram: max_memory_bytes - keydir_max_ram,
      hot_cache_min_ram: hot_cache_min_ram,
      memory_limit: memory_limit,
      last_pressure_level: :ok,
      last_hot_cache_budget: initial_budget,
      keydir_pressure_level: :ok
    }

    schedule_check(interval_ms)
    {:ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state), do: {:reply, compute_stats(state), state}
  def handle_call(:eviction_policy, _from, state), do: {:reply, state.eviction_policy, state}

  def handle_call(:reject_writes?, _from, state) do
    {:reply, state.last_pressure_level == :reject and state.eviction_policy == :noeviction, state}
  end

  def handle_call(:keydir_full?, _from, state) do
    {:reply, state.keydir_pressure_level == :reject, state}
  end

  def handle_call({:reconfigure, params}, _from, state) do
    new_state =
      state
      |> maybe_update(:keydir_max_ram, Map.get(params, :keydir_max_ram))
      |> maybe_update(:hot_cache_min_ram, Map.get(params, :hot_cache_min_ram))
      |> maybe_update(:max_memory_bytes, Map.get(params, :max_memory_bytes))
      |> maybe_update(:eviction_policy, Map.get(params, :eviction_policy))

    new_state =
      case Map.get(params, :hot_cache_max_ram) do
        nil ->
          %{new_state | hot_cache_max_ram: new_state.max_memory_bytes - new_state.keydir_max_ram}

        :auto ->
          %{new_state | hot_cache_max_ram: new_state.max_memory_bytes - new_state.keydir_max_ram}

        val ->
          %{new_state | hot_cache_max_ram: val}
      end

    {:reply, :ok, new_state}
  end

  def handle_call(:force_check, _from, state) do
    new_state = perform_check(state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast(:nudge, state) do
    {:noreply, perform_check(state)}
  end

  @impl true
  def handle_info(:check, state) do
    state = perform_check(state)
    schedule_check(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp perform_check(state) do
    stats = compute_stats(state)
    level = to_spec_level(stats.pressure_level)

    :telemetry.execute(
      [:ferricstore, :memory, :check],
      %{total_bytes: stats.total_bytes, rss_bytes: stats.rss_bytes},
      %{
        pressure_level: stats.pressure_level,
        ratio: stats.ratio,
        max_bytes: stats.max_bytes,
        rss_ratio: stats.rss_ratio,
        rss_pressure_level: stats.rss_pressure_level,
        memory_limit: stats.memory_limit
      }
    )

    emit_spec_pressure_level(stats, level)

    state = emit_hot_cache_budget_events(state, stats, level)

    # Emit keydir-specific pressure telemetry
    if stats.keydir_pressure_level in [:pressure, :reject] do
      :telemetry.execute(
        [:ferricstore, :memory, :keydir_pressure],
        %{
          keydir_bytes: stats.keydir_bytes,
          keydir_max_ram: stats.keydir_max_ram,
          keydir_ratio: stats.keydir_ratio
        },
        %{keydir_pressure_level: stats.keydir_pressure_level}
      )
    end

    case stats.pressure_level do
      :ok ->
        if state.last_pressure_level in [:pressure, :reject] do
          Logger.info("MemoryGuard: memory pressure resolved")

          :telemetry.execute(
            [:ferricstore, :memory, :recovered],
            %{total_bytes: stats.total_bytes, max_bytes: stats.max_bytes, ratio: stats.ratio},
            %{previous_level: state.last_pressure_level}
          )
        end

      :warning ->
        if state.last_pressure_level not in [:warning, :pressure, :reject] do
          Logger.warning("MemoryGuard: memory warning (#{Float.round(stats.ratio * 100, 1)}%)")
        end

        # Gentle eviction: evict down to 65% target
        target_evict(state, stats, 0.65, 50)

      :pressure ->
        Logger.error("MemoryGuard: high memory pressure (#{Float.round(stats.ratio * 100, 1)}%)")
        emit_shard_pressure_events(stats)
        # Aggressive eviction: evict down to 75% target
        target_evict(state, stats, 0.75, 200)

      :reject ->
        Logger.critical("MemoryGuard: critical memory (#{Float.round(stats.ratio * 100, 1)}%)")
        emit_shard_pressure_events(stats)
        # Emergency eviction: evict down to 80% target
        target_evict(state, stats, 0.80, 1000)
    end

    publish_pressure_flags(stats, state)

    %{
      state
      | last_pressure_level: stats.pressure_level,
        keydir_pressure_level: stats.keydir_pressure_level
    }
  end

  # Target-based eviction: evict hot values until memory drops to target_ratio.
  # sample_size controls how many entries to sample per shard (scales with pressure).
  # Eviction order is determined by the configured policy (LFU/LRU/TTL).
  defp target_evict(%{eviction_policy: :noeviction}, _stats, _target_ratio, _sample_size), do: :ok

  defp target_evict(
         %{eviction_policy: policy, shard_count: shard_count} = _state,
         stats,
         target_ratio,
         sample_size
       )
       when policy in [:volatile_lfu, :allkeys_lfu, :volatile_lru, :allkeys_lru, :volatile_ttl] do
    target_bytes = trunc(stats.max_bytes * target_ratio)
    bytes_to_free = max(0, stats.total_bytes - target_bytes)

    if bytes_to_free == 0 do
      :ok
    else
      {total_evicted, _remaining_deficit} =
        Enum.reduce(0..(shard_count - 1), {0, bytes_to_free}, fn i, {evicted_acc, deficit} ->
          if deficit <= 0 do
            {evicted_acc, deficit}
          else
            {shard_evicted, shard_freed} = evict_from_shard(i, policy, sample_size, deficit)
            {evicted_acc + shard_evicted, deficit - shard_freed}
          end
        end)

      if total_evicted > 0 do
        case default_instance() do
          nil -> :ok
          ctx -> Ferricstore.Stats.incr_evicted_keys(ctx, total_evicted)
        end
      end

      :ok
    end
  end

  defp target_evict(_state, _stats, _target_ratio, _sample_size), do: :ok

  # Evicts entries from a single shard's keydir. Returns {count_evicted, bytes_freed}.
  #
  # Uses :ets.select/3 with a match spec instead of foldl — the filtering
  # (value != nil, fid != :pending, TTL check) runs in the ETS C engine.
  # Cold entries are skipped without creating Erlang terms, saving CPU.
  defp evict_from_shard(shard_index, policy, sample_size, deficit) do
    keydir = :"keydir_#{shard_index}"
    now = HLC.now_ms()

    try do
      # Match spec: {key, value, exp, lfu, fid, _off, vsize}
      #   $1=key, $2=value, $3=exp, $4=lfu, $5=fid, $6=vsize
      # Guards filter in C: value != nil, fid != :pending, + TTL filter.
      # Returns {key, exp, lfu, vsize} — cold entries never become Erlang terms.
      guards =
        case policy do
          p when p in [:volatile_lfu, :volatile_lru, :volatile_ttl] ->
            [{:"/=", :"$2", nil}, {:"/=", :"$5", :pending}, {:>, :"$3", 0}, {:>, :"$3", now}]

          _ ->
            [{:"/=", :"$2", nil}, {:"/=", :"$5", :pending}]
        end

      ms = [
        {
          {:"$1", :"$2", :"$3", :"$4", :"$5", :_, :"$6"},
          guards,
          [{{:"$1", :"$3", :"$4", :"$6"}}]
        }
      ]

      eligible =
        case :ets.select(keydir, ms, sample_size) do
          {entries, _continuation} -> entries
          :"$end_of_table" -> []
        end

      if eligible == [] do
        {0, 0}
      else
        # Sort by eviction priority (lowest value first = evict first)
        sorted =
          case policy do
            :volatile_ttl ->
              Enum.sort_by(eligible, fn {_k, exp, _lfu, _vs} -> exp end)

            p when p in [:volatile_lfu, :allkeys_lfu] ->
              Enum.sort_by(eligible, fn {_k, _exp, lfu, _vs} ->
                Ferricstore.Store.LFU.effective_counter(lfu)
              end)

            p when p in [:volatile_lru, :allkeys_lru] ->
              Enum.sort_by(eligible, fn {_k, _exp, lfu, _vs} ->
                Ferricstore.Store.LFU.unpack(lfu) |> elem(0)
              end)
          end

        # Evict until we've freed enough bytes or run out of candidates.
        binary_ref = keydir_binary_bytes_ref()

        {evict_count, bytes_freed} =
          Enum.reduce_while(sorted, {0, 0}, fn {key, _exp, _lfu, vsize}, {cnt, freed} ->
            if freed >= deficit do
              {:halt, {cnt, freed}}
            else
              # Track off-heap binary bytes removed by eviction (value -> nil)
              if binary_ref do
                case :ets.lookup(keydir, key) do
                  [{^key, val, _, _, _, _, _}] when val != nil ->
                    val_bytes = evict_offheap_size(val)
                    if val_bytes > 0, do: :atomics.sub(binary_ref, shard_index + 1, val_bytes)

                  _ ->
                    :ok
                end
              end

              :ets.update_element(keydir, key, {2, nil})
              {:cont, {cnt + 1, freed + vsize}}
            end
          end)

        {evict_count, bytes_freed}
      end
    rescue
      _ -> {0, 0}
    catch
      _, _ -> {0, 0}
    end
  end

  defp compute_stats(state) do
    # Off-heap binary bytes tracked by insert/delete hooks in state machine + router.
    binary_bytes_ref = keydir_binary_bytes_ref()

    {keydir_bytes, shard_stats} =
      Enum.reduce(shard_indexes(state.shard_count), {0, %{}}, fn i, {kd_acc, shards_acc} ->
        ets_bytes = safe_ets_memory(:"keydir_#{i}")
        bin_bytes = safe_atomics_get(binary_bytes_ref, i + 1)
        kd_bytes = ets_bytes + bin_bytes
        per_shard_max = div(state.max_memory_bytes, max(state.shard_count, 1))

        ratio =
          cond do
            per_shard_max > 0 -> kd_bytes / per_shard_max
            kd_bytes > 0 -> 1.0
            true -> 0.0
          end

        {kd_acc + kd_bytes, Map.put(shards_acc, i, %{bytes: kd_bytes, ratio: ratio})}
      end)

    # Rust NIF heap allocations (Vec, String, etc.) tracked by the global
    # AtomicUsize counter in tracking_alloc.rs. Returns -1 when the tracking
    # allocator is not installed (production cdylib), 0+ when active.
    nif_allocated_bytes =
      case Ferricstore.Bitcask.NIF.rust_allocated_bytes() do
        n when is_integer(n) and n >= 0 -> n
        _ -> 0
      end

    total_bytes = keydir_bytes + nif_allocated_bytes
    ratio = if state.max_memory_bytes > 0, do: total_bytes / state.max_memory_bytes, else: 0.0
    pressure_level = classify_pressure(ratio)

    keydir_ratio = if state.keydir_max_ram > 0, do: keydir_bytes / state.keydir_max_ram, else: 0.0
    keydir_pressure_level = classify_pressure(keydir_ratio)

    # RSS-based pressure: the real physical memory footprint including ETS,
    # NIF allocations, BEAM heaps, and page cache residency.
    # Only used in standalone mode where we own the entire BEAM process.
    # In embedded mode, RSS includes the host app's memory — not meaningful
    # for our pressure decisions.
    {rss_bytes, rss_ratio, rss_pressure_level} =
      case default_process_rss_fn() do
        nil ->
          {0, 0.0, :ok}

        rss_fn ->
          rss = rss_fn.() || 0

          if state.max_memory_bytes > 0 do
            limit = state.memory_limit
            r = if limit > 0, do: rss / limit, else: 0.0
            {rss, r, classify_pressure(r)}
          else
            {rss, 0.0, :ok}
          end
      end

    # Overall pressure is the worse of keydir-based and RSS-based.
    overall_pressure = worse_pressure(pressure_level, rss_pressure_level)

    %{
      total_bytes: total_bytes,
      max_bytes: state.max_memory_bytes,
      ratio: ratio,
      pressure_level: overall_pressure,
      shards: shard_stats,
      eviction_policy: state.eviction_policy,
      keydir_bytes: keydir_bytes,
      hot_cache_bytes: 0,
      keydir_max_ram: state.keydir_max_ram,
      hot_cache_max_ram: state.hot_cache_max_ram,
      hot_cache_min_ram: state.hot_cache_min_ram,
      keydir_pressure_level: keydir_pressure_level,
      keydir_ratio: keydir_ratio,
      rss_bytes: rss_bytes,
      rss_ratio: rss_ratio,
      rss_pressure_level: rss_pressure_level,
      memory_limit: state.memory_limit,
      nif_allocated_bytes: nif_allocated_bytes
    }
  end

  @pressure_order %{ok: 0, warning: 1, pressure: 2, reject: 3}

  defp worse_pressure(a, b) do
    if Map.get(@pressure_order, a, 0) >= Map.get(@pressure_order, b, 0), do: a, else: b
  end

  defp safe_ets_memory(table_name) do
    case :ets.info(table_name, :memory) do
      :undefined -> 0
      memory when is_integer(memory) -> memory * :erlang.system_info(:wordsize)
      _ -> 0
    end
  rescue
    _ -> 0
  catch
    _, _ -> 0
  end

  defp shard_indexes(count) when is_integer(count) and count > 0, do: 0..(count - 1)
  defp shard_indexes(_count), do: []

  defp keydir_binary_bytes_ref do
    ctx = FerricStore.Instance.get(:default)
    ctx && ctx.keydir_binary_bytes
  rescue
    _ -> nil
  end

  defp evict_offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp evict_offheap_size(_), do: 0

  defp safe_atomics_get(nil, _slot), do: 0

  defp safe_atomics_get(ref, slot) do
    val = :atomics.get(ref, slot)
    max(val, 0)
  rescue
    _ -> 0
  end

  defp classify_pressure(ratio) when ratio >= @reject_threshold, do: :reject
  defp classify_pressure(ratio) when ratio >= @pressure_threshold, do: :pressure
  defp classify_pressure(ratio) when ratio >= @warning_threshold, do: :warning
  defp classify_pressure(_ratio), do: :ok

  defp emit_spec_pressure_level(stats, level) do
    :telemetry.execute(
      [:ferricstore, :memory, :pressure],
      %{total_bytes: stats.total_bytes, max_bytes: stats.max_bytes, ratio: stats.ratio},
      %{level: level}
    )
  end

  defp emit_hot_cache_budget_events(state, stats, level) do
    new_budget = hot_cache_budget(stats.max_bytes, level)
    old_budget = state.last_hot_cache_budget

    cond do
      old_budget != nil and new_budget < old_budget ->
        :telemetry.execute(
          [:ferricstore, :hot_cache, :limit_reduced],
          %{new_budget_bytes: new_budget, old_budget_bytes: old_budget},
          %{level: level, shard_count: state.shard_count}
        )

        %{state | last_hot_cache_budget: new_budget}

      old_budget != nil and new_budget > old_budget ->
        :telemetry.execute(
          [:ferricstore, :hot_cache, :limit_restored],
          %{new_budget_bytes: new_budget, old_budget_bytes: old_budget},
          %{level: level, shard_count: state.shard_count}
        )

        %{state | last_hot_cache_budget: new_budget}

      true ->
        %{state | last_hot_cache_budget: new_budget}
    end
  end

  defp emit_shard_pressure_events(stats) do
    Enum.each(stats.shards, fn {index, shard_stat} ->
      if shard_stat.ratio >= @pressure_threshold do
        per_shard_max = div(stats.max_bytes, max(map_size(stats.shards), 1))

        :telemetry.execute(
          [:ferricstore, :memory, :pressure],
          %{
            shard_index: index,
            bytes: shard_stat.bytes,
            max_bytes: per_shard_max,
            ratio: shard_stat.ratio
          },
          %{pressure_level: stats.pressure_level, eviction_policy: stats.eviction_policy}
        )
      end
    end)
  end

  defp to_spec_level(:reject), do: :full
  defp to_spec_level(:pressure), do: :pressure
  defp to_spec_level(:warning), do: :warn
  defp to_spec_level(:ok), do: :ok

  defp hot_cache_budget(max_memory_bytes, :ok), do: div(max_memory_bytes * 50, 100)
  defp hot_cache_budget(max_memory_bytes, :warn), do: div(max_memory_bytes * 30, 100)
  defp hot_cache_budget(max_memory_bytes, :pressure), do: div(max_memory_bytes * 15, 100)
  defp hot_cache_budget(max_memory_bytes, :full), do: div(max_memory_bytes * 5, 100)

  defp schedule_check(interval_ms), do: Process.send_after(self(), :check, interval_ms)

  defp default_interval,
    do: Application.get_env(:ferricstore, :memory_guard_interval_ms, @check_interval_ms)

  defp default_max_memory,
    do: Application.get_env(:ferricstore, :max_memory_bytes, default_system_memory())

  defp default_system_memory do
    limit = detect_memory_limit()
    # Use 75% of the detected limit as the default budget.
    trunc(limit * 0.75)
  end

  # Detects the effective memory limit: cgroup v2 → cgroup v1 → host RAM.
  # In Kubernetes, the cgroup limit reflects the pod's memory limit,
  # which is the correct ceiling (not the host's total RAM).
  @doc false
  def detect_memory_limit do
    cgroup_v2_limit() ||
      cgroup_v1_limit() ||
      host_total_memory() ||
      proc_meminfo_total() ||
      1_073_741_824
  end

  defp cgroup_v2_limit do
    case File.read("/sys/fs/cgroup/memory.max") do
      {:ok, "max\n"} ->
        nil

      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          {bytes, _} when bytes > 0 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp cgroup_v1_limit do
    case File.read("/sys/fs/cgroup/memory/memory.limit_in_bytes") do
      {:ok, data} ->
        case Integer.parse(String.trim(data)) do
          # Very large values (>= 2^62) mean "no limit"
          {bytes, _} when bytes > 0 and bytes < 4_611_686_018_427_387_904 -> bytes
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp host_total_memory do
    memsup_total_memory() || sysctl_total_memory()
  end

  defp memsup_total_memory do
    data = apply(:memsup, :get_system_memory_data, [])

    case data do
      list when is_list(list) -> Keyword.get(list, :total_memory)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp sysctl_total_memory do
    case System.cmd("sysctl", ["-n", "hw.memsize"], stderr_to_stdout: true) do
      {output, 0} -> parse_sysctl_memsize(output)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  @doc false
  def parse_sysctl_memsize(output) when is_binary(output) do
    case Integer.parse(String.trim(output)) do
      {bytes, ""} when bytes > 0 -> bytes
      _ -> nil
    end
  end

  defp proc_meminfo_total do
    case File.read("/proc/meminfo") do
      {:ok, content} ->
        case Regex.run(~r/MemTotal:\s+(\d+)\s+kB/, content) do
          [_, kb_str] ->
            case Integer.parse(kb_str) do
              {kb, _} -> kb * 1024
              _ -> nil
            end

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # Returns the current process RSS (Resident Set Size) in bytes.
  # This is the actual physical memory used by the BEAM process,
  # including ETS, NIF allocations, mmap'd file pages, and BEAM heaps.
  # Falls back to `ps rss` on macOS/Unix, then :erlang.memory(:total) which only
  # covers BEAM-managed memory.
  @doc false
  def process_rss_bytes do
    read_proc_self_rss() ||
      read_ps_rss() ||
      erlang_total_memory()
  end

  # Linux: parse VmRSS from /proc/self/status (in kB)
  defp read_proc_self_rss do
    case File.read("/proc/self/status") do
      {:ok, content} ->
        case Regex.run(~r/VmRSS:\s+(\d+)\s+kB/, content) do
          [_, kb_str] ->
            case Integer.parse(kb_str) do
              {kb, _} -> kb * 1024
              _ -> nil
            end

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  defp read_ps_rss do
    pid = :os.getpid() |> List.to_string()

    case System.cmd("ps", ["-o", "rss=", "-p", pid], stderr_to_stdout: true) do
      {output, 0} -> parse_ps_rss_kb(output)
      _ -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  @doc false
  def parse_ps_rss_kb(output) when is_binary(output) do
    output
    |> String.split()
    |> List.first()
    |> case do
      nil ->
        nil

      value ->
        case Integer.parse(value) do
          {kb, ""} when kb > 0 -> kb * 1024
          _ -> nil
        end
    end
  end

  defp erlang_total_memory do
    try do
      :erlang.memory(:total)
    rescue
      _ -> nil
    catch
      _, _ -> nil
    end
  end

  defp pressure_flag?(slot) do
    case :persistent_term.get({FerricStore.Instance, :default}, nil) do
      %{pressure_flags: ref} -> :atomics.get(ref, slot) == 1
      _missing_or_stale -> false
    end
  end

  defp publish_pressure_flags(stats, state) do
    case default_pressure_flags() do
      nil ->
        :ok

      ref ->
        # Slot 1: keydir_full — reject new key writes (only at :reject)
        :atomics.put(ref, 1, if(stats.keydir_pressure_level == :reject, do: 1, else: 0))
        # Slot 2: reject_writes — reject ALL writes (only :reject + :noeviction)
        reject? = stats.pressure_level == :reject and state.eviction_policy == :noeviction
        :atomics.put(ref, 2, if(reject?, do: 1, else: 0))
        # Slot 3: skip_promotion — don't re-cache cold reads (at :pressure and :reject)
        :atomics.put(ref, 3, if(stats.pressure_level in [:pressure, :reject], do: 1, else: 0))
        :ok
    end
  end

  defp default_pressure_flags do
    case :persistent_term.get({FerricStore.Instance, :default}, nil) do
      %{pressure_flags: ref} -> ref
      _missing_or_stale -> nil
    end
  end

  defp default_instance do
    :persistent_term.get({FerricStore.Instance, :default}, nil)
  end

  defp default_process_rss_fn do
    case default_instance() do
      %{process_rss_fn: fun} -> fun
      _missing_or_stale -> nil
    end
  end

  defp default_eviction_policy,
    do: Application.get_env(:ferricstore, :eviction_policy, :volatile_lru)

  defp default_shard_count, do: Application.get_env(:ferricstore, :shard_count, 4)

  defp default_keydir_max_ram,
    do: Application.get_env(:ferricstore, :keydir_max_ram, 256 * 1024 * 1024)

  defp maybe_update(state, _key, nil), do: state
  defp maybe_update(state, key, value), do: Map.put(state, key, value)
end
