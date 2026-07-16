defmodule Ferricstore.Store.LFU do
  @moduledoc """
  LFU (Least Frequently Used) counter with time-based decay, matching Redis's
  implementation.

  ## Packed format

  The LFU field in each keydir ETS tuple is a single integer packing two values:

    * Upper 16 bits: `ldt` (last decrement time) — minutes since epoch, wraps
      at 2^16 (~45 days).
    * Lower 8 bits: `counter` — logarithmic frequency counter (0-255).

  ## Access algorithm

  On every key access:

  1. **Decay**: elapsed = `now_minutes - ldt`. Reduce counter by
     `elapsed / lfu_decay_time`.
  2. **Increment**: probabilistic log increment with probability
     `1 / (counter * lfu_log_factor + 1)`.
  3. **Update ldt** to current minutes.

  ## Configuration

    * `:lfu_decay_time` — minutes per decay step (default 1, 0 = no decay)
    * `:lfu_log_factor` — controls increment probability curve (default 10)

  New keys start at counter = 5 with the current ldt.
  """

  import Bitwise

  @initial_counter 5

  @doc """
  Initializes persistent_term cache for LFU config values.

  Called once at application startup. Subsequent reads from `touch/3` and
  `effective_counter/1` use `:persistent_term.get/1` (~5ns) instead of
  `Application.get_env/3` (~200-250ns), saving ~400ns per hot GET.
  """
  @spec init_config_cache() :: :ok
  def init_config_cache do
    :persistent_term.put(
      :ferricstore_lfu_decay_time,
      Application.get_env(:ferricstore, :lfu_decay_time, 1)
    )

    :persistent_term.put(
      :ferricstore_lfu_log_factor,
      Application.get_env(:ferricstore, :lfu_log_factor, 10)
    )

    # One atomic word is the per-minute initial value cache. Keeping the minute
    # inside the packed value prevents readers from observing a partially
    # published cache refresh.
    # Avoids persistent_term.put once per minute (global GC in embedded).
    ref = :atomics.new(1, signed: false)
    :persistent_term.put(:ferricstore_lfu_initial_ref, ref)
    :ok
  end

  @doc """
  Returns the initial packed LFU value for a new key (counter=5, ldt=now).

  Uses a per-minute cache in persistent_term to avoid calling
  `System.os_time/1` on every ETS insert (~50ns/call). The cached value
  is refreshed lazily when the current minute changes.
  """
  @spec initial() :: non_neg_integer()
  def initial do
    current_min = now_minutes()
    ref = initial_ref()
    packed = pack(current_min, @initial_counter)

    case :atomics.get(ref, 1) do
      ^packed ->
        packed

      _ ->
        :atomics.put(ref, 1, packed)
        packed
    end
  end

  @doc "Returns the initial packed LFU value using instance ctx."
  @spec initial(FerricStore.Instance.t()) :: non_neg_integer()
  def initial(ctx) do
    current_min = now_minutes()
    ref = ctx.lfu_initial_ref
    packed = pack(current_min, @initial_counter)

    case :atomics.get(ref, 1) do
      ^packed ->
        packed

      _ ->
        :atomics.put(ref, 1, packed)
        packed
    end
  end

  @doc "Returns the initial counter value (5)."
  @spec initial_counter() :: non_neg_integer()
  def initial_counter, do: @initial_counter

  defp initial_ref do
    case :persistent_term.get(:ferricstore_lfu_initial_ref, nil) do
      nil ->
        ref = :atomics.new(1, signed: false)
        :persistent_term.put(:ferricstore_lfu_initial_ref, ref)
        ref

      ref ->
        ref
    end
  end

  @doc "Packs ldt (16-bit minutes) and counter (8-bit) into a single integer."
  @spec pack(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def pack(ldt_minutes, counter) do
    bsl(band(ldt_minutes, 0xFFFF), 8) ||| band(counter, 0xFF)
  end

  @doc "Unpacks a packed LFU integer into `{ldt_minutes, counter}`."
  @spec unpack(non_neg_integer()) :: {non_neg_integer(), non_neg_integer()}
  def unpack(packed) do
    {bsr(packed, 8) &&& 0xFFFF, packed &&& 0xFF}
  end

  @doc "Returns the current time in minutes, masked to 16 bits."
  @spec now_minutes() :: non_neg_integer()
  def now_minutes do
    div(System.os_time(:second), 60) &&& 0xFFFF
  end

  @doc """
  Computes elapsed minutes between `now` and `ldt`, handling 16-bit wraparound.
  """
  @spec elapsed_minutes(non_neg_integer(), non_neg_integer()) :: non_neg_integer()
  def elapsed_minutes(now, ldt) when now >= ldt, do: now - ldt
  def elapsed_minutes(now, ldt), do: 0xFFFF - ldt + now + 1

  @type packed_or_tagged ::
          non_neg_integer() | {:flow_state_version, integer(), non_neg_integer()}

  @doc """
  Returns the effective (decayed) counter for a packed LFU value.

  Applies time-based decay without updating the stored value. Used for eviction
  comparison and OBJECT FREQ.
  """
  @spec effective_counter(packed_or_tagged()) :: non_neg_integer()
  def effective_counter({:flow_state_version, _version, packed}) when is_integer(packed) do
    effective_counter(packed)
  end

  def effective_counter(packed) do
    {ldt, counter} = unpack(packed)
    decay_time = :persistent_term.get(:ferricstore_lfu_decay_time, 1)
    elapsed = elapsed_minutes(now_minutes(), ldt)

    if decay_time > 0 do
      max(0, counter - div(elapsed, decay_time))
    else
      counter
    end
  end

  @doc "Returns the effective (decayed) counter using instance ctx."
  @spec effective_counter(FerricStore.Instance.t(), packed_or_tagged()) :: non_neg_integer()
  def effective_counter(ctx, {:flow_state_version, _version, packed}) when is_integer(packed) do
    effective_counter(ctx, packed)
  end

  def effective_counter(ctx, packed) do
    {ldt, counter} = unpack(packed)
    decay_time = ctx.lfu_decay_time
    elapsed = elapsed_minutes(now_minutes(), ldt)

    if decay_time > 0 do
      max(0, counter - div(elapsed, decay_time))
    else
      counter
    end
  end

  @doc """
  Performs an LFU touch: decays the counter, then probabilistically increments it.

  Returns the new packed LFU value. Updates the ETS entry at position 4.
  """
  @spec touch(atom(), binary(), packed_or_tagged()) :: :ok
  def touch(keydir, key, {:flow_state_version, version, packed}) when is_integer(packed) do
    decay_time = :persistent_term.get(:ferricstore_lfu_decay_time, 1)
    log_factor = :persistent_term.get(:ferricstore_lfu_log_factor, 10)

    touch_packed(keydir, key, packed, decay_time, log_factor, fn new_packed ->
      {:flow_state_version, version, new_packed}
    end)
  end

  def touch(keydir, key, packed) do
    decay_time = :persistent_term.get(:ferricstore_lfu_decay_time, 1)
    log_factor = :persistent_term.get(:ferricstore_lfu_log_factor, 10)

    touch_packed(keydir, key, packed, decay_time, log_factor, & &1)
  end

  @doc "Performs an LFU touch using instance ctx for config values."
  @spec touch(FerricStore.Instance.t(), atom(), binary(), packed_or_tagged()) :: :ok
  def touch(ctx, keydir, key, {:flow_state_version, version, packed}) when is_integer(packed) do
    touch_packed(keydir, key, packed, ctx.lfu_decay_time, ctx.lfu_log_factor, fn new_packed ->
      {:flow_state_version, version, new_packed}
    end)
  end

  def touch(ctx, keydir, key, packed) do
    touch_packed(keydir, key, packed, ctx.lfu_decay_time, ctx.lfu_log_factor, & &1)
  end

  defp touch_packed(keydir, key, packed, decay_time, log_factor, wrap_fun) do
    new_packed = next_packed(packed, decay_time, log_factor)

    # Skip write when unchanged; wrapper metadata is already present in ETS.
    if new_packed != packed do
      :ets.update_element(keydir, key, {4, wrap_fun.(new_packed)})
    end

    :ok
  end

  defp next_packed(packed, decay_time, log_factor) do
    {ldt, counter} = unpack(packed)
    now_min = now_minutes()
    elapsed = elapsed_minutes(now_min, ldt)

    decayed_counter =
      if decay_time > 0 do
        max(0, counter - div(elapsed, decay_time))
      else
        counter
      end

    new_counter =
      if :rand.uniform() < 1.0 / (decayed_counter * log_factor + 1) do
        min(decayed_counter + 1, 255)
      else
        decayed_counter
      end

    pack(now_min, new_counter)
  end
end
