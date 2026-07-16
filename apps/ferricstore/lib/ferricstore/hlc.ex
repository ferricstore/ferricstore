defmodule Ferricstore.HLC do
  @moduledoc """
  Hybrid Logical Clock (HLC) for FerricStore.

  An HLC combines a physical wall-clock component (milliseconds since epoch)
  with a logical counter to produce timestamps that are:

    1. **Monotonically increasing** -- even when the wall clock is not (NTP
       corrections, VM migration, etc.).
    2. **Causally ordered** -- merging a remote timestamp via `update/1`
       ensures happens-before relationships are preserved across nodes.
    3. **Close to real time** -- the physical component tracks
       `System.os_time(:millisecond)` and only diverges when the wall clock
       jumps backward or a remote node is ahead.

  ## Spec reference (2G.6)

  * HLC is piggybacked on Raft heartbeats.
  * Inter-node TTL precision is bounded to Raft heartbeat RTT (~10 ms).
  * A telemetry warning is emitted at 500 ms drift between HLC physical and
    wall clock.
  * TTL-sensitive reads are rejected at 1 000 ms drift.
  * The HLC millisecond component is used for Stream ID generation (XADD `*`).
  * HLC timestamps are stamped on commands **before** they enter Raft -- they
    are not computed inside `apply()`.

  ## Architecture

  The hot-path functions `now/0` and `now_ms/0` are **lock-free**: they use
  a single `:atomics` slot (stored in `:persistent_term`) instead of a
  `GenServer.call`. Physical ms and logical counter are packed into one
  64-bit integer, eliminating the two-slot race that broke monotonicity
  under contention.

  The GenServer is retained only for:

    * Process supervision -- the `init/1` callback creates the atomics ref and
      stores it in `:persistent_term`.

  Packed layout in a single unsigned 64-bit atomic:

      |-- physical_ms (48 bits) --|-- logical (16 bits) --|

  48 bits covers ~8,920 years of milliseconds. 16 bits allows 65,535
  increments per millisecond. If logical overflows, it spills into the
  physical bits — effectively advancing the clock by 1 ms, which is safe.

  ## Usage

  The application supervision tree starts a single named HLC process
  (`Ferricstore.HLC`). Other modules obtain timestamps via:

      {physical_ms, logical} = Ferricstore.HLC.now()
      ms = Ferricstore.HLC.now_ms()

  Command handlers that can run inside Raft apply should use
  `Ferricstore.CommandTime.now_ms/0` instead. It falls back to HLC outside
  Raft and returns the stamped log-entry time during state-machine apply.

  When receiving a Raft heartbeat with a piggybacked timestamp:

      :ok = Ferricstore.HLC.update(remote_timestamp)

  ## Timestamp representation

  A timestamp is a 2-tuple `{physical_ms, logical}` where:

    * `physical_ms` -- milliseconds since Unix epoch (same scale as
      `System.os_time(:millisecond)`)
    * `logical` -- a non-negative integer counter that disambiguates events
      within the same physical millisecond

  Timestamps are ordered lexicographically: physical first, then logical.

  ## Cross-node synchronization

  HLC sync across nodes currently happens via Raft heartbeats (~6/sec), giving
  ~150ms max drift. This is sufficient for all current use cases:

    * **Read-your-writes** on the writing node: guaranteed by local apply
      barriers on committed entries.
    * **Cross-node reads** after quorum write: guaranteed by Raft consensus —
      all quorum nodes applied the command before the write returns.
    * **TTL expiry**: second-granularity TTLs are unaffected by 150ms drift.
    * **Cross-shard WATCH**: uses keydir/version tokens, not timestamps.

  ### Per-command HLC stamping

  Raft write paths stamp commands before submission via
  `Ferricstore.Raft.CommandClock`. The state machine handles commands wrapped
  as `{inner_command, %{hlc_ts: {physical_ms, logical}}}`. When `apply/3`
  processes a wrapped command, it calls `update/1` to merge the leader's
  clock into the follower's HLC and uses the stamped physical millisecond
  for TTL and lock expiry decisions. This gives deterministic per-command
  causal time instead of follower-local apply time.

  This is important for:

    1. **Sub-second TTLs with cross-node reads** — a key with `PX 100`
       (100ms TTL) could appear alive on one node and expired on another
       if followers used local apply time.
    2. **Cross-node causal ordering** — if a feature needs "write A
       happened-before write B" guarantees across nodes beyond Raft log
       ordering (e.g., conflict resolution in multi-leader setups).
    3. **Observed drift exceeding 500ms** — if heartbeat intervals increase
       due to network issues or overloaded nodes.

  Submit commands through `Ferricstore.Raft.CommandClock`:

      Ferricstore.Raft.CommandClock.process_command(shard_id, command)
      Ferricstore.Raft.CommandClock.pipeline_command(shard_id, command, corr, :low)

  Cost: one lock-free HLC stamp on submit plus one `update/1` merge per
  follower during `apply/3`.

  ## Graceful fallback

  When the HLC GenServer has not been started (e.g. in unit tests that exercise
  command modules without the full application), `now/0` and `now_ms/0` fall
  back to `System.os_time(:millisecond)` with a logical counter of 0.
  """

  use GenServer
  import Bitwise

  require Logger

  # ---------------------------------------------------------------------------
  # Types
  # ---------------------------------------------------------------------------

  @typedoc "An HLC timestamp: `{physical_ms, logical_counter}`."
  @type timestamp :: {non_neg_integer(), non_neg_integer()}

  # Drift thresholds (milliseconds).
  @drift_warning_ms 500
  @drift_reject_ms 1_000

  # :persistent_term key for the atomics ref.
  @atomics_key :ferricstore_hlc_ref

  # Single packed slot.
  @slot 1

  # Bit layout: 48-bit physical | 16-bit logical.
  @logical_bits 16
  @logical_mask Bitwise.bsl(1, 16) - 1
  @max_physical_ms Bitwise.bsl(1, 48) - 1
  @max_packed Bitwise.bsl(1, 64) - 1

  # ---------------------------------------------------------------------------
  # Client API
  # ---------------------------------------------------------------------------

  @doc """
  Starts the HLC GenServer.

  The GenServer creates an `:atomics` ref on init and stores it in
  `:persistent_term` so that `now/0` and `now_ms/0` can read/write it
  without a GenServer call.

  ## Options

    * `:name` -- process name (default: `Ferricstore.HLC`)

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the current HLC timestamp as `{physical_ms, logical}`.

  This function is **lock-free** -- it reads and updates a single packed
  `:atomics` slot directly, bypassing the GenServer. Physical ms and logical
  counter are packed into one 64-bit integer, so updates are truly atomic
  with no torn-read races.

  On the common path (same millisecond), uses `add_get(ref, 1, 1)` which
  atomically increments the logical counter with zero contention. CAS is
  only used at millisecond boundaries.

  Falls back to `{System.os_time(:millisecond), 0}` when the HLC GenServer
  has not been started.

  ## Examples

      iex> {phys, logical} = Ferricstore.HLC.now()
      iex> phys > 0
      true
      iex> logical >= 0
      true

  """
  @spec now() :: timestamp()
  def now do
    case atomics_ref() do
      nil ->
        {System.os_time(:millisecond), 0}

      ref ->
        hlc_now_packed(ref)
    end
  end

  @doc """
  Convenience: returns only the physical (millisecond) component of `now/0`.

  This is the value used as the millisecond part of Redis Stream IDs and as
  the base for TTL computations. Lock-free; does not call the GenServer.

  Falls back to `System.os_time(:millisecond)` when the HLC GenServer is
  not running.
  """
  @spec now_ms() :: non_neg_integer()
  def now_ms do
    {physical, _logical} = now()
    physical
  end

  @doc """
  Merges a remote HLC timestamp received from another node.

  Called in two contexts:

    * **Raft heartbeats** (~6 calls/sec) -- the leader's HLC timestamp is
      piggybacked on heartbeat RPCs. This is the current sync mechanism,
      giving ~150ms max drift between nodes.
    * **Per-command stamping** (not wired up) -- if commands are wrapped as
      `{inner_command, %{hlc_ts: ts}}`, the state machine calls `update/1`
      during `apply/3` on each follower, giving per-command causal sync.
      See the module doc for when to enable this.

  This is lock-free on the normal path. Remote timestamp merges use the same
  packed atomic as `now/0` and publish with compare-and-swap, so state-machine
  apply does not serialize through the HLC GenServer when commands carry
  stamped HLC metadata.

  The merge rule follows the standard HLC algorithm:

    1. `new_physical = max(wall_clock, local_physical, remote_physical)`
    2. If all three physical values tie, `logical = max(local_logical,
       remote_logical) + 1`.
    3. If two tie at the max, the logical from the winner is incremented.
    4. If wall clock alone wins, logical resets to 0.

  ## Parameters

    * `remote_ts` -- the remote HLC timestamp `{physical_ms, logical}`

  """
  @spec update(timestamp()) :: :ok
  def update(remote_ts) do
    hlc_update_packed(remote_ts)
  end

  @doc """
  Returns the future drift in milliseconds between the HLC physical
  component and the current wall clock.

  This function is **lock-free** -- it reads the atomics ref directly.

  Under normal single-node operation this is 0 or near-0. A non-zero drift
  indicates that `update/1` received a future timestamp from a remote node
  or the wall clock jumped backward.
  """
  @spec drift_ms() :: non_neg_integer()
  def drift_ms do
    case atomics_ref() do
      nil ->
        0

      ref ->
        {phys, _logical} = unpack(:atomics.get(ref, @slot))
        wall = System.os_time(:millisecond)
        max(phys - wall, 0)
    end
  end

  @doc """
  Returns `true` when drift exceeds the reject threshold (1 000 ms), meaning
  TTL-sensitive reads should not be served.

  Lock-free; does not call the GenServer.
  """
  @spec drift_exceeded?() :: boolean()
  def drift_exceeded? do
    drift_ms() >= @drift_reject_ms
  end

  @doc """
  Compares two HLC timestamps.

  Returns `:lt`, `:eq`, or `:gt` following the same convention as
  `DateTime.compare/2`.

  ## Examples

      iex> Ferricstore.HLC.compare({100, 0}, {200, 0})
      :lt
      iex> Ferricstore.HLC.compare({100, 1}, {100, 0})
      :gt
      iex> Ferricstore.HLC.compare({100, 0}, {100, 0})
      :eq

  """
  @spec compare(timestamp(), timestamp()) :: :lt | :eq | :gt
  def compare({p1, l1}, {p2, l2}) do
    cond do
      p1 > p2 -> :gt
      p1 < p2 -> :lt
      l1 > l2 -> :gt
      l1 < l2 -> :lt
      true -> :eq
    end
  end

  @doc """
  Extracts the millisecond (physical) component from an HLC timestamp.

  This is used when a caller needs a plain integer millisecond value, for
  example as the ms part of a Redis Stream ID.

  ## Examples

      iex> Ferricstore.HLC.encode_ms({1_234_567_890, 42})
      1_234_567_890

  """
  @spec encode_ms(timestamp()) :: non_neg_integer()
  def encode_ms({physical_ms, _logical}), do: physical_ms

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(_opts) do
    ref = :atomics.new(1, signed: false)
    :atomics.put(ref, @slot, 0)
    :persistent_term.put(@atomics_key, ref)
    {:ok, %{}}
  end

  @impl true
  def terminate(_reason, _state) do
    :persistent_term.erase(@atomics_key)
    :ok
  rescue
    ArgumentError -> :ok
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp merge_timestamps(wall, local_phys, local_logical, remote_phys, remote_log) do
    max_phys = Enum.max([wall, local_phys, remote_phys])
    merge_by_max(max_phys, wall, local_phys, local_logical, remote_phys, remote_log)
  end

  # Wall clock is strictly ahead of both local and remote.
  defp merge_by_max(wall, wall, local_phys, _ll, remote_phys, _rl)
       when wall > local_phys and wall > remote_phys,
       do: {wall, 0}

  # Local and remote physical tie -- merge logical counters.
  defp merge_by_max(_max, _wall, lp, ll, lp, rl), do: {lp, max(ll, rl) + 1}

  # Wall ties with local physical (both >= remote).
  defp merge_by_max(lp, lp, lp, ll, _rp, _rl), do: {lp, ll + 1}

  # Wall ties with remote physical (both >= local).
  defp merge_by_max(rp, rp, _lp, _ll, rp, rl), do: {rp, rl + 1}

  # Local physical is the sole max.
  defp merge_by_max(lp, _wall, lp, ll, _rp, _rl), do: {lp, ll + 1}

  # Remote physical is the sole max.
  defp merge_by_max(rp, _wall, _lp, _ll, rp, rl), do: {rp, rl + 1}

  # Fallback.
  defp merge_by_max(max_phys, _wall, _lp, _ll, _rp, _rl), do: {max_phys, 0}

  # Pack {physical_ms, logical} into a single unsigned 64-bit integer.
  # Physical occupies the upper 48 bits, logical the lower 16 bits.
  defp pack(physical_ms, logical) do
    bsl(physical_ms, @logical_bits) + logical
  end

  # Unpack a single unsigned 64-bit integer into {physical_ms, logical}.
  defp unpack(packed) do
    physical = bsr(packed, @logical_bits)
    logical = band(packed, @logical_mask)
    {physical, logical}
  end

  # Lock-free HLC now() using a single packed 64-bit atomic.
  #
  # Common path (same millisecond): add_get(ref, 1, 1) — atomically
  # increments the logical counter with zero contention.
  #
  # Millisecond boundary: CAS to jump the packed value forward.
  # If CAS fails, another process already advanced — just add_get.
  #
  # Logical overflow (>65535 per ms): spills into physical bits,
  # effectively advancing the clock by 1ms. This is correct behavior.
  defp hlc_now_packed(ref) do
    wall_packed = pack(System.os_time(:millisecond), 0)
    current = :atomics.get(ref, @slot)

    if wall_packed > current do
      # Wall clock is ahead — try to jump forward via CAS.
      case :atomics.compare_exchange(ref, @slot, current, wall_packed) do
        :ok ->
          unpack(wall_packed)

        _actual ->
          # Another process already advanced. Increment by 1.
          unpack(:atomics.add_get(ref, @slot, 1))
      end
    else
      # Same or earlier wall clock — just increment logical by 1.
      unpack(:atomics.add_get(ref, @slot, 1))
    end
  end

  defp hlc_update_packed({remote_phys, remote_log})
       when is_integer(remote_phys) and remote_phys >= 0 and
              remote_phys <= @max_physical_ms and is_integer(remote_log) and remote_log >= 0 and
              remote_log <= @logical_mask do
    case atomics_ref() do
      nil ->
        :ok

      ref ->
        hlc_update_packed(ref, remote_phys, remote_log)
    end
  end

  defp hlc_update_packed(_remote_ts), do: :ok

  defp hlc_update_packed(ref, remote_phys, remote_log) do
    current = :atomics.get(ref, @slot)
    wall = System.os_time(:millisecond)
    {local_phys, local_logical} = unpack(current)

    {new_physical, new_logical} =
      merge_timestamps(wall, local_phys, local_logical, remote_phys, remote_log)

    target = pack(new_physical, new_logical)

    cond do
      target > @max_packed ->
        :ok

      target <= current ->
        maybe_emit_drift_warning(local_phys, wall)
        :ok

      true ->
        case :atomics.compare_exchange(ref, @slot, current, target) do
          :ok ->
            maybe_emit_drift_warning(new_physical, wall)
            :ok

          _actual ->
            hlc_update_packed(ref, remote_phys, remote_log)
        end
    end
  end

  # Returns the atomics ref from :persistent_term, or nil if not yet created.
  @spec atomics_ref() :: reference() | nil
  defp atomics_ref do
    :persistent_term.get(@atomics_key)
  rescue
    ArgumentError -> nil
  end

  defp maybe_emit_drift_warning(hlc_physical, wall_clock) do
    drift = max(hlc_physical - wall_clock, 0)

    if drift >= @drift_warning_ms do
      :telemetry.execute(
        [:ferricstore, :hlc, :drift_warning],
        %{drift_ms: drift},
        %{hlc_physical: hlc_physical, wall_clock: wall_clock}
      )
    end

    if drift >= @drift_reject_ms do
      Logger.error(
        "HLC drift #{drift}ms exceeds reject threshold (#{@drift_reject_ms}ms); " <>
          "TTL-sensitive reads will be rejected"
      )
    end
  end
end
