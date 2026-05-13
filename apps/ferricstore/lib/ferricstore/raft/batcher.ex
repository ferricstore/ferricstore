defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  Namespace-aware group commit batcher for a single FerricStore shard.

  Per spec sections 2C.5 and 2F.3, each shard has its own Batcher GenServer
  that accumulates write commands into per-namespace buffers, each with its
  own commit window. Writes are quorum-only; the old namespace-local weaker
  write mode has been removed.

  ## How it works

  1. A client calls `write/2` which sends a `GenServer.call` to the batcher.
  2. The batcher extracts the key's namespace prefix (e.g. `"session"` from
     `"session:abc123"`, `"_root"` for keys without a colon).
  3. The namespace config is looked up from the `:ferricstore_ns_config` ETS
     table to determine `window_ms` for this prefix.
  4. The command and caller are appended to the namespace's quorum buffer slot.
  5. On the first write to an empty slot, a timer is started using the
     namespace's `window_ms`.
  6. When the timer fires (`:flush_slot`), only that slot's commands are
     submitted to Raft via `ra:pipeline_command/3`.
  7. Each caller receives their individual result from the batch once the
     ra command commits and the batcher receives the `ra_event` notification.

  ## Pipelined ra submission (non-blocking)

  To avoid serializing all writers through one GenServer while the previous
  batch is in-flight through Raft consensus, the batcher uses
  `ra:pipeline_command/3` instead of the blocking `ra:process_command/2`.

  `pipeline_command` is a cast -- it returns immediately with `:ok`, and
  the batcher receives an async `{ra_event, Leader, {applied, [...]}}` message
  when the command is committed and applied by the state machine.

  The batcher maintains a `pending` map keyed by correlation reference, which
  maps each in-flight batch to the list of callers (`froms`) that are waiting
  for a reply. When the `ra_event` arrives, the batcher extracts the result
  and calls `GenServer.reply/2` for each caller.

  This means the GenServer never blocks on Raft. During the time a batch is
  in-flight, the batcher continues to accept new writes and accumulate them
  into fresh slots. This eliminates the throughput bottleneck where 50 writers
  were serialized through one blocked GenServer.

  ## Namespace configuration

  Per-prefix configuration is originally sourced from the `:ferricstore_ns_config`
  ETS table managed by `Ferricstore.NamespaceConfig`. To avoid two ETS lookups
  (~400ns) on every write, the batcher caches namespace config in its process
  state (`ns_cache`). The first write for a given prefix fetches from ETS and
  caches the result; subsequent writes for the same prefix use the cached
  value with zero ETS overhead.

  When namespace config changes (via `FERRICSTORE.CONFIG SET` or `RESET`),
  `NamespaceConfig` broadcasts `:ns_config_changed` to all batcher processes,
  which clears their caches. The next write for any prefix then re-reads
  from ETS.

  If no configuration exists for a prefix, the default window is used:
  `window_ms = 1`.

  Quorum commands are submitted to ra via `:ra.pipeline_command/4` with a
  correlation reference and low priority so Ra can batch the hot write stream.
  Callers are replied to when the `ra_event`
  notification arrives confirming the command was applied.

  The module still has an internal `{prefix, :origin_replay}` slot for
  low-level local-origin replication helpers such as release-cursor pokes.
  That path is not namespace durability: it is explicit, internal, and wraps
  commands as `{:async, origin, cmd}` so the state machine can origin-skip
  effects that were already applied locally.

  ## Why a separate GenServer?

  The batcher is intentionally separate from the Shard GenServer and the
  ra state machine. This separation keeps the batching logic independent
  of the consensus layer and allows the Shard to remain focused on read
  operations and ETS management.

  ## Configuration

    * `:shard_id` (required) -- the ra server ID for this shard
    * `:shard_index` (required) -- zero-based shard index
    * `:max_batch_size` -- flush single-write slot when it reaches this size (default: 50_000)
    * `:max_batch_bytes` -- flush slot before estimated payload bytes exceed this cap (default: 4 MiB)
    * `:max_pending_batches` -- max in-flight Ra batches before backpressure (default: 256)
    * `:max_pending_bytes` -- max serialized in-flight Ra payload bytes before backpressure (default: 0, disabled)
  """

  use GenServer

  require Logger

  alias Ferricstore.ErrorReasons
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.CommandClock
  alias Ferricstore.Raft.PerfToggles

  @default_max_batch_size 50_000
  @default_max_batch_bytes 4 * 1024 * 1024
  @default_max_pending_batches 256
  @default_max_pending_bytes 0
  @sync_pause_error {:error, "ERR shard writes paused for sync"}

  # Origin retry tuning (Option R1 from the rejected-retry design). When Ra
  # returns :rejected {:not_leader, hint, corr} for an origin replay batch, the
  # Batcher re-submits to the hinted leader up to @max_origin_retries times
  # before giving up.
  @max_origin_retries 3

  # Pending entries that don't receive :applied or :rejected within this
  # window are dropped by the periodic sweep. Guards against lost ra_event
  # messages (shouldn't happen, but bounded memory beats unbounded leak).
  @origin_pending_ttl_ms 30_000
  # Quorum callers (blocked on :single/:batch) get a longer TTL — their
  # `GenServer.call` default timeout is 5s, but Router's quorum_write uses
  # 10s. Use 30s as a safety net: anything pending longer than 30s means
  # ra lost the ack entirely and the caller's own call has already errored.
  @quorum_pending_ttl_ms 30_000

  # Periodic sweep interval. Tight enough to catch stalls quickly, loose
  # enough not to burn CPU scanning an empty pending map.
  @origin_pending_sweep_ms 10_000

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:put_blob_ref, binary(), binary(), non_neg_integer()}
          | {:set_blob_ref, binary(), binary(), non_neg_integer(), map()}
          | {:put_batch, [{binary(), binary(), non_neg_integer()}]}
          | {:put_blob_batch, [{binary(), binary(), non_neg_integer(), :value | :blob_ref}]}
          | {:delete, binary()}
          | {:delete_batch, [binary()]}
          | {:incr, binary(), integer()}
          | {:incr_float, binary(), float()}
          | {:append, binary(), binary()}
          | {:append_blob_ref, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:getdel, binary()}
          | {:getex, binary(), non_neg_integer()}
          | {:setrange, binary(), non_neg_integer(), binary()}
          | {:setrange_blob_ref, binary(), non_neg_integer(), binary()}
          | {:cas, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:cas_blob_ref, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:locked_put, binary(), binary(), non_neg_integer(), term()}
          | {:locked_put_blob_ref, binary(), binary(), non_neg_integer(), term()}
          | {:getset_blob_ref, binary(), binary()}
          | {:lock, binary(), binary(), non_neg_integer()}
          | {:unlock, binary(), binary()}
          | {:extend, binary(), binary(), non_neg_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer(),
             non_neg_integer()}
          | {:list_op, binary(), term()}
          | {:compound_put, binary(), binary(), non_neg_integer()}
          | {:compound_put_blob_ref, binary(), binary(), non_neg_integer()}
          | {:compound_batch_put, binary(), [{binary(), binary(), non_neg_integer()}]}
          | {:compound_blob_batch_put, binary(),
             [{binary(), binary(), non_neg_integer(), :value | :blob_ref}]}
          | {:compound_delete, binary()}
          | {:compound_batch_delete, binary(), [binary()]}
          | {:compound_delete_prefix, binary()}
          | {:pfadd, binary(), [binary()]}
          | {:pfmerge, binary(), [binary()]}
          | {:spop, binary(), nil | non_neg_integer()}
          | {:zpop, binary(), non_neg_integer(), :min | :max}
          | {:json_set, binary(), binary() | list(), binary(), list()}
          | {:json_del, binary(), binary() | list()}
          | {:json_numincrby, binary(), binary() | list(), number()}
          | {:json_arrappend, binary(), binary() | list(), [binary()]}
          | {:json_toggle, binary(), binary() | list()}
          | {:json_clear, binary(), binary() | list()}
          | {:cross_shard_tx, list()}
          | {:flow_create, binary(), map()}
          | {:flow_create_many, binary(), map()}
          | {:flow_spawn_children, binary(), map()}
          | {:flow_claim_due, binary(), map()}
          | {:flow_extend_lease, binary(), map()}
          | {:flow_complete, binary(), map()}
          | {:flow_complete_many, binary(), map()}
          | {:flow_transition, binary(), map()}
          | {:flow_transition_many, binary(), map()}
          | {:flow_retry, binary(), map()}
          | {:flow_retry_many, binary(), map()}
          | {:flow_fail, binary(), map()}
          | {:flow_fail_many, binary(), map()}
          | {:flow_cancel, binary(), map()}
          | {:flow_cancel_many, binary(), map()}
          | {:flow_rewind, binary(), map()}
          | {:origin_checked, binary(), command(), binary() | nil, non_neg_integer()}
          | {:origin_checked, binary(), command(), binary() | nil, non_neg_integer(),
             binary() | nil, non_neg_integer()}

  @typedoc """
  A slot key identifies a unique batching bucket by namespace prefix and
  submission mode. Normal writes always use `:quorum`; `:origin_replay` is
  reserved for explicit internal local-origin replication helpers.
  """
  @type slot_key :: {binary(), :quorum | :origin_replay}

  @typedoc """
  A slot holds the accumulated commands and callers for a single namespace
  buffer, along with the timer reference for that slot's commit window.
  """
  @type slot :: %{
          cmds: [command()],
          froms: [GenServer.from()],
          timer_ref: reference() | nil,
          window_ms: non_neg_integer(),
          count: non_neg_integer(),
          bytes: non_neg_integer(),
          created_mono: integer()
        }

  defstruct [
    :shard_id,
    :shard_index,
    :max_batch_size,
    :max_batch_bytes,
    :max_pending_batches,
    :max_pending_bytes,
    slots: %{},
    ns_cache: %{},
    # Map from correlation ref -> pending entry for in-flight ra commands.
    # Entries carry serialized bytes so large-payload bursts are bounded by
    # byte pressure, not only by batch count.
    pending: %{},
    pending_bytes: 0,
    # List of {from} callers waiting for all in-flight to drain (flush barrier)
    flush_waiters: [],
    # Highest ra_index this node's state machine has applied locally. Updated
    # via `{:locally_applied, ra_index}` messages emitted by the local state
    # machine apply via a `:local` send_msg effect (every voter node fires).
    # We use this to gate quorum-write replies so the originating node only
    # tells the user `:ok` after its OWN ETS has the entry, fixing the
    # read-your-write hole on followers.
    last_local_applied: 0,
    # Replies pending local apply: list of {ra_index, kind, froms, result}
    # waiting for `last_local_applied >= ra_index`. Drained in order on each
    # `:locally_applied` message.
    local_apply_waiters: [],
    # Highest origin replay Ra index whose leader-applied event has arrived but that
    # this node may not have locally applied yet. Async callers are not blocked
    # on this, but flush/shutdown barriers must wait so local state-machine
    # side effects are caught up before reporting the shard drained.
    origin_local_apply_index: 0,
    # Set by cluster data sync before copying shard storage. The batcher
    # bypasses the Shard GenServer for optimized pipeline batches, so the
    # Shard's own writes_paused flag is not enough to freeze a join snapshot.
    writes_paused: false
  ]

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Starts a batcher GenServer for the given shard.

  ## Options

    * `:shard_id` (required) -- ra server ID `{name, node()}` for this shard
    * `:shard_index` (required) -- zero-based shard index (used for process name)
    * `:max_batch_size` -- max commands per slot before forced flush (default: #{@default_max_batch_size})
    * `:max_batch_bytes` -- max estimated payload bytes per slot before forced flush (default: #{@default_max_batch_bytes})
    * `:max_pending_bytes` -- max serialized in-flight Ra payload bytes before backpressure (default: #{@default_max_pending_bytes})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    shard_index = Keyword.fetch!(opts, :shard_index)
    GenServer.start_link(__MODULE__, opts, name: batcher_name(shard_index))
  end

  @doc """
  Submits a write command to the batcher for the given shard.

  The command is accumulated into the appropriate namespace buffer and
  submitted when the namespace's commit window expires or the buffer
  reaches `max_batch_size`.

  This call blocks until the ra command is committed and applied.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `command` -- a write command tuple, e.g. `{:put, key, value, expire_at_ms}`

  ## Returns

    * `:ok` on success
    * `{:error, reason}` on failure
  """
  @spec write(non_neg_integer(), command()) :: :ok | {:error, term()}
  def write(shard_index, command) do
    case Process.get(:ferricstore_forward_origin) do
      nil ->
        GenServer.call(batcher_name(shard_index), {:write, command}, 10_000)

      origin_node ->
        GenServer.call(
          batcher_name(shard_index),
          {:write_forwarded, command, origin_node},
          10_000
        )
    end
  end

  @doc """
  Pauses user-visible writes for shard data sync and waits for queued writes to drain.

  DataSync copies Bitcask, promoted files, and blob side-channel files as a
  point-in-time baseline for a joining node. Optimized pipeline batches enter
  through this batcher directly, bypassing the Shard GenServer's pause flag, so
  sync must pause both layers before copying.
  """
  @spec pause_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def pause_writes_for_sync(shard_index, timeout \\ 30_000) do
    safe_sync_pause_call(shard_index, :pause_writes_for_sync, timeout)
  end

  @doc false
  @spec resume_writes_for_sync(non_neg_integer(), timeout()) :: :ok | {:error, term()}
  def resume_writes_for_sync(shard_index, timeout \\ 5_000) do
    safe_sync_pause_call(shard_index, :resume_writes_for_sync, timeout)
  end

  defp safe_sync_pause_call(shard_index, request, timeout) do
    GenServer.call(batcher_name(shard_index), request, timeout)
  catch
    :exit, {:noproc, _} = reason -> {:error, reason}
    :exit, {:timeout, _} = reason -> {:error, reason}
    :exit, reason -> {:error, reason}
  end

  @doc """
  Submits a write command asynchronously, replying directly to `reply_to`.

  Unlike `write/2`, this function does not block the calling process.
  The batcher accepts the command via `GenServer.cast` (non-blocking) and
  will call `GenServer.reply(reply_to, result)` when the command is committed.

  This is used by the Shard GenServer to avoid blocking on Raft consensus.
  The Shard returns `{:noreply, state}` and the Batcher replies directly
  to the original caller (Router/connection process).

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `command` -- a write command tuple
    * `reply_to` -- the `from` ref from the caller's `GenServer.call`
  """
  @spec write_async(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async(shard_index, command, reply_to) do
    GenServer.cast(batcher_name(shard_index), {:write, command, reply_to})
  end

  @doc """
  Like `write_async/3` but routes through the explicit quorum slot. Used by
  operations that need the quorum write path even when the caller enters via
  a non-blocking GenServer cast.
  """
  @spec write_async_quorum(non_neg_integer(), command(), GenServer.from()) :: :ok
  def write_async_quorum(shard_index, command, reply_to) do
    GenServer.cast(batcher_name(shard_index), {:write_quorum, command, reply_to})
  end

  @doc false
  @spec write_async_quorum_forwarded(non_neg_integer(), command(), GenServer.from(), node()) ::
          :ok
  def write_async_quorum_forwarded(shard_index, command, reply_to, origin_node) do
    GenServer.cast(
      batcher_name(shard_index),
      {:write_quorum, command, remote_origin_from(origin_node, reply_to)}
    )
  end

  @doc """
  Submits multiple quorum write commands in a single message.

  Takes a list of commands and a single `from` ref. All commands are
  enqueued into the quorum slot as a batch. The `from` receives a
  single `{:ok, [results]}` reply after Ra commit — one round-trip
  per shard, not per command.
  """
  @spec write_batch(non_neg_integer(), [command()], GenServer.from()) :: :ok
  def write_batch(shard_index, cmds, from) do
    GenServer.cast(batcher_name(shard_index), {:write_batch, cmds, length(cmds), from})
  end

  @doc """
  Submits a pre-normalized SET batch.

  This is the hot RESP pipeline path: callers should build the final
  `{:put_batch, entries}` shape directly instead of allocating one
  `{:put, key, value, expire_at_ms}` tuple per key and asking the batcher
  to compact it later.

  The apply-side contract matters as much as the term shape. `{:put_batch, _}`
  is a pure write batch, so the state machine must stage disk records and
  publish ETS once after Bitcask returns ordered append locations. It should
  not create temporary pending ETS rows unless a future term needs
  read-your-own-write inside the same Ra entry.
  """
  @spec write_put_batch(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from()
        ) :: :ok
  def write_put_batch(shard_index, entries, from) do
    GenServer.cast(batcher_name(shard_index), {:write_put_batch, entries, length(entries), from})
  end

  @doc false
  @spec write_put_batch_forwarded(
          non_neg_integer(),
          [{binary(), binary(), non_neg_integer()}],
          GenServer.from(),
          node()
        ) :: :ok
  def write_put_batch_forwarded(shard_index, entries, from, origin_node) do
    GenServer.cast(
      batcher_name(shard_index),
      {:write_put_batch, entries, length(entries), remote_origin_from(origin_node, from)}
    )
  end

  @doc """
  Submits a pre-normalized DELETE batch.

  This keeps delete-heavy internal callers and future RESP pipeline fast paths
  on the final `{:delete_batch, keys}` Raft shape from the start.

  Future specialized Ra command terms should follow the same shape: build the
  final compact term before Ra serialization, preserve the logical command
  count for replies, and add a matching bulk apply path with rollback/order
  tests before using it on a hot path.
  """
  @spec write_delete_batch(non_neg_integer(), [binary()], GenServer.from()) :: :ok
  def write_delete_batch(shard_index, keys, from) do
    GenServer.cast(batcher_name(shard_index), {:write_delete_batch, keys, length(keys), from})
  end

  @doc false
  @spec write_delete_batch_forwarded(non_neg_integer(), [binary()], GenServer.from(), node()) ::
          :ok
  def write_delete_batch_forwarded(shard_index, keys, from, origin_node) do
    GenServer.cast(
      batcher_name(shard_index),
      {:write_delete_batch, keys, length(keys), remote_origin_from(origin_node, from)}
    )
  end

  @doc false
  @spec write_batch_forwarded(non_neg_integer(), [command()], GenServer.from(), node()) :: :ok
  def write_batch_forwarded(shard_index, cmds, from, origin_node) do
    GenServer.cast(
      batcher_name(shard_index),
      {:write_batch, cmds, length(cmds), remote_origin_from(origin_node, from)}
    )
  end

  @doc false
  @spec remote_origin_from(node(), GenServer.from()) :: {:remote_origin, node(), GenServer.from()}
  # Forwarded writes arrive at the leader through an erpc server process, so
  # the plain GenServer.from() looks local to the leader. Carry the real origin
  # node explicitly so replies include a Ra index and the origin can wait for
  # its own local state machine before acknowledging the client.
  def remote_origin_from(origin_node, from), do: {:remote_origin, origin_node, from}

  @doc """
  Submits an explicit internal local-origin replication command.

  This is not a public write mode. Callers use this only after they have already
  applied the local side effect and need to send a best-effort replicated poke
  or delta through Raft without waiting for local re-application.

  Commands are wrapped as `{:async, inner_cmd}` before submission so the
  state machine can distinguish them: on the origin node (which has the entry
  in ETS) apply/3 will skip; on replicas the inner command is applied normally.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `inner_command` -- the raw write command (e.g. `{:put, k, v, exp}`)
  """
  @spec origin_submit(non_neg_integer(), command()) :: :ok
  def origin_submit(shard_index, inner_command) do
    GenServer.cast(batcher_name(shard_index), {:origin_submit, inner_command})
  end

  @doc """
  Enqueues an explicit internal local-origin replication command and waits
  until the Batcher has submitted its local slot to Raft.
  """
  @spec origin_submit_ordered(non_neg_integer(), command()) ::
          :ok | {:error, :overloaded | {:ra_target_down, term()}}
  def origin_submit_ordered(shard_index, inner_command) do
    GenServer.call(batcher_name(shard_index), {:origin_submit_ordered, inner_command}, 5_000)
  end

  @doc """
  Enqueues an explicit internal local-origin replication command and returns
  after the local Batcher has accepted it into a slot.

  This is intentionally weaker than `origin_submit_ordered/2`: it preserves the
  low-latency local-origin helper path where waiting for
  `ra.pipeline_command/4` would dominate command latency. It must not be used
  to reintroduce namespace-local weak writes.
  """
  @spec origin_enqueue_ordered(non_neg_integer(), command()) ::
          :ok | {:error, :overloaded | {:ra_target_down, term()}}
  def origin_enqueue_ordered(shard_index, inner_command) do
    GenServer.call(batcher_name(shard_index), {:origin_enqueue_ordered, inner_command}, 5_000)
  end

  @doc """
  Submits multiple explicit internal local-origin replication commands as one
  Raft pipeline batch.
  """
  @spec origin_submit_batch_ordered(non_neg_integer(), [command()]) :: :ok | {:error, :overloaded}
  def origin_submit_batch_ordered(_shard_index, []), do: :ok

  def origin_submit_batch_ordered(shard_index, commands) do
    GenServer.call(batcher_name(shard_index), {:origin_submit_batch_ordered, commands}, 5_000)
  end

  @doc """
  Returns whether the local origin replay Batcher can currently accept another
  shard-local origin replay submission.

  Router uses this as a cheap preflight before multi-shard origin replay batches so
  an already-overloaded shard does not let earlier shard writes become locally
  visible before the batch returns an overall error. The actual submit still
  performs the authoritative check.
  """
  @spec origin_accepting?(non_neg_integer()) :: boolean()
  def origin_accepting?(shard_index) do
    GenServer.call(batcher_name(shard_index), :origin_accepting?, 5_000)
  catch
    :exit, _ -> false
  end

  @doc """
  Submits a list of origin replay commands to the batcher in a single cast.

  Same semantics as calling `origin_submit/2` for each command, but sends
  one GenServer cast instead of N, reducing message passing overhead for
  batched internal submissions.
  """
  @spec origin_submit_batch(non_neg_integer(), [command()]) :: :ok
  def origin_submit_batch(shard_index, commands) do
    GenServer.cast(batcher_name(shard_index), {:origin_submit_batch, commands})
  end

  @doc """
  Returns the registered process name for the batcher at `shard_index`.

  ## Examples

      iex> Ferricstore.Raft.Batcher.batcher_name(0)
      :"Ferricstore.Raft.Batcher.0"
  """
  @spec batcher_name(non_neg_integer()) :: atom()
  def batcher_name(shard_index), do: :"Ferricstore.Raft.Batcher.#{shard_index}"

  @doc """
  Blocks until this node's local state machine for `shard_index` has applied
  the entry at `ra_index`. Used by `Router.forward_to_leader/4` to barrier
  read-your-write after a quorum write was redirected to a peer leader.

  Returns `:ok` once `last_local_applied >= ra_index`, or `{:error, :timeout}`
  if the local apply hasn't caught up within `timeout_ms`.
  """
  @spec await_local_applied(non_neg_integer(), non_neg_integer(), non_neg_integer()) ::
          :ok | {:error, :timeout}
  def await_local_applied(shard_index, ra_index, timeout_ms \\ 10_000) do
    name = batcher_name(shard_index)

    try do
      GenServer.call(name, {:await_local_applied, ra_index}, timeout_ms)
    catch
      :exit, {:timeout, _} ->
        :telemetry.execute(
          [:ferricstore, :batcher, :local_apply_timeout],
          %{count: 1},
          %{shard_index: shard_index, ra_index: ra_index, timeout_ms: timeout_ms}
        )

        GenServer.cast(name, {:cancel_await_local_applied, self()})
        {:error, :timeout}
    end
  end

  @doc """
  Synchronously flushes all pending writes across all namespace slots.

  Used in tests and before shard shutdown to ensure all writes are committed.
  Waits for all in-flight pipelined ra commands to complete before returning.
  """
  @spec flush(non_neg_integer()) :: :ok
  def flush(shard_index) do
    flush(shard_index, 10_000)
  end

  @spec flush(non_neg_integer(), timeout()) :: :ok
  def flush(shard_index, timeout) do
    GenServer.call(batcher_name(shard_index), :flush, timeout)
  end

  @doc """
  Flushes all running Raft batchers.

  Missing batchers are ignored so partial test/custom-instance topologies can
  reuse this helper. Live batchers that fail, time out, or return unexpected
  results are reported with their shard index so shutdown does not fail open.
  """
  @spec flush_all(non_neg_integer(), timeout()) :: :ok | {:error, [{non_neg_integer(), term()}]}
  def flush_all(shard_count \\ 4, timeout \\ 10_000) do
    failures =
      shard_count
      |> shard_indices()
      |> Enum.reduce([], fn shard_index, acc ->
        case flush_batcher_for_all(shard_index, timeout) do
          :ok -> acc
          {:error, reason} -> [{shard_index, reason} | acc]
          other -> [{shard_index, {:unexpected_flush_result, other}} | acc]
        end
      end)

    case failures do
      [] -> :ok
      failures -> {:error, Enum.reverse(failures)}
    end
  end

  defp shard_indices(0), do: []
  defp shard_indices(shard_count), do: 0..(shard_count - 1)

  defp flush_batcher_for_all(shard_index, timeout) do
    try do
      flush(shard_index, timeout)
    rescue
      error -> {:error, {:flush_exception, error}}
    catch
      :exit, {:noproc, _call} -> :ok
      :exit, reason -> {:error, {:flush_exit, reason}}
      kind, reason -> {:error, {:flush_throw, kind, reason}}
    end
  end

  @doc """
  Force-resets the Batcher's pending correlations and flush waiters.

  Intended for test setup/teardown when a prior test left the Batcher in a
  stuck state (ra leader crash, disk errors, orphan correlations). Callers
  blocked on `:single`/`:batch` pending entries receive `{:error, :reset}`;
  flush waiters receive `:ok`.

  Do not call this from production code — it silently drops replication
  acks.
  """
  @spec reset_pending(non_neg_integer()) :: :ok
  def reset_pending(shard_index) do
    try do
      GenServer.call(batcher_name(shard_index), :__reset_pending__, 5_000)
    catch
      :exit, _ -> :ok
    end
  end

  @doc """
  Extracts the namespace prefix from a command's key.

  The prefix is the portion of the key before the first colon (`:`).
  Keys without a colon are assigned to the `"_root"` namespace.

  ## Parameters

    * `command` -- a write command tuple

  ## Examples

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "session:abc", "v", 0})
      "session"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:delete, "nocolon"})
      "_root"

      iex> Ferricstore.Raft.Batcher.extract_prefix({:put, "ts:sensor:42", "v", 0})
      "ts"
  """
  @spec extract_prefix(command()) :: binary()
  def extract_prefix(command) when is_tuple(command) do
    key =
      case command do
        {:put_batch, [{first_key, _value, _expire_at_ms} | _rest]} ->
          first_key

        {:delete_batch, [first_key | _rest]} ->
          first_key

        {:origin_checked, key, _inner, _before_value, _before_exp, _expected_value, _expire_at_ms} ->
          key

        {:origin_checked, key, _inner, _expected_value, _expire_at_ms} ->
          key

        _ ->
          elem(command, 1)
      end

    if is_binary(key) do
      key
      |> Ferricstore.Store.CompoundKey.extract_redis_key()
      |> extract_namespace_prefix()
    else
      "_root"
    end
  end

  defp extract_namespace_prefix("") do
    "_root"
  end

  defp extract_namespace_prefix(key) do
    case :binary.split(key, ":") do
      [^key] -> "_root"
      [prefix | _rest] -> prefix
    end
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl true
  def init(opts) do
    # Trap exits so terminate/2 is called on shutdown, allowing us to
    # reply to all pending callers instead of leaving them hung.
    Process.flag(:trap_exit, true)

    shard_id = Keyword.fetch!(opts, :shard_id)
    shard_index = Keyword.fetch!(opts, :shard_index)
    max_batch_size = Keyword.get(opts, :max_batch_size, @default_max_batch_size)
    max_batch_bytes = Keyword.get(opts, :max_batch_bytes, @default_max_batch_bytes)
    max_pending_batches = Keyword.get(opts, :max_pending_batches, @default_max_pending_batches)
    max_pending_bytes = Keyword.get(opts, :max_pending_bytes, @default_max_pending_bytes)

    state = %__MODULE__{
      shard_id: shard_id,
      shard_index: shard_index,
      max_batch_size: max_batch_size,
      max_batch_bytes: max_batch_bytes,
      max_pending_batches: max_pending_batches,
      max_pending_bytes: max_pending_bytes
    }

    # Kick off the periodic origin-pending sweep (TTL-drop lost entries).
    Process.send_after(self(), :sweep_origin_pending, @origin_pending_sweep_ms)

    {:ok, state}
  end

  @impl true
  def handle_call(:pause_writes_for_sync, from, state) do
    new_state =
      state
      |> Map.put(:writes_paused, true)
      |> flush_all_slots()

    if flush_drained?(new_state) do
      {:reply, :ok, new_state}
    else
      {:noreply, %{new_state | flush_waiters: [from | new_state.flush_waiters]}}
    end
  end

  def handle_call(:resume_writes_for_sync, _from, state) do
    {:reply, :ok, %{state | writes_paused: false}}
  end

  def handle_call({:write, command}, from, state) do
    enqueue_write(command, from, state)
  end

  def handle_call({:write_forwarded, command, origin_node}, from, state) do
    enqueue_write(command, remote_origin_from(origin_node, from), state)
  end

  def handle_call({:origin_submit_ordered, command}, from, state) do
    if pending_full?(state) do
      emit_origin_submit_overloaded(state)
      {:reply, {:error, :overloaded}, state}
    else
      enqueue_origin_submit_under_capacity(command, state, from)
    end
  end

  def handle_call({:origin_enqueue_ordered, command}, _from, state) do
    cond do
      pending_full?(state) ->
        emit_origin_submit_overloaded(state)
        {:reply, {:error, :overloaded}, state}

      not local_pipeline_target_alive?(state.shard_id) ->
        error = {:error, {:ra_target_down, state.shard_id}}
        emit_origin_submit_failed(state, 1, error)
        {:reply, error, state}

      true ->
        {:noreply, new_state} = enqueue_origin_submit_under_capacity(command, state)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:origin_submit_batch_ordered, commands}, from, state) do
    if pending_full?(state) do
      emit_origin_submit_overloaded(state)
      {:reply, {:error, :overloaded}, state}
    else
      {:noreply, submit_origin_replay(state, commands, [from])}
    end
  end

  def handle_call(:origin_accepting?, _from, state) do
    {:reply, not pending_full?(state), state}
  end

  def handle_call(:flush, from, state) do
    # Flush all pending slots (submits pipelined ra commands)
    new_state = flush_all_slots(state)

    # Reply only after in-flight commands have applied through Ra and the
    # local state machine has caught up to those Ra indexes.
    if flush_drained?(new_state) do
      {:reply, :ok, new_state}
    else
      {:noreply, %{new_state | flush_waiters: [from | new_state.flush_waiters]}}
    end
  end

  # Caller waits until the local state machine has applied at least `ra_index`.
  # If we've already passed that, reply immediately; otherwise queue the from
  # in `local_apply_waiters` and reply when `:locally_applied` catches up.
  def handle_call({:await_local_applied, ra_index}, from, state) do
    if state.last_local_applied >= ra_index do
      {:reply, :ok, state}
    else
      {:noreply, enqueue_local_apply_waiter(state, ra_index, :await_caller, [from], :ok)}
    end
  end

  # Test-only hooks. See the __-prefixed public functions at the end of
  # the module for the corresponding API.
  def handle_call({:__inject_origin_pending__, corr, batch, retry_count, mono}, _from, state) do
    entry = {[], :origin_no_reply, batch, retry_count, mono, 0}
    {:reply, :ok, put_pending(state, corr, entry)}
  end

  def handle_call({:__inject_quorum_pending__, corr, froms, kind, mono, bytes}, _from, state)
      when kind in [:single, :batch] do
    entry = {froms, kind, mono, bytes}
    {:reply, :ok, put_pending(state, corr, entry)}
  end

  def handle_call(:__latest_origin_corr__, _from, state) do
    latest =
      Enum.reduce(state.pending, {nil, 0}, fn
        {corr, {_froms, :origin_no_reply, _batch, _retry, mono, _bytes}}, {_best_corr, best_mono}
        when mono > best_mono ->
          {corr, mono}

        {corr, {_froms, :origin_no_reply, _batch, _retry, mono}}, {_best_corr, best_mono}
        when mono > best_mono ->
          {corr, mono}

        _, acc ->
          acc
      end)

    {:reply, elem(latest, 0), state}
  end

  def handle_call({:__has_pending__, corr}, _from, state) do
    {:reply, Map.has_key?(state.pending, corr), state}
  end

  def handle_call(:__pending_bytes__, _from, state) do
    {:reply, state.pending_bytes, state}
  end

  def handle_call(:__sweep_pending_now__, _from, state) do
    {:reply, :ok, sweep_origin_pending(state)}
  end

  # Test-only: force-clear all pending correlations and flush waiters.
  # Used by test cleanup to unstick a Batcher whose ra shard lost
  # correlations after a leader crash or disk error. Replies to blocked
  # callers (`:single`/`:batch` froms) with `{:error, :reset}` so they
  # don't hang forever.
  def handle_call(:__reset_pending__, _from, state) do
    Enum.each(state.pending, fn
      {_corr, {froms, :single, _mono, _bytes}} -> reply_all_froms(froms, {:error, :reset})
      {_corr, {froms, :batch, _mono, _bytes}} -> reply_all_froms(froms, {:error, :reset})
      {_corr, {froms, :single, _mono}} -> reply_all_froms(froms, {:error, :reset})
      {_corr, {froms, :batch, _mono}} -> reply_all_froms(froms, {:error, :reset})
      {_corr, {froms, :single}} -> reply_all_froms(froms, {:error, :reset})
      {_corr, {froms, :batch}} -> reply_all_froms(froms, {:error, :reset})
      _ -> :ok
    end)

    Enum.each(state.local_apply_waiters, fn waiter ->
      do_reply(waiter_kind(waiter), waiter_froms(waiter), {:error, :reset}, 0)
    end)

    Enum.each(state.flush_waiters, &GenServer.reply(&1, :ok))

    new_state =
      %{
        state
        | pending: %{},
          pending_bytes: 0,
          flush_waiters: [],
          local_apply_waiters: [],
          origin_local_apply_index: state.last_local_applied
      }
      |> emit_local_apply_waiters()

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:write, command, reply_to}, state) do
    enqueue_write(command, reply_to, state)
  end

  def handle_cast({:origin_submit, inner_command}, state) do
    enqueue_origin_submit(inner_command, state)
  end

  def handle_cast({:origin_submit_batch, commands}, state) do
    Enum.reduce(commands, {:noreply, state}, fn cmd, {:noreply, st} ->
      enqueue_origin_submit(cmd, st)
    end)
  end

  def handle_cast({:write_quorum, command, reply_to}, state) do
    enqueue_write_forced_quorum(command, reply_to, state)
  end

  def handle_cast({:cancel_await_local_applied, caller_pid}, state) do
    waiters =
      Enum.reject(state.local_apply_waiters, fn
        {_idx, :await_caller, [{pid, _tag}], _result} -> pid == caller_pid
        {_idx, :await_caller, [{pid, _tag}], _result, _mono} -> pid == caller_pid
        _waiter -> false
      end)

    new_state =
      %{state | local_apply_waiters: waiters}
      |> emit_local_apply_waiters()
      |> maybe_reply_flush_waiters()

    {:noreply, new_state}
  end

  def handle_cast({:write_batch, cmds, cmd_count, from}, state) do
    enqueue_write_batch(cmds, cmd_count, from, state)
  end

  def handle_cast({:write_put_batch, [], _cmd_count, from}, state) do
    reply_from(from, {:ok, []})
    {:noreply, state}
  end

  def handle_cast({:write_put_batch, entries, cmd_count, from}, state) do
    enqueue_write_batch([{:put_batch, entries}], cmd_count, from, state)
  end

  def handle_cast({:write_delete_batch, [], _cmd_count, from}, state) do
    reply_from(from, {:ok, []})
    {:noreply, state}
  end

  def handle_cast({:write_delete_batch, keys, cmd_count, from}, state) do
    enqueue_write_batch([{:delete_batch, keys}], cmd_count, from, state)
  end

  # Backwards compat: old callers without count
  def handle_cast({:write_batch, cmds, from}, state) do
    enqueue_write_batch(cmds, length(cmds), from, state)
  end

  @impl true
  def handle_info({:flush_slot, slot_key}, state) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      slot ->
        # Clear the timer ref since the timer has already fired
        updated_slot = %{slot | timer_ref: nil}
        state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        case do_flush_slot(state, slot_key) do
          {:noreply, new_state} -> {:noreply, new_state}
        end
    end
  end

  # Handle ra_event notifications from pipeline_command.
  # Applied commands: {ra_event, Leader, {applied, [{correlation, result}]}}
  #
  # The leader sends `:applied` after IT has applied the entry. The originating
  # node's local state machine may not have applied yet (especially when the
  # originating node is a follower). For *quorum* writes we hold the reply
  # until `last_local_applied >= ra_index` so reads-after-write on the same
  # node are consistent. The state machine wraps every result as
  # `{:applied_at, ra_index, real_result}` to give us the index here.
  def handle_info({:ra_event, _leader, {:applied, applied_list}}, state) do
    now = System.monotonic_time()

    new_state =
      Enum.reduce(applied_list, state, fn {corr, raw_result}, acc ->
        {ra_index, result} = unwrap_applied(raw_result)

        case pop_pending(acc, corr) do
          {nil, acc_after_pop} ->
            acc_after_pop

          # Async entries: no callers to reply to. Just track and clear.
          {{_froms, :origin_no_reply}, acc_after_pop} ->
            track_origin_local_apply(acc_after_pop, ra_index)

          {{_froms, :origin_no_reply, _batch, _retry, _mono}, acc_after_pop} ->
            track_origin_local_apply(acc_after_pop, ra_index)

          {{_froms, :origin_no_reply, _batch, _retry, _mono, _bytes}, acc_after_pop} ->
            track_origin_local_apply(acc_after_pop, ra_index)

          {{froms, :single, mono}, acc_after_pop} ->
            emit_quorum_applied_telemetry(
              acc_after_pop,
              mono,
              now,
              :single,
              froms,
              ra_index,
              result
            )

            gate_reply(acc_after_pop, ra_index, :single, froms, result)

          {{froms, :batch, mono}, acc_after_pop} ->
            emit_quorum_applied_telemetry(
              acc_after_pop,
              mono,
              now,
              :batch,
              froms,
              ra_index,
              result
            )

            gate_reply(acc_after_pop, ra_index, :batch, froms, result)

          {{froms, :single, mono, _bytes}, acc_after_pop} ->
            emit_quorum_applied_telemetry(
              acc_after_pop,
              mono,
              now,
              :single,
              froms,
              ra_index,
              result
            )

            gate_reply(acc_after_pop, ra_index, :single, froms, result)

          {{froms, :batch, mono, _bytes}, acc_after_pop} ->
            emit_quorum_applied_telemetry(
              acc_after_pop,
              mono,
              now,
              :batch,
              froms,
              ra_index,
              result
            )

            gate_reply(acc_after_pop, ra_index, :batch, froms, result)
        end
      end)

    new_state = maybe_reply_flush_waiters(new_state)
    {:noreply, new_state}
  end

  # Local state machine applied an entry at `ra_index`. Bump our high-water
  # mark and drain any waiters whose entries are now safe to reply.
  def handle_info({:locally_applied, ra_index}, state) do
    new_state =
      if ra_index > state.last_local_applied do
        %{state | last_local_applied: ra_index}
        |> drain_local_apply_waiters()
        |> maybe_reply_flush_waiters()
      else
        state
      end

    {:noreply, new_state}
  end

  # Handle rejected commands (not_leader). For origin replay entries we re-submit
  # to the hinted leader up to @max_origin_retries times before dropping.
  # For quorum entries we reply :error to the blocked caller so the
  # application can retry itself.
  def handle_info({:ra_event, _from_id, {:rejected, {not_leader, maybe_leader, corr}}}, state) do
    case pop_pending(state, corr) do
      {nil, state_after_pop} ->
        {:noreply, state_after_pop}

      # Retry-aware origin replay entry. Has the original batch + retry_count.
      {{_froms, :origin_no_reply, batch, retry_count, _mono}, state_without} ->
        handle_rejected_origin(state_without, state, batch, retry_count, not_leader, maybe_leader)

      {{_froms, :origin_no_reply, batch, retry_count, _mono, _bytes}, state_without} ->
        handle_rejected_origin(state_without, state, batch, retry_count, not_leader, maybe_leader)

      # Legacy origin shape without retry info — drop silently.
      {{_froms, :origin_no_reply}, state_without} ->
        {:noreply, maybe_reply_flush_waiters(state_without)}

      # Quorum entry — local server isn't leader. Reply :not_leader so
      # Router.forward_to_leader takes over (and barriers on local apply
      # via await_local_applied/2 after the leader replies, fixing
      # read-your-write across the redirect).
      {pending_entry, state_without}
      when is_tuple(pending_entry) and elem(pending_entry, 1) in [:single, :batch] ->
        froms = elem(pending_entry, 0)

        target =
          case {not_leader, maybe_leader} do
            {:not_leader, leader} when leader != :undefined and leader != nil -> leader
            _ -> state_without.shard_id
          end

        reply_all_froms(froms, {:error, {:not_leader, target}})

        {:noreply, maybe_reply_flush_waiters(state_without)}
    end
  end

  # Periodic sweep of pending entries whose :applied or :rejected never
  # arrived (bounds memory against lost ra_events / pathological cluster
  # states). Only affects the retry-aware origin replay entries; everything else
  # is either still in flight or will be resolved when the ra_event arrives.
  def handle_info(:sweep_origin_pending, state) do
    new_state = sweep_origin_pending(state)
    Process.send_after(self(), :sweep_origin_pending, @origin_pending_sweep_ms)
    {:noreply, new_state}
  end

  # Handle legacy :flush messages (e.g. from cancel_timer race conditions)
  def handle_info(:flush, state) do
    {:noreply, state}
  end

  # Invalidate the namespace config cache when config changes.
  # Sent by NamespaceConfig after any set/reset operation.
  def handle_info(:ns_config_changed, state) do
    # Flush all open slots immediately so queued commands with the old
    # window_ms are processed. Next commands create fresh slots with
    # the new config values.
    new_state = flush_all_slots(state)
    {:noreply, %{new_state | ns_cache: %{}}}
  end

  # Catch-all for unexpected messages (e.g. stale Task results, DOWN messages
  # from previous implementation). Silently discard.
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp handle_rejected_origin(
         state_without,
         original_state,
         batch,
         retry_count,
         not_leader,
         maybe_leader
       ) do
    target =
      case {not_leader, maybe_leader} do
        {:not_leader, leader} when leader != :undefined and leader != nil -> leader
        _ -> state_without.shard_id
      end

    new_state =
      if retry_count < @max_origin_retries do
        :telemetry.execute(
          [:ferricstore, :batcher, :origin_retry],
          %{retry_count: retry_count + 1, batch_size: length(batch)},
          %{shard_index: original_state.shard_index, target: inspect(target)}
        )

        submit_origin_with_retry(state_without, batch, target, retry_count + 1)
      else
        :telemetry.execute(
          [:ferricstore, :batcher, :origin_dropped],
          %{batch_size: length(batch)},
          %{shard_index: original_state.shard_index, reason: :max_retries}
        )

        Logger.warning(
          "Batcher shard=#{original_state.shard_index}: origin replay batch of #{length(batch)} commands dropped after #{@max_origin_retries} retries"
        )

        state_without
      end

    {:noreply, maybe_reply_flush_waiters(new_state)}
  end

  # If the new-shape wrapped reply is present, unwrap; otherwise pass through
  # for backward compatibility with WAL entries that pre-date this change.
  defp unwrap_applied({:applied_at, ra_index, real_result}), do: {ra_index, real_result}
  defp unwrap_applied(other), do: {0, other}

  # Reply now if:
  #   * the legacy result has no Ra index,
  #   * the local state machine has already applied this index, or
  #   * every caller originated on this node.
  #
  # The local-caller fast path is critical for SET pipeline throughput. The
  # `:ra_event {:applied, ...}` for this batcher is emitted by the local Ra
  # server, so local callers do not need a second mailbox round trip through
  # `{:locally_applied, ra_index}`. Forwarded callers are different: the
  # leader's batcher may reply to another node, so those keep the explicit
  # local-apply barrier via `:remote_origin`.
  defp gate_reply(state, 0, kind, froms, result) do
    do_reply(kind, froms, result, 0)
    state
  end

  defp gate_reply(%{last_local_applied: lla} = state, ra_index, kind, froms, result)
       when lla >= ra_index do
    do_reply(kind, froms, result, ra_index)
    state
  end

  defp gate_reply(state, ra_index, kind, froms, result) do
    if all_local_reply?(froms) do
      do_reply(kind, froms, result, ra_index)
      state
    else
      enqueue_local_apply_waiter(state, ra_index, kind, froms, result)
    end
  end

  defp all_local_reply?(froms) do
    Enum.all?(froms, &local_reply_target?/1)
  end

  defp local_reply_target?({:batch_from, from, _count}), do: local_reply_target?(from)
  defp local_reply_target?({:remote_origin, _origin_node, _from}), do: false

  defp local_reply_target?({pid, _ref}) when is_pid(pid), do: node(pid) == node()

  # Router batch waits use process aliases through ReplyAwaiter. An alias is
  # only valid in this BEAM process, so it is a local caller and can use the
  # immediate local Raft-applied reply path.
  defp local_reply_target?({alias_ref, {Ferricstore.Raft.ReplyAwaiter, _tag}})
       when is_reference(alias_ref),
       do: true

  defp local_reply_target?(_from), do: false

  defp track_origin_local_apply(state, ra_index) when is_integer(ra_index) and ra_index > 0 do
    %{state | origin_local_apply_index: max(state.origin_local_apply_index, ra_index)}
  end

  defp track_origin_local_apply(state, _ra_index), do: state

  # When the caller is on a different node, it was forwarded by
  # Router.forward_to_leader. Send the result wrapped with the ra_index so
  # the originator can barrier on its own local apply before returning to
  # the user (read-your-write across a leader redirect).
  defp do_reply(:single, [from], result, ra_index) do
    case from do
      {:batch_from, inner_from, _count} ->
        reply_from(inner_from, {:ok, [maybe_wrap_remote(inner_from, result, ra_index)]})

      _ ->
        reply_from(from, maybe_wrap_remote(from, result, ra_index))
    end
  end

  defp do_reply(:batch, froms, result, ra_index) do
    # Each `from` gets its slice of the batch results. Forwarded batch callers
    # get each result tagged with the Ra index so the origin node can barrier
    # on local apply before returning to the client.
    reply_batch(froms, result, ra_index)
  end

  defp do_reply(:await_caller, [from], result, _ra_index) do
    GenServer.reply(from, result)
  end

  defp maybe_wrap_remote({pid, _ref}, result, ra_index)
       when node(pid) != node() and ra_index > 0 do
    {:remote_applied_at, ra_index, result}
  end

  defp maybe_wrap_remote({:remote_origin, origin_node, _from}, result, ra_index)
       when origin_node != node() and ra_index > 0 do
    {:remote_applied_at, ra_index, result}
  end

  defp maybe_wrap_remote(_from, result, _ra_index), do: result

  defp drain_local_apply_waiters(%{last_local_applied: lla} = state) do
    now = System.monotonic_time()

    {ready, still_waiting} =
      Enum.split_with(state.local_apply_waiters, fn waiter -> waiter_index(waiter) <= lla end)

    Enum.each(ready, fn waiter ->
      emit_local_apply_gate_telemetry(state, waiter, now)

      do_reply(
        waiter_kind(waiter),
        waiter_froms(waiter),
        waiter_result(waiter),
        waiter_index(waiter)
      )
    end)

    new_state = %{state | local_apply_waiters: still_waiting}

    if ready == [] do
      new_state
    else
      emit_local_apply_waiters(new_state)
    end
  end

  defp enqueue_local_apply_waiter(state, ra_index, kind, froms, result) do
    waiter = {ra_index, kind, froms, result, System.monotonic_time()}

    %{state | local_apply_waiters: [waiter | state.local_apply_waiters]}
    |> emit_local_apply_waiters()
  end

  defp emit_local_apply_waiters(state, now \\ System.monotonic_time()) do
    waiters = state.local_apply_waiters

    :telemetry.execute(
      [:ferricstore, :batcher, :local_apply_waiters],
      %{depth: length(waiters), oldest_age_ms: oldest_local_apply_waiter_age_ms(waiters, now)},
      %{shard_index: state.shard_index}
    )

    state
  end

  defp emit_quorum_applied_telemetry(state, submitted_mono, now, kind, froms, ra_index, result) do
    :telemetry.execute(
      [:ferricstore, :batcher, :quorum_applied],
      %{
        duration_us:
          (now - submitted_mono)
          |> max(0)
          |> System.convert_time_unit(:native, :microsecond),
        caller_count: length(froms)
      },
      %{
        shard_index: state.shard_index,
        kind: kind,
        result: quorum_result_class(result),
        ra_index: ra_index
      }
    )
  end

  defp emit_local_apply_gate_telemetry(state, waiter, now) do
    :telemetry.execute(
      [:ferricstore, :batcher, :local_apply_gate],
      %{
        duration_us:
          (now - waiter_enqueued_at(waiter))
          |> max(0)
          |> System.convert_time_unit(:native, :microsecond),
        caller_count: length(waiter_froms(waiter))
      },
      %{
        shard_index: state.shard_index,
        kind: waiter_kind(waiter),
        ra_index: waiter_index(waiter)
      }
    )
  end

  defp quorum_result_class({:error, _}), do: :error
  defp quorum_result_class(_), do: :ok

  defp oldest_local_apply_waiter_age_ms([], _now), do: 0

  defp oldest_local_apply_waiter_age_ms(waiters, now) do
    oldest =
      Enum.reduce(waiters, now, fn waiter, acc ->
        min(waiter_enqueued_at(waiter), acc)
      end)

    (now - oldest)
    |> max(0)
    |> System.convert_time_unit(:native, :millisecond)
  end

  defp waiter_index({idx, _kind, _froms, _result}), do: idx
  defp waiter_index({idx, _kind, _froms, _result, _mono}), do: idx

  defp waiter_kind({_idx, kind, _froms, _result}), do: kind
  defp waiter_kind({_idx, kind, _froms, _result, _mono}), do: kind

  defp waiter_froms({_idx, _kind, froms, _result}), do: froms
  defp waiter_froms({_idx, _kind, froms, _result, _mono}), do: froms

  defp waiter_result({_idx, _kind, _froms, result}), do: result
  defp waiter_result({_idx, _kind, _froms, result, _mono}), do: result

  defp waiter_enqueued_at({_idx, _kind, _froms, _result}), do: System.monotonic_time()
  defp waiter_enqueued_at({_idx, _kind, _froms, _result, mono}), do: mono

  @impl true
  def terminate(_reason, state) do
    # Reply to all callers waiting in unflushed slots.
    Enum.each(state.slots, fn {_slot_key, slot} ->
      Enum.each(slot.froms, fn
        {:batch_from, from, _count} -> safe_reply(from, {:error, :batcher_terminated})
        from -> safe_reply(from, {:error, :batcher_terminated})
      end)
    end)

    # Reply to all callers waiting for in-flight Raft commands. Async
    # entries (retry-aware 5-tuple or legacy 3-tuple) have no froms to
    # reply to — Router already got :ok.
    Enum.each(state.pending, fn
      {_corr, {_froms, :origin_no_reply, _batch, _retry, _mono, _bytes}} ->
        :ok

      {_corr, {_froms, :origin_no_reply, _batch, _retry, _mono}} ->
        :ok

      {_corr, {froms, _kind, _mono, _bytes}} ->
        Enum.each(froms, fn from ->
          safe_reply(from, {:error, :batcher_terminated})
        end)

      {_corr, {froms, _kind, _mono}} ->
        Enum.each(froms, fn from ->
          safe_reply(from, {:error, :batcher_terminated})
        end)
    end)

    # Reply to callers whose Ra entry committed but whose local state machine
    # had not caught up before shutdown.
    Enum.each(state.local_apply_waiters, fn waiter ->
      Enum.each(waiter_froms(waiter), fn from ->
        safe_reply(from, {:error, :batcher_terminated})
      end)
    end)

    # Reply to flush barrier waiters.
    Enum.each(state.flush_waiters, fn from ->
      safe_reply(from, {:error, :batcher_terminated})
    end)

    :ok
  end

  defp reply_batch(froms, {:ok, results}, ra_index) when is_list(results) do
    expected =
      Enum.reduce(froms, 0, fn
        {:batch_from, _, count}, acc -> acc + count
        _, acc -> acc + 1
      end)

    if length(results) == expected do
      dispatch_batch_results(froms, results, ra_index)
    else
      Logger.error(
        "Batcher: batch result count mismatch — " <>
          "expected #{expected} results but got #{length(results)}"
      )

      reply_batch_error(froms, {:error, :batch_result_mismatch})
    end
  end

  defp reply_batch(froms, other, _ra_index) do
    reply_batch_error(froms, other)
  end

  defp dispatch_batch_results([], [], _ra_index), do: :ok

  defp dispatch_batch_results([{:batch_from, from, count} | rest_froms], results, ra_index) do
    {slice, rest_results} = Enum.split(results, count)
    reply_from(from, {:ok, wrap_batch_results(from, slice, ra_index)})
    dispatch_batch_results(rest_froms, rest_results, ra_index)
  end

  defp dispatch_batch_results([from | rest_froms], [result | rest_results], ra_index) do
    reply_from(from, maybe_wrap_remote(from, result, ra_index))
    dispatch_batch_results(rest_froms, rest_results, ra_index)
  end

  defp wrap_batch_results(from, results, ra_index) do
    Enum.map(results, &maybe_wrap_remote(from, &1, ra_index))
  end

  defp reply_batch_error(froms, error) do
    Enum.each(froms, fn
      {:batch_from, from, _count} -> reply_from(from, error)
      from -> reply_from(from, error)
    end)
  end

  defp safe_reply({:batch_from, from, _count}, msg), do: safe_reply(from, msg)
  defp safe_reply({:remote_origin, _origin_node, from}, msg), do: safe_reply(from, msg)

  defp safe_reply(from, msg) do
    try do
      reply_from(from, msg)
    catch
      _, _ -> :ok
    end
  end

  defp reply_all_froms(froms, msg) do
    Enum.each(froms, fn
      {:batch_from, from, _count} -> reply_from(from, msg)
      from -> reply_from(from, msg)
    end)
  end

  defp reply_from({:remote_origin, _origin_node, from}, msg), do: GenServer.reply(from, msg)
  defp reply_from(from, msg), do: GenServer.reply(from, msg)

  # ---------------------------------------------------------------------------
  # Private: write enqueue (shared by handle_call and handle_cast)
  # ---------------------------------------------------------------------------

  # Accumulates write_batch commands into the quorum slot, flushing when
  # count >= max_batch_size or on next message loop (timer 0). Rejects
  # new batches when too many ra commands are already in flight.
  defp enqueue_write_batch([], _cmd_count, from, state) do
    reply_from(from, {:ok, []})
    {:noreply, state}
  end

  defp enqueue_write_batch(_cmds, _cmd_count, from, %{writes_paused: true} = state) do
    reply_from(from, sync_pause_error())
    {:noreply, state}
  end

  defp enqueue_write_batch(cmds, cmd_count, from, state) do
    if pending_full?(state) do
      reply_from(from, {:error, :overloaded})
      {:noreply, state}
    else
      prefix = extract_prefix(hd(cmds))
      slot_key = {prefix, :quorum}
      cmd_bytes = commands_estimated_bytes(cmds)

      case slot_for_append(state, slot_key, 0, cmd_bytes) do
        {:ok, state, slot} ->
          updated_slot = %{
            slot
            | cmds: prepend_reversed(cmds, slot.cmds),
              froms: [{:batch_from, from, cmd_count} | slot.froms],
              count: slot.count + cmd_count,
              bytes: slot.bytes + cmd_bytes
          }

          state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

          if slot_flush_due?(state, updated_slot) do
            case do_flush_slot(state, slot_key) do
              {:noreply, new_state} -> {:noreply, new_state}
            end
          else
            if updated_slot.timer_ref == nil do
              ref = Process.send_after(self(), {:flush_slot, slot_key}, 0)
              updated_slot = %{updated_slot | timer_ref: ref}
              {:noreply, %{state | slots: Map.put(state.slots, slot_key, updated_slot)}}
            else
              {:noreply, state}
            end
          end

        {:overloaded, state} ->
          reply_from(from, {:error, :overloaded})
          {:noreply, state}
      end
    end
  end

  # Prepend items in reverse order onto acc, equivalent to Enum.reverse(list) ++ acc
  # but avoids allocating the intermediate reversed list.
  defp prepend_reversed([], acc), do: acc
  defp prepend_reversed([h | t], acc), do: prepend_reversed(t, [h | acc])

  # Enqueues a write command into the appropriate namespace slot.
  # Returns `{:noreply, state}` -- the caller is replied to later when the
  # batch is flushed and committed.
  @spec enqueue_write(command(), GenServer.from(), %__MODULE__{}) :: {:noreply, %__MODULE__{}}
  defp enqueue_write(_command, from, %{writes_paused: true} = state) do
    reply_from(from, sync_pause_error())
    {:noreply, state}
  end

  defp enqueue_write(command, from, state) do
    if pending_full?(state) do
      reply_from(from, {:error, :overloaded})
      {:noreply, state}
    else
      enqueue_write_under_capacity(command, from, state)
    end
  end

  defp enqueue_write_under_capacity(command, from, state) do
    prefix = extract_prefix(command)
    {window_ms, state} = lookup_ns_config(prefix, state)
    slot_key = {prefix, :quorum}
    cmd_bytes = command_estimated_bytes(command)

    case slot_for_append(state, slot_key, window_ms, cmd_bytes) do
      {:ok, state, slot} ->
        updated_slot = %{
          slot
          | cmds: [command | slot.cmds],
            froms: [from | slot.froms],
            window_ms: window_ms,
            count: slot.count + 1,
            bytes: slot.bytes + cmd_bytes
        }

        updated_slot =
          if updated_slot.timer_ref == nil do
            ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
            %{updated_slot | timer_ref: ref}
          else
            updated_slot
          end

        new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        if slot_flush_due?(state, updated_slot) do
          do_flush_slot(new_state, slot_key)
        else
          {:noreply, new_state}
        end

      {:overloaded, state} ->
        reply_from(from, {:error, :overloaded})
        {:noreply, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: flush logic
  # ---------------------------------------------------------------------------

  @spec flush_all_slots(%__MODULE__{}) :: %__MODULE__{}
  defp flush_all_slots(state) do
    Enum.reduce(Map.keys(state.slots), state, fn slot_key, acc ->
      case do_flush_slot(acc, slot_key) do
        {:noreply, new_state} -> new_state
      end
    end)
  end

  @spec do_flush_slot(%__MODULE__{}, slot_key()) :: {:noreply, %__MODULE__{}}
  defp do_flush_slot(state, slot_key) do
    case Map.get(state.slots, slot_key) do
      nil ->
        {:noreply, state}

      %{cmds: []} = slot ->
        cancel_timer(slot.timer_ref)
        new_slots = Map.delete(state.slots, slot_key)
        {:noreply, %{state | slots: new_slots}}

      slot ->
        cancel_timer(slot.timer_ref)

        batch = Enum.reverse(slot.cmds)
        froms = Enum.reverse(slot.froms)

        {prefix, write_path} = slot_key
        emit_slot_flush_telemetry(state, slot, prefix, write_path, batch, froms)

        new_state =
          case write_path do
            :origin_replay ->
              # Explicit internal local-origin helper. Wrap each command as
              # {:async, origin, inner} so state machine can origin-skip
              # effects that were already applied locally.
              submit_origin_replay(state, batch, froms)

            :quorum ->
              pipeline_submit(state, batch, froms)
          end

        # Remove the slot entirely once flushed (clean up empty slots)
        new_slots = Map.delete(new_state.slots, slot_key)
        {:noreply, %{new_state | slots: new_slots}}
    end
  end

  # Submit a batch via ra:pipeline_command/4 with a correlation ref.
  # For single commands, submit directly (no batch wrapper).
  # For multiple commands, wrap in {:batch, commands}.
  # Returns updated state with the correlation tracked in `pending`.
  @spec pipeline_submit(%__MODULE__{}, [command()], [GenServer.from()]) :: %__MODULE__{}
  defp pipeline_submit(state, [{:put_batch, _entries} = command], froms) do
    pipeline_submit_batch_command(state, command, froms, batch_from_count(froms), :put_batch)
  end

  defp pipeline_submit(state, [{:delete_batch, _keys} = command], froms) do
    pipeline_submit_batch_command(state, command, froms, batch_from_count(froms), :delete_batch)
  end

  defp pipeline_submit(state, [single_cmd], froms) do
    corr = make_ref()
    started_at = System.monotonic_time()

    case prepare_quorum_command(state, single_cmd) do
      {:ok, prepared_cmd} ->
        {:ttb, bin} = serialized = CommandClock.to_ttb(prepared_cmd)
        priority = raft_pipeline_priority()
        command_bytes = byte_size(bin)

        submit_result =
          if pending_bytes_would_exceed?(state, command_bytes) do
            {:error, :overloaded}
          else
            pipeline_command(state.shard_id, serialized, corr, priority)
          end

        emit_quorum_submit_telemetry(
          state,
          started_at,
          :single,
          1,
          length(froms),
          command_bytes,
          command_shape(prepared_cmd),
          priority,
          submit_result
        )

        track_or_reject_quorum_submit(state, corr, froms, :single, submit_result, command_bytes)

      {:error, reason} ->
        reject_prepared_quorum_command(state, started_at, froms, :single, 1, reason)
    end
  end

  defp pipeline_submit(state, batch, froms) do
    corr = make_ref()
    started_at = System.monotonic_time()
    {command, logical_count} = compact_hot_batch(batch)

    case prepare_quorum_command(state, command) do
      {:ok, prepared_command} ->
        {:ttb, bin} = serialized = CommandClock.to_ttb(prepared_command)
        priority = raft_pipeline_priority()
        command_bytes = byte_size(bin)

        submit_result =
          if pending_bytes_would_exceed?(state, command_bytes) do
            {:error, :overloaded}
          else
            pipeline_command(state.shard_id, serialized, corr, priority)
          end

        emit_quorum_submit_telemetry(
          state,
          started_at,
          :batch,
          logical_count,
          length(froms),
          command_bytes,
          command_shape(prepared_command),
          priority,
          submit_result
        )

        track_or_reject_quorum_submit(state, corr, froms, :batch, submit_result, command_bytes)

      {:error, reason} ->
        reject_prepared_quorum_command(state, started_at, froms, :batch, logical_count, reason)
    end
  end

  defp pipeline_submit_batch_command(state, command, froms, logical_count, command_shape) do
    corr = make_ref()
    started_at = System.monotonic_time()

    case prepare_quorum_command(state, command) do
      {:ok, prepared_command} ->
        {:ttb, bin} = serialized = CommandClock.to_ttb(prepared_command)
        priority = raft_pipeline_priority()
        command_bytes = byte_size(bin)

        submit_result =
          if pending_bytes_would_exceed?(state, command_bytes) do
            {:error, :overloaded}
          else
            pipeline_command(state.shard_id, serialized, corr, priority)
          end

        emit_quorum_submit_telemetry(
          state,
          started_at,
          :batch,
          logical_count,
          length(froms),
          command_bytes,
          prepared_command_shape(command_shape, prepared_command),
          priority,
          submit_result
        )

        track_or_reject_quorum_submit(state, corr, froms, :batch, submit_result, command_bytes)

      {:error, reason} ->
        reject_prepared_quorum_command(state, started_at, froms, :batch, logical_count, reason)
    end
  end

  defp prepare_quorum_command(state, command) do
    case default_instance_ctx() do
      nil ->
        {:ok, command}

      ctx ->
        if BlobCommand.side_channel_candidate?(ctx, command) do
          BlobCommand.prepare(ctx, state.shard_index, command,
            single_member?: single_member_raft_group?(state.shard_index)
          )
        else
          {:ok, command}
        end
    end
  end

  defp default_instance_ctx do
    FerricStore.Instance.get(:default)
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  defp single_member_raft_group?(shard_index) do
    case Ferricstore.Raft.Cluster.members(shard_index) do
      {:ok, members, _leader} when is_list(members) -> length(members) == 1
      _other -> false
    end
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp prepared_command_shape(original_shape, prepared_command) do
    prepared_shape = command_shape(prepared_command)

    if prepared_shape == original_shape do
      original_shape
    else
      prepared_shape
    end
  end

  defp reject_prepared_quorum_command(state, started_at, froms, kind, logical_count, reason) do
    priority = raft_pipeline_priority()
    submit_result = {:error, reason}

    emit_quorum_submit_telemetry(
      state,
      started_at,
      kind,
      logical_count,
      length(froms),
      0,
      :blob_prepare_failed,
      priority,
      submit_result
    )

    reply_all_froms(froms, {:error, reason})

    Logger.warning(
      "Batcher shard=#{state.shard_index}: blob side-channel command preparation failed: #{inspect(reason)}"
    )

    maybe_reply_flush_waiters(state)
  end

  # Flush path for commands accumulated via Batcher.origin_submit. The caller
  # has already performed the local side effect before calling origin_submit.
  # Commands are wrapped as `{:async, origin, inner}` because that is the
  # state-machine replay tag; it is not a client-visible durability mode.
  # Ordered callers are replied after pipeline submission; fire-and-forget
  # callers have no reply.
  #
  # Tracks the correlation in `pending` with `:origin_no_reply` so that
  # Batcher.flush waiters can observe when all in-flight origin replay commands have
  # applied to the state machine. The stored tuple carries the already-
  # wrapped batch + retry_count + submission timestamp so that the
  # :rejected handler can re-submit to a hinted leader and the periodic
  # sweep can drop stalled entries.
  defp submit_origin_replay(state, batch, submit_froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :origin_flush],
      %{batch_size: length(batch)},
      %{shard_index: state.shard_index, origin: true}
    )

    # Wrap each command with the originating node so the state machine on
    # every peer can decide skip-vs-apply by *node identity*, not by ETS
    # presence. The presence-based heuristic was wrong for repeated RMW on
    # the same key — the second `incr` arrives at a follower whose ETS now
    # contains the first incr's result, and the heuristic mis-classifies the
    # follower as the origin and skips. Origin tagging is deterministic.
    origin = node()
    wrapped = Enum.map(batch, fn cmd -> {:async, origin, cmd} end)
    submit_origin_with_retry(state, wrapped, state.shard_id, 0, submit_froms)
  end

  # Shared helper for initial origin replay submission and retries after :rejected.
  # `wrapped_batch` is the already-prepared payload (with or without the
  # {:async, ...} replay wrapper — caller's choice, not interpreted here). We
  # serialize once and hand to Ra via pipeline_command on `target`, then
  # track the correlation so :rejected can re-submit and :applied can clean
  # up. `retry_count` is the number of retries already attempted; starts
  # at 0 for the first submission.
  defp submit_origin_with_retry(state, wrapped_batch, target, retry_count, submit_froms \\ []) do
    corr = make_ref()
    {:ttb, bin} = serialized = CommandClock.to_ttb({:batch, wrapped_batch})
    command_bytes = byte_size(bin)

    submit_result =
      if pending_bytes_would_exceed?(state, command_bytes) do
        {:error, :overloaded}
      else
        pipeline_command(target, serialized, corr, raft_pipeline_priority())
      end

    if pipeline_submit_status(submit_result) == :ok do
      reply_all_froms(submit_froms, :ok)

      entry =
        {[], :origin_no_reply, wrapped_batch, retry_count, System.monotonic_time(), command_bytes}

      put_pending(state, corr, entry)
    else
      reply_all_froms(submit_froms, normalize_pipeline_error(submit_result))

      :telemetry.execute(
        [:ferricstore, :batcher, :origin_dropped],
        %{batch_size: length(wrapped_batch)},
        %{shard_index: state.shard_index, reason: submit_result}
      )

      Logger.warning(
        "Batcher shard=#{state.shard_index}: origin replay batch of #{length(wrapped_batch)} commands not submitted to Raft: #{inspect(submit_result)}"
      )

      maybe_reply_flush_waiters(state)
    end
  end

  defp track_or_reject_quorum_submit(state, corr, froms, kind, submit_result, command_bytes) do
    if pipeline_submit_status(submit_result) == :ok do
      mono = System.monotonic_time()
      put_pending(state, corr, {froms, kind, mono, command_bytes})
    else
      reply_all_froms(froms, normalize_pipeline_error(submit_result))

      Logger.warning(
        "Batcher shard=#{state.shard_index}: #{kind} command not submitted to Raft: #{inspect(submit_result)}"
      )

      maybe_reply_flush_waiters(state)
    end
  end

  defp pipeline_command(target, serialized, corr, priority) do
    if local_pipeline_target_alive?(target) do
      :ra.pipeline_command(target, serialized, corr, priority)
    else
      {:error, {:ra_target_down, target}}
    end
  end

  defp local_pipeline_target_alive?({name, target_node})
       when target_node == node() and is_atom(name) do
    Process.whereis(name) != nil
  end

  defp local_pipeline_target_alive?(name) when is_atom(name) do
    Process.whereis(name) != nil
  end

  defp local_pipeline_target_alive?(_target), do: true

  defp normalize_pipeline_error({:error, _reason} = error), do: error
  defp normalize_pipeline_error(other), do: {:error, other}

  @doc false
  @spec compact_hot_batch([command()]) :: {command(), non_neg_integer()}
  def compact_hot_batch(commands) when is_list(commands) do
    if PerfToggles.compact_hot_batches?() do
      compact_hot_batch(commands, commands, :unknown, [], 0)
    else
      generic_hot_batch(commands)
    end
  end

  defp compact_hot_batch(_original, [], :put, entries, count),
    do: {{:put_batch, Enum.reverse(entries)}, count}

  defp compact_hot_batch(_original, [], :put_batch, entries, count),
    do: {{:put_batch, Enum.reverse(entries)}, count}

  defp compact_hot_batch(_original, [], :delete, keys, count),
    do: {{:delete_batch, Enum.reverse(keys)}, count}

  defp compact_hot_batch(_original, [], :delete_batch, keys, count),
    do: {{:delete_batch, Enum.reverse(keys)}, count}

  defp compact_hot_batch(original, [{:put, key, value, expire_at_ms} | rest], :unknown, [], 0),
    do: compact_hot_batch(original, rest, :put, [{key, value, expire_at_ms}], 1)

  defp compact_hot_batch(original, [{:put, key, value, expire_at_ms} | rest], :put, acc, count),
    do: compact_hot_batch(original, rest, :put, [{key, value, expire_at_ms} | acc], count + 1)

  defp compact_hot_batch(
         original,
         [{:put, key, value, expire_at_ms} | rest],
         :put_batch,
         acc,
         count
       ),
       do:
         compact_hot_batch(
           original,
           rest,
           :put_batch,
           [{key, value, expire_at_ms} | acc],
           count + 1
         )

  defp compact_hot_batch(original, [{:put_batch, entries} | rest], :unknown, [], 0),
    do:
      compact_hot_batch(
        original,
        rest,
        :put_batch,
        prepend_reversed(entries, []),
        length(entries)
      )

  defp compact_hot_batch(original, [{:put_batch, entries} | rest], :put_batch, acc, count),
    do:
      compact_hot_batch(
        original,
        rest,
        :put_batch,
        prepend_reversed(entries, acc),
        count + length(entries)
      )

  defp compact_hot_batch(original, [{:put_batch, entries} | rest], :put, acc, count),
    do:
      compact_hot_batch(
        original,
        rest,
        :put,
        prepend_reversed(entries, acc),
        count + length(entries)
      )

  defp compact_hot_batch(original, [{:delete, key} | rest], :unknown, [], 0),
    do: compact_hot_batch(original, rest, :delete, [key], 1)

  defp compact_hot_batch(original, [{:delete, key} | rest], :delete, acc, count),
    do: compact_hot_batch(original, rest, :delete, [key | acc], count + 1)

  defp compact_hot_batch(original, [{:delete, key} | rest], :delete_batch, acc, count),
    do: compact_hot_batch(original, rest, :delete_batch, [key | acc], count + 1)

  defp compact_hot_batch(original, [{:delete_batch, keys} | rest], :unknown, [], 0),
    do: compact_hot_batch(original, rest, :delete_batch, prepend_reversed(keys, []), length(keys))

  defp compact_hot_batch(original, [{:delete_batch, keys} | rest], :delete_batch, acc, count),
    do:
      compact_hot_batch(
        original,
        rest,
        :delete_batch,
        prepend_reversed(keys, acc),
        count + length(keys)
      )

  defp compact_hot_batch(original, [{:delete_batch, keys} | rest], :delete, acc, count),
    do:
      compact_hot_batch(
        original,
        rest,
        :delete,
        prepend_reversed(keys, acc),
        count + length(keys)
      )

  defp compact_hot_batch(_original, [], {:compound_put, redis_key}, entries, count),
    do: {{:compound_batch_put, redis_key, Enum.reverse(entries)}, count}

  defp compact_hot_batch(_original, [], {:compound_delete, redis_key}, keys, count),
    do: {{:compound_batch_delete, redis_key, Enum.reverse(keys)}, count}

  defp compact_hot_batch(
         original,
         [{:compound_put, compound_key, value, expire_at_ms} | rest],
         :unknown,
         [],
         0
       ) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    compact_hot_batch(
      original,
      rest,
      {:compound_put, redis_key},
      [{compound_key, value, expire_at_ms}],
      1
    )
  end

  defp compact_hot_batch(
         original,
         [{:compound_put, compound_key, value, expire_at_ms} | rest],
         {:compound_put, redis_key},
         acc,
         count
       ) do
    if Ferricstore.Store.CompoundKey.extract_redis_key(compound_key) == redis_key do
      compact_hot_batch(
        original,
        rest,
        {:compound_put, redis_key},
        [{compound_key, value, expire_at_ms} | acc],
        count + 1
      )
    else
      generic_hot_batch(original)
    end
  end

  defp compact_hot_batch(
         original,
         [{:compound_delete, compound_key} | rest],
         :unknown,
         [],
         0
       ) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)
    compact_hot_batch(original, rest, {:compound_delete, redis_key}, [compound_key], 1)
  end

  defp compact_hot_batch(
         original,
         [{:compound_delete, compound_key} | rest],
         {:compound_delete, redis_key},
         acc,
         count
       ) do
    if Ferricstore.Store.CompoundKey.extract_redis_key(compound_key) == redis_key do
      compact_hot_batch(
        original,
        rest,
        {:compound_delete, redis_key},
        [compound_key | acc],
        count + 1
      )
    else
      generic_hot_batch(original)
    end
  end

  defp compact_hot_batch(original, [], _mode, _acc, _count), do: generic_hot_batch(original)

  defp compact_hot_batch(original, [_other | _rest], _mode, _acc, _count),
    do: generic_hot_batch(original)

  defp generic_hot_batch(commands) do
    expanded = expand_put_delete_batch_terms(commands)
    {{:batch, expanded}, length(expanded)}
  end

  defp expand_put_delete_batch_terms(commands) do
    Enum.flat_map(commands, fn
      {:put_batch, entries} ->
        Enum.map(entries, fn {key, value, expire_at_ms} -> {:put, key, value, expire_at_ms} end)

      {:delete_batch, keys} ->
        Enum.map(keys, fn key -> {:delete, key} end)

      command ->
        [command]
    end)
  end

  defp batch_from_count(froms) do
    Enum.reduce(froms, 0, fn
      {:batch_from, _from, count}, acc -> acc + count
      _from, acc -> acc + 1
    end)
  end

  # Enqueue a write that enters through a non-blocking call but must use the
  # quorum slot like every other user-visible write.
  defp enqueue_write_forced_quorum(_command, from, %{writes_paused: true} = state) do
    reply_from(from, sync_pause_error())
    {:noreply, state}
  end

  defp enqueue_write_forced_quorum(command, from, state) do
    if pending_full?(state) do
      reply_from(from, {:error, :overloaded})
      {:noreply, state}
    else
      enqueue_write_forced_quorum_under_capacity(command, from, state)
    end
  end

  defp enqueue_write_forced_quorum_under_capacity(command, from, state) do
    prefix = extract_prefix(command)
    {window_ms, state} = lookup_ns_config(prefix, state)
    slot_key = {prefix, :quorum}
    cmd_bytes = command_estimated_bytes(command)

    case slot_for_append(state, slot_key, window_ms, cmd_bytes) do
      {:ok, state, slot} ->
        updated_slot = %{
          slot
          | cmds: [command | slot.cmds],
            froms: [from | slot.froms],
            window_ms: window_ms,
            count: slot.count + 1,
            bytes: slot.bytes + cmd_bytes
        }

        updated_slot =
          if updated_slot.timer_ref == nil do
            ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
            %{updated_slot | timer_ref: ref}
          else
            updated_slot
          end

        new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        if slot_flush_due?(state, updated_slot) do
          do_flush_slot(new_state, slot_key)
        else
          {:noreply, new_state}
        end

      {:overloaded, state} ->
        reply_from(from, {:error, :overloaded})
        {:noreply, state}
    end
  end

  # Enqueue an origin_submit cast. No `from` to track for fire-and-forget calls.
  defp enqueue_origin_submit(command, state) do
    if pending_full?(state) do
      emit_origin_submit_overloaded(state)
      {:noreply, state}
    else
      enqueue_origin_submit_under_capacity(command, state)
    end
  end

  defp emit_origin_submit_overloaded(state) do
    :telemetry.execute(
      [:ferricstore, :batcher, :origin_dropped],
      %{batch_size: 1},
      %{shard_index: state.shard_index, reason: :overloaded}
    )

    Logger.warning(
      "Batcher shard=#{state.shard_index}: origin replay command not enqueued because pending is full"
    )
  end

  defp emit_origin_submit_failed(state, batch_size, reason) do
    :telemetry.execute(
      [:ferricstore, :batcher, :origin_dropped],
      %{batch_size: batch_size},
      %{shard_index: state.shard_index, reason: reason}
    )

    Logger.warning(
      "Batcher shard=#{state.shard_index}: origin replay command not enqueued because Raft target is unavailable: #{inspect(reason)}"
    )
  end

  defp enqueue_origin_submit_under_capacity(command, state, from \\ nil) do
    prefix = extract_prefix(command)
    {window_ms, state} = lookup_ns_config(prefix, state)
    # Dedicated internal slot: commands here will be wrapped as
    # {:async, origin, inner} during flush so the state machine can origin-skip
    # effects that were already applied locally. This is not a namespace
    # durability mode.
    slot_key = {prefix, :origin_replay}
    cmd_bytes = command_estimated_bytes(command)

    case slot_for_append(state, slot_key, window_ms, cmd_bytes) do
      {:ok, state, slot} ->
        updated_slot = %{
          slot
          | cmds: [command | slot.cmds],
            froms: maybe_add_origin_submit_from(slot.froms, from),
            window_ms: window_ms,
            count: slot.count + 1,
            bytes: slot.bytes + cmd_bytes
        }

        updated_slot =
          if updated_slot.timer_ref == nil do
            ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
            %{updated_slot | timer_ref: ref}
          else
            updated_slot
          end

        new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

        if slot_flush_due?(state, updated_slot) do
          do_flush_slot(new_state, slot_key)
        else
          {:noreply, new_state}
        end

      {:overloaded, state} ->
        emit_origin_submit_overloaded(state)
        if from != nil, do: reply_from(from, {:error, :overloaded})
        {:noreply, state}
    end
  end

  defp maybe_add_origin_submit_from(froms, nil), do: froms
  defp maybe_add_origin_submit_from(froms, from), do: [from | froms]

  defp sync_pause_error, do: @sync_pause_error

  defp pending_full?(state) do
    map_size(state.pending) >= state.max_pending_batches or
      pending_bytes_full?(state)
  end

  defp pending_bytes_full?(state) do
    state.max_pending_bytes > 0 and map_size(state.pending) > 0 and
      state.pending_bytes >= state.max_pending_bytes
  end

  # Let one oversized write through when the queue is empty. Backpressure is
  # for unbounded in-flight buildup, not for rejecting valid large commands.
  defp pending_bytes_would_exceed?(state, incoming_bytes) do
    state.max_pending_bytes > 0 and state.pending_bytes > 0 and
      state.pending_bytes + incoming_bytes > state.max_pending_bytes
  end

  defp put_pending(state, corr, entry) do
    bytes = pending_entry_bytes(entry)

    %{
      state
      | pending: Map.put(state.pending, corr, entry),
        pending_bytes: state.pending_bytes + bytes
    }
  end

  defp pop_pending(state, corr) do
    case Map.pop(state.pending, corr) do
      {nil, pending} ->
        {nil, %{state | pending: pending}}

      {entry, pending} ->
        {entry,
         %{
           state
           | pending: pending,
             pending_bytes: max(0, state.pending_bytes - pending_entry_bytes(entry))
         }}
    end
  end

  defp pending_entries_bytes(pending) do
    Enum.reduce(pending, 0, fn {_corr, entry}, acc -> acc + pending_entry_bytes(entry) end)
  end

  defp pending_entry_bytes({_froms, kind, _mono, bytes})
       when kind in [:single, :batch] and is_integer(bytes) and bytes >= 0,
       do: bytes

  defp pending_entry_bytes({_froms, :origin_no_reply, _batch, _retry, _mono, bytes})
       when is_integer(bytes) and bytes >= 0,
       do: bytes

  defp pending_entry_bytes(_entry), do: 0

  # Reply to flush waiters once everything is drained: in-flight ra commands
  # are all applied AND the local state machine has caught up to all their
  # indices (no entries left in `local_apply_waiters`).
  @spec maybe_reply_flush_waiters(%__MODULE__{}) :: %__MODULE__{}
  defp maybe_reply_flush_waiters(%{flush_waiters: waiters} = state) do
    if flush_drained?(state) and waiters != [] do
      Enum.each(waiters, fn from -> GenServer.reply(from, :ok) end)
      %{state | flush_waiters: []}
    else
      state
    end
  end

  defp flush_drained?(%{
         pending: pending,
         local_apply_waiters: lwaiters,
         last_local_applied: last_local_applied,
         origin_local_apply_index: origin_local_apply_index
       }) do
    map_size(pending) == 0 and lwaiters == [] and last_local_applied >= origin_local_apply_index
  end

  # Drop pending entries whose submission timestamp is older than the TTL.
  #
  # Two classes of entries expire:
  #
  #   * `:origin_no_reply` (retry-aware) — silently drop; caller already
  #     got `:ok` when the command was enqueued. Emits
  #     `[:ferricstore, :batcher, :origin_dropped]` telemetry.
  #
  #   * `:single` / `:batch` (quorum callers) — reply with an unknown-outcome
  #     timeout error to every blocked `from` so they unblock, then drop. Emits
  #     `[:ferricstore, :batcher, :quorum_timeout]` telemetry. This is
  #     the production safety net for lost ra_events after a leader
  #     crash: without it, a caller blocked on `GenServer.call(..., :flush)`
  #     or on a `Batcher.write/2` would hang until its own `GenServer.call`
  #     timeout, and flush_waiters could hang indefinitely.
  @spec sweep_origin_pending(%__MODULE__{}) :: %__MODULE__{}
  defp sweep_origin_pending(state) do
    ttl_native = System.convert_time_unit(@origin_pending_ttl_ms, :millisecond, :native)
    quorum_ttl_native = System.convert_time_unit(@quorum_pending_ttl_ms, :millisecond, :native)
    now = System.monotonic_time()
    origin_cutoff = now - ttl_native
    quorum_cutoff = now - quorum_ttl_native

    {expired, kept} =
      Enum.split_with(state.pending, fn
        {_corr, {_froms, :origin_no_reply, _batch, _retry, mono, _bytes}} ->
          mono < origin_cutoff

        {_corr, {_froms, :origin_no_reply, _batch, _retry, mono}} ->
          mono < origin_cutoff

        {_corr, {_froms, :single, mono, _bytes}} ->
          mono < quorum_cutoff

        {_corr, {_froms, :batch, mono, _bytes}} ->
          mono < quorum_cutoff

        {_corr, {_froms, :single, mono}} ->
          mono < quorum_cutoff

        {_corr, {_froms, :batch, mono}} ->
          mono < quorum_cutoff

        _ ->
          false
      end)

    Enum.each(expired, fn
      {_corr, {_froms, :origin_no_reply, batch, _retry, _mono, _bytes}} ->
        :telemetry.execute(
          [:ferricstore, :batcher, :origin_dropped],
          %{batch_size: length(batch)},
          %{shard_index: state.shard_index, reason: :ttl}
        )

        Logger.warning(
          "Batcher shard=#{state.shard_index}: origin replay batch of #{length(batch)} commands dropped after #{@origin_pending_ttl_ms}ms TTL (ra_event lost)"
        )

      {_corr, {_froms, :origin_no_reply, batch, _retry, _mono}} ->
        :telemetry.execute(
          [:ferricstore, :batcher, :origin_dropped],
          %{batch_size: length(batch)},
          %{shard_index: state.shard_index, reason: :ttl}
        )

        Logger.warning(
          "Batcher shard=#{state.shard_index}: origin replay batch of #{length(batch)} commands dropped after #{@origin_pending_ttl_ms}ms TTL (ra_event lost)"
        )

      {_corr, {froms, kind, _mono, _bytes}} when kind in [:single, :batch] ->
        reply_all_froms(froms, ErrorReasons.write_timeout_unknown())

        :telemetry.execute(
          [:ferricstore, :batcher, :quorum_timeout],
          %{caller_count: length(froms), kind: kind},
          %{shard_index: state.shard_index, reason: :ttl}
        )

        Logger.warning(
          "Batcher shard=#{state.shard_index}: #{kind} pending entry with #{length(froms)} caller(s) timed out after #{@quorum_pending_ttl_ms}ms (ra_event lost after leader crash or disk error)"
        )

      {_corr, {froms, kind, _mono}} when kind in [:single, :batch] ->
        reply_all_froms(froms, ErrorReasons.write_timeout_unknown())

        :telemetry.execute(
          [:ferricstore, :batcher, :quorum_timeout],
          %{caller_count: length(froms), kind: kind},
          %{shard_index: state.shard_index, reason: :ttl}
        )

        Logger.warning(
          "Batcher shard=#{state.shard_index}: #{kind} pending entry with #{length(froms)} caller(s) timed out after #{@quorum_pending_ttl_ms}ms (ra_event lost after leader crash or disk error)"
        )
    end)

    new_pending = Map.new(kept)
    new_state = %{state | pending: new_pending, pending_bytes: pending_entries_bytes(new_pending)}

    # Swept quorum entries could unblock flush waiters whose pending ref
    # count drops to zero.
    maybe_reply_flush_waiters(new_state)
  end

  # ---------------------------------------------------------------------------
  # Private: namespace config lookup
  # ---------------------------------------------------------------------------

  @spec lookup_ns_config(binary(), %__MODULE__{}) :: {non_neg_integer(), %__MODULE__{}}
  defp lookup_ns_config(prefix, state) do
    case Map.get(state.ns_cache, prefix) do
      nil ->
        window = NamespaceConfig.window_for(prefix)
        new_cache = Map.put(state.ns_cache, prefix, window)
        {window, %{state | ns_cache: new_cache}}

      cached ->
        {cached, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: slot helpers
  # ---------------------------------------------------------------------------

  @spec new_slot(non_neg_integer()) :: slot()
  defp new_slot(window_ms) do
    %{
      cmds: [],
      froms: [],
      timer_ref: nil,
      window_ms: window_ms,
      count: 0,
      bytes: 0,
      created_mono: System.monotonic_time()
    }
  end

  defp slot_for_append(state, slot_key, window_ms, incoming_bytes) do
    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    if slot_byte_overflow?(state, slot, incoming_bytes) do
      case do_flush_slot(state, slot_key) do
        {:noreply, flushed_state} ->
          if pending_full?(flushed_state) do
            {:overloaded, flushed_state}
          else
            {:ok, flushed_state, new_slot(window_ms)}
          end
      end
    else
      {:ok, state, slot}
    end
  end

  defp slot_byte_overflow?(state, slot, incoming_bytes) do
    state.max_batch_bytes > 0 and slot.cmds != [] and
      slot.bytes + incoming_bytes > state.max_batch_bytes
  end

  defp slot_flush_due?(state, slot) do
    slot.count >= state.max_batch_size or
      (state.max_batch_bytes > 0 and slot.bytes >= state.max_batch_bytes)
  end

  defp commands_estimated_bytes(commands) do
    Enum.reduce(commands, 0, fn command, acc -> acc + command_estimated_bytes(command) end)
  end

  defp command_estimated_bytes({:put, key, value, _expire_at_ms})
       when is_binary(key) and is_binary(value),
       do: byte_size(key) + byte_size(value) + 32

  defp command_estimated_bytes({:put_batch, entries}) when is_list(entries) do
    Enum.reduce(entries, 16, fn
      {key, value, _expire_at_ms}, acc when is_binary(key) and is_binary(value) ->
        acc + byte_size(key) + byte_size(value) + 32

      other, acc ->
        acc + :erlang.external_size(other)
    end)
  end

  defp command_estimated_bytes({:delete, key}) when is_binary(key),
    do: byte_size(key) + 24

  defp command_estimated_bytes({:delete_batch, keys}) when is_list(keys) do
    Enum.reduce(keys, 16, fn
      key, acc when is_binary(key) -> acc + byte_size(key) + 24
      other, acc -> acc + :erlang.external_size(other)
    end)
  end

  defp command_estimated_bytes({:batch, commands}) when is_list(commands),
    do: commands_estimated_bytes(commands) + 16

  defp command_estimated_bytes(command), do: :erlang.external_size(command)

  defp emit_slot_flush_telemetry(state, slot, prefix, write_path, batch, froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :slot_flush],
      %{
        batch_size: Map.get(slot, :count, length(batch)),
        batch_bytes: Map.get(slot, :bytes, 0),
        caller_count: length(froms),
        queue_wait_us: duration_us(Map.get(slot, :created_mono, System.monotonic_time()))
      },
      %{
        shard_index: state.shard_index,
        prefix: prefix,
        window_ms: Map.get(slot, :window_ms, 0),
        write_path: write_path
      }
    )
  end

  defp emit_quorum_submit_telemetry(
         state,
         started_at,
         kind,
         batch_size,
         caller_count,
         command_bytes,
         command_shape,
         raft_priority,
         submit_result
       ) do
    :telemetry.execute(
      [:ferricstore, :batcher, :quorum_submit],
      %{
        duration_us: duration_us(started_at),
        batch_size: batch_size,
        caller_count: caller_count,
        command_bytes: command_bytes,
        pending_bytes: state.pending_bytes,
        max_pending_bytes: state.max_pending_bytes
      },
      %{
        shard_index: state.shard_index,
        kind: kind,
        command_shape: command_shape,
        raft_priority: raft_priority,
        status: pipeline_submit_status(submit_result)
      }
    )
  end

  defp raft_pipeline_priority, do: PerfToggles.pipeline_priority()

  defp command_shape({:put_batch, _entries}), do: :put_batch
  defp command_shape({:delete_batch, _keys}), do: :delete_batch
  defp command_shape({:compound_batch_put, _redis_key, _entries}), do: :compound_batch_put

  defp command_shape({:compound_blob_batch_put, _redis_key, _entries}),
    do: :compound_blob_batch_put

  defp command_shape({:compound_batch_delete, _redis_key, _keys}), do: :compound_batch_delete
  defp command_shape({:batch, _commands}), do: :batch
  defp command_shape(command) when is_tuple(command), do: elem(command, 0)
  defp command_shape(_command), do: :unknown

  defp pipeline_submit_status(:ok), do: :ok
  defp pipeline_submit_status({:ok, _}), do: :ok
  defp pipeline_submit_status({:error, _}), do: :error
  defp pipeline_submit_status(_), do: :unknown

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end

  defp cancel_timer(nil), do: :ok

  # Uses `info: false` to avoid a return-value message, and skips the
  # selective receive that scanned the entire mailbox (~1-5us under load).
  # Stale {:flush_slot, _} messages are harmless: handle_info already
  # handles the case where the slot no longer exists (returns {:noreply, state}).
  defp cancel_timer(ref), do: Process.cancel_timer(ref, info: false)

  # ---------------------------------------------------------------------------
  # Test-only hooks
  #
  # These functions exist to drive the origin retry flow from tests without
  # needing a live Ra cluster to produce :rejected events. They're public
  # but leading-underscore-flagged to discourage non-test callers. Nothing
  # in `lib/` depends on them.
  # ---------------------------------------------------------------------------

  @doc false
  @spec __inject_origin_pending__(non_neg_integer(), reference(), [tuple()], non_neg_integer()) ::
          :ok
  def __inject_origin_pending__(shard_index, corr, batch, retry_count) do
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_origin_pending__, corr, batch, retry_count, System.monotonic_time()}
    )
  end

  @doc false
  @spec __inject_origin_pending_at__(
          non_neg_integer(),
          reference(),
          [tuple()],
          non_neg_integer(),
          integer()
        ) :: :ok
  def __inject_origin_pending_at__(shard_index, corr, batch, retry_count, submitted_mono) do
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_origin_pending__, corr, batch, retry_count, submitted_mono}
    )
  end

  @doc false
  @spec __inject_quorum_pending_at__(
          non_neg_integer(),
          reference(),
          [GenServer.from()],
          :single | :batch,
          integer()
        ) :: :ok
  def __inject_quorum_pending_at__(shard_index, corr, froms, kind, submitted_mono) do
    __inject_quorum_pending_at__(shard_index, corr, froms, kind, submitted_mono, 0)
  end

  @doc false
  @spec __inject_quorum_pending_at__(
          non_neg_integer(),
          reference(),
          [GenServer.from()],
          :single | :batch,
          integer(),
          non_neg_integer()
        ) :: :ok
  def __inject_quorum_pending_at__(shard_index, corr, froms, kind, submitted_mono, bytes) do
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_quorum_pending__, corr, froms, kind, submitted_mono, bytes}
    )
  end

  @doc false
  @spec __latest_origin_corr__(non_neg_integer()) :: reference() | nil
  def __latest_origin_corr__(shard_index) do
    GenServer.call(batcher_name(shard_index), :__latest_origin_corr__)
  end

  @doc false
  @spec __has_pending__(non_neg_integer(), reference()) :: boolean()
  def __has_pending__(shard_index, corr) do
    GenServer.call(batcher_name(shard_index), {:__has_pending__, corr})
  end

  @doc false
  @spec __pending_bytes__(non_neg_integer()) :: non_neg_integer()
  def __pending_bytes__(shard_index) do
    GenServer.call(batcher_name(shard_index), :__pending_bytes__)
  end

  @doc false
  @spec __sweep_pending_now__(non_neg_integer()) :: :ok
  def __sweep_pending_now__(shard_index) do
    GenServer.call(batcher_name(shard_index), :__sweep_pending_now__)
  end
end
