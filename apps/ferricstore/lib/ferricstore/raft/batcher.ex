defmodule Ferricstore.Raft.Batcher do
  @moduledoc """
  Namespace-aware group commit batcher for a single FerricStore shard.

  Per spec sections 2C.5 and 2F.3, each shard has its own Batcher GenServer
  that accumulates write commands into per-namespace buffers, each with its
  own commit window and durability mode. When a namespace's timer fires, only
  that namespace's buffer is flushed.

  ## How it works

  1. A client calls `write/2` which sends a `GenServer.call` to the batcher.
  2. The batcher extracts the key's namespace prefix (e.g. `"session"` from
     `"session:abc123"`, `"_root"` for keys without a colon).
  3. The namespace config is looked up from the `:ferricstore_ns_config` ETS
     table to determine `window_ms` and `durability` for this prefix.
  4. The command and caller are appended to the namespace's buffer slot,
     identified by `{prefix, durability}`.
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

  If no configuration exists for a prefix, the defaults are used:
  `window_ms = 1`, `durability = :quorum`.

  For `:quorum` durability, commands are submitted to ra via
  `:ra.pipeline_command/3` with a correlation reference. Callers are replied to
  when the `ra_event` notification arrives confirming the command was applied.

  For `:async` durability (spec 2F.3), there are two entry points:

  - `Batcher.async_submit/2` (preferred) is called by `Router.async_write_*`
    after Router has already persisted locally (ETS + Bitcask for big values).
    Commands accumulate in a dedicated `{prefix, :async_origin}` slot and
    flush as one `ra.pipeline_command({:batch, [{:async, cmd}, ...]})` for
    replication. The state machine's `{:async, inner}` clause origin-skips
    on the node that already has the ETS entry. Ordered callers are replied
    after the pipeline submission; fire-and-forget callers have no reply.

  - `Batcher.write/2` on an async namespace (legacy callers) is the blocking
    entry; the caller is replied after Ra accepts the slot submission, commands
    go to Raft as a regular `{:batch, [cmds]}` (no `{:async, ...}` wrapper)
    and the state machine applies them normally on every node.

  ## Why a separate GenServer?

  The batcher is intentionally separate from the Shard GenServer and the
  ra state machine. This separation keeps the batching logic independent
  of the consensus layer and allows the Shard to remain focused on read
  operations and ETS management.

  ## Configuration

    * `:shard_id` (required) -- the ra server ID for this shard
    * `:shard_index` (required) -- zero-based shard index
    * `:max_batch_size` -- flush single-write slot when it reaches this size (default: 50_000)
  """

  use GenServer

  require Logger

  alias Ferricstore.ErrorReasons
  alias Ferricstore.NamespaceConfig
  alias Ferricstore.Raft.CommandClock

  @default_max_batch_size 50_000

  # Async retry tuning (Option R1 from the rejected-retry design). When Ra
  # returns :rejected {:not_leader, hint, corr} for an async batch, the
  # Batcher re-submits to the hinted leader up to @max_async_retries times
  # before giving up.
  @max_async_retries 3

  # Pending entries that don't receive :applied or :rejected within this
  # window are dropped by the periodic sweep. Guards against lost ra_event
  # messages (shouldn't happen, but bounded memory beats unbounded leak).
  @async_pending_ttl_ms 30_000
  # Quorum callers (blocked on :single/:batch) get a longer TTL — their
  # `GenServer.call` default timeout is 5s, but Router's quorum_write uses
  # 10s. Use 30s as a safety net: anything pending longer than 30s means
  # ra lost the ack entirely and the caller's own call has already errored.
  @quorum_pending_ttl_ms 30_000

  # Periodic sweep interval. Tight enough to catch stalls quickly, loose
  # enough not to burn CPU scanning an empty pending map.
  @async_pending_sweep_ms 10_000

  @type command ::
          {:put, binary(), binary(), non_neg_integer()}
          | {:delete, binary()}
          | {:incr, binary(), integer()}
          | {:incr_float, binary(), float()}
          | {:append, binary(), binary()}
          | {:getset, binary(), binary()}
          | {:getdel, binary()}
          | {:getex, binary(), non_neg_integer()}
          | {:setrange, binary(), non_neg_integer(), binary()}
          | {:cas, binary(), binary(), binary(), non_neg_integer() | nil}
          | {:lock, binary(), binary(), non_neg_integer()}
          | {:unlock, binary(), binary()}
          | {:extend, binary(), binary(), non_neg_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer()}
          | {:ratelimit_add, binary(), pos_integer(), pos_integer(), pos_integer(),
             non_neg_integer()}
          | {:list_op, binary(), term()}
          | {:compound_put, binary(), binary(), non_neg_integer()}
          | {:compound_delete, binary()}
          | {:compound_delete_prefix, binary()}
          | {:pfadd, binary(), [binary()]}
          | {:spop, binary(), nil | non_neg_integer()}
          | {:zpop, binary(), non_neg_integer(), :min | :max}
          | {:json_set, binary(), binary(), binary(), list()}
          | {:json_del, binary(), binary()}
          | {:json_numincrby, binary(), binary(), number()}
          | {:json_arrappend, binary(), binary(), [binary()]}
          | {:cross_shard_tx, list()}
          | {:flow_create, binary(), map()}
          | {:flow_claim_due, binary(), map()}
          | {:flow_complete, binary(), map()}
          | {:flow_transition, binary(), map()}
          | {:flow_retry, binary(), map()}
          | {:flow_fail, binary(), map()}
          | {:flow_cancel, binary(), map()}
          | {:flow_rewind, binary(), map()}
          | {:origin_checked, binary(), command(), binary() | nil, non_neg_integer()}
          | {:origin_checked, binary(), command(), binary() | nil, non_neg_integer(),
             binary() | nil, non_neg_integer()}

  @typedoc """
  A slot key identifies a unique batching bucket by namespace prefix and
  durability mode. Commands with the same prefix but different durability
  modes (which can happen if config changes mid-flight) are batched
  separately.
  """
  @type slot_key :: {binary(), :quorum | :async | :async_origin}

  @typedoc """
  A slot holds the accumulated commands and callers for a single namespace
  buffer, along with the timer reference for that slot's commit window.
  """
  @type slot :: %{
          cmds: [command()],
          froms: [GenServer.from()],
          timer_ref: reference() | nil,
          window_ms: non_neg_integer()
        }

  defstruct [
    :shard_id,
    :shard_index,
    :max_batch_size,
    slots: %{},
    ns_cache: %{},
    # Map from correlation ref -> {froms, :single | :batch} for in-flight ra commands
    pending: %{},
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
    # Highest async Ra index whose leader-applied event has arrived but that
    # this node may not have locally applied yet. Async callers are not blocked
    # on this, but flush/shutdown barriers must wait so local state-machine
    # side effects are caught up before reporting the shard drained.
    async_local_apply_index: 0
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

  For `:quorum` durability, this call blocks until the ra command is
  committed and applied. For `:async` durability, the call returns as
  soon as the slot is flushed (state machine application continues in
  the background on the origin and on replicas).

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
  Like `write_async/3` but forces quorum durability regardless of namespace
  config. Used by RMW operations (INCR, APPEND, GETSET, etc.) that need
  consensus for atomicity even when the namespace is configured async.
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
  Submits an async-durability write. Fire-and-forget.

  Called by Router on the origin node AFTER it has already written the value
  locally to ETS (and Bitcask for large values). The Batcher accumulates async
  commands in a slot and flushes them as a single batched `ra.pipeline_command`
  for replication. The caller already has `:ok` from Router — no reply needed.

  Commands are wrapped as `{:async, inner_cmd}` before submission so the
  state machine can distinguish them: on the origin node (which has the entry
  in ETS) apply/3 will skip; on replicas the inner command is applied normally.

  ## Parameters

    * `shard_index` -- zero-based shard index
    * `inner_command` -- the raw write command (e.g. `{:put, k, v, exp}`)
  """
  @spec async_submit(non_neg_integer(), command()) :: :ok
  def async_submit(shard_index, inner_command) do
    GenServer.cast(batcher_name(shard_index), {:async_submit, inner_command})
  end

  @doc """
  Enqueues an async-durability write and waits until the Batcher has submitted
  its local slot to Raft. State-machine application is still asynchronous, but
  callers do not observe success while the command only lives in a local timer
  slot.
  """
  @spec async_submit_ordered(non_neg_integer(), command()) ::
          :ok | {:error, :overloaded | {:ra_target_down, term()}}
  def async_submit_ordered(shard_index, inner_command) do
    GenServer.call(batcher_name(shard_index), {:async_submit_ordered, inner_command}, 5_000)
  end

  @doc """
  Enqueues an async-durability write and returns after the local Batcher has
  accepted it into a slot.

  This is intentionally weaker than `async_submit_ordered/2`: it preserves the
  low-latency async RMW path where waiting for `ra.pipeline_command/4` would
  dominate command latency. Use it only for commands whose caller explicitly
  accepts async durability.
  """
  @spec async_enqueue_ordered(non_neg_integer(), command()) ::
          :ok | {:error, :overloaded | {:ra_target_down, term()}}
  def async_enqueue_ordered(shard_index, inner_command) do
    GenServer.call(batcher_name(shard_index), {:async_enqueue_ordered, inner_command}, 5_000)
  end

  @doc """
  Submits multiple async-durability writes as one Raft pipeline batch.
  """
  @spec async_submit_batch_ordered(non_neg_integer(), [command()]) :: :ok | {:error, :overloaded}
  def async_submit_batch_ordered(_shard_index, []), do: :ok

  def async_submit_batch_ordered(shard_index, commands) do
    GenServer.call(batcher_name(shard_index), {:async_submit_batch_ordered, commands}, 5_000)
  end

  @doc """
  Returns whether the local async Batcher can currently accept another
  shard-local async submission.

  Router uses this as a cheap preflight before multi-shard async batches so
  an already-overloaded shard does not let earlier shard writes become locally
  visible before the batch returns an overall error. The actual submit still
  performs the authoritative check.
  """
  @spec async_accepting?(non_neg_integer()) :: boolean()
  def async_accepting?(shard_index) do
    GenServer.call(batcher_name(shard_index), :async_accepting?, 5_000)
  catch
    :exit, _ -> false
  end

  @doc """
  Submits a list of async commands to the batcher in a single cast.

  Same semantics as calling `async_submit/2` for each command, but sends
  one GenServer cast instead of N — reduces message passing overhead for
  batch async writes.
  """
  @spec async_submit_batch(non_neg_integer(), [command()]) :: :ok
  def async_submit_batch(shard_index, commands) do
    GenServer.cast(batcher_name(shard_index), {:async_submit_batch, commands})
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

    state = %__MODULE__{
      shard_id: shard_id,
      shard_index: shard_index,
      max_batch_size: max_batch_size
    }

    # Kick off the periodic async-pending sweep (TTL-drop lost entries).
    Process.send_after(self(), :sweep_async_pending, @async_pending_sweep_ms)

    {:ok, state}
  end

  @impl true
  def handle_call({:write, command}, from, state) do
    enqueue_write(command, from, state)
  end

  def handle_call({:write_forwarded, command, origin_node}, from, state) do
    enqueue_write(command, remote_origin_from(origin_node, from), state)
  end

  def handle_call({:async_submit_ordered, command}, from, state) do
    if pending_full?(state) do
      emit_async_submit_overloaded(state)
      {:reply, {:error, :overloaded}, state}
    else
      enqueue_async_submit_under_capacity(command, state, from)
    end
  end

  def handle_call({:async_enqueue_ordered, command}, _from, state) do
    cond do
      pending_full?(state) ->
        emit_async_submit_overloaded(state)
        {:reply, {:error, :overloaded}, state}

      not local_pipeline_target_alive?(state.shard_id) ->
        error = {:error, {:ra_target_down, state.shard_id}}
        emit_async_submit_failed(state, 1, error)
        {:reply, error, state}

      true ->
        {:noreply, new_state} = enqueue_async_submit_under_capacity(command, state)
        {:reply, :ok, new_state}
    end
  end

  def handle_call({:async_submit_batch_ordered, commands}, from, state) do
    if pending_full?(state) do
      emit_async_submit_overloaded(state)
      {:reply, {:error, :overloaded}, state}
    else
      {:noreply, submit_async_origin(state, commands, [from])}
    end
  end

  def handle_call(:async_accepting?, _from, state) do
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
  def handle_call({:__inject_async_pending__, corr, batch, retry_count, mono}, _from, state) do
    entry = {[], :async_no_reply, batch, retry_count, mono}
    {:reply, :ok, %{state | pending: Map.put(state.pending, corr, entry)}}
  end

  def handle_call({:__inject_quorum_pending__, corr, froms, kind, mono}, _from, state)
      when kind in [:single, :batch] do
    entry = {froms, kind, mono}
    {:reply, :ok, %{state | pending: Map.put(state.pending, corr, entry)}}
  end

  def handle_call(:__latest_async_corr__, _from, state) do
    latest =
      Enum.reduce(state.pending, {nil, 0}, fn
        {corr, {_froms, :async_no_reply, _batch, _retry, mono}}, {_best_corr, best_mono}
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

  def handle_call(:__sweep_pending_now__, _from, state) do
    {:reply, :ok, sweep_async_pending(state)}
  end

  # Test-only: force-clear all pending correlations and flush waiters.
  # Used by test cleanup to unstick a Batcher whose ra shard lost
  # correlations after a leader crash or disk error. Replies to blocked
  # callers (`:single`/`:batch` froms) with `{:error, :reset}` so they
  # don't hang forever.
  def handle_call(:__reset_pending__, _from, state) do
    Enum.each(state.pending, fn
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
          flush_waiters: [],
          local_apply_waiters: [],
          async_local_apply_index: state.last_local_applied
      }
      |> emit_local_apply_waiters()

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:write, command, reply_to}, state) do
    enqueue_write(command, reply_to, state)
  end

  def handle_cast({:async_submit, inner_command}, state) do
    enqueue_async_submit(inner_command, state)
  end

  def handle_cast({:async_submit_batch, commands}, state) do
    Enum.reduce(commands, {:noreply, state}, fn cmd, {:noreply, st} ->
      enqueue_async_submit(cmd, st)
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
    new_state =
      Enum.reduce(applied_list, state, fn {corr, raw_result}, acc ->
        {ra_index, result} = unwrap_applied(raw_result)

        case Map.pop(acc.pending, corr) do
          {nil, _pending} ->
            acc

          # Async entries: no callers to reply to. Just track and clear.
          {{_froms, :async_no_reply}, new_pending} ->
            track_async_local_apply(%{acc | pending: new_pending}, ra_index)

          {{_froms, :async_no_reply, _batch, _retry, _mono}, new_pending} ->
            track_async_local_apply(%{acc | pending: new_pending}, ra_index)

          {{froms, :single, _mono}, new_pending} ->
            acc2 = %{acc | pending: new_pending}
            gate_reply(acc2, ra_index, :single, froms, result)

          {{froms, :batch, _mono}, new_pending} ->
            acc2 = %{acc | pending: new_pending}
            gate_reply(acc2, ra_index, :batch, froms, result)
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

  # Handle rejected commands (not_leader). For async entries we re-submit
  # to the hinted leader up to @max_async_retries times before dropping.
  # For quorum entries we reply :error to the blocked caller so the
  # application can retry itself.
  def handle_info({:ra_event, _from_id, {:rejected, {not_leader, maybe_leader, corr}}}, state) do
    case Map.pop(state.pending, corr) do
      {nil, _pending} ->
        {:noreply, state}

      # Retry-aware async entry. Has the original batch + retry_count.
      {{_froms, :async_no_reply, batch, retry_count, _mono}, new_pending} ->
        state_without = %{state | pending: new_pending}

        target =
          case {not_leader, maybe_leader} do
            {:not_leader, leader} when leader != :undefined and leader != nil -> leader
            _ -> state.shard_id
          end

        new_state =
          if retry_count < @max_async_retries do
            :telemetry.execute(
              [:ferricstore, :batcher, :async_retry],
              %{retry_count: retry_count + 1, batch_size: length(batch)},
              %{shard_index: state.shard_index, target: inspect(target)}
            )

            submit_async_with_retry(state_without, batch, target, retry_count + 1)
          else
            :telemetry.execute(
              [:ferricstore, :batcher, :async_dropped],
              %{batch_size: length(batch)},
              %{shard_index: state.shard_index, reason: :max_retries}
            )

            Logger.warning(
              "Batcher shard=#{state.shard_index}: async batch of #{length(batch)} commands dropped after #{@max_async_retries} retries"
            )

            state_without
          end

        {:noreply, maybe_reply_flush_waiters(new_state)}

      # Legacy async shape without retry info — drop silently.
      {{_froms, :async_no_reply}, new_pending} ->
        {:noreply, maybe_reply_flush_waiters(%{state | pending: new_pending})}

      # Quorum entry — local server isn't leader. Reply :not_leader so
      # Router.forward_to_leader takes over (and barriers on local apply
      # via await_local_applied/2 after the leader replies, fixing
      # read-your-write across the redirect).
      {pending_entry, new_pending}
      when is_tuple(pending_entry) and elem(pending_entry, 1) in [:single, :batch] ->
        froms = elem(pending_entry, 0)
        new_state = %{state | pending: new_pending}

        leader =
          case {not_leader, maybe_leader} do
            {:not_leader, leader} when leader != :undefined and leader != nil -> leader
            _ -> state.shard_id
          end

        reply_all_froms(froms, {:error, {:not_leader, leader}})

        {:noreply, maybe_reply_flush_waiters(new_state)}
    end
  end

  # Periodic sweep of pending entries whose :applied or :rejected never
  # arrived (bounds memory against lost ra_events / pathological cluster
  # states). Only affects the retry-aware async entries; everything else
  # is either still in flight or will be resolved when the ra_event arrives.
  def handle_info(:sweep_async_pending, state) do
    new_state = sweep_async_pending(state)
    Process.send_after(self(), :sweep_async_pending, @async_pending_sweep_ms)
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

  # If the new-shape wrapped reply is present, unwrap; otherwise pass through
  # for backward compatibility with WAL entries that pre-date this change.
  defp unwrap_applied({:applied_at, ra_index, real_result}), do: {ra_index, real_result}
  defp unwrap_applied(other), do: {0, other}

  # Reply now if the legacy result has no Ra index or if the local state machine
  # has already applied this index. Otherwise all callers wait for
  # `{:locally_applied, ra_index}`. That includes local callers: Ra's applied
  # event is not a sufficient read-your-write barrier for the Router/Shard read
  # path, which observes this node's ETS.
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
    enqueue_local_apply_waiter(state, ra_index, kind, froms, result)
  end

  defp track_async_local_apply(state, ra_index) when is_integer(ra_index) and ra_index > 0 do
    %{state | async_local_apply_index: max(state.async_local_apply_index, ra_index)}
  end

  defp track_async_local_apply(state, _ra_index), do: state

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
    {ready, still_waiting} =
      Enum.split_with(state.local_apply_waiters, fn waiter -> waiter_index(waiter) <= lla end)

    Enum.each(ready, fn waiter ->
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
      {_corr, {_froms, :async_no_reply, _batch, _retry, _mono}} ->
        :ok

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

  @max_pending 64

  # Accumulates write_batch commands into the quorum slot, flushing when
  # count >= max_batch_size or on next message loop (timer 0). Rejects
  # new batches when too many ra commands are already in flight.
  defp enqueue_write_batch([], _cmd_count, from, state) do
    reply_from(from, {:ok, []})
    {:noreply, state}
  end

  defp enqueue_write_batch(cmds, cmd_count, from, state) do
    if map_size(state.pending) >= @max_pending do
      reply_from(from, {:error, :overloaded})
      {:noreply, state}
    else
      prefix = extract_prefix(hd(cmds))
      slot_key = {prefix, :quorum}

      slot = Map.get(state.slots, slot_key, new_slot(0))

      updated_slot = %{
        slot
        | cmds: prepend_reversed(cmds, slot.cmds),
          froms: [{:batch_from, from, cmd_count} | slot.froms],
          count: Map.get(slot, :count, 0) + cmd_count
      }

      state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

      if updated_slot.count >= state.max_batch_size do
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
    {{window_ms, ns_durability}, state} = lookup_ns_config(prefix, state)

    # Linearizable primitives (CAS/LOCK/UNLOCK/EXTEND/RATELIMIT, prob structures)
    # must take the quorum path even on async-configured namespaces — the caller
    # needs the actual state machine result, not a premature :ok.
    durability =
      if Ferricstore.Store.Router.always_quorum?(command), do: :quorum, else: ns_durability

    slot_key = {prefix, durability}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: [from | slot.froms],
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    # Flush immediately if slot is full (O(1) count check instead of O(n) length)
    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
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

        {_prefix, durability} = slot_key
        emit_slot_flush_telemetry(state, slot, durability, batch, froms)

        new_state =
          case durability do
            :async_origin ->
              # Commands came via Batcher.async_submit (Router wrote locally).
              # Wrap each command as {:async, inner} so state machine can
              # origin-skip on the node that already has the ETS entry.
              submit_async_origin(state, batch, froms)

            :async ->
              # Commands came via Batcher.write on an :async namespace
              # (blocking callers, e.g. RMW ops via Shard). Router did NOT
              # write locally — state machine must apply the inner command.
              # Reply after Ra accepts the regular batch (no {:async, ...}
              # wrapper), so target-down/submit errors are visible.
              submit_async_ns(state, batch, froms)

            :quorum ->
              pipeline_submit(state, batch, froms)
          end

        # Remove the slot entirely once flushed (clean up empty slots)
        new_slots = Map.delete(new_state.slots, slot_key)
        {:noreply, %{new_state | slots: new_slots}}
    end
  end

  # Submit a batch via ra:pipeline_command/3 with a correlation ref.
  # For single commands, submit directly (no batch wrapper).
  # For multiple commands, wrap in {:batch, commands}.
  # Returns updated state with the correlation tracked in `pending`.
  @spec pipeline_submit(%__MODULE__{}, [command()], [GenServer.from()]) :: %__MODULE__{}
  defp pipeline_submit(state, [single_cmd], froms) do
    corr = make_ref()
    started_at = System.monotonic_time()
    {:ttb, bin} = serialized = CommandClock.to_ttb(single_cmd)
    submit_result = pipeline_command(state.shard_id, serialized, corr, :normal)

    emit_quorum_submit_telemetry(
      state,
      started_at,
      :single,
      1,
      length(froms),
      byte_size(bin),
      submit_result
    )

    track_or_reject_quorum_submit(state, corr, froms, :single, submit_result)
  end

  defp pipeline_submit(state, batch, froms) do
    corr = make_ref()
    started_at = System.monotonic_time()
    {:ttb, bin} = serialized = CommandClock.to_ttb({:batch, batch})
    submit_result = pipeline_command(state.shard_id, serialized, corr, :normal)

    emit_quorum_submit_telemetry(
      state,
      started_at,
      :batch,
      length(batch),
      length(froms),
      byte_size(bin),
      submit_result
    )

    track_or_reject_quorum_submit(state, corr, froms, :batch, submit_result)
  end

  # Flush path for commands accumulated via Batcher.async_submit (called by
  # Router.async_write_*). Router has already persisted locally (ETS + Bitcask
  # for big values) before calling async_submit. Commands are wrapped as
  # `{:async, inner}` so the state machine can origin-skip on the node that
  # already has the ETS entry. Ordered callers are replied after pipeline
  # submission; fire-and-forget callers have no reply.
  #
  # Tracks the correlation in `pending` with `:async_no_reply` so that
  # Batcher.flush waiters can observe when all in-flight async commands have
  # applied to the state machine. The stored tuple carries the already-
  # wrapped batch + retry_count + submission timestamp so that the
  # :rejected handler can re-submit to a hinted leader and the periodic
  # sweep can drop stalled entries.
  defp submit_async_origin(state, batch, submit_froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_flush],
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
    submit_async_with_retry(state, wrapped, state.shard_id, 0, submit_froms)
  end

  # Flush path for commands accumulated via Batcher.write (blocking) on an
  # :async-durability namespace. Router did NOT write locally — the state
  # machine must apply the inner command. Reply to blocked callers after Ra
  # accepts the submission, then track the correlation in `pending` so flush
  # waiters observe completion.
  defp submit_async_ns(state, batch, froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_flush],
      %{batch_size: length(batch)},
      %{shard_index: state.shard_index, origin: false}
    )

    submit_async_with_retry(state, batch, state.shard_id, 0, froms)
  end

  # Shared helper for initial async submission and retries after :rejected.
  # `wrapped_batch` is the already-prepared payload (with or without the
  # {:async, ...} wrapper — caller's choice, not interpreted here). We
  # serialize once and hand to Ra via pipeline_command on `target`, then
  # track the correlation so :rejected can re-submit and :applied can clean
  # up. `retry_count` is the number of retries already attempted; starts
  # at 0 for the first submission.
  defp submit_async_with_retry(state, wrapped_batch, target, retry_count, submit_froms \\ []) do
    corr = make_ref()
    serialized = CommandClock.to_ttb({:batch, wrapped_batch})
    submit_result = pipeline_command(target, serialized, corr, :normal)

    if pipeline_submit_status(submit_result) == :ok do
      reply_all_froms(submit_froms, :ok)

      entry = {[], :async_no_reply, wrapped_batch, retry_count, System.monotonic_time()}
      %{state | pending: Map.put(state.pending, corr, entry)}
    else
      reply_all_froms(submit_froms, normalize_pipeline_error(submit_result))

      :telemetry.execute(
        [:ferricstore, :batcher, :async_dropped],
        %{batch_size: length(wrapped_batch)},
        %{shard_index: state.shard_index, reason: submit_result}
      )

      Logger.warning(
        "Batcher shard=#{state.shard_index}: async batch of #{length(wrapped_batch)} commands not submitted to Raft: #{inspect(submit_result)}"
      )

      maybe_reply_flush_waiters(state)
    end
  end

  defp track_or_reject_quorum_submit(state, corr, froms, kind, submit_result) do
    if pipeline_submit_status(submit_result) == :ok do
      mono = System.monotonic_time()
      %{state | pending: Map.put(state.pending, corr, {froms, kind, mono})}
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

  # Enqueue a write that MUST go through quorum (RMW ops where the caller
  # needs atomicity). Bypasses namespace-config durability lookup and uses
  # the quorum slot regardless of how the namespace is configured.
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
    # Still need window_ms from config, but force durability to quorum.
    {{window_ms, _durability}, state} = lookup_ns_config(prefix, state)
    slot_key = {prefix, :quorum}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: [from | slot.froms],
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  # Enqueue an async_submit cast. No `from` to track for fire-and-forget calls.
  defp enqueue_async_submit(command, state) do
    if pending_full?(state) do
      emit_async_submit_overloaded(state)
      {:noreply, state}
    else
      enqueue_async_submit_under_capacity(command, state)
    end
  end

  defp emit_async_submit_overloaded(state) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_dropped],
      %{batch_size: 1},
      %{shard_index: state.shard_index, reason: :overloaded}
    )

    Logger.warning(
      "Batcher shard=#{state.shard_index}: async command not enqueued because pending is full"
    )
  end

  defp emit_async_submit_failed(state, batch_size, reason) do
    :telemetry.execute(
      [:ferricstore, :batcher, :async_dropped],
      %{batch_size: batch_size},
      %{shard_index: state.shard_index, reason: reason}
    )

    Logger.warning(
      "Batcher shard=#{state.shard_index}: async command not enqueued because Raft target is unavailable: #{inspect(reason)}"
    )
  end

  defp enqueue_async_submit_under_capacity(command, state, from \\ nil) do
    prefix = extract_prefix(command)
    {{window_ms, _durability}, state} = lookup_ns_config(prefix, state)
    # Dedicated slot: commands here will be wrapped as {:async, inner} during
    # flush so the state machine knows Router has already persisted locally
    # on the origin (origin-skip optimization in state_machine.ex). This
    # differs from {prefix, :async} slots where commands came through
    # Batcher.write on an :async namespace — those go to the state machine
    # unwrapped because Router did NOT write locally (it was a blocking call
    # through Shard, e.g. for RMW ops routed to quorum).
    slot_key = {prefix, :async_origin}

    slot = Map.get(state.slots, slot_key, new_slot(window_ms))

    updated_slot = %{
      slot
      | cmds: [command | slot.cmds],
        froms: maybe_add_async_submit_from(slot.froms, from),
        window_ms: window_ms,
        count: Map.get(slot, :count, 0) + 1
    }

    updated_slot =
      if updated_slot.timer_ref == nil do
        ref = Process.send_after(self(), {:flush_slot, slot_key}, window_ms)
        %{updated_slot | timer_ref: ref}
      else
        updated_slot
      end

    new_state = %{state | slots: Map.put(state.slots, slot_key, updated_slot)}

    if updated_slot.count >= state.max_batch_size do
      do_flush_slot(new_state, slot_key)
    else
      {:noreply, new_state}
    end
  end

  defp maybe_add_async_submit_from(froms, nil), do: froms
  defp maybe_add_async_submit_from(froms, from), do: [from | froms]

  defp pending_full?(state), do: map_size(state.pending) >= @max_pending

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
         async_local_apply_index: async_local_apply_index
       }) do
    map_size(pending) == 0 and lwaiters == [] and last_local_applied >= async_local_apply_index
  end

  # Drop pending entries whose submission timestamp is older than the TTL.
  #
  # Two classes of entries expire:
  #
  #   * `:async_no_reply` (retry-aware) — silently drop; caller already
  #     got `:ok` when the command was enqueued. Emits
  #     `[:ferricstore, :batcher, :async_dropped]` telemetry.
  #
  #   * `:single` / `:batch` (quorum callers) — reply with an unknown-outcome
  #     timeout error to every blocked `from` so they unblock, then drop. Emits
  #     `[:ferricstore, :batcher, :quorum_timeout]` telemetry. This is
  #     the production safety net for lost ra_events after a leader
  #     crash: without it, a caller blocked on `GenServer.call(..., :flush)`
  #     or on a `Batcher.write/2` would hang until its own `GenServer.call`
  #     timeout, and flush_waiters could hang indefinitely.
  @spec sweep_async_pending(%__MODULE__{}) :: %__MODULE__{}
  defp sweep_async_pending(state) do
    ttl_native = System.convert_time_unit(@async_pending_ttl_ms, :millisecond, :native)
    quorum_ttl_native = System.convert_time_unit(@quorum_pending_ttl_ms, :millisecond, :native)
    now = System.monotonic_time()
    async_cutoff = now - ttl_native
    quorum_cutoff = now - quorum_ttl_native

    {expired, kept} =
      Enum.split_with(state.pending, fn
        {_corr, {_froms, :async_no_reply, _batch, _retry, mono}} -> mono < async_cutoff
        {_corr, {_froms, :single, mono}} -> mono < quorum_cutoff
        {_corr, {_froms, :batch, mono}} -> mono < quorum_cutoff
        _ -> false
      end)

    Enum.each(expired, fn
      {_corr, {_froms, :async_no_reply, batch, _retry, _mono}} ->
        :telemetry.execute(
          [:ferricstore, :batcher, :async_dropped],
          %{batch_size: length(batch)},
          %{shard_index: state.shard_index, reason: :ttl}
        )

        Logger.warning(
          "Batcher shard=#{state.shard_index}: async batch of #{length(batch)} commands dropped after #{@async_pending_ttl_ms}ms TTL (ra_event lost)"
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

    new_state = %{state | pending: Map.new(kept)}

    # Swept quorum entries could unblock flush waiters whose pending ref
    # count drops to zero.
    maybe_reply_flush_waiters(new_state)
  end

  # ---------------------------------------------------------------------------
  # Private: namespace config lookup
  # ---------------------------------------------------------------------------

  @spec lookup_ns_config(binary(), %__MODULE__{}) ::
          {{pos_integer(), :quorum | :async}, %__MODULE__{}}
  defp lookup_ns_config(prefix, state) do
    case Map.get(state.ns_cache, prefix) do
      nil ->
        window = NamespaceConfig.window_for(prefix)
        durability = NamespaceConfig.durability_for(prefix)
        new_cache = Map.put(state.ns_cache, prefix, {window, durability})
        {{window, durability}, %{state | ns_cache: new_cache}}

      cached ->
        {cached, state}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: slot helpers
  # ---------------------------------------------------------------------------

  @spec new_slot(pos_integer()) :: slot()
  defp new_slot(window_ms) do
    %{
      cmds: [],
      froms: [],
      timer_ref: nil,
      window_ms: window_ms,
      count: 0,
      created_mono: System.monotonic_time()
    }
  end

  defp emit_slot_flush_telemetry(state, slot, durability, batch, froms) do
    :telemetry.execute(
      [:ferricstore, :batcher, :slot_flush],
      %{
        batch_size: length(batch),
        caller_count: length(froms),
        queue_wait_us: duration_us(Map.get(slot, :created_mono, System.monotonic_time()))
      },
      %{shard_index: state.shard_index, durability: durability}
    )
  end

  defp emit_quorum_submit_telemetry(
         state,
         started_at,
         kind,
         batch_size,
         caller_count,
         command_bytes,
         submit_result
       ) do
    :telemetry.execute(
      [:ferricstore, :batcher, :quorum_submit],
      %{
        duration_us: duration_us(started_at),
        batch_size: batch_size,
        caller_count: caller_count,
        command_bytes: command_bytes
      },
      %{shard_index: state.shard_index, kind: kind, status: pipeline_submit_status(submit_result)}
    )
  end

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
  # These functions exist to drive the async retry flow from tests without
  # needing a live Ra cluster to produce :rejected events. They're public
  # but leading-underscore-flagged to discourage non-test callers. Nothing
  # in `lib/` depends on them.
  # ---------------------------------------------------------------------------

  @doc false
  @spec __inject_async_pending__(non_neg_integer(), reference(), [tuple()], non_neg_integer()) ::
          :ok
  def __inject_async_pending__(shard_index, corr, batch, retry_count) do
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_async_pending__, corr, batch, retry_count, System.monotonic_time()}
    )
  end

  @doc false
  @spec __inject_async_pending_at__(
          non_neg_integer(),
          reference(),
          [tuple()],
          non_neg_integer(),
          integer()
        ) :: :ok
  def __inject_async_pending_at__(shard_index, corr, batch, retry_count, submitted_mono) do
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_async_pending__, corr, batch, retry_count, submitted_mono}
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
    GenServer.call(
      batcher_name(shard_index),
      {:__inject_quorum_pending__, corr, froms, kind, submitted_mono}
    )
  end

  @doc false
  @spec __latest_async_corr__(non_neg_integer()) :: reference() | nil
  def __latest_async_corr__(shard_index) do
    GenServer.call(batcher_name(shard_index), :__latest_async_corr__)
  end

  @doc false
  @spec __has_pending__(non_neg_integer(), reference()) :: boolean()
  def __has_pending__(shard_index, corr) do
    GenServer.call(batcher_name(shard_index), {:__has_pending__, corr})
  end

  @doc false
  @spec __sweep_pending_now__(non_neg_integer()) :: :ok
  def __sweep_pending_now__(shard_index) do
    GenServer.call(batcher_name(shard_index), :__sweep_pending_now__)
  end
end
