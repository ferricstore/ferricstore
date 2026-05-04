defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using consistent hashing via `:erlang.phash2/2`.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/2` map a key to its
     owning shard index and registered process name respectively. Supports
     Redis hash tags: keys containing `{tag}` are hashed on the tag content,
     allowing related keys to co-locate on the same shard.

  2. **Convenience accessors** -- `get/2`, `put/4`, `delete/2`, `exists?/2`,
     `keys/1`, and `dbsize/1` dispatch to the correct shard GenServer
     transparently.

  All public functions take a `ctx` (`FerricStore.Instance.t()`) as the first
  argument, replacing all persistent_term lookups with instance-local state.
  """

  alias Ferricstore.{CommandTime, HLC}
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Stats
  alias Ferricstore.Store.{CompoundKey, LFU, ListOps, Promotion, TypeRegistry, ValueCodec}
  alias Ferricstore.Store.Shard.ZSetIndex

  import Bitwise, only: [band: 2]

  @slot_mask 1023
  @cold_batch_read_timeout_ms 10_000
  @cold_location_retry_attempts 8
  @cold_location_retry_sleep_ms 1
  @default_async_key_latch_timeout_ms 30_000
  @async_list_rollback_key :ferricstore_async_list_originals

  defguardp valid_cold_file_ref(file_id, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(value_size) and
                   value_size >= 0

  defguardp valid_cold_location(file_id, offset, value_size)
            when valid_cold_file_ref(file_id, value_size) and is_integer(offset) and offset >= 0

  defguardp valid_pending_value_size(value_size)
            when is_integer(value_size) and value_size >= 0

  # ---------------------------------------------------------------------------
  # Shard resolution helpers
  # ---------------------------------------------------------------------------

  @spec resolve_shard(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  @doc false
  def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)

  @doc false
  def safe_read_call(ctx, idx, request) do
    {:ok, GenServer.call(resolve_shard(ctx, idx), request)}
  catch
    :exit, {:noproc, _} ->
      emit_shard_unavailable(ctx, idx, request, :noproc)
      :unavailable

    :exit, {:timeout, _} ->
      emit_shard_unavailable(ctx, idx, request, :timeout)
      :unavailable
  end

  defp safe_write_call(ctx, idx, request) do
    GenServer.call(resolve_shard(ctx, idx), request)
  catch
    :exit, {:noproc, _} ->
      emit_shard_unavailable(ctx, idx, request, :noproc)
      {:error, "ERR shard not available"}

    :exit, {:timeout, _} ->
      emit_shard_unavailable(ctx, idx, request, :timeout)
      ErrorReasons.write_timeout_unknown()

    :exit, _reason ->
      emit_shard_unavailable(ctx, idx, request, :exit)
      {:error, "ERR shard not available"}
  end

  defp emit_shard_unavailable(ctx, idx, request, reason) do
    :telemetry.execute(
      [:ferricstore, :store, :shard_unavailable],
      %{count: 1},
      %{
        instance: ctx.name,
        shard_index: idx,
        request: request_name(request),
        reason: reason
      }
    )
  end

  defp request_name(request) when is_tuple(request), do: elem(request, 0)
  defp request_name(request), do: request

  @spec resolve_keydir(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
  @doc false
  def resolve_keydir(ctx, idx), do: elem(ctx.keydir_refs, idx)
  @spec effective_shard_count(FerricStore.Instance.t()) :: non_neg_integer()
  @doc false
  def effective_shard_count(ctx), do: ctx.shard_count

  # ---------------------------------------------------------------------------
  # Write-path dispatch: quorum writes bypass Shard, async writes use Shard
  # ---------------------------------------------------------------------------

  @spec quorum_write(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  defp quorum_write(ctx, idx, command) do
    result =
      try do
        GenServer.call(elem(ctx.shard_names, idx), command, 10_000)
      catch
        :exit, {:timeout, _} ->
          ErrorReasons.write_timeout_unknown()

        :exit, {:noproc, _} ->
          {:error, "ERR shard not available"}
      end

    case result do
      {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, _} ->
        result

      _ ->
        size = :counters.info(ctx.write_version).size
        if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
        result
    end
  end

  defp forward_to_leader(ctx, leader_node, idx, command) do
    if leader_node == node() do
      {:error, "ERR not leader, election in progress"}
    else
      forward_via_shard_call(ctx, leader_node, idx, command)
    end
  end

  defp forward_via_shard_call(ctx, leader_node, idx, command) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

      result =
        :erpc.call(
          leader_node,
          GenServer,
          :call,
          [elem(remote_ctx.shard_names, idx), {:forwarded_quorum, node(), command}, 10_000],
          10_000
        )

      case result do
        # Leader's batcher detected a cross-node caller and tagged the reply
        # with the ra_index. Barrier on local apply so reads on this node
        # see the just-written value (read-your-write across redirects).
        {:remote_applied_at, _ra_index, _real_result} ->
          barrier_forwarded_result(idx, result, 5_000)

        other ->
          barrier_forwarded_result(idx, other, 5_000)
      end
    catch
      _, reason ->
        if forward_timeout?(reason) do
          ErrorReasons.write_timeout_unknown()
        else
          try do
            GenServer.call(elem(ctx.shard_names, idx), command, 10_000)
          catch
            _, _ -> {:error, "ERR leader unavailable"}
          end
        end
    end
  end

  @doc false
  def __barrier_forwarded_result__(idx, result, timeout_ms \\ 5_000),
    do: barrier_forwarded_result(idx, result, timeout_ms)

  @doc false
  @spec __forward_batch_failure_results__(term(), non_neg_integer()) :: [
          {:error, binary() | {:timeout, :unknown_outcome}}
        ]
  def __forward_batch_failure_results__(reason, count) when is_integer(count) and count >= 0 do
    List.duplicate(forward_failure_result(reason), count)
  end

  @spec forward_failure_result(term()) :: {:error, binary() | {:timeout, :unknown_outcome}}
  defp forward_failure_result(reason) do
    if forward_timeout?(reason) do
      ErrorReasons.write_timeout_unknown()
    else
      {:error, "ERR leader unavailable"}
    end
  end

  defp forward_timeout?({:erpc, :timeout}), do: true
  defp forward_timeout?(:timeout), do: true
  defp forward_timeout?({:timeout, _}), do: true
  defp forward_timeout?(_reason), do: false

  defp barrier_forwarded_result(idx, {:remote_applied_at, ra_index, real_result}, timeout_ms) do
    case Ferricstore.Raft.Batcher.await_local_applied(idx, ra_index, timeout_ms) do
      :ok -> real_result
      {:error, _reason} -> ErrorReasons.write_timeout_unknown()
    end
  end

  defp barrier_forwarded_result(_idx, other, _timeout_ms), do: other

  @doc "Public wrapper for durability_for_key, used by batch SET fast path."
  @spec durability_for_key_public(FerricStore.Instance.t(), binary()) :: :quorum | :async
  def durability_for_key_public(ctx, key), do: durability_for_key(ctx, key)

  @spec durability_for_key(FerricStore.Instance.t(), binary()) :: :quorum | :async
  defp durability_for_key(ctx, key) do
    case ctx.durability_mode do
      :all_quorum ->
        :quorum

      :all_async ->
        :async

      :mixed ->
        prefix =
          case :binary.split(key, ":") do
            [^key] -> "_root"
            [p | _] -> p
          end

        Ferricstore.NamespaceConfig.durability_for(prefix)
    end
  end

  # Dispatches writes based on namespace durability mode.
  #
  # Quorum: submit to Raft, wait for quorum apply. Strongest guarantee.
  # Async:  write ETS immediately, submit to Raft non-blocking (fire-and-forget).
  #         Like Redis Cluster — client sees the write before replication completes.
  #         Leader crash before replication = data loss (documented trade-off).
  #
  # Linearizable commands (CAS, LOCK, UNLOCK, EXTEND, RATELIMIT) always go
  # through `quorum_write`, regardless of the namespace's durability setting.
  # Their contract is linearizability — running them through the async path
  # makes their result observable on origin before replication, breaking the
  # primitive. Whitelisting at this seam keeps the rule local and obvious.
  @spec raft_write(FerricStore.Instance.t(), non_neg_integer(), binary(), tuple()) :: term()
  defp raft_write(ctx, idx, key, command) do
    if ctx.name == :default do
      cond do
        always_quorum?(command) -> quorum_write(ctx, idx, command)
        durability_for_key(ctx, key) == :quorum -> quorum_write(ctx, idx, command)
        true -> async_write(ctx, idx, command)
      end
    else
      # Custom embedded instances are local/direct. The default application
      # instance owns Raft; there is no public switch that can disable it.
      GenServer.call(elem(ctx.shard_names, idx), command)
    end
  end

  # Linearizable primitives — must NEVER take the async path even if the
  # namespace is configured `:async`. Adding a new coordination primitive?
  # Add it here.
  @doc false
  def always_quorum?({:set, _, _, _, _}), do: true
  def always_quorum?({:cas, _, _, _, _}), do: true
  def always_quorum?({:lock, _, _, _}), do: true
  def always_quorum?({:unlock, _, _}), do: true
  def always_quorum?({:extend, _, _, _}), do: true
  def always_quorum?({:ratelimit_add, _, _, _, _}), do: true
  def always_quorum?({:ratelimit_add, _, _, _, _, _}), do: true

  # Probabilistic structures: results (e.g., Bloom "was it new?", TopK
  # evicted member, CMS post-increment count) are computed by the state
  # machine. The async fast path early-replies before that result is
  # available, so prob commands must run through quorum_write to return
  # the materialized value. Tuple shapes match those in
  # `state_machine.ex` apply/3 clauses.
  def always_quorum?({:bloom_create, _, _, _, _}), do: true
  def always_quorum?({:bloom_add, _, _, _}), do: true
  def always_quorum?({:bloom_madd, _, _, _}), do: true
  def always_quorum?({:cuckoo_create, _, _, _}), do: true
  def always_quorum?({:cuckoo_add, _, _, _}), do: true
  def always_quorum?({:cuckoo_addnx, _, _, _}), do: true
  def always_quorum?({:cuckoo_del, _, _}), do: true
  def always_quorum?({:cms_create, _, _, _}), do: true
  def always_quorum?({:cms_incrby, _, _}), do: true
  def always_quorum?({:cms_merge, _, _, _, _}), do: true
  def always_quorum?({:topk_create, _, _, _, _, _}), do: true
  def always_quorum?({:topk_add, _, _}), do: true
  def always_quorum?({:topk_incrby, _, _}), do: true
  def always_quorum?(_), do: false

  # NOTE: json/bitmap/geo/hll/tdigest ops route through async_write →
  # Batcher.write_async_quorum, which goes directly to the Batcher (bypassing
  # the Shard GenServer that has no handler for these tuples). The
  # forced-quorum slot returns the actual state machine result, so RMW
  # semantics are preserved without quorum_write deadlocking.

  # Async write path (like Redis Cluster — async replication):
  # 1. Execute locally: direct ETS write + BitcaskWriter (no GenServer)
  # 2. Submit to Raft fire-and-forget (replication to followers)
  #
  # All writes bypass the Shard GenServer entirely — ETS is :public with
  # write_concurrency so any process can write. BitcaskWriter is a cast.
  # This eliminates the GenServer serialization bottleneck.
  #
  # For read-modify-write (INCR etc.), concurrent same-key mutations are
  # serialized via the per-key latch + RmwCoordinator worker. No lost updates.

  defp shard_under_disk_pressure?(ctx, idx) do
    size = :atomics.info(ctx.disk_pressure).size
    idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1
  end

  defp async_write(ctx, idx, {:put, key, value, expire_at_ms}) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = if idx < size, do: :atomics.get(ctx.disk_pressure, idx + 1) == 1, else: false

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      async_write_put(ctx, idx, key, value, expire_at_ms)
    end
  end

  defp async_write(ctx, idx, {:delete, key}) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = if idx < size, do: :atomics.get(ctx.disk_pressure, idx + 1) == 1, else: false

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      with_async_key_latch(ctx, idx, key, fn ->
        previous = snapshot_live_value(ctx, idx, key)
        raft_cmd = origin_checked_command(key, {:delete, key}, previous, nil, 0)

        case async_enqueue_to_raft(idx, raft_cmd) do
          :ok ->
            keydir = elem(ctx.keydir_refs, idx)
            flush_pending_writer_for_key(ctx, idx, keydir, key)
            track_keydir_binary_delete(ctx, idx, keydir, key)
            :ets.delete(keydir, key)

            wv_size = :counters.info(ctx.write_version).size
            if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
            :ok

          {:error, _} = error ->
            error
        end
      end)
    end
  end

  # Read-modify-write async paths. Latch-first, worker fallback. See
  # docs/async-rmw-design.md. Caller tries `:ets.insert_new` on the per-shard
  # latch table; if it wins, runs the RMW inline in its own process. If it
  # loses (someone else already holds the latch), falls through to the
  # per-shard RmwCoordinator GenServer which serializes via its mailbox.
  defp async_write(ctx, idx, {:incr, key, _delta} = cmd), do: async_rmw(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:incr_float, key, _delta} = cmd), do: async_rmw(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:append, key, _suffix} = cmd), do: async_rmw(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:getset, key, _new_value} = cmd), do: async_rmw(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:getdel, key} = cmd), do: async_rmw(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:getex, key, _exp} = cmd), do: async_rmw(ctx, idx, key, cmd)

  defp async_write(ctx, idx, {:setrange, key, _off, _value} = cmd),
    do: async_rmw(ctx, idx, key, cmd)

  # List ops are RMW at the structural level (LPUSH reads head pointer,
  # writes new element + new head). Same latch+worker pattern as plain RMW.
  # The latch is on the user-facing list key, serializing all list_ops on
  # that list.
  defp async_write(ctx, idx, {:list_op, key, _op} = cmd), do: async_list_op(ctx, idx, key, cmd)

  defp async_write(ctx, idx, {:list_op_lmove, src_key, dst_key, _from, _to} = cmd) do
    # Single-shard LMOVE goes async under both source and destination latches.
    # Cross-shard LMOVE never reaches here (Router.list_op for lmove already splits
    # across shards via quorum_write before calling async_write).
    async_list_lmove(ctx, idx, src_key, dst_key, cmd)
  end

  # Any other command in an async namespace — CAS, LOCK, UNLOCK, EXTEND,
  # RATELIMIT, and all prob commands (bloom/cuckoo/cms/topk) — needs its
  # computed result returned to the caller and must serialize via Raft
  # for correctness:
  #
  #   * CAS/LOCK/EXTEND/RATELIMIT are distributed-coordination primitives
  #     — their whole contract is linearizability.
  #   * Prob commands return values (count of bits newly set, list of
  #     evicted items, per-element counter deltas) computed by the state
  #     machine; they can't be fire-and-forget.
  #
  # We could not use `quorum_write` directly because it routes through
  # Shard → Batcher, and Batcher.enqueue_write replies `:ok` prematurely
  # when the namespace's durability is `:async` (correct for put/delete,
  # wrong for everything else). Use the Batcher's forced-quorum path
  # (`write_async_quorum`) which ignores namespace durability and puts
  # the command in the quorum slot regardless.
  defp async_write(ctx, idx, command) do
    forced_quorum_write(ctx, idx, command, node())
  end

  defp forced_quorum_write(ctx, idx, command, origin_node) do
    {from, token} = ReplyAwaiter.new()

    if origin_node == node() do
      Ferricstore.Raft.Batcher.write_async_quorum(idx, command, from)
    else
      Ferricstore.Raft.Batcher.write_async_quorum_forwarded(idx, command, from, origin_node)
    end

    result = ReplyAwaiter.await(token, 10_000, ErrorReasons.write_timeout_unknown())

    case result do
      # Local node isn't the leader for this shard. Forward via the same
      # path quorum_write uses so callers get a real result, not the
      # internal :not_leader error.
      {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
        forward_to_leader(ctx, leader_node, idx, command)

      {:error, _} ->
        result

      _ ->
        bump_write_version(ctx, idx)
        result
    end
  end

  defp bump_write_version(%{write_version: write_version}, idx) do
    size = :counters.info(write_version).size
    if idx < size, do: :counters.add(write_version, idx + 1, 1)
    :ok
  end

  defp bump_write_version(_ctx, _idx), do: :ok

  # ---------------------------------------------------------------------------
  # Async RMW: latch-first, worker fallback
  # ---------------------------------------------------------------------------

  # Latch-first dispatch for async RMW. Wins → run in caller process.
  # Loses → fall through to the shard's RmwCoordinator.
  #
  # See docs/async-rmw-design.md. All 7 RMW commands flow through here
  # (INCR, INCR_FLOAT, APPEND, GETSET, GETDEL, GETEX, SETRANGE).
  defp async_rmw(ctx, idx, key, cmd) do
    latch_tab = elem(ctx.latch_refs, idx)

    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        try do
          :telemetry.execute([:ferricstore, :rmw, :latch], %{}, %{shard_index: idx})
          execute_rmw_inline(ctx, idx, cmd)
        after
          :ets.take(latch_tab, key)
        end

      false ->
        # Fall through to the shard's RmwCoordinator. Use a direct
        # GenServer.call with the registered name rather than
        # `RmwCoordinator.execute/2`: RmwCoordinator calls back into
        # Router.execute_rmw_inline, and referencing it here would create
        # a compile-time dependency cycle (RmwCoordinator → Router →
        # RmwCoordinator). A runtime GenServer.call breaks the cycle.
        try do
          GenServer.call(
            :"Ferricstore.Store.RmwCoordinator.#{idx}",
            {:rmw, ctx, cmd},
            10_000
          )
        catch
          :exit, {:timeout, _} -> ErrorReasons.write_timeout_unknown()
          :exit, {:noproc, _} -> {:error, "ERR RMW worker unavailable"}
          :exit, _ -> {:error, "ERR RMW worker crashed"}
        end
    end
  end

  @doc """
  Executes an RMW command inline against local ETS + BitcaskWriter and
  submits the delta to Raft via `Batcher.async_submit`.

  **Called with the per-key latch held.** The latch guarantees exclusive
  access to `key`'s ETS row among RMW paths. `Router.async_rmw/4` (latch
  path) and `Ferricstore.Store.RmwCoordinator` (worker path) both call
  this after winning the latch.

  Returns the command's natural result shape (e.g. `{:ok, new_int}` for
  INCR, `old_value_or_nil` for GETSET/GETDEL).
  """
  @spec execute_rmw_inline(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  def execute_rmw_inline(ctx, idx, cmd) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      with :ok <- ensure_string_rmw_key(ctx, idx, cmd) do
        do_rmw_inline(ctx, idx, cmd)
      end
    end
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key, _arg}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(ctx, idx, {_op, key, _arg1, _arg2}) when is_binary(key) do
    ensure_string_rmw_key_name(ctx, idx, key)
  end

  defp ensure_string_rmw_key(_ctx, _idx, _cmd), do: :ok

  defp ensure_string_rmw_key_name(ctx, idx, key) do
    if compound_marker_present?(ctx, idx, key) do
      case TypeRegistry.get_type(key, ctx) do
        type when type in ["none", "string"] ->
          :ok

        _compound_type ->
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
      end
    else
      :ok
    end
  end

  defp compound_marker_present?(ctx, idx, key) do
    keydir = elem(ctx.keydir_refs, idx)

    :ets.lookup(keydir, CompoundKey.type_key(key)) != []
  end

  # Per-command RMW implementations. Mirror state_machine.ex do_incr et al.,
  # but operate on origin-local ETS + cast BitcaskWriter + submit a DELTA
  # command to the Batcher for replication.

  defp do_rmw_inline(ctx, idx, {:incr, key, delta}) do
    case read_live(ctx, idx, key) do
      :missing ->
        if delta > 9_223_372_036_854_775_807 or delta < -9_223_372_036_854_775_808 do
          {:error, "ERR increment or decrement would overflow"}
        else
          install_rmw_and_submit(
            ctx,
            idx,
            key,
            delta,
            0,
            {:incr, key, delta},
            nil,
            0,
            {:ok, delta}
          )
        end

      {:hit, value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta

            if new_val > 9_223_372_036_854_775_807 or new_val < -9_223_372_036_854_775_808 do
              {:error, "ERR increment or decrement would overflow"}
            else
              install_rmw_and_submit(
                ctx,
                idx,
                key,
                new_val,
                expire_at_ms,
                {:incr, key, delta},
                value,
                expire_at_ms,
                {:ok, new_val}
              )
            end

          :error ->
            {:error, "ERR value is not an integer or out of range"}
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:incr_float, key, delta}) do
    case read_live(ctx, idx, key) do
      :missing ->
        new_val = delta * 1.0

        install_rmw_and_submit(
          ctx,
          idx,
          key,
          new_val,
          0,
          {:incr_float, key, delta},
          nil,
          0,
          {:ok, new_val}
        )

      {:hit, value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta

            install_rmw_and_submit(
              ctx,
              idx,
              key,
              new_val,
              expire_at_ms,
              {:incr_float, key, delta},
              value,
              expire_at_ms,
              {:ok, new_val}
            )

          :error ->
            {:error, "ERR value is not a valid float"}
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:append, key, suffix}) do
    {old_val, before_value, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", nil, 0}
        {:hit, v, exp} -> {to_disk_binary(v), v, exp}
      end

    new_val = old_val <> suffix

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_val,
      expire_at_ms,
      {:append, key, suffix},
      before_value,
      expire_at_ms,
      {:ok, byte_size(new_val)}
    )
  end

  defp do_rmw_inline(ctx, idx, {:getset, key, new_value}) do
    {old, old_expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {nil, 0}
        {:hit, v, exp} -> {v, exp}
      end

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_value,
      0,
      {:getset, key, new_value},
      old,
      old_expire_at_ms,
      old
    )
  end

  defp do_rmw_inline(ctx, idx, {:getdel, key}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, exp} ->
        previous = {:value, v, exp}
        raft_cmd = origin_checked_command(key, {:getdel, key}, previous, nil, 0)

        with :ok <- async_submit_to_raft(idx, raft_cmd) do
          keydir = elem(ctx.keydir_refs, idx)
          flush_pending_writer_for_key(ctx, idx, keydir, key)
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)

          wv_size = :counters.info(ctx.write_version).size
          if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

          v
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:getex, key, new_expire_at_ms}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, old_expire_at_ms} ->
        install_rmw_and_submit(
          ctx,
          idx,
          key,
          v,
          new_expire_at_ms,
          {:getex, key, new_expire_at_ms},
          v,
          old_expire_at_ms,
          v
        )
    end
  end

  defp do_rmw_inline(ctx, idx, {:setrange, key, offset, value}) do
    {old_val, before_value, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", nil, 0}
        {:hit, v, exp} -> {to_disk_binary(v), v, exp}
      end

    new_val = apply_setrange(old_val, offset, value)

    install_rmw_and_submit(
      ctx,
      idx,
      key,
      new_val,
      expire_at_ms,
      {:setrange, key, offset, value},
      before_value,
      expire_at_ms,
      {:ok, byte_size(new_val)}
    )
  end

  defp install_rmw_and_submit(
         ctx,
         idx,
         key,
         value,
         expire_at_ms,
         raft_cmd,
         before_value,
         before_expire_at_ms,
         success
       ) do
    checked_cmd =
      {:origin_checked, key, raft_cmd, origin_check_value(before_value), before_expire_at_ms,
       origin_check_value(value), expire_at_ms}

    if large_value_for_hot_cache?(ctx, value) do
      install_large_rmw_and_submit(ctx, idx, key, value, expire_at_ms, checked_cmd, success)
    else
      with :ok <- async_enqueue_to_raft(idx, checked_cmd),
           :ok <- install_rmw_value(ctx, idx, key, value, expire_at_ms) do
        success
      end
    end
  end

  defp install_large_rmw_and_submit(ctx, idx, key, value, expire_at_ms, checked_cmd, success) do
    keydir = elem(ctx.keydir_refs, idx)
    previous = snapshot_live_value(ctx, idx, key)
    disk_value = to_disk_binary(value)

    case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
      {:ok, file_id, [{offset, _record_size}]} ->
        case async_submit_to_raft(idx, checked_cmd) do
          :ok ->
            track_keydir_binary_insert(ctx, idx, keydir, key, nil)

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
            )

            wv_size = :counters.info(ctx.write_version).size
            if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
            success

          {:error, _} = err ->
            rollback_unaccepted_large_put(ctx, idx, key, previous)
            err
        end

      {:error, reason} ->
        {:error, "ERR disk write failed: #{inspect(reason)}"}
    end
  end

  defp large_value_for_hot_cache?(ctx, value) when is_binary(value),
    do: byte_size(value) > ctx.hot_cache_max_value_size

  defp large_value_for_hot_cache?(_ctx, _value), do: false

  defp origin_check_value(value) when is_integer(value), do: Integer.to_string(value)
  defp origin_check_value(value) when is_float(value), do: Float.to_string(value)
  defp origin_check_value(value), do: value

  # Read the live value for a key (treating expired TTL as missing).
  defp read_live(ctx, idx, key) do
    keydir = elem(ctx.keydir_refs, idx)
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, value, exp, _, _, _, _}]
      when value != nil and (exp == 0 or exp > now) ->
        {:hit, value, exp}

      [{^key, nil, exp, _, file_id, offset, value_size}]
      when (exp == 0 or exp > now) and valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)
        original_location = {file_id, offset, value_size}

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {:hit, value, exp}

          _ ->
            case retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
              {:hot, value, retry_expire_at_ms} ->
                {:hit, value, retry_expire_at_ms}

              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {:hit, value, retry_expire_at_ms}

              :miss ->
                :missing
            end
        end

      [{^key, _value, _exp, _, _, _, _}] ->
        track_keydir_binary_delete(ctx, idx, keydir, key)
        :ets.delete(keydir, key)
        :missing

      _ ->
        :missing
    end
  end

  # Write the new RMW value into ETS, cast BitcaskWriter for disk, bump
  # write_version. Matches the shape of async_write_put for small values.
  defp install_rmw_value(ctx, idx, key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)

    value_for_ets =
      case value do
        v when is_integer(v) ->
          Integer.to_string(v)

        v when is_float(v) ->
          Float.to_string(v)

        v when is_binary(v) ->
          if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
      end

    disk_value = to_disk_binary(value)

    result =
      if value_for_ets == nil do
        # Large — sync NIF write then ETS with real offset.
        case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
          {:ok, file_id, [{offset, _record_size}]} ->
            track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

            :ets.insert(
              keydir,
              {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
            )

            :ok

          {:error, reason} ->
            {:error, "ERR disk write failed: #{inspect(reason)}"}
        end
      else
        {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)
        track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
        :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})

        Ferricstore.Store.BitcaskWriter.write(
          ctx,
          idx,
          file_path,
          file_id,
          keydir,
          key,
          disk_value,
          expire_at_ms
        )

        :ok
      end

    if result == :ok do
      wv_size = :counters.info(ctx.write_version).size
      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
      :ok
    else
      result
    end
  end

  defp flush_pending_writer_for_key(ctx, idx, keydir, key) do
    case :ets.lookup(keydir, key) do
      [{^key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}] ->
        Ferricstore.Store.BitcaskWriter.flush(ctx, idx)

      _ ->
        :ok
    end
  end

  # Coerce a stored value (integer, float, binary-digits) to an integer.
  defp coerce_integer(v) when is_integer(v), do: {:ok, v}
  defp coerce_integer(v) when is_float(v), do: :error

  defp coerce_integer(v) when is_binary(v) do
    case Integer.parse(v) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  # Coerce a stored value to a float (ints upcast).
  defp coerce_float(v) when is_float(v), do: {:ok, v}
  defp coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}

  defp coerce_float(v) when is_binary(v) do
    ValueCodec.parse_float(v)
  end

  # SETRANGE helper: overwrite bytes at offset, zero-padding if needed.
  defp apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len,
          do: old <> :binary.copy(<<0>>, offset - old_len),
          else: old

      offset > old_len ->
        old <> :binary.copy(<<0>>, offset - old_len) <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  defp async_write_put(ctx, idx, key, value, expire_at_ms) do
    with_async_key_latch(ctx, idx, key, fn ->
      do_async_write_put(ctx, idx, key, value, expire_at_ms)
    end)
  end

  defp do_async_write_put(ctx, idx, key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)
    previous = snapshot_live_value(ctx, idx, key)

    raft_cmd =
      origin_checked_command(key, {:put, key, value, expire_at_ms}, previous, value, expire_at_ms)

    value_for_ets =
      case value do
        v when is_integer(v) ->
          Integer.to_string(v)

        v when is_float(v) ->
          Float.to_string(v)

        v when is_binary(v) ->
          if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
      end

    disk_value = to_disk_binary(value)

    if value_for_ets == nil do
      # Large value: sync NIF write to get offset, then ETS with real location.
      # Cannot use async BitcaskWriter because ETS value is nil (too large for
      # hot cache) and readers would see nil until the async write completes.
      case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
        {:ok, file_id, [{offset, _record_size}]} ->
          maybe_after_large_async_prewrite_hook(ctx, idx, key)

          case async_enqueue_to_raft(idx, raft_cmd) do
            :ok ->
              clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)
              track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

              :ets.insert(
                keydir,
                {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
              )

              size = :counters.info(ctx.write_version).size
              if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)

              :ok

            {:error, _} = err ->
              rollback_unaccepted_large_put(ctx, idx, key, previous)
              err
          end

        {:error, reason} ->
          {:error, "ERR disk write failed: #{inspect(reason)}"}
      end
    else
      # Small value: ETS insert only. Bitcask write deferred to state machine
      # apply (flush_pending_writes) — avoids per-key NIF overhead in Router.
      with :ok <- async_enqueue_to_raft(idx, raft_cmd) do
        clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)
        track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
        :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})
        size = :counters.info(ctx.write_version).size
        if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
        :ok
      end
    end
  end

  defp snapshot_live_value(ctx, idx, key) do
    case read_live(ctx, idx, key) do
      {:hit, value, expire_at_ms} -> {:value, value, expire_at_ms}
      :missing -> :missing
    end
  end

  defp origin_checked_command(key, inner_cmd, previous, expected_value, expire_at_ms) do
    {before_value, before_expire_at_ms} =
      case previous do
        {:value, value, exp} -> {origin_check_value(value), exp}
        :missing -> {nil, 0}
      end

    {:origin_checked, key, inner_cmd, before_value, before_expire_at_ms,
     origin_check_value(expected_value), expire_at_ms}
  end

  defp rollback_unaccepted_large_put(ctx, idx, key, {:value, previous_value, expire_at_ms}) do
    disk_value = to_disk_binary(previous_value)
    _ = nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}])
    :ok
  end

  defp rollback_unaccepted_large_put(ctx, idx, key, :missing) do
    _ = append_delete_tombstone_nosync(ctx, idx, key)
    :ok
  end

  defp rollback_installed_async_value(ctx, idx, key, {:value, previous_value, expire_at_ms}) do
    keydir = elem(ctx.keydir_refs, idx)
    disk_value = to_disk_binary(previous_value)

    case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
      {:ok, file_id, [{offset, _record_size}]} ->
        ets_value =
          case previous_value do
            v when is_integer(v) ->
              Integer.to_string(v)

            v when is_float(v) ->
              Float.to_string(v)

            v when is_binary(v) ->
              if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
          end

        track_keydir_binary_insert(ctx, idx, keydir, key, ets_value)

        :ets.insert(
          keydir,
          {key, ets_value, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
        )

        maybe_apply_async_zset_put(ctx, idx, key, disk_value)

      {:error, _reason} ->
        :ok
    end
  end

  defp rollback_installed_async_value(ctx, idx, key, :missing) do
    keydir = elem(ctx.keydir_refs, idx)
    _ = append_delete_tombstone_nosync(ctx, idx, key)
    Ferricstore.Store.BitcaskWriter.discard_pending(ctx, idx, key)
    track_keydir_binary_delete(ctx, idx, keydir, key)
    :ets.delete(keydir, key)
    maybe_apply_async_zset_delete(ctx, idx, key)
    :ok
  end

  defp install_async_put_value(ctx, idx, key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)
    value_for_ets = value_for_hot_cache(ctx, value)
    disk_value = to_disk_binary(value)

    if value_for_ets == nil do
      case nif_append_batch_with_file(ctx, idx, [{key, disk_value, expire_at_ms}]) do
        {:ok, file_id, [{offset, _record_size}]} ->
          track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

          :ets.insert(
            keydir,
            {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)}
          )

          maybe_apply_async_zset_put(ctx, idx, key, disk_value)
          :ok

        {:error, reason} ->
          {:error, "ERR disk write failed: #{inspect(reason)}"}
      end
    else
      track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
      :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})
      maybe_apply_async_zset_put(ctx, idx, key, disk_value)
      :ok
    end
  end

  defp maybe_apply_async_zset_put(ctx, idx, compound_key, score_str) do
    with {:ok, redis_key} <- zset_redis_key(compound_key),
         {:ok, index, lookup} <- zset_index_tables(ctx, idx) do
      ZSetIndex.apply_put_to_tables(index, lookup, redis_key, compound_key, score_str)
    end

    :ok
  end

  defp maybe_apply_async_zset_delete(ctx, idx, compound_key) do
    with {:ok, redis_key} <- zset_redis_key(compound_key),
         {:ok, index, lookup} <- zset_index_tables(ctx, idx) do
      ZSetIndex.apply_delete_to_tables(index, lookup, redis_key, compound_key)
    end

    :ok
  end

  defp zset_redis_key(compound_key) do
    redis_key = CompoundKey.extract_redis_key(compound_key)

    if String.starts_with?(compound_key, CompoundKey.zset_prefix(redis_key)) do
      {:ok, redis_key}
    else
      :error
    end
  end

  defp zset_index_tables(ctx, idx) do
    {index, lookup} = ZSetIndex.table_names(ctx.name, idx)

    if :ets.info(index) != :undefined and :ets.info(lookup) != :undefined do
      {:ok, index, lookup}
    else
      :error
    end
  end

  defp with_async_key_latch(ctx, idx, key, fun) do
    case acquire_async_key_latches(ctx, [{idx, key}]) do
      {:ok, [{latch_tab, ^key}]} ->
        try do
          fun.()
        after
          release_async_key_latches([{latch_tab, key}])
        end

      {:error, {:timeout, wait_ms}} ->
        async_key_latch_timeout_error(wait_ms)
    end
  end

  defp acquire_async_key_latches(ctx, locks) do
    result =
      locks
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.reduce_while([], fn {idx, key}, held ->
        latch_tab = elem(ctx.latch_refs, idx)

        case wait_for_async_key_latch(latch_tab, idx, key) do
          :ok ->
            {:cont, [{latch_tab, key} | held]}

          {:error, reason} ->
            release_async_key_latches(held)
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:error, _reason} = error -> error
      held -> {:ok, Enum.reverse(held)}
    end
  end

  defp try_acquire_async_key_latches(ctx, locks) do
    locks = locks |> Enum.uniq() |> Enum.sort()

    case Enum.reduce_while(locks, [], fn {idx, key}, held ->
           latch_tab = elem(ctx.latch_refs, idx)

           if :ets.insert_new(latch_tab, {key, self()}) do
             {:cont, [{latch_tab, key} | held]}
           else
             {:halt, {:error, held}}
           end
         end) do
      {:error, held} ->
        release_async_key_latches(held)
        :error

      held ->
        {:ok, Enum.reverse(held)}
    end
  end

  defp release_async_key_latches(held_latches) do
    Enum.each(held_latches, fn {latch_tab, key} ->
      :ets.select_delete(latch_tab, [{{key, self()}, [], [true]}])
    end)
  end

  defp wait_for_async_key_latch(latch_tab, idx, key) do
    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        :ok

      false ->
        emit_async_key_latch_event(:blocked, idx, key, 0)

        wait_for_async_key_latch(
          latch_tab,
          idx,
          key,
          System.monotonic_time(:millisecond),
          async_key_latch_timeout_ms()
        )
    end
  end

  defp wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms) do
    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        :ok

      false ->
        wait_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

        if wait_ms >= timeout_ms do
          emit_async_key_latch_event(:timeout, idx, key, wait_ms)
          {:error, {:timeout, wait_ms}}
        else
          wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms)
        end
    end
  end

  defp wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms) do
    case :ets.lookup(latch_tab, key) do
      [{^key, holder}] when is_pid(holder) ->
        if Process.alive?(holder) do
          latch_retry_backoff()
        else
          :ets.select_delete(latch_tab, [{{key, holder}, [], [true]}])
        end

      _ ->
        latch_retry_backoff()
    end

    wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms)
  end

  defp async_key_latch_timeout_ms do
    Application.get_env(
      :ferricstore,
      :router_async_key_latch_timeout_ms,
      @default_async_key_latch_timeout_ms
    )
  end

  defp async_key_latch_timeout_error(wait_ms) do
    {:error, async_key_latch_timeout_error_message(wait_ms)}
  end

  defp async_key_latch_timeout_error_message(wait_ms),
    do: "ERR async key latch timeout after #{wait_ms}ms"

  defp emit_async_key_latch_event(status, idx, key, wait_ms) do
    :telemetry.execute(
      [:ferricstore, :store, :async_key_latch],
      %{count: 1, wait_ms: wait_ms},
      %{status: status, shard_index: idx, redis_key_hash: :erlang.phash2(key)}
    )
  end

  defp latch_retry_backoff do
    receive do
    after
      1 -> :ok
    end
  end

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  defp value_for_hot_cache(_ctx, value) when is_integer(value), do: Integer.to_string(value)
  defp value_for_hot_cache(_ctx, value) when is_float(value), do: Float.to_string(value)

  defp value_for_hot_cache(ctx, value) when is_binary(value) do
    if byte_size(value) > ctx.hot_cache_max_value_size, do: nil, else: value
  end

  defp clear_compound_data_structure_for_string_put(ctx, idx, keydir, key) do
    if CompoundKey.internal_key?(key) do
      :ok
    else
      case compound_marker_for_string_put(ctx, idx, keydir, key) do
        :none ->
          :ok

        {:type, type_key, type} ->
          {_, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)
          clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, type)
          delete_local_key(ctx, idx, keydir, file_path, type_key)
      end
    end
  end

  defp compound_marker_for_string_put(ctx, idx, keydir, key) do
    type_key = CompoundKey.type_key(key)

    case live_compound_marker(ctx, idx, keydir, type_key) do
      {:ok, type} ->
        {:type, type_key, type}

      :none ->
        :none
    end
  end

  # Keep ordinary string SET on the cheap path: a missing marker needs only
  # direct ETS lookup, while a present marker still uses origin_compound_get/4
  # so stale/expired/cold markers are handled correctly before clearing.
  defp live_compound_marker(ctx, idx, keydir, marker_key) do
    case :ets.lookup(keydir, marker_key) do
      [] ->
        :none

      _ ->
        case origin_compound_get(ctx, idx, keydir, marker_key) do
          nil -> :none
          marker -> {:ok, marker}
        end
    end
  end

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "hash"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.hash_prefix(key))

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "list") do
    delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.list_prefix(key))
    delete_local_key(ctx, idx, keydir, file_path, CompoundKey.list_meta_key(key))
  end

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "set"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.set_prefix(key))

  defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "zset"),
    do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.zset_prefix(key))

  defp clear_compound_prefix_for_string_put(_ctx, _idx, _keydir, _file_path, _key, _type),
    do: :ok

  defp delete_local_prefix(ctx, idx, keydir, file_path, prefix) do
    keydir
    |> Ferricstore.Store.Shard.ETS.prefix_collect_keys(prefix)
    |> Enum.each(fn compound_key ->
      delete_local_key(ctx, idx, keydir, file_path, compound_key)
    end)
  end

  defp delete_local_key(ctx, idx, keydir, file_path, key) do
    track_keydir_binary_delete(ctx, idx, keydir, key)
    :ets.delete(keydir, key)
    Ferricstore.Store.BitcaskWriter.delete(ctx, idx, file_path, key)
  end

  # NIF batch write with retry on stale active file (ENOENT after rotation).
  # Returns {:ok, file_id, locations} or {:error, reason}.
  defp nif_append_batch_with_file(ctx, idx, batch) do
    {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(file_path, batch) do
      {:ok, locations} ->
        mark_checkpoint_dirty(ctx, idx)
        {:ok, file_id, locations}

      {:error, reason} when is_binary(reason) ->
        if String.contains?(reason, "No such file") do
          {fresh_id, fresh_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

          case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(fresh_path, batch) do
            {:ok, locations} ->
              mark_checkpoint_dirty(ctx, idx)
              {:ok, fresh_id, locations}

            {:error, _} = err ->
              err
          end
        else
          {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

  defp mark_checkpoint_dirty(%{checkpoint_flags: flags}, idx) when is_integer(idx) do
    flag_idx = idx + 1

    if flag_idx <= :atomics.info(flags).size do
      :atomics.put(flags, flag_idx, 1)
    end
  rescue
    _ -> :ok
  end

  defp mark_checkpoint_dirty(_ctx, _idx), do: :ok

  defp append_delete_tombstone_nosync(ctx, idx, key) do
    {_, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    case Ferricstore.Bitcask.NIF.v2_append_ops_batch_nosync(file_path, [{:delete, key}]) do
      {:ok, _locations} = ok ->
        mark_checkpoint_dirty(ctx, idx)
        ok

      other ->
        other
    end
  end

  defp maybe_after_large_async_prewrite_hook(ctx, idx, key) do
    case Process.get(:ferricstore_router_after_large_async_prewrite_hook) do
      fun when is_function(fun, 3) -> fun.(ctx, idx, key)
      _ -> :ok
    end
  end

  # -- Keydir binary memory tracking --
  # Only counts binaries > 64 bytes (refc binaries, off-heap).
  # Smaller binaries are inlined in the ETS tuple and counted by :ets.info(:memory).

  defp track_keydir_binary_insert(ctx, idx, keydir, key, new_val) do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case :ets.lookup(keydir, key) do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ctx.keydir_binary_bytes, idx + 1, delta)
  end

  defp track_keydir_binary_delete(ctx, idx, keydir, key) do
    bytes =
      case :ets.lookup(keydir, key) do
        [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
        _ -> 0
      end

    if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
  end

  defp track_keydir_binary_delete_known(ctx, idx, key, value) do
    bytes = offheap_size(key) + offheap_size(value)
    if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
  end

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0

  defp stored_value_size(value) when is_binary(value), do: byte_size(value)
  defp stored_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp stored_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  defp stored_value_size(value), do: value |> to_string() |> byte_size()

  # Stronger async boundary for flows that need Ra submit before publishing a
  # local effect. Plain async SET/DEL use async_enqueue_to_raft/2 below so
  # their latency is not tied to the namespace batch window.
  defp async_submit_to_raft(idx, command) do
    case Ferricstore.Raft.Batcher.async_submit_ordered(idx, command) do
      :ok -> :ok
      {:error, :overloaded} -> {:error, "ERR async replication overloaded"}
      {:error, reason} -> {:error, "ERR async replication failed: #{inspect(reason)}"}
    end
  end

  # Latency-critical async writes wait only for local Batcher acceptance. That
  # preserves async semantics while still surfacing local overload/down errors.
  defp async_enqueue_to_raft(idx, command) do
    case Ferricstore.Raft.Batcher.async_enqueue_ordered(idx, command) do
      :ok -> :ok
      {:error, :overloaded} -> {:error, "ERR async replication overloaded"}
      {:error, reason} -> {:error, "ERR async replication failed: #{inspect(reason)}"}
    end
  end

  defp async_submit_batch_to_raft(_idx, []), do: :ok

  defp async_submit_batch_to_raft(idx, commands) do
    case Ferricstore.Raft.Batcher.async_submit_batch_ordered(idx, commands) do
      :ok -> :ok
      {:error, :overloaded} -> {:error, "ERR async replication overloaded"}
      {:error, reason} -> {:error, "ERR async replication failed: #{inspect(reason)}"}
    end
  end

  # -------------------------------------------------------------------
  # Routing helpers
  # -------------------------------------------------------------------

  @doc """
  Returns the slot (0-1023) for a key, respecting hash tags.
  """
  @spec slot_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def slot_for(_ctx, key) do
    hash_input = extract_hash_tag(key) || key
    :erlang.phash2(hash_input) |> band(@slot_mask)
  end

  @doc """
  Returns the shard index (0-based) that owns `key`.

  Routes through the 1,024-slot indirection layer:
  `key -> phash2(key) & 0x3FF -> slot -> slot_map[slot] -> shard_index`

  Supports Redis hash tags: if the key contains `{tag}` (non-empty content
  between the first `{` and the next `}`), the tag is used for hashing
  instead of the full key.
  """
  @spec shard_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def shard_for(ctx, key) do
    slot = slot_for(ctx, key)
    elem(ctx.slot_map, slot)
  end

  @doc """
  Extracts the hash tag from a key, following Redis hash tag semantics.

  If the key contains a substring enclosed in `{...}` where the content
  between the first `{` and the next `}` is non-empty, that substring is
  used for hashing instead of the full key. This allows related keys to
  be routed to the same shard.

  ## Examples

      iex> Ferricstore.Store.Router.extract_hash_tag("{user:42}:session")
      "user:42"

      iex> Ferricstore.Store.Router.extract_hash_tag("no_tag")
      nil

      iex> Ferricstore.Store.Router.extract_hash_tag("{}empty")
      nil

  """
  @spec extract_hash_tag(binary()) :: binary() | nil
  def extract_hash_tag(key) do
    case :binary.match(key, "{") do
      {start, 1} ->
        rest_start = start + 1
        rest_len = byte_size(key) - rest_start

        case :binary.match(key, "}", [{:scope, {rest_start, rest_len}}]) do
          {end_pos, 1} when end_pos > rest_start ->
            binary_part(key, rest_start, end_pos - rest_start)

          _ ->
            nil
        end

      :nomatch ->
        nil
    end
  end

  @doc """
  Returns the registered process name for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec shard_name(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  def shard_name(ctx, index), do: elem(ctx.shard_names, index)

  @doc """
  Returns the keydir ETS table ref for the shard at `index`.

  Uses the pre-computed tuple from the instance context for O(1) lookup.
  """
  @spec keydir_name(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
  def keydir_name(ctx, index), do: elem(ctx.keydir_refs, index)

  # -------------------------------------------------------------------
  # Convenience accessors (dispatch to correct shard)
  # -------------------------------------------------------------------

  @doc """
  Returns the on-disk file reference for a key's value, or `nil`.

  Used by the sendfile optimisation in standalone TCP mode. Returns
  `{file_path, value_byte_offset, value_size}` for cold (on-disk) keys.
  Returns `nil` for hot keys (ETS), expired keys, or missing keys --
  the caller should fall back to the normal read path.

  Only cold keys benefit from sendfile: hot keys are already in BEAM memory
  and would need a normal `get` + `transport.send`.
  """
  @spec get_file_ref(FerricStore.Instance.t(), binary()) ::
          {binary(), non_neg_integer(), non_neg_integer()} | nil
  def get_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        # Hot key — value is in ETS, sendfile not applicable.
        nil

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            {path, value_offset, value_size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                {retry_path, value_offset, retry_size}

              _ ->
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — fall back to GenServer.
        case safe_read_call(ctx, idx, {:get_file_ref, key}) do
          {:ok, result} -> result
          :unavailable -> nil
        end

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key doesn't exist. No GenServer needed.
        nil

      :no_table ->
        nil
    end
  end

  @doc """
  Unified GET that returns everything from a single ETS lookup.

  Returns:
    - `{:hot, value}` — value is in ETS, ready to return
    - `{:cold_ref, path, offset, size}` — value is on disk, file ref for sendfile
    - `{:cold_value, value}` — value was on disk, GenServer fetched it
    - `:miss` — key doesn't exist
  """
  @spec get_with_file_ref(FerricStore.Instance.t(), binary()) ::
          {:hot, binary()}
          | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
          | {:cold_value, binary()}
          | :miss
  def get_with_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            Stats.record_cold_read(ctx, key)
            {:cold_ref, path, value_offset, value_size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                Stats.record_cold_read(ctx, key)
                {:cold_ref, retry_path, value_offset, retry_size}

              {:hot, value} ->
                {:hot, value}

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                :miss
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but no valid file ref — ask GenServer
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        :miss

      :miss ->
        # Key not in ETS = doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        :miss

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end
    end
  end

  defp validated_file_ref(path, record_offset, key, value_size) do
    case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
      {:ok, {value_offset, ^value_size}} ->
        {path, value_offset, value_size}

      _ ->
        maybe_run_validate_file_ref_miss_hook()
        nil
    end
  end

  defp retry_changed_file_ref(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :file_ref)
        )

      result ->
        result
    end
  end

  defp retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} -> {:cold_ref, path, value_offset, value_size}
          nil -> :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp cold_file_path(ctx, idx, file_id) do
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  defp maybe_run_validate_file_ref_miss_hook do
    case Process.get(:ferricstore_router_validate_file_ref_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
  # Single lookup provides everything needed — no second ETS read for bookkeeping.
  defp ets_get_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          {:hit, value, lfu}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, 0, _lfu, :pending, off, vsize}] ->
          {:cold, :pending, off, vsize}

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:hit, value, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp > now ->
          {:cold, :pending, off, vsize}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  defp ets_get_meta_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, lfu, _fid, _off, _vsize}]
        when value != nil and (exp == 0 or exp > now) ->
          {:hit, value, exp, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp == 0 or exp > now ->
          {:cold, :pending, off, vsize, exp}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  @doc """
  Retrieves the value for `key`, or `nil` if the key does not exist or is
  expired.

  Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
  Falls back to a GenServer call for cache misses or when the ETS table is
  temporarily unavailable (e.g. during a shard restart).

  Each successful read is recorded as either *hot* (ETS hit) or *cold*
  (Bitcask fallback) in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS`
  command and the `INFO stats` hot/cold fields.
  """
  @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil
  def get(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — value evicted from ETS but disk location known.
        # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
        # The ETS entry has valid file_id/offset from when the write committed,
        # so pread works without flushing pending async writes.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            # Warm ETS: promote back to hot if value fits in cache
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            value

          _ ->
            case retry_changed_cold_value(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                value

              {:hot, value} ->
                value

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        # Key not in ETS at all — doesn't exist. No GenServer needed.
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result
    end
  end

  @spec batch_get(FerricStore.Instance.t(), [binary()]) :: [binary() | nil]
  def batch_get(ctx, keys) do
    now = HLC.now_ms()

    {results, {cold_entries, _cold_count}} =
      Enum.map_reduce(keys, {[], 0}, fn key, {cold_entries, cold_count} ->
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)

        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
            {{:value, value}, {cold_entries, cold_count}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1}}

          {:cold, _file_id, _offset, _value_size} ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}

          :expired ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            {{:value, nil}, {cold_entries, cold_count}}

          :no_table ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              Stats.incr_keyspace_misses(ctx)
            end

            {{:value, result}, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_cold_batch_async(now)
      |> List.to_tuple()

    Enum.map(results, fn
      {:value, value} -> value
      {:cold, index} -> elem(cold_values, index)
    end)
  end

  defp read_cold_batch_async([], _now), do: []

  defp read_cold_batch_async(entries, now) do
    {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)
    unique_values = read_unique_cold_batch_async(unique_entries, now) |> List.to_tuple()

    Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
  end

  defp dedupe_cold_batch_entries(entries) do
    {unique_entries, _index_by_location, value_indexes} =
      Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
        location = cold_batch_entry_location(entry)

        case Map.fetch(index_acc, location) do
          {:ok, index} ->
            {unique_acc, index_acc, [index | value_index_acc]}

          :error ->
            index = map_size(index_acc)
            {[entry | unique_acc], Map.put(index_acc, location, index), [index | value_index_acc]}
        end
      end)

    {Enum.reverse(unique_entries), Enum.reverse(value_indexes)}
  end

  defp cold_batch_entry_location({_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size}) do
    {path, offset, key}
  end

  defp read_unique_cold_batch_async(entries, now) do
    locations =
      Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
        {path, offset, key}
      end)

    values =
      case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    entry_values = Enum.zip(entries, values)

    corrupt_by_path =
      Enum.reduce(entry_values, %{}, fn
        {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
        corrupt_by_path
        when is_binary(value) ->
          corrupt_by_path

        {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
        corrupt_by_path ->
          reason = cold_batch_read_error_reason(value)
          Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
      end)

    emit_batch_cold_read_corruption(corrupt_by_path)

    Enum.map(entry_values, fn
      {{ctx, idx, keydir, key, _path, file_id, offset, _value_size}, value}
      when is_binary(value) ->
        Stats.record_cold_read(ctx, key)
        warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
        value

      {{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value} ->
        case retry_changed_cold_value(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            value

          {:hot, value} ->
            value

          :miss ->
            Stats.incr_keyspace_misses(ctx)
            nil
        end
    end)
  end

  defp router_pread_batch_keyed(locations, timeout_ms) do
    case Process.get(:ferricstore_router_pread_batch_keyed_result) do
      nil -> Ferricstore.Store.ColdRead.pread_batch_keyed(locations, timeout_ms)
      forced_result -> forced_result
    end
  end

  defp emit_batch_cold_read_corruption(corrupt_by_path) when map_size(corrupt_by_path) == 0,
    do: :ok

  defp emit_batch_cold_read_corruption(corrupt_by_path) do
    Enum.each(corrupt_by_path, fn {{path, reason}, count} ->
      :telemetry.execute(
        [:ferricstore, :bitcask, :pread_corrupt],
        %{count: count},
        %{path: path, reason: reason}
      )
    end)
  end

  defp cold_batch_read_error_reason({:error, reason}) when is_binary(reason) do
    if String.contains?(reason, "missing_file"), do: :missing_file, else: :corrupt_record
  end

  defp cold_batch_read_error_reason({:error, reason}) when reason in [:missing_file, :enoent],
    do: :missing_file

  defp cold_batch_read_error_reason({:error, :timeout}), do: :timeout

  defp cold_batch_read_error_reason({:error, :batch_result_length_mismatch}),
    do: :batch_result_length_mismatch

  defp cold_batch_read_error_reason({:error, _reason}), do: :corrupt_record

  defp cold_batch_read_error_reason(_value), do: :nil_from_cold_location

  defp read_cold_async(path, offset, expected_key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, expected_key, @cold_batch_read_timeout_ms)
  end

  defp retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :value)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :meta)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, expire_at_ms, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size, _expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata) when is_function(retry_fun, 0) do
    retry_after_unchanged_cold_location(retry_fun, metadata, @cold_location_retry_attempts)
  end

  defp retry_after_unchanged_cold_location(_retry_fun, metadata, 0) do
    emit_cold_retry_exhausted(metadata)
    :miss
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left) do
    maybe_run_cold_location_miss_hook()
    Process.sleep(@cold_location_retry_sleep_ms)

    case retry_fun.() do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left - 1)

      result ->
        result
    end
  end

  defp cold_retry_metadata(ctx, idx, key, operation) do
    %{
      instance: ctx.name,
      shard_index: idx,
      operation: operation,
      reason: :unchanged_cold_location,
      redis_key_hash: :erlang.phash2(key)
    }
  end

  defp emit_cold_retry_exhausted(nil), do: :ok

  defp emit_cold_retry_exhausted(metadata) do
    :telemetry.execute(
      [:ferricstore, :store, :cold_read_retry_exhausted],
      %{count: 1, attempts: @cold_location_retry_attempts},
      metadata
    )
  end

  defp maybe_run_cold_location_miss_hook do
    case Process.get(:ferricstore_router_cold_location_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  @doc """
  Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
  not exist or is expired.

  Hot path: reads directly from ETS for cached keys. Each read is recorded
  as hot or cold in `Ferricstore.Stats`.
  """
  @spec get_meta(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — read value from disk directly, return with expire_at_ms.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            case retry_changed_cold_meta(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {value, retry_expire_at_ms}

              {:hot, value, retry_expire_at_ms} ->
                {value, retry_expire_at_ms}

              :miss ->
                Stats.incr_keyspace_misses(ctx)
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size, _expire_at_ms} ->
        # Invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          Stats.incr_keyspace_misses(ctx)
        end

        result
    end
  end

  @doc """
  Returns the expiry timestamp for a live plain key without reading its value.

  This is used by expiry-time commands so cold large values do not pay a
  Bitcask pread just to report TTL metadata.
  """
  @spec expire_at_ms(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def expire_at_ms(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          0

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_cold_location(fid, off, vsize) ->
          0

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          0

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          exp

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          exp

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          exp

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :expire_at_ms, nil)
    end
  end

  @doc """
  Returns the live plain key value size without reading a cold value.

  Hot entries use the in-memory value size; cold entries use the keydir
  `value_size` field populated by Bitcask append/recovery.
  """
  @spec value_size(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def value_size(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          stored_value_size(value)

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          vsize

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          vsize

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          stored_value_size(value)

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          vsize

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          vsize

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :value_size, nil)
    end
  end

  # Sampling rate for read-side bookkeeping (LFU touch + hot/cold stats).
  # 1 in N reads performs the ETS writes. Reduces write contention at high
  # concurrency with negligible impact on LFU accuracy (logarithmic counter)
  # and stats precision (ratio stays the same).
  # Default 100 = sample 1 in 100 reads. Set to 1 to disable sampling.

  # LFU counter already available from the initial ets_get_full lookup.
  # Eliminates the second ETS lookup that sampled_read_bookkeeping does.
  defp sampled_read_bookkeeping_fast(ctx, keydir, key, lfu) do
    rate = ctx.read_sample_rate

    if rate <= 1 or :rand.uniform(rate) == 1 do
      Stats.incr_keyspace_hits(ctx)
      LFU.touch(ctx, keydir, key, lfu)
      Stats.record_hot_read(ctx, key)
    end
  end

  # After a cold read, promote the value back to ETS (hot) if it fits
  # under the hot cache max value size threshold. ETS is :public with
  # write_concurrency so this is safe from any process.
  @doc false
  def warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset) do
    warm_ets_after_cold_read(ctx, keydir_index(ctx, keydir), keydir, key, value, file_id, offset)
  end

  @doc false
  def warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset) do
    # Skip promotion when under memory pressure — prevents evict/re-promote
    # thrashing where MemoryGuard evicts values and cold reads immediately
    # re-cache them. skip_promotion? is set at :pressure level (85%+).
    skip_promotion = :atomics.get(ctx.pressure_flags, 3) == 1

    if byte_size(value) <= ctx.hot_cache_max_value_size and not skip_promotion do
      lfu = LFU.initial()

      try do
        replaced =
          :ets.select_replace(keydir, [
            {
              {key, nil, :"$1", :"$2", file_id, offset, :"$3"},
              [],
              [{{key, value, :"$1", lfu, file_id, offset, :"$3"}}]
            }
          ])

        if replaced > 0 do
          track_keydir_binary_warm(ctx, idx, value)
        end

        :ok
      rescue
        ArgumentError -> :ok
      end
    end
  end

  defp keydir_index(%{keydir_refs: refs, shard_count: count}, keydir)
       when is_tuple(refs) and is_integer(count) and count > 0 do
    Enum.find(0..(count - 1), fn idx -> elem(refs, idx) == keydir end)
  end

  defp keydir_index(_ctx, _keydir), do: nil

  defp track_keydir_binary_warm(%{keydir_binary_bytes: ref}, idx, value)
       when is_integer(idx) do
    bytes = offheap_size(value)

    if bytes > 0 and idx >= 0 and idx < :atomics.info(ref).size do
      :atomics.add(ref, idx + 1, bytes)
    end
  end

  defp track_keydir_binary_warm(_ctx, _idx, _value), do: :ok

  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024

  @spec max_key_size() :: pos_integer()
  @doc "Returns the maximum allowed key size in bytes."
  def max_key_size, do: @max_key_size

  @spec max_value_size() :: pos_integer()
  @doc "Returns the maximum allowed value size in bytes."
  def max_value_size, do: @max_value_size

  @doc """
  Batch async PUT for pipelined SET commands without options.

  Takes a list of `{key, value}` tuples. All keys must target async
  durability namespaces. Groups by shard, does batch ETS inserts per
  shard, fires BitcaskWriter casts and Raft submissions individually
  (they batch internally). Returns `:ok` or `{:error, reason}`.

  Caller must validate key/value sizes before calling. This skips per-key
  validation for speed, but still rejects pressured shards before publishing
  any writes so a mixed batch cannot partially bypass disk-pressure backoff.
  """
  @spec batch_async_put(FerricStore.Instance.t(), [{binary(), binary()}]) ::
          :ok | {:error, binary()}
  def batch_async_put(%{name: name} = ctx, kv_pairs) when name != :default do
    case pressured_batch_shard(ctx, kv_pairs) do
      nil -> batch_local_put(ctx, kv_pairs)
      idx -> {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    end
  end

  def batch_async_put(ctx, kv_pairs) do
    lfu_val = LFU.initial()
    hot_max = ctx.hot_cache_max_value_size
    wv_size = :counters.info(ctx.write_version).size

    shard_batches =
      kv_pairs
      |> Enum.group_by(fn {key, _value} -> shard_for(ctx, key) end)
      |> Enum.map(fn {idx, shard_kvs} ->
        keydir = elem(ctx.keydir_refs, idx)
        effective_kvs = dedupe_last_kvs(shard_kvs)

        {entries, large_disk_batch} =
          Enum.reduce(effective_kvs, {[], []}, fn {key, value}, {entry_acc, disk_acc} ->
            value_for_ets = if byte_size(value) > hot_max, do: nil, else: value

            disk_acc =
              if value_for_ets == nil do
                [{key, value, 0} | disk_acc]
              else
                disk_acc
              end

            {[{key, value, value_for_ets} | entry_acc], disk_acc}
          end)

        {idx, keydir, shard_kvs, Enum.reverse(entries), large_disk_batch}
      end)

    pressured_idx =
      Enum.find_value(shard_batches, fn {idx, _keydir, _shard_kvs, _entries, _large_disk_batch} ->
        if shard_under_disk_pressure?(ctx, idx), do: idx, else: nil
      end)

    if pressured_idx do
      {:error, "ERR disk pressure on shard #{pressured_idx}, rejecting async write"}
    else
      locks =
        for {idx, _keydir, _shard_kvs, entries, _large_disk_batch} <- shard_batches,
            {key, _value, _value_for_ets} <- entries do
          {idx, key}
        end

      case acquire_async_key_latches(ctx, locks) do
        {:ok, held_latches} ->
          try do
            overloaded? =
              Enum.any?(shard_batches, fn {idx, _keydir, _shard_kvs, _entries, _large_disk_batch} ->
                not Ferricstore.Raft.Batcher.async_accepting?(idx)
              end)

            if overloaded?, do: throw({:async_error, "ERR async replication overloaded"})

            large_previous = snapshot_batch_large_values(ctx, shard_batches)

            disk_locations =
              Enum.reduce(
                shard_batches,
                %{},
                fn {idx, _keydir, _shard_kvs, _entries, large_disk_batch}, acc ->
                  if large_disk_batch == [] do
                    acc
                  else
                    reversed = Enum.reverse(large_disk_batch)

                    case nif_append_batch_with_file(ctx, idx, reversed) do
                      {:ok, file_id, locations} ->
                        shard_locations =
                          Enum.zip(reversed, locations)
                          |> Map.new(fn {{key, value, _exp}, {offset, _rec_size}} ->
                            {key, {file_id, offset, byte_size(value)}}
                          end)

                        Map.put(acc, idx, shard_locations)

                      {:error, reason} ->
                        throw({:disk_error, reason})
                    end
                  end
                end
              )

            Enum.reduce(shard_batches, MapSet.new(), fn {idx, _keydir, _shard_kvs, entries,
                                                         _large_disk_batch},
                                                        accepted_idxs ->
              raft_cmds = build_origin_checked_batch_put_commands(ctx, idx, entries)

              case async_submit_batch_to_raft(idx, raft_cmds) do
                :ok ->
                  MapSet.put(accepted_idxs, idx)

                {:error, reason} ->
                  rollback_batch_large_puts(ctx, large_previous, accepted_idxs)

                  if MapSet.size(accepted_idxs) > 0 do
                    throw({:partial_async_error, reason})
                  else
                    throw({:async_error, reason})
                  end
              end
            end)

            Enum.each(shard_batches, fn {idx, keydir, shard_kvs, entries, _large_disk_batch} ->
              shard_locations = Map.get(disk_locations, idx, %{})
              install_batch_async_entries(ctx, idx, keydir, entries, shard_locations, lfu_val)

              if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, length(shard_kvs))
            end)
          after
            release_async_key_latches(held_latches)
          end

        {:error, {:timeout, wait_ms}} ->
          throw({:async_error, async_key_latch_timeout_error_message(wait_ms)})
      end

      :ok
    end
  catch
    :throw, {:disk_error, reason} ->
      {:error, "ERR disk write failed: #{inspect(reason)}"}

    :throw, {:async_error, reason} ->
      {:error, reason}

    :throw, {:partial_async_error, reason} ->
      {:error, "ERR async replication partial outcome unknown: #{reason}"}
  end

  @doc false
  def __install_batch_async_entries_for_test__(ctx, idx, entries, shard_locations) do
    keydir = elem(ctx.keydir_refs, idx)
    install_batch_async_entries(ctx, idx, keydir, entries, shard_locations, LFU.initial())
  end

  defp install_batch_async_entries(ctx, idx, keydir, entries, shard_locations, lfu_val) do
    Enum.each(entries, fn {key, value, value_for_ets} ->
      clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)

      case Map.get(shard_locations, key) do
        {file_id, offset, value_size} ->
          track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
          :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, file_id, offset, value_size})

        nil ->
          install_batch_async_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val)
      end
    end)
  end

  defp install_batch_async_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val) do
    case :ets.lookup(keydir, key) do
      [{^key, ^value_for_ets, 0, _lfu, fid, off, value_size}]
      when fid != :pending and valid_cold_location(fid, off, value_size) ->
        :ok

      _ ->
        track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
        :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, :pending, 0, byte_size(value)})
    end
  end

  defp batch_local_put(ctx, kv_pairs) do
    # Embedded/custom instances are local/direct. They must not consult the
    # default instance's Raft batchers; those global names are owned by the
    # application instance and can be under unrelated backpressure.
    Enum.reduce_while(kv_pairs, :ok, fn {key, value}, :ok ->
      case put(ctx, key, value, 0) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
        other -> {:halt, {:error, inspect(other)}}
      end
    end)
  end

  defp pressured_batch_shard(ctx, kv_pairs) do
    Enum.find_value(kv_pairs, fn {key, _value} ->
      idx = shard_for(ctx, key)
      if shard_under_disk_pressure?(ctx, idx), do: idx, else: nil
    end)
  end

  defp dedupe_last_kvs(kv_pairs) do
    last_index_by_key =
      kv_pairs
      |> Enum.with_index()
      |> Map.new(fn {{key, _value}, index} -> {key, index} end)

    kv_pairs
    |> Enum.with_index()
    |> Enum.filter(fn {{key, _value}, index} -> Map.fetch!(last_index_by_key, key) == index end)
    |> Enum.map(fn {kv, _index} -> kv end)
  end

  defp snapshot_batch_large_values(ctx, shard_batches) do
    Enum.reduce(shard_batches, %{}, fn {idx, _keydir, _shard_kvs, _entries, large_disk_batch},
                                       acc ->
      Enum.reduce(large_disk_batch, acc, fn {key, _value, _expire_at_ms}, snapshot_acc ->
        Map.put(snapshot_acc, {idx, key}, snapshot_live_value(ctx, idx, key))
      end)
    end)
  end

  defp build_origin_checked_batch_put_commands(ctx, idx, entries) do
    Enum.map(entries, fn {key, value, _value_for_ets} ->
      previous = snapshot_live_value(ctx, idx, key)
      origin_checked_command(key, {:put, key, value, 0}, previous, value, 0)
    end)
  end

  defp rollback_batch_large_puts(ctx, large_previous, accepted_idxs) do
    Enum.each(large_previous, fn {{idx, key}, previous} ->
      unless MapSet.member?(accepted_idxs, idx) do
        rollback_unaccepted_large_put(ctx, idx, key, previous)
      end
    end)
  end

  @doc """
  Batch quorum PUT for pipelined SET commands.

  Groups commands by shard, submits each group as a single batch to its
  Batcher with ONE reply ref per shard, then waits for all shard replies.
  Returns a list of results in input order.

  Uses one ref per shard (not per command) so the connection process's
  selective receive scans at most shard_count refs instead of N*pipeline
  refs — critical for high concurrency where TCP messages flood the
  mailbox.
  """
  @spec batch_quorum_put(FerricStore.Instance.t(), [{binary(), binary()}]) :: [
          :ok | {:error, binary() | {:timeout, :unknown_outcome}}
        ]
  def batch_quorum_put(ctx, kv_pairs) do
    batch_quorum_put(ctx, kv_pairs, nil)
  end

  defp batch_quorum_put(_ctx, [], _origin_node), do: []

  defp batch_quorum_put(ctx, kv_pairs, origin_node) do
    wv_size = :counters.info(ctx.write_version).size

    # Single pass: group by shard, build cmds + indices lists simultaneously,
    # also remember the kv_pairs subset per shard so we can re-issue the
    # batch via erpc on :not_leader replies.
    {by_shard, count, by_shard_kvs} =
      kv_pairs
      |> Enum.reduce({%{}, 0, %{}}, fn {key, value}, {shards, i, kvs_map} ->
        idx = shard_for(ctx, key)
        cmd = {:put, key, value, 0}
        entry = Map.get(shards, idx, {[], []})
        {cmds, indices} = entry
        shards = Map.put(shards, idx, {[cmd | cmds], [i | indices]})
        kvs_map = Map.update(kvs_map, idx, [{key, value}], fn acc -> [{key, value} | acc] end)
        {shards, i + 1, kvs_map}
      end)

    shard_refs =
      Enum.map(by_shard, fn {shard_idx, {cmds, indices}} ->
        {from, token} = ReplyAwaiter.new()
        cmds = Enum.reverse(cmds)

        if origin_node == nil do
          Ferricstore.Raft.Batcher.write_batch(shard_idx, cmds, from)
        else
          Ferricstore.Raft.Batcher.write_batch_forwarded(shard_idx, cmds, from, origin_node)
        end

        {token, shard_idx, Enum.reverse(indices)}
      end)

    results =
      collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))

    # Per-shard not_leader → forward that shard's slice to its hinted leader.
    # Each shard reports independently; we re-issue just the failing shard.
    results =
      Enum.reduce(by_shard_kvs, results, fn {shard_idx, kvs}, acc ->
        # All indices for this shard share the same shard-level reply
        first_index = Map.get(by_shard, shard_idx) |> elem(1) |> List.last()

        case Map.get(acc, first_index) do
          {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
            merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx)

          {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
            merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx)

          _ ->
            acc
        end
      end)

    0..(count - 1)
    |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
  end

  defp merge_forwarded(acc, by_shard, shard_idx, kvs, leader_node, ctx) do
    {_, indices} = Map.fetch!(by_shard, shard_idx)
    indices = Enum.reverse(indices)
    new_results = forward_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(kvs))

    Enum.zip(indices, new_results)
    |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
  end

  defp collect_shard_replies([], _wv_size, _ctx, acc, _start), do: acc

  defp collect_shard_replies(remaining_refs, wv_size, ctx, acc, start) do
    elapsed = System.monotonic_time(:millisecond) - start
    timeout = max(10_000 - elapsed, 0)

    refs_by_token =
      Map.new(remaining_refs, fn {token, shard_idx, indices} ->
        {token, {shard_idx, indices}}
      end)

    {_status, replies, _unresolved} =
      remaining_refs
      |> Enum.map(fn {token, _shard_idx, _indices} -> token end)
      |> ReplyAwaiter.collect(timeout)

    Enum.reduce(replies, acc, fn {token, result}, next_acc ->
      {shard_idx, indices} = Map.fetch!(refs_by_token, token)
      apply_shard_results(result, indices, shard_idx, wv_size, ctx, next_acc)
    end)
  end

  defp apply_shard_results({:ok, results}, indices, shard_idx, wv_size, ctx, acc)
       when is_list(results) do
    ok_count = Enum.count(results, fn r -> r == :ok or not match?({:error, _}, r) end)

    if ok_count > 0 and shard_idx < wv_size do
      :counters.add(ctx.write_version, shard_idx + 1, ok_count)
    end

    Enum.zip(indices, results)
    |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
  end

  defp apply_shard_results(result, indices, shard_idx, wv_size, ctx, acc) do
    case result do
      {:error, _} ->
        Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)

      _ ->
        if shard_idx < wv_size,
          do: :counters.add(ctx.write_version, shard_idx + 1, length(indices))

        Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)
    end
  end

  # Forward a batch to the leader's node. Used by batch_quorum_put when
  # the local Batcher rejects with :not_leader. Issues an erpc to run
  # batch_quorum_put on the leader, then returns its results.
  defp forward_batch_to_leader(_ctx, leader_node, _shard_idx, kv_pairs)
       when leader_node == node() do
    Enum.map(kv_pairs, fn _ -> {:error, "ERR not leader, election in progress"} end)
  end

  defp forward_batch_to_leader(_ctx, leader_node, shard_idx, kv_pairs) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

      leader_results =
        :erpc.call(
          leader_node,
          __MODULE__,
          :__forwarded_batch_quorum_put__,
          [remote_ctx, kv_pairs, node()],
          10_000
        )

      unwrap_forwarded_batch_results(shard_idx, leader_results)
    catch
      _, reason ->
        require Logger
        Logger.warning("batch forward to #{inspect(leader_node)} failed: #{inspect(reason)}")
        __forward_batch_failure_results__(reason, length(kv_pairs))
    end
  end

  @doc false
  def __forwarded_batch_quorum_put__(ctx, kv_pairs, origin_node) do
    batch_quorum_put(ctx, kv_pairs, origin_node)
  end

  defp unwrap_forwarded_batch_results(shard_idx, results) when is_list(results) do
    Enum.map(results, &barrier_forwarded_result(shard_idx, &1, 5_000))
  end

  defp unwrap_forwarded_batch_results(_shard_idx, other), do: other

  @doc """
  Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
  timestamp in milliseconds; pass `0` for no expiry.
  """
  @spec put(FerricStore.Instance.t(), binary(), binary(), non_neg_integer()) ::
          :ok | {:error, binary()}
  def put(ctx, key, value, expire_at_ms \\ 0) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full(ctx, key) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:put, key, value, expire_at_ms})

          {:error, _} = err ->
            err
        end
    end
  end

  @doc """
  Atomically applies Redis SET options in Raft order.

  Unlike `put/4`, this keeps NX/XX/GET/KEEPTTL checks inside the state
  machine so concurrent conditional SETs serialize correctly.
  """
  @spec set(FerricStore.Instance.t(), binary(), binary(), map()) :: term()
  def set(ctx, key, value, opts) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full_for_set(ctx, key, opts) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:set, key, value, opts.expire_at_ms, opts})

          {:error, _} = err ->
            err
        end
    end
  end

  # Checks if the keydir is full. If so, only allows writes to existing keys.
  # Checks both `keydir_full?` (ETS-level memory guard) and `reject_writes?`
  # (noeviction policy with reject-level pressure). The Shard GenServer has its
  # own `reject_writes?` check in `handle_call({:put, ...})`, but when the
  # quorum bypass path is used, the Shard is skipped, so we must check here.
  # Reads from ctx.pressure_flags atomics instead of persistent_term.
  defp check_keydir_full(ctx, key) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      # Allow updates to existing keys — use ETS direct check
      if exists_fast?(ctx, key) do
        :ok
      else
        # Nudge MemoryGuard to run eviction immediately (async, non-blocking).
        # Without this, the next eviction cycle is up to 100ms away.
        Ferricstore.MemoryGuard.nudge()
        {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  defp check_keydir_full_for_set(ctx, key, opts) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      existing? = exists_fast?(ctx, key)

      cond do
        existing? ->
          :ok

        opts.xx ->
          :ok

        true ->
          Ferricstore.MemoryGuard.nudge()
          {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
  @spec delete(FerricStore.Instance.t(), binary()) :: :ok
  def delete(ctx, key) do
    idx = shard_for(ctx, key)

    raft_write(ctx, idx, key, {:delete, key})
  end

  @doc """
  Submits a server command through Raft for replication to all nodes.

  Server commands are opaque to the library — the state machine dispatches
  them via the `raft_apply_hook` callback on the Instance struct. Routed
  through shard 0 for consistent ordering.
  """
  @spec server_command(FerricStore.Instance.t(), term()) :: term()
  def server_command(ctx, command) do
    raft_write(ctx, 0, "__server__", {:server_command, command})
  end

  @doc """
  Routes a probabilistic data structure write command through Raft.
  """
  @spec prob_write(FerricStore.Instance.t(), tuple()) :: term()
  def prob_write(ctx, command) do
    key = extract_prob_key(command)
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, command)
  end

  defp extract_prob_key({:bloom_create, key, _, _, _}), do: key
  defp extract_prob_key({:bloom_add, key, _, _}), do: key
  defp extract_prob_key({:bloom_madd, key, _, _}), do: key
  defp extract_prob_key({:cms_create, key, _, _}), do: key
  defp extract_prob_key({:cms_incrby, key, _}), do: key
  defp extract_prob_key({:cms_merge, dst_key, _, _, _}), do: dst_key
  defp extract_prob_key({:cuckoo_create, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_add, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_addnx, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_del, key, _}), do: key
  defp extract_prob_key({:topk_create, key, _, _, _, _}), do: key
  defp extract_prob_key({:topk_add, key, _}), do: key
  defp extract_prob_key({:topk_incrby, key, _}), do: key

  @doc """
  Returns `true` if `key` exists and is not expired.

  Uses direct ETS lookup (no GenServer roundtrip) for hot and cold keys.
  A key is considered existing if it is in the keydir and not expired,
  regardless of whether its value is hot (in ETS) or cold (on disk only).
  """
  @spec exists?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :exists, false)
    end
  end

  @doc """
  Fast ETS-direct existence check for a key.

  Returns `true` if the key exists in ETS and is not expired, `false` otherwise.
  This bypasses the GenServer entirely, saving ~1-3us per call. Used in the
  hot write path (`check_keydir_full/2`) where we only need a boolean answer
  and can tolerate the fact that cold keys (value=nil but still in keydir)
  are correctly detected as existing.
  """
  @spec exists_fast?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists_fast?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> false
    end
  end

  @doc """
  Atomically increments the integer value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
  on success or `{:error, reason}` if the value is not a valid integer.
  """
  @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
          {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(FerricStore.Instance.t(), binary(), float()) ::
          {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr_float, key, delta})
  end

  @doc """
  Atomically appends `suffix` to the value of `key`.

  If the key does not exist, it is created with value `suffix`.
  Returns `{:ok, new_byte_length}`.
  """
  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
  end

  @doc """
  Atomically gets the old value and sets a new value for `key`.

  Returns the old value, or `nil` if the key did not exist.
  """
  @spec getset(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def getset(ctx, key, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns the value, or `nil` if the key did not exist.
  """
  @spec getdel(FerricStore.Instance.t(), binary()) :: binary() | nil
  def getdel(ctx, key) do
    raft_write(ctx, shard_for(ctx, key), key, {:getdel, key})
  end

  @doc """
  Atomically gets the value and updates the expiry of `key`.

  `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
  pass `0` to persist (remove expiry). Returns the value, or `nil` if
  the key did not exist.
  """
  @spec getex(FerricStore.Instance.t(), binary(), non_neg_integer()) :: binary() | nil
  def getex(ctx, key, expire_at_ms) do
    raft_write(ctx, shard_for(ctx, key), key, {:getex, key, expire_at_ms})
  end

  @doc """
  Atomically overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.
  """
  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
  end

  @doc """
  Atomically sets the bit at `offset` to `bit_val` (0 or 1). Returns the
  previous bit value (0 or 1). Extends the bitmap with zero bytes if
  necessary. Goes through Raft so concurrent SETBITs on the same key
  never lose updates — the state machine is the sole mutator.
  """
  @spec setbit(FerricStore.Instance.t(), binary(), non_neg_integer(), 0 | 1) :: 0 | 1
  def setbit(ctx, key, offset, bit_val) do
    raft_write(ctx, shard_for(ctx, key), key, {:setbit, key, offset, bit_val})
  end

  @doc """
  Atomically increments the integer value of hash field `field` in `key` by
  `delta`. Returns `{:ok, new_int}` or `{:error, reason}`. Shares ordering
  with the parent hash's shard (routes by the hash's redis_key).
  """
  @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
          integer() | {:error, binary()}
  def hincrby(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrby, key, field, delta})
  end

  @doc """
  Atomically increments the float value of hash field `field` in `key` by
  `delta`. Returns the new value as a string, or `{:error, reason}`.
  """
  @spec hincrbyfloat(FerricStore.Instance.t(), binary(), binary(), float()) ::
          binary() | {:error, binary()}
  def hincrbyfloat(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrbyfloat, key, field, delta})
  end

  @doc """
  Atomically increments the score of `member` in the sorted set at `key` by
  `increment`. Returns the new score as a string.
  """
  @spec zincrby(FerricStore.Instance.t(), binary(), number(), binary()) ::
          binary() | {:error, binary()}
  def zincrby(ctx, key, increment, member) do
    raft_write(ctx, shard_for(ctx, key), key, {:zincrby, key, increment, member})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys(FerricStore.Instance.t()) :: [binary()]
  def keys(ctx) do
    sc = ctx.shard_count

    Enum.flat_map(0..(sc - 1), fn i ->
      case safe_read_call(ctx, i, :keys) do
        {:ok, keys} -> keys
        :unavailable -> []
      end
    end)
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
  def dbsize(ctx) do
    sc = ctx.shard_count
    now = HLC.now_ms()

    Enum.reduce(0..(sc - 1), 0, fn i, acc ->
      acc + live_keydir_size(ctx, i, resolve_keydir(ctx, i), now)
    end)
  end

  defp live_keydir_size(ctx, idx, keydir, now) do
    {count, expired_keys} =
      :ets.foldl(
        fn
          {_key, _value, 0, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count + 1, expired_keys}

          {_key, _value, exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} when exp > now ->
            {count + 1, expired_keys}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count, [key | expired_keys]}
        end,
        {0, []},
        keydir
      )

    Enum.each(expired_keys, fn key ->
      track_keydir_binary_delete(ctx, idx, keydir, key)
      :ets.delete(keydir, key)
    end)

    count
  rescue
    ArgumentError ->
      keydir_unavailable(ctx, idx, :dbsize, 0)
  end

  defp keydir_unavailable(ctx, idx, request, fallback) do
    emit_shard_unavailable(ctx, idx, request, :keydir_unavailable)
    fallback
  end

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def get_version(ctx, key) do
    idx = shard_for(ctx, key)

    case safe_read_call(ctx, idx, {:get_version, key}) do
      {:ok, version} -> version
      :unavailable -> shared_write_version(ctx, idx)
    end
  end

  defp shared_write_version(%{write_version: write_version}, idx) do
    size = :counters.info(write_version).size
    if idx < size, do: :counters.get(write_version, idx + 1), else: 0
  rescue
    _ -> 0
  end

  @doc """
  Returns the keydir disk location for a key, or `:miss`.

  Reads the `{file_id, offset, value_size}` fields directly from the keydir
  ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
  for live keys, or `:miss` if the key is not in the keydir or is expired.

  Used by sendfile zero-copy and STRLEN on cold keys.
  """
  @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :miss
  def get_keydir_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{_, _, 0, _, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, 0, _, fid, _off, _vsize}] when is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, 0, _, _fid, _off, _vsize}] ->
          :miss

        [{_, _, exp, _, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, exp, _, fid, _off, _vsize}] when exp > now and is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, exp, _, _fid, _off, _vsize}] when exp > now ->
          :miss

        [{^key, _, _exp, _, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :miss
    end
  end

  # -------------------------------------------------------------------
  # Native command accessors
  # -------------------------------------------------------------------

  @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
          1 | 0 | nil
  def cas(ctx, key, expected, new_value, ttl_ms) do
    expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
    raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
  end

  @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          :ok | {:error, binary()}
  def lock(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
  end

  @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(ctx, key, owner) do
    raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
  end

  @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          1 | {:error, binary()}
  def extend(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
  end

  @spec ratelimit_add(
          FerricStore.Instance.t(),
          binary(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: [term()]
  def ratelimit_add(ctx, key, window_ms, max, count) do
    raft_write(
      ctx,
      shard_for(ctx, key),
      key,
      {:ratelimit_add, key, window_ms, max, count}
    )
  end

  # -------------------------------------------------------------------
  # Compound key operations
  # -------------------------------------------------------------------

  @spec compound_get(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def compound_get(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
              {:ok, value} -> value
              :unavailable -> nil
            end
        end

      _ ->
        case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
          {:ok, value} -> value
          :unavailable -> nil
        end
    end
  end

  @spec compound_batch_get(FerricStore.Instance.t(), binary(), [binary()]) :: [binary() | nil]
  def compound_batch_get(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    {results, fallback_keys} =
      Enum.map_reduce(compound_keys, [], fn compound_key, fallback_keys ->
        case ets_get_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, lfu} ->
            sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
            {{:value, value}, fallback_keys}

          _ ->
            {:fallback, [compound_key | fallback_keys]}
        end
      end)

    fallback_values =
      case fallback_keys do
        [] ->
          []

        keys ->
          pending_keys = Enum.reverse(keys)

          case safe_read_call(ctx, idx, {:compound_batch_get, redis_key, pending_keys}) do
            {:ok, values} -> values
            :unavailable -> List.duplicate(nil, length(pending_keys))
          end
      end

    {values, []} =
      Enum.map_reduce(results, fallback_values, fn
        {:value, value}, remaining -> {value, remaining}
        :fallback, [value | remaining] -> {value, remaining}
      end)

    values
  end

  @spec compound_get_meta(FerricStore.Instance.t(), binary(), binary()) ::
          {binary(), non_neg_integer()} | nil
  def compound_get_meta(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
              {:ok, meta} -> meta
              :unavailable -> nil
            end
        end

      _ ->
        case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
          {:ok, meta} -> meta
          :unavailable -> nil
        end
    end
  end

  @spec compound_batch_get_meta(FerricStore.Instance.t(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil]
  def compound_batch_get_meta(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_batch_get_meta, redis_key, compound_keys}) do
      {:ok, metas} -> metas
      :unavailable -> List.duplicate(nil, length(compound_keys))
    end
  end

  @spec compound_put(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer()) ::
          :ok | {:error, term()}
  def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      case durability_for_key(ctx, redis_key) do
        :quorum ->
          quorum_write(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})

        :async ->
          if promoted_parent?(ctx, idx, redis_key) do
            quorum_write(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
          else
            async_compound_put(ctx, idx, redis_key, compound_key, value, expire_at_ms)
          end
      end
    else
      safe_write_call(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    end
  end

  @spec compound_batch_put(
          FerricStore.Instance.t(),
          binary(),
          [{binary(), binary(), non_neg_integer()}]
        ) :: :ok | {:error, term()}
  def compound_batch_put(_ctx, _redis_key, []), do: :ok

  def compound_batch_put(ctx, redis_key, entries) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      case durability_for_key(ctx, redis_key) do
        :quorum ->
          quorum_write(ctx, idx, {:compound_batch_put, redis_key, entries})

        :async ->
          if promoted_parent?(ctx, idx, redis_key) do
            quorum_write(ctx, idx, {:compound_batch_put, redis_key, entries})
          else
            async_compound_batch_put(ctx, idx, entries)
          end
      end
    else
      safe_write_call(ctx, idx, {:compound_batch_put, redis_key, entries})
    end
  end

  @spec compound_delete(FerricStore.Instance.t(), binary(), binary()) :: :ok | {:error, term()}
  def compound_delete(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)

    if ctx.name == :default do
      case durability_for_key(ctx, redis_key) do
        :quorum ->
          quorum_write(ctx, idx, {:compound_delete, redis_key, compound_key})

        :async ->
          if promoted_parent?(ctx, idx, redis_key) do
            quorum_write(ctx, idx, {:compound_delete, redis_key, compound_key})
          else
            async_compound_delete(ctx, idx, compound_key)
          end
      end
    else
      safe_write_call(ctx, idx, {:compound_delete, redis_key, compound_key})
    end
  end

  # ---------------------------------------------------------------------------
  # Async compound implementations (Group A in async-compound-list-prob-design.md)
  #
  # Structurally identical to async_write_put/delete — the only difference is
  # that the key is a compound_key (e.g. "H:user:1:name") built from a
  # redis_key + field. Durability was already decided by the caller (compound_put/
  # compound_delete) based on the PARENT redis_key's namespace, so the user-
  # facing abstraction "HSET is in the `user` namespace" holds.
  #
  # Promotion growth is intentionally skipped on the async path. Hashes that
  # grow large in an async namespace stay in the shared Bitcask log instead
  # of being promoted to a dedicated file. Already-promoted parents are the
  # exception: their mutations must route through the quorum/promoted path so
  # ETS file locations keep pointing at the dedicated Bitcask directory.
  # ---------------------------------------------------------------------------

  defp promoted_parent?(ctx, idx, redis_key) do
    keydir = elem(ctx.keydir_refs, idx)
    marker = Promotion.marker_key(redis_key)

    case :ets.lookup(keydir, marker) do
      [{^marker, type, expire_at_ms, _lfu, _fid, _off, _value_size}]
      when type in ["hash", "set", "zset"] ->
        expire_at_ms == 0 or expire_at_ms > CommandTime.now_ms()

      _ ->
        false
    end
  end

  # Async list_op latch-first dispatch. On CAS win: execute inline under
  # the per-key latch. On loss: bounce to RmwCoordinator via direct
  # GenServer.call (avoids the compile-time cycle
  # RmwCoordinator → Router → RmwCoordinator that a direct call to
  # `RmwCoordinator.execute/2` would otherwise create).
  defp async_list_op(ctx, idx, key, cmd) do
    latch_tab = elem(ctx.latch_refs, idx)

    case :ets.insert_new(latch_tab, {key, self()}) do
      true ->
        try do
          :telemetry.execute([:ferricstore, :list_op, :latch], %{}, %{shard_index: idx})
          execute_list_op_inline(ctx, idx, cmd)
        after
          :ets.take(latch_tab, key)
        end

      false ->
        async_list_op_worker_call(ctx, idx, cmd)
    end
  end

  defp async_list_lmove(ctx, idx, src_key, dst_key, cmd) do
    case try_acquire_async_key_latches(ctx, [{idx, src_key}, {idx, dst_key}]) do
      {:ok, held_latches} ->
        try do
          :telemetry.execute([:ferricstore, :list_op, :latch], %{}, %{shard_index: idx})
          execute_list_op_inline(ctx, idx, cmd)
        after
          release_async_key_latches(held_latches)
        end

      :error ->
        async_list_op_worker_call(ctx, idx, cmd)
    end
  end

  defp async_list_op_worker_call(ctx, idx, cmd) do
    try do
      GenServer.call(
        :"Ferricstore.Store.RmwCoordinator.#{idx}",
        {:rmw, ctx, cmd},
        10_000
      )
    catch
      :exit, {:timeout, _} -> ErrorReasons.write_timeout_unknown()
      :exit, {:noproc, _} -> {:error, "ERR RMW worker unavailable"}
      :exit, _ -> {:error, "ERR RMW worker crashed"}
    end
  end

  @doc """
  Executes a list_op inline under a held latch. Called from
  `Router.async_list_op` (fast path) and `RmwCoordinator.handle_call`
  (contended path). The latch guarantees exclusive access to the list's
  compound keys.

  Reserves Batcher capacity before touching origin-local compound state.
  That prevents overloaded async replication from exposing a local-only
  list mutation as successful.
  """
  @spec execute_list_op_inline(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  def execute_list_op_inline(ctx, idx, {:list_op, key, operation} = cmd) do
    :telemetry.execute([:ferricstore, :rmw, :worker_list_op], %{}, %{shard_index: idx})

    if list_operation_mutating?(operation) do
      execute_mutating_list_op_inline(ctx, idx, cmd, fn ->
        do_execute_list_op_inline(ctx, idx, key, operation)
      end)
    else
      do_execute_list_op_inline(ctx, idx, key, operation)
    end
  end

  def execute_list_op_inline(ctx, idx, {:list_op_lmove, src_key, dst_key, from_dir, to_dir} = cmd) do
    execute_mutating_list_op_inline(ctx, idx, cmd, fn ->
      store = build_origin_compound_store(ctx, idx)

      checked_lmove(src_key, dst_key, store, from_dir, to_dir)
    end)
  end

  defp do_execute_list_op_inline(ctx, idx, key, operation) do
    store = build_origin_compound_store(ctx, idx)

    case ensure_list_type_for_operation(key, operation, store) do
      :ok ->
        # Resolve the module at runtime to avoid the compile-time cycle
        # ListOps → Ops → Router → ListOps. `list_ops_mod/0` returns an atom
        # that xref cannot trace through.
        :erlang.apply(list_ops_mod(), :execute, [key, store, operation])

      {:error, _} = err ->
        err
    end
  end

  defp execute_mutating_list_op_inline(ctx, idx, cmd, fun) do
    previous_rollback = Process.get(@async_list_rollback_key)
    Process.put(@async_list_rollback_key, %{ctx: ctx, idx: idx, originals: %{}})

    # Do not enqueue the async Raft command until the origin-local list write
    # succeeds. Large list elements can fail during Bitcask append; accepting
    # Raft first would let a client-visible error apply later via replication.
    try do
      result = fun.()

      cond do
        match?({:error, _}, result) ->
          rollback_async_list_originals(ctx, idx)
          result

        true ->
          case async_submit_to_raft(idx, cmd) do
            :ok ->
              bump_write_version(ctx, idx)
              result

            {:error, _} = error ->
              rollback_async_list_originals(ctx, idx)
              error
          end
      end
    after
      case previous_rollback do
        nil -> Process.delete(@async_list_rollback_key)
        previous -> Process.put(@async_list_rollback_key, previous)
      end
    end
  end

  defp list_operation_mutating?({:lrange, _, _}), do: false
  defp list_operation_mutating?(:llen), do: false
  defp list_operation_mutating?({:lindex, _}), do: false
  defp list_operation_mutating?({:lpos, _, _, _, _}), do: false
  defp list_operation_mutating?(_), do: true

  defp ensure_list_type_for_operation(key, operation, store)

  defp ensure_list_type_for_operation(key, {:lpush, _elements}, store),
    do: TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, {:rpush, _elements}, store),
    do: TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, _operation, store),
    do: TypeRegistry.check_type(key, :list, store)

  # The module atom is constructed at runtime from a string so it doesn't
  # appear as a BEAM atom-literal reference to the target module. This
  # breaks the compile-time dependency edge Router -> ListOps while keeping
  # the call semantically identical.
  @compile {:inline, list_ops_mod: 0}
  defp list_ops_mod, do: String.to_atom("Elixir.Ferricstore.Store.ListOps")

  # Build an origin-local compound store that ListOps.execute can drive.
  # Each put closes over the current active file (file_id, path); a file
  # rotation between put and submit is rare but harmless — the submission
  # carries the raw command, replicas apply against their own active file.
  defp build_origin_compound_store(ctx, idx) do
    keydir = elem(ctx.keydir_refs, idx)
    {_file_id, file_path, shard_data_path} = Ferricstore.Store.ActiveFile.get(ctx, idx)

    %{
      compound_get: fn _redis_key, compound_key ->
        origin_compound_get(ctx, idx, keydir, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, exp ->
        record_async_list_original(ctx, idx, compound_key)
        install_rmw_value(ctx, idx, compound_key, value, exp)
      end,
      compound_delete: fn _redis_key, compound_key ->
        record_async_list_original(ctx, idx, compound_key)
        track_keydir_binary_delete(ctx, idx, keydir, compound_key)
        :ets.delete(keydir, compound_key)
        Ferricstore.Store.BitcaskWriter.delete(ctx, idx, file_path, compound_key)
        :ok
      end,
      compound_scan: fn _redis_key, prefix ->
        state = %{keydir: keydir, index: idx, instance_ctx: ctx}

        Ferricstore.Store.Shard.ETS.prefix_scan_entries(state, prefix, shard_data_path)
        |> Enum.sort_by(fn {field, _} -> field end)
      end,
      compound_count: fn _redis_key, prefix ->
        state = %{keydir: keydir, index: idx, instance_ctx: ctx}
        Ferricstore.Store.Shard.ETS.prefix_count_entries(state, prefix)
      end,
      exists?: fn k ->
        origin_key_exists?(ctx, idx, keydir, k)
      end
    }
  end

  defp record_async_list_original(ctx, idx, compound_key) do
    case Process.get(@async_list_rollback_key) do
      %{ctx: ^ctx, idx: ^idx, originals: originals} = rollback ->
        unless Map.has_key?(originals, compound_key) do
          Process.put(@async_list_rollback_key, %{
            rollback
            | originals:
                Map.put(originals, compound_key, snapshot_live_value(ctx, idx, compound_key))
          })
        end

      _ ->
        :ok
    end
  end

  defp rollback_async_list_originals(ctx, idx) do
    originals =
      case Process.get(@async_list_rollback_key) do
        %{ctx: ^ctx, idx: ^idx, originals: originals} -> originals
        _ -> %{}
      end

    Enum.each(originals, fn
      {compound_key, :missing} ->
        rollback_async_list_key_to_missing(ctx, idx, compound_key)

      {compound_key, {:value, value, expire_at_ms}} ->
        rollback_async_list_key_to_value(ctx, idx, compound_key, value, expire_at_ms)
    end)
  end

  defp rollback_async_list_key_to_missing(ctx, idx, compound_key) do
    keydir = elem(ctx.keydir_refs, idx)

    _ = append_delete_tombstone_nosync(ctx, idx, compound_key)
    Ferricstore.Store.BitcaskWriter.discard_pending(ctx, idx, compound_key)
    track_keydir_binary_delete(ctx, idx, keydir, compound_key)
    :ets.delete(keydir, compound_key)
  end

  defp rollback_async_list_key_to_value(ctx, idx, compound_key, value, expire_at_ms) do
    keydir = elem(ctx.keydir_refs, idx)
    disk_value = to_disk_binary(value)

    case nif_append_batch_with_file(ctx, idx, [{compound_key, disk_value, expire_at_ms}]) do
      {:ok, file_id, [{offset, _record_size}]} ->
        ets_value =
          case value do
            v when is_integer(v) ->
              Integer.to_string(v)

            v when is_float(v) ->
              Float.to_string(v)

            v when is_binary(v) ->
              if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
          end

        track_keydir_binary_insert(ctx, idx, keydir, compound_key, ets_value)

        :ets.insert(
          keydir,
          {compound_key, ets_value, expire_at_ms, LFU.initial(), file_id, offset,
           byte_size(disk_value)}
        )

      {:error, _reason} ->
        :ok
    end
  end

  defp origin_compound_get(ctx, idx, keydir, compound_key) do
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, _lfu} ->
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

        path =
          Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case read_cold_async(path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  defp origin_key_exists?(ctx, idx, keydir, key) do
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        true

      {:cold, file_id, _offset, value_size}
      when valid_cold_file_ref(file_id, value_size) ->
        true

      _ ->
        false
    end
  end

  defp async_compound_put(ctx, idx, _redis_key, compound_key, value, expire_at_ms) do
    with_async_key_latch(ctx, idx, compound_key, fn ->
      do_async_compound_put(ctx, idx, compound_key, value, expire_at_ms)
    end)
  end

  defp do_async_compound_put(ctx, idx, compound_key, value, expire_at_ms) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      previous = snapshot_live_value(ctx, idx, compound_key)

      raft_cmd =
        origin_checked_command(
          compound_key,
          {:put, compound_key, value, expire_at_ms},
          previous,
          value,
          expire_at_ms
        )

      if large_value_for_hot_cache?(ctx, value) do
        # Large cold values need a durable local file location before ETS can
        # point readers at them. Keep disk-first, then roll back if Raft rejects.
        with :ok <- install_async_put_value(ctx, idx, compound_key, value, expire_at_ms) do
          case async_enqueue_to_raft(idx, raft_cmd) do
            :ok ->
              bump_write_version(ctx, idx)
              :ok

            {:error, _} = error ->
              rollback_installed_async_value(ctx, idx, compound_key, previous)
              error
          end
        end
      else
        # Small values do not need a disk location before publication, so match
        # plain async SET: do not expose a value until Raft accepts the command.
        with :ok <- async_enqueue_to_raft(idx, raft_cmd),
             :ok <- install_async_put_value(ctx, idx, compound_key, value, expire_at_ms) do
          bump_write_version(ctx, idx)
          :ok
        end
      end
    end
  end

  defp async_compound_batch_put(ctx, idx, entries) do
    locks = Enum.map(entries, fn {compound_key, _value, _expire_at_ms} -> {idx, compound_key} end)

    case acquire_async_key_latches(ctx, locks) do
      {:ok, held_latches} ->
        try do
          do_async_compound_batch_put(ctx, idx, entries)
        after
          release_async_key_latches(held_latches)
        end

      {:error, {:timeout, wait_ms}} ->
        async_key_latch_timeout_error(wait_ms)
    end
  end

  defp do_async_compound_batch_put(ctx, idx, entries) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    cond do
      under_pressure ->
        {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}

      Enum.any?(entries, fn {_compound_key, value, _expire_at_ms} ->
        large_value_for_hot_cache?(ctx, value)
      end) ->
        Enum.reduce_while(entries, :ok, fn {compound_key, value, expire_at_ms}, :ok ->
          case do_async_compound_put(ctx, idx, compound_key, value, expire_at_ms) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, inspect(other)}}
          end
        end)

      true ->
        commands =
          Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
            previous = snapshot_live_value(ctx, idx, compound_key)

            origin_checked_command(
              compound_key,
              {:put, compound_key, value, expire_at_ms},
              previous,
              value,
              expire_at_ms
            )
          end)

        with :ok <- async_submit_batch_to_raft(idx, commands) do
          Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
            :ok = install_async_put_value(ctx, idx, compound_key, value, expire_at_ms)
          end)

          size = :counters.info(ctx.write_version).size
          if idx < size, do: :counters.add(ctx.write_version, idx + 1, length(entries))
          :ok
        end
    end
  end

  defp async_compound_delete(ctx, idx, compound_key) do
    with_async_key_latch(ctx, idx, compound_key, fn ->
      do_async_compound_delete(ctx, idx, compound_key)
    end)
  end

  defp do_async_compound_delete(ctx, idx, compound_key) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      previous = snapshot_live_value(ctx, idx, compound_key)
      raft_cmd = origin_checked_command(compound_key, {:delete, compound_key}, previous, nil, 0)

      with :ok <- async_enqueue_to_raft(idx, raft_cmd) do
        keydir = elem(ctx.keydir_refs, idx)
        track_keydir_binary_delete(ctx, idx, keydir, compound_key)
        :ets.delete(keydir, compound_key)
        maybe_apply_async_zset_delete(ctx, idx, compound_key)

        wv_size = :counters.info(ctx.write_version).size
        if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

        :ok
      end
    end
  end

  @spec compound_scan(FerricStore.Instance.t(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_scan, redis_key, prefix}) do
      {:ok, results} -> results
      :unavailable -> []
    end
  end

  @spec compound_count(FerricStore.Instance.t(), binary(), binary()) :: non_neg_integer()
  def compound_count(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    case safe_read_call(ctx, idx, {:compound_count, redis_key, prefix}) do
      {:ok, count} -> count
      :unavailable -> 0
    end
  end

  @spec zset_score_range(FerricStore.Instance.t(), binary(), term(), term(), boolean()) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
    |> unwrap_zset_index_reply()
  end

  @spec zset_score_range_slice(
          FerricStore.Instance.t(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range_slice(ctx, redis_key, min_bound, max_bound, reverse?, offset, count) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(
      idx,
      {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count}
    )
    |> unwrap_zset_index_reply()
  end

  @spec zset_score_count(FerricStore.Instance.t(), binary(), term(), term()) ::
          {:ok, non_neg_integer()} | :unavailable
  def zset_score_count(ctx, redis_key, min_bound, max_bound) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_score_count, redis_key, min_bound, max_bound})
    |> unwrap_zset_index_reply()
  end

  @spec zset_rank_range(
          FerricStore.Instance.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          boolean()
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_rank_range(ctx, redis_key, start_idx, stop_idx, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
    |> unwrap_zset_index_reply()
  end

  @spec zset_member_rank(FerricStore.Instance.t(), binary(), binary(), boolean()) ::
          {:ok, non_neg_integer() | nil} | :unavailable
  def zset_member_rank(ctx, redis_key, member, reverse?) do
    idx = shard_for(ctx, redis_key)

    ctx
    |> safe_read_call(idx, {:zset_member_rank, redis_key, member, reverse?})
    |> unwrap_zset_index_reply()
  end

  defp unwrap_zset_index_reply({:ok, {:ok, result}}), do: {:ok, result}
  defp unwrap_zset_index_reply(other), do: other

  @spec compound_delete_prefix(FerricStore.Instance.t(), binary(), binary()) :: :ok
  def compound_delete_prefix(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)
    quorum_write(ctx, idx, {:compound_delete_prefix, redis_key, prefix})
  end

  # -------------------------------------------------------------------
  # List operations
  # -------------------------------------------------------------------

  @spec list_op(FerricStore.Instance.t(), binary(), term()) :: term()
  def list_op(ctx, key, {:lmove, destination, from_dir, to_dir}) do
    source_idx = shard_for(ctx, key)

    if source_idx == shard_for(ctx, destination) do
      raft_write(ctx, source_idx, key, {:list_op_lmove, key, destination, from_dir, to_dir})
    else
      Ferricstore.CrossShardOp.execute(
        [{key, :read_write}, {destination, :write}],
        fn unified_store ->
          checked_lmove(key, destination, unified_store, from_dir, to_dir)
        end,
        intent: %{command: :lmove, keys: %{source: key, dest: destination}},
        instance: ctx
      )
    end
  end

  def list_op(ctx, key, operation) do
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, {:list_op, key, operation})
  end

  defp checked_lmove(source, destination, store, from_dir, to_dir) do
    with :ok <- TypeRegistry.check_type(source, :list, store) do
      case ListOps.read_meta(source, store) do
        nil ->
          nil

        {0, _, _} ->
          nil

        _meta ->
          with :ok <- TypeRegistry.check_or_set(destination, :list, store) do
            ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
          end
      end
    end
  end
end
