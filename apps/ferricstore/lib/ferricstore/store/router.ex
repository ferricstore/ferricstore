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

  alias Ferricstore.HLC
  alias Ferricstore.Stats
  alias Ferricstore.Store.LFU

  import Bitwise, only: [band: 2]

  @slot_mask 1023

  # ---------------------------------------------------------------------------
  # Shard resolution helpers
  # ---------------------------------------------------------------------------

  @spec resolve_shard(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  @doc false
  def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)
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
          {:error, "ERR write timeout"}

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
      cond do
        bypass_shard?(command) ->
          # No shard handler exists for these tuples (json/bitmap/geo/hll/
          # tdigest "*_op"). Forward by re-running async_write on the leader
          # node — its local Batcher is the leader and won't reject.
          forward_bypass_to_leader(leader_node, idx, command)

        true ->
          forward_via_shard_call(ctx, leader_node, idx, command)
      end
    end
  end

  defp forward_via_shard_call(ctx, leader_node, idx, command) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)
      result =
        :erpc.call(leader_node, GenServer, :call, [elem(remote_ctx.shard_names, idx), command, 10_000], 10_000)

      case result do
        # Leader's batcher detected a cross-node caller and tagged the reply
        # with the ra_index. Barrier on local apply so reads on this node
        # see the just-written value (read-your-write across redirects).
        {:remote_applied_at, ra_index, real_result} ->
          _ = Ferricstore.Raft.Batcher.await_local_applied(idx, ra_index, 5_000)
          real_result

        other ->
          other
      end
    catch
      _, _ ->
        try do
          GenServer.call(elem(ctx.shard_names, idx), command, 10_000)
        catch
          _, _ -> {:error, "ERR leader unavailable"}
        end
    end
  end

  defp forward_bypass_to_leader(leader_node, _idx, command) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)
      :erpc.call(leader_node, __MODULE__, :run_bypass_locally, [remote_ctx, command], 10_000)
    catch
      _, _ -> {:error, "ERR leader unavailable"}
    end
  end

  @doc false
  # Public-but-undocumented entry point invoked via erpc from peer nodes
  # when a bypass_shard? command is forwarded to its shard's leader.
  # Re-runs the command on the local (leader) node, going through the
  # normal raft_write path which now recognizes us as the leader.
  def run_bypass_locally(ctx, command) do
    key =
      case command do
        {:json_op, _, [k | _]} -> k
        {:bitmap_op, "BITOP", [_op, dest | _]} -> dest
        {:bitmap_op, _, [k | _]} -> k
        {:geo_op, "GEOSEARCHSTORE", [dest | _]} -> dest
        {:geo_op, _, [k | _]} -> k
        {:hll_op, _, [k | _]} -> k
        {:tdigest_op, "TDIGEST.MERGE", [dest | _]} -> dest
        {:tdigest_op, _, [k | _]} -> k
      end

    raft_write(ctx, shard_for(ctx, key), key, command)
  end


  @doc "Public wrapper for durability_for_key, used by batch SET fast path."
  @spec durability_for_key_public(FerricStore.Instance.t(), binary()) :: :quorum | :async
  def durability_for_key_public(ctx, key), do: durability_for_key(ctx, key)

  @spec durability_for_key(FerricStore.Instance.t(), binary()) :: :quorum | :async
  defp durability_for_key(ctx, key) do
    case ctx.durability_mode do
      :all_quorum -> :quorum
      :all_async -> :async
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
    if ctx.raft_enabled do
      cond do
        # Commands the Shard GenServer doesn't handle (json/bitmap/geo/hll/
        # tdigest "*_op" tuples). These must skip the shard and go directly
        # via Batcher to Raft. async_write uses Batcher.write_async_quorum
        # which submits to the forced-quorum slot and returns the actual
        # state machine result (RMW-safe).
        bypass_shard?(command) -> async_write(ctx, idx, command)
        always_quorum?(command) -> quorum_write(ctx, idx, command)
        durability_for_key(ctx, key) == :quorum -> quorum_write(ctx, idx, command)
        true -> async_write(ctx, idx, command)
      end
    else
      # No Raft — direct GenServer.call to the shard
      GenServer.call(elem(ctx.shard_names, idx), command)
    end
  end

  # Tuples that have no Shard.handle_call clause. Sending them through
  # quorum_write would deadlock the shard caller for 10s. The forced-quorum
  # batcher slot already gives us strong consistency and the real return
  # value, so these route there directly regardless of namespace.
  defp bypass_shard?({:json_op, _, _}), do: true
  defp bypass_shard?({:bitmap_op, _, _}), do: true
  defp bypass_shard?({:geo_op, _, _}), do: true
  defp bypass_shard?({:hll_op, _, _}), do: true
  defp bypass_shard?({:tdigest_op, _, _}), do: true
  defp bypass_shard?(_), do: false

  # Linearizable primitives — must NEVER take the async path even if the
  # namespace is configured `:async`. Adding a new coordination primitive?
  # Add it here.
  @doc false
  def always_quorum?({:set, _, _, _, _}), do: true
  def always_quorum?({:cas, _, _, _, _}), do: true
  def always_quorum?({:lock, _, _, _}), do: true
  def always_quorum?({:unlock, _, _}), do: true
  def always_quorum?({:extend, _, _, _}), do: true
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
      keydir = elem(ctx.keydir_refs, idx)
      track_keydir_binary_delete(ctx, idx, keydir, key)
      :ets.delete(keydir, key)

      {_, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)
      Ferricstore.Store.BitcaskWriter.delete(idx, file_path, key)

      wv_size = :counters.info(ctx.write_version).size
      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
      async_submit_to_raft(idx, {:delete, key})
      :ok
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
  defp async_write(ctx, idx, {:setrange, key, _off, _value} = cmd), do: async_rmw(ctx, idx, key, cmd)

  # List ops are RMW at the structural level (LPUSH reads head pointer,
  # writes new element + new head). Same latch+worker pattern as plain RMW.
  # The latch is on the user-facing list key, serializing all list_ops on
  # that list.
  defp async_write(ctx, idx, {:list_op, key, _op} = cmd), do: async_list_op(ctx, idx, key, cmd)
  defp async_write(ctx, idx, {:list_op_lmove, src_key, _dst, _from, _to} = cmd) do
    # Single-shard LMOVE goes async on the source's latch. Cross-shard
    # LMOVE never reaches here (Router.list_op for lmove already splits
    # across shards via quorum_write before calling async_write).
    async_list_op(ctx, idx, src_key, cmd)
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
    ref = make_ref()
    from = {self(), ref}

    Ferricstore.Raft.Batcher.write_async_quorum(idx, command, from)

    result =
      receive do
        {^ref, reply} -> reply
      after
        10_000 -> {:error, "ERR async command timeout"}
      end

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
        size = :counters.info(ctx.write_version).size
        if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
        result
    end
  end

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
            {:rmw, cmd},
            10_000
          )
        catch
          :exit, {:timeout, _} -> {:error, "ERR RMW timeout"}
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
      do_rmw_inline(ctx, idx, cmd)
    end
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
          install_rmw_value(ctx, idx, key, delta, 0)
          async_submit_to_raft(idx, {:incr, key, delta})
          {:ok, delta}
        end

      {:hit, value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, int_val} ->
            new_val = int_val + delta

            if new_val > 9_223_372_036_854_775_807 or new_val < -9_223_372_036_854_775_808 do
              {:error, "ERR increment or decrement would overflow"}
            else
              install_rmw_value(ctx, idx, key, new_val, expire_at_ms)
              async_submit_to_raft(idx, {:incr, key, delta})
              {:ok, new_val}
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
        install_rmw_value(ctx, idx, key, new_val, 0)
        async_submit_to_raft(idx, {:incr_float, key, delta})
        {:ok, new_val}

      {:hit, value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, float_val} ->
            new_val = float_val + delta
            install_rmw_value(ctx, idx, key, new_val, expire_at_ms)
            async_submit_to_raft(idx, {:incr_float, key, delta})
            {:ok, new_val}

          :error ->
            {:error, "ERR value is not a valid float"}
        end
    end
  end

  defp do_rmw_inline(ctx, idx, {:append, key, suffix}) do
    {old_val, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", 0}
        {:hit, v, exp} -> {to_disk_binary(v), exp}
      end

    new_val = old_val <> suffix
    install_rmw_value(ctx, idx, key, new_val, expire_at_ms)
    async_submit_to_raft(idx, {:append, key, suffix})
    {:ok, byte_size(new_val)}
  end

  defp do_rmw_inline(ctx, idx, {:getset, key, new_value}) do
    old =
      case read_live(ctx, idx, key) do
        :missing -> nil
        {:hit, v, _exp} -> v
      end

    install_rmw_value(ctx, idx, key, new_value, 0)
    async_submit_to_raft(idx, {:getset, key, new_value})
    old
  end

  defp do_rmw_inline(ctx, idx, {:getdel, key}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, _exp} ->
        # Delete locally + submit delta for replicas.
        keydir = elem(ctx.keydir_refs, idx)
        track_keydir_binary_delete(ctx, idx, keydir, key)
        :ets.delete(keydir, key)

        {_, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)
        Ferricstore.Store.BitcaskWriter.delete(idx, file_path, key)

        wv_size = :counters.info(ctx.write_version).size
        if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

        async_submit_to_raft(idx, {:getdel, key})
        v
    end
  end

  defp do_rmw_inline(ctx, idx, {:getex, key, new_expire_at_ms}) do
    case read_live(ctx, idx, key) do
      :missing ->
        nil

      {:hit, v, _old_exp} ->
        install_rmw_value(ctx, idx, key, v, new_expire_at_ms)
        async_submit_to_raft(idx, {:getex, key, new_expire_at_ms})
        v
    end
  end

  defp do_rmw_inline(ctx, idx, {:setrange, key, offset, value}) do
    {old_val, expire_at_ms} =
      case read_live(ctx, idx, key) do
        :missing -> {"", 0}
        {:hit, v, exp} -> {to_disk_binary(v), exp}
      end

    new_val = apply_setrange(old_val, offset, value)
    install_rmw_value(ctx, idx, key, new_val, expire_at_ms)
    async_submit_to_raft(idx, {:setrange, key, offset, value})
    {:ok, byte_size(new_val)}
  end

  # Read the live value for a key (treating expired TTL as missing).
  defp read_live(ctx, idx, key) do
    keydir = elem(ctx.keydir_refs, idx)
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, value, exp, _, _, _, _}]
      when value != nil and (exp == 0 or exp > now) ->
        {:hit, value, exp}

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
        v when is_integer(v) -> Integer.to_string(v)
        v when is_float(v) -> Float.to_string(v)
        v when is_binary(v) ->
          if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
      end

    disk_value = to_disk_binary(value)

    track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

    if value_for_ets == nil do
      # Large — sync NIF write then ETS with real offset.
      case nif_append_batch_with_file(idx, [{key, disk_value, expire_at_ms}]) do
        {:ok, file_id, [{offset, _record_size}]} ->
          :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)})

        {:error, _} ->
          :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_value)})
      end
    else
      {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)
      :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})
      Ferricstore.Store.BitcaskWriter.write(idx, file_path, file_id, keydir, key, disk_value, expire_at_ms)
    end

    wv_size = :counters.info(ctx.write_version).size
    if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)
    :ok
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
    case Float.parse(v) do
      {f, _} -> {:ok, f}
      :error ->
        case Integer.parse(v) do
          {i, ""} -> {:ok, i * 1.0}
          _ -> :error
        end
    end
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
    keydir = elem(ctx.keydir_refs, idx)
    value_for_ets = case value do
      v when is_integer(v) -> Integer.to_string(v)
      v when is_float(v) -> Float.to_string(v)
      v when is_binary(v) ->
        if byte_size(v) > ctx.hot_cache_max_value_size, do: nil, else: v
    end
    disk_value = to_disk_binary(value)
    # Track off-heap binary bytes for MemoryGuard accuracy
    track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

    if value_for_ets == nil do
      # Large value: sync NIF write to get offset, then ETS with real location.
      # Cannot use async BitcaskWriter because ETS value is nil (too large for
      # hot cache) and readers would see nil until the async write completes.
      case nif_append_batch_with_file(idx, [{key, disk_value, expire_at_ms}]) do
        {:ok, file_id, [{offset, _record_size}]} ->
          :ets.insert(keydir, {key, nil, expire_at_ms, LFU.initial(), file_id, offset, byte_size(disk_value)})
          size = :counters.info(ctx.write_version).size
          if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
          async_submit_to_raft(idx, {:put, key, value, expire_at_ms})
          :ok

        {:error, reason} ->
          {:error, "ERR disk write failed: #{inspect(reason)}"}
      end
    else
      # Small value: ETS insert only. Bitcask write deferred to state machine
      # apply (flush_pending_writes) — avoids per-key NIF overhead in Router.
      :ets.insert(keydir, {key, value_for_ets, expire_at_ms, LFU.initial(), :pending, 0, 0})
      size = :counters.info(ctx.write_version).size
      if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
      async_submit_to_raft(idx, {:put, key, value, expire_at_ms})
      :ok
    end
  end

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v

  # NIF batch write with retry on stale active file (ENOENT after rotation).
  # Returns {:ok, file_id, locations} or {:error, reason}.
  defp nif_append_batch_with_file(idx, batch) do
    {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)

    case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(file_path, batch) do
      {:ok, locations} ->
        {:ok, file_id, locations}

      {:error, reason} when is_binary(reason) ->
        if String.contains?(reason, "No such file") do
          {fresh_id, fresh_path, _} = Ferricstore.Store.ActiveFile.get(idx)

          case Ferricstore.Bitcask.NIF.v2_append_batch_nosync(fresh_path, batch) do
            {:ok, locations} -> {:ok, fresh_id, locations}
            {:error, _} = err -> err
          end
        else
          {:error, reason}
        end

      {:error, _} = err ->
        err
    end
  end

  # -- Keydir binary memory tracking --
  # Only counts binaries > 64 bytes (refc binaries, off-heap).
  # Smaller binaries are inlined in the ETS tuple and counted by :ets.info(:memory).

  defp track_keydir_binary_insert(ctx, idx, keydir, key, new_val) do
    new_bytes = offheap_size(key) + offheap_size(new_val)
    old_bytes = case :ets.lookup(keydir, key) do
      [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
      _ -> 0
    end
    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ctx.keydir_binary_bytes, idx + 1, delta)
  end

  defp track_keydir_binary_delete(ctx, idx, keydir, key) do
    bytes = case :ets.lookup(keydir, key) do
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

  # Submit an async write to the shard's Batcher, which batches many async
  # commands into a single `ra.pipeline_command({:batch, [{:async, cmd}, ...]})`
  # call. Router has already persisted locally (ETS + Bitcask for big values)
  # by the time this is called — the Raft submission is for replication only.
  # Fire-and-forget: Router has already returned :ok to the caller.
  defp async_submit_to_raft(idx, command) do
    Ferricstore.Raft.Batcher.async_submit(idx, command)
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
  @spec get_file_ref(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer(), non_neg_integer()} | nil
  def get_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        # Hot key — value is in ETS, sendfile not applicable.
        nil

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — return file ref directly, no GenServer needed.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
        # Adjust offset to skip header and key bytes (sendfile needs value offset).
        value_offset = offset + 26 + byte_size(key)
        {path, value_offset, value_size}

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — fall back to GenServer.
        GenServer.call(resolve_shard(ctx, idx), {:get_file_ref, key})

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
  @spec get_with_file_ref(FerricStore.Instance.t(), binary()) :: {:hot, binary()} | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()} | {:cold_value, binary()} | :miss
  def get_with_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Value is on disk — return file ref for potential sendfile.
        # Use DataDir directly to avoid GenServer roundtrip.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
        Stats.record_cold_read(ctx, key)
        {:cold_ref, path, offset, value_size}

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but no valid file ref — ask GenServer
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})
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
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})
        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          Stats.incr_keyspace_misses(ctx)
          :miss
        end
    end
  end

  # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
  # Single lookup provides everything needed — no second ETS read for bookkeeping.
  defp ets_get_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          {:hit, value, lfu}

        [{^key, nil, 0, _lfu, fid, off, vsize}] ->
          {:cold, fid, off, vsize}

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:hit, value, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}] when exp > now ->
          {:cold, fid, off, vsize}

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

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — value evicted from ETS but disk location known.
        # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
        # The ETS entry has valid file_id/offset from when the write committed,
        # so pread works without flushing pending async writes.
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case Ferricstore.Bitcask.NIF.v2_pread_at(path, offset) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            # Warm ETS: promote back to hot if value fits in cache
            warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset)
            value

          _ ->
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but invalid file ref (file_id=0 or value_size=0) — ask GenServer.
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})

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
        result = GenServer.call(resolve_shard(ctx, idx), {:get, key})

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

    Enum.map(keys, fn key ->
      idx = shard_for(ctx, key)
      keydir = resolve_keydir(ctx, idx)

      case ets_get_full(ctx, idx, keydir, key, now) do
        {:hit, value, lfu} ->
          sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
          value

        {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
          shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
          path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

          case Ferricstore.Bitcask.NIF.v2_pread_at(path, offset) do
            {:ok, value} ->
              Stats.record_cold_read(ctx, key)
              warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset)
              value
            _ ->
              nil
          end

        _ ->
          Stats.incr_keyspace_misses(ctx)
          nil
      end
    end)
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

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        # Recover expire_at_ms from ETS (ets_get_full returns lfu, not exp).
        expire_at_ms =
          try do
            case :ets.lookup(keydir, key) do
              [{^key, _val, exp, _lfu, _fid, _off, _vsize}] -> exp
              _ -> 0
            end
          rescue
            ArgumentError -> 0
          end
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size} when file_id > 0 and value_size > 0 ->
        # Cold key — read value from disk directly, return with expire_at_ms.
        expire_at_ms =
          try do
            case :ets.lookup(keydir, key) do
              [{^key, _val, exp, _lfu, _fid, _off, _vsize}] -> exp
              _ -> 0
            end
          rescue
            ArgumentError -> 0
          end

        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
        path = Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")

        case Ferricstore.Bitcask.NIF.v2_pread_at(path, offset) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — ask GenServer.
        Stats.record_cold_read(ctx, key)
        GenServer.call(resolve_shard(ctx, idx), {:get_meta, key})

      :expired ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :miss ->
        Stats.incr_keyspace_misses(ctx)
        nil

      :no_table ->
        Stats.record_cold_read(ctx, key)
        GenServer.call(resolve_shard(ctx, idx), {:get_meta, key})
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
  defp warm_ets_after_cold_read(ctx, keydir, key, value, _file_id, _offset) do
    # Skip promotion when under memory pressure — prevents evict/re-promote
    # thrashing where MemoryGuard evicts values and cold reads immediately
    # re-cache them. skip_promotion? is set at :pressure level (85%+).
    skip_promotion = :atomics.get(ctx.pressure_flags, 3) == 1

    if byte_size(value) <= ctx.hot_cache_max_value_size and not skip_promotion do
      try do
        :ets.update_element(keydir, key, {2, value})
      rescue
        ArgumentError -> :ok
      end
    end
  end

  @max_key_size 65_535
  @max_value_size 512 * 1024 * 1024

  @spec max_value_size() :: pos_integer()
  @doc "Returns the maximum allowed value size in bytes."
  def max_value_size, do: @max_value_size

  @doc """
  Batch async PUT for pipelined SET commands without options.

  Takes a list of `{key, value}` tuples. All keys must target async
  durability namespaces. Groups by shard, does batch ETS inserts per
  shard, fires BitcaskWriter casts and Raft submissions individually
  (they batch internally). Returns `:ok` or `{:error, reason}`.

  Caller must validate key/value sizes and check pressure flags before
  calling. This skips all per-key validation for speed.
  """
  @spec batch_async_put(FerricStore.Instance.t(), [{binary(), binary()}]) :: :ok | {:error, binary()}
  def batch_async_put(ctx, kv_pairs) do
    lfu_val = LFU.initial()
    hot_max = ctx.hot_cache_max_value_size
    wv_size = :counters.info(ctx.write_version).size

    kv_pairs
    |> Enum.group_by(fn {key, _value} -> shard_for(ctx, key) end)
    |> Enum.each(fn {idx, shard_kvs} ->
      keydir = elem(ctx.keydir_refs, idx)

      {ets_tuples, raft_cmds, large_disk_batch} =
        Enum.reduce(shard_kvs, {[], [], []}, fn {key, value}, {ets_acc, raft_acc, disk_acc} ->
          value_for_ets = if byte_size(value) > hot_max, do: nil, else: value
          track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)

          ets_tuple = {key, value_for_ets, 0, lfu_val, :pending, 0, 0}
          raft_cmd = {:put, key, value, 0}

          disk_acc =
            if value_for_ets == nil do
              [{key, value, 0} | disk_acc]
            else
              disk_acc
            end

          {[ets_tuple | ets_acc], [raft_cmd | raft_acc], disk_acc}
        end)

      :ets.insert(keydir, ets_tuples)

      if large_disk_batch != [] do
        reversed = Enum.reverse(large_disk_batch)
        case nif_append_batch_with_file(idx, reversed) do
          {:ok, file_id, locations} ->
            Enum.zip(reversed, locations)
            |> Enum.each(fn {{key, value, _exp}, {offset, _rec_size}} ->
              :ets.update_element(keydir, key, [{5, file_id}, {6, offset}, {7, byte_size(value)}])
            end)
          {:error, reason} ->
            Enum.each(large_disk_batch, fn {key, _, _} -> :ets.delete(keydir, key) end)
            throw({:disk_error, reason})
        end
      end

      Ferricstore.Raft.Batcher.async_submit_batch(idx, raft_cmds)

      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, length(shard_kvs))
    end)

    :ok
  catch
    :throw, {:disk_error, reason} ->
      {:error, "ERR disk write failed: #{inspect(reason)}"}
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
  @spec batch_quorum_put(FerricStore.Instance.t(), [{binary(), binary()}]) :: [:ok | {:error, binary()}]
  def batch_quorum_put(ctx, kv_pairs) do
    wv_size = :counters.info(ctx.write_version).size
    me = self()

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
        ref = make_ref()
        Ferricstore.Raft.Batcher.write_batch(shard_idx, Enum.reverse(cmds), {me, ref})
        {ref, shard_idx, Enum.reverse(indices)}
      end)

    results = collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))

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
    |> Enum.map(fn i -> Map.get(results, i, {:error, "ERR write timeout"}) end)
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

    receive do
      {ref, result} when is_reference(ref) ->
        case List.keytake(remaining_refs, ref, 0) do
          {{^ref, shard_idx, indices}, rest} ->
            acc = apply_shard_results(result, indices, shard_idx, wv_size, ctx, acc)
            collect_shard_replies(rest, wv_size, ctx, acc, start)

          nil ->
            collect_shard_replies(remaining_refs, wv_size, ctx, acc, start)
        end
    after
      timeout -> acc
    end
  end

  defp apply_shard_results({:ok, results}, indices, shard_idx, wv_size, ctx, acc) when is_list(results) do
    ok_count = Enum.count(results, fn r -> r == :ok or (not match?({:error, _}, r)) end)
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
        if shard_idx < wv_size, do: :counters.add(ctx.write_version, shard_idx + 1, length(indices))
        Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)
    end
  end

  # Forward a batch to the leader's node. Used by batch_quorum_put when
  # the local Batcher rejects with :not_leader. Issues an erpc to run
  # batch_quorum_put on the leader, then returns its results.
  defp forward_batch_to_leader(_ctx, leader_node, _shard_idx, kv_pairs) when leader_node == node() do
    Enum.map(kv_pairs, fn _ -> {:error, "ERR not leader, election in progress"} end)
  end

  defp forward_batch_to_leader(_ctx, leader_node, _shard_idx, kv_pairs) do
    try do
      remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)
      :erpc.call(leader_node, __MODULE__, :batch_quorum_put, [remote_ctx, kv_pairs], 10_000)
    catch
      _, reason ->
        require Logger
        Logger.warning("batch forward to #{inspect(leader_node)} failed: #{inspect(reason)}")
        Enum.map(kv_pairs, fn _ -> {:error, "ERR leader unavailable"} end)
    end
  end

  @doc """
  Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
  timestamp in milliseconds; pass `0` for no expiry.
  """
  @spec put(FerricStore.Instance.t(), binary(), binary(), non_neg_integer()) :: :ok | {:error, binary()}
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
    exists_fast?(ctx, key)
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
        [{^key, _val, 0, _lfu, _fid, _off, _vsize}] -> true
        [{^key, _val, exp, _lfu, _fid, _off, _vsize}] when exp > now -> true
        _ -> false
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
  @spec incr(FerricStore.Instance.t(), binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(FerricStore.Instance.t(), binary(), float()) :: {:ok, binary()} | {:error, binary()}
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
  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
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

  @doc """
  Runs a JSON RMW command atomically via Raft. `cmd` is the Redis command
  name (e.g. "JSON.SET"), `args` is the argument list starting with the key.
  The state machine dispatches to `Ferricstore.Commands.Json.handle/3` with
  a state-machine-scoped store, so concurrent callers serialize through
  the Raft log — no lost updates.
  """
  @spec json_op(FerricStore.Instance.t(), binary(), [term()]) :: term()
  def json_op(ctx, cmd, [key | _] = args) do
    raft_write(ctx, shard_for(ctx, key), key, {:json_op, cmd, args})
  end

  @doc """
  Runs a HyperLogLog RMW command (PFADD / PFMERGE) atomically via Raft.
  PFCOUNT is read-only and should not go through this path.
  """
  @spec hll_op(FerricStore.Instance.t(), binary(), [term()]) :: term()
  def hll_op(ctx, cmd, [key | _] = args) do
    raft_write(ctx, shard_for(ctx, key), key, {:hll_op, cmd, args})
  end

  @doc """
  Runs a Bitmap RMW command (BITOP / BITFIELD) atomically via Raft. SETBIT
  has its own `setbit/4` path. GETBIT/BITCOUNT/BITPOS are read-only and do
  not go through this path.
  """
  @spec bitmap_op(FerricStore.Instance.t(), binary(), [term()]) :: term()
  # BITOP reads multiple source keys and writes a destination — the sources
  # may live on different shards. Use CrossShardOp for cross-shard correctness;
  # if all keys (sources + dest) hash to the same shard, the same path
  # collapses to a single-shard quorum write transparently.
  def bitmap_op(ctx, "BITOP", [_op_str, destkey | source_keys] = args) do
    keys_with_roles = [{destkey, :write} | Enum.map(source_keys, &{&1, :read})]

    Ferricstore.CrossShardOp.execute(
      keys_with_roles,
      fn _store ->
        # Dispatch through the normal raft path with the dest's shard as the
        # coordinator. With CrossShardOp's locks held this is now safe even
        # if the source is on a different shard (the read in execute_bitop
        # is consistent because the lock blocks concurrent writers).
        raft_write(ctx, shard_for(ctx, destkey), destkey, {:bitmap_op, "BITOP", args})
      end,
      intent: %{command: :bitop, keys: %{targets: [destkey | source_keys]}}
    )
  end

  def bitmap_op(ctx, cmd, [key | _] = args) do
    raft_write(ctx, shard_for(ctx, key), key, {:bitmap_op, cmd, args})
  end

  @doc """
  Runs a Geo RMW command (GEOADD, GEOSEARCHSTORE) atomically via Raft.
  GEOSEARCHSTORE routes by the destination key; GEOADD routes by the key.
  Read-only ops (GEOPOS, GEODIST, GEOHASH, GEOSEARCH) don't go through here.
  """
  @spec geo_op(FerricStore.Instance.t(), binary(), [term()]) :: term()
  def geo_op(ctx, "GEOSEARCHSTORE", [dest | _] = args) do
    raft_write(ctx, shard_for(ctx, dest), dest, {:geo_op, "GEOSEARCHSTORE", args})
  end

  def geo_op(ctx, cmd, [key | _] = args) do
    raft_write(ctx, shard_for(ctx, key), key, {:geo_op, cmd, args})
  end

  @doc """
  Runs a TDigest RMW command (TDIGEST.ADD / RESET / MERGE / CREATE)
  atomically via Raft. Read-only ops (QUANTILE, CDF, INFO, RANK, etc.)
  stay in the caller process.
  """
  @spec tdigest_op(FerricStore.Instance.t(), binary(), [term()]) :: term()
  def tdigest_op(ctx, "TDIGEST.MERGE", [dest | _] = args) do
    raft_write(ctx, shard_for(ctx, dest), dest, {:tdigest_op, "TDIGEST.MERGE", args})
  end

  def tdigest_op(ctx, cmd, [key | _] = args) do
    raft_write(ctx, shard_for(ctx, key), key, {:tdigest_op, cmd, args})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys(FerricStore.Instance.t()) :: [binary()]
  def keys(ctx) do
    sc = ctx.shard_count
    Enum.flat_map(0..(sc - 1), fn i ->
      GenServer.call(resolve_shard(ctx, i), :keys)
    end)
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
  def dbsize(ctx) do
    sc = ctx.shard_count
    Enum.reduce(0..(sc - 1), 0, fn i, acc ->
      try do
        acc + :ets.info(resolve_keydir(ctx, i), :size)
      rescue
        ArgumentError -> acc
      end
    end)
  end

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def get_version(ctx, key) do
    GenServer.call(resolve_shard(ctx, shard_for(ctx, key)), {:get_version, key})
  end

  @doc """
  Returns the keydir disk location for a key, or `:miss`.

  Reads the `{file_id, offset, value_size}` fields directly from the keydir
  ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
  for live keys, or `:miss` if the key is not in the keydir or is expired.

  Used by sendfile zero-copy and STRLEN on cold keys.
  """
  @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) :: {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :miss
  def get_keydir_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{_, _, 0, _, fid, off, vsize}] ->
          {:ok, {fid, off, vsize}}

        [{_, _, exp, _, fid, off, vsize}] when exp > now ->
          {:ok, {fid, off, vsize}}

        [{_, _, _exp, _, _fid, _off, _vsize}] ->
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

  @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) :: 1 | 0 | nil
  def cas(ctx, key, expected, new_value, ttl_ms) do
    expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
    raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
  end

  @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
  end

  @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(ctx, key, owner) do
    raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
  end

  @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) :: 1 | {:error, binary()}
  def extend(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
  end

  @spec ratelimit_add(FerricStore.Instance.t(), binary(), pos_integer(), pos_integer(), pos_integer()) :: [term()]
  def ratelimit_add(ctx, key, window_ms, max, count) do
    now_ms = HLC.now_ms()
    raft_write(ctx, shard_for(ctx, key), key, {:ratelimit_add, key, window_ms, max, count, now_ms})
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

      _ ->
        shard = elem(ctx.shard_names, idx)
        GenServer.call(shard, {:compound_get, redis_key, compound_key})
    end
  end

  @spec compound_get_meta(FerricStore.Instance.t(), binary(), binary()) :: {binary(), non_neg_integer()} | nil
  def compound_get_meta(ctx, redis_key, compound_key) do
    shard = elem(ctx.shard_names, shard_for(ctx, redis_key))
    GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
  end

  @spec compound_put(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
    idx = shard_for(ctx, redis_key)

    case durability_for_key(ctx, redis_key) do
      :quorum ->
        quorum_write(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})

      :async ->
        async_compound_put(ctx, idx, redis_key, compound_key, value, expire_at_ms)
    end
  end

  @spec compound_delete(FerricStore.Instance.t(), binary(), binary()) :: :ok | {:error, term()}
  def compound_delete(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)

    case durability_for_key(ctx, redis_key) do
      :quorum ->
        quorum_write(ctx, idx, {:compound_delete, redis_key, compound_key})

      :async ->
        async_compound_delete(ctx, idx, compound_key)
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
  # Promotion check is intentionally skipped on the async path. Hashes that
  # grow large in an async namespace stay in the shared Bitcask log instead
  # of being promoted to a dedicated file. Acceptable trade-off; users who
  # want promotion can configure the namespace as quorum.
  # ---------------------------------------------------------------------------

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
        try do
          GenServer.call(
            :"Ferricstore.Store.RmwCoordinator.#{idx}",
            {:rmw, cmd},
            10_000
          )
        catch
          :exit, {:timeout, _} -> {:error, "ERR list_op timeout"}
          :exit, {:noproc, _} -> {:error, "ERR RMW worker unavailable"}
          :exit, _ -> {:error, "ERR RMW worker crashed"}
        end
    end
  end

  @doc """
  Executes a list_op inline under a held latch. Called from
  `Router.async_list_op` (fast path) and `RmwCoordinator.handle_call`
  (contended path). The latch guarantees exclusive access to the list's
  compound keys.

  Uses `ListOps.execute/3` with an origin-local compound store that
  writes ETS + casts BitcaskWriter for each mutation. Then submits the
  original `{:list_op, key, op}` command to Raft via
  `Batcher.async_submit` so replicas re-execute against their own state
  in Raft log order (deterministic convergence).
  """
  @spec execute_list_op_inline(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
  def execute_list_op_inline(ctx, idx, {:list_op, key, operation} = cmd) do
    :telemetry.execute([:ferricstore, :rmw, :worker_list_op], %{}, %{shard_index: idx})
    store = build_origin_compound_store(ctx, idx)
    # Resolve the module at runtime to avoid the compile-time cycle
    # ListOps → Ops → Router → ListOps. `list_ops_mod/0` returns an atom
    # that xref cannot trace through.
    result = :erlang.apply(list_ops_mod(), :execute, [key, store, operation])

    wv_size = :counters.info(ctx.write_version).size
    if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

    async_submit_to_raft(idx, cmd)
    result
  end

  def execute_list_op_inline(ctx, idx, {:list_op_lmove, src_key, dst_key, from_dir, to_dir} = cmd) do
    store = build_origin_compound_store(ctx, idx)
    result =
      :erlang.apply(list_ops_mod(), :execute_lmove, [src_key, dst_key, store, from_dir, to_dir])

    wv_size = :counters.info(ctx.write_version).size
    if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

    async_submit_to_raft(idx, cmd)
    result
  end

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
    {file_id, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)

    %{
      compound_get: fn _redis_key, compound_key ->
        now = Ferricstore.HLC.now_ms()
        case :ets.lookup(keydir, compound_key) do
          [{_, value, exp, _, _, _, _}]
          when value != nil and (exp == 0 or exp > now) ->
            value

          _ ->
            nil
        end
      end,
      compound_put: fn _redis_key, compound_key, value, exp ->
        disk_value = to_disk_binary(value)
        :ets.insert(keydir, {compound_key, value, exp, LFU.initial(), :pending, 0, byte_size(disk_value)})
        Ferricstore.Store.BitcaskWriter.write(idx, file_path, file_id, keydir, compound_key, disk_value, exp)
        :ok
      end,
      compound_delete: fn _redis_key, compound_key ->
        :ets.delete(keydir, compound_key)
        Ferricstore.Store.BitcaskWriter.delete(idx, file_path, compound_key)
        :ok
      end,
      compound_scan: fn _redis_key, prefix ->
        Ferricstore.Store.Shard.ETS.prefix_scan_entries(keydir, prefix, file_path)
        |> Enum.sort_by(fn {field, _} -> field end)
      end,
      compound_count: fn _redis_key, prefix ->
        Ferricstore.Store.Shard.ETS.prefix_count_entries(keydir, prefix)
      end,
      exists?: fn k ->
        case :ets.lookup(keydir, k) do
          [_] -> true
          [] -> false
        end
      end
    }
  end

  defp async_compound_put(ctx, idx, _redis_key, compound_key, value, expire_at_ms) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      install_rmw_value(ctx, idx, compound_key, value, expire_at_ms)
      async_submit_to_raft(idx, {:put, compound_key, value, expire_at_ms})
      :ok
    end
  end

  defp async_compound_delete(ctx, idx, compound_key) do
    size = :atomics.info(ctx.disk_pressure).size
    under_pressure = idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1

    if under_pressure do
      {:error, "ERR disk pressure on shard #{idx}, rejecting async write"}
    else
      keydir = elem(ctx.keydir_refs, idx)
      track_keydir_binary_delete(ctx, idx, keydir, compound_key)
      :ets.delete(keydir, compound_key)

      {_, file_path, _} = Ferricstore.Store.ActiveFile.get(idx)
      Ferricstore.Store.BitcaskWriter.delete(idx, file_path, compound_key)

      wv_size = :counters.info(ctx.write_version).size
      if idx < wv_size, do: :counters.add(ctx.write_version, idx + 1, 1)

      async_submit_to_raft(idx, {:delete, compound_key})
      :ok
    end
  end

  @spec compound_scan(FerricStore.Instance.t(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(ctx, redis_key, prefix) do
    shard = elem(ctx.shard_names, shard_for(ctx, redis_key))
    GenServer.call(shard, {:compound_scan, redis_key, prefix})
  end

  @spec compound_count(FerricStore.Instance.t(), binary(), binary()) :: non_neg_integer()
  def compound_count(ctx, redis_key, prefix) do
    shard = elem(ctx.shard_names, shard_for(ctx, redis_key))
    GenServer.call(shard, {:compound_count, redis_key, prefix})
  end

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
    src_idx = shard_for(ctx, key)
    dst_idx = shard_for(ctx, destination)

    if src_idx == dst_idx do
      raft_write(ctx, src_idx, key, {:list_op_lmove, key, destination, from_dir, to_dir})
    else
      # Cross-shard: pop from source, push to destination
      case raft_write(ctx, src_idx, key, {:list_op, key, {:pop_for_move, from_dir}}) do
        nil -> nil
        {:error, _} = err -> err
        element ->
          push_op = if to_dir == :left, do: {:lpush, [element]}, else: {:rpush, [element]}
          case raft_write(ctx, dst_idx, destination, {:list_op, destination, push_op}) do
            {:error, _} = err -> err
            _length -> element
          end
      end
    end
  end

  def list_op(ctx, key, operation) do
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, {:list_op, key, operation})
  end
end
