defmodule Ferricstore.Store.AsyncRmwTest do
  @moduledoc """
  TDD tests for the async RMW latch+worker design
  (see docs/async-rmw-design.md).

  Target behavior:
  - Uncontended RMW runs inline in caller process under per-key ETS latch
    (:ets.insert_new). No GenServer hop, ~15μs p50.
  - Contended RMW (another caller holds the latch) falls through to
    Ferricstore.Store.RmwCoordinator, which serializes via its mailbox.
  - Concurrent RMWs on the same key never lose updates; each caller gets
    a distinct, correctly-ordered result.
  - Router.async_submit replicates the DELTA command (e.g. {:incr, k, δ})
    so replicas apply in Raft log order for deterministic convergence.
  - Latch leaks on caller crash are cleaned up by a periodic sweep.
  - SET vs RMW on the same key is last-write-wins (async semantics).

  These tests will fail against the current quorum_write fallback path
  (commit 88ff185). They pass once the latch+worker design is in place.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BitcaskWriter
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.RmwCoordinator
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  @ns "rmw_async"

  setup do
    ShardHelpers.flush_all_keys()
    Ferricstore.NamespaceConfig.set(@ns, "durability", "async")

    on_exit(fn ->
      Ferricstore.NamespaceConfig.set(@ns, "durability", "quorum")
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  defp ctx, do: FerricStore.Instance.get(:default)
  defp ukey(base), do: "#{@ns}:#{base}_#{:erlang.unique_integer([:positive])}"

  defp same_shard_key(base, shard_idx) do
    Stream.iterate(1, &(&1 + 1))
    |> Enum.find_value(fn n ->
      key = ukey("#{base}_#{n}")
      if Router.shard_for(ctx(), key) == shard_idx, do: key
    end)
  end

  defp hash_key(base) do
    key = ukey(base)
    assert :ok = FerricStore.hset(key, %{"field" => "value"})
    key
  end

  defp assert_hash_intact(key) do
    assert {:ok, "value"} = FerricStore.hget(key, "field")
    assert_no_plain_key(key)
  end

  defp assert_no_plain_key(key) do
    keydir = elem(ctx().keydir_refs, Router.shard_for(ctx(), key))
    assert [] == :ets.lookup(keydir, key)
  end

  describe "instance context on fallback path" do
    test "RmwCoordinator accepts the caller instance context" do
      isolated = minimal_instance_context()

      try do
        key = ukey("isolated_missing_getdel")
        idx = Router.shard_for(isolated, key)

        assert nil == RmwCoordinator.execute(idx, isolated, {:getdel, key})
        assert [] == :ets.lookup(elem(isolated.latch_refs, idx), key)
      after
        cleanup_minimal_instance_context(isolated)
      end
    end

    test "RmwCoordinator sweeps stale latches for custom instance contexts it has seen" do
      isolated = minimal_instance_context()

      try do
        idx = 0
        latch_tab = elem(isolated.latch_refs, idx)
        stale_key = ukey("custom_stale_latch")

        dead_holder =
          spawn(fn ->
            :ok
          end)

        ref = Process.monitor(dead_holder)
        assert_receive {:DOWN, ^ref, :process, ^dead_holder, :normal}, 500

        :ets.insert(latch_tab, {stale_key, dead_holder})

        assert nil ==
                 RmwCoordinator.execute(idx, isolated, {:getdel, ukey("register_custom_ctx")})

        assert :ok = RmwCoordinator.sweep_latches(idx)

        assert [] == :ets.lookup(latch_tab, stale_key)
      after
        cleanup_minimal_instance_context(isolated)
      end
    end

    test "RmwCoordinator prunes checked-in custom instance contexts after sweep" do
      isolated = minimal_instance_context()
      idx = 0
      name = isolated.name

      assert nil == RmwCoordinator.execute(idx, isolated, {:getdel, ukey("register_prune_ctx")})
      assert Map.has_key?(:sys.get_state(RmwCoordinator.name(idx)).contexts, name)

      cleanup_minimal_instance_context(isolated)

      assert :ok = RmwCoordinator.sweep_latches(idx)
      refute Map.has_key?(:sys.get_state(RmwCoordinator.name(idx)).contexts, name)
    end

    test "RmwCoordinator does not serialize same-key RMW across different instances" do
      ctx_a = IsolatedInstance.checkout(shard_count: 1)
      ctx_b = IsolatedInstance.checkout(shard_count: 1)
      key = ukey("cross_instance_same_key")
      idx = 0

      holder_a = latch_holder()
      holder_b = latch_holder()

      try do
        :ets.insert(elem(ctx_a.latch_refs, idx), {key, holder_a})
        :ets.insert(elem(ctx_b.latch_refs, idx), {key, holder_b})

        task_a = Task.async(fn -> RmwCoordinator.execute(idx, ctx_a, {:getdel, key}) end)
        assert Task.yield(task_a, 50) == nil

        task_b = Task.async(fn -> RmwCoordinator.execute(idx, ctx_b, {:getdel, key}) end)
        assert Task.yield(task_b, 50) == nil

        ref_b = Process.monitor(holder_b)
        send(holder_b, :release)
        assert_receive {:DOWN, ^ref_b, :process, ^holder_b, :normal}, 500

        assert {:ok, nil} == Task.yield(task_b, 500)
        assert Task.yield(task_a, 50) == nil
      after
        release_latch_holder(holder_a)
        release_latch_holder(holder_b)

        IsolatedInstance.checkin(ctx_a)
        IsolatedInstance.checkin(ctx_b)
      end
    end

    test "Router passes ctx when falling back to the RMW worker" do
      # The worker is registered globally per shard, so the caller context must
      # be part of the GenServer message. Otherwise contended async commands
      # silently rehydrate the default instance inside RmwCoordinator.
      path = Path.expand("../../../lib/ferricstore/store/router.ex", __DIR__)

      assert rmw_worker_calls_missing_ctx(path) == []
    end
  end

  # ---------------------------------------------------------------------------
  # Uncontended single-caller correctness — one assertion per RMW command
  # ---------------------------------------------------------------------------

  describe "uncontended RMW correctness" do
    test "INCR on nonexistent key returns delta and stores it" do
      k = ukey("incr_nokey")
      assert {:ok, 5} = Router.incr(ctx(), k, 5)
      assert Router.get(ctx(), k) == "5"
    end

    test "INCR on existing integer returns sum" do
      k = ukey("incr_existing")
      :ok = Router.put(ctx(), k, "10", 0)
      assert {:ok, 13} = Router.incr(ctx(), k, 3)
      assert Router.get(ctx(), k) == "13"
    end

    test "INCR on non-integer string returns error, leaves value unchanged" do
      k = ukey("incr_bad")
      :ok = Router.put(ctx(), k, "hello", 0)
      assert {:error, _msg} = Router.incr(ctx(), k, 1)
      assert Router.get(ctx(), k) == "hello"
    end

    test "INCR does not become locally visible when Batcher is overloaded" do
      k = ukey("incr_overloaded")
      idx = Router.shard_for(ctx(), k)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:incr, k, 1}}],
          0
        )
      end

      assert {:error, "ERR async replication overloaded"} = Router.incr(ctx(), k, 1)
      assert Router.get(ctx(), k) == nil
    end

    test "DELETE does not remove the local value when Batcher is overloaded" do
      k = ukey("delete_overloaded")
      idx = Router.shard_for(ctx(), k)

      :ok = Router.put(ctx(), k, "keep", 0)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:delete, k}}],
          0
        )
      end

      assert {:error, "ERR async replication overloaded"} = Router.delete(ctx(), k)
      assert Router.get(ctx(), k) == "keep"
    end

    test "APPEND on nonexistent key creates it, returns byte size" do
      k = ukey("append_nokey")
      assert {:ok, 5} = Router.append(ctx(), k, "hello")
      assert Router.get(ctx(), k) == "hello"
    end

    test "APPEND on existing value concatenates" do
      k = ukey("append_existing")
      :ok = Router.put(ctx(), k, "hello", 0)
      assert {:ok, 11} = Router.append(ctx(), k, " world")
      assert Router.get(ctx(), k) == "hello world"
    end

    test "GETSET returns old value and installs new" do
      k = ukey("getset")
      :ok = Router.put(ctx(), k, "old", 0)
      assert "old" = Router.getset(ctx(), k, "new")
      assert Router.get(ctx(), k) == "new"
    end

    test "GETSET on nonexistent returns nil and installs" do
      k = ukey("getset_new")
      assert nil == Router.getset(ctx(), k, "val")
      assert Router.get(ctx(), k) == "val"
    end

    test "GETDEL returns value and deletes key" do
      k = ukey("getdel")
      :ok = Router.put(ctx(), k, "value", 0)
      assert "value" = Router.getdel(ctx(), k)
      assert Router.get(ctx(), k) == nil
    end

    test "GETDEL persists a single tombstone after async Ra apply" do
      c = ctx()
      k = ukey("getdel_single_tombstone")
      idx = Router.shard_for(c, k)

      :ok = Router.put(c, k, "value", 0)
      :ok = Batcher.flush(idx)
      :ok = BitcaskWriter.flush_all(c.shard_count)

      assert "value" = Router.getdel(c, k)
      assert Router.get(c, k) == nil

      :ok = Batcher.flush(idx)
      :ok = BitcaskWriter.flush_all(c.shard_count)

      assert tombstone_count(c, k) == 1
    end

    test "GETDEL on nonexistent returns nil" do
      k = ukey("getdel_new")
      assert nil == Router.getdel(ctx(), k)
    end
  end

  describe "compound type protection" do
    test "INCR rejects hash keys and does not create a plain value" do
      key = hash_key("incr_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.incr(key)
      assert_hash_intact(key)
    end

    test "INCRBYFLOAT rejects hash keys and does not create a plain value" do
      key = hash_key("incr_float_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.incr_by_float(key, 1.5)
      assert_hash_intact(key)
    end

    test "APPEND rejects hash keys and does not create a plain value" do
      key = hash_key("append_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.append(key, "suffix")
      assert_hash_intact(key)
    end

    test "GETSET rejects hash keys and does not create a plain value" do
      key = hash_key("getset_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.getset(key, "replacement")
      assert_hash_intact(key)
    end

    test "GETDEL rejects hash keys and does not remove the hash" do
      key = hash_key("getdel_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.getdel(key)
      assert_hash_intact(key)
    end

    test "GETEX rejects hash keys and does not create a plain value" do
      key = hash_key("getex_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.getex(key, ttl: 60_000)
      assert_hash_intact(key)
    end

    test "SETRANGE rejects hash keys and does not create a plain value" do
      key = hash_key("setrange_hash")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.setrange(key, 0, "x")
      assert_hash_intact(key)
    end
  end

  # ---------------------------------------------------------------------------
  # Path selection — which path did each RMW take?
  # ---------------------------------------------------------------------------

  describe "path selection telemetry" do
    test "uncontended single RMW takes the latch path" do
      key = ukey("path_uncontended")
      :ok = Router.put(ctx(), key, "0", 0)

      handler_id = {:rmw_test, :uncontended_latch}

      _ =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :rmw, :latch],
            [:ferricstore, :rmw, :worker]
          ],
          fn event, _meas, _meta, test_pid ->
            send(test_pid, {:rmw_path, event})
          end,
          self()
        )

      try do
        {:ok, _} = Router.incr(ctx(), key, 1)
        assert_receive {:rmw_path, [:ferricstore, :rmw, :latch]}, 500
        refute_received {:rmw_path, [:ferricstore, :rmw, :worker]}
      after
        :telemetry.detach(handler_id)
      end
    end

    test "contended RMWs on the same key use the worker path" do
      key = ukey("path_contended")
      :ok = Router.put(ctx(), key, "0", 0)

      handler_id = {:rmw_test, :contended_worker}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :rmw, :worker],
          fn _event, _meas, _meta, test_pid ->
            send(test_pid, :worker_path_taken)
          end,
          self()
        )

      try do
        # Many concurrent callers on the same key should force at least
        # one caller onto the worker path.
        tasks =
          for _ <- 1..50 do
            Task.async(fn -> Router.incr(ctx(), key, 1) end)
          end

        Task.await_many(tasks, 10_000)

        assert_receive :worker_path_taken, 2_000
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity under concurrency (the bug this whole design addresses)
  # ---------------------------------------------------------------------------

  describe "concurrent RMW atomicity" do
    test "50 concurrent INCRs on the same key sum to +50" do
      key = ukey("concurrent_50")
      :ok = Router.put(ctx(), key, "0", 0)

      tasks =
        for _ <- 1..50 do
          Task.async(fn -> Router.incr(ctx(), key, 1) end)
        end

      results = Task.await_many(tasks, 10_000)

      # Every task got {:ok, N}; no errors.
      assert Enum.all?(results, fn
               {:ok, n} when is_integer(n) -> true
               _ -> false
             end)

      # The 50 returned values are a permutation of 1..50 (every apply
      # saw a distinct moment in the serialization order).
      ns =
        Enum.map(results, fn {:ok, n} -> n end)
        |> Enum.sort()

      assert ns == Enum.to_list(1..50)

      # Final ETS value reflects the total.
      assert Router.get(ctx(), key) == "50"
    end

    @tag timeout: 120_000
    test "1000 INCRs from 25 concurrent tasks sum to 1000" do
      key = ukey("concurrent_1000")
      :ok = Router.put(ctx(), key, "0", 0)

      tasks =
        for _ <- 1..25 do
          Task.async(fn ->
            for _ <- 1..40 do
              incr_with_retry(ctx(), key, 1)
            end
          end)
        end

      results = Task.await_many(tasks, 90_000) |> List.flatten()

      assert Enum.all?(results, fn
               {:ok, n} when is_integer(n) -> true
               _ -> false
             end)

      Ferricstore.Test.Utils.eventually(
        fn ->
          assert Router.get(ctx(), key) == "1000"
        end,
        5000
      )
    end

    test "concurrent APPENDs produce a string of the correct total length" do
      key = ukey("concurrent_append")

      tasks =
        for _ <- 1..20 do
          Task.async(fn -> Router.append(ctx(), key, "x") end)
        end

      Task.await_many(tasks, 10_000)

      final = Router.get(ctx(), key)
      assert is_binary(final)
      assert byte_size(final) == 20
      assert final == String.duplicate("x", 20)
    end

    test "concurrent distinct-key INCRs all succeed via latch path" do
      handler_id = {:rmw_test, :distinct_keys}

      counts = :counters.new(2, [:atomics])
      # counts[1] = latch count, counts[2] = worker count

      _ =
        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :rmw, :latch],
            [:ferricstore, :rmw, :worker]
          ],
          fn
            [:ferricstore, :rmw, :latch], _, _, c -> :counters.add(c, 1, 1)
            [:ferricstore, :rmw, :worker], _, _, c -> :counters.add(c, 2, 1)
          end,
          counts
        )

      try do
        tasks =
          for i <- 1..50 do
            Task.async(fn ->
              Router.incr(ctx(), ukey("distinct_#{i}"), 1)
            end)
          end

        Task.await_many(tasks, 10_000)

        latch_n = :counters.get(counts, 1)
        worker_n = :counters.get(counts, 2)

        # Distinct keys → near-zero contention → mostly latch.
        assert latch_n >= 40,
               "expected ≥40 latch path hits for 50 distinct keys, got #{latch_n}"

        assert worker_n <= 10,
               "expected ≤10 worker path hits for 50 distinct keys, got #{worker_n}"
      after
        :telemetry.detach(handler_id)
      end
    end

    test "blocked worker RMW for one key does not delay another key on the same shard" do
      key_a = ukey("worker_hol_a")
      idx = Router.shard_for(ctx(), key_a)
      key_b = same_shard_key("worker_hol_b", idx)
      latch_tab = elem(ctx().latch_refs, idx)

      assert :ets.insert_new(latch_tab, {key_a, self()})

      task_a = Task.async(fn -> Router.incr(ctx(), key_a, 1) end)

      try do
        assert Task.yield(task_a, 50) == nil

        assert :ets.insert_new(latch_tab, {key_b, self()})
        task_b = Task.async(fn -> Router.incr(ctx(), key_b, 1) end)

        try do
          assert Task.yield(task_b, 50) == nil
          :ets.take(latch_tab, key_b)
          assert {:ok, {:ok, 1}} = Task.yield(task_b, 1_000)
          assert Router.get(ctx(), key_b) == "1"
        after
          :ets.take(latch_tab, key_b)

          if Process.alive?(task_b.pid) do
            Task.shutdown(task_b, :brutal_kill)
          end
        end
      after
        :ets.take(latch_tab, key_a)
      end

      assert {:ok, 1} = Task.await(task_a, 1_000)
    end
  end

  # ---------------------------------------------------------------------------
  # SET + RMW interaction (last-write-wins)
  # ---------------------------------------------------------------------------

  describe "mixed SET + INCR" do
    test "async SET waits behind an existing same-key RMW latch" do
      key = ukey("set_latch")
      idx = Router.shard_for(ctx(), key)
      latch_tab = elem(ctx().latch_refs, idx)

      assert :ets.insert_new(latch_tab, {key, self()})
      task = Task.async(fn -> Router.put(ctx(), key, "1", 0) end)

      try do
        assert Task.yield(task, 50) == nil
        assert Router.get(ctx(), key) == nil
      after
        :ets.take(latch_tab, key)
      end

      assert :ok = Task.await(task, 1_000)
      assert Router.get(ctx(), key) == "1"
    end

    test "async batch SET waits behind existing same-key RMW latches before publishing" do
      key = ukey("batch_set_latch")
      other_key = ukey("batch_set_latch_other")
      idx = Router.shard_for(ctx(), key)
      latch_tab = elem(ctx().latch_refs, idx)

      assert :ets.insert_new(latch_tab, {key, self()})
      task = Task.async(fn -> Router.batch_async_put(ctx(), [{key, "1"}, {other_key, "2"}]) end)

      try do
        assert Task.yield(task, 50) == nil
        assert Router.get(ctx(), key) == nil
        assert Router.get(ctx(), other_key) == nil
      after
        :ets.take(latch_tab, key)
      end

      assert :ok = Task.await(task, 1_000)
      assert Router.get(ctx(), key) == "1"
      assert Router.get(ctx(), other_key) == "2"
    end

    test "async DELETE waits behind an existing same-key RMW latch" do
      key = ukey("delete_latch")
      :ok = Router.put(ctx(), key, "1", 0)
      idx = Router.shard_for(ctx(), key)
      latch_tab = elem(ctx().latch_refs, idx)

      assert :ets.insert_new(latch_tab, {key, self()})
      task = Task.async(fn -> Router.delete(ctx(), key) end)

      try do
        assert Task.yield(task, 50) == nil
        assert Router.get(ctx(), key) == "1"
      after
        :ets.take(latch_tab, key)
      end

      assert :ok = Task.await(task, 1_000)
      assert Router.get(ctx(), key) == nil
    end

    test "concurrent SETs and INCRs on same key never crash; final value is valid" do
      key = ukey("mixed")
      :ok = Router.put(ctx(), key, "0", 0)

      setters =
        for i <- 1..25 do
          Task.async(fn -> Router.put(ctx(), key, "literal_#{i}", 0) end)
        end

      incrementers =
        for _ <- 1..25 do
          Task.async(fn -> Router.incr(ctx(), key, 1) end)
        end

      _set_results = Task.await_many(setters, 10_000)
      incr_results = Task.await_many(incrementers, 10_000)

      # All INCRs return either a valid integer or an error (because a SET
      # landed a non-integer in between). None crash.
      assert Enum.all?(incr_results, fn
               {:ok, n} when is_integer(n) -> true
               {:error, _msg} -> true
               _ -> false
             end)

      final = Router.get(ctx(), key)
      # Last write wins: could be an integer string (from INCR) or
      # "literal_N" (from SET). Don't assert which — just valid shape.
      assert is_binary(final)
    end
  end

  # ---------------------------------------------------------------------------
  # Latency budget
  # ---------------------------------------------------------------------------

  describe "latency" do
    @tag :latency
    test "uncontended async INCR p50 under 500μs" do
      # Generous threshold — the design targets ~15μs p50 but CI + ETS
      # write_concurrency contention with other tests can push this up.
      # 500μs is comfortably below the 2-7ms quorum baseline, confirming
      # the latch path is faster than the fallback.
      key_prefix = "lat_#{:erlang.unique_integer([:positive])}"

      for i <- 1..20 do
        Router.put(ctx(), "#{@ns}:#{key_prefix}_warm_#{i}", "0", 0)
        Router.incr(ctx(), "#{@ns}:#{key_prefix}_warm_#{i}", 1)
      end

      samples =
        for i <- 1..200, reduce: [] do
          acc ->
            key = "#{@ns}:#{key_prefix}_bench_#{i}"
            Router.put(ctx(), key, "0", 0)

            t0 = System.monotonic_time(:microsecond)

            case Router.incr(ctx(), key, 1) do
              {:ok, _} ->
                [System.monotonic_time(:microsecond) - t0 | acc]

              {:error, _} ->
                # Disk pressure under load — skip this sample
                acc
            end
        end

      if length(samples) < 50 do
        # Too many disk pressure rejections under load — skip latency assertion
        :ok
      else
        sorted = Enum.sort(samples)
        p50 = Enum.at(sorted, div(length(sorted), 2))
        p99 = Enum.at(sorted, trunc(length(sorted) * 0.99))

        assert p50 < 500,
               "async INCR p50 #{p50}μs exceeded 500μs budget " <>
                 "(p99 #{p99}μs); latch path probably not engaged"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Latch leak recovery on caller crash
  # ---------------------------------------------------------------------------

  describe "latch leak recovery" do
    @tag timeout: 15_000
    test "RMW on a key whose latch was leaked by a crashed caller recovers" do
      key = ukey("leak")
      :ok = Router.put(ctx(), key, "0", 0)

      # Start a task that acquires the latch and then crashes without
      # releasing it. We simulate this by spawning a linked process that
      # pokes the latch table directly (representing a mid-RMW crash).
      parent = self()

      {:ok, latch_tab} = latch_tab_for_key(ctx(), key)

      # Insert a fake latch owned by a now-dead pid to simulate a leak.
      dead_pid =
        spawn(fn ->
          # Briefly alive then exit. The caller could have died here mid-RMW.
          receive do
            :die -> :ok
          after
            10 -> :ok
          end
        end)

      # Wait for it to die.
      ref = Process.monitor(dead_pid)
      send(dead_pid, :die)
      assert_receive {:DOWN, ^ref, :process, ^dead_pid, _}, 500

      refute Process.alive?(dead_pid)
      :ets.insert(latch_tab, {key, dead_pid})

      # Now a new RMW on the same key should eventually recover (sweeper
      # removes the dead entry; then INCR succeeds). Sweeper runs every
      # 5s — allow up to 10s for recovery.
      Task.start(fn ->
        result = Router.incr(ctx(), key, 1)
        send(parent, {:recovered, result})
      end)

      assert_receive {:recovered, {:ok, _n}}, 10_000
    end
  end

  # ---------------------------------------------------------------------------
  # Worker crash graceful handling
  # ---------------------------------------------------------------------------

  describe "worker crash" do
    @tag timeout: 10_000
    test "killing the RmwCoordinator returns an error then recovers" do
      key = ukey("worker_crash")
      :ok = Router.put(ctx(), key, "0", 0)

      idx = Router.shard_for(ctx(), key)
      pid = Process.whereis(Ferricstore.Store.RmwCoordinator.name(idx))
      assert is_pid(pid)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

      # Next RMW while worker is still restarting may return an error.
      # The error must be graceful (not a raise). Eventually, a retry
      # succeeds.
      result = retry_rmw(fn -> Router.incr(ctx(), key, 1) end, 100)
      assert match?({:ok, _}, result)
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp latch_tab_for_key(ctx, key) do
    idx = Router.shard_for(ctx, key)

    case Map.get(ctx, :latch_refs) do
      nil -> :error
      refs -> {:ok, elem(refs, idx)}
    end
  end

  defp incr_with_retry(ctx, key, delta) do
    case Router.incr(ctx, key, delta) do
      {:ok, _} = ok ->
        ok

      {:error, "ERR disk pressure" <> _} ->
        :timer.sleep(5)
        incr_with_retry(ctx, key, delta)

      other ->
        other
    end
  end

  defp retry_rmw(_fun, 0), do: {:error, :exhausted}

  defp retry_rmw(fun, n) do
    case fun.() do
      {:ok, _} = ok ->
        ok

      _ ->
        :timer.sleep(50)
        retry_rmw(fun, n - 1)
    end
  end

  defp latch_holder do
    spawn(fn ->
      receive do
        :release -> :ok
      end
    end)
  end

  defp release_latch_holder(pid) when is_pid(pid) do
    if Process.alive?(pid), do: send(pid, :release)
    :ok
  end

  defp minimal_instance_context do
    name = :"rmw_context_test_#{System.unique_integer([:positive])}"
    dir = Path.join(System.tmp_dir!(), Atom.to_string(name))

    ctx =
      FerricStore.Instance.build(name,
        data_dir: dir,
        shard_count: 2,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024
      )

    Enum.each(0..(ctx.shard_count - 1), fn i ->
      :ets.new(elem(ctx.keydir_refs, i), [
        :set,
        :public,
        :named_table,
        {:read_concurrency, true},
        {:write_concurrency, :auto}
      ])
    end)

    ctx
  end

  defp cleanup_minimal_instance_context(ctx) do
    Enum.each(0..(ctx.shard_count - 1), fn i ->
      try do
        :ets.delete(elem(ctx.keydir_refs, i))
      rescue
        _ -> :ok
      end

      try do
        :ets.delete(elem(ctx.latch_refs, i))
      rescue
        _ -> :ok
      end
    end)

    try do
      :ets.delete(ctx.hotness_table)
    rescue
      _ -> :ok
    end

    try do
      :ets.delete(ctx.config_table)
    rescue
      _ -> :ok
    end

    FerricStore.Instance.cleanup(ctx.name)
    File.rm_rf!(ctx.data_dir)
  end

  defp rmw_worker_calls_missing_ctx(path) do
    source = File.read!(path)
    lines = String.split(source, "\n")
    {:ok, ast} = Code.string_to_quoted(source, columns: true)

    {_ast, violations} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [{:__aliases__, _, [:GenServer]}, :call]}, _call_meta, args} = node, acc ->
          case args do
            [target, {:rmw, _cmd}, _timeout] ->
              line_no = Keyword.get(meta, :line, 1)

              if rmw_coordinator_target?(target) do
                {node, [{line_no, line_at(lines, line_no)} | acc]}
              else
                {node, acc}
              end

            _ ->
              {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(violations)
  end

  defp rmw_coordinator_target?(
         {{:., _, [:erlang, :binary_to_atom]}, _,
          [{:<<>>, _, ["Ferricstore.Store.RmwCoordinator." | _]}, :utf8]}
       ),
       do: true

  defp rmw_coordinator_target?(_target), do: false

  defp line_at(lines, line_no) do
    lines
    |> Enum.at(line_no - 1, "")
    |> String.trim()
  end

  defp tombstone_count(ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    shard_path
    |> Path.join("*.log")
    |> Path.wildcard()
    |> Enum.reduce(0, fn path, acc ->
      case NIF.v2_scan_tombstones(path) do
        {:ok, tombstones} ->
          acc +
            Enum.count(tombstones, fn {tombstone_key, _off, _size, _exp} ->
              tombstone_key == key
            end)

        {:error, _reason} ->
          acc
      end
    end)
  end
end
