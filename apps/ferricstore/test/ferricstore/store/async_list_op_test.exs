defmodule Ferricstore.Store.AsyncListOpTest do
  @moduledoc """
  TDD tests for async list_op (Group B in
  docs/async-compound-list-prob-design.md).

  Target behavior:

  - Router.list_op dispatches on durability_for_key(list_key). Quorum
    path unchanged. Async path tries :ets.insert_new on a per-list-key
    latch; on win, executes ListOps.execute/3 inline against an origin-
    local compound store; on lose, falls through to RmwCoordinator.

  - RmwCoordinator accepts {:list_op, key, operation} and dispatches to
    Router.execute_list_op_inline.

  - State machine's `async_key_for/1` handles {:list_op, ...} so the
    origin-skip logic applies. Inner command is replicated as
    {:list_op, key, operation} and replicas apply it against their own
    state in Raft log order.

  - LMOVE cross-shard stays on quorum (single-shard LMOVE uses async).

  These tests fail until Router.async_list_op + RmwCoordinator dispatch
  extension + state_machine async_key_for extension are implemented.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Test.ShardHelpers

  @ns "list_async"

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
  defp keydir_binary_bytes(ctx, idx), do: :atomics.get(ctx.keydir_binary_bytes, idx + 1)

  # ---------------------------------------------------------------------------
  # Single-caller correctness — round trip via list_op + compound_scan
  # ---------------------------------------------------------------------------

  describe "uncontended LPUSH / RPUSH" do
    test "RPUSH does not become locally visible when Batcher is overloaded" do
      key = ukey("rpush_overloaded")
      idx = Router.shard_for(ctx(), key)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:list_op, key, {:rpush, ["a"]}}}],
          0
        )
      end

      assert {:error, "ERR async replication overloaded"} =
               Router.list_op(ctx(), key, {:rpush, ["a"]})

      Batcher.reset_pending(idx)
      assert [] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LPUSH single element creates list with one member" do
      key = ukey("lpush_new")

      assert 1 = Router.list_op(ctx(), key, {:lpush, ["a"]})

      # The contents are stored as compound keys under the "L:" prefix.
      # list_range exposes them in order via the read path.
      assert ["a"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "RPUSH then LPUSH produces expected order" do
      key = ukey("push_order")

      assert 1 = Router.list_op(ctx(), key, {:rpush, ["b"]})
      assert 2 = Router.list_op(ctx(), key, {:lpush, ["a"]})
      assert 3 = Router.list_op(ctx(), key, {:rpush, ["c"]})

      assert ["a", "b", "c"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LPOP on empty list returns nil" do
      key = ukey("lpop_empty")
      assert nil == Router.list_op(ctx(), key, {:lpop, 1})
    end

    test "LPOP returns head and shrinks list" do
      key = ukey("lpop_shrinks")

      Router.list_op(ctx(), key, {:rpush, ["a", "b", "c"]})

      # :lpop with count=1 returns a single element or list of 1? check impl
      result = Router.list_op(ctx(), key, {:lpop, 1})
      assert result == ["a"] or result == "a"
      assert ["b", "c"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "RPOP returns tail" do
      key = ukey("rpop")

      Router.list_op(ctx(), key, {:rpush, ["a", "b", "c"]})
      result = Router.list_op(ctx(), key, {:rpop, 1})
      assert result == ["c"] or result == "c"
      assert ["a", "b"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LLEN returns element count" do
      key = ukey("llen")

      Router.list_op(ctx(), key, {:rpush, ["a", "b", "c", "d"]})
      assert 4 == Router.list_op(ctx(), key, :llen)
    end

    test "LPUSH treats an expired plain value as missing" do
      key = ukey("expired_plain_to_list")
      expired_at = Ferricstore.HLC.now_ms() - 1_000

      assert :ok = Router.put(ctx(), key, "old-string", expired_at)
      assert nil == Router.get(ctx(), key)

      assert 1 == Router.list_op(ctx(), key, {:lpush, ["fresh"]})
      assert ["fresh"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LPUSH treats an unswept expired plain value as missing" do
      key = ukey("unswept_expired_plain_to_list")
      expired_at = Ferricstore.HLC.now_ms() - 1_000

      assert :ok = Router.put(ctx(), key, "old-string", expired_at)

      assert 1 == Router.list_op(ctx(), key, {:lpush, ["fresh"]})
      assert ["fresh"] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LRANGE treats an unswept fully expired hash as a missing list" do
      key = ukey("expired_hash_to_list_read")
      expired_at = Ferricstore.HLC.now_ms() - 1_000

      assert :ok = Router.compound_put(ctx(), key, CompoundKey.type_key(key), "hash", 0)

      assert :ok =
               Router.compound_put(
                 ctx(),
                 key,
                 CompoundKey.hash_field(key, "field"),
                 "value",
                 expired_at
               )

      assert [] == Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "LPUSH on a live plain value returns WRONGTYPE and leaves the value intact" do
      key = ukey("plain_to_list")

      assert :ok = Router.put(ctx(), key, "plain-string")

      assert {:error, "WRONGTYPE" <> _} = Router.list_op(ctx(), key, {:lpush, ["fresh"]})
      assert "plain-string" == Router.get(ctx(), key)
      assert {:error, "WRONGTYPE" <> _} = Router.list_op(ctx(), key, {:lrange, 0, -1})
    end

    test "RPUSH large element stores cold location instead of full value in ETS" do
      ctx = ctx()
      key = ukey("large_element")
      large = :binary.copy("x", ctx.hot_cache_max_value_size + 1024)

      assert 1 = Router.list_op(ctx, key, {:rpush, [large]})
      assert [^large] = Router.list_op(ctx, key, {:lrange, 0, -1})

      keydir = elem(ctx.keydir_refs, Router.shard_for(ctx, key))
      prefix = CompoundKey.list_prefix(key)

      entries =
        keydir
        |> :ets.tab2list()
        |> Enum.filter(fn {compound_key, _value, _exp, _lfu, _fid, _off, _vsize} ->
          String.starts_with?(compound_key, prefix)
        end)

      assert [{_compound_key, nil, _exp, _lfu, fid, off, value_size}] = entries
      assert is_integer(fid) and fid >= 0
      assert is_integer(off) and off >= 0
      assert value_size == byte_size(large)
    end

    test "RPUSH large element disk error is not accepted for async replication" do
      ctx = ctx()
      key = ukey("large_disk_error")
      idx = Router.shard_for(ctx, key)
      large = :binary.copy("x", ctx.hot_cache_max_value_size + 1024)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(idx)

      missing_path =
        Path.join([
          System.tmp_dir!(),
          "missing_ferricstore_#{System.unique_integer([:positive])}",
          "00000.log"
        ])

      handler_id = {:async_list_op, self(), make_ref()}

      :telemetry.attach(
        handler_id,
        [:ferricstore, :batcher, :async_flush],
        fn _event, _measurements, meta, test_pid ->
          if meta.shard_index == idx and meta.origin do
            send(test_pid, :unexpected_async_flush)
          end
        end,
        self()
      )

      Ferricstore.Store.ActiveFile.publish(idx, file_id, missing_path, Path.dirname(missing_path))

      try do
        assert {:error, "ERR disk write failed" <> _} =
                 Router.list_op(ctx, key, {:rpush, [large]})

        batcher_state = :sys.get_state(Batcher.batcher_name(idx))

        assert batcher_state.slots == %{},
               "failed local list write must not leave an async Raft command queued"
      after
        Ferricstore.Store.ActiveFile.publish(idx, file_id, file_path, shard_path)
      end

      try do
        Batcher.flush(idx)
        refute_receive :unexpected_async_flush, 200
        assert [] == Router.list_op(ctx, key, {:lrange, 0, -1})
      after
        :telemetry.detach(handler_id)
      end
    end

    test "LPOP releases keydir binary memory for hot off-heap elements" do
      ctx = ctx()
      key = ukey("binary_accounting")
      idx = Router.shard_for(ctx, key)
      value = :binary.copy("v", 1024)
      before_push = keydir_binary_bytes(ctx, idx)

      assert 1 = Router.list_op(ctx, key, {:rpush, [value]})
      after_push = keydir_binary_bytes(ctx, idx)
      assert after_push >= before_push + byte_size(value)

      assert value == Router.list_op(ctx, key, {:lpop, 1})
      after_pop = keydir_binary_bytes(ctx, idx)

      assert after_pop <= after_push - byte_size(value)
    end
  end

  # ---------------------------------------------------------------------------
  # Latch path telemetry — uncontended list_ops take the fast path
  # ---------------------------------------------------------------------------

  describe "path selection" do
    test "uncontended LPUSH takes the latch path" do
      key = ukey("path_latch")

      test_pid = self()
      handler_id = {:list_async_test, :latch}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :list_op, :latch],
          fn _event, _meas, _meta, pid ->
            send(pid, :list_op_latch)
          end,
          test_pid
        )

      try do
        Router.list_op(ctx(), key, {:rpush, ["a"]})
        assert_receive :list_op_latch, 500
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Atomicity under concurrency — the main reason for the latch pattern
  # ---------------------------------------------------------------------------

  describe "concurrent list_ops" do
    test "50 concurrent LPUSHes on same key sum to length 50" do
      key = ukey("concurrent_lpush")

      tasks =
        for i <- 1..50 do
          Task.async(fn -> Router.list_op(ctx(), key, {:rpush, ["e#{i}"]}) end)
        end

      results = Task.await_many(tasks, 20_000)
      # Every push returned an integer (new length after that push).
      assert Enum.all?(results, &is_integer/1)

      # Final list has exactly 50 elements.
      assert 50 == Router.list_op(ctx(), key, :llen)

      # All values are distinct ("e1" through "e50"), regardless of order.
      elements = Router.list_op(ctx(), key, {:lrange, 0, -1})
      expected = for i <- 1..50, into: MapSet.new(), do: "e#{i}"
      assert MapSet.new(elements) == expected
    end

    test "concurrent LPUSH + LPOP leave a consistent list" do
      key = ukey("concurrent_mixed")

      Router.list_op(ctx(), key, {:rpush, for(i <- 1..50, do: "e#{i}")})
      assert 50 == Router.list_op(ctx(), key, :llen)

      pushers =
        for i <- 51..80 do
          Task.async(fn -> Router.list_op(ctx(), key, {:rpush, ["e#{i}"]}) end)
        end

      poppers =
        for _ <- 1..20 do
          Task.async(fn -> Router.list_op(ctx(), key, {:lpop, 1}) end)
        end

      _ = Task.await_many(pushers ++ poppers, 20_000)

      # Pushes added 30, pops removed up to 20 → length in [60, 60].
      final_len = Router.list_op(ctx(), key, :llen)

      assert final_len == 50 + 30 - 20,
             "expected list length #{50 + 30 - 20}, got #{final_len}"
    end
  end

  # ---------------------------------------------------------------------------
  # Durability respects the list key's namespace
  # ---------------------------------------------------------------------------

  describe "namespace routing" do
    test "quorum-namespace list key still works" do
      quorum_key = "quorum_list_#{:erlang.unique_integer([:positive])}"

      assert 1 = Router.list_op(ctx(), quorum_key, {:rpush, ["x"]})
      assert ["x"] == Router.list_op(ctx(), quorum_key, {:lrange, 0, -1})
    end
  end
end
