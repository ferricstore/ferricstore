defmodule Ferricstore.Store.AsyncCompoundTest do
  @moduledoc """
  TDD tests for async compound_put / compound_delete (Group A in
  docs/async-compound-list-prob-design.md).

  Target behavior:

  - Router.compound_put and Router.compound_delete dispatch on
    durability_for_key(ctx, redis_key). When the parent redis_key's
    namespace is configured async, the write goes through an async fast
    path mirroring async_write_put: ETS + BitcaskWriter cast +
    Batcher.async_enqueue, caller returns ~15-30μs.

  - Read-your-writes holds on the origin — the compound_key is in ETS
    before the caller gets :ok.

  - State machine's existing {:async, {:put, k, v, e}} clause handles
    replication without new code (compound_key is just a binary key
    from the state machine's view).

  - Promotion check is skipped on the async path (documented trade-off).

  - Namespace is decided by the parent redis_key, NOT by the compound_key.
    HSET user:1 name "alice" uses "user" namespace, not "H".

  These tests guard async compound writes against local-only acknowledgements,
  duplicate origin writes, and cold large-value read regressions.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.{CompoundKey, Router}
  alias Ferricstore.Test.ShardHelpers

  @ns "cmpd_async"

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

  defp hash_field(redis_key, field), do: CompoundKey.hash_field(redis_key, field)

  defp pending_async_commands(idx) do
    Batcher.batcher_name(idx)
    |> :sys.get_state()
    |> Map.get(:slots, %{})
    |> Map.values()
    |> Enum.flat_map(fn slot -> Enum.reverse(slot.cmds) end)
  end

  # ---------------------------------------------------------------------------
  # Single-caller correctness — HSET + HGET
  # ---------------------------------------------------------------------------

  describe "uncontended compound_put / compound_delete" do
    test "compound_put followed by compound_get returns the value" do
      redis_key = ukey("hash")
      ck = hash_field(redis_key, "name")

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      assert "alice" = Router.compound_get(ctx(), redis_key, ck)
    end

    test "small compound_put writes one Bitcask record on the origin" do
      redis_key = ukey("hash_single_disk_write")
      ck = hash_field(redis_key, "name")
      idx = Router.shard_for(ctx(), redis_key)
      {_file_id, file_path, _shard_path} = Ferricstore.Store.ActiveFile.get(ctx(), idx)

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      :ok = Batcher.flush(idx)
      :ok = Ferricstore.Store.BitcaskWriter.flush(idx)

      assert {:ok, records} = Ferricstore.Bitcask.NIF.v2_scan_file(file_path)

      writes =
        Enum.count(records, fn
          {^ck, _off, _size, _exp, false} -> true
          _ -> false
        end)

      assert writes == 1
    end

    test "compound_batch_get preserves input order and missing entries" do
      redis_key = ukey("hash_batch")
      f1 = hash_field(redis_key, "first")
      missing = hash_field(redis_key, "missing")
      f2 = hash_field(redis_key, "second")

      :ok = Router.compound_put(ctx(), redis_key, f1, "one", 0)
      :ok = Router.compound_put(ctx(), redis_key, f2, "two", 0)

      assert ["one", nil, "two"] == Router.compound_batch_get(ctx(), redis_key, [f1, missing, f2])
    end

    test "compound_delete removes the field" do
      redis_key = ukey("hash_del")
      ck = hash_field(redis_key, "name")

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      assert "alice" = Router.compound_get(ctx(), redis_key, ck)

      :ok = Router.compound_delete(ctx(), redis_key, ck)
      assert nil == Router.compound_get(ctx(), redis_key, ck)
    end

    test "compound_put waits on the per-field async latch" do
      redis_key = ukey("hash_latch_put")
      ck = hash_field(redis_key, "name")
      idx = Router.shard_for(ctx(), redis_key)
      latch_tab = elem(ctx().latch_refs, idx)

      assert :ets.insert_new(latch_tab, {ck, self()})

      task =
        Task.async(fn ->
          Router.compound_put(ctx(), redis_key, ck, "alice", 0)
        end)

      refute Task.yield(task, 25)
      assert nil == Router.compound_get(ctx(), redis_key, ck)

      :ets.select_delete(latch_tab, [{{ck, self()}, [], [true]}])
      assert :ok = Task.await(task, 1_000)
      assert "alice" == Router.compound_get(ctx(), redis_key, ck)
    end

    test "compound_delete waits on the per-field async latch" do
      redis_key = ukey("hash_latch_delete")
      ck = hash_field(redis_key, "name")
      idx = Router.shard_for(ctx(), redis_key)
      latch_tab = elem(ctx().latch_refs, idx)

      :ok = Router.compound_put(ctx(), redis_key, ck, "alice", 0)
      assert :ets.insert_new(latch_tab, {ck, self()})

      task =
        Task.async(fn ->
          Router.compound_delete(ctx(), redis_key, ck)
        end)

      refute Task.yield(task, 25)
      assert "alice" == Router.compound_get(ctx(), redis_key, ck)

      :ets.select_delete(latch_tab, [{{ck, self()}, [], [true]}])
      assert :ok = Task.await(task, 1_000)
      assert nil == Router.compound_get(ctx(), redis_key, ck)
    end

    test "compound_put with TTL is readable before expiry" do
      redis_key = ukey("hash_ttl")
      ck = hash_field(redis_key, "ephemeral")
      exp = System.os_time(:millisecond) + 60_000

      :ok = Router.compound_put(ctx(), redis_key, ck, "val", exp)
      assert "val" = Router.compound_get(ctx(), redis_key, ck)
    end

    test "large value (>64KB) written via compound_put is readable" do
      redis_key = ukey("hash_big")
      ck = hash_field(redis_key, "big_field")
      big = :binary.copy("x", 100 * 1024)

      :ok = Router.compound_put(ctx(), redis_key, ck, big, 0)

      Ferricstore.Test.Utils.eventually(
        fn ->
          result = Router.compound_get(ctx(), redis_key, ck)
          assert result != nil, "compound_get returned nil for large value"
          assert big == result
        end,
        15_000
      )
    end

    test "compound_batch_get reads large values" do
      redis_key = ukey("hash_batch_big")
      small_key = hash_field(redis_key, "small")
      big_key = hash_field(redis_key, "big")
      big = :binary.copy("x", 100 * 1024)

      :ok = Router.compound_put(ctx(), redis_key, small_key, "small", 0)
      :ok = Router.compound_put(ctx(), redis_key, big_key, big, 0)

      Ferricstore.Test.Utils.eventually(
        fn ->
          assert ["small", big] ==
                   Router.compound_batch_get(ctx(), redis_key, [small_key, big_key])
        end,
        15_000
      )
    end

    test "large compound_put restores previous value when Batcher is overloaded" do
      redis_key = ukey("hash_big_overloaded")
      ck = hash_field(redis_key, "big_field")
      old = :binary.copy("o", ctx().hot_cache_max_value_size + 1024)
      new = :binary.copy("n", ctx().hot_cache_max_value_size + 1024)
      idx = Router.shard_for(ctx(), redis_key)

      :ok = Router.compound_put(ctx(), redis_key, ck, old, 0)
      assert old == Router.compound_get(ctx(), redis_key, ck)

      on_exit(fn -> Batcher.reset_pending(idx) end)

      for _ <- 1..64 do
        Batcher.__inject_async_pending__(
          idx,
          make_ref(),
          [{:async, node(), {:put, ck, "pending", 0}}],
          0
        )
      end

      assert {:error, "ERR async replication overloaded"} =
               Router.compound_put(ctx(), redis_key, ck, new, 0)

      Batcher.reset_pending(idx)
      assert old == Router.compound_get(ctx(), redis_key, ck)
    end

    test "large compound_put disk error is not accepted for async replication" do
      redis_key = ukey("hash_big_disk_error")
      ck = hash_field(redis_key, "big_field")
      big = :binary.copy("x", ctx().hot_cache_max_value_size + 1024)
      idx = Router.shard_for(ctx(), redis_key)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(ctx(), idx)

      missing_path =
        Path.join([
          System.tmp_dir!(),
          "missing_ferricstore_#{System.unique_integer([:positive])}",
          "00000.log"
        ])

      :ok = Ferricstore.NamespaceConfig.set(@ns, "window_ms", "5000")

      Ferricstore.Store.ActiveFile.publish(
        ctx(),
        idx,
        file_id,
        missing_path,
        Path.dirname(missing_path)
      )

      try do
        assert {:error, "ERR disk write failed" <> _} =
                 Router.compound_put(ctx(), redis_key, ck, big, 0)

        assert [] == pending_async_commands(idx)
        assert nil == Router.compound_get(ctx(), redis_key, ck)
      after
        Ferricstore.Store.ActiveFile.publish(ctx(), idx, file_id, file_path, shard_path)
        Batcher.reset_pending(idx)
        Ferricstore.NamespaceConfig.set(@ns, "window_ms", "1")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Namespace decision uses the parent redis_key
  # ---------------------------------------------------------------------------

  describe "namespace routing by parent key" do
    test "quorum-namespace parent key still works end-to-end" do
      # Non-telemetry correctness check: a redis_key NOT in @ns uses the
      # default (quorum) path. Verify round-trip works regardless of the
      # fact that the compound_key starts with "H:" (which isn't the
      # namespace — the namespace is the parent redis_key's prefix).
      quorum_redis_key = "quorum_only_test_#{:erlang.unique_integer([:positive])}"
      ck = hash_field(quorum_redis_key, "name")

      :ok = Router.compound_put(ctx(), quorum_redis_key, ck, "val", 0)
      assert "val" = Router.compound_get(ctx(), quorum_redis_key, ck)
    end

    test "async-namespace parent key uses the async path (origin:true flush)" do
      redis_key = ukey("hash_async_routed")
      ck = hash_field(redis_key, "name")

      test_pid = self()
      handler_id = {:compound_test, :ns_async}

      _ =
        :telemetry.attach(
          handler_id,
          [:ferricstore, :batcher, :async_flush],
          fn _event, _meas, meta, pid ->
            send(pid, {:batcher_flush, meta})
          end,
          test_pid
        )

      try do
        :ok = Router.compound_put(ctx(), redis_key, ck, "val", 0)
        assert "val" = Router.compound_get(ctx(), redis_key, ck)

        assert_receive {:batcher_flush, %{origin: true}}, 1_000
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Concurrent distinct-compound-key writes scale linearly (no Shard bottleneck)
  # ---------------------------------------------------------------------------

  describe "concurrent distinct fields" do
    test "50 concurrent HSETs on distinct fields all succeed" do
      redis_key = ukey("hash_concurrent")

      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            ck = hash_field(redis_key, "field_#{i}")
            Router.compound_put(ctx(), redis_key, ck, "value_#{i}", 0)
          end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      for i <- 1..50 do
        ck = hash_field(redis_key, "field_#{i}")
        assert "value_#{i}" == Router.compound_get(ctx(), redis_key, ck)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Same-field concurrent writes: last-write-wins is acceptable async semantics
  # ---------------------------------------------------------------------------

  describe "concurrent same field" do
    test "concurrent HSETs on same field don't crash; final value is one of them" do
      redis_key = ukey("hash_same_field")
      ck = hash_field(redis_key, "contested")

      values = for i <- 1..30, do: "v#{i}"

      tasks =
        for v <- values do
          Task.async(fn -> Router.compound_put(ctx(), redis_key, ck, v, 0) end)
        end

      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, &(&1 == :ok))

      final = Router.compound_get(ctx(), redis_key, ck)
      assert final in values
    end
  end

  # ---------------------------------------------------------------------------
  # Mixed-family: same namespace, both SET (plain) and HSET (compound)
  # ---------------------------------------------------------------------------

  describe "mixed plain + compound under same async namespace" do
    test "plain SET and HSET on different keys in same namespace both fast" do
      # The namespace is configured async — both plain puts and compound
      # puts should take async paths. Verify via round-trip correctness.
      plain_key = ukey("plain")
      redis_key = ukey("hash_mixed")
      ck = hash_field(redis_key, "f1")

      :ok = Router.put(ctx(), plain_key, "plain_val", 0)
      :ok = Router.compound_put(ctx(), redis_key, ck, "hash_val", 0)

      assert Router.get(ctx(), plain_key) == "plain_val"
      assert Router.compound_get(ctx(), redis_key, ck) == "hash_val"
    end

    test "SET over an existing hash clears compound metadata and fields" do
      key = ukey("set_over_hash")

      assert :ok = FerricStore.hset(key, %{"field" => "hash_val"})
      assert {:ok, "hash_val"} = FerricStore.hget(key, "field")

      assert :ok = FerricStore.set(key, "plain_val")

      assert {:ok, "plain_val"} = FerricStore.get(key)
      assert {:error, "WRONGTYPE" <> _} = FerricStore.hget(key, "field")
      assert nil == Router.compound_get(ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(ctx(), key, hash_field(key, "field"))
    end

    test "SET over an existing list clears list metadata and elements via type marker" do
      key = ukey("set_over_list")

      assert {:ok, 2} = FerricStore.rpush(key, ["one", "two"])
      assert {:ok, ["one", "two"]} = FerricStore.lrange(key, 0, -1)
      assert "list" == Router.compound_get(ctx(), key, CompoundKey.type_key(key))

      assert :ok = FerricStore.set(key, "plain_val")

      assert {:ok, "plain_val"} = FerricStore.get(key)
      assert {:error, "WRONGTYPE" <> _} = FerricStore.lrange(key, 0, -1)
      assert nil == Router.compound_get(ctx(), key, CompoundKey.type_key(key))
      assert nil == Router.compound_get(ctx(), key, CompoundKey.list_meta_key(key))
    end

    test "plain SET without compound markers does not fetch active file" do
      key = ukey("plain_no_marker")
      assert Router.durability_for_key_public(ctx(), key) == :async
      cache_key = active_file_cache_key(ctx(), key)
      Process.delete(cache_key)

      assert :ok = Router.put(ctx(), key, "plain_val", 0)

      refute Process.get(cache_key)
      assert Router.get(ctx(), key) == "plain_val"
    end

    test "compound_count releases tracked key bytes for expired fields" do
      key = ukey("count_expired_field")
      field = :binary.copy("expired_field", 8)
      compound_key = hash_field(key, field)
      prefix = CompoundKey.hash_prefix(key)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.compound_put(ctx(), key, compound_key, "value", expired_at)

      assert [{^compound_key, _value, ^expired_at, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(keydir, compound_key)

      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      assert 0 == Router.compound_count(ctx(), key, prefix)
      assert :ets.lookup(keydir, compound_key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end

    test "compound_scan releases tracked key bytes for expired fields" do
      key = ukey("scan_expired_field")
      field = :binary.copy("expired_field", 8)
      compound_key = hash_field(key, field)
      prefix = CompoundKey.hash_prefix(key)
      idx = Router.shard_for(ctx(), key)
      keydir = elem(ctx().keydir_refs, idx)
      expired_at = Ferricstore.HLC.now_ms() - 1

      before_bytes = :atomics.get(ctx().keydir_binary_bytes, idx + 1)
      assert :ok = Router.compound_put(ctx(), key, compound_key, "value", expired_at)

      assert [{^compound_key, _value, ^expired_at, _lfu, _fid, _off, _vsize}] =
               :ets.lookup(keydir, compound_key)

      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) > before_bytes

      assert [] == Router.compound_scan(ctx(), key, prefix)
      assert :ets.lookup(keydir, compound_key) == []
      assert :atomics.get(ctx().keydir_binary_bytes, idx + 1) == before_bytes
    end

    test "SET paths do not use list metadata as a fallback type marker" do
      # Modern list writes always create T:key. LM:key is list payload metadata,
      # so probing it on every plain SET is wasted hot-path work.
      forbidden = [
        :legacy_list,
        :legacy_list_marker_for_string_put,
        :clear_legacy_list_metadata,
        :clear_legacy_list_metadata_for_string_put
      ]

      findings =
        [
          "lib/ferricstore/store/router.ex",
          "lib/ferricstore/commands/strings.ex",
          "lib/ferricstore/raft/state_machine.ex"
        ]
        |> Enum.flat_map(fn path ->
          path
          |> app_path()
          |> forbidden_identifiers(forbidden)
          |> Enum.map(fn {identifier, line} -> "#{path}:#{line}: #{identifier}" end)
        end)

      assert findings == [],
             "SET paths must rely on T:key only, found legacy LM fallback markers:\n" <>
               Enum.join(findings, "\n")
    end
  end

  defp active_file_cache_key(ctx, key) do
    idx = Router.shard_for(ctx, key)
    table_key = if ctx.name == :default, do: idx, else: {ctx.name, idx}
    {:active_file_cache, table_key}
  end

  defp app_path(path), do: Path.expand("../../../#{path}", __DIR__)

  defp forbidden_identifiers(path, forbidden) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    {_ast, findings} =
      Macro.prewalk(ast, [], fn
        {name, meta, _args} = node, acc when is_atom(name) ->
          if name in forbidden do
            {node, [{name, meta[:line]} | acc]}
          else
            {node, acc}
          end

        node, acc when is_atom(node) ->
          if node in forbidden do
            {node, [{node, :unknown}]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(findings)
  end

  # ---------------------------------------------------------------------------
  # Latency budget — async compound should not be ms-range
  # ---------------------------------------------------------------------------

  describe "latency" do
    @tag :latency
    test "uncontended HSET p50 under 1 ms" do
      redis_key = ukey("hash_lat")

      # Warm up
      for i <- 1..10 do
        ck = hash_field(redis_key, "warm_#{i}")
        Router.compound_put(ctx(), redis_key, ck, "warm", 0)
      end

      samples =
        for i <- 1..100 do
          ck = hash_field(redis_key, "bench_#{i}")
          t0 = System.monotonic_time(:microsecond)
          :ok = Router.compound_put(ctx(), redis_key, ck, "v", 0)
          System.monotonic_time(:microsecond) - t0
        end

      sorted = Enum.sort(samples)
      p50 = Enum.at(sorted, div(length(sorted), 2))
      p99 = Enum.at(sorted, trunc(length(sorted) * 0.99))

      assert p50 < 1_000,
             "async compound_put p50 #{p50}μs exceeded 1ms budget " <>
               "(p99 #{p99}μs); async path probably not engaged"
    end
  end
end
