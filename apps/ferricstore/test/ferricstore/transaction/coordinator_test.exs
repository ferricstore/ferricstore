defmodule Ferricstore.Transaction.CoordinatorTest do
  @moduledoc """
  Unit tests for the Transaction Coordinator.

  Raft-enabled transactions, including single-shard writes, execute atomically
  via an anchor-shard Raft entry.

  All key-to-shard mappings are discovered dynamically via ShardHelpers so
  tests work with any shard count (not just 4).
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Commands.PreparedCommand
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.WriteVersion
  alias Ferricstore.Test.PreparedTransactionCoordinator, as: Coordinator
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.flush_all_keys()

    [k0, k1, k2, k3] = ShardHelpers.keys_on_different_shards(4)
    {same1, same2} = ShardHelpers.keys_on_same_shard()

    %{
      cross_keys: [k0, k1, k2, k3],
      k0: k0,
      k1: k1,
      k2: k2,
      k3: k3,
      same1: same1,
      same2: same2
    }
  end

  # Verify key-to-shard mapping assumptions used throughout these tests.
  describe "test infrastructure" do
    test "keys route to different shards", %{cross_keys: keys} do
      shards =
        Enum.map(keys, fn k -> Router.shard_for(FerricStore.Instance.get(:default), k) end)
        |> Enum.uniq()

      assert length(shards) == length(keys)
    end

    test "same-shard keys route to same shard", %{same1: same1, same2: same2} do
      assert Router.shard_for(FerricStore.Instance.get(:default), same1) ==
               Router.shard_for(FerricStore.Instance.get(:default), same2)
    end
  end

  describe "performance guards" do
    test "result reassembly does not scan shard result lists by position" do
      source =
        Path.expand("../../../lib/ferricstore/transaction/coordinator.ex", __DIR__)
        |> File.read!()

      refute source =~ "Enum.at(results_for_shard",
             "EXEC result reassembly must stay linear; zip shard results to original indices instead"
    end
  end

  describe "single-shard transactions" do
    test "backend exits return a typed transaction error", %{same1: key} do
      Process.put(:ferricstore_tx_backend_write_hook, fn _shard_idx, _command ->
        exit({:timeout, :synthetic_backend_timeout})
      end)

      on_exit(fn -> Process.delete(:ferricstore_tx_backend_write_hook) end)

      assert {:ok, prepared} = PreparedCommand.prepare("SET", [key, "value"])

      assert {:error, "ERR transaction raft unavailable: :pipeline_rejected"} =
               Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)
    end

    test "production coordinator rejects queue entries without a prepared AST", %{same1: key} do
      assert {:error, "ERR invalid transaction command"} =
               Ferricstore.Transaction.Coordinator.execute([{"GET", [key]}], %{}, nil)
    end

    test "production coordinator rejects raw tuples even when they contain an AST", %{same1: key} do
      assert {:error, "ERR invalid transaction command"} =
               Ferricstore.Transaction.Coordinator.execute(
                 [{"GET", [key], {:get, key}}],
                 %{},
                 nil
               )
    end

    test "production coordinator rejects commands that cannot run in replicated apply" do
      for {command, args} <- [
            {"PUBLISH", ["tx-channel", "must-not-publish"]},
            {"KEY_INFO", ["tx-key-info"]},
            {"FETCH_OR_COMPUTE", ["tx-fetch", "1000"]},
            {"SPOP", ["tx-random-set"]},
            {"BF.ADD", ["tx-bloom", "member"]}
          ] do
        assert {:ok, prepared} = PreparedCommand.prepare(command, args)

        expected =
          "ERR command '#{String.downcase(command)}' is not supported inside transactions"

        assert {:error, ^expected} =
                 Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)
      end
    end

    @tag :transaction_native_router_escape
    test "rejects native mutations before they can route out of replicated apply" do
      ctx = FerricStore.Instance.get(:default)
      cas_key = "{native-tx}:cas"
      untouched_keys = Enum.map(["lock", "unlock", "extend", "ratelimit"], &"{native-tx}:#{&1}")
      :ok = Router.put(ctx, cas_key, "old", 0)

      for {command, args} <- [
            {"CAS", [cas_key, "old", "new"]},
            {"LOCK", [Enum.at(untouched_keys, 0), "owner", "1000"]},
            {"UNLOCK", [Enum.at(untouched_keys, 1), "owner"]},
            {"EXTEND", [Enum.at(untouched_keys, 2), "owner", "1000"]},
            {"RATELIMIT.ADD", [Enum.at(untouched_keys, 3), "1000", "10", "1"]}
          ] do
        assert {:ok, prepared} = PreparedCommand.prepare(command, args)

        assert {:error, reason} =
                 Ferricstore.Transaction.Coordinator.execute([prepared], %{}, nil)

        assert reason ==
                 "ERR command '#{String.downcase(command)}' is not supported inside transactions"
      end

      assert Router.get(ctx, cas_key) == "old"
      assert Enum.all?(untouched_keys, &(Router.get(ctx, &1) == nil))
    end

    test "executes when all commands target the same shard", %{same1: s1, same2: s2} do
      queue = [{"SET", [s1, "100"]}, {"SET", [s2, "200"]}, {"GET", [s1]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "100"]
    end

    test "single-shard write transaction applies through Raft", %{same1: s1} do
      idx = Router.shard_for(FerricStore.Instance.get(:default), s1)
      before_count = raft_applied_count(idx)

      assert [:ok] == Coordinator.execute([{"SET", [s1, "via_raft"]}], %{}, nil)

      assert raft_applied_count(idx) == before_count + 1
    end

    test "returns results in original command order", %{same1: s1, same2: s2} do
      queue = [
        {"SET", [s1, "first"]},
        {"SET", [s2, "second"]},
        {"GET", [s2]},
        {"GET", [s1]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "second", "first"]
    end

    test "INCR works atomically within single shard", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "10", 0)

      queue = [{"INCR", [s1]}, {"INCR", [s1]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 12}]
      assert Router.get(FerricStore.Instance.get(:default), s1) == "12"
    end

    test "DEL within single shard", %{same1: s1, same2: s2} do
      Router.put(FerricStore.Instance.get(:default), s1, "v", 0)
      Router.put(FerricStore.Instance.get(:default), s2, "v", 0)

      queue = [{"DEL", [s1]}, {"DEL", [s2]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1]
      assert Router.get(FerricStore.Instance.get(:default), s1) == nil
      assert Router.get(FerricStore.Instance.get(:default), s2) == nil
    end

    test "mixed GET and SET on same shard", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "existing", 0)

      queue = [
        {"GET", [s1]},
        {"SET", [s1, "updated"]},
        {"GET", [s1]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing", :ok, "updated"]
    end

    test "prepared SET NX options execute through Rust AST normalization", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "existing", 0)

      result = Coordinator.execute([{"SET", [s1, "skipped", "NX"]}], %{}, nil)

      assert result == [nil]
      assert Router.get(FerricStore.Instance.get(:default), s1) == "existing"
    end

    test "prepared SET rejects conflicting NX and XX through Rust AST normalization", %{
      same1: s1
    } do
      result = Coordinator.execute([{"SET", [s1, "value", "NX", "XX"]}], %{}, nil)

      assert result == [{:error, "ERR XX and NX options at the same time are not compatible"}]
      assert Router.get(FerricStore.Instance.get(:default), s1) == nil
    end

    test "large SET is visible to later GET in same transaction", %{same1: s1} do
      ctx = FerricStore.Instance.get(:default)
      large = :binary.copy("x", ctx.hot_cache_max_value_size + 1024)

      result = Coordinator.execute([{"SET", [s1, large]}, {"GET", [s1]}], %{}, nil)

      assert result == [:ok, large]
    end

    test "large HSET is visible to later HGET and HGETALL in same transaction", %{same1: s1} do
      ctx = FerricStore.Instance.get(:default)
      large = :binary.copy("h", ctx.hot_cache_max_value_size + 1024)

      result =
        Coordinator.execute(
          [{"HSET", [s1, "field", large]}, {"HGET", [s1, "field"]}, {"HGETALL", [s1]}],
          %{},
          nil
        )

      assert result == [1, large, ["field", large]]
    end

    @tag :transaction_compound_member_index
    test "committed hash mutations publish compound member-index updates", %{same1: shard_seed} do
      suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      key = "#{shard_seed}:tx-hash-index:#{suffix}"
      prefix = Ferricstore.Store.CompoundKey.hash_prefix(key)

      assert :ok = FerricStore.hset(key, %{"baseline" => "old"})
      assert {:ok, %{"baseline" => "old"}} = FerricStore.hgetall(key)

      ctx = FerricStore.Instance.get(:default)
      shard_index = Router.shard_for(ctx, key)

      member_index =
        Ferricstore.Store.Shard.CompoundMemberIndex.table_name(:default, shard_index)

      assert [{{^prefix, "baseline"}, _compound_key}] =
               :ets.lookup(member_index, {prefix, "baseline"})

      assert [1] = Coordinator.execute([{"HSET", [key, "new", "value"]}], %{}, nil)
      assert {:ok, %{"baseline" => "old", "new" => "value"}} = FerricStore.hgetall(key)

      assert [{{^prefix, "new"}, _compound_key}] =
               :ets.lookup(member_index, {prefix, "new"})

      assert [1] = Coordinator.execute([{"HDEL", [key, "baseline"]}], %{}, nil)
      assert [] == :ets.lookup(member_index, {prefix, "baseline"})
      assert {:ok, %{"new" => "value"}} = FerricStore.hgetall(key)
    end

    @tag :transaction_namespace_batch_barrier
    test "namespace windows do not nest transaction entries inside generic batches" do
      suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      first = "{tx-batch-barrier:#{suffix}}:first"
      second = "{tx-batch-barrier:#{suffix}}:second"
      ctx = FerricStore.Instance.get(:default)

      assert Router.shard_for(ctx, first) == Router.shard_for(ctx, second)

      for key <- [first, second] do
        assert :ok = FerricStore.hset(key, %{"baseline" => "old"})
        assert {:ok, %{"baseline" => "old"}} = FerricStore.hgetall(key)
      end

      :ok = Ferricstore.NamespaceConfig.set("_root", "window_ms", "50")
      on_exit(fn -> Ferricstore.NamespaceConfig.reset("_root") end)
      parent = self()

      tasks =
        for key <- [first, second] do
          Task.async(fn ->
            send(parent, {:transaction_batch_ready, self()})

            receive do
              :run_transaction_batch ->
                Coordinator.execute([{"HSET", [key, "new", "value"]}], %{}, nil)
            end
          end)
        end

      pids =
        for _ <- tasks do
          assert_receive {:transaction_batch_ready, pid}, 1_000
          pid
        end

      Enum.each(pids, &send(&1, :run_transaction_batch))
      assert [[1], [1]] == Enum.map(tasks, &Task.await(&1, 5_000))

      shard_index = Router.shard_for(ctx, first)

      member_index =
        Ferricstore.Store.Shard.CompoundMemberIndex.table_name(:default, shard_index)

      for key <- [first, second] do
        prefix = Ferricstore.Store.CompoundKey.hash_prefix(key)

        assert [{{^prefix, "new"}, _compound_key}] =
                 :ets.lookup(member_index, {prefix, "new"})

        assert {:ok, %{"baseline" => "old", "new" => "value"}} = FerricStore.hgetall(key)
      end
    end

    @tag :transaction_zset_index
    test "committed ZADD publishes score-index updates", %{same1: shard_seed} do
      suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      key = "#{shard_seed}:tx-zset:#{suffix}"

      assert {:ok, 1} = FerricStore.zadd(key, [{10.0, "baseline"}])
      assert {:ok, ["baseline"]} = FerricStore.zrange(key, 0, -1)

      ctx = FerricStore.Instance.get(:default)
      shard_index = Router.shard_for(ctx, key)

      {_score_index, score_lookup} =
        Ferricstore.Store.Shard.ZSetIndex.table_names(:default, shard_index)

      shard = Router.shard_name(ctx, shard_index)
      refute GenServer.call(shard, {:promoted?, key})

      assert {:ok, [{"baseline", 10.0}]} =
               GenServer.call(shard, {:zset_rank_range, key, 0, 0, false})

      assert [{{:ready, ^key}, true}] = :ets.lookup(score_lookup, {:ready, key})

      assert [1] = Coordinator.execute([{"ZADD", [key, "1", "new"]}], %{}, nil)
      assert {:ok, ["new", "baseline"]} = FerricStore.zrange(key, 0, -1)

      assert [0] = Coordinator.execute([{"ZADD", [key, "0", "baseline"]}], %{}, nil)
      assert {:ok, ["baseline", "new"]} = FerricStore.zrange(key, 0, -1)

      assert [1] = Coordinator.execute([{"ZREM", [key, "new"]}], %{}, nil)
      assert {:ok, ["baseline"]} = FerricStore.zrange(key, 0, -1)
      refute GenServer.call(shard, {:promoted?, key})

      refute File.dir?(
               Ferricstore.Store.Promotion.dedicated_path(ctx.data_dir, shard_index, :zset, key)
             )

      assert [1] = Coordinator.execute([{"DEL", [key]}], %{}, nil)
      assert {:ok, []} = FerricStore.zrange(key, 0, -1)
    end

    @tag :transaction_orphan_promotion_dir
    test "orphaned dedicated directories do not block compound mutations" do
      suffix = Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)
      key = "tx-orphan-promotion:#{suffix}"
      ctx = FerricStore.Instance.get(:default)
      shard_index = Router.shard_for(ctx, key)

      dedicated_path =
        Ferricstore.Store.Promotion.dedicated_path(ctx.data_dir, shard_index, :hash, key)

      File.mkdir_p!(dedicated_path)
      on_exit(fn -> File.rm_rf(dedicated_path) end)

      refute GenServer.call(Router.shard_name(ctx, shard_index), {:promoted?, key})

      assert [1, "value"] =
               Coordinator.execute(
                 [{"HSET", [key, "field", "value"]}, {"HGET", [key, "field"]}],
                 %{},
                 nil
               )
    end
  end

  describe "cross-shard transactions" do
    test "rejects independent Raft groups before applying any mutation", %{k0: k0, k1: k1} do
      assert {:error, "CROSSSLOT Keys in request don't hash to the same slot"} =
               Coordinator.execute(
                 [{"SET", [k0, "unsafe-0"]}, {"SET", [k1, "unsafe-1"]}],
                 %{},
                 nil
               )

      assert Router.get(FerricStore.Instance.get(:default), k0) == nil
      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
    end

    @tag :prepared_multi_routing
    test "prepared MSET spanning shards is rejected without version changes", %{k0: k0, k1: k1} do
      ctx = FerricStore.Instance.get(:default)
      idx0 = Router.shard_for(ctx, k0)
      idx1 = Router.shard_for(ctx, k1)
      before0 = WriteVersion.get(idx0)
      before1 = WriteVersion.get(idx1)

      assert {:ok, prepared} = PreparedCommand.prepare("MSET", [k0, "v0", k1, "v1"])
      assert_crossslot(Coordinator.execute([prepared], %{}, nil))

      assert Router.get(ctx, k0) == nil
      assert Router.get(ctx, k1) == nil
      assert WriteVersion.get(idx0) == before0
      assert WriteVersion.get(idx1) == before1
    end

    @tag :prepared_multi_routing
    test "prepared RENAME spanning shards leaves the source intact", %{
      k0: source,
      k1: destination
    } do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, source, "move-once", 0)

      assert {:ok, prepared} = PreparedCommand.prepare("RENAME", [source, destination])
      assert_crossslot(Coordinator.execute([prepared], %{}, nil))

      assert Router.get(ctx, source) == "move-once"
      assert Router.get(ctx, destination) == nil
    end

    @tag :prepared_multi_routing
    test "prepared COPY spanning shards leaves the destination absent", %{
      k0: source,
      k1: destination
    } do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, source, "copy-me", 0)

      assert {:ok, prepared} = PreparedCommand.prepare("COPY", [source, destination])
      assert_crossslot(Coordinator.execute([prepared], %{}, nil))

      assert Router.get(ctx, source) == "copy-me"
      assert Router.get(ctx, destination) == nil
    end

    @tag :prepared_unlink_keys
    test "prepared UNLINK spanning shards leaves every key intact", %{k0: k0, k1: k1} do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, k0, "value-0", 0)
      :ok = Router.put(ctx, k1, "value-1", 0)
      idx0 = Router.shard_for(ctx, k0)
      idx1 = Router.shard_for(ctx, k1)
      before0 = WriteVersion.get(idx0)
      before1 = WriteVersion.get(idx1)

      assert {:ok, prepared} = PreparedCommand.prepare("UNLINK", [k0, k1])
      assert prepared.acl_keys == [k0, k1]
      assert prepared.routing_keys == [k0, k1]
      assert prepared.write_keys == [k0, k1]
      assert_crossslot(Coordinator.execute([prepared], %{}, nil))

      assert Router.get(ctx, k0) == "value-0"
      assert Router.get(ctx, k1) == "value-1"
      assert WriteVersion.get(idx0) == before0
      assert WriteVersion.get(idx1) == before1
    end

    test "mixed reads and writes spanning shards are rejected", %{k0: k0, k1: k1} do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, k0, "existing", 0)

      assert_crossslot(Coordinator.execute([{"GET", [k0]}, {"SET", [k1, "new"]}], %{}, nil))
      assert Router.get(ctx, k0) == "existing"
      assert Router.get(ctx, k1) == nil
    end
  end

  describe "hash tags co-locate keys" do
    test "keys with same hash tag route to same shard" do
      queue = [
        {"SET", ["{user:42}:name", "Alice"]},
        {"SET", ["{user:42}:email", "alice@example.com"]},
        {"GET", ["{user:42}:name"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, "Alice"]
    end
  end

  describe "WATCH conflict detection" do
    test "aborts when a watched key was modified before EXEC", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "original", 0)

      watched = %{s1 => Router.watch_token(FerricStore.Instance.get(:default), s1)}

      # Simulate another client modifying the key
      Router.put(FerricStore.Instance.get(:default), s1, "modified_by_other", 0)

      queue = [{"SET", [s1, "should_not_apply"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == nil
      assert Router.get(FerricStore.Instance.get(:default), s1) == "modified_by_other"
    end

    test "aborts when a watched hot key is rewritten to the same value", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "same", 0)
      watched = %{s1 => Router.watch_token(FerricStore.Instance.get(:default), s1)}

      # Redis WATCH invalidates on writes, not only visible value changes.
      Router.put(FerricStore.Instance.get(:default), s1, "same", 0)

      queue = [{"SET", [s1, "should_not_apply"]}]

      assert Coordinator.execute(queue, watched, nil) == nil
      assert Router.get(FerricStore.Instance.get(:default), s1) == "same"
    end

    test "proceeds when watched keys are unmodified", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "original", 0)

      watched = %{s1 => Router.watch_token(FerricStore.Instance.get(:default), s1)}

      queue = [{"SET", [s1, "updated"]}]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok]
      assert Router.get(FerricStore.Instance.get(:default), s1) == "updated"
    end

    test "aborts when a watched key changes after preflight before apply", %{same1: s1, same2: s2} do
      Router.put(FerricStore.Instance.get(:default), s1, "original", 0)

      watched = %{s1 => Router.watch_token(FerricStore.Instance.get(:default), s1)}

      Process.put(:ferricstore_tx_after_watch_preflight_hook, fn ->
        Router.put(FerricStore.Instance.get(:default), s1, "raced", 0)
      end)

      try do
        result = Coordinator.execute([{"SET", [s2, "should_not_commit"]}], watched, nil)

        assert result == nil
        assert Router.get(FerricStore.Instance.get(:default), s1) == "raced"
        assert Router.get(FerricStore.Instance.get(:default), s2) == nil
      after
        Process.delete(:ferricstore_tx_after_watch_preflight_hook)
      end
    end

    test "an empty transaction still aborts when its watched key changed", %{same1: key} do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, key, "before", 0)
      watched = %{key => Router.watch_token(ctx, key)}
      :ok = Router.put(ctx, key, "after", 0)

      assert Coordinator.execute([], watched, nil) == nil
    end

    test "a compound-field mutation aborts the watched transaction", %{
      same1: hash_key,
      same2: target_key
    } do
      ctx = FerricStore.Instance.get(:default)
      :ok = FerricStore.hset(hash_key, %{"field" => "before"})
      watched = %{hash_key => Router.watch_token(ctx, hash_key)}
      :ok = FerricStore.hset(hash_key, %{"field" => "after"})

      assert Coordinator.execute([{"SET", [target_key, "must-not-commit"]}], watched, nil) == nil
      assert Router.get(ctx, target_key) == nil
    end

    @tag :transaction_watch_catalog
    test "batched WATCH bounds aggregate compound catalog work" do
      ctx = FerricStore.Instance.get(:default)
      tag = System.unique_integer([:positive, :monotonic])
      first = "watch-budget:{#{tag}}:first"
      second = "watch-budget:{#{tag}}:second"
      shard_index = Router.shard_for(ctx, first)
      keydir = elem(ctx.keydir_refs, shard_index)
      index = Ferricstore.Store.Shard.CompoundMemberIndex.table_name(ctx.name, shard_index)

      internal_keys =
        Enum.flat_map([first, second], fn key ->
          [
            Ferricstore.Store.CompoundKey.type_key(key)
            | Enum.map(
                1..5_001,
                &Ferricstore.Store.CompoundKey.hash_field(key, "field:#{&1}")
              )
          ]
        end)

      rows =
        Enum.map(internal_keys, fn key ->
          value = if String.starts_with?(key, "T:"), do: "hash", else: "value"
          {key, value, 0, Ferricstore.Store.LFU.initial(), :pending, 0, 0}
        end)

      true = :ets.insert(keydir, rows)

      Enum.each(internal_keys, fn key ->
        Ferricstore.Store.Shard.CompoundMemberIndex.put(index, key)
      end)

      on_exit(fn ->
        Enum.each(internal_keys, fn key ->
          :ets.delete(keydir, key)
          Ferricstore.Store.Shard.CompoundMemberIndex.delete(index, key)
        end)
      end)

      assert {:error, :watch_scan_budget_exceeded} = Router.watch_tokens(ctx, [first, second])
    end

    @tag :transaction_watch_catalog
    test "compound WATCH uses the exact catalog instead of scanning shard ETS" do
      source =
        Path.expand(
          "../../../lib/ferricstore/raft/state_machine/sections/cross_shard_dispatch.ex",
          __DIR__
        )
        |> File.read!()

      [_prefix, body_and_rest] =
        String.split(source, "defp transaction_compound_watch_keys", parts: 2)

      [body | _rest] = String.split(body_and_rest, "\n      defp ", parts: 2)

      assert body =~ "CompoundMemberIndex.keys_for_prefix"
      refute body =~ "prefix_collect_keys"
      refute body =~ ":ets.select"
    end

    test "cross-shard WATCH is rejected when watches pass", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "orig_k0", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      queue = [
        {"SET", [k0, "new_k0"]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert_crossslot(result)
      assert Router.get(FerricStore.Instance.get(:default), k0) == "orig_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
    end

    test "cross-shard WATCH conflict still returns CROSSSLOT", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "orig_k0", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      # Modify watched key
      Router.put(FerricStore.Instance.get(:default), k0, "changed", 0)

      queue = [
        {"SET", [k0, "new_k0"]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert_crossslot(result)
    end
  end

  describe "concurrent single-shard transactions" do
    test "serialize correctly via GenServer", %{same1: s1} do
      Router.put(FerricStore.Instance.get(:default), s1, "0", 0)

      task1 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", [s1]}], %{}, nil)
        end)

      task2 =
        Task.async(fn ->
          Coordinator.execute([{"INCR", [s1]}], %{}, nil)
        end)

      result1 = Task.await(task1)
      result2 = Task.await(task2)

      assert is_list(result1)
      assert is_list(result2)

      assert Router.get(FerricStore.Instance.get(:default), s1) == "2"
    end
  end

  describe "sandbox namespace support" do
    @tag :transaction_sandbox_multi_key
    test "namespaces both SMOVE keys during replicated apply" do
      namespace = "tenant:"
      source = "{sandbox-smove}:source"
      destination = "{sandbox-smove}:destination"

      assert {:ok, 1} = FerricStore.sadd(namespace <> source, ["member"])
      assert {:ok, 1} = FerricStore.sadd(destination, ["outside"])
      assert {:ok, prepared} = PreparedCommand.prepare("SMOVE", [source, destination, "member"])

      assert [1] =
               Ferricstore.Transaction.Coordinator.execute([prepared], %{}, namespace)

      assert {:ok, []} = FerricStore.smembers(namespace <> source)
      assert {:ok, ["member"]} = FerricStore.smembers(namespace <> destination)
      assert {:ok, ["outside"]} = FerricStore.smembers(destination)
    end

    @tag :transaction_sandbox_multi_key
    test "namespaces both RPOPLPUSH keys during replicated apply" do
      namespace = "tenant:"
      source = "{sandbox-rpoplpush}:source"
      destination = "{sandbox-rpoplpush}:destination"

      assert {:ok, 2} = FerricStore.rpush(namespace <> source, ["first", "last"])
      assert {:ok, 1} = FerricStore.rpush(destination, ["outside"])
      assert {:ok, prepared} = PreparedCommand.prepare("RPOPLPUSH", [source, destination])

      assert ["last"] =
               Ferricstore.Transaction.Coordinator.execute([prepared], %{}, namespace)

      assert {:ok, ["first"]} = FerricStore.lrange(namespace <> source, 0, -1)
      assert {:ok, ["last"]} = FerricStore.lrange(namespace <> destination, 0, -1)
      assert {:ok, ["outside"]} = FerricStore.lrange(destination, 0, -1)
    end

    @tag :transaction_sandbox_multi_key
    test "namespaces GEOSEARCHSTORE source and destination during replicated apply" do
      namespace = "tenant:"
      source = "{sandbox-geosearchstore}:source"
      destination = "{sandbox-geosearchstore}:destination"
      coordinates = {13.361389, 38.115556}
      {longitude, latitude} = coordinates

      assert {:ok, 1} =
               FerricStore.geoadd(namespace <> source, [{longitude, latitude, "inside"}])

      assert {:ok, 1} = FerricStore.geoadd(source, [{longitude, latitude, "outside"}])

      assert {:ok, prepared} =
               PreparedCommand.prepare("GEOSEARCHSTORE", [
                 destination,
                 source,
                 "FROMLONLAT",
                 Float.to_string(longitude),
                 Float.to_string(latitude),
                 "BYRADIUS",
                 "1",
                 "KM"
               ])

      assert [1] =
               Ferricstore.Transaction.Coordinator.execute([prepared], %{}, namespace)

      assert {:ok, ["inside"]} = FerricStore.zrange(namespace <> destination, 0, -1)
      assert {:ok, []} = FerricStore.zrange(destination, 0, -1)
    end

    @tag :prepared_multi_routing
    test "prepared routing namespaces metadata keys before shard classification" do
      ctx = FerricStore.Instance.get(:default)
      namespace = "prepared-routing-namespace:"

      key =
        Enum.find_value(1..10_000, fn suffix ->
          candidate = "key-#{suffix}"
          raw_idx = Router.shard_for(ctx, candidate)
          namespaced_idx = Router.shard_for(ctx, namespace <> candidate)
          if raw_idx != namespaced_idx, do: candidate
        end)

      assert is_binary(key)
      raw_idx = Router.shard_for(ctx, key)
      namespaced_idx = Router.shard_for(ctx, namespace <> key)
      raw_before = WriteVersion.get(raw_idx)
      namespaced_before = WriteVersion.get(namespaced_idx)

      assert {:ok, prepared} = PreparedCommand.prepare("SET", [key, "namespaced"])
      assert Coordinator.execute([prepared], %{}, namespace) == [:ok]

      assert Router.get(ctx, key) == nil
      assert Router.get(ctx, namespace <> key) == "namespaced"
      assert WriteVersion.get(raw_idx) == raw_before
      assert WriteVersion.get(namespaced_idx) == namespaced_before + 1
    end

    test "respects sandbox namespace for key routing", %{same1: s1} do
      ns = "test_ns:"

      Router.put(FerricStore.Instance.get(:default), ns <> s1, "ns_value", 0)

      # Single key avoids cross-shard issues with namespace prefix
      queue = [{"GET", [s1]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert length(result) == 1
      assert hd(result) == "ns_value"
    end

    test "sandbox namespace SET and GET on same key", %{same1: s1} do
      ns = "test_ns:"

      queue = [{"SET", [s1, "ns_val"]}, {"GET", [s1]}]

      result = Coordinator.execute(queue, %{}, ns)

      assert is_list(result)
      assert result == [:ok, "ns_val"]
    end
  end

  describe "empty transaction" do
    test "returns empty list for empty queue" do
      result = Coordinator.execute([], %{}, nil)
      assert result == []
    end
  end

  defp raft_applied_count(shard_index) do
    {:ok, {:raft_log_pos, index, _term}} =
      Ferricstore.Raft.WARaftBackend.storage_position(shard_index)

    index
  end

  defp assert_crossslot(result) do
    assert result == {:error, "CROSSSLOT Keys in request don't hash to the same slot"}
  end
end
