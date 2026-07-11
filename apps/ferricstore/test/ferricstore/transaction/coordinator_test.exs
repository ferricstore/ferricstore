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
    test "production coordinator rejects queue entries without a prepared AST", %{same1: key} do
      assert {:error, "ERR invalid transaction command"} =
               Ferricstore.Transaction.Coordinator.execute([{"GET", [key]}], %{}, nil)
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
  end

  describe "cross-shard succeeds atomically" do
    @tag :prepared_multi_routing
    test "prepared MSET tracks every write shard while returning one result", %{k0: k0, k1: k1} do
      ctx = FerricStore.Instance.get(:default)
      idx0 = Router.shard_for(ctx, k0)
      idx1 = Router.shard_for(ctx, k1)
      before0 = WriteVersion.get(idx0)
      before1 = WriteVersion.get(idx1)

      assert {:ok, prepared} = PreparedCommand.prepare("MSET", [k0, "v0", k1, "v1"])
      assert Coordinator.execute([prepared], %{}, nil) == [:ok]

      assert Router.get(ctx, k0) == "v0"
      assert Router.get(ctx, k1) == "v1"
      assert WriteVersion.get(idx0) == before0 + 1
      assert WriteVersion.get(idx1) == before1 + 1
    end

    @tag :prepared_multi_routing
    test "prepared RENAME spanning shards executes exactly once", %{k0: source, k1: destination} do
      ctx = FerricStore.Instance.get(:default)
      :ok = Router.put(ctx, source, "move-once", 0)

      assert {:ok, prepared} = PreparedCommand.prepare("RENAME", [source, destination])
      assert Coordinator.execute([prepared], %{}, nil) == [:ok]

      assert Router.get(ctx, source) == nil
      assert Router.get(ctx, destination) == "move-once"
    end

    @tag :prepared_multi_routing
    test "prepared COPY bumps only its destination write shard", %{k0: source, k1: destination} do
      ctx = FerricStore.Instance.get(:default)
      source_idx = Router.shard_for(ctx, source)
      destination_idx = Router.shard_for(ctx, destination)
      :ok = Router.put(ctx, source, "copy-me", 0)
      source_before = WriteVersion.get(source_idx)
      destination_before = WriteVersion.get(destination_idx)

      assert {:ok, prepared} = PreparedCommand.prepare("COPY", [source, destination])
      assert Coordinator.execute([prepared], %{}, nil) == [1]

      assert Router.get(ctx, source) == "copy-me"
      assert Router.get(ctx, destination) == "copy-me"
      assert WriteVersion.get(source_idx) == source_before
      assert WriteVersion.get(destination_idx) == destination_before + 1
    end

    test "two shards succeeds", %{k0: k0, k1: k1} do
      queue = [{"SET", [k0, "val_k0"]}, {"SET", [k1, "val_k1"]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "val_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "val_k1"
    end

    test "four shards succeeds", %{k0: k0, k1: k1, k2: k2, k3: k3} do
      queue = [
        {"SET", [k0, "v0"]},
        {"SET", [k1, "v1"]},
        {"SET", [k2, "v2"]},
        {"SET", [k3, "v3"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, :ok, :ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "v0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "v1"
      assert Router.get(FerricStore.Instance.get(:default), k2) == "v2"
      assert Router.get(FerricStore.Instance.get(:default), k3) == "v3"
    end

    test "mixed read/write across shards succeeds", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "existing_k0", 0)

      queue = [
        {"GET", [k0]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == ["existing_k0", :ok]
      assert Router.get(FerricStore.Instance.get(:default), k1) == "new_k1"
    end

    test "large SET is visible to later GET in same cross-shard transaction", %{
      k0: k0,
      k1: k1
    } do
      ctx = FerricStore.Instance.get(:default)
      large = :binary.copy("y", ctx.hot_cache_max_value_size + 1024)

      queue = [
        {"SET", [k0, large]},
        {"GET", [k0]},
        {"SET", [k1, "other"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [:ok, large, :ok]
    end

    test "large HSET is visible to later HGETALL in same cross-shard transaction", %{
      k0: k0,
      k1: k1
    } do
      ctx = FerricStore.Instance.get(:default)
      large = :binary.copy("c", ctx.hot_cache_max_value_size + 1024)

      queue = [
        {"HSET", [k0, "field", large]},
        {"HGETALL", [k0]},
        {"SET", [k1, "other"]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, ["field", large], :ok]
    end

    test "INCR across shards succeeds", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "10", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "20", 0)

      queue = [{"INCR", [k0]}, {"INCR", [k1]}]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [{:ok, 11}, {:ok, 21}]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "11"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "21"
    end

    test "DEL across shards succeeds", %{k0: k0, k1: k1, k2: k2} do
      Router.put(FerricStore.Instance.get(:default), k0, "v", 0)
      Router.put(FerricStore.Instance.get(:default), k1, "v", 0)
      Router.put(FerricStore.Instance.get(:default), k2, "v", 0)

      queue = [
        {"DEL", [k0]},
        {"DEL", [k1]},
        {"DEL", [k2]}
      ]

      result = Coordinator.execute(queue, %{}, nil)

      assert result == [1, 1, 1]
      assert Router.get(FerricStore.Instance.get(:default), k0) == nil
      assert Router.get(FerricStore.Instance.get(:default), k1) == nil
      assert Router.get(FerricStore.Instance.get(:default), k2) == nil
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

    test "cross-shard WATCH succeeds when watches pass", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "orig_k0", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      queue = [
        {"SET", [k0, "new_k0"]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      assert result == [:ok, :ok]
      assert Router.get(FerricStore.Instance.get(:default), k0) == "new_k0"
      assert Router.get(FerricStore.Instance.get(:default), k1) == "new_k1"
    end

    test "cross-shard WATCH conflict returns nil", %{k0: k0, k1: k1} do
      Router.put(FerricStore.Instance.get(:default), k0, "orig_k0", 0)

      watched = %{k0 => Router.watch_token(FerricStore.Instance.get(:default), k0)}

      # Modify watched key
      Router.put(FerricStore.Instance.get(:default), k0, "changed", 0)

      queue = [
        {"SET", [k0, "new_k0"]},
        {"SET", [k1, "new_k1"]}
      ]

      result = Coordinator.execute(queue, watched, nil)

      # WATCH fails first -> nil (before classify even runs)
      assert result == nil
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
end
