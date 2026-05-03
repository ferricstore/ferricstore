defmodule Ferricstore.InstanceTest do
  @moduledoc "Tests that use FerricStore pattern works end-to-end."
  use ExUnit.Case, async: false

  defmodule EmbeddedA do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedB do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedDefaultOptions do
    use FerricStore, shard_count: 1
  end

  # Use the :default instance (created at app boot)
  # In future: test with a custom isolated instance

  setup do
    Ferricstore.Test.ShardHelpers.flush_all_keys()
  end

  describe "use FerricStore embedded instances" do
    test "start with isolated shard supervisors and data dirs" do
      root =
        Path.join(System.tmp_dir!(), "ferricstore_embedded_#{System.unique_integer([:positive])}")

      dir_a = Path.join(root, "a")
      dir_b = Path.join(root, "b")
      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedA.stop()
        EmbeddedB.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid_a} = EmbeddedA.start_link(data_dir: dir_a, shard_count: 1)

      assert {:ok, _pid_b} = EmbeddedB.start_link(data_dir: dir_b, shard_count: 1)

      assert :ok = EmbeddedA.set("same-key", "from-a")
      assert :ok = EmbeddedB.set("same-key", "from-b")

      assert {:ok, "from-a"} = EmbeddedA.get("same-key")
      assert {:ok, "from-b"} = EmbeddedB.get("same-key")
      refute Map.has_key?(EmbeddedA.__instance__(), :raft_enabled)
      refute Map.has_key?(EmbeddedB.__instance__(), :raft_enabled)
    end

    test "custom instances reject the raft_enabled option" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_raft_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        File.rm_rf(root)
      end)

      assert_raise ArgumentError,
                   ~r/:raft_enabled is not supported for custom FerricStore instances/,
                   fn ->
                     Code.compile_string("""
                     defmodule Ferricstore.InstanceTest.EmbeddedRaftRequested#{System.unique_integer([:positive])} do
                       use FerricStore, shard_count: 1, raft_enabled: true
                     end
                     """)
                   end

      assert {:error, {:unsupported_custom_option, EmbeddedA, :raft_enabled}} =
               EmbeddedA.start_link(
                 data_dir: root,
                 shard_count: 1,
                 raft_enabled: false
               )

      ctx = FerricStore.Instance.get(:default)
      key = "embedded-raft-guard:#{System.unique_integer([:positive])}"
      assert :ok = Ferricstore.Store.Router.put(ctx, key, "default-still-runs", 0)
      assert "default-still-runs" = Ferricstore.Store.Router.get(ctx, key)
    end

    test "custom instances default to non-Raft local mode" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_local_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedDefaultOptions.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedDefaultOptions.start_link(data_dir: root, shard_count: 1)
      refute Map.has_key?(EmbeddedDefaultOptions.__instance__(), :raft_enabled)
      assert :ok = EmbeddedDefaultOptions.set("same-key", "local")
      assert {:ok, "local"} = EmbeddedDefaultOptions.get("same-key")
    end

    test "direct instance builds reject raft_enabled option" do
      name = :"custom_direct_build_#{System.unique_integer([:positive])}"

      assert_raise ArgumentError, ~r/:raft_enabled is not supported/, fn ->
        FerricStore.Instance.build(name, shard_count: 1, raft_enabled: true)
      end
    end

    test "custom instances start isolated merge schedulers" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_merge_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedDefaultOptions.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedDefaultOptions.start_link(data_dir: root, shard_count: 1)

      custom_scheduler = :"#{EmbeddedDefaultOptions}.Merge.Scheduler.0"
      custom_semaphore = :"#{EmbeddedDefaultOptions}.Merge.Semaphore"

      assert is_pid(Process.whereis(custom_scheduler))
      assert is_pid(Process.whereis(custom_semaphore))

      assert Process.whereis(custom_scheduler) !=
               Process.whereis(Ferricstore.Merge.Scheduler.scheduler_name(0))

      status = Ferricstore.Merge.Scheduler.status(custom_scheduler)
      assert status.shard_index == 0
    end

    test "custom shard rotations notify the custom merge scheduler" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_rotation_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedDefaultOptions.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} =
               EmbeddedDefaultOptions.start_link(
                 data_dir: root,
                 shard_count: 1,
                 max_active_file_size: 1,
                 merge_config: %{min_files_for_merge: 1_000}
               )

      custom_scheduler = :"#{EmbeddedDefaultOptions}.Merge.Scheduler.0"

      assert :ok = EmbeddedDefaultOptions.set("rotate-a", String.duplicate("a", 128))
      assert :ok = EmbeddedDefaultOptions.set("rotate-b", String.duplicate("b", 128))

      assert eventually(fn ->
               Ferricstore.Merge.Scheduler.status(custom_scheduler).file_count >= 2
             end)
    end
  end

  describe "custom instance cleanup" do
    test "parent supervisor shutdown removes cached custom instance context" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_parent_stop_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        FerricStore.Instance.cleanup(EmbeddedDefaultOptions)
        File.rm_rf(root)
      end)

      {:ok, parent} =
        Supervisor.start_link(
          [EmbeddedDefaultOptions.child_spec(data_dir: root, shard_count: 1)],
          strategy: :one_for_one
        )

      assert :ok = EmbeddedDefaultOptions.set("parent-stop", "value")
      assert {:ok, "value"} = EmbeddedDefaultOptions.get("parent-stop")

      assert :ok = Supervisor.stop(parent)

      assert_raise ArgumentError, fn ->
        FerricStore.Instance.get(EmbeddedDefaultOptions)
      end
    end

    test "removes latch ETS tables" do
      name = :"cleanup_latch_#{System.unique_integer([:positive])}"
      on_exit(fn -> FerricStore.Instance.cleanup(name) end)

      ctx =
        FerricStore.Instance.build(name,
          data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
          shard_count: 2
        )

      latch_0 = elem(ctx.latch_refs, 0)
      latch_1 = elem(ctx.latch_refs, 1)

      assert :ets.whereis(latch_0) != :undefined
      assert :ets.whereis(latch_1) != :undefined

      FerricStore.Instance.cleanup(name)

      assert :ets.whereis(latch_0) == :undefined
      assert :ets.whereis(latch_1) == :undefined
    end
  end

  describe "FerricStore.Impl with default instance" do
    test "set and get" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_key", "impl_value")
      assert {:ok, "impl_value"} = FerricStore.Impl.get(ctx, "impl_key")
    end

    test "del" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_del", "val")
      assert {:ok, 1} = FerricStore.Impl.del(ctx, ["impl_del"])
      assert {:ok, nil} = FerricStore.Impl.get(ctx, "impl_del")
    end

    test "incr" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 1} = FerricStore.Impl.incr(ctx, "impl_counter", 1)
      assert {:ok, 6} = FerricStore.Impl.incr(ctx, "impl_counter", 5)
    end

    test "hash operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 2} = FerricStore.Impl.hset(ctx, "impl_hash", %{"f1" => "v1", "f2" => "v2"})
      assert {:ok, "v1"} = FerricStore.Impl.hget(ctx, "impl_hash", "f1")
      assert {:ok, map} = FerricStore.Impl.hgetall(ctx, "impl_hash")
      assert map == %{"f1" => "v1", "f2" => "v2"}
    end

    test "hash read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_hash:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.hget(ctx, "impl_hash:string", "f")
      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.hexists(ctx, "impl_hash:string", "f")
    end

    test "set operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 3} = FerricStore.Impl.sadd(ctx, "impl_set", ["a", "b", "c"])
      assert {:ok, true} = FerricStore.Impl.sismember(ctx, "impl_set", "a")
      assert {:ok, false} = FerricStore.Impl.sismember(ctx, "impl_set", "z")
      assert {:ok, 3} = FerricStore.Impl.scard(ctx, "impl_set")
    end

    test "set read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_set:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.sismember(ctx, "impl_set:string", "a")
    end

    test "list operations" do
      ctx = FerricStore.Instance.get(:default)
      assert {:ok, 3} = FerricStore.Impl.lpush(ctx, "impl_list", ["a", "b", "c"])
      assert {:ok, 3} = FerricStore.Impl.llen(ctx, "impl_list")
    end

    test "sorted set read operations return WRONGTYPE directly" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.set(ctx, "impl_zset:string", "plain")

      assert {:error, "WRONGTYPE" <> _} = FerricStore.Impl.zscore(ctx, "impl_zset:string", "a")
    end

    test "bloom filter" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.bf_reserve(ctx, "impl_bf", 0.01, 100)
      assert {:ok, 1} = FerricStore.Impl.bf_add(ctx, "impl_bf", "hello")
      assert {:ok, 1} = FerricStore.Impl.bf_exists(ctx, "impl_bf", "hello")
      assert {:ok, 0} = FerricStore.Impl.bf_exists(ctx, "impl_bf", "missing")
    end

    test "CMS" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.cms_initbydim(ctx, "impl_cms", 100, 7)
      assert {:ok, [5]} = FerricStore.Impl.cms_incrby(ctx, "impl_cms", [{"apple", 5}])
      assert {:ok, [5]} = FerricStore.Impl.cms_query(ctx, "impl_cms", ["apple"])
    end

    test "cuckoo filter" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.cf_reserve(ctx, "impl_cf", 1024)
      assert {:ok, 1} = FerricStore.Impl.cf_add(ctx, "impl_cf", "elem")
      assert {:ok, 1} = FerricStore.Impl.cf_exists(ctx, "impl_cf", "elem")
    end

    test "topk" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.topk_reserve(ctx, "impl_topk", 3)
      FerricStore.Impl.topk_add(ctx, "impl_topk", ["a", "b", "c"])
      assert {:ok, items} = FerricStore.Impl.topk_list(ctx, "impl_topk")
      assert is_list(items)
    end

    test "tdigest" do
      ctx = FerricStore.Instance.get(:default)
      assert :ok = FerricStore.Impl.tdigest_create(ctx, "impl_td")
      assert :ok = FerricStore.Impl.tdigest_add(ctx, "impl_td", [1, 2, 3, 4, 5])
    end

    test "keys and dbsize" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_k1", "v1")
      FerricStore.Impl.set(ctx, "impl_k2", "v2")
      {:ok, keys} = FerricStore.Impl.keys(ctx)
      assert "impl_k1" in keys
      assert "impl_k2" in keys
    end

    test "flushdb" do
      ctx = FerricStore.Instance.get(:default)
      FerricStore.Impl.set(ctx, "impl_flush", "val")
      assert {:ok, "val"} = FerricStore.Impl.get(ctx, "impl_flush")
      :ok = FerricStore.Impl.flushdb(ctx)
      assert {:ok, nil} = FerricStore.Impl.get(ctx, "impl_flush")
    end
  end

  defp eventually(fun, attempts \\ 50)

  defp eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(10)
      eventually(fun, attempts - 1)
    end
  end

  defp eventually(_fun, 0), do: false
end
