defmodule Ferricstore.InstanceTest do
  @moduledoc "Tests that use FerricStore pattern works end-to-end."
  use ExUnit.Case, async: false
  @moduletag :global_state

  defmodule EmbeddedA do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedB do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedDefaultOptions do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedStopFailure do
    use FerricStore, shard_count: 1
  end

  defmodule EmbeddedFlow do
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
      assert :ok = EmbeddedDefaultOptions.set("same-key", "local")
      assert {:ok, "local"} = EmbeddedDefaultOptions.get("same-key")
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

    test "custom instances automatically sweep overdue Flow records" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_retention_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedFlow.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} =
               EmbeddedFlow.start_link(
                 data_dir: root,
                 shard_count: 1,
                 flow_retention_sweeper: [initial_delay_ms: 0, interval_ms: 10]
               )

      sweeper_name = :"#{EmbeddedFlow}.Flow.RetentionSweeper"
      assert is_pid(Process.whereis(sweeper_name))

      now = System.system_time(:millisecond)
      id = "embedded-flow-active-timeout"

      assert :ok =
               EmbeddedFlow.flow_create(id,
                 type: "embedded-active-timeout",
                 max_active_ms: 5,
                 run_at_ms: now + 60_000,
                 now_ms: now
               )

      assert eventually(fn ->
               case EmbeddedFlow.flow_get(id, full: true) do
                 {:ok, %{state: "failed", error: %{reason: "max_active_ms"}}} -> true
                 _other -> false
               end
             end)
    end

    test "custom instances persist active timeout failure while rejecting completion" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_timeout_complete_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedFlow.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedFlow.start_link(data_dir: root, shard_count: 1)

      id = "embedded-flow-timeout-complete"
      type = "embedded-flow-timeout-complete"
      create_now = System.system_time(:millisecond) + 60_000
      timeout_now = create_now + 100

      assert :ok =
               EmbeddedFlow.flow_create(id,
                 type: type,
                 max_active_ms: 100,
                 run_at_ms: create_now,
                 now_ms: create_now
               )

      assert {:ok, [claimed]} =
               EmbeddedFlow.flow_claim_due(type,
                 worker: "embedded-timeout-worker",
                 lease_ms: 1_000,
                 now_ms: create_now
               )

      assert {:error, reason} =
               EmbeddedFlow.flow_complete(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 now_ms: timeout_now
               )

      assert reason =~ "max_active_ms"

      assert {:ok, %{state: "failed", error: %{reason: "max_active_ms"}}} =
               EmbeddedFlow.flow_get(id, full: true)
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

    test "custom instances expose the Flow embedded API" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_flow_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedFlow.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedFlow.start_link(data_dir: root, shard_count: 1)

      id = "embedded-flow-1"
      type = "embedded-flow"
      partition = "tenant-a"

      assert :ok =
               EmbeddedFlow.flow_create(id,
                 type: type,
                 run_at_ms: 1_000,
                 partition_key: partition,
                 parent_flow_id: "parent-1",
                 root_flow_id: "root-1",
                 correlation_id: "corr-1",
                 now_ms: 1_000
               )

      assert {:ok, %{id: ^id, state: "queued"}} =
               EmbeddedFlow.flow_get(id, partition_key: partition)

      assert {:ok, [%{id: ^id}]} = EmbeddedFlow.flow_list(type, partition_key: partition)

      assert {:ok, [%{id: ^id}]} =
               EmbeddedFlow.flow_by_parent("parent-1", partition_key: partition)

      assert {:ok, [%{id: ^id}]} = EmbeddedFlow.flow_by_root("root-1", partition_key: partition)

      assert {:ok, [%{id: ^id}]} =
               EmbeddedFlow.flow_by_correlation("corr-1", partition_key: partition)

      assert {:ok, [claimed]} =
               EmbeddedFlow.flow_claim_due(type,
                 worker: "worker-1",
                 partition_key: partition,
                 now_ms: 1_000
               )

      assert {:ok, %{id: ^id, state: "running", lease_deadline_ms: 6_000}} =
               EmbeddedFlow.flow_extend_lease(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 lease_ms: 5_000,
                 partition_key: partition,
                 now_ms: 1_000
               )

      assert :ok =
               EmbeddedFlow.flow_complete(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition,
                 now_ms: 2_000
               )

      assert {:ok, %{id: ^id, state: "completed"}} =
               EmbeddedFlow.flow_get(id, partition_key: partition)

      assert {:ok, %{completed: 1, inflight: 0}} =
               EmbeddedFlow.flow_info(type, partition_key: partition)

      assert {:ok, history} = EmbeddedFlow.flow_history(id, partition_key: partition)
      assert Enum.any?(history, fn {_event_id, fields} -> fields["event"] == "completed" end)

      spawn_parent = "embedded-spawn-parent-1"
      spawn_child = "embedded-spawn-child-1"

      assert :ok =
               EmbeddedFlow.flow_create(spawn_parent,
                 type: "embedded-parent",
                 state: "dispatch",
                 partition_key: partition,
                 now_ms: 3_000
               )

      assert {:ok, created_parent} = EmbeddedFlow.flow_get(spawn_parent, partition_key: partition)

      assert :ok =
               EmbeddedFlow.flow_spawn_children(
                 spawn_parent,
                 [%{id: spawn_child, type: "embedded-child", payload: "child-payload"}],
                 group_id: "embedded-fanout",
                 wait: :none,
                 on_child_failed: :ignore,
                 on_parent_closed: :abandon_children,
                 exhaust_to: %{success: "dispatched", failure: "dispatch_failed"},
                 partition_key: partition,
                 from_state: "dispatch",
                 fencing_token: created_parent.fencing_token,
                 now_ms: 3_010
               )

      assert {:ok, %{id: ^spawn_parent, state: "dispatched"}} =
               EmbeddedFlow.flow_get(spawn_parent, partition_key: partition)

      assert {:ok, [%{id: ^spawn_child, parent_flow_id: ^spawn_parent}]} =
               EmbeddedFlow.flow_by_parent(spawn_parent, partition_key: partition)
    end

    test "custom instances expose indexed Flow state_meta search and retention cleanup" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_flow_state_meta_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedFlow.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedFlow.start_link(data_dir: root, shard_count: 1)

      id = "embedded-flow-state-meta-1"
      type = "embedded-flow-state-meta"
      partition = "tenant-state-meta"
      now = System.system_time(:millisecond)

      assert {:ok, %{indexed_state_meta: "version"}} =
               EmbeddedFlow.flow_policy_set(type, indexed_state_meta: "version")

      assert :ok =
               EmbeddedFlow.flow_create(id,
                 type: type,
                 state: "accept",
                 partition_key: partition,
                 state_meta: %{"version" => 1},
                 retention_ttl_ms: 60_000,
                 run_at_ms: now,
                 now_ms: now
               )

      assert {:ok, [claimed]} =
               EmbeddedFlow.flow_claim_due(type,
                 states: ["accept"],
                 partition_key: partition,
                 worker: "worker-state-meta",
                 limit: 1,
                 now_ms: now + 1
               )

      assert :ok =
               EmbeddedFlow.flow_complete(id, claimed.lease_token,
                 fencing_token: claimed.fencing_token,
                 partition_key: partition,
                 state_meta: %{"version" => 3},
                 now_ms: now + 2
               )

      assert {:ok, completed} = EmbeddedFlow.flow_get(id, partition_key: partition)
      cleanup_now_ms = completed.terminal_retention_until_ms + 1

      assert eventually(fn ->
               case EmbeddedFlow.flow_search(
                      type: type,
                      partition_key: partition,
                      state_meta: %{"accept" => %{"version" => 1}},
                      consistent_projection: true,
                      count: 10
                    ) do
                 {:ok, [%{id: ^id}]} -> true
                 _ -> false
               end
             end)

      assert eventually(fn ->
               case EmbeddedFlow.flow_search(
                      type: type,
                      partition_key: partition,
                      state_meta: %{"completed" => %{"version" => 3}},
                      consistent_projection: true,
                      count: 10
                    ) do
                 {:ok, [%{id: ^id}]} -> true
                 _ -> false
               end
             end)

      assert {:ok, cleaned} =
               EmbeddedFlow.flow_retention_cleanup(limit: 10, now_ms: cleanup_now_ms)

      assert cleaned.flows >= 1

      assert eventually(fn ->
               case EmbeddedFlow.flow_search(
                      type: type,
                      partition_key: partition,
                      state_meta: %{"completed" => %{"version" => 3}},
                      consistent_projection: true,
                      count: 10
                    ) do
                 {:ok, []} -> true
                 _ -> false
               end
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

    test "custom instance shutdown flushes pending BitcaskWriter entries" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_writer_shutdown_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)

      on_exit(fn ->
        EmbeddedDefaultOptions.stop()
        File.rm_rf(root)
      end)

      assert {:ok, _pid} = EmbeddedDefaultOptions.start_link(data_dir: root, shard_count: 1)

      ctx = EmbeddedDefaultOptions.__instance__()
      writer = Process.whereis(Ferricstore.Store.BitcaskWriter.writer_name(ctx, 0))
      keydir = elem(ctx.keydir_refs, 0)
      {file_id, file_path, _shard_path} = Ferricstore.Store.ActiveFile.get(ctx, 0)
      key = "writer-shutdown-pending"
      value = "survives-stop"

      :ets.insert(
        keydir,
        {key, value, 0, Ferricstore.Store.LFU.initial(), :pending, 0, 0}
      )

      :sys.replace_state(writer, fn state ->
        %{
          state
          | pending: [{:write, ctx, file_path, file_id, keydir, key, value, 0}],
            pending_count: 1
        }
      end)

      assert :ok = EmbeddedDefaultOptions.stop()
      assert {:ok, _pid} = EmbeddedDefaultOptions.start_link(data_dir: root, shard_count: 1)
      assert {:ok, ^value} = EmbeddedDefaultOptions.get(key)
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

    test "releases native Flow index resources" do
      name = :"cleanup_flow_index_#{System.unique_integer([:positive])}"
      on_exit(fn -> FerricStore.Instance.cleanup(name) end)

      _ctx =
        FerricStore.Instance.build(name,
          data_dir: Path.join(System.tmp_dir!(), Atom.to_string(name)),
          shard_count: 2
        )

      {index_0, lookup_0} = Ferricstore.Flow.NativeOrderedIndex.table_names(name, 0)
      {index_1, lookup_1} = Ferricstore.Flow.NativeOrderedIndex.table_names(name, 1)

      assert is_reference(Ferricstore.Flow.NativeOrderedIndex.reset(index_0, lookup_0))
      assert is_reference(Ferricstore.Flow.NativeOrderedIndex.reset(index_1, lookup_1))

      FerricStore.Instance.cleanup(name)

      assert nil == Ferricstore.Flow.NativeOrderedIndex.get(index_0, lookup_0)
      assert nil == Ferricstore.Flow.NativeOrderedIndex.get(index_1, lookup_1)
    end

    test "failed custom startup cleans cached context and ETS tables" do
      root =
        Path.join(
          System.tmp_dir!(),
          "ferricstore_embedded_failed_start_#{System.unique_integer([:positive])}"
        )

      File.rm_rf!(root)
      File.mkdir_p!(root)
      invalid_data_dir = Path.join(root, "not_a_directory")
      File.write!(invalid_data_dir, "not a directory")

      on_exit(fn ->
        FerricStore.Instance.cleanup(EmbeddedDefaultOptions)
        File.rm_rf(root)
      end)

      previous_trap_exit = Process.flag(:trap_exit, true)

      try do
        assert {:error, _reason} =
                 EmbeddedDefaultOptions.start_link(data_dir: invalid_data_dir, shard_count: 1)
      after
        Process.flag(:trap_exit, previous_trap_exit)
      end

      assert_raise ArgumentError, fn ->
        FerricStore.Instance.get(EmbeddedDefaultOptions)
      end

      assert :ets.whereis(:"#{EmbeddedDefaultOptions}_latch_0") == :undefined
      assert :ets.whereis(:"#{EmbeddedDefaultOptions}_hotness") == :undefined
      assert :ets.whereis(:"#{EmbeddedDefaultOptions}_config") == :undefined
    end

    test "custom instance stop surfaces supervisor stop failures" do
      name = :"#{EmbeddedStopFailure}.Supervisor"

      fake_supervisor =
        spawn(fn ->
          receive do
            _message -> exit(:boom)
          end
        end)

      Process.register(fake_supervisor, name)

      on_exit(fn ->
        if pid = Process.whereis(name), do: Process.exit(pid, :kill)
        FerricStore.Instance.cleanup(EmbeddedStopFailure)
      end)

      assert {{:boom, {:sys, :terminate, [^fake_supervisor, :normal, :infinity]}},
              {GenServer, :stop, [^fake_supervisor, :normal, :infinity]}} =
               catch_exit(EmbeddedStopFailure.stop())
    end
  end

  describe "FerricStore.Impl with default instance" do
    setup do
      # Restart-heavy suites can leave the default instance serving while Raft
      # leaders are still electing. These tests exercise public quorum writes,
      # so wait for readiness instead of racing the first command timeout.
      FerricStore.await_ready(timeout: 60_000)
      Ferricstore.Test.ShardHelpers.wait_default_quorum_writable(60_000)
      Ferricstore.Test.ShardHelpers.flush_all_keys()
      :ok
    end

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

    test "tdigest preserves custom integer compression through the instance API" do
      ctx = FerricStore.Instance.get(:default)
      key = "impl_td_custom_#{System.unique_integer([:positive])}"

      assert :ok = FerricStore.Impl.tdigest_create(ctx, key, compression: 200)
      assert {:ok, ["Compression", 200 | _rest]} = FerricStore.Impl.tdigest_info(ctx, key)
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

    test "flushdb preserves durable server control-plane records" do
      ctx = FerricStore.Instance.get(:default)
      namespace = "flush-control-plane-#{System.unique_integer([:positive, :monotonic])}"
      key = Ferricstore.ServerCatalog.revision_key(namespace)
      value = Ferricstore.ServerCatalog.encode_revision(17)

      assert :ok = Ferricstore.Store.Router.put(ctx, key, value, 0)
      assert :ok = FerricStore.Impl.set(ctx, "impl_flush_user_data", "value")
      on_exit(fn -> Ferricstore.Store.Router.delete(ctx, key) end)

      assert :ok = FerricStore.Impl.flushdb(ctx)
      assert value == Ferricstore.Store.Router.get(ctx, key)
      assert {:ok, nil} = FerricStore.Impl.get(ctx, "impl_flush_user_data")
    end

    test "flushdb uses quorum deletes after async pressure path removal" do
      ctx = FerricStore.Instance.get(:default)
      key = "impl_flush_pressure_#{System.unique_integer([:positive])}"
      shard_index = Ferricstore.Store.Router.shard_for(ctx, key)

      :ok = Ferricstore.Store.Router.put(ctx, key, "value", 0)
      Ferricstore.Store.DiskPressure.set(ctx, shard_index)

      try do
        assert :ok = FerricStore.Impl.flushdb(ctx)
        assert {:ok, nil} = FerricStore.Impl.get(ctx, key)
      after
        Ferricstore.Store.DiskPressure.clear(ctx, shard_index)
        Ferricstore.Store.Router.delete(ctx, key)
      end
    end

    test "flushdb invalidates Flow retention trust before a destructive failure" do
      ctx = FerricStore.Instance.get(:default)

      Enum.each(0..(ctx.shard_count - 1), fn shard_index ->
        :persistent_term.put(
          {Ferricstore.Flow.SharedRefBackfill, :verified_complete, ctx.name, shard_index},
          true
        )
      end)

      broken_ctx = %{
        ctx
        | keydir_refs: put_elem(ctx.keydir_refs, 1, make_ref())
      }

      on_exit(fn -> FerricStore.Impl.flushdb(ctx) end)

      assert {:error, {:flush_internal_keydir_unavailable, %ArgumentError{}}} =
               Ferricstore.Store.Ops.flush(broken_ctx)

      for shard_index <- 0..(ctx.shard_count - 1) do
        refute Ferricstore.Flow.SharedRefBackfill.verified_complete?(ctx.name, shard_index)
      end
    end

    test "flushdb surfaces probabilistic directory fsync failures" do
      ctx = FerricStore.Instance.get(:default)
      prob_dir = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "prob")

      File.mkdir_p!(prob_dir)
      File.write!(Path.join(prob_dir, "impl_stale.bloom"), "bits")

      Process.put(:ferricstore_prob_command_fsync_dir_hook, fn
        ^prob_dir -> {:error, :eio}
        _path -> :ok
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_prob_command_fsync_dir_hook)
      end)

      assert {:error, {:fsync_dir_failed, :flush_prob_dir, :eio}} =
               FerricStore.Impl.flushdb(ctx)
    end

    test "flushdb surfaces probabilistic directory listing failures" do
      ctx = FerricStore.Instance.get(:default)
      prob_dir = Path.join(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0), "prob")

      File.rm_rf!(prob_dir)
      File.mkdir_p!(Path.dirname(prob_dir))
      File.write!(prob_dir, "not a directory")

      on_exit(fn ->
        File.rm_rf!(prob_dir)
      end)

      assert {:error, {:list_prob_dir_failed, ^prob_dir, {:not_a_directory, _message}}} =
               FerricStore.Impl.flushdb(ctx)
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
