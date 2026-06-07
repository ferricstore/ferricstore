defmodule Ferricstore.Raft.WARaftBackendTest.Sections.Part14 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

  test "Flow retention cleanup scans all WARaft shards", %{root: root} do
    ctx = build_ctx(Path.join(root, "flow-retention-cleanup"), shard_count: 2)
    flow_type = "router-flow-retention-#{System.unique_integer([:positive])}"
    flow_a = "router-flow-retention-a-#{System.unique_integer([:positive])}"
    flow_b = "router-flow-retention-b-#{System.unique_integer([:positive])}"
    partition_a = flow_partition_for_shard(ctx, flow_a, 0)
    partition_b = flow_partition_for_shard(ctx, flow_b, 1)
    now_ms = System.system_time(:millisecond)

    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      for {id, partition, worker} <- [
            {flow_a, partition_a, "worker-retention-a"},
            {flow_b, partition_b, "worker-retention-b"}
          ] do
        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: flow_type,
                   state: "queued",
                   partition_key: partition,
                   payload: %{id: id},
                   retention_ttl_ms: 10,
                   run_at_ms: now_ms,
                   now_ms: now_ms
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition,
                   worker: worker,
                   limit: 1,
                   now_ms: now_ms
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   result: %{ok: true},
                   now_ms: now_ms + 10
                 )
      end

      cleanup_now_ms = System.system_time(:millisecond) + 60_000

      assert {:ok, cleaned} =
               Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: cleanup_now_ms)

      assert cleaned.flows == 2
      assert cleaned.values >= 4

      assert {:ok, nil} = Ferricstore.Flow.get(ctx, flow_a, partition_key: partition_a)
      assert {:ok, nil} = Ferricstore.Flow.get(ctx, flow_b, partition_key: partition_b)
    after
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "Flow cross-shard terminal many commands resolve parents through WARaft", %{root: root} do
    ctx = build_ctx(Path.join(root, "flow-cross-terminal-many"), shard_count: 2)
    complete_parent = "router-flow-many-parent-complete-#{System.unique_integer([:positive])}"
    complete_child = "router-flow-many-child-complete-#{System.unique_integer([:positive])}"
    retry_parent = "router-flow-many-parent-retry-#{System.unique_integer([:positive])}"
    retry_child = "router-flow-many-child-retry-#{System.unique_integer([:positive])}"
    fail_parent = "router-flow-many-parent-fail-#{System.unique_integer([:positive])}"
    fail_child = "router-flow-many-child-fail-#{System.unique_integer([:positive])}"
    cancel_parent = "router-flow-many-parent-cancel-#{System.unique_integer([:positive])}"
    cancel_child = "router-flow-many-child-cancel-#{System.unique_integer([:positive])}"

    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      {complete_parent_partition, complete_child_partition} =
        setup_cross_shard_child_for_many!(ctx, complete_parent, complete_child, "many-complete")

      complete_claim = claim_flow_child!(ctx, complete_child, complete_child_partition, "many-c")

      assert :ok =
               Ferricstore.Flow.complete_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: complete_child,
                     partition_key: complete_child_partition,
                     lease_token: complete_claim.lease_token,
                     fencing_token: complete_claim.fencing_token
                   }
                 ],
                 now_ms: 2_000
               )

      assert {:ok, complete_done} =
               Ferricstore.Flow.get(ctx, complete_parent,
                 partition_key: complete_parent_partition
               )

      assert complete_done.state == "children_done"

      assert complete_done.child_groups["many-complete"]["children"][complete_child] ==
               "completed"

      {retry_parent_partition, retry_child_partition} =
        setup_cross_shard_child_for_many!(ctx, retry_parent, retry_child, "many-retry",
          on_child_failed: :fail_parent
        )

      retry_claim = claim_flow_child!(ctx, retry_child, retry_child_partition, "many-r")

      assert :ok =
               Ferricstore.Flow.retry_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: retry_child,
                     partition_key: retry_child_partition,
                     lease_token: retry_claim.lease_token,
                     fencing_token: retry_claim.fencing_token
                   }
                 ],
                 now_ms: 2_000,
                 retry: [max_retries: 0, exhausted_to: "failed"]
               )

      assert {:ok, retry_failed} =
               Ferricstore.Flow.get(ctx, retry_parent, partition_key: retry_parent_partition)

      assert retry_failed.state == "children_failed"
      assert retry_failed.child_groups["many-retry"]["children"][retry_child] == "failed"

      {fail_parent_partition, fail_child_partition} =
        setup_cross_shard_child_for_many!(ctx, fail_parent, fail_child, "many-fail",
          on_child_failed: :fail_parent
        )

      fail_claim = claim_flow_child!(ctx, fail_child, fail_child_partition, "many-f")

      assert :ok =
               Ferricstore.Flow.fail_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: fail_child,
                     partition_key: fail_child_partition,
                     lease_token: fail_claim.lease_token,
                     fencing_token: fail_claim.fencing_token
                   }
                 ],
                 error: "boom",
                 now_ms: 2_000
               )

      assert {:ok, fail_done} =
               Ferricstore.Flow.get(ctx, fail_parent, partition_key: fail_parent_partition)

      assert fail_done.state == "children_failed"
      assert fail_done.child_groups["many-fail"]["children"][fail_child] == "failed"

      {cancel_parent_partition, cancel_child_partition} =
        setup_cross_shard_child_for_many!(ctx, cancel_parent, cancel_child, "many-cancel",
          on_parent_closed: :cancel_children
        )

      assert {:ok, waiting_cancel_parent} =
               Ferricstore.Flow.get(ctx, cancel_parent, partition_key: cancel_parent_partition)

      assert :ok =
               Ferricstore.Flow.cancel_many(
                 ctx,
                 nil,
                 [
                   %{
                     id: cancel_parent,
                     partition_key: cancel_parent_partition,
                     fencing_token: waiting_cancel_parent.fencing_token
                   }
                 ],
                 now_ms: 2_000
               )

      assert {:ok, cancelled_child} =
               Ferricstore.Flow.get(ctx, cancel_child, partition_key: cancel_child_partition)

      assert cancelled_child.state == "cancelled"
    after
      FerricStore.Instance.cleanup(ctx.name)
    end
  end

  test "file-backed probabilistic commands use WARaft as the selected backend", %{ctx: ctx} do
    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      meta = {:bloom_meta, %{capacity: 128, error_rate: 0.01}}
      assert :ok = Router.prob_write(ctx, {:bloom_create, "router:bf", 128, 3, meta})
      assert {:ok, 1} = Router.prob_write(ctx, {:bloom_add, "router:bf", "a", nil})

      assert File.exists?(
               Path.join(
                 Path.join(
                   Ferricstore.DataDir.shard_data_path(
                     ctx.data_dir,
                     Router.shard_for(ctx, "router:bf")
                   ),
                   "prob"
                 ),
                 "#{Base.url_encode64("router:bf", padding: false)}.bloom"
               )
             )
    after
    end
  end

  test "CMS Cuckoo TopK and TDigest commands use WARaft as the selected backend", %{ctx: ctx} do
    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, "router:cms", 64, 4}, ctx)

      assert [3] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_incrby, "router:cms", [{"hot", 3}]},
                 ctx
               )

      assert [3] = Ferricstore.Commands.CMS.handle_ast({:cms_query, ["router:cms", "hot"]}, ctx)

      assert :ok = Ferricstore.Commands.Cuckoo.handle_ast({:cf_reserve, "router:cf", 128}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_add, ["router:cf", "seen"]}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_exists, ["router:cf", "seen"]}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_del, ["router:cf", "seen"]}, ctx)
      assert 0 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_exists, ["router:cf", "seen"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_reserve, "router:topk", 3, 8, 4, 0.9},
                 ctx
               )

      assert [nil, nil] =
               Ferricstore.Commands.TopK.handle_ast({:topk_add, ["router:topk", "a", "b"]}, ctx)

      assert [nil] =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_incrby, "router:topk", [{"a", 5}]},
                 ctx
               )

      assert [6] = Ferricstore.Commands.TopK.handle_ast({:topk_count, ["router:topk", "a"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_create, "router:td", 100},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_add, "router:td", [1.0, 2.0, 3.0]},
                 ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, "router:td", [0.5]},
                 ctx
               )

      assert median != "nan"
    after
    end
  end

  test "file-backed probabilistic commands survive WARaft restart", %{root: root, ctx: ctx} do
    suffix = System.unique_integer([:positive])

    bloom_key = "router:bf-restart:#{suffix}"
    cms_key = "router:cms-restart:#{suffix}"
    cuckoo_key = "router:cf-restart:#{suffix}"
    topk_key = "router:topk-restart:#{suffix}"
    tdigest_key = "router:td-restart:#{suffix}"

    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Commands.Bloom.handle_ast({:bf_reserve, bloom_key, 0.01, 128}, ctx)

      assert 1 = Ferricstore.Commands.Bloom.handle_ast({:bf_add, [bloom_key, "sensor-a"]}, ctx)
      assert 1 = Ferricstore.Commands.Bloom.handle_ast({:bf_exists, [bloom_key, "sensor-a"]}, ctx)

      assert :ok = Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, cms_key, 64, 4}, ctx)

      assert [7] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_key, [{"hot", 7}]}, ctx)

      assert :ok = Ferricstore.Commands.Cuckoo.handle_ast({:cf_reserve, cuckoo_key, 128}, ctx)
      assert 1 = Ferricstore.Commands.Cuckoo.handle_ast({:cf_add, [cuckoo_key, "seen"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_reserve, topk_key, 3, 8, 4, 0.9},
                 ctx
               )

      assert [nil] = Ferricstore.Commands.TopK.handle_ast({:topk_add, [topk_key, "a"]}, ctx)

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_create, tdigest_key, 100},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_add, tdigest_key, [1.0, 2.0, 3.0]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 1 =
               Ferricstore.Commands.Bloom.handle_ast(
                 {:bf_exists, [bloom_key, "sensor-a"]},
                 restarted_ctx
               )

      assert [7] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_query, [cms_key, "hot"]},
                 restarted_ctx
               )

      assert 1 =
               Ferricstore.Commands.Cuckoo.handle_ast(
                 {:cf_exists, [cuckoo_key, "seen"]},
                 restarted_ctx
               )

      assert [1] =
               Ferricstore.Commands.TopK.handle_ast(
                 {:topk_count, [topk_key, "a"]},
                 restarted_ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, tdigest_key, [0.5]},
                 restarted_ctx
               )

      assert median != "nan"
      FerricStore.Instance.cleanup(restarted_ctx.name)
    after
    end
  end

  test "probabilistic merge commands survive WARaft restart", %{root: root, ctx: ctx} do
    suffix = System.unique_integer([:positive])

    hll_src1 = "router:hll-merge-src1:#{suffix}"
    hll_src2 = "router:hll-merge-src2:#{suffix}"
    hll_dest = "router:hll-merge-dest:#{suffix}"

    cms_src1 = "router:cms-merge-src1:#{suffix}"
    cms_src2 = "router:cms-merge-src2:#{suffix}"
    cms_dest = "router:cms-merge-dest:#{suffix}"

    td_src1 = "router:td-merge-src1:#{suffix}"
    td_src2 = "router:td-merge-src2:#{suffix}"
    td_dest = "router:td-merge-dest:#{suffix}"

    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 1 =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfadd, [hll_src1, "a", "b"]},
                 ctx
               )

      assert 1 =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfadd, [hll_src2, "c", "d"]},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.HyperLogLog.handle_ast(
                 {:pfmerge, [hll_dest, hll_src1, hll_src2]},
                 ctx
               )

      for key <- [cms_src1, cms_src2, cms_dest] do
        assert :ok = Ferricstore.Commands.CMS.handle_ast({:cms_initbydim, key, 64, 4}, ctx)
      end

      assert [2] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_src1, [{"hot", 2}]}, ctx)

      assert [4] =
               Ferricstore.Commands.CMS.handle_ast({:cms_incrby, cms_src2, [{"hot", 4}]}, ctx)

      assert :ok =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_merge, cms_dest, [cms_src1, cms_src2], [1, 1]},
                 ctx
               )

      for {key, values} <- [{td_src1, [1.0, 2.0]}, {td_src2, [9.0, 10.0]}] do
        assert :ok = Ferricstore.Commands.TDigest.handle_ast({:tdigest_create, key, 100}, ctx)
        assert :ok = Ferricstore.Commands.TDigest.handle_ast({:tdigest_add, key, values}, ctx)
      end

      assert :ok =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_merge, td_dest, [td_src1, td_src2], []},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      hll_count =
        Ferricstore.Commands.HyperLogLog.handle_ast(
          {:pfcount, [hll_dest]},
          restarted_ctx
        )

      assert hll_count >= 3

      assert [6] =
               Ferricstore.Commands.CMS.handle_ast(
                 {:cms_query, [cms_dest, "hot"]},
                 restarted_ctx
               )

      assert [median] =
               Ferricstore.Commands.TDigest.handle_ast(
                 {:tdigest_quantile, td_dest, [0.5]},
                 restarted_ctx
               )

      assert median != "nan"
    after
    end
  end

  test "stream XADD uses WARaft-backed compound writes", %{ctx: ctx} do
    try do
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:stream:#{System.unique_integer([:positive])}"

      assert "1-0" =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xadd, key, {{:explicit, 1, 0}, ["f", "v"], nil, false}},
                 ctx
               )

      assert 1 = Ferricstore.Commands.Stream.handle_ast({:xlen, key}, ctx)
    after
    end
  end

  test "stream consumer group state survives WARaft restart", %{root: root, ctx: ctx} do
    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:stream-group:#{System.unique_integer([:positive])}"

      assert "1-0" =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xadd, key, {{:explicit, 1, 0}, ["f", "v"], nil, false}},
                 ctx
               )

      assert :ok =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xgroup_create, key, "group-a", "0", false},
                 ctx
               )

      assert [[^key, [["1-0", "f", "v"]]]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, ">"}]}},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert [[^key, [["1-0", "f", "v"]]]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, "0"}]}},
                 restarted_ctx
               )

      assert 1 =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xack, key, "group-a", ["1-0"]},
                 restarted_ctx
               )

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(restarted_ctx.name)

      acked_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(acked_ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert [] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xreadgroup, "group-a", "consumer-a", {10, :no_block, [{key, "0"}]}},
                 acked_ctx
               )
    after
    end
  end

  test "stream XDEL and XTRIM mutations survive WARaft restart", %{root: root, ctx: ctx} do
    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:stream-trim:#{System.unique_integer([:positive])}"

      for id <- ["1-0", "2-0", "3-0", "4-0", "5-0"] do
        assert ^id =
                 Ferricstore.Commands.Stream.handle_ast(
                   {:xadd, key,
                    {{:explicit, parse_stream_ms(id), 0}, ["f", "v:#{id}"], nil, false}},
                   ctx
                 )
      end

      assert 1 = Ferricstore.Commands.Stream.handle_ast({:xdel, key, ["2-0"]}, ctx)
      assert 2 = Ferricstore.Commands.Stream.handle_ast({:xtrim, key, {:maxlen, false, 2}}, ctx)

      assert [["4-0", "f", "v:4-0"], ["5-0", "f", "v:5-0"]] =
               Ferricstore.Commands.Stream.handle_ast({:xrange, key, "-", "+", nil}, ctx)

      assert :ok = WARaftBackend.stop()
      Ferricstore.Commands.Stream.clear_local_state()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      prefix = CompoundKey.stream_prefix(key)
      assert 2 = Router.compound_count(restarted_ctx, key, prefix)
      assert ["4-0", "5-0"] = Router.compound_fields(restarted_ctx, key, prefix)
      assert 2 = Ferricstore.Commands.Stream.handle_ast({:xlen, key}, restarted_ctx)

      assert [["4-0", "f", "v:4-0"], ["5-0", "f", "v:5-0"]] =
               Ferricstore.Commands.Stream.handle_ast(
                 {:xrange, key, "-", "+", nil},
                 restarted_ctx
               )
    after
    end
  end

  test "hash field metadata reads survive WARaft restart", %{root: root, ctx: ctx} do
    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:hash-ttl:#{System.unique_integer([:positive])}"

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetex, key, 60, ["field", "value"]},
                 ctx
               )

      assert [ttl_before] = Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, ctx)
      assert ttl_before > 0

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["value"] =
               Ferricstore.Commands.Hash.handle_ast({:hmget, [key, "field"]}, restarted_ctx)

      assert [ttl_after] =
               Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, restarted_ctx)

      assert ttl_after > 0
      assert ttl_after <= ttl_before

      assert ["value"] =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hgetex, key, {:px, 120_000}, ["field"]},
                 restarted_ctx
               )

      assert [extended_ttl] =
               Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["field"]}, restarted_ctx)

      assert extended_ttl > ttl_after
    after
    end
  end

  test "advanced hash mutations survive WARaft restart", %{root: root, ctx: ctx} do
    key = "router:hash-advanced:#{System.unique_integer([:positive])}"

    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 3 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hset, [key, "int", "1", "float", "1.5", "delete", "gone"]},
                 ctx
               )

      assert 4 = Ferricstore.Commands.Hash.handle_ast({:hincrby, key, "int", 3}, ctx)

      assert "2.0" =
               Ferricstore.Commands.Hash.handle_ast({:hincrbyfloat, key, "float", 0.5}, ctx)

      assert 1 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetnx, key, "created-once", "first"},
                 ctx
               )

      assert 0 =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hsetnx, key, "created-once", "second"},
                 ctx
               )

      assert ["gone", nil] =
               Ferricstore.Commands.Hash.handle_ast({:hgetdel, key, ["delete", "missing"]}, ctx)

      assert [1] = Ferricstore.Commands.Hash.handle_ast({:hpexpire, key, 60_000, ["int"]}, ctx)
      assert [ttl_before] = Ferricstore.Commands.Hash.handle_ast({:hpttl, key, ["int"]}, ctx)
      assert ttl_before > 0
      assert [1] = Ferricstore.Commands.Hash.handle_ast({:hpersist, key, ["int"]}, ctx)

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert ["4", "2.0", "first", nil] =
               Ferricstore.Commands.Hash.handle_ast(
                 {:hmget, [key, "int", "float", "created-once", "delete"]},
                 restarted_ctx
               )

      assert [cursor, flat_fields] =
               Ferricstore.Commands.Hash.handle_ast({:hscan, key, 0, []}, restarted_ctx)

      assert cursor in ["0", 0]
      assert "int" in flat_fields
      assert "float" in flat_fields
      assert "created-once" in flat_fields
      refute "delete" in flat_fields

      assert [expiretime] =
               Ferricstore.Commands.Hash.handle_ast({:hexpiretime, key, ["int"]}, restarted_ctx)

      assert expiretime == -1
    after
    end
  end

  test "zset index helpers read directly from WARaft keydir after restart", %{
    root: root,
    ctx: ctx
  } do
    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      key = "router:zset-index:#{System.unique_integer([:positive])}"

      assert 3 =
               Ferricstore.Commands.SortedSet.handle_ast(
                 {:zadd, key, [], [{2.0, "b"}, {1.0, "a"}, {3.0, "c"}]},
                 ctx
               )

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert {:ok, [{"a", 1.0}, {"b", 2.0}]} =
               Router.zset_rank_range(restarted_ctx, key, 0, 1, false)

      assert {:ok, [{"c", 3.0}, {"b", 2.0}]} =
               Router.zset_rank_range(restarted_ctx, key, 0, 1, true)

      assert {:ok, 1} = Router.zset_member_rank(restarted_ctx, key, "b", false)
      assert {:ok, 2} = Router.zset_score_count(restarted_ctx, key, {:inclusive, 1.5}, :inf)

      assert {:ok, [{"b", 2.0}, {"c", 3.0}]} =
               Router.zset_score_range(restarted_ctx, key, {:inclusive, 2.0}, :inf, false)

      assert {:ok, [{"b", 2.0}]} =
               Router.zset_score_range_slice(restarted_ctx, key, :neg_inf, :inf, false, 1, 1)
    after
    end
  end

  test "zset update and pop mutations survive WARaft restart", %{root: root, ctx: ctx} do
    key = "router:zset-mutate:#{System.unique_integer([:positive])}"

    try do
      assert :ok =
               WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert 3 =
               Ferricstore.Commands.SortedSet.handle(
                 "ZADD",
                 [key, "1.0", "a", "2.0", "b", "3.0", "c"],
                 ctx
               )

      assert 1 = Ferricstore.Commands.SortedSet.handle("ZREM", [key, "a"], ctx)
      incr_result = Ferricstore.Commands.SortedSet.handle("ZINCRBY", [key, "2.0", "b"], ctx)
      {score, ""} = Float.parse(incr_result)
      assert_in_delta 4.0, score, 0.001
      assert ["b", score_text] = Ferricstore.Commands.SortedSet.handle("ZPOPMAX", [key], ctx)
      {popped_score, ""} = Float.parse(score_text)
      assert_in_delta 4.0, popped_score, 0.001

      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert 1 = Ferricstore.Commands.SortedSet.handle("ZCARD", [key], restarted_ctx)
      assert nil == Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, "a"], restarted_ctx)
      assert nil == Ferricstore.Commands.SortedSet.handle("ZSCORE", [key, "b"], restarted_ctx)

      assert ["c", "3.0"] =
               Ferricstore.Commands.SortedSet.handle(
                 "ZRANGE",
                 [key, "0", "-1", "WITHSCORES"],
                 restarted_ctx
               )
    after
    end
  end

    end
  end
end
