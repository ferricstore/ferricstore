defmodule Ferricstore.Raft.WARaftBackendTest.Sections.SnapshotTransferWrapsWaraftTransportExits do
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

      test "snapshot transfer wraps WARaft transport exits" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert [_, transfer_source] = String.split(source, "defp transfer_snapshot", parts: 2)

        assert [transfer_source, _rest] =
                 String.split(transfer_source, "defp wait_peer_ready", parts: 2)

        assert transfer_source =~ "backend_call(fn ->"
        assert transfer_source =~ ":wa_raft_transport.transfer_snapshot("
      end

      test "startup status polling wraps WARaft exits" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert [_, wait_source] = String.split(source, "defp wait_status", parts: 2)

        assert [wait_source, _rest] =
                 String.split(wait_source, "defp maybe_cache_current_config", parts: 2)

        assert wait_source =~ "backend_call(fn -> :wa_raft_server.status(server) end)"
      end

      test "startup wait budget is configurable for large recovered logs" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert source =~ "defp startup_wait_attempts"
        assert source =~ ":waraft_start_wait_timeout_ms"

        assert [_, finish_source] = String.split(source, "defp finish_start_partition", parts: 2)

        assert [finish_source, _rest] =
                 String.split(finish_source, "defp finish_start_status", parts: 2)

        assert finish_source =~ "wait_attempts = startup_wait_attempts()"
        assert finish_source =~ "wait_status(server, wait_attempts)"
        assert finish_source =~ "wait_log_replayed(server, replay_target, wait_attempts)"
        assert finish_source =~ "wait_storage_replayed(shard_index, replay_target, wait_attempts)"
        refute finish_source =~ "wait_status(server, 100)"
      end

      test "startup leader and replay waits retry transient status misses" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert [_, leader_source] = String.split(source, "defp wait_leader", parts: 2)

        assert [leader_source, replay_and_rest] =
                 String.split(leader_source, "defp wait_log_replayed", parts: 2)

        assert [replay_source, _rest] =
                 String.split(replay_and_rest, "defp log_replayed?", parts: 2)

        assert leader_source =~ "wait_leader(server, attempts - 1, {:status_error, reason})"

        assert replay_source =~
                 "wait_log_replayed(server, target_index, attempts - 1, {:status_error, reason})"

        refute leader_source =~ "{:error, _reason} = error ->\n        error"
        refute replay_source =~ "{:error, _reason} = error ->\n        error"
      end

      test "storage durable-position polling wraps WARaft exits" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert [_, storage_source] =
                 String.split(source, "defp internal_storage_status", parts: 2)

        assert [storage_source, _rest] =
                 String.split(storage_source, "defp position_reached?", parts: 2)

        assert storage_source =~ "backend_call(fn -> :wa_raft_storage.status(storage) end)"
      end

      test "transfer leadership wraps WARaft exits" do
        source = Ferricstore.Test.SourceFiles.waraft_backend_source()

        assert [_, transfer_source] =
                 String.split(source, "defp local_transfer_leadership", parts: 2)

        assert [transfer_source, _rest] =
                 String.split(transfer_source, "defp maybe_redirect_transfer", parts: 2)

        assert transfer_source =~ "backend_call(fn ->"
        assert transfer_source =~ ":wa_raft_server.handover(target_node)"
      end

      test "backend stop cleanup tracks configured shard counts above 64" do
        names = WARaftBackend.__registered_names_for_test__(65)

        assert :raft_server_ferricstore_waraft_backend_65 in names
        assert :raft_storage_ferricstore_waraft_backend_65 in names
        assert :raft_acceptor_ferricstore_waraft_backend_65 in names
      end

      test "backend releases in-flight byte accounting after commit replies", %{ctx: ctx} do
        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   max_inflight_commit_bytes: 256
                 )

        assert :ok = WARaftBackend.write(0, {:put, "bytes:k1", "v1", 0})
        assert 0 == WARaftBackend.inflight_commit_bytes(0)
        assert :ok = WARaftBackend.write(0, {:put, "bytes:k2", "v2", 0})
        assert 0 == WARaftBackend.inflight_commit_bytes(0)

        assert "v1" == Router.get(ctx, "bytes:k1")
        assert "v2" == Router.get(ctx, "bytes:k2")
      end

      test "Router can use WARaft as the selected durable write backend", %{ctx: ctx} do
        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, "router:k", "router:v", 0)
          assert "router:v" == Router.get(ctx, "router:k")

          assert [:ok, :ok] =
                   Router.batch_quorum_put(ctx, [{"router:b1", "v1"}, {"router:b2", "v2"}])

          assert "v1" == Router.get(ctx, "router:b1")
          assert "v2" == Router.get(ctx, "router:b2")

          assert [:ok, :ok] = Router.batch_quorum_delete(ctx, ["router:b1", "router:b2"])
          assert nil == Router.get(ctx, "router:b1")
          assert nil == Router.get(ctx, "router:b2")
        after
        end
      end

      test "Router forced-quorum commands use WARaft as the selected backend", %{ctx: ctx} do
        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 1 = Router.pfadd(ctx, "router:pf", ["a"])
          assert 0 = Router.pfadd(ctx, "router:pf", ["a"])
          assert is_binary(Router.get(ctx, "router:pf"))
        after
        end
      end

      test "Router get_version uses WARaft shared counters without shard fallback telemetry", %{
        ctx: ctx
      } do
        handler_id = {__MODULE__, :waraft_get_version_no_shard_fallback, make_ref()}
        parent = self()

        :telemetry.attach(
          handler_id,
          [:ferricstore, :store, :shard_unavailable],
          &__MODULE__.handle_store_unavailable_telemetry/4,
          parent
        )

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = Router.put(ctx, "router:version:k", "v", 0)
          assert Router.get_version(ctx, "router:version:k") > 0

          refute_receive {:store_unavailable, _event, _measurements, _metadata}, 50
        after
          :telemetry.detach(handler_id)
        end
      end

      test "key expiry and persist commands survive WARaft restart", %{root: root, ctx: ctx} do
        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          key = "router:ttl:#{System.unique_integer([:positive])}"

          assert :ok = Ferricstore.Commands.Strings.handle_ast({:set, key, "ttl-value"}, ctx)
          assert 1 = Ferricstore.Commands.Expiry.handle_ast({:pexpire, key, 60_000}, ctx)
          ttl_before = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, ctx)
          assert ttl_before > 0

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "ttl-value" == Router.get(restarted_ctx, key)

          ttl_after = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, restarted_ctx)
          assert ttl_after > 0
          assert ttl_after <= ttl_before

          assert 1 = Ferricstore.Commands.Expiry.handle_ast({:persist, key}, restarted_ctx)
          assert -1 = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, restarted_ctx)

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(restarted_ctx.name)

          persisted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(persisted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert "ttl-value" == Router.get(persisted_ctx, key)
          assert -1 = Ferricstore.Commands.Expiry.handle_ast({:pttl, key}, persisted_ctx)
        after
        end
      end

      test "Router compound commands use WARaft as the selected backend", %{ctx: ctx} do
        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          redis_key = "router:hash"
          field_key = CompoundKey.hash_field(redis_key, "field")

          assert :ok = Router.compound_put(ctx, redis_key, field_key, "value", 0)
          assert "value" == Router.compound_get(ctx, redis_key, field_key)

          assert :ok = Router.compound_delete(ctx, redis_key, field_key)
          assert nil == Router.compound_get(ctx, redis_key, field_key)
        after
        end
      end

      test "Router bitmap and native commands use WARaft as the selected backend", %{
        ctx: ctx
      } do
        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 0 = Router.setbit(ctx, "router:bitmap", 7, 1)
          assert <<1>> == Router.get(ctx, "router:bitmap")

          assert ["allowed", 1, 2, reset_ms] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 1)
          assert is_integer(reset_ms)
          assert ["allowed", 3, 0, _] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 2)
          assert ["denied", 3, 0, _] = Router.ratelimit_add(ctx, "router:rl", 1_000, 3, 1)
        after
        end
      end

      test "bitmap mutations survive WARaft restart", %{root: root, ctx: ctx} do
        suffix = System.unique_integer([:positive])
        bitmap_a = "router:bitmap-restart:a:#{suffix}"
        bitmap_b = "router:bitmap-restart:b:#{suffix}"
        bitmap_dest = "router:bitmap-restart:dest:#{suffix}"

        try do
          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 0 = Router.setbit(ctx, bitmap_a, 1, 1)
          assert 0 = Router.setbit(ctx, bitmap_b, 7, 1)

          assert 1 =
                   Ferricstore.Commands.Bitmap.handle_ast(
                     {:bitop, :bor, bitmap_dest, [bitmap_a, bitmap_b]},
                     ctx
                   )

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert 2 =
                   Ferricstore.Commands.Bitmap.handle_ast(
                     {:bitcount, bitmap_dest},
                     restarted_ctx
                   )
        after
        end
      end

      test "Flow writes claims and transitions use WARaft as the selected backend", %{ctx: ctx} do
        flow_type = "router-flow-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-id-#{System.unique_integer([:positive])}"

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: "tenant-a",
                     run_at_ms: 1,
                     now_ms: 1,
                     payload: "payload"
                   )

          assert {:ok, created} = Ferricstore.Flow.get(ctx, flow_id, partition_key: "tenant-a")
          assert created.id == flow_id
          assert created.state == "queued"

          assert {:ok, [claimed]} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_key: "tenant-a",
                     worker: "worker-a",
                     limit: 1,
                     now_ms: 2,
                     lease_ms: 10_000
                   )

          assert claimed.id == flow_id
          assert claimed.state == "running"
          assert is_binary(claimed.lease_token)
          assert is_integer(claimed.fencing_token)

          assert :ok =
                   Ferricstore.Flow.transition(
                     ctx,
                     flow_id,
                     "running",
                     "waiting",
                     partition_key: "tenant-a",
                     lease_token: claimed.lease_token,
                     fencing_token: claimed.fencing_token,
                     now_ms: 3,
                     payload: "next-payload"
                   )

          assert {:ok, transitioned} =
                   Ferricstore.Flow.get(ctx, flow_id, partition_key: "tenant-a")

          assert transitioned.state == "waiting"
          assert transitioned.version >= claimed.version + 1
        after
        end
      end

      test "Flow WARaft apply does not use standalone Bitcask append fallback", %{ctx: ctx} do
        parent = self()
        previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
        flow_type = "router-flow-segment-only-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-segment-only-id-#{System.unique_integer([:positive])}"
        partition = "tenant-segment-only"

        try do
          Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
            send(parent, {:unexpected_standalone_bitcask_append, batch})
            {:error, :standalone_bitcask_append_forbidden}
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
          )

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1,
                     now_ms: 1,
                     payload: "payload"
                   )

          assert {:ok, created} = Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)
          assert created.id == flow_id
          refute_receive {:unexpected_standalone_bitcask_append, _batch}, 100

          assert {:ok, [claimed]} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_key: partition,
                     worker: "worker-segment-only",
                     limit: 1,
                     now_ms: 2,
                     lease_ms: 10_000
                   )

          assert :ok =
                   Ferricstore.Flow.complete(ctx, flow_id, claimed.lease_token,
                     partition_key: partition,
                     fencing_token: claimed.fencing_token,
                     now_ms: 3,
                     result: "done"
                   )

          assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)
        after
          restore_env(:standalone_durability_hook, previous_hook)
        end
      end

      @tag :terminal_projection_cache_entry
      test "Flow terminal LMDB flush prunes WARaft apply projection cache", %{ctx: ctx} do
        flow_type = "router-flow-terminal-projection-prune-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-terminal-projection-prune-id-#{System.unique_integer([:positive])}"
        partition = "tenant-terminal-projection-prune"

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter, shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
        )

        assert :ok =
                 Ferricstore.Flow.create(ctx, flow_id,
                   type: flow_type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition,
                   worker: "worker-terminal-projection-prune",
                   limit: 1,
                   now_ms: 2,
                   lease_ms: 10_000
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, flow_id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: 3
                 )

        state_key = Ferricstore.Flow.Keys.state_key(flow_id, partition)

        assert [
                 {^state_key, _value, _expire_at_ms, _lfu,
                  {:waraft_apply_projection, projection_index}, _offset, _value_size}
               ] =
                 :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)
        assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

        refute apply_projection_cache_contains?(
                 ctx.data_dir,
                 0,
                 projection_index,
                 state_key
               )
      end

      test "Flow projection copies generated values to history log before dropping apply cache",
           %{root: root, ctx: ctx} do
        flow_type = "router-flow-history-value-copy-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-history-value-copy-id-#{System.unique_integer([:positive])}"
        partition = "tenant-history-value-copy"

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert :ok =
                 Ferricstore.Flow.create(ctx, flow_id,
                   type: flow_type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1,
                   payload: "payload"
                 )

        assert {:ok, created} = Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)
        assert is_binary(created.payload_ref)
        payload_ref = created.payload_ref

        assert {:ok, ["payload"]} = Ferricstore.Flow.value_mget(ctx, [payload_ref])

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition,
                   worker: "worker-segment-locator",
                   limit: 1,
                   now_ms: 2,
                   lease_ms: 10_000
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, flow_id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: 3,
                   result: "done"
                 )

        state_key = Ferricstore.Flow.Keys.state_key(flow_id, partition)

        assert [
                 {^state_key, _value, _expire_at_ms, _lfu, _fid, _offset, value_size}
               ] = :ets.lookup(elem(ctx.keydir_refs, 0), state_key)

        assert value_size > 0
        shard_data_path = Path.join([root, "data", "shard_0"])

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter, shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
        )

        start_supervised!(
          {Ferricstore.Flow.HistoryProjector,
           [
             shard_index: 0,
             shard_data_path: shard_data_path,
             instance_ctx: ctx,
             recover_on_init: false
           ]}
        )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 30_000)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)

        lmdb_path = Ferricstore.Flow.LMDB.path(shard_data_path)

        assert eventually(fn ->
                 match?({:ok, _}, Ferricstore.Flow.LMDB.get(lmdb_path, created.payload_ref))
               end)

        assert {:ok, payload_locator} = Ferricstore.Flow.LMDB.get(lmdb_path, created.payload_ref)

        assert {:ok, {{:flow_history, history_file_id}, offset, payload_size}} =
                 Ferricstore.Flow.LMDB.decode_value_locator(
                   payload_locator,
                   System.system_time(:millisecond)
                 )

        assert is_integer(history_file_id) and history_file_id >= 0
        assert is_integer(offset) and offset >= 0
        assert payload_size > 0
        assert {:ok, ["payload"]} = Ferricstore.Flow.value_mget(ctx, [created.payload_ref])
        assert [] = :ets.lookup(elem(ctx.keydir_refs, 0), created.payload_ref)

        refute Enum.any?(:ets.tab2list(:ferricstore_waraft_apply_projection_cache), fn
                 {{_root, _index, ^payload_ref}, _value, _expire_at_ms} -> true
                 _other -> false
               end)
      end

      @tag :empty_generated_projection_cache
      test "Flow history projection materializes empty generated values without pinning WARaft apply cache",
           %{ctx: ctx} do
        flow_type =
          "router-flow-empty-value-projection-prune-#{System.unique_integer([:positive])}"

        flow_id =
          "router-flow-empty-value-projection-prune-id-#{System.unique_integer([:positive])}"

        partition = "tenant-empty-value-projection-prune"

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        start_supervised!(
          {Ferricstore.Flow.LMDBWriter, shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
        )

        start_supervised!(
          {Ferricstore.Flow.HistoryProjector,
           [
             shard_index: 0,
             shard_data_path: Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0),
             instance_ctx: ctx,
             recover_on_init: false
           ]}
        )

        assert :ok =
                 Ferricstore.Flow.create(ctx, flow_id,
                   type: flow_type,
                   partition_key: partition,
                   run_at_ms: 1,
                   now_ms: 1,
                   payload: <<>>
                 )

        assert {:ok, created} = Ferricstore.Flow.get(ctx, flow_id, partition_key: partition)
        assert is_binary(created.payload_ref)

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition,
                   worker: "worker-empty-value-projection-prune",
                   limit: 1,
                   now_ms: 2,
                   lease_ms: 10_000
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, flow_id, claimed.lease_token,
                   partition_key: partition,
                   fencing_token: claimed.fencing_token,
                   now_ms: 3
                 )

        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)
        assert :ok = Ferricstore.Flow.HistoryProjector.flush(ctx, 0, 30_000)
        assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)

        assert eventually(fn ->
                 :ets.lookup(elem(ctx.keydir_refs, 0), created.payload_ref) == []
               end)

        assert {:ok, [<<>>]} = Ferricstore.Flow.value_mget(ctx, [created.payload_ref])

        refute apply_projection_cache_contains_key?(ctx.data_dir, 0, created.payload_ref)
      end

      test "Flow create_many and transition_many use WARaft as the selected backend", %{ctx: ctx} do
        flow_type = "router-flow-many-#{System.unique_integer([:positive])}"
        partition = "tenant-many-#{System.unique_integer([:positive])}"
        ids = for n <- 1..3, do: "router-flow-many-#{n}-#{System.unique_integer([:positive])}"

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.LMDBWriter,
             shard_index: 0, data_dir: ctx.data_dir, instance_ctx: ctx}
          )

          assert :ok =
                   Ferricstore.Flow.create_many(
                     ctx,
                     partition,
                     Enum.map(ids, &%{id: &1, payload: "payload:#{&1}"}),
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert :ok =
                   Ferricstore.Flow.transition_many(
                     ctx,
                     partition,
                     "queued",
                     "ready",
                     Enum.map(ids, &%{id: &1, fencing_token: 0}),
                     run_at_ms: 2_000,
                     now_ms: 1_000
                   )

          assert {:ok, claimed} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_key: partition,
                     state: "ready",
                     worker: "worker-many",
                     limit: 10,
                     now_ms: 2_000
                   )

          assert claimed |> Enum.map(& &1.id) |> MapSet.new() == MapSet.new(ids)
          assert Enum.all?(claimed, &(&1.state == "running"))
          assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, 0)
        after
        end
      end

      test "multi-shard Flow terminal batches publish WARaft keydir locations", %{root: root} do
        shard_count = 16
        flow_type = "router-flow-pending-publish-#{System.unique_integer([:positive])}"
        ids = for n <- 1..64, do: "router-flow-pending-publish-#{n}"

        root =
          Path.join(
            root,
            "multi-shard-flow-pending-#{System.unique_integer([:positive])}"
          )

        File.rm_rf!(root)
        Ferricstore.DataDir.ensure_layout!(root, shard_count)
        Ferricstore.Store.ActiveFile.init(shard_count)
        ctx = build_ctx(root, shard_count: shard_count)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          items =
            Enum.map(ids, fn id ->
              %{id: id, partition_key: "tenant-#{id}", payload: "payload:#{id}"}
            end)

          assert :ok =
                   Ferricstore.Flow.create_many(ctx, nil, items,
                     type: flow_type,
                     state: "queued",
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert_pending_keydir_rows(ctx, 0)

          assert {:ok, claimed} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_keys: Enum.map(items, & &1.partition_key),
                     worker: "worker-pending-publish",
                     limit: length(items),
                     now_ms: 1_000,
                     lease_ms: 10_000
                   )

          assert length(claimed) > 0
          assert_pending_keydir_rows(ctx, 0)

          complete_items =
            Enum.map(claimed, fn record ->
              %{
                id: record.id,
                partition_key: record.partition_key,
                lease_token: record.lease_token,
                fencing_token: record.fencing_token,
                result: "done:#{record.id}"
              }
            end)

          assert :ok =
                   Ferricstore.Flow.complete_many(ctx, nil, complete_items, now_ms: 1_100)

          assert_pending_keydir_rows(ctx, 0)
        after
          WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
          File.rm_rf!(root)
        end
      end

      test "async Flow history projection gates WARaft durable position, not applied position", %{
        root: root,
        ctx: ctx
      } do
        previous_async_history = Application.get_env(:ferricstore, :flow_async_history)

        previous_history_flush =
          Application.get_env(:ferricstore, :flow_history_projector_flush_interval_ms)

        previous_history_batch =
          Application.get_env(:ferricstore, :flow_history_projector_batch_size)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)
        flow_type = "router-flow-history-sync-#{System.unique_integer([:positive])}"
        flow_id = "router-flow-history-sync-id-#{System.unique_integer([:positive])}"
        partition = "tenant-history-sync-#{System.unique_integer([:positive])}"
        shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)

        try do
          Application.put_env(:ferricstore, :flow_async_history, true)
          Application.put_env(:ferricstore, :flow_history_projector_flush_interval_ms, 60_000)
          Application.put_env(:ferricstore, :flow_history_projector_batch_size, 10_000)
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)

          assert :ok =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          start_supervised!(
            {Ferricstore.Flow.HistoryProjector,
             [
               shard_index: 0,
               shard_data_path: shard_data_path,
               instance_ctx: ctx,
               recover_on_init: false
             ]}
          )

          assert {:ok, {:raft_log_pos, before_index, _term}} = WARaftBackend.storage_position(0)

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert {:ok, {:raft_log_pos, applied_index, _term}} = WARaftBackend.storage_position(0)
          projected_index = Ferricstore.Flow.HistoryProjectedIndex.read(shard_data_path)
          storage_status = waraft_storage_status(0)

          {:raft_log_pos, durable_index, _durable_term} =
            Keyword.fetch!(storage_status, :durable_position)

          assert applied_index > projected_index
          assert durable_index <= before_index

          assert %{position: persisted_position} = waraft_latest_storage_metadata(root, 0)
          assert position_index(persisted_position) <= before_index

          case read_segment_projection_header(root, 0) do
            {:ok, {{:raft_log_pos, projection_index, _term}, count}} ->
              assert projection_index < applied_index or count == 0

            :not_found ->
              :ok
          end
        after
          restore_env(:flow_async_history, previous_async_history)
          restore_env(:flow_history_projector_flush_interval_ms, previous_history_flush)
          restore_env(:flow_history_projector_batch_size, previous_history_batch)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
        end
      end
    end
  end
end
