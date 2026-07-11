defmodule Ferricstore.Store.BlobSideChannelTest.Sections.RaGenericBatchAcceptsPreExternalizedBlobRefs do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Store.{
        BlobRef,
        BlobStore,
        ColdRead,
        CompoundKey,
        LFU,
        LocalTxStore,
        Ops,
        Router
      }

      alias Ferricstore.Store.Shard.Compound, as: ShardCompound
      alias Ferricstore.Store.Shard.ETS, as: ShardETS
      alias Ferricstore.Raft.StateMachine
      alias Ferricstore.Test.IsolatedInstance

      test "Ra generic batch accepts pre-externalized blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "blob:auto:raft-ref-generic-batch"
        payload = :binary.copy("G", 1536)
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = BlobRef.encode!(ref)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          {:ok, [:ok]},
          StateMachine.apply(%{index: 1}, {:batch, [{:put_blob_ref, key, encoded_ref, 0}]}, state)
        )

        assert payload == Router.get(ctx, key)
        assert {:ok, ^encoded_ref, ^ref} = raw_disk_blob_ref(ctx, keydir, key)
      end

      test "Ra generic batch RMW sees preceding pre-externalized blob ref", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "blob:auto:raft-ref-generic-rmw"
        payload = :binary.copy("R", 1536)
        suffix = "!"
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        encoded_ref = BlobRef.encode!(ref)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          {:ok, [:ok, {:ok, byte_size(payload) + byte_size(suffix)}]},
          StateMachine.apply(
            %{index: 1},
            {:batch, [{:put_blob_ref, key, encoded_ref, 0}, {:append, key, suffix}]},
            state
          )
        )

        assert payload <> suffix == Router.get(ctx, key)
      end

      test "Ra apply returns an error and rolls back staged writes when blob persistence fails",
           %{
             ctx: ctx,
             keydir: keydir
           } do
        small_key = "blob:auto:raft-blob-fail-small"
        large_key = "blob:auto:raft-blob-fail-large"
        payload = :binary.copy("F", 1536)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        Process.put(:ferricstore_blob_store_fsync_dir_hook, fn _path -> {:error, :eio} end)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        try do
          assert_state_machine_result(
            {:error, {:blob_externalize_failed, :eio}},
            StateMachine.apply(
              %{index: 1},
              {:batch, [{:put, small_key, "small", 0}, {:put, large_key, payload, 0}]},
              state
            )
          )

          assert [] == :ets.lookup(keydir, small_key)
          assert [] == :ets.lookup(keydir, large_key)
        after
          Process.delete(:ferricstore_blob_store_fsync_dir_hook)
        end
      end

      test "Ra read-modify-write materializes blob refs before mutation", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "blob:auto:raft:append"
        payload = :binary.copy("A", 1536)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          :ok,
          StateMachine.apply(%{index: 1}, {:put, key, payload, 0}, state)
        )

        assert_state_machine_result(
          byte_size(payload) + 1,
          StateMachine.apply(%{index: 2}, {:append, key, "!"}, state)
        )

        expected = payload <> "!"
        assert expected == Router.get(ctx, key)
        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
        assert {:ok, ^expected} = BlobStore.get(ctx.data_dir, 0, ref)
      end

      test "transaction-local GET materializes cold blob refs", %{
        ctx: ctx,
        shard: shard
      } do
        key = "blob:auto:local-tx-get"
        payload = :binary.copy("T", 1536)

        assert :ok = Router.put(ctx, key, payload, 0)
        tx = LocalTxStore.new(:sys.get_state(shard))

        assert payload == Ops.get(tx, key)
        assert {payload, 0} == Ops.get_meta(tx, key)
        assert [payload] == Ops.batch_get(tx, [key])
      end

      test "transaction-local RMW reads cold blob refs before mutation", %{
        ctx: ctx,
        shard: shard
      } do
        key = "blob:auto:local-tx-append"
        payload = :binary.copy("A", 1536)
        suffix = "!"

        assert :ok = Router.put(ctx, key, payload, 0)
        tx = LocalTxStore.new(:sys.get_state(shard))

        try do
          assert {:ok, byte_size(payload) + byte_size(suffix)} == Ops.append(tx, key, suffix)
          assert_receive {:tx_pending_write, ^key, written, 0}
          assert written == payload <> suffix
        after
          Process.delete(:tx_pending_values)
          Process.delete(:tx_deleted_keys)
        end
      end

      test "transaction-local value_size and GETRANGE use logical blob size", %{
        ctx: ctx,
        shard: shard
      } do
        key = "blob:auto:local-tx-getrange"
        payload = :binary.copy("A", 128) <> :binary.copy("B", 128)

        assert :ok = Router.put(ctx, key, payload, 0)
        tx = LocalTxStore.new(:sys.get_state(shard))

        assert byte_size(payload) == Ops.value_size(tx, key)
        assert binary_part(payload, 128, 8) == Ops.getrange(tx, key, 128, 135)

        assert [binary_part(payload, 128, 8)] ==
                 GenServer.call(
                   shard,
                   {:tx_execute, [{"GETRANGE", [key, "128", "135"], {:getrange, key, 128, 135}}],
                    nil}
                 )
      end

      test "transaction-local promoted compound GET materializes cold blob refs", %{
        shard: shard
      } do
        redis_key = "blob:auto:local-tx-promoted-hash"
        field = CompoundKey.hash_field(redis_key, "large")
        field_b = CompoundKey.hash_field(redis_key, "small")
        payload = :binary.copy("P", 1536)

        assert :ok = GenServer.call(shard, {:compound_put, redis_key, field, payload, 0})
        assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, "small", 0})

        tx = LocalTxStore.new(:sys.get_state(shard))

        assert payload == Ops.compound_get(tx, redis_key, field)
        assert [payload] == Ops.compound_batch_get(tx, redis_key, [field])
      end

      test "origin replay pending PUT persists large hot values as blob refs" do
        ctx =
          IsolatedInstance.checkout(
            shard_count: 1,
            hot_cache_max_value_size: 4096,
            blob_side_channel_threshold_bytes: 128
          )

        on_exit(fn -> IsolatedInstance.checkin(ctx) end)

        keydir = elem(ctx.keydir_refs, 0)
        key = "blob:auto:origin-replay"
        payload = :binary.copy("O", 1024)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        :ets.insert(keydir, {key, payload, 0, 1, :pending, 0, 0})

        assert_state_machine_result(
          :ok,
          StateMachine.apply(%{index: 1}, {:async, node(), {:put, key, payload, 0}}, state)
        )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
        assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
        assert payload == Router.get(ctx, key)
      end

      test "Flow-owned large payload values are persisted as blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        id = "blob-flow-payload"
        partition_key = "tenant-blob"
        payload = :binary.copy("P", 1024)
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        payload_key = Ferricstore.Flow.Keys.value_key(id, :payload, 1, partition_key)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        {:ok, policy_snapshot} =
          Ferricstore.Flow.RetryPolicy.normalize_flow_policy("blob-flow", [])

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          :ok,
          StateMachine.apply(
            %{index: 1, system_time: 1_000},
            {:flow_create, state_key,
             %{
               id: id,
               type: "blob-flow",
               state: "queued",
               partition_key: partition_key,
               payload: payload,
               now_ms: 1_000,
               policy_generation: 0,
               policy_snapshot: policy_snapshot,
               policy_snapshot_captured: true
             }},
            state
          )
        )

        assert [{^payload_key, nil, 0, _lfu, file_id, offset, value_size}] =
                 :ets.lookup(keydir, payload_key)

        assert is_integer(file_id) and file_id >= 0
        assert is_integer(offset) and offset >= 0
        assert value_size > 0

        assert {:ok, encoded_ref, _raw_ref} = raw_disk_blob_ref(ctx, keydir, payload_key)
        assert {:ok, ref} = BlobRef.decode(encoded_ref)
        assert {:ok, encoded_payload} = BlobStore.get(ctx.data_dir, 0, ref)
        assert Ferricstore.Flow.decode_value(encoded_payload) == payload
        assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [payload_key])
      end

      test "owner-scoped Flow named values store large values as live blob refs", %{ctx: ctx} do
        id = "blob-flow-named-value"
        partition_key = "tenant-blob-named-value"
        payload = :binary.copy("N", 512)

        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

        try do
          assert :ok =
                   Ferricstore.Flow.create(ctx, id,
                     type: "blob-flow-named-value",
                     partition_key: partition_key,
                     run_at_ms: 1,
                     now_ms: 1
                   )

          assert {:ok, %{ref: value_ref}} =
                   Ferricstore.Flow.value_put(ctx, payload,
                     partition_key: partition_key,
                     owner_flow_id: id,
                     name: "doc",
                     now_ms: 2
                   )

          assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [value_ref])
          assert {:ok, %{deleted_files: 0}} = Router.sweep_blob_garbage(ctx)
          assert {:ok, [^payload]} = Ferricstore.Flow.value_mget(ctx, [value_ref])
        after
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
        end
      end

      test "active Flow state records with large metadata are persisted as blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        id = "blob-flow-active-state"
        partition_key = "tenant-blob-active"
        correlation_id = :binary.copy("c", 256)
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: "blob-active",
                   partition_key: partition_key,
                   correlation_id: correlation_id,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
        assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)

        assert %{id: ^id, state: "queued", correlation_id: ^correlation_id} =
                 Ferricstore.Flow.decode_record(encoded_state)

        assert {:ok, %{id: ^id, state: "queued", correlation_id: ^correlation_id}} =
                 Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
      end

      test "Flow retention cleanup decodes terminal state stored as a blob ref", %{
        ctx: ctx,
        keydir: keydir
      } do
        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)

        on_exit(fn ->
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
        end)

        id = "blob-flow-retention"
        partition_key = "tenant-blob-retention"
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: "blob-retention",
                   partition_key: partition_key,
                   correlation_id: :binary.copy("c", 256),
                   retention_ttl_ms: 1,
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, "blob-retention",
                   partition_key: partition_key,
                   worker: "blob-worker",
                   limit: 1,
                   now_ms: 1
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2
                 )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
        assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)
        assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(encoded_state)

        assert {:ok, %{deleted_files: 0}} = Router.sweep_blob_garbage(ctx)
        assert {:ok, _encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)

        cleanup_now = System.system_time(:millisecond) + 10_000

        assert {:ok, cleaned} =
                 Ferricstore.Flow.retention_cleanup(ctx, limit: 10, now_ms: cleanup_now)

        assert cleaned.flows >= 1
        assert {:ok, nil} = Ferricstore.Flow.get(ctx, id, partition_key: partition_key)
      end

      test "Flow LMDB rebuild decodes terminal state stored as a blob ref", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        id = "blob-flow-lmdb-rebuild"
        partition_key = "tenant-blob-lmdb-rebuild"
        state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        flow_type = "blob-lmdb-rebuild"

        assert :ok =
                 Ferricstore.Flow.create(ctx, id,
                   type: flow_type,
                   partition_key: partition_key,
                   correlation_id: :binary.copy("c", 256),
                   run_at_ms: 1,
                   now_ms: 1
                 )

        assert {:ok, [claimed]} =
                 Ferricstore.Flow.claim_due(ctx, flow_type,
                   partition_key: partition_key,
                   worker: "blob-lmdb-worker",
                   limit: 1,
                   now_ms: 1
                 )

        assert :ok =
                 Ferricstore.Flow.complete(ctx, id, claimed.lease_token,
                   partition_key: partition_key,
                   fencing_token: claimed.fencing_token,
                   now_ms: 2
                 )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, state_key)
        assert {:ok, encoded_state} = BlobStore.get(ctx.data_dir, 0, ref)
        assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(encoded_state)

        state = :sys.get_state(shard)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)

        if pid = Process.whereis(Ferricstore.Flow.LMDBWriter.name(ctx.name, 0)) do
          GenServer.stop(pid, :normal, 5_000)
        end

        File.rm_rf!(lmdb_path)

        assert :ok =
                 Ferricstore.Flow.LMDBRebuilder.reconcile_shard(
                   shard_path,
                   keydir,
                   0,
                   ctx,
                   state.zset_score_index,
                   state.zset_score_lookup,
                   state.flow_index,
                   state.flow_lookup
                 )

        assert {:ok, lmdb_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
        assert {:ok, rebuilt_state} = Ferricstore.Flow.LMDB.decode_value(lmdb_blob, 10)
        assert %{id: ^id, state: "completed"} = Ferricstore.Flow.decode_record(rebuilt_state)
      end

      test "Ra compound batch apply persists large values as blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        redis_key = "blob:auto:raft-hash"
        field = CompoundKey.hash_field(redis_key, "large")
        payload = :binary.copy("C", 1536)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          [:ok],
          StateMachine.apply(
            %{index: 1, system_time: 1_000},
            {:compound_batch_put, redis_key, [{field, payload, 0}]},
            state
          )
        )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, field)
        assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
        assert payload == Router.compound_get(ctx, redis_key, field)
      end

      test "cross-shard SET apply persists large values as blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        key = "blob:auto:cross-shard"
        payload = :binary.copy("X", 2048)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        assert_state_machine_result(
          %{0 => [:ok]},
          StateMachine.apply(
            %{index: 1, system_time: 1_000},
            {:cross_shard_tx, [{0, [prepared_tx_entry("SET", [key, payload])], nil}]},
            state
          )
        )

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, key)
        assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)

        assert_state_machine_result(
          %{0 => [payload]},
          StateMachine.apply(
            %{index: 2, system_time: 1_001},
            {:cross_shard_tx, [{0, [prepared_tx_entry("GET", [key])], nil}]},
            state
          )
        )

        assert payload == Router.get(ctx, key)
      end

      test "cross-shard apply returns an error and rolls back when blob persistence fails", %{
        ctx: ctx,
        keydir: keydir
      } do
        small_key = "blob:auto:cross-shard-fail-small"
        large_key = "blob:auto:cross-shard-fail-large"
        payload = :binary.copy("X", 2048)
        shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
        active_file_path = ShardETS.file_path(shard_path, 0)

        Process.put(:ferricstore_blob_store_fsync_dir_hook, fn _path -> {:error, :eio} end)

        state =
          StateMachine.init(%{
            shard_index: 0,
            shard_data_path: shard_path,
            active_file_id: 0,
            active_file_path: active_file_path,
            ets: keydir,
            data_dir: ctx.data_dir,
            instance_ctx: ctx,
            instance_name: ctx.name
          })

        try do
          assert_state_machine_result(
            {:error, {:blob_externalize_failed, :eio}},
            StateMachine.apply(
              %{index: 1, system_time: 1_000},
              {:cross_shard_tx,
               [
                 {0,
                  [
                    prepared_tx_entry("SET", [small_key, "small"]),
                    prepared_tx_entry("SET", [large_key, payload])
                  ], nil}
               ]},
              state
            )
          )

          assert [] == :ets.lookup(keydir, small_key)
          assert [] == :ets.lookup(keydir, large_key)
        after
          Process.delete(:ferricstore_blob_store_fsync_dir_hook)
        end
      end

      test "shared compound cold batch reads materialize blob refs", %{
        ctx: ctx,
        keydir: keydir
      } do
        redis_key = "blob:auto:hash"
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")
        payload_a = :binary.copy("A", 1024)
        payload_b = :binary.copy("B", 1536)
        previous_threshold = :persistent_term.get(:ferricstore_promotion_threshold)
        :persistent_term.put(:ferricstore_promotion_threshold, 1_000_000)

        try do
          assert :ok = Router.compound_put(ctx, redis_key, field_a, payload_a, 0)
          assert :ok = Router.compound_put(ctx, redis_key, field_b, payload_b, 0)

          assert {:ok, _encoded_ref_a, ref_a} = raw_disk_blob_ref(ctx, keydir, field_a)
          assert {:ok, _encoded_ref_b, ref_b} = raw_disk_blob_ref(ctx, keydir, field_b)
          assert {:ok, ^payload_a} = BlobStore.get(ctx.data_dir, 0, ref_a)
          assert {:ok, ^payload_b} = BlobStore.get(ctx.data_dir, 0, ref_b)

          assert payload_a == Router.compound_get(ctx, redis_key, field_a)

          assert [payload_a, payload_b] ==
                   Router.compound_batch_get(ctx, redis_key, [field_a, field_b])

          assert [{^payload_a, 0}, {^payload_b, 0}] =
                   Router.compound_batch_get_meta(ctx, redis_key, [field_a, field_b])
        after
          :persistent_term.put(:ferricstore_promotion_threshold, previous_threshold)
        end
      end

      test "direct native list writes persist large elements as blob refs", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        key = "blob:auto:list"
        element_key = CompoundKey.list_element(key, 0)
        payload = :binary.copy("L", 2048)

        assert 1 == GenServer.call(shard, {:list_op, key, {:rpush, [payload]}})

        assert {:ok, _encoded_ref, ref} = raw_disk_blob_ref(ctx, keydir, element_key)
        assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
        assert [payload] == GenServer.call(shard, {:list_op, key, {:lrange, 0, -1}})
      end

      test "shared Bitcask compaction rewrites only blob refs and leaves blob bytes untouched", %{
        ctx: ctx,
        shard: shard
      } do
        payload = :binary.copy("A", 1024)
        assert {:ok, ref} = BlobStore.put(ctx.data_dir, 0, payload)
        blob_path = BlobRef.path(ctx.data_dir, 0, ref)
        blob_ref = BlobRef.encode!(ref)

        assert :ok = GenServer.call(shard, {:put, "blob:shared", blob_ref, 0})
        assert :ok = GenServer.call(shard, {:put, "blob:dead", "dead", 0})
        assert :ok = GenServer.call(shard, :flush)
        assert :ok = GenServer.call(shard, {:delete, "blob:dead"})

        force_rotate_active_file(shard)

        assert {:ok, {_written, _dropped, _reclaimed}} =
                 GenServer.call(shard, {:run_compaction, [0]})

        assert blob_ref == GenServer.call(shard, {:get, "blob:shared"})
        assert File.exists?(blob_path)
        assert {:ok, ^payload} = BlobStore.get(ctx.data_dir, 0, ref)
      end

      test "promoted dedicated compaction rewrites blob refs without owning blob bytes", %{
        ctx: ctx,
        shard: shard
      } do
        first_payload = :binary.copy("B", 2048)
        second_payload = :binary.copy("C", 4096)

        assert {:ok, first_blob_ref} = BlobStore.put(ctx.data_dir, 0, first_payload)
        assert {:ok, second_blob_ref} = BlobStore.put(ctx.data_dir, 0, second_payload)

        blob_path = BlobRef.path(ctx.data_dir, 0, first_blob_ref)
        first_ref = BlobRef.encode!(first_blob_ref)
        second_ref = BlobRef.encode!(second_blob_ref)

        redis_key = "blob_hash"
        field_a = CompoundKey.hash_field(redis_key, "a")
        field_b = CompoundKey.hash_field(redis_key, "b")

        assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_a, first_ref, 0})
        assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, second_ref, 0})

        state = :sys.get_state(shard)
        dedicated_path = state.promoted_instances[redis_key].path

        refute String.starts_with?(
                 dedicated_path,
                 Ferricstore.DataDir.blob_shard_path(ctx.data_dir, 0)
               )

        :sys.replace_state(shard, fn state ->
          ShardCompound.compact_dedicated(state, redis_key, dedicated_path)
        end)

        assert first_ref == GenServer.call(shard, {:compound_get, redis_key, field_a})
        assert second_ref == GenServer.call(shard, {:compound_get, redis_key, field_b})
        assert File.exists?(blob_path)
        assert {:ok, ^first_payload} = BlobStore.get(ctx.data_dir, 0, first_blob_ref)
        assert {:ok, ^second_payload} = BlobStore.get(ctx.data_dir, 0, second_blob_ref)
      end

      test "promoted compound batch put externalizes large values with one blob segment fsync", %{
        shard: shard
      } do
        redis_key = "blob:promoted:batch-put"
        seed_a = CompoundKey.hash_field(redis_key, "seed-a")
        seed_b = CompoundKey.hash_field(redis_key, "seed-b")

        assert :ok = GenServer.call(shard, {:compound_put, redis_key, seed_a, "small-a", 0})
        assert :ok = GenServer.call(shard, {:compound_put, redis_key, seed_b, "small-b", 0})

        state = :sys.get_state(shard)
        assert Map.has_key?(state.promoted_instances, redis_key)

        parent = self()

        Process.put(:ferricstore_blob_store_fsync_file_hook, fn path ->
          send(parent, {:blob_fsync_file, path})
          Ferricstore.Bitcask.NIF.v2_fsync(path)
        end)

        on_exit(fn ->
          Process.delete(:ferricstore_blob_store_fsync_file_hook)
        end)

        field_a = CompoundKey.hash_field(redis_key, "large-a")
        field_b = CompoundKey.hash_field(redis_key, "large-b")
        payload_a = :binary.copy("A", 1024)
        payload_b = :binary.copy("B", 1024)

        assert {:reply, :ok, _new_state} =
                 ShardCompound.handle_compound_batch_put(
                   redis_key,
                   [{field_a, payload_a, 0}, {field_b, payload_b, 0}],
                   state
                 )

        assert payload_a == GenServer.call(shard, {:compound_get, redis_key, field_a})
        assert payload_b == GenServer.call(shard, {:compound_get, redis_key, field_b})

        assert_receive {:blob_fsync_file, first_path}, 1000
        refute_receive {:blob_fsync_file, _second_path}, 100
        assert String.ends_with?(first_path, ".bloblog")
      end

      test "blob garbage sweep removes deleted direct blobs and preserves promoted live refs", %{
        ctx: ctx,
        shard: shard,
        keydir: keydir
      } do
        attach_blob_gc_handler()

        live_key = "blob:gc:live"
        dead_key = "blob:gc:dead"
        redis_key = "blob_gc_hash"
        field = CompoundKey.hash_field(redis_key, "field")
        field_b = CompoundKey.hash_field(redis_key, "field-b")

        assert :ok = Router.put(ctx, live_key, :binary.copy("L", 1024), 0)
        assert :ok = Router.put(ctx, dead_key, :binary.copy("D", 1024), 0)

        assert :ok =
                 GenServer.call(
                   shard,
                   {:compound_put, redis_key, field, :binary.copy("P", 1024), 0}
                 )

        assert :ok = GenServer.call(shard, {:compound_put, redis_key, field_b, "small", 0})

        assert {:ok, _live_encoded, live_ref} = raw_disk_blob_ref(ctx, keydir, live_key)
        assert {:ok, _dead_encoded, dead_ref} = raw_disk_blob_ref(ctx, keydir, dead_key)

        assert {:ok, _promoted_encoded, promoted_ref} =
                 promoted_disk_blob_ref(shard, keydir, redis_key, field)

        live_path = BlobRef.path(ctx.data_dir, 0, live_ref)
        dead_path = BlobRef.path(ctx.data_dir, 0, dead_ref)
        promoted_path = BlobRef.path(ctx.data_dir, 0, promoted_ref)

        assert :ok = GenServer.call(shard, {:delete, dead_key})

        assert {:ok, %{deleted_files: 0} = stats} = Router.sweep_blob_garbage(ctx)

        assert_receive {:blob_gc, [:ferricstore, :blob, :gc], measurements,
                        %{result: :ok, shard_count: 1}}

        assert measurements.deleted_files == stats.deleted_files
        assert measurements.deleted_bytes == stats.deleted_bytes
        assert measurements.kept_files == stats.kept_files

        assert File.exists?(live_path)
        assert File.exists?(dead_path)
        assert File.exists?(promoted_path)
        assert :binary.copy("L", 1024) == Router.get(ctx, live_key)
        assert :binary.copy("P", 1024) == GenServer.call(shard, {:compound_get, redis_key, field})
      end
    end
  end
end
