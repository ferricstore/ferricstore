defmodule Ferricstore.FlowGovernanceReleaseOutboxTest do
  use Ferricstore.Test.FlowCase

  alias Ferricstore.Flow.Governance.LimitReconciler
  alias Ferricstore.Flow.Governance.LimitRecord
  alias Ferricstore.Flow.Governance.LimitStore
  alias Ferricstore.Flow.Governance.ReleaseOutbox
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.Router

  @partition "tenant-governance-release-outbox"

  setup do
    old_enabled = Application.get_env(:ferricstore, :flow_governance_limit_cache_enabled)
    Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, false)

    on_exit(fn ->
      if is_nil(old_enabled) do
        Application.delete_env(:ferricstore, :flow_governance_limit_cache_enabled)
      else
        Application.put_env(:ferricstore, :flow_governance_limit_cache_enabled, old_enabled)
      end
    end)

    :ok
  end

  test "release outbox codecs reject non-canonical and invalid records" do
    intent = release_intent(String.duplicate("flow", 1_024), "scope", "reservation")
    encoded_intent = ReleaseOutbox.encode_intent(intent)
    encoded_meta = ReleaseOutbox.encode_meta(%{head: 1, tail: 1})

    compressed_intent =
      encoded_intent
      |> :erlang.binary_to_term([:safe])
      |> :erlang.term_to_binary(compressed: 9)

    assert <<131, 80, _rest::binary>> = compressed_intent

    assert {:error, "ERR flow governance release intent is corrupt"} =
             ReleaseOutbox.decode_intent(compressed_intent)

    assert {:error, "ERR flow governance release intent is corrupt"} =
             ReleaseOutbox.decode_intent(encoded_intent <> <<0>>)

    assert {:error, "ERR flow governance release outbox is corrupt"} =
             ReleaseOutbox.decode_meta(encoded_meta <> <<0>>)

    assert_raise ArgumentError, fn -> ReleaseOutbox.encode_meta(%{head: 2, tail: 0}) end

    for invalid <- [
          %{intent | reservation_id: ""},
          %{intent | created_at_ms: -1},
          Map.put(intent, :unexpected, "field")
        ] do
      assert_raise ArgumentError, fn -> ReleaseOutbox.encode_intent(invalid) end
    end
  end

  test "governed claims do not rewrite the limit owner to bind Flow metadata" do
    type = unique_flow_id("outbox-no-bind-type")
    scope = unique_flow_id("outbox-no-bind-scope")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)

    assert {:ok, [claimed]} = claim(type, scope, 1, 1_001)
    reservation_id = raw_record(claimed.id).governance_limit.reservation_id

    ctx = FerricStore.Instance.get(:default)
    owner = raw_owner(ctx, scope)
    lease = owner.leases[0]

    reservation_key =
      Keys.governance_limit_reservation_key(scope, 0, lease.epoch, reservation_id)

    assert lease.reservations == %{}
    assert Router.get(ctx, reservation_key) != nil
  end

  test "terminal Raft apply leaves a durable release intent when API release is skipped" do
    type = unique_flow_id("outbox-terminal-type")
    scope = unique_flow_id("outbox-terminal-scope")
    create_due_flows(type, 1)
    lease_limit!(scope, 1)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, %{reservation_ids: [reservation_id]}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 1_000,
               now_ms: 1_001
             )

    assert {:ok, [claimed]} =
             Router.flow_claim_due(ctx, %{
               type: type,
               state: "queued",
               worker: "raw-outbox-worker",
               lease_ms: 1_000,
               limit: 1,
               priority: nil,
               partition_key: @partition,
               now_ms: 1_001,
               governance_limit: %{
                 scope: scope,
                 shard_id: 0,
                 enforcement: :strict_global,
                 reservation_ids: [reservation_id]
               }
             })

    assert {:ok, attrs} =
             Ferricstore.Flow.MutationAttrs.complete_attrs(claimed.id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               now_ms: 1_002
             )

    _committed_without_api_release = Router.flow_complete(ctx, attrs)
    assert raw_record(claimed.id).state == "completed"
    assert raw_owner(ctx, scope).leases[0].in_use == 1

    handler_id = "governance-release-outbox-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :governance, :limit_reconcile],
        fn event, measurements, metadata, _config ->
          send(parent, {:limit_reconcile, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, %{released: 1, errors: 0}} =
             LimitReconciler.run_once(ctx, now_ms: 1_003, reservation_limit: 16)

    assert_receive {:limit_reconcile, [:ferricstore, :flow, :governance, :limit_reconcile],
                    %{count: 1},
                    %{status: :ok, released: 1, retained: 0, errors: 0, read_batches: 1}}

    assert raw_owner(ctx, scope).leases[0].in_use == 0
  end

  test "release reconciliation bounds reservations and batches Flow reads by partition" do
    type = unique_flow_id("outbox-bounded-type")
    scope = unique_flow_id("outbox-bounded-scope")
    create_due_flows(type, 7)
    lease_limit!(scope, 7)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, claimed} = claim(type, scope, 7, 1_001)
    assert length(claimed) == 7

    Enum.each(claimed, fn record ->
      assert {:ok, attrs} =
               Ferricstore.Flow.MutationAttrs.complete_attrs(record.id, record.lease_token,
                 partition_key: @partition,
                 fencing_token: record.fencing_token,
                 now_ms: 1_002
               )

      _committed_without_api_release = Router.flow_complete(ctx, attrs)
    end)

    assert raw_owner(ctx, scope).leases[0].in_use == 7

    assert {:ok, first} =
             LimitReconciler.run_once(ctx, now_ms: 1_003, reservation_limit: 3)

    assert first.released == 3
    assert first.read_batches == 1
    assert first.next_cursor != nil
    assert raw_owner(ctx, scope).leases[0].in_use == 4

    assert {:ok, second} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_004,
               reservation_limit: 3,
               cursor: first.next_cursor
             )

    assert second.released == 3
    assert second.read_batches == 1
    assert raw_owner(ctx, scope).leases[0].in_use == 1

    assert {:ok, final} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_005,
               reservation_limit: 3,
               cursor: second.next_cursor
             )

    assert final.released == 1
    assert final.read_batches == 1
    assert final.next_cursor != nil
    assert raw_owner(ctx, scope).leases[0].in_use == 0

    assert {:ok, %{released: 0, retained: 0, errors: 0, read_batches: 0, next_cursor: nil}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_006,
               reservation_limit: 3,
               cursor: final.next_cursor
             )
  end

  test "release reconciliation fails closed when the Flow record is corrupt" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("outbox-corrupt-flow-scope")
    flow_id = unique_flow_id("outbox-corrupt-flow")
    reservation_id = reserve_limit!(ctx, scope)
    state_key = Keys.state_key(flow_id, @partition)
    state_shard = Router.shard_for(ctx, state_key)

    assert :ok = WARaftBackend.write(state_shard, {:put, state_key, <<0, 1, 2, 3>>, 0})

    seed_outbox!(0, [release_intent(flow_id, scope, reservation_id)])

    assert {:ok, %{released: 0, retained: 0, errors: 1}} =
             LimitReconciler.run_once(ctx, now_ms: 1_002, reservation_limit: 16)

    assert raw_owner(ctx, scope).leases[0].in_use == 1
    assert {:ok, %{entries: [{1, _intent}]}} = ReleaseOutbox.read_page(ctx, 0, 16)
  end

  test "release reconciliation fails closed when the Flow shard is unavailable" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("outbox-unavailable-flow-scope")
    owner_shard = Router.shard_for(ctx, Keys.governance_limit_key(scope))
    partition_key = partition_outside_shard(ctx, owner_shard)
    flow_id = unique_flow_id("outbox-unavailable-flow")
    type = unique_flow_id("outbox-unavailable-type")

    assert :ok =
             FerricStore.flow_create(flow_id,
               type: type,
               state: "queued",
               partition_key: partition_key,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    lease_limit!(scope, 1)

    assert {:ok, [running]} =
             FerricStore.flow_claim_due(type,
               states: ["queued"],
               partition_key: partition_key,
               worker: "outbox-unavailable-worker",
               limit: 1,
               lease_ms: 30_000,
               now_ms: 1_001,
               governance_limit_scope: scope,
               governance_shard_id: 0
             )

    reservation_id =
      ctx
      |> Router.flow_get(running.id, partition_key)
      |> Ferricstore.Flow.decode_record()
      |> get_in([:governance_limit, :reservation_id])

    state_key = Keys.state_key(flow_id, partition_key)
    state_shard = Router.shard_for(ctx, state_key)
    outbox_shard = Enum.find(0..(ctx.shard_count - 1), &(&1 != state_shard))

    :ok = Ferricstore.Flow.LMDBWriter.flush_all(ctx.name, ctx.shard_count)

    lmdb_path =
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(state_shard)
      |> Ferricstore.Flow.LMDB.path()

    assert :ok = Ferricstore.Flow.LMDB.delete_state_artifacts(lmdb_path, state_key)

    seed_outbox!(outbox_shard, [
      release_intent(flow_id, scope, reservation_id, partition_key)
    ])

    unavailable_ctx = %{
      ctx
      | keydir_refs: put_elem(ctx.keydir_refs, state_shard, :ferricstore_unavailable_flow_keydir),
        shard_names: put_elem(ctx.shard_names, state_shard, :ferricstore_unavailable_flow_shard)
    }

    assert [:unavailable] =
             Router.flow_batch_get_with_status(unavailable_ctx, [flow_id], partition_key)

    assert {:ok, %{released: 0, retained: 0, errors: 1}} =
             LimitReconciler.run_once(unavailable_ctx,
               now_ms: 1_002,
               reservation_limit: 16,
               cursor: ReleaseOutbox.encode_reconcile_cursor(outbox_shard, %{})
             )

    assert raw_owner(ctx, scope).leases[0].in_use == 1

    assert {:ok, %{entries: [{1, _intent}]}} =
             ReleaseOutbox.read_page(ctx, outbox_shard, 16)
  end

  test "replayed duplicate intents release one reservation and acknowledge every replay" do
    ctx = FerricStore.Instance.get(:default)
    scope = unique_flow_id("outbox-duplicate-scope")
    flow_id = unique_flow_id("outbox-duplicate-flow")
    reservation_id = reserve_limit!(ctx, scope)
    intent = release_intent(flow_id, scope, reservation_id)

    seed_outbox!(0, [intent, intent])

    assert {:ok, %{released: 2, retained: 0, errors: 0}} =
             LimitReconciler.run_once(ctx, now_ms: 1_002, reservation_limit: 16)

    assert raw_owner(ctx, scope).leases[0].in_use == 0
    assert {:ok, %{entries: []}} = ReleaseOutbox.read_page(ctx, 0, 16)
  end

  test "a retained head rotates reconciliation to a nonempty later shard" do
    type = unique_flow_id("outbox-fairness-type")
    retained_scope = unique_flow_id("outbox-fairness-retained-scope")
    releasable_scope = unique_flow_id("outbox-fairness-releasable-scope")
    create_due_flows(type, 1)
    lease_limit!(retained_scope, 1)
    ctx = FerricStore.Instance.get(:default)

    assert {:ok, [running]} = claim(type, retained_scope, 1, 1_001)
    retained_reservation_id = raw_record(running.id).governance_limit.reservation_id
    releasable_reservation_id = reserve_limit!(ctx, releasable_scope)

    seed_outbox!(0, [release_intent(running.id, retained_scope, retained_reservation_id)])

    seed_outbox!(1, [
      release_intent(
        unique_flow_id("outbox-fairness-missing-flow"),
        releasable_scope,
        releasable_reservation_id
      )
    ])

    assert {:ok, %{released: 0, retained: 1, errors: 0, next_cursor: next_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_002,
               reservation_limit: 16,
               cursor: ReleaseOutbox.encode_reconcile_cursor(0, %{})
             )

    assert next_cursor == ReleaseOutbox.encode_reconcile_cursor(1, %{})

    assert {:ok, %{released: 1, retained: 0, errors: 0}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_003,
               reservation_limit: 16,
               cursor: next_cursor
             )

    assert raw_owner(ctx, retained_scope).leases[0].in_use == 1
    assert raw_owner(ctx, releasable_scope).leases[0].in_use == 0
  end

  test "a corrupt outbox shard is reported without starving a healthy later shard" do
    ctx = FerricStore.Instance.get(:default)
    corrupt_scope = unique_flow_id("outbox-corrupt-shard-scope")
    healthy_scope = unique_flow_id("outbox-healthy-shard-scope")
    corrupt_reservation_id = reserve_limit!(ctx, corrupt_scope)
    healthy_reservation_id = reserve_limit!(ctx, healthy_scope)

    seed_outbox!(0, [
      release_intent(
        unique_flow_id("outbox-corrupt-shard-flow"),
        corrupt_scope,
        corrupt_reservation_id
      )
    ])

    corrupt_key = Keys.governance_release_outbox_intent_key(0, 1)
    assert :ok = WARaftBackend.write(0, {:put, corrupt_key, <<0, 1, 2, 3>>, 0})

    seed_outbox!(1, [
      release_intent(
        unique_flow_id("outbox-healthy-shard-flow"),
        healthy_scope,
        healthy_reservation_id
      )
    ])

    assert {:ok, %{released: 1, retained: 0, errors: 1, next_cursor: next_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_002,
               reservation_limit: 16,
               cursor: ReleaseOutbox.encode_reconcile_cursor(0, %{})
             )

    assert is_binary(next_cursor)
    assert raw_owner(ctx, corrupt_scope).leases[0].in_use == 1
    assert raw_owner(ctx, healthy_scope).leases[0].in_use == 0

    assert {:error, "ERR flow governance release outbox entry is missing or corrupt"} =
             ReleaseOutbox.read_page(ctx, 0, 16)
  end

  test "release outbox acknowledgements are bounded to one page" do
    assert {:error, "ERR invalid flow governance release outbox acknowledgement"} =
             ReleaseOutbox.acknowledge(%{head: 1, tail: 257}, 1, 257)

    ctx = FerricStore.Instance.get(:default)

    assert {:error, "ERR invalid flow governance release outbox acknowledgement"} =
             Router.flow_governance_release_outbox_ack(ctx, 0, 1, 257)

    assert {:error, "ERR invalid flow governance release outbox completion"} =
             Router.flow_governance_release_outbox_mark_completed(ctx, 0, Enum.to_list(1..257))
  end

  test "release reconciliation uses one bounded cursor format" do
    cursor = ReleaseOutbox.encode_reconcile_cursor(2, %{0 => 257, 1 => 17})

    assert String.starts_with?(cursor, "v1:")
    refute String.starts_with?(cursor, "v2:")

    assert {:ok, %{next_shard: 2, positions: %{0 => 257, 1 => 17}}} =
             ReleaseOutbox.decode_reconcile_cursor(cursor, 4)

    assert {:error, "ERR invalid flow governance release outbox cursor"} =
             ReleaseOutbox.decode_reconcile_cursor("v1:2", 4)

    assert {:error, "ERR invalid flow governance release outbox cursor"} =
             ReleaseOutbox.decode_reconcile_cursor("v2:2|0=257", 4)

    assert {:error, "ERR invalid flow governance release outbox cursor"} =
             ReleaseOutbox.decode_reconcile_cursor("v1:2|0=1,0=2", 4)
  end

  test "release classification distinguishes missing from malformed read status" do
    intent = %{reservation_id: "reservation-1"}

    assert LimitReconciler.classify_release(intent, nil) == :release
    assert LimitReconciler.classify_release(intent, :unavailable) == :error
    assert LimitReconciler.classify_release(intent, :unexpected_status) == :error
    assert LimitReconciler.classify_release(intent, {:error, :timeout}) == :error

    assert LimitReconciler.normalize_flow_batch_results([nil, :unavailable], 2) ==
             [nil, :unavailable]

    assert LimitReconciler.normalize_flow_batch_results([nil], 2) ==
             [:unavailable, :unavailable]

    assert LimitReconciler.normalize_flow_batch_results(:malformed, 2) ==
             [:unavailable, :unavailable]
  end

  test "release outbox page materializes 256 cold entries with one shard batch call" do
    ctx = FerricStore.Instance.get(:default)
    shard_index = 0
    scope = unique_flow_id("outbox-batch-read-scope")
    reservation_id = unique_flow_id("outbox-batch-read-reservation")

    intent =
      release_intent(
        unique_flow_id("outbox-batch-read-flow"),
        scope,
        reservation_id
      )

    meta_key = Keys.governance_release_outbox_meta_key(shard_index)
    encoded_meta = ReleaseOutbox.encode_meta(%{head: 1, tail: 256})

    values =
      1..256
      |> Enum.flat_map(fn sequence ->
        [
          {Keys.governance_release_outbox_intent_key(shard_index, sequence),
           ReleaseOutbox.encode_intent(intent)},
          {Keys.governance_release_outbox_completed_key(shard_index, sequence), nil}
        ]
      end)
      |> Map.new()
      |> Map.put(meta_key, encoded_meta)

    parent = self()
    reader = spawn(fn -> shard_batch_reader(parent, values) end)
    on_exit(fn -> Process.exit(reader, :kill) end)

    cold_ctx = %{
      ctx
      | name: :release_outbox_batch_read_probe,
        keydir_refs: put_elem(ctx.keydir_refs, shard_index, make_ref()),
        shard_names: put_elem(ctx.shard_names, shard_index, reader)
    }

    assert {:ok, %{entries: entries}} = ReleaseOutbox.read_page(cold_ctx, shard_index, 256)
    assert length(entries) == 256

    assert_receive {:shard_batch_read, {:get, ^meta_key}}
    assert_receive {:shard_batch_read, {:get_many, keys, deadline_ms}}
    assert length(keys) == 512
    assert deadline_ms > System.monotonic_time(:millisecond)
    refute_receive {:shard_batch_read, _request}, 50
  end

  test "shard batch reads reject unbounded and oversized requests before dispatch" do
    ctx = FerricStore.Instance.get(:default)

    assert {:error, "ERR invalid shard batch read request"} =
             Router.read_shard_values(ctx, 0, List.duplicate("key", 513))

    assert {:error, "ERR invalid shard batch read request"} =
             Router.read_shard_values(ctx, 0, [:not_a_binary])

    assert {:error, "ERR invalid shard batch read request"} =
             Router.read_shard_values(ctx, 0, [:binary.copy("k", 65_536)])

    assert {:error, "ERR invalid shard batch read request"} =
             Router.read_shard_values(ctx, 0, List.duplicate(:binary.copy("k", 65_535), 17))

    assert {:error, "ERR invalid shard batch read request"} =
             GenServer.call(elem(ctx.shard_names, 0), {:get_many, List.duplicate("key", 513)})
  end

  test "512 cold shard reads are deferred as one batch without blocking the shard caller" do
    keydir = :ets.new(:release_outbox_deferred_batch_keydir, [:set, :public])
    keys = Enum.map(1..512, &"outbox-deferred-cold-#{&1}")

    Enum.with_index(keys)
    |> Enum.each(fn {key, offset} ->
      :ets.insert(keydir, {key, nil, 0, 0, 0, offset, 1})
    end)

    parent = self()

    batch_reader = fn locations, _timeout ->
      send(parent, {:deferred_cold_batch_started, length(locations)})
      Process.sleep(250)
      {:error, :injected_timeout}
    end

    state = %{
      keydir: keydir,
      shard_data_path: System.tmp_dir!(),
      get_many_pread_batch: batch_reader
    }

    started_at = System.monotonic_time(:millisecond)

    assert {:noreply, admitted_state} =
             Ferricstore.Store.Shard.Reads.handle_get_many(
               keys,
               {self(), make_ref()},
               state
             )

    assert Map.take(admitted_state, Map.keys(state)) == state
    assert map_size(admitted_state.get_many_workers) == 1

    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 100
    assert_receive {:deferred_cold_batch_started, 512}, 100
    refute_receive {:deferred_cold_batch_started, _other}, 50
  end

  test "a fail-closed head does not starve entries beyond the first page" do
    ctx = FerricStore.Instance.get(:default)
    corrupt_scope = unique_flow_id("outbox-page-corrupt-scope")
    corrupt_flow_id = unique_flow_id("outbox-page-corrupt-flow")
    corrupt_reservation_id = reserve_limit!(ctx, corrupt_scope)
    releasable_scope = unique_flow_id("outbox-page-releasable-scope")
    releasable_reservation_id = reserve_limit!(ctx, releasable_scope)
    state_key = Keys.state_key(corrupt_flow_id, @partition)
    state_shard = Router.shard_for(ctx, state_key)

    assert :ok = WARaftBackend.write(state_shard, {:put, state_key, <<0, 1, 2, 3>>, 0})

    corrupt_intent =
      release_intent(corrupt_flow_id, corrupt_scope, corrupt_reservation_id)

    tail_intent =
      release_intent(
        unique_flow_id("outbox-page-missing-flow"),
        releasable_scope,
        releasable_reservation_id
      )

    seed_outbox!(0, List.duplicate(corrupt_intent, 256) ++ [tail_intent])

    handler_id = "governance-release-outbox-sparse-#{System.unique_integer([:positive])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :flow, :governance, :limit_release],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:sparse_limit_release, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, %{released: 0, retained: 0, errors: 256, next_cursor: next_cursor}} =
             LimitReconciler.run_once(ctx, now_ms: 1_002, reservation_limit: 256)

    assert raw_owner(ctx, corrupt_scope).leases[0].in_use == 1
    assert raw_owner(ctx, releasable_scope).leases[0].in_use == 1

    assert {:ok, %{released: 1, retained: 0, errors: 0, next_cursor: retry_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_003,
               reservation_limit: 256,
               cursor: next_cursor
             )

    assert_receive {:sparse_limit_release, %{scope: ^releasable_scope}}
    assert raw_owner(ctx, corrupt_scope).leases[0].in_use == 1
    assert raw_owner(ctx, releasable_scope).leases[0].in_use == 0

    assert {:ok, %{released: 0, errors: 256, next_cursor: completed_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_004,
               reservation_limit: 256,
               cursor: retry_cursor
             )

    assert {:ok, %{released: 0, retained: 0, errors: 0, next_cursor: head_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_005,
               reservation_limit: 256,
               cursor: completed_cursor
             )

    refute_receive {:sparse_limit_release, %{scope: ^releasable_scope}}, 100

    assert :ok = WARaftBackend.write(state_shard, {:delete, state_key})

    assert {:ok, %{released: 256, errors: 0, next_cursor: marker_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_006,
               reservation_limit: 256,
               cursor: head_cursor
             )

    assert_receive {:sparse_limit_release, %{scope: ^corrupt_scope}}

    assert {:ok, %{released: 0, errors: 0, next_cursor: idle_cursor}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_007,
               reservation_limit: 256,
               cursor: marker_cursor
             )

    assert is_binary(idle_cursor)

    assert {:ok, %{released: 0, errors: 0, next_cursor: nil}} =
             LimitReconciler.run_once(ctx,
               now_ms: 1_008,
               reservation_limit: 256,
               cursor: idle_cursor
             )

    refute_receive {:sparse_limit_release, %{scope: ^releasable_scope}}, 100
    assert {:ok, %{entries: []}} = ReleaseOutbox.read_page(ctx, 0, 256)
  end

  defp create_due_flows(type, count) do
    Enum.each(1..count, fn index ->
      assert :ok =
               FerricStore.flow_create(unique_flow_id("outbox-flow-#{index}"),
                 type: type,
                 state: "queued",
                 partition_key: @partition,
                 run_at_ms: 1_000,
                 now_ms: 1_000
               )
    end)
  end

  defp lease_limit!(scope, limit) do
    assert {:ok, _lease} =
             FerricStore.flow_limit_lease(scope,
               shard_id: 0,
               amount: limit,
               limit: limit,
               ttl_ms: 30_000,
               now_ms: 1_000
             )
  end

  defp reserve_limit!(ctx, scope) do
    lease_limit!(scope, 1)

    assert {:ok, %{reservation_ids: [reservation_id]}} =
             LimitStore.spend(ctx, scope,
               shard_id: 0,
               amount: 1,
               ttl_ms: 30_000,
               now_ms: 1_001
             )

    reservation_id
  end

  defp release_intent(flow_id, scope, reservation_id, partition_key \\ @partition) do
    %{
      flow_id: flow_id,
      partition_key: partition_key,
      scope: scope,
      shard_id: 0,
      reservation_id: reservation_id,
      enforcement: :strict_global,
      created_at_ms: 1_001
    }
  end

  defp seed_outbox!(shard_index, intents) do
    meta_key = Keys.governance_release_outbox_meta_key(shard_index)
    meta = %{head: 1, tail: length(intents)}

    entries =
      intents
      |> Enum.with_index(1)
      |> Enum.map(fn {intent, sequence} ->
        key = Keys.governance_release_outbox_intent_key(shard_index, sequence)
        {key, ReleaseOutbox.encode_intent(intent), 0}
      end)
      |> Kernel.++([{meta_key, ReleaseOutbox.encode_meta(meta), 0}])

    assert {:ok, results} = WARaftBackend.write_put_batch(shard_index, entries)
    assert length(results) == length(entries)
    assert Enum.all?(results, &(&1 == :ok))
  end

  defp partition_outside_shard(ctx, excluded_shard) do
    Enum.find_value(0..1_000, fn index ->
      partition_key = "outbox-unavailable-partition-#{index}"
      state_key = Keys.state_key("outbox-unavailable-probe", partition_key)

      if Router.shard_for(ctx, state_key) != excluded_shard, do: partition_key
    end)
  end

  defp shard_batch_reader(parent, values) do
    receive do
      {:"$gen_call", from, {:get, key} = request} ->
        send(parent, {:shard_batch_read, request})
        GenServer.reply(from, Map.get(values, key))
        shard_batch_reader(parent, values)

      {:"$gen_call", from, {:get_many, keys} = request} ->
        send(parent, {:shard_batch_read, request})
        GenServer.reply(from, Enum.map(keys, &Map.get(values, &1)))
        shard_batch_reader(parent, values)

      {:"$gen_call", from, {:get_many, keys, _deadline_ms} = request} ->
        send(parent, {:shard_batch_read, request})
        GenServer.reply(from, Enum.map(keys, &Map.get(values, &1)))
        shard_batch_reader(parent, values)
    end
  end

  defp claim(type, scope, limit, now_ms) do
    FerricStore.flow_claim_due(type,
      states: ["queued"],
      partition_key: @partition,
      worker: "outbox-worker",
      limit: limit,
      lease_ms: 1_000,
      now_ms: now_ms,
      governance_limit_scope: scope,
      governance_shard_id: 0
    )
  end

  defp raw_record(id) do
    ctx = FerricStore.Instance.get(:default)
    value = Router.flow_get(ctx, id, @partition)
    Ferricstore.Flow.decode_record(value)
  end

  defp raw_owner(ctx, scope) do
    value = Router.get(ctx, Keys.governance_limit_key(scope))
    {:ok, owner} = LimitRecord.decode_owner(value)
    owner
  end
end
