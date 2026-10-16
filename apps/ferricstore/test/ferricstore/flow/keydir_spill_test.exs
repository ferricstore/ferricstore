defmodule Ferricstore.Flow.KeydirSpillTest do
  use ExUnit.Case, async: false
  @moduletag :flow
  @moduletag :global_state

  alias Ferricstore.Flow.{
    Keys,
    KeydirSpill,
    LMDB,
    LMDBWriter,
    PolicyMigration,
    RetentionCleanupMember
  }

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  setup do
    old_spill = Application.get_env(:ferricstore, :flow_keydir_spill)
    old_sweeper = Application.get_env(:ferricstore, :flow_retention_sweeper_enabled)
    Application.put_env(:ferricstore, :flow_retention_sweeper_enabled, false)

    isolated = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      restore_env(:flow_keydir_spill, old_spill)
      restore_env(:flow_retention_sweeper_enabled, old_sweeper)
      ShardHelpers.teardown_isolated_data_dir(isolated)
    end)

    {:ok, isolated: isolated}
  end

  test "the writer copies a row into LMDB only while its keydir row is unchanged" do
    ets = :ets.new(:keydir_spill_expand_test, [:set, :public])
    key = Keys.type_catalog_member_key("expand-type", Keys.state_key("expand", "partition"))
    row = {key, "value-1", 0, 3, {:waraft_apply_projection, 7}, 0, 7}
    op = {:keydir_spill_from_source, ets, row}
    acc = %{ops: [], op_count: 0}

    expand = fn ->
      Ferricstore.Flow.LMDBWriter.ProjectionOps.expand_op(%{path: "unused"}, op, acc)
    end

    # A read only bumps the LFU counter; the row is still the one that was requested.
    :ets.insert(ets, put_elem(row, 3, 9))
    assert {:ok, %{ops: [{:put, spill_key, encoded}], op_count: 1}} = expand.()
    assert spill_key == KeydirSpill.key(key)
    assert LMDB.decode_value(encoded, 0) == {:ok, "value-1"}

    :ets.insert(ets, {key, "value-2", 0, 3, {:waraft_apply_projection, 8}, 0, 7})
    assert {:ok, %{ops: []}} = expand.()

    :ets.insert(ets, {key, nil, 0, :flow_state_deleted, :deleted, 0, 0})
    assert {:ok, %{ops: []}} = expand.()

    :ets.delete(ets, key)
    assert {:ok, %{ops: []}} = expand.()
  end

  test "reads outside apply resolve hot, spilled and deleted catalog rows in order" do
    data_dir = Path.join(System.tmp_dir!(), "keydir-spill-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf(data_dir) end)

    lmdb_path = data_dir |> Ferricstore.DataDir.shard_data_path(0) |> LMDB.path()
    File.mkdir_p!(lmdb_path)
    keydir = :ets.new(:keydir_spill_read_test, [:set, :public])
    instance = :"keydir_spill_read_#{System.unique_integer([:positive])}"
    ctx = %{name: instance, data_dir: data_dir, keydir_refs: {keydir}}

    [hot, spilled, deleted, absent] =
      for id <- ~w(hot spilled deleted absent) do
        Keys.type_catalog_member_key("read-type", Keys.state_key(id, "read-partition"))
      end

    :ets.insert(keydir, {hot, "hot-value", 0, 1, {:waraft_apply_projection, 1}, 0, 9})
    :ets.insert(keydir, {deleted, nil, 0, :flow_state_deleted, :deleted, 0, 0})

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, KeydirSpill.key(spilled), LMDB.encode_value("spilled-value", 0)},
               {:put, KeydirSpill.key(deleted), LMDB.encode_value("stale-value", 0)}
             ])

    keydir_read = fn keys -> {:ok, Enum.map(keys, &"keydir:#{&1 == hot}")} end

    assert :keydir = KeydirSpill.read_local(ctx, 0, spilled),
           "without spilled rows on the shard, reads use the keydir"

    assert :ok = KeydirSpill.refresh_present(instance, 0, lmdb_path)
    assert :keydir = KeydirSpill.read_local(ctx, 0, hot)
    assert {:ok, "spilled-value"} = KeydirSpill.read_local(ctx, 0, spilled)
    assert {:ok, nil} = KeydirSpill.read_local(ctx, 0, deleted)
    assert {:ok, nil} = KeydirSpill.read_local(ctx, 0, absent)

    assert {:ok, ["keydir:true", "spilled-value", nil, nil]} =
             KeydirSpill.read_values(ctx, 0, [hot, spilled, deleted, absent], keydir_read)
  end

  test "type catalog rows stay in the keydir when spilling is off", %{isolated: isolated} do
    Application.delete_env(:ferricstore, :flow_keydir_spill)
    flow = finish_flow(isolated, ttl_ms: nil)

    assert_state_pruned(flow)
    assert [{_key, value, _, _, _, _, _}] = :ets.lookup(flow.keydir, flow.catalog_key)
    assert is_binary(value)
    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
  end

  test "a pruned terminal flow moves its type catalog row from the keydir to LMDB",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: nil)

    assert_state_pruned(flow)
    assert_catalog_spilled(flow)

    assert {:ok, mirror} = LMDB.get(flow.lmdb_path, flow.catalog_key)
    assert {:ok, spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
    assert LMDB.decode_value(spilled, 0) == LMDB.decode_value(mirror, 0)

    assert {:ok, catalog_value} = LMDB.decode_value(spilled, 0)
    assert {:ok, %{state_key: state_key}} = PolicyMigration.decode_catalog(catalog_value)
    assert state_key == flow.state_key
  end

  test "retention of a spilled terminal flow still removes its type catalog rows",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: 1)

    assert_state_pruned(flow)
    assert_catalog_spilled(flow)
    revision = descriptor_revision(flow)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 FerricStore.flow_get(flow.id, partition_key: flow.partition) == {:ok, nil}
               end,
               "the cancelled Flow should expire"
             )

    drain_retention(System.system_time(:millisecond) + 10_000)
    assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)

    assert :not_found = LMDB.get(flow.lmdb_path, flow.catalog_key)
    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
    assert [] = :ets.lookup(flow.keydir, flow.catalog_key)

    assert {:ok, []} =
             LMDB.prefix_entries(
               flow.lmdb_path,
               Keys.policy_catalog_projection_prefix(flow.type),
               10
             )

    assert descriptor_revision(flow) > revision,
           "removing the spilled member must advance the type membership revision"
  end

  test "retention right after a flush cleans up a terminal flow whose catalog row is spilling",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: 1, flush: false)

    # The flush prunes the terminal Flow and requests the spill of its catalog
    # row. That spill must not hold back retention, which waits for pending
    # Flow LMDB projection work.
    assert :ok = Ferricstore.Flow.HistoryProjector.flush(flow.ctx, flow.shard_index)
    assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)

    assert {:ok, %{flows: 1}} =
             Router.flow_retention_cleanup(flow.ctx, %{
               limit: 10,
               now_ms: System.system_time(:millisecond) + 10_000
             })
  end

  test "a type catalog row deleted before its LMDB copy is removed reads as absent",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: 1)

    assert_state_pruned(flow)
    assert_catalog_spilled(flow)

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 FerricStore.flow_get(flow.id, partition_key: flow.partition) == {:ok, nil}
               end,
               "the cancelled Flow should expire"
             )

    # Fail this shard's LMDB flushes so the spilled copy stays in LMDB while
    # apply deletes the member and re-creates the Flow.
    shard_index = flow.shard_index

    Ferricstore.FaultInjection.put_hook(fn
      :before_flow_lmdb_flush_write, %{shard_index: ^shard_index} -> {:error, :keydir_spill_test}
      _point, _metadata -> :ok
    end)

    try do
      drain_retention(System.system_time(:millisecond) + 10_000)
      assert {:ok, _spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
      revision = descriptor_revision(flow)

      now_ms = System.system_time(:millisecond)

      assert :ok =
               FerricStore.flow_create(flow.id,
                 type: flow.type,
                 state: "queued",
                 partition_key: flow.partition,
                 run_at_ms: now_ms,
                 now_ms: now_ms
               )

      assert descriptor_revision(flow) > revision,
             "re-creating the Flow must add a new type member, not reuse the deleted spilled one"
    after
      Ferricstore.FaultInjection.clear_hook()
    end

    assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)
    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
  end

  test "flushing a shard with spilled rows leaves no delete markers in the keydir",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: nil)

    assert_state_pruned(flow)
    assert_catalog_spilled(flow)

    # A live Flow on the same shard keeps its type catalog row in the keydir,
    # so the flush deletes a spillable row while the shard has spilled rows.
    live_id = unique("spill-live")
    live_state_key = Keys.state_key(live_id, flow.partition)
    live_catalog_key = Keys.type_catalog_member_key(flow.type, live_state_key)
    now_ms = System.system_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(live_id,
               type: flow.type,
               state: "queued",
               partition_key: flow.partition,
               run_at_ms: now_ms,
               now_ms: now_ms
             )

    assert Router.shard_for(flow.ctx, live_state_key) == flow.shard_index
    assert [{_key, value, _, _, _, _, _}] = :ets.lookup(flow.keydir, live_catalog_key)
    assert is_binary(value)

    assert :ok = Ferricstore.Store.Ops.Flush.flush(flow.ctx)

    assert [] =
             :ets.match_object(
               flow.keydir,
               {:_, nil, 0, :flow_state_deleted, :deleted, :_, :_}
             )

    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))

    refute KeydirSpill.present?(flow.ctx.name, flow.shard_index),
           "an emptied shard has no spilled rows left to look up"
  end

  test "a pruned terminal flow moves its retention cleanup members from the keydir to LMDB",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: nil, payload: "spill-payload")

    assert is_binary(flow.payload_ref)
    assert_state_pruned(flow)
    assert_spilled(flow, flow.member_key)

    assert {:ok, spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.member_key))
    assert {:ok, member} = LMDB.decode_value(spilled, 0)
    assert {:ok, {_index_key, owned_key}} = RetentionCleanupMember.decode(member)
    assert owned_key == flow.payload_ref
  end

  test "a pruned terminal flow moves its retention guard from the keydir to LMDB",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: nil)

    assert_state_pruned(flow)
    assert_spilled(flow, flow.guard_key)

    assert {:ok, spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.guard_key))
    assert {:ok, guard} = LMDB.decode_value(spilled, 0)
    assert {:ok, {version, {:state_enter_seq, _seq}}} = Ferricstore.TermCodec.decode(guard)

    assert {:ok, %{version: ^version}} =
             FerricStore.flow_get(flow.id, partition_key: flow.partition)
  end

  test "a policy catalog backfill planned before a flow finished is stale once its guard spilled",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = start_flow(isolated)
    %{ctx: ctx, shard_index: shard_index} = flow

    {:ok, source_token} = PolicyMigration.source_token(ctx, shard_index)
    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)
    step = &Router.flow_policy_catalog_backfill_step(ctx, shard_index, &1)

    base = %{run_token: run_token, source_token: source_token, candidates: [], done?: false}

    assert {:ok, _} =
             step.(
               %{base | candidates: []}
               |> Map.merge(%{expected_cursor: "", cursor: PolicyMigration.snapshot_cursor()})
             )

    :ok = PolicyMigration.snapshot_primary_keydir(ctx, shard_index, run_token, 256, 2_097_152)
    work_cursor = PolicyMigration.work_cursor(run_token)

    assert {:ok, _} =
             step.(
               Map.merge(base, %{
                 expected_cursor: PolicyMigration.snapshot_cursor(),
                 cursor: work_cursor
               })
             )

    assert {:ok, page} =
             PolicyMigration.backfill_page(ctx, shard_index, work_cursor, 256, 2_097_152)

    assert [candidate] =
             Enum.filter(
               page.candidates,
               &match?(%{kind: :state, state_key: state_key} when state_key == flow.state_key, &1)
             )

    # The planned record is the active Flow; it then finishes and its guard spills.
    flow = cancel_flow(isolated, flow)
    assert_state_pruned(flow)
    assert_spilled(flow, flow.guard_key)

    assert {:error, "ERR stale flow policy catalog record plan"} =
             step.(
               Map.merge(base, %{
                 expected_cursor: work_cursor,
                 cursor: page.cursor,
                 candidates: [candidate],
                 done?: page.done?
               })
             )
  end

  test "retention deletes the owned values of a spilled terminal flow", %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: 1, payload: "spill-payload")

    assert_state_pruned(flow)
    assert_spilled(flow, flow.member_key)
    expire_flow(flow)
    assert {:ok, ["spill-payload"]} = payload(flow)

    drain_retention(System.system_time(:millisecond) + 10_000)
    assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)

    assert {:ok, [nil]} = payload(flow),
           "retention must delete the payload the spilled cleanup member points to"

    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.member_key))
    assert [] = :ets.lookup(flow.keydir, flow.member_key)
  end

  test "after a restart, retention deletes the owned values of a spilled terminal flow",
       %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    # Restart inside the retention window: an expired value is not reloaded.
    flow = finish_flow(isolated, ttl_ms: 60_000, payload: "spill-payload")

    assert_state_pruned(flow)
    assert_spilled(flow, flow.member_key)

    flow = restart_as_new_process(isolated, flow)

    assert [] = :ets.lookup(flow.keydir, flow.member_key),
           "the spilled cleanup member should stay out of the keydir after a restart"

    assert {:ok, ["spill-payload"]} = payload(flow)

    drain_retention(System.system_time(:millisecond) + 120_000)
    assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)

    assert {:ok, [nil]} = payload(flow),
           "retention after a restart must still delete the payload of a spilled member"

    assert :not_found = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.member_key))
  end

  test "policy migration still migrates a spilled terminal flow", %{isolated: isolated} do
    Application.put_env(:ferricstore, :flow_keydir_spill, true)
    flow = finish_flow(isolated, ttl_ms: nil, state_meta: %{"version" => 1})

    assert_state_pruned(flow)
    assert_catalog_spilled(flow)

    assert {:ok, worker} =
             Ferricstore.Flow.PolicyMigrationWorker.start_link(
               instance_ctx: flow.ctx,
               name: :"keydir_spill_policy_worker_#{System.unique_integer([:positive])}",
               enabled: true,
               initial_delay_ms: 10,
               interval_ms: 50
             )

    try do
      assert {:ok, _policy} =
               FerricStore.flow_policy_set(flow.type, indexed_state_meta: "version")

      assert :ok =
               ShardHelpers.eventually(
                 fn ->
                   _ = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)

                   match?(
                     {:ok, %{migration_generation: 1}},
                     flow |> catalog_value() |> PolicyMigration.decode_catalog()
                   )
                 end,
                 "the policy migration should reach the spilled Flow",
                 100,
                 100
               )
    after
      GenServer.stop(worker)
    end

    assert {:ok, %{state_key: state_key}} =
             flow |> catalog_value() |> PolicyMigration.decode_catalog()

    assert state_key == flow.state_key
  end

  defp finish_flow(isolated, opts) do
    isolated |> start_flow(opts) |> then(&cancel_flow(isolated, &1, opts))
  end

  defp start_flow(isolated, opts \\ []) do
    id = unique("spill-flow")
    type = unique("spill-type")
    partition = unique("spill-partition")
    now_ms = System.system_time(:millisecond)

    assert :ok =
             FerricStore.flow_create(
               id,
               [
                 type: type,
                 state: "queued",
                 partition_key: partition,
                 run_at_ms: now_ms,
                 now_ms: now_ms
               ] ++
                 if(opts[:state_meta], do: [state_meta: opts[:state_meta]], else: []) ++
                 if(opts[:payload], do: [payload: opts[:payload]], else: [])
             )

    assert {:ok, %{payload_ref: payload_ref}} =
             FerricStore.flow_get(id, partition_key: partition)

    ctx = FerricStore.Instance.get(:default)
    state_key = Keys.state_key(id, partition)
    shard_index = Router.shard_for(ctx, state_key)

    lmdb_path =
      isolated.tmp_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> LMDB.path()

    assert :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count, 45_000)

    %{
      ctx: ctx,
      id: id,
      type: type,
      partition: partition,
      state_key: state_key,
      catalog_key: Keys.type_catalog_member_key(type, state_key),
      payload_ref: payload_ref,
      guard_key: Keys.retention_guard_key(id, partition),
      member_key: payload_ref && Keys.retention_cleanup_member_key(id, payload_ref, partition),
      shard_index: shard_index,
      keydir: elem(ctx.keydir_refs, shard_index),
      lmdb_path: lmdb_path
    }
  end

  defp cancel_flow(_isolated, flow, opts \\ []) do
    now_ms = System.system_time(:millisecond)

    assert {:ok, [%{id: id} = claimed]} =
             FerricStore.flow_claim_due(flow.type,
               partition_key: flow.partition,
               states: ["queued"],
               worker: "keydir-spill-test",
               limit: 1,
               now_ms: now_ms + 1
             )

    assert id == flow.id

    cancel_opts =
      [
        partition_key: flow.partition,
        lease_token: claimed.lease_token,
        fencing_token: claimed.fencing_token,
        now_ms: System.system_time(:millisecond)
      ] ++ if(opts[:ttl_ms], do: [ttl_ms: opts[:ttl_ms]], else: [])

    assert :ok = FerricStore.flow_cancel(flow.id, cancel_opts)

    if Keyword.get(opts, :flush, true) do
      assert :ok = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)
    end

    flow
  end

  defp assert_state_pruned(flow) do
    assert :ok =
             ShardHelpers.eventually(
               fn -> :ets.lookup(flow.keydir, flow.state_key) == [] end,
               "the terminal Flow state row should be pruned from the keydir"
             )
  end

  defp assert_catalog_spilled(flow), do: assert_spilled(flow, flow.catalog_key)

  defp assert_spilled(flow, key) do
    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 _ = LMDBWriter.flush_all(flow.ctx.name, flow.ctx.shard_count, 45_000)
                 :ets.lookup(flow.keydir, key) == []
               end,
               "#{inspect(key)} should leave the keydir"
             )

    assert {:ok, _spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(key))
  end

  defp payload(flow),
    do: FerricStore.flow_value_mget([flow.payload_ref], partition_key: flow.partition)

  defp expire_flow(flow) do
    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 FerricStore.flow_get(flow.id, partition_key: flow.partition) == {:ok, nil}
               end,
               "the cancelled Flow should expire"
             )
  end

  # A real process restart: no VM-local LMDB writer generations survive it.
  defp restart_as_new_process(%{tmp_dir: data_dir, server_started?: server_started?}, flow) do
    for app <- [:ferricstore_server, :ferricstore],
        Enum.any?(Application.started_applications(), &(elem(&1, 0) == app)) do
      _ = Application.stop(app)
    end

    Ferricstore.Raft.WARaftBackend.stop()

    for shard_index <- 0..(flow.ctx.shard_count - 1) do
      :persistent_term.erase(LMDBWriter.Registry.enqueue_seq_key(:default, shard_index))
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    ctx = FerricStore.Instance.get(:default)
    %{flow | ctx: ctx, keydir: elem(ctx.keydir_refs, flow.shard_index)}
  end

  defp catalog_value(flow) do
    case :ets.lookup(flow.keydir, flow.catalog_key) do
      [{_key, value, _, _, _, _, _}] when is_binary(value) ->
        value

      [] ->
        assert {:ok, spilled} = LMDB.get(flow.lmdb_path, KeydirSpill.key(flow.catalog_key))
        assert {:ok, value} = LMDB.decode_value(spilled, 0)
        value
    end
  end

  defp descriptor_revision(flow) do
    key = Keys.type_catalog_descriptor_key(flow.type)
    assert [{^key, value, _, _, _, _, _}] = :ets.lookup(flow.keydir, key)
    assert {:ok, %{membership_revision: revision}} = PolicyMigration.decode_type_descriptor(value)
    revision
  end

  defp drain_retention(now_ms), do: drain_retention(now_ms, nil, 64)

  defp drain_retention(_now_ms, _continuation, 0),
    do: flunk("retention cleanup did not finish within 64 pages")

  defp drain_retention(now_ms, continuation, remaining) do
    assert {:ok, page} =
             FerricStore.flow_retention_cleanup(
               limit: 10,
               now_ms: now_ms,
               continuation: continuation
             )

    if page.more?, do: drain_retention(now_ms, page.continuation, remaining - 1), else: :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)

  defp unique(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"
end
