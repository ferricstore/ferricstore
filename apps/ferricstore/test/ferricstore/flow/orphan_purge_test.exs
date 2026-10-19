defmodule Ferricstore.Flow.OrphanPurgeTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter, OrphanPurge, PolicyMigration}
  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  # A registered Flow keeps its state in the keydir or, once pruned or
  # hibernated, in LMDB. One whose state record is gone from both keeps its
  # registry marker, retention guard, type catalog entry and values for good:
  # retention starts from the state record. The purge removes what it left.

  @partition "orphan-purge"

  setup do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, ctx: FerricStore.Instance.get(:default)}
  end

  test "a dry run lists a flow whose state record is gone and changes nothing", %{ctx: ctx} do
    orphan = orphaned_flow!(ctx, "dry-run")
    healthy = completed_flow!(ctx, "dry-run-healthy")

    assert {:ok, %{dry_run?: true, orphans: orphans, purged: 0}} =
             FerricStore.flow_purge_orphans()

    found = Enum.map(orphans, & &1.state_key)
    assert orphan.state_key in found
    refute healthy.state_key in found

    assert {:ok, <<1>>} = Router.read_shard_value(ctx, orphan.shard_index, orphan.registry_key)
    assert {:ok, guard} = Router.read_shard_value(ctx, orphan.shard_index, orphan.guard_key)
    assert is_binary(guard)
  end

  test "purging waits for cluster feature level 3", %{ctx: ctx} do
    orphan = orphaned_flow!(ctx, "gated")

    assert {:error, {:feature_level_inactive, 3}} =
             FerricStore.flow_purge_orphans(dry_run: false)

    # Apply checks the level too, on the shard that applies the command.
    assert {:ok, found} = OrphanPurge.find(ctx, orphan.shard_index)

    assert {:error, "ERR flow orphan purge requires cluster feature level 3"} =
             Router.flow_purge_orphans(ctx, orphan.shard_index, %{orphans: found})

    assert {:ok, <<1>>} = Router.read_shard_value(ctx, orphan.shard_index, orphan.registry_key)
  end

  test "a purge removes what a lost flow left and keeps healthy flows", %{ctx: ctx} do
    orphan = orphaned_flow!(ctx, "purge")
    healthy = completed_flow!(ctx, "purge-healthy")
    assert {:ok, %{active: 3}} = FerricStore.raise_cluster_feature_level(3)

    assert {:ok, %{dry_run?: false, purged: purged}} =
             FerricStore.flow_purge_orphans(dry_run: false)

    assert purged >= 1
    assert :ok = LMDBWriter.flush_all(ctx.name, ctx.shard_count)
    path = lmdb_path(ctx, orphan.shard_index)
    keydir = elem(ctx.keydir_refs, orphan.shard_index)

    for key <-
          [orphan.registry_key, orphan.guard_key, orphan.svr_key, orphan.catalog_key] ++
            orphan.value_keys do
      assert [] == :ets.lookup(keydir, key), "#{inspect(key)} is still in the keydir"
      assert {:ok, nil} = Router.read_shard_value(ctx, orphan.shard_index, key)
    end

    for key <- [orphan.catalog_key, orphan.legacy_value_key] do
      assert :not_found == LMDB.get(path, key), "#{inspect(key)} is still in LMDB"
    end

    assert [] == query_index_rows(path, orphan.id)

    for ref <- orphan.shared_refs do
      count_key = Keys.shared_value_ref_count_key(ref, orphan.shard_index)
      assert {:ok, nil} = Router.read_shard_value(ctx, orphan.shard_index, count_key)
    end

    assert {:ok, %{id: id}} = FerricStore.flow_get(healthy.id, partition_key: @partition)
    assert id == healthy.id

    assert {:ok, %{dry_run?: false, purged: 0, orphans: []}} =
             FerricStore.flow_purge_orphans(dry_run: false)
  end

  test "the purge command leaves a flow whose retention guard changed", %{ctx: ctx} do
    orphan = orphaned_flow!(ctx, "guard")
    assert {:ok, %{active: 3}} = FerricStore.raise_cluster_feature_level(3)
    assert {:ok, found} = OrphanPurge.find(ctx, orphan.shard_index)
    assert %{} = entry = Enum.find(found, &(&1.state_key == orphan.state_key))

    assert {:ok, %{purged: 0, skipped: 1}} =
             Router.flow_purge_orphans(ctx, orphan.shard_index, %{
               orphans: [%{entry | guard: "another incarnation"}]
             })

    assert {:ok, <<1>>} = Router.read_shard_value(ctx, orphan.shard_index, orphan.registry_key)
  end

  test "the policy catalog backfill finishes once the lost flow is purged", %{ctx: ctx} do
    orphan = orphaned_flow!(ctx, "backfill")
    assert {:ok, %{active: 3}} = FerricStore.raise_cluster_feature_level(3)
    assert {:ok, %{purged: purged}} = FerricStore.flow_purge_orphans(dry_run: false)
    assert purged >= 1

    run_token = Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false)

    :ok =
      PolicyMigration.snapshot_primary_keydir(ctx, orphan.shard_index, run_token, 256, 1_048_576)

    assert {:ok, %{candidates: candidates}} =
             PolicyMigration.backfill_page(
               ctx,
               orphan.shard_index,
               PolicyMigration.work_cursor(run_token),
               256,
               2 * 1_024 * 1_024
             )

    refute Enum.any?(candidates, &match?(%{state_key: key} when key == orphan.state_key, &1))
    :ok = PolicyMigration.cleanup_snapshot(ctx, orphan.shard_index, run_token)
  end

  defp completed_flow!(ctx, prefix) do
    id = "#{prefix}-#{System.unique_integer([:positive, :monotonic])}"
    type = "orphan-purge-#{prefix}"

    assert :ok =
             FerricStore.flow_create(id,
               type: type,
               partition_key: @partition,
               payload: "payload-bytes",
               values: %{"order" => "order-bytes"},
               correlation_id: "correlation-" <> id,
               run_at_ms: 1_000,
               now_ms: 1_000
             )

    assert {:ok, [claimed]} =
             FerricStore.flow_claim_due(type,
               partition_key: @partition,
               worker: "worker",
               limit: 1,
               now_ms: 1_000,
               payload: false
             )

    assert :ok =
             FerricStore.flow_complete(id, claimed.lease_token,
               partition_key: @partition,
               fencing_token: claimed.fencing_token,
               result: "result-bytes",
               now_ms: 1_100
             )

    assert {:ok, record} = FerricStore.flow_get(id, partition_key: @partition)
    state_key = Keys.state_key(id, @partition)
    shard_index = Router.shard_for(ctx, state_key)
    assert :ok = LMDBWriter.flush(ctx.name, shard_index, 30_000)

    %{
      id: id,
      type: type,
      record: record,
      state_key: state_key,
      shard_index: shard_index,
      registry_key: Keys.registry_key(id, @partition),
      guard_key: Keys.retention_guard_key(id, @partition),
      svr_key: Keys.shared_value_ref_registry_key(id, @partition),
      catalog_key: Keys.type_catalog_member_key(type, state_key)
    }
  end

  # The state record is lost from the keydir and LMDB; everything else stays.
  # Flows from before cleanup members kept their values in LMDB alone, so the
  # Flow also gets such a value.
  defp orphaned_flow!(ctx, prefix) do
    flow = completed_flow!(ctx, prefix)
    path = lmdb_path(ctx, flow.shard_index)
    keydir = elem(ctx.keydir_refs, flow.shard_index)

    shared_refs =
      flow.record.value_refs
      |> Map.values()
      |> Enum.map(& &1.ref)
      |> Enum.filter(&Keys.shared_value_ref?/1)

    value_keys = [flow.record.payload_ref, flow.record.result_ref]
    legacy_value_key = Keys.value_key(flow.id, :error, 9, @partition)
    :ok = LMDB.write_batch(path, [{:put, legacy_value_key, LMDB.encode_value("legacy", 0)}])

    assert shared_refs != []
    assert Enum.all?(value_keys, &is_binary/1)
    assert {:ok, <<1>>} = Router.read_shard_value(ctx, flow.shard_index, flow.registry_key)
    assert {:ok, svr} = Router.read_shard_value(ctx, flow.shard_index, flow.svr_key)
    assert is_binary(svr)

    :ets.delete(keydir, flow.state_key)
    :ok = LMDB.write_batch(path, [{:delete, flow.state_key}])
    # Its query index rows outlive the lost state record they came from.
    assert [_ | _] = query_index_rows(path, flow.id)

    Map.merge(flow, %{
      shared_refs: shared_refs,
      value_keys: value_keys,
      legacy_value_key: legacy_value_key
    })
  end

  defp query_index_rows(path, id) do
    prefix = Ferricstore.Flow.LMDB.IndexCodec.query_index_global_prefix()
    assert {:ok, entries} = LMDB.prefix_entries(path, prefix, 100_000)

    for {key, _value} <- entries,
        Ferricstore.Flow.LMDB.IndexCodec.query_index_key_id(key) == {:ok, id},
        do: key
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
