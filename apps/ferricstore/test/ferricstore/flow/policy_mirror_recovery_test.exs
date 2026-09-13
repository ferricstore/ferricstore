defmodule Ferricstore.Flow.PolicyMirrorRecoveryTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.PolicyMigration
  alias Ferricstore.Flow.PolicyMirrorRecovery
  alias Ferricstore.Flow.PolicyAttributeCatalog
  alias Ferricstore.Flow.Query.SourceCatalog
  alias Ferricstore.Flow.RetryPolicy

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_policy_mirror_recovery_#{System.unique_integer([:positive, :monotonic])}"
      )

    Ferricstore.DataDir.ensure_layout!(data_dir, 1)
    keydir = :ets.new(:policy_mirror_recovery_keydir, [:set, :public])

    ctx = %{
      name: String.to_atom("policy_mirror_recovery_#{System.unique_integer([:positive])}"),
      data_dir: data_dir,
      shard_count: 1,
      keydir_refs: {keydir},
      max_value_size: 1_048_576,
      query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled
    }

    on_exit(fn ->
      lmdb_path = LMDB.path(Ferricstore.DataDir.shard_data_path(data_dir, 0))
      _ = LMDB.release(lmdb_path)

      if :ets.info(keydir) != :undefined do
        :ets.delete(keydir)
      end

      File.rm_rf!(data_dir)
    end)

    {:ok, ctx: ctx, keydir: keydir}
  end

  test "repairs policy, migration, catalog, and attribute mirrors from one paged source scan", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "recovery-type"
    state_key = Keys.state_key("recovery-flow", "recovery-partition")
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    policy_key = Keys.policy_key(type)
    descriptor_key = Keys.type_catalog_descriptor_key(type)
    job_key = Keys.policy_migration_job_key(type)
    marker_key = Keys.policy_migration_marker_key(type)
    attribute = "owner"
    count_key = Keys.policy_indexed_attribute_count_key(attribute)
    member_key = Keys.policy_indexed_attribute_member_key(attribute, type)
    revision_key = Keys.policy_indexed_attribute_revision_key(attribute)
    repair_key = Keys.policy_indexed_attribute_repair_key(attribute)
    backfill_key = Keys.policy_catalog_backfill_key(0)

    policy_value = RetryPolicy.encode_flow_policy(%{type: type, version: 1}, 2)
    descriptor_value = PolicyMigration.encode_type_descriptor(type, 7)
    job_value = PolicyMigration.encode_job(type, 3, 7, "version", :active)
    marker_value = PolicyMigration.encode_job(type, 3, 7, "version", :done)
    catalog_value = PolicyMigration.encode_catalog(type, state_key, 3)
    backfill_value = PolicyMigration.encode_backfill_progress("run", "source", <<0>>, :active)
    repair_value = PolicyAttributeCatalog.encode_repair_request(attribute)

    rows = [
      {policy_key, policy_value},
      {descriptor_key, descriptor_value},
      {job_key, job_value},
      {marker_key, marker_value},
      {catalog_key, catalog_value},
      {count_key, <<1::unsigned-big-64>>},
      {member_key, <<1>>},
      {revision_key, <<1::unsigned-big-64>>},
      {repair_key, repair_value},
      {backfill_key, backfill_value}
    ]

    Enum.each(rows, fn {key, value} ->
      true = :ets.insert(keydir, {key, value, 0, 0, :hot, 0, byte_size(value)})
    end)

    true =
      :ets.insert(
        keydir,
        {"user}:tc:1:not-a-flow-catalog", "ordinary-value", 0, 0, :hot, 0, 14}
      )

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, "unrelated", <<9>>}])

    assert {:ok, 10} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert {:ok, ^policy_value} = read_mirror(lmdb_path, policy_key)
    assert {:ok, ^descriptor_value} = read_mirror(lmdb_path, descriptor_key)
    assert {:ok, ^job_value} = read_mirror(lmdb_path, job_key)
    assert {:ok, ^marker_value} = read_mirror(lmdb_path, marker_key)
    assert {:ok, ^catalog_value} = read_mirror(lmdb_path, catalog_key)
    assert {:ok, <<1>>} = LMDB.get(lmdb_path, projection_key(catalog_key, 3))
    assert {:ok, ^state_key} = LMDB.get(lmdb_path, source_entry_key(catalog_key))
    assert {:ok, <<1::unsigned-big-64>>} = read_mirror(lmdb_path, count_key)
    assert {:ok, <<1>>} = read_mirror(lmdb_path, member_key)
    assert {:ok, <<1::unsigned-big-64>>} = read_mirror(lmdb_path, revision_key)
    assert {:ok, ^repair_value} = read_mirror(lmdb_path, repair_key)
    assert {:ok, ^backfill_value} = read_mirror(lmdb_path, backfill_key)
    assert {:ok, <<9>>} = LMDB.get(lmdb_path, "unrelated")
  end

  test "malformed policy source fails closed without writing the current page", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "corrupt-recovery-type"
    job_key = Keys.policy_migration_job_key(type)
    old_value = PolicyMigration.encode_job(type, 1, 1, nil, :active)
    bad_value = <<"not-a-policy-job">>

    true = :ets.insert(keydir, {job_key, bad_value, 0, 0, :hot, 0, byte_size(bad_value)})

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, job_key, LMDB.encode_value(old_value, 0)}])

    assert {:error, :corrupt_policy_migration_job} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert {:ok, ^old_value} = read_mirror(lmdb_path, job_key)
  end

  test "repairs a cold policy source from its physical locator without a shard process", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "cold-recovery-type"
    policy_key = Keys.policy_key(type)
    policy_value = RetryPolicy.encode_flow_policy(%{type: type, version: 1}, 2)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    log_path = Path.join(shard_path, "00000.log")

    assert {:ok, {offset, value_size}} =
             NIF.v2_append_record(log_path, policy_key, policy_value, 0)

    true = :ets.insert(keydir, {policy_key, nil, 0, 0, 0, offset, value_size})

    lmdb_path = LMDB.path(shard_path)

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert {:ok, ^policy_value} = read_mirror(lmdb_path, policy_key)
  end

  test "expired cold policy sources remove their mirrors without restoring payloads", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "expired-cold-recovery"
    key = Keys.policy_key(type)
    value = RetryPolicy.encode_flow_policy(%{type: type, version: 1}, 2)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    log_path = Path.join(shard_path, "00000.log")
    lmdb_path = LMDB.path(shard_path)

    assert {:ok, {offset, size}} = NIF.v2_append_record(log_path, key, value, 1)
    :ets.insert(keydir, {key, nil, 1, 0, 0, offset, size})
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, key, LMDB.encode_value(value, 0)}])

    assert {:ok, 1} = PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)
    assert :not_found = LMDB.get(lmdb_path, key)
  end

  test "uses the supplied keydir when the context has a stale source table", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "stale-keydir-recovery-type"
    policy_key = Keys.policy_key(type)
    policy_value = RetryPolicy.encode_flow_policy(%{type: type, version: 1}, 2)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    log_path = Path.join(shard_path, "00000.log")

    assert {:ok, {offset, value_size}} =
             NIF.v2_append_record(log_path, policy_key, policy_value, 0)

    true = :ets.insert(keydir, {policy_key, nil, 0, 0, 0, offset, value_size})

    stale_keydir = :ets.new(:policy_mirror_recovery_stale_keydir, [:set, :public])
    :ets.delete(stale_keydir)
    ctx = %{ctx | keydir_refs: {stale_keydir}}
    lmdb_path = LMDB.path(shard_path)

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert {:ok, ^policy_value} = read_mirror(lmdb_path, policy_key)
  end

  test "source tombstones remove an obsolete policy mirror", %{ctx: ctx, keydir: keydir} do
    type = "deleted-recovery-type"
    job_key = Keys.policy_migration_job_key(type)
    old_value = PolicyMigration.encode_job(type, 1, 1, nil, :active)

    true = :ets.insert(keydir, {job_key, nil, 0, 0, :deleted, 0, 0})

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = LMDB.path(shard_path)
    assert :ok = LMDB.write_batch(lmdb_path, [{:put, job_key, LMDB.encode_value(old_value, 0)}])

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert :not_found = LMDB.get(lmdb_path, job_key)
  end

  test "bounded cleanup removes absent policy members but preserves source members", %{
    ctx: ctx,
    keydir: keydir
  } do
    attribute = "owner"
    live_type = "live-member-recovery"
    live_key = Keys.policy_indexed_attribute_member_key(attribute, live_type)

    stale_keys =
      for i <- 1..513 do
        type = "stale-member-recovery-#{i}"
        Keys.policy_indexed_attribute_member_key(attribute, type)
      end

    live_value = <<1>>
    live_encoded = LMDB.encode_value(live_value, 0)
    :ets.insert(keydir, {live_key, live_value, 0, 0, :hot, 0, byte_size(live_value)})

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = LMDB.path(shard_path)

    assert :ok =
             LMDB.write_batch(
               lmdb_path,
               Enum.map(stale_keys, &{:put, &1, live_encoded}) ++ [{:put, live_key, live_encoded}]
             )

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert {:ok, ^live_encoded} = LMDB.get(lmdb_path, live_key)

    assert {:ok, 1} =
             LMDB.prefix_count(lmdb_path, Keys.policy_indexed_attribute_member_prefix(attribute))

    assert {:ok, [{^live_key, ^live_encoded}]} =
             LMDB.prefix_entries(
               lmdb_path,
               Keys.policy_indexed_attribute_member_prefix(attribute),
               1
             )

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)
  end

  test "catalog tombstones remove primary, type, and source-catalog mirrors", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "deleted-catalog-type"
    state_key = Keys.state_key("deleted-catalog-flow", "deleted-catalog-partition")
    catalog_key = Keys.type_catalog_member_key(type, state_key)
    catalog_value = PolicyMigration.encode_catalog(type, state_key, 5)
    projection_key = projection_key(catalog_key, 5)
    source_key = source_entry_key(catalog_key)

    true = :ets.insert(keydir, {catalog_key, nil, 0, 0, :deleted, 0, 0})

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    lmdb_path = LMDB.path(shard_path)

    assert :ok =
             LMDB.write_batch(lmdb_path, [
               {:put, catalog_key, LMDB.encode_value(catalog_value, 0)},
               {:put, projection_key, <<1>>},
               {:put, source_key, state_key}
             ])

    assert {:ok, 1} =
             PolicyMirrorRecovery.reconcile_shard(lmdb_path, keydir, shard_path, 0, ctx)

    assert :not_found = LMDB.get(lmdb_path, catalog_key)
    assert :not_found = LMDB.get(lmdb_path, projection_key)
    assert :not_found = LMDB.get(lmdb_path, source_key)
  end

  test "replacing a catalog generation removes the obsolete projection", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "generation-recovery"
    state_key = Keys.state_key("generation-flow", "generation-partition")
    key = Keys.type_catalog_member_key(type, state_key)
    old_value = PolicyMigration.encode_catalog(type, state_key, 1)
    new_value = PolicyMigration.encode_catalog(type, state_key, 2)
    old_projection = Keys.policy_catalog_projection_key(type, key, 1)
    new_projection = Keys.policy_catalog_projection_key(type, key, 2)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = LMDB.path(shard_path)

    :ets.insert(keydir, {key, new_value, 0, 0, :hot, 0, byte_size(new_value)})

    assert :ok =
             LMDB.write_batch(path, [
               {:put, key, LMDB.encode_value(old_value, 0)},
               {:put, old_projection, <<1>>}
             ])

    for _ <- 1..2 do
      assert {:ok, 1} = PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)
      assert :not_found = LMDB.get(path, old_projection)
      assert {:ok, <<1>>} = LMDB.get(path, new_projection)
      assert {:ok, ^new_value} = read_mirror(path, key)
    end
  end

  test "policy repair traverses multiple bounded pages", %{ctx: ctx, keydir: keydir} do
    rows =
      for i <- 1..1_025 do
        type = "paged-policy-#{i}"
        key = Keys.policy_migration_job_key(type)
        value = PolicyMigration.encode_job(type, 1, 1, nil, :active)
        :ets.insert(keydir, {key, value, 0, 0, :hot, 0, byte_size(value)})
        {key, value}
      end

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = LMDB.path(shard_path)
    assert {:ok, 1_025} = PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)
    for {key, value} <- rows, do: assert({:ok, ^value} = read_mirror(path, key))
  end

  test "a missing non-expired cold source preserves existing catalog mirrors", %{
    ctx: ctx,
    keydir: keydir
  } do
    type = "missing-cold-catalog"
    state_key = Keys.state_key("missing-cold-flow", "missing-cold-partition")
    key = Keys.type_catalog_member_key(type, state_key)
    value = PolicyMigration.encode_catalog(type, state_key, 1)
    projection = Keys.policy_catalog_projection_key(type, key, 1)
    source_key = source_entry_key(key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = LMDB.path(shard_path)

    root =
      Path.join([ctx.data_dir, "waraft", "ferricstore_waraft_backend.1", "apply_projection_log"])

    assert :ok =
             :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
               to_charlist(root),
               [{{:raft_log_pos, 1, 0}, [{"unrelated", "value", 0}]}]
             )

    :ets.insert(keydir, {key, nil, 0, 0, {:waraft_apply_projection, 1}, 0, 0})

    assert :ok =
             LMDB.write_batch(path, [
               {:put, key, LMDB.encode_value(value, 0)},
               {:put, projection, <<1>>},
               {:put, source_key, state_key}
             ])

    assert {:error, {:policy_mirror_source_missing, ^key}} =
             PolicyMirrorRecovery.reconcile_shard(path, keydir, shard_path, 0, ctx)

    assert {:ok, ^value} = read_mirror(path, key)
    assert {:ok, <<1>>} = LMDB.get(path, projection)
    assert {:ok, ^state_key} = LMDB.get(path, source_key)
  end

  defp read_mirror(path, key) do
    with {:ok, encoded} <- LMDB.get(path, key),
         {:ok, value} <- LMDB.decode_value(encoded, 0) do
      {:ok, value}
    else
      other -> other
    end
  end

  defp projection_key(catalog_key, generation) do
    {:ok, descriptor_key} = Keys.type_catalog_descriptor_key_from_member(catalog_key)
    <<"f:{f}:td:1:", type_digest::binary>> = descriptor_key

    Keys.policy_catalog_projection_global_prefix() <>
      type_digest <> ":" <> <<generation::unsigned-big-64, catalog_key::binary>>
  end

  defp source_entry_key(catalog_key), do: SourceCatalog.entry_prefix() <> catalog_key
end
