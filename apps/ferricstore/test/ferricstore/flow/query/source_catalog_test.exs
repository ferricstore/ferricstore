defmodule Ferricstore.Flow.Query.SourceCatalogTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.{Keys, LMDB, PolicyMigration}
  alias Ferricstore.Flow.Query.SourceCatalog

  setup do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_source_catalog_#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn -> File.rm_rf!(data_dir) end)
    %{ctx: %{data_dir: data_dir, shard_count: 1}}
  end

  test "uses a bounded collision-free key while preserving the full Flow state key" do
    state_key = Keys.state_key(String.duplicate("r", 60_000), "tenant-a")
    catalog_key = Keys.type_catalog_member_key("invoice", state_key)

    assert {:ok, {:put, entry_key, ^state_key}} = SourceCatalog.put_op(catalog_key, state_key)
    assert byte_size(entry_key) <= 511
    assert String.starts_with?(entry_key, SourceCatalog.entry_prefix())
    assert {:ok, ^catalog_key, ^state_key} = SourceCatalog.decode_entry(entry_key, state_key)
    assert {:ok, {:delete, ^entry_key}} = SourceCatalog.delete_op(catalog_key)
  end

  test "rejects a catalog member that does not own its state key" do
    first = Keys.state_key("first", "tenant-a")
    second = Keys.state_key("second", "tenant-a")
    catalog_key = Keys.type_catalog_member_key("invoice", first)

    assert {:error, :invalid_query_source_catalog_entry} =
             SourceCatalog.put_op(catalog_key, second)
  end

  test "decodes the global exact-type projection without trusting its components" do
    state_key = Keys.state_key("candidate", "tenant-a")
    catalog_key = Keys.type_catalog_member_key("invoice", state_key)
    projection_key = Keys.policy_catalog_projection_key("invoice", catalog_key, 7)

    assert {:ok, %{catalog_key: ^catalog_key, migration_generation: 7, type_digest: type_digest}} =
             Keys.decode_policy_catalog_projection_key(projection_key)

    assert byte_size(type_digest) == 43

    malformed =
      Keys.policy_catalog_projection_global_prefix() <>
        String.duplicate("x", byte_size(projection_key))

    assert :error = Keys.decode_policy_catalog_projection_key(malformed)
  end

  test "bootstrap cannot resurrect a catalog member deleted after hydration", %{ctx: ctx} do
    state_key = Keys.state_key("deleted", "tenant-a")
    catalog_key = Keys.type_catalog_member_key("invoice", state_key)
    projection_key = Keys.policy_catalog_projection_key("invoice", catalog_key, 1)
    catalog = PolicyMigration.encode_catalog("invoice", state_key, 1)
    path = lmdb_path(ctx)

    assert {:ok, {:put, source_key, ^state_key}} = SourceCatalog.put_op(catalog_key, state_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, catalog_key, LMDB.encode_value(catalog, 0)},
               {:put, projection_key, <<1>>}
             ])

    write_batch = fn ^path, ops ->
      assert :ok =
               LMDB.write_batch(path, [
                 {:delete, catalog_key},
                 {:delete, projection_key},
                 {:delete, source_key}
               ])

      LMDB.write_batch(path, ops)
    end

    assert {:error, {:compare_failed, ^catalog_key}} =
             SourceCatalog.bootstrap_page(ctx, 0, 1, 1_024 * 1_024, write_batch_fun: write_batch)

    assert :not_found = LMDB.get(path, source_key)
  end

  test "bootstrap cannot overwrite a source row replaced after collision validation", %{ctx: ctx} do
    state_key = Keys.state_key("expected", "tenant-a")
    conflicting_state_key = Keys.state_key("conflicting", "tenant-a")
    catalog_key = Keys.type_catalog_member_key("invoice", state_key)
    projection_key = Keys.policy_catalog_projection_key("invoice", catalog_key, 1)
    catalog = PolicyMigration.encode_catalog("invoice", state_key, 1)
    path = lmdb_path(ctx)

    assert {:ok, {:put, source_key, ^state_key}} = SourceCatalog.put_op(catalog_key, state_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, catalog_key, LMDB.encode_value(catalog, 0)},
               {:put, projection_key, <<1>>}
             ])

    write_batch = fn ^path, ops ->
      assert :ok = LMDB.write_batch(path, [{:put, source_key, conflicting_state_key}])
      LMDB.write_batch(path, ops)
    end

    assert {:error, {:compare_failed, ^source_key}} =
             SourceCatalog.bootstrap_page(ctx, 0, 1, 1_024 * 1_024, write_batch_fun: write_batch)

    assert {:ok, ^conflicting_state_key} = LMDB.get(path, source_key)
  end

  test "bootstrap rejects a corrupt durable completion marker", %{ctx: ctx} do
    path = lmdb_path(ctx)
    assert :ok = LMDB.write_batch(path, [{:put, <<0, "fqsc:c">>, "not-complete"}])

    assert {:error, :corrupt_query_source_catalog_complete} =
             SourceCatalog.bootstrap_page(ctx, 0, 1, 1_024)
  end

  test "bootstrap counts the durable completion marker against the page budget", %{ctx: ctx} do
    parent = self()

    write_batch = fn _path, _ops ->
      send(parent, :unexpected_write)
      :ok
    end

    assert {:error, :query_source_catalog_page_too_large} =
             SourceCatalog.bootstrap_page(ctx, 0, 1, 1, write_batch_fun: write_batch)

    refute_received :unexpected_write
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end
end
