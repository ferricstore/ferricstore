defmodule FerricStore.EmbeddedInternalKeyAccessTest do
  use ExUnit.Case, async: false

  alias Ferricstore.ServerCatalog
  alias Ferricstore.Store.{CompoundKey, Router}

  @error "ERR access to internal keys is not allowed"

  defmodule CustomProtected do
    use FerricStore, shard_count: 1
  end

  setup do
    ctx = FerricStore.Instance.get(:default)
    digest = Base.url_encode64(:crypto.hash(:sha256, inspect(make_ref())), padding: false)
    reserved = "f:{f:#{digest}}:s:embedded-probe"
    reserved_hash = "f:{f:#{digest}}:s:embedded-hash"
    ordinary = "embedded-probe:#{System.unique_integer([:positive, :monotonic])}"

    on_exit(fn ->
      for key <- [reserved, reserved_hash, ordinary] do
        Router.delete(ctx, key)
        Router.compound_delete_prefix(ctx, key, CompoundKey.hash_prefix(key))
        Router.compound_delete(ctx, key, CompoundKey.type_key(key))
      end
    end)

    {:ok, ctx: ctx, reserved: reserved, reserved_hash: reserved_hash, ordinary: ordinary}
  end

  test "scalar reads and writes cannot access Flow internals", context do
    assert :ok = Router.put(context.ctx, context.reserved, "secret", 0)

    assert {:error, @error} = FerricStore.get(context.reserved)
    assert {:error, @error} = FerricStore.set(context.reserved, "forged")
    assert {:error, @error} = FerricStore.del(context.reserved)
    assert {:error, @error} = FerricStore.exists(context.reserved)
    assert "secret" == Router.get(context.ctx, context.reserved)
  end

  test "embedded bulk access rejects the whole request", context do
    assert {:error, @error} = FerricStore.mget([context.ordinary, context.reserved])

    assert {:error, @error} =
             FerricStore.mset(%{context.ordinary => "value", context.reserved => "forged"})

    assert nil == Router.get(context.ctx, context.ordinary)
    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "embedded data-structure origins cannot create compound state under Flow keys", context do
    assert {:error, @error} = FerricStore.hset(context.reserved_hash, %{"field" => "forged"})
    assert {:error, @error} = FerricStore.hget(context.reserved_hash, "field")

    assert nil ==
             Router.compound_get(
               context.ctx,
               context.reserved_hash,
               CompoundKey.type_key(context.reserved_hash)
             )
  end

  test "embedded direct access to physical compound keys is rejected", context do
    physical = "T:" <> context.ordinary

    assert {:error, @error} = FerricStore.set(physical, "hash")
    assert {:error, @error} = FerricStore.get(physical)
    assert nil == Router.get(context.ctx, physical)
  end

  test "embedded access cannot read or overwrite the durable server catalog", context do
    namespace = "security-probe-#{System.unique_integer([:positive, :monotonic])}"
    key = ServerCatalog.revision_key(namespace)
    encoded_revision = ServerCatalog.encode_revision(7)
    assert :ok = Router.put(context.ctx, key, encoded_revision, 0)
    on_exit(fn -> Router.delete(context.ctx, key) end)

    original = Router.get(context.ctx, key)
    assert original == encoded_revision

    assert {:error, @error} = FerricStore.get(key)
    assert {:error, @error} = FerricStore.set(key, "forged")
    assert {:error, @error} = FerricStore.del(key)
    assert original == Router.get(context.ctx, key)
  end

  test "Flow value hydration cannot be used as a generic key read", context do
    namespace = "flow-value-read-probe"
    catalog_key = ServerCatalog.revision_key(namespace)
    catalog_value = ServerCatalog.encode_revision(11)

    assert :ok = Router.put(context.ctx, context.ordinary, "ordinary-secret", 0)
    assert :ok = Router.put(context.ctx, catalog_key, catalog_value, 0)
    on_exit(fn -> Router.delete(context.ctx, catalog_key) end)

    assert {:ok, [nil, nil]} =
             FerricStore.flow_value_mget([context.ordinary, catalog_key])

    flow_id = "value-read-probe-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok =
             FerricStore.flow_create(flow_id,
               type: "security-probe",
               state: "queued",
               payload_ref: context.ordinary
             )

    assert {:ok, hydrated} = FerricStore.flow_get(flow_id, full: true)
    assert hydrated.payload_ref == context.ordinary
    assert hydrated.payload == nil
    assert hydrated.payload_missing
  end

  test "durable server catalog keys stay out of public enumeration and counts" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_catalog_visibility_#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn ->
      CustomProtected.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = CustomProtected.start_link(data_dir: data_dir, shard_count: 1)
    ctx = CustomProtected.__instance__()
    catalog_key = ServerCatalog.revision_key("visibility-probe")

    assert :ok = Router.put(ctx, catalog_key, ServerCatalog.encode_revision(3), 0)
    assert :ok = CustomProtected.set("ordinary", "value")

    assert {:ok, ["ordinary"]} = CustomProtected.keys()
    assert {:ok, 1} = CustomProtected.dbsize()
    assert 1 == Router.dbsize(ctx)
  end

  test "flushdb removes user data without deleting the durable server catalog" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_catalog_flush_#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn ->
      CustomProtected.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = CustomProtected.start_link(data_dir: data_dir, shard_count: 1)
    ctx = CustomProtected.__instance__()
    namespace = "flush-probe"

    catalog = %{
      ServerCatalog.entry_key(namespace, "subject") =>
        ServerCatalog.encode_entry(5, %{role: "admin"}),
      ServerCatalog.revision_key(namespace) => ServerCatalog.encode_revision(5),
      ServerCatalog.live_count_key(namespace) => ServerCatalog.encode_live_count(1)
    }

    Enum.each(catalog, fn {key, value} -> assert :ok = Router.put(ctx, key, value, 0) end)
    assert :ok = CustomProtected.set("ordinary", "value")

    assert :ok = CustomProtected.flushdb()
    assert {:ok, nil} = CustomProtected.get("ordinary")

    Enum.each(catalog, fn {key, value} ->
      assert value == Router.get(ctx, key)
    end)
  end

  test "custom flushdb deletes Flow state from the instance being flushed" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_flow_flush_#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn ->
      CustomProtected.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = CustomProtected.start_link(data_dir: data_dir, shard_count: 1)
    flow_id = "custom-flow-flush-#{System.unique_integer([:positive, :monotonic])}"

    assert :ok = CustomProtected.flow_create(flow_id, type: "flush-probe", state: "queued")
    assert {:ok, %{id: ^flow_id}} = CustomProtected.flow_get(flow_id)

    assert :ok = CustomProtected.flushdb()
    assert {:ok, nil} = CustomProtected.flow_get(flow_id)
  end

  test "custom flushdb deletes orphan compound rows from their observed shard" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_compound_flush_#{System.unique_integer([:positive, :monotonic])}"
      )

    on_exit(fn ->
      CustomProtected.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = CustomProtected.start_link(data_dir: data_dir, shard_count: 2)
    ctx = CustomProtected.__instance__()

    {parent, physical} =
      Enum.find_value(1..1_000, fn suffix ->
        parent = "compound-flush-parent-#{suffix}"
        physical = CompoundKey.hash_field(parent, "field")

        if Router.shard_for(ctx, parent) != Router.shard_for(ctx, physical) do
          {parent, physical}
        end
      end)

    assert :ok = Router.compound_put(ctx, parent, physical, "orphan", 0)
    assert "orphan" == Router.compound_get(ctx, parent, physical)

    assert :ok = CustomProtected.flushdb()
    assert nil == Router.compound_get(ctx, parent, physical)
  end

  test "embedded transactions and pipelines reject before any command executes", context do
    assert {:error, @error} =
             FerricStore.multi(fn tx ->
               tx
               |> FerricStore.Tx.set(context.ordinary, "value")
               |> FerricStore.Tx.set(context.reserved, "forged")
             end)

    assert {:error, @error} =
             FerricStore.pipeline(fn pipe ->
               pipe
               |> FerricStore.Pipe.set(context.ordinary, "value")
               |> FerricStore.Pipe.set(context.reserved, "forged")
             end)

    assert nil == Router.get(context.ctx, context.ordinary)
    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "embedded batch helpers reserve the namespace", context do
    assert {:error, @error} = FerricStore.batch_get([context.ordinary, context.reserved])

    assert {:error, @error} =
             FerricStore.batch_set([
               {context.ordinary, "value"},
               {context.reserved, "forged"}
             ])

    assert nil == Router.get(context.ctx, context.ordinary)
    assert nil == Router.get(context.ctx, context.reserved)
  end

  test "dedicated embedded Flow operations remain trusted", context do
    id = context.reserved

    assert :ok = FerricStore.flow_create(id, type: "security-probe", state: "queued")
    assert {:ok, %{id: ^id}} = FerricStore.flow_get(id)
  end

  test "custom embedded instances apply the same public boundary" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_internal_key_#{System.unique_integer([:positive, :monotonic])}"
      )

    digest = Base.url_encode64(:crypto.hash(:sha256, data_dir), padding: false)
    reserved = "f:{f:#{digest}}:s:custom-probe"

    on_exit(fn ->
      CustomProtected.stop()
      File.rm_rf(data_dir)
    end)

    assert {:ok, _pid} = CustomProtected.start_link(data_dir: data_dir, shard_count: 1)
    assert :ok = CustomProtected.set("ordinary", "value")
    assert {:ok, "value"} = CustomProtected.get("ordinary")
    assert {:error, @error} = CustomProtected.set(reserved, "forged")
    assert {:error, @error} = CustomProtected.get(reserved)
    assert {:error, @error} = CustomProtected.hset(reserved, %{"field" => "forged"})
    assert {:error, @error} = CustomProtected.bf_reserve(reserved, 0.01, 100)
    assert {:error, @error} = CustomProtected.cms_merge("ordinary", [reserved])
    assert {:error, @error} = CustomProtected.cms_merge(reserved, ["ordinary"])
    assert :ok = CustomProtected.flow_create(reserved, type: "security-probe", state: "queued")
    assert {:ok, ["ordinary"]} = CustomProtected.keys()
    assert {:ok, 1} = CustomProtected.dbsize()
  end
end
