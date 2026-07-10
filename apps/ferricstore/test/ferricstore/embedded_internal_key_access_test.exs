defmodule FerricStore.EmbeddedInternalKeyAccessTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.{CompoundKey, Router}

  @error "ERR access to internal Flow keys is not allowed"

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
