defmodule Ferricstore.Store.CompoundMemberCatalogReadinessTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Shard.CompoundMemberIndex

  setup do
    keydir = :ets.new(:compound_member_catalog_keydir, [:set, :public])
    index = :ets.new(:compound_member_catalog_index, [:ordered_set, :public])
    %{keydir: keydir, index: index}
  end

  test "a partial unready catalog is never authoritative", %{keydir: keydir, index: index} do
    prefix = CompoundKey.set_prefix("partial")
    indexed_key = CompoundKey.set_member("partial", "indexed")
    missing_key = CompoundKey.set_member("partial", "missing")

    :ets.insert(keydir, {indexed_key, "1", 0, 0, 0, 0, 1})
    :ets.insert(keydir, {missing_key, "1", 0, 0, 0, 0, 1})

    assert :ok = CompoundMemberIndex.put(index, indexed_key)
    refute CompoundMemberIndex.ready?(index)

    state = %{keydir: keydir}

    assert :unavailable = CompoundMemberIndex.keys_for_prefix(index, prefix)
    assert :unavailable = CompoundMemberIndex.keys_for_prefix(index, prefix, 10)
    assert :unavailable = CompoundMemberIndex.any_live?(index, state, prefix)
    assert :unavailable = CompoundMemberIndex.count_live(index, state, prefix)
    assert :unavailable = CompoundMemberIndex.scan_entries(index, state, prefix)
    assert :unavailable = CompoundMemberIndex.scan_rows(index, state, prefix)

    assert :unavailable =
             CompoundMemberIndex.reduce_rows_while(index, state, prefix, [], fn row, rows ->
               {:cont, [row | rows]}
             end)

    assert :unavailable =
             CompoundMemberIndex.member_slice(
               index,
               state,
               prefix,
               "",
               1,
               {:replicated_apply, 1},
               %{}
             )

    assert :unavailable = CompoundMemberIndex.row_slice(index, state, prefix, 0, 1, 2)
    assert :unavailable = CompoundMemberIndex.scan_page(index, state, prefix, 0, 10, nil)
  end

  test "point mutations do not confer readiness on a replacement catalog", %{index: index} do
    member_key = CompoundKey.set_member("partial", "member")

    assert :ok = CompoundMemberIndex.put(index, member_key)
    refute CompoundMemberIndex.ready?(index)

    assert :ok = CompoundMemberIndex.delete(index, member_key)
    refute CompoundMemberIndex.ready?(index)
  end
end

defmodule Ferricstore.Store.CompoundMemberCatalogLifecycleTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.{Hash, Strings}
  alias Ferricstore.Store.{CompoundKey, Promotion}
  alias Ferricstore.Store.ETSTableHeir
  alias Ferricstore.Store.KeydirTableOwner
  alias Ferricstore.Store.Router

  alias Ferricstore.Store.Shard.{
    CompoundMemberIndex,
    CompoundRevisionIndex,
    LogicalKeyIndex,
    NamespaceUsageIndex
  }

  alias Ferricstore.Test.IsolatedInstance

  setup test_context do
    checkout_opts =
      [shard_count: 1, start_shards: false]
      |> maybe_put_promotion_threshold(test_context)

    ctx = IsolatedInstance.checkout(checkout_opts)
    {:ok, heir} = ETSTableHeir.start_link(name: KeydirTableOwner.table_heir_name(ctx))
    {:ok, owner} = KeydirTableOwner.start_link(instance_ctx: ctx)

    {:ok, _shard} =
      Ferricstore.Store.Shard.start_link(index: 0, data_dir: ctx.data_dir, instance_ctx: ctx)

    on_exit(fn ->
      IsolatedInstance.checkin(ctx)
      safe_stop(owner)
      safe_stop(heir)
    end)

    {:ok, ctx: ctx, owner: owner}
  end

  test "keydir owner crashes cannot destroy authoritative tables", %{ctx: ctx, owner: owner} do
    keydir = elem(ctx.keydir_refs, 0)
    catalog = CompoundMemberIndex.table_name(ctx.name, 0)
    revision = CompoundRevisionIndex.table_name(ctx.name, 0)
    {logical_keys, logical_slots} = LogicalKeyIndex.table_names(ctx.name, 0)
    {namespace_usage, namespace_usage_expiry} = NamespaceUsageIndex.table_names(ctx.name, 0)
    key = CompoundKey.set_member("catalog-owner-crash", "member")
    row = {key, "1", 0, 0, 0, 0, 1}

    :ets.insert(keydir, row)
    assert :ok = CompoundMemberIndex.put(catalog, key)
    keydir_tid = :ets.whereis(keydir)
    catalog_tid = :ets.whereis(catalog)
    revision_tid = :ets.whereis(revision)
    logical_keys_tid = :ets.whereis(logical_keys)
    logical_slots_tid = :ets.whereis(logical_slots)
    namespace_usage_tid = :ets.whereis(namespace_usage)
    namespace_usage_expiry_tid = :ets.whereis(namespace_usage_expiry)
    keydir_owner = :ets.info(keydir, :owner)

    Process.unlink(owner)
    monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, 5_000

    assert keydir_tid == :ets.whereis(keydir)
    assert catalog_tid == :ets.whereis(catalog)
    assert revision_tid == :ets.whereis(revision)
    assert logical_keys_tid == :ets.whereis(logical_keys)
    assert logical_slots_tid == :ets.whereis(logical_slots)
    assert namespace_usage_tid == :ets.whereis(namespace_usage)
    assert namespace_usage_expiry_tid == :ets.whereis(namespace_usage_expiry)
    assert [^row] = :ets.lookup(keydir, key)

    assert {:ok, [^key]} =
             CompoundMemberIndex.keys_for_prefix(
               catalog,
               CompoundKey.set_prefix("catalog-owner-crash")
             )

    {:ok, restarted_owner} = KeydirTableOwner.start_link(instance_ctx: ctx)
    on_exit(fn -> safe_stop(restarted_owner) end)

    assert keydir_owner == :ets.info(keydir, :owner)
    assert restarted_owner == :ets.info(catalog, :owner)
    assert restarted_owner == :ets.info(revision, :owner)
    assert restarted_owner == :ets.info(logical_keys, :owner)
    assert restarted_owner == :ets.info(logical_slots, :owner)
    assert restarted_owner == :ets.info(namespace_usage, :owner)
    assert restarted_owner == :ets.info(namespace_usage_expiry, :owner)
  end

  test "a restarted keydir heir is rearmed before a later owner crash", %{
    ctx: ctx,
    owner: owner
  } do
    catalog = CompoundMemberIndex.table_name(ctx.name, 0)
    catalog_tid = :ets.whereis(catalog)
    key = CompoundKey.set_member("catalog-heir-restart", "member")
    assert :ok = CompoundMemberIndex.put(catalog, key)

    heir_name = KeydirTableOwner.table_heir_name(ctx)
    old_heir = Process.whereis(heir_name)
    Process.unlink(old_heir)
    heir_monitor = Process.monitor(old_heir)
    Process.exit(old_heir, :kill)
    assert_receive {:DOWN, ^heir_monitor, :process, ^old_heir, :killed}, 5_000

    {:ok, restarted_heir} = ETSTableHeir.start_link(name: heir_name)
    on_exit(fn -> safe_stop(restarted_heir) end)
    assert_table_info_eventually(catalog, :heir, restarted_heir, 100)

    Process.unlink(owner)
    owner_monitor = Process.monitor(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 5_000

    assert catalog_tid == :ets.whereis(catalog)

    assert {:ok, [^key]} =
             CompoundMemberIndex.keys_for_prefix(
               catalog,
               CompoundKey.set_prefix("catalog-heir-restart")
             )

    {:ok, restarted_owner} = KeydirTableOwner.start_link(instance_ctx: ctx)
    on_exit(fn -> safe_stop(restarted_owner) end)
    assert_table_info_eventually(catalog, :owner, restarted_owner, 100)
  end

  test "a one-for-one shard restart cannot replace its authoritative member catalog", %{ctx: ctx} do
    redis_key = "catalog-restart:#{System.unique_integer([:positive])}"
    prefix = CompoundKey.set_prefix(redis_key)
    first = CompoundKey.set_member(redis_key, "first")
    second = CompoundKey.set_member(redis_key, "second")
    shard_name = elem(ctx.shard_names, 0)
    catalog = CompoundMemberIndex.table_name(ctx.name, 0)

    assert :ok = Router.compound_put(ctx, redis_key, first, "1", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "1", 0)
    assert {:ok, [^first, ^second]} = CompoundMemberIndex.keys_for_prefix(catalog, prefix)

    old_shard = Process.whereis(shard_name)
    old_catalog = :ets.whereis(catalog)
    catalog_owner = :ets.info(catalog, :owner)

    refute catalog_owner == old_shard
    GenServer.stop(old_shard, :normal, 5_000)

    {:ok, restarted_shard} =
      Ferricstore.Store.Shard.start_link(index: 0, data_dir: ctx.data_dir, instance_ctx: ctx)

    refute restarted_shard == old_shard

    assert old_catalog == :ets.whereis(catalog)
    assert catalog_owner == :ets.info(catalog, :owner)
    assert CompoundMemberIndex.ready?(catalog)
    assert {:ok, [^first, ^second]} = CompoundMemberIndex.keys_for_prefix(catalog, prefix)
  end

  defp assert_table_info_eventually(table, item, expected, 0) do
    assert :ets.info(table, item) == expected
  end

  defp assert_table_info_eventually(table, item, expected, attempts) do
    if :ets.info(table, item) == expected do
      :ok
    else
      Process.sleep(10)
      assert_table_info_eventually(table, item, expected, attempts - 1)
    end
  end

  @tag promotion_threshold: 1
  test "promoted hash enumeration remains complete after a standalone shard restart", %{ctx: ctx} do
    redis_key = "catalog-promoted-restart:#{System.unique_integer([:positive])}"
    type_key = CompoundKey.type_key(redis_key)
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")
    shard_name = elem(ctx.shard_names, 0)

    assert :ok = Router.compound_put(ctx, redis_key, type_key, "hash", 0)
    assert :ok = Router.compound_put(ctx, redis_key, first, "one", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "two", 0)

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        shard_name
        |> :sys.get_state()
        |> Map.fetch!(:promoted_instances)
        |> Map.has_key?(redis_key)
      end,
      "expected hash to be promoted before restart"
    )

    old_shard = Process.whereis(shard_name)
    GenServer.stop(old_shard, :normal, 5_000)

    {:ok, _restarted_shard} =
      Ferricstore.Store.Shard.start_link(index: 0, data_dir: ctx.data_dir, instance_ctx: ctx)

    assert 2 = Hash.handle_ast({:hlen, redis_key}, ctx)

    assert %{"first" => "one", "second" => "two"} =
             {:hgetall, redis_key}
             |> Hash.handle_ast(ctx)
             |> Enum.chunk_every(2)
             |> Map.new(fn [field, value] -> {field, value} end)
  end

  @tag promotion_threshold: 1
  test "deleting a promoted hash removes its durable instance before restart", %{ctx: ctx} do
    redis_key = "catalog-promoted-delete:#{System.unique_integer([:positive])}"
    type_key = CompoundKey.type_key(redis_key)
    first = CompoundKey.hash_field(redis_key, "first")
    second = CompoundKey.hash_field(redis_key, "second")
    shard_name = elem(ctx.shard_names, 0)

    assert :ok = Router.compound_put(ctx, redis_key, type_key, "hash", 0)
    assert :ok = Router.compound_put(ctx, redis_key, first, "one", 0)
    assert :ok = Router.compound_put(ctx, redis_key, second, "two", 0)

    Ferricstore.Test.ShardHelpers.eventually(
      fn ->
        shard_name
        |> :sys.get_state()
        |> Map.fetch!(:promoted_instances)
        |> Map.has_key?(redis_key)
      end,
      "expected hash to be promoted before deletion"
    )

    dedicated_path =
      shard_name
      |> :sys.get_state()
      |> Map.fetch!(:promoted_instances)
      |> Map.fetch!(redis_key)
      |> Map.fetch!(:path)

    assert File.dir?(dedicated_path)
    assert 1 = Strings.handle_ast({:del, [redis_key]}, ctx)

    shard_state = :sys.get_state(shard_name)
    refute Map.has_key?(shard_state.promoted_instances, redis_key)
    refute File.dir?(dedicated_path)
    assert [] = :ets.lookup(shard_state.keydir, Promotion.marker_key(redis_key))

    old_shard = Process.whereis(shard_name)
    GenServer.stop(old_shard, :normal, 5_000)

    {:ok, _restarted_shard} =
      Ferricstore.Store.Shard.start_link(index: 0, data_dir: ctx.data_dir, instance_ctx: ctx)

    assert 0 = Hash.handle_ast({:hlen, redis_key}, ctx)
    refute Map.has_key?(:sys.get_state(shard_name).promoted_instances, redis_key)
  end

  defp maybe_put_promotion_threshold(opts, %{promotion_threshold: threshold}),
    do: Keyword.put(opts, :promotion_threshold, threshold)

  defp maybe_put_promotion_threshold(opts, _test_context), do: opts

  defp safe_stop(pid) do
    try do
      GenServer.stop(pid, :normal, 5_000)
    catch
      :exit, {:noproc, _call} -> :ok
      :exit, :noproc -> :ok
      :exit, {:shutdown, _call} -> :ok
      :exit, :shutdown -> :ok
    end
  end
end
