defmodule FerricstoreServer.Health.Dashboard.InternalKeyVisibilityTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router
  alias FerricstoreServer.Health.Dashboard.Data.KV

  setup do
    ctx = FerricStore.Instance.get(:default)
    digest = Base.url_encode64(:crypto.hash(:sha256, inspect(make_ref())), padding: false)
    reserved = "f:{f:#{digest}}:s:dashboard-probe"
    physical = "T:dashboard-probe:#{System.unique_integer([:positive, :monotonic])}"
    ordinary = "dashboard-probe:#{System.unique_integer([:positive, :monotonic])}"

    on_exit(fn ->
      Router.delete(ctx, reserved)
      Router.delete(ctx, physical)
      Router.delete(ctx, ordinary)
    end)

    {:ok, ctx: ctx, reserved: reserved, physical: physical, ordinary: ordinary}
  end

  test "exact lookup rejects Flow and physical keys before exposing metadata", context do
    assert :ok = Router.put(context.ctx, context.reserved, "secret", 0)
    assert :ok = Router.put(context.ctx, context.physical, "hash", 0)

    for key <- [context.reserved, context.physical] do
      page = KV.collect_keyspace_page(%{"key" => key, "include_internal" => "1"})
      assert page.rows == []

      assert page.inspected == %{
               key: key,
               found?: false,
               type: "none",
               ttl: "-",
               size: "-",
               location: "-",
               shard: "-"
             }
    end
  end

  test "ordinary exact lookup remains available", context do
    assert :ok = Router.put(context.ctx, context.ordinary, "value", 0)

    page = KV.collect_keyspace_page(%{"key" => context.ordinary})
    assert page.inspected.found?
    assert page.inspected.key == context.ordinary
  end

  test "prefix sampling does not count reserved storage keys", context do
    before_count = prefix_count(KV.collect_prefixes_page(), "f")
    assert :ok = Router.put(context.ctx, context.reserved, "secret", 0)
    after_count = prefix_count(KV.collect_prefixes_page(), "f")

    assert after_count == before_count
  end

  test "sampled dashboard traversal has a global scan cap" do
    keydir = :keydir_0
    marker = "T:dashboard-scan-#{System.unique_integer([:positive, :monotonic])}:"

    keys =
      Enum.map(1..10_100, fn index ->
        key = marker <> Integer.to_string(index)
        {key, "hash", 0, 0, 0, 0, 4}
      end)

    :ets.insert(keydir, keys)

    on_exit(fn ->
      Enum.each(keys, fn {key, _value, _exp, _lfu, _fid, _off, _size} ->
        :ets.delete(keydir, key)
      end)
    end)

    page =
      KV.collect_keyspace_page(%{
        "prefix" => "dashboard-prefix-that-does-not-exist",
        "include_internal" => "1",
        "limit" => "1"
      })

    assert page.total_sampled <= 10_000
  end

  test "prefix sampling uses bounded ETS continuations instead of a full fold" do
    source_path =
      Path.expand(
        "../../../../lib/ferricstore_server/health/dashboard/data/kv.ex",
        __DIR__
      )

    source = File.read!(source_path)
    refute source =~ ":ets.foldl"
    assert source =~ "@keyspace_dashboard_max_scan"
  end

  defp prefix_count(page, prefix) do
    case Enum.find(page.prefixes, &(&1.prefix == prefix)) do
      nil -> 0
      row -> row.keys
    end
  end
end
