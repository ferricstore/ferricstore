defmodule Ferricstore.Store.LogicalKeyDbsizeGuardTest do
  use ExUnit.Case, async: true

  @router_path Path.expand("../../../lib/ferricstore/store/router/part_09.ex", __DIR__)
  @impl_path Path.expand("../../../lib/ferricstore/impl.ex", __DIR__)
  @logical_index_path Path.expand(
                        "../../../lib/ferricstore/store/shard/logical_key_index.ex",
                        __DIR__
                      )
  @waraft_recovery_path Path.expand(
                          "../../../lib/ferricstore/raft/waraft_storage/sections/recovery.ex",
                          __DIR__
                        )
  @waraft_projection_path Path.expand(
                            "../../../lib/ferricstore/raft/waraft_storage/sections/segment_projection.ex",
                            __DIR__
                          )

  test "DBSIZE uses the logical catalog without enumerating public keys" do
    router_source = File.read!(@router_path)
    impl_source = File.read!(@impl_path)

    assert router_source =~ "LogicalKeyIndex.count_live("
    assert impl_source =~ "Router.dbsize(ctx)"

    refute impl_source =~ ~r/def dbsize\(ctx\) do\s+case keys\(ctx\) do/

    logical_index_source = File.read!(@logical_index_path)
    assert logical_index_source =~ "validated_slot_count("
    assert logical_index_source =~ "@expiry_count_key"
    refute logical_index_source =~ "defp do_count_live("
  end

  test "KEYS uses the validated logical catalog for every backend" do
    router_source = File.read!(@router_path)

    assert router_source =~ "LogicalKeyIndex.all_live("
    refute router_source =~ "defp live_keydir_keys("
  end

  test "RANDOMKEY weights shards by exact live logical counts" do
    router_source = File.read!(@router_path)

    assert router_source =~ "LogicalKeyIndex.count_live("
    assert router_source =~ "weighted_random_logical_key("
  end

  test "WARaft rebuild and segment projection keep the logical catalog exact" do
    recovery_source = File.read!(@waraft_recovery_path)
    projection_source = File.read!(@waraft_projection_path)

    assert recovery_source =~ "LogicalKeyIndex.rebuild("
    assert recovery_source =~ "logical_key_index_name: logical_key_index"
    assert projection_source =~ "logical_key_index: Map.get(sm_state, :logical_key_index_name)"
    assert projection_source =~ "logical_key_slots: Map.get(sm_state, :logical_key_slots_name)"
  end
end
