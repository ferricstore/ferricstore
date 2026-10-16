defmodule Ferricstore.Cluster.FeatureLevelTest do
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.Test.ShardHelpers

  setup do
    # Feature levels survive FLUSHDB, so raise them only on a throwaway data dir.
    isolated = ShardHelpers.setup_isolated_data_dir()
    on_exit(fn -> ShardHelpers.teardown_isolated_data_dir(isolated) end)
    {:ok, isolated: isolated, ctx: FerricStore.Instance.get(:default)}
  end

  test "feature-level markers are reserved internal keys" do
    assert FeatureLevel.supported() >= 1
    key = FeatureLevel.key(1)
    assert FeatureLevel.key?(key)
    refute FeatureLevel.key?("user:feature-level:1")
    assert InternalKey.reserved?(key)
    assert {:error, _reason} = InternalKey.authorize_public([key])
  end

  test "a fresh cluster is at level 0 on every shard", %{ctx: ctx} do
    assert {:ok, %{supported: supported, active: 0, shards: shards}} = FeatureLevel.status(ctx)
    assert supported == FeatureLevel.supported()
    assert shards == List.duplicate(0, ctx.shard_count)
    assert {:ok, %{active: 0}} = FerricStore.cluster_feature_level()
  end

  test "the public API raises the level" do
    assert {:ok, %{active: 1}} = FerricStore.raise_cluster_feature_level(1)
    assert {:ok, %{active: 1}} = FerricStore.cluster_feature_level()
  end

  test "CLUSTER.FEATURES reports the levels and CLUSTER.FEATURES.RAISE raises them", %{ctx: ctx} do
    zeros = Enum.map_join(0..(ctx.shard_count - 1), fn i -> "\r\nshard_#{i}: 0" end)
    ones = String.replace(zeros, ": 0", ": 1")
    supported = "supported: #{FeatureLevel.supported()}\r\n"

    assert Dispatcher.dispatch("CLUSTER.FEATURES", [], ctx) == supported <> "active: 0" <> zeros

    assert Dispatcher.dispatch("CLUSTER.FEATURES.RAISE", ["1"], ctx) ==
             supported <> "active: 1" <> ones

    assert Dispatcher.dispatch("CLUSTER.FEATURES", [], ctx) == supported <> "active: 1" <> ones
    assert {:ok, %{active: 1}} = FeatureLevel.status(ctx)
  end

  test "CLUSTER.FEATURES commands reject bad arguments", %{ctx: ctx} do
    above = Integer.to_string(FeatureLevel.supported() + 1)

    for {command, args} <- [
          {"CLUSTER.FEATURES", ["extra"]},
          {"CLUSTER.FEATURES.RAISE", []},
          {"CLUSTER.FEATURES.RAISE", ["1", "extra"]},
          {"CLUSTER.FEATURES.RAISE", ["one"]},
          {"CLUSTER.FEATURES.RAISE", ["0"]},
          {"CLUSTER.FEATURES.RAISE", [above]}
        ] do
      assert {:error, "ERR " <> _message} = Dispatcher.dispatch(command, args, ctx),
             "#{command} #{inspect(args)} should fail"
    end

    assert {:error, message} = Dispatcher.dispatch("CLUSTER.FEATURES.RAISE", [above], ctx)
    assert message =~ "supports up to #{FeatureLevel.supported()}"
    assert {:ok, %{active: 0}} = FeatureLevel.status(ctx)
  end

  test "raising writes the level to every shard, is idempotent and bounded", %{ctx: ctx} do
    assert {:ok, %{active: 1, shards: shards}} = FeatureLevel.raise(ctx, 1)
    assert shards == List.duplicate(1, ctx.shard_count)
    assert {:ok, %{active: 1}} = FeatureLevel.raise(ctx, 1)

    unsupported = FeatureLevel.supported() + 1

    assert {:error, {:unsupported_feature_level, ^unsupported, _supported}} =
             FeatureLevel.raise(ctx, unsupported)

    assert {:error, :invalid_feature_level} = FeatureLevel.raise(ctx, 0)
    assert {:error, :invalid_feature_level} = FeatureLevel.raise(ctx, "1")

    # Raising never lowers: asking for a lower level keeps the higher one.
    assert {:ok, %{active: 1}} = FeatureLevel.status(ctx)
  end

  test "raising is refused while a member does not support the level", %{ctx: ctx} do
    old_node = :"old-release@127.0.0.1"
    members = fn -> {:ok, [node(), old_node]} end

    below = fn
      member when member == node() -> {:ok, FeatureLevel.supported()}
      _old -> {:ok, 0}
    end

    assert {:error, {:members_below_feature_level, 1, [{^old_node, 0}]}} =
             FeatureLevel.raise(ctx, 1, members: members, member_supported: below)

    unreachable = fn
      member when member == node() -> {:ok, FeatureLevel.supported()}
      _old -> {:error, :noconnection}
    end

    assert {:error, {:feature_level_probe_failed, [{^old_node, :noconnection}]}} =
             FeatureLevel.raise(ctx, 1, members: members, member_supported: unreachable)

    assert {:ok, %{active: 0}} = FeatureLevel.status(ctx)
  end

  test "a node without the feature-level module counts as level 0" do
    assert {:ok, 0} =
             FeatureLevel.member_supported(:"no-such-node@127.0.0.1", fn _node ->
               {:error, {:exception, :undef, []}}
             end)
  end

  test "the level survives FLUSHDB and a restart", %{ctx: ctx, isolated: isolated} do
    assert {:ok, %{active: 1}} = FeatureLevel.raise(ctx, 1)
    assert :ok = FerricStore.set("user-key", "value")
    assert :ok = FerricStore.flushdb()
    assert {:ok, 0} = FerricStore.dbsize()
    assert {:ok, %{active: 1}} = FeatureLevel.status(ctx)

    assert :ok = ShardHelpers.restart_current_data_dir(isolated)
    ctx = FerricStore.Instance.get(:default)
    assert {:ok, %{active: 1}} = FeatureLevel.status(ctx)
  end

  test "a joining node below the active level is refused", %{ctx: ctx} do
    target = :"joining@127.0.0.1"
    old = fn _node -> {:ok, 0} end
    current = fn _node -> {:ok, FeatureLevel.supported()} end

    assert :ok = FeatureLevel.validate_join_target(target, ctx, member_supported: old)
    assert {:ok, %{active: 1}} = FeatureLevel.raise(ctx, 1)

    assert {:error, {:target_below_feature_level, ^target, 0, 1}} =
             FeatureLevel.validate_join_target(target, ctx, member_supported: old)

    assert :ok = FeatureLevel.validate_join_target(target, ctx, member_supported: current)
  end

  test "the cluster manager refuses a join it cannot verify once a level is active", %{ctx: ctx} do
    unreachable = :"unreachable-join-target@127.0.0.1"

    # At level 0 nothing is gated, so the first check to refuse an unreachable
    # target is the data format probe.
    assert {:error, {:data_format_probe_failed, ^unreachable, _reason}} =
             Ferricstore.Cluster.Manager.add_node(unreachable)

    assert {:ok, %{active: 1}} = FeatureLevel.raise(ctx, 1)

    assert {:error, {:feature_level_probe_failed, [{^unreachable, _reason}]}} =
             Ferricstore.Cluster.Manager.add_node(unreachable)
  end
end
