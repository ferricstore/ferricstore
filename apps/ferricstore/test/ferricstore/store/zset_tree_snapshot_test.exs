defmodule Ferricstore.Store.ZSetTreeSnapshotTest do
  @moduledoc """
  A node that installs a snapshot gets the tree files of tree-encoded sorted
  sets with it, and answers from them as the node that took it did.
  """
  use Ferricstore.Test.WARaftBackendCase, async: false

  import Ferricstore.Test.Eventually

  alias Ferricstore.Cluster.FeatureLevel
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Store.Promotion

  defp build_ctx(root) do
    FerricStore.Instance.build(:"zset_tree_snapshot_#{System.unique_integer([:positive])}",
      data_dir: root,
      shard_count: 1,
      query_index_provider: FerricStore.Flow.QueryIndexProvider.Disabled,
      max_memory_bytes: 256 * 1024 * 1024,
      keydir_max_ram: 64 * 1024 * 1024,
      hot_cache_max_value_size: 65_536,
      blob_side_channel_threshold_bytes: 256 * 1024,
      max_active_file_size: 64 * 1024 * 1024,
      read_sample_rate: 100,
      lfu_decay_time: 1,
      lfu_log_factor: 10
    )
  end

  defp cmd(ctx, name, args), do: Dispatcher.dispatch(name, args, ctx)

  defp tree_files(ctx, key) do
    ctx.data_dir
    |> Promotion.dedicated_path(0, :zset_tree, key)
    |> Path.join("*.tree")
    |> Path.wildcard()
  end

  test "an installed snapshot carries tree-encoded sorted sets", %{root: root} do
    source_root = Path.join(root, "source")
    target_root = Path.join(root, "target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source = build_ctx(source_root)
    assert :ok = WARaftBackend.start(source, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, %{active: 9}} = FeatureLevel.raise(source, 9)

    key = "snapshot:board"
    pairs = Enum.flat_map(1..200, fn i -> ["#{rem(i * 37, 101)}", "m#{i}"] end)
    assert 200 = cmd(source, "ZADD", [key | pairs])
    assert "20" = cmd(source, "ZINCRBY", [key, "10", "m3"])
    assert 2 = cmd(source, "ZREM", [key, "m1", "m2"])
    assert [_tree] = tree_files(source, key)

    expected = cmd(source, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    rank = cmd(source, "ZRANK", [key, "m150"])

    assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

    snapshot_path =
      Path.join([
        source_root,
        "waraft",
        "ferricstore_waraft_backend.1",
        "snapshot.#{index}.#{term}"
      ])

    assert File.dir?(snapshot_path)
    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(source.name)

    target = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)

    assert_eventually(fn ->
      assert ^expected = cmd(target, "ZRANGE", [key, "0", "-1", "WITHSCORES"])
    end)

    assert [_tree] = tree_files(target, key)
    assert 198 = cmd(target, "ZCARD", [key])
    assert ^rank = cmd(target, "ZRANK", [key, "m150"])
    assert "20" = cmd(target, "ZSCORE", [key, "m3"])

    FerricStore.Instance.cleanup(target.name)
  end
end
