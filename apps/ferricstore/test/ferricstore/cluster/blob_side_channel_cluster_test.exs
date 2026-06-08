defmodule Ferricstore.Cluster.BlobSideChannelClusterTest do
  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :blob_side_channel

  alias Ferricstore.Test.ClusterHelper

  @shards 2
  @large_value_size 300 * 1024

  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    :ok
  end

  @tag timeout: 180_000
  test "joined node reads a blob-backed value copied from one-node Raft baseline" do
    source = ClusterHelper.start_node(shards: @shards)
    on_exit(fn -> ClusterHelper.stop_node(source) end)

    key = "blob:join:large"
    payload = :binary.copy("J", @large_value_size)

    assert :ok = remote_put(source, key, payload)
    assert payload == remote_get(source, key)
    assert remote_blob_file_count(source) > 0

    target = ClusterHelper.start_node(shards: @shards)
    on_exit(fn -> ClusterHelper.stop_node(target) end)

    assert :ok = join_cluster(target, source)

    eventually(fn ->
      assert payload == remote_get(target, key)
      assert remote_blob_file_count(target) > 0
    end)
  end

  @tag timeout: 180_000
  test "multi-member Raft writes materialize large values on peers and survive leader failover" do
    nodes = ClusterHelper.start_cluster(3, shards: @shards)
    on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

    [writer | _] = nodes
    key = "blob:cluster:large"
    payload = :binary.copy("M", @large_value_size)

    assert :ok = remote_put(writer.name, key, payload)

    Enum.each(nodes, fn node ->
      eventually_blob_materialized(node.name, key, payload)
    end)

    shard = remote_shard_for(writer.name, key)
    {_killed, remaining} = ClusterHelper.kill_leader(nodes, shard)

    assert :ok = ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 30_000)

    Enum.each(remaining, fn node ->
      eventually(fn -> assert payload == remote_get(node.name, key) end)
    end)
  end

  defp remote_put(node, key, value) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, value, 0], 30_000)
  end

  defp remote_get(node, key) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :get, [ctx, key], 30_000)
  end

  defp remote_shard_for(node, key) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :shard_for, [ctx, key], 10_000)
  end

  defp remote_blob_file_count(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)

    legacy_files = Path.wildcard(Path.join([ctx.data_dir, "blob", "shard_*", "*", "*.blob"]))

    segment_files =
      Path.wildcard(Path.join([ctx.data_dir, "blob", "shard_*", "segments", "*.bloblog"]))

    length(legacy_files) + length(segment_files)
  end

  defp eventually_blob_materialized(node, key, payload, attempts \\ 120)

  defp eventually_blob_materialized(node, key, payload, attempts) when attempts > 0 do
    status = blob_status(node, key, payload)

    if status.value_matches? and status.blob_file_count > 0 do
      :ok
    else
      Process.sleep(250)
      eventually_blob_materialized(node, key, payload, attempts - 1)
    end
  end

  defp eventually_blob_materialized(node, key, payload, 0) do
    status = blob_status(node, key, payload)

    flunk(
      "blob value was not materialized on #{node}: " <>
        inspect(%{
          value_matches?: status.value_matches?,
          value_size: status.value_size,
          blob_file_count: status.blob_file_count,
          threshold: status.threshold,
          data_dir: status.data_dir
        })
    )
  end

  defp blob_status(node, key, payload) do
    value = remote_get(node, key)
    count = remote_blob_file_count(node)
    ctx = :erpc.call(node_name(node), FerricStore.Instance, :get, [:default], 10_000)

    %{
      value_matches?: value == payload,
      value_size: if(is_binary(value), do: byte_size(value), else: value),
      blob_file_count: count,
      threshold: ctx.blob_side_channel_threshold_bytes,
      data_dir: ctx.data_dir
    }
  end

  defp join_cluster(new_node, existing_node) do
    :erpc.call(
      node_name(existing_node),
      Ferricstore.Cluster.Manager,
      :add_node,
      [node_name(new_node)],
      120_000
    )
  end

  defp eventually(fun) do
    Ferricstore.Test.ShardHelpers.eventually(fun, "blob cluster condition", 120, 250)
  end

  defp node_name(%{name: name}), do: name
  defp node_name(name) when is_atom(name), do: name
end
