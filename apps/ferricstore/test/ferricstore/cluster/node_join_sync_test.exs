Code.require_file("node_join_sync_test/sections/part_01.exs", __DIR__)
Code.require_file("node_join_sync_test/sections/part_02.exs", __DIR__)
defmodule Ferricstore.Cluster.NodeJoinSyncTest do
  @moduledoc """
  Tests that a new node joining the cluster receives a complete, consistent
  copy of all data — even while writes continue during the sync.

  Validates:
    1. Writes continue on the cluster while a new node is syncing
    2. After sync completes, the new node has all data
    3. Data directory checksums are identical across all nodes
    4. No writes are lost during the sync process
    5. The new node can serve reads for all keys (including those written during sync)

  Requires: multi-node Raft (Phase 1) + ClusterManager + DataSync
  """

  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag :node_join

  alias Ferricstore.Test.ClusterHelper

  @shards 2

  # Skip if :peer module not available (OTP < 25)
  setup_all do
    unless ClusterHelper.peer_available?() do
      raise "OTP 25+ required for :peer module"
    end

    # Kill any orphan peer processes from previous test runs
    cleanup_orphan_peers()
    :ok
  end

  # Clean state before each test — kill orphan peers, remove temp dirs
  setup do
    cleanup_orphan_peers()
    :ok
  end

  use Ferricstore.Cluster.NodeJoinSyncTest.Sections.Part01

  use Ferricstore.Cluster.NodeJoinSyncTest.Sections.Part02

  defp cleanup_orphan_peers do
    # Stop any peers registered in the ETS table (from start_node)
    if :ets.whereis(:ferricstore_solo_peers) != :undefined do
      :ets.tab2list(:ferricstore_solo_peers)
      |> Enum.each(fn {name, peer_pid, _dir} ->
        try do
          :peer.stop(peer_pid)
        catch
          _, _ -> :ok
        end

        :ets.delete(:ferricstore_solo_peers, name)
      end)
    end

    # Kill any lingering peer nodes via erlang:halt (forceful)
    ferric_nodes =
      Node.list()
      |> Enum.filter(fn n -> n |> Atom.to_string() |> String.contains?("ferric_") end)

    Enum.each(ferric_nodes, fn n ->
      try do
        :erpc.call(n, :erlang, :halt, [0], 2_000)
      catch
        _, _ -> :ok
      end

      Node.disconnect(n)
    end)

    if ferric_nodes != [] do
      Enum.each(1..30, fn _ ->
        remaining =
          Node.list()
          |> Enum.filter(fn n -> n |> Atom.to_string() |> String.contains?("ferric_") end)

        if remaining != [], do: Process.sleep(100)
      end)
    end

    # Clean temp dirs created by peer nodes. Never remove the live default
    # instance data dir: earlier cluster tests may restart the local app into a
    # temp dir with a similar prefix, and deleting it hides real marker/state
    # invariants from later tests.
    current_data_dir =
      case FerricStore.Instance.get(:default) do
        %{data_dir: data_dir} when is_binary(data_dir) -> Path.expand(data_dir)
        _ -> nil
      end

    ["ferricstore_cluster_*", "ferricstore_solo_*", "ferricstore_clone_*"]
    |> Enum.flat_map(fn pattern -> Path.wildcard(Path.join(System.tmp_dir!(), pattern)) end)
    |> Enum.reject(fn path -> current_data_dir && Path.expand(path) == current_data_dir end)
    |> Enum.each(&File.rm_rf/1)
  end










  # ---------------------------------------------------------------------------
  # Helpers — these call into :peer nodes via :erpc
  # ---------------------------------------------------------------------------

  # Extract node name from map or pass through atom
  defp node_name(%{name: name}), do: name
  defp node_name(name) when is_atom(name), do: name

  defp write_keys(node, prefix, range) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)

    Enum.map(range, fn i ->
      key = "#{prefix}_#{i}"
      :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{i}", 0], 10_000)
      key
    end)
  end

  defp write_key(node, key, value) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, value, 0], 10_000)
  end

  defp read_key(node, key) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default], 10_000)
    :erpc.call(n, Ferricstore.Store.Router, :get, [ctx, key], 10_000)
  end

  defp write_keys_to_shard(node, shard_idx, prefix, range) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    Enum.flat_map(range, fn i ->
      key = find_key_for_shard(n, ctx, "#{prefix}_#{i}", shard_idx)
      :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{i}", 0])
      [key]
    end)
  end

  defp find_key_for_shard(n, ctx, base_key, target_shard) do
    Enum.find_value(0..1000, fn suffix ->
      key = "#{base_key}_#{suffix}"
      shard = :erpc.call(n, Ferricstore.Store.Router, :shard_for, [ctx, key])
      if shard == target_shard, do: key
    end)
  end

  defp assert_keys_readable(node, keys) do
    Enum.each(keys, fn key ->
      eventually(
        fn ->
          value = read_key(node, key)
          assert value != nil, "key #{key} not readable on #{inspect(node)}"
        end,
        "key #{key} not readable",
        20,
        50
      )
    end)
  end

  defp start_continuous_writer(node, prefix) do
    n = node_name(node)
    parent = self()

    spawn_link(fn ->
      ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])
      continuous_write_loop(n, ctx, prefix, 1, [], parent)
    end)
  end

  defp continuous_write_loop(n, ctx, prefix, seq, keys, parent) do
    receive do
      :stop ->
        send(parent, {:writer_done, keys, seq - 1})
    after
      0 ->
        key = "#{prefix}_#{seq}"

        try do
          case :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0]) do
            :ok ->
              continuous_write_loop(n, ctx, prefix, seq + 1, [key | keys], parent)

            {:error, _} ->
              Process.sleep(10)
              continuous_write_loop(n, ctx, prefix, seq, keys, parent)
          end
        rescue
          _ ->
            Process.sleep(10)
            continuous_write_loop(n, ctx, prefix, seq, keys, parent)
        end
    end
  end

  defp stop_continuous_writer(pid) do
    send(pid, :stop)

    receive do
      {:writer_done, keys, count} -> {Enum.reverse(keys), count}
    after
      5_000 -> raise "continuous writer did not stop"
    end
  end

  defp write_loop(node, prefix, write_log, seq) do
    n = node_name(node)

    receive do
      :stop -> :ok
    after
      0 ->
        key = "#{prefix}_#{seq}"
        ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

        try do
          case :erpc.call(n, Ferricstore.Store.Router, :put, [ctx, key, "value_#{seq}", 0]) do
            :ok -> :ets.insert(write_log, {key, seq})
            _ -> :ok
          end
        rescue
          _ -> :ok
        end

        write_loop(node, prefix, write_log, seq + 1)
    end
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

  defp get_shard_count(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])
    ctx.shard_count
  end

  defp dump_keydir(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      keydir =
        :erpc.call(n, FerricStore.Instance, :get, [:default])
        |> Map.get(:keydir_refs)
        |> elem(shard_idx)

      :erpc.call(n, :ets, :tab2list, [keydir])
    end
    |> List.flatten()
  end

  defp dump_keydir_sorted(node) do
    dump_keydir(node)
    |> Enum.map(fn {key, _value, _exp, _lfu, _fid, _off, _vsize} -> key end)
    |> Enum.reject(&String.starts_with?(&1, "PM:"))
    |> Enum.sort()
  end

  defp trigger_compaction(node) do
    n = node_name(node)
    ctx = :erpc.call(n, FerricStore.Instance, :get, [:default])

    for shard_idx <- 0..(ctx.shard_count - 1) do
      shard = elem(ctx.shard_names, shard_idx)
      :erpc.call(n, GenServer, :call, [shard, {:run_compaction, []}])
    end
  end

  defp dump_raft_diagnostics(leader_node, joiner_node, shard_count, label) do
    leader_n = node_name(leader_node)
    joiner_n = node_name(joiner_node)

    IO.puts("\n=== RAFT DIAGNOSTICS [#{label}] ===")

    for shard <- 0..(shard_count - 1) do
      leader_info =
        try do
          {:ok, members, leader} =
            :erpc.call(leader_n, Ferricstore.Raft.Cluster, :members, [shard, 5_000])

          position =
            :erpc.call(leader_n, Ferricstore.Raft.WARaftBackend, :storage_position, [shard])

          %{members: length(members), leader: leader, storage_position: position}
        catch
          _, e -> %{error: inspect(e)}
        end

      joiner_info =
        try do
          position =
            :erpc.call(joiner_n, Ferricstore.Raft.WARaftBackend, :storage_position, [shard])

          status = :erpc.call(joiner_n, Ferricstore.Raft.WARaftBackend, :status, [shard])
          %{storage_position: position, status: status}
        catch
          _, e -> %{error: inspect(e)}
        end

      IO.puts("  shard #{shard}:")
      IO.puts("    leader_node: #{inspect(leader_info)}")
      IO.puts("    joiner_node: #{inspect(joiner_info)}")
    end

    IO.puts("=== END DIAGNOSTICS ===\n")
  end

  defp eventually(fun, msg, attempts, interval) do
    Ferricstore.Test.ShardHelpers.eventually(fun, msg, attempts, interval)
  end
end
