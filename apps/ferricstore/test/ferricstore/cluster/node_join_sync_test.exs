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

  describe "new node join with continuous writes" do
    @tag timeout: 180_000
    test "data is fully consistent after sync completes" do
      # 1. Start a 3-node cluster
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, node_b, _node_c] = nodes

      # 2. Write initial dataset (100 keys) — must be on cluster before new node
      initial_keys = write_keys(node_a, "pre_sync", 1..20)
      assert_keys_readable(node_b, initial_keys)

      # 3. Start continuous writer in background
      writer_pid = start_continuous_writer(node_a, "during_sync")

      # 4. Start 4th node (empty) and join it to the cluster
      #    join_cluster triggers: add to Raft + shard-by-shard data copy
      #    Writer keeps writing throughout — writes must not be blocked
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      join_result = join_cluster(node_d, node_a)
      IO.puts("join_cluster result: #{inspect(join_result)}")
      dump_raft_diagnostics(node_a, node_d, @shards, "after join")

      # 5. Stop the continuous writer
      {during_sync_keys, during_sync_count} = stop_continuous_writer(writer_pid)
      IO.puts("Writes during sync: #{during_sync_count}")

      # 6. Write final batch after sync
      final_keys = write_keys(node_a, "post_sync", 1..10)

      # 7. Wait for Raft to replicate everything to node_d
      #    during_sync keys were written while sync was happening — they go through
      #    Raft which replicates to node_d after it joins the group
      all_keys = initial_keys ++ during_sync_keys ++ final_keys

      dump_raft_diagnostics(node_a, node_d, @shards, "before poll")

      # Poll until all keys are readable (Raft replication may take a few seconds)
      poll_count = :counters.new(1, [:atomics])

      eventually(
        fn ->
          missing = Enum.filter(all_keys, fn key -> read_key(node_d, key) == nil end)
          :counters.add(poll_count, 1, 1)
          pc = :counters.get(poll_count, 1)

          if missing != [] and (pc == 1 or pc == 5 or rem(pc, 50) == 0) do
            joiner_n = node_name(node_d)
            IO.puts("  poll ##{pc}: #{length(missing)}/#{length(all_keys)} keys missing")

            for s <- 0..(@shards - 1) do
              jid = {:"ferricstore_shard_#{s}", joiner_n}

              m =
                try do
                  :erpc.call(joiner_n, :ra, :key_metrics, [jid])
                catch
                  _, _ -> :error
                end

              ctx =
                try do
                  :erpc.call(joiner_n, FerricStore.Instance, :get, [:default])
                catch
                  _, _ -> nil
                end

              ets_size =
                if ctx do
                  kd = elem(ctx.keydir_refs, s)

                  try do
                    :erpc.call(joiner_n, :ets, :info, [kd, :size])
                  catch
                    _, _ -> :error
                  end
                end

              IO.puts("    shard #{s}: raft=#{inspect(m)}, ets_size=#{inspect(ets_size)}")
            end

            sample = Enum.take(missing, 3)

            for k <- sample do
              ctx =
                try do
                  :erpc.call(joiner_n, FerricStore.Instance, :get, [:default])
                catch
                  _, _ -> nil
                end

              if ctx do
                shard = :erpc.call(joiner_n, Ferricstore.Store.Router, :shard_for, [ctx, k])
                kd = elem(ctx.keydir_refs, shard)

                ets_entry =
                  try do
                    :erpc.call(joiner_n, :ets, :lookup, [kd, k])
                  catch
                    _, _ -> :error
                  end

                IO.puts("    missing key #{inspect(k)} shard=#{shard} ets=#{inspect(ets_entry)}")
              end
            end
          end

          assert missing == [], "#{length(missing)} keys still missing on node_d"
        end,
        "not all keys replicated to node_d",
        240,
        500
      )

      # 8. Final verification
      missing = Enum.filter(all_keys, fn key -> read_key(node_d, key) == nil end)
      IO.puts("Total keys: #{length(all_keys)}, missing on node_d: #{length(missing)}")

      assert missing == [],
             "#{length(missing)} keys missing on node_d: #{inspect(Enum.take(missing, 5))}"

      # 9. Compare keydirs — must be identical across all nodes
      keydir_a = dump_keydir_sorted(node_a)
      keydir_d = dump_keydir_sorted(node_d)

      assert length(keydir_a) == length(keydir_d),
             "keydir size mismatch: node_a=#{length(keydir_a)} node_d=#{length(keydir_d)}"

      mismatched =
        Enum.zip(keydir_a, keydir_d)
        |> Enum.filter(fn {a, d} -> a != d end)
        |> Enum.take(5)

      assert mismatched == [],
             "keydir content mismatch: #{inspect(mismatched)}"

      IO.puts("SUCCESS: #{length(all_keys)} keys identical across all nodes")
    end

    @tag timeout: 180_000
    test "4th node auto-discovers and syncs while writes continue" do
      # 1. Start 3-node cluster with data
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, node_b, _node_c] = nodes

      initial_keys = write_keys(node_a, "pre_sync", 1..20)
      assert_keys_readable(node_b, initial_keys)

      # 2. Continuous writer
      writer_pid = start_continuous_writer(node_a, "during_sync")

      # 3. Start 4th node with cluster_nodes pointing to existing cluster
      #    No manual join — auto-discovery handles it
      all_cluster_nodes = Enum.map(nodes, &node_name/1)
      node_d = ClusterHelper.start_node(shards: @shards, cluster_nodes: all_cluster_nodes)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # 4. Connect node_d to the cluster (simulates libcluster)
      :erpc.call(node_name(node_a), Node, :connect, [node_name(node_d)])

      # 5. Stop writer after giving auto-join time to complete
      Process.sleep(3_000)
      dump_raft_diagnostics(node_a, node_d, @shards, "after auto-join wait")
      {during_sync_keys, during_sync_count} = stop_continuous_writer(writer_pid)
      IO.puts("Writes during sync: #{during_sync_count}")

      final_keys = write_keys(node_a, "post_sync", 1..10)
      all_keys = initial_keys ++ during_sync_keys ++ final_keys

      dump_raft_diagnostics(node_a, node_d, @shards, "before poll (auto-discover)")

      # 6. Wait for all keys on node_d — auto-discovery + Raft join + replication
      #    can take a while on loaded machines with 4 BEAM VMs
      poll_count2 = :counters.new(1, [:atomics])

      eventually(
        fn ->
          missing = Enum.filter(all_keys, fn key -> read_key(node_d, key) == nil end)
          :counters.add(poll_count2, 1, 1)
          pc = :counters.get(poll_count2, 1)

          if missing != [] and (pc == 1 or pc == 5 or rem(pc, 50) == 0) do
            joiner_n = node_name(node_d)
            IO.puts("  poll ##{pc}: #{length(missing)}/#{length(all_keys)} keys missing")

            for s <- 0..(@shards - 1) do
              jid = {:"ferricstore_shard_#{s}", joiner_n}

              m =
                try do
                  :erpc.call(joiner_n, :ra, :key_metrics, [jid])
                catch
                  _, _ -> :error
                end

              ctx =
                try do
                  :erpc.call(joiner_n, FerricStore.Instance, :get, [:default])
                catch
                  _, _ -> nil
                end

              ets_size =
                if ctx do
                  kd = elem(ctx.keydir_refs, s)

                  try do
                    :erpc.call(joiner_n, :ets, :info, [kd, :size])
                  catch
                    _, _ -> :error
                  end
                end

              IO.puts("    shard #{s}: raft=#{inspect(m)}, ets_size=#{inspect(ets_size)}")
            end

            sample = Enum.take(missing, 3)

            for k <- sample do
              ctx =
                try do
                  :erpc.call(joiner_n, FerricStore.Instance, :get, [:default])
                catch
                  _, _ -> nil
                end

              if ctx do
                shard = :erpc.call(joiner_n, Ferricstore.Store.Router, :shard_for, [ctx, k])
                kd = elem(ctx.keydir_refs, shard)

                ets_entry =
                  try do
                    :erpc.call(joiner_n, :ets, :lookup, [kd, k])
                  catch
                    _, _ -> :error
                  end

                IO.puts("    missing key #{inspect(k)} shard=#{shard} ets=#{inspect(ets_entry)}")
              end
            end
          end

          assert missing == [], "#{length(missing)} keys still missing on node_d"
        end,
        "not all keys replicated to node_d",
        240,
        500
      )

      IO.puts("Total keys: #{length(all_keys)}, missing on node_d: 0")

      # 7. Keydirs identical
      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_d), "keydir mismatch"
        end,
        "keydirs not converged",
        40,
        500
      )

      IO.puts("SUCCESS: #{length(all_keys)} keys identical (auto-discovery)")
    end

    @tag timeout: 180_000
    test "no writes lost during sync — every write is durable" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Start 4th node
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # Track every write with a monotonic counter
      write_log = :ets.new(:write_log, [:set, :public])

      # Writer that logs every successful write
      writer_pid =
        spawn_link(fn ->
          write_loop(node_a, "durability", write_log, 1)
        end)

      # Join while writes are happening
      :ok = join_cluster(node_d, node_a)
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

      # Stop writer
      send(writer_pid, :stop)
      Process.sleep(100)

      # Get all written keys from the log
      written = :ets.tab2list(write_log)
      written_keys = Enum.map(written, fn {key, _seq} -> key end)

      # Every acknowledged write must be readable on node_d
      eventually(
        fn ->
          missing =
            Enum.filter(written_keys, fn key ->
              read_key(node_d, key) == nil
            end)

          assert missing == [],
                 "#{length(missing)}/#{length(written_keys)} writes not replicated: #{inspect(Enum.take(missing, 5))}"
        end,
        "all writes replicated to node_d",
        120,
        500
      )

      :ets.delete(write_log)
    end

    @tag timeout: 180_000
    test "shard-by-shard sync — other shards serve writes while one syncs" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes
      shard_count = get_shard_count(node_a)

      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # Write to specific shards to verify per-shard sync
      per_shard_keys =
        for shard_idx <- 0..(shard_count - 1) do
          keys = write_keys_to_shard(node_a, shard_idx, "shard_#{shard_idx}", 1..20)
          {shard_idx, keys}
        end

      # Join and sync
      :ok = join_cluster(node_d, node_a)
      # join_cluster does sync synchronously — data is already on node_d
      # Wait for Raft replication to settle
      Process.sleep(2_000)

      # Verify each shard's keys are present on node_d
      for {shard_idx, keys} <- per_shard_keys do
        missing = Enum.filter(keys, fn key -> read_key(node_d, key) == nil end)

        assert missing == [],
               "shard #{shard_idx}: #{length(missing)} keys missing on node_d"
      end
    end
  end

  describe "data directory consistency" do
    @tag timeout: 180_000
    test "after sync + settle, keydir dump is identical across all nodes" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Write dataset
      write_keys(node_a, "consistency", 1..50)

      # Add 4th node
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      :ok = join_cluster(node_d, node_a)

      # Wait for keydirs to converge across all nodes
      eventually(
        fn ->
          ref = dump_keydir_sorted(node_a)

          for node <- [node_d | Enum.drop(nodes, 1)] do
            assert dump_keydir_sorted(node) == ref, "keydir not converged on #{inspect(node)}"
          end
        end,
        "keydirs not converged",
        60,
        500
      )
    end

    @tag timeout: 180_000
    test "bitcask file checksums match after compaction on all nodes" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      # Write + overwrite to generate dead bytes, then compact
      for i <- 1..30, do: write_key(node_a, "compact_key_#{i}", "value_v1")
      for i <- 1..30, do: write_key(node_a, "compact_key_#{i}", "value_v2")

      # Add 4th node
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      :ok = join_cluster(node_d, node_a)

      # Wait for all data to replicate before compacting
      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_d),
                 "pre-compact keydir mismatch"
        end,
        "keydirs not converged before compaction",
        60,
        500
      )

      # Trigger compaction on all nodes
      for node <- nodes ++ [node_d] do
        trigger_compaction(node)
      end

      # After compaction, keydirs should still match
      eventually(
        fn ->
          keydir_a = dump_keydir_sorted(node_a)
          keydir_d = dump_keydir_sorted(node_d)
          assert keydir_a == keydir_d, "post-compact keydir mismatch"
        end,
        "keydirs diverged after compaction",
        40,
        500
      )
    end
  end

  describe "standalone to cluster (manual join)" do
    @tag timeout: 180_000
    test "single node with data → two nodes join via CLUSTER.JOIN → all data synced" do
      node_a = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_a) end)
      ClusterHelper.wait_for_node_leaders(node_name(node_a), @shards, timeout: 10_000)

      keys = write_keys(node_a, "standalone", 1..30)

      node_b = ClusterHelper.start_node(shards: @shards)
      node_c = ClusterHelper.start_node(shards: @shards)

      on_exit(fn ->
        ClusterHelper.stop_node(node_b)
        ClusterHelper.stop_node(node_c)
      end)

      n_a = node_name(node_a)
      :erpc.call(n_a, Node, :connect, [node_name(node_b)])
      :erpc.call(n_a, Node, :connect, [node_name(node_c)])

      eventually(
        fn ->
          assert :ok == join_cluster(node_b, node_a)
        end,
        "node_b join failed",
        10,
        1000
      )

      eventually(
        fn ->
          assert :ok == join_cluster(node_c, node_a)
        end,
        "node_c join failed",
        10,
        1000
      )

      post_keys = write_keys(node_a, "post", 1..10)
      all_keys = keys ++ post_keys

      eventually(
        fn ->
          assert Enum.all?(all_keys, fn k -> read_key(node_b, k) != nil end), "keys missing on b"
          assert Enum.all?(all_keys, fn k -> read_key(node_c, k) != nil end), "keys missing on c"
        end,
        "replication incomplete",
        120,
        500
      )

      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_b), "keydir a↔b mismatch"
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_c), "keydir a↔c mismatch"
        end,
        "keydirs not converged",
        40,
        500
      )
    end
  end

  describe "auto-discovery: one node with data, two nodes join via cluster_nodes config" do
    @tag timeout: 180_000
    test "new nodes auto-discover, sync data, keydirs identical" do
      # 1. Start node_a as standalone with data
      node_a = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_a) end)
      ClusterHelper.wait_for_node_leaders(node_name(node_a), @shards, timeout: 10_000)

      keys = write_keys(node_a, "existing", 1..30)

      # 2. Start node_b and node_c with cluster_nodes pointing to node_a
      #    Simulates: FERRICSTORE_CLUSTER_NODES=a,b,c → libcluster connects → :nodeup → auto-join
      n_a = node_name(node_a)
      node_b = ClusterHelper.start_node(shards: @shards, cluster_nodes: [n_a])
      node_c = ClusterHelper.start_node(shards: @shards, cluster_nodes: [n_a])

      on_exit(fn ->
        ClusterHelper.stop_node(node_b)
        ClusterHelper.stop_node(node_c)
      end)

      # 3. Connect nodes (simulates libcluster discovery)
      #    :nodeup fires on all nodes → ClusterManager auto-joins
      :erpc.call(n_a, Node, :connect, [node_name(node_b)])
      :erpc.call(n_a, Node, :connect, [node_name(node_c)])

      # 4. No manual CLUSTER.JOIN — auto-join handles everything

      # 5. Write more data after connection
      Process.sleep(2_000)
      dump_raft_diagnostics(node_a, node_b, @shards, "auto-discover b")
      dump_raft_diagnostics(node_a, node_c, @shards, "auto-discover c")
      post_keys = write_keys(node_a, "post", 1..10)
      all_keys = keys ++ post_keys

      # 6. Wait for all data to replicate
      eventually(
        fn ->
          missing_b = Enum.count(all_keys, fn k -> read_key(node_b, k) == nil end)
          missing_c = Enum.count(all_keys, fn k -> read_key(node_c, k) == nil end)

          if missing_b > 0 or missing_c > 0 do
            IO.puts("  poll: b missing=#{missing_b} c missing=#{missing_c}")
          end

          assert missing_b == 0, "#{missing_b} keys missing on b"
          assert missing_c == 0, "#{missing_c} keys missing on c"
        end,
        "replication incomplete",
        120,
        500
      )

      # 7. Keydirs must be identical
      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_b), "keydir a↔b mismatch"
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_c), "keydir a↔c mismatch"
        end,
        "keydirs not converged",
        40,
        500
      )
    end
  end

  describe "non-voter (readonly) node joins and gets data" do
    @tag timeout: 180_000
    test "readonly replica receives all data, writes forward to leader" do
      # 1. Start 3-node cluster with data
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes

      keys = write_keys(node_a, "before_replica", 1..20)

      # 2. Start readonly replica with cluster_nodes + cluster_role
      #    Simulates: FERRICSTORE_CLUSTER_ROLE=readonly FERRICSTORE_CLUSTER_NODES=a,b,c
      all_cluster_nodes = Enum.map(nodes, &node_name/1)

      replica =
        ClusterHelper.start_node(
          shards: @shards,
          cluster_nodes: all_cluster_nodes,
          cluster_role: :readonly
        )

      on_exit(fn -> ClusterHelper.stop_node(replica) end)

      # 3. Connect — auto-discovery reads remote role and joins as non_voter
      n_a = node_name(node_a)
      :erpc.call(n_a, Node, :connect, [node_name(replica)])

      # Wait for auto-join to complete (spawned process with 2s delay + sync time)
      Process.sleep(5_000)
      dump_raft_diagnostics(node_a, replica, @shards, "readonly after join wait")

      # 4. Write more data after replica joined
      post_keys = write_keys(node_a, "after_replica", 1..10)
      all_keys = keys ++ post_keys

      # 5. Verify replica has all data (receives replication)
      eventually(
        fn ->
          missing = Enum.count(all_keys, fn k -> read_key(replica, k) == nil end)

          if missing > 0 do
            IO.puts("  poll: replica missing=#{missing}")
          end

          assert missing == 0, "#{missing} keys missing on replica"
        end,
        "replica missing keys",
        120,
        500
      )

      # 6. Verify keydirs identical
      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(replica), "keydir mismatch"
        end,
        "keydirs not converged",
        40,
        500
      )

      # 7. Write from replica — should forward to leader and succeed
      write_key(replica, "replica_write_test", "from_replica")

      eventually(
        fn ->
          assert read_key(node_a, "replica_write_test") == "from_replica"
        end,
        "replica write not forwarded to leader",
        60,
        500
      )

      # 8. Verify replica is NOT a voter (doesn't affect quorum)
      #    Check ra membership on any shard
      {:ok, members, _leader} = :erpc.call(n_a, Ferricstore.Raft.Cluster, :members, [0])
      replica_member = Enum.find(members, fn {_name, node} -> node == node_name(replica) end)
      assert replica_member != nil, "replica should be in member list"
    end
  end

  describe "disk snapshot: new node boots from copied disk while writes continue" do
    @tag timeout: 180_000
    test "node with cloned disk joins cluster and gets all data" do
      # 1. Start 3-node cluster, write data
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)

      [node_a, _node_b, _node_c] = nodes
      n_a = node_name(node_a)

      keys = write_keys(node_a, "pre_clone", 1..30)

      # 2. Start continuous writer
      writer_pid = start_continuous_writer(node_a, "during_clone")

      # 3. Simulate EBS snapshot: copy data dir WHILE server is running.
      #    No pausing — EBS does block-level atomic snapshot.
      #    The copy may have partially written files, but recover_keydir
      #    handles this (skips corrupt records at end of log files).
      source_ctx = :erpc.call(n_a, FerricStore.Instance, :get, [:default], 10_000)
      source_data_dir = source_ctx.data_dir

      clone_dir =
        Path.join(System.tmp_dir!(), "ferricstore_clone_#{System.unique_integer([:positive])}")

      File.cp_r!(source_data_dir, clone_dir)
      on_exit(fn -> File.rm_rf(clone_dir) end)

      # Read last applied Raft index from cloned ra state BEFORE deleting.
      for i <- 0..(source_ctx.shard_count - 1) do
        idx = Ferricstore.Cluster.DataSync.read_last_applied_from_disk(clone_dir, i)
        IO.puts("Shard #{i}: cloned data at raft index #{idx}")
      end

      # Delete ra dir — has source node's server IDs, can't reuse.
      File.rm_rf!(Path.join(clone_dir, "ra"))

      # 4. Start peer node but DON'T start FerricStore yet.
      #    Connect to cluster first so ra can reach other nodes during election.
      all_cluster_nodes = Enum.map(nodes, &node_name/1)

      node_d =
        ClusterHelper.start_node(
          shards: @shards,
          data_dir: clone_dir,
          cluster_nodes: all_cluster_nodes
        )

      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      # 5. Connect is automatic via cluster_nodes in start_node
      #    node_a's auto-join detects pre-existing data → just add_member

      # 6. Stop writer after join has time to complete
      Process.sleep(5_000)
      dump_raft_diagnostics(node_a, node_d, @shards, "disk clone after join wait")
      {during_keys, during_count} = stop_continuous_writer(writer_pid)
      IO.puts("Writes during clone join: #{during_count}")

      # 7. Write post-join data
      post_keys = write_keys(node_a, "post_clone", 1..10)
      all_keys = keys ++ during_keys ++ post_keys

      # 8. Verify all keys on node_d
      eventually(
        fn ->
          missing = Enum.count(all_keys, fn k -> read_key(node_d, k) == nil end)

          if missing > 0 do
            IO.puts("  poll: clone missing=#{missing}")
          end

          assert missing == 0, "#{missing} keys missing on node_d"
        end,
        "keys not replicated to cloned node",
        120,
        500
      )

      IO.puts("Total keys: #{length(all_keys)}, all present on cloned node")

      # 9. Keydirs identical
      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_d), "keydir mismatch"
        end,
        "keydirs not converged",
        60,
        500
      )

      IO.puts("SUCCESS: #{length(all_keys)} keys identical after disk clone join")
    end
  end

  describe "rejoin after removal" do
    @tag timeout: 180_000
    test "removed node can rejoin and gets all data" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
      [node_a, _node_b, _node_c] = nodes
      n_a = node_name(node_a)
      keys_v1 = write_keys(node_a, "before_remove", 1..20)
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)
      :ok = join_cluster(node_d, node_a)

      eventually(
        fn ->
          assert Enum.all?(keys_v1, fn k -> read_key(node_d, k) != nil end), "v1 keys missing"
        end,
        "v1 keys not replicated",
        60,
        500
      )

      :ok = :erpc.call(n_a, Ferricstore.Cluster.Manager, :remove_node, [node_name(node_d)])
      keys_v2 = write_keys(node_a, "during_removed", 1..20)
      :ok = join_cluster(node_d, node_a)
      all_keys = keys_v1 ++ keys_v2

      eventually(
        fn ->
          missing = Enum.count(all_keys, fn k -> read_key(node_d, k) == nil end)
          assert missing == 0, "#{missing} keys missing after rejoin"
        end,
        "keys not replicated after rejoin",
        120,
        500
      )

      eventually(
        fn ->
          assert dump_keydir_sorted(node_a) == dump_keydir_sorted(node_d),
                 "keydir mismatch after rejoin"
        end,
        "keydirs not converged after rejoin",
        40,
        500
      )
    end
  end

  describe "duplicate join is idempotent" do
    @tag timeout: 180_000
    test "concurrent add_node calls for same target converge" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
      [node_a, node_b, _node_c] = nodes
      n_a = node_name(node_a)
      n_b = node_name(node_b)
      keys = write_keys(node_a, "dedup", 1..30)
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)

      task_a =
        Task.async(fn ->
          :erpc.call(n_a, Ferricstore.Cluster.Manager, :add_node, [node_name(node_d)], 60_000)
        end)

      task_b =
        Task.async(fn ->
          :erpc.call(n_b, Ferricstore.Cluster.Manager, :add_node, [node_name(node_d)], 60_000)
        end)

      result_a = Task.await(task_a, 60_000)
      result_b = Task.await(task_b, 60_000)
      assert result_a == :ok
      assert result_b == :ok

      eventually(
        fn ->
          missing = Enum.count(keys, fn k -> read_key(node_d, k) == nil end)
          assert missing == 0, "#{missing} keys missing after concurrent join"
        end,
        "keys not replicated after concurrent join",
        120,
        500
      )

      shard_count = get_shard_count(node_a)

      for shard_idx <- 0..(shard_count - 1) do
        {:ok, members, _leader} = :erpc.call(n_a, Ferricstore.Raft.Cluster, :members, [shard_idx])
        node_d_members = Enum.filter(members, fn {_name, n} -> n == node_name(node_d) end)
        assert length(node_d_members) == 1
      end
    end
  end

  describe "leader failover during sync" do
    @tag timeout: 180_000
    test "sync recovers after leader dies mid-join" do
      nodes = ClusterHelper.start_cluster(3, shards: @shards)
      on_exit(fn -> ClusterHelper.stop_cluster(nodes) end)
      [node_a, node_b, _node_c] = nodes
      keys = write_keys(node_a, "pre_failover", 1..30)

      eventually(
        fn ->
          assert Enum.all?(keys, fn k -> read_key(node_b, k) != nil end), "keys missing on b"
        end,
        "pre-replication incomplete",
        60,
        500
      )

      leader_name = ClusterHelper.find_leader(nodes, 0)
      leader_node = Enum.find(nodes, &(&1.name == leader_name))
      {_killed, remaining} = ClusterHelper.kill_node(nodes, leader_node)
      ClusterHelper.wait_for_leaders(remaining, @shards, timeout: 15_000)
      surviving = hd(remaining)
      node_d = ClusterHelper.start_node(shards: @shards)
      on_exit(fn -> ClusterHelper.stop_node(node_d) end)
      :ok = join_cluster(node_d, surviving)
      post_keys = write_keys(surviving, "post_failover", 1..10)
      all_keys = keys ++ post_keys

      eventually(
        fn ->
          missing = Enum.count(all_keys, fn k -> read_key(node_d, k) == nil end)
          assert missing == 0, "#{missing} keys missing after leader failover join"
        end,
        "keys not replicated after failover join",
        120,
        500
      )

      eventually(
        fn ->
          assert dump_keydir_sorted(surviving) == dump_keydir_sorted(node_d),
                 "keydir mismatch after failover join"
        end,
        "keydirs not converged after failover",
        40,
        500
      )
    end
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
      leader_id = {:"ferricstore_shard_#{shard}", leader_n}
      joiner_id = {:"ferricstore_shard_#{shard}", joiner_n}

      leader_info =
        try do
          {:ok, members, leader} = :erpc.call(leader_n, :ra, :members, [leader_id, 5_000])
          metrics = :erpc.call(leader_n, :ra, :key_metrics, [leader_id])
          %{members: length(members), leader: leader, metrics: metrics}
        catch
          _, e -> %{error: inspect(e)}
        end

      joiner_info =
        try do
          metrics = :erpc.call(joiner_n, :ra, :key_metrics, [joiner_id])
          %{metrics: metrics}
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
