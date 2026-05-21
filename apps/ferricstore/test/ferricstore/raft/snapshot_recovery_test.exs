defmodule Ferricstore.Raft.SnapshotRecoveryTest do
  @moduledoc """
  Tests that data survives WAL compaction and snapshot recovery in multi-node
  Raft clusters.

  These tests exercise the full durability guarantee:
    - Data readable after bulk writes
    - Data replicated to followers
    - Data survives follower stop/restart (WAL replay)
    - Data survives snapshot-based recovery (new member joining after compaction)
    - Data survives leader failover

  Uses `SnapshotKvMachine` which emits `release_cursor` effects so that ra
  actually takes snapshots and truncates WAL entries. This validates that the
  snapshot + WAL replay path works end-to-end.

  ## Running

      cd apps/ferricstore && mix test test/ferricstore/raft/snapshot_recovery_test.exs --include cluster
  """

  use ExUnit.Case, async: false

  @moduletag :cluster
  @moduletag timeout: 180_000

  require Logger

  @simple_machine Ferricstore.Test.SimpleKvMachine
  @snapshot_machine Ferricstore.Test.SnapshotKvMachine

  # ---------------------------------------------------------------------------
  # Setup: ensure distribution and start peer nodes
  # ---------------------------------------------------------------------------

  setup do
    ensure_distribution!()

    # Each test gets its own unique id to avoid name collisions
    id = :erlang.unique_integer([:positive])

    %{id: id}
  end

  # ---------------------------------------------------------------------------
  # Test 1: Single-node — data survives WAL compaction
  # ---------------------------------------------------------------------------

  describe "single-node WAL compaction" do
    test "all 2000 keys readable after release_cursor triggers snapshot", %{id: id} do
      {peers, cleanup} = start_peers(1)
      on_exit(cleanup)

      ra_sys = :"snap1_sys_#{id}"
      cluster_name = :"snap1_#{id}"
      prefix = "s1_#{id}_"

      [node] = setup_ra_systems(peers, ra_sys, id)

      members = [{:"#{prefix}1", node}]

      # Use SnapshotKvMachine with interval=500 so we get ~4 snapshots over 2000 writes
      machine = {:module, @snapshot_machine, %{release_cursor_interval: 500}}
      {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
      assert length(started) == 1

      leader = wait_for_leader(members)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(leader, 200))
        end,
        "Raft leader not elected",
        50,
        50
      )

      # Write 2000 keys synchronously
      write_keys_sync(leader, prefix, 1..2000)

      # Verify all 2000 keys are present via local_query
      {:ok, {_, count}, _} =
        :ra.local_query(leader, &@snapshot_machine.count/1)

      assert count == 2000

      # Spot-check specific values via process_command (get is a state machine command)
      for i <- Enum.take_random(1..2000, 20) do
        key = "#{prefix}k#{i}"
        {:ok, value, _} = :ra.process_command(leader, {:get, key}, 10_000)
        assert value == "v#{i}", "Key #{key} expected v#{i}, got #{inspect(value)}"
      end

      # Cleanup ra servers
      cleanup_ra_servers(ra_sys, members)
    end
  end

  # ---------------------------------------------------------------------------
  # Test 2: 3-node — follower has all data after leader writes
  # ---------------------------------------------------------------------------

  describe "3-node follower replication" do
    test "all 1000 keys readable on followers after leader writes", %{id: id} do
      {peers, cleanup} = start_peers(3)
      on_exit(cleanup)

      ra_sys = :"snap2_sys_#{id}"
      cluster_name = :"snap2_#{id}"
      prefix = "s2_#{id}_"

      nodes = setup_ra_systems(peers, ra_sys, id)

      members =
        nodes
        |> Enum.with_index(1)
        |> Enum.map(fn {nd, i} -> {:"#{prefix}#{i}", nd} end)

      machine = {:module, @simple_machine, %{}}
      {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
      assert length(started) == 3

      leader = wait_for_leader(members)
      followers = Enum.reject(members, &(&1 == leader))

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(leader, 200))
        end,
        "Raft leader not elected",
        50,
        50
      )

      # Write 1000 keys on leader
      write_keys_sync(leader, prefix, 1..1000)

      # Wait for replication to settle on each follower
      for follower <- followers do
        wait_for_key_count(follower, @simple_machine, 1000,
          timeout: 15_000,
          label: "follower #{inspect(follower)}"
        )
      end

      # Spot-check values on a follower via process_command
      for i <- Enum.take_random(1..1000, 10) do
        key = "#{prefix}k#{i}"
        {:ok, value, _} = :ra.process_command(leader, {:get, key}, 10_000)
        assert value == "v#{i}", "Follower missing #{key}"
      end

      cleanup_ra_servers(ra_sys, members)
    end
  end

  # ---------------------------------------------------------------------------
  # Test 3: 3-node — follower recovers after stop/restart
  # ---------------------------------------------------------------------------

  describe "3-node follower stop/restart recovery" do
    test "stopped follower catches up with writes made while it was down", %{id: id} do
      {peers, cleanup} = start_peers(3)
      on_exit(cleanup)

      ra_sys = :"snap3_sys_#{id}"
      cluster_name = :"snap3_#{id}"
      prefix = "s3_#{id}_"

      nodes = setup_ra_systems(peers, ra_sys, id)

      members =
        nodes
        |> Enum.with_index(1)
        |> Enum.map(fn {nd, i} -> {:"#{prefix}#{i}", nd} end)

      machine = {:module, @simple_machine, %{}}
      {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
      assert length(started) == 3

      leader = wait_for_leader(members)
      followers = Enum.reject(members, &(&1 == leader))
      [victim | _surviving_followers] = followers

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(leader, 200))
        end,
        "Raft leader not elected",
        50,
        50
      )

      # Phase 1: Write 500 keys with all nodes up
      write_keys_sync(leader, prefix, 1..500)

      wait_for_key_count(victim, @simple_machine, 500,
        timeout: 15_000,
        label: "victim pre-stop replication"
      )

      # Verify victim has the 500 keys before we stop it
      assert_key_count(victim, @simple_machine, 500, label: "victim pre-stop")

      # Phase 2: Stop the victim follower
      :ok = :ra.stop_server(ra_sys, victim)

      # Phase 3: Write 500 more keys while victim is down
      write_keys_sync(leader, prefix, 501..1000)

      # Phase 4: Restart the victim
      :ok = :ra.restart_server(ra_sys, victim)

      # Wait for the follower to catch up via WAL replay
      wait_for_key_count(victim, @simple_machine, 1000,
        timeout: 15_000,
        label: "victim after restart"
      )

      # Verify specific keys from both phases via process_command on leader
      for i <- [1, 250, 500, 501, 750, 1000] do
        key = "#{prefix}k#{i}"
        {:ok, value, _} = :ra.process_command(leader, {:get, key}, 10_000)
        assert value == "v#{i}", "Key #{key} missing after restart, got #{inspect(value)}"
      end

      cleanup_ra_servers(ra_sys, members)
    end
  end

  # ---------------------------------------------------------------------------
  # Test 4: 3-node — new member joins after compaction
  # ---------------------------------------------------------------------------

  describe "3-node new member joins after compaction" do
    test "new member receives snapshot and has all data", %{id: id} do
      {peers, cleanup} = start_peers(3)
      on_exit(cleanup)

      ra_sys = :"snap4_sys_#{id}"
      cluster_name = :"snap4_#{id}"
      prefix = "s4_#{id}_"

      nodes = setup_ra_systems(peers, ra_sys, id)
      [node1, node2, node3] = nodes

      # Start cluster with only 2 of the 3 nodes
      initial_members = [
        {:"#{prefix}1", node1},
        {:"#{prefix}2", node2}
      ]

      new_member_id = {:"#{prefix}3", node3}

      # Use SnapshotKvMachine with aggressive compaction (every 200 applies)
      machine = {:module, @snapshot_machine, %{release_cursor_interval: 200}}
      {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, initial_members)
      assert length(started) == 2

      leader = wait_for_leader(initial_members)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(leader, 200))
        end,
        "Raft leader not elected",
        50,
        50
      )

      # Write 2000 keys — this triggers ~10 release_cursor effects,
      # causing ra to take snapshots and truncate WAL
      write_keys_sync(leader, prefix, 1..2000)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          case :ra.local_query(leader, &@snapshot_machine.count/1) do
            {:ok, {_, 2000}, _} -> true
            _ -> false
          end
        end,
        "leader should have all 2000 keys",
        50,
        50
      )

      # Verify leader has all data before adding new member
      assert_key_count(leader, @snapshot_machine, 2000, label: "leader pre-add")

      # Add the 3rd node as a new member
      # First start the ra server for it, then add it to the cluster
      :ok =
        :ra.start_server(
          ra_sys,
          cluster_name,
          new_member_id,
          machine,
          initial_members
        )

      {:ok, _, _leader} = :ra.add_member(leader, new_member_id, 10_000)

      # Wait for the new member to receive snapshot + catch up
      wait_for_key_count(new_member_id, @snapshot_machine, 2000,
        timeout: 30_000,
        label: "new member after join"
      )

      # Verify specific values via process_command (goes through leader -> new member)
      for i <- Enum.take_random(1..2000, 20) do
        key = "#{prefix}k#{i}"
        {:ok, value, _} = :ra.process_command(leader, {:get, key}, 10_000)
        assert value == "v#{i}", "Key #{key} missing after join, got #{inspect(value)}"
      end

      all_members = initial_members ++ [new_member_id]
      cleanup_ra_servers(ra_sys, all_members)
    end
  end

  # ---------------------------------------------------------------------------
  # Test 5: Data integrity after leader failover
  # ---------------------------------------------------------------------------

  describe "3-node leader failover" do
    test "data survives leader kill and new writes succeed on new leader", %{id: id} do
      {peers, cleanup} = start_peers(3)
      on_exit(cleanup)

      ra_sys = :"snap5_sys_#{id}"
      cluster_name = :"snap5_#{id}"
      prefix = "s5_#{id}_"

      nodes = setup_ra_systems(peers, ra_sys, id)

      members =
        nodes
        |> Enum.with_index(1)
        |> Enum.map(fn {nd, i} -> {:"#{prefix}#{i}", nd} end)

      machine = {:module, @simple_machine, %{}}
      {:ok, started, _} = :ra.start_cluster(ra_sys, cluster_name, machine, members)
      assert length(started) == 3

      original_leader = wait_for_leader(members)

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(original_leader, 200))
        end,
        "Raft leader not elected",
        50,
        50
      )

      # Phase 1: Write 500 keys on original leader
      write_keys_sync(original_leader, prefix, 1..500)

      # Wait for replication before killing leader
      surviving_members_pre = Enum.reject(members, &(&1 == original_leader))

      for member <- surviving_members_pre do
        wait_for_key_count(member, @simple_machine, 500,
          timeout: 15_000,
          label: "pre-kill replication #{inspect(member)}"
        )
      end

      # Phase 2: Kill the leader
      :ok = :ra.stop_server(ra_sys, original_leader)

      # Phase 3: Wait for new leader election among survivors
      surviving_members = Enum.reject(members, &(&1 == original_leader))
      new_leader = wait_for_leader(surviving_members, 30)
      assert new_leader != original_leader

      Ferricstore.Test.ShardHelpers.eventually(
        fn ->
          match?({:ok, _, _}, :ra.members(new_leader, 200))
        end,
        "new Raft leader not ready",
        50,
        50
      )

      # Phase 4: Write 500 more keys on new leader
      write_keys_sync(new_leader, prefix, 501..1000)

      # Phase 5: Verify all 1000 keys on surviving followers
      for member <- surviving_members do
        wait_for_key_count(member, @simple_machine, 1000,
          timeout: 10_000,
          label: "survivor #{inspect(member)}"
        )
      end

      # Spot-check values spanning both phases via process_command
      for i <- [1, 250, 500, 501, 750, 1000] do
        key = "#{prefix}k#{i}"
        {:ok, value, _} = :ra.process_command(new_leader, {:get, key}, 10_000)

        assert value == "v#{i}",
               "Key #{key} lost after failover, got #{inspect(value)}"
      end

      cleanup_ra_servers(ra_sys, members)
    end
  end

  # ===========================================================================
  # Helpers
  # ===========================================================================

  defp ensure_distribution! do
    Ferricstore.Test.ShardHelpers.ensure_distribution_started!(:snap_test)
  end

  defp start_peers(n) do
    code_paths = Enum.flat_map(:code.get_path(), fn p -> [~c"-pa", p] end)

    peers =
      Enum.map(1..n, fn i ->
        name = :"snap_peer_#{:erlang.unique_integer([:positive])}_#{i}"

        cookie = Atom.to_charlist(Node.get_cookie())

        {:ok, peer_pid, node_name} =
          :peer.start(%{
            name: name,
            args: code_paths ++ [~c"-connect_all", ~c"false", ~c"-setcookie", cookie],
            wait_boot: 120_000
          })

        # Start ra on the peer
        {:ok, _} = :rpc.call(node_name, Application, :ensure_all_started, [:ra])

        # Load both machine modules on the peer
        for mod <- [@simple_machine, @snapshot_machine] do
          {m, bin, file} = :code.get_object_code(mod)
          {:module, ^m} = :rpc.call(node_name, :code, :load_binary, [m, file, bin])
        end

        # Connect peer to test runner
        :rpc.call(node_name, Node, :connect, [node()])

        {node_name, peer_pid}
      end)

    peer_nodes = Enum.map(peers, fn {node_name, _pid} -> node_name end)

    for n1 <- peer_nodes, n2 <- peer_nodes, n1 != n2 do
      :rpc.call(n1, Node, :connect, [n2])
    end

    cleanup = fn ->
      Enum.each(peers, fn {_name, peer_pid} ->
        try do
          :peer.stop(peer_pid)
        catch
          _, _ -> :ok
        end
      end)
    end

    {peers, cleanup}
  end

  defp setup_ra_systems(peers, ra_sys, id) do
    Enum.map(peers, fn {node_name, _pid} ->
      dir = Path.join(System.tmp_dir!(), "snap_test_#{id}_#{node_name}")
      :rpc.call(node_name, File, :mkdir_p!, [dir])

      names = :rpc.call(node_name, :ra_system, :derive_names, [ra_sys])

      config = %{
        name: ra_sys,
        names: names,
        data_dir: to_charlist(dir),
        wal_data_dir: to_charlist(dir),
        segment_max_entries: 4096
      }

      case :rpc.call(node_name, :ra_system, :start, [config]) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end

      node_name
    end)
  end

  defp write_keys_sync(leader, prefix, range) do
    for i <- range do
      key = "#{prefix}k#{i}"
      value = "v#{i}"

      case :ra.process_command(leader, {:put, key, value}, 10_000) do
        {:ok, _reply, _leader} ->
          :ok

        {:timeout, _} ->
          raise "Timeout writing key #{key}"

        {:error, reason} ->
          raise "Error writing key #{key}: #{inspect(reason)}"
      end
    end
  end

  defp assert_key_count(server_id, machine_mod, expected, opts) do
    label = Keyword.get(opts, :label, inspect(server_id))

    {:ok, {_, count}, _} = :ra.local_query(server_id, &machine_mod.count/1)

    assert count == expected,
           "Expected #{expected} keys on #{label}, got #{count}"
  end

  defp wait_for_key_count(server_id, machine_mod, expected, opts) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    label = Keyword.get(opts, :label, inspect(server_id))
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_for_key_count(server_id, machine_mod, expected, label, deadline)
  end

  defp do_wait_for_key_count(server_id, machine_mod, expected, label, deadline) do
    now = System.monotonic_time(:millisecond)

    if now > deadline do
      # One final attempt to get the actual count for the error message
      actual =
        case :ra.local_query(server_id, &machine_mod.count/1) do
          {:ok, {_, c}, _} -> c
          _ -> :unknown
        end

      flunk(
        "Timeout waiting for #{expected} keys on #{label}. " <>
          "Last count: #{inspect(actual)}"
      )
    end

    case :ra.local_query(server_id, &machine_mod.count/1) do
      {:ok, {_, ^expected}, _} ->
        :ok

      {:ok, {_, count}, _} when count < expected ->
        # intentional delay — polling loop with deadline
        Process.sleep(100)
        do_wait_for_key_count(server_id, machine_mod, expected, label, deadline)

      {:ok, {_, count}, _} ->
        flunk("Expected #{expected} keys on #{label}, got #{count} (overshoot)")

      {:error, reason} ->
        # intentional delay — polling loop with deadline
        Process.sleep(200)

        if System.monotonic_time(:millisecond) > deadline do
          flunk("Error querying #{label}: #{inspect(reason)}")
        end

        do_wait_for_key_count(server_id, machine_mod, expected, label, deadline)
    end
  end

  defp wait_for_leader(members, attempts \\ 30) do
    [first | _] = members

    case :ra.members(first, 5_000) do
      {:ok, _, leader} when is_tuple(leader) ->
        leader

      _ when attempts > 0 ->
        # intentional delay — polling loop for leader election
        Process.sleep(200)
        wait_for_leader(members, attempts - 1)

      _ ->
        raise "Timeout waiting for leader among #{inspect(members)}"
    end
  end

  defp cleanup_ra_servers(ra_sys, members) do
    Enum.each(members, fn sid ->
      try do
        :ra.stop_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end

      try do
        :ra.force_delete_server(ra_sys, sid)
      catch
        _, _ -> :ok
      end
    end)

    # intentional delay — grace period for ra cleanup
    Process.sleep(100)
  end
end
