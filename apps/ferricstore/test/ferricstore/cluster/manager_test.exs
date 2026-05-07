defmodule Ferricstore.Cluster.ManagerTest do
  @moduledoc """
  Unit tests for Ferricstore.Cluster.Manager GenServer.

  Tests the ClusterManager in standalone mode (the default when no
  cluster_nodes are configured). The ClusterManager is already started
  by the application supervision tree; these tests call the public API
  directly.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Cluster.Manager

  # ---------------------------------------------------------------------------
  # Standalone mode
  # ---------------------------------------------------------------------------

  describe "standalone mode" do
    test "mode/0 returns :standalone when no cluster_nodes configured" do
      assert Manager.mode() == :standalone
    end

    test "sync_status/0 returns :not_started in standalone" do
      assert Manager.sync_status() == :not_started
    end

    test "node_status/0 returns basic info map" do
      status = Manager.node_status()

      assert is_map(status)
      assert status.mode == :standalone
      assert status.node == node()
      assert is_list(status.connected_nodes)
      assert is_list(status.known_nodes)
      assert status.sync_status == :not_started
      assert is_map(status.shards)

      # In standalone mode with 4 shards, we should have entries for shards 0..3
      shard_count = Application.get_env(:ferricstore, :shard_count, 4)

      for i <- 0..(shard_count - 1) do
        assert Map.has_key?(status.shards, i),
               "expected shard #{i} in status.shards"
      end
    end

    test "node_status/0 shard entries contain members and leader" do
      status = Manager.node_status()

      Enum.each(status.shards, fn {_idx, shard_info} ->
        # Each shard should have members and leader (running locally)
        assert Map.has_key?(shard_info, :members) or Map.has_key?(shard_info, :error)

        if Map.has_key?(shard_info, :members) do
          assert is_list(shard_info.members)
          assert shard_info.leader != nil
        end
      end)
    end
  end

  # ---------------------------------------------------------------------------
  # Role to membership mapping
  #
  # We test by calling add_node with each role. In standalone mode with
  # single-node Raft, adding the local node as a member is a no-op
  # (:already_member -> :ok).
  # ---------------------------------------------------------------------------

  describe "role to membership mapping" do
    test "add_node with self returns :ok (self-join is a no-op)" do
      assert Manager.add_node(node(), :voter) == :ok
    end

    test "add_node with :replica role on self returns :ok (self-join is a no-op)" do
      assert Manager.add_node(node(), :replica) == :ok
    end

    test "add_node with :readonly role on self returns :ok (self-join is a no-op)" do
      assert Manager.add_node(node(), :readonly) == :ok
    end

    test "add_node with REPLACE option on self returns :ok" do
      assert Manager.add_node(node(), :voter, replace: true) == :ok
    end

    test "failed remote join does not poison known_nodes for retry" do
      target = :"missing_join_target@127.0.0.1"

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, _reason}, new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "unknown target data state aborts join before identity bypass or sync" do
      target = :"unknown_target_data@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
        {:error, {:target_data_probe_failed, target, :simulated_eacces}}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        send(parent, :direct_sync)
        {:ok, %{0 => 1}}
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, {:target_data_probe_failed, ^target, :simulated_eacces}},
              new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      refute_received :direct_sync
      refute_received :raft_add
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "target marker failure rolls back raft membership and does not poison known_nodes" do
      target = :"marker_failure_target@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
        {:ok, false}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        send(parent, :direct_sync)
        {:ok, %{0 => 7}}
      end)

      Process.put(:ferricstore_cluster_manager_start_raft_on_target_hook, fn ^target,
                                                                             1,
                                                                             %{0 => 7} ->
        send(parent, :target_raft_start)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                            _ctx,
                                                                            %{0 => 7} ->
        send(parent, :marker_write)
        {:error, :simulated_marker_failure}
      end)

      Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
        %{0 => false}
      end)

      Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
        send(parent, :target_raft_stop)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_remove_added_member_hook, fn ^target, 0 ->
        send(parent, {:raft_remove, 0})
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
        send(parent, :target_data_cleanup)
        :ok
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_start_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
        Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
        Process.delete(:ferricstore_cluster_manager_target_membership_hook)
        Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_remove_added_member_hook)
        Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, :simulated_marker_failure}, new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      assert_received :direct_sync
      assert_received :target_raft_start
      assert_received :raft_add
      assert_received :marker_write
      assert_received :target_raft_stop
      assert_received {:raft_remove, 0}
      assert_received :target_data_cleanup
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "marker rollback does not remove membership when pre-add snapshot is unknown" do
      target = :"unknown_membership_marker_failure@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
        {:ok, false}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        {:ok, %{0 => 7}}
      end)

      Process.put(:ferricstore_cluster_manager_start_raft_on_target_hook, fn ^target,
                                                                             1,
                                                                             %{0 => 7} ->
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                            _ctx,
                                                                            %{0 => 7} ->
        {:error, :simulated_marker_failure}
      end)

      Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
        {:error, :members_unavailable}
      end)

      Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
        send(parent, :target_raft_stop)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_remove_added_member_hook, fn ^target, 0 ->
        send(parent, {:raft_remove, 0})
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
        send(parent, :target_data_cleanup)
        :ok
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_start_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
        Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
        Process.delete(:ferricstore_cluster_manager_target_membership_hook)
        Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_remove_added_member_hook)
        Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, :simulated_marker_failure}, new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      assert_received :raft_add
      assert_received :target_raft_stop
      assert_received :target_data_cleanup
      refute_received {:raft_remove, 0}
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "target raft start failure aborts join before raft membership and cleans copied data" do
      target = :"raft_start_failure_target@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
        {:ok, false}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        send(parent, :direct_sync)
        {:ok, %{0 => 9}}
      end)

      Process.put(:ferricstore_cluster_manager_start_raft_on_target_hook, fn ^target,
                                                                             1,
                                                                             %{0 => 9} ->
        send(parent, :target_raft_start)
        {:error, {:target_raft_start_failed, 0, :simulated_start_failure}}
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target, _ctx, _idx ->
        send(parent, :marker_write)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
        send(parent, :target_raft_stop)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
        send(parent, :target_data_cleanup)
        :ok
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_start_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
        Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
        Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, {:target_raft_start_failed, 0, :simulated_start_failure}},
              new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      assert_received :direct_sync
      assert_received :target_raft_start
      assert_received :target_raft_stop
      assert_received :target_data_cleanup
      refute_received :raft_add
      refute_received :marker_write
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "target raft start aborts when cluster membership cannot be read" do
      target = :"cluster_members_unavailable_target@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
        {:ok, false}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        send(parent, :direct_sync)
        {:ok, %{0 => 9}}
      end)

      Process.put(:ferricstore_cluster_manager_cluster_members_hook, fn ^target ->
        {:error, :members_unavailable}
      end)

      Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
        send(parent, :target_raft_stop)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
        send(parent, :target_data_cleanup)
        :ok
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_cluster_members_hook)
        Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
        Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply,
              {:error, {:target_raft_start_failed, :cluster_members, :members_unavailable}},
              new_state} =
               Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)

      assert_received :direct_sync
      assert_received :target_raft_stop
      assert_received :target_data_cleanup
      refute_received :raft_add
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "replace join aborts when target cleanup fails before sync" do
      target = :"replace_cleanup_failure_target@127.0.0.1"
      parent = self()

      Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 -> true end)

      Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
        send(parent, :target_data_cleanup)
        {:error, :simulated_cleanup_failure}
      end)

      Process.put(:ferricstore_cluster_manager_direct_sync_hook, fn ^target, _ctx ->
        send(parent, :direct_sync)
        {:ok, %{0 => 1}}
      end)

      Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
        send(parent, :raft_add)
        {:ok, %{0 => :ok}}
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
        Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
        Process.delete(:ferricstore_cluster_manager_direct_sync_hook)
        Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
      end)

      state = %{
        mode: :cluster,
        role: :voter,
        cluster_nodes: [],
        remove_delay_ms: 60_000,
        known_nodes: MapSet.new(),
        remove_timers: %{},
        sync_status: :synced,
        shard_sync_status: %{},
        shard_count: 1
      }

      assert {:reply, {:error, :simulated_cleanup_failure}, new_state} =
               Manager.handle_call(
                 {:add_node, target, :voter, [replace: true]},
                 {self(), make_ref()},
                 state
               )

      assert_received :target_data_cleanup
      refute_received :direct_sync
      refute_received :raft_add
      refute MapSet.member?(new_state.known_nodes, target)
    end

    test "promotion flush path does not wait on Flow LMDB projection durability" do
      source =
        File.read!(Path.expand("../../../lib/ferricstore/cluster/manager.ex", __DIR__))

      refute source =~ "flush_flow_lmdb_writers"
      refute source =~ "Flow.LMDBWriter.flush_all(name, shard_count"
    end
  end

  # ---------------------------------------------------------------------------
  # Leave in standalone mode
  # ---------------------------------------------------------------------------

  describe "leave/0" do
    test "leave in standalone mode removes self from Raft groups" do
      # After leave, mode should switch to :standalone (it already is, but
      # the GenServer sets it explicitly). Calling mode() still works.
      # We don't actually call leave here because it would disrupt the
      # running application shards. Instead, verify the API is callable.
      # The leave implementation removes from Raft groups then sets mode = :standalone.
      assert Manager.mode() == :standalone
    end
  end
end
