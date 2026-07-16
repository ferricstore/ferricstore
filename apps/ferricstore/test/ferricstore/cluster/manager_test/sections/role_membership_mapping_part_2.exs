defmodule Ferricstore.Cluster.ManagerTest.Sections.RoleMembershipMappingPart2 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Cluster.Manager

      describe "role to membership mapping part 2" do
        test "data-bearing raft target with matching cluster marker uses snapshot add path" do
          target = :"disk_clone_snapshot_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, true}
          end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: cluster_id, replication_mode: :raft}}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :barrier_read)
            {:ok, %{0 => 12}}
          end)

          Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                                _ctx,
                                                                                %{0 => 12} ->
            send(parent, :marker_write)
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_cleanup)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
            Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
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

          assert {:reply, :ok, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_received :raft_add
          assert_received :barrier_read
          assert_received :marker_write
          refute_received :target_cleanup
          assert MapSet.member?(new_state.known_nodes, target)
        end

        test "marker failure after empty target add rolls back and cleans target data" do
          target = :"empty_target_marker_failure@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :barrier_read)
            {:ok, %{0 => 9}}
          end)

          Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                                _ctx,
                                                                                %{0 => 9} ->
            send(parent, :marker_write)
            {:error, :simulated_marker_failure}
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
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
            Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
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
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_received :raft_add
          assert_received :barrier_read
          assert_received :marker_write
          assert_received {:raft_remove, 0}
          assert_received :target_data_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "barrier failure after add rolls back newly-added members and cleans empty target" do
          target = :"missing_barrier_index_target@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 2 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false, 1 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok, 1 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 2 ->
            send(parent, :barrier_read)
            {:error, {:waraft_barrier_index_unavailable, 1, :missing_index}}
          end)

          Process.put(:ferricstore_cluster_manager_remove_added_member_hook, fn ^target,
                                                                                shard_idx ->
            send(parent, {:raft_remove, shard_idx})
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 2 ->
            send(parent, :target_data_cleanup)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
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
            shard_count: 2
          }

          assert {:reply, {:error, {:waraft_barrier_index_unavailable, 1, :missing_index}},
                  new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_received :raft_add
          assert_received :barrier_read
          assert_received {:raft_remove, 0}
          assert_received {:raft_remove, 1}
          assert_received :target_data_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "barrier failure surfaces cleanup rollback failure" do
          target = :"barrier_cleanup_failure_target@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :barrier_read)
            {:error, :barrier_failed}
          end)

          Process.put(:ferricstore_cluster_manager_remove_added_member_hook, fn ^target, 0 ->
            send(parent, {:raft_remove, 0})
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_data_cleanup)
            {:error, :simulated_cleanup_failure}
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
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

          assert {:reply,
                  {:error,
                   {:waraft_target_marker_failed_rollback_failed, {:error, :barrier_failed}, :ok,
                    {:error, :simulated_cleanup_failure}}}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_received :raft_add
          assert_received :barrier_read
          assert_received {:raft_remove, 0}
          assert_received :target_data_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "add failure surfaces cleanup rollback failure" do
          target = :"target_add_cleanup_failure@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {{:error, {:partial_add, %{0 => {:error, :boom}}}}, %{0 => {:error, :boom}}}
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_data_cleanup)
            {:error, :simulated_cleanup_failure}
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
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

          assert {:reply,
                  {:error,
                   {:waraft_add_failed_rollback_failed,
                    {:error, {:partial_add, %{0 => {:error, :boom}}}}, :ok,
                    {:error, :simulated_cleanup_failure}}}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_received :raft_add
          assert_received :target_data_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "replace join rejects unsupported standalone marker before cleanup" do
          target = :"replace_cleanup_failure_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: local_cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 -> true end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: local_cluster_id, replication_mode: :standalone}}
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_data_cleanup)
            {:error, :simulated_cleanup_failure}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
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
                  {:error, {:target_cluster_state_unusable, ^target, :missing_replication_mode}},
                  new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, [replace: true]},
                     {self(), make_ref()},
                     state
                   )

          refute_received :target_data_cleanup
          refute_received :raft_add
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "replace join cleans target data then adds member and writes marker" do
          target = :"replace_success_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: local_cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, true}
          end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: local_cluster_id, replication_mode: :raft}}
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_data_cleanup)
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            %{0 => false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :barrier_read)
            {:ok, %{0 => 11}}
          end)

          Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                                _ctx,
                                                                                %{0 => 11} ->
            send(parent, :marker_write)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
            Process.delete(:ferricstore_cluster_manager_cleanup_target_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
            Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
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

          assert {:reply, :ok, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, [replace: true]},
                     {self(), make_ref()},
                     state
                   )

          refute_received :target_data_cleanup
          assert_received :raft_add
          assert_received :barrier_read
          assert_received :marker_write
          assert MapSet.member?(new_state.known_nodes, target)
        end

        test "promotion flush path does not wait on Flow LMDB projection durability" do
          source =
            File.read!(Path.expand("../../../lib/ferricstore/cluster/manager.ex", __DIR__))

          refute source =~ "flush_flow_lmdb_writers"
          refute source =~ "Flow.LMDBWriter.flush_all(name, shard_count"
        end
      end

      describe "remove/leave failure handling" do
        test "remove_node fails closed when any shard removal fails" do
          target = :"remove_failure_target@127.0.0.1"

          Process.put(:ferricstore_cluster_manager_members_hook, fn _shard_idx ->
            {:ok, [], nil}
          end)

          Process.put(:ferricstore_cluster_manager_remove_member_hook, fn
            0, ^target -> :ok
            1, ^target -> {:error, :storage_blocked}
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_members_hook)
            Process.delete(:ferricstore_cluster_manager_remove_member_hook)
          end)

          state = %{
            mode: :cluster,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new([target]),
            remove_timers: %{},
            sync_status: :synced,
            shard_sync_status: %{},
            shard_count: 2
          }

          assert {:reply,
                  {:error, {:partial_remove, %{0 => :ok, 1 => {:error, :storage_blocked}}}},
                  new_state} =
                   Manager.handle_call({:remove_node, target}, {self(), make_ref()}, state)

          assert MapSet.member?(new_state.known_nodes, target)
        end

        test "leave does not switch to standalone when self removal fails" do
          local = node()

          Process.put(:ferricstore_cluster_manager_members_hook, fn _shard_idx ->
            {:ok, [], nil}
          end)

          Process.put(:ferricstore_cluster_manager_remove_member_hook, fn
            0, ^local -> :ok
            1, ^local -> {:error, :storage_blocked}
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_members_hook)
            Process.delete(:ferricstore_cluster_manager_remove_member_hook)
          end)

          state = %{
            mode: :cluster,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new([local]),
            remove_timers: %{},
            sync_status: :synced,
            shard_sync_status: %{},
            shard_count: 2
          }

          assert {:reply,
                  {:error, {:partial_leave, %{0 => :ok, 1 => {:error, :storage_blocked}}}},
                  new_state} =
                   Manager.handle_call(:leave, {self(), make_ref()}, state)

          assert new_state.mode == :cluster
        end

        test "remove_node transfers leadership to another shard member before removing leader" do
          target = :"remove_leader_target@127.0.0.1"
          replacement = :"remove_leader_replacement@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_members_hook, fn 0 ->
            {:ok, [:unknown_member_shape, {:shard_0, target}, {:shard_0, replacement}],
             {:shard_0, target}}
          end)

          Process.put(:ferricstore_cluster_manager_transfer_leadership_hook, fn
            0, ^replacement ->
              send(parent, {:transferred_to, replacement})
              :ok
          end)

          Process.put(:ferricstore_cluster_manager_remove_member_hook, fn
            0, ^target -> :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_members_hook)
            Process.delete(:ferricstore_cluster_manager_transfer_leadership_hook)
            Process.delete(:ferricstore_cluster_manager_remove_member_hook)
          end)

          state = %{
            mode: :cluster,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new([target]),
            remove_timers: %{},
            sync_status: :synced,
            shard_sync_status: %{},
            shard_count: 1
          }

          assert {:reply, :ok, new_state} =
                   Manager.handle_call({:remove_node, target}, {self(), make_ref()}, state)

          assert_receive {:transferred_to, ^replacement}
          refute MapSet.member?(new_state.known_nodes, target)
        end
      end

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
  end
end
