defmodule Ferricstore.Cluster.ManagerTest.Sections.StandaloneMode do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Cluster.Manager

      describe "standalone mode" do
        test "init resolves automatic shard count to the published runtime value" do
          previous_shard_count = Application.get_env(:ferricstore, :shard_count)
          Application.put_env(:ferricstore, :shard_count, 0)

          on_exit(fn ->
            case previous_shard_count do
              nil -> Application.delete_env(:ferricstore, :shard_count)
              count -> Application.put_env(:ferricstore, :shard_count, count)
            end
          end)

          expected =
            :persistent_term.get(:ferricstore_shard_count, System.schedulers_online())

          assert {:ok, state} = Manager.init([])
          assert state.shard_count == expected
          assert state.shard_count > 0
        end

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

        test "node_status/1 supports bounded shard membership probes" do
          status = Manager.node_status(1_000)

          assert is_map(status)
          assert status.mode == :standalone
          assert status.node == node()
          assert is_map(status.shards)
        end

        test "node_status probes shards concurrently" do
          parent = self()
          release_ref = make_ref()
          probe_count = min(System.schedulers_online(), 4)

          state = %{
            mode: :cluster,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new(),
            remove_timers: %{},
            sync_status: :synced,
            shard_sync_status: %{},
            shard_count: probe_count
          }

          status_task =
            Task.async(fn ->
              Process.put(
                :ferricstore_cluster_manager_node_status_members_hook,
                fn shard_idx, _timeout ->
                  send(parent, {:membership_probe_started, shard_idx, self()})

                  receive do
                    {:release_membership_probe, ^release_ref} ->
                      {:ok, [{:member, shard_idx}], {:leader, shard_idx}}
                  end
                end
              )

              Manager.handle_call(
                {:node_status, 5_000},
                {self(), make_ref()},
                state
              )
            end)

          probe_pids =
            for shard_idx <- 0..(probe_count - 1) do
              assert_receive {:membership_probe_started, ^shard_idx, probe_pid}, 5_000
              probe_pid
            end

          Enum.each(probe_pids, fn probe_pid ->
            send(probe_pid, {:release_membership_probe, release_ref})
          end)

          assert {:reply, status, ^state} = Task.await(status_task, 7_000)

          last_shard = probe_count - 1

          assert status.shards[last_shard] == %{
                   members: [{:member, last_shard}],
                   leader: {:leader, last_shard}
                 }
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

      describe "role to membership mapping part 1" do
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

        test "add_node rejects unsupported roles without crashing the manager" do
          state = %{
            mode: :standalone,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new(),
            remove_timers: %{},
            sync_status: :not_started,
            shard_sync_status: %{},
            shard_count: 1
          }

          assert {:reply, {:error, {:invalid_cluster_role, :observer}}, ^state} =
                   Manager.handle_call(
                     {:add_node, :"candidate@127.0.0.1", :observer, []},
                     {self(), make_ref()},
                     state
                   )
        end

        test "failed remote join does not poison known_nodes for retry" do
          target = :"missing_join_target@127.0.0.1"

          state = %{
            mode: :standalone,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new(),
            remove_timers: %{},
            sync_status: :not_started,
            shard_sync_status: %{},
            shard_count: 1
          }

          assert {:reply, {:error, _reason}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "WARaft remote join uses the WARaft add/barrier/marker path" do
          target = :"waraft_join_target@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            {:ok, %{0 => false}}
          end)

          Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
            send(parent, :target_raft_stop_called)
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_start_raft_on_target_hook, fn ^target,
                                                                                 1,
                                                                                 _idx ->
            send(parent, :target_raft_start_called)
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :backend_add_called)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :backend_barrier_called)
            {:ok, %{0 => 42}}
          end)

          Process.put(:ferricstore_cluster_manager_write_target_marker_hook, fn ^target,
                                                                                _ctx,
                                                                                %{0 => 42} ->
            send(parent, :marker_written)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
            Process.delete(:ferricstore_cluster_manager_start_raft_on_target_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
            Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
          end)

          state = %{
            mode: :standalone,
            role: :voter,
            cluster_nodes: [],
            remove_delay_ms: 60_000,
            known_nodes: MapSet.new(),
            remove_timers: %{},
            sync_status: :not_started,
            shard_sync_status: %{},
            shard_count: 1
          }

          assert {:reply, :ok, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_receive :backend_add_called
          assert_receive :backend_barrier_called
          assert_receive :marker_written
          refute_received :target_raft_stop_called
          refute_received :target_raft_start_called
          assert MapSet.member?(new_state.known_nodes, target)
          assert new_state.mode == :cluster
          assert new_state.sync_status == :synced
        end

        test "WARaft partial add rolls back only shards added by this failed attempt" do
          target = :"waraft_partial_add_target@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 2 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_target_membership_hook, fn ^target, _state ->
            {:ok, %{0 => false, 1 => false}}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            {{:error, {:partial_add, %{0 => :ok, 1 => {:error, :boom}}}},
             %{0 => :ok, 1 => {:error, :boom}}}
          end)

          Process.put(:ferricstore_cluster_manager_remove_added_member_hook, fn ^target,
                                                                                shard_idx ->
            send(parent, {:remove_added_member, shard_idx})
            :ok
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 2 ->
            send(parent, :target_cleanup)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
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

          assert {:reply, {:error, {:partial_add, %{0 => :ok, 1 => {:error, :boom}}}}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          assert_receive {:remove_added_member, 0}
          refute_received {:remove_added_member, 1}
          assert_receive :target_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "target data probe treats blob side-channel files as existing shard data" do
          root =
            Path.join(
              System.tmp_dir!(),
              "cluster_manager_blob_probe_#{System.unique_integer([:positive])}"
            )

          blob_dir = Ferricstore.DataDir.blob_shard_path(root, 0)

          on_exit(fn -> File.rm_rf!(root) end)

          File.mkdir_p!(blob_dir)
          File.write!(Path.join(blob_dir, "payload.blob"), "large-payload")

          assert {:ok, true} = Manager.__target_shard_has_data_for_test__(node(), root, 0)
        end

        test "target cleanup removes shard-owned side stores" do
          root =
            Path.join(
              System.tmp_dir!(),
              "cluster_manager_blob_cleanup_#{System.unique_integer([:positive])}"
            )

          on_exit(fn -> File.rm_rf!(root) end)

          File.mkdir_p!(Ferricstore.DataDir.shard_data_path(root, 0))
          File.mkdir_p!(Path.join([root, "dedicated", "shard_0", "hash:abc"]))
          File.mkdir_p!(Path.join([root, "blob", "shard_0", "aa"]))
          File.mkdir_p!(Path.join([root, "prob", "shard_0", "filter:abc"]))
          File.mkdir_p!(Path.join([root, "ra", "server"]))
          File.mkdir_p!(Path.join([root, "waraft", "ferricstore_waraft_backend.1"]))
          File.write!(Path.join([root, "data", "shard_0", "00000.log"]), "data")

          File.write!(
            Path.join([root, "dedicated", "shard_0", "hash:abc", "00000.log"]),
            "dedicated"
          )

          File.write!(Path.join([root, "blob", "shard_0", "aa", "payload.blob"]), "blob")
          File.write!(Path.join([root, "prob", "shard_0", "filter:abc", "00000.log"]), "prob")
          File.write!(Path.join([root, "ra", "server", "state"]), "stale-ra")

          File.write!(
            Path.join([root, "waraft", "ferricstore_waraft_backend.1", "state"]),
            "waraft"
          )

          File.write!(Ferricstore.ReplicationMode.marker_path(root), "cluster-marker")

          File.write!(
            Ferricstore.ReplicationMode.marker_path(root) <> ".tmp",
            "partial-cluster-marker"
          )

          assert :ok = Manager.__cleanup_target_data_dir_for_test__(node(), root, 1)

          refute File.exists?(Path.join([root, "data", "shard_0"]))
          refute File.exists?(Path.join(root, "dedicated"))
          refute File.exists?(Path.join(root, "blob"))
          refute File.exists?(Path.join(root, "prob"))
          refute File.exists?(Path.join(root, "ra"))
          refute File.exists?(Path.join(root, "waraft"))
          refute File.exists?(Ferricstore.ReplicationMode.marker_path(root))
          refute File.exists?(Ferricstore.ReplicationMode.marker_path(root) <> ".tmp")
        end

        test "target cleanup rejects intermediate symlinks without deleting outside data" do
          root =
            Path.join(
              System.tmp_dir!(),
              "cluster_manager_symlink_cleanup_#{System.unique_integer([:positive])}"
            )

          outside = root <> "_outside"
          sentinel = Path.join([outside, "shard_0", "sentinel"])

          on_exit(fn ->
            File.rm_rf!(root)
            File.rm_rf!(outside)
          end)

          File.mkdir_p!(Path.dirname(sentinel))
          File.write!(sentinel, "keep")
          File.mkdir_p!(root)
          File.ln_s!(outside, Path.join(root, "data"))

          assert {:error, {:target_cleanup_failed, unsafe_path, _reason}} =
                   Manager.__cleanup_target_data_dir_for_test__(node(), root, 1)

          assert unsafe_path == Ferricstore.DataDir.shard_data_path(root, 0)
          assert File.read!(sentinel) == "keep"
        end

        test "target cleanup fails when the replication marker cannot be removed" do
          root =
            Path.join(
              System.tmp_dir!(),
              "cluster_manager_marker_cleanup_#{System.unique_integer([:positive])}"
            )

          marker_path = Ferricstore.ReplicationMode.marker_path(root)

          on_exit(fn -> File.rm_rf!(root) end)

          File.mkdir_p!(marker_path)

          assert {:error, {:target_cleanup_failed, ^marker_path, _reason}} =
                   Manager.__cleanup_target_data_dir_for_test__(node(), root, 1)
        end

        test "unknown target data state aborts join before identity or snapshot mutation" do
          target = :"unknown_target_data@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:error, {:target_data_probe_failed, target, :simulated_eacces}}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
            send(parent, :target_raft_stop)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
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
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          refute_received :raft_add
          refute_received :target_raft_stop
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "identity rejection does not stop or delete target raft" do
          target = :"foreign_cluster_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: local_cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, true}
          end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: "foreign-#{local_cluster_id}"}}
          end)

          Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
            send(parent, :target_raft_stop)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
            Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
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
                   {:target_cluster_id_mismatch, ^target, ^local_cluster_id,
                    "foreign-" <> _local_cluster_id}}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          refute_received :target_raft_stop
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "unsupported standalone target marker is rejected without replace even with same cluster id" do
          target = :"standalone_same_cluster_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: local_cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, true}
          end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: local_cluster_id, replication_mode: :standalone}}
          end)

          Process.put(:ferricstore_cluster_manager_stop_raft_on_target_hook, fn ^target, 1 ->
            send(parent, :target_raft_stop)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
            Process.delete(:ferricstore_cluster_manager_stop_raft_on_target_hook)
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
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          refute_received :target_raft_stop
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "replace join rejects foreign target data before cleanup" do
          target = :"replace_foreign_target@127.0.0.1"
          parent = self()
          ctx = FerricStore.Instance.get(:default)
          {:ok, %{cluster_id: local_cluster_id}} = Ferricstore.ReplicationMode.read(ctx.data_dir)

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, true}
          end)

          Process.put(:ferricstore_cluster_manager_read_target_cluster_state_hook, fn ^target ->
            {:ok, %{cluster_id: "foreign-" <> local_cluster_id, replication_mode: :raft}}
          end)

          Process.put(:ferricstore_cluster_manager_cleanup_target_data_hook, fn ^target, 1 ->
            send(parent, :target_data_cleanup)
            :ok
          end)

          on_exit(fn ->
            Process.delete(:ferricstore_cluster_manager_target_has_data_hook)
            Process.delete(:ferricstore_cluster_manager_read_target_cluster_state_hook)
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
                   {:target_cluster_id_mismatch, ^target, ^local_cluster_id,
                    "foreign-" <> _local_cluster_id}}, new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, [replace: true]},
                     {self(), make_ref()},
                     state
                   )

          refute_received :target_data_cleanup
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "remote-driven auto-join is disabled by default for unknown nodes" do
          old_auto_join = Application.get_env(:ferricstore, :cluster_auto_join)
          Application.put_env(:ferricstore, :cluster_auto_join, false)

          on_exit(fn ->
            case old_auto_join do
              nil -> Application.delete_env(:ferricstore, :cluster_auto_join)
              value -> Application.put_env(:ferricstore, :cluster_auto_join, value)
            end
          end)

          target = :"remote_auto_join_disabled_target@127.0.0.1"

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

          assert {:noreply, new_state} = Manager.handle_info({:nodeup, target, []}, state)
          refute MapSet.member?(new_state.known_nodes, target)
        end

        test "target marker failure rolls back raft membership and does not poison known_nodes" do
          target = :"marker_failure_target@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
          end)

          Process.put(:ferricstore_cluster_manager_do_add_node_hook, fn ^target, :voter, _state ->
            send(parent, :raft_add)
            {:ok, %{0 => :ok}}
          end)

          Process.put(:ferricstore_cluster_manager_waraft_barrier_indices_hook, fn 1 ->
            send(parent, :barrier_read)
            {:ok, %{0 => 7}}
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
            Process.delete(:ferricstore_cluster_manager_do_add_node_hook)
            Process.delete(:ferricstore_cluster_manager_waraft_barrier_indices_hook)
            Process.delete(:ferricstore_cluster_manager_write_target_marker_hook)
            Process.delete(:ferricstore_cluster_manager_target_membership_hook)
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

        test "unknown pre-add membership aborts before target mutation or raft add" do
          target = :"unknown_membership_marker_failure@127.0.0.1"
          parent = self()

          Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn ^target, 1 ->
            {:ok, false}
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

          assert {:reply,
                  {:error, {:target_membership_snapshot_failed, ^target, :members_unavailable}},
                  new_state} =
                   Manager.handle_call(
                     {:add_node, target, :voter, []},
                     {self(), make_ref()},
                     state
                   )

          refute_received :raft_add
          refute_received :target_raft_stop
          refute_received :target_data_cleanup
          refute_received {:raft_remove, 0}
          refute MapSet.member?(new_state.known_nodes, target)
        end
      end
    end
  end
end
