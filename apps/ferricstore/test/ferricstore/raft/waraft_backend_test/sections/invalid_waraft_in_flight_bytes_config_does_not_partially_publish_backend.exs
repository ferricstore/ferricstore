defmodule Ferricstore.Raft.WARaftBackendTest.Sections.InvalidWaraftInFlightBytesConfigDoesNotPartiallyPublishBackend do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

      test "invalid WARaft in-flight bytes config does not partially publish backend app env", %{
        ctx: ctx
      } do
        previous_public = Application.get_env(:ferricstore, :waraft_max_inflight_commit_bytes)
        previous_database = Application.get_env(:ferricstore_waraft_backend, :raft_database)

        try do
          Application.put_env(:ferricstore, :waraft_max_inflight_commit_bytes, :bad)
          Application.put_env(:ferricstore_waraft_backend, :raft_database, ~c"sentinel-waraft-db")

          assert_raise ArgumentError, ~r/waraft_max_inflight_commit_bytes/, fn ->
            WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          end

          assert ~c"sentinel-waraft-db" ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_database)

          assert_raise ArgumentError, fn ->
            WARaftBackend.context!(:ferricstore_waraft_backend)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        after
          restore_env(:waraft_max_inflight_commit_bytes, previous_public)
          restore_waraft_app_env(:raft_database, previous_database)
        end
      end

      test "WARaft info cleanup tolerates the app info table already being stopped" do
        assert {:ok, _apps} = Application.ensure_all_started(:wa_raft)
        assert :undefined != :ets.whereis(:wa_raft_info)
        assert :ok = Application.stop(:wa_raft)
        assert :undefined == :ets.whereis(:wa_raft_info)

        try do
          assert true = :wa_raft_info.clear(:ferricstore_waraft_backend, 1, :raft_server_test)
        after
          assert {:ok, _apps} = Application.ensure_all_started(:wa_raft)
        end
      end

      test "invalid WARaft module options fail closed before publishing backend state", %{
        ctx: ctx
      } do
        previous_database = Application.get_env(:ferricstore_waraft_backend, :raft_database)

        try do
          Application.put_env(:ferricstore_waraft_backend, :raft_database, ~c"sentinel-waraft-db")

          assert_raise ArgumentError, ~r/log_module/, fn ->
            WARaftBackend.start(ctx, log_module: :missing_waraft_log_provider)
          end

          assert ~c"sentinel-waraft-db" ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_database)

          assert_raise ArgumentError, fn ->
            WARaftBackend.context!(:ferricstore_waraft_backend)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)

          assert_raise ArgumentError, ~r/label_module/, fn ->
            WARaftBackend.start(ctx,
              log_module: :ferricstore_waraft_spike_segment_log,
              label_module: :missing_label_provider
            )
          end

          assert ~c"sentinel-waraft-db" ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_database)

          assert_raise ArgumentError, fn ->
            WARaftBackend.context!(:ferricstore_waraft_backend)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        after
          restore_waraft_app_env(:raft_database, previous_database)
        end
      end

      test "invalid WARaft module options do not stop an already running backend", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "module-preflight:live", "v1", 0})
        assert "v1" == Router.get(ctx, "module-preflight:live")

        assert_raise ArgumentError, ~r/log_module/, fn ->
          WARaftBackend.start(ctx, log_module: :missing_waraft_log_provider)
        end

        assert %FerricStore.Instance{} = WARaftBackend.context!(:ferricstore_waraft_backend)
        assert Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        assert "v1" == Router.get(ctx, "module-preflight:live")
        assert :ok = WARaftBackend.write(0, {:put, "module-preflight:after", "v2", 0})
        assert "v2" == Router.get(ctx, "module-preflight:after")
      end

      test "bootstrap storage failure during start fails closed and clears backend context", %{
        ctx: ctx
      } do
        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn _path ->
            {:error, :forced_bootstrap_metadata_fsync_failure}
          end)

          assert {:error, _reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert_raise ArgumentError, fn ->
            WARaftBackend.context!(:ferricstore_waraft_backend)
          end

          refute Process.whereis(:raft_sup_ferricstore_waraft_backend_1)
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
        end
      end

      test "compact storage metadata failure does not publish newer journal", %{
        root: root,
        ctx: ctx
      } do
        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            if String.contains?(path, "ferricstore_storage.term.tmp.") do
              {:error, :forced_compact_metadata_fsync_failure}
            else
              :ok
            end
          end)

          assert {:error, _reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert nil ==
                   latest_storage_metadata_journal(waraft_storage_metadata_journal_path(root, 0))
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
        end
      end

      test "FerricStore app env configures WARaft queue and commit knobs", %{ctx: ctx} do
        previous_pending_reads = Application.get_env(:ferricstore, :waraft_max_pending_reads)

        previous_commit_interval =
          Application.get_env(:ferricstore, :waraft_commit_batch_interval_ms)

        previous_commit_max = Application.get_env(:ferricstore, :waraft_commit_batch_max)
        previous_async_append = Application.get_env(:ferricstore, :waraft_async_log_append)

        previous_ra_flush_size =
          Application.get_env(:ferricstore, :ra_low_priority_commands_flush_size)

        previous_ra_app_flush_size =
          Application.get_env(:ra, :low_priority_commands_flush_size)

        previous_backend_pending_reads =
          Application.get_env(:ferricstore_waraft_backend, :raft_max_pending_reads)

        previous_backend_commit_interval =
          Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_interval_ms)

        previous_backend_commit_max =
          Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_max)

        previous_backend_async_append =
          Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_max_pending_reads, 12_345)
          Application.put_env(:ferricstore, :waraft_commit_batch_interval_ms, 7)
          Application.put_env(:ferricstore, :waraft_commit_batch_max, 2048)
          Application.put_env(:ferricstore, :waraft_async_log_append, true)
          Application.put_env(:ferricstore, :ra_low_priority_commands_flush_size, 768)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert 12_345 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_max_pending_reads)

          assert 7 ==
                   Application.get_env(
                     :ferricstore_waraft_backend,
                     :raft_commit_batch_interval_ms
                   )

          assert 2048 ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_commit_batch_max)

          assert true ==
                   Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

          assert 768 == Application.get_env(:ra, :low_priority_commands_flush_size)
        after
          restore_env(:waraft_max_pending_reads, previous_pending_reads)
          restore_env(:waraft_commit_batch_interval_ms, previous_commit_interval)
          restore_env(:waraft_commit_batch_max, previous_commit_max)
          restore_env(:waraft_async_log_append, previous_async_append)
          restore_env(:ra_low_priority_commands_flush_size, previous_ra_flush_size)
          restore_ra_env(:low_priority_commands_flush_size, previous_ra_app_flush_size)
          restore_waraft_app_env(:raft_max_pending_reads, previous_backend_pending_reads)
          restore_waraft_app_env(:raft_commit_batch_interval_ms, previous_backend_commit_interval)
          restore_waraft_app_env(:raft_commit_batch_max, previous_backend_commit_max)
          restore_waraft_app_env(:raft_async_log_append, previous_backend_async_append)
        end
      end

      test "WARaft default commit batch interval uses the production server config" do
        assert 6 == WARaftBackend.default_commit_batch_interval_ms()
      end

      test "WARaft production commit batch cap uses the production server config" do
        assert 10_000 == WARaftBackend.default_commit_batch_max()
      end

      test "WARaft production Ra low-priority flush size uses sustained default" do
        assert 512 == Application.get_env(:ferricstore, :ra_low_priority_commands_flush_size)
      end

      test "WARaft redirect timeouts keep unknown-outcome semantics" do
        assert {:error, :timeout} ==
                 WARaftBackend.__redirect_write_failure_for_test__(:exit, {:erpc, :timeout})

        assert {:error, :leader_unavailable} ==
                 WARaftBackend.__redirect_write_failure_for_test__(:exit, {:erpc, :noconnection})

        assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                 WARaftBackend.__redirect_membership_failure_for_test__(:exit, {:erpc, :timeout})

        assert {:error, :leader_unavailable} ==
                 WARaftBackend.__redirect_membership_failure_for_test__(
                   :exit,
                   {:erpc, :noconnection}
                 )

        assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                 WARaftBackend.__redirect_transfer_failure_for_test__(:exit, {:erpc, :timeout})

        assert {:error, :leader_unavailable} ==
                 WARaftBackend.__redirect_transfer_failure_for_test__(
                   :exit,
                   {:erpc, :noconnection}
                 )
      end

      test "WARaft post-submit not-leader outcome is not redirectable as a fresh write" do
        refute WARaftBackend.__redirectable_write_error_for_test__(
                 {:error, :not_leader_after_submit}
               )

        assert WARaftBackend.__redirectable_write_error_for_test__({:error, :not_leader})
      end

      test "WARaft redirect peer normalization rejects boolean pseudo-nodes" do
        assert :valid_peer@nohost == WARaftBackend.__peer_node_for_test__(:valid_peer@nohost)

        assert :valid_peer@nohost ==
                 WARaftBackend.__peer_node_for_test__(
                   {:raft_identity, :raft_server_ferricstore_waraft_backend_1, :valid_peer@nohost}
                 )

        assert nil == WARaftBackend.__peer_node_for_test__(nil)
        assert nil == WARaftBackend.__peer_node_for_test__(true)
        assert nil == WARaftBackend.__peer_node_for_test__(false)
        assert nil == WARaftBackend.__peer_node_for_test__({:server, true})
        assert nil == WARaftBackend.__peer_node_for_test__({:raft_identity, :server, false})
      end

      test "bootstrap_cluster rejects empty and malformed membership before publishing config", %{
        ctx: ctx
      } do
        assert :ok =
                 WARaftBackend.start(ctx,
                   bootstrap: false,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert {:error, :empty_cluster} = WARaftBackend.bootstrap_cluster([])

        assert {:error, {:invalid_node, "not-a-node"}} =
                 WARaftBackend.bootstrap_cluster(["not-a-node"])

        assert {:error, {:invalid_node, nil}} = WARaftBackend.bootstrap_cluster([nil])
        current_node = node()

        assert {:error, {:duplicate_node, ^current_node}} =
                 WARaftBackend.bootstrap_cluster([current_node, current_node])

        status = WARaftBackend.status(0)
        assert Keyword.get(status, :state) == :stalled
        assert {:ok, {:raft_log_pos, 0, 0}} = WARaftBackend.storage_position(0)
      end

      test "add_member rejects invalid timeouts before membership mutation", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        original_membership = WARaftBackend.membership(0)
        target_node = :waraft_invalid_timeout_target@nohost

        assert {:error, {:invalid_timeout_ms, -1}} =
                 WARaftBackend.add_member(0, target_node, timeout_ms: -1)

        assert {:error, {:invalid_timeout_ms, :bad}} =
                 WARaftBackend.add_participant(0, target_node, timeout_ms: :bad)

        assert original_membership == WARaftBackend.membership(0)
      end

      test "membership mutations reject non-node atoms before config append", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        original_membership = WARaftBackend.membership(0)

        assert {:error, {:invalid_node, nil}} = WARaftBackend.add_member(0, nil)
        assert {:error, {:invalid_node, false}} = WARaftBackend.add_participant(0, false)

        assert {:error, {:invalid_node, true}} =
                 WARaftBackend.adjust_membership(0, :add_participant, true)

        assert original_membership == WARaftBackend.membership(0)
      end

      test "membership mutations reject unknown actions before config append", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        original_membership = WARaftBackend.membership(0)

        assert {:error, {:invalid_membership_action, :replace_everyone}} =
                 WARaftBackend.adjust_membership(0, :replace_everyone, node())

        assert {:error, {:invalid_membership_action, :replace_everyone}} =
                 WARaftBackend.adjust_membership_redirected(
                   0,
                   :replace_everyone,
                   node(),
                   10_000,
                   0
                 )

        assert original_membership == WARaftBackend.membership(0)
      end

      test "redirected public WARaft APIs reject malformed redirect counts", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        original_membership = WARaftBackend.membership(0)

        assert {:error, {:invalid_redirects_left, -1}} =
                 WARaftBackend.write_redirected(0, {:put, "redirect-count:write", "v", 0}, -1)

        assert {:error, {:invalid_redirects_left, :bad}} =
                 WARaftBackend.transfer_leadership_redirected(0, node(), :bad)

        assert {:error, {:invalid_redirects_left, -1}} =
                 WARaftBackend.adjust_membership_redirected(
                   0,
                   :add_participant,
                   node(),
                   10_000,
                   -1
                 )

        assert {:error, {:invalid_redirects_left, :bad}} =
                 WARaftBackend.add_member_redirected(0, node(), 10_000, :bad)

        assert {:error, {:invalid_redirects_left, -1}} =
                 WARaftBackend.add_participant_redirected(0, node(), 10_000, -1)

        assert original_membership == WARaftBackend.membership(0)
        assert nil == Router.get(ctx, "redirect-count:write")
      end

      test "bootstrap_cluster does not cache a conflicting config after bootstrap", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        original_membership = WARaftBackend.membership(0)

        assert {:error, {:already_bootstrapped, _actual_nodes}} =
                 WARaftBackend.bootstrap_cluster([
                   node(),
                   :waraft_conflicting_bootstrap_1@nohost,
                   :waraft_conflicting_bootstrap_2@nohost
                 ])

        assert original_membership == WARaftBackend.membership(0)
        assert :ok = WARaftBackend.write(0, {:put, "bootstrap-conflict:still-live", "v", 0})
        assert "v" == Router.get(ctx, "bootstrap-conflict:still-live")
      end

      test "cached voter extraction ignores non-node atoms from malformed metadata", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        WARaftBackend.cache_config(0, %{
          membership: [
            {:raft_server_ferricstore_waraft_backend_1, nil},
            {:raft_server_ferricstore_waraft_backend_1, false},
            {:raft_server_ferricstore_waraft_backend_1, node()}
          ]
        })

        assert :ok = WARaftBackend.write(0, {:put, "cache-config:valid-voter", "v", 0})
        assert "v" == Router.get(ctx, "cache-config:valid-voter")
      end

      test "cached voter extraction accepts participants-only WARaft configs", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        voter_a = :"participants_cache_a@127.0.0.1"
        voter_b = :"participants_cache_b@127.0.0.1"

        WARaftBackend.cache_config(0, %{
          participants: [
            {:raft_server_ferricstore_waraft_backend_1, voter_a},
            {:raft_server_ferricstore_waraft_backend_1, voter_b}
          ],
          witness: []
        })

        assert {:ok, members, _leader} = WARaftBackend.cached_members(0)

        assert Enum.sort(members) ==
                 Enum.sort([
                   {:raft_server_ferricstore_waraft_backend_1, voter_a},
                   {:raft_server_ferricstore_waraft_backend_1, voter_b}
                 ])
      end

      test "public WARaft APIs reject invalid shard indices without crashing", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.write(-1, {:put, "invalid-shard:write", "v", 0})

        assert [
                 {:error, {:invalid_shard_index, -1}},
                 :ok
               ] =
                 WARaftBackend.write_many([
                   {-1, {:put, "invalid-shard:batch-bad", "v", 0}},
                   {0, {:put, "invalid-shard:batch-good", "v", 0}}
                 ])

        assert "v" == Router.get(ctx, "invalid-shard:batch-good")

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.write_batch(-1, [{:put, "invalid-shard:write-batch", "v", 0}])

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.write_put_batch(-1, [{"invalid-shard:put-batch", "v", 0}])

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.write_delete_batch(-1, ["invalid-shard:delete-batch"])

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.local_get(-1, "invalid-shard:local-get")

        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.status(-1)
        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.membership(-1)
        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.storage_position(-1)
        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.create_snapshot(-1)

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.install_snapshot(-1, "/tmp/no-snapshot", {:raft_log_pos, 0, 0})

        assert {:error, {:invalid_snapshot_path, nil}} =
                 WARaftBackend.install_snapshot(0, nil, {:raft_log_pos, 0, 0})

        assert {:error, {:invalid_snapshot_position, :bad_position}} =
                 WARaftBackend.install_snapshot(0, "/tmp/no-snapshot", :bad_position)

        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.trigger_election(-1)

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.transfer_leadership(-1, node())

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.add_member(-1, :invalid_shard_target@nohost)

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.add_participant(-1, :invalid_shard_target@nohost)

        assert {:error, {:invalid_shard_index, -1}} =
                 WARaftBackend.adjust_membership(
                   -1,
                   :add_participant,
                   :invalid_shard_target@nohost
                 )

        assert {:error, {:invalid_shard_index, -1}} = WARaftBackend.peer_ready(-1, node())
        assert 0 == WARaftBackend.inflight_commit_bytes(-1)
      end

      test "out-of-range shard write fails closed when in-flight byte cap is enabled", %{ctx: ctx} do
        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   max_inflight_commit_bytes: 128
                 )

        assert {:error, {:invalid_shard_index, 1}} =
                 WARaftBackend.write(1, {:put, "invalid-shard:over-cap", "v", 0})

        assert 0 == WARaftBackend.inflight_commit_bytes(1)
        assert :ok = WARaftBackend.write(0, {:put, "invalid-shard:cap-good", "v", 0})
        assert "v" == Router.get(ctx, "invalid-shard:cap-good")
      end

      test "public WARaft batch write APIs reject malformed payloads without crashing", %{
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert {:error, {:invalid_write_many_entries, :not_a_list}} =
                 WARaftBackend.write_many(:not_a_list)

        assert [
                 {:error, {:invalid_write_many_entry, :bad_entry}},
                 :ok
               ] =
                 WARaftBackend.write_many([
                   :bad_entry,
                   {0, {:put, "invalid-payload:write-many-good", "v", 0}}
                 ])

        assert "v" == Router.get(ctx, "invalid-payload:write-many-good")

        assert {:error, {:invalid_command_batch, :not_a_list}} =
                 WARaftBackend.write_batch(0, :not_a_list)

        assert {:error, {:invalid_put_batch, :not_a_list}} =
                 WARaftBackend.write_put_batch(0, :not_a_list)

        assert {:error, {:invalid_delete_batch, :not_a_list}} =
                 WARaftBackend.write_delete_batch(0, :not_a_list)

        assert {:error, {:invalid_key, :not_a_key}} =
                 WARaftBackend.local_get(0, :not_a_key)
      end

      test "configured namespace windows coalesce WARaft writes before segment apply", %{ctx: ctx} do
        handler_id = {__MODULE__, :namespace_window_flush, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :batcher, :slot_flush],
          &__MODULE__.handle_namespace_batcher_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        try do
          assert :ok = Ferricstore.NamespaceConfig.set("waraftns", "window_ms", "25")
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write(0, {:put, "waraftns:key#{i}", "v#{i}", 0})
              end)
            end

          assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))

          assert_receive {:waraft_namespace_batcher_flush,
                          [:ferricstore, :waraft, :batcher, :slot_flush], %{batch_size: 2},
                          %{prefix: "waraftns"}},
                         1_000

          assert "v1" == Router.get(ctx, "waraftns:key1")
          assert "v2" == Router.get(ctx, "waraftns:key2")
        after
          Ferricstore.NamespaceConfig.reset("waraftns")
        end
      end

      test "configured namespace SET windows commit with the compact put batch shape", %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

        try do
          Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})
          assert :ok = Ferricstore.NamespaceConfig.set("waraftput", "window_ms", "25")
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write(0, {:put, "waraftput:key#{i}", "v#{i}", 0})
              end)
            end

          assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, ref,
                          worker},
                         1_000

          send(worker, {ref, :continue})
          assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))
          assert "v1" == Router.get(ctx, "waraftput:key1")
          assert "v2" == Router.get(ctx, "waraftput:key2")
        after
          restore_env(:waraft_backend_batcher_call_hook, previous_hook)
          Ferricstore.NamespaceConfig.reset("waraftput")
        end
      end

      test "configured namespace DEL windows commit with the compact delete batch shape", %{
        ctx: ctx
      } do
        previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

        try do
          assert :ok = Ferricstore.NamespaceConfig.set("waraftdel", "window_ms", "25")

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 1,
                     commit_batch_max: 10_000
                   )

          assert :ok == WARaftBackend.write(0, {:put, "waraftdel:key1", "v1", 0})
          assert :ok == WARaftBackend.write(0, {:put, "waraftdel:key2", "v2", 0})
          Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write(0, {:delete, "waraftdel:key#{i}"})
              end)
            end

          assert_receive {:waraft_backend_batcher_call, :__commit_delete_batch_direct__, ref,
                          worker},
                         1_000

          send(worker, {ref, :continue})
          assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))
          assert nil == Router.get(ctx, "waraftdel:key1")
          assert nil == Router.get(ctx, "waraftdel:key2")
        after
          restore_env(:waraft_backend_batcher_call_hook, previous_hook)
          Ferricstore.NamespaceConfig.reset("waraftdel")
        end
      end

      test "configured namespace windows ignore stale flush messages", %{ctx: ctx} do
        try do
          assert :ok = Ferricstore.NamespaceConfig.set("stale-win", "window_ms", "200")
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          task =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, "stale-win:key", "v", 0})
            end)

          Process.sleep(10)

          send(
            Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)),
            {:flush, "stale-win"}
          )

          ref = task.ref
          refute_receive {^ref, _reply}, 50
          assert :ok = Task.await(task, 1_000)
          assert "v" == Router.get(ctx, "stale-win:key")
        after
          Ferricstore.NamespaceConfig.reset("stale-win")
        end
      end

      test "hot put batches ignore stale flush messages", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 200)
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          task =
            Task.async(fn ->
              WARaftBackend.write_put_batch(0, [{"stale-hot:key", "v", 0}])
            end)

          Process.sleep(10)

          send(
            Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)),
            :flush_hot_put_batch
          )

          ref = task.ref
          refute_receive {^ref, _reply}, 50
          assert {:ok, [:ok]} = Task.await(task, 1_000)
          assert "v" == Router.get(ctx, "stale-hot:key")
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
        end
      end

      test "hot delete batches ignore stale flush messages", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 200)
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert {:ok, [:ok]} =
                   WARaftBackend.write_put_batch(0, [{"stale-hot-delete:key", "v", 0}])

          task =
            Task.async(fn ->
              WARaftBackend.write_delete_batch(0, ["stale-hot-delete:key"])
            end)

          Process.sleep(10)

          send(
            Process.whereis(Ferricstore.Raft.WARaftBackend.Batcher.name(0)),
            :flush_hot_delete_batch
          )

          ref = task.ref
          refute_receive {^ref, _reply}, 50
          assert {:ok, [:ok]} = Task.await(task, 1_000)
          assert nil == Router.get(ctx, "stale-hot-delete:key")
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
        end
      end

      test "committed hot batch terms keep ordered per-command replies", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert {:ok, [:ok, :ok]} =
                 WARaftBackend.write_put_batch(0, [
                   {"batch:k1", "v1", 0},
                   {"batch:k2", "v2", 0}
                 ])

        assert "v1" == Router.get(ctx, "batch:k1")
        assert "v2" == Router.get(ctx, "batch:k2")

        assert {:ok, [:ok, :ok]} = WARaftBackend.write_delete_batch(0, ["batch:k1", "batch:k2"])
        assert nil == Router.get(ctx, "batch:k1")
        assert nil == Router.get(ctx, "batch:k2")
      end

      test "generic batches flatten nested hot batch terms before apply", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert {:ok, [:ok, :ok, :ok, :ok]} =
                 WARaftBackend.write_batch(0, [
                   {:put_batch, [{"nested-batch:k1", "v1", 0}, {"nested-batch:k2", "v2", 0}]},
                   {:delete_batch, ["nested-batch:k1"]},
                   {:put, "nested-batch:k3", "v3", 0}
                 ])

        assert nil == Router.get(ctx, "nested-batch:k1")
        assert "v2" == Router.get(ctx, "nested-batch:k2")
        assert "v3" == Router.get(ctx, "nested-batch:k3")

        log = waraft_segment_log_record(0)
        index = :ferricstore_waraft_spike_segment_log.last_index(log)

        assert {:ok,
                %{
                  "nested-batch:k2" => "v2",
                  "nested-batch:k3" => "v3"
                }} =
                 Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                   ctx,
                   0,
                   {:waraft_segment, index},
                   ["nested-batch:k1", "nested-batch:k2", "nested-batch:k3"]
                 )
      end

      test "single-member WARaft externalizes large put batches to blob refs", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        payload = :binary.copy("large-waraft-value", 20_000)
        assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes

        assert {:ok, [:ok]} = WARaftBackend.write_put_batch(0, [{"blob:large", payload, 0}])
        assert payload == Router.get(ctx, "blob:large")

        assert [{_, nil, 0, _lfu, fid, _off, value_size}] =
                 :ets.lookup(elem(ctx.keydir_refs, 0), "blob:large")

        assert value_size == byte_size(payload)

        assert {:ok, encoded_ref} =
                 Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                   ctx,
                   0,
                   fid,
                   "blob:large"
                 )

        assert BlobRef.encoded_size?(byte_size(encoded_ref))
        assert {:ok, %BlobRef{size: ^value_size}} = BlobRef.decode(encoded_ref)
      end

      test "single-member WARaft fails closed when blob preparation raises", %{ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        payload = :binary.copy("large-waraft-prepare-fails", 20_000)
        assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes
        parent = self()
        handler_id = {__MODULE__, :blob_prepare_failed, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :blob_prepare_failed],
          &__MODULE__.handle_blob_prepare_failed_telemetry/4,
          parent
        )

        write_hook = fn _io, _iodata ->
          raise RuntimeError, "blob write failed before raft submit"
        end

        previous_write_hook = Application.get_env(:ferricstore, :blob_store_write_hook)
        Process.put(:ferricstore_blob_store_write_hook, write_hook)
        Application.put_env(:ferricstore, :blob_store_write_hook, write_hook)

        on_exit(fn ->
          :telemetry.detach(handler_id)
          Process.delete(:ferricstore_blob_store_write_hook)
          restore_env(:blob_store_write_hook, previous_write_hook)
        end)

        assert {:error,
                {:blob_prepare_failed, {RuntimeError, "blob write failed before raft submit"}}} =
                 WARaftBackend.write_put_batch(0, [{"blob:prepare-fails", payload, 0}])

        assert_receive {:waraft_blob_prepare_failed,
                        [:ferricstore, :waraft, :blob_prepare_failed], %{count: 1},
                        %{
                          shard_index: 0,
                          reason: {RuntimeError, "blob write failed before raft submit"},
                          command_shape: :put_batch
                        }}

        assert nil == Router.get(ctx, "blob:prepare-fails")
      end

      test "restart reopens real Bitcask state at the last durable apply position", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "restart:k", "restart:v", 0})
        assert "restart:v" == Router.get(ctx, "restart:k")

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)

        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "restart:v" == Router.get(restarted_ctx, "restart:k")
        assert {:ok, position} = WARaftBackend.storage_position(0)
        assert elem(position, 1) >= 2
      end

      @tag :shard_kill
    end
  end
end
