defmodule Ferricstore.Raft.WARaftSpikeTest.Sections.CustomDurableSegmentLogStaysQuietTelemetryAppNotStarted do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      test "custom durable segment log stays quiet when telemetry app is not started", %{
        root: root
      } do
        telemetry_started? = telemetry_started?()

        if telemetry_started? do
          Application.stop(:telemetry)
        end

        on_exit(fn ->
          if telemetry_started? do
            Application.ensure_all_started(:telemetry)
          end
        end)

        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        status = :ferricstore_waraft_spike.status()
        term = Keyword.fetch!(status, :current_term)
        view = segment_log_view(status)

        entry =
          {term,
           {make_ref(), {:write, :ferricstore_waraft_spike, "segment:no-telemetry", "value"}}}

        log =
          capture_log(fn ->
            assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])
          end)

        refute log =~ "Failed to lookup telemetry handlers"
      end

      test "custom durable segment log ignores removed sync method config", %{root: root} do
        previous_method = Application.get_env(:ferricstore, :waraft_segment_log_sync_method)

        try do
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)

          Application.put_env(:ferricstore, :waraft_segment_log_sync_method, :bad_sync_method)

          entry =
            {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:bad-sync", "value"}}}

          assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])

          {:log_view, log, _first, _last, _config} = view

          assert {:ok, {_term, _command}} =
                   :ferricstore_waraft_spike_segment_log.get(log, log_view_last(view) + 1)
        after
          restore_env(:waraft_segment_log_sync_method, previous_method)
        end
      end

      test "custom durable segment log preallocates new segments without extending EOF", %{
        root: root
      } do
        previous_records =
          Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        previous_preallocate =
          Application.get_env(:ferricstore, :waraft_segment_log_preallocate_bytes)

        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_preallocate_hook)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)
          view = advance_to_next_segment_boundary!(view, term, 2)

          Application.put_env(:ferricstore, :waraft_segment_log_preallocate_bytes, 4096)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_preallocate_hook,
            {:notify, self()}
          )

          entry =
            {term,
             {make_ref(), {:write, :ferricstore_waraft_spike, "segment:preallocate", "value"}}}

          assert {:ok, _new_view} = :wa_raft_log.append(view, [entry])

          segment_dir = segment_log_dir(root)
          assert_receive {:waraft_segment_log_preallocate, path, 4096}, 1_000
          assert String.starts_with?(path, segment_dir)
          assert Path.extname(path) == ".seg"
          assert File.stat!(path).size < 4096
        after
          restore_env(:waraft_segment_log_records_per_segment, previous_records)
          restore_env(:waraft_segment_log_preallocate_bytes, previous_preallocate)
          restore_env(:waraft_segment_log_preallocate_hook, previous_hook)
        end
      end

      test "custom durable segment log rejects invalid preallocation config", %{
        root: root
      } do
        previous_records =
          Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        previous_preallocate =
          Application.get_env(:ferricstore, :waraft_segment_log_preallocate_bytes)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)
          view = advance_to_next_segment_boundary!(view, term, 2)
          failed_index = log_view_last(view) + 1

          Application.put_env(:ferricstore, :waraft_segment_log_preallocate_bytes, -1)

          entry =
            {term,
             {make_ref(), {:write, :ferricstore_waraft_spike, "segment:bad-preallocate", "value"}}}

          assert {:error, {:bad_segment_preallocate_bytes, -1}} =
                   :wa_raft_log.append(view, [entry])

          {:log_view, log, _first, _last, _config} = view
          assert :not_found = :ferricstore_waraft_spike_segment_log.get(log, failed_index)
        after
          restore_env(:waraft_segment_log_records_per_segment, previous_records)
          restore_env(:waraft_segment_log_preallocate_bytes, previous_preallocate)
        end
      end

      test "custom durable segment log rejects append when new segment preallocation fails", %{
        root: root
      } do
        previous_records =
          Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        previous_preallocate =
          Application.get_env(:ferricstore, :waraft_segment_log_preallocate_bytes)

        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_preallocate_hook)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)
          view = advance_to_next_segment_boundary!(view, term, 2)
          failed_index = log_view_last(view) + 1

          Application.put_env(:ferricstore, :waraft_segment_log_preallocate_bytes, 4096)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_preallocate_hook,
            {:fail_once, self()}
          )

          entry =
            {term,
             {make_ref(),
              {:write, :ferricstore_waraft_spike, "segment:preallocate-failed", "value"}}}

          assert {:error, {:preallocate_hook, path, 4096}} = :wa_raft_log.append(view, [entry])
          assert_receive {:waraft_segment_log_preallocate, ^path, 4096}, 1_000

          {:log_view, log, _first, _last, _config} = view
          assert :not_found = :ferricstore_waraft_spike_segment_log.get(log, failed_index)

          assert :ok = :ferricstore_waraft_spike.stop()
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          assert :not_found = :ferricstore_waraft_spike.storage_get("segment:preallocate-failed")
        after
          restore_env(:waraft_segment_log_records_per_segment, previous_records)
          restore_env(:waraft_segment_log_preallocate_bytes, previous_preallocate)
          restore_env(:waraft_segment_log_preallocate_hook, previous_hook)
        end
      end

      test "custom durable segment log rolls back bytes when append file fsync fails", %{
        root: root
      } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        try do
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          last_before = Keyword.fetch!(status, :log_last)
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)
          segment_dir = segment_log_dir(root)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:fail_once, self()}
          )

          entry =
            {term,
             {make_ref(), {:write, :ferricstore_waraft_spike, "segment:fsync-failed", "value"}}}

          assert {:error, {:file_sync_hook, path}} = :wa_raft_log.append(view, [entry])
          assert String.starts_with?(path, segment_dir)
          assert_receive {:waraft_segment_log_file_sync, ^path}, 1_000

          assert :ok = :ferricstore_waraft_spike.stop()
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          assert :not_found = :ferricstore_waraft_spike.storage_get("segment:fsync-failed")
          assert Keyword.fetch!(:ferricstore_waraft_spike.status(), :log_last) <= last_before + 1
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
        end
      end

      test "custom durable segment log fsyncs one same-segment append group once", %{
        root: root
      } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        try do
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

          entries = [
            {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:group:1", "v1"}}},
            {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:group:2", "v2"}}}
          ]

          assert {:ok, _new_view} = :wa_raft_log.append(view, entries)
          assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
          refute_receive {:waraft_segment_log_file_sync, _path}, 100
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
        end
      end

      test "custom durable segment log rejects new segment when directory fsync fails", %{
        root: root
      } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)

        previous_records =
          Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)
          view = segment_log_view(status)
          view = advance_to_next_segment_boundary!(view, term, 2)
          failed_index = log_view_last(view) + 1

          segment_dir = segment_log_dir(root)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_sync_dir_hook,
            {:fail_once, self()}
          )

          entry =
            {term, {make_ref(), {:write, :ferricstore_waraft_spike, "segment:rejected", "value"}}}

          assert {:error, {:sync_new_segment_dir, {:sync_dir_hook, ^segment_dir}}} =
                   :wa_raft_log.append(view, [entry])

          assert_receive {:waraft_segment_log_sync_dir, ^segment_dir}, 1_000

          {:log_view, log, _first, _last, _config} = view
          assert :not_found = :ferricstore_waraft_spike_segment_log.get(log, failed_index)

          assert :ok = :ferricstore_waraft_spike.stop()
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          assert :not_found = :ferricstore_waraft_spike.storage_get("segment:rejected")

          recovered_view = segment_log_view(:ferricstore_waraft_spike.status())
          {:log_view, recovered_log, _first, recovered_last, _config} = recovered_view

          if recovered_last >= failed_index do
            case :ferricstore_waraft_spike_segment_log.get(recovered_log, failed_index) do
              {:ok,
               {^term, {_ref, {:write, :ferricstore_waraft_spike, "segment:rejected", "value"}}}} ->
                flunk("recovered the rejected segment append")

              _other ->
                :ok
            end
          end
        after
          restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
          restore_env(:waraft_segment_log_records_per_segment, previous_records)
        end
      end

      test "custom durable segment log records a logical trim floor after apply rotation", %{
        root: root
      } do
        previous_interval = Application.get_env(:ferricstore, :raft_max_log_records_per_file)
        previous_keep = Application.get_env(:ferricstore, :raft_max_log_records)

        try do
          Application.put_env(:ferricstore, :raft_max_log_records_per_file, 1)
          Application.put_env(:ferricstore, :raft_max_log_records, 0)

          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

          for i <- 1..8 do
            assert :ok = :ferricstore_waraft_spike.put("trim:k#{i}", "v#{i}")
          end

          assert eventually(fn -> logical_trim_floor(root) > 1 end)

          assert :ok = :ferricstore_waraft_spike.stop()
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          assert {:ok, "v8"} = :ferricstore_waraft_spike.get("trim:k8")
          assert logical_trim_floor(root) > 1
        after
          restore_env(:raft_max_log_records_per_file, previous_interval)
          restore_env(:raft_max_log_records, previous_keep)
        end
      end

      test "custom durable segment log logical trim does not rewrite the live segment directory",
           %{
             root: root
           } do
        previous_interval = Application.get_env(:ferricstore, :raft_max_log_records_per_file)
        previous_keep = Application.get_env(:ferricstore, :raft_max_log_records)

        try do
          Application.put_env(:ferricstore, :raft_max_log_records_per_file, 1)
          Application.put_env(:ferricstore, :raft_max_log_records, 0)

          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

          for i <- 1..20 do
            assert :ok = :ferricstore_waraft_spike.put("logical-trim:k#{i}", "v#{i}")
          end

          assert eventually(fn -> logical_trim_floor(root) > 1 end)
          refute_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}, 200

          assert :ok = :ferricstore_waraft_spike.stop()
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
          assert {:ok, "v20"} = :ferricstore_waraft_spike.get("logical-trim:k20")
        after
          restore_env(:raft_max_log_records_per_file, previous_interval)
          restore_env(:raft_max_log_records, previous_keep)
        end
      end

      test "custom durable segment log startup rolls back an interrupted rewrite marker", %{
        root: root
      } do
        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        assert :ok = :ferricstore_waraft_spike.put("rewrite-marker:seed", "v0")
        assert :ok = :ferricstore_waraft_spike.stop()

        segment_dir = segment_log_dir(root)
        parent = Path.dirname(segment_dir)
        staging = Path.join(parent, "segment_log.rewrite.staging.manual")
        backup = Path.join(parent, "segment_log.rewrite.backup.manual")
        marker = Path.join(parent, "segment_log.rewrite.term")

        File.rm_rf!(staging)
        File.rm_rf!(backup)
        File.rename!(segment_dir, backup)
        File.mkdir_p!(staging)

        File.write!(
          marker,
          :erlang.term_to_binary(%{
            version: 1,
            dir: String.to_charlist(segment_dir),
            staging: String.to_charlist(staging),
            backup: String.to_charlist(backup)
          })
        )

        rewind_spike_storage!(root, ["rewrite-marker:seed"], {:raft_log_pos, 1, 1})

        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        assert {:ok, "v0"} = :ferricstore_waraft_spike.get("rewrite-marker:seed")
        assert File.dir?(segment_dir)
        refute File.exists?(staging)
        refute File.exists?(backup)
        refute File.exists?(marker)
      end

      test "custom durable segment log rejects logical trim when trim floor fsync fails", %{
        root: root
      } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_sync_dir_hook)

        try do
          assert :ok = :ferricstore_waraft_spike.start_segment_log(root)

          status = :ferricstore_waraft_spike.status()
          term = Keyword.fetch!(status, :current_term)

          view =
            status
            |> segment_log_view()
            |> append_entries!([
              {term,
               {make_ref(), {:write, :ferricstore_waraft_spike, "rewrite-cleanup:1", "v1"}}},
              {term, {make_ref(), {:write, :ferricstore_waraft_spike, "rewrite-cleanup:2", "v2"}}}
            ])

          {:log_view, log, _first, _last, _config} = view
          assert :ok = :ferricstore_waraft_spike_segment_log.close_process_writers(log)

          segment_dir = segment_log_dir(root)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_sync_dir_hook,
            {:fail_once, self()}
          )

          assert {:error, {:sync_dir_hook, ^segment_dir}} =
                   :wa_raft_log.trim(view, 2)

          assert_receive {:waraft_segment_log_sync_dir, ^segment_dir}, 1_000
        after
          restore_env(:waraft_segment_log_sync_dir_hook, previous_hook)
        end
      end

      test "custom durable segment log rejects rewrite markers with paths outside log root", %{
        root: root
      } do
        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        assert :ok = :ferricstore_waraft_spike.put("rewrite-marker-path:seed", "v0")
        assert :ok = :ferricstore_waraft_spike.stop()

        segment_dir = segment_log_dir(root)
        parent = Path.dirname(segment_dir)
        marker = Path.join(parent, "segment_log.rewrite.term")
        outside_staging = Path.join(List.to_string(root), "outside_rewrite_staging")
        outside_backup = Path.join(List.to_string(root), "outside_rewrite_backup")

        File.mkdir_p!(outside_staging)
        File.mkdir_p!(outside_backup)
        File.write!(Path.join(outside_staging, "sentinel"), "keep")

        File.write!(
          marker,
          :erlang.term_to_binary(%{
            version: 1,
            dir: String.to_charlist(segment_dir),
            staging: String.to_charlist(outside_staging),
            backup: String.to_charlist(outside_backup)
          })
        )

        assert {:error, reason} = :ferricstore_waraft_spike.start_segment_log(root)
        assert inspect(reason) =~ "bad_rewrite_marker_path"
        assert File.exists?(Path.join(outside_staging, "sentinel"))
        assert File.dir?(outside_backup)
      end

      test "custom durable segment log replays entries when storage lags the log", %{root: root} do
        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        assert :ok = :ferricstore_waraft_spike.put("replay:k1", "v1")
        assert :ok = :ferricstore_waraft_spike.put("replay:k2", "v2")
        assert {:ok, "v2"} = :ferricstore_waraft_spike.get("replay:k2")

        status = :ferricstore_waraft_spike.status()
        assert Keyword.fetch!(status, :log_last) >= 3

        assert :ok = :ferricstore_waraft_spike.stop()
        rewind_spike_storage!(root, ["replay:k1", "replay:k2"], {:raft_log_pos, 1, 1})

        assert :ok = :ferricstore_waraft_spike.start_segment_log(root)
        assert {:ok, "v1"} = :ferricstore_waraft_spike.get("replay:k1")
        assert {:ok, "v2"} = :ferricstore_waraft_spike.get("replay:k2")
      end

      test "stalled member can install a WARaft snapshot", %{root: root} do
        source_root = Path.join(List.to_string(root), "source")
        target_root = Path.join(List.to_string(root), "target")
        File.mkdir_p!(source_root)
        File.mkdir_p!(target_root)

        assert :ok = :ferricstore_waraft_spike.start_segment_log(String.to_charlist(source_root))
        assert :ok = :ferricstore_waraft_spike.put("snap:k1", "v1")
        assert {:ok, "v1"} = :ferricstore_waraft_spike.get("snap:k1")

        assert {:ok, {:raft_log_pos, index, term} = position} =
                 :ferricstore_waraft_spike.create_snapshot()

        snapshot_path =
          Path.join([
            source_root,
            "ferricstore_waraft_spike.1",
            "snapshot.#{index}.#{term}"
          ])

        assert File.dir?(snapshot_path)
        assert :ok = :ferricstore_waraft_spike.stop()

        assert :ok =
                 :ferricstore_waraft_spike.start_cluster_member_segment_log(
                   String.to_charlist(target_root)
                 )

        assert :ok =
                 :ferricstore_waraft_spike.install_snapshot(
                   String.to_charlist(snapshot_path),
                   position
                 )

        assert {:ok, "v1"} = :ferricstore_waraft_spike.storage_get("snap:k1")
      end

      @tag :cluster
      test "three peer nodes can bootstrap and commit a batched quorum write" do
        unless Ferricstore.Test.ClusterHelper.peer_available?() do
          flunk(":peer is required for WARaft cluster spike test")
        end

        ensure_distribution!()

        unique = :erlang.unique_integer([:positive])
        nodes = start_waraft_peers(unique, 3)

        on_exit(fn ->
          Enum.each(nodes, fn node ->
            try do
              :peer.stop(node.peer)
            catch
              _, _ -> :ok
            end

            File.rm_rf(node.data_dir)
          end)
        end)

        names = Enum.map(nodes, & &1.name)

        for left <- names, right <- names, left != right do
          :rpc.call(left, Node, :connect, [right])
        end

        for node <- nodes do
          assert :ok =
                   :rpc.call(node.name, :ferricstore_waraft_spike, :start_cluster_member, [
                     String.to_charlist(node.data_dir)
                   ])
        end

        for node <- names do
          assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
        end

        assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])

        leader = wait_for_waraft_leader(names)

        assert :ok =
                 :rpc.call(leader, :ferricstore_waraft_spike, :put_many, [
                   [{"cluster:k1", "v1"}, {"cluster:k2", "v2"}]
                 ])

        assert {:ok, "v1"} = :rpc.call(leader, :ferricstore_waraft_spike, :get, ["cluster:k1"])
      end

      @tag :cluster
      test "three peer nodes can commit through the custom segment log" do
        unless Ferricstore.Test.ClusterHelper.peer_available?() do
          flunk(":peer is required for WARaft cluster spike test")
        end

        ensure_distribution!()

        unique = :erlang.unique_integer([:positive])
        nodes = start_waraft_peers(unique, 3)

        on_exit(fn ->
          Enum.each(nodes, fn node ->
            try do
              :peer.stop(node.peer)
            catch
              _, _ -> :ok
            end

            File.rm_rf(node.data_dir)
          end)
        end)

        names = Enum.map(nodes, & &1.name)

        for left <- names, right <- names, left != right do
          :rpc.call(left, Node, :connect, [right])
        end

        for node <- nodes do
          assert :ok =
                   :rpc.call(
                     node.name,
                     :ferricstore_waraft_spike,
                     :start_cluster_member_segment_log,
                     [
                       String.to_charlist(node.data_dir)
                     ]
                   )
        end

        for node <- names do
          assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
        end

        assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])

        leader = wait_for_waraft_leader(names)

        assert :ok =
                 :rpc.call(leader, :ferricstore_waraft_spike, :put_many, [
                   [{"cluster-segment:k1", "v1"}, {"cluster-segment:k2", "v2"}]
                 ])

        assert {:ok, "v1"} =
                 :rpc.call(leader, :ferricstore_waraft_spike, :get, ["cluster-segment:k1"])

        assert Enum.any?(nodes, fn node ->
                 Path.wildcard(
                   Path.join(node.data_dir, "ferricstore_waraft_spike.1/segment_log/*.seg")
                 ) != []
               end)
      end

      @tag :cluster
      test "three peer nodes can commit a dynamic membership removal" do
        unless Ferricstore.Test.ClusterHelper.peer_available?() do
          flunk(":peer is required for WARaft cluster spike test")
        end

        ensure_distribution!()

        unique = :erlang.unique_integer([:positive])
        nodes = start_waraft_peers(unique, 3)

        on_exit(fn ->
          Enum.each(nodes, fn node ->
            try do
              :peer.stop(node.peer)
            catch
              _, _ -> :ok
            end

            File.rm_rf(node.data_dir)
          end)
        end)

        names = Enum.map(nodes, & &1.name)

        for left <- names, right <- names, left != right do
          :rpc.call(left, Node, :connect, [right])
        end

        for node <- nodes do
          assert :ok =
                   :rpc.call(node.name, :ferricstore_waraft_spike, :start_cluster_member, [
                     String.to_charlist(node.data_dir)
                   ])
        end

        for node <- names do
          assert :ok = :rpc.call(node, :ferricstore_waraft_spike, :bootstrap_cluster, [names])
        end

        assert :ok = :rpc.call(hd(names), :ferricstore_waraft_spike, :trigger_election, [])
        leader = wait_for_waraft_leader(names)
        removed = Enum.find(names, &(&1 != leader))
        removed_peer = {:raft_server_ferricstore_waraft_spike_1, removed}

        assert {:ok, {:raft_log_pos, _, _}} =
                 :rpc.call(leader, :ferricstore_waraft_spike, :adjust_membership, [
                   :remove,
                   removed_peer
                 ])

        assert eventually(fn ->
                 membership = :rpc.call(leader, :ferricstore_waraft_spike, :membership, [])
                 is_list(membership) and removed_peer not in membership
               end)
      end
    end
  end
end
