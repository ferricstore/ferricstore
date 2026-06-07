defmodule Ferricstore.Raft.WARaftBackendTest.Sections.Part11 do
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

  test "startup finalizes interrupted snapshot swap after metadata persisted", %{root: root} do
    source_root = Path.join(root, "finalize-source")
    target_root = Path.join(root, "finalize-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize", "new", 0})
    assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

    snapshot_path =
      Path.join([
        source_root,
        "waraft",
        "ferricstore_waraft_backend.1",
        "snapshot.#{index}.#{term}"
      ])

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(source_ctx.name)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize", "old", 0})
    assert :ok = WARaftBackend.write(0, {:put, "snapshot:finalize:target-only", "drop", 0})
    assert "old" == Router.get(target_ctx, "snapshot:finalize")
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    backup_root = Path.join(root_dir, "snapshot_install_backup.finalize")
    staging_root = Path.join(root_dir, "snapshot_install_staging.finalize")

    File.rm_rf!(backup_root)
    File.rm_rf!(staging_root)
    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    for {kind, dest} <- shard_dir_specs(target_ctx, 0) do
      backup = Path.join(backup_root, Atom.to_string(kind))
      source = Path.join(snapshot_path, Atom.to_string(kind))

      File.mkdir_p!(Path.dirname(backup))
      File.rename!(dest, backup)
      {:ok, _copied} = File.cp_r(source, dest)
    end

    marker = %{
      version: 1,
      snapshot_position: position,
      backup_root: backup_root,
      staging_root: staging_root
    }

    File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

    metadata_path = Path.join(root_dir, "ferricstore_storage.term")

    stale_journal_metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, {:raft_log_pos, index + 100, term})

    storage_metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, position)
      |> Map.put(:snapshot_boundary_position, position)

    snapshot_projection = Path.join(snapshot_path, "segment_projection_log")
    target_projection = Path.join(root_dir, "segment_projection_log")

    if File.exists?(snapshot_projection) do
      File.rm_rf!(target_projection)
      {:ok, _copied} = File.cp_r(snapshot_projection, target_projection)
    end

    File.write!(metadata_path, :erlang.term_to_binary(storage_metadata))

    write_storage_metadata_journal!(
      waraft_storage_metadata_journal_path(target_root, 0),
      stale_journal_metadata
    )

    FerricStore.Instance.cleanup(target_ctx.name)
    recovered_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(recovered_ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert "new" == Router.get(recovered_ctx, "snapshot:finalize")
    assert nil == Router.get(recovered_ctx, "snapshot:finalize:target-only")
    assert {:ok, ^position} = WARaftBackend.storage_position(0)

    assert nil ==
             latest_storage_metadata_journal(waraft_storage_metadata_journal_path(target_root, 0))

    refute File.exists?(Path.join(root_dir, "snapshot_install.term"))
    refute File.exists?(backup_root)
  end

  test "startup keeps snapshot install marker when finalize cleanup fails", %{root: root} do
    target_root = Path.join(root, "finalize-cleanup-fails")
    File.mkdir_p!(target_root)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:cleanup-fails", "value", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.cleanup_fails")
    staging_root = Path.join(root_dir, "snapshot_install_staging.cleanup_fails")

    metadata =
      root_dir
      |> Path.join("ferricstore_storage.term")
      |> File.read!()
      |> :erlang.binary_to_term([:safe])

    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    marker = %{
      version: 1,
      snapshot_position: Map.fetch!(metadata, :position),
      backup_root: backup_root,
      staging_root: staging_root
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_cleanup_hook)

    try do
      Application.put_env(:ferricstore, :waraft_snapshot_cleanup_hook, fn
        {:remove, :backup, ^backup_root} -> {:error, :injected_cleanup_failure}
        _event -> :ok
      end)

      FerricStore.Instance.cleanup(target_ctx.name)
      recovered_ctx = build_ctx(target_root)

      assert {:error, reason} =
               WARaftBackend.start(recovered_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert inspect(reason) =~ "injected_cleanup_failure"
      assert File.exists?(marker_path)
    after
      restore_env(:waraft_snapshot_cleanup_hook, previous_hook)
    end
  end

  test "snapshot install blocks storage when final cleanup fails after metadata persists", %{
    root: root,
    ctx: ctx
  } do
    snapshot_path = Path.join(root, "finalize-fails-live-snapshot")
    position = {:raft_log_pos, 10, 1}
    File.mkdir_p!(snapshot_path)

    File.write!(
      Path.join(snapshot_path, "ferricstore_snapshot.term"),
      :erlang.term_to_binary(%{
        version: 1,
        position: position,
        label: nil,
        config: nil,
        payload_dirs: [:data, :blob, :prob],
        empty_payload_dirs: [:data, :blob, :prob]
      })
    )

    handle = %{
      ctx: ctx,
      shard_index: 0,
      root_dir: Path.join([root, "waraft", "ferricstore_waraft_backend.1"]),
      sm_state: nil,
      position: {:raft_log_pos, 2, 1},
      label: nil,
      config: nil
    }

    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_cleanup_hook)
    parent = self()
    handler_id = {__MODULE__, :snapshot_finalize_storage_blocked, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :storage_blocked],
      &__MODULE__.handle_test_telemetry/4,
      parent
    )

    try do
      Application.put_env(:ferricstore, :waraft_snapshot_cleanup_hook, fn
        {:remove, :backup, _path} -> {:error, :injected_finalize_cleanup_failure}
        _event -> :ok
      end)

      assert {:ok, new_handle} =
               Ferricstore.Raft.WARaftStorage.open_snapshot(snapshot_path, position, handle)

      assert {:finalize_snapshot_install_failed,
              {:snapshot_cleanup_hook, :injected_finalize_cleanup_failure}} =
               Map.fetch!(new_handle, :blocked_error)

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :snapshot_install_finalize_failure,
                        reason:
                          {:finalize_snapshot_install_failed,
                           {:snapshot_cleanup_hook, :injected_finalize_cleanup_failure}},
                        attempted_position: ^position,
                        shard_index: 0
                      }},
                     500

      assert {{:error, {:storage_blocked, _reason}}, ^new_handle} =
               Ferricstore.Raft.WARaftStorage.apply(
                 {:put, "snapshot:after-finalize-failure", "unsafe", 0},
                 {:raft_log_pos, 11, 1},
                 new_handle
               )

      assert File.exists?(Path.join(handle.root_dir, "snapshot_install.term"))
    after
      :telemetry.detach(handler_id)
      restore_env(:waraft_snapshot_cleanup_hook, previous_hook)
    end
  end

  test "startup fails closed on oversized snapshot install marker", %{root: root} do
    target_root = Path.join(root, "oversized-install-marker")
    File.mkdir_p!(target_root)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:marker-too-large", "value", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.too_large")
    staging_root = Path.join(root_dir, "snapshot_install_staging.too_large")

    metadata =
      root_dir
      |> Path.join("ferricstore_storage.term")
      |> File.read!()
      |> :erlang.binary_to_term([:safe])

    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    marker = %{
      version: 1,
      snapshot_position: Map.fetch!(metadata, :position),
      backup_root: backup_root,
      staging_root: staging_root,
      label: :binary.copy("x", 1_048_576)
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    FerricStore.Instance.cleanup(target_ctx.name)
    recovered_ctx = build_ctx(target_root)

    assert {:error, reason} =
             WARaftBackend.start(recovered_ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "snapshot_install_marker_file_too_large"
    assert File.exists?(marker_path)
    assert File.exists?(backup_root)
  end

  test "startup fails closed when pending snapshot install metadata is unreadable", %{root: root} do
    source_root = Path.join(root, "unreadable-source")
    target_root = Path.join(root, "unreadable-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)

    assert :ok =
             WARaftBackend.start(source_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:unreadable", "new", 0})
    assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

    snapshot_path =
      Path.join([
        source_root,
        "waraft",
        "ferricstore_waraft_backend.1",
        "snapshot.#{index}.#{term}"
      ])

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(source_ctx.name)

    target_ctx = build_ctx(target_root)

    assert :ok =
             WARaftBackend.start(target_ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:unreadable", "old", 0})
    assert :ok = WARaftBackend.stop()

    root_dir = Path.join([target_root, "waraft", "ferricstore_waraft_backend.1"])
    marker_path = Path.join(root_dir, "snapshot_install.term")
    backup_root = Path.join(root_dir, "snapshot_install_backup.unreadable")
    staging_root = Path.join(root_dir, "snapshot_install_staging.unreadable")

    File.rm_rf!(backup_root)
    File.rm_rf!(staging_root)
    File.mkdir_p!(backup_root)
    File.mkdir_p!(staging_root)

    for {kind, dest} <- shard_dir_specs(target_ctx, 0) do
      backup = Path.join(backup_root, Atom.to_string(kind))
      source = Path.join(snapshot_path, Atom.to_string(kind))

      File.mkdir_p!(Path.dirname(backup))
      File.rename!(dest, backup)
      {:ok, _copied} = File.cp_r(source, dest)
    end

    marker = %{
      version: 1,
      snapshot_position: position,
      backup_root: backup_root,
      staging_root: staging_root
    }

    File.write!(marker_path, :erlang.term_to_binary(marker))

    metadata_path = Path.join(root_dir, "ferricstore_storage.term")

    storage_metadata =
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
      |> Map.put(:position, position)

    File.write!(metadata_path, :erlang.term_to_binary(storage_metadata))
    File.chmod!(metadata_path, 0)

    try do
      FerricStore.Instance.cleanup(target_ctx.name)
      recovered_ctx = build_ctx(target_root)

      assert {:error, _reason} =
               WARaftBackend.start(recovered_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert File.exists?(marker_path)
      assert File.exists?(backup_root)
    after
      _ = File.chmod(metadata_path, 0o600)
    end
  end

  test "startup rejects atom-bearing local pending snapshot install marker without interning atoms",
       %{
         root: root,
         ctx: ctx
       } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    atom_name = "ferricstore_waraft_snapshot_marker_#{System.unique_integer([:positive])}"
    refute existing_atom?(atom_name)

    File.write!(Path.join(root_dir, "snapshot_install.term"), unknown_atom_payload(atom_name))

    assert {:error, reason} =
             WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    assert inspect(reason) =~ "decode_snapshot_install_marker"
    refute existing_atom?(atom_name)
  end

  test "startup rejects pending snapshot install marker paths outside storage root", %{
    root: root,
    ctx: ctx
  } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    outside_staging = Path.join(root, "outside_snapshot_install_staging")
    outside_backup = Path.join(root, "outside_snapshot_install_backup")
    File.mkdir_p!(outside_staging)
    File.mkdir_p!(outside_backup)
    File.write!(Path.join(outside_staging, "sentinel"), "keep")

    marker = %{
      version: 1,
      snapshot_position: {:raft_log_pos, 3, 1},
      staging_root: outside_staging,
      backup_root: outside_backup
    }

    File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))

    assert {:error, reason} =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "bad_snapshot_install_marker"
    assert File.exists?(Path.join(outside_staging, "sentinel"))
    assert File.dir?(outside_backup)
  end

  test "startup fails closed when pending snapshot install marker position differs from metadata",
       %{
         root: root,
         ctx: ctx
       } do
    root_dir = Path.join([root, "waraft", "ferricstore_waraft_backend.1"])
    File.mkdir_p!(root_dir)

    current_position = {:raft_log_pos, 2, 1}
    snapshot_position = {:raft_log_pos, 3, 1}

    marker = %{
      version: 1,
      snapshot_position: snapshot_position,
      staging_root: Path.join(root_dir, "snapshot_install_staging.mismatch"),
      backup_root: Path.join(root_dir, "snapshot_install_backup.mismatch")
    }

    metadata = %{
      version: 1,
      position: current_position,
      label: nil,
      config: nil
    }

    File.write!(Path.join(root_dir, "snapshot_install.term"), :erlang.term_to_binary(marker))
    File.write!(Path.join(root_dir, "ferricstore_storage.term"), :erlang.term_to_binary(metadata))

    assert {:error, reason} =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert inspect(reason) =~ "snapshot_install_position_mismatch"
    assert File.exists?(Path.join(root_dir, "snapshot_install.term"))
  end

  test "snapshot creation excludes writes that arrive while snapshot is in progress", %{
    root: root
  } do
    source_root = Path.join(root, "concurrent-source")
    target_root = Path.join(root, "concurrent-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)
    test_pid = self()
    previous_hook = Application.get_env(:ferricstore, :waraft_snapshot_create_hook)

    try do
      assert :ok =
               WARaftBackend.start(source_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert :ok = WARaftBackend.write(0, {:put, "snapshot:base", "base", 0})

      Application.put_env(:ferricstore, :waraft_snapshot_create_hook, fn
        {:copied, :data} ->
          send(test_pid, {:snapshot_create_paused, self()})

          receive do
            :resume_snapshot_create -> :ok
          after
            2_000 -> {:error, :snapshot_create_hook_timeout}
          end

        _event ->
          :ok
      end)

      snapshot_task = Task.async(fn -> WARaftBackend.create_snapshot(0) end)
      assert_receive {:snapshot_create_paused, storage_pid}, 1_000

      write_task =
        Task.async(fn ->
          result = WARaftBackend.write(0, {:put, "snapshot:late", "late", 0})
          send(test_pid, {:late_write_done, result})
          result
        end)

      refute_receive {:late_write_done, _result}, 50
      send(storage_pid, :resume_snapshot_create)

      assert {:ok, {:raft_log_pos, index, term} = position} = Task.await(snapshot_task, 5_000)
      assert :ok = Task.await(write_task, 5_000)
      assert_receive {:late_write_done, :ok}, 1_000

      snapshot_path =
        Path.join([
          source_root,
          "waraft",
          "ferricstore_waraft_backend.1",
          "snapshot.#{index}.#{term}"
        ])

      assert "late" == Router.get(source_ctx, "snapshot:late")
      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(source_ctx.name)

      target_ctx = build_ctx(target_root)

      assert :ok =
               WARaftBackend.start(target_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 bootstrap: false
               )

      assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
      assert_eventually(fn -> Router.get(target_ctx, "snapshot:base") end, "base")
      assert nil == Router.get(target_ctx, "snapshot:late")
    after
      restore_env(:waraft_snapshot_create_hook, previous_hook)
    end
  end

  test "snapshot install clears stale Flow apply projection cache for target root", %{root: root} do
    source_root = Path.join(root, "cache-install-source")
    target_root = Path.join(root, "cache-install-target")
    File.mkdir_p!(source_root)
    File.mkdir_p!(target_root)

    source_ctx = build_ctx(source_root)
    target_ctx = build_ctx(target_root)
    stale_key = "snapshot:stale-apply-projection-cache"

    assert :ok =
             WARaftBackend.start(source_ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert :ok = WARaftBackend.write(0, {:put, "snapshot:cache-source", "base", 0})
    assert {:ok, {:raft_log_pos, index, term} = position} = WARaftBackend.create_snapshot(0)

    snapshot_path =
      Path.join([
        source_root,
        "waraft",
        "ferricstore_waraft_backend.1",
        "snapshot.#{index}.#{term}"
      ])

    assert :ok = WARaftBackend.stop()
    FerricStore.Instance.cleanup(source_ctx.name)

    clear_apply_projection_cache!()

    assert :ok =
             Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(target_root, 0, 123, [
               {stale_key, "stale", 0}
             ])

    assert apply_projection_cache_rows(target_root, 0) == 1

    assert :ok =
             WARaftBackend.start(target_ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               bootstrap: false
             )

    assert :ok = WARaftBackend.install_snapshot(0, snapshot_path, position)
    assert_eventually(fn -> Router.get(target_ctx, "snapshot:cache-source") end, "base")
    assert apply_projection_cache_rows(target_root, 0) == 0
  end

  test "backend maps WARaft commit backpressure to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               max_pending_low_priority_commits: 0
             )

    assert {:error, :overloaded} = WARaftBackend.write(0, {:put, "blocked:k", "v", 0})
    assert nil == Router.get(ctx, "blocked:k")
  end

  test "backend maps async WARaft commit backpressure to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               max_pending_low_priority_commits: 0,
               max_inflight_commit_bytes: 256
             )

    assert [{:error, :overloaded}] =
             WARaftBackend.write_many([{0, {:put, "blocked:async", "v", 0}}])

    assert nil == Router.get(ctx, "blocked:async")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend async submit does not assert on commit_async after reserving bytes" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert [_, submit_source] =
             String.split(source, "defp submit_acquired_commit_async", parts: 2)

    assert [submit_source, _] = String.split(submit_source, "defp await_commit_async", parts: 2)

    refute submit_source =~ ":ok =\n              :wa_raft_acceptor.commit_async",
           "async submit must not crash after reserving in-flight bytes"

    assert submit_source =~ "release_commit_bytes(shard_index, acquired_bytes)"
    assert source =~ "defp commit_async_safely"
    assert source =~ "catch"
  end

  test "backend sync submit wraps acceptor exits after reserving bytes" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert [_, commit_source] =
             String.split(source, "defp commit(shard_index, command)", parts: 2)

    assert [commit_source, _] =
             String.split(commit_source, "defp submit_commit_async", parts: 2)

    assert commit_source =~ "commit_safely(",
           "sync submit must not leak an acceptor exit after reserving in-flight bytes"

    assert source =~ "defp commit_safely"
    assert source =~ "catch"
  end

  test "backend async await flushes timed-out reply aliases" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert [_, await_source] = String.split(source, "defp await_commit_async", parts: 2)

    assert [await_source, _] =
             String.split(await_source, "defp normalize_commit_transport_result", parts: 2)

    assert await_source =~ "flush_reply_alias(reply_alias, reply_ref)",
           "timed-out async commits must flush late alias replies instead of leaving mailbox junk"

    assert source =~ "defp flush_reply_alias"
    assert source =~ "{^reply_ref, _late_result} -> :ok"
  end

  test "backend maps in-flight byte rejection to Ra-compatible overload", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               max_inflight_commit_bytes: 256
             )

    assert {:error, :overloaded} =
             WARaftBackend.write(0, {:put, "blocked:bytes", String.duplicate("x", 1024), 0})

    assert nil == Router.get(ctx, "blocked:bytes")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend rejects over byte cap before blob side-channel writes", %{ctx: ctx} do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               max_inflight_commit_bytes: 0
             )

    parent = self()
    payload = :binary.copy("blocked-large-blob", 300_000)
    assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes

    Process.put(:ferricstore_blob_store_write_hook, fn _io, _iodata ->
      send(parent, :unexpected_blob_write)
      :ok
    end)

    on_exit(fn -> Process.delete(:ferricstore_blob_store_write_hook) end)

    assert {:error, :overloaded} = WARaftBackend.write(0, {:put, "blocked:blob", payload, 0})
    refute_received :unexpected_blob_write
    assert nil == Router.get(ctx, "blocked:blob")
    assert 0 == WARaftBackend.inflight_commit_bytes(0)
  end

  test "backend maps missing local acceptor to shard unavailable" do
    assert :ok = WARaftBackend.stop()

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write(0, {:put, "stopped:k", "v", 0})

    assert [{:error, "ERR shard not available"}] =
             WARaftBackend.write_many([{0, {:put, "stopped:many", "v", 0}}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_batch(0, [{:put, "stopped:batch", "v", 0}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_put_batch(0, [{"stopped:put-batch", "v", 0}])

    assert {:error, "ERR shard not available"} =
             WARaftBackend.write_delete_batch(0, ["stopped:delete-batch"])
  end

  test "stop clears WARaft partition option cache before a different data dir restart", %{
    ctx: ctx
  } do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    option_key = {:wa_raft_part_sup, :ferricstore_waraft_backend, 1}

    refute :persistent_term.get(option_key, :missing) == :missing

    assert :ok = WARaftBackend.stop()
    assert :persistent_term.get(option_key, :missing) == :missing
  end

  @tag timeout: 20_000
  test "commit timeout emits telemetry with shard and command shape", %{ctx: ctx} do
    handler_id = {__MODULE__, :commit_timeout, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :commit, :timeout],
      &__MODULE__.handle_commit_timeout_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    acceptor = :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, 1)
    :ok = :sys.suspend(acceptor)

    try do
      expected = ErrorReasons.write_timeout_unknown()
      assert ^expected = WARaftBackend.write(0, {:put, "timeout:k", "v", 0})
    after
      _ = :sys.resume(acceptor)
    end

    assert_receive {:waraft_commit_timeout, [:ferricstore, :waraft, :commit, :timeout],
                    %{count: 1, timeout_ms: 10_000, duration_us: duration_us},
                    %{shard_index: 0, command_shape: :put, path: :sync, reason: :timeout}},
                   1_000

    assert duration_us >= 10_000_000
  end

  test "backend admin APIs fail closed when WARaft is stopped" do
    assert :ok = WARaftBackend.stop()

    assert {:error, :backend_unavailable} = WARaftBackend.status(0)
    assert {:error, :backend_unavailable} = WARaftBackend.membership(0)
    assert {:error, :backend_unavailable} = WARaftBackend.storage_position(0)
    assert {:error, :backend_unavailable} = WARaftBackend.create_snapshot(0)
    assert {:error, :backend_unavailable} = WARaftBackend.trigger_election(0)
    assert {:error, :backend_unavailable} = WARaftBackend.peer_ready(0, node())
    assert {:error, "ERR shard not available"} = WARaftBackend.transfer_leadership(0, node())

    assert {:error, :backend_unavailable} =
             WARaftBackend.adjust_membership(0, :add_participant, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.add_participant(0, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.add_member(0, :stopped_target@nohost)

    assert {:error, :backend_unavailable} =
             WARaftBackend.install_snapshot(0, "/tmp/no-snapshot", {:raft_log_pos, 0, 0})

    assert {:error, :backend_unavailable} = WARaftBackend.local_get(0, "stopped:local")
    assert {:error, :backend_unavailable} = WARaftBackend.bootstrap_cluster([node()])
  end

  test "bootstrap fails closed when context exists but WARaft server is missing", %{ctx: ctx} do
    assert :ok = WARaftBackend.stop()

    context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
    :persistent_term.put(context_key, ctx)

    on_exit(fn -> :persistent_term.erase(context_key) end)

    assert {:error, :backend_unavailable} = WARaftBackend.bootstrap_cluster([node()])
  end

  test "membership storage config polling wraps WARaft exits" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert [_, participant_source] = String.split(source, "defp storage_participant?", parts: 2)

    assert [participant_source, member_and_rest] =
             String.split(participant_source, "defp wait_storage_member", parts: 2)

    assert [_, member_source] = String.split(member_and_rest, "defp storage_member?", parts: 2)

    assert [member_source, _rest] =
             String.split(member_source, "defp create_transfer_snapshot", parts: 2)

    assert participant_source =~ "backend_call(fn -> :wa_raft_storage.config(storage) end)"
    assert member_source =~ "backend_call(fn -> :wa_raft_storage.config(storage) end)"
  end

  test "startup promotion wraps WARaft exits" do
    source = Ferricstore.Test.SourceFiles.waraft_backend_source()

    assert [_, finish_source] = String.split(source, "defp finish_start_status", parts: 2)
    assert [finish_source, _rest] = String.split(finish_source, "defp bootstrap", parts: 2)

    assert finish_source =~ "backend_call(fn -> :wa_raft_server.promote(server, :next, true) end)"
  end

    end
  end
end
