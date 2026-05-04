defmodule Ferricstore.Merge.ManifestTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias Ferricstore.Merge.Manifest

  setup do
    dir = Path.join(System.tmp_dir!(), "manifest_test_#{:erlang.unique_integer([:positive])}")
    File.mkdir_p!(dir)

    on_exit(fn ->
      File.rm_rf(dir)
    end)

    %{dir: dir}
  end

  describe "write/2" do
    test "writes a manifest file that can be read back", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1, 2, 3]}
      assert :ok = Manifest.write(dir, plan)
      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.shard_index == 0
      assert read_plan.input_file_ids == [1, 2, 3]
      assert is_integer(read_plan.started_at)
      assert read_plan.version == 1
    end

    test "overwrites existing manifest", %{dir: dir} do
      plan1 = %{shard_index: 0, input_file_ids: [1, 2]}
      plan2 = %{shard_index: 0, input_file_ids: [3, 4, 5]}

      assert :ok = Manifest.write(dir, plan1)
      assert :ok = Manifest.write(dir, plan2)

      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.input_file_ids == [3, 4, 5]
    end

    test "stores started_at timestamp", %{dir: dir} do
      before = System.system_time(:millisecond)
      plan = %{shard_index: 1, input_file_ids: [10]}
      assert :ok = Manifest.write(dir, plan)
      after_write = System.system_time(:millisecond)

      assert {:ok, read_plan} = Manifest.read(dir)
      assert read_plan.started_at >= before
      assert read_plan.started_at <= after_write
    end

    test "reports tmp cleanup failure when manifest write fails", %{dir: dir} do
      tmp_path = Path.join(dir, "merge_manifest.bin.tmp")
      File.mkdir!(tmp_path)

      log =
        capture_log(fn ->
          assert {:error, _reason} = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1]})
        end)

      assert log =~ "Failed to remove merge manifest tmp file"
    end
  end

  describe "read/1" do
    test "returns :none when no manifest exists", %{dir: dir} do
      assert :none = Manifest.read(dir)
    end

    test "returns {:error, :corrupt_manifest} for invalid binary", %{dir: dir} do
      path = Path.join(dir, "merge_manifest.bin")
      File.write!(path, "not a valid erlang term")
      assert {:error, :corrupt_manifest} = Manifest.read(dir)
    end
  end

  describe "delete/1" do
    test "removes the manifest file", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1]}
      assert :ok = Manifest.write(dir, plan)
      assert Manifest.exists?(dir)

      assert :ok = Manifest.delete(dir)
      refute Manifest.exists?(dir)
      assert :none = Manifest.read(dir)
    end

    test "returns :ok when no manifest exists", %{dir: dir} do
      assert :ok = Manifest.delete(dir)
    end

    test "reports directory fsync failure after removing the manifest", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1]}
      assert :ok = Manifest.write(dir, plan)

      Process.put(:ferricstore_merge_manifest_fsync_dir_hook, fn ^dir ->
        {:error, :eio}
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_merge_manifest_fsync_dir_hook)
      end)

      assert {:error, {:fsync_dir_failed, :delete_manifest, :eio}} = Manifest.delete(dir)
      refute Manifest.exists?(dir)
    end
  end

  describe "exists?/1" do
    test "returns false when no manifest", %{dir: dir} do
      refute Manifest.exists?(dir)
    end

    test "returns true after write", %{dir: dir} do
      plan = %{shard_index: 0, input_file_ids: [1]}
      assert :ok = Manifest.write(dir, plan)
      assert Manifest.exists?(dir)
    end
  end

  describe "recover_if_needed/2" do
    test "returns :ok when no manifest exists", %{dir: dir} do
      assert :ok = Manifest.recover_if_needed(dir, 0)
    end

    test "cleans up manifest and partial temp files", %{dir: dir} do
      # Simulate input files.
      File.write!(Path.join(dir, "00000000000000000001.log"), "input1")
      File.write!(Path.join(dir, "00000000000000000002.log"), "input2")

      # Simulate partial output from a crashed merge.
      File.write!(Path.join(dir, "compact_1.log"), "partial_output")

      # Write manifest indicating files 1 and 2 were being merged.
      plan = %{shard_index: 0, input_file_ids: [1, 2]}
      assert :ok = Manifest.write(dir, plan)

      # Run recovery.
      assert :ok = Manifest.recover_if_needed(dir, 0)

      # Manifest should be deleted.
      refute Manifest.exists?(dir)

      # Input files should still exist.
      assert File.exists?(Path.join(dir, "00000000000000000001.log"))
      assert File.exists?(Path.join(dir, "00000000000000000002.log"))

      # Partial output file should be cleaned up.
      refute File.exists?(Path.join(dir, "compact_1.log"))
    end

    test "handles corrupt manifest gracefully", %{dir: dir} do
      path = Path.join(dir, "merge_manifest.bin")
      File.write!(path, "garbage data that is not a valid erlang term")

      assert :ok = Manifest.recover_if_needed(dir, 0)
      refute Manifest.exists?(dir)
    end

    test "leaves unrelated files untouched during recovery", %{dir: dir} do
      # Input file.
      File.write!(Path.join(dir, "00000000000000000005.log"), "input")

      # File that is NOT greater than max input id (should be untouched).
      File.write!(Path.join(dir, "00000000000000000003.log"), "older_file")

      plan = %{shard_index: 0, input_file_ids: [5]}
      assert :ok = Manifest.write(dir, plan)

      assert :ok = Manifest.recover_if_needed(dir, 0)

      # Both files should still exist (only files > max_input_id=5 are removed).
      assert File.exists?(Path.join(dir, "00000000000000000005.log"))
      assert File.exists?(Path.join(dir, "00000000000000000003.log"))
    end

    test "preserves active log files newer than merge inputs", %{dir: dir} do
      # The merge scheduler writes the manifest before compaction starts.
      # If the node crashes in that window, the active file is newer than
      # the selected non-active input files and must survive recovery.
      File.write!(Path.join(dir, "00000000000000000001.log"), "input")
      File.write!(Path.join(dir, "00000000000000000002.log"), "active_data")

      assert :ok = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1]})

      assert :ok = Manifest.recover_if_needed(dir, 0)

      assert File.exists?(Path.join(dir, "00000000000000000001.log"))
      assert File.read!(Path.join(dir, "00000000000000000002.log")) == "active_data"
      refute Manifest.exists?(dir)
    end
  end

  # -------------------------------------------------------------------
  # Manifest with new canonical directory layout (spec 2B.4)
  # -------------------------------------------------------------------

  describe "manifest with canonical data/shard_N/ paths" do
    test "manifest writes to correct shard data directory under data/" do
      root =
        Path.join(System.tmp_dir!(), "manifest_layout_#{:erlang.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      :ok = Ferricstore.DataDir.ensure_layout!(root, 4)

      shard_dir = Ferricstore.DataDir.shard_data_path(root, 1)
      assert shard_dir == Path.join([root, "data", "shard_1"])

      plan = %{shard_index: 1, input_file_ids: [10, 20, 30]}
      assert :ok = Manifest.write(shard_dir, plan)

      # Manifest should exist inside data/shard_1/, not at the root.
      assert Manifest.exists?(shard_dir)
      refute File.exists?(Path.join(root, "merge_manifest.bin"))

      {:ok, read_plan} = Manifest.read(shard_dir)
      assert read_plan.shard_index == 1
      assert read_plan.input_file_ids == [10, 20, 30]
    end

    test "manifest does not leak to other shard directories" do
      root =
        Path.join(System.tmp_dir!(), "manifest_isolation_#{:erlang.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      :ok = Ferricstore.DataDir.ensure_layout!(root, 4)

      shard0_dir = Ferricstore.DataDir.shard_data_path(root, 0)
      shard1_dir = Ferricstore.DataDir.shard_data_path(root, 1)

      plan = %{shard_index: 0, input_file_ids: [1, 2]}
      assert :ok = Manifest.write(shard0_dir, plan)

      # Shard 0 has the manifest; shard 1 does not.
      assert Manifest.exists?(shard0_dir)
      refute Manifest.exists?(shard1_dir)
    end

    test "manifest cleanup works with canonical paths" do
      root =
        Path.join(System.tmp_dir!(), "manifest_cleanup_#{:erlang.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      :ok = Ferricstore.DataDir.ensure_layout!(root, 4)

      shard_dir = Ferricstore.DataDir.shard_data_path(root, 2)

      plan = %{shard_index: 2, input_file_ids: [5, 6]}
      assert :ok = Manifest.write(shard_dir, plan)
      assert Manifest.exists?(shard_dir)

      assert :ok = Manifest.delete(shard_dir)
      refute Manifest.exists?(shard_dir)
    end

    test "interrupted merge recovery finds manifest in canonical path" do
      root =
        Path.join(System.tmp_dir!(), "manifest_recovery_#{:erlang.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      :ok = Ferricstore.DataDir.ensure_layout!(root, 4)

      shard_dir = Ferricstore.DataDir.shard_data_path(root, 3)

      # Simulate an interrupted merge: write manifest + partial temp output.
      File.write!(Path.join(shard_dir, "00000000000000000001.log"), "input_data")
      File.write!(Path.join(shard_dir, "compact_1.log"), "partial_output")

      plan = %{shard_index: 3, input_file_ids: [1]}
      assert :ok = Manifest.write(shard_dir, plan)

      # Recovery should clean up.
      assert :ok = Manifest.recover_if_needed(shard_dir, 3)

      refute Manifest.exists?(shard_dir)
      # Input file preserved.
      assert File.exists?(Path.join(shard_dir, "00000000000000000001.log"))
      # Partial output removed.
      refute File.exists?(Path.join(shard_dir, "compact_1.log"))
    end

    test "canonical shard path wins even if stale root shard directory exists" do
      root =
        Path.join(System.tmp_dir!(), "manifest_canonical_#{:erlang.unique_integer([:positive])}")

      on_exit(fn -> File.rm_rf(root) end)

      stale_root_shard = Path.join(root, "shard_0")
      File.mkdir_p!(stale_root_shard)

      :ok = Ferricstore.DataDir.ensure_layout!(root, 4)

      shard_dir = Ferricstore.DataDir.shard_data_path(root, 0)
      assert shard_dir == Path.join([root, "data", "shard_0"])

      plan = %{shard_index: 0, input_file_ids: [7, 8]}
      assert :ok = Manifest.write(shard_dir, plan)

      {:ok, read_plan} = Manifest.read(shard_dir)
      assert read_plan.input_file_ids == [7, 8]
      refute Manifest.exists?(stale_root_shard)

      assert :ok = Manifest.delete(shard_dir)
      refute Manifest.exists?(shard_dir)
    end
  end

  describe "durability" do
    test "delete/1 fsyncs the directory after removing the manifest", %{dir: dir} do
      # Write then delete — after delete, neither the manifest file
      # nor any lingering tmp should be visible. The dir fsync inside
      # delete/1 is what makes the removal survive a kernel panic.
      plan = %{shard_index: 0, input_file_ids: [1, 2]}
      :ok = Manifest.write(dir, plan)
      assert Manifest.exists?(dir)

      assert :ok = Manifest.delete(dir)

      refute Manifest.exists?(dir),
             "delete/1 must remove the manifest from the dir listing"

      # Manifest.delete on an already-missing manifest is a no-op :ok.
      assert :ok = Manifest.delete(dir)
    end

    test "recover_if_needed removes partial-output temp files and fsyncs the dir", %{dir: dir} do
      # Simulate an interrupted merge: manifest exists listing inputs
      # [1, 2], plus a stale temp file left from the prior crashed merge attempt.
      :ok = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1, 2]})
      File.touch!(Path.join(dir, "00001.log"))
      File.touch!(Path.join(dir, "00002.log"))
      stale = Path.join(dir, "compact_1.log")
      File.touch!(stale)

      Manifest.recover_if_needed(dir, 0)

      refute File.exists?(stale),
             "recover_if_needed must remove partial-output files"

      refute Manifest.exists?(dir),
             "recover_if_needed must also delete the manifest"

      # Inputs remain.
      assert File.exists?(Path.join(dir, "00001.log"))
      assert File.exists?(Path.join(dir, "00002.log"))
    end

    test "recover_if_needed keeps manifest when partial-output cleanup fsync fails", %{dir: dir} do
      :ok = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1, 2]})
      File.touch!(Path.join(dir, "compact_1.log"))

      Process.put(:ferricstore_merge_manifest_fsync_dir_hook, fn ^dir ->
        {:error, :eio}
      end)

      on_exit(fn ->
        Process.delete(:ferricstore_merge_manifest_fsync_dir_hook)
      end)

      assert {:error, {:fsync_dir_failed, :cleanup_partial_output, :eio}} =
               Manifest.recover_if_needed(dir, 0)

      assert Manifest.exists?(dir),
             "manifest must remain until cleanup removals are durable"
    end

    test "recover_if_needed keeps manifest when removing partial output fails", %{dir: dir} do
      :ok = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1, 2]})
      stale_dir = Path.join(dir, "compact_1.log")
      File.mkdir!(stale_dir)

      assert {:error, {:remove_partial_failed, ^stale_dir, {_kind, _message}}} =
               Manifest.recover_if_needed(dir, 0)

      assert File.dir?(stale_dir)

      assert Manifest.exists?(dir),
             "manifest must remain until all partial output removals succeed"
    end

    test "recover_if_needed reports partial-output list failures", %{dir: dir} do
      :ok = Manifest.write(dir, %{shard_index: 0, input_file_ids: [1, 2]})

      Process.put(:ferricstore_merge_manifest_before_cleanup_hook, fn ->
        File.rm_rf!(dir)
        File.write!(dir, "not a directory")
      end)

      try do
        assert {:error, {:cleanup_partial_output_list_failed, ^dir, {_kind, _message}}} =
                 Manifest.recover_if_needed(dir, 0)
      after
        Process.delete(:ferricstore_merge_manifest_before_cleanup_hook)
      end
    end
  end
end
