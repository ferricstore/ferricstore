defmodule Ferricstore.DataDirTest do
  use ExUnit.Case, async: true

  alias Ferricstore.DataDir

  test "shard_data_path always returns canonical data shard path" do
    root =
      Path.join(System.tmp_dir!(), "data_dir_canonical_#{System.unique_integer([:positive])}")

    legacy = Path.join(root, "shard_0")
    canonical = Path.join([root, "data", "shard_0"])

    try do
      File.mkdir_p!(legacy)
      File.mkdir_p!(canonical)

      assert DataDir.shard_data_path(root, 0) == canonical
    after
      File.rm_rf!(root)
    end
  end

  test "root_from_shard_path accepts only canonical data shard paths" do
    root = Path.join(System.tmp_dir!(), "data_dir_root_#{System.unique_integer([:positive])}")

    assert DataDir.root_from_shard_path(Path.join([root, "data", "shard_2"])) == root

    assert_raise ArgumentError, ~r/canonical/, fn ->
      DataDir.root_from_shard_path(Path.join(root, "shard_2"))
    end
  end

  test "blob_shard_path returns canonical blob shard path and layout creates it" do
    root =
      Path.join(System.tmp_dir!(), "data_dir_blob_#{System.unique_integer([:positive])}")

    try do
      assert DataDir.blob_shard_path(root, 1) == Path.join([root, "blob", "shard_1"])

      assert :ok = DataDir.ensure_layout!(root, 2)
      assert File.dir?(Path.join([root, "blob", "shard_0"]))
      assert File.dir?(Path.join([root, "blob", "shard_1"]))
    after
      File.rm_rf!(root)
    end
  end

  test "ensure_layout reports directory fsync failures for newly created layout" do
    root =
      Path.join(System.tmp_dir!(), "data_dir_fsync_fail_#{System.unique_integer([:positive])}")

    parent = self()

    Process.put(:ferricstore_data_dir_fsync_dir_hook, fn path ->
      send(parent, {:data_dir_fsync, path})
      {:error, :eio}
    end)

    try do
      assert_raise RuntimeError, ~r/DataDir layout fsync failed.*create_root.*:eio/, fn ->
        DataDir.ensure_layout!(root, 1)
      end

      assert_received {:data_dir_fsync, fsync_path}
      assert fsync_path == Path.dirname(root)
    after
      Process.delete(:ferricstore_data_dir_fsync_dir_hook)
      File.rm_rf!(root)
    end
  end

  test "ensure_layout does not fsync on idempotent existing layout" do
    root =
      Path.join(
        System.tmp_dir!(),
        "data_dir_idempotent_fsync_#{System.unique_integer([:positive])}"
      )

    try do
      assert :ok = DataDir.ensure_layout!(root, 1)

      Process.put(:ferricstore_data_dir_fsync_dir_hook, fn path ->
        flunk("did not expect idempotent layout to fsync #{path}")
      end)

      assert :ok = DataDir.ensure_layout!(root, 1)
    after
      Process.delete(:ferricstore_data_dir_fsync_dir_hook)
      File.rm_rf!(root)
    end
  end
end
