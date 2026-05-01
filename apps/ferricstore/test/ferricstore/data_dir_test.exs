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
end
