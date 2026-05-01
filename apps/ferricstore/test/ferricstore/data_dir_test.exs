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
end
