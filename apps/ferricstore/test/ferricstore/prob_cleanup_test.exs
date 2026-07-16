defmodule Ferricstore.ProbCleanupTest do
  use ExUnit.Case, async: true

  alias Ferricstore.ProbCleanup

  test "rejects a nonpositive shard count without deleting sidecars" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_prob_cleanup_#{System.unique_integer([:positive])}"
      )

    paths =
      for shard <- [0, -1] do
        path = Path.join([root, "data", "shard_#{shard}", "prob", "sidecar"])
        File.mkdir_p!(Path.dirname(path))
        File.write!(path, "keep")
        path
      end

    on_exit(fn -> File.rm_rf!(root) end)

    assert {:error, :invalid_shard_count} = ProbCleanup.flush_all(root, 0)
    assert Enum.all?(paths, &File.exists?/1)
  end
end
