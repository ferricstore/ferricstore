defmodule Ferricstore.Flow.LMDBMovedEnvTest do
  @moduledoc """
  A WARaft snapshot install moves a shard's data directory, Flow LMDB mirror
  included, into a backup and puts the snapshot's directory in its place. A
  rename keeps the inode, so an environment cached for the path follows the
  moved files. Anything that reopened the path between the install's release
  and the move used to keep that environment, and every later read and write
  for the shard went to the backup while the live directory stayed empty.
  """
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB

  @moduletag :tmp_dir

  test "a path whose directory was replaced opens the new directory", %{tmp_dir: root} do
    path = Path.join(root, "flow_lmdb")
    moved = Path.join(root, "snapshot_install_backup")

    assert :ok = LMDB.write_batch(path, [{:put, "key", "before"}])
    assert {:ok, "before"} = LMDB.get(path, "key")

    # As an install does: the old directory moves, a new one takes the path.
    File.rename!(path, moved)
    File.mkdir_p!(path)

    assert :not_found = LMDB.get(path, "key")
    assert :ok = LMDB.write_batch(path, [{:put, "key", "after"}])
    assert {:ok, "after"} = LMDB.get(path, "key")
    assert LMDB.env_present?(path)

    # The moved environment keeps what it had and gets nothing new.
    assert {:ok, "before"} = LMDB.get(moved, "key")

    _ = LMDB.release(moved)
    _ = LMDB.release(path)
  end
end
