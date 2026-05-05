defmodule Ferricstore.Cluster.DataSyncTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Cluster.DataSync

  test "directory copy includes nested Flow LMDB directory" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_data_sync_#{System.unique_integer([:positive])}")

    source = Path.join(root, "source")
    target = Path.join(root, "target")
    lmdb_dir = Path.join(source, "flow_lmdb")

    on_exit(fn -> File.rm_rf!(root) end)

    File.mkdir_p!(lmdb_dir)
    File.write!(Path.join(source, "00000.log"), "bitcask")
    File.write!(Path.join(source, "flow_lmdb_replay_safe.index"), "123\n")
    File.write!(Path.join(lmdb_dir, "data.mdb"), "lmdb-data")
    File.write!(Path.join(lmdb_dir, "lock.mdb"), "lmdb-lock")

    assert :ok = DataSync.copy_directory_from(node(), source, node(), target)

    assert File.read!(Path.join(target, "00000.log")) == "bitcask"
    assert File.read!(Path.join(target, "flow_lmdb_replay_safe.index")) == "123\n"
    assert File.read!(Path.join([target, "flow_lmdb", "data.mdb"])) == "lmdb-data"
    assert File.read!(Path.join([target, "flow_lmdb", "lock.mdb"])) == "lmdb-lock"
  end
end
