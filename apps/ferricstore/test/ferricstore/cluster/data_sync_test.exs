defmodule Ferricstore.Cluster.DataSyncTest do
  use ExUnit.Case, async: false

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

  test "directory copy streams large files in bounded chunks" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_data_sync_#{System.unique_integer([:positive])}")

    source = Path.join(root, "source")
    target = Path.join(root, "target")
    payload = :binary.copy("x", 2_500_000)
    parent = self()

    on_exit(fn ->
      Process.delete(:ferricstore_data_sync_copy_chunk_hook)
      File.rm_rf!(root)
    end)

    File.mkdir_p!(source)
    File.write!(Path.join(source, "large.blob"), payload)

    Process.put(:ferricstore_data_sync_copy_chunk_hook, fn source_path, target_path, bytes ->
      send(parent, {:copy_chunk, source_path, target_path, bytes})
    end)

    assert :ok = DataSync.copy_directory_from(node(), source, node(), target)
    assert File.read!(Path.join(target, "large.blob")) == payload

    chunks = collect_chunks([])

    assert length(chunks) > 1
    assert Enum.all?(chunks, fn {_source_path, _target_path, bytes} -> bytes <= 1_048_576 end)

    assert Enum.sum(Enum.map(chunks, fn {_source_path, _target_path, bytes} -> bytes end)) ==
             2_500_000
  end

  test "directory copy fsyncs copied files and target directories" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_data_sync_#{System.unique_integer([:positive])}")

    source = Path.join(root, "source")
    target = Path.join(root, "target")
    subdir = Path.join(source, "nested")
    parent = self()

    on_exit(fn ->
      Process.delete(:ferricstore_data_sync_file_sync_hook)
      Process.delete(:ferricstore_data_sync_fsync_dir_hook)
      File.rm_rf!(root)
    end)

    File.mkdir_p!(subdir)
    File.write!(Path.join(source, "root.blob"), "root-payload")
    File.write!(Path.join(subdir, "nested.blob"), "nested-payload")

    Process.put(:ferricstore_data_sync_file_sync_hook, fn path ->
      send(parent, {:file_sync, path})
      :ok
    end)

    Process.put(:ferricstore_data_sync_fsync_dir_hook, fn path ->
      send(parent, {:dir_sync, path})
      :ok
    end)

    assert :ok = DataSync.copy_directory_from(node(), source, node(), target)

    root_blob = Path.join(target, "root.blob")
    nested_blob = Path.join([target, "nested", "nested.blob"])
    copied_nested = Path.join(target, "nested")

    assert_received {:file_sync, ^root_blob}
    assert_received {:file_sync, ^nested_blob}
    assert_received {:dir_sync, ^target}
    assert_received {:dir_sync, ^copied_nested}
  end

  test "shard storage copy includes promoted dedicated data and blob side-channel data" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_data_sync_#{System.unique_integer([:positive])}")

    source = Path.join(root, "source")
    target = Path.join(root, "target")
    source_data = Ferricstore.DataDir.shard_data_path(source, 0)
    source_dedicated = Path.join([source, "dedicated", "shard_0", "hash:abc"])
    source_blob = Path.join([source, "blob", "shard_0", "aa"])

    on_exit(fn -> File.rm_rf!(root) end)

    File.mkdir_p!(source_data)
    File.mkdir_p!(source_dedicated)
    File.mkdir_p!(source_blob)
    File.write!(Path.join(source_data, "00000.log"), "shared")
    File.write!(Path.join(source_dedicated, "00000.log"), "promoted")
    File.write!(Path.join(source_blob, "payload.blob"), "large-payload")

    assert :ok = DataSync.copy_shard_storage_from(node(), source, node(), target, 0)

    assert File.read!(Path.join([target, "data", "shard_0", "00000.log"])) == "shared"

    assert File.read!(Path.join([target, "dedicated", "shard_0", "hash:abc", "00000.log"])) ==
             "promoted"

    assert File.read!(Path.join([target, "blob", "shard_0", "aa", "payload.blob"])) ==
             "large-payload"
  end

  test "blob side-channel files force full resync instead of WAL-only bridge" do
    assert :needs_resync =
             DataSync.__maybe_require_blob_resync_for_test__(:wal_bridgeable, {:ok, true})

    assert :wal_bridgeable =
             DataSync.__maybe_require_blob_resync_for_test__(:wal_bridgeable, {:ok, false})

    assert :needs_resync =
             DataSync.__maybe_require_blob_resync_for_test__(:needs_resync, {:ok, true})
  end

  test "blob side-channel inspection errors fail closed to full resync" do
    assert :needs_resync =
             DataSync.__maybe_require_blob_resync_for_test__(
               :wal_bridgeable,
               {:error, {:ls_failed, :eacces}}
             )
  end

  test "WARaft never uses legacy Ra WAL bridge checks for resync decisions" do
    source = File.read!("lib/ferricstore/cluster/data_sync.ex")

    assert source =~ "RaftBackend.waraft?()"
    assert source =~ "do_needs_resync?"
  end

  test "WARaft data sync fails closed instead of using legacy batcher pause path" do
    previous_backend = Application.get_env(:ferricstore, :raft_backend)
    Application.put_env(:ferricstore, :raft_backend, :waraft)

    ctx = %FerricStore.Instance{data_dir: System.tmp_dir!(), shard_count: 1}

    on_exit(fn ->
      restore_backend(previous_backend)
    end)

    assert {:error, :unsupported_waraft_data_sync} = DataSync.sync_shard(0, node(), ctx)
    assert {:error, :unsupported_waraft_data_sync} = DataSync.retry_sync_shard(0, node(), ctx)
    assert {:error, :unsupported_waraft_data_sync} = DataSync.sync_all_shards(node(), ctx)

    assert {:error, :unsupported_waraft_data_sync} =
             DataSync.__pause_batcher_for_test__(node(), 0)
  end

  test "remote batcher pause failures return error tuples instead of exiting" do
    missing_node = :"missing_data_sync_node@127.0.0.1"

    assert {:error, {:pause_batcher_failed, _reason}} =
             DataSync.__pause_batcher_for_test__(missing_node, 0)
  end

  test "remote shard pause failures return error tuples instead of exiting" do
    missing_node = :"missing_data_sync_node@127.0.0.1"

    assert {:error, {:pause_shard_failed, _reason}} =
             DataSync.__pause_shard_for_test__(missing_node, :missing_shard)
  end

  test "partial cleanup removes target shard data, dedicated data, and blob data" do
    root =
      Path.join(System.tmp_dir!(), "ferricstore_data_sync_#{System.unique_integer([:positive])}")

    source = Path.join(root, "source")
    target = Path.join(root, "target")

    on_exit(fn -> File.rm_rf!(root) end)

    File.mkdir_p!(Ferricstore.DataDir.shard_data_path(source, 0))
    File.mkdir_p!(Path.join([target, "data", "shard_0"]))
    File.mkdir_p!(Path.join([target, "dedicated", "shard_0", "hash:abc"]))
    File.mkdir_p!(Path.join([target, "blob", "shard_0", "aa"]))
    File.write!(Path.join([target, "data", "shard_0", "00000.log"]), "partial")
    File.write!(Path.join([target, "dedicated", "shard_0", "hash:abc", "00000.log"]), "partial")
    File.write!(Path.join([target, "blob", "shard_0", "aa", "payload.blob"]), "partial")

    ctx = %FerricStore.Instance{data_dir: source}

    assert :ok = DataSync.cleanup_partial_sync(0, node(), ctx, target)

    refute File.exists?(Path.join([target, "data", "shard_0"]))
    refute File.exists?(Path.join([target, "dedicated", "shard_0"]))
    refute File.exists?(Path.join([target, "blob", "shard_0"]))
    assert File.exists?(source)
  end

  defp collect_chunks(acc) do
    receive do
      {:copy_chunk, source_path, target_path, bytes} ->
        collect_chunks([{source_path, target_path, bytes} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp restore_backend(nil), do: Application.delete_env(:ferricstore, :raft_backend)
  defp restore_backend(value), do: Application.put_env(:ferricstore, :raft_backend, value)
end
