defmodule Ferricstore.Raft.RuntimeOffsetScanTest do
  use ExUnit.Case, async: false
  alias :ferricstore_waraft_spike_segment_log, as: SegmentLog

  setup do
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)
    parent = Path.join(System.tmp_dir!(), "runtime-offset-#{System.pid()}-#{suffix}")
    root = Path.join(parent, "apply_projection_log")
    dir = Path.join(root, "segment_log")
    on_exit(fn -> File.rm_rf!(parent) end)
    %{root: root, dir: dir}
  end

  test "fallback crosses read windows and resolves the latest merged projection frame", %{
    root: root,
    dir: dir
  } do
    old = String.duplicate("a", 180_000)
    padding = String.duplicate("p", 220_000)
    new = String.duplicate("b", 160_000)

    assert :ok =
             SegmentLog.write_projection_batches_sync(String.to_charlist(root), [
               {{:raft_log_pos, 1, 0}, [{"old", old, 0}]},
               {{:raft_log_pos, 2, 0}, [{"padding", padding, 0}]}
             ])

    assert :ok =
             SegmentLog.write_projection_batches_sync(String.to_charlist(root), [
               {{:raft_log_pos, 1, 0}, [{"new", new, 0}]}
             ])

    assert {:ok, expected} = SegmentLog.location_for_index(String.to_charlist(root), 1)
    force_fallback(dir)
    assert {:ok, ^expected} = SegmentLog.location_for_index(String.to_charlist(root), 1)
    {_ordinal, offset, size} = expected

    assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, entries}}} =
             SegmentLog.read_disk_at(String.to_charlist(root), 1, offset, size)

    assert {"old", old, 0} in entries
    assert {"new", new, 0} in entries
    assert :not_found = SegmentLog.location_for_index(String.to_charlist(root), 3)
  end

  test "fallback rejects a later corrupt frame even after finding the wanted projection", %{
    root: root,
    dir: dir
  } do
    assert :ok =
             SegmentLog.write_projection_batches_sync(String.to_charlist(root), [
               {{:raft_log_pos, 1, 0}, [{"wanted", "valid", 0}]},
               {{:raft_log_pos, 2, 0}, [{"padding", String.duplicate("p", 300_000), 0}]}
             ])

    path = Path.join(dir, "0.seg")
    assert {:ok, {_, offset, _}} = SegmentLog.location_for_index(String.to_charlist(root), 2)
    {:ok, fd} = :file.open(String.to_charlist(path), [:read, :write, :raw, :binary])

    try do
      assert :ok = :file.pwrite(fd, offset + 4, <<0, 0, 0, 0>>)
    after
      :file.close(fd)
    end

    force_fallback(dir)

    assert {:error, {:crc_mismatch, ^offset}} =
             SegmentLog.location_for_index(String.to_charlist(root), 1)

    assert {:error, {:crc_mismatch, ^offset}} =
             SegmentLog.location_for_index(String.to_charlist(root), 3)
  end

  defp force_fallback(dir) do
    dir_key = :erlang.iolist_to_binary(String.to_charlist(dir))
    :ets.match_delete(:ferricstore_waraft_segment_offset_registry, {{dir_key, :_}, :_, :_, :_})
    File.rm!(Path.join(dir, "0.idx"))
  end
end
