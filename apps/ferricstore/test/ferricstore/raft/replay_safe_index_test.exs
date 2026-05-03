defmodule Ferricstore.Raft.ReplaySafeIndexTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.ReplaySafeIndex

  test "persists and reads replay-safe index" do
    dir = tmp_dir()

    assert :ok = ReplaySafeIndex.persist(dir, 123)
    assert ReplaySafeIndex.read(dir) == 123
  end

  test "missing or invalid marker reads as zero" do
    dir = tmp_dir()

    assert ReplaySafeIndex.read(dir) == 0

    File.mkdir_p!(dir)
    File.write!(ReplaySafeIndex.path(dir), "bad\n")

    assert ReplaySafeIndex.read(dir) == 0
  end

  test "persist returns error when marker directory cannot be created" do
    dir = tmp_dir()
    File.write!(dir, "not a directory")

    on_exit(fn -> File.rm(dir) end)

    assert {:error, :enotdir} = ReplaySafeIndex.persist(dir, 456)
    assert ReplaySafeIndex.read(dir) == 0
  end

  defp tmp_dir do
    Path.join(System.tmp_dir!(), "replay_safe_index_#{System.unique_integer([:positive])}")
  end
end
