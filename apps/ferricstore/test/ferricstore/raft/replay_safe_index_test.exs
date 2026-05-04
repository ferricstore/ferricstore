defmodule Ferricstore.Raft.ReplaySafeIndexTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

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

  test "persist reports tmp cleanup failure" do
    dir = tmp_dir()
    File.mkdir_p!(dir)
    tmp_path = ReplaySafeIndex.path(dir) <> ".tmp"
    File.mkdir!(tmp_path)
    parent = self()
    handler_id = {:replay_safe_index_cleanup_failed, parent, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :raft, :replay_safe_index, :cleanup_failed],
      fn event, measurements, metadata, _config ->
        send(parent, {:cleanup_failed, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      File.rm_rf(dir)
    end)

    log =
      capture_log(fn ->
        assert {:error, _reason} = ReplaySafeIndex.persist(dir, 789)
      end)

    assert log =~ "failed to remove raft replay-safe tmp index"

    assert_receive {:cleanup_failed, [:ferricstore, :raft, :replay_safe_index, :cleanup_failed],
                    %{count: 1}, %{path: ^tmp_path, reason: {_kind, _message}}},
                   1_000
  end

  defp tmp_dir do
    Path.join(System.tmp_dir!(), "replay_safe_index_#{System.unique_integer([:positive])}")
  end
end
