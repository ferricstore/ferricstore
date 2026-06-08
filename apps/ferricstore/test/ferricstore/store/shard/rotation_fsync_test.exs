defmodule Ferricstore.Store.Shard.RotationFsyncTest do
  @moduledoc """
  Tests that `shard/flush.ex:maybe_rotate_file/1` performs the two
  fsyncs required for crash-safe rotation:

    1. Synchronous `v2_fsync` of the outgoing active file BEFORE
       publishing the new active file — otherwise the last bytes
       written to the old file can be lost if the checkpointer ticks
       on the NEW file next.
    2. `v2_fsync_dir` of the shard dir AFTER `File.touch!(new_path)`
       — otherwise the new filename entry can vanish on kernel panic.

  We verify by telemetry-instrumenting the rotation path (two
  `[:ferricstore, :bitcask, :rotation_fsync]` events emitted with
  `:old_file` and `:new_dir` meta tags). The test triggers rotation
  by filling the active file past `max_active_file_size` and asserts
  both events fire in the correct order.
  """
  use ExUnit.Case, async: false

  describe "maybe_rotate_file fsync ordering" do
    test "rotation emits :old_file fsync BEFORE :new_dir fsync" do
      parent = self()
      handler_id = "rotation-fsync-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:ferricstore, :bitcask, :rotation_fsync],
        fn _evt, _meas, meta, _ -> send(parent, {:rotation_fsync, meta}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      # Exercise maybe_rotate_file directly. We build a minimal state
      # struct with a tiny max_active_file_size so one flush triggers
      # rotation, and call the public `maybe_rotate_file/1`.
      tmp = Path.join(System.tmp_dir!(), "rot_fsync_#{:erlang.unique_integer([:positive])}")
      File.mkdir_p!(tmp)
      on_exit(fn -> File.rm_rf!(tmp) end)

      active_path = Path.join(tmp, "00000.log")
      File.touch!(active_path)

      state = %{
        index: 0,
        shard_data_path: tmp,
        active_file_id: 0,
        active_file_path: active_path,
        active_file_size: 10_000,
        max_active_file_size: 1_024,
        file_stats: %{0 => {10_000, 0}},
        keydir: :ets.new(:rot_keydir, [:public, :set]),
        pending: []
      }

      new_state = Ferricstore.Store.Shard.Flush.maybe_rotate_file(state)

      assert new_state.active_file_id == 1,
             "rotation must bump active_file_id"

      assert new_state.active_file_path != active_path,
             "rotation must change active_file_path"

      assert File.exists?(new_state.active_file_path),
             "new active file must exist on disk"

      Process.sleep(50)
      events = drain_rotation_events([])

      assert Enum.any?(events, fn m -> m[:kind] == :old_file end),
             "expected :old_file rotation_fsync event, got #{inspect(events)}"

      assert Enum.any?(events, fn m -> m[:kind] == :new_dir end),
             "expected :new_dir rotation_fsync event, got #{inspect(events)}"

      # old_file must precede new_dir in emission order
      old_idx = Enum.find_index(events, fn m -> m[:kind] == :old_file end)
      new_idx = Enum.find_index(events, fn m -> m[:kind] == :new_dir end)

      assert old_idx < new_idx,
             "old_file fsync must precede new_dir fsync; got events: #{inspect(events)}"
    end
  end

  defp drain_rotation_events(acc) do
    receive do
      {:rotation_fsync, meta} -> drain_rotation_events([meta | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end
end
