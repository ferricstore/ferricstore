defmodule Ferricstore.Store.Shard.FlushTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.DiskPressure
  alias Ferricstore.Store.Shard.Flush

  defmodule CaptureScheduler do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))
    end

    @impl true
    def init(opts), do: {:ok, Map.new(opts)}

    @impl true
    def handle_cast(message, %{parent: parent} = state) do
      send(parent, {:scheduler_cast, message})
      {:noreply, state}
    end
  end

  test "flush_pending_sync keeps checkpoint dirty when empty-pending fsync fails" do
    ctx = %{
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false)
    }

    :atomics.put(ctx.checkpoint_flags, 1, 1)

    state = %{
      active_file_path:
        Path.join(
          System.tmp_dir!(),
          "missing_flush_sync_#{System.unique_integer([:positive])}.log"
        ),
      index: 0,
      instance_ctx: ctx,
      pending: []
    }

    flushed = Flush.flush_pending_sync(state)

    assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    assert DiskPressure.under_pressure?(ctx, 0)
    assert Map.fetch!(flushed, :last_flush_error) != nil
  end

  test "maybe_rotate_file does not publish a new active file when old-file fsync fails" do
    Ferricstore.Store.ActiveFile.init(1)

    ctx = %{
      name: :"rotation_fsync_failure_#{System.unique_integer([:positive])}",
      disk_pressure: :atomics.new(1, signed: false)
    }

    dir =
      Path.join(System.tmp_dir!(), "rotation_fsync_failure_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)

    keydir =
      :ets.new(:"rotation_fsync_failure_#{System.unique_integer([:positive])}", [
        :set,
        :public
      ])

    active_path = Path.join(dir, "00000.log")
    new_path = Path.join(dir, "00001.log")

    Ferricstore.Store.ActiveFile.publish(ctx, 0, 0, active_path, dir)

    state = %{
      active_file_id: 0,
      active_file_path: active_path,
      active_file_size: 10_000,
      file_stats: %{0 => {10_000, 0}},
      index: 0,
      instance_ctx: ctx,
      keydir: keydir,
      max_active_file_size: 1_024,
      pending: [],
      shard_data_path: dir
    }

    try do
      assert ^state = Flush.maybe_rotate_file(state)
      assert {0, ^active_path, ^dir} = Ferricstore.Store.ActiveFile.get(ctx, 0)
      refute File.exists?(new_path)
      assert DiskPressure.under_pressure?(ctx, 0)
    after
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end

  test "maybe_rotate_file reports actual file count after file id gaps" do
    Ferricstore.Store.ActiveFile.init(1)

    instance_name = :"rotation_gap_#{System.unique_integer([:positive])}"
    scheduler_name = :"#{instance_name}.Merge.Scheduler.0"
    ctx = %{name: instance_name}
    dir = Path.join(System.tmp_dir!(), "rotation_gap_#{System.unique_integer([:positive])}")

    File.mkdir_p!(dir)
    active_path = Path.join(dir, "00042.log")
    File.write!(active_path, "active")

    keydir = :ets.new(:"rotation_gap_#{System.unique_integer([:positive])}", [:set, :public])
    {:ok, scheduler} = CaptureScheduler.start_link(name: scheduler_name, parent: self())

    state = %{
      active_file_id: 42,
      active_file_path: active_path,
      active_file_size: 10_000,
      file_stats: %{42 => {10_000, 0}},
      index: 0,
      instance_ctx: ctx,
      keydir: keydir,
      max_active_file_size: 1_024,
      pending: [],
      shard_data_path: dir
    }

    try do
      new_state = Flush.maybe_rotate_file(state)

      assert new_state.active_file_id == 43
      assert_receive {:scheduler_cast, {:file_rotated, 2}}, 1_000
    after
      if Process.alive?(scheduler), do: GenServer.stop(scheduler)
      :ets.delete(keydir)
      File.rm_rf!(dir)
    end
  end
end
