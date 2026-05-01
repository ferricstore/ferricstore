defmodule Ferricstore.Store.Shard.FlushTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Store.DiskPressure
  alias Ferricstore.Store.Shard.Flush

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

    assert ^state = Flush.flush_pending_sync(state)

    assert :atomics.get(ctx.checkpoint_flags, 1) == 1
    assert DiskPressure.under_pressure?(ctx, 0)
  end
end
