defmodule Ferricstore.Flow.LMDBMirrorEnqueueRepairTest do
  @moduledoc """
  When the LMDB writer refuses a Flow write's projection (its queue is full),
  the shard's mirror is marked degraded. The writer must also reconcile that
  shard from the keydir once it catches up: otherwise the Flow stays out of
  query rows, catalogs and indexes, and the mirror stays degraded until the
  node restarts.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, LMDBWriter}

  test "a refused projection enqueue is repaired once the writer catches up" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    old_limit = Application.get_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops)

    on_exit(fn ->
      restore_env(:flow_lmdb_writer_max_enqueue_ops, old_limit)
      Ferricstore.Test.IsolatedInstance.checkin(ctx)
    end)

    partition_key = "tenant-refused-enqueue"
    Application.put_env(:ferricstore, :flow_lmdb_writer_max_enqueue_ops, 0)

    assert :ok =
             Ferricstore.Flow.create(ctx, "refused-enqueue",
               type: "refused-enqueue",
               partition_key: partition_key,
               run_at_ms: 1,
               now_ms: 1
             )

    restore_env(:flow_lmdb_writer_max_enqueue_ops, old_limit)
    assert :atomics.get(ctx.flow_lmdb_mirror_enqueue_failures, 1) == 1
    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 1

    assert :ok = LMDBWriter.flush(ctx.name, 0)

    path = LMDB.path(Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0))
    assert {:ok, _row} = LMDB.get(path, Keys.state_key("refused-enqueue", partition_key))
    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 0
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
