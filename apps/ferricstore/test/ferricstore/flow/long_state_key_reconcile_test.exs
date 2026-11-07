defmodule Ferricstore.Flow.LongStateKeyReconcileTest do
  @moduledoc """
  Index reverse rows keep a Flow's state key raw up to 256 bytes and a digest
  of it beyond that. A state key is the Flow id plus a 52-byte partition
  prefix, so ids over 204 bytes have digest reverse keys, and reconciling the
  shard must still find their owners.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{Keys, LMDB, LMDBRebuilder, LMDBWriter}

  test "reconciling a shard keeps Flows whose state keys are longer than 256 bytes" do
    ctx = Ferricstore.Test.IsolatedInstance.checkout(shard_count: 1)
    on_exit(fn -> Ferricstore.Test.IsolatedInstance.checkin(ctx) end)

    partition = "long-state-key"
    active_id = String.duplicate("a", 250)
    done_id = String.duplicate("d", 250)
    assert byte_size(Keys.state_key(active_id, partition)) > 256

    for id <- [active_id, done_id] do
      assert :ok =
               Ferricstore.Flow.create(ctx, id,
                 type: "long-state-key",
                 partition_key: partition,
                 run_at_ms: 1,
                 now_ms: 1
               )
    end

    assert {:ok, [claimed]} =
             Ferricstore.Flow.claim_due(ctx, "long-state-key",
               partition_key: partition,
               worker: "w",
               limit: 1,
               now_ms: 2
             )

    assert :ok =
             Ferricstore.Flow.complete(ctx, claimed.id, claimed.lease_token,
               partition_key: partition,
               fencing_token: claimed.fencing_token,
               now_ms: 3
             )

    assert :ok = LMDBWriter.flush(ctx.name, 0)

    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, 0)
    path = LMDB.path(shard_path)
    done_state_key = Keys.state_key(claimed.id, partition)
    assert {:ok, _terminal_key} = LMDB.get(path, LMDB.terminal_by_state_key_key(done_state_key))

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               elem(ctx.keydir_refs, 0),
               0,
               ctx,
               nil,
               nil,
               nil,
               nil
             )

    assert :atomics.get(ctx.flow_lmdb_mirror_degraded, 1) == 0
    assert {:ok, _terminal_key} = LMDB.get(path, LMDB.terminal_by_state_key_key(done_state_key))
  end
end
